// SPDX-License-Identifier: Apache-2.0

use super::policy_runtime::validate_explicit_profile_assignments;
use super::*;

#[derive(Clone, Copy)]
pub(super) struct ExplicitRecheckRestoreState {
    pub(super) was_completed: bool,
    pub(super) was_manually_paused: bool,
}

/// A dropped API request must not strand the torrent in `checking`. The guard
/// schedules the same cancellation cleanup used by pause/stop/move, while the
/// root permit itself is released by the recheck executor's RAII scope.
struct ExplicitRecheckDropGuard {
    runtime: DaemonRuntime,
    hash: InfoHash,
    operation: ExplicitRecheckOperation,
    restore: Option<ExplicitRecheckRestoreState>,
}

impl ExplicitRecheckDropGuard {
    fn new(
        runtime: DaemonRuntime,
        hash: InfoHash,
        operation: ExplicitRecheckOperation,
        restore: ExplicitRecheckRestoreState,
    ) -> Self {
        Self {
            runtime,
            hash,
            operation,
            restore: Some(restore),
        }
    }

    fn disarm(&mut self) {
        self.restore = None;
    }
}

impl Drop for ExplicitRecheckDropGuard {
    fn drop(&mut self) {
        let Some(restore) = self.restore.take() else {
            return;
        };
        self.operation.cancel();
        let runtime = self.runtime.clone();
        let hash = self.hash;
        let operation = self.operation.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                runtime
                    .finish_cancelled_explicit_recheck(hash, operation, restore)
                    .await;
            });
        } else {
            // A daemon operation normally always runs on Tokio. Do not leave
            // a concurrent stop waiting forever if a caller drops it during
            // runtime teardown, even though registry restoration cannot run.
            operation.finish();
        }
    }
}

impl DaemonRuntime {
    #[cfg(test)]
    pub(super) async fn pause_root_control_replacement_after_transition_lock(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.root_control_replacement_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    async fn wait_at_root_control_replacement_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) =
            self.root_control_replacement_pause.lock().await.take()
        {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    #[cfg(test)]
    pub(super) fn inject_generic_config_persistence_failure_after_rename(&self) {
        self.generic_config_fail_after_rename
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn generic_config_persistence_failure_after_rename_injected(&self) -> bool {
        self.generic_config_fail_after_rename
            .swap(false, Ordering::AcqRel)
    }

    #[cfg(test)]
    pub(super) async fn pause_explicit_recheck_before_persist(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.explicit_recheck_before_persist_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    async fn wait_at_explicit_recheck_before_persist_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self
            .explicit_recheck_before_persist_pause
            .lock()
            .await
            .take()
        {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    pub(super) async fn cancel_explicit_recheck(
        &self,
        hash: &InfoHash,
    ) -> Option<ExplicitRecheckOperation> {
        let operation = self.explicit_rechecks.lock().await.get(hash).cloned();
        if let Some(operation) = &operation {
            operation.cancel();
        }
        operation
    }

    async fn finish_explicit_recheck_operation(
        &self,
        hash: InfoHash,
        operation: ExplicitRecheckOperation,
    ) {
        let mut operations = self.explicit_rechecks.lock().await;
        if operations
            .get(&hash)
            .is_some_and(|current| current.is_same_operation(&operation))
        {
            operations.remove(&hash);
        }
        drop(operations);
        operation.finish();
    }

    pub(super) async fn finish_cancelled_explicit_recheck(
        &self,
        hash: InfoHash,
        operation: ExplicitRecheckOperation,
        restore: ExplicitRecheckRestoreState,
    ) {
        let owns_operation = self
            .explicit_rechecks
            .lock()
            .await
            .get(&hash)
            .is_some_and(|current| current.is_same_operation(&operation));
        if !owns_operation {
            operation.finish();
            return;
        }

        let (restored_state, should_persist) = {
            let mut registry = self.registry.lock().await;
            match registry.get_mut(&hash) {
                None => (None, false),
                // Verification may have already finalized the state when the
                // caller is dropped at the first persistence await. Preserve
                // that final state durably instead of treating it as a no-op.
                Some(torrent) if torrent.state != TorrentState::Checking => (None, true),
                Some(torrent) if restore.was_completed => {
                    torrent.containment_recovery_intent = None;
                    if restore.was_manually_paused {
                        torrent.state = TorrentState::Paused;
                        torrent.seeding_status = SeedingStatus::StoppedManual;
                        (Some(TorrentState::Paused), true)
                    } else {
                        torrent.state = TorrentState::Completed;
                        torrent.seeding_status = SeedingStatus::Queued;
                        (Some(TorrentState::Completed), true)
                    }
                }
                Some(torrent) => {
                    torrent.containment_recovery_intent = None;
                    torrent.state = TorrentState::Paused;
                    torrent.seeding_status = SeedingStatus::NotEligible;
                    (Some(TorrentState::Paused), true)
                }
            }
        };

        if should_persist {
            self.persist_state_best_effort("recheck_cancelled").await;
        }
        if let Some(state) = restored_state {
            self.publish_torrent_event("torrent_changed", hash, state);
            self.publish_event(stats_updated_event());
            if state == TorrentState::Completed {
                self.reconcile_seeders().await;
            }
        }
        self.finish_explicit_recheck_operation(hash, operation)
            .await;
    }
}

#[async_trait]
impl DaemonOps for DaemonRuntime {
    async fn list_torrents(&self) -> Vec<TorrentSummary> {
        let config = self.config.read().await.clone();
        let positions: HashMap<InfoHash, usize> = self
            .queue
            .lock()
            .await
            .order
            .iter()
            .enumerate()
            .map(|(i, hash)| (*hash, i + 1))
            .collect();
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        self.registry
            .lock()
            .await
            .list()
            .iter()
            .map(|t| {
                let mut summary = t.to_summary();
                summary.queue_position = positions.get(&t.info_hash()).copied();
                let policy = Self::effective_policy_with_config(&config, t);
                summary.effective_ratio_limit = (!policy.seed_forever.value)
                    .then_some(policy.ratio_limit.value)
                    .flatten();
                summary.effective_idle_limit = (!policy.seed_forever.value)
                    .then_some(policy.idle_limit.value)
                    .flatten();
                summary
            })
            .collect()
    }

    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        let config = self.config.read().await.clone();
        let position = self.queue.lock().await.position(hash);
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        self.registry.lock().await.get(hash).map(|t| {
            let mut summary = t.to_summary();
            summary.queue_position = position;
            let policy = Self::effective_policy_with_config(&config, t);
            summary.effective_ratio_limit = (!policy.seed_forever.value)
                .then_some(policy.ratio_limit.value)
                .flatten();
            summary.effective_idle_limit = (!policy.seed_forever.value)
                .then_some(policy.idle_limit.value)
                .flatten();
            summary
        })
    }

    async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        options: AddTorrentOptions,
    ) -> Result<InfoHash> {
        self.add_torrent_file_with_options(bytes, options).await
    }

    async fn add_magnet(&self, magnet: &str, options: AddTorrentOptions) -> Result<InfoHash> {
        self.add_magnet_with_options(magnet, options).await
    }

    async fn remove_torrent(&self, hash: &InfoHash, delete_data: bool) -> Result<()> {
        let removed = self
            .remove_torrents_with_single_reconcile(vec![*hash], delete_data)
            .await?;
        if removed.is_empty() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        Ok(())
    }

    async fn remove_torrents(
        &self,
        hashes: Vec<InfoHash>,
        delete_data: bool,
    ) -> Result<Vec<InfoHash>> {
        self.remove_torrents_with_single_reconcile(hashes, delete_data)
            .await
    }

    async fn pause(&self, hash: &InfoHash) -> Result<()> {
        // Stop the live engine; the torrent stays in the registry as paused.
        self.stop_engine(hash).await;
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.containment_recovery_intent = None;
                    t.state = TorrentState::Paused;
                    t.seeding_status = if t.progress.is_complete() {
                        SeedingStatus::StoppedManual
                    } else {
                        SeedingStatus::NotEligible
                    };
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        self.queue.lock().await.clear_bypass(hash);
        self.reconcile_queue().await;
        self.persist_state().await?;
        self.publish_torrent_event("torrent_changed", *hash, TorrentState::Paused);
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn resume(&self, hash: &InfoHash) -> Result<()> {
        self.engine_retry_after.write().await.remove(hash);
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.containment_recovery_intent = None;
                    if t.progress.is_complete() {
                        t.state = TorrentState::Completed;
                        t.seeding_status = SeedingStatus::Queued;
                    } else {
                        t.state = TorrentState::Queued;
                        t.seeding_status = SeedingStatus::NotEligible;
                    }
                    t.error = None;
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(*hash);
            queue.start_now(hash);
        }
        self.reconcile_queue().await;
        self.reconcile_seeders().await;
        self.persist_state().await?;
        let state = self
            .registry
            .lock()
            .await
            .get(hash)
            .map(|torrent| torrent.state)
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        self.publish_torrent_event("torrent_changed", *hash, state);
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn start_now(&self, hash: &InfoHash) -> Result<()> {
        let manually_stopped_complete =
            self.registry.lock().await.get(hash).is_some_and(|torrent| {
                torrent.progress.is_complete()
                    && (torrent.state == TorrentState::Paused
                        || torrent.seeding_status == SeedingStatus::StoppedManual)
            });
        if manually_stopped_complete {
            return self.resume(hash).await;
        }
        self.engine_retry_after.write().await.remove(hash);
        {
            let mut reg = self.registry.lock().await;
            if let Some(torrent) = reg.get_mut(hash) {
                torrent.containment_recovery_intent = None;
            } else {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(*hash);
            queue.start_now(hash);
        }
        self.reconcile_queue().await;
        self.persist_state().await?;
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn stop(&self, hash: &InfoHash) -> Result<()> {
        self.pause(hash).await
    }

    async fn recheck(&self, hash: &InfoHash) -> Result<()> {
        self.stop_engine(hash).await;
        let torrent = {
            let reg = self.registry.lock().await;
            reg.get(hash)
                .cloned()
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?
        };
        // A selected-file completion can be `Completed` even when the full
        // piece set is not complete. Keep the logical lifecycle state
        // separate from the physical location decision below, which still
        // follows whether all payload pieces had been completed/moved.
        let payload_in_complete_dir = torrent.progress.is_complete();
        let restore = ExplicitRecheckRestoreState {
            was_completed: matches!(
                torrent.state,
                TorrentState::Completed | TorrentState::Seeding
            ) || payload_in_complete_dir,
            was_manually_paused: torrent.state == TorrentState::Paused
                || torrent.seeding_status == SeedingStatus::StoppedManual,
        };
        let complete_dir = self.resolve_download_dir(&torrent).await;
        let storage_dir = if payload_in_complete_dir {
            complete_dir
        } else {
            self.resolve_incomplete_dir_for(&torrent).await
        };
        let operation = ExplicitRecheckOperation::new();
        let mut drop_guard =
            ExplicitRecheckDropGuard::new(self.clone(), *hash, operation.clone(), restore);
        self.explicit_rechecks
            .lock()
            .await
            .insert(*hash, operation.clone());
        let marked_checking = {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.containment_recovery_intent = None;
                    t.state = TorrentState::Checking;
                    t.seeding_status = SeedingStatus::NotEligible;
                    true
                }
                None => false,
            }
        };
        if !marked_checking {
            drop_guard.disarm();
            self.finish_explicit_recheck_operation(*hash, operation)
                .await;
            return Err(CoreError::NotFound("torrent".into()));
        }
        self.publish_torrent_event("torrent_changed", *hash, TorrentState::Checking);
        self.publish_event(stats_updated_event());

        // Run a real storage recheck on disk through the root-scoped executor.
        let cfg = self.config.read().await.clone();
        let metrics = self
            .storage_metrics
            .metrics_for_path(&cfg, Path::new(&storage_dir));
        let storage = storage_io_with_config(
            torrent.meta.clone(),
            std::path::PathBuf::from(&storage_dir),
            &cfg,
        )
        .with_metrics(Some(metrics));
        let cancellation = operation.cancellation();
        match self
            .recheck_storage_under_root_control(&storage, Some(&cancellation))
            .await
        {
            Ok(bf) => {
                if cancellation.is_cancelled() {
                    drop_guard.disarm();
                    self.finish_cancelled_explicit_recheck(*hash, operation, restore)
                        .await;
                    return Ok(());
                }
                let mut final_state = None;
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.progress
                        .replace_from_bitfield(&bf, torrent.meta.piece_count());
                    t.recompute_file_bytes_completed();
                    if torrent_selection_complete(t, &bf)? {
                        t.state = TorrentState::Completed;
                        t.seeding_status = if t.progress.is_complete() {
                            SeedingStatus::Queued
                        } else {
                            SeedingStatus::NotEligible
                        };
                        t.date_completed = Some(now());
                        final_state = Some(TorrentState::Completed);
                    } else if t.state == TorrentState::Checking {
                        t.state = TorrentState::Paused;
                        t.seeding_status = SeedingStatus::NotEligible;
                        final_state = Some(TorrentState::Paused);
                    }
                }
                drop(reg);
                if let Some(state) = final_state {
                    self.publish_torrent_event("torrent_changed", *hash, state);
                    if state == TorrentState::Completed {
                        self.publish_torrent_event("torrent_completed", *hash, state);
                    }
                    self.publish_event(stats_updated_event());
                }
                self.wait_at_explicit_recheck_before_persist_test_pause()
                    .await;
                self.persist_state().await?;
                self.reconcile_seeders().await;
            }
            Err(e) if is_storage_work_cancelled(&e) || cancellation.is_cancelled() => {
                drop_guard.disarm();
                self.finish_cancelled_explicit_recheck(*hash, operation, restore)
                    .await;
                return Ok(());
            }
            Err(e) => {
                drop_guard.disarm();
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.state = TorrentState::StorageError;
                    t.error = Some(e.to_string());
                }
                drop(reg);
                self.publish_torrent_event("torrent_error", *hash, TorrentState::StorageError);
                self.publish_event(stats_updated_event());
                self.persist_state_best_effort("recheck_failed").await;
                self.finish_explicit_recheck_operation(*hash, operation)
                    .await;
                return Err(e);
            }
        }
        drop_guard.disarm();
        self.finish_explicit_recheck_operation(*hash, operation)
            .await;
        Ok(())
    }

    async fn reannounce(&self, hash: &InfoHash) -> Result<()> {
        // If the engine is running, send a reannounce command; otherwise
        // restart the engine which announces on start.
        let tx = self.engine_cmds.lock().await.get(hash).cloned();
        if let Some(tx) = tx {
            let _ = tx.send(EngineCommand::Reannounce).await;
            Ok(())
        } else {
            self.resume(hash).await
        }
    }

    async fn move_data(&self, hash: &InfoHash, path: String) -> Result<()> {
        if path.trim().is_empty() {
            return Err(CoreError::Storage(
                "torrent data destination must not be empty".into(),
            ));
        }
        let storage_ownership = self.storage_ownership_lock.lock().await;
        let torrent = self
            .registry
            .lock()
            .await
            .get(hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        let cfg = self.config.read().await.clone();
        let (old_complete, old_active) = Self::policy_storage_paths_with_config(&cfg, &torrent);
        let mut destination_policy = torrent.clone();
        destination_policy.download_dir = Some(path.clone());
        let (destination_complete, destination_active) =
            Self::policy_storage_paths_with_config(&cfg, &destination_policy);
        self.ensure_storage_paths_available_at_paths_except(
            &torrent.meta,
            &destination_complete,
            &destination_active,
            Some(*hash),
        )
        .await?;
        let was_active = matches!(
            torrent.state,
            TorrentState::Downloading | TorrentState::DownloadingMetadata
        );
        let state_completed = matches!(
            torrent.state,
            TorrentState::Completed | TorrentState::Seeding
        );
        let payload_in_complete = torrent.progress.is_complete();
        self.stop_engine(hash).await;
        let source = if payload_in_complete {
            old_complete
        } else {
            old_active
        };
        let destination = if payload_in_complete {
            destination_complete
        } else {
            destination_active
        };
        let source_path = PathBuf::from(source);
        let storage = storage_io_with_config(torrent.meta.clone(), source_path.clone(), &cfg);
        let moved_storage = match storage.move_to(PathBuf::from(destination)).await {
            Ok(storage) => storage,
            Err(error) => {
                drop(storage_ownership);
                if was_active {
                    self.restart_engine_for_settings(hash).await;
                } else if state_completed {
                    self.reconcile_seeders().await;
                }
                return Err(error);
            }
        };
        if let Some(current) = self.registry.lock().await.get_mut(hash) {
            current.download_dir = Some(path);
        }
        let persist_result = self.persist_state().await;
        let result = if let Err(persist_error) = persist_result {
            match moved_storage.move_to(source_path).await {
                Ok(_) => {
                    if let Some(current) = self.registry.lock().await.get_mut(hash) {
                        current.download_dir = torrent.download_dir.clone();
                    }
                    Err(persist_error)
                }
                Err(rollback_error) => Err(CoreError::Storage(format!(
                    "{persist_error}; data move rollback also failed: {rollback_error}"
                ))),
            }
        } else {
            Ok(())
        };
        drop(storage_ownership);
        if was_active {
            self.restart_engine_for_settings(hash).await;
        } else if state_completed {
            self.reconcile_seeders().await;
        }
        result
    }

    async fn rename_path(
        &self,
        hash: &InfoHash,
        file_index: usize,
        new_path: String,
    ) -> Result<()> {
        let components = validated_relative_path(&new_path)?;
        let storage_ownership = self.storage_ownership_lock.lock().await;
        let torrent = self
            .registry
            .lock()
            .await
            .get(hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        if file_index >= torrent.meta.files.len() {
            return Err(CoreError::NotFound("torrent file".into()));
        }
        let mut renamed_meta = torrent.meta.clone();
        renamed_meta.files[file_index].path = components;
        let cfg = self.config.read().await.clone();
        let (complete_dir, active_dir) = Self::policy_storage_paths_with_config(&cfg, &torrent);
        self.ensure_storage_paths_available_at_paths_except(
            &renamed_meta,
            &complete_dir,
            &active_dir,
            Some(*hash),
        )
        .await?;
        let was_active = matches!(
            torrent.state,
            TorrentState::Downloading | TorrentState::DownloadingMetadata
        );
        let state_completed = matches!(
            torrent.state,
            TorrentState::Completed | TorrentState::Seeding
        );
        let payload_in_complete = torrent.progress.is_complete();
        self.stop_engine(hash).await;
        let complete_dir = self.resolve_download_dir(&torrent).await;
        let storage_dir = if payload_in_complete {
            complete_dir
        } else {
            self.resolve_incomplete_dir_for(&torrent).await
        };
        let old_storage =
            storage_io_with_config(torrent.meta.clone(), PathBuf::from(&storage_dir), &cfg);
        let old_path = old_storage.file_path(file_index)?;
        let new_storage =
            storage_io_with_config(renamed_meta.clone(), PathBuf::from(storage_dir), &cfg);
        let new_file_path = new_storage.file_path(file_index)?;
        if old_path == new_file_path {
            drop(storage_ownership);
            if was_active {
                self.restart_engine_for_settings(hash).await;
            } else if state_completed {
                self.reconcile_seeders().await;
            }
            return Ok(());
        }
        let disk_outcome = match rename_payload_exclusive(&old_path, &new_file_path).await {
            Ok(outcome) => outcome,
            Err(error) => {
                drop(storage_ownership);
                if was_active {
                    self.restart_engine_for_settings(hash).await;
                } else if state_completed {
                    self.reconcile_seeders().await;
                }
                return Err(error);
            }
        };
        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
            torrent.meta = renamed_meta;
            torrent.files[file_index].path = new_path;
        }
        let result = if let Err(persist_error) = self.persist_state().await {
            match rollback_payload_rename(&old_path, &new_file_path, disk_outcome).await {
                Ok(()) => {
                    if let Some(current) = self.registry.lock().await.get_mut(hash) {
                        *current = torrent;
                    }
                    Err(persist_error)
                }
                Err(rollback_error) => Err(CoreError::Storage(format!(
                    "{persist_error}; payload rename rollback also failed: {rollback_error}"
                ))),
            }
        } else {
            Ok(())
        };
        drop(storage_ownership);
        if was_active {
            self.restart_engine_for_settings(hash).await;
        } else if state_completed {
            self.reconcile_seeders().await;
        }
        result
    }

    async fn set_labels(&self, hash: &InfoHash, labels: Vec<String>) -> Result<()> {
        // Labels can select a profile, so hold the same transaction boundary
        // as profile replacement through the durable torrent-state write.
        let _config_transaction = self.config_write_lock.lock().await;
        let config = self.config.read().await.clone();
        let (previous, mode_changed) = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.clone();
            let previous_mode = Self::effective_policy_with_config(&config, &previous)
                .encryption_mode
                .value;
            // Labels can select a profile. Snapshot before changing labels so
            // that selection can affect only live fields for existing data.
            Self::snapshot_initial_admission(&config, torrent);
            Self::snapshot_existing_storage(&config, torrent);
            torrent.labels = labels;
            let next_mode = Self::effective_policy_with_config(&config, torrent)
                .encryption_mode
                .value;
            (previous, previous_mode != next_mode)
        };
        // Profile selection can change as a consequence of label updates.
        // Persist the record before applying its live queue, seeding, and
        // bandwidth effects; a failed write restores the exact old labels.
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                *torrent = previous;
            }
            return Err(error);
        }
        // Label-derived storage is intentionally creation-time only. Existing
        // torrents retain their resolved/snapshotted storage while these live
        // profile fields are re-evaluated.
        self.refresh_profile_runtime_fields().await;
        if mode_changed {
            self.restart_changed_encryption_policy_work(std::slice::from_ref(hash))
                .await;
        }
        self.schedule_reconcile_queue("torrent_labels_changed")
            .await;
        self.reconcile_seeders().await;
        Ok(())
    }

    async fn set_torrent_limits(
        &self,
        hash: &InfoHash,
        limits: swarmotter_core::bandwidth::TorrentBandwidth,
    ) -> Result<()> {
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.download_limit = limits.download;
                    t.upload_limit = limits.upload;
                    t.policy.overrides.download_limit = Some(limits.download);
                    t.policy.overrides.upload_limit = Some(limits.upload);
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        // Apply live through the one retained Arc shared by the downloader and
        // active/queued seeder registration. No task restart is required.
        if let Some(rl) = self.torrent_limiters.read().await.get(hash).cloned() {
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Download,
                limits.download,
            );
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Upload,
                limits.upload,
            );
        }
        self.persist_state().await
    }

    async fn set_torrent_seeding(
        &self,
        hash: &InfoHash,
        seeding: swarmotter_core::ratio::TorrentSeeding,
    ) -> Result<TorrentSummary> {
        if seeding
            .ratio_limit
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(CoreError::InvalidArgument(
                "ratio_limit must be a finite non-negative number or null".into(),
            ));
        }

        // Keep tentative policy invisible to lifecycle reconciliation and API
        // readers until durable replacement succeeds or the prior value is
        // restored. Reconciliation reacquires this lock only after success.
        let lifecycle = self.seeder_lifecycle_lock.lock().await;
        let previous = {
            let mut reg = self.registry.lock().await;
            let torrent = reg
                .get_mut(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.seeding.clone();
            let previous_overrides = torrent.policy.overrides.clone();
            // Persist policy independently of runtime lifecycle. A live
            // registry entry remains Seeding+Active until synchronized
            // reconciliation stops it after the durable write succeeds.
            torrent.seeding = seeding;
            // The native policy setter is a per-torrent override. Preserve a
            // nullable ratio/idle target as inheritance and retain the bool
            // explicitly so `false` can override a seed-forever profile.
            torrent.policy.overrides.ratio_limit = torrent.seeding.ratio_limit;
            torrent.policy.overrides.idle_limit = torrent.seeding.idle_limit;
            torrent.policy.overrides.seed_forever = Some(torrent.seeding.seed_forever);
            (previous, previous_overrides)
        };

        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                torrent.seeding = previous.0;
                torrent.policy.overrides = previous.1;
            }
            return Err(error);
        }

        drop(lifecycle);
        self.reconcile_seeders().await;
        self.get_torrent(hash)
            .await
            .ok_or_else(|| CoreError::NotFound("torrent".into()))
    }

    async fn torrent_policy(
        &self,
        hash: &InfoHash,
    ) -> Option<swarmotter_core::policy::EffectiveTorrentPolicy> {
        let config = self.config.read().await.clone();
        self.registry
            .lock()
            .await
            .get(hash)
            .map(|torrent| Self::effective_policy_with_config(&config, torrent))
    }

    async fn set_torrent_profile(&self, hash: &InfoHash, profile: Option<String>) -> Result<()> {
        self.assign_torrent_profile(hash, profile).await
    }

    async fn set_torrent_encryption_mode(
        &self,
        hash: &InfoHash,
        encryption_mode: Option<swarmotter_core::config::PeerEncryptionMode>,
    ) -> Result<()> {
        self.assign_torrent_encryption_mode(hash, encryption_mode)
            .await
    }

    async fn list_files(&self, hash: &InfoHash) -> Option<Vec<TorrentFile>> {
        self.registry
            .lock()
            .await
            .get(hash)
            .map(|t| t.files.clone())
    }

    async fn set_wanted(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        wanted: bool,
    ) -> Result<()> {
        let should_restart = {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            if file_indices.iter().any(|index| *index >= t.wanted.len()) {
                return Err(CoreError::NotFound("torrent file".into()));
            }
            for i in file_indices {
                t.wanted[i] = wanted;
                t.files[i].wanted = wanted;
            }
            matches!(
                t.state,
                TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
                    | TorrentState::Completed
            )
        };
        self.persist_state().await?;
        if should_restart {
            self.restart_engine_for_settings(hash).await;
        }
        Ok(())
    }

    async fn set_priority(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        priority: FilePriority,
    ) -> Result<()> {
        let should_restart = {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            if file_indices
                .iter()
                .any(|index| *index >= t.priorities.len())
            {
                return Err(CoreError::NotFound("torrent file".into()));
            }
            for i in file_indices {
                t.priorities[i] = priority;
                t.files[i].priority = priority;
            }
            matches!(
                t.state,
                TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
                    | TorrentState::Completed
            )
        };
        self.persist_state().await?;
        if should_restart {
            self.restart_engine_for_settings(hash).await;
        }
        Ok(())
    }

    async fn list_trackers(&self, hash: &InfoHash) -> Option<Vec<TrackerInfo>> {
        // Reflect real per-tracker announce results from the live engine, if
        // present. Success text is kept separate from last_error so the UI and
        // Transmission emulation do not report successful announces as errors.
        let (engine_trackers, engine_scrapes, tracker_interval_seconds) = self
            .engine_states
            .read()
            .await
            .get(hash)
            .and_then(|s| s.try_lock().ok())
            .map(|s| {
                (
                    s.tracker_announces.clone(),
                    s.tracker_scrapes.clone(),
                    s.tracker_interval_seconds,
                )
            })
            .unwrap_or_default();
        self.registry.lock().await.get(hash).map(|t| {
            let mut out = Vec::new();
            let tiers = tracker::announce_tiers(t.meta.announce.as_deref(), &t.meta.announce_list);
            for (tier, urls) in tiers.iter().enumerate() {
                for url in urls {
                    let mut info = make_tracker(url, tier);
                    if let Some(snapshot) = engine_trackers.get(url) {
                        info.status = snapshot.status;
                        info.seeders = snapshot.seeders;
                        info.leechers = snapshot.leechers;
                        info.downloads = snapshot.downloads;
                        info.last_error = snapshot.last_error.clone();
                        info.last_message = snapshot.last_message.clone();
                        info.last_announce = snapshot.last_announce;
                        info.next_announce = snapshot
                            .last_announce
                            .map(|last| last.saturating_add(tracker_interval_seconds.max(30)));
                    }
                    if let Some(scrape) = engine_scrapes.get(url) {
                        info.scrape_status = scrape.status;
                        info.last_scrape = scrape.last_scrape;
                        info.scrape_seeders = scrape.seeders;
                        info.scrape_leechers = scrape.leechers;
                        info.scrape_downloads = scrape.downloads;
                        info.last_scrape_error = scrape.last_error.clone();
                        // Preserve successful announce counts as the primary
                        // compatibility view. When announce has not succeeded,
                        // fall back to the separately retained scrape counts.
                        if !matches!(info.status, TrackerStatus::Working | TrackerStatus::Ok) {
                            info.seeders = scrape.seeders.unwrap_or(info.seeders);
                            info.leechers = scrape.leechers.unwrap_or(info.leechers);
                        }
                        info.downloads = scrape.downloads.unwrap_or(info.downloads);
                    }
                    out.push(info);
                }
            }
            out
        })
    }

    async fn add_tracker(&self, hash: &InfoHash, url: String) -> Result<()> {
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.is_none() {
                    t.meta.announce = Some(url);
                } else {
                    t.meta.announce_list.push(vec![url]);
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
    }

    async fn remove_tracker(&self, hash: &InfoHash, url: String) -> Result<()> {
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.as_deref() == Some(&url) {
                    t.meta.announce = None;
                }
                t.meta.announce_list.retain_mut(|tier| {
                    tier.retain(|u| u != &url);
                    !tier.is_empty()
                });
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
    }

    async fn edit_tracker(&self, hash: &InfoHash, old_url: String, new_url: String) -> Result<()> {
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.as_deref() == Some(&old_url) {
                    t.meta.announce = Some(new_url);
                } else {
                    for tier in t.meta.announce_list.iter_mut() {
                        for u in tier.iter_mut() {
                            if *u == old_url {
                                *u = new_url.clone();
                            }
                        }
                    }
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
    }

    async fn list_peers(&self, hash: &InfoHash) -> Option<Vec<Peer>> {
        // Manual bans are a global operator action, and this read-only lookup
        // intentionally does not call `PeerFilter::admit_ip`: rendering peer
        // rows must not increment admission/audit counters.
        let manual_ban_ips = {
            let config = self.config.read().await;
            config
                .peer_filter
                .manual_bans
                .iter()
                .filter_map(|ban| ban.ip.trim().parse::<std::net::IpAddr>().ok())
                .collect::<HashSet<_>>()
        };
        let states = self.engine_states.read().await;
        let state = states.get(hash)?;
        let s = state.lock().await;
        let peers = s
            .peers
            .iter()
            .map(|pa| Peer {
                address: pa.socket_addr().to_string(),
                ip: pa.ip,
                port: pa.port,
                direction: swarmotter_core::models::peer::PeerDirection::Outbound,
                client: None,
                progress: 0.0,
                rate_down: 0,
                rate_up: 0,
                flags: swarmotter_core::models::peer::PeerFlags::default(),
                banned: manual_ban_ips.contains(&pa.ip),
            })
            .collect();
        Some(peers)
    }

    async fn peer_filter_status(&self) -> swarmotter_core::peer_filter::PeerFilterStatus {
        self.peer_filter.read().await.status()
    }

    async fn replace_peer_filter(
        &self,
        peer_filter: swarmotter_core::peer_filter::PeerFilterConfig,
    ) -> Result<swarmotter_core::peer_filter::PeerFilterStatus> {
        let _config_transaction = self.config_write_lock.lock().await;
        let mut next = self.config.read().await.clone();
        next.peer_filter = peer_filter;
        self.apply_peer_filter_mutation_locked(next).await
    }

    async fn ban_peer(
        &self,
        hash: &InfoHash,
        ban: swarmotter_core::peer_filter::ManualPeerBan,
    ) -> Result<swarmotter_core::peer_filter::PeerFilterStatus> {
        let _config_transaction = self.config_write_lock.lock().await;
        if self.registry.lock().await.get(hash).is_none() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        let ip = ban.ip.trim().parse::<std::net::IpAddr>().map_err(|error| {
            CoreError::InvalidArgument(format!("manual peer ban IP '{}': {error}", ban.ip))
        })?;
        let ban = swarmotter_core::peer_filter::ManualPeerBan {
            ip: ip.to_string(),
            reason: ban.reason.map(|reason| reason.trim().to_string()),
        };
        let mut next = self.config.read().await.clone();
        // A manual operator ban must take effect rather than being retained in
        // a disabled policy section. It is global by design, so future
        // candidate sources and inbound sessions receive it as well.
        next.peer_filter.enabled = true;
        if let Some(existing) = next.peer_filter.manual_bans.iter_mut().find(|existing| {
            existing
                .ip
                .trim()
                .parse::<std::net::IpAddr>()
                .is_ok_and(|current| current == ip)
        }) {
            *existing = ban;
        } else {
            next.peer_filter.manual_bans.push(ban);
        }
        self.apply_peer_filter_mutation_locked(next).await
    }

    async fn unban_peer(
        &self,
        hash: &InfoHash,
        ip: String,
    ) -> Result<swarmotter_core::peer_filter::PeerFilterStatus> {
        let _config_transaction = self.config_write_lock.lock().await;
        if self.registry.lock().await.get(hash).is_none() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        self.remove_global_manual_ban_locked(ip).await
    }

    async fn unban_global_peer(
        &self,
        ip: String,
    ) -> Result<swarmotter_core::peer_filter::PeerFilterStatus> {
        let _config_transaction = self.config_write_lock.lock().await;
        self.remove_global_manual_ban_locked(ip).await
    }

    async fn queue_move_up(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_up(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }
    async fn queue_move_down(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_down(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }
    async fn queue_move_to_top(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_to_top(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }
    async fn queue_move_to_bottom(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_to_bottom(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }

    async fn get_config(&self) -> Config {
        self.config.read().await.clone()
    }

    async fn update_settings(&self, patch: swarmotter_api::state::SettingsPatch) -> Result<()> {
        let _config_transaction = self.config_write_lock.lock().await;
        let previous = self.config.read().await.clone();
        let mut next = previous.clone();
        if let Some(bandwidth) = patch.bandwidth {
            next.bandwidth = bandwidth;
        }
        if let Some(queue) = patch.queue {
            next.queue = queue;
        }
        if let Some(seeding) = patch.seeding {
            next.seeding = seeding;
        }
        if let Some(autopilot) = patch.autopilot {
            next.autopilot = autopilot;
        }
        next.validate()?;

        if peer_limits_changed(&previous, &next) {
            let peer_permits = self.build_peer_permit_configuration(&next).await?;
            self.apply_peer_budget_runtime_update(next, peer_permits, None, false)
                .await?;
        } else {
            *self.config.write().await = next;
            self.apply_runtime_config_fields().await;
        }
        self.notify_port_mapping_reconcile();
        self.publish_event(Event::new("settings_changed", json!({})));
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn replace_config(&self, mut next: Config) -> Result<ConfigUpdateResult> {
        let _config_transaction = self.config_write_lock.lock().await;
        // A binder blocks the gate synchronously and queues teardown details.
        // Drain that report before replacement validation so stale listeners
        // cannot make an otherwise-correct explicit recovery look occupied.
        if !self.containment_gate.traffic_allowed() {
            self.network_health_tick().await;
        }
        let (previous, config_path) = {
            let cfg = self.config.read().await;
            (cfg.clone(), self.config_path.clone())
        };
        if next.api.auth_token.is_none() {
            next.api.auth_token = previous.api.auth_token.clone();
        }
        if next.network.socks5.password.is_none()
            && next.network.socks5.username == previous.network.socks5.username
        {
            next.network.socks5.password = previous.network.socks5.password.clone();
        }
        next.validate()?;
        // Compile the candidate before a config file write or live state
        // mutation. This turns a changed/deleted local blocklist into a
        // clean failed update instead of a partially installed policy.
        let next_peer_filter = Arc::new(swarmotter_core::peer_filter::PeerFilter::from_config(
            &next.peer_filter,
        )?);
        let next_network_health = net::evaluate(&next.network, self.interface_probe.as_ref());
        let recovering_latched_failure = self.bind_failure_latched.read().await.is_some();
        if recovering_latched_failure {
            self.validate_replacement_bind_path(&next).await?;
        }
        if !next.api.require_auth
            && next
                .api
                .bind_address
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|bind| !bind.ip().is_loopback())
        {
            tracing::warn!(
                bind = %next.api.bind_address,
                "configuration update disables API and Web UI authentication on a non-loopback listener; every client that can reach this address can control SwarmOtter"
            );
        }
        let mut torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        // Older records predate durable policy snapshots. Build their
        // candidate migration from the pre-replacement configuration before
        // validation, so a new label/profile mapping cannot redirect old
        // payloads during this same PUT.
        let legacy_policy_migration =
            Self::prepare_legacy_policy_snapshot_migration(&previous, &next, &mut torrents);
        validate_explicit_profile_assignments(&next, &torrents)?;
        validate_storage_config_transition(&previous, &next, &torrents)?;
        // A profile or label-map edit is a live encryption-policy update only
        // for torrents whose resolved value changes. The global mode remains
        // a full data-plane reconfiguration below, so do not rebuild those
        // engines twice.
        let encryption_mode_changes =
            Self::effective_encryption_mode_changes(&previous, &next, &torrents);

        let peer_limits_changed = peer_limits_changed(&previous, &next);
        // A peer-policy replacement needs the same transactional lifecycle as
        // a permit replacement: persist only after the newly compiled policy
        // has reconstructed its sessions, and restore the exact old policy if
        // either reconstruction or persistence fails. Build this fallible
        // candidate before the legacy state migration is made durable.
        let peer_policy_changed = previous.peer_filter != next.peer_filter;
        let next_peer_permits = if peer_limits_changed || peer_policy_changed {
            Some(self.build_peer_permit_configuration(&next).await?)
        } else {
            None
        };
        // Capture this fallible rollback input before installing a legacy
        // state migration. The generic path can then fail only at its
        // explicitly handled persistence boundary.
        let generic_config_file_snapshot = if next_peer_permits.is_none() {
            config_path
                .as_deref()
                .map(capture_config_file)
                .transpose()?
        } else {
            None
        };
        let restart_required_fields = restart_required_fields(&previous, &next);

        if !legacy_policy_migration.is_empty() {
            self.install_legacy_policy_snapshot_migration(&legacy_policy_migration)
                .await?;
            // This state write occurs before either config replacement path.
            // Its own file snapshot protects the post-rename sync error case;
            // if it fails, disk is back on the old generation and only the
            // two migration fields need restoring in memory.
            if let Err(error) = self.persist_state_with_file_rollback().await {
                let runtime_rollback = self
                    .rollback_legacy_policy_snapshot_migration(&legacy_policy_migration)
                    .await;
                return Err(CoreError::Internal(format!(
                    "legacy policy migration persistence failed: {error}; runtime rollback: {runtime_rollback:?}"
                )));
            }
        }

        if let Some(peer_permits) = next_peer_permits {
            if let Err(error) = self
                .apply_peer_budget_runtime_update(
                    next.clone(),
                    peer_permits,
                    config_path.as_deref(),
                    recovering_latched_failure,
                )
                .await
            {
                let state_rollback = self
                    .restore_legacy_policy_snapshot_migration(&legacy_policy_migration)
                    .await;
                return Err(CoreError::Internal(format!(
                    "configuration replacement failed: {error}; legacy policy rollback: {state_rollback:?}"
                )));
            }
        } else {
            // `write_config_bytes_atomically` can report a directory-sync
            // error after its rename has made the candidate visible. Capture
            // the old bytes before this generic transaction touches the file
            // so that failure cannot leave disk on a generation runtime did
            // not install.
            let rebuild_data_plane = data_plane_config_changed(&previous, &next);
            // Root-control replacement has no reason to tear down healthy
            // engines, but it does change the admission decision made while
            // an engine is being constructed. Serialize that decision with
            // the config install so a tightening PUT cannot race a start that
            // reads the old limits after the replacement is committed.
            let root_controls_changed =
                previous.storage.root_controls != next.storage.root_controls;
            let serialize_data_plane_admission = rebuild_data_plane || root_controls_changed;
            let data_plane_transition = if serialize_data_plane_admission {
                Some(self.data_plane_transition_lock.lock().await)
            } else {
                None
            };
            // Persist only after obtaining the same transition lock that
            // protects storage admission. This makes a root-control update's
            // on-disk generation and its live admission decision change at
            // one serialized commit point. A write failure attempts to
            // restore the previous file generation while runtime remains
            // unchanged.
            if let Some(path) = &config_path {
                #[cfg(test)]
                let persisted = if self.generic_config_persistence_failure_after_rename_injected() {
                    write_config_atomically_with_post_rename_sync_failure(path, &next)
                } else {
                    write_config_atomically(path, &next)
                };
                #[cfg(not(test))]
                let persisted = write_config_atomically(path, &next);

                if let Err(error) = persisted {
                    let file_rollback = generic_config_file_snapshot
                        .as_ref()
                        .map_or(Ok(()), |snapshot| restore_config_file(path, snapshot));
                    drop(data_plane_transition);
                    let state_rollback = self
                        .restore_legacy_policy_snapshot_migration(&legacy_policy_migration)
                        .await;
                    return Err(CoreError::Internal(format!(
                        "configuration persistence failed: {error}; configuration rollback: {file_rollback:?}; legacy policy rollback: {state_rollback:?}"
                    )));
                }
            }
            if root_controls_changed {
                self.wait_at_root_control_replacement_test_pause().await;
            }
            if rebuild_data_plane {
                // Snapshot progress before stopping every task created from the old
                // containment policy. No old binder, DHT runner, listener, tracker
                // sidecar, or accepted peer session may survive the config swap.
                self.reconcile_engine_progress_for_transition().await;
                let recovery_intents = if !next_network_health.traffic_allowed
                    && next_network_health.mode != NetworkContainmentMode::Disabled
                {
                    let intents = self.live_containment_recovery_intents().await;
                    self.containment_gate.block(
                        next_network_health.status,
                        next_network_health.detail.clone(),
                    );
                    intents
                } else {
                    HashMap::new()
                };
                let registry_hashes = self
                    .registry
                    .lock()
                    .await
                    .torrents
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                self.stop_all_torrent_tasks(&registry_hashes).await;
                *self.dht_runner.lock().await = None;
                if !recovery_intents.is_empty() {
                    let _lifecycle = self.seeder_lifecycle_lock.lock().await;
                    let mut registry = self.registry.lock().await;
                    for (hash, intent) in recovery_intents {
                        if let Some(torrent) = registry.get_mut(&hash) {
                            torrent.containment_recovery_intent = Some(intent);
                            torrent.state = TorrentState::NetworkBlocked;
                            torrent.error = Some(next_network_health.detail.clone());
                        }
                    }
                }
            }
            {
                let mut cfg = self.config.write().await;
                *cfg = next.clone();
            }
            // Engines/listeners are reconstructed while the transition lock
            // is held, so every newly admitted peer sees this same immutable
            // policy generation as the config snapshot.
            *self.peer_filter.write().await = next_peer_filter;
            self.selfish_completion_enabled
                .store(next.torrent.selfish, Ordering::Release);
            drop(data_plane_transition);
            if recovering_latched_failure {
                *self.bind_failure_latched.write().await = None;
                let health = net::evaluate(&next.network, self.interface_probe.as_ref());
                if health.traffic_allowed {
                    self.recover_containment_work(health).await;
                } else {
                    self.transition_data_plane_to_blocked(health.status, health.detail)
                        .await;
                }
            }
            self.apply_runtime_config_fields().await;
            if !rebuild_data_plane {
                self.restart_changed_encryption_policy_work(&encryption_mode_changes)
                    .await;
            }
        }
        self.notify_port_mapping_reconcile();
        self.publish_event(Event::new("settings_changed", json!({})));
        self.publish_event(stats_updated_event());

        Ok(ConfigUpdateResult {
            persisted: config_path.is_some(),
            config_path: config_path.map(|p| p.display().to_string()),
            restart_required: !restart_required_fields.is_empty(),
            restart_required_fields,
            applied_runtime_fields: vec![
                "bandwidth".into(),
                "queue".into(),
                "seeding".into(),
                "network".into(),
                "torrent.allow_ipv6".into(),
                "torrent.utp_enabled".into(),
                "torrent.utp_prefer_tcp".into(),
                "torrent.listen_port".into(),
                "torrent.encryption_mode".into(),
                "torrent.selfish".into(),
                "peer_filter".into(),
                "port_mapping".into(),
                "dht".into(),
                "storage".into(),
                "watch".into(),
                "autopilot".into(),
            ],
            config: redact_config(next),
        })
    }

    async fn reset_downloads(&self) -> Result<ResetResult> {
        let cfg = self.config.read().await.clone();
        let torrents: Vec<Torrent> = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        tracing::warn!(
            torrents_requested = torrents.len(),
            "download state reset requested by API request"
        );
        let registry_hashes: Vec<InfoHash> = torrents.iter().map(Torrent::info_hash).collect();
        self.stop_all_torrent_tasks(&registry_hashes).await;
        self.clear_download_runtime_state().await;

        let mut storage_paths = Vec::new();
        for torrent in &torrents {
            let complete_dir = self.resolve_download_dir(torrent).await;
            let active_dir = self.resolve_incomplete_dir_for(torrent).await;
            for dir in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
                let storage = storage_io_with_config(torrent.meta.clone(), dir.clone(), &cfg);
                storage.remove_all().await?;
                push_display_path(&mut storage_paths, &dir);
            }
        }

        let download_dir = cfg
            .storage
            .download_dir
            .clone()
            .unwrap_or_else(|| default_download_dir_string(&cfg));
        let incomplete_dir = cfg
            .storage
            .incomplete_dir
            .clone()
            .unwrap_or_else(|| download_dir.clone());
        let mut storage_entries_removed = 0usize;
        for dir in unique_pathbufs([PathBuf::from(incomplete_dir), PathBuf::from(download_dir)]) {
            storage_entries_removed =
                storage_entries_removed.saturating_add(remove_directory_contents(&dir).await?);
            push_display_path(&mut storage_paths, &dir);
        }

        let mut log_paths = Vec::new();
        let mut log_files_cleared = 0usize;
        if let Some(path) = &self.log_file_path {
            truncate_log_file(path).await?;
            log_files_cleared = 1;
            push_display_path(&mut log_paths, path);
        }

        self.clear_download_runtime_state().await;
        self.persist_state().await?;

        tracing::warn!(
            torrents_removed = torrents.len(),
            storage_entries_removed,
            log_files_cleared,
            storage_paths = ?storage_paths,
            log_paths = ?log_paths,
            "download state reset by API request"
        );
        for hash in registry_hashes {
            self.publish_event(torrent_removed_event(hash, true));
        }
        self.publish_event(stats_updated_event());

        Ok(ResetResult {
            torrents_removed: torrents.len(),
            storage_paths,
            storage_entries_removed,
            log_paths,
            log_files_cleared,
        })
    }

    async fn network_health(&self) -> NetworkHealth {
        self.network_health.read().await.clone()
    }

    async fn port_test_status(&self) -> swarmotter_core::port_test::PortTestStatus {
        self.listen_port_test_status().await
    }

    async fn run_port_test(&self) -> swarmotter_core::port_test::PortTestStatus {
        // Native callers intentionally honor the runtime cache. Mapping
        // lifecycle integration may use the crate-visible forced method after
        // a successful renewal without exposing a bypass to API callers.
        self.run_listen_port_test(false).await
    }

    async fn port_mapping_status(&self) -> swarmotter_core::port_mapping::PortMappingStatus {
        DaemonRuntime::port_mapping_status(self).await
    }

    async fn refresh_port_mapping(&self) -> swarmotter_core::port_mapping::PortMappingStatus {
        DaemonRuntime::refresh_port_mapping(self).await
    }

    async fn network_diagnostics(&self) -> NetworkDiagnostics {
        let cfg = self.config.read().await.clone();
        let health = self.network_health.read().await.clone();
        let probe = OsInterfaceProbe;
        let interfaces = probe
            .list()
            .into_iter()
            .map(|iface| {
                let has_ipv4 = iface.addresses.iter().any(std::net::IpAddr::is_ipv4);
                let has_ipv6 = iface.addresses.iter().any(std::net::IpAddr::is_ipv6);
                NetworkInterfaceDiagnostic {
                    selected: cfg.network.required_interface.as_deref()
                        == Some(iface.name.as_str()),
                    name: iface.name,
                    status: format!("{:?}", iface.status).to_ascii_lowercase(),
                    addresses: iface.addresses.iter().map(ToString::to_string).collect(),
                    has_ipv4,
                    has_ipv6,
                }
            })
            .collect();
        let traffic_level = if health.traffic_allowed {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Invalid
        };
        NetworkDiagnostics {
            health: health.clone(),
            listen_port: cfg.torrent.listen_port,
            dht_port: cfg.dht.port,
            torrent_allow_ipv6: cfg.torrent.allow_ipv6,
            utp_enabled: cfg.torrent.utp_enabled,
            utp_prefer_tcp: cfg.torrent.utp_prefer_tcp,
            peer_encryption_mode: cfg.torrent.encryption_mode,
            socks5_enabled: cfg.network.socks5.enabled,
            socks5_udp_blocked: cfg.network.socks5.enabled,
            interfaces,
            checks: vec![
                NetworkPathCheck {
                    id: "containment_status".into(),
                    label: "Containment state".into(),
                    level: traffic_level,
                    detail: health.detail.clone(),
                },
                NetworkPathCheck {
                    id: "ipv6_policy".into(),
                    label: "IPv4/IPv6 policy".into(),
                    level: if cfg.network.allow_ipv6 && cfg.torrent.allow_ipv6 {
                        DiagnosticLevel::Ok
                    } else {
                        DiagnosticLevel::Warning
                    },
                    detail: format!(
                        "network.allow_ipv6={}, torrent.allow_ipv6={}",
                        cfg.network.allow_ipv6, cfg.torrent.allow_ipv6
                    ),
                },
                NetworkPathCheck {
                    id: "dns_validation".into(),
                    label: "DNS containment validation".into(),
                    level: if cfg.network.validate_dns {
                        traffic_level
                    } else {
                        DiagnosticLevel::Warning
                    },
                    detail: if cfg.network.validate_dns {
                        "DNS validation is enabled for the configured path".into()
                    } else {
                        "DNS validation is disabled; IP-literal peers and contained namespaces remain safest".into()
                    },
                },
                NetworkPathCheck {
                    id: "transport_selection".into(),
                    label: "Peer transport selection".into(),
                    level: DiagnosticLevel::Ok,
                    detail: format!(
                        "TCP is {}, uTP is {}, preference is {}, peer encryption is {:?}",
                        "enabled",
                        if cfg.torrent.utp_enabled {
                            "enabled"
                        } else {
                            "disabled"
                        },
                        if cfg.torrent.utp_prefer_tcp {
                            "tcp-first"
                        } else {
                            "utp-first"
                        },
                        cfg.torrent.encryption_mode.as_str()
                    ),
                },
                NetworkPathCheck {
                    id: "socks5_proxy".into(),
                    label: "SOCKS5 proxy transport".into(),
                    level: DiagnosticLevel::Ok,
                    detail: if cfg.network.socks5.enabled {
                        "SOCKS5 TCP CONNECT is enabled; proxy DNS and connection use the contained path, target DNS is remote, and UDP tracker, DHT, and uTP are blocked".into()
                    } else {
                        "SOCKS5 TCP CONNECT is disabled".into()
                    },
                },
            ],
            containment_matrix: containment_matrix(&cfg, traffic_level),
        }
    }

    async fn storage_roots(&self) -> StorageDiagnostics {
        let cfg = self.config.read().await.clone();
        let mut roots: HashMap<String, StorageRootAccumulator> = HashMap::new();
        let configured_control_roots = cfg
            .storage
            .root_controls
            .iter()
            .filter_map(|control| control.normalized_path().ok())
            .collect::<HashSet<_>>();
        for path in &configured_control_roots {
            add_storage_root_role(
                &mut roots,
                path.display().to_string(),
                StorageRootRole::Policy,
            );
        }
        if let Some(path) = cfg.storage.resume_dir.as_ref() {
            add_storage_root_role(&mut roots, path.clone(), StorageRootRole::Resume);
        }
        if let Some(path) = cfg.storage.temp_dir.as_ref() {
            add_storage_root_role(&mut roots, path.clone(), StorageRootRole::Temporary);
        }
        if let Some(path) = self.state_path.as_ref().and_then(|path| path.parent()) {
            add_storage_root_role(
                &mut roots,
                path.display().to_string(),
                StorageRootRole::State,
            );
        }
        if let Some(path) = self.log_file_path.as_ref().and_then(|path| path.parent()) {
            add_storage_root_role(&mut roots, path.display().to_string(), StorageRootRole::Log);
        }
        let admission_records = self.storage_admissions.records().await;
        let active_rechecks = self.storage_rechecks.active_counts();

        let download_dir = resolve_download_dir_from_config(None, &cfg);
        add_storage_root_role(
            &mut roots,
            download_dir.clone(),
            if cfg.storage.download_dir.is_some() {
                StorageRootRole::Download
            } else {
                StorageRootRole::DefaultDownload
            },
        );
        let incomplete_dir = resolve_incomplete_dir_from_config(&download_dir, &cfg);
        add_storage_root_role(
            &mut roots,
            incomplete_dir.clone(),
            StorageRootRole::Incomplete,
        );

        for folder in &cfg.watch {
            if let Some(path) = folder.download_dir.as_ref() {
                add_storage_root_role(&mut roots, path.clone(), StorageRootRole::WatchDownload);
            }
        }

        let mut control_usage: HashMap<PathBuf, (usize, u64, u64)> = HashMap::new();
        {
            let reg = self.registry.lock().await;
            for torrent in reg.torrents.values() {
                let policy = Self::effective_policy_with_config(&cfg, torrent);
                let (complete_dir, active_dir) =
                    Self::policy_storage_paths_with_config(&cfg, torrent);
                if torrent.download_dir.is_some() {
                    add_storage_root_role(
                        &mut roots,
                        complete_dir.clone(),
                        StorageRootRole::TorrentOverride,
                    );
                } else if !matches!(
                    policy.download_dir.source,
                    swarmotter_core::policy::PolicyValueSource::Global
                ) {
                    add_storage_root_role(
                        &mut roots,
                        complete_dir.clone(),
                        StorageRootRole::Policy,
                    );
                }
                add_storage_root_usage(&mut roots, complete_dir.clone(), torrent);
                add_storage_root_role(&mut roots, active_dir.clone(), StorageRootRole::Incomplete);
                if !matches!(
                    policy.incomplete_dir.source,
                    swarmotter_core::policy::PolicyValueSource::Global
                ) {
                    add_storage_root_role(&mut roots, active_dir.clone(), StorageRootRole::Policy);
                }
                if active_dir != complete_dir {
                    add_storage_root_usage(&mut roots, active_dir, torrent);
                }
            }
            for record in &admission_records {
                let entry = control_usage.entry(record.root.clone()).or_default();
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry.1.saturating_add(record.declared_bytes);
                entry.2 = entry.2.saturating_add(
                    reg.get(&record.hash)
                        .map(|torrent| torrent.rate_down)
                        .unwrap_or(0),
                );
            }
        }

        let mut roots = roots
            .into_iter()
            .map(|(path, acc)| {
                let normalized_path =
                    swarmotter_core::config::lexical_absolute_path(Path::new(&path)).ok();
                let controlled_usage = normalized_path
                    .as_ref()
                    .filter(|path| configured_control_roots.contains(*path))
                    .and_then(|path| control_usage.get(path));
                let active_rechecks = normalized_path
                    .as_ref()
                    .and_then(|path| active_rechecks.get(path))
                    .copied()
                    .unwrap_or(0);
                let throughput = self
                    .storage_metrics
                    .throughput_for_path(&cfg, Path::new(&path));
                swarmotter_core::storage::inspect_storage_root(
                    Path::new(&path),
                    acc.roles,
                    &cfg.storage,
                    swarmotter_core::storage::StorageRootUsage {
                        torrent_count: acc.torrent_count,
                        active_torrents: controlled_usage
                            .map(|usage| usage.0)
                            .unwrap_or(acc.active_torrents),
                        active_bytes: controlled_usage
                            .map(|usage| usage.1)
                            .unwrap_or(acc.active_bytes),
                        active_write_rate: controlled_usage
                            .map(|usage| usage.2)
                            .unwrap_or(acc.active_write_rate),
                        active_recheck_rate: Some(throughput.verification_bytes_per_second),
                        sustained_write_bytes_per_second: throughput.write_bytes_per_second,
                        sustained_verification_bytes_per_second: throughput
                            .verification_bytes_per_second,
                        active_rechecks,
                    },
                )
            })
            .collect::<Vec<StorageRootDiagnostics>>();
        roots.sort_by(|a, b| a.path.cmp(&b.path));

        StorageDiagnostics {
            roots,
            minimum_free_space_bytes: cfg.storage.minimum_free_space_bytes,
            minimum_free_space_percent: cfg.storage.minimum_free_space_percent,
            generated_at: now(),
        }
    }

    async fn doctor_report(&self) -> DoctorReport {
        let cfg = self.config.read().await.clone();
        let network = self.network_health.read().await.clone();
        let mut checks = Vec::new();
        push_check(
            &mut checks,
            "config",
            "Configuration validation",
            if cfg.validate().is_ok() {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Invalid
            },
            "the active configuration parses and validates",
            None,
        );
        push_check(
            &mut checks,
            "network",
            "Network containment",
            if network.traffic_allowed {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Invalid
            },
            network.detail,
            Some(
                "fix the configured interface/source/namespace before torrent traffic can continue",
            ),
        );
        self.add_config_file_check(&mut checks).await;
        self.add_log_file_check(&mut checks).await;
        self.add_storage_checks(&cfg, &mut checks).await;
        self.add_watch_checks(&cfg, &mut checks).await;
        self.add_torrent_runtime_check(&mut checks).await;

        let level = checks.iter().fold(DiagnosticLevel::Ok, |level, check| {
            DiagnosticLevel::worst(level, check.level)
        });
        let summary = match level {
            DiagnosticLevel::Ok => "all doctor checks passed".into(),
            DiagnosticLevel::Warning => "one or more doctor checks need attention".into(),
            DiagnosticLevel::Invalid => "one or more doctor checks are invalid".into(),
        };
        DoctorReport {
            level,
            summary,
            checks,
        }
    }

    async fn recent_logs(&self, max_lines: usize) -> LogSnapshot {
        let Some(path) = self.log_file_path.clone() else {
            return LogSnapshot {
                enabled: false,
                path: None,
                lines: Vec::new(),
                truncated: false,
            };
        };
        let lines = read_last_lines(&path, max_lines).unwrap_or_default();
        LogSnapshot {
            enabled: true,
            path: Some(path.display().to_string()),
            truncated: lines.len() >= max_lines,
            lines,
        }
    }

    async fn global_stats(&self) -> GlobalStats {
        let desired = self.desired_download_hashes().await;
        let scheduler = self.scheduler_diagnostics(&desired).await;
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let active_seeds = self.seeder_registry.len().await;
        let reg = self.registry.lock().await;

        let mut active_downloads = 0;
        let mut paused = 0;
        let mut download_rate = 0;
        let mut upload_rate = 0;
        let mut total_downloaded = 0;
        let mut total_uploaded = 0;
        for t in reg.torrents.values() {
            match t.state {
                TorrentState::Downloading | TorrentState::DownloadingMetadata => {
                    active_downloads += 1;
                }
                TorrentState::Paused => {
                    paused += 1;
                }
                _ => {}
            }
            download_rate += t.rate_down;
            upload_rate += t.rate_up;
            total_downloaded += t.downloaded;
            total_uploaded += t.uploaded;
        }

        GlobalStats {
            download_rate,
            upload_rate,
            torrent_count: reg.torrents.len(),
            active_downloads,
            active_seeds,
            paused,
            total_downloaded,
            total_uploaded,
            scheduler,
            ..Default::default()
        }
    }

    async fn torrent_stats(&self, hash: &InfoHash) -> Option<TorrentDiagnostics> {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let engine_state = self.engine_states.read().await.get(hash).cloned();
        let live = if let Some(state) = engine_state {
            let s = state.lock().await;
            Some(LiveTorrentDiagnostics::from_engine_state(
                &s,
                Instant::now(),
            ))
        } else {
            None
        };
        let reg = self.registry.lock().await;
        let t = reg.get(hash)?;
        let progress = if t.meta.total_length == 0 {
            0.0
        } else {
            t.bytes_completed() as f64 / t.meta.total_length as f64
        };
        let live = live.unwrap_or_default();
        Some(TorrentDiagnostics {
            info_hash: t.info_hash(),
            name: t.name().to_string(),
            state: t.state,
            total_length: t.meta.total_length,
            bytes_completed: t.bytes_completed(),
            downloaded: t.downloaded,
            uploaded: t.uploaded,
            piece_count: t.meta.piece_count(),
            pieces_have: t.pieces_have(),
            piece_length: t.meta.piece_length,
            progress,
            rate_down: t.rate_down,
            rate_up: t.rate_up,
            download_limit: t.download_limit,
            upload_limit: t.upload_limit,
            active_peer_workers: live.active_peer_workers,
            known_peers: live.known_peers,
            peer_scheduler: live.peer_scheduler,
            useful_peers: live.useful_peers,
            choked_peers: live.choked_peers,
            unchoked_peers: live.unchoked_peers,
            recent_peer_failures: live.recent_peer_failures,
            recent_tracker_failures: live.recent_tracker_failures,
            tracker_ok: live.tracker_ok,
            tracker_message: live.tracker_message,
            last_announce: live.last_announce,
            tracker_last_ok_seconds_ago: live.tracker_last_ok_seconds_ago,
            dht_discovery_ok: live.dht_discovery_ok,
            dht_last_seen_seconds_ago: live.dht_last_seen_seconds_ago,
            pex_discovery_ok: live.pex_discovery_ok,
            pex_last_seen_seconds_ago: live.pex_last_seen_seconds_ago,
            private: t.meta.is_private(),
        })
    }

    async fn autopilot_status(&self) -> AutopilotConfig {
        self.config.read().await.autopilot.clone()
    }

    async fn torrent_autopilot_decision(&self, hash: &InfoHash) -> Option<AutopilotDecision> {
        let torrent = self.registry.lock().await.get(hash).cloned()?;
        let cfg = self.config.read().await.clone();
        let network = self.network_health.read().await.clone();
        let mode = effective_autopilot_mode(cfg.autopilot.mode, torrent.autopilot_mode_override);
        let state = self.engine_states.read().await.get(hash).cloned();
        let state = match state {
            Some(state) => tokio::time::timeout(AUTOPILOT_STATE_LOCK_TIMEOUT, state.lock())
                .await
                .ok()
                .map(|guard| guard.clone()),
            None => None,
        };
        let input = build_autopilot_input(
            &torrent,
            state.as_ref(),
            self.rate_samples.read().await.get(hash).copied(),
            Instant::now(),
            &network,
        );
        let decision = AutopilotAnalyzer::new().analyze(&input, mode);
        self.autopilot_decisions
            .write()
            .await
            .insert(*hash, decision.clone());
        Some(decision)
    }

    async fn set_torrent_autopilot_mode_override(
        &self,
        hash: &InfoHash,
        mode: Option<AutopilotMode>,
    ) -> Result<()> {
        {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            t.autopilot_mode_override = mode;
        }
        self.refresh_autopilot_decisions(false).await;
        self.persist_state().await
    }

    async fn watch_scan(&self) -> Result<()> {
        self.scan_watch_folders().await
    }

    async fn watch_status(&self) -> WatchStatus {
        let cfg = self.config.read().await.clone();
        let history = self
            .watch_imports
            .lock()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let observations = self.watch_observations.lock().await.clone();
        let enabled = !cfg.watch.is_empty();
        let mut folders = Vec::with_capacity(cfg.watch.len());
        for folder in cfg.watch {
            let scan_folder = folder.clone();
            let scan = tokio::task::spawn_blocking(move || watch::scan_watch_folder(&scan_folder))
                .await
                .ok()
                .and_then(|result| result.ok());
            let exists = scan.is_some();
            let pending_torrent_files = scan
                .as_ref()
                .map(|scan| {
                    scan.files
                        .iter()
                        .filter(|file| {
                            observations.get(&file.key).is_none_or(|observation| {
                                observation.fingerprint != file.fingerprint
                                    || observation.processed_fingerprint != Some(file.fingerprint)
                            })
                        })
                        .count()
                })
                .unwrap_or(0);
            let root = scan
                .as_ref()
                .map(|scan| scan.root.clone())
                .or_else(|| watch::lexical_absolute(Path::new(&folder.path)).ok());
            let last_result = history
                .iter()
                .rev()
                .find(|result| {
                    root.as_ref()
                        .is_some_and(|root| Path::new(&result.path).starts_with(root))
                })
                .cloned();
            folders.push(WatchFolderStatus {
                config: folder,
                exists,
                pending_torrent_files,
                last_result,
            });
        }
        WatchStatus {
            enabled,
            folders,
            recent_imports: history,
        }
    }

    async fn watch_history(&self) -> Vec<watch::ImportResult> {
        self.watch_imports.lock().await.iter().cloned().collect()
    }
}

impl DaemonRuntime {
    /// Remove a manual ban while the caller owns `config_write_lock`.
    async fn remove_global_manual_ban_locked(
        &self,
        ip: String,
    ) -> Result<swarmotter_core::peer_filter::PeerFilterStatus> {
        let ip = ip.trim().parse::<std::net::IpAddr>().map_err(|error| {
            CoreError::InvalidArgument(format!("manual peer ban IP '{ip}': {error}"))
        })?;
        let mut next = self.config.read().await.clone();
        next.peer_filter
            .manual_bans
            .retain(|existing| existing.ip.trim().parse::<std::net::IpAddr>() != Ok(ip));
        self.apply_peer_filter_mutation_locked(next).await
    }

    /// Commit a peer-policy-only configuration mutation while the caller holds
    /// `config_write_lock`. The peer reconfiguration transaction writes the
    /// config only after policy/session reconstruction and restores both the
    /// prior file and the exact prior immutable policy on failure.
    async fn apply_peer_filter_mutation_locked(
        &self,
        next: Config,
    ) -> Result<swarmotter_core::peer_filter::PeerFilterStatus> {
        next.validate()?;
        let peer_permits = self.build_peer_permit_configuration(&next).await?;
        let config_path = self.config_path.clone();
        self.apply_peer_budget_runtime_update(next, peer_permits, config_path.as_deref(), false)
            .await?;
        self.publish_event(Event::new("settings_changed", json!({})));
        self.publish_event(stats_updated_event());
        Ok(self.peer_filter.read().await.status())
    }
}
