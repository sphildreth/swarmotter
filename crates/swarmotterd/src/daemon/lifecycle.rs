// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait]
impl DaemonOps for DaemonRuntime {
    async fn list_torrents(&self) -> Vec<TorrentSummary> {
        let global_seeding = self.config.read().await.seeding.clone();
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
                summary.effective_ratio_limit = t.seeding.effective_ratio_limit(&global_seeding);
                summary.effective_idle_limit = t.seeding.effective_idle_limit(&global_seeding);
                summary
            })
            .collect()
    }

    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        let global_seeding = self.config.read().await.seeding.clone();
        let position = self.queue.lock().await.position(hash);
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        self.registry.lock().await.get(hash).map(|t| {
            let mut summary = t.to_summary();
            summary.queue_position = position;
            summary.effective_ratio_limit = t.seeding.effective_ratio_limit(&global_seeding);
            summary.effective_idle_limit = t.seeding.effective_idle_limit(&global_seeding);
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
        let was_completed = self
            .registry
            .lock()
            .await
            .get(hash)
            .map(|torrent| torrent.progress.is_complete())
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        self.stop_engine(hash).await;
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.containment_recovery_intent = None;
                    t.state = TorrentState::Checking;
                    t.seeding_status = SeedingStatus::NotEligible;
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        self.publish_torrent_event("torrent_changed", *hash, TorrentState::Checking);
        self.publish_event(stats_updated_event());
        // Run a real storage recheck on disk.
        let (meta, storage_dir) = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            let complete_dir = self.resolve_download_dir(t).await;
            let storage_dir = if was_completed {
                complete_dir
            } else {
                self.resolve_incomplete_dir(&complete_dir).await
            };
            (t.meta.clone(), storage_dir)
        };
        let storage = swarmotter_core::storage::StorageIo::new(
            meta.clone(),
            std::path::PathBuf::from(&storage_dir),
        );
        match storage.recheck().await {
            Ok(bf) => {
                let mut final_state = None;
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.progress.replace_from_bitfield(&bf, meta.piece_count());
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
                self.persist_state().await?;
                self.reconcile_seeders().await;
            }
            Err(e) => {
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.state = TorrentState::StorageError;
                    t.error = Some(e.to_string());
                }
                drop(reg);
                self.publish_torrent_event("torrent_error", *hash, TorrentState::StorageError);
                self.publish_event(stats_updated_event());
                self.persist_state_best_effort("recheck_failed").await;
                return Err(e);
            }
        }
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
        self.ensure_storage_paths_available_except(&torrent.meta, Some(&path), Some(*hash))
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
        let cfg = self.config.read().await.clone();
        let old_complete = resolve_download_dir_from_config(torrent.download_dir.as_deref(), &cfg);
        let source = if payload_in_complete {
            old_complete
        } else {
            resolve_incomplete_dir_from_config(&old_complete, &cfg)
        };
        let destination = if payload_in_complete {
            path.clone()
        } else {
            resolve_incomplete_dir_from_config(&path, &cfg)
        };
        let source_path = PathBuf::from(source);
        let storage =
            swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), source_path.clone());
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
        self.ensure_storage_paths_available_except(
            &renamed_meta,
            torrent.download_dir.as_deref(),
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
            self.resolve_incomplete_dir(&complete_dir).await
        };
        let old_storage = swarmotter_core::storage::StorageIo::new(
            torrent.meta.clone(),
            PathBuf::from(&storage_dir),
        );
        let old_path = old_storage.file_path(file_index)?;
        let new_storage = swarmotter_core::storage::StorageIo::new(
            renamed_meta.clone(),
            PathBuf::from(storage_dir),
        );
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
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                t.labels = labels;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
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
            // Persist policy independently of runtime lifecycle. A live
            // registry entry remains Seeding+Active until synchronized
            // reconciliation stops it after the durable write succeeds.
            torrent.seeding = seeding;
            previous
        };

        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                torrent.seeding = previous;
            }
            return Err(error);
        }

        drop(lifecycle);
        self.reconcile_seeders().await;
        self.get_torrent(hash)
            .await
            .ok_or_else(|| CoreError::NotFound("torrent".into()))
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
                banned: false,
            })
            .collect();
        Some(peers)
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
        next.validate()?;
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
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        validate_storage_config_transition(&previous, &next, &torrents)?;

        let peer_limits_changed = peer_limits_changed(&previous, &next);
        let restart_required_fields = restart_required_fields(&previous, &next);
        if peer_limits_changed {
            let peer_permits = self.build_peer_permit_configuration(&next).await?;
            self.apply_peer_budget_runtime_update(
                next.clone(),
                peer_permits,
                config_path.as_deref(),
                recovering_latched_failure,
            )
            .await?;
        } else {
            if let Some(path) = &config_path {
                write_config_atomically(path, &next)?;
            }

            let rebuild_data_plane = data_plane_config_changed(&previous, &next);
            let data_plane_transition = if rebuild_data_plane {
                Some(self.data_plane_transition_lock.lock().await)
            } else {
                None
            };
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
        }
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
                "dht".into(),
                "storage".into(),
                "watch".into(),
                "autopilot".into(),
            ],
            config: redact_config(next),
        })
    }

    async fn reset_downloads(&self) -> Result<ResetResult> {
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
            let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
            for dir in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
                let storage =
                    swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), dir.clone());
                storage.remove_all().await?;
                push_display_path(&mut storage_paths, &dir);
            }
        }

        let cfg = self.config.read().await.clone();
        let download_dir = cfg
            .storage
            .download_dir
            .clone()
            .unwrap_or_else(default_download_dir_string);
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
            ],
            containment_matrix: containment_matrix(&cfg, traffic_level),
        }
    }

    async fn storage_roots(&self) -> StorageDiagnostics {
        let cfg = self.config.read().await.clone();
        let mut roots: HashMap<String, StorageRootAccumulator> = HashMap::new();

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

        {
            let reg = self.registry.lock().await;
            for torrent in reg.torrents.values() {
                let complete_dir =
                    resolve_download_dir_from_config(torrent.download_dir.as_deref(), &cfg);
                if torrent.download_dir.is_some() {
                    add_storage_root_role(
                        &mut roots,
                        complete_dir.clone(),
                        StorageRootRole::TorrentOverride,
                    );
                }
                add_storage_root_usage(&mut roots, complete_dir.clone(), torrent);
                let active_dir = resolve_incomplete_dir_from_config(&complete_dir, &cfg);
                add_storage_root_role(&mut roots, active_dir.clone(), StorageRootRole::Incomplete);
                if active_dir != complete_dir {
                    add_storage_root_usage(&mut roots, active_dir, torrent);
                }
            }
        }

        let mut roots = roots
            .into_iter()
            .map(|(path, acc)| {
                swarmotter_core::storage::inspect_storage_root(
                    Path::new(&path),
                    acc.roles,
                    &cfg.storage,
                    swarmotter_core::storage::StorageRootUsage {
                        torrent_count: acc.torrent_count,
                        active_torrents: acc.active_torrents,
                        active_write_rate: acc.active_write_rate,
                        active_recheck_rate: Some(0),
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
