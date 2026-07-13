// SPDX-License-Identifier: Apache-2.0

use super::*;
use swarmotter_core::peer_filter::PeerFilter;

/// The complete pre-transition generation needed to roll back a peer runtime
/// reconfiguration. Keeping these values together prevents a restore from
/// mixing state from different configuration generations.
struct PeerReconfigurationRollback<'a> {
    previous: &'a Config,
    previous_peer_filter: &'a Arc<PeerFilter>,
    previous_permits: &'a PeerPermitConfiguration,
    previous_health: &'a NetworkHealth,
    previous_bind_failure: &'a Option<HealthReport>,
    previous_lifecycle: &'a HashMap<InfoHash, LiveTorrentTaskSnapshot>,
    live_work: &'a LivePeerWorkSnapshot,
}

impl DaemonRuntime {
    pub(super) async fn apply_runtime_config_fields(&self) {
        self.apply_runtime_config_fields_impl(true).await;
    }

    /// Apply runtime fields whose effects can be exactly rolled back. The
    /// peer reconfiguration transaction uses this before persistent commit;
    /// irreversible selfish removals run only after commit succeeds.
    pub(super) async fn apply_runtime_config_fields_reversible(&self) {
        self.apply_runtime_config_fields_impl(false).await;
    }

    pub(super) async fn apply_runtime_config_fields_impl(&self, allow_irreversible: bool) {
        let cfg = self.config.read().await.clone();
        self.queue.lock().await.limits = cfg.queue.clone();
        self.global_limiter.set_capacity(
            swarmotter_core::bandwidth::RateDirection::Download,
            cfg.bandwidth.effective_download(),
        );
        self.global_limiter.set_capacity(
            swarmotter_core::bandwidth::RateDirection::Upload,
            cfg.bandwidth.effective_upload(),
        );
        // Profile bandwidth is inherited live. Storage profile paths are
        // deliberately excluded: those were snapshotted at torrent creation.
        self.refresh_profile_runtime_fields().await;
        // Evaluate configuration changes through the same transition operation
        // as periodic path monitoring. Updating the health snapshot directly
        // would hide the healthy-to-blocked edge from the next tick and leave
        // cancelled task registries/state unreconciled.
        self.network_health_tick().await;
        self.apply_peer_worker_limits().await;
        self.storage_admissions.notify_waiters();
        self.storage_rechecks.notify_waiters();
        self.schedule_reconcile_queue("runtime_config").await;
        if allow_irreversible {
            self.sweep_selfish_completed_torrents_best_effort("runtime_config")
                .await;
        }
        self.reconcile_seeders().await;
    }

    pub(super) async fn stop_data_plane_for_reconfiguration(&self) {
        self.reconcile_engine_progress_for_transition().await;
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
    }

    pub(super) async fn live_peer_work_snapshot(&self) -> LivePeerWorkSnapshot {
        let running = self
            .engine_handles
            .read()
            .await
            .iter()
            .filter_map(|(hash, handle)| (!handle.is_finished()).then_some(*hash))
            .collect::<HashSet<_>>();
        let downloads = {
            let registry = self.registry.lock().await;
            running
                .into_iter()
                .filter_map(|hash| {
                    registry
                        .get(&hash)
                        .map(|torrent| LiveTorrentTaskSnapshot::from_torrent(hash, torrent))
                })
                .collect()
        };
        let seeder_hashes = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry.info_hashes().await
        };
        let seeders = {
            let registry = self.registry.lock().await;
            seeder_hashes
                .into_iter()
                .filter_map(|hash| {
                    registry
                        .get(&hash)
                        .map(|torrent| LiveTorrentTaskSnapshot::from_torrent(hash, torrent))
                })
                .collect()
        };
        LivePeerWorkSnapshot { downloads, seeders }
    }

    pub(super) async fn torrent_lifecycle_snapshot(
        &self,
    ) -> HashMap<InfoHash, LiveTorrentTaskSnapshot> {
        self.registry
            .lock()
            .await
            .torrents
            .iter()
            .map(|(hash, torrent)| (*hash, LiveTorrentTaskSnapshot::from_torrent(*hash, torrent)))
            .collect()
    }

    pub(super) async fn restore_torrent_lifecycle_snapshot(
        &self,
        snapshot: &HashMap<InfoHash, LiveTorrentTaskSnapshot>,
    ) {
        let mut registry = self.registry.lock().await;
        for (hash, prior) in snapshot {
            if let Some(torrent) = registry.get_mut(hash) {
                torrent.state = prior.state;
                torrent.seeding_status = prior.seeding_status;
                torrent.error = prior.error.clone();
                torrent.containment_recovery_intent = prior.containment_recovery_intent;
            }
        }
    }

    pub(super) async fn reconstruct_live_peer_work_while_transition_locked(
        &self,
        snapshot: &LivePeerWorkSnapshot,
    ) -> Result<()> {
        if snapshot.is_empty() {
            return Ok(());
        }
        let health = self.network_health.read().await.clone();
        if !health.traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
            return Err(CoreError::Internal(format!(
                "cannot reconstruct peer work while containment is blocked: {}",
                health.detail
            )));
        }

        for prior in &snapshot.downloads {
            {
                let mut registry = self.registry.lock().await;
                let torrent = registry.get_mut(&prior.hash).ok_or_else(|| {
                    CoreError::Internal(format!(
                        "cannot reconstruct missing download torrent {}",
                        prior.hash
                    ))
                })?;
                torrent.state = if prior.state == TorrentState::DownloadingMetadata {
                    TorrentState::DownloadingMetadata
                } else {
                    TorrentState::Downloading
                };
                torrent.error = None;
                torrent.containment_recovery_intent = None;
            }
            // The captured task was already scheduler-authorized. Restart it
            // directly without mutating queue order or granting a durable
            // `start_now` bypass as a side effect of reconfiguration.
            self.start_engine_while_transition_locked(prior.hash).await;
        }

        for prior in &snapshot.seeders {
            {
                let mut registry = self.registry.lock().await;
                let torrent = registry.get_mut(&prior.hash).ok_or_else(|| {
                    CoreError::Internal(format!(
                        "cannot reconstruct missing seeding torrent {}",
                        prior.hash
                    ))
                })?;
                torrent.state = TorrentState::Completed;
                torrent.seeding_status = SeedingStatus::Queued;
                torrent.error = None;
                torrent.containment_recovery_intent = None;
            }
            self.start_recovered_seeder_while_transition_locked(prior.hash)
                .await?;
        }

        // A rollback restores the exact modeled lifecycle fields captured
        // with the prior task ownership. In particular, provisional blocked
        // recovery intents must not leak into the restored healthy runtime.
        {
            let mut registry = self.registry.lock().await;
            for prior in snapshot.downloads.iter().chain(&snapshot.seeders) {
                let torrent = registry.get_mut(&prior.hash).ok_or_else(|| {
                    CoreError::Internal(format!(
                        "cannot restore lifecycle for missing torrent {}",
                        prior.hash
                    ))
                })?;
                torrent.state = prior.state;
                torrent.seeding_status = prior.seeding_status;
                torrent.error = prior.error.clone();
                torrent.containment_recovery_intent = prior.containment_recovery_intent;
            }
        }

        self.verify_live_peer_work(snapshot).await
    }

    pub(super) async fn verify_live_peer_work(
        &self,
        snapshot: &LivePeerWorkSnapshot,
    ) -> Result<()> {
        tokio::task::yield_now().await;
        let missing_downloads = {
            let handles = self.engine_handles.read().await;
            snapshot
                .downloads
                .iter()
                .filter_map(|prior| {
                    (!handles
                        .get(&prior.hash)
                        .is_some_and(|handle| !handle.is_finished()))
                    .then_some(prior.hash)
                })
                .collect::<Vec<_>>()
        };
        let missing_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live = self
                .seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>();
            snapshot
                .seeders
                .iter()
                .filter_map(|prior| (!live.contains(&prior.hash)).then_some(prior.hash))
                .collect::<Vec<_>>()
        };
        if missing_downloads.is_empty() && missing_seeders.is_empty() {
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "peer work reconstruction incomplete: missing downloads {missing_downloads:?}, missing seeders {missing_seeders:?}"
            )))
        }
    }

    pub(super) async fn verify_eligible_peer_work(&self) -> Result<()> {
        tokio::task::yield_now().await;
        let desired_downloads = self.desired_download_hashes().await;
        let (missing_downloads, unexpected_downloads) = {
            let handles = self.engine_handles.read().await;
            let live = handles
                .iter()
                .filter_map(|(hash, handle)| (!handle.is_finished()).then_some(*hash))
                .collect::<HashSet<_>>();
            (
                desired_downloads
                    .iter()
                    .filter(|hash| !live.contains(hash))
                    .copied()
                    .collect::<Vec<_>>(),
                live.iter()
                    .filter(|hash| !desired_downloads.contains(hash))
                    .copied()
                    .collect::<Vec<_>>(),
            )
        };
        let expected_seeders = self.eligible_seeder_hashes().await;
        let (seeder_mismatch, missing_seeders, unexpected_seeders) = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live = self
                .seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>();
            let registry = self.registry.lock().await;
            (
                live.iter().any(|hash| {
                    !registry.get(hash).is_some_and(|torrent| {
                        torrent.state == TorrentState::Seeding
                            && torrent.seeding_status == SeedingStatus::Active
                    })
                }) || registry.torrents.iter().any(|(hash, torrent)| {
                    (torrent.state == TorrentState::Seeding
                        || torrent.seeding_status == SeedingStatus::Active)
                        && !live.contains(hash)
                }),
                expected_seeders
                    .difference(&live)
                    .copied()
                    .collect::<Vec<_>>(),
                live.difference(&expected_seeders)
                    .copied()
                    .collect::<Vec<_>>(),
            )
        };
        if missing_downloads.is_empty()
            && unexpected_downloads.is_empty()
            && missing_seeders.is_empty()
            && unexpected_seeders.is_empty()
            && !seeder_mismatch
        {
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "eligible peer work verification failed: missing downloads {missing_downloads:?}, unexpected downloads {unexpected_downloads:?}, missing seeders {missing_seeders:?}, unexpected seeders {unexpected_seeders:?}, seeder mismatch {seeder_mismatch}"
            )))
        }
    }

    pub(super) async fn eligible_seeder_hashes(&self) -> HashSet<InfoHash> {
        let cfg = self.config.read().await.clone();
        let samples = self.rate_samples.read().await.clone();
        let completed = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .filter(|torrent| {
                torrent.progress.is_complete()
                    && matches!(
                        torrent.state,
                        TorrentState::Completed | TorrentState::Seeding
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut expected = HashSet::new();
        for torrent in completed {
            let hash = torrent.info_hash();
            let idle_seconds = samples
                .get(&hash)
                .and_then(|sample| sample.last_upload_at)
                .map(|at| Instant::now().saturating_duration_since(at).as_secs())
                .unwrap_or_else(|| {
                    now().saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added))
                });
            if automatic_seeding_status(&torrent, &cfg, idle_seconds) == SeedingStatus::Queued
                && (cfg.queue.max_active_seeds == 0 || expected.len() < cfg.queue.max_active_seeds)
            {
                expected.insert(hash);
            }
        }
        expected
    }

    pub(super) async fn reconstruct_eligible_peer_work_while_transition_locked(
        &self,
    ) -> Result<()> {
        for hash in self.desired_download_hashes().await {
            self.start_engine_while_transition_locked(hash).await;
        }
        for hash in self.eligible_seeder_hashes().await {
            self.start_recovered_seeder_while_transition_locked(hash)
                .await?;
        }
        self.verify_eligible_peer_work().await
    }

    async fn restore_peer_reconfiguration(
        &self,
        rollback: &PeerReconfigurationRollback<'_>,
    ) -> Result<()> {
        let transition = self.data_plane_transition_lock.lock().await;
        self.restore_peer_reconfiguration_while_transition_locked(rollback)
            .await?;
        drop(transition);
        self.apply_runtime_config_fields().await;
        self.verify_peer_permit_configuration_identity(rollback.previous_permits)
            .await?;
        self.verify_live_peer_work(rollback.live_work).await?;
        self.persist_state().await
    }

    async fn restore_peer_reconfiguration_while_transition_locked(
        &self,
        rollback: &PeerReconfigurationRollback<'_>,
    ) -> Result<()> {
        self.stop_data_plane_for_reconfiguration().await;
        *self.config.write().await = rollback.previous.clone();
        // Restore the exact prior immutable policy generation alongside the
        // persisted configuration. This keeps a failed manual ban/config
        // replacement from leaving any newly compiled policy live.
        *self.peer_filter.write().await = Arc::clone(rollback.previous_peer_filter);
        self.install_peer_permit_configuration(rollback.previous_permits.clone())
            .await;
        *self.network_health.write().await = rollback.previous_health.clone();
        *self.bind_failure_latched.write().await = rollback.previous_bind_failure.clone();
        if rollback.previous_health.traffic_allowed
            || rollback.previous_health.mode == NetworkContainmentMode::Disabled
        {
            self.containment_gate.allow();
        } else {
            self.containment_gate.block(
                rollback.previous_health.status,
                rollback.previous_health.detail.clone(),
            );
        }
        self.restore_torrent_lifecycle_snapshot(rollback.previous_lifecycle)
            .await;
        self.reconstruct_live_peer_work_while_transition_locked(rollback.live_work)
            .await?;
        self.verify_peer_permit_configuration_identity(rollback.previous_permits)
            .await?;
        self.verify_live_peer_work(rollback.live_work).await?;
        self.persist_state().await
    }

    pub(super) async fn apply_peer_budget_runtime_update(
        &self,
        next: Config,
        peer_permits: PeerPermitConfiguration,
        persist_path: Option<&Path>,
        clear_bind_failure_latch: bool,
    ) -> Result<()> {
        let previous = self.config.read().await.clone();
        // Compile before touching a live task, permit, or persisted file. A
        // validation/load failure therefore cannot produce a partial policy
        // replacement, and rollback can restore this exact prior generation.
        let next_peer_filter = Arc::new(PeerFilter::from_config(&next.peer_filter)?);
        let previous_peer_filter = self.peer_filter.read().await.clone();
        let previous_permits = self.current_peer_permit_configuration().await;
        let previous_health = self.network_health.read().await.clone();
        let previous_bind_failure = self.bind_failure_latched.read().await.clone();
        let file_snapshot = persist_path.map(capture_config_file).transpose()?;
        let next_health = if previous_bind_failure.is_some() && !clear_bind_failure_latch {
            previous_health.clone()
        } else {
            net::evaluate(&next.network, self.interface_probe.as_ref())
        };
        let peer_limits_only = configs_differ_only_in_peer_limits(&previous, &next);

        let transition = self.data_plane_transition_lock.lock().await;
        self.reconcile_engine_progress_for_transition().await;
        let live_work = self.live_peer_work_snapshot().await;
        let previous_lifecycle = self.torrent_lifecycle_snapshot().await;
        let rollback_snapshot = PeerReconfigurationRollback {
            previous: &previous,
            previous_peer_filter: &previous_peer_filter,
            previous_permits: &previous_permits,
            previous_health: &previous_health,
            previous_bind_failure: &previous_bind_failure,
            previous_lifecycle: &previous_lifecycle,
            live_work: &live_work,
        };
        let next_is_blocked =
            !next_health.traffic_allowed && next_health.mode != NetworkContainmentMode::Disabled;
        if next_is_blocked {
            self.containment_gate
                .block(next_health.status, next_health.detail.clone());
        }
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
        if let Err(error) = self
            .wait_for_peer_permit_configuration_drain(&previous_permits)
            .await
        {
            let rollback = self
                .restore_peer_reconfiguration_while_transition_locked(&rollback_snapshot)
                .await;
            drop(transition);
            if rollback.is_ok() {
                self.apply_runtime_config_fields().await;
            }
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return Err(CoreError::Internal(format!(
                "old peer permit drain failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
            )));
        }

        // This is the provisional ownership boundary. The injected failure is
        // deliberately evaluated only after both candidate objects are live.
        self.install_peer_permit_configuration(peer_permits).await;
        *self.config.write().await = next.clone();
        *self.peer_filter.write().await = next_peer_filter;
        if clear_bind_failure_latch {
            *self.bind_failure_latched.write().await = None;
        }
        *self.network_health.write().await = next_health.clone();
        if next_is_blocked {
            let mut registry = self.registry.lock().await;
            for prior in &live_work.downloads {
                if let Some(torrent) = registry.get_mut(&prior.hash) {
                    torrent.containment_recovery_intent =
                        Some(if prior.state == TorrentState::DownloadingMetadata {
                            ContainmentRecoveryIntent::DownloadingMetadata
                        } else {
                            ContainmentRecoveryIntent::Downloading
                        });
                    torrent.state = TorrentState::NetworkBlocked;
                    torrent.error = Some(next_health.detail.clone());
                }
            }
            for prior in &live_work.seeders {
                if let Some(torrent) = registry.get_mut(&prior.hash) {
                    torrent.containment_recovery_intent = Some(ContainmentRecoveryIntent::Seeding);
                    torrent.state = TorrentState::NetworkBlocked;
                    torrent.error = Some(next_health.detail.clone());
                }
            }
        } else {
            self.containment_gate.allow();
        }
        if self.peer_reconfiguration_failure_injected() {
            let rollback = self
                .restore_peer_reconfiguration_while_transition_locked(&rollback_snapshot)
                .await;
            drop(transition);
            let rollback = async {
                rollback?;
                self.apply_runtime_config_fields().await;
                self.verify_peer_permit_configuration_identity(&previous_permits)
                    .await?;
                self.verify_live_peer_work(&live_work).await
            }
            .await;
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return match (rollback, file_rollback) {
                (Ok(()), Ok(())) => Err(CoreError::Internal(
                    "injected peer permit reconstruction failure after provisional install"
                        .into(),
                )),
                (runtime, file) => Err(CoreError::Internal(format!(
                    "injected peer permit reconstruction failure; runtime rollback: {runtime:?}; configuration rollback: {file:?}"
                ))),
            };
        }

        if !next_is_blocked
            && (!previous_health.traffic_allowed
                && previous_health.mode != NetworkContainmentMode::Disabled
                || previous_bind_failure.is_some())
        {
            let mut registry = self.registry.lock().await;
            for torrent in registry.torrents.values_mut() {
                let Some(intent) = torrent.containment_recovery_intent.take() else {
                    continue;
                };
                torrent.error = None;
                match intent {
                    ContainmentRecoveryIntent::Downloading
                    | ContainmentRecoveryIntent::DownloadingMetadata => {
                        torrent.state = TorrentState::Queued;
                        torrent.seeding_status = SeedingStatus::NotEligible;
                    }
                    ContainmentRecoveryIntent::Seeding => {
                        torrent.state = TorrentState::Completed;
                        torrent.seeding_status = SeedingStatus::Queued;
                    }
                }
            }
        }
        self.wait_at_peer_reconfiguration_test_pause().await;

        let reconstruction = if next_is_blocked {
            let has_live_tasks = !self.engine_handles.read().await.is_empty()
                || !self.seeder_registry.is_empty().await;
            if has_live_tasks {
                Err(CoreError::Internal(
                    "peer tasks remained live after blocked configuration install".into(),
                ))
            } else {
                Ok(())
            }
        } else if peer_limits_only {
            match self
                .reconstruct_live_peer_work_while_transition_locked(&live_work)
                .await
            {
                Ok(()) => self.verify_live_peer_work(&live_work).await,
                Err(error) => Err(error),
            }
        } else {
            self.reconstruct_eligible_peer_work_while_transition_locked()
                .await
        };
        if let Err(error) = reconstruction {
            let rollback = self
                .restore_peer_reconfiguration_while_transition_locked(&rollback_snapshot)
                .await;
            drop(transition);
            let rollback = async {
                rollback?;
                self.apply_runtime_config_fields().await;
                self.verify_peer_permit_configuration_identity(&previous_permits)
                    .await?;
                self.verify_live_peer_work(&live_work).await
            }
            .await;
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return Err(CoreError::Internal(format!(
                "peer permit reconstruction failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
            )));
        }
        drop(transition);
        self.apply_runtime_config_fields_reversible().await;
        let post_reconcile_verification = if next_is_blocked {
            if !self.engine_handles.read().await.is_empty()
                || !self.seeder_registry.is_empty().await
            {
                Err(CoreError::Internal(
                    "peer tasks started after blocked configuration reconstruction".into(),
                ))
            } else {
                Ok(())
            }
        } else if peer_limits_only {
            self.verify_live_peer_work(&live_work).await
        } else {
            self.verify_eligible_peer_work().await
        };
        if let Err(error) = post_reconcile_verification {
            let rollback = self.restore_peer_reconfiguration(&rollback_snapshot).await;
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return Err(CoreError::Internal(format!(
                "peer permit post-reconcile verification failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
            )));
        }

        self.wait_at_peer_reconfiguration_persistence_test_pause()
            .await;
        if let Some(path) = persist_path {
            let persisted = if self.peer_reconfiguration_persistence_failure_injected() {
                Err(CoreError::Internal(
                    "injected peer permit configuration persistence failure".into(),
                ))
            } else {
                write_config_atomically(path, &next)
            };
            if let Err(error) = persisted {
                let rollback = self.restore_peer_reconfiguration(&rollback_snapshot).await;
                let file_rollback = file_snapshot
                    .as_ref()
                    .map_or(Ok(()), |snapshot| restore_config_file(path, snapshot));
                return Err(CoreError::Internal(format!(
                    "peer permit configuration persistence failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
                )));
            }
        }

        self.selfish_completion_enabled
            .store(next.torrent.selfish, Ordering::Release);
        // The candidate is now committed in memory and, for full PUT, on
        // disk. Irreversible policy effects must never run before this point.
        self.sweep_selfish_completed_torrents_best_effort("runtime_config_commit")
            .await;
        debug_assert_eq!(
            self.config.read().await.bandwidth.max_peers,
            self.peer_permit_snapshot().await.limit
        );
        Ok(())
    }
}
