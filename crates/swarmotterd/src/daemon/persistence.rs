// SPDX-License-Identifier: Apache-2.0

use super::*;

impl DaemonRuntime {
    pub async fn restore_persisted_state(&self) -> Result<usize> {
        let Some(path) = self.state_path.clone() else {
            return Ok(0);
        };
        let Some(mut stored) = tokio::task::spawn_blocking(move || crate::state_store::load(&path))
            .await
            .map_err(|error| CoreError::Storage(format!("load daemon state task: {error}")))??
        else {
            return Ok(0);
        };
        let traffic_allowed = self.network_health.read().await.traffic_allowed;
        let restore_config = self.config.read().await.clone();
        let mut restored = TorrentRegistry::default();
        for mut torrent in stored.torrents.drain(..) {
            let persisted_state = torrent.state;
            if torrent
                .seeding
                .ratio_limit
                .is_some_and(|value| !value.is_finite() || value < 0.0)
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has invalid seeding.ratio_limit",
                    torrent.info_hash()
                )));
            }
            torrent.meta.validate().map_err(|error| {
                CoreError::Storage(format!(
                    "invalid metadata for restored torrent {}: {error}",
                    torrent.info_hash()
                ))
            })?;
            if torrent.files.len() != torrent.meta.files.len()
                || torrent.priorities.len() != torrent.meta.files.len()
                || torrent.wanted.len() != torrent.meta.files.len()
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent file settings",
                    torrent.info_hash()
                )));
            }
            if torrent.needs_metadata != torrent.magnet_info_hash.is_some() {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent magnet identity",
                    torrent.info_hash()
                )));
            }
            let piece_count = torrent.meta.piece_count();
            let expected_bitfield_bytes = piece_count.div_ceil(8);
            if torrent.progress.total != piece_count
                || torrent.progress.bitfield().as_bytes().len() != expected_bitfield_bytes
                || (piece_count..expected_bitfield_bytes.saturating_mul(8))
                    .any(|index| torrent.progress.bitfield().has(index))
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent piece progress",
                    torrent.info_hash()
                )));
            }
            let restored_bitfield = torrent.progress.bitfield().clone();
            torrent
                .progress
                .replace_from_bitfield(&restored_bitfield, piece_count);
            let previous_files = std::mem::take(&mut torrent.files);
            torrent.files = torrent
                .meta
                .files
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    let bytes_completed = previous_files[index].bytes_completed;
                    if bytes_completed > file.length {
                        return Err(CoreError::Storage(format!(
                            "daemon state for {} has file progress beyond file length",
                            torrent.info_hash()
                        )));
                    }
                    Ok(TorrentFile {
                        index,
                        path: file.path.join("/"),
                        length: file.length,
                        bytes_completed,
                        priority: torrent.priorities[index],
                        wanted: torrent.wanted[index],
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            torrent.recompute_file_bytes_completed();
            torrent.rate_down = 0;
            torrent.rate_up = 0;
            torrent.active_peer_workers = 0;
            torrent.known_peers = 0;
            torrent.health = swarmotter_core::models::torrent::TorrentHealth::unknown();
            torrent.state = match torrent.state {
                TorrentState::Downloading => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::Downloading);
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::DownloadingMetadata => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::DownloadingMetadata);
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::Checking => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        // Recheck is storage-only and was not a live torrent
                        // transport. Preserve the block without granting an
                        // automatic network recovery intent.
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::Seeding => {
                    if traffic_allowed {
                        TorrentState::Completed
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::Seeding);
                        TorrentState::NetworkBlocked
                    }
                }
                state => state,
            };
            if traffic_allowed && torrent.state == TorrentState::NetworkBlocked {
                if let Some(intent) = torrent.containment_recovery_intent.take() {
                    torrent.error = None;
                    torrent.state = match intent {
                        ContainmentRecoveryIntent::Downloading
                        | ContainmentRecoveryIntent::DownloadingMetadata => TorrentState::Queued,
                        ContainmentRecoveryIntent::Seeding => TorrentState::Completed,
                    };
                }
            }
            recompute_restored_seeding_lifecycle(
                &mut torrent,
                persisted_state,
                &restore_config,
                now(),
            );
            let hash = torrent.info_hash();
            restored.add(torrent).map_err(|_| {
                CoreError::Storage(format!("duplicate torrent {hash} in daemon state"))
            })?;
        }

        let config = self.config.read().await.clone();
        validate_restored_storage_ownership(restored.torrents.values(), &config)?;

        let known = restored.torrents.keys().copied().collect::<HashSet<_>>();
        let stale_queue_entries = stored
            .queue
            .order
            .iter()
            .filter(|hash| !known.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        stored.queue.remove_many(stale_queue_entries);
        stored.queue.add_many(known.iter().copied());
        stored.queue.limits = self.config.read().await.queue.clone();
        let count = restored.torrents.len();
        *self.torrent_limiters.write().await = restored
            .torrents
            .iter()
            .map(|(hash, torrent)| {
                let policy = Self::effective_policy_with_config(&config, torrent);
                (
                    *hash,
                    Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                        policy.download_limit.value,
                        policy.upload_limit.value,
                    )),
                )
            })
            .collect();
        let per_torrent_peer_limit =
            Self::effective_per_torrent_peer_limit(config.bandwidth.max_peers_per_torrent);
        *self.torrent_peer_permit_pools.write().await = restored
            .torrents
            .keys()
            .map(|hash| {
                PeerPermitPool::new(per_torrent_peer_limit, self.peer_sessions_denied.clone())
                    .map(|pool| (*hash, pool))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        *self.registry.lock().await = restored;
        *self.queue.lock().await = stored.queue;
        self.verify_restored_completed_torrents().await?;
        self.reconcile_queue().await;
        self.reconcile_seeders().await;
        self.persist_state().await?;
        tracing::info!(count, path = %self.state_path.as_ref().unwrap().display(), "restored daemon state");
        Ok(count)
    }

    pub(super) async fn verify_restored_completed_torrents(&self) -> Result<()> {
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Completed)
            .cloned()
            .collect::<Vec<_>>();
        for torrent in torrents {
            let hash = torrent.info_hash();
            let complete_dir = self.resolve_download_dir(&torrent).await;
            let storage_dir = if torrent.progress.is_complete() {
                complete_dir
            } else {
                self.resolve_incomplete_dir_for(&torrent).await
            };
            let storage = swarmotter_core::storage::StorageIo::new(
                torrent.meta.clone(),
                PathBuf::from(storage_dir),
            );
            match self
                .recheck_storage_under_root_control(&storage, None)
                .await
            {
                Ok(bitfield) => {
                    let selection_complete = torrent_selection_complete(&torrent, &bitfield)?;
                    let traffic_allowed = self.network_health.read().await.traffic_allowed;
                    if let Some(restored) = self.registry.lock().await.get_mut(&hash) {
                        restored
                            .progress
                            .replace_from_bitfield(&bitfield, restored.meta.piece_count());
                        restored.recompute_file_bytes_completed();
                        if !selection_complete {
                            restored.state = if traffic_allowed {
                                TorrentState::Queued
                            } else {
                                TorrentState::NetworkBlocked
                            };
                            restored.error = Some(
                                "restored payload failed verification; selected pieces queued for recovery"
                                    .into(),
                            );
                            restored.seeding_status = SeedingStatus::NotEligible;
                        } else {
                            restored.seeding_status = SeedingStatus::Queued;
                        }
                    }
                }
                Err(error) => {
                    if let Some(restored) = self.registry.lock().await.get_mut(&hash) {
                        restored.state = TorrentState::StorageError;
                        restored.error = Some(error.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn persist_state(&self) -> Result<()> {
        let Some(path) = self.state_path.clone() else {
            return Ok(());
        };
        let _write_guard = self.state_write_lock.lock().await;
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        let queue = self.queue.lock().await.clone();
        let state = crate::state_store::DaemonState::new(torrents, queue);
        tokio::task::spawn_blocking(move || crate::state_store::save(&path, &state))
            .await
            .map_err(|error| CoreError::Storage(format!("save daemon state task: {error}")))??;
        Ok(())
    }

    /// Persist a state transition that must be paired with another durable
    /// transaction. A state-directory sync can fail after rename, so capture
    /// and restore the previous raw file while holding the state-write lock.
    /// On error the state file is therefore returned to the generation that
    /// existed before this call whenever the rollback succeeds.
    pub(super) async fn persist_state_with_file_rollback(&self) -> Result<()> {
        let Some(path) = self.state_path.clone() else {
            return Ok(());
        };
        let _write_guard = self.state_write_lock.lock().await;
        let capture_path = path.clone();
        let snapshot =
            tokio::task::spawn_blocking(move || crate::state_store::capture_file(&capture_path))
                .await
                .map_err(|error| {
                    CoreError::Storage(format!("capture daemon state task: {error}"))
                })??;
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        let queue = self.queue.lock().await.clone();
        let state = crate::state_store::DaemonState::new(torrents, queue);
        let write_path = path.clone();
        let persisted =
            tokio::task::spawn_blocking(move || crate::state_store::save(&write_path, &state))
                .await
                .map_err(|error| CoreError::Storage(format!("save daemon state task: {error}")))?;
        if let Err(error) = persisted {
            let rollback_path = path.clone();
            let rollback_snapshot = snapshot.clone();
            let rollback = tokio::task::spawn_blocking(move || {
                crate::state_store::restore_file(&rollback_path, &rollback_snapshot)
            })
            .await
            .map_err(|join_error| {
                CoreError::Storage(format!("restore daemon state task: {join_error}"))
            })?;
            return Err(CoreError::Storage(format!(
                "save daemon state: {error}; state rollback: {rollback:?}"
            )));
        }
        Ok(())
    }

    pub(super) async fn persist_state_best_effort(&self, reason: &'static str) {
        if let Err(error) = self.persist_state().await {
            tracing::error!(reason, %error, "failed to persist daemon state");
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.reconcile_engine_progress().await;
        let hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.stop_all_torrent_tasks(&hashes).await;
        self.persist_state().await
    }

    pub(super) fn publish_event(&self, event: Event) {
        self.event_broker.publish(event);
    }

    pub(super) fn publish_torrent_event(
        &self,
        kind: &'static str,
        hash: InfoHash,
        state: TorrentState,
    ) {
        self.publish_event(torrent_event(kind, hash, state));
    }

    #[allow(dead_code)]
    pub async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        download_dir: Option<String>,
    ) -> Result<InfoHash> {
        self.add_torrent_file_with_options(bytes, AddTorrentOptions::new(download_dir, false))
            .await
    }

    #[allow(dead_code)]
    pub async fn add_magnet(&self, magnet: &str, download_dir: Option<String>) -> Result<InfoHash> {
        self.add_magnet_with_options(magnet, AddTorrentOptions::new(download_dir, false))
            .await
    }

    pub async fn add_torrent_file_with_options(
        &self,
        bytes: Vec<u8>,
        options: AddTorrentOptions,
    ) -> Result<InfoHash> {
        // Keep profile resolution and the durable registration in the same
        // transaction as profile replacement. Otherwise a profile could be
        // removed after this add validates it but before its attachment is
        // persisted into daemon state.
        let _config_transaction = self.config_write_lock.lock().await;
        let parsed = match meta::parse_torrent(&bytes) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    error_code = %e.code(),
                    "torrent file add rejected"
                );
                return Err(e);
            }
        };
        let hash = parsed.info_hash;
        let mut t = Torrent::new(parsed, now());
        if let Some(d) = options.download_dir {
            t.download_dir = Some(d);
        }
        let paused = self
            .apply_add_profile(
                &mut t,
                options.profile,
                options.labels,
                options.start_behavior_explicit,
                options.paused,
            )
            .await?;
        match self
            .add_torrent_mutation(t, paused, "torrent_file_added")
            .await?
        {
            TorrentAddMutationOutcome::Inserted { state, .. } => {
                tracing::info!(
                    info_hash = %hash,
                    network_blocked = state == TorrentState::NetworkBlocked,
                    paused = state == TorrentState::Paused,
                    "torrent file added"
                );
                Ok(hash)
            }
            TorrentAddMutationOutcome::Duplicate { .. } => {
                tracing::warn!(
                    info_hash = %hash,
                    error_code = %CoreError::DuplicateTorrent(hash.to_hex()).code(),
                    "torrent file add rejected: duplicate"
                );
                Err(CoreError::DuplicateTorrent(hash.to_hex()))
            }
        }
    }

    pub(super) async fn add_magnet_with_options(
        &self,
        magnet: &str,
        options: AddTorrentOptions,
    ) -> Result<InfoHash> {
        // See `add_torrent_file_with_options`: this lock makes profile
        // selection and durable registration indivisible from profile PUT.
        let _config_transaction = self.config_write_lock.lock().await;
        let m = Magnet::parse(magnet)?;
        let hash = m.info_hash;
        let name = m.display_name.clone().unwrap_or_else(|| hash.to_hex());
        // Build a placeholder single-file torrent so the registry has a record;
        // the real metadata is fetched via BEP 9 from peers once the engine
        // starts. The registry is keyed by the magnet's real info hash.
        let bytes = meta::build_single_file_torrent(
            &name,
            b"magnet placeholder data",
            16,
            m.trackers.first().map(|s| s.as_str()),
            false,
        );
        let mut parsed = meta::parse_torrent(&bytes)?;
        // Placeholder storage ownership must use the magnet's real identity.
        // Otherwise two different magnets with the same display name produce
        // the same synthetic metainfo hash and bypass conflict detection.
        parsed.info_hash = hash;
        let mut t = Torrent::new(parsed, now());
        t.needs_metadata = true;
        t.magnet_info_hash = Some(hash);
        t.magnet_name = Some(name);
        t.magnet_trackers = m.trackers.clone();
        if let Some(d) = options.download_dir {
            t.download_dir = Some(d);
        }
        let paused = self
            .apply_add_profile(
                &mut t,
                options.profile,
                options.labels,
                options.start_behavior_explicit,
                options.paused,
            )
            .await?;
        match self.add_torrent_mutation(t, paused, "magnet_added").await? {
            TorrentAddMutationOutcome::Inserted { state, .. } => {
                tracing::info!(
                    info_hash = %hash,
                    network_blocked = state == TorrentState::NetworkBlocked,
                    paused = state == TorrentState::Paused,
                    tracker_count = m.trackers.len(),
                    "magnet added"
                );
                Ok(hash)
            }
            TorrentAddMutationOutcome::Duplicate { .. } => {
                Err(CoreError::DuplicateTorrent(hash.to_hex()))
            }
        }
    }

    /// Shared durable add transaction for API, magnet, and watch ingestion.
    /// Parsing happens before entry. Storage and containment preflight mutate
    /// only the candidate. The storage-ownership lock then spans path
    /// validation, exact hash snapshots, insertion, persistence, and rollback.
    pub(super) async fn add_torrent_mutation(
        &self,
        mut torrent: Torrent,
        requested_paused: bool,
        schedule_reason: &'static str,
    ) -> Result<TorrentAddMutationOutcome> {
        let hash = torrent.info_hash();
        let mutation_guard = self.storage_ownership_lock.lock().await;
        let previous_torrent = self.registry.lock().await.get(&hash).cloned();
        let previous_queue = self.queue.lock().await.membership_snapshot(&hash);
        if previous_torrent.is_some() {
            return Ok(TorrentAddMutationOutcome::Duplicate { hash });
        }
        self.preflight_storage_for_torrent(
            &torrent,
            if torrent.needs_metadata {
                0
            } else {
                torrent.meta.total_length
            },
        )
        .await?;
        apply_network_state(&mut torrent, &self.network_health).await;
        if requested_paused && torrent.state != TorrentState::NetworkBlocked {
            torrent.state = TorrentState::Paused;
        }
        let committed_state = torrent.state;

        self.ensure_storage_paths_available_for_torrent(&torrent, None)
            .await?;

        self.registry
            .lock()
            .await
            .add(torrent)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        self.queue.lock().await.add(hash);

        let persistence = if self.add_mutation_persistence_failure_injected() {
            Err(CoreError::Storage(
                "injected shared torrent-add persistence failure".into(),
            ))
        } else {
            self.persist_state().await
        };
        if let Err(error) = persistence {
            let mut registry = self.registry.lock().await;
            registry.remove(&hash);
            if let Some(previous) = previous_torrent {
                registry.torrents.insert(hash, previous);
            }
            drop(registry);
            self.queue
                .lock()
                .await
                .restore_membership(hash, previous_queue);
            return Err(error);
        }

        let inserted = self
            .registry
            .lock()
            .await
            .get(&hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        let policy = self.effective_policy(&inserted).await;
        self.ensure_torrent_limiter(hash, policy.download_limit.value, policy.upload_limit.value)
            .await;
        self.ensure_torrent_peer_permit_pool(hash).await;
        drop(mutation_guard);
        if committed_state == TorrentState::Queued {
            self.schedule_reconcile_queue(schedule_reason).await;
        }
        self.publish_torrent_event("torrent_added", hash, committed_state);
        self.publish_event(stats_updated_event());
        Ok(TorrentAddMutationOutcome::Inserted {
            hash,
            state: committed_state,
        })
    }

    pub(super) async fn remove_torrents_with_single_reconcile(
        &self,
        hashes: Vec<InfoHash>,
        delete_data: bool,
    ) -> Result<Vec<InfoHash>> {
        let mut unique_hashes = Vec::with_capacity(hashes.len());
        let mut seen = HashSet::with_capacity(hashes.len());
        for hash in hashes {
            if seen.insert(hash) {
                unique_hashes.push(hash);
            }
        }

        let targets = {
            let reg = self.registry.lock().await;
            unique_hashes
                .into_iter()
                .filter_map(|hash| reg.get(&hash).cloned().map(|torrent| (hash, torrent)))
                .collect::<Vec<_>>()
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        for (hash, _) in &targets {
            self.force_stop_engine(hash).await;
        }
        if delete_data {
            for (hash, torrent) in &targets {
                let complete_dir = self.resolve_download_dir(torrent).await;
                let active_dir = self.resolve_incomplete_dir_for(torrent).await;
                let mut dirs = vec![active_dir, complete_dir];
                dirs.dedup();
                for dir in dirs {
                    let storage = swarmotter_core::storage::StorageIo::new(
                        torrent.meta.clone(),
                        std::path::PathBuf::from(&dir),
                    );
                    if let Err(error) = storage.remove_all().await {
                        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                            torrent.state = TorrentState::StorageError;
                            torrent.error = Some(error.to_string());
                        }
                        self.persist_state_best_effort("remove_failed").await;
                        return Err(error);
                    }
                }
            }
        }
        {
            let mut reg = self.registry.lock().await;
            for (hash, _) in &targets {
                reg.remove(hash);
            }
        }
        self.queue
            .lock()
            .await
            .remove_many(targets.iter().map(|(hash, _)| *hash));
        {
            let mut rate_samples = self.rate_samples.write().await;
            let mut decisions = self.autopilot_decisions.write().await;
            let mut last_actions = self.autopilot_last_action.write().await;
            let mut limiters = self.torrent_limiters.write().await;
            let mut peer_permit_pools = self.torrent_peer_permit_pools.write().await;
            for (hash, _) in &targets {
                rate_samples.remove(hash);
                decisions.remove(hash);
                last_actions.remove(hash);
                limiters.remove(hash);
                peer_permit_pools.remove(hash);
            }
        }
        self.persist_state().await?;
        self.reconcile_queue().await;
        let removed_hashes = targets
            .into_iter()
            .map(|(hash, _)| {
                self.publish_event(torrent_removed_event(hash, delete_data));
                hash
            })
            .collect();
        self.publish_event(stats_updated_event());
        Ok(removed_hashes)
    }

    /// Resolve the download directory for a torrent: per-torrent override,
    /// profile creation snapshot, then global config, then a default temp dir.
    pub(super) async fn resolve_download_dir(&self, t: &Torrent) -> String {
        self.policy_storage_paths(t).await.0
    }

    /// Resolve the active incomplete path for a torrent. Unlike the legacy
    /// helper, this sees a profile's create-time incomplete-path snapshot.
    pub(super) async fn resolve_incomplete_dir_for(&self, t: &Torrent) -> String {
        self.policy_storage_paths(t).await.1
    }

    pub(super) async fn ensure_torrent_limiter(
        &self,
        hash: InfoHash,
        download_limit: u64,
        upload_limit: u64,
    ) -> Arc<swarmotter_core::bandwidth::RateLimiter> {
        self.torrent_limiters
            .write()
            .await
            .entry(hash)
            .or_insert_with(|| {
                Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                    download_limit,
                    upload_limit,
                ))
            })
            .clone()
    }

    pub(super) fn effective_per_torrent_peer_limit(configured: usize) -> usize {
        if configured == 0 {
            DEFAULT_PER_TORRENT_PEER_LIMIT
        } else {
            configured
        }
    }

    pub(super) async fn ensure_torrent_peer_permit_pool(
        &self,
        hash: InfoHash,
    ) -> Arc<PeerPermitPool> {
        if let Some(pool) = self
            .torrent_peer_permit_pools
            .read()
            .await
            .get(&hash)
            .cloned()
        {
            return pool;
        }
        let configured = self.config.read().await.bandwidth.max_peers_per_torrent;
        let limit = Self::effective_per_torrent_peer_limit(configured);
        let candidate = PeerPermitPool::new(limit, self.peer_sessions_denied.clone())
            .unwrap_or_else(|_| {
                PeerPermitPool::invalid_fail_closed(limit, self.peer_sessions_denied.clone())
            });
        self.torrent_peer_permit_pools
            .write()
            .await
            .entry(hash)
            .or_insert(candidate)
            .clone()
    }

    pub(super) async fn peer_session_budget(&self, hash: InfoHash) -> PeerSessionBudget {
        let global = self.peer_permit_pool.read().await.clone();
        let torrent = self.ensure_torrent_peer_permit_pool(hash).await;
        PeerSessionBudget::new(global, torrent)
    }

    pub(super) async fn peer_permit_snapshot(&self) -> PeerPermitSnapshot {
        self.peer_permit_pool.read().await.snapshot()
    }

    pub(super) async fn build_peer_permit_configuration(
        &self,
        config: &Config,
    ) -> Result<PeerPermitConfiguration> {
        let global = PeerPermitPool::new(
            config.bandwidth.max_peers,
            self.peer_sessions_denied.clone(),
        )?;
        let per_torrent_limit =
            Self::effective_per_torrent_peer_limit(config.bandwidth.max_peers_per_torrent);
        let hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let per_torrent = hashes
            .into_iter()
            .map(|hash| {
                PeerPermitPool::new(per_torrent_limit, self.peer_sessions_denied.clone())
                    .map(|pool| (hash, pool))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(PeerPermitConfiguration {
            global,
            per_torrent,
        })
    }

    pub(super) async fn install_peer_permit_configuration(&self, next: PeerPermitConfiguration) {
        *self.peer_permit_pool.write().await = next.global;
        *self.torrent_peer_permit_pools.write().await = next.per_torrent;
    }

    pub(super) async fn current_peer_permit_configuration(&self) -> PeerPermitConfiguration {
        PeerPermitConfiguration {
            global: self.peer_permit_pool.read().await.clone(),
            per_torrent: self.torrent_peer_permit_pools.read().await.clone(),
        }
    }

    pub(super) async fn verify_peer_permit_configuration_identity(
        &self,
        expected: &PeerPermitConfiguration,
    ) -> Result<()> {
        let actual = self.current_peer_permit_configuration().await;
        let same = Arc::ptr_eq(&actual.global, &expected.global)
            && actual.global.snapshot().limit == expected.global.snapshot().limit
            && actual.per_torrent.len() == expected.per_torrent.len()
            && expected.per_torrent.iter().all(|(hash, pool)| {
                actual.per_torrent.get(hash).is_some_and(|actual| {
                    Arc::ptr_eq(actual, pool) && actual.snapshot().limit == pool.snapshot().limit
                })
            });
        if same {
            Ok(())
        } else {
            Err(CoreError::Internal(
                "peer permit configuration identity or size mismatch".into(),
            ))
        }
    }

    pub(super) async fn wait_for_peer_permit_configuration_drain(
        &self,
        permits: &PeerPermitConfiguration,
    ) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let global_drained = permits.global.snapshot().in_use == 0;
                let torrents_drained = permits
                    .per_torrent
                    .values()
                    .all(|pool| pool.snapshot().in_use == 0);
                if global_drained && torrents_drained {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            CoreError::Internal(
                "timed out awaiting old peer-session permits during reconstruction".into(),
            )
        })
    }

    #[cfg(test)]
    pub(super) fn inject_peer_reconfiguration_failure_after_teardown(&self) {
        self.peer_reconfiguration_fail_after_teardown
            .store(true, Ordering::Release);
    }

    pub(super) fn peer_reconfiguration_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.peer_reconfiguration_fail_after_teardown
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    pub(super) fn inject_peer_reconfiguration_persistence_failure(&self) {
        self.peer_reconfiguration_fail_persistence
            .store(true, Ordering::Release);
    }

    pub(super) fn peer_reconfiguration_persistence_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.peer_reconfiguration_fail_persistence
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    pub(super) async fn pause_peer_reconfiguration_before_reconstruction(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.peer_reconfiguration_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    pub(super) async fn wait_at_peer_reconfiguration_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self.peer_reconfiguration_pause.lock().await.take() {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    #[cfg(test)]
    pub(super) async fn pause_peer_reconfiguration_before_persistence(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.peer_reconfiguration_persistence_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    pub(super) async fn wait_at_peer_reconfiguration_persistence_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self
            .peer_reconfiguration_persistence_pause
            .lock()
            .await
            .take()
        {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    #[cfg(test)]
    pub(super) fn inject_add_mutation_persistence_failure(&self) {
        self.add_mutation_fail_persistence
            .store(true, Ordering::Release);
    }

    pub(super) fn add_mutation_persistence_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.add_mutation_fail_persistence
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    pub(super) async fn pause_watch_after_bounded_read(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.watch_after_read_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    pub(super) async fn wait_at_watch_after_read_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self.watch_after_read_pause.lock().await.take() {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    /// Preflight a candidate using its resolved profile storage snapshot.
    pub(super) async fn preflight_storage_for_torrent(
        &self,
        torrent: &Torrent,
        total_length: u64,
    ) -> Result<()> {
        let cfg = self.config.read().await.clone();
        if cfg.storage.minimum_free_space_bytes == 0 && cfg.storage.minimum_free_space_percent == 0
        {
            return Ok(());
        }
        let (complete_dir, active_dir) = Self::policy_storage_paths_with_config(&cfg, torrent);
        for dir in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
            swarmotter_core::storage::check_storage_preflight(&dir, &cfg.storage, total_length)?;
        }
        Ok(())
    }

    /// Validate ownership for a candidate torrent at its effective storage
    /// paths. Existing profile-derived paths are resolved from their durable
    /// snapshots; no profile value is copied into an override.
    pub(super) async fn ensure_storage_paths_available_for_torrent(
        &self,
        torrent: &Torrent,
        exclude: Option<InfoHash>,
    ) -> Result<()> {
        let cfg = self.config.read().await.clone();
        let (complete_dir, active_dir) = Self::policy_storage_paths_with_config(&cfg, torrent);
        self.ensure_storage_paths_available_at_paths_except(
            &torrent.meta,
            &complete_dir,
            &active_dir,
            exclude,
        )
        .await
    }

    pub(super) async fn ensure_storage_paths_available_at_paths_except(
        &self,
        meta: &meta::TorrentMeta,
        complete_dir: &str,
        active_dir: &str,
        exclude: Option<InfoHash>,
    ) -> Result<()> {
        let candidates = unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)])
            .into_iter()
            .map(|root| {
                swarmotter_core::storage::StorageIo::new(meta.clone(), root).path_ownership()
            })
            .collect::<Result<Vec<_>>>()?;
        let cfg = self.config.read().await.clone();
        let existing = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for torrent in existing {
            if exclude.is_some_and(|hash| torrent.info_hash() == hash) {
                continue;
            }
            let (complete_dir, active_dir) = Self::policy_storage_paths_with_config(&cfg, &torrent);
            for root in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
                let ownership =
                    swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), root)
                        .path_ownership()?;
                for candidate in &candidates {
                    candidate.ensure_compatible_with(&ownership)?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn reserve_resolved_magnet_metadata(
        &self,
        hash: InfoHash,
        resolved: meta::TorrentMeta,
        complete_dir: String,
        active_dir: String,
        cancellation: StorageWorkCancellation,
    ) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(storage_work_cancelled_error());
        }
        if resolved.info_hash != hash {
            return Err(CoreError::MalformedTorrent(
                "resolved magnet metadata info hash changed during preflight".into(),
            ));
        }
        // Validate ownership before waiting for a root budget, but never hold
        // the ownership mutex while a bounded root is saturated. Other adds,
        // moves, and metadata resolutions must remain able to make progress.
        let initial_previous = {
            let _storage_ownership = tokio::select! {
                guard = self.storage_ownership_lock.lock() => guard,
                _ = cancellation.cancelled() => return Err(storage_work_cancelled_error()),
            };
            self.ensure_storage_paths_available_at_paths_except(
                &resolved,
                &complete_dir,
                &active_dir,
                Some(hash),
            )
            .await?;
            self.registry
                .lock()
                .await
                .get(&hash)
                .cloned()
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?
        };
        let storage_admission = loop {
            if cancellation.is_cancelled() {
                return Err(storage_work_cancelled_error());
            }
            let storage_admission = {
                let cfg = self.config.read().await;
                storage_root_admission_for_path(&cfg, Path::new(&active_dir))
            };
            let Some(admission) = storage_admission else {
                break None;
            };
            if !self
                .storage_admissions
                .declared_bytes_can_fit(&admission, resolved.total_length)
            {
                return Err(CoreError::Storage(format!(
                    "storage root admission cannot fit resolved magnet metadata for {}: declared payload {} bytes exceeds configured active-byte limit {}",
                    admission.root.display(),
                    resolved.total_length,
                    admission.max_active_bytes
                )));
            }
            // Register before testing the atomic reservation so a completed
            // root engine or a configuration change cannot lose its wake-up.
            let changed = self.storage_admissions.changed();
            if self
                .storage_admissions
                .reserve(hash, &admission, resolved.total_length)
                .await
                .is_ok()
            {
                break Some(admission);
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Err(storage_work_cancelled_error()),
                _ = changed => {}
            }
        };
        if cancellation.is_cancelled() {
            if storage_admission.is_some() {
                self.storage_admissions.release(&hash).await;
            }
            return Err(storage_work_cancelled_error());
        }
        let _storage_ownership = tokio::select! {
            guard = self.storage_ownership_lock.lock() => guard,
            _ = cancellation.cancelled() => {
                if storage_admission.is_some() {
                    self.storage_admissions.release(&hash).await;
                }
                return Err(storage_work_cancelled_error());
            }
        };
        if cancellation.is_cancelled() {
            if storage_admission.is_some() {
                self.storage_admissions.release(&hash).await;
            }
            return Err(storage_work_cancelled_error());
        }
        if let Err(error) = self
            .ensure_storage_paths_available_at_paths_except(
                &resolved,
                &complete_dir,
                &active_dir,
                Some(hash),
            )
            .await
        {
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, initial_previous.meta.total_length)
                    .await;
            }
            return Err(error);
        }
        if cancellation.is_cancelled() {
            if storage_admission.is_some() {
                self.storage_admissions.release(&hash).await;
            }
            return Err(storage_work_cancelled_error());
        }
        let previous = self.registry.lock().await.get(&hash).cloned();
        let Some(previous) = previous else {
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, initial_previous.meta.total_length)
                    .await;
            }
            return Err(CoreError::NotFound("torrent".into()));
        };
        let updated = {
            let mut registry = self.registry.lock().await;
            registry.get_mut(&hash).map(|torrent| {
                let empty_state = EngineState {
                    piece_count: resolved.piece_count(),
                    total_length: resolved.total_length,
                    ..EngineState::default()
                };
                apply_resolved_metadata(torrent, &resolved, &empty_state);
            })
        };
        if updated.is_none() {
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, previous.meta.total_length)
                    .await;
            }
            return Err(CoreError::NotFound("torrent".into()));
        }
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                *torrent = previous.clone();
            }
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, previous.meta.total_length)
                    .await;
            }
            return Err(error);
        }
        self.publish_event(torrent_metadata_event(hash));
        Ok(())
    }
}
