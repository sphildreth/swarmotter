// SPDX-License-Identifier: Apache-2.0

use super::*;

impl DaemonRuntime {
    pub(super) async fn configured_peer_worker_limit(&self) -> usize {
        let cfg = self.config.read().await;
        Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent)
    }

    pub(super) async fn apply_peer_worker_limits(&self) {
        let limit = self.configured_peer_worker_limit().await;
        let senders: Vec<tokio::sync::mpsc::Sender<EngineCommand>> =
            self.engine_cmds.lock().await.values().cloned().collect();
        for tx in senders {
            let _ = tx.send(EngineCommand::UpdatePeerWorkerLimit(limit)).await;
        }
    }

    pub(super) async fn scheduler_diagnostics(
        &self,
        desired: &[TorrentKey],
    ) -> SchedulerDiagnostics {
        let cfg = self.config.read().await.clone();
        let mut queue = self.queue.lock().await.clone();
        queue.limits = cfg.queue.clone();
        let retry_after = self.engine_retry_after.read().await.clone();
        let running: HashSet<TorrentKey> =
            self.engine_handles.read().await.keys().copied().collect();
        let now = Instant::now();
        let reg = self.registry.lock().await;

        let mut requested_downloads = 0usize;
        let mut requested_metadata_fetches = 0usize;
        let mut seen = HashSet::new();
        let bypass_set = queue.bypass.iter().copied().collect::<HashSet<_>>();
        for hash in queue.bypass.iter().chain(queue.order.iter()) {
            if !seen.insert(*hash) {
                continue;
            }
            if retry_after
                .get(hash)
                .is_some_and(|retry_at| *retry_at > now)
            {
                continue;
            }
            let Some(torrent) = reg.get(hash) else {
                continue;
            };
            let bypass = bypass_set.contains(hash);
            let already_active = matches!(
                torrent.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            );
            let policy = Self::effective_policy_with_config(&cfg, torrent);
            let profile_auto_start = matches!(
                policy.start_behavior.value,
                swarmotter_core::config::StartBehavior::Start
            );
            let metadata_preview = torrent.policy.preview_until_started && torrent.needs_metadata;
            if !(profile_auto_start || bypass || already_active || metadata_preview) {
                continue;
            }
            if !matches!(
                torrent.state,
                TorrentState::Queued
                    | TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
            ) {
                continue;
            }
            if torrent.needs_metadata {
                requested_metadata_fetches += 1;
            } else {
                requested_downloads += 1;
            }
        }

        let mut granted_downloads = 0usize;
        let mut granted_metadata_fetches = 0usize;
        for hash in desired {
            if reg.get(hash).is_some_and(|torrent| torrent.needs_metadata) {
                granted_metadata_fetches += 1;
            } else {
                granted_downloads += 1;
            }
        }

        let mut running_downloads = 0usize;
        let mut running_metadata_fetches = 0usize;
        for hash in &running {
            let Some(torrent) = reg.get(hash) else {
                continue;
            };
            if !matches!(
                torrent.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            ) {
                continue;
            }
            if torrent.needs_metadata {
                running_metadata_fetches += 1;
            } else {
                running_downloads += 1;
            }
        }

        let active_peer_workers = reg
            .torrents
            .values()
            .map(|torrent| torrent.active_peer_workers)
            .sum();
        let running_engines = running.len();
        let effective_peer_worker_limit =
            Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent);
        let peer_worker_budget = effective_peer_worker_limit.saturating_mul(running_engines);
        let peer_permits = self.peer_permit_snapshot().await;

        SchedulerDiagnostics {
            managed_torrents: reg.torrents.len(),
            queued_torrents: reg
                .torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Queued)
                .count(),
            running_engines,
            running_downloads,
            running_metadata_fetches,
            requested_downloads,
            requested_metadata_fetches,
            granted_downloads,
            granted_metadata_fetches,
            retry_backoff_torrents: retry_after
                .values()
                .filter(|retry_at| **retry_at > now)
                .count(),
            active_download_limit: cfg.queue.max_active_downloads,
            active_metadata_fetch_limit: cfg.queue.max_active_metadata_fetches,
            active_seed_limit: cfg.queue.max_active_seeds,
            peer_worker_global_limit: cfg.bandwidth.max_peers,
            peer_worker_per_torrent_limit: cfg.bandwidth.max_peers_per_torrent,
            effective_peer_worker_limit,
            peer_worker_budget,
            active_peer_workers,
            peer_limit: peer_permits.limit,
            peer_permits_in_use: peer_permits.in_use,
            peer_permits_available: peer_permits.available,
            peer_sessions_denied: peer_permits.denied,
            download_slots_saturated: cfg.queue.max_active_downloads > 0
                && requested_downloads > granted_downloads
                && granted_downloads >= cfg.queue.max_active_downloads,
            metadata_fetch_slots_saturated: cfg.queue.max_active_metadata_fetches > 0
                && requested_metadata_fetches > granted_metadata_fetches
                && granted_metadata_fetches >= cfg.queue.max_active_metadata_fetches,
            peer_worker_budget_saturated: peer_worker_budget > 0
                && active_peer_workers >= peer_worker_budget,
        }
    }

    pub(super) async fn active_download_hashes(&self) -> Vec<TorrentKey> {
        let running: Vec<TorrentKey> = self.engine_handles.read().await.keys().copied().collect();
        let reg = self.registry.lock().await;
        running
            .into_iter()
            .filter(|hash| {
                reg.get(hash).is_some_and(|t| {
                    matches!(
                        t.state,
                        TorrentState::Downloading | TorrentState::DownloadingMetadata
                    )
                })
            })
            .collect()
    }

    pub(super) async fn desired_download_hashes(&self) -> Vec<TorrentKey> {
        self.desired_download_hashes_excluding(None).await
    }

    pub(super) async fn desired_download_hashes_excluding(
        &self,
        excluded: Option<TorrentKey>,
    ) -> Vec<TorrentKey> {
        let cfg = self.config.read().await.clone();
        let retry_after = self.engine_retry_after.read().await.clone();
        let mut storage_plan =
            StorageAdmissionPlan::from_records(self.storage_admissions.records().await);
        let mut queue = self.queue.lock().await;
        queue.limits = cfg.queue.clone();
        let reg = self.registry.lock().await;
        let now = Instant::now();
        let stale_queue_entries = queue
            .order
            .iter()
            .chain(queue.bypass.iter())
            .filter(|hash| !reg.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        queue.remove_many(stale_queue_entries);

        let download_limit = queue.limits.max_active_downloads;
        let metadata_limit = queue.limits.max_active_metadata_fetches;
        let mut active = Vec::new();
        let mut active_set = HashSet::new();
        let mut active_downloads = 0usize;
        let mut active_metadata_fetches = 0usize;
        let bypass_set = queue.bypass.iter().copied().collect::<HashSet<_>>();
        // Preserve user queue order within a priority band. Profile priorities
        // are resolved live, so editing a profile takes effect at the next
        // reconciliation without rewriting durable queue order.
        let mut prioritized_order = queue
            .order
            .iter()
            .enumerate()
            .map(|(position, hash)| {
                let priority = reg
                    .get(hash)
                    .map(|torrent| {
                        Self::effective_policy_with_config(&cfg, torrent)
                            .queue_priority
                            .value
                            .weight()
                    })
                    .unwrap_or(swarmotter_core::policy::QueuePriority::Normal.weight());
                (*hash, position, priority)
            })
            .collect::<Vec<_>>();
        prioritized_order
            .sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.1.cmp(&right.1)));
        for hash in queue
            .bypass
            .iter()
            .chain(prioritized_order.iter().map(|(hash, _, _)| hash))
        {
            if excluded.is_some_and(|excluded| &excluded == hash) {
                continue;
            }
            let download_slots_full = download_limit > 0 && active_downloads >= download_limit;
            let metadata_slots_full =
                metadata_limit > 0 && active_metadata_fetches >= metadata_limit;
            if download_slots_full && metadata_slots_full {
                break;
            }
            if !active_set.insert(*hash) {
                continue;
            }
            if retry_after
                .get(hash)
                .is_some_and(|retry_at| *retry_at > now)
            {
                continue;
            }
            let Some(t) = reg.get(hash) else {
                continue;
            };
            let bypass = bypass_set.contains(hash);
            let already_active = matches!(
                t.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            );
            let policy = Self::effective_policy_with_config(&cfg, t);
            let profile_auto_start = matches!(
                policy.start_behavior.value,
                swarmotter_core::config::StartBehavior::Start
            );
            let metadata_preview = t.policy.preview_until_started && t.needs_metadata;
            let auto_startable = profile_auto_start || bypass || already_active || metadata_preview;
            let metadata_fetch = t.needs_metadata;
            if auto_startable
                && matches!(
                    t.state,
                    TorrentState::Queued
                        | TorrentState::Downloading
                        | TorrentState::DownloadingMetadata
                )
            {
                if metadata_fetch {
                    if metadata_slots_full {
                        continue;
                    }
                } else {
                    if download_slots_full {
                        continue;
                    }
                }
                if !metadata_preview {
                    if let Some(admission) = storage_root_admission_for_torrent(&cfg, t) {
                        // A configured root controls the complete engine lifetime,
                        // including a magnet's metadata phase. This avoids a burst
                        // of metadata resolutions silently exceeding a root's
                        // later payload budget.
                        if storage_plan
                            .admit(*hash, &admission, t.meta.total_length)
                            .is_err()
                        {
                            continue;
                        }
                    }
                }
                if metadata_fetch {
                    active_metadata_fetches += 1;
                } else {
                    active_downloads += 1;
                }
                active.push(*hash);
            }
        }
        active
    }

    pub(super) async fn reconcile_queue(&self) {
        let inactive_recovered = self.sweep_inactive_engine_handles("queue_reconcile").await;
        let stale_recovered = self.sweep_stale_active_torrents("queue_reconcile").await;
        let desired = self.desired_download_hashes().await;
        let current = self.active_download_hashes().await;
        tracing::debug!(
            inactive_recovered,
            stale_recovered,
            desired_downloads = desired.len(),
            current_downloads = current.len(),
            "queue reconciliation planned"
        );

        for hash in current {
            if !desired.contains(&hash) {
                self.force_stop_engine(&hash).await;
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(&hash) {
                    if !matches!(t.state, TorrentState::Paused | TorrentState::Completed) {
                        t.state = TorrentState::Queued;
                    }
                }
            }
        }

        for hash in desired {
            self.start_engine(hash).await;
        }
        self.apply_peer_worker_limits().await;
    }

    pub(super) async fn sweep_stale_active_torrents(&self, reason: &'static str) -> usize {
        let running: HashSet<TorrentKey> =
            self.engine_handles.read().await.keys().copied().collect();
        let retry_after = self.engine_retry_after.read().await.clone();
        let now = Instant::now();
        let recovered = {
            let mut reg = self.registry.lock().await;
            let mut recovered = Vec::new();
            for (hash, torrent) in reg.torrents.iter_mut() {
                if matches!(
                    torrent.state,
                    TorrentState::Downloading | TorrentState::DownloadingMetadata
                ) && !running.contains(hash)
                {
                    torrent.state = TorrentState::Queued;
                    torrent.error = Some(STALE_ACTIVE_RECOVERY_MESSAGE.into());
                    recovered.push(*hash);
                }
            }
            recovered
        };

        if recovered.is_empty() {
            return 0;
        }

        {
            let mut queue = self.queue.lock().await;
            queue.add_many(recovered.iter().copied());
            queue.clear_bypass_many(recovered.iter().copied());
            queue.move_many_to_bottom(recovered.iter().copied());
        }

        for hash in &recovered {
            tracing::warn!(
                info_hash = %hash,
                reason,
                retry_suppressed = retry_after
                    .get(hash)
                    .is_some_and(|retry_at| *retry_at > now),
                "stale active torrent queued for lifecycle recovery"
            );
        }
        recovered.len()
    }

    pub(super) async fn sweep_inactive_engine_handles(&self, reason: &'static str) -> usize {
        let running: Vec<TorrentKey> = self.engine_handles.read().await.keys().copied().collect();
        let stale: Vec<(TorrentKey, Option<TorrentState>)> = {
            let reg = self.registry.lock().await;
            running
                .into_iter()
                .filter_map(|hash| match reg.get(&hash) {
                    Some(t)
                        if matches!(
                            t.state,
                            TorrentState::Downloading | TorrentState::DownloadingMetadata
                        ) =>
                    {
                        None
                    }
                    Some(t) => Some((hash, Some(t.state))),
                    None => Some((hash, None)),
                })
                .collect()
        };

        for (hash, state) in &stale {
            tracing::warn!(
                info_hash = %hash,
                reason,
                state = ?state,
                "stale inactive engine bookkeeping cleared"
            );
            self.force_stop_engine(hash).await;
            if matches!(state, Some(TorrentState::Queued)) {
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.error = Some(STALE_INACTIVE_ENGINE_RECOVERY_MESSAGE.into());
                }
            }
        }

        stale.len()
    }

    pub(super) async fn schedule_reconcile_queue(&self, reason: &'static str) {
        let mut state = self.queue_reconcile.lock().await;
        if state.scheduled {
            state.dirty = true;
            tracing::debug!(
                reason,
                "queue reconciliation already scheduled; marked dirty"
            );
            return;
        }

        state.scheduled = true;
        state.dirty = false;
        drop(state);

        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.run_scheduled_reconcile_queue(reason).await;
        });
    }

    pub(super) fn schedule_delayed_reconcile_queue(&self, reason: &'static str, delay: Duration) {
        let runtime = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            runtime.schedule_reconcile_queue(reason).await;
        });
    }

    pub(super) async fn run_scheduled_reconcile_queue(self, reason: &'static str) {
        tokio::time::sleep(QUEUE_RECONCILE_DEBOUNCE).await;
        loop {
            {
                let mut state = self.queue_reconcile.lock().await;
                state.dirty = false;
            }
            tracing::debug!(reason, "queue reconciliation started");
            self.reconcile_queue().await;

            let mut state = self.queue_reconcile.lock().await;
            if state.dirty {
                state.dirty = false;
                tracing::debug!(reason, "queue reconciliation dirty; running again");
                drop(state);
                continue;
            }

            state.scheduled = false;
            tracing::debug!(reason, "queue reconciliation complete");
            break;
        }
    }

    pub(super) async fn engine_task_finished(&self, hash: TorrentKey) {
        self.engine_cmds.lock().await.remove(&hash);
        self.engine_handles.write().await.remove(&hash);
        self.engine_storage_cancellations.lock().await.remove(&hash);
        self.storage_admissions.release(&hash).await;
    }

    pub(super) async fn record_engine_containment_cancellation(
        &self,
        hash: TorrentKey,
        needs_metadata: bool,
    ) {
        let mut reg = self.registry.lock().await;
        let Some(torrent) = reg.get_mut(&hash) else {
            return;
        };
        if matches!(
            torrent.state,
            TorrentState::Downloading | TorrentState::DownloadingMetadata | TorrentState::Queued
        ) {
            torrent.containment_recovery_intent = Some(if needs_metadata {
                ContainmentRecoveryIntent::DownloadingMetadata
            } else {
                ContainmentRecoveryIntent::Downloading
            });
        }
    }

    pub(super) async fn queue_torrent_for_retry(
        &self,
        hash: TorrentKey,
        message: &'static str,
        delay: Duration,
    ) -> bool {
        let queued = {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(&hash) else {
                return false;
            };
            if !matches!(
                t.state,
                TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
                    | TorrentState::Queued
            ) {
                return false;
            }
            t.state = TorrentState::Queued;
            t.error = Some(message.into());
            true
        };
        if !queued {
            return false;
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(hash);
            queue.clear_bypass(&hash);
            queue.move_to_bottom(&hash);
        }
        self.engine_retry_after
            .write()
            .await
            .insert(hash, Instant::now() + delay);
        tracing::warn!(
            info_hash = %hash,
            reason = message,
            retry_delay_seconds = delay.as_secs(),
            "torrent queued for retry"
        );
        true
    }

    pub(super) async fn handle_engine_task_error(
        &self,
        hash: TorrentKey,
        needs_metadata: bool,
        error: CoreError,
    ) -> bool {
        let retry_metadata = needs_metadata && is_retryable_magnet_metadata_discovery_error(&error);
        if retry_metadata {
            tracing::debug!(
                info_hash = %hash,
                error = %error,
                "magnet metadata discovery found no peers; retry scheduled"
            );
            let _ = self
                .queue_torrent_for_retry(
                    hash,
                    MAGNET_METADATA_NO_PEERS_RETRY_MESSAGE,
                    MAGNET_METADATA_NO_PEERS_RETRY_DELAY,
                )
                .await;
            self.schedule_delayed_reconcile_queue("magnet_metadata_no_peers", Duration::ZERO);
            return true;
        }

        let state = if error.is_network_blocked() {
            TorrentState::NetworkBlocked
        } else if matches!(&error, CoreError::Storage(_)) {
            TorrentState::StorageError
        } else {
            TorrentState::Error
        };
        tracing::warn!(info_hash = %hash, error = %error, "engine task failed");
        let mut changed = false;
        {
            let mut reg = self.registry.lock().await;
            if let Some(t) = reg.get_mut(&hash) {
                t.state = state;
                t.error = Some(error.to_string());
                changed = true;
            }
        }
        if changed {
            self.publish_torrent_event("torrent_error", hash, state);
            self.publish_event(stats_updated_event());
        }
        false
    }

    pub(super) async fn shared_dht_runner(
        &self,
        binder: Arc<dyn swarmotter_core::net::NetworkBinder>,
        peer_id: [u8; 20],
    ) -> Option<Arc<crate::dht::DhtRunner>> {
        let (dht_enabled, socks5_enabled, bootstrap_nodes, dht_port) = {
            let cfg = self.config.read().await;
            (
                cfg.dht.enabled,
                cfg.network.socks5.enabled,
                cfg.dht.bootstrap_nodes.clone(),
                cfg.dht.port,
            )
        };
        // SOCKS5 support intentionally has no UDP ASSOCIATE path. Configuration
        // validation requires DHT to be disabled, and this guard remains a
        // defense-in-depth check for runtime generations created by older API
        // clients or partially restored state.
        if !dht_enabled || socks5_enabled || !self.network_health.read().await.traffic_allowed {
            return None;
        }
        if let Some(existing) = self.dht_runner.lock().await.clone() {
            return Some(existing);
        }
        let bootstrap =
            crate::dht::resolve_bootstrap_with_binder(binder.as_ref(), &bootstrap_nodes).await;
        let self_id = crate::dht::DhtRunner::derive_from_peer_id(&peer_id);
        let runner = Arc::new(crate::dht::DhtRunner::new(
            self_id, binder, bootstrap, dht_port,
        ));
        *self.dht_runner.lock().await = Some(runner.clone());
        Some(runner)
    }

    /// Start the live engine task for a torrent (downloading). No-op if the
    /// torrent is paused, queued, or already running.
    pub async fn start_engine(&self, hash: TorrentKey) {
        let _data_plane_transition = self.data_plane_transition_lock.lock().await;
        self.start_engine_while_transition_locked(hash).await;
    }

    #[cfg(test)]
    pub(super) async fn pause_engine_start_before_storage_admission(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.storage_admission_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    async fn wait_at_storage_admission_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self.storage_admission_pause.lock().await.take() {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    /// Start one engine while the caller owns `data_plane_transition_lock`.
    /// This is used only by serialized reconstruction transactions so normal
    /// API/queue starts cannot interleave with a partially rebuilt live set.
    pub(super) async fn start_engine_while_transition_locked(&self, hash: TorrentKey) {
        let health = self.network_health.read().await.clone();
        if !health.traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
            // Network blocked: do not start the engine; mark torrent.
            let mut changed = false;
            {
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(&hash) {
                    t.state = TorrentState::NetworkBlocked;
                    t.error = Some(health.detail.clone());
                    changed = true;
                }
            }
            if changed {
                self.publish_torrent_event("torrent_changed", hash, TorrentState::NetworkBlocked);
                self.publish_event(stats_updated_event());
            }
            return;
        }

        // Already running?
        if self.engine_handles.read().await.contains_key(&hash) {
            return;
        }
        self.engine_retry_after.write().await.remove(&hash);

        let config = self.config.read().await.clone();
        let peer_filter = self.peer_filter.read().await.clone();
        let snapshot = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(&hash) else {
                return;
            };
            EngineStartSnapshot::from_torrent(t, &config)
        };
        let magnet = match snapshot.magnet_params() {
            Ok(magnet) => magnet,
            Err(error) => {
                tracing::error!(
                    info_hash = %hash,
                    error = %error,
                    "torrent engine start rejected malformed unresolved magnet"
                );
                if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                    torrent.state = TorrentState::Error;
                    torrent.error = Some(error.to_string());
                }
                self.publish_torrent_event("torrent_error", hash, TorrentState::Error);
                self.publish_event(stats_updated_event());
                return;
            }
        };

        let (
            meta,
            active_dir,
            complete_dir,
            listen_port,
            preallocate,
            sparse,
            max_peer_workers,
            allow_ipv6,
            pex_enabled,
            pex_max_peers,
            minimum_free_space_bytes,
            minimum_free_space_percent,
            direct_peers,
            magnet,
            needs_metadata,
            metadata_only,
            intake_selection,
            partial_file_suffix,
            tracker_host_rules,
        ) = {
            let complete_dir = snapshot.complete_dir.clone();
            let active_dir = snapshot.active_dir.clone();
            // `x.pe` is parsed only as an IP literal. These candidates still
            // flow through the engine's normal peer filter and contained
            // binder for both metadata and payload discovery.
            let direct_peers = snapshot
                .magnet_direct_peers
                .iter()
                .map(|peer| swarmotter_core::peer::PeerAddr {
                    ip: peer.ip,
                    port: peer.port,
                })
                .collect::<Vec<_>>();
            let cfg = &config;
            let preallocate = cfg.storage.preallocate;
            let sparse = cfg.storage.sparse;
            let allow_ipv6 = cfg.torrent.allow_ipv6 && cfg.network.allow_ipv6;
            let pex_enabled = cfg.pex.enabled;
            let pex_max_peers = cfg.pex.max_peers;
            let minimum_free_space_bytes = cfg.storage.minimum_free_space_bytes;
            let minimum_free_space_percent = cfg.storage.minimum_free_space_percent;
            let max_peer_workers =
                Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent);
            (
                snapshot.meta.clone(),
                active_dir,
                complete_dir,
                cfg.torrent.listen_port,
                preallocate,
                sparse,
                max_peer_workers,
                allow_ipv6,
                pex_enabled,
                pex_max_peers,
                minimum_free_space_bytes,
                minimum_free_space_percent,
                direct_peers,
                magnet,
                snapshot.needs_metadata,
                snapshot.metadata_only,
                snapshot.intake_selection.clone(),
                snapshot.partial_file_suffix.clone(),
                snapshot.tracker_host_rules.clone(),
            )
        };

        if !self.registry.lock().await.contains(&hash) {
            return;
        }

        let preflight_content_bytes = if needs_metadata { 0 } else { meta.total_length };
        if !metadata_only
            && (preflight_content_bytes > 0
                || minimum_free_space_bytes > 0
                || minimum_free_space_percent > 0)
        {
            let mut cfg = self.config.read().await.storage.clone();
            cfg.minimum_free_space_bytes = minimum_free_space_bytes;
            cfg.minimum_free_space_percent = minimum_free_space_percent;
            for dir in unique_pathbufs([PathBuf::from(&active_dir), PathBuf::from(&complete_dir)]) {
                if let Err(e) = swarmotter_core::storage::check_storage_preflight(
                    &dir,
                    &cfg,
                    preflight_content_bytes,
                ) {
                    tracing::warn!(
                        info_hash = %hash,
                        error = %e,
                        error_code = %e.code(),
                        "engine start blocked by storage preflight"
                    );
                    let mut reg = self.registry.lock().await;
                    if let Some(t) = reg.get_mut(&hash) {
                        t.state = TorrentState::StorageError;
                        t.error = Some(e.to_string());
                    }
                    self.publish_torrent_event("torrent_error", hash, TorrentState::StorageError);
                    self.publish_event(stats_updated_event());
                    return;
                }
            }
        }

        let storage_write_limiter = if metadata_only {
            None
        } else {
            self.wait_at_storage_admission_test_pause().await;
            let storage_admission = {
                let cfg = self.config.read().await;
                storage_root_admission_for_path(&cfg, Path::new(&active_dir))
            };
            if let Some(admission) = &storage_admission {
                if let Err(error) = self
                    .storage_admissions
                    .reserve(hash, admission, preflight_content_bytes)
                    .await
                {
                    // This is queue admission, not a payload/storage failure. Keep
                    // the torrent eligible for a later reconcile when root work
                    // finishes rather than turning a bounded queue into an error.
                    tracing::debug!(
                        info_hash = %hash,
                        error = %error,
                        root = %admission.root.display(),
                        "engine start deferred by storage-root admission"
                    );
                    let mut changed = false;
                    if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                        if matches!(
                            torrent.state,
                            TorrentState::Queued
                                | TorrentState::Downloading
                                | TorrentState::DownloadingMetadata
                        ) {
                            torrent.state = TorrentState::Queued;
                            torrent.error = Some(error.to_string());
                            changed = true;
                        }
                    }
                    if changed {
                        self.publish_torrent_event("torrent_changed", hash, TorrentState::Queued);
                        self.publish_event(stats_updated_event());
                    }
                    return;
                }
                self.storage_admissions.write_limiter(admission).await
            } else {
                None
            }
        };
        let resume_dir = config.storage.resume_dir.as_ref().map(PathBuf::from);
        let cow_strategy = config.storage.cow_strategy;
        let storage_metrics = Some(
            self.storage_metrics
                .metrics_for_path(&config, Path::new(&active_dir)),
        );

        let state = Arc::new(Mutex::new(EngineState::default()));
        self.engine_states.write().await.insert(hash, state.clone());
        // Installed before task visibility so lifecycle operations can cancel
        // a saturated metadata-admission or startup-recheck wait.
        let storage_work_cancellation = StorageWorkCancellation::new();
        self.engine_storage_cancellations
            .lock()
            .await
            .insert(hash, storage_work_cancellation.clone());

        let binder: Arc<dyn swarmotter_core::net::NetworkBinder> = self.make_binder().await;
        let peer_id = make_peer_id();
        let (tx, rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
        self.engine_cmds.lock().await.insert(hash, tx);

        // A torrent owns one limiter for its entire retained lifetime. Engine
        // restarts and the downloader-to-seeder transition reuse these exact
        // buckets, while the process-wide limiter remains a separate layer.
        let limiter = {
            let mut limiters = self.torrent_limiters.write().await;
            limiters
                .entry(hash)
                .or_insert_with(|| {
                    Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                        snapshot.download_limit,
                        snapshot.upload_limit,
                    ))
                })
                .clone()
        };
        let peer_session_budget = self.peer_session_budget(hash).await;
        // Peer transport selection from config. SOCKS5 currently supports
        // only TCP CONNECT, so uTP is defensively disabled here as well as by
        // full-config validation; no direct UDP fallback is possible.
        let (utp_enabled, utp_prefer_tcp, encryption_mode) = (
            config.torrent.utp_enabled && !config.network.socks5.enabled,
            config.torrent.utp_prefer_tcp,
            snapshot.encryption_mode,
        );

        let state_for_summary = state.clone();
        let hash_for_task = hash;
        let registry = self.registry.clone();
        let selfish_completion_enabled = self.selfish_completion_enabled.clone();
        let runtime_for_task = self.clone();
        let storage_work_cancellation_for_task = storage_work_cancellation.clone();
        // DHT runner for trackerless peer discovery. Gated by config and
        // containment; the engine disables DHT for private torrents.
        let dht_runner = self.shared_dht_runner(binder.clone(), peer_id).await;
        let mut engine = TorrentEngine::with_limiter(
            meta.clone(),
            active_dir.clone().into(),
            peer_id,
            binder,
            state.clone(),
            rx,
            direct_peers,
            listen_port,
            limiter,
            magnet,
        )
        .with_complete_dir(complete_dir.clone().into())
        .with_global_limiter(Some(self.global_limiter.clone()))
        .with_transport(utp_enabled, utp_prefer_tcp)
        .with_encryption_mode(encryption_mode)
        .with_preallocate(preallocate)
        .with_sparse(sparse)
        .with_cow_strategy(cow_strategy)
        .with_resume_dir(resume_dir)
        .with_torrent_key(hash)
        .with_partial_file_suffix(partial_file_suffix)
        .with_storage_reserve(minimum_free_space_bytes, minimum_free_space_percent)
        .with_storage_write_limiter(storage_write_limiter)
        .with_storage_metrics(storage_metrics);
        engine = match engine
            .with_file_selection(snapshot.priorities.clone(), snapshot.wanted.clone())
        {
            Ok(engine) => engine,
            Err(error) => {
                tracing::error!(info_hash = %hash, error = %error, "torrent file layout rejected");
                if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                    torrent.state = TorrentState::StorageError;
                    torrent.error = Some(error.to_string());
                }
                self.engine_storage_cancellations.lock().await.remove(&hash);
                self.storage_admissions.release(&hash).await;
                self.publish_torrent_event("torrent_changed", hash, TorrentState::StorageError);
                self.publish_event(stats_updated_event());
                return;
            }
        };
        engine = engine
            .with_peer_worker_limit(max_peer_workers)
            .with_peer_session_budget(peer_session_budget)
            .with_allow_ipv6(allow_ipv6)
            .with_peer_filter(peer_filter)
            .with_pex(pex_enabled, pex_max_peers);
        if !tracker_host_rules.is_empty() {
            engine = engine.with_tracker_host_rules(tracker_host_rules);
        }
        if let Some(selection) = intake_selection {
            engine = engine.with_intake_selection(selection);
        }
        if metadata_only {
            engine = engine.with_metadata_only();
        }
        {
            let runtime = self.clone();
            let cancellation = storage_work_cancellation.clone();
            engine = engine.with_storage_recheck_executor(Arc::new(move |storage| {
                let runtime = runtime.clone();
                let cancellation = cancellation.clone();
                Box::pin(async move {
                    runtime
                        .recheck_storage_under_root_control(&storage, Some(&cancellation))
                        .await
                })
            }));
        }
        if needs_metadata {
            let runtime = self.clone();
            if metadata_only {
                // A preview resolves and persists its file tree before the
                // engine reports metadata success. It intentionally bypasses
                // payload-root admission: this engine never opens storage,
                // announces a payload session, or requests pieces.
                engine = engine.with_metadata_preflight(Arc::new(move |resolved| {
                    let runtime = runtime.clone();
                    Box::pin(async move {
                        runtime
                            .commit_metadata_preview_resolution(hash, resolved)
                            .await
                    })
                }));
            } else {
                let metadata_complete_dir = snapshot.complete_dir.clone();
                let metadata_active_dir = snapshot.active_dir.clone();
                let cancellation = storage_work_cancellation.clone();
                engine = engine.with_metadata_preflight(Arc::new(move |resolved| {
                    let runtime = runtime.clone();
                    let metadata_complete_dir = metadata_complete_dir.clone();
                    let metadata_active_dir = metadata_active_dir.clone();
                    let cancellation = cancellation.clone();
                    Box::pin(async move {
                        runtime
                            .reserve_resolved_magnet_metadata(
                                hash,
                                resolved,
                                metadata_complete_dir,
                                metadata_active_dir,
                                cancellation,
                            )
                            .await
                    })
                }));
            }
        }
        if let Some(dht) = dht_runner {
            engine = engine.with_dht(dht);
        }
        // Do not let the engine run until its handle and related bookkeeping
        // are visible. Otherwise a fast failure can remove an empty slot and
        // leave its completed JoinHandle inserted as stale state.
        let (task_start_tx, task_start_rx) = tokio::sync::oneshot::channel();
        let containment_gate = self.containment_gate.clone();
        let containment_generation = containment_gate.generation();
        let handle = tokio::spawn(async move {
            if task_start_rx.await.is_err() {
                runtime_for_task.engine_task_finished(hash_for_task).await;
                return;
            }
            let engine_result = tokio::select! {
                biased;
                _ = containment_gate.cancelled_since(containment_generation) => {
                    runtime_for_task
                        .record_engine_containment_cancellation(hash_for_task, needs_metadata)
                        .await;
                    runtime_for_task.engine_task_finished(hash_for_task).await;
                    return;
                }
                result = engine.run() => result,
            };
            match engine_result {
                Ok(final_state) => {
                    let finished = final_state.finished;
                    let stopped_by_command = final_state.stopped_by_command;
                    let terminal_tracker_error = final_state.terminal_tracker_error();
                    let mut metadata_received = false;
                    let mut changed_state = None;
                    // The metadata-only callback commits the resolved file
                    // tree before this engine can return success. Do not
                    // apply it again here: this event is intentionally the
                    // first public visibility of that durable preview.
                    let metadata_preview_committed =
                        metadata_only && final_state.resolved_meta.is_some();
                    if metadata_preview_committed {
                        metadata_received = true;
                        changed_state = Some(TorrentState::Paused);
                    } else {
                        let mut reg = registry.lock().await;
                        if let Some(t) = reg.get_mut(&hash_for_task) {
                            let previous_state = t.state;
                            let needed_metadata = t.needs_metadata;
                            // If metadata was fetched via BEP 9, replace the
                            // placeholder meta with the real one and rebuild the
                            // file/piece bookkeeping.
                            if let Some(real) = final_state.resolved_meta.as_ref() {
                                apply_resolved_metadata(t, real, &final_state);
                                metadata_received = needed_metadata && !t.needs_metadata;
                            }
                            t.downloaded = final_state.downloaded;
                            t.uploaded = final_state.uploaded;
                            t.progress.replace_from_bitfield(
                                &final_state.pieces_have,
                                final_state.piece_count,
                            );
                            t.recompute_file_bytes_completed();
                            if final_state.finished {
                                t.state = TorrentState::Completed;
                                t.seeding_status = if t.progress.is_complete() {
                                    SeedingStatus::Queued
                                } else {
                                    SeedingStatus::NotEligible
                                };
                                t.date_completed = Some(now());
                            } else if let Some(error) = terminal_tracker_error.as_ref() {
                                t.state = TorrentState::TrackerError;
                                t.seeding_status = SeedingStatus::NotEligible;
                                t.error = Some(error.clone());
                            } else if metadata_only && metadata_received {
                                // Metadata-first previews finish their
                                // contained BEP 9 retrieval without entering
                                // any payload path. An explicit start/resume
                                // clears the durable gate and launches a
                                // normal engine later.
                                t.state = TorrentState::Paused;
                                t.seeding_status = SeedingStatus::NotEligible;
                                t.error = None;
                            } else if t.state == TorrentState::DownloadingMetadata {
                                // Metadata fetched but download incomplete; mark
                                // downloading.
                                t.state = TorrentState::Downloading;
                            }
                            if t.state != previous_state {
                                changed_state = Some(t.state);
                            }
                        }
                    }
                    if metadata_received {
                        runtime_for_task.publish_event(torrent_metadata_event(hash_for_task));
                    }
                    if let Some(state) = changed_state {
                        let event = if state == TorrentState::TrackerError {
                            "torrent_error"
                        } else {
                            "torrent_changed"
                        };
                        runtime_for_task.publish_torrent_event(event, hash_for_task, state);
                        if state == TorrentState::Completed {
                            runtime_for_task.publish_torrent_event(
                                "torrent_completed",
                                hash_for_task,
                                state,
                            );
                        }
                    }
                    runtime_for_task.publish_event(stats_updated_event());
                    // Selfish completion policy: when enabled, immediately
                    // remove the finished torrent from the daemon (engine and
                    // seeder stopped, record removed) while preserving the
                    // downloaded data. This must run after the registry update
                    // above so final stats/name are captured before removal.
                    if finished && selfish_completion_enabled.load(Ordering::Acquire) {
                        runtime_for_task
                            .selfish_remove_completed(hash_for_task)
                            .await;
                    } else if !metadata_only
                        && !finished
                        && !stopped_by_command
                        && terminal_tracker_error.is_none()
                    {
                        let queued = runtime_for_task
                            .queue_torrent_for_retry(
                                hash_for_task,
                                "engine stopped before completion; queued for retry",
                                ENGINE_INCOMPLETE_RETRY_DELAY,
                            )
                            .await;
                        if queued {
                            runtime_for_task.schedule_delayed_reconcile_queue(
                                "engine_incomplete_retry",
                                Duration::ZERO,
                            );
                            runtime_for_task.schedule_delayed_reconcile_queue(
                                "engine_incomplete_retry",
                                ENGINE_INCOMPLETE_RETRY_DELAY,
                            );
                        }
                    }
                }
                Err(e) => {
                    if storage_work_cancellation_for_task.is_cancelled() {
                        tracing::debug!(
                            info_hash = %hash_for_task,
                            "engine storage work cancelled by lifecycle operation"
                        );
                    } else {
                        let retry_metadata = runtime_for_task
                            .handle_engine_task_error(hash_for_task, needs_metadata, e)
                            .await;
                        if retry_metadata {
                            runtime_for_task.schedule_delayed_reconcile_queue(
                                "magnet_metadata_no_peers",
                                MAGNET_METADATA_NO_PEERS_RETRY_DELAY,
                            );
                        }
                    }
                }
            }
            runtime_for_task.engine_task_finished(hash_for_task).await;
            runtime_for_task.reconcile_seeders().await;
            runtime_for_task
                .schedule_delayed_reconcile_queue("engine_task_finished", Duration::ZERO);
            let _ = state_for_summary;
        });
        self.engine_handles.write().await.insert(hash, handle);

        if !self.registry.lock().await.contains(&hash) {
            self.force_stop_engine(&hash).await;
            return;
        }

        let should_run = self
            .registry
            .lock()
            .await
            .get(&hash)
            .is_some_and(|torrent| {
                matches!(
                    torrent.state,
                    TorrentState::Queued
                        | TorrentState::Downloading
                        | TorrentState::DownloadingMetadata
                )
            });
        if !should_run {
            self.force_stop_engine(&hash).await;
            return;
        }

        // Mark the torrent as downloading.
        let mut changed_state = None;
        {
            let mut reg = self.registry.lock().await;
            if let Some(t) = reg.get_mut(&hash) {
                if t.state == TorrentState::Queued || t.state == TorrentState::NetworkBlocked {
                    t.containment_recovery_intent = None;
                    t.state = if needs_metadata {
                        TorrentState::DownloadingMetadata
                    } else {
                        TorrentState::Downloading
                    };
                    t.error = None;
                    changed_state = Some(t.state);
                }
            }
        }
        if let Some(state) = changed_state {
            self.publish_torrent_event("torrent_changed", hash, state);
            self.publish_event(stats_updated_event());
        }
        let _ = task_start_tx.send(());
    }

    async fn cancel_engine_storage_work(&self, hash: &TorrentKey) {
        if let Some(cancellation) = self
            .engine_storage_cancellations
            .lock()
            .await
            .get(hash)
            .cloned()
        {
            cancellation.cancel();
        }
    }

    pub(super) async fn stop_engine(&self, hash: &TorrentKey) {
        self.engine_retry_after.write().await.remove(hash);
        self.cancel_engine_storage_work(hash).await;
        let explicit_recheck = self.cancel_explicit_recheck(hash).await;
        if let Some(tx) = self.engine_cmds.lock().await.remove(hash) {
            let _ = tx.send(EngineCommand::Stop).await;
        }
        let handle = self.engine_handles.write().await.remove(hash);
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        if let Some(recheck) = explicit_recheck {
            recheck.wait_finished().await;
        }
        // Stop the inbound peer listener / seeder too.
        self.stop_seeder(hash).await;
        self.engine_states.write().await.remove(hash);
        self.rate_samples.write().await.remove(hash);
        self.engine_storage_cancellations.lock().await.remove(hash);
        self.storage_admissions.release(hash).await;
    }

    pub(super) async fn force_stop_engine(&self, hash: &TorrentKey) {
        self.engine_retry_after.write().await.remove(hash);
        self.cancel_engine_storage_work(hash).await;
        let explicit_recheck = self.cancel_explicit_recheck(hash).await;
        if let Some(tx) = self.engine_cmds.lock().await.remove(hash) {
            let _ = tx.try_send(EngineCommand::Stop);
        }
        let handle = self.engine_handles.write().await.remove(hash);
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(recheck) = explicit_recheck {
            recheck.wait_finished().await;
        }
        self.force_stop_seeder(hash).await;
        self.engine_states.write().await.remove(hash);
        self.rate_samples.write().await.remove(hash);
        self.engine_storage_cancellations.lock().await.remove(hash);
        self.storage_admissions.release(hash).await;
    }

    pub(super) async fn restart_engine_for_settings(&self, hash: &TorrentKey) {
        self.stop_engine(hash).await;
        {
            let mut registry = self.registry.lock().await;
            if let Some(torrent) = registry.get_mut(hash) {
                torrent.state = TorrentState::Queued;
                torrent.error = None;
            } else {
                return;
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(*hash);
            queue.start_now(hash);
        }
        self.reconcile_queue().await;
    }

    pub(super) async fn stop_all_torrent_tasks(&self, registry_hashes: &[TorrentKey]) {
        let mut hashes = registry_hashes.to_vec();
        hashes.extend(self.engine_handles.read().await.keys().copied());
        hashes.extend(self.seeder_shutdowns.lock().await.keys().copied());
        hashes.extend(self.explicit_rechecks.lock().await.keys().copied());
        hashes.sort();
        hashes.dedup();
        for hash in hashes {
            self.force_stop_engine(&hash).await;
        }
    }

    pub(super) async fn clear_download_runtime_state(&self) {
        {
            let mut reg = self.registry.lock().await;
            reg.torrents.clear();
        }
        {
            let mut queue = self.queue.lock().await;
            queue.clear();
        }
        self.engine_states.write().await.clear();
        self.engine_cmds.lock().await.clear();
        self.engine_handles.write().await.clear();
        self.engine_storage_cancellations.lock().await.clear();
        self.explicit_rechecks.lock().await.clear();
        self.torrent_limiters.write().await.clear();
        self.torrent_peer_permit_pools.write().await.clear();
        self.seeder_shutdowns.lock().await.clear();
        self.seeder_registry.clear().await;
        self.stop_seeder_listener(false).await;
        self.seeder_handles.lock().await.clear();
        self.rate_samples.write().await.clear();
        self.engine_retry_after.write().await.clear();
        self.autopilot_decisions.write().await.clear();
        self.autopilot_last_action.write().await.clear();
        self.storage_admissions.clear().await;
    }
}
