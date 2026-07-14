// SPDX-License-Identifier: Apache-2.0

use super::*;

impl DaemonRuntime {
    pub(super) async fn start_seeder(
        &self,
        hash: TorrentKey,
        meta: swarmotter_core::meta::TorrentMeta,
        active_dir: String,
        complete_dir: String,
        state: Arc<Mutex<EngineState>>,
    ) -> Result<()> {
        let _data_plane_transition = self.data_plane_transition_lock.lock().await;
        self.start_seeder_while_transition_locked(hash, meta, active_dir, complete_dir, state)
            .await
    }

    pub(super) async fn start_seeder_while_transition_locked(
        &self,
        hash: TorrentKey,
        meta: swarmotter_core::meta::TorrentMeta,
        active_dir: String,
        complete_dir: String,
        state: Arc<Mutex<EngineState>>,
    ) -> Result<()> {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        {
            let mut shutdowns = self.seeder_shutdowns.lock().await;
            if shutdowns.contains_key(&hash) {
                if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                    torrent.state = TorrentState::Seeding;
                    torrent.seeding_status = SeedingStatus::Active;
                }
                return Ok(());
            }
            shutdowns.insert(hash, shutdown_tx.clone());
        }
        let peer_id = make_peer_id();
        let config = self.config.read().await.clone();
        let listen_port = config.torrent.listen_port;
        let encryption_mode = self
            .registry
            .lock()
            .await
            .get(&hash)
            .map(|torrent| {
                Self::effective_policy_with_config(&config, torrent)
                    .encryption_mode
                    .value
            })
            .unwrap_or(config.torrent.encryption_mode);
        // Reuse the torrent's retained limiter; never replace it when the
        // downloader completes or a queued seed slot becomes available.
        let (dl_limit, ul_limit) = {
            let reg = self.registry.lock().await;
            reg.get(&hash)
                .map(|t| (t.download_limit, t.upload_limit))
                .unwrap_or((0, 0))
        };
        let limiter = {
            let mut limiters = self.torrent_limiters.write().await;
            limiters
                .entry(hash)
                .or_insert_with(|| {
                    Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                        dl_limit, ul_limit,
                    ))
                })
                .clone()
        };
        let storage = Arc::new(storage_io_with_config(
            meta.clone(),
            std::path::PathBuf::from(&active_dir),
            &config,
        ));
        let complete_storage = if active_dir == complete_dir {
            None
        } else {
            Some(Arc::new(storage_io_with_config(
                meta.clone(),
                std::path::PathBuf::from(&complete_dir),
                &config,
            )))
        };
        let registration = SeedRegistration::new(
            meta.clone(),
            storage,
            complete_storage,
            state.clone(),
            peer_id,
            limiter,
            Some(self.global_limiter.clone()),
            self.peer_session_budget(hash).await,
            shutdown_rx,
        )
        .with_key(hash)
        .with_encryption_mode(encryption_mode);
        self.seeder_registry.register(registration).await?;
        if let Err(error) = self.ensure_seeder_listener().await {
            if let Some(shutdown) = self.seeder_shutdowns.lock().await.remove(&hash) {
                let _ = shutdown.send(true);
            }
            self.seeder_registry.unregister(&hash).await;
            return Err(error);
        }
        let announce_handle = self
            .spawn_seeder_announce(
                hash,
                meta.clone(),
                peer_id,
                listen_port,
                state,
                shutdown_tx.subscribe(),
            )
            .await;
        if let Some(handle) = announce_handle {
            self.seeder_handles.lock().await.insert(hash, handle);
        }
        if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
            torrent.state = TorrentState::Seeding;
            torrent.seeding_status = SeedingStatus::Active;
            torrent.error = None;
        }
        self.persist_state_best_effort("seeder_started").await;
        Ok(())
    }

    pub(super) async fn ensure_seeder_listener(&self) -> Result<()> {
        let mut handle_slot = self.seeder_listener_handle.lock().await;
        if handle_slot
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Ok(());
        }
        if let Some(finished) = handle_slot.take() {
            let _ = finished.await;
        }
        let cfg = self.config.read().await.clone();
        let peer_filter = self.peer_filter.read().await.clone();
        let binder = self.make_binder().await;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let hub = SeederHub::new(
            self.seeder_registry.clone(),
            binder,
            cfg.torrent.listen_port,
            cfg.torrent.encryption_mode,
            shutdown_rx,
            self.peer_permit_pool.read().await.clone(),
        )
        .with_peer_filter(peer_filter)
        .with_bound_addr(bound_tx);
        *self.seeder_listener_shutdown.lock().await = Some(shutdown_tx);
        let containment_gate = self.containment_gate.clone();
        let containment_generation = containment_gate.generation();
        *handle_slot = Some(tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = containment_gate.cancelled_since(containment_generation) => {}
                result = hub.run() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "shared seeding listener ended");
                    }
                }
            }
        }));
        match tokio::time::timeout(Duration::from_secs(5), bound_rx).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => {
                drop(handle_slot);
                self.stop_seeder_listener(true).await;
                Err(CoreError::NetworkBlocked(
                    "shared inbound peer listener failed to bind".into(),
                ))
            }
            Err(_) => {
                drop(handle_slot);
                self.stop_seeder_listener(true).await;
                Err(CoreError::NetworkBlocked(
                    "shared inbound peer listener bind timed out".into(),
                ))
            }
        }
    }

    pub(super) async fn stop_seeder_listener(&self, force: bool) {
        if let Some(shutdown) = self.seeder_listener_shutdown.lock().await.take() {
            let _ = shutdown.send(true);
        }
        if let Some(handle) = self.seeder_listener_handle.lock().await.take() {
            if force {
                handle.abort();
            }
            let _ = handle.await;
        }
    }

    /// Stop the shared DHT runner if one is active. Used by containment
    /// transitions and data-plane reconstruction. See ADR-0051.
    pub(super) async fn stop_dht_runner(&self) {
        // The runner is a shared resource with no long-running task of its own;
        // dropping the stored Arc stops it.
        *self.dht_runner.lock().await = None;
    }

    /// Snapshot only work that is demonstrably live at the containment edge.
    /// Queued, paused, completed-without-a-live-seeder, automatically stopped,
    /// and pre-existing blocked torrents receive no recovery intent.
    pub(super) async fn live_containment_recovery_intents(
        &self,
    ) -> HashMap<TorrentKey, ContainmentRecoveryIntent> {
        let running_engines = self
            .engine_handles
            .read()
            .await
            .iter()
            .filter_map(|(hash, handle)| (!handle.is_finished()).then_some(*hash))
            .collect::<HashSet<_>>();
        let cfg = self.config.read().await.clone();
        let samples = self.rate_samples.read().await.clone();
        let now_secs = now();
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let running_seeders = self
            .seeder_registry
            .keys()
            .await
            .into_iter()
            .collect::<HashSet<_>>();
        let reg = self.registry.lock().await;
        let mut intents = HashMap::new();
        for (hash, torrent) in &reg.torrents {
            if let Some(intent) = torrent.containment_recovery_intent {
                intents.insert(*hash, intent);
                continue;
            }
            let engine_was_live = running_engines.contains(hash);
            if engine_was_live {
                intents.insert(
                    *hash,
                    if torrent.needs_metadata || torrent.state == TorrentState::DownloadingMetadata
                    {
                        ContainmentRecoveryIntent::DownloadingMetadata
                    } else {
                        ContainmentRecoveryIntent::Downloading
                    },
                );
                continue;
            }

            if !running_seeders.contains(hash)
                || !matches!(
                    torrent.state,
                    TorrentState::Completed | TorrentState::Seeding
                )
            {
                continue;
            }
            let idle_seconds = samples
                .get(hash)
                .and_then(|sample| sample.last_upload_at)
                .map(|at| Instant::now().saturating_duration_since(at).as_secs())
                .unwrap_or_else(|| {
                    now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added))
                });
            let accounting = TorrentAccounting {
                downloaded: torrent.downloaded,
                uploaded: torrent.uploaded,
                idle_seconds,
            };
            let (global, per_torrent) = Self::effective_ratio_policy(&cfg, torrent);
            if ratio::evaluate_seeding(&accounting, &global, &per_torrent) == SeedDecision::Continue
            {
                intents.insert(*hash, ContainmentRecoveryIntent::Seeding);
            }
        }
        intents
    }

    /// Abort every task that can own a torrent data-plane socket. All handles
    /// are aborted before any are awaited, so teardown never waits for graceful
    /// peer/tracker/TLS protocol completion. Engine state is retained until the
    /// caller reconciles already-reported progress.
    pub(super) async fn abort_data_plane_tasks_for_containment(
        &self,
        recovery_intents: &HashMap<TorrentKey, ContainmentRecoveryIntent>,
        preserved_seeding_statuses: &HashMap<TorrentKey, SeedingStatus>,
        detail: &str,
    ) -> Vec<TorrentKey> {
        self.engine_cmds.lock().await.clear();
        let engine_handles = {
            let mut handles = self.engine_handles.write().await;
            std::mem::take(&mut *handles)
                .into_values()
                .collect::<Vec<_>>()
        };
        let announce_handles = {
            let mut handles = self.seeder_handles.lock().await;
            std::mem::take(&mut *handles)
                .into_values()
                .collect::<Vec<_>>()
        };

        let changed = {
            // Readers, live registration ownership, listener teardown, final
            // progress reconciliation, and the modeled blocked state share one
            // lifecycle critical section. No API snapshot can observe a live
            // `seeding` state after its accepting task has stopped, or an
            // `active` status without an authoritative registration.
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live_seeders = self
                .seeder_registry
                .keys()
                .await
                .into_iter()
                .collect::<HashSet<_>>();
            self.stop_seeder_listener(true).await;
            for handle in &engine_handles {
                handle.abort();
            }
            for handle in &announce_handles {
                handle.abort();
            }
            let shutdowns = {
                let mut shutdowns = self.seeder_shutdowns.lock().await;
                std::mem::take(&mut *shutdowns)
                    .into_values()
                    .collect::<Vec<_>>()
            };
            for shutdown in shutdowns {
                let _ = shutdown.send(true);
            }
            self.seeder_registry.clear().await;

            // Keep the pre-teardown registration snapshot while copying final
            // task-owned counters so progress reconciliation does not publish a
            // fictitious completed/queued transition between active and
            // network-blocked. Seeder reconciliation is deliberately skipped
            // while this lifecycle lock is held.
            self.reconcile_engine_progress_with_seeders(live_seeders, false)
                .await;

            let mut changed = Vec::new();
            let mut reg = self.registry.lock().await;
            for (hash, intent) in recovery_intents {
                let Some(torrent) = reg.get_mut(hash) else {
                    continue;
                };
                torrent.containment_recovery_intent = Some(*intent);
                torrent.state = TorrentState::NetworkBlocked;
                if let Some(status) = preserved_seeding_statuses.get(hash) {
                    torrent.seeding_status = *status;
                }
                torrent.error = Some(detail.to_owned());
                changed.push(*hash);
            }
            for (hash, torrent) in &mut reg.torrents {
                if torrent.containment_recovery_intent.is_none()
                    && matches!(
                        torrent.state,
                        TorrentState::Downloading
                            | TorrentState::DownloadingMetadata
                            | TorrentState::Seeding
                    )
                {
                    // A modeled active state without a live owning task is not
                    // evidence of recoverable activity. Block the stale state
                    // for truthful API output, but deliberately grant no
                    // automatic resume intent.
                    torrent.state = TorrentState::NetworkBlocked;
                    torrent.error = Some(detail.to_owned());
                    changed.push(*hash);
                }
            }
            changed
        };

        for handle in engine_handles {
            let _ = handle.await;
        }
        for handle in announce_handles {
            let _ = handle.await;
        }
        self.engine_retry_after.write().await.clear();
        changed
    }

    pub(super) async fn transition_data_plane_to_blocked(
        &self,
        status: NetworkContainmentStatus,
        detail: String,
    ) {
        let _transition = self.data_plane_transition_lock.lock().await;

        // The ordering here is the ADR-0051 contract. Never move a socket-owning
        // shutdown ahead of the gate block or a state mutation ahead of progress
        // reconciliation.
        self.containment_gate.block(status, detail.clone());
        // Mapping traffic shares the live containment gate. Wake its loop so
        // an active lease is marked blocked immediately without attempting a
        // router delete over an unavailable path.
        self.notify_port_mapping_reconcile();
        let recovery_intents = self.live_containment_recovery_intents().await;
        let preserved_seeding_statuses = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let registry = self.registry.lock().await;
            recovery_intents
                .iter()
                .filter_map(|(hash, intent)| {
                    if *intent != ContainmentRecoveryIntent::Seeding {
                        return None;
                    }
                    registry
                        .get(hash)
                        .map(|torrent| (*hash, torrent.seeding_status))
                })
                .collect::<HashMap<_, _>>()
        };
        self.stop_dht_runner().await;
        let changed = self
            .abort_data_plane_tasks_for_containment(
                &recovery_intents,
                &preserved_seeding_statuses,
                &detail,
            )
            .await;
        // Progress is now durable in Torrent records; drop all stale task-owned
        // objects so recovery reconstructs them under the new gate generation.
        self.engine_states.write().await.clear();
        // Retain torrent limiters across fail-closed teardown so recovered
        // downloaders/seeders keep the same live policy object.

        {
            let mut health = self.network_health.write().await;
            health.status = status;
            health.detail = detail.clone();
            health.traffic_allowed = false;
        }
        self.persist_state_best_effort("network_blocked").await;
        for hash in changed {
            self.publish_torrent_event("torrent_changed", hash, TorrentState::NetworkBlocked);
        }
        self.publish_event(Event::new(
            "network_status_changed",
            json!({
                "status": status.as_str(),
                "traffic_allowed": false,
                "detail": detail,
            }),
        ));
        self.publish_event(stats_updated_event());
    }

    pub(super) async fn recover_containment_work(&self, health: NetworkHealth) {
        let _transition = self.data_plane_transition_lock.lock().await;
        *self.network_health.write().await = health.clone();
        self.containment_gate.allow();
        self.notify_port_mapping_reconcile();

        let mut changed = Vec::new();
        let mut downloads = Vec::new();
        let mut seeders = Vec::new();
        {
            let mut reg = self.registry.lock().await;
            for (hash, torrent) in &mut reg.torrents {
                let Some(intent) = torrent.containment_recovery_intent.take() else {
                    continue;
                };
                torrent.error = None;
                torrent.state = match intent {
                    ContainmentRecoveryIntent::Downloading
                    | ContainmentRecoveryIntent::DownloadingMetadata => {
                        downloads.push(*hash);
                        torrent.seeding_status = SeedingStatus::NotEligible;
                        TorrentState::Queued
                    }
                    ContainmentRecoveryIntent::Seeding => {
                        seeders.push(*hash);
                        torrent.seeding_status = SeedingStatus::Queued;
                        TorrentState::Completed
                    }
                };
                changed.push((*hash, torrent.state));
            }
        }
        self.persist_state_best_effort("network_recovered").await;
        drop(_transition);

        // Rebuild only from consumed durable intents. Global reconciliation
        // would also auto-start unrelated queued/completed torrents, violating
        // the recovery-set contract.
        for hash in downloads {
            {
                let mut queue = self.queue.lock().await;
                queue.add(hash);
                queue.start_now(&hash);
            }
            self.start_engine(hash).await;
        }
        for hash in seeders {
            if let Err(error) = self.start_recovered_containment_seeder(hash).await {
                tracing::warn!(info_hash = %hash, %error, "failed to reconstruct recovered seeder");
            }
        }
        for (hash, _) in changed {
            let state = {
                let _lifecycle = self.seeder_lifecycle_lock.lock().await;
                self.registry
                    .lock()
                    .await
                    .get(&hash)
                    .map(|torrent| torrent.state)
            };
            if let Some(state) = state {
                self.publish_torrent_event("torrent_changed", hash, state);
            }
        }
        self.publish_event(Event::new(
            "network_status_changed",
            json!({
                "status": health.status.as_str(),
                "traffic_allowed": true,
                "detail": health.detail,
            }),
        ));
        self.publish_event(stats_updated_event());
    }

    pub(super) async fn start_recovered_containment_seeder(&self, hash: TorrentKey) -> Result<()> {
        let Some(start) = self.prepare_recovered_seeder_start(hash).await? else {
            return Ok(());
        };
        self.start_seeder(
            hash,
            start.meta,
            start.active_dir,
            start.complete_dir,
            start.state,
        )
        .await
    }

    pub(super) async fn start_recovered_seeder_while_transition_locked(
        &self,
        hash: TorrentKey,
    ) -> Result<()> {
        let Some(start) = self.prepare_recovered_seeder_start(hash).await? else {
            return Ok(());
        };
        self.start_seeder_while_transition_locked(
            hash,
            start.meta,
            start.active_dir,
            start.complete_dir,
            start.state,
        )
        .await
    }

    pub(super) async fn prepare_recovered_seeder_start(
        &self,
        hash: TorrentKey,
    ) -> Result<Option<RecoveredSeederStart>> {
        let mut torrent = self
            .registry
            .lock()
            .await
            .get(&hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        let config = self.config.read().await.clone();
        let idle_seconds =
            now().saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added));
        let status = automatic_seeding_status(&torrent, &config, idle_seconds);
        if status != SeedingStatus::Queued {
            if let Some(stored) = self.registry.lock().await.get_mut(&hash) {
                stored.state = TorrentState::Completed;
                stored.seeding_status = status;
            }
            self.persist_state_best_effort("containment_recovery_seed_target")
                .await;
            return Ok(None);
        }
        torrent.seeding_status = SeedingStatus::Queued;
        let complete_dir = self.resolve_download_dir(&torrent).await;
        let active_dir = self.resolve_incomplete_dir_for(&torrent).await;
        let state = Arc::new(Mutex::new(EngineState {
            piece_count: torrent
                .meta
                .data_piece_count()
                .unwrap_or_else(|_| torrent.meta.piece_count()),
            total_length: torrent.meta.total_length,
            downloaded: torrent.downloaded,
            uploaded: torrent.uploaded,
            pieces_have: torrent.progress.bitfield().clone(),
            finished: true,
            ..EngineState::default()
        }));
        self.engine_states.write().await.insert(hash, state.clone());
        Ok(Some(RecoveredSeederStart {
            meta: torrent.meta,
            active_dir,
            complete_dir,
            state,
        }))
    }

    /// Spawn an owned sidecar task that periodically announces the seeder to
    /// the torrent's trackers, so the seeder is visible in the swarm. The
    /// returned handle is awaited by the seeder task after signaling shutdown,
    /// so the sidecar cannot outlive the seeder lifecycle.
    pub(super) async fn spawn_seeder_announce(
        &self,
        hash: TorrentKey,
        meta: swarmotter_core::meta::TorrentMeta,
        peer_id: [u8; 20],
        listen_port: u16,
        state: Arc<Mutex<EngineState>>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Option<JoinHandle<()>> {
        let tracker_tiers = tracker::announce_tiers(meta.announce.as_deref(), &meta.announce_list);
        if tracker_tiers.is_empty() {
            return None;
        }
        let binder = self.make_binder().await;
        let containment_gate = self.containment_gate.clone();
        let containment_generation = containment_gate.generation();
        Some(tokio::spawn(async move {
            let announce_loop = async move {
                if *shutdown_rx.borrow() {
                    return;
                }
                // Initial announce: started event so trackers see the seeder
                // immediately rather than waiting for the first interval tick.
                let mut announce_after = tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            return;
                        }
                        Duration::from_secs(300)
                    }
                    interval = Self::seeder_announce_once(
                        &tracker_tiers,
                        hash,
                        peer_id,
                        listen_port,
                        binder.clone(),
                        state.clone(),
                        AnnounceEvent::Started,
                    ) => Duration::from_secs(interval)
                };
                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                // Best-effort stopped announce, bounded by the
                                // per-tracker announce timeout.
                                Self::seeder_announce_once(
                                    &tracker_tiers,
                                    hash,
                                    peer_id,
                                    listen_port,
                                    binder.clone(),
                                    state.clone(),
                                    AnnounceEvent::Stopped,
                                )
                                .await;
                                return;
                            }
                        }
                        _ = tokio::time::sleep(announce_after) => {
                            let interval = Self::seeder_announce_once(
                                &tracker_tiers,
                                hash,
                                peer_id,
                                listen_port,
                                binder.clone(),
                                state.clone(),
                                AnnounceEvent::Empty,
                            )
                            .await;
                            announce_after = Duration::from_secs(interval);
                        }
                    }
                }
            };
            tokio::select! {
                biased;
                _ = containment_gate.cancelled_since(containment_generation) => {}
                _ = announce_loop => {}
            }
        }))
    }

    pub(super) async fn stop_seeder(&self, hash: &TorrentKey) {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        if let Some(tx) = self.seeder_shutdowns.lock().await.remove(hash) {
            let _ = tx.send(true);
        }
        self.seeder_registry.unregister(hash).await;
        let handle = self.seeder_handles.lock().await.remove(hash);
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        if self.seeder_registry.is_empty().await {
            self.stop_seeder_listener(false).await;
        }
        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
            if torrent.state == TorrentState::Seeding
                && torrent.seeding_status == SeedingStatus::Active
            {
                torrent.state = TorrentState::Completed;
                torrent.seeding_status = if torrent.progress.is_complete() {
                    SeedingStatus::Queued
                } else {
                    SeedingStatus::NotEligible
                };
            }
        }
    }

    /// One-shot tracker announce for a seeder. Best-effort per tracker; logs
    /// and continues on failure. Times out aggressively so a slow or
    /// unreachable tracker cannot stall the announce loop.
    pub(super) async fn seeder_announce_once(
        tracker_tiers: &[Vec<String>],
        hash: TorrentKey,
        peer_id: [u8; 20],
        port: u16,
        binder: Arc<dyn swarmotter_core::net::NetworkBinder>,
        state: Arc<Mutex<EngineState>>,
        event: AnnounceEvent,
    ) -> u64 {
        let mut interval_seconds = 0u64;
        let scrape_urls = tracker_tiers.iter().flatten().cloned().collect::<Vec<_>>();
        let mut any_success = false;
        'tiers: for tier in tracker_tiers {
            for url in tier {
                let req = AnnounceRequest {
                    tracker_url: url.clone(),
                    info_hash: hash.peer_info_hash(),
                    peer_id,
                    port,
                    uploaded: 0,
                    downloaded: 0,
                    left: 0,
                    event,
                    numwant: Some(0),
                    compact: true,
                };
                let outcome = if url.starts_with("udp://") {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        udp_tracker::udp_announce(binder.as_ref(), &req),
                    )
                    .await
                } else {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        tracker::http_announce(binder.as_ref(), &req),
                    )
                    .await
                };
                let announce_at = now();
                let (succeeded, snapshot) = match outcome {
                    Ok(Ok(response)) if response.failure_reason.is_none() => {
                        let interval = response
                            .interval
                            .max(response.min_interval.unwrap_or(0))
                            .clamp(30, 86_400);
                        interval_seconds = interval;
                        tracing::info!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            "seeder announce ok"
                        );
                        (
                            true,
                            crate::engine::TrackerAnnounceSnapshot {
                                status: TrackerStatus::Ok,
                                seeders: response.seeders,
                                leechers: response.leechers,
                                downloads: 0,
                                last_error: None,
                                last_message: Some("seeder announce ok".into()),
                                last_announce: Some(announce_at),
                            },
                        )
                    }
                    Ok(Ok(response)) => {
                        let error = response
                            .failure_reason
                            .unwrap_or_else(|| "tracker failure".into());
                        tracing::debug!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            error = %error,
                            "seeder announce failed"
                        );
                        (
                            false,
                            crate::engine::TrackerAnnounceSnapshot {
                                status: TrackerStatus::Error,
                                seeders: response.seeders,
                                leechers: response.leechers,
                                downloads: 0,
                                last_error: Some(error),
                                last_message: None,
                                last_announce: Some(announce_at),
                            },
                        )
                    }
                    Ok(Err(e)) => {
                        let error = e.to_string();
                        tracing::debug!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            error = %error,
                            "seeder announce failed"
                        );
                        (
                            false,
                            crate::engine::TrackerAnnounceSnapshot {
                                status: TrackerStatus::Error,
                                seeders: 0,
                                leechers: 0,
                                downloads: 0,
                                last_error: Some(error),
                                last_message: None,
                                last_announce: Some(announce_at),
                            },
                        )
                    }
                    Err(_) => {
                        tracing::debug!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            "seeder announce timed out"
                        );
                        (
                            false,
                            crate::engine::TrackerAnnounceSnapshot {
                                status: TrackerStatus::Error,
                                seeders: 0,
                                leechers: 0,
                                downloads: 0,
                                last_error: Some("seeder announce timed out".into()),
                                last_message: None,
                                last_announce: Some(announce_at),
                            },
                        )
                    }
                };
                {
                    let mut engine = state.lock().await;
                    engine.tracker_announces.insert(url.clone(), snapshot);
                    engine.last_announce = Some(announce_at);
                    if succeeded {
                        engine.tracker_ok = true;
                        engine.tracker_last_ok = Some(Instant::now());
                        engine.tracker_interval_seconds = interval_seconds;
                    } else {
                        engine.tracker_failures_recent =
                            engine.tracker_failures_recent.saturating_add(1);
                    }
                }
                any_success |= succeeded;
                if succeeded {
                    break 'tiers;
                }
            }
        }
        state.lock().await.tracker_ok = any_success;
        if event != AnnounceEvent::Stopped {
            crate::engine::run_tracker_scrapes(state, binder, hash.peer_info_hash(), scrape_urls)
                .await;
        }
        if interval_seconds == 0 {
            300
        } else {
            interval_seconds
        }
    }

    pub(super) async fn force_stop_seeder(&self, hash: &TorrentKey) {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        if let Some(tx) = self.seeder_shutdowns.lock().await.remove(hash) {
            let _ = tx.send(true);
        }
        self.seeder_registry.unregister(hash).await;
        let handle = self.seeder_handles.lock().await.remove(hash);
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
        if self.seeder_registry.is_empty().await {
            self.stop_seeder_listener(true).await;
        }
        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
            if torrent.state == TorrentState::Seeding
                && torrent.seeding_status == SeedingStatus::Active
            {
                torrent.state = TorrentState::Completed;
                torrent.seeding_status = if torrent.progress.is_complete() {
                    SeedingStatus::Queued
                } else {
                    SeedingStatus::NotEligible
                };
            }
        }
    }

    pub(super) async fn deactivate_seeders_after_listener_failure(
        &self,
        hashes: &[TorrentKey],
        error: &CoreError,
    ) {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        for hash in hashes {
            if let Some(shutdown) = self.seeder_shutdowns.lock().await.remove(hash) {
                let _ = shutdown.send(true);
            }
            self.seeder_registry.unregister(hash).await;
            if let Some(handle) = self.seeder_handles.lock().await.remove(hash) {
                handle.abort();
                let _ = handle.await;
            }
        }
        let mut registry = self.registry.lock().await;
        for hash in hashes {
            if let Some(torrent) = registry.get_mut(hash) {
                if torrent.progress.is_complete()
                    && torrent.state != TorrentState::Paused
                    && torrent.state != TorrentState::NetworkBlocked
                {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = SeedingStatus::Queued;
                    torrent.error = Some(error.to_string());
                }
            }
        }
    }

    pub(super) async fn reconcile_seeders(&self) {
        let lifecycle_before = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.registry
                .lock()
                .await
                .torrents
                .iter()
                .map(|(hash, torrent)| (*hash, (torrent.state, torrent.seeding_status)))
                .collect::<HashMap<_, _>>()
        };
        let now_secs = now();
        let cfg = self.config.read().await.clone();
        let seeding_limit = cfg.queue.max_active_seeds;
        let samples = self.rate_samples.read().await.clone();
        let mut running_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry.keys().await
        };
        if !running_seeders.is_empty() {
            if let Err(error) = self.ensure_seeder_listener().await {
                tracing::warn!(%error, "shared inbound seeding listener unavailable");
                self.deactivate_seeders_after_listener_failure(&running_seeders, &error)
                    .await;
                running_seeders.clear();
            }
        }

        let completed: Vec<(TorrentKey, Torrent)> = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let reg = self.registry.lock().await;
            reg.torrents
                .iter()
                .filter(|(_, torrent)| {
                    torrent.progress.is_complete()
                        && matches!(
                            torrent.state,
                            TorrentState::Completed | TorrentState::Seeding
                        )
                })
                .map(|(key, torrent)| (*key, torrent.clone()))
                .collect()
        };

        let mut allowed = Vec::new();
        let mut desired_status = HashMap::new();
        for (hash, torrent) in &completed {
            let idle_seconds = samples
                .get(hash)
                .and_then(|sample| sample.last_upload_at)
                .map(|at| Instant::now().saturating_duration_since(at).as_secs())
                .unwrap_or_else(|| {
                    now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added))
                });
            let status = automatic_seeding_status(torrent, &cfg, idle_seconds);
            desired_status.insert(*hash, status);
            if status == SeedingStatus::Queued
                && (seeding_limit == 0 || allowed.len() < seeding_limit)
            {
                allowed.push(*hash);
            }
        }

        for hash in &running_seeders {
            if !allowed.contains(hash) {
                self.stop_seeder(hash).await;
                if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                    if torrent.state != TorrentState::NetworkBlocked
                        && torrent.state != TorrentState::Paused
                    {
                        torrent.state = TorrentState::Completed;
                        torrent.seeding_status = desired_status
                            .get(hash)
                            .copied()
                            .unwrap_or(SeedingStatus::NotEligible);
                    }
                }
            }
        }

        {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live = self.seeder_registry.keys().await;
            let mut reg = self.registry.lock().await;
            for (hash, torrent) in &mut reg.torrents {
                if live.contains(hash) {
                    torrent.state = TorrentState::Seeding;
                    torrent.seeding_status = SeedingStatus::Active;
                } else if let Some(status) = desired_status.get(hash).copied() {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = status;
                } else if torrent.state != TorrentState::NetworkBlocked
                    && torrent.state != TorrentState::Paused
                    && !torrent.progress.is_complete()
                {
                    torrent.seeding_status = SeedingStatus::NotEligible;
                } else if torrent.state == TorrentState::Seeding
                    || torrent.seeding_status == SeedingStatus::Active
                {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = SeedingStatus::Queued;
                }
            }
        }

        for hash in allowed {
            if self.seeder_registry.contains(&hash).await {
                continue;
            }
            let Some(torrent_for_dir) = completed
                .iter()
                .find(|(key, _)| *key == hash)
                .map(|(_, torrent)| torrent.clone())
            else {
                continue;
            };
            let complete_dir = self.resolve_download_dir(&torrent_for_dir).await;
            let active_dir = self.resolve_incomplete_dir_for(&torrent_for_dir).await;
            let existing_state = self.engine_states.read().await.get(&hash).cloned();
            let state = if let Some(state) = existing_state {
                state
            } else {
                let pieces_have = torrent_for_dir.progress.bitfield().clone();
                let state = Arc::new(Mutex::new(EngineState {
                    piece_count: torrent_for_dir
                        .meta
                        .data_piece_count()
                        .unwrap_or_else(|_| torrent_for_dir.meta.piece_count()),
                    total_length: torrent_for_dir.meta.total_length,
                    downloaded: torrent_for_dir.downloaded,
                    uploaded: torrent_for_dir.uploaded,
                    pieces_have,
                    finished: true,
                    ..EngineState::default()
                }));
                self.engine_states.write().await.insert(hash, state.clone());
                state
            };
            if let Err(error) = self
                .start_seeder(hash, torrent_for_dir.meta, active_dir, complete_dir, state)
                .await
            {
                tracing::warn!(info_hash = %hash, %error, "inbound seeding listener unavailable");
                if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = SeedingStatus::Queued;
                    torrent.error = Some(error.to_string());
                }
            }
        }
        let lifecycle_changes = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.registry
                .lock()
                .await
                .torrents
                .iter()
                .filter_map(|(hash, torrent)| {
                    (lifecycle_before.get(hash) != Some(&(torrent.state, torrent.seeding_status)))
                        .then_some((*hash, torrent.state))
                })
                .collect::<Vec<_>>()
        };
        self.persist_state_best_effort("seeder_reconcile").await;
        for (hash, state) in &lifecycle_changes {
            self.publish_torrent_event("torrent_changed", *hash, *state);
        }
        if !lifecycle_changes.is_empty() {
            self.publish_event(stats_updated_event());
        }
    }

    pub(super) async fn sweep_selfish_completed_torrents_best_effort(&self, reason: &'static str) {
        if let Err(e) = self.sweep_selfish_completed_torrents(reason).await {
            tracing::warn!(
                reason,
                error = %e,
                "selfish completed torrent sweep failed"
            );
        }
    }

    pub(super) async fn sweep_selfish_completed_torrents(
        &self,
        reason: &'static str,
    ) -> Result<Vec<TorrentKey>> {
        if !self.config.read().await.torrent.selfish {
            return Ok(Vec::new());
        }

        let hashes: Vec<TorrentKey> = {
            let reg = self.registry.lock().await;
            reg.torrents
                .iter()
                .filter_map(|(hash, torrent)| {
                    matches!(
                        torrent.state,
                        TorrentState::Completed | TorrentState::Seeding
                    )
                    .then_some(*hash)
                })
                .collect()
        };

        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let removed = self
            .remove_torrents_with_single_reconcile(hashes, false)
            .await?;
        if !removed.is_empty() {
            tracing::info!(
                count = removed.len(),
                reason,
                selfish = true,
                delete_data = false,
                "selfish mode removed already-completed torrents; downloaded data preserved"
            );
        }
        Ok(removed)
    }

    /// Selfish-mode completion: remove a finished torrent from the daemon
    /// without deleting its downloaded data. Stops the inbound seeder and
    /// clears all live engine/seeder bookkeeping, then removes the torrent
    /// record from the registry. Equivalent to `remove_torrent` with
    /// `delete_data = false`, but safe to call from within the engine task
    /// itself because it does NOT await the engine task's own join handle
    /// (that would deadlock); the already-returning task is simply detached.
    pub(super) async fn selfish_remove_completed(&self, hash: TorrentKey) {
        let name = self
            .registry
            .lock()
            .await
            .get(&hash)
            .map(|t| t.name().to_string())
            .unwrap_or_default();
        // Stop the inbound seeder (a separate task; safe to await).
        self.stop_seeder(&hash).await;
        // Clear live engine bookkeeping. We deliberately do NOT await the
        // engine join handle: it belongs to the engine task that is calling
        // this method, so awaiting it would deadlock. Dropping the detached
        // handle is safe because the task is already returning.
        self.engine_cmds.lock().await.remove(&hash);
        self.engine_states.write().await.remove(&hash);
        self.torrent_limiters.write().await.remove(&hash);
        self.torrent_peer_permit_pools.write().await.remove(&hash);
        let engine_handle = self.engine_handles.write().await.remove(&hash);
        if let Some(handle) = engine_handle {
            drop(handle);
        }
        // Remove the torrent record; downloaded data is preserved (no
        // delete-data behavior is invoked).
        self.registry.lock().await.remove(&hash);
        self.queue.lock().await.remove(&hash);
        tracing::info!(
            info_hash = %hash,
            name = %name,
            selfish = true,
            delete_data = false,
            "selfish mode removed completed torrent; downloaded data preserved"
        );
        self.publish_event(torrent_removed_event(hash, false));
        self.publish_event(stats_updated_event());
        self.persist_state_best_effort("selfish_completion").await;
    }
}
