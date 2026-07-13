// SPDX-License-Identifier: Apache-2.0

use super::*;

impl DaemonRuntime {
    /// Build the base contained binder without a TCP proxy layer. This is
    /// reserved for narrow local-router control operations (NAT-PMP/UPnP),
    /// which must use the selected interface directly and cannot be expressed
    /// through SOCKS5 CONNECT. It still has the exact same containment gate,
    /// interface/source binding, and fail-closed behavior as the data plane.
    pub(super) async fn make_unproxied_contained_binder(
        &self,
    ) -> Arc<dyn swarmotter_core::net::NetworkBinder> {
        let cfg = self.config.read().await.clone();
        Arc::new(
            ContainedBinder::new(cfg.network.clone(), self.interface_probe.clone())
                .with_gate_and_health(self.containment_gate.clone(), self.health_report_tx.clone()),
        )
    }

    pub(super) async fn make_binder(&self) -> Arc<dyn swarmotter_core::net::NetworkBinder> {
        let cfg = self.config.read().await.clone();
        let contained: Arc<dyn swarmotter_core::net::NetworkBinder> = Arc::new(
            ContainedBinder::new(cfg.network.clone(), self.interface_probe.clone())
                .with_gate_and_health(self.containment_gate.clone(), self.health_report_tx.clone()),
        );
        if cfg.network.socks5.enabled {
            Arc::new(swarmotter_core::net::Socks5Binder::new(
                contained,
                cfg.network.socks5.clone(),
            ))
        } else {
            contained
        }
    }

    /// Revalidate the concrete source/interface/listener bind operations before
    /// an explicit configuration replacement is allowed to clear a latched
    /// bind failure. This binder is intentionally not attached to the blocked
    /// live gate; it opens only ephemeral validation sockets and immediately
    /// drops them.
    pub(super) async fn validate_replacement_bind_path(&self, config: &Config) -> Result<()> {
        if config.network.mode == NetworkContainmentMode::Disabled {
            return Ok(());
        }
        let binder = ContainedBinder::new(config.network.clone(), self.interface_probe.clone());
        // SOCKS5 CONNECT has no UDP transport. Avoid even creating an
        // otherwise-contained UDP validation socket in that mode so a repair
        // check cannot be mistaken for an enabled direct UDP path.
        if !config.network.socks5.enabled {
            let udp = binder.udp_socket().await.map_err(|error| {
                CoreError::NetworkBlocked(format!(
                    "replacement containment UDP bind validation failed: {error}"
                ))
            })?;
            drop(udp);
        }
        let listener = binder
            .bind_peer_listener(config.torrent.listen_port)
            .await
            .map_err(|error| {
                CoreError::NetworkBlocked(format!(
                    "replacement containment listener bind validation failed: {error}"
                ))
            })?;
        drop(listener);
        Ok(())
    }

    /// Periodically re-evaluate network containment health and flip torrent
    /// states between active and `network_blocked` as the path appears or
    /// disappears. Stop running engines when the path becomes unavailable.
    pub async fn network_health_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.network_health_tick().await;
        }
    }

    /// One iteration of the network containment health monitor, extracted so
    /// tests can drive it deterministically without sleeping. It evaluates the
    /// injected interface probe, processes pending bind-failure health reports,
    /// and on a healthy-to-unhealthy transition follows the exact order required
    /// by ADR-0051: block the gate, stop the listener/DHT, abort data-plane tasks,
    /// reconcile progress, set torrents `network_blocked`, persist, and publish.
    pub async fn network_health_tick(&self) {
        // Binder failures already blocked the gate synchronously. Drain their
        // reports to drive centralized teardown and latch the operational
        // failure so a healthy interface probe cannot silently reopen traffic.
        let reported = {
            let mut rx = self.health_report_rx.lock().await;
            let mut latest = None;
            while let Ok(report) = rx.try_recv() {
                latest = Some(report);
            }
            latest
        };
        if let Some(report) = reported {
            if matches!(
                report.status,
                NetworkContainmentStatus::SocketBindFailed
                    | NetworkContainmentStatus::BlockedFailClosed
            ) {
                *self.bind_failure_latched.write().await = Some(report.clone());
            }
            self.transition_data_plane_to_blocked(report.status, report.detail)
                .await;
            return;
        }

        if let Some(report) = self.bind_failure_latched.read().await.clone() {
            // Recovery is deliberately explicit: only a successfully validated
            // full configuration replacement clears this latch.
            if self.containment_gate.traffic_allowed() {
                self.containment_gate
                    .block(report.status, report.detail.clone());
            }
            let mut health = self.network_health.write().await;
            health.status = report.status;
            health.detail = report.detail;
            health.traffic_allowed = false;
            return;
        }

        let cfg = self.config.read().await.clone();
        let health = net::evaluate(&cfg.network, self.interface_probe.as_ref());
        let previous = self.network_health.read().await.clone();

        if !health.traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
            if previous.traffic_allowed
                || previous.status != health.status
                || previous.detail != health.detail
                || self.containment_gate.traffic_allowed()
            {
                self.transition_data_plane_to_blocked(health.status, health.detail)
                    .await;
            }
            return;
        }

        if health.traffic_allowed && !previous.traffic_allowed {
            self.recover_containment_work(health).await;
            return;
        }

        let network_changed = previous.status != health.status
            || previous.traffic_allowed != health.traffic_allowed
            || previous.detail != health.detail;
        *self.network_health.write().await = health.clone();
        if health.traffic_allowed {
            self.containment_gate.allow();
        }
        self.reconcile_engine_progress().await;
        self.reconcile_queue().await;
        if network_changed {
            self.publish_event(Event::new(
                "network_status_changed",
                json!({
                    "status": health.status.as_str(),
                    "traffic_allowed": health.traffic_allowed,
                    "detail": health.detail,
                }),
            ));
            self.publish_event(stats_updated_event());
        }
    }

    /// Copy live engine state (pieces, byte counts) into the torrent records
    /// so API/UI summaries reflect real progress while downloading.
    pub(super) async fn reconcile_engine_progress(&self) {
        let live_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>()
        };
        self.reconcile_engine_progress_with_seeders(live_seeders, true)
            .await;
    }

    /// Snapshot task-owned counters while a data-plane reconstruction holds
    /// the transition lock. This deliberately skips seeder/task
    /// reconciliation, which could otherwise try to start work recursively
    /// under that same lock.
    pub(super) async fn reconcile_engine_progress_for_transition(&self) {
        let live_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>()
        };
        self.reconcile_engine_progress_with_seeders(live_seeders, false)
            .await;
    }

    pub(super) async fn reconcile_engine_progress_with_seeders(
        &self,
        live_seeders: HashSet<InfoHash>,
        finish_lifecycle_reconciliation: bool,
    ) {
        let states = self.engine_states.read().await.clone();
        let running_engines: HashSet<InfoHash> =
            self.engine_handles.read().await.keys().copied().collect();
        let now = Instant::now();
        let retry_after = self.engine_retry_after.read().await.clone();
        let previous_samples = self.rate_samples.read().await.clone();
        let global_download_limit = self.config.read().await.bandwidth.effective_download();
        let network_health = self.network_health.read().await.clone();
        let mut snapshots = Vec::with_capacity(states.len());
        for (hash, state) in states {
            let state = state.lock().await.clone();
            let engine_is_running = running_engines.contains(&hash);
            let retry_suppressed = retry_after
                .get(&hash)
                .is_some_and(|retry_at| *retry_at > now);
            snapshots.push((hash, state, engine_is_running, retry_suppressed));
        }

        let mut sample_updates = Vec::new();
        let mut events = Vec::new();
        let mut reg = self.registry.lock().await;
        let calc = HealthCalculator::new();
        for (hash, s, engine_is_running, retry_suppressed) in &snapshots {
            if let Some(t) = reg.get_mut(hash) {
                let previous_state = t.state;
                let needed_metadata = t.needs_metadata;
                if let Some(real) = s.resolved_meta.as_ref() {
                    apply_resolved_metadata(t, real, s);
                    if needed_metadata && !t.needs_metadata {
                        events.push(torrent_metadata_event(*hash));
                    }
                }
                let mut peak = previous_samples
                    .get(hash)
                    .map(|p| p.peak_rate_down)
                    .unwrap_or(0);
                if let Some(prev) = previous_samples.get(hash).copied() {
                    let elapsed = now.duration_since(prev.at);
                    if elapsed >= Duration::from_millis(250) {
                        let secs = elapsed.as_secs_f64();
                        let down_delta = s.downloaded.saturating_sub(prev.downloaded);
                        let up_delta = s.uploaded.saturating_sub(prev.uploaded);
                        let inst_down = ((down_delta as f64) / secs) as u64;
                        let inst_up = ((up_delta as f64) / secs) as u64;
                        let (last_download_at, no_download_since) = if down_delta > 0 {
                            (Some(now), None)
                        } else {
                            (
                                prev.last_download_at,
                                Some(prev.no_download_since.unwrap_or(prev.at)),
                            )
                        };
                        let last_upload_at = if up_delta > 0 {
                            Some(now)
                        } else {
                            prev.last_upload_at
                        };
                        t.rate_down = smooth_rate(prev.rate_down, inst_down, last_download_at, now);
                        t.rate_up = smooth_rate(prev.rate_up, inst_up, last_upload_at, now);
                        let previous_peak_down = prev.peak_rate_down;
                        let previous_peak_up = prev.peak_rate_up;
                        let observed_down = t.rate_down.max(inst_down);
                        let observed_up = t.rate_up.max(inst_up);
                        peak = previous_peak_down.max(observed_down);
                        let peak_rate_up = previous_peak_up.max(observed_up);
                        if peak > previous_peak_down || peak_rate_up > previous_peak_up {
                            log_torrent_throughput_peak(
                                hash,
                                t,
                                s,
                                inst_down,
                                inst_up,
                                previous_peak_down,
                                previous_peak_up,
                                peak,
                                peak_rate_up,
                                now,
                            );
                        }
                        sample_updates.push((
                            *hash,
                            RateSample {
                                downloaded: s.downloaded,
                                uploaded: s.uploaded,
                                rate_down: t.rate_down,
                                rate_up: t.rate_up,
                                last_download_at,
                                last_upload_at,
                                no_download_since,
                                at: now,
                                peak_rate_down: peak,
                                peak_rate_up,
                            },
                        ));
                    }
                } else {
                    sample_updates.push((
                        *hash,
                        RateSample {
                            downloaded: s.downloaded,
                            uploaded: s.uploaded,
                            rate_down: t.rate_down,
                            rate_up: t.rate_up,
                            last_download_at: None,
                            last_upload_at: None,
                            no_download_since: Some(now),
                            at: now,
                            peak_rate_down: 0,
                            peak_rate_up: 0,
                        },
                    ));
                }
                t.progress
                    .replace_from_bitfield(&s.pieces_have, s.piece_count);
                t.recompute_file_bytes_completed();
                t.downloaded = s.downloaded;
                t.uploaded = s.uploaded;
                t.active_peer_workers = s.active_peers;
                t.known_peers = s.peers.len();
                if !t.state.is_error() && t.state != TorrentState::Paused {
                    if s.finished {
                        if !t.progress.is_complete() {
                            t.state = TorrentState::Completed;
                            t.seeding_status = SeedingStatus::NotEligible;
                        } else if live_seeders.contains(hash) {
                            t.state = TorrentState::Seeding;
                            t.seeding_status = SeedingStatus::Active;
                        } else {
                            t.state = TorrentState::Completed;
                            t.seeding_status = SeedingStatus::Queued;
                        }
                    } else if *engine_is_running && !*retry_suppressed {
                        t.seeding_status = SeedingStatus::NotEligible;
                        if t.needs_metadata {
                            t.state = TorrentState::DownloadingMetadata;
                        } else if t.state == TorrentState::Queued
                            || t.state == TorrentState::DownloadingMetadata
                        {
                            t.state = TorrentState::Downloading;
                        }
                    }
                }

                // Compute per-torrent health from real engine state. Health
                // is exposed on every summary, so the Web UI can render a
                // signal-bars indicator without an extra round-trip.
                let health_input = build_health_input(
                    t,
                    s.piece_count,
                    &s.pieces_have,
                    &s.peer_health,
                    &s.tracker_ok,
                    s.dht_discovery_ok,
                    s.pex_discovery_ok,
                    s.tracker_failures_recent,
                    s.peer_disconnects_recent,
                    s.hash_failures,
                    s.timeout_failures,
                    s.last_valid_block,
                    s.block_last_seen,
                    s.webseed_last_seen,
                    s.dht_last_seen,
                    s.pex_last_seen,
                    s.tracker_last_ok,
                    s.peers.len(),
                    s.tracker_message.as_deref(),
                    peak,
                    global_download_limit,
                    network_health.clone(),
                );
                t.health = calc.compute(&health_input);
                if t.state != previous_state {
                    events.push(torrent_event("torrent_changed", *hash, t.state));
                    if t.state == TorrentState::Completed {
                        events.push(torrent_event("torrent_completed", *hash, t.state));
                    }
                }
            }
        }
        drop(reg);
        for event in events {
            self.publish_event(event);
        }
        if !snapshots.is_empty() {
            self.publish_event(stats_updated_event());
        }
        if !sample_updates.is_empty() {
            let mut samples = self.rate_samples.write().await;
            for (hash, sample) in sample_updates {
                samples.insert(hash, sample);
            }
        }
        if finish_lifecycle_reconciliation {
            self.sweep_selfish_completed_torrents_best_effort("engine_progress")
                .await;
            self.reconcile_seeders().await;
            if !snapshots.is_empty() {
                self.persist_state_best_effort("engine_progress").await;
            }
        }
    }

    /// Periodically compute autopilot decisions from contained runtime
    /// telemetry. In `act` mode this applies only bounded daemon/engine
    /// commands that use existing contained data-plane paths.
    pub async fn autopilot_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(AUTOPILOT_INTERVAL).await;
            self.reconcile_queue().await;
            self.refresh_autopilot_decisions(true).await;
        }
    }

    pub(super) async fn refresh_autopilot_decisions(&self, apply_actions: bool) {
        self.reconcile_engine_progress().await;

        let cfg = self.config.read().await.clone();
        let global_mode = cfg.autopilot.mode;
        let network = self.network_health.read().await.clone();
        let states = self.engine_states.read().await.clone();
        let samples = self.rate_samples.read().await.clone();
        let torrents: Vec<Torrent> = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        let analyzer = AutopilotAnalyzer::new();
        let mut decisions = HashMap::new();
        let now = Instant::now();

        for torrent in torrents {
            let hash = torrent.info_hash();
            let state = match states.get(&hash) {
                Some(state) => Some(state.lock().await.clone()),
                None => None,
            };
            let input = build_autopilot_input(
                &torrent,
                state.as_ref(),
                samples.get(&hash).copied(),
                now,
                &network,
            );
            let mode = effective_autopilot_mode(global_mode, torrent.autopilot_mode_override);
            let decision = analyzer.analyze(&input, mode);
            if apply_actions && mode == AutopilotMode::Act {
                self.apply_autopilot_decision(hash, &decision, &cfg).await;
            }
            decisions.insert(hash, decision);
        }

        *self.autopilot_decisions.write().await = decisions;
    }

    pub(super) async fn apply_autopilot_decision(
        &self,
        hash: InfoHash,
        decision: &AutopilotDecision,
        cfg: &Config,
    ) {
        if !decision.apply {
            return;
        }
        let Some(action) = decision.action.as_ref() else {
            return;
        };
        let now = Instant::now();
        if self
            .autopilot_last_action
            .read()
            .await
            .get(&hash)
            .is_some_and(|at| now.saturating_duration_since(*at) < AUTOPILOT_ACTION_COOLDOWN)
        {
            return;
        }

        let applied = match action.kind {
            AutopilotActionKind::IncreasePeerWorkers => {
                self.apply_autopilot_peer_worker_limit(hash, decision, cfg)
                    .await
            }
            AutopilotActionKind::ExpandDiscovery => {
                self.send_engine_command(hash, EngineCommand::Reannounce)
                    .await
            }
            AutopilotActionKind::RelaxPeerBackoff => {
                self.send_engine_command(hash, EngineCommand::RelaxPeerBackoff)
                    .await
            }
            AutopilotActionKind::ReleaseQueueSlot => self.apply_autopilot_queue_release(hash).await,
            AutopilotActionKind::RaiseDownloadCeiling => {
                self.apply_autopilot_download_ceiling(hash, action.suggested_download_limit)
                    .await
            }
        };

        if applied {
            self.autopilot_last_action.write().await.insert(hash, now);
            tracing::info!(
                info_hash = %hash,
                action_kind = ?action.kind,
                rationale = %action.rationale,
                causes = ?decision.snapshot.causes,
                "autopilot applied action"
            );
        }
    }

    pub(super) async fn send_engine_command(&self, hash: InfoHash, command: EngineCommand) -> bool {
        let tx = self.engine_cmds.lock().await.get(&hash).cloned();
        let Some(tx) = tx else {
            return false;
        };
        tx.send(command).await.is_ok()
    }

    pub(super) async fn apply_autopilot_peer_worker_limit(
        &self,
        hash: InfoHash,
        decision: &AutopilotDecision,
        cfg: &Config,
    ) -> bool {
        let current = decision.snapshot.peer_worker_limit.max(1);
        let hard_limit =
            Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent);
        let next = current.saturating_add(1).min(hard_limit).max(1);
        if next <= current {
            tracing::debug!(
                info_hash = %hash,
                current_peer_worker_limit = current,
                hard_peer_worker_limit = hard_limit,
                "autopilot peer worker increase skipped by configured hard cap"
            );
            return false;
        }
        self.send_engine_command(hash, EngineCommand::UpdatePeerWorkerLimit(next))
            .await
    }

    pub(super) async fn apply_autopilot_queue_release(&self, hash: InfoHash) -> bool {
        if !self.engine_handles.read().await.contains_key(&hash) {
            return false;
        }
        if self
            .desired_download_hashes_excluding(Some(hash))
            .await
            .is_empty()
        {
            tracing::debug!(
                info_hash = %hash,
                "autopilot queue-slot release skipped because no queued replacement is currently eligible"
            );
            return false;
        }
        self.force_stop_engine(&hash).await;
        {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(&hash) else {
                return false;
            };
            if matches!(
                t.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            ) {
                t.state = TorrentState::Queued;
                t.error = Some("autopilot released active queue slot after no progress".into());
            }
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
            .insert(hash, Instant::now() + AUTOPILOT_QUEUE_RELEASE_RETRY_DELAY);
        self.schedule_reconcile_queue("autopilot_queue_release")
            .await;
        true
    }

    pub(super) async fn apply_autopilot_download_ceiling(
        &self,
        hash: InfoHash,
        suggested_download_limit: Option<u64>,
    ) -> bool {
        let Some(download_limit) = suggested_download_limit else {
            tracing::debug!(
                info_hash = %hash,
                "autopilot download ceiling change skipped without a bounded suggestion"
            );
            return false;
        };
        let mut reg = self.registry.lock().await;
        let Some(t) = reg.get_mut(&hash) else {
            return false;
        };
        if t.download_limit == 0 || download_limit <= t.download_limit {
            return false;
        }
        t.download_limit = download_limit;
        drop(reg);
        if let Some(rl) = self.torrent_limiters.read().await.get(&hash).cloned() {
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Download,
                download_limit,
            );
        }
        true
    }
}
