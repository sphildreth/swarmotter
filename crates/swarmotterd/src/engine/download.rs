// SPDX-License-Identifier: Apache-2.0

use super::*;

impl TorrentEngine {
    pub async fn run(mut self) -> Result<EngineState> {
        // If this is a magnet (no real metadata yet), fetch the `info` dict
        // from a peer via BEP 9 before downloading. The real info hash,
        // name, and trackers come from the magnet parameters.
        if let Some(magnet) = self.magnet.clone() {
            self.state.lock().await.tracker_message = Some("fetching metadata via BEP 9".into());
            let info = self.fetch_magnet_metadata(&magnet).await?;
            let rebuilt =
                crate::metadata::build_meta_from_info(&info, &magnet.name, &magnet.trackers)?;
            if let Some(preflight) = &self.metadata_preflight {
                preflight(rebuilt.clone()).await?;
            }
            // Stash the real metadata so the daemon can update the record.
            self.state.lock().await.resolved_meta = Some(rebuilt.clone());
            // Replace the placeholder meta with the real one.
            self.meta = rebuilt;
        }
        if self.file_priorities.len() != self.meta.files.len()
            || self.wanted.len() != self.meta.files.len()
        {
            self.file_priorities = vec![FilePriority::Normal; self.meta.files.len()];
            self.wanted = vec![true; self.meta.files.len()];
        }
        self.piece_selection =
            PieceSelection::from_files(&self.meta, &self.file_priorities, &self.wanted)?;

        let piece_count = self.meta.piece_count();
        let total_length = self.meta.total_length;
        // Initialize state.
        {
            let mut s = self.state.lock().await;
            s.piece_count = piece_count;
            s.total_length = total_length;
        }

        // Containment check: do not start any torrent traffic if the path is
        // unavailable.
        if !self.binder.traffic_allowed() {
            let mut s = self.state.lock().await;
            s.tracker_message = Some("torrent data plane blocked by containment".into());
            return Ok(s.clone());
        }

        self.storage_preflight()?;

        let complete_storage = StorageIo::new(self.meta.clone(), self.complete_dir.clone());
        if self.download_dir != self.complete_dir {
            let complete_have = self.load_or_recheck(&complete_storage).await?;
            if self.piece_selection.complete(&complete_have) {
                self.update_progress(&complete_have).await;
                self.finish_selection(&complete_storage, &complete_have)
                    .await?;
                return Ok(self.state.lock().await.clone());
            }
        }

        let storage = StorageIo::new(self.meta.clone(), self.download_dir.clone());
        let selected_files = self
            .file_priorities
            .iter()
            .zip(&self.wanted)
            .map(|(priority, wanted)| *wanted && *priority != FilePriority::Unwanted)
            .collect::<Vec<_>>();
        if self.preallocate || !self.sparse {
            storage.preallocate_files(&selected_files).await?;
        } else {
            storage
                .ensure_active_layout_for_files(&selected_files)
                .await?;
        }

        // Load fast resume if present; otherwise recheck what's already on disk.
        let mut have = self.load_or_recheck(&storage).await?;
        self.update_progress(&have).await;

        if self.piece_selection.complete(&have) {
            self.finish_selection(&storage, &have).await?;
            return Ok(self.state.lock().await.clone());
        }

        // Discover peers via tracker announce (HTTP/UDP) on each tier.
        let mut discovered = self.announce(AnnounceEvent::Started).await;
        // Merge any directly-supplied seed peers (local swarm / PEX / DHT).
        for p in &self.seed_peers {
            if !discovered.contains(p) {
                discovered.push(*p);
            }
        }
        let dht_peers = self.discover_dht_peers().await;
        merge_unique_peers(&mut discovered, dht_peers);
        dedupe_peers(&mut discovered);
        self.state.lock().await.peers = discovered.clone();

        // Download loop: connect to peers, request missing pieces, write and
        // verify. Bounded by the configured per-torrent worker limit.
        let mut bad_peers: HashMap<SocketAddr, Instant> = HashMap::new();
        let mut peer_backoff: HashMap<SocketAddr, Instant> = HashMap::new();
        let mut last_discovery_refresh = Instant::now();
        let mut candidate_cursor: usize = 0;
        // Bounded consecutive no-peer rounds: if we never discover any peers
        // after a bounded number of announce attempts, give up gracefully
        // rather than looping forever. This handles trackerless torrents with
        // no seed peers and no DHT result without hanging the engine.
        const NO_PEER_ROUNDS_MAX: u32 = 5;
        let mut no_peer_rounds: u32 = 0;

        loop {
            // Handle pending commands.
            match self.poll_commands().await {
                CommandOutcome::Stop => {
                    self.state.lock().await.stopped_by_command = true;
                    break;
                }
                CommandOutcome::Reannounce => {
                    let refreshed = self.refresh_discovery_peers(true).await;
                    merge_unique_peers(&mut discovered, refreshed);
                    dedupe_peers(&mut discovered);
                    self.state.lock().await.peers = discovered.clone();
                    last_discovery_refresh = Instant::now();
                }
                CommandOutcome::RelaxPeerBackoff => {
                    peer_backoff.clear();
                    candidate_cursor = 0;
                    self.state.lock().await.peer_scheduler.backed_off_peers = 0;
                }
                CommandOutcome::Continue | CommandOutcome::Pause => {}
            }
            let max_concurrent = self.current_peer_worker_limit();
            self.sync_have_from_state(&mut have, piece_count).await;

            if self.piece_selection.complete(&have) {
                self.finish_selection(&storage, &have).await?;
                // Announce completion to trackers.
                self.announce(AnnounceEvent::Completed).await;
                break;
            }

            // Periodically re-announce to refresh peers.
            if last_discovery_refresh.elapsed() > PEER_REFRESH_INTERVAL {
                let refreshed = self.refresh_discovery_peers(false).await;
                merge_unique_peers(&mut discovered, refreshed);
                dedupe_peers(&mut discovered);
                self.state.lock().await.peers = discovered.clone();
                last_discovery_refresh = Instant::now();
            }

            let mut made_progress = self.run_webseed_round(&storage, &mut have).await;
            if self.piece_selection.complete(&have) {
                continue;
            }

            let remaining = self.piece_selection.remaining(&have);
            prune_peer_backoff(&mut bad_peers);
            prune_peer_backoff(&mut peer_backoff);
            let (mut eligible, candidate_counts) =
                classify_peer_candidates(&discovered, &bad_peers, &peer_backoff, self.allow_ipv6);
            balance_peer_families(&mut eligible);
            let mut scheduler = PeerSchedulerDiagnostics {
                discovered_peers: candidate_counts.discovered,
                eligible_peers: candidate_counts.eligible,
                filtered_peers: candidate_counts.filtered,
                failed_peers: candidate_counts.failed,
                backed_off_peers: candidate_counts.backed_off,
                peer_worker_limit: max_concurrent,
                parallel_candidates: eligible.len().min(max_concurrent),
                last_reason: peer_scheduler_reason(&candidate_counts),
                ..Default::default()
            };
            self.record_peer_scheduler(scheduler.clone()).await;
            if !discovered.is_empty() && eligible.is_empty() {
                tracing::debug!(
                    info_hash = %self.meta.info_hash,
                    discovered_peers = candidate_counts.discovered,
                    filtered_peers = candidate_counts.filtered,
                    failed_peers = candidate_counts.failed,
                    backed_off_peers = candidate_counts.backed_off,
                    "no eligible peer candidates after scheduler filtering"
                );
            } else if eligible.len() == 1 {
                tracing::debug!(
                    info_hash = %self.meta.info_hash,
                    discovered_peers = discovered.len(),
                    "single eligible peer candidate; serial fallback likely"
                );
            }

            // Endgame mode: when few pieces remain, request the remaining
            // blocks from multiple peers concurrently and cancel duplicates
            // as they complete. Falls back to the normal sequential path when
            // endgame is inactive or there are too few usable peers.
            if swarmotter_core::endgame::is_endgame(remaining) {
                let candidates =
                    rotated_peer_candidates(&eligible, &mut candidate_cursor, max_concurrent);
                if !candidates.is_empty() {
                    let progressed = self
                        .run_endgame(&candidates, &storage, &mut have, &mut bad_peers)
                        .await;
                    if progressed || self.piece_selection.complete(&have) {
                        continue;
                    }
                }
            }

            let candidates =
                rotated_peer_candidates(&eligible, &mut candidate_cursor, eligible.len());
            scheduler.parallel_candidates = candidates.len();
            scheduler.last_reason = peer_scheduler_reason(&candidate_counts);
            self.record_peer_scheduler(scheduler).await;

            if candidates.len() > 1 {
                let (progressed, pex_peers) = self
                    .run_parallel_peer_round(
                        &candidates,
                        max_concurrent,
                        &storage,
                        &mut have,
                        &mut bad_peers,
                        &mut peer_backoff,
                    )
                    .await;
                made_progress = progressed;
                for peer in pex_peers {
                    if self.peer_allowed(&peer) && !discovered.contains(&peer) {
                        discovered.push(peer);
                    }
                }
                dedupe_peers(&mut discovered);
                self.state.lock().await.peers = discovered.clone();
            }

            // Single-peer fallback and diagnostic path. This also preserves
            // the PEX behavior where the only known peer can advertise more
            // peers during the session.
            let mut to_try = if made_progress {
                Vec::new()
            } else {
                candidates
            };

            if !to_try.is_empty() {
                self.set_peer_scheduler_serial_active(true).await;
            }
            while let Some(peer_addr) = to_try.pop() {
                if self.piece_selection.complete(&have) {
                    break;
                }
                match self
                    .download_from_peer(&peer_addr, &storage, &mut have, &mut discovered)
                    .await
                {
                    Ok((progressed, session_reason)) => {
                        if progressed {
                            made_progress = true;
                        } else {
                            tracing::debug!(
                                peer = %peer_addr.socket_addr(),
                                reason = session_reason,
                                "serial peer session produced no progress; backing off"
                            );
                            backoff_peer(&mut peer_backoff, peer_addr.socket_addr());
                        }
                    }
                    Err(e) => {
                        tracing::debug!(peer = %peer_addr.socket_addr(), error = %e, "peer failed; suppressing");
                        backoff_failed_peer(&mut bad_peers, peer_addr.socket_addr());
                    }
                }
            }
            self.set_peer_scheduler_serial_active(false).await;

            if !made_progress {
                let (_, latest_counts) = classify_peer_candidates(
                    &discovered,
                    &bad_peers,
                    &peer_backoff,
                    self.allow_ipv6,
                );
                if no_usable_peer_candidates(&latest_counts) {
                    // No usable peers; back off briefly and retry announce.
                    self.sleep_or_stop(Duration::from_secs(2)).await;
                    let refreshed = self.refresh_discovery_peers(false).await;
                    merge_unique_peers(&mut discovered, refreshed);
                    dedupe_peers(&mut discovered);
                    self.state.lock().await.peers = discovered.clone();
                    let (_, refreshed_counts) = classify_peer_candidates(
                        &discovered,
                        &bad_peers,
                        &peer_backoff,
                        self.allow_ipv6,
                    );
                    if no_usable_peer_candidates(&refreshed_counts) {
                        no_peer_rounds = no_peer_rounds.saturating_add(1);
                        let mut state = self.state.lock().await;
                        let existing = state.tracker_message.clone();
                        let reason = peer_scheduler_reason(&refreshed_counts)
                            .unwrap_or_else(|| "no usable peer candidates".into());
                        if !existing.as_deref().unwrap_or_default().starts_with("no ") {
                            state.tracker_message = Some(match existing {
                                Some(msg) => format!("{reason}; last announce: {msg}"),
                                None => reason,
                            });
                        }
                        drop(state);
                        // Bounded give-up: a torrent that never has usable peers
                        // (no peers, or only peers filtered/failed out) cannot
                        // progress. Stop the engine so the daemon/test does not
                        // hang; the torrent remains incomplete and the user can
                        // add trackers or seed peers and re-start it.
                        if no_peer_rounds >= NO_PEER_ROUNDS_MAX {
                            let tracker_message = self.state.lock().await.tracker_message.clone();
                            tracing::info!(
                                info_hash = %self.meta.info_hash,
                                tracker_message = ?tracker_message,
                                "stopping engine: no usable peers after bounded retries"
                            );
                            break;
                        }
                    } else {
                        no_peer_rounds = 0;
                    }
                } else {
                    self.sleep_or_stop(Duration::from_millis(500)).await;
                }
            }
        }

        Ok(self.state.lock().await.clone())
    }
}

impl TorrentEngine {
    pub(super) fn storage_preflight(&self) -> Result<()> {
        if self.minimum_free_space_bytes == 0 && self.minimum_free_space_percent == 0 {
            return Ok(());
        }
        let mut paths = vec![self.download_dir.clone()];
        if self.complete_dir != self.download_dir {
            paths.push(self.complete_dir.clone());
        }
        for path in paths {
            swarmotter_core::storage::check_storage_preflight(
                &path,
                &swarmotter_core::config::StorageConfig {
                    minimum_free_space_bytes: self.minimum_free_space_bytes,
                    minimum_free_space_percent: self.minimum_free_space_percent,
                    ..Default::default()
                },
                self.meta.total_length,
            )?;
        }
        Ok(())
    }
}

impl TorrentEngine {
    /// Pick a piece we don't have that the peer has.
    pub(super) fn pick_piece(
        &self,
        peer_bf: Option<&Bitfield>,
        have: &PieceBitfield,
    ) -> Option<usize> {
        let peer_bf = peer_bf?;
        (0..self.meta.piece_count())
            .filter(|&i| self.piece_selection.includes(i) && peer_bf.has(i) && !have.has(i))
            .max_by_key(|&i| self.piece_selection.priority(i))
    }
    pub(super) fn piece_length(&self, index: usize) -> u64 {
        if index + 1 == self.meta.piece_count() {
            self.meta.last_piece_length()
        } else {
            self.meta.piece_length
        }
    }
}

impl TorrentEngine {
    pub(super) async fn update_progress(&self, have: &PieceBitfield) {
        update_progress_state(&self.state, &self.meta, have).await;
    }

    pub(super) async fn mark_finished(&self) {
        let mut s = self.state.lock().await;
        s.finished = true;
    }

    pub(super) async fn load_or_recheck(&self, storage: &StorageIo) -> Result<PieceBitfield> {
        if let Some(resume) = storage.load_resume(&self.meta.info_hash).await? {
            let payload_bytes = storage.payload_bytes_on_disk().await?;
            let current_stamps = storage.resume_file_stamps().await?;
            let stamps_match = !resume.file_stamps.is_empty()
                && resume.file_stamps.len() == current_stamps.len()
                && resume.file_stamps == current_stamps;
            let sparse_bytes_mismatch =
                self.sparse && !self.preallocate && payload_bytes != resume.bytes_completed;
            if sparse_bytes_mismatch || !stamps_match {
                tracing::info!(
                    info_hash = %self.meta.info_hash,
                    payload_bytes,
                    resume_bytes_completed = resume.bytes_completed,
                    stamps_match,
                    "fast resume does not match on-disk payload; rechecking storage"
                );
                storage.recheck().await
            } else {
                Ok(resume.piece_bitfield)
            }
        } else {
            storage.recheck().await
        }
    }

    pub(super) async fn complete_storage(&self, storage: &StorageIo) -> Result<StorageIo> {
        if self.download_dir == self.complete_dir {
            return Ok(storage.clone());
        }
        tracing::info!(
            info_hash = %self.meta.info_hash,
            active_dir = %self.download_dir.display(),
            complete_dir = %self.complete_dir.display(),
            "moving completed torrent data to download directory"
        );
        storage.move_to(self.complete_dir.clone()).await
    }

    pub(super) async fn finish_without_resume(&self, storage: &StorageIo) -> Result<()> {
        self.mark_finished().await;
        storage.remove_resume().await?;
        if self.download_dir != self.complete_dir {
            let active_storage = StorageIo::new(self.meta.clone(), self.download_dir.clone());
            active_storage.remove_resume().await?;
        }
        Ok(())
    }

    pub(super) async fn finish_selection(
        &self,
        storage: &StorageIo,
        have: &PieceBitfield,
    ) -> Result<()> {
        if have.count(self.meta.piece_count()) == self.meta.piece_count() {
            let final_storage = if storage.base_dir() == self.complete_dir.as_path() {
                storage.clone()
            } else {
                self.complete_storage(storage).await?
            };
            self.finish_without_resume(&final_storage).await
        } else {
            // A selected-file download is complete without claiming pieces
            // that were intentionally skipped. Keep its resume metadata and
            // active-root data so changing the selection can continue later.
            self.mark_finished().await;
            self.persist_resume(storage, have).await
        }
    }

    pub(super) async fn persist_resume(
        &self,
        storage: &StorageIo,
        have: &PieceBitfield,
    ) -> Result<()> {
        let piece_byte_lengths: Vec<u64> = (0..self.meta.piece_count())
            .map(|i| self.piece_length(i))
            .collect();
        let s = self.state.lock().await;
        let mut resume = swarmotter_core::storage::io::build_resume_with_wanted(
            self.meta.info_hash,
            self.meta.name.clone(),
            have.clone(),
            self.meta.piece_count(),
            s.downloaded,
            s.uploaded,
            s.total_length,
            Some(storage.base_dir().display().to_string()),
            now_secs(),
            if s.finished { Some(now_secs()) } else { None },
            &self.file_priorities,
            &self.wanted,
            &piece_byte_lengths,
        );
        drop(s);
        resume.file_stamps = storage.resume_file_stamps().await?;
        storage.save_resume(&resume).await?;
        Ok(())
    }

    pub(super) async fn poll_commands(&self) -> CommandOutcome {
        let mut rx = self.commands.lock().await;
        match rx.try_recv() {
            Ok(EngineCommand::Stop) => CommandOutcome::Stop,
            Ok(EngineCommand::Pause) => CommandOutcome::Pause,
            Ok(EngineCommand::Resume) => CommandOutcome::Continue,
            Ok(EngineCommand::Reannounce) => CommandOutcome::Reannounce,
            Ok(EngineCommand::Recheck) => CommandOutcome::Continue,
            Ok(EngineCommand::RelaxPeerBackoff) => CommandOutcome::RelaxPeerBackoff,
            Ok(EngineCommand::UpdatePeerWorkerLimit(limit)) => {
                self.set_peer_worker_limit(limit);
                CommandOutcome::Continue
            }
            Err(_) => CommandOutcome::Continue,
        }
    }

    pub(super) async fn sleep_or_stop(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}
