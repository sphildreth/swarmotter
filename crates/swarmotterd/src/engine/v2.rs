// SPDX-License-Identifier: Apache-2.0

//! Pure BEP 52 payload transfer.
//!
//! This deliberately stays separate from the v1 engine path. A v2 logical
//! piece is file-aligned and verified by a SHA-256 Merkle subtree root, so it
//! must never pass through v1 contiguous-piece storage arithmetic or SHA-1
//! verification. Outbound sockets still use the central `NetworkBinder` via
//! `utp::connect_peer_stream` and therefore retain fail-closed containment.

use super::*;

use swarmotter_core::hash::PeerInfoHash;
use swarmotter_core::peer::{V2Handshake, V2_RESERVED_BIT, V2_RESERVED_BYTE_INDEX};
use swarmotter_core::utp::PeerDuplex;
use swarmotter_core::v2::V2PieceLayout;

impl TorrentEngine {
    /// Run the pure-v2 baseline for complete metainfo with verified piece
    /// layers. Hybrid torrents intentionally remain on the existing v1 path
    /// until dual-format per-piece verification is enabled; this avoids
    /// advertising an upgrade that cannot yet prove both hash formats.
    pub(super) async fn run_v2(&mut self) -> Result<EngineState> {
        let layout = self.meta.v2_piece_layout()?;
        self.prepare_v2_file_selection();

        {
            let mut state = self.state.lock().await;
            state.piece_count = layout.piece_count();
            state.total_length = self.meta.total_length;
            if self.encryption_mode == PeerEncryptionMode::Preferred {
                state.tracker_message = Some(
                    "pure-v2 peer sessions prefer contained MSE/PE with plaintext fallback".into(),
                );
            }
        }

        if !self.binder.traffic_allowed() {
            let mut state = self.state.lock().await;
            state.tracker_message = Some("torrent data plane blocked by containment".into());
            return Ok(state.clone());
        }
        self.storage_preflight()?;

        let complete_storage = StorageIo::new(self.meta.clone(), self.complete_dir.clone())
            .with_torrent_key(self.torrent_key)
            .with_resume_dir(self.resume_dir.clone())
            .with_cow_strategy(self.cow_strategy)
            .with_metrics(self.storage_metrics.clone());
        if self.download_dir != self.complete_dir {
            let complete_have = complete_storage.recheck_v2(&layout).await?;
            if self.v2_selection_complete(&layout, &complete_have) {
                self.update_v2_progress(&layout, &complete_have).await;
                self.finish_v2_selection(&complete_storage, &layout, &complete_have)
                    .await?;
                return Ok(self.state.lock().await.clone());
            }
        }

        let storage = StorageIo::new(self.meta.clone(), self.download_dir.clone())
            .with_torrent_key(self.torrent_key)
            .with_resume_dir(self.resume_dir.clone())
            .with_partial_file_suffix(self.partial_file_suffix.clone())
            .with_cow_strategy(self.cow_strategy)
            .with_metrics(self.storage_metrics.clone())
            .with_write_limiter(self.storage_write_limiter.clone());
        let selected_files = self.v2_selected_files();
        if self.preallocate || !self.sparse {
            storage.preallocate_files(&selected_files).await?;
        } else {
            storage
                .ensure_active_layout_for_files(&selected_files)
                .await?;
        }

        // Fast-resume state is keyed by the full SHA-256 identity and its
        // bitfield is validated against the BEP 52 file-aligned layout before
        // use. A stale/malformed record falls back to the bounded recheck.
        let mut have = self.load_or_recheck_v2(&storage, &layout).await?;
        self.update_v2_progress(&layout, &have).await;
        if self.v2_selection_complete(&layout, &have) {
            self.finish_v2_selection(&storage, &layout, &have).await?;
            return Ok(self.state.lock().await.clone());
        }

        // BEP 52 tracker/DHT requests use the explicit 20-byte truncated
        // SHA-256 wire identity. The full SHA-256 torrent key remains local
        // to storage/registry boundaries and is never replaced by a zeroed
        // v1 placeholder.
        let mut peers = self.announce(AnnounceEvent::Started).await;
        merge_unique_peers(
            &mut peers,
            self.filter_allowed_peers(self.seed_peers.clone()),
        );
        merge_unique_peers(&mut peers, self.discover_dht_peers().await);
        dedupe_peers(&mut peers);
        self.state.lock().await.peers = peers.clone();
        if peers.is_empty() {
            let mut state = self.state.lock().await;
            state.tracker_message = Some("pure-v2 discovery found no eligible peers".into());
            return Ok(state.clone());
        }

        let mut progressed = false;
        for peer in peers {
            match self
                .download_v2_from_peer(&peer, &storage, &layout, &mut have)
                .await
            {
                Ok(made_progress) => progressed |= made_progress,
                Err(error) => {
                    record_peer_disconnect(&self.state).await;
                    tracing::debug!(
                        peer = %peer.socket_addr(),
                        error = %error,
                        "pure-v2 peer session ended without a usable transfer"
                    );
                }
            }
            if self.v2_selection_complete(&layout, &have) {
                break;
            }
        }

        if self.v2_selection_complete(&layout, &have) {
            self.finish_v2_selection(&storage, &layout, &have).await?;
        } else if !progressed {
            self.state.lock().await.tracker_message =
                Some("pure-v2 direct peers supplied no missing verified pieces".into());
        }
        Ok(self.state.lock().await.clone())
    }

    fn prepare_v2_file_selection(&mut self) {
        if self.file_priorities.len() != self.meta.files.len()
            || self.wanted.len() != self.meta.files.len()
        {
            self.file_priorities = vec![FilePriority::Normal; self.meta.files.len()];
            self.wanted = vec![true; self.meta.files.len()];
        }
        if let Some(selection) = self.intake_selection.as_ref() {
            swarmotter_core::policy::apply_intake_selection(
                &self.meta.files,
                &mut self.file_priorities,
                &mut self.wanted,
                selection,
            );
        }
    }

    fn v2_selected_files(&self) -> Vec<bool> {
        self.file_priorities
            .iter()
            .zip(&self.wanted)
            .map(|(priority, wanted)| *wanted && *priority != FilePriority::Unwanted)
            .collect()
    }

    fn v2_piece_selected(&self, layout: &V2PieceLayout, index: usize) -> bool {
        let Some(piece) = layout.piece(index) else {
            return false;
        };
        self.wanted.get(piece.file_index).copied().unwrap_or(false)
            && self
                .file_priorities
                .get(piece.file_index)
                .is_some_and(|priority| *priority != FilePriority::Unwanted)
    }

    fn v2_selection_complete(&self, layout: &V2PieceLayout, have: &PieceBitfield) -> bool {
        (0..layout.piece_count())
            .all(|index| !self.v2_piece_selected(layout, index) || have.has(index))
    }

    fn pick_v2_piece(
        &self,
        layout: &V2PieceLayout,
        peer_bitfield: &Bitfield,
        have: &PieceBitfield,
    ) -> Option<usize> {
        (0..layout.piece_count())
            .filter(|index| {
                self.v2_piece_selected(layout, *index)
                    && peer_bitfield.has(*index)
                    && !have.has(*index)
            })
            .max_by_key(|index| {
                layout
                    .piece(*index)
                    .and_then(|piece| self.file_priorities.get(piece.file_index))
                    .map_or(i32::MIN, |priority| priority.weight())
            })
    }

    async fn update_v2_progress(&self, layout: &V2PieceLayout, have: &PieceBitfield) {
        let bytes_completed = (0..layout.piece_count())
            .filter(|index| have.has(*index))
            .filter_map(|index| layout.piece(index))
            .fold(0u64, |total, piece| total.saturating_add(piece.length));
        let mut state = self.state.lock().await;
        state.pieces_have = have.clone();
        state.piece_count = layout.piece_count();
        state.total_length = self.meta.total_length;
        state.bytes_completed = bytes_completed.min(self.meta.total_length);
    }

    async fn finish_v2_selection(
        &self,
        storage: &StorageIo,
        layout: &V2PieceLayout,
        have: &PieceBitfield,
    ) -> Result<()> {
        let all_pieces_verified = have.count(layout.piece_count()) == layout.piece_count();
        if all_pieces_verified {
            let final_storage = if storage.base_dir() != self.complete_dir.as_path() {
                storage.move_to(self.complete_dir.clone()).await?
            } else {
                storage.finalize_partial_file_suffix().await?
            };
            self.finish_v2_without_resume(&final_storage).await?;
        } else {
            // A selected-file completion remains at the active root. Persist
            // its v2 file-aligned progress so a later selection change can
            // continue without accepting unverified bytes.
            self.state.lock().await.finished = true;
            self.persist_v2_resume(storage, layout, have).await?;
        }
        Ok(())
    }

    async fn download_v2_from_peer(
        &self,
        peer_addr: &PeerAddr,
        storage: &StorageIo,
        layout: &V2PieceLayout,
        have: &mut PieceBitfield,
    ) -> Result<bool> {
        if !self.binder.traffic_allowed() {
            return Ok(false);
        }
        if !self.peer_allowed(peer_addr) {
            return Ok(false);
        }
        let _peer_permit = self.peer_session_budget.acquire_outbound().await?;
        let wire_hash = self.v2_wire_hash()?;
        let (mut reader, mut write_half, transport) = connect_v2_peer_wire_with_transport(
            self.binder.clone(),
            *peer_addr,
            wire_hash,
            self.peer_id,
            self.utp_enabled,
            self.utp_prefer_tcp,
            self.encryption_mode,
            self.peer_filter.as_ref(),
        )
        .await?;
        tracing::debug!(
            peer = %peer_addr.socket_addr(),
            transport = transport.as_str(),
            "pure-v2 peer connected through contained transport"
        );
        record_peer_connected(&self.state, *peer_addr).await;

        let mut ours = Bitfield::new(layout.piece_count());
        for index in 0..layout.piece_count() {
            if have.has(index) {
                ours.set(index);
            }
        }
        peer::write_message(&mut write_half, &ours.encode_message()).await?;
        peer::write_message(&mut write_half, &Message::Interested).await?;
        write_half.flush().await.map_err(CoreError::from)?;

        let deadline = Instant::now() + Duration::from_secs(45);
        let mut peer_bitfield: Option<Bitfield> = None;
        let mut peer_choking = true;
        let mut made_progress = false;

        loop {
            if !self.binder.traffic_allowed() || Instant::now() >= deadline {
                return Ok(made_progress);
            }

            if !peer_choking {
                if let Some(piece_index) = peer_bitfield
                    .as_ref()
                    .and_then(|bits| self.pick_v2_piece(layout, bits, have))
                {
                    let Some(piece) = layout.piece(piece_index) else {
                        return Err(CoreError::Internal(
                            "v2 layout lost a selected piece".into(),
                        ));
                    };
                    let piece_length = u32::try_from(piece.length).map_err(|_| {
                        CoreError::MalformedTorrent(format!(
                            "BEP 52 piece {piece_index} length {} exceeds peer-wire range",
                            piece.length
                        ))
                    })?;
                    let requests = peer::block_requests(piece_length);
                    let expected = requests.iter().copied().collect::<HashMap<_, _>>();
                    for (offset, length) in &requests {
                        peer::write_message(
                            &mut write_half,
                            &Message::Request {
                                piece: u32::try_from(piece_index).map_err(|_| {
                                    CoreError::MalformedTorrent(
                                        "BEP 52 piece index exceeds peer-wire range".into(),
                                    )
                                })?,
                                offset: *offset,
                                length: *length,
                            },
                        )
                        .await?;
                    }
                    write_half.flush().await.map_err(CoreError::from)?;

                    let mut assembler =
                        peer::PieceAssembler::new(piece_index as u32, piece_length as usize);
                    let mut received = 0usize;
                    let mut rejected = false;
                    while received < requests.len() {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            break;
                        }
                        let message = match timeout(remaining, reader.read_message()).await {
                            Ok(Ok(Some(message))) => message,
                            Ok(Ok(None)) => break,
                            Ok(Err(error)) => return Err(error),
                            Err(_) => break,
                        };
                        match message {
                            Message::Piece {
                                piece: received_piece,
                                offset,
                                block,
                            } if received_piece as usize == piece_index => {
                                let Some(expected_length) = expected.get(&offset).copied() else {
                                    continue;
                                };
                                if block.len() != expected_length as usize {
                                    continue;
                                }
                                let block_index = offset as usize / peer::BLOCK_SIZE as usize;
                                let was_missing = assembler
                                    .received
                                    .get(block_index)
                                    .is_some_and(|value| !*value);
                                if assembler.add_block(offset, &block).is_ok() && was_missing {
                                    received = received.saturating_add(1);
                                    record_peer_block(&self.state, *peer_addr, block.len() as u64)
                                        .await;
                                }
                            }
                            Message::Reject {
                                piece: rejected_piece,
                                ..
                            } if rejected_piece as usize == piece_index => {
                                rejected = true;
                                break;
                            }
                            Message::Choke => {
                                peer_choking = true;
                                record_peer_choked(&self.state, *peer_addr).await;
                                break;
                            }
                            Message::Unchoke => {
                                peer_choking = false;
                                record_peer_unchoked(&self.state, *peer_addr).await;
                            }
                            Message::Bitfield { bits } => {
                                let bits = Bitfield::from_bytes(bits, layout.piece_count());
                                record_peer_availability(
                                    &self.state,
                                    *peer_addr,
                                    &bits,
                                    have,
                                    layout.piece_count(),
                                )
                                .await;
                                peer_bitfield = Some(bits);
                            }
                            Message::Have { piece } => {
                                apply_peer_have(&mut peer_bitfield, layout.piece_count(), piece);
                            }
                            Message::Keepalive
                            | Message::Interested
                            | Message::NotInterested
                            | Message::Request { .. }
                            | Message::Cancel { .. }
                            | Message::Reject { .. }
                            | Message::HashRequest { .. }
                            | Message::Hashes { .. }
                            | Message::HashReject { .. }
                            | Message::Extended { .. }
                            | Message::Unknown { .. }
                            | Message::Piece { .. } => {}
                        }
                    }

                    if rejected || received != requests.len() {
                        continue;
                    }
                    let data = assembler.data().to_vec();
                    if !layout.verify_piece(piece_index, &data) {
                        record_peer_hash_failure(&self.state, *peer_addr).await;
                        continue;
                    }
                    self.limiter
                        .acquire(RateDirection::Download, data.len() as u64)
                        .await;
                    storage.write_v2_piece(layout, piece_index, &data).await?;
                    have.set(piece_index);
                    made_progress = true;
                    self.update_v2_progress(layout, have).await;
                    self.persist_v2_resume(storage, layout, have).await?;
                    peer::write_message(
                        &mut write_half,
                        &Message::Have {
                            piece: u32::try_from(piece_index).map_err(|_| {
                                CoreError::MalformedTorrent(
                                    "BEP 52 piece index exceeds peer-wire range".into(),
                                )
                            })?,
                        },
                    )
                    .await?;
                    write_half.flush().await.map_err(CoreError::from)?;
                    continue;
                }
                return Ok(made_progress);
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let message = match timeout(remaining, reader.read_message()).await {
                Ok(Ok(Some(message))) => message,
                Ok(Ok(None)) => return Ok(made_progress),
                Ok(Err(error)) => return Err(error),
                Err(_) => return Ok(made_progress),
            };
            match message {
                Message::Unchoke => {
                    peer_choking = false;
                    record_peer_unchoked(&self.state, *peer_addr).await;
                }
                Message::Choke => {
                    peer_choking = true;
                    record_peer_choked(&self.state, *peer_addr).await;
                }
                Message::Bitfield { bits } => {
                    let bits = Bitfield::from_bytes(bits, layout.piece_count());
                    record_peer_availability(
                        &self.state,
                        *peer_addr,
                        &bits,
                        have,
                        layout.piece_count(),
                    )
                    .await;
                    peer_bitfield = Some(bits);
                }
                Message::Have { piece } => {
                    apply_peer_have(&mut peer_bitfield, layout.piece_count(), piece);
                }
                Message::Keepalive
                | Message::Interested
                | Message::NotInterested
                | Message::Request { .. }
                | Message::Piece { .. }
                | Message::Cancel { .. }
                | Message::Reject { .. }
                | Message::HashRequest { .. }
                | Message::Hashes { .. }
                | Message::HashReject { .. }
                | Message::Extended { .. }
                | Message::Unknown { .. } => {}
            }
        }
    }

    fn v2_wire_hash(&self) -> Result<PeerInfoHash> {
        self.meta.identity.v2_info_hash().map_or_else(
            || {
                Err(CoreError::UnsupportedTorrentFeature(
                    "pure-v2 engine requires a full SHA-256 identity".into(),
                ))
            },
            |identity| Ok(identity.peer_info_hash()),
        )
    }

    async fn load_or_recheck_v2(
        &self,
        storage: &StorageIo,
        layout: &V2PieceLayout,
    ) -> Result<PieceBitfield> {
        if let Some(resume) = storage.load_resume_v2(&self.torrent_key, layout).await? {
            let current_stamps = storage.resume_file_stamps().await?;
            let stamps_match = !resume.file_stamps.is_empty()
                && resume.file_stamps.len() == current_stamps.len()
                && resume.file_stamps == current_stamps;
            if !stamps_match {
                let payload_bytes = storage.payload_bytes_on_disk().await?;
                tracing::info!(
                    torrent_key = %self.torrent_key,
                    payload_bytes,
                    resume_bytes_completed = resume.bytes_completed,
                    stamps_match,
                    "pure-v2 fast resume does not match on-disk payload; rechecking storage"
                );
                storage.recheck_v2(layout).await
            } else {
                Ok(resume.piece_bitfield)
            }
        } else {
            storage.recheck_v2(layout).await
        }
    }

    async fn persist_v2_resume(
        &self,
        storage: &StorageIo,
        layout: &V2PieceLayout,
        have: &PieceBitfield,
    ) -> Result<()> {
        let piece_lengths = (0..layout.piece_count())
            .filter_map(|index| layout.piece(index))
            .map(|piece| piece.length)
            .collect::<Vec<_>>();
        let state = self.state.lock().await;
        let mut resume = swarmotter_core::storage::io::build_resume_with_wanted(
            self.torrent_key,
            self.meta.name.clone(),
            have.clone(),
            layout.piece_count(),
            state.downloaded,
            state.uploaded,
            state.total_length,
            Some(storage.base_dir().display().to_string()),
            now_secs(),
            state.finished.then(now_secs),
            &self.file_priorities,
            &self.wanted,
            &piece_lengths,
        );
        drop(state);
        resume.file_stamps = storage.resume_file_stamps().await?;
        storage.save_resume(&resume).await?;
        Ok(())
    }

    async fn finish_v2_without_resume(&self, storage: &StorageIo) -> Result<()> {
        self.state.lock().await.finished = true;
        storage.remove_resume().await?;
        if self.download_dir != self.complete_dir {
            let active_storage = StorageIo::new(self.meta.clone(), self.download_dir.clone())
                .with_torrent_key(self.torrent_key)
                .with_resume_dir(self.resume_dir.clone())
                .with_partial_file_suffix(self.partial_file_suffix.clone())
                .with_cow_strategy(self.cow_strategy)
                .with_metrics(self.storage_metrics.clone());
            active_storage.remove_resume().await?;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_v2_peer_wire_with_transport(
    binder: Arc<dyn NetworkBinder>,
    peer_addr: PeerAddr,
    wire_hash: PeerInfoHash,
    peer_id: [u8; 20],
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    peer_filter: &swarmotter_core::peer_filter::PeerFilter,
) -> Result<(PeerReader<PeerReadHalf>, PeerWriteHalf, PeerTransport)> {
    let decision = peer_filter.admit_ip(peer_addr.ip);
    if !decision.is_allowed() {
        return Err(CoreError::Internal(
            decision
                .rejection_message()
                .unwrap_or_else(|| "peer rejected by admission policy".into()),
        ));
    }

    let transports = peer_transport_order(utp_enabled, utp_prefer_tcp, encryption_mode);
    let mut last_error = None;
    for transport in transports {
        match utp::connect_peer_stream(binder.clone(), transport, peer_addr.socket_addr()).await {
            Ok((stream, selected)) => {
                let stream: Box<dyn PeerDuplex> = match encryption_mode {
                    PeerEncryptionMode::Disabled => stream,
                    PeerEncryptionMode::Required => {
                        let encrypted = timeout(
                            Duration::from_secs(10),
                            swarmotter_core::mse::connect(stream, wire_hash),
                        )
                        .await??;
                        Box::new(encrypted)
                    }
                    PeerEncryptionMode::Preferred => {
                        match timeout(
                            Duration::from_secs(10),
                            swarmotter_core::mse::connect(stream, wire_hash),
                        )
                        .await
                        {
                            Ok(Ok(encrypted)) => Box::new(encrypted),
                            Ok(Err(error)) => {
                                tracing::debug!(
                                    peer = %peer_addr.socket_addr(),
                                    transport = selected.as_str(),
                                    %error,
                                    "pure-v2 MSE/PE negotiation failed; retrying contained plaintext transport"
                                );
                                let (plain, _) = utp::connect_peer_stream(
                                    binder.clone(),
                                    selected,
                                    peer_addr.socket_addr(),
                                )
                                .await?;
                                plain
                            }
                            Err(error) => {
                                tracing::debug!(
                                    peer = %peer_addr.socket_addr(),
                                    transport = selected.as_str(),
                                    %error,
                                    "pure-v2 MSE/PE negotiation timed out; retrying contained plaintext transport"
                                );
                                let (plain, _) = utp::connect_peer_stream(
                                    binder.clone(),
                                    selected,
                                    peer_addr.socket_addr(),
                                )
                                .await?;
                                plain
                            }
                        }
                    }
                };
                let (read_half, mut write_half) = tokio::io::split(stream);
                let handshake = V2Handshake {
                    info_hash: wire_hash,
                    peer_id,
                    reserved: peer::with_v2_support(
                        swarmotter_core::extensions::EXTENSION_RESERVED,
                    ),
                };
                peer::write_v2_handshake(&mut write_half, &handshake).await?;
                write_half.flush().await.map_err(CoreError::from)?;
                let mut reader = PeerReader::new(read_half);
                let theirs = timeout(Duration::from_secs(10), reader.read_v2_handshake()).await??;
                if theirs.info_hash != wire_hash {
                    last_error = Some(CoreError::Internal(
                        "pure-v2 peer handshake hash mismatch".into(),
                    ));
                    continue;
                }
                let decision = peer_filter.admit_client_id(&theirs.peer_id);
                if !decision.is_allowed() {
                    return Err(CoreError::Internal(
                        decision
                            .rejection_message()
                            .unwrap_or_else(|| "peer rejected by admission policy".into()),
                    ));
                }
                // A responder need not set the upgrade bit for an already-v2
                // swarm; matching the v2 truncated identity is the wire
                // interoperability condition here.
                let _advertises_v2 = theirs.reserved[V2_RESERVED_BYTE_INDEX] & V2_RESERVED_BIT != 0;
                return Ok((reader, write_half, selected));
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| CoreError::Internal("no peer transport configured".into())))
}
