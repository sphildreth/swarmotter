// SPDX-License-Identifier: Apache-2.0

use super::*;

impl TorrentEngine {
    pub(super) fn peer_allowed(&self, peer: &PeerAddr) -> bool {
        if !peer_allowed_by_config(peer, self.allow_ipv6) {
            return false;
        }
        let decision = self.peer_filter.admit_ip(peer.ip);
        if !decision.is_allowed() {
            tracing::info!(
                peer = %peer.socket_addr(),
                reason = decision.audit_reason(),
                detail = ?decision.rejection_message(),
                "peer rejected by outbound admission policy"
            );
        }
        decision.is_allowed()
    }

    pub(super) fn filter_allowed_peers(&self, peers: Vec<PeerAddr>) -> Vec<PeerAddr> {
        peers
            .into_iter()
            .filter(|peer| self.peer_allowed(peer))
            .collect()
    }

    pub(super) async fn record_peer_scheduler(&self, diagnostics: PeerSchedulerDiagnostics) {
        self.state.lock().await.peer_scheduler = diagnostics;
    }

    pub(super) async fn set_peer_scheduler_serial_active(&self, active: bool) {
        self.state.lock().await.peer_scheduler.serial_peer_active = active;
    }

    pub(super) async fn update_peer_scheduler_parallel_workers(&self, workers: usize) {
        let mut state = self.state.lock().await;
        state.peer_scheduler.parallel_workers_started = workers;
        state.peer_scheduler.serial_peer_active = false;
    }
}

impl TorrentEngine {
    /// Attempt to download missing pieces from a single peer. Returns true if
    /// at least one new piece was verified and written.
    pub(super) async fn download_from_peer(
        &self,
        peer_addr: &PeerAddr,
        storage: &StorageIo,
        have: &mut PieceBitfield,
        discovered: &mut Vec<PeerAddr>,
    ) -> Result<(bool, &'static str)> {
        if !self.binder.traffic_allowed() {
            return Ok((false, "transport_blocked"));
        }
        if !self.peer_allowed(peer_addr) {
            return Ok((false, "peer_rejected_by_policy"));
        }
        let _peer_permit = self.peer_session_budget.acquire_outbound().await?;
        let (mut reader, mut write_half, transport) = connect_peer_wire_with_transport(
            self.binder.clone(),
            *peer_addr,
            self.meta.info_hash,
            self.peer_id,
            self.utp_enabled,
            self.utp_prefer_tcp,
            self.encryption_mode,
            self.peer_filter.as_ref(),
        )
        .await?;
        tracing::debug!(peer = %peer_addr.socket_addr(), transport = transport.as_str(), "peer connected");

        // Exchange bitfields.
        let mut our_bf = Bitfield::new(self.meta.piece_count());
        for i in 0..self.meta.piece_count() {
            if have.has(i) {
                our_bf.set(i);
            }
        }
        peer::write_message(&mut write_half, &our_bf.encode_message()).await?;
        write_half.flush().await.ok();

        // Register a per-peer health entry so the daemon's health calculator
        // can see this peer. We update `last_seen`/`has_missing_pieces` on
        // every meaningful event.
        record_peer_connected(&self.state, *peer_addr).await;

        // Send a BEP 10 extension handshake advertising configured extensions.
        // PEX is honored only for non-private torrents and only when enabled.
        let local_pex_id: u8 = 1u8;
        let local_metadata_id: u8 = 2u8;
        let mut extensions = vec![(
            swarmotter_core::extensions::UT_METADATA_NAME,
            local_metadata_id,
        )];
        if self.pex_enabled && !self.meta.is_private() {
            extensions.push((swarmotter_core::extensions::UT_PEX_NAME, local_pex_id));
        }
        let ext_payload = swarmotter_core::extensions::encode_extension_handshake_with_reqq(
            &extensions,
            "SwarmOtter/0.1",
            None,
        );
        peer::write_message(
            &mut write_half,
            &Message::Extended {
                id: swarmotter_core::extensions::EXTENSION_HANDSHAKE_ID,
                payload: ext_payload,
            },
        )
        .await?;
        write_half.flush().await.ok();

        // We are interested; ask to be unchoked.
        peer::write_message(&mut write_half, &Message::Interested).await?;

        let mut peer_bf: Option<Bitfield> = None;
        let mut peer_choking = true;
        let mut made_progress = false;
        let piece_count = self.meta.piece_count();
        let mut remote_pex_id: Option<u8> = None;
        let mut no_progress_reason: Option<&'static str> = None;

        // Drive a small download loop: pick a missing piece the peer has,
        // request its blocks, assemble, verify, write.
        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            if Instant::now() > deadline {
                no_progress_reason = Some("deadline_exceeded");
                break;
            }
            if self.piece_selection.complete(have) {
                no_progress_reason = Some("torrent_complete");
                break;
            }

            // If unchoked and we have a candidate piece, request blocks.
            if !peer_choking && peer_bf.is_some() {
                if let Some(piece_index) = self.pick_piece(peer_bf.as_ref(), have) {
                    let plen = self.meta.piece_length_for_index_u32(piece_index)?;
                    let reqs = block_requests(plen);
                    let expected_blocks: HashMap<u32, u32> = reqs.iter().copied().collect();
                    // Send all block requests for this piece.
                    for (off, len) in &reqs {
                        peer::write_message(
                            &mut write_half,
                            &Message::Request {
                                piece: piece_index as u32,
                                offset: *off,
                                length: *len,
                            },
                        )
                        .await?;
                    }
                    write_half.flush().await.ok();

                    // Assemble the piece from incoming blocks.
                    let mut assembler =
                        peer::PieceAssembler::new(piece_index as u32, plen as usize);
                    let mut received_blocks = 0usize;
                    let piece_deadline = Instant::now() + Duration::from_secs(30);
                    while received_blocks < reqs.len() {
                        let remaining = piece_deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            no_progress_reason = Some("piece_download_timeout");
                            break;
                        }
                        let msg = match timeout(remaining, reader.read_message()).await {
                            Ok(Ok(Some(m))) => m,
                            Ok(Ok(None)) => {
                                no_progress_reason =
                                    Some("peer_closed_connection_during_piece_download");
                                break;
                            }
                            Ok(Err(_)) => {
                                no_progress_reason = Some("peer_message_read_error");
                                break;
                            }
                            Err(_) => {
                                no_progress_reason = Some("piece_download_timeout");
                                break;
                            }
                        };
                        match msg {
                            Message::Piece {
                                piece,
                                offset,
                                block,
                            } => {
                                if piece as usize == piece_index {
                                    let Some(expected_len) = expected_blocks.get(&offset).copied()
                                    else {
                                        continue;
                                    };
                                    if block.len() != expected_len as usize {
                                        continue;
                                    }
                                    let block_index = offset as usize / peer::BLOCK_SIZE as usize;
                                    let was_missing = assembler
                                        .received
                                        .get(block_index)
                                        .map(|received| !*received)
                                        .unwrap_or(false);
                                    if assembler.add_block(offset, &block).is_ok() && was_missing {
                                        received_blocks += 1;
                                        record_peer_block(
                                            &self.state,
                                            *peer_addr,
                                            block.len() as u64,
                                        )
                                        .await;
                                    }
                                }
                            }
                            Message::Choke => {
                                peer_choking = true;
                                no_progress_reason = Some("peer_choked_us");
                                record_peer_choked(&self.state, *peer_addr).await;
                                break;
                            }
                            Message::Unchoke => {
                                peer_choking = false;
                                record_peer_unchoked(&self.state, *peer_addr).await;
                            }
                            Message::Have { piece } => {
                                apply_peer_have(&mut peer_bf, piece_count, piece);
                                if let Some(bf) = &peer_bf {
                                    record_peer_availability(
                                        &self.state,
                                        *peer_addr,
                                        bf,
                                        have,
                                        piece_count,
                                    )
                                    .await;
                                }
                            }
                            Message::Bitfield { bits } => {
                                let bf = Bitfield::from_bytes(bits, piece_count);
                                record_peer_availability(
                                    &self.state,
                                    *peer_addr,
                                    &bf,
                                    have,
                                    piece_count,
                                )
                                .await;
                                peer_bf = Some(bf);
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
                            | Message::Unknown { .. } => {}
                        }
                    }

                    if received_blocks == reqs.len() {
                        let data = assembler.data().to_vec();
                        if swarmotter_core::storage::verify_piece(&self.meta, piece_index, &data) {
                            // Live download rate shaping: acquire tokens for the
                            // downloaded bytes before committing them.
                            self.limiter
                                .acquire(RateDirection::Download, data.len() as u64)
                                .await;
                            storage.write_piece(piece_index, &data).await?;
                            have.set(piece_index);
                            made_progress = true;
                            self.update_progress(have).await;
                            self.persist_resume(storage, have).await?;
                            // Tell the peer we have it.
                            peer::write_message(
                                &mut write_half,
                                &Message::Have {
                                    piece: piece_index as u32,
                                },
                            )
                            .await?;
                        } else {
                            tracing::warn!(piece = piece_index, "piece hash mismatch; rejecting");
                            record_peer_hash_failure(&self.state, *peer_addr).await;
                            // Bad hash: do not mark; try a different piece.
                            no_progress_reason = Some("piece_hash_mismatch");
                        }
                    } else if no_progress_reason.is_none() {
                        no_progress_reason = Some("piece_download_incomplete");
                    }
                    continue;
                } else {
                    // No missing piece this peer has; not interesting.
                    no_progress_reason = Some("peer_has_no_missing_pieces");
                    peer::write_message(&mut write_half, &Message::NotInterested).await?;
                    break;
                }
            }

            // Wait for unchoke / bitfield / have.
            let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
                Ok(Ok(Some(m))) => m,
                Ok(Ok(None)) => {
                    no_progress_reason = Some("peer_closed_connection");
                    break;
                }
                Ok(Err(_)) => {
                    no_progress_reason = Some("peer_message_read_error");
                    break;
                }
                Err(_) => {
                    no_progress_reason = Some("state_wait_timeout");
                    break;
                }
            };
            match msg {
                Message::Unchoke => {
                    peer_choking = false;
                    record_peer_unchoked(&self.state, *peer_addr).await;
                }
                Message::Choke => {
                    no_progress_reason = Some("peer_choked_us");
                    peer_choking = true;
                    record_peer_choked(&self.state, *peer_addr).await;
                }
                Message::Bitfield { bits } => {
                    let bf = Bitfield::from_bytes(bits, piece_count);
                    record_peer_availability(&self.state, *peer_addr, &bf, have, piece_count).await;
                    peer_bf = Some(bf);
                }
                Message::Have { piece } => {
                    apply_peer_have(&mut peer_bf, piece_count, piece);
                    if let Some(bf) = &peer_bf {
                        record_peer_availability(&self.state, *peer_addr, bf, have, piece_count)
                            .await;
                    }
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
                | Message::Unknown { .. } => {}
                Message::Extended { id, payload } => {
                    // BEP 10 extension: handshake (id 0) or a PEX message.
                    if id == swarmotter_core::extensions::EXTENSION_HANDSHAKE_ID {
                        if let Ok(hs) =
                            swarmotter_core::extensions::parse_extension_handshake(&payload)
                        {
                            if self.pex_enabled && !self.meta.is_private() {
                                remote_pex_id = hs.id_for(swarmotter_core::extensions::UT_PEX_NAME);
                            }
                        }
                    } else if self.pex_enabled
                        && Some(id) == remote_pex_id
                        && !self.meta.is_private()
                    {
                        if let Ok(pex) = swarmotter_core::extensions::parse_pex(&payload) {
                            let max_peers = self.pex_max_peers;
                            let before = discovered.len();
                            add_pex_peers(
                                discovered,
                                pex.added.into_iter().chain(pex.added6),
                                self.allow_ipv6,
                                self.peer_filter.as_ref(),
                                max_peers,
                            );
                            if discovered.len() > before {
                                let mut st = self.state.lock().await;
                                st.pex_discovery_ok = true;
                                st.pex_last_seen = Some(Instant::now());
                                st.peers = discovered.clone();
                            }
                        }
                    }
                }
            }
        }

        if made_progress {
            return Ok((true, "progressed"));
        }
        let no_progress_reason =
            no_progress_reason.unwrap_or("session_ended_without_terminal_reason");
        tracing::debug!(
            peer = %peer_addr.socket_addr(),
            reason = no_progress_reason,
            "serial peer session ended without progress"
        );
        Ok((false, no_progress_reason))
    }
}

pub(super) fn peer_bitfield_has_missing(
    peer_bf: &Bitfield,
    have: &PieceBitfield,
    piece_count: usize,
) -> bool {
    (0..piece_count).any(|i| peer_bf.has(i) && !have.has(i))
}

pub(super) fn peer_bitfield_snapshot(peer_bf: &Bitfield, piece_count: usize) -> PieceBitfield {
    let mut out = PieceBitfield::new(piece_count);
    for i in 0..piece_count {
        if peer_bf.has(i) {
            out.set(i);
        }
    }
    out
}

pub(super) fn apply_peer_have(peer_bf: &mut Option<Bitfield>, piece_count: usize, piece: u32) {
    peer_bf
        .get_or_insert_with(|| Bitfield::new(piece_count))
        .set(piece as usize);
}

pub(super) async fn record_peer_connected(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    st.peer_health
        .entry(peer_addr.socket_addr())
        .or_default()
        .last_seen = Some(Instant::now());
}

pub(super) async fn record_peer_unchoked(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.unchoked = true;
    entry.last_seen = Some(Instant::now());
}

pub(super) async fn record_peer_choked(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.unchoked = false;
    entry.last_seen = Some(Instant::now());
}

pub(super) async fn record_peer_availability(
    state: &Arc<Mutex<EngineState>>,
    peer_addr: PeerAddr,
    peer_bf: &Bitfield,
    have: &PieceBitfield,
    piece_count: usize,
) {
    let mut st = state.lock().await;
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.piece_bitfield = Some(peer_bitfield_snapshot(peer_bf, piece_count));
    entry.has_missing_pieces = peer_bitfield_has_missing(peer_bf, have, piece_count);
    entry.last_seen = Some(Instant::now());
}

pub(super) async fn record_peer_block(
    state: &Arc<Mutex<EngineState>>,
    peer_addr: PeerAddr,
    bytes: u64,
) {
    if bytes == 0 {
        return;
    }
    let mut st = state.lock().await;
    st.downloaded = st.downloaded.saturating_add(bytes);
    st.block_last_seen = Some(Instant::now());
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.last_valid_block = Some(Instant::now());
    entry.has_missing_pieces = true;
    entry.useful_recently = true;
    entry.unchoked = true;
    entry.last_seen = Some(Instant::now());
}

pub(super) async fn record_webseed_block(state: &Arc<Mutex<EngineState>>, bytes: u64) {
    if bytes == 0 {
        return;
    }
    let mut st = state.lock().await;
    st.downloaded = st.downloaded.saturating_add(bytes);
    st.last_valid_block = Some(Instant::now());
    st.block_last_seen = Some(Instant::now());
    st.webseed_last_seen = Some(Instant::now());
}

pub(super) async fn record_peer_timeout(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    st.timeout_failures = st.timeout_failures.saturating_add(1);
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.last_seen = Some(Instant::now());
}

pub(super) async fn record_peer_hash_failure(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    st.hash_failures = st.hash_failures.saturating_add(1);
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.blocked = true;
    entry.last_seen = Some(Instant::now());
}

pub(super) async fn record_peer_disconnect(state: &Arc<Mutex<EngineState>>) {
    let mut st = state.lock().await;
    st.peer_disconnects_recent = st.peer_disconnects_recent.saturating_add(1);
}

pub(super) fn prune_peer_backoff(backoff: &mut HashMap<SocketAddr, Instant>) {
    let now = Instant::now();
    backoff.retain(|_, until| *until > now);
}

pub(super) fn peer_is_backed_off(backoff: &HashMap<SocketAddr, Instant>, peer: SocketAddr) -> bool {
    backoff
        .get(&peer)
        .is_some_and(|until| *until > Instant::now())
}

pub(super) fn backoff_peer_for(
    backoff: &mut HashMap<SocketAddr, Instant>,
    peer: SocketAddr,
    duration: Duration,
) {
    backoff.insert(peer, Instant::now() + duration);
}

pub(super) fn backoff_peer(backoff: &mut HashMap<SocketAddr, Instant>, peer: SocketAddr) {
    backoff_peer_for(backoff, peer, PEER_IDLE_BACKOFF);
}

pub(super) fn backoff_failed_peer(backoff: &mut HashMap<SocketAddr, Instant>, peer: SocketAddr) {
    backoff_peer_for(backoff, peer, PEER_FAILURE_BACKOFF);
}

pub(super) fn rotated_peer_candidates(
    eligible: &[PeerAddr],
    cursor: &mut usize,
    limit: usize,
) -> Vec<PeerAddr> {
    if eligible.is_empty() || limit == 0 {
        return Vec::new();
    }
    let start = *cursor % eligible.len();
    let take = eligible.len().min(limit);
    let mut out = Vec::with_capacity(take);
    for offset in 0..take {
        out.push(eligible[(start + offset) % eligible.len()]);
    }
    *cursor = (start + take) % eligible.len();
    out
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn connect_peer_wire_with_transport(
    binder: Arc<dyn NetworkBinder>,
    peer_addr: PeerAddr,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    peer_filter: &swarmotter_core::peer_filter::PeerFilter,
) -> Result<(PeerReader<PeerReadHalf>, PeerWriteHalf, PeerTransport)> {
    let decision = peer_filter.admit_ip(peer_addr.ip);
    if !decision.is_allowed() {
        tracing::info!(
            peer = %peer_addr.socket_addr(),
            reason = decision.audit_reason(),
            detail = ?decision.rejection_message(),
            "peer rejected before contained outbound transport admission"
        );
        return Err(CoreError::Internal(
            decision
                .rejection_message()
                .unwrap_or_else(|| "peer rejected by admission policy".into()),
        ));
    }
    let transports = peer_transport_order(utp_enabled, utp_prefer_tcp, encryption_mode);

    let mut last_error = None;
    for (idx, transport) in transports.iter().copied().enumerate() {
        match attempt_peer_wire_transport(
            binder.clone(),
            transport,
            peer_addr,
            info_hash,
            peer_id,
            encryption_mode,
            peer_filter,
        )
        .await
        {
            Ok(session) => return Ok(session),
            Err(e) => {
                if idx + 1 < transports.len() {
                    tracing::debug!(
                        peer = %peer_addr.socket_addr(),
                        transport = transport.as_str(),
                        error = %e,
                        "peer transport failed before usable handshake; trying fallback"
                    );
                }
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| CoreError::Internal("no peer transport configured".into())))
}

pub(super) fn peer_transport_order(
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    _encryption_mode: PeerEncryptionMode,
) -> Vec<PeerTransport> {
    if !utp_enabled {
        return vec![PeerTransport::Tcp];
    }
    if utp_prefer_tcp {
        vec![PeerTransport::Tcp, PeerTransport::Utp]
    } else {
        vec![PeerTransport::Utp, PeerTransport::Tcp]
    }
}

pub(super) async fn attempt_peer_wire_transport(
    binder: Arc<dyn NetworkBinder>,
    transport: PeerTransport,
    peer_addr: PeerAddr,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    encryption_mode: PeerEncryptionMode,
    peer_filter: &swarmotter_core::peer_filter::PeerFilter,
) -> Result<(PeerReader<PeerReadHalf>, PeerWriteHalf, PeerTransport)> {
    let (stream, selected) =
        utp::connect_peer_stream(binder.clone(), transport, peer_addr.socket_addr()).await?;
    let stream = match encryption_mode {
        PeerEncryptionMode::Disabled => stream,
        PeerEncryptionMode::Required => {
            let encrypted = timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, PeerInfoHash::from_v1(info_hash)),
            )
            .await??;
            Box::new(encrypted) as Box<dyn utp::PeerDuplex>
        }
        PeerEncryptionMode::Preferred => {
            match timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, PeerInfoHash::from_v1(info_hash)),
            )
            .await
            {
                Ok(Ok(encrypted)) => Box::new(encrypted) as Box<dyn utp::PeerDuplex>,
                Ok(Err(e)) => {
                    tracing::debug!(
                        peer = %peer_addr.socket_addr(),
                        transport = selected.as_str(),
                        error = %e,
                        "MSE/PE negotiation failed; retrying contained peer transport as plaintext"
                    );
                    let (plain, _) =
                        utp::connect_peer_stream(binder, selected, peer_addr.socket_addr()).await?;
                    plain
                }
                Err(e) => {
                    tracing::debug!(
                        peer = %peer_addr.socket_addr(),
                        transport = selected.as_str(),
                        error = %e,
                        "MSE/PE negotiation timed out; retrying contained peer transport as plaintext"
                    );
                    let (plain, _) =
                        utp::connect_peer_stream(binder, selected, peer_addr.socket_addr()).await?;
                    plain
                }
            }
        }
    };
    let (read_half, mut write_half) = tokio::io::split(stream);
    let hs = Handshake {
        info_hash,
        peer_id,
        reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
    };
    peer::write_handshake(&mut write_half, &hs).await?;
    write_half.flush().await?;
    let mut reader = PeerReader::new(read_half);
    let their_hs = timeout(Duration::from_secs(10), reader.read_handshake()).await??;
    if their_hs.info_hash != info_hash {
        return Err(CoreError::Internal(
            "peer handshake info hash mismatch".into(),
        ));
    }
    let decision = peer_filter.admit_client_id(&their_hs.peer_id);
    if !decision.is_allowed() {
        tracing::info!(
            peer = %peer_addr.socket_addr(),
            reason = decision.audit_reason(),
            detail = ?decision.rejection_message(),
            "peer rejected after contained outbound handshake"
        );
        return Err(CoreError::Internal(
            decision
                .rejection_message()
                .unwrap_or_else(|| "peer rejected by admission policy".into()),
        ));
    }

    Ok((reader, write_half, selected))
}
