// SPDX-License-Identifier: Apache-2.0

use super::*;

impl TorrentEngine {
    /// Concurrent endgame download: request the remaining pieces' blocks from
    /// multiple peers at once, sharing a verified `have` bitfield, and cancel
    /// duplicate outstanding requests as pieces complete. Returns true if any
    /// new piece was verified and written.
    ///
    /// This implements real endgame behavior: the same remaining blocks are
    /// requested from several peers (bounded by the outstanding-request
    /// duplicate cap), and once a piece completes the still-outstanding
    /// blocks of that piece are cancelled to avoid request explosion. The
    /// request queues stay bounded by `ENDGAME_MAX_PEERS` and the duplicate
    /// cap.
    pub(super) async fn run_endgame(
        &self,
        candidates: &[PeerAddr],
        storage: &StorageIo,
        have: &mut PieceBitfield,
        bad_peers: &mut HashMap<SocketAddr, Instant>,
    ) -> bool {
        use swarmotter_core::endgame::{is_endgame, OutstandingRequests};
        const ENDGAME_MAX_PEERS: usize = 4;
        const ENDGAME_STEP_DEADLINE: Duration = Duration::from_secs(30);

        let shared_have = Arc::new(Mutex::new(have.clone()));
        let outstanding = Arc::new(Mutex::new(OutstandingRequests::new(ENDGAME_MAX_PEERS)));
        let made_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let storage = storage.clone();
        let selection = self.piece_selection.clone();

        let peers: Vec<PeerAddr> = candidates.iter().take(ENDGAME_MAX_PEERS).copied().collect();
        let mut handles = AbortOnDropHandles::new();
        let deadline = Instant::now() + ENDGAME_STEP_DEADLINE;
        for peer_addr in peers {
            let meta = self.meta.clone();
            let binder = self.binder.clone();
            let peer_id = self.peer_id;
            let shared_have = shared_have.clone();
            let outstanding = outstanding.clone();
            let made_progress = made_progress.clone();
            let storage = storage.clone();
            let state = self.state.clone();
            let limiter = self.limiter.clone();
            let utp_enabled = self.utp_enabled;
            let utp_prefer_tcp = self.utp_prefer_tcp;
            let encryption_mode = self.encryption_mode;
            let peer_filter = self.peer_filter.clone();
            let selection = selection.clone();
            let peer_session_budget = self.peer_session_budget.clone();
            handles.push(tokio::spawn(async move {
                endgame_peer_session(
                    binder,
                    peer_addr,
                    meta,
                    selection,
                    peer_id,
                    shared_have,
                    outstanding,
                    storage,
                    deadline,
                    made_progress,
                    state,
                    limiter,
                    utp_enabled,
                    utp_prefer_tcp,
                    encryption_mode,
                    peer_filter,
                    peer_session_budget,
                )
                .await
            }));
        }

        // Wait for all endgame peer sessions; record bad peers on failure.
        let mut any_progress = false;
        for (peer_addr, h) in candidates
            .iter()
            .take(ENDGAME_MAX_PEERS)
            .zip(handles.drain())
        {
            match h.await {
                Ok(Ok(progressed)) => {
                    if progressed {
                        any_progress = true;
                    }
                }
                Ok(Err(_)) => {
                    backoff_failed_peer(bad_peers, peer_addr.socket_addr());
                }
                // Task panic/cancellation: treat as a failed peer.
                Err(_) => {
                    backoff_failed_peer(bad_peers, peer_addr.socket_addr());
                }
            }
        }

        // Merge the shared have back into the local copy and persist progress.
        let merged = shared_have.lock().await.clone();
        let progressed = any_progress || made_progress.load(std::sync::atomic::Ordering::Relaxed);
        let _still_endgame = is_endgame(self.piece_selection.remaining(&merged));
        if progressed {
            *have = merged.clone();
            self.update_progress(&merged).await;
            if let Err(e) = self.persist_resume(&storage, &merged).await {
                tracing::warn!(error = %e, "endgame resume persist failed");
            }
        }
        progressed
    }
}

pub(super) struct AbortOnDropHandles<T> {
    pub(super) handles: Vec<AbortOnDropHandle<T>>,
}

impl<T> AbortOnDropHandles<T> {
    pub(super) fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, handle: tokio::task::JoinHandle<T>) {
        self.handles.push(AbortOnDropHandle { handle });
    }

    pub(super) fn drain(&mut self) -> std::vec::Drain<'_, AbortOnDropHandle<T>> {
        self.handles.drain(..)
    }
}

pub(super) struct AbortOnDropHandle<T> {
    pub(super) handle: tokio::task::JoinHandle<T>,
}

impl<T> std::future::Future for AbortOnDropHandle<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.handle).poll(cx)
    }
}

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn endgame_peer_session(
    binder: Arc<dyn NetworkBinder>,
    peer_addr: PeerAddr,
    meta: TorrentMeta,
    selection: PieceSelection,
    peer_id: [u8; 20],
    shared_have: Arc<Mutex<PieceBitfield>>,
    outstanding: Arc<Mutex<swarmotter_core::endgame::OutstandingRequests>>,
    storage: StorageIo,
    deadline: Instant,
    made_progress: Arc<std::sync::atomic::AtomicBool>,
    state: Arc<Mutex<EngineState>>,
    limiter: ShapedLimiter,
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    peer_filter: Arc<swarmotter_core::peer_filter::PeerFilter>,
    peer_session_budget: PeerSessionBudget,
) -> Result<bool> {
    if !binder.traffic_allowed() {
        return Ok(false);
    }
    let decision = peer_filter.admit_ip(peer_addr.ip);
    if !decision.is_allowed() {
        tracing::info!(
            peer = %peer_addr.socket_addr(),
            reason = decision.audit_reason(),
            detail = ?decision.rejection_message(),
            "endgame peer rejected before contained outbound admission"
        );
        return Ok(false);
    }
    let _peer_permit = peer_session_budget.acquire_outbound().await?;
    let (mut reader, mut write_half, transport) = connect_peer_wire_with_transport(
        binder.clone(),
        peer_addr,
        meta.info_hash,
        peer_id,
        utp_enabled,
        utp_prefer_tcp,
        encryption_mode,
        peer_filter.as_ref(),
    )
    .await?;
    tracing::debug!(peer = %peer_addr.socket_addr(), transport = transport.as_str(), "endgame peer connected");
    record_peer_connected(&state, peer_addr).await;

    // Send our bitfield and express interest.
    let mut our_bf = Bitfield::new(meta.piece_count());
    {
        let have = shared_have.lock().await;
        for i in 0..meta.piece_count() {
            if have.has(i) {
                our_bf.set(i);
            }
        }
    }
    peer::write_message(&mut write_half, &our_bf.encode_message()).await?;
    peer::write_message(&mut write_half, &Message::Interested).await?;
    write_half.flush().await.ok();

    let mut peer_bf: Option<Bitfield> = None;
    let mut peer_choking = true;
    let mut progressed = false;
    let piece_count = meta.piece_count();

    loop {
        if Instant::now() > deadline {
            break;
        }
        // Already complete?
        let complete = {
            let have = shared_have.lock().await;
            selection.complete(&have)
        };
        if complete {
            break;
        }

        if !peer_choking {
            // Pick a remaining piece the peer has and request its blocks,
            // honoring the outstanding duplicate cap.
            let candidate = {
                let have = shared_have.lock().await;
                let bf = match &peer_bf {
                    Some(b) => b,
                    None => return Ok(progressed),
                };
                (0..piece_count)
                    .filter(|&i| selection.includes(i) && bf.has(i) && !have.has(i))
                    .max_by_key(|&i| selection.priority(i))
            };
            let Some(piece_index) = candidate else {
                // Nothing this peer can give us right now.
                peer::write_message(&mut write_half, &Message::NotInterested).await?;
                break;
            };
            let piece_len = meta.piece_length_for_index_u32(piece_index)?;
            let reqs = block_requests(piece_len);
            // Request blocks respecting the duplicate cap.
            let mut sent_any = false;
            let mut session_outstanding = HashMap::new();
            for (off, len) in &reqs {
                let allowed = outstanding.lock().await.request(piece_index as u32, *off);
                if allowed {
                    peer::write_message(
                        &mut write_half,
                        &Message::Request {
                            piece: piece_index as u32,
                            offset: *off,
                            length: *len,
                        },
                    )
                    .await?;
                    session_outstanding.insert(*off, *len);
                    sent_any = true;
                }
            }
            write_half.flush().await.ok();
            if !sent_any {
                // All blocks already at the duplicate cap from other peers;
                // wait briefly for progress.
                continue;
            }

            // Assemble the piece from blocks this peer returns.
            let mut assembler = peer::PieceAssembler::new(piece_index as u32, piece_len as usize);
            let mut received = 0usize;
            let piece_deadline = Instant::now() + Duration::from_secs(20);
            while received < reqs.len() {
                let remaining = piece_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let msg = match timeout(remaining, reader.read_message()).await {
                    Ok(Ok(Some(m))) => m,
                    _ => break,
                };
                match msg {
                    Message::Piece {
                        piece,
                        offset,
                        block,
                    } => {
                        if piece as usize == piece_index {
                            let Some(expected_len) = session_outstanding.get(&offset).copied()
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
                            if assembler.add_block(offset, &block).is_ok() {
                                session_outstanding.remove(&offset);
                                if was_missing {
                                    received += 1;
                                    record_peer_block(&state, peer_addr, block.len() as u64).await;
                                    outstanding.lock().await.delivered(piece, offset);
                                }
                            }
                        } else if piece as usize != piece_index {
                            // A block for a piece we no longer need (completed
                            // by another peer): cancel outstanding duplicates
                            // and ignore.
                            let stale = outstanding.lock().await.outstanding_for_piece(piece);
                            for (p, o) in &stale {
                                peer::write_message(
                                    &mut write_half,
                                    &Message::Cancel {
                                        piece: *p,
                                        offset: *o,
                                        length: peer::BLOCK_SIZE,
                                    },
                                )
                                .await?;
                            }
                            write_half.flush().await.ok();
                        }
                    }
                    Message::Choke => {
                        peer_choking = true;
                        record_peer_choked(&state, peer_addr).await;
                        break;
                    }
                    Message::Unchoke => {
                        peer_choking = false;
                        record_peer_unchoked(&state, peer_addr).await;
                    }
                    Message::Have { piece } => {
                        apply_peer_have(&mut peer_bf, piece_count, piece);
                        if let Some(bf) = &peer_bf {
                            let have = shared_have.lock().await.clone();
                            record_peer_availability(&state, peer_addr, bf, &have, piece_count)
                                .await;
                        }
                    }
                    Message::Bitfield { bits } => {
                        let bf = Bitfield::from_bytes(bits, piece_count);
                        let have = shared_have.lock().await.clone();
                        record_peer_availability(&state, peer_addr, &bf, &have, piece_count).await;
                        peer_bf = Some(bf);
                    }
                    _ => {}
                }
            }

            if received == reqs.len() {
                let data = assembler.data().to_vec();
                if swarmotter_core::storage::verify_piece(&meta, piece_index, &data) {
                    // Only the first peer to complete writes it.
                    let already = {
                        let have = shared_have.lock().await;
                        have.has(piece_index)
                    };
                    if !already {
                        // Live download rate shaping for the endgame path too.
                        limiter
                            .acquire(RateDirection::Download, data.len() as u64)
                            .await;
                        storage.write_piece(piece_index, &data).await?;
                        shared_have.lock().await.set(piece_index);
                        outstanding.lock().await.clear_piece(piece_index as u32);
                        progressed = true;
                        made_progress.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    // Cancel any still-outstanding duplicates of this piece.
                    let stale = outstanding
                        .lock()
                        .await
                        .outstanding_for_piece(piece_index as u32);
                    for (p, o) in &stale {
                        peer::write_message(
                            &mut write_half,
                            &Message::Cancel {
                                piece: *p,
                                offset: *o,
                                length: peer::BLOCK_SIZE,
                            },
                        )
                        .await?;
                    }
                    write_half.flush().await.ok();
                } else {
                    record_peer_hash_failure(&state, peer_addr).await;
                }
            } else {
                release_endgame_session_requests(
                    &outstanding,
                    piece_index as u32,
                    &session_outstanding,
                )
                .await;
                record_peer_timeout(&state, peer_addr).await;
            }
            continue;
        }

        // Wait for unchoke / bitfield / have.
        let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
            Ok(Ok(Some(m))) => m,
            _ => break,
        };
        match msg {
            Message::Unchoke => {
                peer_choking = false;
                record_peer_unchoked(&state, peer_addr).await;
            }
            Message::Choke => {
                peer_choking = true;
                record_peer_choked(&state, peer_addr).await;
            }
            Message::Bitfield { bits } => {
                let bf = Bitfield::from_bytes(bits, piece_count);
                let have = shared_have.lock().await.clone();
                record_peer_availability(&state, peer_addr, &bf, &have, piece_count).await;
                peer_bf = Some(bf);
            }
            Message::Have { piece } => {
                apply_peer_have(&mut peer_bf, piece_count, piece);
                if let Some(bf) = &peer_bf {
                    let have = shared_have.lock().await.clone();
                    record_peer_availability(&state, peer_addr, bf, &have, piece_count).await;
                }
            }
            _ => {}
        }
    }

    Ok(progressed)
}

pub(super) async fn release_endgame_session_requests(
    outstanding: &Arc<Mutex<swarmotter_core::endgame::OutstandingRequests>>,
    piece: u32,
    session_outstanding: &HashMap<u32, u32>,
) {
    if session_outstanding.is_empty() {
        return;
    }
    let mut outstanding = outstanding.lock().await;
    for offset in session_outstanding.keys().copied() {
        outstanding.cancel_request(piece, offset);
    }
}
