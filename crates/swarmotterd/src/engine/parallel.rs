// SPDX-License-Identifier: Apache-2.0

use super::*;

impl TorrentEngine {
    /// Normal-mode parallel download: several peers fetch distinct reserved
    /// pieces concurrently. Unlike endgame, duplicate piece requests are
    /// avoided; endgame remains responsible for deliberate duplicate requests
    /// near completion.
    pub(super) async fn run_parallel_peer_round(
        &self,
        candidates: &[PeerAddr],
        max_active: usize,
        storage: &StorageIo,
        have: &mut PieceBitfield,
        bad_peers: &mut HashMap<SocketAddr, Instant>,
        peer_backoff: &mut HashMap<SocketAddr, Instant>,
    ) -> (bool, Vec<PeerAddr>) {
        if candidates.len() < 2 {
            return (false, Vec::new());
        }

        const PEER_REFILL_INTERVAL: Duration = Duration::from_secs(5);
        let shared = Arc::new(Mutex::new(ParallelPieceState::new(
            have.clone(),
            self.meta.piece_count(),
            self.piece_selection.clone(),
        )));
        let made_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let pex_peers = Arc::new(Mutex::new(Vec::new()));
        let storage = Arc::new(storage.clone());
        let deadline = Instant::now() + NORMAL_PEER_SESSION_DEADLINE;
        let max_active = max_active.max(1);
        let mut candidates = candidates.to_vec();
        let mut seen_candidates: HashSet<SocketAddr> =
            candidates.iter().map(|p| p.socket_addr()).collect();
        let mut discovered_pex = Vec::new();
        let mut tasks = tokio::task::JoinSet::new();
        let mut next_candidate = 0usize;
        let mut next_discovery_refresh = Instant::now() + PEER_REFRESH_INTERVAL;

        let planned_session_count = max_active.min(candidates.len()).max(1);
        while next_candidate < candidates.len() && tasks.len() < max_active {
            spawn_parallel_peer_task(
                &mut tasks,
                candidates[next_candidate],
                self.meta.clone(),
                self.binder.clone(),
                self.peer_id,
                shared.clone(),
                storage.clone(),
                self.state.clone(),
                deadline,
                made_progress.clone(),
                pex_peers.clone(),
                self.limiter.clone(),
                self.utp_enabled,
                self.utp_prefer_tcp,
                self.encryption_mode,
                self.pex_enabled && !self.meta.is_private(),
                self.allow_ipv6,
                self.pex_max_peers,
                planned_session_count,
                self.peer_session_budget.clone(),
            );
            next_candidate += 1;
        }

        if tasks.is_empty() {
            self.update_peer_scheduler_parallel_workers(0).await;
            return (false, Vec::new());
        }

        {
            let mut s = self.state.lock().await;
            s.active_peers = tasks.len();
            s.peer_scheduler.parallel_workers_started = tasks.len();
            s.peer_scheduler.serial_peer_active = false;
        }

        let mut any_progress = false;
        while !tasks.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tasks.abort_all();
                break;
            }

            let wait_for = remaining.min(PEER_REFILL_INTERVAL);
            match timeout(wait_for, tasks.join_next()).await {
                Ok(Some(joined)) => match joined {
                    Ok((_, Ok(PeerSessionOutcome::Progressed))) => {
                        any_progress = true;
                    }
                    Ok((peer_addr, Ok(PeerSessionOutcome::NoProgress))) => {
                        tracing::debug!(
                            peer = %peer_addr.socket_addr(),
                            "parallel peer session ended without progress"
                        );
                        backoff_peer(peer_backoff, peer_addr.socket_addr());
                    }
                    Ok((_, Ok(PeerSessionOutcome::NoWorkAvailable))) => {
                        tracing::debug!("parallel peer session had no immediate in-session work");
                        // This peer had useful pieces, but all currently useful work
                        // was already reserved by other workers. Do not penalize it.
                    }
                    Ok((peer_addr, Err(e))) => {
                        tracing::debug!(peer = %peer_addr.socket_addr(), error = %e, "parallel peer failed; suppressing");
                        record_peer_disconnect(&self.state).await;
                        backoff_failed_peer(bad_peers, peer_addr.socket_addr());
                    }
                    Err(_) => {
                        record_peer_disconnect(&self.state).await;
                    }
                },
                Ok(None) => break,
                Err(_) => {}
            }

            let complete = {
                let work = shared.lock().await;
                work.selection.complete(&work.have)
            };
            if complete {
                tasks.abort_all();
                break;
            }

            merge_dynamic_parallel_candidates(
                &mut candidates,
                &mut seen_candidates,
                &mut discovered_pex,
                &pex_peers,
                bad_peers,
                peer_backoff,
                self.allow_ipv6,
            )
            .await;
            if Instant::now() >= next_discovery_refresh {
                let refreshed = self.refresh_discovery_peers(false).await;
                merge_parallel_candidate_iter(
                    &mut candidates,
                    &mut seen_candidates,
                    refreshed,
                    bad_peers,
                    peer_backoff,
                    self.allow_ipv6,
                );
                next_discovery_refresh = Instant::now() + PEER_REFRESH_INTERVAL;
            }

            let planned_session_count = max_active.min(candidates.len()).max(1);
            while !complete && next_candidate < candidates.len() && tasks.len() < max_active {
                spawn_parallel_peer_task(
                    &mut tasks,
                    candidates[next_candidate],
                    self.meta.clone(),
                    self.binder.clone(),
                    self.peer_id,
                    shared.clone(),
                    storage.clone(),
                    self.state.clone(),
                    deadline,
                    made_progress.clone(),
                    pex_peers.clone(),
                    self.limiter.clone(),
                    self.utp_enabled,
                    self.utp_prefer_tcp,
                    self.encryption_mode,
                    self.pex_enabled && !self.meta.is_private(),
                    self.allow_ipv6,
                    self.pex_max_peers,
                    planned_session_count,
                    self.peer_session_budget.clone(),
                );
                next_candidate += 1;
            }

            self.state.lock().await.active_peers = tasks.len();
        }

        let merged = shared.lock().await.have.clone();
        let progressed = any_progress || made_progress.load(std::sync::atomic::Ordering::Relaxed);
        if progressed {
            *have = merged.clone();
            self.update_progress(&merged).await;
            if let Err(e) = self.persist_resume(storage.as_ref(), &merged).await {
                tracing::warn!(error = %e, "parallel resume persist failed");
            }
        }
        {
            let mut s = self.state.lock().await;
            s.active_peers = 0;
            s.peer_scheduler.serial_peer_active = false;
        }
        merge_dynamic_parallel_candidates(
            &mut candidates,
            &mut seen_candidates,
            &mut discovered_pex,
            &pex_peers,
            bad_peers,
            peer_backoff,
            self.allow_ipv6,
        )
        .await;
        (progressed, discovered_pex)
    }
}

pub(super) async fn merge_dynamic_parallel_candidates(
    candidates: &mut Vec<PeerAddr>,
    seen: &mut HashSet<SocketAddr>,
    discovered_pex: &mut Vec<PeerAddr>,
    pex_peers: &Arc<Mutex<Vec<PeerAddr>>>,
    bad_peers: &HashMap<SocketAddr, Instant>,
    peer_backoff: &HashMap<SocketAddr, Instant>,
    allow_ipv6: bool,
) {
    let peers = {
        let mut peers = pex_peers.lock().await;
        std::mem::take(&mut *peers)
    };
    for peer in peers {
        if push_parallel_candidate(candidates, seen, peer, bad_peers, peer_backoff, allow_ipv6) {
            discovered_pex.push(peer);
        }
    }
}

pub(super) fn merge_parallel_candidate_iter<I>(
    candidates: &mut Vec<PeerAddr>,
    seen: &mut HashSet<SocketAddr>,
    peers: I,
    bad_peers: &HashMap<SocketAddr, Instant>,
    peer_backoff: &HashMap<SocketAddr, Instant>,
    allow_ipv6: bool,
) where
    I: IntoIterator<Item = PeerAddr>,
{
    for peer in peers {
        push_parallel_candidate(candidates, seen, peer, bad_peers, peer_backoff, allow_ipv6);
    }
}

pub(super) fn push_parallel_candidate(
    candidates: &mut Vec<PeerAddr>,
    seen: &mut HashSet<SocketAddr>,
    peer: PeerAddr,
    bad_peers: &HashMap<SocketAddr, Instant>,
    peer_backoff: &HashMap<SocketAddr, Instant>,
    allow_ipv6: bool,
) -> bool {
    if !allow_ipv6 && peer.ip.is_ipv6() {
        return false;
    }
    let addr = peer.socket_addr();
    if peer_is_backed_off(bad_peers, addr) || peer_is_backed_off(peer_backoff, addr) {
        return false;
    }
    if !seen.insert(addr) {
        return false;
    }
    candidates.push(peer);
    true
}

pub(super) type PeerReadHalf = tokio::io::ReadHalf<Box<dyn utp::PeerDuplex>>;
pub(super) type PeerWriteHalf = tokio::io::WriteHalf<Box<dyn utp::PeerDuplex>>;

#[derive(Debug, Clone)]
pub(super) struct ParallelPieceState {
    pub(super) have: PieceBitfield,
    pub(super) reserved: HashSet<usize>,
    pub(super) availability: Vec<u16>,
    pub(super) peer_pieces: HashMap<SocketAddr, Bitfield>,
    pub(super) selection: PieceSelection,
}

/// Compute a stable shard offset in `[0, piece_count)` for a peer's
/// piece-reservation search. Hashes the peer's socket address so each peer
/// gets a deterministic but distinct starting point in the piece space,
/// which keeps concurrent workers from all reserving the same low-index
/// pieces first.
pub(super) fn piece_shard(peer_addr: SocketAddr, piece_count: usize) -> usize {
    if piece_count == 0 {
        return 0;
    }
    // FNV-1a over the address bytes: cheap, deterministic, no allocation.
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut hash_byte = |byte: u8| {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    };
    match peer_addr.ip() {
        std::net::IpAddr::V4(ip) => {
            for byte in ip.octets() {
                hash_byte(byte);
            }
        }
        std::net::IpAddr::V6(ip) => {
            for byte in ip.octets() {
                hash_byte(byte);
            }
        }
    }
    for byte in peer_addr.port().to_be_bytes() {
        hash_byte(byte);
    }
    (hash as usize) % piece_count
}

impl ParallelPieceState {
    pub(super) fn new(have: PieceBitfield, piece_count: usize, selection: PieceSelection) -> Self {
        Self {
            have,
            reserved: HashSet::new(),
            availability: vec![0; piece_count],
            peer_pieces: HashMap::new(),
            selection,
        }
    }

    pub(super) fn note_peer_bitfield(
        &mut self,
        peer: SocketAddr,
        bitfield: &Bitfield,
        piece_count: usize,
    ) {
        if let Some(previous) = self.peer_pieces.insert(peer, bitfield.clone()) {
            for i in 0..piece_count {
                if previous.has(i) {
                    self.availability[i] = self.availability[i].saturating_sub(1);
                }
            }
        }
        for i in 0..piece_count {
            if bitfield.has(i) {
                self.availability[i] = self.availability[i].saturating_add(1);
            }
        }
    }

    pub(super) fn note_peer_have(&mut self, peer: SocketAddr, piece: u32, piece_count: usize) {
        let piece = piece as usize;
        if piece >= piece_count {
            return;
        }
        let entry = self
            .peer_pieces
            .entry(peer)
            .or_insert_with(|| Bitfield::new(piece_count));
        if !entry.has(piece) {
            entry.set(piece);
            self.availability[piece] = self.availability[piece].saturating_add(1);
        }
    }

    pub(super) fn remove_peer(&mut self, peer: SocketAddr, piece_count: usize) {
        let Some(previous) = self.peer_pieces.remove(&peer) else {
            return;
        };
        for i in 0..piece_count {
            if previous.has(i) {
                self.availability[i] = self.availability[i].saturating_sub(1);
            }
        }
    }

    pub(super) fn reserve_piece(
        &mut self,
        peer_bf: &Bitfield,
        peer_addr: SocketAddr,
        piece_count: usize,
    ) -> Option<usize> {
        // Spread work across concurrent peer workers by offsetting each peer's
        // search start to a different point in the piece space. Without this,
        // when a peer's piece window is wider than the total number of pieces
        // remaining, a single fast peer monopolises the work and other peers
        // never get a chance to contribute (no useful blocks → marked
        // unhelpful by the engine). The shard index is a stable hash of the
        // peer socket address so each peer gets a deterministic, distinct
        // starting point — peers with identical bitfields (e.g. seeds) still
        // get different shards.
        let shard = piece_shard(peer_addr, piece_count);
        let piece = (0..piece_count)
            .map(|offset| (shard + offset) % piece_count)
            .filter(|&i| {
                self.selection.includes(i)
                    && peer_bf.has(i)
                    && !self.have.has(i)
                    && !self.reserved.contains(&i)
            })
            .min_by_key(|&i| {
                (
                    std::cmp::Reverse(self.selection.priority(i)),
                    self.availability.get(i).copied().unwrap_or(0).max(1),
                )
            })?;
        self.reserved.insert(piece);
        Some(piece)
    }

    pub(super) fn peer_has_missing_piece(&self, peer_bf: &Bitfield, piece_count: usize) -> bool {
        (0..piece_count).any(|i| self.selection.includes(i) && peer_bf.has(i) && !self.have.has(i))
    }

    pub(super) fn release_piece(&mut self, piece: usize) {
        self.reserved.remove(&piece);
    }
}

/// Spawn one normal-mode peer session in the bounded parallel downloader.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_parallel_peer_task(
    tasks: &mut tokio::task::JoinSet<(PeerAddr, Result<PeerSessionOutcome>)>,
    peer_addr: PeerAddr,
    meta: TorrentMeta,
    binder: Arc<dyn NetworkBinder>,
    peer_id: [u8; 20],
    shared: Arc<Mutex<ParallelPieceState>>,
    storage: Arc<StorageIo>,
    state: Arc<Mutex<EngineState>>,
    deadline: Instant,
    made_progress: Arc<std::sync::atomic::AtomicBool>,
    pex_peers: Arc<Mutex<Vec<PeerAddr>>>,
    limiter: ShapedLimiter,
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    pex_enabled: bool,
    allow_ipv6: bool,
    pex_max_peers: usize,
    candidate_count: usize,
    peer_session_budget: PeerSessionBudget,
) {
    tasks.spawn(async move {
        let result = parallel_peer_session(
            binder,
            peer_addr,
            meta,
            peer_id,
            shared,
            storage,
            state,
            deadline,
            made_progress,
            pex_peers,
            limiter,
            utp_enabled,
            utp_prefer_tcp,
            encryption_mode,
            pex_enabled,
            allow_ipv6,
            pex_max_peers,
            candidate_count,
            peer_session_budget,
        )
        .await;
        (peer_addr, result)
    });
}

pub(super) struct ParallelPieceDownload {
    pub(super) piece_index: usize,
    reqs: Vec<(u32, u32)>,
    next_req: usize,
    pub(super) in_flight: usize,
    pub(super) outstanding_blocks: HashMap<u32, u32>,
    assembler: peer::PieceAssembler,
}

impl ParallelPieceDownload {
    pub(super) fn new(piece_index: usize, piece_len: u32) -> Self {
        Self {
            piece_index,
            reqs: block_requests(piece_len),
            next_req: 0,
            in_flight: 0,
            outstanding_blocks: HashMap::new(),
            assembler: peer::PieceAssembler::new(piece_index as u32, piece_len as usize),
        }
    }

    pub(super) async fn send_more<W>(
        &mut self,
        write_half: &mut W,
        global_in_flight: &mut usize,
        request_budget: usize,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        while self.next_req < self.reqs.len() && *global_in_flight < request_budget {
            let (offset, length) = self.reqs[self.next_req];
            peer::write_message(
                write_half,
                &Message::Request {
                    piece: self.piece_index as u32,
                    offset,
                    length,
                },
            )
            .await?;
            self.next_req += 1;
            self.in_flight += 1;
            self.outstanding_blocks.insert(offset, length);
            *global_in_flight += 1;
        }
        Ok(())
    }

    pub(super) fn record_block(
        &mut self,
        offset: u32,
        block: &[u8],
        global_in_flight: &mut usize,
    ) -> Result<Option<bool>> {
        let Some(expected_len) = self.outstanding_blocks.get(&offset).copied() else {
            return Ok(None);
        };
        if block.len() != expected_len as usize {
            return Ok(None);
        }
        let complete = self.assembler.add_block(offset, block)?;
        self.outstanding_blocks.remove(&offset);
        self.in_flight = self.in_flight.saturating_sub(1);
        *global_in_flight = (*global_in_flight).saturating_sub(1);
        Ok(Some(complete))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerSessionOutcome {
    Progressed,
    NoProgress,
    NoWorkAvailable,
}

#[derive(Debug, Clone)]
pub(super) struct PeerRequestWindow {
    cap: usize,
    smoothed_rate_bps: u64,
    pub(super) sample_bytes: u64,
    pub(super) sample_started_at: Instant,
}

impl PeerRequestWindow {
    pub(super) fn new(remote_reqq: Option<usize>, now: Instant) -> Self {
        let cap = remote_reqq
            .filter(|cap| *cap > 0)
            .unwrap_or(NORMAL_REQUEST_FALLBACK_CAP)
            .clamp(1, NORMAL_REQUEST_LOCAL_CAP);
        Self {
            cap,
            smoothed_rate_bps: 0,
            sample_bytes: 0,
            sample_started_at: now,
        }
    }

    pub(super) fn set_remote_reqq(&mut self, remote_reqq: Option<usize>) {
        let Some(remote_reqq) = remote_reqq.filter(|cap| *cap > 0) else {
            return;
        };
        self.cap = remote_reqq.clamp(1, NORMAL_REQUEST_LOCAL_CAP);
    }

    pub(super) fn record_block(&mut self, bytes: u64, now: Instant) {
        self.sample_bytes = self.sample_bytes.saturating_add(bytes);
        let elapsed = now.saturating_duration_since(self.sample_started_at);
        if elapsed < Duration::from_millis(500) {
            return;
        }
        let secs = elapsed.as_secs_f64();
        let instantaneous = ((self.sample_bytes as f64) / secs) as u64;
        self.smoothed_rate_bps = if self.smoothed_rate_bps == 0 {
            instantaneous
        } else {
            ((self.smoothed_rate_bps as f64 * 0.65) + (instantaneous as f64 * 0.35)) as u64
        };
        self.sample_bytes = 0;
        self.sample_started_at = now;
    }

    pub(super) fn desired_in_flight(&self) -> usize {
        let floor = NORMAL_REQUEST_FLOOR.min(self.cap);
        let estimated = ((self
            .smoothed_rate_bps
            .saturating_mul(NORMAL_REQUEST_TARGET_BUFFER_SECS))
            / peer::BLOCK_SIZE as u64) as usize;
        estimated.max(floor).min(self.cap)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn fill_parallel_piece_window<W>(
    write_half: &mut W,
    downloads: &mut HashMap<usize, ParallelPieceDownload>,
    global_in_flight: &mut usize,
    shared: &Arc<Mutex<ParallelPieceState>>,
    peer_bf: &Bitfield,
    peer_addr: SocketAddr,
    meta: &TorrentMeta,
    piece_count: usize,
    request_budget: usize,
    candidate_count: usize,
) -> Result<bool>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut reserved_any = false;
    // Cap the per-peer reservation count at min(NORMAL_PEER_PIECE_WINDOW,
    // ceil(remaining_pieces / active_session_count)). The active session count
    // is the bounded number of peer sessions sharing this round's piece pool. With
    // a wide per-peer window and a small piece count, reserving the full
    // window monopolises all pieces for one peer; dividing the available
    // work by the candidate count keeps fairness across peers.
    let remaining_pieces = {
        let work = shared.lock().await;
        let mut count = 0usize;
        for i in 0..piece_count {
            if work.selection.includes(i)
                && peer_bf.has(i)
                && !work.have.has(i)
                && !work.reserved.contains(&i)
            {
                count += 1;
            }
        }
        count
    };
    let candidate_share = remaining_pieces.div_ceil(candidate_count.max(1));
    let max_for_this_session = NORMAL_PEER_PIECE_WINDOW.min(candidate_share);
    while downloads.len() < max_for_this_session && *global_in_flight < request_budget {
        let Some(piece_index) = ({
            let mut work = shared.lock().await;
            work.reserve_piece(peer_bf, peer_addr, piece_count)
        }) else {
            break;
        };
        let piece_len = meta.piece_length_for_index_u32(piece_index)?;
        let mut download = ParallelPieceDownload::new(piece_index, piece_len);
        if let Err(e) = download
            .send_more(write_half, global_in_flight, request_budget)
            .await
        {
            shared.lock().await.release_piece(piece_index);
            return Err(e);
        }
        downloads.insert(piece_index, download);
        reserved_any = true;
    }
    if reserved_any {
        write_half.flush().await.ok();
    }
    Ok(reserved_any)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn parallel_peer_session(
    binder: Arc<dyn NetworkBinder>,
    peer_addr: PeerAddr,
    meta: TorrentMeta,
    peer_id: [u8; 20],
    shared: Arc<Mutex<ParallelPieceState>>,
    storage: Arc<StorageIo>,
    state: Arc<Mutex<EngineState>>,
    deadline: Instant,
    made_progress: Arc<std::sync::atomic::AtomicBool>,
    pex_peers: Arc<Mutex<Vec<PeerAddr>>>,
    limiter: ShapedLimiter,
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    pex_enabled: bool,
    allow_ipv6: bool,
    pex_max_peers: usize,
    candidate_count: usize,
    peer_session_budget: PeerSessionBudget,
) -> Result<PeerSessionOutcome> {
    if !binder.traffic_allowed() {
        let reason = "transport_blocked";
        tracing::debug!(
            peer = %peer_addr.socket_addr(),
            reason = reason,
            "parallel peer session skipped (no traffic allowed)"
        );
        tracing::trace!(
            peer = %peer_addr.socket_addr(),
            reason = reason,
            "parallel peer session skipped before transport negotiation"
        );
        return Ok(PeerSessionOutcome::NoProgress);
    }
    let _peer_permit = peer_session_budget.acquire_outbound().await?;
    let mut no_progress_reason: &'static str = "session_in_progress";

    let (mut reader, mut write_half, transport) = connect_peer_wire_with_transport(
        binder,
        peer_addr,
        meta.info_hash,
        peer_id,
        utp_enabled,
        utp_prefer_tcp,
        encryption_mode,
    )
    .await?;
    tracing::debug!(
        peer = %peer_addr.socket_addr(),
        transport = transport.as_str(),
        "parallel peer connected"
    );
    record_peer_connected(&state, peer_addr).await;

    let piece_count = meta.piece_count();
    let mut our_bf = Bitfield::new(piece_count);
    {
        let work = shared.lock().await;
        for i in 0..piece_count {
            if work.have.has(i) {
                our_bf.set(i);
            }
        }
    }
    peer::write_message(&mut write_half, &our_bf.encode_message()).await?;
    let extensions = if pex_enabled {
        vec![(swarmotter_core::extensions::UT_PEX_NAME, 1u8)]
    } else {
        Vec::new()
    };
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
    peer::write_message(&mut write_half, &Message::Interested).await?;
    write_half.flush().await.ok();

    let mut peer_bf: Option<Bitfield> = None;
    let mut peer_choking = true;
    let mut progressed = false;
    let mut no_work_available = false;
    let mut remote_pex_id: Option<u8> = None;
    let mut request_window = PeerRequestWindow::new(None, Instant::now());
    let peer_socket = peer_addr.socket_addr();

    loop {
        if Instant::now() > deadline {
            no_progress_reason = "deadline_exceeded";
            break;
        }
        let complete = {
            let work = shared.lock().await;
            work.selection.complete(&work.have)
        };
        if complete {
            no_progress_reason = "torrent_complete";
            break;
        }

        if !peer_choking {
            if let Some(peer_bf_snapshot) = peer_bf.clone() {
                let mut downloads: HashMap<usize, ParallelPieceDownload> = HashMap::new();
                let mut global_in_flight = 0usize;
                let mut session_error = None;
                if let Err(e) = fill_parallel_piece_window(
                    &mut write_half,
                    &mut downloads,
                    &mut global_in_flight,
                    &shared,
                    &peer_bf_snapshot,
                    peer_addr.socket_addr(),
                    &meta,
                    piece_count,
                    request_window.desired_in_flight(),
                    candidate_count,
                )
                .await
                {
                    no_progress_reason = "fill_window_failed";
                    session_error = Some(e);
                }
                if downloads.is_empty() {
                    let has_missing = shared
                        .lock()
                        .await
                        .peer_has_missing_piece(&peer_bf_snapshot, piece_count);
                    if has_missing {
                        no_progress_reason = "peer_has_no_assignable_work";
                        no_work_available = true;
                    } else if let Err(e) =
                        peer::write_message(&mut write_half, &Message::NotInterested).await
                    {
                        no_progress_reason = "send_not_interested_failed";
                        session_error = Some(e);
                    }
                    if let Some(e) = session_error {
                        shared.lock().await.remove_peer(peer_socket, piece_count);
                        return Err(e);
                    }
                    break;
                }

                let mut last_block_at = Instant::now();
                let mut received_any = false;
                while !downloads.is_empty() {
                    let remaining = (last_block_at + Duration::from_secs(20))
                        .saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        no_progress_reason = "piece_window_timeout";
                        break;
                    }
                    let msg = match timeout(remaining, reader.read_message()).await {
                        Ok(Ok(Some(m))) => {
                            no_progress_reason = "awaiting_piece_or_control_message";
                            m
                        }
                        Ok(Ok(None)) => {
                            no_progress_reason = "peer_closed_connection_during_piece_window";
                            break;
                        }
                        Ok(Err(_)) => {
                            no_progress_reason = "peer_message_read_error";
                            break;
                        }
                        Err(_) => {
                            no_progress_reason = "piece_window_idle_timeout";
                            break;
                        }
                    };
                    match msg {
                        Message::Piece {
                            piece,
                            offset,
                            block,
                        } => {
                            let piece_index = piece as usize;
                            let mut complete_data = None;
                            if let Some(download) = downloads.get_mut(&piece_index) {
                                match download.record_block(offset, &block, &mut global_in_flight) {
                                    Ok(Some(complete)) => {
                                        record_peer_block(&state, peer_addr, block.len() as u64)
                                            .await;
                                        let now = Instant::now();
                                        request_window.record_block(block.len() as u64, now);
                                        last_block_at = now;
                                        received_any = true;
                                        no_progress_reason = "piece_downloaded_some_blocks";
                                        if complete {
                                            no_progress_reason =
                                                "piece_download_complete_data_ready";
                                            complete_data =
                                                Some(download.assembler.data().to_vec());
                                        } else if let Err(e) = download
                                            .send_more(
                                                &mut write_half,
                                                &mut global_in_flight,
                                                request_window.desired_in_flight(),
                                            )
                                            .await
                                        {
                                            no_progress_reason = "request_refill_failed";
                                            session_error = Some(e);
                                            break;
                                        } else {
                                            write_half.flush().await.ok();
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        no_progress_reason = "record_block_failed";
                                        session_error = Some(e);
                                        break;
                                    }
                                }
                            }
                            if let Some(data) = complete_data {
                                downloads.remove(&piece_index);
                                if swarmotter_core::storage::verify_piece(&meta, piece_index, &data)
                                {
                                    limiter
                                        .acquire(RateDirection::Download, data.len() as u64)
                                        .await;
                                    if let Err(e) = storage.write_piece(piece_index, &data).await {
                                        shared.lock().await.release_piece(piece_index);
                                        no_progress_reason = "storage_write_piece_failed";
                                        session_error = Some(e);
                                        break;
                                    }
                                    let have_snapshot = {
                                        let mut work = shared.lock().await;
                                        if !work.have.has(piece_index) {
                                            work.have.set(piece_index);
                                            progressed = true;
                                            made_progress
                                                .store(true, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        work.release_piece(piece_index);
                                        work.have.clone()
                                    };
                                    update_progress_state(&state, &meta, &have_snapshot).await;
                                    if let Err(e) = peer::write_message(
                                        &mut write_half,
                                        &Message::Have {
                                            piece: piece_index as u32,
                                        },
                                    )
                                    .await
                                    {
                                        no_progress_reason = "send_have_failed";
                                        session_error = Some(e);
                                        break;
                                    }
                                    if let Err(e) = fill_parallel_piece_window(
                                        &mut write_half,
                                        &mut downloads,
                                        &mut global_in_flight,
                                        &shared,
                                        peer_bf.as_ref().unwrap_or(&peer_bf_snapshot),
                                        peer_addr.socket_addr(),
                                        &meta,
                                        piece_count,
                                        request_window.desired_in_flight(),
                                        candidate_count,
                                    )
                                    .await
                                    {
                                        no_progress_reason = "fill_window_failed";
                                        session_error = Some(e);
                                        break;
                                    }
                                } else {
                                    tracing::warn!(
                                        piece = piece_index,
                                        "piece hash mismatch; rejecting"
                                    );
                                    record_peer_hash_failure(&state, peer_addr).await;
                                    no_progress_reason = "piece_hash_mismatch";
                                    shared.lock().await.release_piece(piece_index);
                                }
                            }
                        }
                        Message::Choke => {
                            no_progress_reason = "peer_choked_us";
                            peer_choking = true;
                            record_peer_choked(&state, peer_addr).await;
                            break;
                        }
                        Message::Unchoke => {
                            no_progress_reason = "peer_unchoked_us";
                            peer_choking = false;
                            record_peer_unchoked(&state, peer_addr).await;
                        }
                        Message::Have { piece } => {
                            no_progress_reason = "peer_sent_have";
                            apply_peer_have(&mut peer_bf, piece_count, piece);
                            shared
                                .lock()
                                .await
                                .note_peer_have(peer_socket, piece, piece_count);
                            if let Some(bf) = &peer_bf {
                                let have = shared.lock().await.have.clone();
                                record_peer_availability(&state, peer_addr, bf, &have, piece_count)
                                    .await;
                            }
                        }
                        Message::Bitfield { bits } => {
                            no_progress_reason = "peer_sent_bitfield";
                            let bf = Bitfield::from_bytes(bits, piece_count);
                            shared
                                .lock()
                                .await
                                .note_peer_bitfield(peer_socket, &bf, piece_count);
                            let have = shared.lock().await.have.clone();
                            record_peer_availability(&state, peer_addr, &bf, &have, piece_count)
                                .await;
                            peer_bf = Some(bf);
                        }
                        Message::Extended { id, payload } => {
                            no_progress_reason = "parallel_pex_message";
                            handle_parallel_pex_message(
                                id,
                                &payload,
                                pex_enabled,
                                &mut remote_pex_id,
                                allow_ipv6,
                                pex_max_peers,
                                &pex_peers,
                                &state,
                                &mut request_window,
                            )
                            .await;
                        }
                        Message::Keepalive
                        | Message::Interested
                        | Message::NotInterested
                        | Message::Request { .. }
                        | Message::Cancel { .. }
                        | Message::Unknown { .. } => {}
                    }
                }

                for piece_index in downloads.keys().copied().collect::<Vec<_>>() {
                    shared.lock().await.release_piece(piece_index);
                }
                if let Some(e) = session_error {
                    shared.lock().await.remove_peer(peer_socket, piece_count);
                    return Err(e);
                }
                if !downloads.is_empty() {
                    no_progress_reason = "piece_window_not_drained";
                    record_peer_timeout(&state, peer_addr).await;
                }
                if !received_any {
                    no_progress_reason = "no_blocks_received_in_window";
                    break;
                }
                continue;
            }
        }

        let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
            Ok(Ok(Some(m))) => {
                no_progress_reason = "awaiting_state_transition";
                m
            }
            Ok(Ok(None)) => {
                no_progress_reason = "peer_closed_connection_waiting_state";
                break;
            }
            Ok(Err(_)) => {
                no_progress_reason = "peer_message_read_error";
                break;
            }
            Err(_) => {
                no_progress_reason = "state_wait_timeout";
                break;
            }
        };
        match msg {
            Message::Unchoke => {
                no_progress_reason = "peer_unchoked_us";
                peer_choking = false;
                record_peer_unchoked(&state, peer_addr).await;
            }
            Message::Choke => {
                no_progress_reason = "peer_choked_us";
                peer_choking = true;
                record_peer_choked(&state, peer_addr).await;
            }
            Message::Bitfield { bits } => {
                no_progress_reason = "peer_sent_bitfield";
                let bf = Bitfield::from_bytes(bits, piece_count);
                shared
                    .lock()
                    .await
                    .note_peer_bitfield(peer_socket, &bf, piece_count);
                let have = shared.lock().await.have.clone();
                record_peer_availability(&state, peer_addr, &bf, &have, piece_count).await;
                peer_bf = Some(bf);
            }
            Message::Have { piece } => {
                no_progress_reason = "peer_sent_have";
                apply_peer_have(&mut peer_bf, piece_count, piece);
                shared
                    .lock()
                    .await
                    .note_peer_have(peer_socket, piece, piece_count);
                if let Some(bf) = &peer_bf {
                    let have = shared.lock().await.have.clone();
                    record_peer_availability(&state, peer_addr, bf, &have, piece_count).await;
                }
            }
            Message::Extended { id, payload } => {
                no_progress_reason = "parallel_pex_message";
                handle_parallel_pex_message(
                    id,
                    &payload,
                    pex_enabled,
                    &mut remote_pex_id,
                    allow_ipv6,
                    pex_max_peers,
                    &pex_peers,
                    &state,
                    &mut request_window,
                )
                .await;
            }
            Message::Keepalive
            | Message::Interested
            | Message::NotInterested
            | Message::Request { .. }
            | Message::Piece { .. }
            | Message::Cancel { .. }
            | Message::Unknown { .. } => {}
        }
    }

    shared.lock().await.remove_peer(peer_socket, piece_count);
    if no_progress_reason == "session_in_progress" {
        no_progress_reason = if no_work_available {
            "no_work_available"
        } else {
            "session_ended_without_terminal_reason"
        };
    }
    let outcome = if progressed {
        PeerSessionOutcome::Progressed
    } else if no_work_available {
        PeerSessionOutcome::NoWorkAvailable
    } else {
        PeerSessionOutcome::NoProgress
    };
    match outcome {
        PeerSessionOutcome::Progressed => {}
        PeerSessionOutcome::NoWorkAvailable => {
            tracing::debug!(
                peer = %peer_addr.socket_addr(),
                reason = no_progress_reason,
                "parallel peer session had no immediate in-session work"
            );
        }
        PeerSessionOutcome::NoProgress => {
            tracing::debug!(
                peer = %peer_addr.socket_addr(),
                reason = no_progress_reason,
                "parallel peer session ended without progress"
            );
        }
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_parallel_pex_message(
    id: u8,
    payload: &[u8],
    pex_enabled: bool,
    remote_pex_id: &mut Option<u8>,
    allow_ipv6: bool,
    pex_max_peers: usize,
    pex_peers: &Arc<Mutex<Vec<PeerAddr>>>,
    state: &Arc<Mutex<EngineState>>,
    request_window: &mut PeerRequestWindow,
) {
    if id == swarmotter_core::extensions::EXTENSION_HANDSHAKE_ID {
        if let Ok(hs) = swarmotter_core::extensions::parse_extension_handshake(payload) {
            if pex_enabled {
                *remote_pex_id = hs.id_for(swarmotter_core::extensions::UT_PEX_NAME);
            }
            request_window.set_remote_reqq(hs.reqq.and_then(|reqq| usize::try_from(reqq).ok()));
        }
        return;
    }
    if !pex_enabled {
        return;
    }
    if Some(id) != *remote_pex_id {
        return;
    }
    let Ok(pex) = swarmotter_core::extensions::parse_pex(payload) else {
        return;
    };
    let mut peers = pex_peers.lock().await;
    let before = peers.len();
    add_pex_peers(
        &mut peers,
        pex.added.into_iter().chain(pex.added6),
        allow_ipv6,
        pex_max_peers,
    );
    if peers.len() > before {
        let mut s = state.lock().await;
        s.pex_discovery_ok = true;
        s.pex_last_seen = Some(Instant::now());
    }
}
