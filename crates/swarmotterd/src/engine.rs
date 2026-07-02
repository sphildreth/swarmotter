// SPDX-License-Identifier: Apache-2.0

//! Live torrent data-plane engine.
//!
//! This module implements the real BitTorrent download loop: tracker
//! announce, TCP peer connections through the network containment layer,
//! peer wire handshake and message exchange, piece request scheduling,
//! block assembly, on-disk writes and verification, and fast-resume
//! persistence. Progress is reported through a shared [`EngineState`] that
//! the daemon reconciles into torrent summaries.
//!
//! All torrent networking goes through the [`NetworkBinder`] abstraction; the
//! engine never creates sockets directly. In strict fail-closed mode the
//! binder blocks new connections and the engine moves the torrent to
//! `network_blocked`.
//!
//! See `design/architecture.md`, `design/vpn-network-containment.md`, and
//! ADR-0012 (peer protocol architecture) / ADR-0013 (task/runtime model).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::bandwidth::{RateDirection, RateLimiter, ShapedLimiter};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{
    self, block_requests, Bitfield, Handshake, Message, PeerAddr, PeerReader,
};
use swarmotter_core::storage::resume::PieceBitfield;
use swarmotter_core::storage::StorageIo;
use swarmotter_core::tracker::{self, AnnounceEvent, AnnounceRequest};
use swarmotter_core::udp_tracker;
use swarmotter_core::utp::{self, PeerTransport};

/// Magnet parameters for a torrent that still needs its metadata fetched
/// (BEP 9). The placeholder `TorrentMeta` in the engine has a dummy info hash;
/// these hold the real info hash, name, and trackers so metadata can be
/// fetched and the meta rebuilt.
#[derive(Debug, Clone)]
pub struct MagnetParams {
    pub info_hash: swarmotter_core::hash::InfoHash,
    pub name: String,
    pub trackers: Vec<String>,
}

/// Live engine state, shared between the engine task and the daemon so the
/// API/UI can observe real progress, speeds, peers, and tracker status.
#[derive(Debug, Clone, Default)]
pub struct EngineState {
    pub pieces_have: PieceBitfield,
    pub piece_count: usize,
    pub downloaded: u64,
    pub uploaded: u64,
    pub bytes_completed: u64,
    pub total_length: u64,
    #[allow(dead_code)]
    pub active_peers: usize,
    pub peers: Vec<PeerAddr>,
    pub tracker_ok: bool,
    pub tracker_message: Option<String>,
    pub last_announce: Option<u64>,
    pub finished: bool,
    /// For magnets: the real metadata once fetched via BEP 9, so the daemon
    /// can replace the placeholder torrent record.
    pub resolved_meta: Option<TorrentMeta>,
}

/// Commands sent to an engine task to control its lifecycle.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum EngineCommand {
    Pause,
    Resume,
    Reannounce,
    Recheck,
    Stop,
}

/// Run a torrent download to completion (or until stopped).
/// `binder` is the contained network path. `seed_peers` are peer addresses to
/// connect to directly (used by the local swarm test and by PEX/DHT once
/// those are live); tracker announce runs in parallel to discover more.
/// `state` is updated as progress is made and is read by the daemon to build
/// torrent summaries. `commands` receives lifecycle commands; `shutdown`
/// completes when the engine should terminate (remove).
pub struct TorrentEngine {
    meta: TorrentMeta,
    download_dir: PathBuf,
    peer_id: [u8; 20],
    binder: Arc<dyn NetworkBinder>,
    state: Arc<Mutex<EngineState>>,
    commands: Arc<Mutex<tokio::sync::mpsc::Receiver<EngineCommand>>>,
    seed_peers: Vec<PeerAddr>,
    listen_port: u16,
    limiter: ShapedLimiter,
    magnet: Option<MagnetParams>,
    /// Optional DHT runner for trackerless peer discovery (disabled for
    /// private torrents).
    dht: Option<Arc<crate::dht::DhtRunner>>,
    /// Peer transport selection: whether uTP is enabled and whether TCP is
    /// preferred over uTP. All transports go through the contained binder.
    utp_enabled: bool,
    utp_prefer_tcp: bool,
}

impl TorrentEngine {
    #[allow(clippy::too_many_arguments, dead_code)]
    pub fn new(
        meta: TorrentMeta,
        download_dir: PathBuf,
        peer_id: [u8; 20],
        binder: Arc<dyn NetworkBinder>,
        state: Arc<Mutex<EngineState>>,
        commands: tokio::sync::mpsc::Receiver<EngineCommand>,
        seed_peers: Vec<PeerAddr>,
        listen_port: u16,
    ) -> Self {
        Self::with_limiter(
            meta,
            download_dir,
            peer_id,
            binder,
            state,
            commands,
            seed_peers,
            listen_port,
            RateLimiter::unlimited(),
            None,
        )
    }

    /// Like [`new`] but with an explicit live rate limiter (download/upload
    /// shaping) wired from the daemon's bandwidth config, and optional magnet
    /// parameters for BEP 9 metadata fetch.
    #[allow(clippy::too_many_arguments)]
    pub fn with_limiter(
        meta: TorrentMeta,
        download_dir: PathBuf,
        peer_id: [u8; 20],
        binder: Arc<dyn NetworkBinder>,
        state: Arc<Mutex<EngineState>>,
        commands: tokio::sync::mpsc::Receiver<EngineCommand>,
        seed_peers: Vec<PeerAddr>,
        listen_port: u16,
        limiter: RateLimiter,
        magnet: Option<MagnetParams>,
    ) -> Self {
        Self {
            meta,
            download_dir,
            peer_id,
            binder,
            state,
            commands: Arc::new(Mutex::new(commands)),
            seed_peers,
            listen_port,
            limiter: ShapedLimiter::from_rate_limiter(limiter),
            magnet,
            dht: None,
            utp_enabled: true,
            utp_prefer_tcp: true,
        }
    }

    /// Attach a shared global rate limiter (the daemon's process-wide download/
    /// upload cap) so transfers are shaped by both the per-torrent and the
    /// global limits.
    #[allow(dead_code)]
    pub fn with_global_limiter(mut self, global: Option<RateLimiter>) -> Self {
        if let Some(g) = global {
            self.limiter = self.limiter.with_global(g);
        }
        self
    }

    /// Attach a DHT runner for trackerless peer discovery (ignored for private
    /// torrents).
    pub fn with_dht(mut self, dht: Arc<crate::dht::DhtRunner>) -> Self {
        self.dht = Some(dht);
        self
    }

    /// Configure peer transport selection. When uTP is enabled, the engine
    /// attempts uTP (with the non-preferred transport as a fallback); when
    /// disabled, only TCP is used. All transports stay on the contained path.
    pub fn with_transport(mut self, utp_enabled: bool, utp_prefer_tcp: bool) -> Self {
        self.utp_enabled = utp_enabled;
        self.utp_prefer_tcp = utp_prefer_tcp;
        self
    }

    /// Main engine loop. Runs announce + peer download until complete or
    /// commanded to stop. Returns the final engine state.
    pub async fn run(mut self) -> Result<EngineState> {
        // If this is a magnet (no real metadata yet), fetch the `info` dict
        // from a peer via BEP 9 before downloading. The real info hash,
        // name, and trackers come from the magnet parameters.
        if let Some(magnet) = self.magnet.clone() {
            self.state.lock().await.tracker_message = Some("fetching metadata via BEP 9".into());
            let info = self.fetch_magnet_metadata(&magnet).await?;
            let rebuilt =
                crate::metadata::build_meta_from_info(&info, &magnet.name, &magnet.trackers)?;
            // Stash the real metadata so the daemon can update the record.
            self.state.lock().await.resolved_meta = Some(rebuilt.clone());
            // Replace the placeholder meta with the real one.
            self.meta = rebuilt;
        }

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

        let storage = StorageIo::new(self.meta.clone(), self.download_dir.clone());
        storage.preallocate().await?;

        // Load fast resume if present; otherwise recheck what's already on disk.
        let mut have = if let Some(resume) = storage.load_resume(&self.meta.info_hash).await? {
            resume.piece_bitfield
        } else {
            storage.recheck().await?
        };
        self.update_progress(&have).await;

        if have.count(piece_count) == piece_count {
            self.mark_finished().await;
            self.persist_resume(&storage, &have).await?;
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
        // Trackerless / supplemental DHT discovery: for non-private torrents,
        // ask the DHT for peers holding this info hash. Bounded by a hard
        // total cap so unreachable bootstrap nodes cannot stall the download.
        if !self.meta.is_private() {
            if let Some(dht) = &self.dht {
                let dht_result = tokio::time::timeout(
                    Duration::from_secs(10),
                    dht.get_peers(self.meta.info_hash, 3),
                )
                .await;
                if let Ok(Ok(peers)) = dht_result {
                    for p in peers {
                        if !discovered.contains(&p) {
                            discovered.push(p);
                        }
                    }
                }
            }
        }
        self.state.lock().await.peers = discovered.clone();

        // Download loop: connect to peers, request missing pieces, write and
        // verify. Bounded to a small number of concurrent peers.
        let max_concurrent = 4usize;
        let mut bad_peers: HashSet<SocketAddr> = HashSet::new();
        let start = Instant::now();
        // Bounded consecutive no-peer rounds: if we never discover any peers
        // after a bounded number of announce attempts, give up gracefully
        // rather than looping forever. This handles trackerless torrents with
        // no seed peers and no DHT result without hanging the engine.
        const NO_PEER_ROUNDS_MAX: u32 = 5;
        let mut no_peer_rounds: u32 = 0;

        loop {
            // Handle pending commands.
            if self.poll_commands().await == CommandOutcome::Stop {
                break;
            }

            if have.count(piece_count) == piece_count {
                self.mark_finished().await;
                self.persist_resume(&storage, &have).await?;
                // Announce completion to trackers.
                self.announce(AnnounceEvent::Completed).await;
                break;
            }

            // Periodically re-announce to refresh peers.
            if start.elapsed() > Duration::from_secs(30) {
                let refreshed = self.announce(AnnounceEvent::Empty).await;
                for p in refreshed {
                    if !discovered.contains(&p) {
                        discovered.push(p);
                    }
                }
            }

            let remaining = piece_count - have.count(piece_count);

            // Endgame mode: when few pieces remain, request the remaining
            // blocks from multiple peers concurrently and cancel duplicates
            // as they complete. Falls back to the normal sequential path when
            // endgame is inactive or there are too few usable peers.
            if swarmotter_core::endgame::is_endgame(remaining) {
                let candidates: Vec<PeerAddr> = discovered
                    .iter()
                    .filter(|p| !bad_peers.contains(&p.socket_addr()))
                    .copied()
                    .take(max_concurrent)
                    .collect();
                if !candidates.is_empty() {
                    let progressed = self
                        .run_endgame(&candidates, &storage, &mut have, &mut bad_peers)
                        .await;
                    if progressed || have.count(piece_count) == piece_count {
                        continue;
                    }
                }
            }

            // Try peers until we make progress or exhaust the list.
            let mut made_progress = false;
            let mut to_try: Vec<PeerAddr> = discovered
                .iter()
                .filter(|p| !bad_peers.contains(&p.socket_addr()))
                .copied()
                .take(max_concurrent * 2)
                .collect();

            while let Some(peer_addr) = to_try.pop() {
                if have.count(piece_count) == piece_count {
                    break;
                }
                match self
                    .download_from_peer(
                        &peer_addr,
                        &storage,
                        &mut have,
                        &mut bad_peers,
                        &mut discovered,
                    )
                    .await
                {
                    Ok(progressed) => {
                        if progressed {
                            made_progress = true;
                        }
                    }
                    Err(e) => {
                        tracing::debug!(peer = %peer_addr.socket_addr(), error = %e, "peer failed; suppressing");
                        bad_peers.insert(peer_addr.socket_addr());
                    }
                }
            }

            if !made_progress {
                if discovered.is_empty() || bad_peers.len() >= discovered.len() {
                    // No usable peers; back off briefly and retry announce.
                    self.sleep_or_stop(Duration::from_secs(2)).await;
                    let refreshed = self.announce(AnnounceEvent::Empty).await;
                    for p in refreshed {
                        if !discovered.contains(&p) {
                            discovered.push(p);
                        }
                    }
                    if discovered.is_empty() {
                        no_peer_rounds = no_peer_rounds.saturating_add(1);
                        self.state.lock().await.tracker_message =
                            Some("no peers discovered".into());
                        // Bounded give-up: a torrent that never discovers peers
                        // (no tracker, no seed peers, no DHT result) cannot
                        // progress. Stop the engine so the daemon/test does not
                        // hang; the torrent remains incomplete and the user can
                        // add trackers or seed peers and re-start it.
                        if no_peer_rounds >= NO_PEER_ROUNDS_MAX {
                            tracing::info!(
                                info_hash = %self.meta.info_hash,
                                "stopping engine: no peers discovered after bounded retries"
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

    /// Open a peer byte stream with transport selection. Tries the preferred
    /// transport first, then falls back to the other if it is available and
    /// the preferred fails. Returns the connected duplex stream and the
    /// transport that succeeded. All connections go through the binder; in
    /// strict fail-closed mode both return `NetworkBlocked`.
    async fn connect_peer(
        &self,
        peer_addr: &PeerAddr,
    ) -> Result<(Box<dyn utp::PeerDuplex>, PeerTransport)> {
        let addr = peer_addr.socket_addr();
        if self.utp_enabled {
            let (first, second) = if self.utp_prefer_tcp {
                (PeerTransport::Tcp, PeerTransport::Utp)
            } else {
                (PeerTransport::Utp, PeerTransport::Tcp)
            };
            match utp::connect_peer_stream(self.binder.clone(), first, addr).await {
                Ok(s) => return Ok(s),
                Err(e) => {
                    tracing::debug!(peer = %addr, transport = first.as_str(), error = %e, "preferred transport failed; trying fallback")
                }
            }
            return utp::connect_peer_stream(self.binder.clone(), second, addr).await;
        }
        utp::connect_peer_stream(self.binder.clone(), PeerTransport::Tcp, addr).await
    }

    /// Attempt to download missing pieces from a single peer. Returns true if
    /// at least one new piece was verified and written.
    async fn download_from_peer(
        &self,
        peer_addr: &PeerAddr,
        storage: &StorageIo,
        have: &mut PieceBitfield,
        bad_peers: &mut HashSet<SocketAddr>,
        discovered: &mut Vec<PeerAddr>,
    ) -> Result<bool> {
        if !self.binder.traffic_allowed() {
            return Ok(false);
        }
        let (stream, transport) = self.connect_peer(peer_addr).await?;
        tracing::debug!(peer = %peer_addr.socket_addr(), transport = transport.as_str(), "peer connected");
        let (read_half, mut write_half) = tokio::io::split(stream);

        // Handshake. Advertise BEP 10 extension support for PEX/metadata.
        let hs = Handshake {
            info_hash: self.meta.info_hash,
            peer_id: self.peer_id,
            reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
        };
        peer::write_handshake(&mut write_half, &hs).await?;
        let mut reader = PeerReader::new(read_half);
        let their_hs = timeout(Duration::from_secs(10), reader.read_handshake()).await??;
        if their_hs.info_hash != self.meta.info_hash {
            bad_peers.insert(peer_addr.socket_addr());
            return Err(CoreError::Internal(
                "peer handshake info hash mismatch".into(),
            ));
        }

        // Exchange bitfields.
        let mut our_bf = Bitfield::new(self.meta.piece_count());
        for i in 0..self.meta.piece_count() {
            if have.has(i) {
                our_bf.set(i);
            }
        }
        peer::write_message(&mut write_half, &our_bf.encode_message()).await?;
        write_half.flush().await.ok();

        // Send a BEP 10 extension handshake advertising ut_pex (and
        // ut_metadata for the magnet metadata path). PEX is honored only for
        // non-private torrents; private torrents skip PEX entirely.
        let local_pex_id: u8 = 1u8;
        let local_metadata_id: u8 = 2u8;
        let ext_payload = swarmotter_core::extensions::encode_extension_handshake(
            &[
                (swarmotter_core::extensions::UT_PEX_NAME, local_pex_id),
                (
                    swarmotter_core::extensions::UT_METADATA_NAME,
                    local_metadata_id,
                ),
            ],
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

        // Drive a small download loop: pick a missing piece the peer has,
        // request its blocks, assemble, verify, write.
        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            if Instant::now() > deadline {
                break;
            }
            if have.count(piece_count) == piece_count {
                break;
            }

            // If unchoked and we have a candidate piece, request blocks.
            if !peer_choking {
                if let Some(piece_index) = self.pick_piece(peer_bf.as_ref(), have) {
                    let plen = self.piece_length(piece_index) as u32;
                    let reqs = block_requests(plen);
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
                            break;
                        }
                        let msg = match timeout(remaining, reader.read_message()).await {
                            Ok(Ok(Some(m))) => m,
                            Ok(Ok(None)) => break,
                            Ok(Err(_)) => break,
                            Err(_) => break,
                        };
                        match msg {
                            Message::Piece {
                                piece,
                                offset,
                                block,
                            } => {
                                if piece as usize == piece_index
                                    && assembler.add_block(offset, &block).is_ok()
                                {
                                    received_blocks += 1;
                                }
                            }
                            Message::Choke => {
                                peer_choking = true;
                                break;
                            }
                            Message::Unchoke => peer_choking = false,
                            Message::Have { piece } => {
                                if let Some(bf) = &mut peer_bf {
                                    bf.set(piece as usize);
                                }
                            }
                            Message::Bitfield { bits } => {
                                peer_bf = Some(Bitfield::from_bytes(bits, piece_count));
                            }
                            Message::Keepalive
                            | Message::Interested
                            | Message::NotInterested
                            | Message::Request { .. }
                            | Message::Cancel { .. }
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
                            storage.write_block(piece_index, 0, &data).await?;
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
                            // Bad hash: do not mark; try a different piece.
                        }
                    }
                    continue;
                } else {
                    // No missing piece this peer has; not interesting.
                    peer::write_message(&mut write_half, &Message::NotInterested).await?;
                    break;
                }
            }

            // Wait for unchoke / bitfield / have.
            let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
                Ok(Ok(Some(m))) => m,
                _ => break,
            };
            match msg {
                Message::Unchoke => peer_choking = false,
                Message::Choke => peer_choking = true,
                Message::Bitfield { bits } => {
                    peer_bf = Some(Bitfield::from_bytes(bits, piece_count));
                }
                Message::Have { piece } => {
                    if let Some(bf) = &mut peer_bf {
                        bf.set(piece as usize);
                    }
                }
                Message::Keepalive
                | Message::Interested
                | Message::NotInterested
                | Message::Request { .. }
                | Message::Piece { .. }
                | Message::Cancel { .. }
                | Message::Unknown { .. } => {}
                Message::Extended { id, payload } => {
                    // BEP 10 extension: handshake (id 0) or a PEX message.
                    if id == swarmotter_core::extensions::EXTENSION_HANDSHAKE_ID {
                        if let Ok(hs) =
                            swarmotter_core::extensions::parse_extension_handshake(&payload)
                        {
                            remote_pex_id = hs.id_for(swarmotter_core::extensions::UT_PEX_NAME);
                        }
                    } else if Some(id) == remote_pex_id && !self.meta.is_private() {
                        if let Ok(pex) = swarmotter_core::extensions::parse_pex(&payload) {
                            for p in pex.added {
                                if !discovered.contains(&p) {
                                    discovered.push(p);
                                }
                            }
                            for p in pex.added6 {
                                if !discovered.contains(&p) {
                                    discovered.push(p);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(made_progress)
    }

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
    async fn run_endgame(
        &self,
        candidates: &[PeerAddr],
        storage: &StorageIo,
        have: &mut PieceBitfield,
        bad_peers: &mut HashSet<SocketAddr>,
    ) -> bool {
        use swarmotter_core::endgame::{is_endgame, OutstandingRequests};
        const ENDGAME_MAX_PEERS: usize = 4;
        const ENDGAME_STEP_DEADLINE: Duration = Duration::from_secs(30);

        let piece_count = self.meta.piece_count();
        let shared_have = Arc::new(Mutex::new(have.clone()));
        let outstanding = Arc::new(Mutex::new(OutstandingRequests::new(ENDGAME_MAX_PEERS)));
        let made_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let download_dir = self.download_dir.clone();

        let peers: Vec<PeerAddr> = candidates.iter().take(ENDGAME_MAX_PEERS).copied().collect();
        let mut handles = Vec::new();
        let deadline = Instant::now() + ENDGAME_STEP_DEADLINE;
        for peer_addr in peers {
            let meta = self.meta.clone();
            let binder = self.binder.clone();
            let peer_id = self.peer_id;
            let shared_have = shared_have.clone();
            let outstanding = outstanding.clone();
            let made_progress = made_progress.clone();
            let download_dir = download_dir.clone();
            let limiter = self.limiter.clone();
            let utp_enabled = self.utp_enabled;
            let utp_prefer_tcp = self.utp_prefer_tcp;
            handles.push(tokio::spawn(async move {
                endgame_peer_session(
                    binder,
                    peer_addr,
                    meta,
                    peer_id,
                    shared_have,
                    outstanding,
                    download_dir,
                    deadline,
                    made_progress,
                    limiter,
                    utp_enabled,
                    utp_prefer_tcp,
                )
                .await
            }));
        }

        // Wait for all endgame peer sessions; record bad peers on failure.
        let mut any_progress = false;
        for (peer_addr, h) in candidates.iter().take(ENDGAME_MAX_PEERS).zip(handles) {
            match h.await {
                Ok(Ok(progressed)) => {
                    if progressed {
                        any_progress = true;
                    }
                }
                Ok(Err(_)) => {
                    bad_peers.insert(peer_addr.socket_addr());
                }
                // Task panic/cancellation: treat as a failed peer.
                Err(_) => {
                    bad_peers.insert(peer_addr.socket_addr());
                }
            }
        }

        // Merge the shared have back into the local copy and persist progress.
        let merged = shared_have.lock().await.clone();
        let progressed = any_progress || made_progress.load(std::sync::atomic::Ordering::Relaxed);
        let _still_endgame = is_endgame(piece_count - merged.count(piece_count));
        if progressed {
            *have = merged.clone();
            self.update_progress(&merged).await;
            if let Err(e) = self.persist_resume(storage, &merged).await {
                tracing::warn!(error = %e, "endgame resume persist failed");
            }
        }
        progressed
    }

    /// Pick a piece we don't have that the peer has.
    fn pick_piece(&self, peer_bf: Option<&Bitfield>, have: &PieceBitfield) -> Option<usize> {
        let peer_bf = peer_bf?;
        (0..self.meta.piece_count()).find(|&i| peer_bf.has(i) && !have.has(i))
    }
    fn piece_length(&self, index: usize) -> u64 {
        if index + 1 == self.meta.piece_count() {
            self.meta.last_piece_length()
        } else {
            self.meta.piece_length
        }
    }

    /// Announce to all HTTP trackers in tier order; return discovered peers.
    async fn announce(&self, event: AnnounceEvent) -> Vec<PeerAddr> {
        let mut all = Vec::new();
        let mut ok = false;
        let mut msg: Option<String> = None;
        let (uploaded, downloaded, left) = {
            let s = self.state.lock().await;
            (
                s.uploaded,
                s.downloaded,
                s.total_length.saturating_sub(s.bytes_completed),
            )
        };
        for tier in tracker::announce_tiers(self.meta.announce.as_deref(), &self.meta.announce_list)
        {
            for url in tier {
                let req = AnnounceRequest {
                    tracker_url: url.clone(),
                    info_hash: self.meta.info_hash,
                    peer_id: self.peer_id,
                    port: self.listen_port,
                    uploaded,
                    downloaded,
                    left,
                    event,
                    numwant: Some(50),
                    compact: true,
                };
                let result = if url.starts_with("udp://") {
                    udp_tracker::udp_announce(self.binder.as_ref(), &req).await
                } else {
                    tracker::http_announce(self.binder.as_ref(), &req).await
                };
                match result {
                    Ok(resp) => {
                        if let Some(fr) = resp.failure_reason {
                            msg = Some(format!("{url}: {fr}"));
                            continue;
                        }
                        ok = true;
                        all.extend(resp.peers);
                    }
                    Err(e) => {
                        msg = Some(format!("{url}: {e}"));
                    }
                }
            }
        }
        let mut s = self.state.lock().await;
        s.tracker_ok = ok;
        s.tracker_message = msg;
        s.last_announce = Some(now_secs());
        all
    }

    /// Fetch magnet metadata via BEP 9. Announces to the magnet's trackers
    /// (using the real info hash) to discover peers, merges directly-supplied
    /// seed peers, then fetches the `info` dict from the candidates. All peer
    /// connections go through the binder.
    async fn fetch_magnet_metadata(&self, magnet: &MagnetParams) -> Result<Vec<u8>> {
        // Build a temporary announce request set against the real info hash
        // using the magnet's trackers. We reuse the engine's announce helper
        // shape but with the magnet info hash by temporarily swapping it in:
        // simpler to announce directly here.
        let mut candidates: Vec<PeerAddr> = Vec::new();
        for tier in tracker::announce_tiers(magnet.trackers.first().map(|s| s.as_str()), &[]) {
            for url in tier {
                let req = AnnounceRequest {
                    tracker_url: url.clone(),
                    info_hash: magnet.info_hash,
                    peer_id: self.peer_id,
                    port: self.listen_port,
                    uploaded: 0,
                    downloaded: 0,
                    left: 0,
                    event: AnnounceEvent::Started,
                    numwant: Some(50),
                    compact: true,
                };
                let result = if url.starts_with("udp://") {
                    udp_tracker::udp_announce(self.binder.as_ref(), &req).await
                } else {
                    tracker::http_announce(self.binder.as_ref(), &req).await
                };
                if let Ok(resp) = result {
                    if resp.failure_reason.is_none() {
                        candidates.extend(resp.peers);
                    }
                }
            }
        }
        // Merge announce-list tiers too.
        if magnet.trackers.len() > 1 {
            let extra = tracker::announce_tiers(None, &[magnet.trackers[1..].to_vec()]);
            for tier in extra {
                for url in tier {
                    let req = AnnounceRequest {
                        tracker_url: url.clone(),
                        info_hash: magnet.info_hash,
                        peer_id: self.peer_id,
                        port: self.listen_port,
                        uploaded: 0,
                        downloaded: 0,
                        left: 0,
                        event: AnnounceEvent::Started,
                        numwant: Some(50),
                        compact: true,
                    };
                    let result = if url.starts_with("udp://") {
                        udp_tracker::udp_announce(self.binder.as_ref(), &req).await
                    } else {
                        tracker::http_announce(self.binder.as_ref(), &req).await
                    };
                    if let Ok(resp) = result {
                        if resp.failure_reason.is_none() {
                            candidates.extend(resp.peers);
                        }
                    }
                }
            }
        }
        for p in &self.seed_peers {
            if !candidates.contains(p) {
                candidates.push(*p);
            }
        }
        // Trackerless magnet fallback: if no trackers/peers, discover via DHT.
        if candidates.is_empty() {
            if let Some(dht) = &self.dht {
                let dht_result = tokio::time::timeout(
                    Duration::from_secs(10),
                    dht.get_peers(magnet.info_hash, 3),
                )
                .await;
                if let Ok(Ok(peers)) = dht_result {
                    candidates.extend(peers);
                }
            }
        }
        self.state.lock().await.peers = candidates.clone();
        if candidates.is_empty() {
            return Err(CoreError::Internal(
                "magnet metadata fetch: no peers discovered".into(),
            ));
        }
        crate::metadata::fetch_metadata_from_candidates(
            self.binder.clone(),
            magnet.info_hash,
            self.peer_id,
            &candidates,
        )
        .await
    }

    async fn update_progress(&self, have: &PieceBitfield) {
        let mut s = self.state.lock().await;
        s.pieces_have = have.clone();
        let completed = (0..s.piece_count)
            .filter(|&i| have.has(i))
            .map(|i| {
                if i + 1 == s.piece_count {
                    self.meta.last_piece_length()
                } else {
                    self.meta.piece_length
                }
            })
            .sum::<u64>();
        s.bytes_completed = completed;
        s.downloaded = completed;
    }

    async fn mark_finished(&self) {
        let mut s = self.state.lock().await;
        s.finished = true;
    }

    async fn persist_resume(&self, storage: &StorageIo, have: &PieceBitfield) -> Result<()> {
        let piece_byte_lengths: Vec<u64> = (0..self.meta.piece_count())
            .map(|i| self.piece_length(i))
            .collect();
        let s = self.state.lock().await;
        let resume = swarmotter_core::storage::io::build_resume(
            self.meta.info_hash,
            self.meta.name.clone(),
            have.clone(),
            self.meta.piece_count(),
            s.downloaded,
            s.uploaded,
            s.total_length,
            Some(self.download_dir.display().to_string()),
            now_secs(),
            if s.finished { Some(now_secs()) } else { None },
            &vec![swarmotter_core::models::torrent::FilePriority::Normal; self.meta.files.len()],
            &piece_byte_lengths,
        );
        drop(s);
        storage.save_resume(&resume).await?;
        Ok(())
    }

    async fn poll_commands(&self) -> CommandOutcome {
        let mut rx = self.commands.lock().await;
        match rx.try_recv() {
            Ok(EngineCommand::Stop) => CommandOutcome::Stop,
            Ok(EngineCommand::Pause) => CommandOutcome::Pause,
            Ok(EngineCommand::Resume) => CommandOutcome::Continue,
            Ok(EngineCommand::Reannounce) => CommandOutcome::Continue,
            Ok(EngineCommand::Recheck) => CommandOutcome::Continue,
            Err(_) => CommandOutcome::Continue,
        }
    }

    async fn sleep_or_stop(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandOutcome {
    Continue,
    Pause,
    Stop,
}

/// A single endgame peer session: connect, handshake, and request the
/// remaining pieces' blocks (bounded by the shared outstanding-request cap),
/// writing and verifying any piece this peer delivers first. Duplicate
/// outstanding requests for a completed piece are cancelled on the
/// connection that receives a now-redundant block.
#[allow(clippy::too_many_arguments)]
async fn endgame_peer_session(
    binder: Arc<dyn NetworkBinder>,
    peer_addr: PeerAddr,
    meta: TorrentMeta,
    peer_id: [u8; 20],
    shared_have: Arc<Mutex<PieceBitfield>>,
    outstanding: Arc<Mutex<swarmotter_core::endgame::OutstandingRequests>>,
    download_dir: PathBuf,
    deadline: Instant,
    made_progress: Arc<std::sync::atomic::AtomicBool>,
    limiter: ShapedLimiter,
    utp_enabled: bool,
    utp_prefer_tcp: bool,
) -> Result<bool> {
    use swarmotter_core::peer::{block_requests, Bitfield, Handshake, Message, PeerReader};
    if !binder.traffic_allowed() {
        return Ok(false);
    }
    let storage = StorageIo::new(meta.clone(), download_dir);
    let addr = peer_addr.socket_addr();
    let stream = if utp_enabled {
        let (first, second) = if utp_prefer_tcp {
            (PeerTransport::Tcp, PeerTransport::Utp)
        } else {
            (PeerTransport::Utp, PeerTransport::Tcp)
        };
        match utp::connect_peer_stream(binder.clone(), first, addr).await {
            Ok((s, _t)) => s,
            Err(_) => {
                utp::connect_peer_stream(binder.clone(), second, addr)
                    .await?
                    .0
            }
        }
    } else {
        utp::connect_peer_stream(binder.clone(), PeerTransport::Tcp, addr)
            .await?
            .0
    };
    let (read_half, mut write_half) = tokio::io::split(stream);

    // Handshake.
    let hs = Handshake {
        info_hash: meta.info_hash,
        peer_id,
        reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
    };
    peer::write_handshake(&mut write_half, &hs).await?;
    let mut reader = PeerReader::new(read_half);
    let their_hs = timeout(Duration::from_secs(10), reader.read_handshake()).await??;
    if their_hs.info_hash != meta.info_hash {
        return Err(CoreError::Internal(
            "peer handshake info hash mismatch".into(),
        ));
    }

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
            have.count(piece_count) == piece_count
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
                (0..piece_count).find(|&i| bf.has(i) && !have.has(i))
            };
            let Some(piece_index) = candidate else {
                // Nothing this peer can give us right now.
                peer::write_message(&mut write_half, &Message::NotInterested).await?;
                break;
            };
            let piece_len = if piece_index + 1 == piece_count {
                meta.last_piece_length()
            } else {
                meta.piece_length
            } as u32;
            let reqs = block_requests(piece_len);
            // Request blocks respecting the duplicate cap.
            let mut sent_any = false;
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
                        if piece as usize == piece_index
                            && assembler.add_block(offset, &block).is_ok()
                        {
                            received += 1;
                            outstanding.lock().await.delivered(piece, offset);
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
                        break;
                    }
                    Message::Unchoke => peer_choking = false,
                    Message::Have { piece } => {
                        if let Some(bf) = &mut peer_bf {
                            bf.set(piece as usize);
                        }
                    }
                    Message::Bitfield { bits } => {
                        peer_bf = Some(Bitfield::from_bytes(bits, piece_count));
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
                        storage.write_block(piece_index, 0, &data).await?;
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
                }
            }
            continue;
        }

        // Wait for unchoke / bitfield / have.
        let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
            Ok(Ok(Some(m))) => m,
            _ => break,
        };
        match msg {
            Message::Unchoke => peer_choking = false,
            Message::Choke => peer_choking = true,
            Message::Bitfield { bits } => {
                peer_bf = Some(Bitfield::from_bytes(bits, piece_count));
            }
            Message::Have { piece } => {
                if let Some(bf) = &mut peer_bf {
                    bf.set(piece as usize);
                }
            }
            _ => {}
        }
    }

    Ok(progressed)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::meta::build_single_file_torrent;

    #[test]
    fn piece_length_last_is_shorter() {
        let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
            PathBuf::from("/tmp"),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        );
        assert_eq!(engine.piece_length(0), 8);
        assert_eq!(engine.piece_length(1), 8);
    }

    #[test]
    fn pick_piece_chooses_missing_peer_has() {
        let bytes =
            build_single_file_torrent("f", b"0123456789abcdef0123456789abcdef", 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
            PathBuf::from("/tmp"),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        );
        let mut peer_bf = Bitfield::new(4);
        peer_bf.set(1);
        peer_bf.set(2);
        let mut have = PieceBitfield::new(4);
        have.set(1);
        let pick = engine.pick_piece(Some(&peer_bf), &have);
        assert_eq!(pick, Some(2));
    }
}
