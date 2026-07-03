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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::bandwidth::{RateDirection, RateLimiter, ShapedLimiter};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::models::peer::EnginePeerHealth;
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{
    self, block_requests, Bitfield, Handshake, Message, PeerAddr, PeerReader,
};
use swarmotter_core::storage::resume::PieceBitfield;
use swarmotter_core::storage::StorageIo;
use swarmotter_core::tracker::{self, AnnounceEvent, AnnounceRequest};
use swarmotter_core::udp_tracker;
use swarmotter_core::utp::{self, PeerTransport};

/// Default simultaneous peer download workers when no per-torrent peer cap is
/// configured. Trackers commonly return far more than 16 usable peers for
/// public Linux distribution torrents, so the default should be high enough to
/// keep several useful peers busy without requiring operator tuning.
pub const DEFAULT_PEER_WORKER_LIMIT: usize = 64;
const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const NORMAL_PEER_SESSION_DEADLINE: Duration = Duration::from_secs(180);
const DHT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const DHT_DISCOVERY_ROUNDS: usize = 6;

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
    /// Bytes received from peers over the network. This intentionally does
    /// not include bytes found by fast-resume or disk recheck.
    pub downloaded: u64,
    pub uploaded: u64,
    /// Verified bytes present on disk, including bytes found by fast-resume
    /// or recheck.
    pub bytes_completed: u64,
    pub total_length: u64,
    #[allow(dead_code)]
    pub active_peers: usize,
    pub peers: Vec<PeerAddr>,
    /// Per-peer telemetry used for health scoring.
    pub peer_health: HashMap<std::net::SocketAddr, EnginePeerHealth>,
    pub tracker_ok: bool,
    pub tracker_message: Option<String>,
    pub last_announce: Option<u64>,
    pub finished: bool,
    /// Recent tracker/announce failures counted across poll windows.
    pub tracker_failures_recent: u32,
    /// Whether DHT discovery succeeded recently.
    pub dht_discovery_ok: bool,
    /// Whether PEX discovery provided peers recently.
    pub pex_discovery_ok: bool,
    /// Number of peer connection attempts that ended in an error.
    pub peer_disconnects_recent: u32,
    /// Number of blocked/invalid blocks encountered since start.
    pub hash_failures: u32,
    /// Number of timeout/bad-response events while downloading blocks.
    pub timeout_failures: u32,
    /// Last time a valid block was successfully validated and written.
    pub last_valid_block: Option<std::time::Instant>,
    /// Timestamp of the latest DHT discovery result.
    pub dht_last_seen: Option<std::time::Instant>,
    /// Timestamp of the latest PEX discovery result.
    pub pex_last_seen: Option<std::time::Instant>,
    /// Timestamp of the latest successful tracker announce.
    pub tracker_last_ok: Option<std::time::Instant>,
    /// Timestamp of the latest successful block receive.
    pub block_last_seen: Option<std::time::Instant>,
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
    UpdatePeerWorkerLimit(usize),
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
    /// Active write directory. For daemon-managed downloads this is the
    /// configured incomplete directory when present.
    download_dir: PathBuf,
    /// Final completed-data directory. This defaults to `download_dir` for
    /// tests and callers that do not configure an incomplete path.
    complete_dir: PathBuf,
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
    preallocate: bool,
    sparse: bool,
    max_peer_workers: Arc<AtomicUsize>,
    allow_ipv6: bool,
    pex_enabled: bool,
    pex_max_peers: usize,
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
            complete_dir: download_dir.clone(),
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
            preallocate: true,
            sparse: true,
            max_peer_workers: Arc::new(AtomicUsize::new(DEFAULT_PEER_WORKER_LIMIT)),
            allow_ipv6: true,
            pex_enabled: true,
            pex_max_peers: 0,
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

    /// Configure whether storage files are preallocated before download.
    pub fn with_preallocate(mut self, preallocate: bool) -> Self {
        self.preallocate = preallocate;
        self
    }

    /// Configure sparse-file behavior. When sparse is disabled, active files
    /// are sized up front even if full preallocation is disabled.
    pub fn with_sparse(mut self, sparse: bool) -> Self {
        self.sparse = sparse;
        self
    }

    /// Configure the maximum simultaneous peer download workers. A value of 0
    /// means no operator cap was configured, so the engine uses its operational
    /// default.
    pub fn with_peer_worker_limit(self, max_peer_workers: usize) -> Self {
        self.set_peer_worker_limit(max_peer_workers);
        self
    }

    /// Configure whether IPv6 peer addresses are eligible for torrent
    /// connections.
    pub fn with_allow_ipv6(mut self, allow_ipv6: bool) -> Self {
        self.allow_ipv6 = allow_ipv6;
        self
    }

    /// Configure PEX discovery. `max_peers = 0` means no PEX import cap.
    pub fn with_pex(mut self, enabled: bool, max_peers: usize) -> Self {
        self.pex_enabled = enabled;
        self.pex_max_peers = max_peers;
        self
    }

    fn set_peer_worker_limit(&self, max_peer_workers: usize) {
        let limit = if max_peer_workers == 0 {
            DEFAULT_PEER_WORKER_LIMIT
        } else {
            max_peer_workers
        };
        self.max_peer_workers.store(limit.max(1), Ordering::Relaxed);
    }

    fn current_peer_worker_limit(&self) -> usize {
        self.max_peer_workers.load(Ordering::Relaxed).max(1)
    }

    /// Configure the final completed-data directory. The engine writes active
    /// pieces under `download_dir` and atomically moves verified completed data
    /// here before marking the torrent finished.
    pub fn with_complete_dir(mut self, complete_dir: PathBuf) -> Self {
        self.complete_dir = complete_dir;
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

        let complete_storage = StorageIo::new(self.meta.clone(), self.complete_dir.clone());
        if self.download_dir != self.complete_dir {
            let complete_have = self.load_or_recheck(&complete_storage).await?;
            if complete_have.count(piece_count) == piece_count {
                self.update_progress(&complete_have).await;
                self.finish_without_resume(&complete_storage).await?;
                return Ok(self.state.lock().await.clone());
            }
        }

        let storage = StorageIo::new(self.meta.clone(), self.download_dir.clone());
        if self.preallocate || !self.sparse {
            storage.preallocate().await?;
        } else {
            storage.ensure_active_layout().await?;
        }

        // Load fast resume if present; otherwise recheck what's already on disk.
        let mut have = self.load_or_recheck(&storage).await?;
        self.update_progress(&have).await;

        if have.count(piece_count) == piece_count {
            let storage = self.complete_storage(&storage).await?;
            self.finish_without_resume(&storage).await?;
            return Ok(self.state.lock().await.clone());
        }

        // Discover peers via tracker announce (HTTP/UDP) on each tier.
        let mut discovered = self.filter_allowed_peers(self.announce(AnnounceEvent::Started).await);
        // Merge any directly-supplied seed peers (local swarm / PEX / DHT).
        for p in &self.seed_peers {
            if self.peer_allowed(p) && !discovered.contains(p) {
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
        let mut last_announce = Instant::now();
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
                CommandOutcome::Stop => break,
                CommandOutcome::Reannounce => {
                    let refreshed = self.refresh_discovery_peers().await;
                    merge_unique_peers(&mut discovered, refreshed);
                    dedupe_peers(&mut discovered);
                    self.state.lock().await.peers = discovered.clone();
                    last_announce = Instant::now();
                }
                CommandOutcome::Continue | CommandOutcome::Pause => {}
            }
            let max_concurrent = self.current_peer_worker_limit();

            if have.count(piece_count) == piece_count {
                let storage = self.complete_storage(&storage).await?;
                self.finish_without_resume(&storage).await?;
                // Announce completion to trackers.
                self.announce(AnnounceEvent::Completed).await;
                break;
            }

            // Periodically re-announce to refresh peers.
            if last_announce.elapsed() > PEER_REFRESH_INTERVAL {
                let refreshed = self.refresh_discovery_peers().await;
                merge_unique_peers(&mut discovered, refreshed);
                dedupe_peers(&mut discovered);
                self.state.lock().await.peers = discovered.clone();
                last_announce = Instant::now();
            }

            let remaining = piece_count - have.count(piece_count);
            prune_peer_backoff(&mut bad_peers);
            prune_peer_backoff(&mut peer_backoff);
            let mut eligible: Vec<PeerAddr> = discovered
                .iter()
                .filter(|p| self.peer_allowed(p))
                .filter(|p| !peer_is_backed_off(&bad_peers, p.socket_addr()))
                .filter(|p| !peer_is_backed_off(&peer_backoff, p.socket_addr()))
                .copied()
                .collect();
            balance_peer_families(&mut eligible);

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
                    if progressed || have.count(piece_count) == piece_count {
                        continue;
                    }
                }
            }

            let candidates =
                rotated_peer_candidates(&eligible, &mut candidate_cursor, eligible.len());
            let mut made_progress = false;

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

            while let Some(peer_addr) = to_try.pop() {
                if have.count(piece_count) == piece_count {
                    break;
                }
                match self
                    .download_from_peer(&peer_addr, &storage, &mut have, &mut discovered)
                    .await
                {
                    Ok(progressed) => {
                        if progressed {
                            made_progress = true;
                        } else {
                            backoff_peer(&mut peer_backoff, peer_addr.socket_addr());
                        }
                    }
                    Err(e) => {
                        tracing::debug!(peer = %peer_addr.socket_addr(), error = %e, "peer failed; suppressing");
                        backoff_failed_peer(&mut bad_peers, peer_addr.socket_addr());
                    }
                }
            }

            if !made_progress {
                if discovered.is_empty() || bad_peers.len() >= discovered.len() {
                    // No usable peers; back off briefly and retry announce.
                    self.sleep_or_stop(Duration::from_secs(2)).await;
                    let refreshed = self.refresh_discovery_peers().await;
                    merge_unique_peers(&mut discovered, refreshed);
                    if discovered.is_empty() {
                        no_peer_rounds = no_peer_rounds.saturating_add(1);
                        let mut state = self.state.lock().await;
                        let existing = state.tracker_message.clone();
                        if !existing
                            .as_deref()
                            .unwrap_or_default()
                            .starts_with("no peers discovered")
                        {
                            state.tracker_message = Some(match existing {
                                Some(msg) => format!("no peers discovered; last announce: {msg}"),
                                None => "no peers discovered".into(),
                            });
                        }
                        drop(state);
                        // Bounded give-up: a torrent that never discovers peers
                        // (no tracker, no seed peers, no DHT result) cannot
                        // progress. Stop the engine so the daemon/test does not
                        // hang; the torrent remains incomplete and the user can
                        // add trackers or seed peers and re-start it.
                        if no_peer_rounds >= NO_PEER_ROUNDS_MAX {
                            let tracker_message = self.state.lock().await.tracker_message.clone();
                            tracing::info!(
                                info_hash = %self.meta.info_hash,
                                tracker_message = ?tracker_message,
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

    fn peer_allowed(&self, peer: &PeerAddr) -> bool {
        self.allow_ipv6 || !peer.ip.is_ipv6()
    }

    fn filter_allowed_peers(&self, peers: Vec<PeerAddr>) -> Vec<PeerAddr> {
        peers
            .into_iter()
            .filter(|peer| self.peer_allowed(peer))
            .collect()
    }

    async fn refresh_discovery_peers(&self) -> Vec<PeerAddr> {
        let mut refreshed = self.filter_allowed_peers(self.announce(AnnounceEvent::Empty).await);
        let dht_peers = self.discover_dht_peers().await;
        merge_unique_peers(&mut refreshed, dht_peers);
        dedupe_peers(&mut refreshed);
        refreshed
    }

    async fn discover_dht_peers(&self) -> Vec<PeerAddr> {
        if self.meta.is_private() {
            return Vec::new();
        }
        let Some(dht) = &self.dht else {
            return Vec::new();
        };
        let result = tokio::time::timeout(
            DHT_DISCOVERY_TIMEOUT,
            dht.get_peers_with_stats(self.meta.info_hash, DHT_DISCOVERY_ROUNDS),
        )
        .await;
        match result {
            Ok(Ok(lookup)) => {
                let peers = self.filter_allowed_peers(lookup.peers);
                if lookup.responding_nodes > 0 || !peers.is_empty() {
                    let mut s = self.state.lock().await;
                    s.dht_discovery_ok = !peers.is_empty();
                    s.dht_last_seen = Some(Instant::now());
                }
                tracing::debug!(
                    queried = lookup.queried_nodes,
                    responding = lookup.responding_nodes,
                    peers = peers.len(),
                    "DHT peer discovery completed"
                );
                peers
            }
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "DHT peer discovery failed");
                Vec::new()
            }
            Err(_) => {
                tracing::debug!("DHT peer discovery timed out");
                Vec::new()
            }
        }
    }

    /// Attempt to download missing pieces from a single peer. Returns true if
    /// at least one new piece was verified and written.
    async fn download_from_peer(
        &self,
        peer_addr: &PeerAddr,
        storage: &StorageIo,
        have: &mut PieceBitfield,
        discovered: &mut Vec<PeerAddr>,
    ) -> Result<bool> {
        if !self.binder.traffic_allowed() {
            return Ok(false);
        }
        if !self.peer_allowed(peer_addr) {
            return Ok(false);
        }
        let (mut reader, mut write_half, transport) = connect_peer_wire_with_transport(
            self.binder.clone(),
            *peer_addr,
            self.meta.info_hash,
            self.peer_id,
            self.utp_enabled,
            self.utp_prefer_tcp,
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
        let ext_payload = swarmotter_core::extensions::encode_extension_handshake(
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
                                    record_peer_block(&self.state, *peer_addr, block.len() as u64)
                                        .await;
                                }
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
                Message::Unchoke => {
                    peer_choking = false;
                    record_peer_unchoked(&self.state, *peer_addr).await;
                }
                Message::Choke => {
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
                                pex.added.into_iter().chain(pex.added6.into_iter()),
                                self.allow_ipv6,
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
        bad_peers: &mut HashMap<SocketAddr, Instant>,
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
        let mut handles = AbortOnDropHandles::new();
        let deadline = Instant::now() + ENDGAME_STEP_DEADLINE;
        for peer_addr in peers {
            let meta = self.meta.clone();
            let binder = self.binder.clone();
            let peer_id = self.peer_id;
            let shared_have = shared_have.clone();
            let outstanding = outstanding.clone();
            let made_progress = made_progress.clone();
            let download_dir = download_dir.clone();
            let state = self.state.clone();
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
                    state,
                    limiter,
                    utp_enabled,
                    utp_prefer_tcp,
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

    /// Normal-mode parallel download: several peers fetch distinct reserved
    /// pieces concurrently. Unlike endgame, duplicate piece requests are
    /// avoided; endgame remains responsible for deliberate duplicate requests
    /// near completion.
    async fn run_parallel_peer_round(
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
        let shared = Arc::new(Mutex::new(ParallelPieceState::new(have.clone())));
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
                self.pex_enabled && !self.meta.is_private(),
                self.allow_ipv6,
                self.pex_max_peers,
            );
            next_candidate += 1;
        }

        if tasks.is_empty() {
            return (false, Vec::new());
        }

        {
            let mut s = self.state.lock().await;
            s.active_peers = tasks.len();
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
                    Ok((peer_addr, Ok(progressed))) => {
                        if progressed {
                            any_progress = true;
                        } else {
                            backoff_peer(peer_backoff, peer_addr.socket_addr());
                        }
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
                work.have.count(self.meta.piece_count()) == self.meta.piece_count()
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
                let refreshed = self.refresh_discovery_peers().await;
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
                    self.pex_enabled && !self.meta.is_private(),
                    self.allow_ipv6,
                    self.pex_max_peers,
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
        self.state.lock().await.active_peers = 0;
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
                    numwant: Some(200),
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
                        if resp.peers.is_empty() {
                            msg = Some(format!(
                                "{url}: announce returned 0 peers (seeders={}, leechers={})",
                                resp.seeders, resp.leechers
                            ));
                        }
                        all.extend(resp.peers);
                    }
                    Err(e) => {
                        msg = Some(format!("{url}: {e}"));
                        tracing::debug!(tracker = %url, error = %e, "tracker announce failed");
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
                    numwant: Some(200),
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
                        numwant: Some(200),
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
        update_progress_state(&self.state, &self.meta, have).await;
    }

    async fn mark_finished(&self) {
        let mut s = self.state.lock().await;
        s.finished = true;
    }

    async fn load_or_recheck(&self, storage: &StorageIo) -> Result<PieceBitfield> {
        if let Some(resume) = storage.load_resume(&self.meta.info_hash).await? {
            let payload_bytes = storage.payload_bytes_on_disk().await?;
            if self.sparse && !self.preallocate && payload_bytes != resume.bytes_completed {
                tracing::info!(
                    info_hash = %self.meta.info_hash,
                    payload_bytes,
                    resume_bytes_completed = resume.bytes_completed,
                    "fast resume byte count differs from on-disk payload; rechecking storage"
                );
                storage.recheck().await
            } else {
                Ok(resume.piece_bitfield)
            }
        } else {
            storage.recheck().await
        }
    }

    async fn complete_storage(&self, storage: &StorageIo) -> Result<StorageIo> {
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

    async fn finish_without_resume(&self, storage: &StorageIo) -> Result<()> {
        self.mark_finished().await;
        storage.remove_resume().await?;
        if self.download_dir != self.complete_dir {
            let active_storage = StorageIo::new(self.meta.clone(), self.download_dir.clone());
            active_storage.remove_resume().await?;
        }
        Ok(())
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
            Some(storage.base_dir().display().to_string()),
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
            Ok(EngineCommand::Reannounce) => CommandOutcome::Reannounce,
            Ok(EngineCommand::Recheck) => CommandOutcome::Continue,
            Ok(EngineCommand::UpdatePeerWorkerLimit(limit)) => {
                self.set_peer_worker_limit(limit);
                CommandOutcome::Continue
            }
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
    Reannounce,
    Stop,
}

const NORMAL_REQUEST_PIPELINE: usize = 64;
const NORMAL_PEER_PIECE_WINDOW: usize = 4;
const PEER_IDLE_BACKOFF: Duration = Duration::from_secs(20);
const PEER_FAILURE_BACKOFF: Duration = Duration::from_secs(120);

fn balance_peer_families(peers: &mut Vec<PeerAddr>) {
    if peers.len() < 2 {
        return;
    }
    let mut ipv4 = Vec::new();
    let mut ipv6 = Vec::new();
    for peer in peers.iter().copied() {
        if peer.ip.is_ipv6() {
            ipv6.push(peer);
        } else {
            ipv4.push(peer);
        }
    }
    if ipv4.is_empty() || ipv6.is_empty() {
        return;
    }

    let mut balanced = Vec::with_capacity(peers.len());
    let mut v4 = 0usize;
    let mut v6 = 0usize;
    while v4 < ipv4.len() || v6 < ipv6.len() {
        if v4 < ipv4.len() {
            balanced.push(ipv4[v4]);
            v4 += 1;
        }
        if v6 < ipv6.len() {
            balanced.push(ipv6[v6]);
            v6 += 1;
        }
    }
    *peers = balanced;
}

fn dedupe_peers(peers: &mut Vec<PeerAddr>) {
    let mut seen = HashSet::new();
    peers.retain(|peer| seen.insert(peer.socket_addr()));
}

fn merge_unique_peers<I>(discovered: &mut Vec<PeerAddr>, peers: I) -> usize
where
    I: IntoIterator<Item = PeerAddr>,
{
    let before = discovered.len();
    for peer in peers {
        if !discovered.contains(&peer) {
            discovered.push(peer);
        }
    }
    discovered.len().saturating_sub(before)
}

fn peer_bitfield_has_missing(peer_bf: &Bitfield, have: &PieceBitfield, piece_count: usize) -> bool {
    (0..piece_count).any(|i| peer_bf.has(i) && !have.has(i))
}

fn peer_bitfield_snapshot(peer_bf: &Bitfield, piece_count: usize) -> PieceBitfield {
    let mut out = PieceBitfield::new(piece_count);
    for i in 0..piece_count {
        if peer_bf.has(i) {
            out.set(i);
        }
    }
    out
}

fn apply_peer_have(peer_bf: &mut Option<Bitfield>, piece_count: usize, piece: u32) {
    peer_bf
        .get_or_insert_with(|| Bitfield::new(piece_count))
        .set(piece as usize);
}

async fn record_peer_connected(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    st.peer_health
        .entry(peer_addr.socket_addr())
        .or_default()
        .last_seen = Some(Instant::now());
}

async fn record_peer_unchoked(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.unchoked = true;
    entry.last_seen = Some(Instant::now());
}

async fn record_peer_choked(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.unchoked = false;
    entry.last_seen = Some(Instant::now());
}

async fn record_peer_availability(
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

async fn record_peer_block(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr, bytes: u64) {
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

async fn record_peer_timeout(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    st.timeout_failures = st.timeout_failures.saturating_add(1);
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.last_seen = Some(Instant::now());
}

async fn record_peer_hash_failure(state: &Arc<Mutex<EngineState>>, peer_addr: PeerAddr) {
    let mut st = state.lock().await;
    st.hash_failures = st.hash_failures.saturating_add(1);
    let entry = st.peer_health.entry(peer_addr.socket_addr()).or_default();
    entry.blocked = true;
    entry.last_seen = Some(Instant::now());
}

async fn record_peer_disconnect(state: &Arc<Mutex<EngineState>>) {
    let mut st = state.lock().await;
    st.peer_disconnects_recent = st.peer_disconnects_recent.saturating_add(1);
}

fn prune_peer_backoff(backoff: &mut HashMap<SocketAddr, Instant>) {
    let now = Instant::now();
    backoff.retain(|_, until| *until > now);
}

fn peer_is_backed_off(backoff: &HashMap<SocketAddr, Instant>, peer: SocketAddr) -> bool {
    backoff
        .get(&peer)
        .is_some_and(|until| *until > Instant::now())
}

fn backoff_peer_for(
    backoff: &mut HashMap<SocketAddr, Instant>,
    peer: SocketAddr,
    duration: Duration,
) {
    backoff.insert(peer, Instant::now() + duration);
}

fn backoff_peer(backoff: &mut HashMap<SocketAddr, Instant>, peer: SocketAddr) {
    backoff_peer_for(backoff, peer, PEER_IDLE_BACKOFF);
}

fn backoff_failed_peer(backoff: &mut HashMap<SocketAddr, Instant>, peer: SocketAddr) {
    backoff_peer_for(backoff, peer, PEER_FAILURE_BACKOFF);
}

fn rotated_peer_candidates(
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

struct AbortOnDropHandles<T> {
    handles: Vec<AbortOnDropHandle<T>>,
}

impl<T> AbortOnDropHandles<T> {
    fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    fn push(&mut self, handle: tokio::task::JoinHandle<T>) {
        self.handles.push(AbortOnDropHandle { handle });
    }

    fn drain(&mut self) -> std::vec::Drain<'_, AbortOnDropHandle<T>> {
        self.handles.drain(..)
    }
}

struct AbortOnDropHandle<T> {
    handle: tokio::task::JoinHandle<T>,
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

fn add_pex_peers<I>(discovered: &mut Vec<PeerAddr>, peers: I, allow_ipv6: bool, max_peers: usize)
where
    I: IntoIterator<Item = PeerAddr>,
{
    for peer in peers {
        if !allow_ipv6 && peer.ip.is_ipv6() {
            continue;
        }
        if max_peers > 0 && discovered.len() >= max_peers {
            break;
        }
        if !discovered.contains(&peer) {
            discovered.push(peer);
        }
    }
}

async fn merge_dynamic_parallel_candidates(
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

fn merge_parallel_candidate_iter<I>(
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

fn push_parallel_candidate(
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

type PeerReadHalf = tokio::io::ReadHalf<Box<dyn utp::PeerDuplex>>;
type PeerWriteHalf = tokio::io::WriteHalf<Box<dyn utp::PeerDuplex>>;

async fn connect_peer_wire_with_transport(
    binder: Arc<dyn NetworkBinder>,
    peer_addr: PeerAddr,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    utp_enabled: bool,
    utp_prefer_tcp: bool,
) -> Result<(PeerReader<PeerReadHalf>, PeerWriteHalf, PeerTransport)> {
    let transports = if utp_enabled {
        if utp_prefer_tcp {
            vec![PeerTransport::Tcp, PeerTransport::Utp]
        } else {
            vec![PeerTransport::Utp, PeerTransport::Tcp]
        }
    } else {
        vec![PeerTransport::Tcp]
    };

    let mut last_error = None;
    for (idx, transport) in transports.iter().copied().enumerate() {
        match attempt_peer_wire_transport(binder.clone(), transport, peer_addr, info_hash, peer_id)
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

async fn attempt_peer_wire_transport(
    binder: Arc<dyn NetworkBinder>,
    transport: PeerTransport,
    peer_addr: PeerAddr,
    info_hash: InfoHash,
    peer_id: [u8; 20],
) -> Result<(PeerReader<PeerReadHalf>, PeerWriteHalf, PeerTransport)> {
    let (stream, selected) =
        utp::connect_peer_stream(binder, transport, peer_addr.socket_addr()).await?;
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

    Ok((reader, write_half, selected))
}

#[derive(Debug, Clone)]
struct ParallelPieceState {
    have: PieceBitfield,
    reserved: HashSet<usize>,
}

impl ParallelPieceState {
    fn new(have: PieceBitfield) -> Self {
        Self {
            have,
            reserved: HashSet::new(),
        }
    }

    fn reserve_piece(&mut self, peer_bf: &Bitfield, piece_count: usize) -> Option<usize> {
        let piece = (0..piece_count)
            .find(|&i| peer_bf.has(i) && !self.have.has(i) && !self.reserved.contains(&i))?;
        self.reserved.insert(piece);
        Some(piece)
    }

    fn release_piece(&mut self, piece: usize) {
        self.reserved.remove(&piece);
    }
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
    state: Arc<Mutex<EngineState>>,
    limiter: ShapedLimiter,
    utp_enabled: bool,
    utp_prefer_tcp: bool,
) -> Result<bool> {
    if !binder.traffic_allowed() {
        return Ok(false);
    }
    let storage = StorageIo::new(meta.clone(), download_dir);
    let (mut reader, mut write_half, transport) = connect_peer_wire_with_transport(
        binder.clone(),
        peer_addr,
        meta.info_hash,
        peer_id,
        utp_enabled,
        utp_prefer_tcp,
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
                            record_peer_block(&state, peer_addr, block.len() as u64).await;
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

/// A normal-mode peer session used by the bounded parallel downloader. Each
/// session reserves one missing piece at a time from shared state, so peers
/// work on distinct pieces until endgame takes over.
#[allow(clippy::too_many_arguments)]
fn spawn_parallel_peer_task(
    tasks: &mut tokio::task::JoinSet<(PeerAddr, Result<bool>)>,
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
    pex_enabled: bool,
    allow_ipv6: bool,
    pex_max_peers: usize,
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
            pex_enabled,
            allow_ipv6,
            pex_max_peers,
        )
        .await;
        (peer_addr, result)
    });
}

struct ParallelPieceDownload {
    piece_index: usize,
    reqs: Vec<(u32, u32)>,
    next_req: usize,
    in_flight: usize,
    received: usize,
    assembler: peer::PieceAssembler,
}

impl ParallelPieceDownload {
    fn new(piece_index: usize, piece_len: u32) -> Self {
        Self {
            piece_index,
            reqs: block_requests(piece_len),
            next_req: 0,
            in_flight: 0,
            received: 0,
            assembler: peer::PieceAssembler::new(piece_index as u32, piece_len as usize),
        }
    }

    async fn send_more<W>(&mut self, write_half: &mut W, global_in_flight: &mut usize) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        while self.next_req < self.reqs.len() && *global_in_flight < NORMAL_REQUEST_PIPELINE {
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
            *global_in_flight += 1;
        }
        Ok(())
    }

    fn record_block(&mut self, offset: u32, block: &[u8], global_in_flight: &mut usize) -> bool {
        if self.assembler.add_block(offset, block).is_err() {
            return false;
        }
        self.received += 1;
        self.in_flight = self.in_flight.saturating_sub(1);
        *global_in_flight = (*global_in_flight).saturating_sub(1);
        self.received == self.reqs.len()
    }
}

#[allow(clippy::too_many_arguments)]
async fn fill_parallel_piece_window<W>(
    write_half: &mut W,
    downloads: &mut HashMap<usize, ParallelPieceDownload>,
    global_in_flight: &mut usize,
    shared: &Arc<Mutex<ParallelPieceState>>,
    peer_bf: &Bitfield,
    meta: &TorrentMeta,
    piece_count: usize,
) -> Result<bool>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut reserved_any = false;
    while downloads.len() < NORMAL_PEER_PIECE_WINDOW && *global_in_flight < NORMAL_REQUEST_PIPELINE
    {
        let Some(piece_index) = ({
            let mut work = shared.lock().await;
            work.reserve_piece(peer_bf, piece_count)
        }) else {
            break;
        };
        let piece_len = if piece_index + 1 == piece_count {
            meta.last_piece_length()
        } else {
            meta.piece_length
        } as u32;
        let mut download = ParallelPieceDownload::new(piece_index, piece_len);
        if let Err(e) = download.send_more(write_half, global_in_flight).await {
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
async fn parallel_peer_session(
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
    pex_enabled: bool,
    allow_ipv6: bool,
    pex_max_peers: usize,
) -> Result<bool> {
    if !binder.traffic_allowed() {
        return Ok(false);
    }

    let (mut reader, mut write_half, transport) = connect_peer_wire_with_transport(
        binder,
        peer_addr,
        meta.info_hash,
        peer_id,
        utp_enabled,
        utp_prefer_tcp,
    )
    .await?;
    tracing::debug!(peer = %peer_addr.socket_addr(), transport = transport.as_str(), "parallel peer connected");
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
    if pex_enabled {
        let ext_payload = swarmotter_core::extensions::encode_extension_handshake(
            &[(swarmotter_core::extensions::UT_PEX_NAME, 1u8)],
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
    }
    peer::write_message(&mut write_half, &Message::Interested).await?;
    write_half.flush().await.ok();

    let mut peer_bf: Option<Bitfield> = None;
    let mut peer_choking = true;
    let mut progressed = false;
    let mut remote_pex_id: Option<u8> = None;

    loop {
        if Instant::now() > deadline {
            break;
        }
        let complete = {
            let work = shared.lock().await;
            work.have.count(piece_count) == piece_count
        };
        if complete {
            break;
        }

        if !peer_choking {
            let Some(peer_bf_snapshot) = peer_bf.clone() else {
                return Ok(progressed);
            };
            let mut downloads: HashMap<usize, ParallelPieceDownload> = HashMap::new();
            let mut global_in_flight = 0usize;
            fill_parallel_piece_window(
                &mut write_half,
                &mut downloads,
                &mut global_in_flight,
                &shared,
                &peer_bf_snapshot,
                &meta,
                piece_count,
            )
            .await?;
            if downloads.is_empty() {
                peer::write_message(&mut write_half, &Message::NotInterested).await?;
                break;
            }

            let mut last_block_at = Instant::now();
            let mut received_any = false;
            while !downloads.is_empty() {
                let remaining = (last_block_at + Duration::from_secs(20))
                    .saturating_duration_since(Instant::now());
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
                        let piece_index = piece as usize;
                        let mut complete_data = None;
                        if let Some(download) = downloads.get_mut(&piece_index) {
                            let complete =
                                download.record_block(offset, &block, &mut global_in_flight);
                            record_peer_block(&state, peer_addr, block.len() as u64).await;
                            last_block_at = Instant::now();
                            received_any = true;
                            if complete {
                                complete_data = Some(download.assembler.data().to_vec());
                            } else {
                                download
                                    .send_more(&mut write_half, &mut global_in_flight)
                                    .await?;
                                write_half.flush().await.ok();
                            }
                        }
                        if let Some(data) = complete_data {
                            downloads.remove(&piece_index);
                            if swarmotter_core::storage::verify_piece(&meta, piece_index, &data) {
                                limiter
                                    .acquire(RateDirection::Download, data.len() as u64)
                                    .await;
                                storage.write_piece(piece_index, &data).await?;
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
                                peer::write_message(
                                    &mut write_half,
                                    &Message::Have {
                                        piece: piece_index as u32,
                                    },
                                )
                                .await?;
                                fill_parallel_piece_window(
                                    &mut write_half,
                                    &mut downloads,
                                    &mut global_in_flight,
                                    &shared,
                                    peer_bf.as_ref().unwrap_or(&peer_bf_snapshot),
                                    &meta,
                                    piece_count,
                                )
                                .await?;
                            } else {
                                tracing::warn!(
                                    piece = piece_index,
                                    "piece hash mismatch; rejecting"
                                );
                                record_peer_hash_failure(&state, peer_addr).await;
                                shared.lock().await.release_piece(piece_index);
                            }
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
                            let have = shared.lock().await.have.clone();
                            record_peer_availability(&state, peer_addr, bf, &have, piece_count)
                                .await;
                        }
                    }
                    Message::Bitfield { bits } => {
                        let bf = Bitfield::from_bytes(bits, piece_count);
                        let have = shared.lock().await.have.clone();
                        record_peer_availability(&state, peer_addr, &bf, &have, piece_count).await;
                        peer_bf = Some(bf);
                    }
                    Message::Extended { id, payload } => {
                        handle_parallel_pex_message(
                            id,
                            &payload,
                            pex_enabled,
                            &mut remote_pex_id,
                            allow_ipv6,
                            pex_max_peers,
                            &pex_peers,
                            &state,
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
            if !downloads.is_empty() {
                record_peer_timeout(&state, peer_addr).await;
            }
            if !received_any {
                return Ok(progressed);
            }
            continue;
        }

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
                let have = shared.lock().await.have.clone();
                record_peer_availability(&state, peer_addr, &bf, &have, piece_count).await;
                peer_bf = Some(bf);
            }
            Message::Have { piece } => {
                apply_peer_have(&mut peer_bf, piece_count, piece);
                if let Some(bf) = &peer_bf {
                    let have = shared.lock().await.have.clone();
                    record_peer_availability(&state, peer_addr, bf, &have, piece_count).await;
                }
            }
            Message::Extended { id, payload } => {
                handle_parallel_pex_message(
                    id,
                    &payload,
                    pex_enabled,
                    &mut remote_pex_id,
                    allow_ipv6,
                    pex_max_peers,
                    &pex_peers,
                    &state,
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

    Ok(progressed)
}

#[allow(clippy::too_many_arguments)]
async fn handle_parallel_pex_message(
    id: u8,
    payload: &[u8],
    pex_enabled: bool,
    remote_pex_id: &mut Option<u8>,
    allow_ipv6: bool,
    pex_max_peers: usize,
    pex_peers: &Arc<Mutex<Vec<PeerAddr>>>,
    state: &Arc<Mutex<EngineState>>,
) {
    if !pex_enabled {
        return;
    }
    if id == swarmotter_core::extensions::EXTENSION_HANDSHAKE_ID {
        if let Ok(hs) = swarmotter_core::extensions::parse_extension_handshake(payload) {
            *remote_pex_id = hs.id_for(swarmotter_core::extensions::UT_PEX_NAME);
        }
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
        pex.added.into_iter().chain(pex.added6.into_iter()),
        allow_ipv6,
        pex_max_peers,
    );
    if peers.len() > before {
        let mut s = state.lock().await;
        s.pex_discovery_ok = true;
        s.pex_last_seen = Some(Instant::now());
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn update_progress_state(
    state: &Arc<Mutex<EngineState>>,
    meta: &TorrentMeta,
    have: &PieceBitfield,
) {
    let mut s = state.lock().await;
    s.pieces_have = have.clone();
    let complete_pieces = have.count(s.piece_count) as u64;
    let mut completed = complete_pieces.saturating_mul(meta.piece_length);
    if s.piece_count > 0 && have.has(s.piece_count - 1) {
        completed = completed.saturating_sub(meta.piece_length - meta.last_piece_length());
    }
    completed = completed.min(meta.total_length);
    s.bytes_completed = completed;
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::meta::build_single_file_torrent;

    fn unique_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "swarmotter-engine-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

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

    #[test]
    fn rotated_peer_candidates_cycles_through_eligible_peers() {
        let peers: Vec<PeerAddr> = (1..=5)
            .map(|i| PeerAddr::from_socket_addr(([127, 0, 0, i], 6881).into()))
            .collect();
        let mut cursor = 0;

        let first = rotated_peer_candidates(&peers, &mut cursor, 2);
        assert_eq!(first, vec![peers[0], peers[1]]);
        assert_eq!(cursor, 2);

        let second = rotated_peer_candidates(&peers, &mut cursor, 2);
        assert_eq!(second, vec![peers[2], peers[3]]);
        assert_eq!(cursor, 4);

        let wrapped = rotated_peer_candidates(&peers, &mut cursor, 3);
        assert_eq!(wrapped, vec![peers[4], peers[0], peers[1]]);
        assert_eq!(cursor, 2);
    }

    #[test]
    fn balance_peer_families_interleaves_ipv4_and_ipv6() {
        let mut peers = vec![
            PeerAddr::from_socket_addr("127.0.0.1:6001".parse().unwrap()),
            PeerAddr::from_socket_addr("127.0.0.2:6002".parse().unwrap()),
            PeerAddr::from_socket_addr("[2001:db8::1]:6003".parse().unwrap()),
            PeerAddr::from_socket_addr("[2001:db8::2]:6004".parse().unwrap()),
            PeerAddr::from_socket_addr("[2001:db8::3]:6005".parse().unwrap()),
        ];

        balance_peer_families(&mut peers);

        assert!(!peers[0].ip.is_ipv6());
        assert!(peers[1].ip.is_ipv6());
        assert!(!peers[2].ip.is_ipv6());
        assert!(peers[3].ip.is_ipv6());
        assert!(peers[4].ip.is_ipv6());
    }

    #[test]
    fn merge_unique_peers_skips_duplicates() {
        let first = PeerAddr::from_socket_addr("127.0.0.1:6001".parse().unwrap());
        let second = PeerAddr::from_socket_addr("[2001:db8::1]:6002".parse().unwrap());
        let mut peers = vec![first];

        let added = merge_unique_peers(&mut peers, [first, second]);

        assert_eq!(added, 1);
        assert_eq!(peers, vec![first, second]);
    }

    #[tokio::test]
    async fn progress_update_does_not_count_rechecked_bytes_as_downloaded() {
        let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let state = Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            downloaded: 123,
            ..Default::default()
        }));
        let mut have = PieceBitfield::new(meta.piece_count());
        have.set(0);

        update_progress_state(&state, &meta, &have).await;

        let state = state.lock().await;
        assert_eq!(state.bytes_completed, 8);
        assert_eq!(state.downloaded, 123);
    }

    #[tokio::test]
    async fn stale_fast_resume_rechecks_payload_ahead_of_resume() {
        let payload = b"abcdefghABCDEFGHijklmnop";
        let bytes = build_single_file_torrent("stale.bin", payload, 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let dir = unique_dir("stale-resume");
        let storage = StorageIo::new(meta.clone(), dir.clone());
        storage.write_piece(0, &payload[0..8]).await.unwrap();
        storage.write_piece(2, &payload[16..24]).await.unwrap();

        let mut stale = PieceBitfield::new(meta.piece_count());
        stale.set(0);
        let piece_lengths: Vec<u64> = (0..meta.piece_count())
            .map(|i| {
                if i + 1 == meta.piece_count() {
                    meta.last_piece_length()
                } else {
                    meta.piece_length
                }
            })
            .collect();
        let resume = swarmotter_core::storage::io::build_resume(
            meta.info_hash,
            meta.name.clone(),
            stale,
            meta.piece_count(),
            0,
            0,
            meta.total_length,
            Some(dir.display().to_string()),
            now_secs(),
            None,
            &vec![swarmotter_core::models::torrent::FilePriority::Normal; meta.files.len()],
            &piece_lengths,
        );
        storage.save_resume(&resume).await.unwrap();

        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta.clone(),
            dir,
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        )
        .with_preallocate(false);

        let recovered = engine.load_or_recheck(&storage).await.unwrap();

        assert!(recovered.has(0));
        assert!(!recovered.has(1));
        assert!(recovered.has(2));
    }

    #[tokio::test]
    async fn stale_fast_resume_rechecks_resume_ahead_of_payload() {
        let payload = b"abcdefghABCDEFGHijklmnop";
        let bytes = build_single_file_torrent("overclaim.bin", payload, 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let dir = unique_dir("stale-resume-overclaim");
        let storage = StorageIo::new(meta.clone(), dir.clone());
        storage.write_piece(0, &payload[0..8]).await.unwrap();

        let mut stale = PieceBitfield::new(meta.piece_count());
        stale.set(0);
        stale.set(1);
        let piece_lengths: Vec<u64> = (0..meta.piece_count())
            .map(|i| {
                if i + 1 == meta.piece_count() {
                    meta.last_piece_length()
                } else {
                    meta.piece_length
                }
            })
            .collect();
        let resume = swarmotter_core::storage::io::build_resume(
            meta.info_hash,
            meta.name.clone(),
            stale,
            meta.piece_count(),
            0,
            0,
            meta.total_length,
            Some(dir.display().to_string()),
            now_secs(),
            None,
            &vec![swarmotter_core::models::torrent::FilePriority::Normal; meta.files.len()],
            &piece_lengths,
        );
        storage.save_resume(&resume).await.unwrap();

        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta.clone(),
            dir.clone(),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        )
        .with_preallocate(false);

        let recovered = engine.load_or_recheck(&storage).await.unwrap();

        assert!(recovered.has(0));
        assert!(!recovered.has(1));
        assert!(!recovered.has(2));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn peer_worker_limit_uses_default_when_uncapped() {
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
        )
        .with_peer_worker_limit(0);

        assert_eq!(
            engine.current_peer_worker_limit(),
            DEFAULT_PEER_WORKER_LIMIT
        );
    }

    #[test]
    fn peer_worker_limit_accepts_operator_cap() {
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
        )
        .with_peer_worker_limit(12);

        assert_eq!(engine.current_peer_worker_limit(), 12);
    }

    #[test]
    fn pex_import_respects_ipv6_and_cap() {
        let mut peers = Vec::new();
        add_pex_peers(
            &mut peers,
            [
                "127.0.0.1:6001".parse::<SocketAddr>().unwrap(),
                "[::1]:6002".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:6003".parse::<SocketAddr>().unwrap(),
            ]
            .into_iter()
            .map(PeerAddr::from_socket_addr),
            false,
            1,
        );

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].socket_addr(), "127.0.0.1:6001".parse().unwrap());
    }

    #[test]
    fn peer_allowed_respects_ipv6_config() {
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
        )
        .with_allow_ipv6(false);

        assert!(engine.peer_allowed(&PeerAddr::from_socket_addr(
            "127.0.0.1:6881".parse().unwrap()
        )));
        assert!(!engine.peer_allowed(&PeerAddr::from_socket_addr("[::1]:6881".parse().unwrap())));
    }

    #[tokio::test]
    async fn completed_active_data_moves_to_complete_dir() {
        let content = b"verified active data moves after completion";
        let bytes = build_single_file_torrent("complete.bin", content, 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let active_dir = unique_dir("active");
        let complete_dir = unique_dir("complete");
        let active_storage = StorageIo::new(meta.clone(), active_dir.clone());
        active_storage.preallocate().await.unwrap();
        for piece in 0..meta.piece_count() {
            let start = piece * 8;
            let end = std::cmp::min(start + 8, content.len());
            active_storage
                .write_block(piece, 0, &content[start..end])
                .await
                .unwrap();
        }

        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta.clone(),
            active_dir.clone(),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        )
        .with_complete_dir(complete_dir.clone());

        let final_state = engine.run().await.unwrap();

        assert!(final_state.finished);
        assert!(!active_storage.file_path(0).unwrap().exists());
        let complete_storage = StorageIo::new(meta.clone(), complete_dir.clone());
        assert_eq!(
            std::fs::read(complete_storage.file_path(0).unwrap()).unwrap(),
            content
        );
        assert!(!active_storage.resume_path().exists());
        assert!(!complete_storage.resume_path().exists());
        std::fs::remove_dir_all(&active_dir).ok();
        std::fs::remove_dir_all(&complete_dir).ok();
    }

    #[tokio::test]
    async fn completed_single_root_removes_resume_metadata() {
        let content = b"verified data complete in place";
        let bytes = build_single_file_torrent("single-root.bin", content, 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let dir = unique_dir("complete-single-root");
        let storage = StorageIo::new(meta.clone(), dir.clone());
        storage.preallocate().await.unwrap();
        for piece in 0..meta.piece_count() {
            let start = piece * 8;
            let end = std::cmp::min(start + 8, content.len());
            storage
                .write_block(piece, 0, &content[start..end])
                .await
                .unwrap();
        }
        let mut have = PieceBitfield::new(meta.piece_count());
        for piece in 0..meta.piece_count() {
            have.set(piece);
        }
        let piece_lengths: Vec<u64> = (0..meta.piece_count())
            .map(|i| {
                if i + 1 == meta.piece_count() {
                    meta.last_piece_length()
                } else {
                    meta.piece_length
                }
            })
            .collect();
        let resume = swarmotter_core::storage::io::build_resume(
            meta.info_hash,
            meta.name.clone(),
            have,
            meta.piece_count(),
            content.len() as u64,
            0,
            meta.total_length,
            Some(dir.display().to_string()),
            now_secs(),
            Some(now_secs()),
            &vec![swarmotter_core::models::torrent::FilePriority::Normal; meta.files.len()],
            &piece_lengths,
        );
        storage.save_resume(&resume).await.unwrap();
        assert!(storage.resume_path().exists());

        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta.clone(),
            dir.clone(),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        );

        let final_state = engine.run().await.unwrap();

        assert!(final_state.finished);
        assert_eq!(
            std::fs::read(storage.file_path(0).unwrap()).unwrap(),
            content
        );
        assert!(!storage.resume_path().exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn engine_start_creates_active_single_file_placeholder() {
        let bytes =
            build_single_file_torrent("started.bin", b"payload waits for peers", 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let active_dir = unique_dir("started-active");
        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(EngineCommand::Stop).await.unwrap();
        let engine = TorrentEngine::new(
            meta.clone(),
            active_dir.clone(),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        )
        .with_preallocate(false);

        let final_state = engine.run().await.unwrap();

        assert!(!final_state.finished);
        let storage = StorageIo::new(meta, active_dir.clone());
        let path = storage.file_path(0).unwrap();
        assert!(path.exists());
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
        std::fs::remove_dir_all(&active_dir).ok();
    }

    #[tokio::test]
    async fn engine_start_sizes_file_when_sparse_disabled() {
        let payload = b"payload waits for peers but file is sized";
        let bytes = build_single_file_torrent("sized.bin", payload, 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let active_dir = unique_dir("sized-active");
        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(EngineCommand::Stop).await.unwrap();
        let engine = TorrentEngine::new(
            meta.clone(),
            active_dir.clone(),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        )
        .with_preallocate(false)
        .with_sparse(false);

        let final_state = engine.run().await.unwrap();

        assert!(!final_state.finished);
        let storage = StorageIo::new(meta, active_dir.clone());
        let path = storage.file_path(0).unwrap();
        assert!(path.exists());
        assert_eq!(std::fs::metadata(path).unwrap().len(), payload.len() as u64);
        std::fs::remove_dir_all(&active_dir).ok();
    }
}
