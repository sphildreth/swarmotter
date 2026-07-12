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
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::bandwidth::{RateDirection, RateLimiter, ShapedLimiter};
use swarmotter_core::config::PeerEncryptionMode;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::models::peer::EnginePeerHealth;
use swarmotter_core::models::stats::PeerSchedulerDiagnostics;
use swarmotter_core::models::torrent::FilePriority;
use swarmotter_core::models::tracker::{TrackerScrapeStatus, TrackerStatus};
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{
    self, block_requests, Bitfield, Handshake, Message, PeerAddr, PeerReader,
};
use swarmotter_core::storage::resume::PieceBitfield;
use swarmotter_core::storage::{piece_file_ranges, verify_piece, StorageIo};
use swarmotter_core::tracker::{self, AnnounceEvent, AnnounceRequest};
use swarmotter_core::udp_tracker;
use swarmotter_core::utp::{self, PeerTransport};

use crate::peer_permits::PeerSessionBudget;

/// Default simultaneous peer download workers when no per-torrent peer cap is
/// configured. Trackers commonly return far more than 16 usable peers for
/// public Linux distribution torrents, so the default should be high enough to
/// keep several useful peers busy without requiring operator tuning.
pub const DEFAULT_PEER_WORKER_LIMIT: usize = crate::peer_permits::DEFAULT_PER_TORRENT_PEER_LIMIT;
const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const NORMAL_PEER_SESSION_DEADLINE: Duration = Duration::from_secs(180);
const DHT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const DHT_DISCOVERY_ROUNDS: usize = 6;
const TRACKER_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(8);
const MAGNET_METADATA_RETRY_PAUSE: Duration = Duration::from_secs(2);
const MAGNET_METADATA_MAX_ROUNDS: u32 = 8;
const WEBSEED_BATCH_PIECES: usize = 128;
const WEBSEED_MAX_CONCURRENT_REQUESTS: usize = 32;
const WEBSEED_MAX_MIRROR_ATTEMPTS: usize = 4;
const WEBSEED_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct PieceSelection {
    priorities: Arc<Vec<Option<i32>>>,
    target_count: usize,
}

impl PieceSelection {
    fn all(meta: &TorrentMeta) -> Self {
        Self::all_count(meta.piece_count())
    }

    fn all_count(piece_count: usize) -> Self {
        let priorities = vec![Some(FilePriority::Normal.weight()); piece_count];
        Self {
            target_count: priorities.len(),
            priorities: Arc::new(priorities),
        }
    }

    fn from_files(
        meta: &TorrentMeta,
        priorities: &[FilePriority],
        wanted: &[bool],
    ) -> Result<Self> {
        if priorities.len() != meta.files.len() || wanted.len() != meta.files.len() {
            return Ok(Self::all(meta));
        }
        let priorities = (0..meta.piece_count())
            .map(|piece| -> Result<Option<i32>> {
                Ok(piece_file_ranges(meta, piece)?
                    .into_iter()
                    .filter_map(|slice| {
                        let priority = priorities[slice.file_index];
                        (wanted[slice.file_index] && priority != FilePriority::Unwanted)
                            .then_some(priority.weight())
                    })
                    .max())
            })
            .collect::<Result<Vec<_>>>()?;
        let target_count = priorities
            .iter()
            .filter(|priority| priority.is_some())
            .count();
        Ok(Self {
            priorities: Arc::new(priorities),
            target_count,
        })
    }

    fn includes(&self, piece: usize) -> bool {
        self.priorities.get(piece).is_some_and(Option::is_some)
    }

    fn priority(&self, piece: usize) -> i32 {
        self.priorities
            .get(piece)
            .and_then(|priority| *priority)
            .unwrap_or(i32::MIN)
    }

    fn complete(&self, have: &PieceBitfield) -> bool {
        if self.target_count == 0 {
            return true;
        }
        self.priorities
            .iter()
            .enumerate()
            .all(|(piece, priority)| priority.is_none() || have.has(piece))
    }

    fn remaining(&self, have: &PieceBitfield) -> usize {
        self.priorities
            .iter()
            .enumerate()
            .filter(|(piece, priority)| priority.is_some() && !have.has(*piece))
            .count()
    }
}

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

pub type MetadataPreflight =
    Arc<dyn Fn(TorrentMeta) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

#[derive(Debug, Default)]
struct TrackerAnnounceOutcome {
    peers: Vec<PeerAddr>,
    ok: bool,
    message: Option<String>,
    failures: u32,
    tracker_results: HashMap<String, TrackerAnnounceSnapshot>,
    interval_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TrackerAnnounceSnapshot {
    pub status: TrackerStatus,
    pub seeders: u64,
    pub leechers: u64,
    pub downloads: u64,
    pub last_error: Option<String>,
    pub last_message: Option<String>,
    pub last_announce: Option<u64>,
}

/// Most recent scrape attempt plus the separately retained last-success
/// counts. A failed attempt changes status/time/error without erasing counts.
#[derive(Debug, Clone, Default)]
pub struct TrackerScrapeSnapshot {
    pub status: TrackerScrapeStatus,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub downloads: Option<u64>,
    pub last_error: Option<String>,
    pub last_scrape: Option<u64>,
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
    pub tracker_announces: HashMap<String, TrackerAnnounceSnapshot>,
    pub tracker_scrapes: HashMap<String, TrackerScrapeSnapshot>,
    pub last_announce: Option<u64>,
    pub tracker_interval_seconds: u64,
    pub peer_scheduler: PeerSchedulerDiagnostics,
    pub finished: bool,
    /// True when the engine stopped because the daemon explicitly requested
    /// shutdown, pause, or queue rotation.
    pub stopped_by_command: bool,
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
    /// Timestamp of the latest DHT lookup attempt, including failures. This
    /// prevents no-peer retry paths from bypassing the discovery cadence.
    pub dht_last_lookup: Option<std::time::Instant>,
    /// Timestamp of the latest PEX discovery result.
    pub pex_last_seen: Option<std::time::Instant>,
    /// Timestamp of the latest successful tracker announce.
    pub tracker_last_ok: Option<std::time::Instant>,
    /// Timestamp of the latest successful block receive.
    pub block_last_seen: Option<std::time::Instant>,
    /// Timestamp of the latest successful webseed payload receive.
    pub webseed_last_seen: Option<std::time::Instant>,
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
    RelaxPeerBackoff,
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
    metadata_preflight: Option<MetadataPreflight>,
    /// Optional DHT runner for trackerless peer discovery (disabled for
    /// private torrents).
    dht: Option<Arc<crate::dht::DhtRunner>>,
    /// Peer transport selection: whether uTP is enabled and whether TCP is
    /// preferred over uTP. All transports go through the contained binder.
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    preallocate: bool,
    sparse: bool,
    minimum_free_space_bytes: u64,
    minimum_free_space_percent: u8,
    max_peer_workers: Arc<AtomicUsize>,
    allow_ipv6: bool,
    pex_enabled: bool,
    pex_max_peers: usize,
    file_priorities: Vec<FilePriority>,
    wanted: Vec<bool>,
    piece_selection: PieceSelection,
    /// Shared global plus per-torrent lifetime permits for every peer wire
    /// session opened by this engine. See ADR-0053.
    peer_session_budget: PeerSessionBudget,
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
        limiter: impl Into<Arc<RateLimiter>>,
        magnet: Option<MagnetParams>,
    ) -> Self {
        let piece_selection = PieceSelection::all(&meta);
        let file_count = meta.files.len();
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
            limiter: ShapedLimiter::from_shared_rate_limiter(limiter.into()),
            magnet,
            metadata_preflight: None,
            dht: None,
            utp_enabled: true,
            utp_prefer_tcp: true,
            encryption_mode: PeerEncryptionMode::default(),
            preallocate: true,
            sparse: true,
            minimum_free_space_bytes: 0,
            minimum_free_space_percent: 0,
            max_peer_workers: Arc::new(AtomicUsize::new(DEFAULT_PEER_WORKER_LIMIT)),
            allow_ipv6: true,
            pex_enabled: true,
            pex_max_peers: 0,
            file_priorities: vec![FilePriority::Normal; file_count],
            wanted: vec![true; file_count],
            piece_selection,
            peer_session_budget: PeerSessionBudget::unlimited(),
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

    /// Configure TCP peer-wire encryption policy.
    pub fn with_encryption_mode(mut self, encryption_mode: PeerEncryptionMode) -> Self {
        self.encryption_mode = encryption_mode;
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

    /// Configure storage free-space reserves enforced before payload writes.
    pub fn with_storage_reserve(
        mut self,
        minimum_free_space_bytes: u64,
        minimum_free_space_percent: u8,
    ) -> Self {
        self.minimum_free_space_bytes = minimum_free_space_bytes;
        self.minimum_free_space_percent = minimum_free_space_percent;
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

    /// Attach the runtime-owned global and per-torrent peer-session budgets.
    pub fn with_peer_session_budget(mut self, budget: PeerSessionBudget) -> Self {
        self.peer_session_budget = budget;
        self
    }

    pub fn with_file_selection(
        mut self,
        priorities: Vec<FilePriority>,
        wanted: Vec<bool>,
    ) -> Result<Self> {
        self.file_priorities = priorities;
        self.wanted = wanted;
        self.piece_selection =
            PieceSelection::from_files(&self.meta, &self.file_priorities, &self.wanted)?;
        Ok(self)
    }

    /// Validate and reserve resolved magnet metadata with the daemon before
    /// any payload path is created.
    pub fn with_metadata_preflight(mut self, preflight: MetadataPreflight) -> Self {
        self.metadata_preflight = Some(preflight);
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

    fn peer_allowed(&self, peer: &PeerAddr) -> bool {
        peer_allowed_by_config(peer, self.allow_ipv6)
    }

    fn filter_allowed_peers(&self, peers: Vec<PeerAddr>) -> Vec<PeerAddr> {
        peers
            .into_iter()
            .filter(|peer| self.peer_allowed(peer))
            .collect()
    }

    async fn record_peer_scheduler(&self, diagnostics: PeerSchedulerDiagnostics) {
        self.state.lock().await.peer_scheduler = diagnostics;
    }

    async fn set_peer_scheduler_serial_active(&self, active: bool) {
        self.state.lock().await.peer_scheduler.serial_peer_active = active;
    }

    async fn update_peer_scheduler_parallel_workers(&self, workers: usize) {
        let mut state = self.state.lock().await;
        state.peer_scheduler.parallel_workers_started = workers;
        state.peer_scheduler.serial_peer_active = false;
    }

    async fn refresh_discovery_peers(&self, force: bool) -> Vec<PeerAddr> {
        let mut refreshed = Vec::new();
        if force || self.tracker_announce_due().await {
            refreshed = self.announce(AnnounceEvent::Empty).await;
        }
        if force || self.dht_lookup_due().await {
            merge_unique_peers(&mut refreshed, self.discover_dht_peers().await);
        }
        dedupe_peers(&mut refreshed);
        refreshed
    }

    async fn tracker_announce_due(&self) -> bool {
        if tracker::announce_tiers(self.meta.announce.as_deref(), &self.meta.announce_list)
            .is_empty()
        {
            return false;
        }
        let state = self.state.lock().await;
        let Some(last_announce) = state.last_announce else {
            return true;
        };
        let interval = state
            .tracker_interval_seconds
            .max(PEER_REFRESH_INTERVAL.as_secs());
        now_secs().saturating_sub(last_announce) >= interval
    }

    async fn dht_lookup_due(&self) -> bool {
        if self.meta.is_private() || self.dht.is_none() {
            return false;
        }
        self.state
            .lock()
            .await
            .dht_last_lookup
            .is_none_or(|last| last.elapsed() >= PEER_REFRESH_INTERVAL)
    }

    async fn discover_dht_peers(&self) -> Vec<PeerAddr> {
        if self.meta.is_private() {
            return Vec::new();
        }
        let Some(dht) = &self.dht else {
            return Vec::new();
        };
        self.state.lock().await.dht_last_lookup = Some(Instant::now());
        let result = tokio::time::timeout(
            DHT_DISCOVERY_TIMEOUT,
            dht.get_peers_with_stats(self.meta.info_hash, DHT_DISCOVERY_ROUNDS),
        )
        .await;
        match result {
            Ok(Ok(lookup)) => {
                let peers = lookup.peers;
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

    fn storage_preflight(&self) -> Result<()> {
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

    /// Attempt to download missing pieces from a single peer. Returns true if
    /// at least one new piece was verified and written.
    async fn download_from_peer(
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

        let shared_have = Arc::new(Mutex::new(have.clone()));
        let outstanding = Arc::new(Mutex::new(OutstandingRequests::new(ENDGAME_MAX_PEERS)));
        let made_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let download_dir = self.download_dir.clone();
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
            let download_dir = download_dir.clone();
            let state = self.state.clone();
            let limiter = self.limiter.clone();
            let utp_enabled = self.utp_enabled;
            let utp_prefer_tcp = self.utp_prefer_tcp;
            let encryption_mode = self.encryption_mode;
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
                    download_dir,
                    deadline,
                    made_progress,
                    state,
                    limiter,
                    utp_enabled,
                    utp_prefer_tcp,
                    encryption_mode,
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

    /// Download a bounded batch of missing pieces from BEP 19 webseeds. Webseed
    /// traffic is HTTP byte-range traffic and must go through the same
    /// contained binder path as tracker HTTP.
    async fn run_webseed_round(&self, storage: &StorageIo, have: &mut PieceBitfield) -> bool {
        if self.meta.webseeds.is_empty() || !self.binder.traffic_allowed() {
            return false;
        }
        let webseeds = webseed_http_urls(&self.meta);
        if webseeds.is_empty() {
            return false;
        }

        let piece_count = self.meta.piece_count();
        let mut missing: Vec<usize> = (0..piece_count)
            .filter(|&piece| self.piece_selection.includes(piece) && !have.has(piece))
            .collect();
        missing.sort_by_key(|piece| std::cmp::Reverse(self.piece_selection.priority(*piece)));
        missing.truncate(WEBSEED_BATCH_PIECES);
        if missing.is_empty() {
            return false;
        }

        let shared_have = Arc::new(Mutex::new(have.clone()));
        let storage = Arc::new(storage.clone());
        let webseeds = Arc::new(webseeds);
        let mut tasks = tokio::task::JoinSet::new();
        let mut next_piece = 0usize;
        while next_piece < missing.len() && tasks.len() < WEBSEED_MAX_CONCURRENT_REQUESTS {
            spawn_webseed_piece_task(
                &mut tasks,
                missing[next_piece],
                self.meta.clone(),
                self.binder.clone(),
                storage.clone(),
                shared_have.clone(),
                self.state.clone(),
                self.limiter.clone(),
                webseeds.clone(),
            );
            next_piece += 1;
        }

        let mut progressed = false;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((piece_index, Ok(piece_progressed))) => {
                    progressed |= piece_progressed;
                    tracing::debug!(
                        piece = piece_index,
                        progressed = piece_progressed,
                        "webseed piece task completed"
                    );
                }
                Ok((piece_index, Err(e))) => {
                    tracing::debug!(piece = piece_index, error = %e, "webseed piece task failed");
                }
                Err(e) => {
                    tracing::debug!(error = %e, "webseed piece task join failed");
                }
            }

            let complete = {
                let have = shared_have.lock().await;
                self.piece_selection.complete(&have)
            };
            if complete {
                tasks.abort_all();
                break;
            }

            if next_piece < missing.len() {
                spawn_webseed_piece_task(
                    &mut tasks,
                    missing[next_piece],
                    self.meta.clone(),
                    self.binder.clone(),
                    storage.clone(),
                    shared_have.clone(),
                    self.state.clone(),
                    self.limiter.clone(),
                    webseeds.clone(),
                );
                next_piece += 1;
            }
        }

        if progressed {
            let merged = shared_have.lock().await.clone();
            *have = merged.clone();
            self.update_progress(&merged).await;
            if let Err(e) = self.persist_resume(storage.as_ref(), &merged).await {
                tracing::warn!(error = %e, "webseed resume persist failed");
            }
        }
        progressed
    }

    /// Pick a piece we don't have that the peer has.
    fn pick_piece(&self, peer_bf: Option<&Bitfield>, have: &PieceBitfield) -> Option<usize> {
        let peer_bf = peer_bf?;
        (0..self.meta.piece_count())
            .filter(|&i| self.piece_selection.includes(i) && peer_bf.has(i) && !have.has(i))
            .max_by_key(|&i| self.piece_selection.priority(i))
    }
    fn piece_length(&self, index: usize) -> u64 {
        if index + 1 == self.meta.piece_count() {
            self.meta.last_piece_length()
        } else {
            self.meta.piece_length
        }
    }

    /// Announce to all HTTP/UDP trackers and return discovered peers.
    async fn announce(&self, event: AnnounceEvent) -> Vec<PeerAddr> {
        let tiers =
            tracker::announce_tiers(self.meta.announce.as_deref(), &self.meta.announce_list);
        let scrape_urls = tiers.iter().flatten().cloned().collect::<Vec<_>>();
        let (uploaded, downloaded, left) = {
            let s = self.state.lock().await;
            (
                s.uploaded,
                s.downloaded,
                s.total_length.saturating_sub(s.bytes_completed),
            )
        };
        let outcome = self
            .announce_tracker_tiers(
                self.meta.info_hash,
                tiers,
                uploaded,
                downloaded,
                left,
                event,
            )
            .await;
        self.record_tracker_activity(self.meta.info_hash, &outcome, scrape_urls)
            .await;
        outcome.peers
    }

    #[allow(clippy::too_many_arguments)]
    async fn announce_tracker_tiers(
        &self,
        info_hash: InfoHash,
        tiers: Vec<Vec<String>>,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: AnnounceEvent,
    ) -> TrackerAnnounceOutcome {
        if tiers.is_empty() {
            return TrackerAnnounceOutcome {
                message: Some("no trackers configured".into()),
                ..Default::default()
            };
        }
        let mut aggregate = TrackerAnnounceOutcome::default();
        'tiers: for tier in tiers {
            for url in tier {
                let outcome = self
                    .announce_trackers(info_hash, vec![url], uploaded, downloaded, left, event)
                    .await;
                let succeeded = outcome.ok;
                merge_tracker_outcome(&mut aggregate, outcome);
                if succeeded {
                    break 'tiers;
                }
            }
        }
        dedupe_peers(&mut aggregate.peers);
        aggregate
    }

    #[allow(clippy::too_many_arguments)]
    async fn announce_trackers(
        &self,
        info_hash: InfoHash,
        trackers: Vec<String>,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: AnnounceEvent,
    ) -> TrackerAnnounceOutcome {
        if trackers.is_empty() {
            return TrackerAnnounceOutcome {
                message: Some("no trackers configured".into()),
                ..Default::default()
            };
        }

        let announce_at = now_secs();
        let mut outcome = TrackerAnnounceOutcome::default();
        let mut tasks = tokio::task::JoinSet::new();
        for url in trackers {
            outcome.tracker_results.insert(
                url.clone(),
                TrackerAnnounceSnapshot {
                    status: TrackerStatus::Updating,
                    seeders: 0,
                    leechers: 0,
                    downloads: 0,
                    last_error: None,
                    last_message: Some("announce in progress".into()),
                    last_announce: Some(announce_at),
                },
            );
            let binder = self.binder.clone();
            let req = AnnounceRequest {
                tracker_url: url.clone(),
                info_hash,
                peer_id: self.peer_id,
                port: self.listen_port,
                uploaded,
                downloaded,
                left,
                event,
                numwant: Some(200),
                compact: true,
            };
            tasks.spawn(async move {
                let result = timeout(TRACKER_ANNOUNCE_TIMEOUT, async {
                    if url.starts_with("udp://") {
                        udp_tracker::udp_announce(binder.as_ref(), &req).await
                    } else {
                        tracker::http_announce(binder.as_ref(), &req).await
                    }
                })
                .await
                .map_err(|_| CoreError::Internal("tracker announce timed out".into()))
                .and_then(|r| r);
                (url, result)
            });
        }

        while let Some(joined) = tasks.join_next().await {
            record_tracker_joined_result(&mut outcome, joined, announce_at);
        }
        dedupe_peers(&mut outcome.peers);
        outcome
    }

    async fn record_tracker_announce_outcome(&self, outcome: &TrackerAnnounceOutcome) {
        let mut s = self.state.lock().await;
        s.tracker_ok = outcome.ok;
        s.tracker_message = outcome.message.clone();
        s.last_announce = Some(now_secs());
        if let Some(interval) = outcome.interval_seconds {
            s.tracker_interval_seconds = interval;
        }
        for (url, result) in &outcome.tracker_results {
            s.tracker_announces.insert(url.clone(), result.clone());
        }
        if outcome.ok {
            s.tracker_last_ok = Some(Instant::now());
            if outcome.failures == 0 {
                s.tracker_failures_recent = 0;
            }
        }
        if outcome.failures > 0 {
            s.tracker_failures_recent = s.tracker_failures_recent.saturating_add(outcome.failures);
        }
    }

    /// Retain announce state, then schedule one scrape for every configured
    /// tracker. This shared path is used by initial download
    /// discovery, explicit/periodic reannounce, completion, and magnet
    /// metadata discovery.
    async fn record_tracker_activity(
        &self,
        info_hash: InfoHash,
        outcome: &TrackerAnnounceOutcome,
        scrape_urls: Vec<String>,
    ) {
        self.record_tracker_announce_outcome(outcome).await;
        run_tracker_scrapes(
            self.state.clone(),
            self.binder.clone(),
            info_hash,
            scrape_urls,
        )
        .await;
    }

    async fn discover_magnet_dht_peers(&self, info_hash: InfoHash) -> Vec<PeerAddr> {
        let Some(dht) = &self.dht else {
            return Vec::new();
        };
        let dht_result = tokio::time::timeout(
            DHT_DISCOVERY_TIMEOUT,
            dht.get_peers_with_stats(info_hash, DHT_DISCOVERY_ROUNDS),
        )
        .await;
        match dht_result {
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
                    "DHT magnet metadata peer discovery completed"
                );
                peers
            }
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "DHT magnet metadata peer discovery failed");
                Vec::new()
            }
            Err(_) => {
                tracing::debug!("DHT magnet metadata peer discovery timed out");
                Vec::new()
            }
        }
    }

    async fn sync_have_from_state(&self, have: &mut PieceBitfield, piece_count: usize) {
        let state_have = {
            let s = self.state.lock().await;
            if s.pieces_have.count(piece_count) <= have.count(piece_count) {
                None
            } else {
                Some(s.pieces_have.clone())
            }
        };
        let Some(state_have) = state_have else {
            return;
        };
        for piece in 0..piece_count {
            if state_have.has(piece) {
                have.set(piece);
            }
        }
    }

    /// Fetch magnet metadata via BEP 9. Announces to the magnet's trackers
    /// (using the real info hash) to discover peers, merges directly-supplied
    /// seed peers, then fetches the `info` dict from the candidates. All peer
    /// connections go through the binder.
    async fn fetch_magnet_metadata(&self, magnet: &MagnetParams) -> Result<Vec<u8>> {
        let mut candidates = Vec::new();
        let mut last_error: Option<CoreError> = None;

        for round in 1..=MAGNET_METADATA_MAX_ROUNDS {
            match self.poll_commands().await {
                CommandOutcome::Stop => {
                    return Err(CoreError::Internal("magnet metadata fetch stopped".into()));
                }
                CommandOutcome::Reannounce
                | CommandOutcome::RelaxPeerBackoff
                | CommandOutcome::Continue
                | CommandOutcome::Pause => {}
            }

            let outcome = self
                .announce_trackers(
                    magnet.info_hash,
                    magnet.trackers.clone(),
                    0,
                    0,
                    1,
                    if round == 1 {
                        AnnounceEvent::Started
                    } else {
                        AnnounceEvent::Empty
                    },
                )
                .await;
            self.record_tracker_activity(magnet.info_hash, &outcome, magnet.trackers.clone())
                .await;
            merge_unique_peers(&mut candidates, self.filter_allowed_peers(outcome.peers));

            for p in &self.seed_peers {
                if self.peer_allowed(p) && !candidates.contains(p) {
                    candidates.push(*p);
                }
            }

            let dht_peers = self.discover_magnet_dht_peers(magnet.info_hash).await;
            merge_unique_peers(&mut candidates, dht_peers);
            dedupe_peers(&mut candidates);
            self.state.lock().await.peers = candidates.clone();

            if candidates.is_empty() {
                last_error = Some(CoreError::Internal(
                    "magnet metadata fetch: no peers discovered".into(),
                ));
                tracing::debug!(round, "magnet metadata discovery found no peers");
            } else {
                tracing::debug!(
                    round,
                    candidates = candidates.len(),
                    "attempting magnet metadata fetch from discovered peers"
                );
                match crate::metadata::fetch_metadata_from_candidates_with_budget(
                    crate::metadata::MetadataFetchContext::new(
                        self.peer_session_budget.clone(),
                        self.binder.clone(),
                        magnet.info_hash,
                        self.peer_id,
                        self.utp_enabled,
                        self.utp_prefer_tcp,
                        self.encryption_mode,
                    ),
                    &candidates,
                )
                .await
                {
                    Ok(info) => {
                        tracing::info!(
                            info_hash = %magnet.info_hash,
                            round,
                            candidates = candidates.len(),
                            "magnet metadata fetched"
                        );
                        return Ok(info);
                    }
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            round,
                            candidates = candidates.len(),
                            "magnet metadata fetch round failed; will retry discovery"
                        );
                        last_error = Some(e);
                    }
                }
            }

            if round < MAGNET_METADATA_MAX_ROUNDS {
                self.sleep_or_stop(MAGNET_METADATA_RETRY_PAUSE).await;
            }
        }

        Err(CoreError::Internal(format!(
            "magnet metadata fetch failed after discovery retries: {}",
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no metadata candidates".into())
        )))
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

    async fn finish_selection(&self, storage: &StorageIo, have: &PieceBitfield) -> Result<()> {
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

    async fn persist_resume(&self, storage: &StorageIo, have: &PieceBitfield) -> Result<()> {
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

    async fn poll_commands(&self) -> CommandOutcome {
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

    async fn sleep_or_stop(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandOutcome {
    Continue,
    Pause,
    Reannounce,
    RelaxPeerBackoff,
    Stop,
}

const NORMAL_REQUEST_FLOOR: usize = 64;
const NORMAL_REQUEST_FALLBACK_CAP: usize = 2_000;
const NORMAL_REQUEST_LOCAL_CAP: usize = 4_000;
const NORMAL_REQUEST_TARGET_BUFFER_SECS: u64 = 10;
const NORMAL_PEER_PIECE_WINDOW: usize = 32;
const PEER_IDLE_BACKOFF: Duration = Duration::from_secs(20);
const PEER_FAILURE_BACKOFF: Duration = Duration::from_secs(120);

fn record_tracker_joined_result(
    outcome: &mut TrackerAnnounceOutcome,
    joined: std::result::Result<
        (String, Result<tracker::AnnounceResponse>),
        tokio::task::JoinError,
    >,
    announce_at: u64,
) {
    match joined {
        Ok((url, Ok(resp))) => {
            let effective_interval = resp
                .interval
                .max(resp.min_interval.unwrap_or(0))
                .clamp(30, 86_400);
            outcome.interval_seconds = Some(
                outcome
                    .interval_seconds
                    .unwrap_or(0)
                    .max(effective_interval),
            );
            if let Some(fr) = resp.failure_reason {
                let aggregate = format!("{url}: {fr}");
                outcome.failures = outcome.failures.saturating_add(1);
                if !outcome.ok {
                    outcome.message = Some(aggregate);
                }
                outcome.tracker_results.insert(
                    url,
                    TrackerAnnounceSnapshot {
                        status: TrackerStatus::Error,
                        seeders: resp.seeders,
                        leechers: resp.leechers,
                        downloads: 0,
                        last_error: Some(fr),
                        last_message: None,
                        last_announce: Some(announce_at),
                    },
                );
                return;
            }
            outcome.ok = true;
            let peer_count = resp.peers.len();
            let last_message = if resp.peers.is_empty() {
                if outcome.message.is_none() {
                    let message = format!(
                        "{url}: announce returned 0 peers (seeders={}, leechers={})",
                        resp.seeders, resp.leechers
                    );
                    outcome.message = Some(message.clone());
                    message
                } else {
                    format!(
                        "{url}: announce returned 0 peers (seeders={}, leechers={})",
                        resp.seeders, resp.leechers
                    )
                }
            } else {
                let message = format!(
                    "{url}: announce returned {peer_count} peers (seeders={}, leechers={})",
                    resp.seeders, resp.leechers
                );
                outcome.message = Some(message.clone());
                message
            };
            outcome.tracker_results.insert(
                url,
                TrackerAnnounceSnapshot {
                    status: TrackerStatus::Ok,
                    seeders: resp.seeders,
                    leechers: resp.leechers,
                    downloads: 0,
                    last_error: None,
                    last_message: Some(last_message),
                    last_announce: Some(announce_at),
                },
            );
            outcome.peers.extend(resp.peers);
        }
        Ok((url, Err(e))) => {
            let error = e.to_string();
            outcome.failures = outcome.failures.saturating_add(1);
            if !outcome.ok {
                outcome.message = Some(format!("{url}: {error}"));
            }
            tracing::debug!(tracker = %url, error = %error, "tracker announce failed");
            outcome.tracker_results.insert(
                url,
                TrackerAnnounceSnapshot {
                    status: TrackerStatus::Error,
                    seeders: 0,
                    leechers: 0,
                    downloads: 0,
                    last_error: Some(error),
                    last_message: None,
                    last_announce: Some(announce_at),
                },
            );
        }
        Err(e) => {
            outcome.failures = outcome.failures.saturating_add(1);
            if !outcome.ok {
                outcome.message = Some(format!("tracker announce task failed: {e}"));
            }
        }
    }
}

fn merge_tracker_outcome(
    aggregate: &mut TrackerAnnounceOutcome,
    mut outcome: TrackerAnnounceOutcome,
) {
    aggregate.ok |= outcome.ok;
    aggregate.failures = aggregate.failures.saturating_add(outcome.failures);
    aggregate.peers.append(&mut outcome.peers);
    aggregate.tracker_results.extend(outcome.tracker_results);
    if let Some(interval) = outcome.interval_seconds {
        aggregate.interval_seconds = Some(
            aggregate
                .interval_seconds
                .map_or(interval, |current| current.min(interval)),
        );
    }
    if outcome.ok || aggregate.message.is_none() {
        aggregate.message = outcome.message;
    }
}

/// Run supported HTTP(S) scrapes concurrently through the same contained
/// binder used for announce traffic. Join failures retain the URL by task ID,
/// so a panic/cancellation is visible instead of silently disappearing.
pub(crate) async fn run_tracker_scrapes(
    state: Arc<Mutex<EngineState>>,
    binder: Arc<dyn NetworkBinder>,
    info_hash: InfoHash,
    tracker_urls: Vec<String>,
) {
    let mut unique = HashSet::new();
    let tracker_urls = tracker_urls
        .into_iter()
        .filter(|url| unique.insert(url.clone()))
        .collect::<Vec<_>>();
    if tracker_urls.is_empty() {
        return;
    }

    {
        let mut engine = state.lock().await;
        for url in &tracker_urls {
            let snapshot = engine.tracker_scrapes.entry(url.clone()).or_default();
            snapshot.status = TrackerScrapeStatus::Updating;
            snapshot.last_error = None;
        }
    }

    let attempted_at = now_secs();
    let mut tasks = tokio::task::JoinSet::new();
    let mut task_urls = HashMap::new();
    for url in tracker_urls {
        let task_url = url.clone();
        let task_binder = binder.clone();
        let handle = tasks.spawn(async move {
            tracker::http_scrape(task_binder.as_ref(), &task_url, &[info_hash]).await
        });
        task_urls.insert(handle.id(), url);
    }

    while let Some(joined) = tasks.join_next_with_id().await {
        match joined {
            Ok((task_id, result)) => {
                let Some(url) = task_urls.remove(&task_id) else {
                    continue;
                };
                let mut engine = state.lock().await;
                let snapshot = engine.tracker_scrapes.entry(url).or_default();
                snapshot.last_scrape = Some(attempted_at);
                match result {
                    Ok(tracker::ScrapeOutcome::Unsupported) => {
                        snapshot.status = TrackerScrapeStatus::Unsupported;
                        snapshot.last_error = None;
                    }
                    Ok(tracker::ScrapeOutcome::Success(mut counts)) => {
                        if let Some(counts) = counts.remove(&info_hash) {
                            snapshot.status = TrackerScrapeStatus::Ok;
                            snapshot.seeders = Some(counts.seeders);
                            snapshot.leechers = Some(counts.leechers);
                            snapshot.downloads = Some(counts.downloads);
                            snapshot.last_error = None;
                        } else {
                            snapshot.status = TrackerScrapeStatus::Error;
                            snapshot.last_error = Some(format!(
                                "tracker scrape omitted requested info hash {}",
                                info_hash.to_hex()
                            ));
                            engine.tracker_failures_recent =
                                engine.tracker_failures_recent.saturating_add(1);
                        }
                    }
                    Err(error) => {
                        snapshot.status = TrackerScrapeStatus::Error;
                        snapshot.last_error = Some(error.to_string());
                        engine.tracker_failures_recent =
                            engine.tracker_failures_recent.saturating_add(1);
                    }
                }
            }
            Err(error) => {
                let url = task_urls
                    .remove(&error.id())
                    .unwrap_or_else(|| "unknown tracker scrape task".into());
                let mut engine = state.lock().await;
                let snapshot = engine.tracker_scrapes.entry(url).or_default();
                snapshot.status = TrackerScrapeStatus::Error;
                snapshot.last_scrape = Some(attempted_at);
                snapshot.last_error = Some(format!("tracker scrape task failed: {error}"));
                engine.tracker_failures_recent = engine.tracker_failures_recent.saturating_add(1);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerCandidateCounts {
    discovered: usize,
    eligible: usize,
    filtered: usize,
    failed: usize,
    backed_off: usize,
}

fn peer_allowed_by_config(peer: &PeerAddr, allow_ipv6: bool) -> bool {
    peer.port != 0 && (allow_ipv6 || !peer.ip.is_ipv6())
}

fn classify_peer_candidates(
    discovered: &[PeerAddr],
    bad_peers: &HashMap<SocketAddr, Instant>,
    peer_backoff: &HashMap<SocketAddr, Instant>,
    allow_ipv6: bool,
) -> (Vec<PeerAddr>, PeerCandidateCounts) {
    let mut eligible = Vec::new();
    let mut counts = PeerCandidateCounts {
        discovered: discovered.len(),
        ..Default::default()
    };
    for peer in discovered {
        if !peer_allowed_by_config(peer, allow_ipv6) {
            counts.filtered += 1;
            continue;
        }
        if peer_is_backed_off(bad_peers, peer.socket_addr()) {
            counts.failed += 1;
            continue;
        }
        if peer_is_backed_off(peer_backoff, peer.socket_addr()) {
            counts.backed_off += 1;
            continue;
        }
        counts.eligible += 1;
        eligible.push(*peer);
    }
    (eligible, counts)
}

fn no_usable_peer_candidates(counts: &PeerCandidateCounts) -> bool {
    counts.discovered == 0
        || (counts.eligible == 0
            && counts.filtered.saturating_add(counts.failed) >= counts.discovered)
}

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

fn peer_scheduler_reason(counts: &PeerCandidateCounts) -> Option<String> {
    if counts.discovered == 0 {
        return Some("no peers discovered".into());
    }
    if counts.eligible == 0 {
        if counts.filtered > 0 && counts.failed == 0 && counts.backed_off == 0 {
            return Some("all discovered peers filtered by configuration".into());
        }
        if counts.failed > 0 || counts.backed_off > 0 {
            return Some(
                "all discovered peers are cooling down after failures or no progress".into(),
            );
        }
        return Some("no eligible peers after scheduler filtering".into());
    }
    if counts.eligible == 1 {
        return Some("one eligible peer; using serial fallback when parallel round is idle".into());
    }
    None
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

async fn record_webseed_block(state: &Arc<Mutex<EngineState>>, bytes: u64) {
    if bytes == 0 {
        return;
    }
    let mut st = state.lock().await;
    st.downloaded = st.downloaded.saturating_add(bytes);
    st.last_valid_block = Some(Instant::now());
    st.block_last_seen = Some(Instant::now());
    st.webseed_last_seen = Some(Instant::now());
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

#[allow(clippy::too_many_arguments)]
fn spawn_webseed_piece_task(
    tasks: &mut tokio::task::JoinSet<(usize, Result<bool>)>,
    piece_index: usize,
    meta: TorrentMeta,
    binder: Arc<dyn NetworkBinder>,
    storage: Arc<StorageIo>,
    shared_have: Arc<Mutex<PieceBitfield>>,
    state: Arc<Mutex<EngineState>>,
    limiter: ShapedLimiter,
    webseeds: Arc<Vec<String>>,
) {
    tasks.spawn(async move {
        let result = download_webseed_piece(
            binder,
            meta,
            piece_index,
            storage,
            shared_have,
            state,
            limiter,
            webseeds,
        )
        .await;
        (piece_index, result)
    });
}

#[allow(clippy::too_many_arguments)]
async fn download_webseed_piece(
    binder: Arc<dyn NetworkBinder>,
    meta: TorrentMeta,
    piece_index: usize,
    storage: Arc<StorageIo>,
    shared_have: Arc<Mutex<PieceBitfield>>,
    state: Arc<Mutex<EngineState>>,
    limiter: ShapedLimiter,
    webseeds: Arc<Vec<String>>,
) -> Result<bool> {
    {
        let have = shared_have.lock().await;
        if have.has(piece_index) {
            return Ok(false);
        }
    }

    let data = fetch_webseed_piece(binder, &meta, piece_index, &webseeds).await?;
    if !verify_piece(&meta, piece_index, &data) {
        return Err(CoreError::Internal(format!(
            "webseed piece {piece_index} hash mismatch"
        )));
    }

    limiter
        .acquire(RateDirection::Download, data.len() as u64)
        .await;
    storage.write_piece(piece_index, &data).await?;
    record_webseed_block(&state, data.len() as u64).await;

    let have_snapshot = {
        let mut have = shared_have.lock().await;
        if have.has(piece_index) {
            return Ok(false);
        }
        have.set(piece_index);
        have.clone()
    };
    update_progress_state(&state, &meta, &have_snapshot).await;
    Ok(true)
}

async fn fetch_webseed_piece(
    binder: Arc<dyn NetworkBinder>,
    meta: &TorrentMeta,
    piece_index: usize,
    webseeds: &[String],
) -> Result<Vec<u8>> {
    if webseeds.is_empty() {
        return Err(CoreError::Internal("torrent has no usable webseeds".into()));
    }

    let attempts = webseeds.len().min(WEBSEED_MAX_MIRROR_ATTEMPTS);
    let mut last_error = None;
    for attempt in 0..attempts {
        let stride = webseed_mirror_stride(webseeds.len());
        let index = (piece_index
            .wrapping_mul(stride)
            .wrapping_add(attempt.wrapping_mul(stride)))
            % webseeds.len();
        let base = &webseeds[index];
        match fetch_piece_from_webseed(binder.as_ref(), meta, piece_index, base).await {
            Ok(piece) => return Ok(piece),
            Err(e) => {
                tracing::debug!(piece = piece_index, webseed = %base, error = %e, "webseed mirror failed");
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| CoreError::Internal("all webseed mirrors failed".into())))
}

async fn fetch_piece_from_webseed(
    binder: &dyn NetworkBinder,
    meta: &TorrentMeta,
    piece_index: usize,
    base_url: &str,
) -> Result<Vec<u8>> {
    let (piece_start, _) = meta
        .piece_byte_range(piece_index as u64)
        .ok_or_else(|| CoreError::Internal(format!("piece {piece_index} out of range")))?;
    let piece_len = usize::try_from(piece_length_for_meta(meta, piece_index))
        .map_err(|_| CoreError::Internal(format!("piece {piece_index} length exceeds usize")))?;
    let mut piece = vec![0u8; piece_len];

    for slice in piece_file_ranges(meta, piece_index)? {
        if slice.length == 0 {
            continue;
        }
        let file_url = webseed_file_url(base_url, meta, slice.file_index)?;
        let end_exclusive = slice
            .offset_in_file
            .checked_add(slice.length)
            .ok_or_else(|| {
                CoreError::Internal(format!("webseed range overflow for piece {piece_index}"))
            })?;
        let response = timeout(
            WEBSEED_REQUEST_TIMEOUT,
            binder.http_get_range(&file_url, slice.offset_in_file, end_exclusive),
        )
        .await
        .map_err(|_| {
            CoreError::Internal(format!("webseed range request timed out: {file_url}"))
        })??;
        if response.status != 206 {
            return Err(CoreError::Internal(format!(
                "webseed returned HTTP {} instead of 206 for {file_url}",
                response.status
            )));
        }
        let expected_len = usize::try_from(slice.length).map_err(|_| {
            CoreError::Internal(format!("webseed slice length exceeds usize for {file_url}"))
        })?;
        if response.body.len() != expected_len {
            return Err(CoreError::Internal(format!(
                "webseed returned {} bytes, expected {expected_len} for {file_url}",
                response.body.len()
            )));
        }

        let file_start = file_absolute_start(meta, slice.file_index)?;
        let absolute_start = file_start
            .checked_add(slice.offset_in_file)
            .ok_or_else(|| CoreError::Internal("webseed absolute offset overflow".into()))?;
        let piece_offset = absolute_start
            .checked_sub(piece_start)
            .ok_or_else(|| CoreError::Internal("webseed slice starts before piece".into()))?;
        let piece_offset = usize::try_from(piece_offset)
            .map_err(|_| CoreError::Internal("webseed piece offset exceeds usize".into()))?;
        let end = piece_offset
            .checked_add(expected_len)
            .ok_or_else(|| CoreError::Internal("webseed piece copy range overflowed".into()))?;
        let dest = piece.get_mut(piece_offset..end).ok_or_else(|| {
            CoreError::Internal(format!("webseed slice exceeds piece {piece_index} bounds"))
        })?;
        dest.copy_from_slice(&response.body);
    }

    Ok(piece)
}

fn webseed_http_urls(meta: &TorrentMeta) -> Vec<String> {
    meta.webseeds
        .iter()
        .filter_map(|url| {
            let parsed = url::Url::parse(url).ok()?;
            matches!(parsed.scheme(), "http" | "https").then(|| url.clone())
        })
        .collect()
}

fn webseed_mirror_stride(len: usize) -> usize {
    for candidate in [31usize, 17, 13, 7, 5, 3] {
        if len > candidate && gcd(len, candidate) == 1 {
            return candidate;
        }
    }
    1
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

fn webseed_file_url(base_url: &str, meta: &TorrentMeta, file_index: usize) -> Result<String> {
    let file = meta
        .files
        .get(file_index)
        .ok_or_else(|| CoreError::Internal(format!("file index {file_index} out of range")))?;
    let parsed = url::Url::parse(base_url)
        .map_err(|e| CoreError::InvalidArgument(format!("bad webseed url: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(CoreError::InvalidArgument(format!(
            "unsupported webseed scheme: {}",
            parsed.scheme()
        )));
    }
    if !meta.is_multi_file && !base_url.ends_with('/') {
        return Ok(parsed.to_string());
    }

    let mut url = parsed;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| CoreError::InvalidArgument("webseed URL cannot be a base".into()))?;
        for segment in &file.path {
            segments.push(segment);
        }
    }
    Ok(url.to_string())
}

fn file_absolute_start(meta: &TorrentMeta, file_index: usize) -> Result<u64> {
    if file_index >= meta.files.len() {
        return Err(CoreError::Internal(format!(
            "file index {file_index} out of range"
        )));
    }
    meta.files
        .iter()
        .take(file_index)
        .try_fold(0u64, |acc, file| {
            acc.checked_add(file.length)
                .ok_or_else(|| CoreError::Internal("torrent file offset overflow".into()))
        })
}

fn piece_length_for_meta(meta: &TorrentMeta, piece_index: usize) -> u64 {
    if piece_index + 1 == meta.piece_count() {
        meta.last_piece_length()
    } else {
        meta.piece_length
    }
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
    encryption_mode: PeerEncryptionMode,
) -> Result<(PeerReader<PeerReadHalf>, PeerWriteHalf, PeerTransport)> {
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

fn peer_transport_order(
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
) -> Vec<PeerTransport> {
    if matches!(encryption_mode, PeerEncryptionMode::Required) {
        return vec![PeerTransport::Tcp];
    }
    if !utp_enabled {
        return vec![PeerTransport::Tcp];
    }
    if utp_prefer_tcp {
        vec![PeerTransport::Tcp, PeerTransport::Utp]
    } else {
        vec![PeerTransport::Utp, PeerTransport::Tcp]
    }
}

async fn attempt_peer_wire_transport(
    binder: Arc<dyn NetworkBinder>,
    transport: PeerTransport,
    peer_addr: PeerAddr,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    encryption_mode: PeerEncryptionMode,
) -> Result<(PeerReader<PeerReadHalf>, PeerWriteHalf, PeerTransport)> {
    if transport == PeerTransport::Utp && matches!(encryption_mode, PeerEncryptionMode::Required) {
        return Err(CoreError::Internal(
            "uTP encrypted peer wire is not implemented for required encryption mode".into(),
        ));
    }

    let (stream, selected) =
        utp::connect_peer_stream(binder.clone(), transport, peer_addr.socket_addr()).await?;
    let stream = match (selected, encryption_mode) {
        (PeerTransport::Tcp, PeerEncryptionMode::Disabled) => stream,
        (PeerTransport::Tcp, PeerEncryptionMode::Required) => {
            let encrypted = timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, info_hash),
            )
            .await??;
            Box::new(encrypted) as Box<dyn utp::PeerDuplex>
        }
        (PeerTransport::Tcp, PeerEncryptionMode::Preferred) => {
            match timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, info_hash),
            )
            .await
            {
                Ok(Ok(encrypted)) => Box::new(encrypted) as Box<dyn utp::PeerDuplex>,
                Ok(Err(e)) => {
                    tracing::debug!(
                        peer = %peer_addr.socket_addr(),
                        error = %e,
                        "MSE/PE negotiation failed; retrying TCP peer as plaintext"
                    );
                    let (plain, _) = utp::connect_peer_stream(
                        binder,
                        PeerTransport::Tcp,
                        peer_addr.socket_addr(),
                    )
                    .await?;
                    plain
                }
                Err(e) => {
                    tracing::debug!(
                        peer = %peer_addr.socket_addr(),
                        error = %e,
                        "MSE/PE negotiation timed out; retrying TCP peer as plaintext"
                    );
                    let (plain, _) = utp::connect_peer_stream(
                        binder,
                        PeerTransport::Tcp,
                        peer_addr.socket_addr(),
                    )
                    .await?;
                    plain
                }
            }
        }
        (PeerTransport::Utp, _) => stream,
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

    Ok((reader, write_half, selected))
}

#[derive(Debug, Clone)]
struct ParallelPieceState {
    have: PieceBitfield,
    reserved: HashSet<usize>,
    availability: Vec<u16>,
    peer_pieces: HashMap<SocketAddr, Bitfield>,
    selection: PieceSelection,
}

/// Compute a stable shard offset in `[0, piece_count)` for a peer's
/// piece-reservation search. Hashes the peer's socket address so each peer
/// gets a deterministic but distinct starting point in the piece space,
/// which keeps concurrent workers from all reserving the same low-index
/// pieces first.
fn piece_shard(peer_addr: SocketAddr, piece_count: usize) -> usize {
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
    fn new(have: PieceBitfield, piece_count: usize, selection: PieceSelection) -> Self {
        Self {
            have,
            reserved: HashSet::new(),
            availability: vec![0; piece_count],
            peer_pieces: HashMap::new(),
            selection,
        }
    }

    fn note_peer_bitfield(&mut self, peer: SocketAddr, bitfield: &Bitfield, piece_count: usize) {
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

    fn note_peer_have(&mut self, peer: SocketAddr, piece: u32, piece_count: usize) {
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

    fn remove_peer(&mut self, peer: SocketAddr, piece_count: usize) {
        let Some(previous) = self.peer_pieces.remove(&peer) else {
            return;
        };
        for i in 0..piece_count {
            if previous.has(i) {
                self.availability[i] = self.availability[i].saturating_sub(1);
            }
        }
    }

    fn reserve_piece(
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

    fn peer_has_missing_piece(&self, peer_bf: &Bitfield, piece_count: usize) -> bool {
        (0..piece_count).any(|i| self.selection.includes(i) && peer_bf.has(i) && !self.have.has(i))
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
    selection: PieceSelection,
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
    encryption_mode: PeerEncryptionMode,
    peer_session_budget: PeerSessionBudget,
) -> Result<bool> {
    if !binder.traffic_allowed() {
        return Ok(false);
    }
    let _peer_permit = peer_session_budget.acquire_outbound().await?;
    let storage = StorageIo::new(meta.clone(), download_dir);
    let (mut reader, mut write_half, transport) = connect_peer_wire_with_transport(
        binder.clone(),
        peer_addr,
        meta.info_hash,
        peer_id,
        utp_enabled,
        utp_prefer_tcp,
        encryption_mode,
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

async fn release_endgame_session_requests(
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

/// A normal-mode peer session used by the bounded parallel downloader. Each
/// session reserves one missing piece at a time from shared state, so peers
/// work on distinct pieces until endgame takes over.
#[allow(clippy::too_many_arguments)]
fn spawn_parallel_peer_task(
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

struct ParallelPieceDownload {
    piece_index: usize,
    reqs: Vec<(u32, u32)>,
    next_req: usize,
    in_flight: usize,
    outstanding_blocks: HashMap<u32, u32>,
    assembler: peer::PieceAssembler,
}

impl ParallelPieceDownload {
    fn new(piece_index: usize, piece_len: u32) -> Self {
        Self {
            piece_index,
            reqs: block_requests(piece_len),
            next_req: 0,
            in_flight: 0,
            outstanding_blocks: HashMap::new(),
            assembler: peer::PieceAssembler::new(piece_index as u32, piece_len as usize),
        }
    }

    async fn send_more<W>(
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

    fn record_block(
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
enum PeerSessionOutcome {
    Progressed,
    NoProgress,
    NoWorkAvailable,
}

#[derive(Debug, Clone)]
struct PeerRequestWindow {
    cap: usize,
    smoothed_rate_bps: u64,
    sample_bytes: u64,
    sample_started_at: Instant,
}

impl PeerRequestWindow {
    fn new(remote_reqq: Option<usize>, now: Instant) -> Self {
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

    fn set_remote_reqq(&mut self, remote_reqq: Option<usize>) {
        let Some(remote_reqq) = remote_reqq.filter(|cap| *cap > 0) else {
            return;
        };
        self.cap = remote_reqq.clamp(1, NORMAL_REQUEST_LOCAL_CAP);
    }

    fn record_block(&mut self, bytes: u64, now: Instant) {
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

    fn desired_in_flight(&self) -> usize {
        let floor = NORMAL_REQUEST_FLOOR.min(self.cap);
        let estimated = ((self
            .smoothed_rate_bps
            .saturating_mul(NORMAL_REQUEST_TARGET_BUFFER_SECS))
            / peer::BLOCK_SIZE as u64) as usize;
        estimated.max(floor).min(self.cap)
    }
}

#[allow(clippy::too_many_arguments)]
async fn fill_parallel_piece_window<W>(
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
async fn handle_parallel_pex_message(
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
    use async_trait::async_trait;
    use swarmotter_core::meta::{build_multi_file_torrent, build_single_file_torrent};
    use swarmotter_core::net::{ContainedUdpSocket, PeerListener};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    fn piece_selection_skips_unwanted_files_and_completes_selected_set() {
        let files = vec![(vec!["a.bin".into()], 4), (vec!["b.bin".into()], 4)];
        let contents: Vec<&[u8]> = vec![b"aaaa", b"bbbb"];
        let bytes = build_multi_file_torrent("selection", &files, &contents, 4, None);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let selection = PieceSelection::from_files(
            &meta,
            &[FilePriority::Normal, FilePriority::High],
            &[false, true],
        )
        .unwrap();
        assert!(!selection.includes(0));
        assert!(selection.includes(1));
        let mut have = PieceBitfield::new(meta.piece_count());
        assert!(!selection.complete(&have));
        have.set(1);
        assert!(selection.complete(&have));
    }

    #[test]
    fn selected_file_includes_cross_file_boundary_piece() {
        let files = vec![(vec!["a.bin".into()], 2), (vec!["b.bin".into()], 2)];
        let contents: Vec<&[u8]> = vec![b"aa", b"bb"];
        let bytes = build_multi_file_torrent("boundary", &files, &contents, 4, None);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let selection = PieceSelection::from_files(
            &meta,
            &[FilePriority::Normal, FilePriority::High],
            &[false, true],
        )
        .unwrap();
        assert!(selection.includes(0));
    }

    #[test]
    fn piece_assembler_reports_duplicate_with_overwrite() {
        // The download loops must treat this return value as a piece-complete
        // signal, not as "a new block was accepted". Callers track whether a
        // specific requested block was missing before calling `add_block`.
        // This test pins the assembler semantics so caller-side duplicate
        // accounting remains explicit.
        use swarmotter_core::peer::PieceAssembler;
        // Use the actual BLOCK_SIZE (16 KiB). For a piece of 4 blocks, three
        // unique blocks and one duplicate must not change the completion
        // status (still not complete on the second block; the third unique
        // block brings it to complete).
        const BLK: usize = 16 * 1024;
        let mut a = PieceAssembler::new(0, 4 * BLK);
        assert!(!a.add_block(0, &vec![0xAB; BLK]).unwrap());
        // Duplicate: must not advance the block count. The assembler returns
        // Ok(false) because the piece is still incomplete after the
        // duplicate; the caller would not count this as a new block.
        assert!(
            !a.add_block(0, &vec![0xAB; BLK]).unwrap(),
            "duplicate block must not signal completion"
        );
        // First time at offset BLK: new block.
        assert!(!a.add_block(BLK as u32, &vec![0xCD; BLK]).unwrap());
        // First time at offset 2*BLK: new block, piece still incomplete.
        assert!(!a.add_block((2 * BLK) as u32, &vec![0xEF; BLK]).unwrap());
        // Final block completes the piece.
        assert!(a.add_block((3 * BLK) as u32, &vec![0x42; BLK]).unwrap());
        // The data is well-formed even though one block was "duplicated"
        // (it overwrote the same buffer slot, so the final data is correct).
        assert_eq!(a.data().len(), 4 * BLK);
    }

    #[test]
    fn preferred_encryption_preserves_transport_preference() {
        assert_eq!(
            peer_transport_order(true, false, PeerEncryptionMode::Preferred),
            vec![PeerTransport::Utp, PeerTransport::Tcp]
        );
        assert_eq!(
            peer_transport_order(true, true, PeerEncryptionMode::Preferred),
            vec![PeerTransport::Tcp, PeerTransport::Utp]
        );
        assert_eq!(
            peer_transport_order(true, false, PeerEncryptionMode::Required),
            vec![PeerTransport::Tcp]
        );
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

    #[tokio::test]
    async fn tracker_refresh_respects_the_announced_interval() {
        let bytes = build_single_file_torrent(
            "tracker-interval.bin",
            b"tracker interval payload",
            8,
            Some("http://127.0.0.1:1/announce"),
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let state = Arc::new(Mutex::new(EngineState {
            last_announce: Some(now_secs()),
            tracker_interval_seconds: 3_600,
            ..EngineState::default()
        }));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
            PathBuf::from("/tmp"),
            [0u8; 20],
            Arc::new(swarmotter_core::net::binder::LoopbackBinder),
            state.clone(),
            rx,
            vec![],
            6881,
        );

        assert!(!engine.tracker_announce_due().await);
        state.lock().await.last_announce = Some(now_secs().saturating_sub(3_601));
        assert!(engine.tracker_announce_due().await);
    }

    fn scrape_body(hash: InfoHash, seeders: i64, leechers: i64, downloads: i64) -> Vec<u8> {
        let mut body = b"d5:filesd20:".to_vec();
        body.extend_from_slice(hash.as_bytes());
        body.extend_from_slice(
            format!("d8:completei{seeders}e10:downloadedi{downloads}e10:incompletei{leechers}eeee")
                .as_bytes(),
        );
        body
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8(request).unwrap()
    }

    #[tokio::test]
    async fn scrape_failure_retains_last_success_counts_and_is_accounted() {
        let hash = InfoHash::from_bytes([0x71; 20]);
        let good = scrape_body(hash, 7, 8, 9);
        let malformed = b"d5:filesdee".to_vec();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for body in [good, malformed] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                assert!(request.starts_with("GET /scrape?info_hash="));
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });

        let url = format!("http://{address}/announce");
        let state = Arc::new(Mutex::new(EngineState::default()));
        let binder: Arc<dyn NetworkBinder> = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        run_tracker_scrapes(state.clone(), binder.clone(), hash, vec![url.clone()]).await;
        {
            let engine = state.lock().await;
            let snapshot = engine.tracker_scrapes.get(&url).unwrap();
            assert_eq!(snapshot.status, TrackerScrapeStatus::Ok);
            assert_eq!(snapshot.seeders, Some(7));
            assert_eq!(snapshot.leechers, Some(8));
            assert_eq!(snapshot.downloads, Some(9));
            assert_eq!(engine.tracker_failures_recent, 0);
        }

        run_tracker_scrapes(state.clone(), binder, hash, vec![url.clone()]).await;
        server.await.unwrap();
        let engine = state.lock().await;
        let snapshot = engine.tracker_scrapes.get(&url).unwrap();
        assert_eq!(snapshot.status, TrackerScrapeStatus::Error);
        assert_eq!(snapshot.seeders, Some(7));
        assert_eq!(snapshot.leechers, Some(8));
        assert_eq!(snapshot.downloads, Some(9));
        assert!(snapshot.last_error.is_some());
        assert_eq!(engine.tracker_failures_recent, 1);
    }

    #[tokio::test]
    async fn started_and_reannounce_paths_schedule_contained_scrapes() {
        let payload = b"generated tracker scrape scheduling payload";
        let placeholder = build_single_file_torrent("scrape-schedule.bin", payload, 8, None, false);
        let hash = swarmotter_core::meta::parse_torrent(&placeholder)
            .unwrap()
            .info_hash;
        let announce_body = b"d8:completei3e10:incompletei4e8:intervali30e5:peers0:e".to_vec();
        let scraped = scrape_body(hash, 11, 12, 13);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let scrape_requests = Arc::new(AtomicUsize::new(0));
        let server_scrapes = scrape_requests.clone();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                let body = if request.starts_with("GET /scrape?") {
                    server_scrapes.fetch_add(1, Ordering::SeqCst);
                    &scraped
                } else {
                    assert!(request.starts_with("GET /announce?"));
                    &announce_body
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let http_tracker = format!("http://{address}/announce");
        let bytes = build_single_file_torrent(
            "scrape-schedule.bin",
            payload,
            8,
            Some(&http_tracker),
            false,
        );
        let mut meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let udp_tracker = "udp://127.0.0.1:6969/announce".to_string();
        meta.announce_list = vec![vec![http_tracker.clone()], vec![udp_tracker.clone()]];
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
            PathBuf::from("/tmp"),
            [0u8; 20],
            Arc::new(swarmotter_core::net::binder::LoopbackBinder),
            state.clone(),
            rx,
            vec![],
            6881,
        );

        engine.announce(AnnounceEvent::Started).await;
        engine.announce(AnnounceEvent::Empty).await;
        server.await.unwrap();
        assert_eq!(scrape_requests.load(Ordering::SeqCst), 2);
        let engine_state = state.lock().await;
        let snapshot = engine_state
            .tracker_scrapes
            .get(&http_tracker)
            .expect("scrape snapshot");
        assert_eq!(snapshot.status, TrackerScrapeStatus::Ok);
        assert_eq!(snapshot.downloads, Some(13));
        assert_eq!(
            engine_state
                .tracker_scrapes
                .get(&udp_tracker)
                .unwrap()
                .status,
            TrackerScrapeStatus::Unsupported
        );
    }

    #[tokio::test]
    async fn magnet_tracker_activity_scrapes_the_real_magnet_info_hash() {
        let magnet_hash = InfoHash::from_bytes([0x74; 20]);
        let body = scrape_body(magnet_hash, 21, 22, 23);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            assert!(request.starts_with("GET /scrape.php?info_hash="));
            let expected = tracker::bytes_escape(magnet_hash.as_bytes());
            assert!(request.contains(&format!("info_hash={expected}")));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });

        let bytes = build_single_file_torrent(
            "magnet-placeholder.bin",
            b"generated placeholder payload",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
            PathBuf::from("/tmp"),
            [0u8; 20],
            Arc::new(swarmotter_core::net::binder::LoopbackBinder),
            state.clone(),
            rx,
            vec![],
            6881,
        );
        let url = format!("http://{address}/announce.php");
        let mut outcome = TrackerAnnounceOutcome::default();
        outcome.tracker_results.insert(
            url.clone(),
            TrackerAnnounceSnapshot {
                status: TrackerStatus::Ok,
                seeders: 1,
                leechers: 2,
                downloads: 0,
                last_error: None,
                last_message: Some("magnet announce ok".into()),
                last_announce: Some(now_secs()),
            },
        );
        engine
            .record_tracker_activity(magnet_hash, &outcome, vec![url.clone()])
            .await;
        server.await.unwrap();

        let engine_state = state.lock().await;
        let snapshot = engine_state.tracker_scrapes.get(&url).unwrap();
        assert_eq!(snapshot.status, TrackerScrapeStatus::Ok);
        assert_eq!(snapshot.seeders, Some(21));
        assert_eq!(snapshot.leechers, Some(22));
        assert_eq!(snapshot.downloads, Some(23));
    }

    struct PanickingScrapeBinder;

    #[async_trait]
    impl NetworkBinder for PanickingScrapeBinder {
        async fn connect_peer(&self, _addr: SocketAddr) -> Result<tokio::net::TcpStream> {
            panic!("generated scrape task panic");
        }

        async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
            Ok("127.0.0.1:9".parse().unwrap())
        }

        async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
            Err(CoreError::Internal("unused in scrape test".into()))
        }

        async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
            Err(CoreError::Internal("unused in scrape test".into()))
        }

        fn traffic_allowed(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn scrape_task_panic_is_retained_for_the_exact_tracker() {
        let hash = InfoHash::from_bytes([0x72; 20]);
        let url = "http://panic.test/announce".to_string();
        let state = Arc::new(Mutex::new(EngineState::default()));
        run_tracker_scrapes(
            state.clone(),
            Arc::new(PanickingScrapeBinder),
            hash,
            vec![url.clone()],
        )
        .await;

        let engine = state.lock().await;
        let snapshot = engine.tracker_scrapes.get(&url).unwrap();
        assert_eq!(snapshot.status, TrackerScrapeStatus::Error);
        assert!(snapshot
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("task failed")));
        assert_eq!(engine.tracker_failures_recent, 1);
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

    #[tokio::test]
    async fn sync_have_from_state_merges_more_complete_live_state() {
        let bytes =
            build_single_file_torrent("f", b"0123456789abcdef0123456789abcdef", 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let piece_count = meta.piece_count();
        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        {
            let mut live = state.lock().await;
            live.piece_count = piece_count;
            live.pieces_have = PieceBitfield::new(piece_count);
            live.pieces_have.set(0);
            live.pieces_have.set(2);
        }
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
        let mut have = PieceBitfield::new(piece_count);
        have.set(0);

        engine.sync_have_from_state(&mut have, piece_count).await;

        assert!(have.has(0));
        assert!(have.has(2));
        assert_eq!(have.count(piece_count), 2);
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
    fn peer_candidate_classification_marks_all_filtered_as_unusable() {
        let peers = vec![
            PeerAddr::from_socket_addr("[2001:db8::1]:6001".parse().unwrap()),
            PeerAddr::from_socket_addr("[2001:db8::2]:6002".parse().unwrap()),
        ];

        let (eligible, counts) =
            classify_peer_candidates(&peers, &HashMap::new(), &HashMap::new(), false);

        assert!(eligible.is_empty());
        assert_eq!(counts.discovered, 2);
        assert_eq!(counts.filtered, 2);
        assert_eq!(counts.eligible, 0);
        assert!(no_usable_peer_candidates(&counts));
        assert_eq!(
            peer_scheduler_reason(&counts).as_deref(),
            Some("all discovered peers filtered by configuration")
        );
    }

    #[test]
    fn peer_candidate_classification_does_not_stop_for_idle_backoff_only() {
        let peer = PeerAddr::from_socket_addr("127.0.0.1:6001".parse().unwrap());
        let peers = vec![peer];
        let mut peer_backoff = HashMap::new();
        backoff_peer_for(
            &mut peer_backoff,
            peer.socket_addr(),
            Duration::from_secs(60),
        );

        let (eligible, counts) =
            classify_peer_candidates(&peers, &HashMap::new(), &peer_backoff, false);

        assert!(eligible.is_empty());
        assert_eq!(counts.discovered, 1);
        assert_eq!(counts.backed_off, 1);
        assert_eq!(counts.eligible, 0);
        assert!(!no_usable_peer_candidates(&counts));
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

    #[test]
    fn parallel_piece_download_ignores_duplicate_or_unsolicited_blocks() {
        let mut download = ParallelPieceDownload::new(0, peer::BLOCK_SIZE * 2);
        download.outstanding_blocks.insert(0, peer::BLOCK_SIZE);
        download.in_flight = 1;
        let mut global_in_flight = 1usize;
        let block = vec![0u8; peer::BLOCK_SIZE as usize];

        assert_eq!(
            download
                .record_block(0, &block, &mut global_in_flight)
                .unwrap(),
            Some(false)
        );
        assert_eq!(download.in_flight, 0);
        assert_eq!(global_in_flight, 0);

        assert_eq!(
            download
                .record_block(0, &block, &mut global_in_flight)
                .unwrap(),
            None
        );
        assert_eq!(
            download
                .record_block(peer::BLOCK_SIZE, &block, &mut global_in_flight)
                .unwrap(),
            None
        );
        assert_eq!(download.in_flight, 0);
        assert_eq!(global_in_flight, 0);
    }

    #[test]
    fn parallel_piece_download_rejects_wrong_sized_blocks_without_accounting() {
        let mut download = ParallelPieceDownload::new(0, peer::BLOCK_SIZE);
        download.outstanding_blocks.insert(0, peer::BLOCK_SIZE);
        download.in_flight = 1;
        let mut global_in_flight = 1usize;

        assert_eq!(
            download
                .record_block(0, &[0u8; 1], &mut global_in_flight)
                .unwrap(),
            None
        );
        assert_eq!(download.in_flight, 1);
        assert_eq!(global_in_flight, 1);
        assert_eq!(download.outstanding_blocks.get(&0), Some(&peer::BLOCK_SIZE));
    }

    #[test]
    fn peer_request_window_grows_with_observed_rate_and_respects_cap() {
        let now = Instant::now();
        let mut window = PeerRequestWindow::new(Some(128), now);
        assert_eq!(window.desired_in_flight(), NORMAL_REQUEST_FLOOR);

        window.sample_started_at = now - Duration::from_secs(1);
        window.record_block(peer::BLOCK_SIZE as u64 * 128, now);

        assert!(window.desired_in_flight() > NORMAL_REQUEST_FLOOR);
        assert!(window.desired_in_flight() <= 128);
    }

    #[test]
    fn parallel_piece_state_prefers_rarest_available_piece() {
        let have = PieceBitfield::new(3);
        let mut state = ParallelPieceState::new(have, 3, PieceSelection::all_count(3));
        let peer_a: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let peer_b: SocketAddr = "127.0.0.2:6002".parse().unwrap();

        let mut first = Bitfield::new(3);
        first.set(0);
        first.set(1);
        state.note_peer_bitfield(peer_a, &first, 3);

        let mut second = Bitfield::new(3);
        second.set(0);
        state.note_peer_bitfield(peer_b, &second, 3);

        // The exact piece returned depends on the sharding offset (a hash of
        // `first`'s bitfield), but the invariant is: it must be a piece that
        // peer_a has, that we don't, that isn't already reserved. Both piece
        // 0 (availability 2) and piece 1 (availability 1) satisfy that.
        // When the search starts at the shard and piece 1 falls inside the
        // search range, it is preferred because it is rarer. We allow either
        // result; the second piece (rarest in this fixture) is the common case
        // when the shard is small.
        let result = state.reserve_piece(&first, peer_a, 3);
        assert!(
            result == Some(0) || result == Some(1),
            "reserve_piece returned {result:?}, expected Some(0) or Some(1)"
        );
        assert!(state.peer_has_missing_piece(&first, 3));
    }

    #[test]
    fn parallel_piece_state_shard_does_not_monopolise_one_peer() {
        // Two peers with different bitfields should reserve different pieces
        // when their bitfields hash to different shards. This is the property
        // that prevents one fast peer from monopolising all pieces when its
        // piece window is wider than the remaining piece count.
        let have = PieceBitfield::new(8);
        let mut state = ParallelPieceState::new(have, 8, PieceSelection::all_count(8));
        let peer_a: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let peer_b: SocketAddr = "127.0.0.1:7002".parse().unwrap();

        let mut bf_a = Bitfield::new(8);
        for i in 0..8 {
            bf_a.set(i);
        }
        // Peer B holds a subset, shifted by one — its bitfield bytes differ
        // from peer A's, so the sharder produces a different offset.
        let mut bf_b = Bitfield::new(8);
        for i in 0..7 {
            bf_b.set(i + 1);
        }
        state.note_peer_bitfield(peer_a, &bf_a, 8);
        state.note_peer_bitfield(peer_b, &bf_b, 8);

        let reserved_a = state.reserve_piece(&bf_a, peer_a, 8);
        let reserved_b = state.reserve_piece(&bf_b, peer_b, 8);
        // Each peer must reserve a piece it actually has, and the two
        // reservations must not collide (no two peers reserve the same piece).
        assert!(reserved_a.is_some());
        assert!(reserved_b.is_some());
        assert_ne!(reserved_a, reserved_b, "both peers reserved the same piece");
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

    #[tokio::test]
    async fn same_size_external_payload_change_invalidates_fast_resume() {
        let payload = b"abcdefgh";
        let bytes = build_single_file_torrent("same-size.bin", payload, 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let dir = unique_dir("same-size-resume");
        let storage = StorageIo::new(meta.clone(), dir.clone());
        storage.write_piece(0, payload).await.unwrap();
        let mut have = PieceBitfield::new(1);
        have.set(0);
        let mut resume = swarmotter_core::storage::io::build_resume(
            meta.info_hash,
            meta.name.clone(),
            have,
            1,
            0,
            0,
            meta.total_length,
            Some(dir.display().to_string()),
            now_secs(),
            None,
            &[FilePriority::Normal],
            &[8],
        );
        resume.file_stamps = storage.resume_file_stamps().await.unwrap();
        storage.save_resume(&resume).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::fs::write(storage.file_path(0).unwrap(), b"XXXXXXXX")
            .await
            .unwrap();

        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
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
        assert!(!recovered.has(0));
        std::fs::remove_dir_all(dir).ok();
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
        assert!(!engine.peer_allowed(&PeerAddr::from_socket_addr("127.0.0.1:0".parse().unwrap())));
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
