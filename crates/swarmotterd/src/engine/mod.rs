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

mod discovery;
mod download;
mod endgame;
mod parallel;
mod peer_session;
mod progress;
mod webseed;

pub(crate) use discovery::run_tracker_scrapes;
use discovery::*;
use parallel::*;
use peer_session::*;
use progress::*;

#[cfg(test)]
mod tests;
