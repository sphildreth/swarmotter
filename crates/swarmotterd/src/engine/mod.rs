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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::bandwidth::{RateDirection, RateLimiter, ShapedLimiter};
use swarmotter_core::config::{CowStrategy, PeerEncryptionMode};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::{InfoHash, PeerInfoHash, TorrentKey};
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::models::peer::EnginePeerHealth;
use swarmotter_core::models::stats::PeerSchedulerDiagnostics;
use swarmotter_core::models::torrent::FilePriority;
use swarmotter_core::models::tracker::{TrackerScrapeStatus, TrackerStatus};
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{
    self, block_requests, Bitfield, Handshake, Message, PeerAddr, PeerReader,
};
use swarmotter_core::peer_filter::PeerFilter;
use swarmotter_core::policy::{IntakePolicySnapshot, TrackerHostRule};
use swarmotter_core::storage::resume::PieceBitfield;
use swarmotter_core::storage::{piece_file_ranges, verify_piece, StorageIo, StorageIoMetrics};
use swarmotter_core::tracker::{self, AnnounceEvent, AnnounceRequest};
use swarmotter_core::udp_tracker;
use swarmotter_core::utp::{self, PeerTransport};

use crate::peer_permits::PeerSessionBudget;

/// Default simultaneous peer download workers when no per-torrent peer cap is
/// configured. Trackers commonly return far more than 16 usable peers for
/// public Linux distribution torrents, so the default should be high enough to
/// keep several useful peers busy without requiring operator tuning.
pub const DEFAULT_PEER_WORKER_LIMIT: usize = crate::peer_permits::DEFAULT_PER_TORRENT_PEER_LIMIT;

/// Process-wide coalesced distribution of the configured per-torrent peer
/// worker limit.
///
/// Reconciliation used to send `UpdatePeerWorkerLimit` to every engine's
/// bounded command channel on every queue tick. A stalled engine with a full
/// eight-slot channel suspended global queue reconciliation behind an update
/// that carried no new information. Replaceable settings must instead
/// converge through lock-free shared state; lifecycle commands keep their
/// reliable channel semantics. See ADR-0071.
///
/// * `store_default` is called by the daemon scheduler whenever the effective
///   configured limit is (re)computed. Identical values are no-ops.
/// * The generation counter advances only when the default value changes, so
///   a per-torrent autopilot override remains valid between configuration
///   replacements and is invalidated exactly when the configured limit does.
#[derive(Debug, Default)]
pub struct SharedPeerWorkerLimit {
    default_limit: AtomicUsize,
    generation: AtomicU64,
}

impl SharedPeerWorkerLimit {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            default_limit: AtomicUsize::new(DEFAULT_PEER_WORKER_LIMIT),
            generation: AtomicU64::new(0),
        })
    }

    /// Store the latest configured per-torrent limit. Returns `true` when the
    /// value changed and per-torrent overrides were therefore invalidated.
    pub fn store_default(&self, limit: usize) -> bool {
        let normalized = limit.max(1);
        if self.default_limit.swap(normalized, Ordering::Relaxed) != normalized {
            self.generation.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn load_default(&self) -> usize {
        self.default_limit.load(Ordering::Relaxed).max(1)
    }

    pub fn load_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

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

/// Resume checkpoint coalescing (ADR-0074): a checkpoint is written when at
/// least this many newly verified pieces have accumulated since the last
/// successful checkpoint, or after this much runtime has elapsed since the
/// last checkpoint, whichever comes first. Lifecycle boundaries (stop,
/// completion, selected-file completion) always force an immediate
/// checkpoint. A crash can lose at most this much verified progress from the
/// resume file; payload bytes remain on disk and a restart rechecks or
/// redownloads only the unrecorded pieces — unverified bytes are never
/// trusted.
const RESUME_CHECKPOINT_PIECES: u64 = 64;
const RESUME_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

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
    /// Full parsed v1/v2/hybrid identity from the magnet exact topics.
    pub identity: swarmotter_core::hash::TorrentIdentity,
    /// v1 compatibility hash when the magnet has one; pure-v2 magnets retain
    /// [`InfoHash::ZERO`] here and use `wire_info_hash` for all peer,
    /// tracker, and DHT traffic.
    pub info_hash: swarmotter_core::hash::InfoHash,
    /// The exact 20-byte peer/tracker/DHT wire identity. For pure-v2 magnets
    /// this is the prescribed truncation of the full SHA-256 identity, never
    /// a synthetic placeholder hash.
    pub wire_info_hash: PeerInfoHash,
    pub name: String,
    pub trackers: Vec<String>,
    /// Deferred BEP 53 select-only file indices. They are checked against the
    /// resolved metadata before any payload-side piece selection is built.
    pub select_only_file_indices: Vec<usize>,
}

pub type MetadataPreflight =
    Arc<dyn Fn(Arc<TorrentMeta>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Daemon-owned execution hook for full on-disk verification. The standalone
/// engine remains usable without it; the daemon installs one so every startup
/// and fast-resume recheck observes root-scoped concurrency controls.
pub type StorageRecheckExecutor = Arc<
    dyn Fn(StorageIo) -> Pin<Box<dyn Future<Output = Result<PieceBitfield>> + Send>> + Send + Sync,
>;

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
    /// A protocol-level rejection (`failure reason` or BEP 15 error), rather
    /// than a timeout, connection failure, or malformed response.
    pub explicit_failure: bool,
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
    /// Population reported by the last successful announce, retained across
    /// transport failures. A thin peer list does not establish an empty swarm.
    pub tracker_swarm_populated: bool,
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
    /// Timestamp of the latest DHT lookup attempt, including failures.
    pub dht_last_lookup: Option<std::time::Instant>,
    /// Start time of the latest completed lookup, including empty results,
    /// errors, and timeouts. Unlike an attempt, this proves discovery finished.
    pub dht_last_lookup_completed: Option<std::time::Instant>,
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
    pub resolved_meta: Option<Arc<TorrentMeta>>,
}

impl EngineState {
    /// Return the terminal tracker error when every attempted configured
    /// tracker explicitly rejected the announce and no non-tracker source
    /// produced a usable candidate or payload. A successful tracker response
    /// (even with zero peers) and any successful DHT, PEX, peer, or webseed
    /// signal prevent this classification.
    pub fn terminal_tracker_error(&self) -> Option<String> {
        if self.finished
            || self.stopped_by_command
            || self.tracker_ok
            || self.tracker_last_ok.is_some()
            || self.tracker_announces.is_empty()
        {
            return None;
        }

        if !self
            .tracker_announces
            .values()
            .all(|snapshot| snapshot.status == TrackerStatus::Error && snapshot.explicit_failure)
        {
            return None;
        }

        let peer_payload_received = self.last_valid_block.is_some()
            || self.block_last_seen.is_some()
            || self
                .peer_health
                .values()
                .any(|peer| peer.last_valid_block.is_some() || peer.useful_recently);
        let alternative_succeeded = self.dht_discovery_ok
            || self.pex_discovery_ok
            || self.webseed_last_seen.is_some()
            || peer_payload_received
            || self.peer_scheduler.eligible_peers > 0;
        if alternative_succeeded {
            return None;
        }

        let detail = self
            .tracker_message
            .as_deref()
            .filter(|message| !message.trim().is_empty())
            .unwrap_or("all configured tracker announces failed");
        Some(format!(
            "all configured trackers failed and no usable alternative source was available: {detail}"
        ))
    }
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
    meta: Arc<TorrentMeta>,
    /// Canonical registry/durable identity. This is intentionally separate
    /// from `meta.info_hash`, which has no v1 value for pure BEP 52 torrents.
    torrent_key: TorrentKey,
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
    /// Stop after contained BEP 9 metadata retrieval, before any payload
    /// storage, tracker payload announce, or piece requests are started.
    metadata_only: bool,
    /// File selection captured before a magnet's real metadata was known.
    /// It is applied immediately after metadata resolves and before payload
    /// piece selection is constructed.
    intake_selection: Option<IntakePolicySnapshot>,
    /// Live profile-scoped tracker host enablement and priority rules. They
    /// shape discovery only; every resulting request still uses the binder.
    tracker_host_rules: Vec<TrackerHostRule>,
    metadata_preflight: Option<MetadataPreflight>,
    storage_recheck_executor: Option<StorageRecheckExecutor>,
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
    cow_strategy: CowStrategy,
    resume_dir: Option<PathBuf>,
    /// Active-only filename suffix selected at intake. The active storage
    /// handle uses it for every v1/v2 file path; completion restores canonical
    /// metainfo names before the engine reports success.
    partial_file_suffix: Option<String>,
    minimum_free_space_bytes: u64,
    minimum_free_space_percent: u8,
    /// Optional shared local-storage payload-write limiter supplied by the
    /// daemon for the configured active storage root.
    storage_write_limiter: Option<RateLimiter>,
    storage_metrics: Option<StorageIoMetrics>,
    storage_handle_budget: Option<Arc<swarmotter_core::storage::StorageHandleBudget>>,
    /// Per-torrent autopilot override for the peer worker limit. `0` means no
    /// override is active and the engine follows [`Self::shared_peer_limit`].
    max_peer_workers: Arc<AtomicUsize>,
    /// Generation of [`Self::shared_peer_limit`] at the time the override was
    /// stored. A configuration replacement bumps the shared generation, which
    /// invalidates the override without any per-engine channel send.
    peer_worker_override_generation: Arc<AtomicU64>,
    /// Process-wide coalesced distribution of the configured per-torrent peer
    /// worker limit. See `SharedPeerWorkerLimit` and ADR-0071.
    shared_peer_limit: Option<Arc<SharedPeerWorkerLimit>>,
    allow_ipv6: bool,
    /// Immutable peer-admission rules for this data-plane configuration
    /// generation. Socket creation still goes through `binder`.
    peer_filter: Arc<PeerFilter>,
    pex_enabled: bool,
    pex_max_peers: usize,
    file_priorities: Vec<FilePriority>,
    wanted: Vec<bool>,
    piece_selection: PieceSelection,
    /// Shared global plus per-torrent lifetime permits for every peer wire
    /// session opened by this engine. See ADR-0053.
    peer_session_budget: PeerSessionBudget,
    /// Verified-piece generation counter for resume checkpoint coalescing
    /// (ADR-0074). Advances once per verified, written piece.
    resume_dirty_generation: Arc<AtomicU64>,
    /// Generation covered by the last successful resume checkpoint.
    resume_checkpointed_generation: Arc<AtomicU64>,
    /// Time of the last successful resume checkpoint (mockable clock).
    resume_checkpoint_at: Arc<Mutex<tokio::time::Instant>>,
    /// Diagnostics: checkpoints actually written by this engine.
    resume_checkpoints_written: Arc<AtomicU64>,
    /// Daemon hook that registers this engine for inbound verified-piece
    /// serving while it downloads (ADR-0075). Called once per run with the
    /// post-resolution metadata and the active storage handle.
    downloader_serve_registration: Option<DownloaderServeHook>,
}

/// Payload delivered to the daemon's downloader-serving registration hook.
pub struct DownloaderServeRegistration {
    pub torrent_key: TorrentKey,
    pub meta: Arc<TorrentMeta>,
    pub storage: Arc<StorageIo>,
    pub state: Arc<Mutex<EngineState>>,
    pub limiter: ShapedLimiter,
    /// The engine's peer id. Inbound serving replies with the same id so the
    /// standard self-connection check drops a daemon's own dial-in.
    pub peer_id: [u8; 20],
}

pub type DownloaderServeHook = Arc<
    dyn Fn(DownloaderServeRegistration) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
>;

impl TorrentEngine {
    /// Attach the daemon hook that registers inbound downloader serving.
    pub fn with_downloader_serve_registration(mut self, hook: DownloaderServeHook) -> Self {
        self.downloader_serve_registration = Some(hook);
        self
    }

    /// Register for inbound serving when the payload download loop starts.
    async fn register_downloader_serve(&self, storage: Arc<StorageIo>) {
        let Some(hook) = self.downloader_serve_registration.clone() else {
            return;
        };
        hook(DownloaderServeRegistration {
            torrent_key: self.torrent_key,
            meta: Arc::clone(&self.meta),
            storage,
            state: self.state.clone(),
            limiter: self.limiter.clone(),
            peer_id: self.peer_id,
        })
        .await;
    }
}

impl TorrentEngine {
    /// Record that one more verified piece was written to storage and is not
    /// yet covered by a durable resume checkpoint.
    pub(super) fn mark_resume_piece_verified(&self) {
        self.resume_dirty_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Decide whether a resume checkpoint is due. Returns the dirty generation
    /// the checkpoint would cover, or `None` when coalescing keeps waiting.
    /// `force` bypasses the pieces-and-interval policy for lifecycle
    /// boundaries that must not lose verified progress.
    async fn resume_checkpoint_due(&self, force: bool) -> Option<u64> {
        let dirty = self.resume_dirty_generation.load(Ordering::Relaxed);
        let checkpointed = self.resume_checkpointed_generation.load(Ordering::Relaxed);
        if force {
            return Some(dirty);
        }
        if dirty == checkpointed {
            return None;
        }
        let elapsed =
            tokio::time::Instant::now().duration_since(*self.resume_checkpoint_at.lock().await);
        let pending_pieces = dirty.saturating_sub(checkpointed);
        if elapsed < RESUME_CHECKPOINT_INTERVAL && pending_pieces < RESUME_CHECKPOINT_PIECES {
            return None;
        }
        Some(dirty)
    }

    /// Record a successful checkpoint. `observed` is the dirty generation the
    /// checkpoint's `have` snapshot covered; pieces verified while the
    /// checkpoint wrote (generation beyond `observed`) remain dirty so the
    /// next checkpoint includes them.
    async fn complete_resume_checkpoint(&self, observed: u64) {
        self.resume_checkpointed_generation
            .store(observed, Ordering::Relaxed);
        self.resume_checkpoints_written
            .fetch_add(1, Ordering::Relaxed);
        *self.resume_checkpoint_at.lock().await = tokio::time::Instant::now();
    }

    /// Diagnostics: how many resume checkpoints this engine has written.
    #[cfg(test)]
    pub(super) fn resume_checkpoints_written(&self) -> u64 {
        self.resume_checkpoints_written.load(Ordering::Relaxed)
    }

    /// Coalesced v1/hybrid resume checkpoint. Returns `Ok(false)` when the
    /// policy deferred the checkpoint.
    pub(super) async fn maybe_persist_resume(
        &self,
        storage: &StorageIo,
        have: &PieceBitfield,
        force: bool,
    ) -> Result<bool> {
        let Some(observed) = self.resume_checkpoint_due(force).await else {
            return Ok(false);
        };
        self.persist_resume(storage, have).await?;
        self.complete_resume_checkpoint(observed).await;
        Ok(true)
    }
}

impl TorrentEngine {
    #[allow(clippy::too_many_arguments, dead_code)]
    pub fn new(
        meta: impl Into<Arc<TorrentMeta>>,
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
        meta: impl Into<Arc<TorrentMeta>>,
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
        let meta = meta.into();
        let piece_selection = PieceSelection::all(&meta);
        let file_count = meta.files.len();
        let torrent_key = meta
            .identity
            .primary_key()
            .unwrap_or_else(|| TorrentKey::v1(meta.info_hash));
        Self {
            meta,
            torrent_key,
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
            metadata_only: false,
            intake_selection: None,
            tracker_host_rules: Vec::new(),
            metadata_preflight: None,
            storage_recheck_executor: None,
            dht: None,
            utp_enabled: true,
            utp_prefer_tcp: true,
            encryption_mode: PeerEncryptionMode::default(),
            preallocate: true,
            sparse: true,
            cow_strategy: CowStrategy::Conservative,
            resume_dir: None,
            partial_file_suffix: None,
            minimum_free_space_bytes: 0,
            minimum_free_space_percent: 0,
            storage_write_limiter: None,
            storage_metrics: None,
            storage_handle_budget: None,
            max_peer_workers: Arc::new(AtomicUsize::new(0)),
            peer_worker_override_generation: Arc::new(AtomicU64::new(0)),
            shared_peer_limit: None,
            allow_ipv6: true,
            peer_filter: Arc::new(PeerFilter::default()),
            pex_enabled: true,
            pex_max_peers: 0,
            file_priorities: vec![FilePriority::Normal; file_count],
            wanted: vec![true; file_count],
            piece_selection,
            peer_session_budget: PeerSessionBudget::unlimited(),
            resume_dirty_generation: Arc::new(AtomicU64::new(0)),
            resume_checkpointed_generation: Arc::new(AtomicU64::new(0)),
            resume_checkpoint_at: Arc::new(Mutex::new(tokio::time::Instant::now())),
            resume_checkpoints_written: Arc::new(AtomicU64::new(0)),
            downloader_serve_registration: None,
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

    /// Configure peer-wire encryption policy for contained TCP/uTP streams.
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

    /// Configure explicit CoW handling for newly created active payload files.
    pub fn with_cow_strategy(mut self, cow_strategy: CowStrategy) -> Self {
        self.cow_strategy = cow_strategy;
        self
    }

    /// Configure a dedicated fast-resume metadata root.
    pub fn with_resume_dir(mut self, resume_dir: Option<PathBuf>) -> Self {
        self.resume_dir = resume_dir;
        self
    }

    /// Attach the canonical daemon key used by storage, resume, and runtime
    /// ownership. Scheduler callers always pass the registry-canonical value
    /// so a hybrid alias cannot create a second resume record.
    pub fn with_torrent_key(mut self, torrent_key: TorrentKey) -> Self {
        self.torrent_key = torrent_key;
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

    /// Configure a shared root-level limiter for verified payload writes.
    /// This is intentionally separate from peer bandwidth shaping: it delays
    /// only local disk writes and leaves all network containment unchanged.
    pub fn with_storage_write_limiter(mut self, limiter: Option<RateLimiter>) -> Self {
        self.storage_write_limiter = limiter;
        self
    }

    /// Attach shared actual-I/O accounting for the active storage root.
    pub fn with_storage_metrics(mut self, metrics: Option<StorageIoMetrics>) -> Self {
        self.storage_metrics = metrics;
        self
    }

    pub fn with_storage_handle_budget(
        mut self,
        budget: Option<Arc<swarmotter_core::storage::StorageHandleBudget>>,
    ) -> Self {
        self.storage_handle_budget = budget;
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

    /// Attach global IP/client-id peer admission rules. The engine checks the
    /// rules before every outbound connection and at every discovery ingress;
    /// this does not replace or relax containment binding.
    pub fn with_peer_filter(mut self, peer_filter: Arc<PeerFilter>) -> Self {
        self.peer_filter = peer_filter;
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

    /// Attach the daemon's coalesced peer worker limit distribution. Engines
    /// without a per-torrent override follow the shared default directly.
    pub fn with_shared_peer_worker_limit(mut self, shared: Arc<SharedPeerWorkerLimit>) -> Self {
        // Preserve an override set before the shared state was attached by
        // recording the current shared generation for it.
        let generation = shared.load_generation();
        self.peer_worker_override_generation
            .store(generation, Ordering::Relaxed);
        self.shared_peer_limit = Some(shared);
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

    /// Fetch a magnet's metadata through the existing contained binder and
    /// return before payload-side storage or transfer begins.
    pub fn with_metadata_only(mut self) -> Self {
        self.metadata_only = true;
        self
    }

    /// Apply a captured add-time file selection after a magnet replaces its
    /// placeholder metadata. This prevents a default-all-files reset from
    /// racing payload transfer.
    pub fn with_intake_selection(mut self, selection: IntakePolicySnapshot) -> Self {
        self.intake_selection = Some(selection);
        self
    }

    /// Apply deterministic profile tracker-host controls to announce and
    /// magnet metadata discovery. Rules affect only tracker ordering and
    /// eligibility, never the contained network path.
    pub fn with_tracker_host_rules(mut self, rules: Vec<TrackerHostRule>) -> Self {
        self.tracker_host_rules = rules;
        self
    }

    /// Validate and reserve resolved magnet metadata with the daemon before
    /// any payload path is created.
    pub fn with_metadata_preflight(mut self, preflight: MetadataPreflight) -> Self {
        self.metadata_preflight = Some(preflight);
        self
    }

    /// Route startup and fast-resume verification through daemon-owned local
    /// storage controls. This covers both active and completed directories.
    pub fn with_storage_recheck_executor(mut self, executor: StorageRecheckExecutor) -> Self {
        self.storage_recheck_executor = Some(executor);
        self
    }

    fn set_peer_worker_limit(&self, max_peer_workers: usize) {
        // `0` clears any per-torrent override so the engine follows the shared
        // configured default again. A nonzero value pins this torrent's limit
        // until the shared default itself changes (generation bump).
        let generation = self
            .shared_peer_limit
            .as_ref()
            .map(|shared| shared.load_generation())
            .unwrap_or(0);
        self.peer_worker_override_generation
            .store(generation, Ordering::Relaxed);
        self.max_peer_workers
            .store(max_peer_workers, Ordering::Relaxed);
    }

    fn current_peer_worker_limit(&self) -> usize {
        let override_value = self.max_peer_workers.load(Ordering::Relaxed);
        if override_value > 0 {
            let override_valid = match &self.shared_peer_limit {
                Some(shared) => {
                    shared.load_generation()
                        == self.peer_worker_override_generation.load(Ordering::Relaxed)
                }
                None => true,
            };
            if override_valid {
                return override_value;
            }
            // A configuration replacement has invalidated this override; stop
            // following it so the latest configured default takes effect.
            self.max_peer_workers.store(0, Ordering::Relaxed);
        }
        match &self.shared_peer_limit {
            Some(shared) => shared.load_default(),
            None => DEFAULT_PEER_WORKER_LIMIT,
        }
    }

    /// Configure the final completed-data directory. The engine writes active
    /// pieces under `download_dir` and atomically moves verified completed data
    /// here before marking the torrent finished.
    pub fn with_complete_dir(mut self, complete_dir: PathBuf) -> Self {
        self.complete_dir = complete_dir;
        self
    }

    /// Configure an active-only payload filename suffix such as `.part`.
    /// Input is validated by the durable intake policy before an engine is
    /// constructed; `StorageIo` validates again at path resolution.
    pub fn with_partial_file_suffix(mut self, partial_file_suffix: Option<String>) -> Self {
        self.partial_file_suffix = partial_file_suffix;
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
mod serve;
mod v2;
mod webseed;

pub(crate) use discovery::run_tracker_scrapes;
use discovery::*;
use parallel::*;
use peer_session::*;
use progress::*;
use serve::*;

#[cfg(test)]
mod tests;
