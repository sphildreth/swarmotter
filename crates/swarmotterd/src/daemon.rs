// SPDX-License-Identifier: Apache-2.0

//! Daemon runtime state implementing the API's `DaemonOps` trait.
//!
//! The runtime holds torrents, configuration, network health, and watch-
//! folder state. Torrent operations enforce network containment: in strict
//! fail-closed mode, torrent data-plane activity is blocked when the
//! configured path is unavailable, and torrents enter a `network_blocked`
//! state. The control plane (API/Web UI) remains available independently.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use serde_json::json;
use swarmotter_api::handlers::events::{Event, EventBroker};
use swarmotter_api::state::{AddTorrentOptions, DaemonOps};
use swarmotter_core::autopilot::{AutopilotAnalyzer, AutopilotConfig, AutopilotMode};
use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::meta;
use swarmotter_core::models::health::{HealthCalculator, HealthInput};
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::peer::{EnginePeerHealth, Peer};
use swarmotter_core::models::stats::{
    AutopilotActionKind, AutopilotDecision, AutopilotInput, GlobalStats, PeerSchedulerDiagnostics,
    SchedulerDiagnostics, TorrentDiagnostics,
};
use swarmotter_core::models::storage::{
    StorageDiagnostics, StorageRootDiagnostics, StorageRootRole,
};
use swarmotter_core::models::torrent::{
    FilePriority, SeedingStatus, TorrentFile, TorrentState, TorrentSummary,
};
use swarmotter_core::models::tracker::{TrackerId, TrackerInfo, TrackerKind, TrackerStatus};
use swarmotter_core::models::{
    ConfigUpdateResult, DiagnosticLevel, DoctorCheck, DoctorReport, LogSnapshot,
    NetworkDiagnostics, NetworkInterfaceDiagnostic, NetworkPathCheck, ResetResult,
    WatchFolderStatus, WatchStatus,
};
use swarmotter_core::net::{self, InterfaceProbe, NetworkBinder, OsInterfaceProbe};
use swarmotter_core::queue::QueueState;
use swarmotter_core::ratio::{self, SeedDecision, TorrentAccounting};
use swarmotter_core::torrent::{ContainmentRecoveryIntent, Torrent, TorrentRegistry};
use swarmotter_core::tracker::{self, AnnounceEvent, AnnounceRequest};
use swarmotter_core::udp_tracker;
use swarmotter_core::watch;

use crate::containment_gate::ContainmentGate;
use crate::engine::{EngineCommand, EngineState, TorrentEngine};
use crate::netbinder::ContainedBinder;
use crate::peer_permits::{
    PeerPermitPool, PeerPermitSnapshot, PeerSessionBudget, DEFAULT_PER_TORRENT_PEER_LIMIT,
};
use crate::seeder::{SeedRegistration, SeedRegistry, SeederHub};

const PEER_DIAGNOSTIC_RECENT_WINDOW: Duration = Duration::from_secs(30);
const QUEUE_RECONCILE_DEBOUNCE: Duration = Duration::from_millis(25);
const AUTOPILOT_INTERVAL: Duration = Duration::from_secs(10);
const AUTOPILOT_ACTION_COOLDOWN: Duration = Duration::from_secs(60);
const AUTOPILOT_QUEUE_RELEASE_RETRY_DELAY: Duration = Duration::from_secs(60);
const ENGINE_INCOMPLETE_RETRY_DELAY: Duration = Duration::from_secs(60);
const AUTOPILOT_STATE_LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const MAGNET_METADATA_NO_PEERS_RETRY_DELAY: Duration = Duration::from_secs(60);
const MAGNET_METADATA_NO_PEERS_RETRY_MESSAGE: &str =
    "magnet metadata fetch: no peers discovered; will retry";
const STALE_ACTIVE_RECOVERY_MESSAGE: &str =
    "active torrent had no running engine; queued for lifecycle recovery";
const STALE_INACTIVE_ENGINE_RECOVERY_MESSAGE: &str =
    "queued torrent had stale engine bookkeeping; queued for lifecycle recovery";
static CONFIG_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct PeerPermitConfiguration {
    global: Arc<PeerPermitPool>,
    per_torrent: HashMap<InfoHash, Arc<PeerPermitPool>>,
}

#[cfg(test)]
type AsyncTestPause = Arc<
    Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
>;

#[derive(Debug, Clone)]
struct WatchObservation {
    fingerprint: watch::FileFingerprint,
    stable_scans: usize,
    processed_fingerprint: Option<watch::FileFingerprint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TorrentAddMutationOutcome {
    Inserted { hash: InfoHash, state: TorrentState },
    Duplicate { hash: InfoHash },
}

enum WatchReadOutcome {
    Stable(Vec<u8>),
    Changed(watch::FileFingerprint),
}

#[derive(Debug, Clone, Default)]
struct LivePeerWorkSnapshot {
    downloads: Vec<LiveTorrentTaskSnapshot>,
    seeders: Vec<LiveTorrentTaskSnapshot>,
}

impl LivePeerWorkSnapshot {
    fn is_empty(&self) -> bool {
        self.downloads.is_empty() && self.seeders.is_empty()
    }
}

#[derive(Debug, Clone)]
struct LiveTorrentTaskSnapshot {
    hash: InfoHash,
    state: TorrentState,
    seeding_status: SeedingStatus,
    error: Option<String>,
    containment_recovery_intent: Option<ContainmentRecoveryIntent>,
}

struct RecoveredSeederStart {
    meta: meta::TorrentMeta,
    active_dir: String,
    complete_dir: String,
    state: Arc<Mutex<EngineState>>,
}

impl LiveTorrentTaskSnapshot {
    fn from_torrent(hash: InfoHash, torrent: &Torrent) -> Self {
        Self {
            hash,
            state: torrent.state,
            seeding_status: torrent.seeding_status,
            error: torrent.error.clone(),
            containment_recovery_intent: torrent.containment_recovery_intent,
        }
    }
}

#[derive(Debug, Clone)]
enum ConfigFileSnapshot {
    Missing,
    Bytes(Vec<u8>),
}

#[derive(Clone)]
pub struct DaemonRuntime {
    pub registry: Arc<Mutex<TorrentRegistry>>,
    pub config: Arc<RwLock<Config>>,
    pub network_health: Arc<RwLock<NetworkHealth>>,
    pub watch_imports: Arc<Mutex<VecDeque<watch::ImportResult>>>,
    watch_observations: Arc<Mutex<HashMap<watch::ObservationKey, WatchObservation>>>,
    /// Prevents the background loop and manual scan endpoint from processing
    /// the same eligible fingerprint concurrently.
    watch_scan_lock: Arc<Mutex<()>>,
    config_path: Option<PathBuf>,
    config_write_lock: Arc<Mutex<()>>,
    /// Serializes engine construction with data-plane configuration swaps.
    /// The guard spans binder creation and task bookkeeping so no task can be
    /// born under a policy that is being replaced.
    data_plane_transition_lock: Arc<Mutex<()>>,
    log_file_path: Option<PathBuf>,
    state_path: Option<PathBuf>,
    state_write_lock: Arc<Mutex<()>>,
    storage_ownership_lock: Arc<Mutex<()>>,
    engine_states: Arc<RwLock<HashMap<InfoHash, Arc<Mutex<EngineState>>>>>,
    engine_cmds: Arc<Mutex<HashMap<InfoHash, tokio::sync::mpsc::Sender<EngineCommand>>>>,
    engine_handles: Arc<RwLock<HashMap<InfoHash, JoinHandle<()>>>>,
    seeder_shutdowns: Arc<Mutex<HashMap<InfoHash, tokio::sync::watch::Sender<bool>>>>,
    seeder_registry: SeedRegistry,
    /// Serializes the live registry with coarse/fine lifecycle transitions.
    seeder_lifecycle_lock: Arc<Mutex<()>>,
    seeder_listener_shutdown: Arc<Mutex<Option<tokio::sync::watch::Sender<bool>>>>,
    seeder_listener_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Per-torrent tracker announce sidecars. The TCP listener itself is
    /// process-wide and stored in `seeder_listener_handle`.
    seeder_handles: Arc<Mutex<HashMap<InfoHash, JoinHandle<()>>>>,
    /// Runtime-owned process-wide peer budget plus retained per-torrent
    /// budgets shared by inbound, metadata, serial, parallel, endgame, TCP,
    /// and uTP sessions. See ADR-0053.
    peer_permit_pool: Arc<RwLock<Arc<PeerPermitPool>>>,
    torrent_peer_permit_pools: Arc<RwLock<HashMap<InfoHash, Arc<PeerPermitPool>>>>,
    peer_sessions_denied: Arc<AtomicU64>,
    /// Last committed selfish-completion policy. Provisional configuration
    /// reconstruction must not perform irreversible removals before a full
    /// replacement is durably committed.
    selfish_completion_enabled: Arc<AtomicBool>,
    /// Deterministic post-teardown failure injection for transactional
    /// reconstruction tests. Production builds never include this hook.
    #[cfg(test)]
    peer_reconfiguration_fail_after_teardown: Arc<AtomicBool>,
    /// Deterministic failure after successful provisional reconstruction but
    /// before the candidate full configuration is persisted.
    #[cfg(test)]
    peer_reconfiguration_fail_persistence: Arc<AtomicBool>,
    /// Test-only pause immediately before candidate reconstruction while the
    /// transition lock is still owned.
    #[cfg(test)]
    peer_reconfiguration_pause: AsyncTestPause,
    #[cfg(test)]
    peer_reconfiguration_persistence_pause: AsyncTestPause,
    /// Deterministic shared-add persistence failure injection.
    #[cfg(test)]
    add_mutation_fail_persistence: Arc<AtomicBool>,
    /// Deterministic pause between bounded watch read and metadata recheck.
    #[cfg(test)]
    watch_after_read_pause: AsyncTestPause,
    global_limiter: swarmotter_core::bandwidth::RateLimiter,
    /// One retained live limiter per torrent. The same buckets are shared by
    /// downloader and seeder tasks and survive lifecycle transitions.
    torrent_limiters: Arc<RwLock<HashMap<InfoHash, Arc<swarmotter_core::bandwidth::RateLimiter>>>>,
    rate_samples: Arc<RwLock<HashMap<InfoHash, RateSample>>>,
    engine_retry_after: Arc<RwLock<HashMap<InfoHash, Instant>>>,
    autopilot_decisions: Arc<RwLock<HashMap<InfoHash, AutopilotDecision>>>,
    autopilot_last_action: Arc<RwLock<HashMap<InfoHash, Instant>>>,
    queue: Arc<Mutex<QueueState>>,
    dht_runner: Arc<Mutex<Option<Arc<crate::dht::DhtRunner>>>>,
    queue_reconcile: Arc<Mutex<QueueReconcileState>>,
    event_broker: EventBroker,
    /// Process-wide containment gate shared by every data-plane component.
    /// See ADR-0051.
    pub(crate) containment_gate: Arc<ContainmentGate>,
    /// Injected interface probe. Production injects `OsInterfaceProbe`; tests
    /// inject a mutable fake. See ADR-0051.
    pub(crate) interface_probe: Arc<dyn InterfaceProbe + Send + Sync>,
    /// Runtime health-report channel for bind/listen/source-bind failures.
    /// A report blocks the gate and exposes `socket_bind_failed`.
    health_report_tx: tokio::sync::mpsc::UnboundedSender<HealthReport>,
    health_report_rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<HealthReport>>>,
    /// A socket/source/listener bind failure remains authoritative until an
    /// explicit configuration replacement successfully revalidates every
    /// required bind. A healthy interface probe alone cannot clear it.
    bind_failure_latched: Arc<RwLock<Option<HealthReport>>>,
}

impl DaemonRuntime {
    /// Borrow the process-wide containment gate. See ADR-0051.
    #[allow(dead_code)]
    pub fn containment_gate(&self) -> &ContainmentGate {
        &self.containment_gate
    }

    /// Send a runtime health report (used by tests to inject bind-failure
    /// transitions). See ADR-0051.
    #[allow(dead_code)]
    pub fn report_health(&self, status: NetworkContainmentStatus, detail: impl Into<String>) {
        let _ = self.health_report_tx.send(HealthReport {
            status,
            detail: detail.into(),
        });
    }

    /// Whether the engine handle registry is empty (test/diagnostic helper).
    #[allow(dead_code)]
    pub async fn engine_handles_empty(&self) -> bool {
        self.engine_handles.read().await.is_empty()
    }

    /// Whether a non-finished engine task is registered for one torrent.
    #[allow(dead_code)]
    pub async fn engine_running_for_test(&self, hash: &InfoHash) -> bool {
        self.engine_handles
            .read()
            .await
            .get(hash)
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Whether the seeder registries are empty (test/diagnostic helper).
    #[allow(dead_code)]
    pub async fn seeder_registries_empty(&self) -> bool {
        self.seeder_shutdowns.lock().await.is_empty() && self.seeder_handles.lock().await.is_empty()
    }

    /// Construct the exact production data-plane binder for containment
    /// integration tests. Tests use this to prove already-created UDP sockets
    /// and policy/bind failure reporting, rather than calling gate/report
    /// helpers directly.
    #[allow(dead_code)]
    pub async fn data_plane_binder_for_test(&self) -> Arc<dyn swarmotter_core::net::NetworkBinder> {
        self.make_binder().await
    }

    /// Expose retained peer-pool identities to production-path integration
    /// tests that verify atomic data-plane reconstruction.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub async fn peer_permit_pools_for_test(
        &self,
        hash: &InfoHash,
    ) -> Option<(Arc<PeerPermitPool>, Arc<PeerPermitPool>)> {
        Some((
            self.peer_permit_pool.read().await.clone(),
            self.torrent_peer_permit_pools
                .read()
                .await
                .get(hash)
                .cloned()?,
        ))
    }
}

/// A runtime health report from a bind/listen/source-bind failure.
#[derive(Debug, Clone)]
pub struct HealthReport {
    pub status: NetworkContainmentStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy)]
struct RateSample {
    downloaded: u64,
    uploaded: u64,
    rate_down: u64,
    rate_up: u64,
    last_download_at: Option<Instant>,
    last_upload_at: Option<Instant>,
    no_download_since: Option<Instant>,
    at: Instant,
    /// Highest observed download rate for this torrent, considering both the
    /// current smoothed rate and the instantaneous reconciliation sample. Used
    /// by the health calculator as a normalization reference when no bandwidth
    /// cap is set.
    peak_rate_down: u64,
    /// Highest observed upload rate for this torrent, considering both the
    /// current smoothed rate and the instantaneous reconciliation sample. This
    /// is recorded for operational troubleshooting and structured performance
    /// logs.
    peak_rate_up: u64,
}

#[derive(Debug, Clone, Default)]
struct LiveTorrentDiagnostics {
    active_peer_workers: usize,
    known_peers: usize,
    peer_scheduler: Option<PeerSchedulerDiagnostics>,
    useful_peers: Option<usize>,
    choked_peers: Option<usize>,
    unchoked_peers: Option<usize>,
    recent_peer_failures: Option<u32>,
    recent_tracker_failures: Option<u32>,
    tracker_ok: bool,
    tracker_message: Option<String>,
    last_announce: Option<u64>,
    tracker_last_ok_seconds_ago: Option<u64>,
    dht_discovery_ok: Option<bool>,
    dht_last_seen_seconds_ago: Option<u64>,
    pex_discovery_ok: Option<bool>,
    pex_last_seen_seconds_ago: Option<u64>,
}

#[derive(Debug, Default)]
struct QueueReconcileState {
    scheduled: bool,
    dirty: bool,
}

#[derive(Debug, Clone, Default)]
struct StorageRootAccumulator {
    roles: Vec<StorageRootRole>,
    torrent_count: usize,
    active_torrents: usize,
    active_write_rate: u64,
}

#[derive(Debug, Clone)]
struct EngineStartSnapshot {
    meta: meta::TorrentMeta,
    download_dir: Option<String>,
    download_limit: u64,
    upload_limit: u64,
    needs_metadata: bool,
    magnet_info_hash: Option<InfoHash>,
    magnet_name: Option<String>,
    magnet_trackers: Vec<String>,
    priorities: Vec<FilePriority>,
    wanted: Vec<bool>,
}

impl EngineStartSnapshot {
    fn from_torrent(torrent: &Torrent) -> Self {
        Self {
            meta: torrent.meta.clone(),
            download_dir: torrent.download_dir.clone(),
            download_limit: torrent.download_limit,
            upload_limit: torrent.upload_limit,
            needs_metadata: torrent.needs_metadata,
            magnet_info_hash: torrent.magnet_info_hash,
            magnet_name: torrent.magnet_name.clone(),
            magnet_trackers: torrent.magnet_trackers.clone(),
            priorities: torrent.priorities.clone(),
            wanted: torrent.wanted.clone(),
        }
    }

    fn magnet_params(&self) -> Option<crate::engine::MagnetParams> {
        self.needs_metadata.then(|| crate::engine::MagnetParams {
            info_hash: self.magnet_info_hash.unwrap_or(self.meta.info_hash),
            name: self
                .magnet_name
                .clone()
                .unwrap_or_else(|| self.meta.name.clone()),
            trackers: self.magnet_trackers.clone(),
        })
    }
}

impl LiveTorrentDiagnostics {
    fn from_engine_state(s: &EngineState, now: Instant) -> Self {
        let unchoked_peers = s
            .peer_health
            .values()
            .filter(|p| {
                p.unchoked
                    && p.last_seen
                        .map(|seen| now.duration_since(seen) < PEER_DIAGNOSTIC_RECENT_WINDOW)
                        .unwrap_or(false)
            })
            .count();
        Self {
            active_peer_workers: s.active_peers,
            known_peers: s.peers.len(),
            peer_scheduler: Some(s.peer_scheduler.clone()),
            useful_peers: Some(useful_peer_count(&s.peer_health, now)),
            choked_peers: None,
            unchoked_peers: Some(unchoked_peers),
            recent_peer_failures: Some(s.peer_disconnects_recent),
            recent_tracker_failures: Some(s.tracker_failures_recent),
            tracker_ok: s.tracker_ok,
            tracker_message: s.tracker_message.clone(),
            last_announce: s.last_announce,
            tracker_last_ok_seconds_ago: instant_age_seconds(now, s.tracker_last_ok),
            dht_discovery_ok: Some(s.dht_discovery_ok),
            dht_last_seen_seconds_ago: instant_age_seconds(now, s.dht_last_seen),
            pex_discovery_ok: Some(s.pex_discovery_ok),
            pex_last_seen_seconds_ago: instant_age_seconds(now, s.pex_last_seen),
        }
    }
}

fn useful_peer_count(
    peer_health: &HashMap<std::net::SocketAddr, EnginePeerHealth>,
    now: Instant,
) -> usize {
    peer_health
        .values()
        .filter(|p| {
            let last_seen_recent = p
                .last_seen
                .map(|seen| now.duration_since(seen) < PEER_DIAGNOSTIC_RECENT_WINDOW)
                .unwrap_or(false);
            p.has_missing_pieces
                && !p.blocked
                && (p.unchoked || p.useful_recently)
                && last_seen_recent
        })
        .count()
}

fn instant_age_seconds(now: Instant, seen: Option<Instant>) -> Option<u64> {
    seen.map(|instant| now.saturating_duration_since(instant).as_secs())
}

fn default_download_dir_string() -> String {
    std::env::temp_dir()
        .join("swarmotter-downloads")
        .display()
        .to_string()
}

fn resolve_download_dir_from_config(download_dir: Option<&str>, cfg: &Config) -> String {
    download_dir
        .map(str::to_string)
        .or_else(|| cfg.storage.download_dir.clone())
        .unwrap_or_else(default_download_dir_string)
}

fn resolve_incomplete_dir_from_config(download_dir: &str, cfg: &Config) -> String {
    cfg.storage
        .incomplete_dir
        .clone()
        .unwrap_or_else(|| download_dir.to_string())
}

fn torrent_event(kind: &'static str, hash: InfoHash, state: TorrentState) -> Event {
    let info_hash = hash.to_hex();
    Event::new(
        kind,
        json!({
            "info_hash": info_hash,
            "state": state.as_str(),
        }),
    )
    .with_info_hash(info_hash)
}

fn torrent_removed_event(hash: InfoHash, delete_data: bool) -> Event {
    let info_hash = hash.to_hex();
    Event::new(
        "torrent_removed",
        json!({
            "info_hash": info_hash,
            "delete_data": delete_data,
        }),
    )
    .with_info_hash(info_hash)
}

fn torrent_metadata_event(hash: InfoHash) -> Event {
    let info_hash = hash.to_hex();
    Event::new(
        "torrent_metadata_received",
        json!({
            "info_hash": info_hash,
        }),
    )
    .with_info_hash(info_hash)
}

fn stats_updated_event() -> Event {
    Event::new("stats_updated", json!({}))
}

impl DaemonRuntime {
    #[allow(dead_code)]
    pub fn new(config: Config, startup_health: NetworkHealth) -> Self {
        Self::with_paths(config, startup_health, None, None)
    }

    pub fn with_paths(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
    ) -> Self {
        Self::with_paths_and_broker(
            config,
            startup_health,
            config_path,
            log_file_path,
            EventBroker::default(),
        )
    }

    pub fn with_paths_and_broker(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
        event_broker: EventBroker,
    ) -> Self {
        Self::with_paths_broker_and_state(
            config,
            startup_health,
            config_path,
            log_file_path,
            None,
            event_broker,
        )
    }

    pub fn with_paths_broker_and_state(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
        state_path: Option<PathBuf>,
        event_broker: EventBroker,
    ) -> Self {
        Self::with_paths_broker_state_and_probe(
            config,
            startup_health,
            config_path,
            log_file_path,
            state_path,
            event_broker,
            Arc::new(OsInterfaceProbe),
        )
    }

    /// Construct a runtime with an injected interface probe for deterministic
    /// containment testing. Production injects `OsInterfaceProbe`; tests inject
    /// a mutable fake. See ADR-0051.
    #[allow(clippy::too_many_arguments)]
    pub fn with_paths_broker_state_and_probe(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
        state_path: Option<PathBuf>,
        event_broker: EventBroker,
        interface_probe: Arc<dyn InterfaceProbe + Send + Sync>,
    ) -> Self {
        let global_limiter = swarmotter_core::bandwidth::RateLimiter::new(
            config.bandwidth.effective_download(),
            config.bandwidth.effective_upload(),
        );
        let selfish_completion_enabled = config.torrent.selfish;
        let peer_sessions_denied = Arc::new(AtomicU64::new(0));
        let peer_permit_pool =
            PeerPermitPool::new(config.bandwidth.max_peers, peer_sessions_denied.clone())
                .unwrap_or_else(|_| {
                    PeerPermitPool::invalid_fail_closed(
                        config.bandwidth.max_peers,
                        peer_sessions_denied.clone(),
                    )
                });
        let containment_gate = ContainmentGate::new(startup_health.traffic_allowed);
        if !startup_health.traffic_allowed {
            containment_gate.block(startup_health.status, startup_health.detail.clone());
        }
        let (health_report_tx, health_report_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            registry: Arc::new(Mutex::new(TorrentRegistry::default())),
            queue: Arc::new(Mutex::new(QueueState::new(config.queue.clone()))),
            config: Arc::new(RwLock::new(config)),
            network_health: Arc::new(RwLock::new(startup_health)),
            watch_imports: Arc::new(Mutex::new(VecDeque::new())),
            watch_observations: Arc::new(Mutex::new(HashMap::new())),
            watch_scan_lock: Arc::new(Mutex::new(())),
            config_path,
            config_write_lock: Arc::new(Mutex::new(())),
            data_plane_transition_lock: Arc::new(Mutex::new(())),
            log_file_path,
            state_path,
            state_write_lock: Arc::new(Mutex::new(())),
            storage_ownership_lock: Arc::new(Mutex::new(())),
            engine_states: Arc::new(RwLock::new(HashMap::new())),
            engine_cmds: Arc::new(Mutex::new(HashMap::new())),
            engine_handles: Arc::new(RwLock::new(HashMap::new())),
            seeder_shutdowns: Arc::new(Mutex::new(HashMap::new())),
            seeder_registry: SeedRegistry::default(),
            seeder_lifecycle_lock: Arc::new(Mutex::new(())),
            seeder_listener_shutdown: Arc::new(Mutex::new(None)),
            seeder_listener_handle: Arc::new(Mutex::new(None)),
            seeder_handles: Arc::new(Mutex::new(HashMap::new())),
            peer_permit_pool: Arc::new(RwLock::new(peer_permit_pool)),
            torrent_peer_permit_pools: Arc::new(RwLock::new(HashMap::new())),
            peer_sessions_denied,
            selfish_completion_enabled: Arc::new(AtomicBool::new(selfish_completion_enabled)),
            #[cfg(test)]
            peer_reconfiguration_fail_after_teardown: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            peer_reconfiguration_fail_persistence: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            peer_reconfiguration_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            peer_reconfiguration_persistence_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            add_mutation_fail_persistence: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            watch_after_read_pause: Arc::new(Mutex::new(None)),
            global_limiter,
            torrent_limiters: Arc::new(RwLock::new(HashMap::new())),
            rate_samples: Arc::new(RwLock::new(HashMap::new())),
            engine_retry_after: Arc::new(RwLock::new(HashMap::new())),
            autopilot_decisions: Arc::new(RwLock::new(HashMap::new())),
            autopilot_last_action: Arc::new(RwLock::new(HashMap::new())),
            dht_runner: Arc::new(Mutex::new(None)),
            queue_reconcile: Arc::new(Mutex::new(QueueReconcileState::default())),
            event_broker,
            containment_gate,
            interface_probe,
            health_report_tx,
            health_report_rx: Arc::new(Mutex::new(health_report_rx)),
            bind_failure_latched: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn restore_persisted_state(&self) -> Result<usize> {
        let Some(path) = self.state_path.clone() else {
            return Ok(0);
        };
        let Some(mut stored) = tokio::task::spawn_blocking(move || crate::state_store::load(&path))
            .await
            .map_err(|error| CoreError::Storage(format!("load daemon state task: {error}")))??
        else {
            return Ok(0);
        };
        let traffic_allowed = self.network_health.read().await.traffic_allowed;
        let restore_seeding_policy = self.config.read().await.seeding.clone();
        let mut restored = TorrentRegistry::default();
        for mut torrent in stored.torrents.drain(..) {
            let persisted_state = torrent.state;
            if torrent
                .seeding
                .ratio_limit
                .is_some_and(|value| !value.is_finite() || value < 0.0)
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has invalid seeding.ratio_limit",
                    torrent.info_hash()
                )));
            }
            torrent.meta.validate().map_err(|error| {
                CoreError::Storage(format!(
                    "invalid metadata for restored torrent {}: {error}",
                    torrent.info_hash()
                ))
            })?;
            if torrent.files.len() != torrent.meta.files.len()
                || torrent.priorities.len() != torrent.meta.files.len()
                || torrent.wanted.len() != torrent.meta.files.len()
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent file settings",
                    torrent.info_hash()
                )));
            }
            if torrent.needs_metadata != torrent.magnet_info_hash.is_some() {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent magnet identity",
                    torrent.info_hash()
                )));
            }
            let piece_count = torrent.meta.piece_count();
            let expected_bitfield_bytes = piece_count.div_ceil(8);
            if torrent.progress.total != piece_count
                || torrent.progress.bitfield().as_bytes().len() != expected_bitfield_bytes
                || (piece_count..expected_bitfield_bytes.saturating_mul(8))
                    .any(|index| torrent.progress.bitfield().has(index))
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent piece progress",
                    torrent.info_hash()
                )));
            }
            let restored_bitfield = torrent.progress.bitfield().clone();
            torrent
                .progress
                .replace_from_bitfield(&restored_bitfield, piece_count);
            let previous_files = std::mem::take(&mut torrent.files);
            torrent.files = torrent
                .meta
                .files
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    let bytes_completed = previous_files[index].bytes_completed;
                    if bytes_completed > file.length {
                        return Err(CoreError::Storage(format!(
                            "daemon state for {} has file progress beyond file length",
                            torrent.info_hash()
                        )));
                    }
                    Ok(TorrentFile {
                        index,
                        path: file.path.join("/"),
                        length: file.length,
                        bytes_completed,
                        priority: torrent.priorities[index],
                        wanted: torrent.wanted[index],
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            torrent.recompute_file_bytes_completed();
            torrent.rate_down = 0;
            torrent.rate_up = 0;
            torrent.active_peer_workers = 0;
            torrent.known_peers = 0;
            torrent.health = swarmotter_core::models::torrent::TorrentHealth::unknown();
            torrent.state = match torrent.state {
                TorrentState::Downloading => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::Downloading);
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::DownloadingMetadata => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::DownloadingMetadata);
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::Checking => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        // Recheck is storage-only and was not a live torrent
                        // transport. Preserve the block without granting an
                        // automatic network recovery intent.
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::Seeding => {
                    if traffic_allowed {
                        TorrentState::Completed
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::Seeding);
                        TorrentState::NetworkBlocked
                    }
                }
                state => state,
            };
            if traffic_allowed && torrent.state == TorrentState::NetworkBlocked {
                if let Some(intent) = torrent.containment_recovery_intent.take() {
                    torrent.error = None;
                    torrent.state = match intent {
                        ContainmentRecoveryIntent::Downloading
                        | ContainmentRecoveryIntent::DownloadingMetadata => TorrentState::Queued,
                        ContainmentRecoveryIntent::Seeding => TorrentState::Completed,
                    };
                }
            }
            recompute_restored_seeding_lifecycle(
                &mut torrent,
                persisted_state,
                &restore_seeding_policy,
                now(),
            );
            let hash = torrent.info_hash();
            restored.add(torrent).map_err(|_| {
                CoreError::Storage(format!("duplicate torrent {hash} in daemon state"))
            })?;
        }

        let config = self.config.read().await.clone();
        validate_restored_storage_ownership(restored.torrents.values(), &config)?;

        let known = restored.torrents.keys().copied().collect::<HashSet<_>>();
        let stale_queue_entries = stored
            .queue
            .order
            .iter()
            .filter(|hash| !known.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        stored.queue.remove_many(stale_queue_entries);
        stored.queue.add_many(known.iter().copied());
        stored.queue.limits = self.config.read().await.queue.clone();
        let count = restored.torrents.len();
        *self.torrent_limiters.write().await = restored
            .torrents
            .iter()
            .map(|(hash, torrent)| {
                (
                    *hash,
                    Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                        torrent.download_limit,
                        torrent.upload_limit,
                    )),
                )
            })
            .collect();
        let per_torrent_peer_limit =
            Self::effective_per_torrent_peer_limit(config.bandwidth.max_peers_per_torrent);
        *self.torrent_peer_permit_pools.write().await = restored
            .torrents
            .keys()
            .map(|hash| {
                PeerPermitPool::new(per_torrent_peer_limit, self.peer_sessions_denied.clone())
                    .map(|pool| (*hash, pool))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        *self.registry.lock().await = restored;
        *self.queue.lock().await = stored.queue;
        self.verify_restored_completed_torrents().await?;
        self.reconcile_queue().await;
        self.reconcile_seeders().await;
        self.persist_state().await?;
        tracing::info!(count, path = %self.state_path.as_ref().unwrap().display(), "restored daemon state");
        Ok(count)
    }

    async fn verify_restored_completed_torrents(&self) -> Result<()> {
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Completed)
            .cloned()
            .collect::<Vec<_>>();
        for torrent in torrents {
            let hash = torrent.info_hash();
            let complete_dir = self.resolve_download_dir(&torrent).await;
            let storage_dir = if torrent.progress.is_complete() {
                complete_dir
            } else {
                self.resolve_incomplete_dir(&complete_dir).await
            };
            let storage = swarmotter_core::storage::StorageIo::new(
                torrent.meta.clone(),
                PathBuf::from(storage_dir),
            );
            match storage.recheck().await {
                Ok(bitfield) => {
                    let selection_complete = torrent_selection_complete(&torrent, &bitfield)?;
                    let traffic_allowed = self.network_health.read().await.traffic_allowed;
                    if let Some(restored) = self.registry.lock().await.get_mut(&hash) {
                        restored
                            .progress
                            .replace_from_bitfield(&bitfield, restored.meta.piece_count());
                        restored.recompute_file_bytes_completed();
                        if !selection_complete {
                            restored.state = if traffic_allowed {
                                TorrentState::Queued
                            } else {
                                TorrentState::NetworkBlocked
                            };
                            restored.error = Some(
                                "restored payload failed verification; selected pieces queued for recovery"
                                    .into(),
                            );
                            restored.seeding_status = SeedingStatus::NotEligible;
                        } else {
                            restored.seeding_status = SeedingStatus::Queued;
                        }
                    }
                }
                Err(error) => {
                    if let Some(restored) = self.registry.lock().await.get_mut(&hash) {
                        restored.state = TorrentState::StorageError;
                        restored.error = Some(error.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    async fn persist_state(&self) -> Result<()> {
        let Some(path) = self.state_path.clone() else {
            return Ok(());
        };
        let _write_guard = self.state_write_lock.lock().await;
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        let queue = self.queue.lock().await.clone();
        let state = crate::state_store::DaemonState::new(torrents, queue);
        tokio::task::spawn_blocking(move || crate::state_store::save(&path, &state))
            .await
            .map_err(|error| CoreError::Storage(format!("save daemon state task: {error}")))??;
        Ok(())
    }

    async fn persist_state_best_effort(&self, reason: &'static str) {
        if let Err(error) = self.persist_state().await {
            tracing::error!(reason, %error, "failed to persist daemon state");
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.reconcile_engine_progress().await;
        let hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.stop_all_torrent_tasks(&hashes).await;
        self.persist_state().await
    }

    fn publish_event(&self, event: Event) {
        self.event_broker.publish(event);
    }

    fn publish_torrent_event(&self, kind: &'static str, hash: InfoHash, state: TorrentState) {
        self.publish_event(torrent_event(kind, hash, state));
    }

    #[allow(dead_code)]
    pub async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        download_dir: Option<String>,
    ) -> Result<InfoHash> {
        self.add_torrent_file_with_options(bytes, AddTorrentOptions::new(download_dir, false))
            .await
    }

    #[allow(dead_code)]
    pub async fn add_magnet(&self, magnet: &str, download_dir: Option<String>) -> Result<InfoHash> {
        self.add_magnet_with_options(magnet, AddTorrentOptions::new(download_dir, false))
            .await
    }

    pub async fn add_torrent_file_with_options(
        &self,
        bytes: Vec<u8>,
        options: AddTorrentOptions,
    ) -> Result<InfoHash> {
        let parsed = match meta::parse_torrent(&bytes) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    error_code = %e.code(),
                    "torrent file add rejected"
                );
                return Err(e);
            }
        };
        let hash = parsed.info_hash;
        let mut t = Torrent::new(parsed, now());
        if let Some(d) = options.download_dir {
            t.download_dir = Some(d);
        }
        match self
            .add_torrent_mutation(t, options.paused, "torrent_file_added")
            .await?
        {
            TorrentAddMutationOutcome::Inserted { state, .. } => {
                tracing::info!(
                    info_hash = %hash,
                    network_blocked = state == TorrentState::NetworkBlocked,
                    paused = state == TorrentState::Paused,
                    "torrent file added"
                );
                Ok(hash)
            }
            TorrentAddMutationOutcome::Duplicate { .. } => {
                tracing::warn!(
                    info_hash = %hash,
                    error_code = %CoreError::DuplicateTorrent(hash.to_hex()).code(),
                    "torrent file add rejected: duplicate"
                );
                Err(CoreError::DuplicateTorrent(hash.to_hex()))
            }
        }
    }

    async fn add_magnet_with_options(
        &self,
        magnet: &str,
        options: AddTorrentOptions,
    ) -> Result<InfoHash> {
        let m = Magnet::parse(magnet)?;
        let hash = m.info_hash;
        let name = m.display_name.clone().unwrap_or_else(|| hash.to_hex());
        // Build a placeholder single-file torrent so the registry has a record;
        // the real metadata is fetched via BEP 9 from peers once the engine
        // starts. The registry is keyed by the magnet's real info hash.
        let bytes = meta::build_single_file_torrent(
            &name,
            b"magnet placeholder data",
            16,
            m.trackers.first().map(|s| s.as_str()),
            false,
        );
        let mut parsed = meta::parse_torrent(&bytes)?;
        // Placeholder storage ownership must use the magnet's real identity.
        // Otherwise two different magnets with the same display name produce
        // the same synthetic metainfo hash and bypass conflict detection.
        parsed.info_hash = hash;
        let mut t = Torrent::new(parsed, now());
        t.needs_metadata = true;
        t.magnet_info_hash = Some(hash);
        t.magnet_name = Some(name);
        t.magnet_trackers = m.trackers.clone();
        if let Some(d) = options.download_dir {
            t.download_dir = Some(d);
        }
        match self
            .add_torrent_mutation(t, options.paused, "magnet_added")
            .await?
        {
            TorrentAddMutationOutcome::Inserted { state, .. } => {
                tracing::info!(
                    info_hash = %hash,
                    network_blocked = state == TorrentState::NetworkBlocked,
                    paused = state == TorrentState::Paused,
                    tracker_count = m.trackers.len(),
                    "magnet added"
                );
                Ok(hash)
            }
            TorrentAddMutationOutcome::Duplicate { .. } => {
                Err(CoreError::DuplicateTorrent(hash.to_hex()))
            }
        }
    }

    /// Shared durable add transaction for API, magnet, and watch ingestion.
    /// Parsing happens before entry. Storage and containment preflight mutate
    /// only the candidate. The storage-ownership lock then spans path
    /// validation, exact hash snapshots, insertion, persistence, and rollback.
    async fn add_torrent_mutation(
        &self,
        mut torrent: Torrent,
        requested_paused: bool,
        schedule_reason: &'static str,
    ) -> Result<TorrentAddMutationOutcome> {
        let hash = torrent.info_hash();
        let mutation_guard = self.storage_ownership_lock.lock().await;
        let previous_torrent = self.registry.lock().await.get(&hash).cloned();
        let previous_queue = self.queue.lock().await.membership_snapshot(&hash);
        if previous_torrent.is_some() {
            return Ok(TorrentAddMutationOutcome::Duplicate { hash });
        }
        self.preflight_storage_for_download(
            torrent.download_dir.as_deref(),
            if torrent.needs_metadata {
                0
            } else {
                torrent.meta.total_length
            },
        )
        .await?;
        apply_network_state(&mut torrent, &self.network_health).await;
        if requested_paused && torrent.state != TorrentState::NetworkBlocked {
            torrent.state = TorrentState::Paused;
        }
        let committed_state = torrent.state;

        self.ensure_storage_paths_available(&torrent.meta, torrent.download_dir.as_deref())
            .await?;

        self.registry
            .lock()
            .await
            .add(torrent)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        self.queue.lock().await.add(hash);

        let persistence = if self.add_mutation_persistence_failure_injected() {
            Err(CoreError::Storage(
                "injected shared torrent-add persistence failure".into(),
            ))
        } else {
            self.persist_state().await
        };
        if let Err(error) = persistence {
            let mut registry = self.registry.lock().await;
            registry.remove(&hash);
            if let Some(previous) = previous_torrent {
                registry.torrents.insert(hash, previous);
            }
            drop(registry);
            self.queue
                .lock()
                .await
                .restore_membership(hash, previous_queue);
            return Err(error);
        }

        self.ensure_torrent_limiter(hash, 0, 0).await;
        self.ensure_torrent_peer_permit_pool(hash).await;
        drop(mutation_guard);
        if committed_state == TorrentState::Queued {
            self.schedule_reconcile_queue(schedule_reason).await;
        }
        self.publish_torrent_event("torrent_added", hash, committed_state);
        self.publish_event(stats_updated_event());
        Ok(TorrentAddMutationOutcome::Inserted {
            hash,
            state: committed_state,
        })
    }

    async fn remove_torrents_with_single_reconcile(
        &self,
        hashes: Vec<InfoHash>,
        delete_data: bool,
    ) -> Result<Vec<InfoHash>> {
        let mut unique_hashes = Vec::with_capacity(hashes.len());
        let mut seen = HashSet::with_capacity(hashes.len());
        for hash in hashes {
            if seen.insert(hash) {
                unique_hashes.push(hash);
            }
        }

        let targets = {
            let reg = self.registry.lock().await;
            unique_hashes
                .into_iter()
                .filter_map(|hash| reg.get(&hash).cloned().map(|torrent| (hash, torrent)))
                .collect::<Vec<_>>()
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        for (hash, _) in &targets {
            self.force_stop_engine(hash).await;
        }
        if delete_data {
            for (hash, torrent) in &targets {
                let complete_dir = self.resolve_download_dir(torrent).await;
                let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
                let mut dirs = vec![active_dir, complete_dir];
                dirs.dedup();
                for dir in dirs {
                    let storage = swarmotter_core::storage::StorageIo::new(
                        torrent.meta.clone(),
                        std::path::PathBuf::from(&dir),
                    );
                    if let Err(error) = storage.remove_all().await {
                        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                            torrent.state = TorrentState::StorageError;
                            torrent.error = Some(error.to_string());
                        }
                        self.persist_state_best_effort("remove_failed").await;
                        return Err(error);
                    }
                }
            }
        }
        {
            let mut reg = self.registry.lock().await;
            for (hash, _) in &targets {
                reg.remove(hash);
            }
        }
        self.queue
            .lock()
            .await
            .remove_many(targets.iter().map(|(hash, _)| *hash));
        {
            let mut rate_samples = self.rate_samples.write().await;
            let mut decisions = self.autopilot_decisions.write().await;
            let mut last_actions = self.autopilot_last_action.write().await;
            let mut limiters = self.torrent_limiters.write().await;
            let mut peer_permit_pools = self.torrent_peer_permit_pools.write().await;
            for (hash, _) in &targets {
                rate_samples.remove(hash);
                decisions.remove(hash);
                last_actions.remove(hash);
                limiters.remove(hash);
                peer_permit_pools.remove(hash);
            }
        }
        self.persist_state().await?;
        self.reconcile_queue().await;
        let removed_hashes = targets
            .into_iter()
            .map(|(hash, _)| {
                self.publish_event(torrent_removed_event(hash, delete_data));
                hash
            })
            .collect();
        self.publish_event(stats_updated_event());
        Ok(removed_hashes)
    }

    /// Resolve the download directory for a torrent: per-torrent override,
    /// then global config, then a default temp dir.
    async fn resolve_download_dir(&self, t: &Torrent) -> String {
        self.resolve_download_dir_override(t.download_dir.as_deref())
            .await
    }

    async fn ensure_torrent_limiter(
        &self,
        hash: InfoHash,
        download_limit: u64,
        upload_limit: u64,
    ) -> Arc<swarmotter_core::bandwidth::RateLimiter> {
        self.torrent_limiters
            .write()
            .await
            .entry(hash)
            .or_insert_with(|| {
                Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                    download_limit,
                    upload_limit,
                ))
            })
            .clone()
    }

    fn effective_per_torrent_peer_limit(configured: usize) -> usize {
        if configured == 0 {
            DEFAULT_PER_TORRENT_PEER_LIMIT
        } else {
            configured
        }
    }

    async fn ensure_torrent_peer_permit_pool(&self, hash: InfoHash) -> Arc<PeerPermitPool> {
        if let Some(pool) = self
            .torrent_peer_permit_pools
            .read()
            .await
            .get(&hash)
            .cloned()
        {
            return pool;
        }
        let configured = self.config.read().await.bandwidth.max_peers_per_torrent;
        let limit = Self::effective_per_torrent_peer_limit(configured);
        let candidate = PeerPermitPool::new(limit, self.peer_sessions_denied.clone())
            .unwrap_or_else(|_| {
                PeerPermitPool::invalid_fail_closed(limit, self.peer_sessions_denied.clone())
            });
        self.torrent_peer_permit_pools
            .write()
            .await
            .entry(hash)
            .or_insert(candidate)
            .clone()
    }

    async fn peer_session_budget(&self, hash: InfoHash) -> PeerSessionBudget {
        let global = self.peer_permit_pool.read().await.clone();
        let torrent = self.ensure_torrent_peer_permit_pool(hash).await;
        PeerSessionBudget::new(global, torrent)
    }

    async fn peer_permit_snapshot(&self) -> PeerPermitSnapshot {
        self.peer_permit_pool.read().await.snapshot()
    }

    async fn build_peer_permit_configuration(
        &self,
        config: &Config,
    ) -> Result<PeerPermitConfiguration> {
        let global = PeerPermitPool::new(
            config.bandwidth.max_peers,
            self.peer_sessions_denied.clone(),
        )?;
        let per_torrent_limit =
            Self::effective_per_torrent_peer_limit(config.bandwidth.max_peers_per_torrent);
        let hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let per_torrent = hashes
            .into_iter()
            .map(|hash| {
                PeerPermitPool::new(per_torrent_limit, self.peer_sessions_denied.clone())
                    .map(|pool| (hash, pool))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(PeerPermitConfiguration {
            global,
            per_torrent,
        })
    }

    async fn install_peer_permit_configuration(&self, next: PeerPermitConfiguration) {
        *self.peer_permit_pool.write().await = next.global;
        *self.torrent_peer_permit_pools.write().await = next.per_torrent;
    }

    async fn current_peer_permit_configuration(&self) -> PeerPermitConfiguration {
        PeerPermitConfiguration {
            global: self.peer_permit_pool.read().await.clone(),
            per_torrent: self.torrent_peer_permit_pools.read().await.clone(),
        }
    }

    async fn verify_peer_permit_configuration_identity(
        &self,
        expected: &PeerPermitConfiguration,
    ) -> Result<()> {
        let actual = self.current_peer_permit_configuration().await;
        let same = Arc::ptr_eq(&actual.global, &expected.global)
            && actual.global.snapshot().limit == expected.global.snapshot().limit
            && actual.per_torrent.len() == expected.per_torrent.len()
            && expected.per_torrent.iter().all(|(hash, pool)| {
                actual.per_torrent.get(hash).is_some_and(|actual| {
                    Arc::ptr_eq(actual, pool) && actual.snapshot().limit == pool.snapshot().limit
                })
            });
        if same {
            Ok(())
        } else {
            Err(CoreError::Internal(
                "peer permit configuration identity or size mismatch".into(),
            ))
        }
    }

    async fn wait_for_peer_permit_configuration_drain(
        &self,
        permits: &PeerPermitConfiguration,
    ) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let global_drained = permits.global.snapshot().in_use == 0;
                let torrents_drained = permits
                    .per_torrent
                    .values()
                    .all(|pool| pool.snapshot().in_use == 0);
                if global_drained && torrents_drained {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            CoreError::Internal(
                "timed out awaiting old peer-session permits during reconstruction".into(),
            )
        })
    }

    #[cfg(test)]
    fn inject_peer_reconfiguration_failure_after_teardown(&self) {
        self.peer_reconfiguration_fail_after_teardown
            .store(true, Ordering::Release);
    }

    fn peer_reconfiguration_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.peer_reconfiguration_fail_after_teardown
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    fn inject_peer_reconfiguration_persistence_failure(&self) {
        self.peer_reconfiguration_fail_persistence
            .store(true, Ordering::Release);
    }

    fn peer_reconfiguration_persistence_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.peer_reconfiguration_fail_persistence
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    async fn pause_peer_reconfiguration_before_reconstruction(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.peer_reconfiguration_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    async fn wait_at_peer_reconfiguration_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self.peer_reconfiguration_pause.lock().await.take() {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    #[cfg(test)]
    async fn pause_peer_reconfiguration_before_persistence(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.peer_reconfiguration_persistence_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    async fn wait_at_peer_reconfiguration_persistence_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self
            .peer_reconfiguration_persistence_pause
            .lock()
            .await
            .take()
        {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    #[cfg(test)]
    fn inject_add_mutation_persistence_failure(&self) {
        self.add_mutation_fail_persistence
            .store(true, Ordering::Release);
    }

    fn add_mutation_persistence_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.add_mutation_fail_persistence
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    async fn pause_watch_after_bounded_read(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.watch_after_read_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    async fn wait_at_watch_after_read_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self.watch_after_read_pause.lock().await.take() {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    async fn resolve_download_dir_override(&self, download_dir: Option<&str>) -> String {
        let cfg = self.config.read().await;
        resolve_download_dir_from_config(download_dir, &cfg)
    }

    /// Resolve the active write directory for a torrent. Incomplete downloads
    /// use the configured incomplete directory when present; otherwise they
    /// write directly to the final download directory.
    async fn resolve_incomplete_dir(&self, download_dir: &str) -> String {
        let cfg = self.config.read().await;
        resolve_incomplete_dir_from_config(download_dir, &cfg)
    }

    async fn preflight_storage_for_download(
        &self,
        download_dir: Option<&str>,
        total_length: u64,
    ) -> Result<()> {
        let cfg = self.config.read().await.clone();
        if cfg.storage.minimum_free_space_bytes == 0 && cfg.storage.minimum_free_space_percent == 0
        {
            return Ok(());
        }
        let complete_dir = resolve_download_dir_from_config(download_dir, &cfg);
        let active_dir = resolve_incomplete_dir_from_config(&complete_dir, &cfg);
        for dir in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
            swarmotter_core::storage::check_storage_preflight(&dir, &cfg.storage, total_length)?;
        }
        Ok(())
    }

    async fn ensure_storage_paths_available(
        &self,
        meta: &meta::TorrentMeta,
        download_dir: Option<&str>,
    ) -> Result<()> {
        self.ensure_storage_paths_available_except(meta, download_dir, None)
            .await
    }

    async fn ensure_storage_paths_available_except(
        &self,
        meta: &meta::TorrentMeta,
        download_dir: Option<&str>,
        exclude: Option<InfoHash>,
    ) -> Result<()> {
        let cfg = self.config.read().await.clone();
        let complete_dir = resolve_download_dir_from_config(download_dir, &cfg);
        let active_dir = resolve_incomplete_dir_from_config(&complete_dir, &cfg);
        let candidates = unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)])
            .into_iter()
            .map(|root| {
                swarmotter_core::storage::StorageIo::new(meta.clone(), root).path_ownership()
            })
            .collect::<Result<Vec<_>>>()?;
        let existing = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for torrent in existing {
            if exclude.is_some_and(|hash| torrent.info_hash() == hash) {
                continue;
            }
            let complete_dir =
                resolve_download_dir_from_config(torrent.download_dir.as_deref(), &cfg);
            let active_dir = resolve_incomplete_dir_from_config(&complete_dir, &cfg);
            for root in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
                let ownership =
                    swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), root)
                        .path_ownership()?;
                for candidate in &candidates {
                    candidate.ensure_compatible_with(&ownership)?;
                }
            }
        }
        Ok(())
    }

    async fn reserve_resolved_magnet_metadata(
        &self,
        hash: InfoHash,
        resolved: meta::TorrentMeta,
        download_dir: Option<String>,
    ) -> Result<()> {
        if resolved.info_hash != hash {
            return Err(CoreError::MalformedTorrent(
                "resolved magnet metadata info hash changed during preflight".into(),
            ));
        }
        let _storage_ownership = self.storage_ownership_lock.lock().await;
        self.ensure_storage_paths_available_except(&resolved, download_dir.as_deref(), Some(hash))
            .await?;
        let previous = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(&hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.clone();
            let empty_state = EngineState {
                piece_count: resolved.piece_count(),
                total_length: resolved.total_length,
                ..EngineState::default()
            };
            apply_resolved_metadata(torrent, &resolved, &empty_state);
            previous
        };
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                *torrent = previous;
            }
            return Err(error);
        }
        self.publish_event(torrent_metadata_event(hash));
        Ok(())
    }

    async fn configured_peer_worker_limit(&self) -> usize {
        let cfg = self.config.read().await;
        Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent)
    }

    async fn apply_peer_worker_limits(&self) {
        let limit = self.configured_peer_worker_limit().await;
        let senders: Vec<tokio::sync::mpsc::Sender<EngineCommand>> =
            self.engine_cmds.lock().await.values().cloned().collect();
        for tx in senders {
            let _ = tx.send(EngineCommand::UpdatePeerWorkerLimit(limit)).await;
        }
    }

    async fn scheduler_diagnostics(&self, desired: &[InfoHash]) -> SchedulerDiagnostics {
        let cfg = self.config.read().await.clone();
        let mut queue = self.queue.lock().await.clone();
        queue.limits = cfg.queue.clone();
        let retry_after = self.engine_retry_after.read().await.clone();
        let running: HashSet<InfoHash> = self.engine_handles.read().await.keys().copied().collect();
        let now = Instant::now();
        let reg = self.registry.lock().await;

        let mut requested_downloads = 0usize;
        let mut requested_metadata_fetches = 0usize;
        let mut seen = HashSet::new();
        let bypass_set = queue.bypass.iter().copied().collect::<HashSet<_>>();
        for hash in queue.bypass.iter().chain(queue.order.iter()) {
            if !seen.insert(*hash) {
                continue;
            }
            if retry_after
                .get(hash)
                .is_some_and(|retry_at| *retry_at > now)
            {
                continue;
            }
            let Some(torrent) = reg.get(hash) else {
                continue;
            };
            let bypass = bypass_set.contains(hash);
            let already_active = matches!(
                torrent.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            );
            if !(queue.limits.auto_start || bypass || already_active) {
                continue;
            }
            if !matches!(
                torrent.state,
                TorrentState::Queued
                    | TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
            ) {
                continue;
            }
            if torrent.needs_metadata {
                requested_metadata_fetches += 1;
            } else {
                requested_downloads += 1;
            }
        }

        let mut granted_downloads = 0usize;
        let mut granted_metadata_fetches = 0usize;
        for hash in desired {
            if reg.get(hash).is_some_and(|torrent| torrent.needs_metadata) {
                granted_metadata_fetches += 1;
            } else {
                granted_downloads += 1;
            }
        }

        let mut running_downloads = 0usize;
        let mut running_metadata_fetches = 0usize;
        for hash in &running {
            let Some(torrent) = reg.get(hash) else {
                continue;
            };
            if !matches!(
                torrent.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            ) {
                continue;
            }
            if torrent.needs_metadata {
                running_metadata_fetches += 1;
            } else {
                running_downloads += 1;
            }
        }

        let active_peer_workers = reg
            .torrents
            .values()
            .map(|torrent| torrent.active_peer_workers)
            .sum();
        let running_engines = running.len();
        let effective_peer_worker_limit =
            Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent);
        let peer_worker_budget = effective_peer_worker_limit.saturating_mul(running_engines);
        let peer_permits = self.peer_permit_snapshot().await;

        SchedulerDiagnostics {
            managed_torrents: reg.torrents.len(),
            queued_torrents: reg
                .torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Queued)
                .count(),
            running_engines,
            running_downloads,
            running_metadata_fetches,
            requested_downloads,
            requested_metadata_fetches,
            granted_downloads,
            granted_metadata_fetches,
            retry_backoff_torrents: retry_after
                .values()
                .filter(|retry_at| **retry_at > now)
                .count(),
            active_download_limit: cfg.queue.max_active_downloads,
            active_metadata_fetch_limit: cfg.queue.max_active_metadata_fetches,
            active_seed_limit: cfg.queue.max_active_seeds,
            peer_worker_global_limit: cfg.bandwidth.max_peers,
            peer_worker_per_torrent_limit: cfg.bandwidth.max_peers_per_torrent,
            effective_peer_worker_limit,
            peer_worker_budget,
            active_peer_workers,
            peer_limit: peer_permits.limit,
            peer_permits_in_use: peer_permits.in_use,
            peer_permits_available: peer_permits.available,
            peer_sessions_denied: peer_permits.denied,
            download_slots_saturated: cfg.queue.max_active_downloads > 0
                && requested_downloads > granted_downloads
                && granted_downloads >= cfg.queue.max_active_downloads,
            metadata_fetch_slots_saturated: cfg.queue.max_active_metadata_fetches > 0
                && requested_metadata_fetches > granted_metadata_fetches
                && granted_metadata_fetches >= cfg.queue.max_active_metadata_fetches,
            peer_worker_budget_saturated: peer_worker_budget > 0
                && active_peer_workers >= peer_worker_budget,
        }
    }

    async fn active_download_hashes(&self) -> Vec<InfoHash> {
        let running: Vec<InfoHash> = self.engine_handles.read().await.keys().copied().collect();
        let reg = self.registry.lock().await;
        running
            .into_iter()
            .filter(|hash| {
                reg.get(hash).is_some_and(|t| {
                    matches!(
                        t.state,
                        TorrentState::Downloading | TorrentState::DownloadingMetadata
                    )
                })
            })
            .collect()
    }

    async fn desired_download_hashes(&self) -> Vec<InfoHash> {
        self.desired_download_hashes_excluding(None).await
    }

    async fn desired_download_hashes_excluding(&self, excluded: Option<InfoHash>) -> Vec<InfoHash> {
        let cfg = self.config.read().await.clone();
        let retry_after = self.engine_retry_after.read().await.clone();
        let mut queue = self.queue.lock().await;
        queue.limits = cfg.queue.clone();
        let reg = self.registry.lock().await;
        let now = Instant::now();
        let stale_queue_entries = queue
            .order
            .iter()
            .chain(queue.bypass.iter())
            .filter(|hash| !reg.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        queue.remove_many(stale_queue_entries);

        let download_limit = queue.limits.max_active_downloads;
        let metadata_limit = queue.limits.max_active_metadata_fetches;
        let mut active = Vec::new();
        let mut active_set = HashSet::new();
        let mut active_downloads = 0usize;
        let mut active_metadata_fetches = 0usize;
        let bypass_set = queue.bypass.iter().copied().collect::<HashSet<_>>();
        for hash in queue.bypass.iter().chain(queue.order.iter()) {
            if excluded.is_some_and(|excluded| &excluded == hash) {
                continue;
            }
            let download_slots_full = download_limit > 0 && active_downloads >= download_limit;
            let metadata_slots_full =
                metadata_limit > 0 && active_metadata_fetches >= metadata_limit;
            if download_slots_full && metadata_slots_full {
                break;
            }
            if !active_set.insert(*hash) {
                continue;
            }
            if retry_after
                .get(hash)
                .is_some_and(|retry_at| *retry_at > now)
            {
                continue;
            }
            let Some(t) = reg.get(hash) else {
                continue;
            };
            let bypass = bypass_set.contains(hash);
            let already_active = matches!(
                t.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            );
            let auto_startable = queue.limits.auto_start || bypass || already_active;
            let metadata_fetch = t.needs_metadata;
            if auto_startable
                && matches!(
                    t.state,
                    TorrentState::Queued
                        | TorrentState::Downloading
                        | TorrentState::DownloadingMetadata
                )
            {
                if metadata_fetch {
                    if metadata_slots_full {
                        continue;
                    }
                    active_metadata_fetches += 1;
                } else {
                    if download_slots_full {
                        continue;
                    }
                    active_downloads += 1;
                }
                active.push(*hash);
            }
        }
        active
    }

    async fn reconcile_queue(&self) {
        let inactive_recovered = self.sweep_inactive_engine_handles("queue_reconcile").await;
        let stale_recovered = self.sweep_stale_active_torrents("queue_reconcile").await;
        let desired = self.desired_download_hashes().await;
        let current = self.active_download_hashes().await;
        tracing::debug!(
            inactive_recovered,
            stale_recovered,
            desired_downloads = desired.len(),
            current_downloads = current.len(),
            "queue reconciliation planned"
        );

        for hash in current {
            if !desired.contains(&hash) {
                self.force_stop_engine(&hash).await;
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(&hash) {
                    if !matches!(t.state, TorrentState::Paused | TorrentState::Completed) {
                        t.state = TorrentState::Queued;
                    }
                }
            }
        }

        for hash in desired {
            self.start_engine(hash).await;
        }
        self.apply_peer_worker_limits().await;
    }

    async fn sweep_stale_active_torrents(&self, reason: &'static str) -> usize {
        let running: HashSet<InfoHash> = self.engine_handles.read().await.keys().copied().collect();
        let retry_after = self.engine_retry_after.read().await.clone();
        let now = Instant::now();
        let recovered = {
            let mut reg = self.registry.lock().await;
            let mut recovered = Vec::new();
            for (hash, torrent) in reg.torrents.iter_mut() {
                if matches!(
                    torrent.state,
                    TorrentState::Downloading | TorrentState::DownloadingMetadata
                ) && !running.contains(hash)
                {
                    torrent.state = TorrentState::Queued;
                    torrent.error = Some(STALE_ACTIVE_RECOVERY_MESSAGE.into());
                    recovered.push(*hash);
                }
            }
            recovered
        };

        if recovered.is_empty() {
            return 0;
        }

        {
            let mut queue = self.queue.lock().await;
            queue.add_many(recovered.iter().copied());
            queue.clear_bypass_many(recovered.iter().copied());
            queue.move_many_to_bottom(recovered.iter().copied());
        }

        for hash in &recovered {
            tracing::warn!(
                info_hash = %hash,
                reason,
                retry_suppressed = retry_after
                    .get(hash)
                    .is_some_and(|retry_at| *retry_at > now),
                "stale active torrent queued for lifecycle recovery"
            );
        }
        recovered.len()
    }

    async fn sweep_inactive_engine_handles(&self, reason: &'static str) -> usize {
        let running: Vec<InfoHash> = self.engine_handles.read().await.keys().copied().collect();
        let stale: Vec<(InfoHash, Option<TorrentState>)> = {
            let reg = self.registry.lock().await;
            running
                .into_iter()
                .filter_map(|hash| match reg.get(&hash) {
                    Some(t)
                        if matches!(
                            t.state,
                            TorrentState::Downloading | TorrentState::DownloadingMetadata
                        ) =>
                    {
                        None
                    }
                    Some(t) => Some((hash, Some(t.state))),
                    None => Some((hash, None)),
                })
                .collect()
        };

        for (hash, state) in &stale {
            tracing::warn!(
                info_hash = %hash,
                reason,
                state = ?state,
                "stale inactive engine bookkeeping cleared"
            );
            self.force_stop_engine(hash).await;
            if matches!(state, Some(TorrentState::Queued)) {
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.error = Some(STALE_INACTIVE_ENGINE_RECOVERY_MESSAGE.into());
                }
            }
        }

        stale.len()
    }

    async fn schedule_reconcile_queue(&self, reason: &'static str) {
        let mut state = self.queue_reconcile.lock().await;
        if state.scheduled {
            state.dirty = true;
            tracing::debug!(
                reason,
                "queue reconciliation already scheduled; marked dirty"
            );
            return;
        }

        state.scheduled = true;
        state.dirty = false;
        drop(state);

        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.run_scheduled_reconcile_queue(reason).await;
        });
    }

    fn schedule_delayed_reconcile_queue(&self, reason: &'static str, delay: Duration) {
        let runtime = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            runtime.schedule_reconcile_queue(reason).await;
        });
    }

    async fn run_scheduled_reconcile_queue(self, reason: &'static str) {
        tokio::time::sleep(QUEUE_RECONCILE_DEBOUNCE).await;
        loop {
            {
                let mut state = self.queue_reconcile.lock().await;
                state.dirty = false;
            }
            tracing::debug!(reason, "queue reconciliation started");
            self.reconcile_queue().await;

            let mut state = self.queue_reconcile.lock().await;
            if state.dirty {
                state.dirty = false;
                tracing::debug!(reason, "queue reconciliation dirty; running again");
                drop(state);
                continue;
            }

            state.scheduled = false;
            tracing::debug!(reason, "queue reconciliation complete");
            break;
        }
    }

    async fn engine_task_finished(&self, hash: InfoHash) {
        self.engine_cmds.lock().await.remove(&hash);
        self.engine_handles.write().await.remove(&hash);
    }

    async fn record_engine_containment_cancellation(&self, hash: InfoHash, needs_metadata: bool) {
        let mut reg = self.registry.lock().await;
        let Some(torrent) = reg.get_mut(&hash) else {
            return;
        };
        if matches!(
            torrent.state,
            TorrentState::Downloading | TorrentState::DownloadingMetadata | TorrentState::Queued
        ) {
            torrent.containment_recovery_intent = Some(if needs_metadata {
                ContainmentRecoveryIntent::DownloadingMetadata
            } else {
                ContainmentRecoveryIntent::Downloading
            });
        }
    }

    async fn queue_torrent_for_retry(
        &self,
        hash: InfoHash,
        message: &'static str,
        delay: Duration,
    ) -> bool {
        let queued = {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(&hash) else {
                return false;
            };
            if !matches!(
                t.state,
                TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
                    | TorrentState::Queued
            ) {
                return false;
            }
            t.state = TorrentState::Queued;
            t.error = Some(message.into());
            true
        };
        if !queued {
            return false;
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(hash);
            queue.clear_bypass(&hash);
            queue.move_to_bottom(&hash);
        }
        self.engine_retry_after
            .write()
            .await
            .insert(hash, Instant::now() + delay);
        tracing::warn!(
            info_hash = %hash,
            reason = message,
            retry_delay_seconds = delay.as_secs(),
            "torrent queued for retry"
        );
        true
    }

    async fn handle_engine_task_error(
        &self,
        hash: InfoHash,
        needs_metadata: bool,
        error: CoreError,
    ) -> bool {
        let retry_metadata = needs_metadata && is_retryable_magnet_metadata_discovery_error(&error);
        if retry_metadata {
            tracing::debug!(
                info_hash = %hash,
                error = %error,
                "magnet metadata discovery found no peers; retry scheduled"
            );
            let _ = self
                .queue_torrent_for_retry(
                    hash,
                    MAGNET_METADATA_NO_PEERS_RETRY_MESSAGE,
                    MAGNET_METADATA_NO_PEERS_RETRY_DELAY,
                )
                .await;
            self.schedule_delayed_reconcile_queue("magnet_metadata_no_peers", Duration::ZERO);
            return true;
        }

        let state = if error.is_network_blocked() {
            TorrentState::NetworkBlocked
        } else if matches!(&error, CoreError::Storage(_)) {
            TorrentState::StorageError
        } else {
            TorrentState::Error
        };
        tracing::warn!(info_hash = %hash, error = %error, "engine task failed");
        let mut changed = false;
        {
            let mut reg = self.registry.lock().await;
            if let Some(t) = reg.get_mut(&hash) {
                t.state = state;
                t.error = Some(error.to_string());
                changed = true;
            }
        }
        if changed {
            self.publish_torrent_event("torrent_error", hash, state);
            self.publish_event(stats_updated_event());
        }
        false
    }

    async fn shared_dht_runner(
        &self,
        binder: Arc<dyn swarmotter_core::net::NetworkBinder>,
        peer_id: [u8; 20],
    ) -> Option<Arc<crate::dht::DhtRunner>> {
        let (dht_enabled, bootstrap_nodes, dht_port) = {
            let cfg = self.config.read().await;
            (
                cfg.dht.enabled,
                cfg.dht.bootstrap_nodes.clone(),
                cfg.dht.port,
            )
        };
        if !dht_enabled || !self.network_health.read().await.traffic_allowed {
            return None;
        }
        if let Some(existing) = self.dht_runner.lock().await.clone() {
            return Some(existing);
        }
        let bootstrap =
            crate::dht::resolve_bootstrap_with_binder(binder.as_ref(), &bootstrap_nodes).await;
        let self_id = crate::dht::DhtRunner::derive_from_peer_id(&peer_id);
        let runner = Arc::new(crate::dht::DhtRunner::new(
            self_id, binder, bootstrap, dht_port,
        ));
        *self.dht_runner.lock().await = Some(runner.clone());
        Some(runner)
    }

    /// Start the live engine task for a torrent (downloading). No-op if the
    /// torrent is paused, queued, or already running.
    pub async fn start_engine(&self, hash: InfoHash) {
        let _data_plane_transition = self.data_plane_transition_lock.lock().await;
        self.start_engine_while_transition_locked(hash).await;
    }

    /// Start one engine while the caller owns `data_plane_transition_lock`.
    /// This is used only by serialized reconstruction transactions so normal
    /// API/queue starts cannot interleave with a partially rebuilt live set.
    async fn start_engine_while_transition_locked(&self, hash: InfoHash) {
        let health = self.network_health.read().await.clone();
        if !health.traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
            // Network blocked: do not start the engine; mark torrent.
            let mut changed = false;
            {
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(&hash) {
                    t.state = TorrentState::NetworkBlocked;
                    t.error = Some(health.detail.clone());
                    changed = true;
                }
            }
            if changed {
                self.publish_torrent_event("torrent_changed", hash, TorrentState::NetworkBlocked);
                self.publish_event(stats_updated_event());
            }
            return;
        }

        // Already running?
        if self.engine_handles.read().await.contains_key(&hash) {
            return;
        }
        self.engine_retry_after.write().await.remove(&hash);

        let snapshot = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(&hash) else {
                return;
            };
            EngineStartSnapshot::from_torrent(t)
        };

        let (
            meta,
            active_dir,
            complete_dir,
            listen_port,
            preallocate,
            sparse,
            max_peer_workers,
            allow_ipv6,
            pex_enabled,
            pex_max_peers,
            minimum_free_space_bytes,
            minimum_free_space_percent,
            magnet,
            needs_metadata,
        ) = {
            let complete_dir = self
                .resolve_download_dir_override(snapshot.download_dir.as_deref())
                .await;
            let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
            let magnet = snapshot.magnet_params();
            let cfg = self.config.read().await;
            let preallocate = cfg.storage.preallocate;
            let sparse = cfg.storage.sparse;
            let allow_ipv6 = cfg.torrent.allow_ipv6 && cfg.network.allow_ipv6;
            let pex_enabled = cfg.pex.enabled;
            let pex_max_peers = cfg.pex.max_peers;
            let minimum_free_space_bytes = cfg.storage.minimum_free_space_bytes;
            let minimum_free_space_percent = cfg.storage.minimum_free_space_percent;
            let max_peer_workers =
                Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent);
            (
                snapshot.meta.clone(),
                active_dir,
                complete_dir,
                cfg.torrent.listen_port,
                preallocate,
                sparse,
                max_peer_workers,
                allow_ipv6,
                pex_enabled,
                pex_max_peers,
                minimum_free_space_bytes,
                minimum_free_space_percent,
                magnet,
                snapshot.needs_metadata,
            )
        };

        if !self.registry.lock().await.contains(&hash) {
            return;
        }

        let preflight_content_bytes = if needs_metadata { 0 } else { meta.total_length };
        if preflight_content_bytes > 0
            || minimum_free_space_bytes > 0
            || minimum_free_space_percent > 0
        {
            let mut cfg = self.config.read().await.storage.clone();
            cfg.minimum_free_space_bytes = minimum_free_space_bytes;
            cfg.minimum_free_space_percent = minimum_free_space_percent;
            for dir in unique_pathbufs([PathBuf::from(&active_dir), PathBuf::from(&complete_dir)]) {
                if let Err(e) = swarmotter_core::storage::check_storage_preflight(
                    &dir,
                    &cfg,
                    preflight_content_bytes,
                ) {
                    tracing::warn!(
                        info_hash = %hash,
                        error = %e,
                        error_code = %e.code(),
                        "engine start blocked by storage preflight"
                    );
                    let mut reg = self.registry.lock().await;
                    if let Some(t) = reg.get_mut(&hash) {
                        t.state = TorrentState::StorageError;
                        t.error = Some(e.to_string());
                    }
                    self.publish_torrent_event("torrent_error", hash, TorrentState::StorageError);
                    self.publish_event(stats_updated_event());
                    return;
                }
            }
        }

        let state = Arc::new(Mutex::new(EngineState::default()));
        self.engine_states.write().await.insert(hash, state.clone());

        let binder: Arc<dyn swarmotter_core::net::NetworkBinder> = self.make_binder().await;
        let peer_id = make_peer_id();
        let (tx, rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
        self.engine_cmds.lock().await.insert(hash, tx);

        // A torrent owns one limiter for its entire retained lifetime. Engine
        // restarts and the downloader-to-seeder transition reuse these exact
        // buckets, while the process-wide limiter remains a separate layer.
        let limiter = {
            let mut limiters = self.torrent_limiters.write().await;
            limiters
                .entry(hash)
                .or_insert_with(|| {
                    Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                        snapshot.download_limit,
                        snapshot.upload_limit,
                    ))
                })
                .clone()
        };
        let peer_session_budget = self.peer_session_budget(hash).await;
        // Peer transport selection (TCP/uTP) from config. All transports stay
        // on the contained binder; fail-closed blocks both.
        let (utp_enabled, utp_prefer_tcp, encryption_mode) = {
            let cfg = self.config.read().await;
            (
                cfg.torrent.utp_enabled,
                cfg.torrent.utp_prefer_tcp,
                cfg.torrent.encryption_mode,
            )
        };

        let state_for_summary = state.clone();
        let hash_for_task = hash;
        let registry = self.registry.clone();
        let selfish_completion_enabled = self.selfish_completion_enabled.clone();
        let runtime_for_task = self.clone();
        // DHT runner for trackerless peer discovery. Gated by config and
        // containment; the engine disables DHT for private torrents.
        let dht_runner = self.shared_dht_runner(binder.clone(), peer_id).await;
        let mut engine = TorrentEngine::with_limiter(
            meta.clone(),
            active_dir.clone().into(),
            peer_id,
            binder,
            state.clone(),
            rx,
            vec![],
            listen_port,
            limiter,
            magnet,
        )
        .with_complete_dir(complete_dir.clone().into())
        .with_global_limiter(Some(self.global_limiter.clone()))
        .with_transport(utp_enabled, utp_prefer_tcp)
        .with_encryption_mode(encryption_mode)
        .with_preallocate(preallocate)
        .with_sparse(sparse)
        .with_storage_reserve(minimum_free_space_bytes, minimum_free_space_percent);
        engine = match engine
            .with_file_selection(snapshot.priorities.clone(), snapshot.wanted.clone())
        {
            Ok(engine) => engine,
            Err(error) => {
                tracing::error!(info_hash = %hash, error = %error, "torrent file layout rejected");
                if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                    torrent.state = TorrentState::StorageError;
                    torrent.error = Some(error.to_string());
                }
                self.publish_torrent_event("torrent_changed", hash, TorrentState::StorageError);
                self.publish_event(stats_updated_event());
                return;
            }
        };
        engine = engine
            .with_peer_worker_limit(max_peer_workers)
            .with_peer_session_budget(peer_session_budget)
            .with_allow_ipv6(allow_ipv6)
            .with_pex(pex_enabled, pex_max_peers);
        if needs_metadata {
            let runtime = self.clone();
            let metadata_download_dir = snapshot.download_dir.clone();
            engine = engine.with_metadata_preflight(Arc::new(move |resolved| {
                let runtime = runtime.clone();
                let metadata_download_dir = metadata_download_dir.clone();
                Box::pin(async move {
                    runtime
                        .reserve_resolved_magnet_metadata(hash, resolved, metadata_download_dir)
                        .await
                })
            }));
        }
        if let Some(dht) = dht_runner {
            engine = engine.with_dht(dht);
        }
        // Do not let the engine run until its handle and related bookkeeping
        // are visible. Otherwise a fast failure can remove an empty slot and
        // leave its completed JoinHandle inserted as stale state.
        let (task_start_tx, task_start_rx) = tokio::sync::oneshot::channel();
        let containment_gate = self.containment_gate.clone();
        let containment_generation = containment_gate.generation();
        let handle = tokio::spawn(async move {
            if task_start_rx.await.is_err() {
                return;
            }
            let engine_result = tokio::select! {
                biased;
                _ = containment_gate.cancelled_since(containment_generation) => {
                    runtime_for_task
                        .record_engine_containment_cancellation(hash_for_task, needs_metadata)
                        .await;
                    runtime_for_task.engine_task_finished(hash_for_task).await;
                    return;
                }
                result = engine.run() => result,
            };
            match engine_result {
                Ok(final_state) => {
                    let finished = final_state.finished;
                    let stopped_by_command = final_state.stopped_by_command;
                    let mut metadata_received = false;
                    let mut changed_state = None;
                    {
                        let mut reg = registry.lock().await;
                        if let Some(t) = reg.get_mut(&hash_for_task) {
                            let previous_state = t.state;
                            let needed_metadata = t.needs_metadata;
                            // If metadata was fetched via BEP 9, replace the
                            // placeholder meta with the real one and rebuild the
                            // file/piece bookkeeping.
                            if let Some(real) = final_state.resolved_meta.as_ref() {
                                apply_resolved_metadata(t, real, &final_state);
                                metadata_received = needed_metadata && !t.needs_metadata;
                            }
                            t.downloaded = final_state.downloaded;
                            t.uploaded = final_state.uploaded;
                            t.progress.replace_from_bitfield(
                                &final_state.pieces_have,
                                final_state.piece_count,
                            );
                            t.recompute_file_bytes_completed();
                            if final_state.finished {
                                t.state = TorrentState::Completed;
                                t.seeding_status = if t.progress.is_complete() {
                                    SeedingStatus::Queued
                                } else {
                                    SeedingStatus::NotEligible
                                };
                                t.date_completed = Some(now());
                            } else if t.state == TorrentState::DownloadingMetadata {
                                // Metadata fetched but download incomplete; mark
                                // downloading.
                                t.state = TorrentState::Downloading;
                            }
                            if t.state != previous_state {
                                changed_state = Some(t.state);
                            }
                        }
                    }
                    if metadata_received {
                        runtime_for_task.publish_event(torrent_metadata_event(hash_for_task));
                    }
                    if let Some(state) = changed_state {
                        runtime_for_task.publish_torrent_event(
                            "torrent_changed",
                            hash_for_task,
                            state,
                        );
                        if state == TorrentState::Completed {
                            runtime_for_task.publish_torrent_event(
                                "torrent_completed",
                                hash_for_task,
                                state,
                            );
                        }
                    }
                    runtime_for_task.publish_event(stats_updated_event());
                    // Selfish completion policy: when enabled, immediately
                    // remove the finished torrent from the daemon (engine and
                    // seeder stopped, record removed) while preserving the
                    // downloaded data. This must run after the registry update
                    // above so final stats/name are captured before removal.
                    if finished && selfish_completion_enabled.load(Ordering::Acquire) {
                        runtime_for_task
                            .selfish_remove_completed(hash_for_task)
                            .await;
                    } else if !finished && !stopped_by_command {
                        let queued = runtime_for_task
                            .queue_torrent_for_retry(
                                hash_for_task,
                                "engine stopped before completion; queued for retry",
                                ENGINE_INCOMPLETE_RETRY_DELAY,
                            )
                            .await;
                        if queued {
                            runtime_for_task.schedule_delayed_reconcile_queue(
                                "engine_incomplete_retry",
                                Duration::ZERO,
                            );
                            runtime_for_task.schedule_delayed_reconcile_queue(
                                "engine_incomplete_retry",
                                ENGINE_INCOMPLETE_RETRY_DELAY,
                            );
                        }
                    }
                }
                Err(e) => {
                    let retry_metadata = runtime_for_task
                        .handle_engine_task_error(hash_for_task, needs_metadata, e)
                        .await;
                    if retry_metadata {
                        runtime_for_task.schedule_delayed_reconcile_queue(
                            "magnet_metadata_no_peers",
                            MAGNET_METADATA_NO_PEERS_RETRY_DELAY,
                        );
                    }
                }
            }
            runtime_for_task.engine_task_finished(hash_for_task).await;
            runtime_for_task.reconcile_seeders().await;
            runtime_for_task
                .schedule_delayed_reconcile_queue("engine_task_finished", Duration::ZERO);
            let _ = state_for_summary;
        });
        self.engine_handles.write().await.insert(hash, handle);

        if !self.registry.lock().await.contains(&hash) {
            self.force_stop_engine(&hash).await;
            return;
        }

        let should_run = self
            .registry
            .lock()
            .await
            .get(&hash)
            .is_some_and(|torrent| {
                matches!(
                    torrent.state,
                    TorrentState::Queued
                        | TorrentState::Downloading
                        | TorrentState::DownloadingMetadata
                )
            });
        if !should_run {
            self.force_stop_engine(&hash).await;
            return;
        }

        // Mark the torrent as downloading.
        let mut changed_state = None;
        {
            let mut reg = self.registry.lock().await;
            if let Some(t) = reg.get_mut(&hash) {
                if t.state == TorrentState::Queued || t.state == TorrentState::NetworkBlocked {
                    t.containment_recovery_intent = None;
                    t.state = if needs_metadata {
                        TorrentState::DownloadingMetadata
                    } else {
                        TorrentState::Downloading
                    };
                    t.error = None;
                    changed_state = Some(t.state);
                }
            }
        }
        if let Some(state) = changed_state {
            self.publish_torrent_event("torrent_changed", hash, state);
            self.publish_event(stats_updated_event());
        }
        let _ = task_start_tx.send(());
    }

    async fn stop_engine(&self, hash: &InfoHash) {
        self.engine_retry_after.write().await.remove(hash);
        if let Some(tx) = self.engine_cmds.lock().await.remove(hash) {
            let _ = tx.send(EngineCommand::Stop).await;
        }
        let handle = self.engine_handles.write().await.remove(hash);
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        // Stop the inbound peer listener / seeder too.
        self.stop_seeder(hash).await;
        self.engine_states.write().await.remove(hash);
        self.rate_samples.write().await.remove(hash);
    }

    async fn force_stop_engine(&self, hash: &InfoHash) {
        self.engine_retry_after.write().await.remove(hash);
        if let Some(tx) = self.engine_cmds.lock().await.remove(hash) {
            let _ = tx.try_send(EngineCommand::Stop);
        }
        let handle = self.engine_handles.write().await.remove(hash);
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
        self.force_stop_seeder(hash).await;
        self.engine_states.write().await.remove(hash);
        self.rate_samples.write().await.remove(hash);
    }

    async fn restart_engine_for_settings(&self, hash: &InfoHash) {
        self.stop_engine(hash).await;
        {
            let mut registry = self.registry.lock().await;
            if let Some(torrent) = registry.get_mut(hash) {
                torrent.state = TorrentState::Queued;
                torrent.error = None;
            } else {
                return;
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(*hash);
            queue.start_now(hash);
        }
        self.reconcile_queue().await;
    }

    async fn stop_all_torrent_tasks(&self, registry_hashes: &[InfoHash]) {
        let mut hashes = registry_hashes.to_vec();
        hashes.extend(self.engine_handles.read().await.keys().copied());
        hashes.extend(self.seeder_shutdowns.lock().await.keys().copied());
        hashes.sort();
        hashes.dedup();
        for hash in hashes {
            self.force_stop_engine(&hash).await;
        }
    }

    async fn clear_download_runtime_state(&self) {
        {
            let mut reg = self.registry.lock().await;
            reg.torrents.clear();
        }
        {
            let mut queue = self.queue.lock().await;
            queue.clear();
        }
        self.engine_states.write().await.clear();
        self.engine_cmds.lock().await.clear();
        self.engine_handles.write().await.clear();
        self.torrent_limiters.write().await.clear();
        self.torrent_peer_permit_pools.write().await.clear();
        self.seeder_shutdowns.lock().await.clear();
        self.seeder_registry.clear().await;
        self.stop_seeder_listener(false).await;
        self.seeder_handles.lock().await.clear();
        self.rate_samples.write().await.clear();
        self.engine_retry_after.write().await.clear();
        self.autopilot_decisions.write().await.clear();
        self.autopilot_last_action.write().await.clear();
    }

    /// Spawn the inbound peer listener / seeder for a torrent. It shares the
    /// live engine state and serves verified pieces through the contained
    /// listener. No-op if already running or if the torrent is private and
    /// inbound listening is not desired (private torrents still allow inbound
    /// peers; the private flag restricts DHT/PEX, not inbound TCP).
    async fn start_seeder(
        &self,
        hash: InfoHash,
        meta: swarmotter_core::meta::TorrentMeta,
        active_dir: String,
        complete_dir: String,
        state: Arc<Mutex<EngineState>>,
    ) -> Result<()> {
        let _data_plane_transition = self.data_plane_transition_lock.lock().await;
        self.start_seeder_while_transition_locked(hash, meta, active_dir, complete_dir, state)
            .await
    }

    async fn start_seeder_while_transition_locked(
        &self,
        hash: InfoHash,
        meta: swarmotter_core::meta::TorrentMeta,
        active_dir: String,
        complete_dir: String,
        state: Arc<Mutex<EngineState>>,
    ) -> Result<()> {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        {
            let mut shutdowns = self.seeder_shutdowns.lock().await;
            if shutdowns.contains_key(&hash) {
                if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                    torrent.state = TorrentState::Seeding;
                    torrent.seeding_status = SeedingStatus::Active;
                }
                return Ok(());
            }
            shutdowns.insert(hash, shutdown_tx.clone());
        }
        let peer_id = make_peer_id();
        let listen_port = self.config.read().await.torrent.listen_port;
        // Reuse the torrent's retained limiter; never replace it when the
        // downloader completes or a queued seed slot becomes available.
        let (dl_limit, ul_limit) = {
            let reg = self.registry.lock().await;
            reg.get(&hash)
                .map(|t| (t.download_limit, t.upload_limit))
                .unwrap_or((0, 0))
        };
        let limiter = {
            let mut limiters = self.torrent_limiters.write().await;
            limiters
                .entry(hash)
                .or_insert_with(|| {
                    Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                        dl_limit, ul_limit,
                    ))
                })
                .clone()
        };
        let storage = Arc::new(swarmotter_core::storage::StorageIo::new(
            meta.clone(),
            std::path::PathBuf::from(&active_dir),
        ));
        let complete_storage = if active_dir == complete_dir {
            None
        } else {
            Some(Arc::new(swarmotter_core::storage::StorageIo::new(
                meta.clone(),
                std::path::PathBuf::from(&complete_dir),
            )))
        };
        let registration = SeedRegistration::new(
            meta.clone(),
            storage,
            complete_storage,
            state,
            peer_id,
            limiter,
            Some(self.global_limiter.clone()),
            self.peer_session_budget(hash).await,
            shutdown_rx,
        );
        self.seeder_registry.register(registration).await;
        if let Err(error) = self.ensure_seeder_listener().await {
            if let Some(shutdown) = self.seeder_shutdowns.lock().await.remove(&hash) {
                let _ = shutdown.send(true);
            }
            self.seeder_registry.unregister(&hash).await;
            return Err(error);
        }
        let announce_handle = self
            .spawn_seeder_announce(
                hash,
                meta.clone(),
                peer_id,
                listen_port,
                shutdown_tx.subscribe(),
            )
            .await;
        if let Some(handle) = announce_handle {
            self.seeder_handles.lock().await.insert(hash, handle);
        }
        if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
            torrent.state = TorrentState::Seeding;
            torrent.seeding_status = SeedingStatus::Active;
            torrent.error = None;
        }
        self.persist_state_best_effort("seeder_started").await;
        Ok(())
    }

    async fn ensure_seeder_listener(&self) -> Result<()> {
        let mut handle_slot = self.seeder_listener_handle.lock().await;
        if handle_slot
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Ok(());
        }
        if let Some(finished) = handle_slot.take() {
            let _ = finished.await;
        }
        let cfg = self.config.read().await.clone();
        let binder = self.make_binder().await;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let hub = SeederHub::new(
            self.seeder_registry.clone(),
            binder,
            cfg.torrent.listen_port,
            cfg.torrent.encryption_mode,
            shutdown_rx,
            self.peer_permit_pool.read().await.clone(),
        )
        .with_bound_addr(bound_tx);
        *self.seeder_listener_shutdown.lock().await = Some(shutdown_tx);
        let containment_gate = self.containment_gate.clone();
        let containment_generation = containment_gate.generation();
        *handle_slot = Some(tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = containment_gate.cancelled_since(containment_generation) => {}
                result = hub.run() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "shared seeding listener ended");
                    }
                }
            }
        }));
        match tokio::time::timeout(Duration::from_secs(5), bound_rx).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => {
                drop(handle_slot);
                self.stop_seeder_listener(true).await;
                Err(CoreError::NetworkBlocked(
                    "shared inbound peer listener failed to bind".into(),
                ))
            }
            Err(_) => {
                drop(handle_slot);
                self.stop_seeder_listener(true).await;
                Err(CoreError::NetworkBlocked(
                    "shared inbound peer listener bind timed out".into(),
                ))
            }
        }
    }

    async fn stop_seeder_listener(&self, force: bool) {
        if let Some(shutdown) = self.seeder_listener_shutdown.lock().await.take() {
            let _ = shutdown.send(true);
        }
        if let Some(handle) = self.seeder_listener_handle.lock().await.take() {
            if force {
                handle.abort();
            }
            let _ = handle.await;
        }
    }

    /// Stop the shared DHT runner if one is active. Used by containment
    /// transitions and data-plane reconstruction. See ADR-0051.
    async fn stop_dht_runner(&self) {
        // The runner is a shared resource with no long-running task of its own;
        // dropping the stored Arc stops it.
        *self.dht_runner.lock().await = None;
    }

    /// Snapshot only work that is demonstrably live at the containment edge.
    /// Queued, paused, completed-without-a-live-seeder, automatically stopped,
    /// and pre-existing blocked torrents receive no recovery intent.
    async fn live_containment_recovery_intents(
        &self,
    ) -> HashMap<InfoHash, ContainmentRecoveryIntent> {
        let running_engines = self
            .engine_handles
            .read()
            .await
            .iter()
            .filter_map(|(hash, handle)| (!handle.is_finished()).then_some(*hash))
            .collect::<HashSet<_>>();
        let cfg = self.config.read().await.clone();
        let samples = self.rate_samples.read().await.clone();
        let now_secs = now();
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let running_seeders = self
            .seeder_registry
            .info_hashes()
            .await
            .into_iter()
            .collect::<HashSet<_>>();
        let reg = self.registry.lock().await;
        let mut intents = HashMap::new();
        for (hash, torrent) in &reg.torrents {
            if let Some(intent) = torrent.containment_recovery_intent {
                intents.insert(*hash, intent);
                continue;
            }
            let engine_was_live = running_engines.contains(hash);
            if engine_was_live {
                intents.insert(
                    *hash,
                    if torrent.needs_metadata || torrent.state == TorrentState::DownloadingMetadata
                    {
                        ContainmentRecoveryIntent::DownloadingMetadata
                    } else {
                        ContainmentRecoveryIntent::Downloading
                    },
                );
                continue;
            }

            if !running_seeders.contains(hash)
                || !matches!(
                    torrent.state,
                    TorrentState::Completed | TorrentState::Seeding
                )
            {
                continue;
            }
            let idle_seconds = samples
                .get(hash)
                .and_then(|sample| sample.last_upload_at)
                .map(|at| Instant::now().saturating_duration_since(at).as_secs())
                .unwrap_or_else(|| {
                    now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added))
                });
            let accounting = TorrentAccounting {
                downloaded: torrent.downloaded,
                uploaded: torrent.uploaded,
                idle_seconds,
            };
            if ratio::evaluate_seeding(&accounting, &cfg.seeding, &torrent.seeding)
                == SeedDecision::Continue
            {
                intents.insert(*hash, ContainmentRecoveryIntent::Seeding);
            }
        }
        intents
    }

    /// Abort every task that can own a torrent data-plane socket. All handles
    /// are aborted before any are awaited, so teardown never waits for graceful
    /// peer/tracker/TLS protocol completion. Engine state is retained until the
    /// caller reconciles already-reported progress.
    async fn abort_data_plane_tasks_for_containment(
        &self,
        recovery_intents: &HashMap<InfoHash, ContainmentRecoveryIntent>,
        preserved_seeding_statuses: &HashMap<InfoHash, SeedingStatus>,
        detail: &str,
    ) -> Vec<InfoHash> {
        self.engine_cmds.lock().await.clear();
        let engine_handles = {
            let mut handles = self.engine_handles.write().await;
            std::mem::take(&mut *handles)
                .into_values()
                .collect::<Vec<_>>()
        };
        let announce_handles = {
            let mut handles = self.seeder_handles.lock().await;
            std::mem::take(&mut *handles)
                .into_values()
                .collect::<Vec<_>>()
        };

        let changed = {
            // Readers, live registration ownership, listener teardown, final
            // progress reconciliation, and the modeled blocked state share one
            // lifecycle critical section. No API snapshot can observe a live
            // `seeding` state after its accepting task has stopped, or an
            // `active` status without an authoritative registration.
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live_seeders = self
                .seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>();
            self.stop_seeder_listener(true).await;
            for handle in &engine_handles {
                handle.abort();
            }
            for handle in &announce_handles {
                handle.abort();
            }
            let shutdowns = {
                let mut shutdowns = self.seeder_shutdowns.lock().await;
                std::mem::take(&mut *shutdowns)
                    .into_values()
                    .collect::<Vec<_>>()
            };
            for shutdown in shutdowns {
                let _ = shutdown.send(true);
            }
            self.seeder_registry.clear().await;

            // Keep the pre-teardown registration snapshot while copying final
            // task-owned counters so progress reconciliation does not publish a
            // fictitious completed/queued transition between active and
            // network-blocked. Seeder reconciliation is deliberately skipped
            // while this lifecycle lock is held.
            self.reconcile_engine_progress_with_seeders(live_seeders, false)
                .await;

            let mut changed = Vec::new();
            let mut reg = self.registry.lock().await;
            for (hash, intent) in recovery_intents {
                let Some(torrent) = reg.get_mut(hash) else {
                    continue;
                };
                torrent.containment_recovery_intent = Some(*intent);
                torrent.state = TorrentState::NetworkBlocked;
                if let Some(status) = preserved_seeding_statuses.get(hash) {
                    torrent.seeding_status = *status;
                }
                torrent.error = Some(detail.to_owned());
                changed.push(*hash);
            }
            for (hash, torrent) in &mut reg.torrents {
                if torrent.containment_recovery_intent.is_none()
                    && matches!(
                        torrent.state,
                        TorrentState::Downloading
                            | TorrentState::DownloadingMetadata
                            | TorrentState::Seeding
                    )
                {
                    // A modeled active state without a live owning task is not
                    // evidence of recoverable activity. Block the stale state
                    // for truthful API output, but deliberately grant no
                    // automatic resume intent.
                    torrent.state = TorrentState::NetworkBlocked;
                    torrent.error = Some(detail.to_owned());
                    changed.push(*hash);
                }
            }
            changed
        };

        for handle in engine_handles {
            let _ = handle.await;
        }
        for handle in announce_handles {
            let _ = handle.await;
        }
        self.engine_retry_after.write().await.clear();
        changed
    }

    async fn transition_data_plane_to_blocked(
        &self,
        status: NetworkContainmentStatus,
        detail: String,
    ) {
        let _transition = self.data_plane_transition_lock.lock().await;

        // The ordering here is the ADR-0051 contract. Never move a socket-owning
        // shutdown ahead of the gate block or a state mutation ahead of progress
        // reconciliation.
        self.containment_gate.block(status, detail.clone());
        let recovery_intents = self.live_containment_recovery_intents().await;
        let preserved_seeding_statuses = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let registry = self.registry.lock().await;
            recovery_intents
                .iter()
                .filter_map(|(hash, intent)| {
                    if *intent != ContainmentRecoveryIntent::Seeding {
                        return None;
                    }
                    registry
                        .get(hash)
                        .map(|torrent| (*hash, torrent.seeding_status))
                })
                .collect::<HashMap<_, _>>()
        };
        self.stop_dht_runner().await;
        let changed = self
            .abort_data_plane_tasks_for_containment(
                &recovery_intents,
                &preserved_seeding_statuses,
                &detail,
            )
            .await;
        // Progress is now durable in Torrent records; drop all stale task-owned
        // objects so recovery reconstructs them under the new gate generation.
        self.engine_states.write().await.clear();
        // Retain torrent limiters across fail-closed teardown so recovered
        // downloaders/seeders keep the same live policy object.

        {
            let mut health = self.network_health.write().await;
            health.status = status;
            health.detail = detail.clone();
            health.traffic_allowed = false;
        }
        self.persist_state_best_effort("network_blocked").await;
        for hash in changed {
            self.publish_torrent_event("torrent_changed", hash, TorrentState::NetworkBlocked);
        }
        self.publish_event(Event::new(
            "network_status_changed",
            json!({
                "status": status.as_str(),
                "traffic_allowed": false,
                "detail": detail,
            }),
        ));
        self.publish_event(stats_updated_event());
    }

    async fn recover_containment_work(&self, health: NetworkHealth) {
        let _transition = self.data_plane_transition_lock.lock().await;
        *self.network_health.write().await = health.clone();
        self.containment_gate.allow();

        let mut changed = Vec::new();
        let mut downloads = Vec::new();
        let mut seeders = Vec::new();
        {
            let mut reg = self.registry.lock().await;
            for (hash, torrent) in &mut reg.torrents {
                let Some(intent) = torrent.containment_recovery_intent.take() else {
                    continue;
                };
                torrent.error = None;
                torrent.state = match intent {
                    ContainmentRecoveryIntent::Downloading
                    | ContainmentRecoveryIntent::DownloadingMetadata => {
                        downloads.push(*hash);
                        torrent.seeding_status = SeedingStatus::NotEligible;
                        TorrentState::Queued
                    }
                    ContainmentRecoveryIntent::Seeding => {
                        seeders.push(*hash);
                        torrent.seeding_status = SeedingStatus::Queued;
                        TorrentState::Completed
                    }
                };
                changed.push((*hash, torrent.state));
            }
        }
        self.persist_state_best_effort("network_recovered").await;
        drop(_transition);

        // Rebuild only from consumed durable intents. Global reconciliation
        // would also auto-start unrelated queued/completed torrents, violating
        // the recovery-set contract.
        for hash in downloads {
            {
                let mut queue = self.queue.lock().await;
                queue.add(hash);
                queue.start_now(&hash);
            }
            self.start_engine(hash).await;
        }
        for hash in seeders {
            if let Err(error) = self.start_recovered_containment_seeder(hash).await {
                tracing::warn!(info_hash = %hash, %error, "failed to reconstruct recovered seeder");
            }
        }
        for (hash, _) in changed {
            let state = {
                let _lifecycle = self.seeder_lifecycle_lock.lock().await;
                self.registry
                    .lock()
                    .await
                    .get(&hash)
                    .map(|torrent| torrent.state)
            };
            if let Some(state) = state {
                self.publish_torrent_event("torrent_changed", hash, state);
            }
        }
        self.publish_event(Event::new(
            "network_status_changed",
            json!({
                "status": health.status.as_str(),
                "traffic_allowed": true,
                "detail": health.detail,
            }),
        ));
        self.publish_event(stats_updated_event());
    }

    async fn start_recovered_containment_seeder(&self, hash: InfoHash) -> Result<()> {
        let Some(start) = self.prepare_recovered_seeder_start(hash).await? else {
            return Ok(());
        };
        self.start_seeder(
            hash,
            start.meta,
            start.active_dir,
            start.complete_dir,
            start.state,
        )
        .await
    }

    async fn start_recovered_seeder_while_transition_locked(&self, hash: InfoHash) -> Result<()> {
        let Some(start) = self.prepare_recovered_seeder_start(hash).await? else {
            return Ok(());
        };
        self.start_seeder_while_transition_locked(
            hash,
            start.meta,
            start.active_dir,
            start.complete_dir,
            start.state,
        )
        .await
    }

    async fn prepare_recovered_seeder_start(
        &self,
        hash: InfoHash,
    ) -> Result<Option<RecoveredSeederStart>> {
        let mut torrent = self
            .registry
            .lock()
            .await
            .get(&hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        let global = self.config.read().await.seeding.clone();
        let idle_seconds =
            now().saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added));
        let status = automatic_seeding_status(&torrent, &global, idle_seconds);
        if status != SeedingStatus::Queued {
            if let Some(stored) = self.registry.lock().await.get_mut(&hash) {
                stored.state = TorrentState::Completed;
                stored.seeding_status = status;
            }
            self.persist_state_best_effort("containment_recovery_seed_target")
                .await;
            return Ok(None);
        }
        torrent.seeding_status = SeedingStatus::Queued;
        let complete_dir = self.resolve_download_dir(&torrent).await;
        let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
        let state = Arc::new(Mutex::new(EngineState {
            piece_count: torrent.meta.piece_count(),
            total_length: torrent.meta.total_length,
            downloaded: torrent.downloaded,
            uploaded: torrent.uploaded,
            pieces_have: torrent.progress.bitfield().clone(),
            finished: true,
            ..EngineState::default()
        }));
        self.engine_states.write().await.insert(hash, state.clone());
        Ok(Some(RecoveredSeederStart {
            meta: torrent.meta,
            active_dir,
            complete_dir,
            state,
        }))
    }

    /// Spawn an owned sidecar task that periodically announces the seeder to
    /// the torrent's trackers, so the seeder is visible in the swarm. The
    /// returned handle is awaited by the seeder task after signaling shutdown,
    /// so the sidecar cannot outlive the seeder lifecycle.
    async fn spawn_seeder_announce(
        &self,
        hash: InfoHash,
        meta: swarmotter_core::meta::TorrentMeta,
        peer_id: [u8; 20],
        listen_port: u16,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Option<JoinHandle<()>> {
        let tracker_tiers = tracker::announce_tiers(meta.announce.as_deref(), &meta.announce_list);
        if tracker_tiers.is_empty() {
            return None;
        }
        let binder = self.make_binder().await;
        let containment_gate = self.containment_gate.clone();
        let containment_generation = containment_gate.generation();
        Some(tokio::spawn(async move {
            let announce_loop = async move {
                if *shutdown_rx.borrow() {
                    return;
                }
                // Initial announce: started event so trackers see the seeder
                // immediately rather than waiting for the first interval tick.
                let mut announce_after = tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            return;
                        }
                        Duration::from_secs(300)
                    }
                    interval = Self::seeder_announce_once(
                        &tracker_tiers,
                        hash,
                        peer_id,
                        listen_port,
                        binder.as_ref(),
                        AnnounceEvent::Started,
                    ) => Duration::from_secs(interval)
                };
                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                // Best-effort stopped announce, bounded by the
                                // per-tracker announce timeout.
                                Self::seeder_announce_once(
                                    &tracker_tiers,
                                    hash,
                                    peer_id,
                                    listen_port,
                                    binder.as_ref(),
                                    AnnounceEvent::Stopped,
                                )
                                .await;
                                return;
                            }
                        }
                        _ = tokio::time::sleep(announce_after) => {
                            let interval = Self::seeder_announce_once(
                                &tracker_tiers,
                                hash,
                                peer_id,
                                listen_port,
                                binder.as_ref(),
                                AnnounceEvent::Empty,
                            )
                            .await;
                            announce_after = Duration::from_secs(interval);
                        }
                    }
                }
            };
            tokio::select! {
                biased;
                _ = containment_gate.cancelled_since(containment_generation) => {}
                _ = announce_loop => {}
            }
        }))
    }

    async fn stop_seeder(&self, hash: &InfoHash) {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        if let Some(tx) = self.seeder_shutdowns.lock().await.remove(hash) {
            let _ = tx.send(true);
        }
        self.seeder_registry.unregister(hash).await;
        let handle = self.seeder_handles.lock().await.remove(hash);
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        if self.seeder_registry.is_empty().await {
            self.stop_seeder_listener(false).await;
        }
        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
            if torrent.state == TorrentState::Seeding
                && torrent.seeding_status == SeedingStatus::Active
            {
                torrent.state = TorrentState::Completed;
                torrent.seeding_status = if torrent.progress.is_complete() {
                    SeedingStatus::Queued
                } else {
                    SeedingStatus::NotEligible
                };
            }
        }
    }

    /// One-shot tracker announce for a seeder. Best-effort per tracker; logs
    /// and continues on failure. Times out aggressively so a slow or
    /// unreachable tracker cannot stall the announce loop.
    async fn seeder_announce_once(
        tracker_tiers: &[Vec<String>],
        hash: InfoHash,
        peer_id: [u8; 20],
        port: u16,
        binder: &dyn swarmotter_core::net::NetworkBinder,
        event: AnnounceEvent,
    ) -> u64 {
        let mut interval_seconds = 0u64;
        'tiers: for tier in tracker_tiers {
            for url in tier {
                let req = AnnounceRequest {
                    tracker_url: url.clone(),
                    info_hash: hash,
                    peer_id,
                    port,
                    uploaded: 0,
                    downloaded: 0,
                    left: 0,
                    event,
                    numwant: Some(0),
                    compact: true,
                };
                let outcome = if url.starts_with("udp://") {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        udp_tracker::udp_announce(binder, &req),
                    )
                    .await
                } else {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        tracker::http_announce(binder, &req),
                    )
                    .await
                };
                let succeeded = match outcome {
                    Ok(Ok(response)) if response.failure_reason.is_none() => {
                        let interval = response
                            .interval
                            .max(response.min_interval.unwrap_or(0))
                            .clamp(30, 86_400);
                        interval_seconds = interval;
                        tracing::info!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            "seeder announce ok"
                        );
                        true
                    }
                    Ok(Ok(response)) => {
                        tracing::debug!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            error = %response.failure_reason.unwrap_or_else(|| "tracker failure".into()),
                            "seeder announce failed"
                        );
                        false
                    }
                    Ok(Err(e)) => {
                        tracing::debug!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            error = %e,
                            "seeder announce failed"
                        );
                        false
                    }
                    Err(_) => {
                        tracing::debug!(
                            info_hash = %hash,
                            tracker = %url,
                            event = event.as_str(),
                            "seeder announce timed out"
                        );
                        false
                    }
                };
                if succeeded {
                    break 'tiers;
                }
            }
        }
        if interval_seconds == 0 {
            300
        } else {
            interval_seconds
        }
    }

    async fn force_stop_seeder(&self, hash: &InfoHash) {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        if let Some(tx) = self.seeder_shutdowns.lock().await.remove(hash) {
            let _ = tx.send(true);
        }
        self.seeder_registry.unregister(hash).await;
        let handle = self.seeder_handles.lock().await.remove(hash);
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
        if self.seeder_registry.is_empty().await {
            self.stop_seeder_listener(true).await;
        }
        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
            if torrent.state == TorrentState::Seeding
                && torrent.seeding_status == SeedingStatus::Active
            {
                torrent.state = TorrentState::Completed;
                torrent.seeding_status = if torrent.progress.is_complete() {
                    SeedingStatus::Queued
                } else {
                    SeedingStatus::NotEligible
                };
            }
        }
    }

    async fn deactivate_seeders_after_listener_failure(
        &self,
        hashes: &[InfoHash],
        error: &CoreError,
    ) {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        for hash in hashes {
            if let Some(shutdown) = self.seeder_shutdowns.lock().await.remove(hash) {
                let _ = shutdown.send(true);
            }
            self.seeder_registry.unregister(hash).await;
            if let Some(handle) = self.seeder_handles.lock().await.remove(hash) {
                handle.abort();
                let _ = handle.await;
            }
        }
        let mut registry = self.registry.lock().await;
        for hash in hashes {
            if let Some(torrent) = registry.get_mut(hash) {
                if torrent.progress.is_complete()
                    && torrent.state != TorrentState::Paused
                    && torrent.state != TorrentState::NetworkBlocked
                {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = SeedingStatus::Queued;
                    torrent.error = Some(error.to_string());
                }
            }
        }
    }

    async fn reconcile_seeders(&self) {
        let lifecycle_before = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.registry
                .lock()
                .await
                .torrents
                .iter()
                .map(|(hash, torrent)| (*hash, (torrent.state, torrent.seeding_status)))
                .collect::<HashMap<_, _>>()
        };
        let now_secs = now();
        let cfg = self.config.read().await.clone();
        let seeding_limit = cfg.queue.max_active_seeds;
        let samples = self.rate_samples.read().await.clone();
        let mut running_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry.info_hashes().await
        };
        if !running_seeders.is_empty() {
            if let Err(error) = self.ensure_seeder_listener().await {
                tracing::warn!(%error, "shared inbound seeding listener unavailable");
                self.deactivate_seeders_after_listener_failure(&running_seeders, &error)
                    .await;
                running_seeders.clear();
            }
        }

        let completed: Vec<Torrent> = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let reg = self.registry.lock().await;
            reg.torrents
                .values()
                .filter(|torrent| {
                    torrent.progress.is_complete()
                        && matches!(
                            torrent.state,
                            TorrentState::Completed | TorrentState::Seeding
                        )
                })
                .cloned()
                .collect()
        };

        let mut allowed = Vec::new();
        let mut desired_status = HashMap::new();
        for torrent in &completed {
            let hash = torrent.info_hash();
            let idle_seconds = samples
                .get(&hash)
                .and_then(|sample| sample.last_upload_at)
                .map(|at| Instant::now().saturating_duration_since(at).as_secs())
                .unwrap_or_else(|| {
                    now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added))
                });
            let status = automatic_seeding_status(torrent, &cfg.seeding, idle_seconds);
            desired_status.insert(hash, status);
            if status == SeedingStatus::Queued
                && (seeding_limit == 0 || allowed.len() < seeding_limit)
            {
                allowed.push(hash);
            }
        }

        for hash in &running_seeders {
            if !allowed.contains(hash) {
                self.stop_seeder(hash).await;
                if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                    if torrent.state != TorrentState::NetworkBlocked
                        && torrent.state != TorrentState::Paused
                    {
                        torrent.state = TorrentState::Completed;
                        torrent.seeding_status = desired_status
                            .get(hash)
                            .copied()
                            .unwrap_or(SeedingStatus::NotEligible);
                    }
                }
            }
        }

        {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live = self.seeder_registry.info_hashes().await;
            let mut reg = self.registry.lock().await;
            for torrent in reg.torrents.values_mut() {
                let hash = torrent.info_hash();
                if live.contains(&hash) {
                    torrent.state = TorrentState::Seeding;
                    torrent.seeding_status = SeedingStatus::Active;
                } else if let Some(status) = desired_status.get(&hash).copied() {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = status;
                } else if torrent.state != TorrentState::NetworkBlocked
                    && torrent.state != TorrentState::Paused
                    && !torrent.progress.is_complete()
                {
                    torrent.seeding_status = SeedingStatus::NotEligible;
                } else if torrent.state == TorrentState::Seeding
                    || torrent.seeding_status == SeedingStatus::Active
                {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = SeedingStatus::Queued;
                }
            }
        }

        for hash in allowed {
            if self.seeder_registry.contains(&hash).await {
                continue;
            }
            let Some(torrent_for_dir) = completed
                .iter()
                .find(|torrent| torrent.info_hash() == hash)
                .cloned()
            else {
                continue;
            };
            let complete_dir = self.resolve_download_dir(&torrent_for_dir).await;
            let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
            let existing_state = self.engine_states.read().await.get(&hash).cloned();
            let state = if let Some(state) = existing_state {
                state
            } else {
                let pieces_have = torrent_for_dir.progress.bitfield().clone();
                let state = Arc::new(Mutex::new(EngineState {
                    piece_count: torrent_for_dir.meta.piece_count(),
                    total_length: torrent_for_dir.meta.total_length,
                    downloaded: torrent_for_dir.downloaded,
                    uploaded: torrent_for_dir.uploaded,
                    pieces_have,
                    finished: true,
                    ..EngineState::default()
                }));
                self.engine_states.write().await.insert(hash, state.clone());
                state
            };
            if let Err(error) = self
                .start_seeder(hash, torrent_for_dir.meta, active_dir, complete_dir, state)
                .await
            {
                tracing::warn!(info_hash = %hash, %error, "inbound seeding listener unavailable");
                if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                    torrent.state = TorrentState::Completed;
                    torrent.seeding_status = SeedingStatus::Queued;
                    torrent.error = Some(error.to_string());
                }
            }
        }
        let lifecycle_changes = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.registry
                .lock()
                .await
                .torrents
                .iter()
                .filter_map(|(hash, torrent)| {
                    (lifecycle_before.get(hash) != Some(&(torrent.state, torrent.seeding_status)))
                        .then_some((*hash, torrent.state))
                })
                .collect::<Vec<_>>()
        };
        self.persist_state_best_effort("seeder_reconcile").await;
        for (hash, state) in &lifecycle_changes {
            self.publish_torrent_event("torrent_changed", *hash, *state);
        }
        if !lifecycle_changes.is_empty() {
            self.publish_event(stats_updated_event());
        }
    }

    async fn sweep_selfish_completed_torrents_best_effort(&self, reason: &'static str) {
        if let Err(e) = self.sweep_selfish_completed_torrents(reason).await {
            tracing::warn!(
                reason,
                error = %e,
                "selfish completed torrent sweep failed"
            );
        }
    }

    async fn sweep_selfish_completed_torrents(
        &self,
        reason: &'static str,
    ) -> Result<Vec<InfoHash>> {
        if !self.config.read().await.torrent.selfish {
            return Ok(Vec::new());
        }

        let hashes: Vec<InfoHash> = {
            let reg = self.registry.lock().await;
            reg.torrents
                .iter()
                .filter_map(|(hash, torrent)| {
                    matches!(
                        torrent.state,
                        TorrentState::Completed | TorrentState::Seeding
                    )
                    .then_some(*hash)
                })
                .collect()
        };

        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let removed = self
            .remove_torrents_with_single_reconcile(hashes, false)
            .await?;
        if !removed.is_empty() {
            tracing::info!(
                count = removed.len(),
                reason,
                selfish = true,
                delete_data = false,
                "selfish mode removed already-completed torrents; downloaded data preserved"
            );
        }
        Ok(removed)
    }

    /// Selfish-mode completion: remove a finished torrent from the daemon
    /// without deleting its downloaded data. Stops the inbound seeder and
    /// clears all live engine/seeder bookkeeping, then removes the torrent
    /// record from the registry. Equivalent to `remove_torrent` with
    /// `delete_data = false`, but safe to call from within the engine task
    /// itself because it does NOT await the engine task's own join handle
    /// (that would deadlock); the already-returning task is simply detached.
    async fn selfish_remove_completed(&self, hash: InfoHash) {
        let name = self
            .registry
            .lock()
            .await
            .get(&hash)
            .map(|t| t.name().to_string())
            .unwrap_or_default();
        // Stop the inbound seeder (a separate task; safe to await).
        self.stop_seeder(&hash).await;
        // Clear live engine bookkeeping. We deliberately do NOT await the
        // engine join handle: it belongs to the engine task that is calling
        // this method, so awaiting it would deadlock. Dropping the detached
        // handle is safe because the task is already returning.
        self.engine_cmds.lock().await.remove(&hash);
        self.engine_states.write().await.remove(&hash);
        self.torrent_limiters.write().await.remove(&hash);
        self.torrent_peer_permit_pools.write().await.remove(&hash);
        let engine_handle = self.engine_handles.write().await.remove(&hash);
        if let Some(handle) = engine_handle {
            drop(handle);
        }
        // Remove the torrent record; downloaded data is preserved (no
        // delete-data behavior is invoked).
        self.registry.lock().await.remove(&hash);
        self.queue.lock().await.remove(&hash);
        tracing::info!(
            info_hash = %hash,
            name = %name,
            selfish = true,
            delete_data = false,
            "selfish mode removed completed torrent; downloaded data preserved"
        );
        self.publish_event(torrent_removed_event(hash, false));
        self.publish_event(stats_updated_event());
        self.persist_state_best_effort("selfish_completion").await;
    }

    async fn make_binder(&self) -> Arc<dyn swarmotter_core::net::NetworkBinder> {
        let cfg = self.config.read().await.clone();
        Arc::new(
            ContainedBinder::new(cfg.network.clone(), self.interface_probe.clone())
                .with_gate_and_health(self.containment_gate.clone(), self.health_report_tx.clone()),
        )
    }

    /// Revalidate the concrete source/interface/listener bind operations before
    /// an explicit configuration replacement is allowed to clear a latched
    /// bind failure. This binder is intentionally not attached to the blocked
    /// live gate; it opens only ephemeral validation sockets and immediately
    /// drops them.
    async fn validate_replacement_bind_path(&self, config: &Config) -> Result<()> {
        if config.network.mode == NetworkContainmentMode::Disabled {
            return Ok(());
        }
        let binder = ContainedBinder::new(config.network.clone(), self.interface_probe.clone());
        let udp = binder.udp_socket().await.map_err(|error| {
            CoreError::NetworkBlocked(format!(
                "replacement containment UDP bind validation failed: {error}"
            ))
        })?;
        drop(udp);
        let listener = binder
            .bind_peer_listener(config.torrent.listen_port)
            .await
            .map_err(|error| {
                CoreError::NetworkBlocked(format!(
                    "replacement containment listener bind validation failed: {error}"
                ))
            })?;
        drop(listener);
        Ok(())
    }

    /// Periodically re-evaluate network containment health and flip torrent
    /// states between active and `network_blocked` as the path appears or
    /// disappears. Stop running engines when the path becomes unavailable.
    pub async fn network_health_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.network_health_tick().await;
        }
    }

    /// One iteration of the network containment health monitor, extracted so
    /// tests can drive it deterministically without sleeping. It evaluates the
    /// injected interface probe, processes pending bind-failure health reports,
    /// and on a healthy-to-unhealthy transition follows the exact order required
    /// by ADR-0051: block the gate, stop the listener/DHT, abort data-plane tasks,
    /// reconcile progress, set torrents `network_blocked`, persist, and publish.
    pub async fn network_health_tick(&self) {
        // Binder failures already blocked the gate synchronously. Drain their
        // reports to drive centralized teardown and latch the operational
        // failure so a healthy interface probe cannot silently reopen traffic.
        let reported = {
            let mut rx = self.health_report_rx.lock().await;
            let mut latest = None;
            while let Ok(report) = rx.try_recv() {
                latest = Some(report);
            }
            latest
        };
        if let Some(report) = reported {
            if matches!(
                report.status,
                NetworkContainmentStatus::SocketBindFailed
                    | NetworkContainmentStatus::BlockedFailClosed
            ) {
                *self.bind_failure_latched.write().await = Some(report.clone());
            }
            self.transition_data_plane_to_blocked(report.status, report.detail)
                .await;
            return;
        }

        if let Some(report) = self.bind_failure_latched.read().await.clone() {
            // Recovery is deliberately explicit: only a successfully validated
            // full configuration replacement clears this latch.
            if self.containment_gate.traffic_allowed() {
                self.containment_gate
                    .block(report.status, report.detail.clone());
            }
            let mut health = self.network_health.write().await;
            health.status = report.status;
            health.detail = report.detail;
            health.traffic_allowed = false;
            return;
        }

        let cfg = self.config.read().await.clone();
        let health = net::evaluate(&cfg.network, self.interface_probe.as_ref());
        let previous = self.network_health.read().await.clone();

        if !health.traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
            if previous.traffic_allowed
                || previous.status != health.status
                || previous.detail != health.detail
                || self.containment_gate.traffic_allowed()
            {
                self.transition_data_plane_to_blocked(health.status, health.detail)
                    .await;
            }
            return;
        }

        if health.traffic_allowed && !previous.traffic_allowed {
            self.recover_containment_work(health).await;
            return;
        }

        let network_changed = previous.status != health.status
            || previous.traffic_allowed != health.traffic_allowed
            || previous.detail != health.detail;
        *self.network_health.write().await = health.clone();
        if health.traffic_allowed {
            self.containment_gate.allow();
        }
        self.reconcile_engine_progress().await;
        self.reconcile_queue().await;
        if network_changed {
            self.publish_event(Event::new(
                "network_status_changed",
                json!({
                    "status": health.status.as_str(),
                    "traffic_allowed": health.traffic_allowed,
                    "detail": health.detail,
                }),
            ));
            self.publish_event(stats_updated_event());
        }
    }

    /// Copy live engine state (pieces, byte counts) into the torrent records
    /// so API/UI summaries reflect real progress while downloading.
    async fn reconcile_engine_progress(&self) {
        let live_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>()
        };
        self.reconcile_engine_progress_with_seeders(live_seeders, true)
            .await;
    }

    /// Snapshot task-owned counters while a data-plane reconstruction holds
    /// the transition lock. This deliberately skips seeder/task
    /// reconciliation, which could otherwise try to start work recursively
    /// under that same lock.
    async fn reconcile_engine_progress_for_transition(&self) {
        let live_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>()
        };
        self.reconcile_engine_progress_with_seeders(live_seeders, false)
            .await;
    }

    async fn reconcile_engine_progress_with_seeders(
        &self,
        live_seeders: HashSet<InfoHash>,
        finish_lifecycle_reconciliation: bool,
    ) {
        let states = self.engine_states.read().await.clone();
        let running_engines: HashSet<InfoHash> =
            self.engine_handles.read().await.keys().copied().collect();
        let now = Instant::now();
        let retry_after = self.engine_retry_after.read().await.clone();
        let previous_samples = self.rate_samples.read().await.clone();
        let global_download_limit = self.config.read().await.bandwidth.effective_download();
        let network_health = self.network_health.read().await.clone();
        let mut snapshots = Vec::with_capacity(states.len());
        for (hash, state) in states {
            let state = state.lock().await.clone();
            let engine_is_running = running_engines.contains(&hash);
            let retry_suppressed = retry_after
                .get(&hash)
                .is_some_and(|retry_at| *retry_at > now);
            snapshots.push((hash, state, engine_is_running, retry_suppressed));
        }

        let mut sample_updates = Vec::new();
        let mut events = Vec::new();
        let mut reg = self.registry.lock().await;
        let calc = HealthCalculator::new();
        for (hash, s, engine_is_running, retry_suppressed) in &snapshots {
            if let Some(t) = reg.get_mut(hash) {
                let previous_state = t.state;
                let needed_metadata = t.needs_metadata;
                if let Some(real) = s.resolved_meta.as_ref() {
                    apply_resolved_metadata(t, real, s);
                    if needed_metadata && !t.needs_metadata {
                        events.push(torrent_metadata_event(*hash));
                    }
                }
                let mut peak = previous_samples
                    .get(hash)
                    .map(|p| p.peak_rate_down)
                    .unwrap_or(0);
                if let Some(prev) = previous_samples.get(hash).copied() {
                    let elapsed = now.duration_since(prev.at);
                    if elapsed >= Duration::from_millis(250) {
                        let secs = elapsed.as_secs_f64();
                        let down_delta = s.downloaded.saturating_sub(prev.downloaded);
                        let up_delta = s.uploaded.saturating_sub(prev.uploaded);
                        let inst_down = ((down_delta as f64) / secs) as u64;
                        let inst_up = ((up_delta as f64) / secs) as u64;
                        let (last_download_at, no_download_since) = if down_delta > 0 {
                            (Some(now), None)
                        } else {
                            (
                                prev.last_download_at,
                                Some(prev.no_download_since.unwrap_or(prev.at)),
                            )
                        };
                        let last_upload_at = if up_delta > 0 {
                            Some(now)
                        } else {
                            prev.last_upload_at
                        };
                        t.rate_down = smooth_rate(prev.rate_down, inst_down, last_download_at, now);
                        t.rate_up = smooth_rate(prev.rate_up, inst_up, last_upload_at, now);
                        let previous_peak_down = prev.peak_rate_down;
                        let previous_peak_up = prev.peak_rate_up;
                        let observed_down = t.rate_down.max(inst_down);
                        let observed_up = t.rate_up.max(inst_up);
                        peak = previous_peak_down.max(observed_down);
                        let peak_rate_up = previous_peak_up.max(observed_up);
                        if peak > previous_peak_down || peak_rate_up > previous_peak_up {
                            log_torrent_throughput_peak(
                                hash,
                                t,
                                s,
                                inst_down,
                                inst_up,
                                previous_peak_down,
                                previous_peak_up,
                                peak,
                                peak_rate_up,
                                now,
                            );
                        }
                        sample_updates.push((
                            *hash,
                            RateSample {
                                downloaded: s.downloaded,
                                uploaded: s.uploaded,
                                rate_down: t.rate_down,
                                rate_up: t.rate_up,
                                last_download_at,
                                last_upload_at,
                                no_download_since,
                                at: now,
                                peak_rate_down: peak,
                                peak_rate_up,
                            },
                        ));
                    }
                } else {
                    sample_updates.push((
                        *hash,
                        RateSample {
                            downloaded: s.downloaded,
                            uploaded: s.uploaded,
                            rate_down: t.rate_down,
                            rate_up: t.rate_up,
                            last_download_at: None,
                            last_upload_at: None,
                            no_download_since: Some(now),
                            at: now,
                            peak_rate_down: 0,
                            peak_rate_up: 0,
                        },
                    ));
                }
                t.progress
                    .replace_from_bitfield(&s.pieces_have, s.piece_count);
                t.recompute_file_bytes_completed();
                t.downloaded = s.downloaded;
                t.uploaded = s.uploaded;
                t.active_peer_workers = s.active_peers;
                t.known_peers = s.peers.len();
                if !t.state.is_error() && t.state != TorrentState::Paused {
                    if s.finished {
                        if !t.progress.is_complete() {
                            t.state = TorrentState::Completed;
                            t.seeding_status = SeedingStatus::NotEligible;
                        } else if live_seeders.contains(hash) {
                            t.state = TorrentState::Seeding;
                            t.seeding_status = SeedingStatus::Active;
                        } else {
                            t.state = TorrentState::Completed;
                            t.seeding_status = SeedingStatus::Queued;
                        }
                    } else if *engine_is_running && !*retry_suppressed {
                        t.seeding_status = SeedingStatus::NotEligible;
                        if t.needs_metadata {
                            t.state = TorrentState::DownloadingMetadata;
                        } else if t.state == TorrentState::Queued
                            || t.state == TorrentState::DownloadingMetadata
                        {
                            t.state = TorrentState::Downloading;
                        }
                    }
                }

                // Compute per-torrent health from real engine state. Health
                // is exposed on every summary, so the Web UI can render a
                // signal-bars indicator without an extra round-trip.
                let health_input = build_health_input(
                    t,
                    s.piece_count,
                    &s.pieces_have,
                    &s.peer_health,
                    &s.tracker_ok,
                    s.dht_discovery_ok,
                    s.pex_discovery_ok,
                    s.tracker_failures_recent,
                    s.peer_disconnects_recent,
                    s.hash_failures,
                    s.timeout_failures,
                    s.last_valid_block,
                    s.block_last_seen,
                    s.webseed_last_seen,
                    s.dht_last_seen,
                    s.pex_last_seen,
                    s.tracker_last_ok,
                    s.peers.len(),
                    s.tracker_message.as_deref(),
                    peak,
                    global_download_limit,
                    network_health.clone(),
                );
                t.health = calc.compute(&health_input);
                if t.state != previous_state {
                    events.push(torrent_event("torrent_changed", *hash, t.state));
                    if t.state == TorrentState::Completed {
                        events.push(torrent_event("torrent_completed", *hash, t.state));
                    }
                }
            }
        }
        drop(reg);
        for event in events {
            self.publish_event(event);
        }
        if !snapshots.is_empty() {
            self.publish_event(stats_updated_event());
        }
        if !sample_updates.is_empty() {
            let mut samples = self.rate_samples.write().await;
            for (hash, sample) in sample_updates {
                samples.insert(hash, sample);
            }
        }
        if finish_lifecycle_reconciliation {
            self.sweep_selfish_completed_torrents_best_effort("engine_progress")
                .await;
            self.reconcile_seeders().await;
            if !snapshots.is_empty() {
                self.persist_state_best_effort("engine_progress").await;
            }
        }
    }

    /// Periodically compute autopilot decisions from contained runtime
    /// telemetry. In `act` mode this applies only bounded daemon/engine
    /// commands that use existing contained data-plane paths.
    pub async fn autopilot_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(AUTOPILOT_INTERVAL).await;
            self.reconcile_queue().await;
            self.refresh_autopilot_decisions(true).await;
        }
    }

    async fn refresh_autopilot_decisions(&self, apply_actions: bool) {
        self.reconcile_engine_progress().await;

        let cfg = self.config.read().await.clone();
        let global_mode = cfg.autopilot.mode;
        let network = self.network_health.read().await.clone();
        let states = self.engine_states.read().await.clone();
        let samples = self.rate_samples.read().await.clone();
        let torrents: Vec<Torrent> = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        let analyzer = AutopilotAnalyzer::new();
        let mut decisions = HashMap::new();
        let now = Instant::now();

        for torrent in torrents {
            let hash = torrent.info_hash();
            let state = match states.get(&hash) {
                Some(state) => Some(state.lock().await.clone()),
                None => None,
            };
            let input = build_autopilot_input(
                &torrent,
                state.as_ref(),
                samples.get(&hash).copied(),
                now,
                &network,
            );
            let mode = effective_autopilot_mode(global_mode, torrent.autopilot_mode_override);
            let decision = analyzer.analyze(&input, mode);
            if apply_actions && mode == AutopilotMode::Act {
                self.apply_autopilot_decision(hash, &decision, &cfg).await;
            }
            decisions.insert(hash, decision);
        }

        *self.autopilot_decisions.write().await = decisions;
    }

    async fn apply_autopilot_decision(
        &self,
        hash: InfoHash,
        decision: &AutopilotDecision,
        cfg: &Config,
    ) {
        if !decision.apply {
            return;
        }
        let Some(action) = decision.action.as_ref() else {
            return;
        };
        let now = Instant::now();
        if self
            .autopilot_last_action
            .read()
            .await
            .get(&hash)
            .is_some_and(|at| now.saturating_duration_since(*at) < AUTOPILOT_ACTION_COOLDOWN)
        {
            return;
        }

        let applied = match action.kind {
            AutopilotActionKind::IncreasePeerWorkers => {
                self.apply_autopilot_peer_worker_limit(hash, decision, cfg)
                    .await
            }
            AutopilotActionKind::ExpandDiscovery => {
                self.send_engine_command(hash, EngineCommand::Reannounce)
                    .await
            }
            AutopilotActionKind::RelaxPeerBackoff => {
                self.send_engine_command(hash, EngineCommand::RelaxPeerBackoff)
                    .await
            }
            AutopilotActionKind::ReleaseQueueSlot => self.apply_autopilot_queue_release(hash).await,
            AutopilotActionKind::RaiseDownloadCeiling => {
                self.apply_autopilot_download_ceiling(hash, action.suggested_download_limit)
                    .await
            }
        };

        if applied {
            self.autopilot_last_action.write().await.insert(hash, now);
            tracing::info!(
                info_hash = %hash,
                action_kind = ?action.kind,
                rationale = %action.rationale,
                causes = ?decision.snapshot.causes,
                "autopilot applied action"
            );
        }
    }

    async fn send_engine_command(&self, hash: InfoHash, command: EngineCommand) -> bool {
        let tx = self.engine_cmds.lock().await.get(&hash).cloned();
        let Some(tx) = tx else {
            return false;
        };
        tx.send(command).await.is_ok()
    }

    async fn apply_autopilot_peer_worker_limit(
        &self,
        hash: InfoHash,
        decision: &AutopilotDecision,
        cfg: &Config,
    ) -> bool {
        let current = decision.snapshot.peer_worker_limit.max(1);
        let hard_limit =
            Self::effective_per_torrent_peer_limit(cfg.bandwidth.max_peers_per_torrent);
        let next = current.saturating_add(1).min(hard_limit).max(1);
        if next <= current {
            tracing::debug!(
                info_hash = %hash,
                current_peer_worker_limit = current,
                hard_peer_worker_limit = hard_limit,
                "autopilot peer worker increase skipped by configured hard cap"
            );
            return false;
        }
        self.send_engine_command(hash, EngineCommand::UpdatePeerWorkerLimit(next))
            .await
    }

    async fn apply_autopilot_queue_release(&self, hash: InfoHash) -> bool {
        if !self.engine_handles.read().await.contains_key(&hash) {
            return false;
        }
        if self
            .desired_download_hashes_excluding(Some(hash))
            .await
            .is_empty()
        {
            tracing::debug!(
                info_hash = %hash,
                "autopilot queue-slot release skipped because no queued replacement is currently eligible"
            );
            return false;
        }
        self.force_stop_engine(&hash).await;
        {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(&hash) else {
                return false;
            };
            if matches!(
                t.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            ) {
                t.state = TorrentState::Queued;
                t.error = Some("autopilot released active queue slot after no progress".into());
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(hash);
            queue.clear_bypass(&hash);
            queue.move_to_bottom(&hash);
        }
        self.engine_retry_after
            .write()
            .await
            .insert(hash, Instant::now() + AUTOPILOT_QUEUE_RELEASE_RETRY_DELAY);
        self.schedule_reconcile_queue("autopilot_queue_release")
            .await;
        true
    }

    async fn apply_autopilot_download_ceiling(
        &self,
        hash: InfoHash,
        suggested_download_limit: Option<u64>,
    ) -> bool {
        let Some(download_limit) = suggested_download_limit else {
            tracing::debug!(
                info_hash = %hash,
                "autopilot download ceiling change skipped without a bounded suggestion"
            );
            return false;
        };
        let mut reg = self.registry.lock().await;
        let Some(t) = reg.get_mut(&hash) else {
            return false;
        };
        if t.download_limit == 0 || download_limit <= t.download_limit {
            return false;
        }
        t.download_limit = download_limit;
        drop(reg);
        if let Some(rl) = self.torrent_limiters.read().await.get(&hash).cloned() {
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Download,
                download_limit,
            );
        }
        true
    }

    /// Watch-folder scan loop: periodically scans configured folders and imports
    /// newly-stabilized `.torrent` files.
    pub async fn watch_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if let Err(error) = self.scan_watch_folders().await {
                tracing::warn!(
                    error = %error,
                    error_code = %error.code(),
                    "automatic watch-folder scan incomplete; observations retained for retry"
                );
            }
        }
    }

    async fn scan_watch_folders(&self) -> Result<()> {
        let _scan_guard = self.watch_scan_lock.lock().await;
        let cfg = self.config.read().await.clone();
        let mut configured_roots = HashSet::new();
        for folder in &cfg.watch {
            configured_roots.insert(watch::lexical_absolute(Path::new(&folder.path))?);
        }
        self.watch_observations
            .lock()
            .await
            .retain(|key, _| configured_roots.contains(&key.root));

        let mut successful_seen: HashMap<PathBuf, HashSet<watch::ObservationKey>> = HashMap::new();
        let mut incomplete_roots = HashSet::new();
        let mut first_error = None;
        for folder in &cfg.watch {
            let scan_folder = folder.clone();
            let scan = tokio::task::spawn_blocking(move || watch::scan_watch_folder(&scan_folder))
                .await
                .map_err(|error| CoreError::Storage(format!("watch scan task failed: {error}")))?;
            let scan = match scan {
                Ok(scan) => scan,
                Err(error) => {
                    if let Ok(root) = watch::lexical_absolute(Path::new(&folder.path)) {
                        incomplete_roots.insert(root);
                    }
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };
            let seen = successful_seen.entry(scan.root.clone()).or_default();
            for file in scan.files {
                seen.insert(file.key.clone());
                if self.observe_watch_file(&file).await {
                    self.process_watch_file(&file, folder).await;
                }
            }
        }

        let mut observations = self.watch_observations.lock().await;
        observations.retain(|key, _| {
            if incomplete_roots.contains(&key.root) {
                return true;
            }
            successful_seen
                .get(&key.root)
                .is_none_or(|seen| seen.contains(key))
        });
        drop(observations);
        first_error.map_or(Ok(()), Err)
    }

    /// Advance one observation. The first sighting and every changed
    /// fingerprint start at one stable scan; the next identical scan is
    /// eligible unless this exact fingerprint already reached a terminal
    /// processed outcome.
    async fn observe_watch_file(&self, file: &watch::ScannedTorrentFile) -> bool {
        let mut observations = self.watch_observations.lock().await;
        match observations.get_mut(&file.key) {
            Some(observation) if observation.fingerprint == file.fingerprint => {
                observation.stable_scans = observation.stable_scans.saturating_add(1);
                observation.stable_scans >= 2
                    && observation.processed_fingerprint != Some(file.fingerprint)
            }
            Some(observation) => {
                observation.fingerprint = file.fingerprint;
                observation.stable_scans = 1;
                observation.processed_fingerprint = None;
                false
            }
            None => {
                observations.insert(
                    file.key.clone(),
                    WatchObservation {
                        fingerprint: file.fingerprint,
                        stable_scans: 1,
                        processed_fingerprint: None,
                    },
                );
                false
            }
        }
    }

    async fn process_watch_file(
        &self,
        file: &watch::ScannedTorrentFile,
        folder: &swarmotter_core::config::WatchFolderConfig,
    ) {
        let path = file.path();
        let bytes = match self.read_stable_watch_file(file).await {
            Ok(WatchReadOutcome::Stable(bytes)) => bytes,
            Ok(WatchReadOutcome::Changed(fingerprint)) => {
                if let Some(observation) = self.watch_observations.lock().await.get_mut(&file.key) {
                    observation.fingerprint = fingerprint;
                    observation.stable_scans = 1;
                    observation.processed_fingerprint = None;
                }
                return;
            }
            Err(error) => {
                self.finish_watch_attempt(file, folder, None, Err(error))
                    .await;
                return;
            }
        };

        let parsed = match meta::parse_torrent(&bytes) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.finish_watch_attempt(file, folder, None, Err(error))
                    .await;
                return;
            }
        };
        let hash = parsed.info_hash;
        let mut torrent = Torrent::new(parsed, now());
        watch::apply_folder_defaults(&mut torrent, folder);
        let paused = matches!(
            folder.start_behavior,
            swarmotter_core::config::StartBehavior::Paused
        );
        let mutation = self
            .add_torrent_mutation(torrent, paused, "watch_import_added")
            .await;
        self.finish_watch_attempt(file, folder, Some(hash), mutation)
            .await;
        tracing::debug!(path = %path.display(), "watch torrent attempt finished");
    }

    async fn read_stable_watch_file(
        &self,
        file: &watch::ScannedTorrentFile,
    ) -> Result<WatchReadOutcome> {
        let path = file.path();
        let expected = file.fingerprint;
        let read_path = path.clone();
        let read = tokio::task::spawn_blocking(move || {
            watch::read_bounded_watch_file(&read_path, expected)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("watch read task failed: {error}")))??;
        let bytes = match read {
            watch::BoundedWatchRead::Stable(bytes) => bytes,
            watch::BoundedWatchRead::Changed(fingerprint) => {
                return Ok(WatchReadOutcome::Changed(fingerprint));
            }
        };
        self.wait_at_watch_after_read_test_pause().await;
        let recheck_path = path;
        let after = tokio::task::spawn_blocking(move || -> Result<watch::FileFingerprint> {
            let metadata = fs::symlink_metadata(&recheck_path).map_err(CoreError::from)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(CoreError::Storage(format!(
                    "watch source is not a regular file after read: {}",
                    recheck_path.display()
                )));
            }
            watch::FileFingerprint::from_metadata(&metadata)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("watch recheck task failed: {error}")))??;
        if after != expected {
            Ok(WatchReadOutcome::Changed(after))
        } else {
            Ok(WatchReadOutcome::Stable(bytes))
        }
    }

    async fn finish_watch_attempt(
        &self,
        file: &watch::ScannedTorrentFile,
        folder: &swarmotter_core::config::WatchFolderConfig,
        parsed_hash: Option<InfoHash>,
        result: Result<TorrentAddMutationOutcome>,
    ) {
        let path = file.path();
        let (outcome, info_hash, error) = match result {
            Ok(TorrentAddMutationOutcome::Inserted { hash, .. }) => {
                (watch::ImportOutcome::Imported, Some(hash), None)
            }
            Ok(TorrentAddMutationOutcome::Duplicate { hash }) => {
                (watch::ImportOutcome::Duplicate, Some(hash), None)
            }
            Err(error) if is_permanent_watch_error(&error) => (
                watch::ImportOutcome::PermanentFailure,
                parsed_hash,
                Some(error.to_string()),
            ),
            Err(error) => (
                watch::ImportOutcome::TransientFailure,
                parsed_hash,
                Some(error.to_string()),
            ),
        };
        let processed = outcome != watch::ImportOutcome::TransientFailure;
        let action = match outcome {
            watch::ImportOutcome::Imported | watch::ImportOutcome::Duplicate => {
                Some(watch::post_import_action(folder, &path))
            }
            watch::ImportOutcome::PermanentFailure => {
                Some(watch::post_failure_action(folder, &path))
            }
            watch::ImportOutcome::TransientFailure => None,
        };
        let post_action_error = if let Some(action) = action {
            let action_path = path.clone();
            tokio::task::spawn_blocking(move || {
                watch::execute_post_import_action(&action_path, &action)
            })
            .await
            .map_err(|join| CoreError::Storage(format!("watch post-action task failed: {join}")))
            .and_then(|result| result)
            .err()
            .map(|error| error.to_string())
        } else {
            None
        };

        if processed {
            if let Some(observation) = self.watch_observations.lock().await.get_mut(&file.key) {
                if observation.fingerprint == file.fingerprint {
                    observation.processed_fingerprint = Some(file.fingerprint);
                }
            }
        }
        let import = watch::ImportResult {
            path: path.display().to_string(),
            success: matches!(
                outcome,
                watch::ImportOutcome::Imported | watch::ImportOutcome::Duplicate
            ),
            info_hash_hex: info_hash.map(|hash| hash.to_hex()),
            error,
            duplicate: outcome == watch::ImportOutcome::Duplicate,
            post_action_error,
            outcome,
        };
        self.record_watch_import(import.clone()).await;
        self.publish_watch_event(&import);
    }

    async fn record_watch_import(&self, result: watch::ImportResult) {
        let mut history = self.watch_imports.lock().await;
        while history.len() >= watch::MAX_IMPORT_HISTORY {
            history.pop_front();
        }
        history.push_back(result);
    }

    fn publish_watch_event(&self, result: &watch::ImportResult) {
        let kind = if result.success {
            "watch_folder_imported"
        } else {
            "watch_folder_failed"
        };
        let payload = json!({
            "path": result.path,
            "outcome": result.outcome.as_str(),
            "success": result.success,
            "duplicate": result.duplicate,
            "info_hash": result.info_hash_hex,
            "error": result.error,
            "post_action_error": result.post_action_error,
        });
        let mut event = Event::new(kind, payload);
        if let Some(hash) = &result.info_hash_hex {
            event = event.with_info_hash(hash.clone());
        }
        self.publish_event(event);
    }

    async fn apply_runtime_config_fields(&self) {
        self.apply_runtime_config_fields_impl(true).await;
    }

    /// Apply runtime fields whose effects can be exactly rolled back. The
    /// peer reconfiguration transaction uses this before persistent commit;
    /// irreversible selfish removals run only after commit succeeds.
    async fn apply_runtime_config_fields_reversible(&self) {
        self.apply_runtime_config_fields_impl(false).await;
    }

    async fn apply_runtime_config_fields_impl(&self, allow_irreversible: bool) {
        let cfg = self.config.read().await.clone();
        self.queue.lock().await.limits = cfg.queue.clone();
        self.global_limiter.set_capacity(
            swarmotter_core::bandwidth::RateDirection::Download,
            cfg.bandwidth.effective_download(),
        );
        self.global_limiter.set_capacity(
            swarmotter_core::bandwidth::RateDirection::Upload,
            cfg.bandwidth.effective_upload(),
        );
        // Evaluate configuration changes through the same transition operation
        // as periodic path monitoring. Updating the health snapshot directly
        // would hide the healthy-to-blocked edge from the next tick and leave
        // cancelled task registries/state unreconciled.
        self.network_health_tick().await;
        self.apply_peer_worker_limits().await;
        self.schedule_reconcile_queue("runtime_config").await;
        if allow_irreversible {
            self.sweep_selfish_completed_torrents_best_effort("runtime_config")
                .await;
        }
        self.reconcile_seeders().await;
    }

    async fn stop_data_plane_for_reconfiguration(&self) {
        self.reconcile_engine_progress_for_transition().await;
        let registry_hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.stop_all_torrent_tasks(&registry_hashes).await;
        *self.dht_runner.lock().await = None;
    }

    async fn live_peer_work_snapshot(&self) -> LivePeerWorkSnapshot {
        let running = self
            .engine_handles
            .read()
            .await
            .iter()
            .filter_map(|(hash, handle)| (!handle.is_finished()).then_some(*hash))
            .collect::<HashSet<_>>();
        let downloads = {
            let registry = self.registry.lock().await;
            running
                .into_iter()
                .filter_map(|hash| {
                    registry
                        .get(&hash)
                        .map(|torrent| LiveTorrentTaskSnapshot::from_torrent(hash, torrent))
                })
                .collect()
        };
        let seeder_hashes = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            self.seeder_registry.info_hashes().await
        };
        let seeders = {
            let registry = self.registry.lock().await;
            seeder_hashes
                .into_iter()
                .filter_map(|hash| {
                    registry
                        .get(&hash)
                        .map(|torrent| LiveTorrentTaskSnapshot::from_torrent(hash, torrent))
                })
                .collect()
        };
        LivePeerWorkSnapshot { downloads, seeders }
    }

    async fn torrent_lifecycle_snapshot(&self) -> HashMap<InfoHash, LiveTorrentTaskSnapshot> {
        self.registry
            .lock()
            .await
            .torrents
            .iter()
            .map(|(hash, torrent)| (*hash, LiveTorrentTaskSnapshot::from_torrent(*hash, torrent)))
            .collect()
    }

    async fn restore_torrent_lifecycle_snapshot(
        &self,
        snapshot: &HashMap<InfoHash, LiveTorrentTaskSnapshot>,
    ) {
        let mut registry = self.registry.lock().await;
        for (hash, prior) in snapshot {
            if let Some(torrent) = registry.get_mut(hash) {
                torrent.state = prior.state;
                torrent.seeding_status = prior.seeding_status;
                torrent.error = prior.error.clone();
                torrent.containment_recovery_intent = prior.containment_recovery_intent;
            }
        }
    }

    async fn reconstruct_live_peer_work_while_transition_locked(
        &self,
        snapshot: &LivePeerWorkSnapshot,
    ) -> Result<()> {
        if snapshot.is_empty() {
            return Ok(());
        }
        let health = self.network_health.read().await.clone();
        if !health.traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
            return Err(CoreError::Internal(format!(
                "cannot reconstruct peer work while containment is blocked: {}",
                health.detail
            )));
        }

        for prior in &snapshot.downloads {
            {
                let mut registry = self.registry.lock().await;
                let torrent = registry.get_mut(&prior.hash).ok_or_else(|| {
                    CoreError::Internal(format!(
                        "cannot reconstruct missing download torrent {}",
                        prior.hash
                    ))
                })?;
                torrent.state = if prior.state == TorrentState::DownloadingMetadata {
                    TorrentState::DownloadingMetadata
                } else {
                    TorrentState::Downloading
                };
                torrent.error = None;
                torrent.containment_recovery_intent = None;
            }
            // The captured task was already scheduler-authorized. Restart it
            // directly without mutating queue order or granting a durable
            // `start_now` bypass as a side effect of reconfiguration.
            self.start_engine_while_transition_locked(prior.hash).await;
        }

        for prior in &snapshot.seeders {
            {
                let mut registry = self.registry.lock().await;
                let torrent = registry.get_mut(&prior.hash).ok_or_else(|| {
                    CoreError::Internal(format!(
                        "cannot reconstruct missing seeding torrent {}",
                        prior.hash
                    ))
                })?;
                torrent.state = TorrentState::Completed;
                torrent.seeding_status = SeedingStatus::Queued;
                torrent.error = None;
                torrent.containment_recovery_intent = None;
            }
            self.start_recovered_seeder_while_transition_locked(prior.hash)
                .await?;
        }

        // A rollback restores the exact modeled lifecycle fields captured
        // with the prior task ownership. In particular, provisional blocked
        // recovery intents must not leak into the restored healthy runtime.
        {
            let mut registry = self.registry.lock().await;
            for prior in snapshot.downloads.iter().chain(&snapshot.seeders) {
                let torrent = registry.get_mut(&prior.hash).ok_or_else(|| {
                    CoreError::Internal(format!(
                        "cannot restore lifecycle for missing torrent {}",
                        prior.hash
                    ))
                })?;
                torrent.state = prior.state;
                torrent.seeding_status = prior.seeding_status;
                torrent.error = prior.error.clone();
                torrent.containment_recovery_intent = prior.containment_recovery_intent;
            }
        }

        self.verify_live_peer_work(snapshot).await
    }

    async fn verify_live_peer_work(&self, snapshot: &LivePeerWorkSnapshot) -> Result<()> {
        tokio::task::yield_now().await;
        let missing_downloads = {
            let handles = self.engine_handles.read().await;
            snapshot
                .downloads
                .iter()
                .filter_map(|prior| {
                    (!handles
                        .get(&prior.hash)
                        .is_some_and(|handle| !handle.is_finished()))
                    .then_some(prior.hash)
                })
                .collect::<Vec<_>>()
        };
        let missing_seeders = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live = self
                .seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>();
            snapshot
                .seeders
                .iter()
                .filter_map(|prior| (!live.contains(&prior.hash)).then_some(prior.hash))
                .collect::<Vec<_>>()
        };
        if missing_downloads.is_empty() && missing_seeders.is_empty() {
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "peer work reconstruction incomplete: missing downloads {missing_downloads:?}, missing seeders {missing_seeders:?}"
            )))
        }
    }

    async fn verify_eligible_peer_work(&self) -> Result<()> {
        tokio::task::yield_now().await;
        let desired_downloads = self.desired_download_hashes().await;
        let (missing_downloads, unexpected_downloads) = {
            let handles = self.engine_handles.read().await;
            let live = handles
                .iter()
                .filter_map(|(hash, handle)| (!handle.is_finished()).then_some(*hash))
                .collect::<HashSet<_>>();
            (
                desired_downloads
                    .iter()
                    .filter(|hash| !live.contains(hash))
                    .copied()
                    .collect::<Vec<_>>(),
                live.iter()
                    .filter(|hash| !desired_downloads.contains(hash))
                    .copied()
                    .collect::<Vec<_>>(),
            )
        };
        let expected_seeders = self.eligible_seeder_hashes().await;
        let (seeder_mismatch, missing_seeders, unexpected_seeders) = {
            let _lifecycle = self.seeder_lifecycle_lock.lock().await;
            let live = self
                .seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>();
            let registry = self.registry.lock().await;
            (
                live.iter().any(|hash| {
                    !registry.get(hash).is_some_and(|torrent| {
                        torrent.state == TorrentState::Seeding
                            && torrent.seeding_status == SeedingStatus::Active
                    })
                }) || registry.torrents.iter().any(|(hash, torrent)| {
                    (torrent.state == TorrentState::Seeding
                        || torrent.seeding_status == SeedingStatus::Active)
                        && !live.contains(hash)
                }),
                expected_seeders
                    .difference(&live)
                    .copied()
                    .collect::<Vec<_>>(),
                live.difference(&expected_seeders)
                    .copied()
                    .collect::<Vec<_>>(),
            )
        };
        if missing_downloads.is_empty()
            && unexpected_downloads.is_empty()
            && missing_seeders.is_empty()
            && unexpected_seeders.is_empty()
            && !seeder_mismatch
        {
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "eligible peer work verification failed: missing downloads {missing_downloads:?}, unexpected downloads {unexpected_downloads:?}, missing seeders {missing_seeders:?}, unexpected seeders {unexpected_seeders:?}, seeder mismatch {seeder_mismatch}"
            )))
        }
    }

    async fn eligible_seeder_hashes(&self) -> HashSet<InfoHash> {
        let cfg = self.config.read().await.clone();
        let samples = self.rate_samples.read().await.clone();
        let completed = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .filter(|torrent| {
                torrent.progress.is_complete()
                    && matches!(
                        torrent.state,
                        TorrentState::Completed | TorrentState::Seeding
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut expected = HashSet::new();
        for torrent in completed {
            let hash = torrent.info_hash();
            let idle_seconds = samples
                .get(&hash)
                .and_then(|sample| sample.last_upload_at)
                .map(|at| Instant::now().saturating_duration_since(at).as_secs())
                .unwrap_or_else(|| {
                    now().saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added))
                });
            if automatic_seeding_status(&torrent, &cfg.seeding, idle_seconds)
                == SeedingStatus::Queued
                && (cfg.queue.max_active_seeds == 0 || expected.len() < cfg.queue.max_active_seeds)
            {
                expected.insert(hash);
            }
        }
        expected
    }

    async fn reconstruct_eligible_peer_work_while_transition_locked(&self) -> Result<()> {
        for hash in self.desired_download_hashes().await {
            self.start_engine_while_transition_locked(hash).await;
        }
        for hash in self.eligible_seeder_hashes().await {
            self.start_recovered_seeder_while_transition_locked(hash)
                .await?;
        }
        self.verify_eligible_peer_work().await
    }

    async fn restore_peer_reconfiguration(
        &self,
        previous: &Config,
        previous_permits: &PeerPermitConfiguration,
        previous_health: &NetworkHealth,
        previous_bind_failure: &Option<HealthReport>,
        previous_lifecycle: &HashMap<InfoHash, LiveTorrentTaskSnapshot>,
        live_work: &LivePeerWorkSnapshot,
    ) -> Result<()> {
        let transition = self.data_plane_transition_lock.lock().await;
        self.restore_peer_reconfiguration_while_transition_locked(
            previous,
            previous_permits,
            previous_health,
            previous_bind_failure,
            previous_lifecycle,
            live_work,
        )
        .await?;
        drop(transition);
        self.apply_runtime_config_fields().await;
        self.verify_peer_permit_configuration_identity(previous_permits)
            .await?;
        self.verify_live_peer_work(live_work).await?;
        self.persist_state().await
    }

    async fn restore_peer_reconfiguration_while_transition_locked(
        &self,
        previous: &Config,
        previous_permits: &PeerPermitConfiguration,
        previous_health: &NetworkHealth,
        previous_bind_failure: &Option<HealthReport>,
        previous_lifecycle: &HashMap<InfoHash, LiveTorrentTaskSnapshot>,
        live_work: &LivePeerWorkSnapshot,
    ) -> Result<()> {
        self.stop_data_plane_for_reconfiguration().await;
        *self.config.write().await = previous.clone();
        self.install_peer_permit_configuration(previous_permits.clone())
            .await;
        *self.network_health.write().await = previous_health.clone();
        *self.bind_failure_latched.write().await = previous_bind_failure.clone();
        if previous_health.traffic_allowed
            || previous_health.mode == NetworkContainmentMode::Disabled
        {
            self.containment_gate.allow();
        } else {
            self.containment_gate
                .block(previous_health.status, previous_health.detail.clone());
        }
        self.restore_torrent_lifecycle_snapshot(previous_lifecycle)
            .await;
        self.reconstruct_live_peer_work_while_transition_locked(live_work)
            .await?;
        self.verify_peer_permit_configuration_identity(previous_permits)
            .await?;
        self.verify_live_peer_work(live_work).await?;
        self.persist_state().await
    }

    async fn apply_peer_budget_runtime_update(
        &self,
        next: Config,
        peer_permits: PeerPermitConfiguration,
        persist_path: Option<&Path>,
        clear_bind_failure_latch: bool,
    ) -> Result<()> {
        let previous = self.config.read().await.clone();
        let previous_permits = self.current_peer_permit_configuration().await;
        let previous_health = self.network_health.read().await.clone();
        let previous_bind_failure = self.bind_failure_latched.read().await.clone();
        let file_snapshot = persist_path.map(capture_config_file).transpose()?;
        let next_health = if previous_bind_failure.is_some() && !clear_bind_failure_latch {
            previous_health.clone()
        } else {
            net::evaluate(&next.network, self.interface_probe.as_ref())
        };
        let peer_limits_only = configs_differ_only_in_peer_limits(&previous, &next);

        let transition = self.data_plane_transition_lock.lock().await;
        self.reconcile_engine_progress_for_transition().await;
        let live_work = self.live_peer_work_snapshot().await;
        let previous_lifecycle = self.torrent_lifecycle_snapshot().await;
        let next_is_blocked =
            !next_health.traffic_allowed && next_health.mode != NetworkContainmentMode::Disabled;
        if next_is_blocked {
            self.containment_gate
                .block(next_health.status, next_health.detail.clone());
        }
        let registry_hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.stop_all_torrent_tasks(&registry_hashes).await;
        *self.dht_runner.lock().await = None;
        if let Err(error) = self
            .wait_for_peer_permit_configuration_drain(&previous_permits)
            .await
        {
            let rollback = self
                .restore_peer_reconfiguration_while_transition_locked(
                    &previous,
                    &previous_permits,
                    &previous_health,
                    &previous_bind_failure,
                    &previous_lifecycle,
                    &live_work,
                )
                .await;
            drop(transition);
            if rollback.is_ok() {
                self.apply_runtime_config_fields().await;
            }
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return Err(CoreError::Internal(format!(
                "old peer permit drain failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
            )));
        }

        // This is the provisional ownership boundary. The injected failure is
        // deliberately evaluated only after both candidate objects are live.
        self.install_peer_permit_configuration(peer_permits).await;
        *self.config.write().await = next.clone();
        if clear_bind_failure_latch {
            *self.bind_failure_latched.write().await = None;
        }
        *self.network_health.write().await = next_health.clone();
        if next_is_blocked {
            let mut registry = self.registry.lock().await;
            for prior in &live_work.downloads {
                if let Some(torrent) = registry.get_mut(&prior.hash) {
                    torrent.containment_recovery_intent =
                        Some(if prior.state == TorrentState::DownloadingMetadata {
                            ContainmentRecoveryIntent::DownloadingMetadata
                        } else {
                            ContainmentRecoveryIntent::Downloading
                        });
                    torrent.state = TorrentState::NetworkBlocked;
                    torrent.error = Some(next_health.detail.clone());
                }
            }
            for prior in &live_work.seeders {
                if let Some(torrent) = registry.get_mut(&prior.hash) {
                    torrent.containment_recovery_intent = Some(ContainmentRecoveryIntent::Seeding);
                    torrent.state = TorrentState::NetworkBlocked;
                    torrent.error = Some(next_health.detail.clone());
                }
            }
        } else {
            self.containment_gate.allow();
        }
        if self.peer_reconfiguration_failure_injected() {
            let rollback = self
                .restore_peer_reconfiguration_while_transition_locked(
                    &previous,
                    &previous_permits,
                    &previous_health,
                    &previous_bind_failure,
                    &previous_lifecycle,
                    &live_work,
                )
                .await;
            drop(transition);
            let rollback = async {
                rollback?;
                self.apply_runtime_config_fields().await;
                self.verify_peer_permit_configuration_identity(&previous_permits)
                    .await?;
                self.verify_live_peer_work(&live_work).await
            }
            .await;
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return match (rollback, file_rollback) {
                (Ok(()), Ok(())) => Err(CoreError::Internal(
                    "injected peer permit reconstruction failure after provisional install"
                        .into(),
                )),
                (runtime, file) => Err(CoreError::Internal(format!(
                    "injected peer permit reconstruction failure; runtime rollback: {runtime:?}; configuration rollback: {file:?}"
                ))),
            };
        }

        if !next_is_blocked
            && (!previous_health.traffic_allowed
                && previous_health.mode != NetworkContainmentMode::Disabled
                || previous_bind_failure.is_some())
        {
            let mut registry = self.registry.lock().await;
            for torrent in registry.torrents.values_mut() {
                let Some(intent) = torrent.containment_recovery_intent.take() else {
                    continue;
                };
                torrent.error = None;
                match intent {
                    ContainmentRecoveryIntent::Downloading
                    | ContainmentRecoveryIntent::DownloadingMetadata => {
                        torrent.state = TorrentState::Queued;
                        torrent.seeding_status = SeedingStatus::NotEligible;
                    }
                    ContainmentRecoveryIntent::Seeding => {
                        torrent.state = TorrentState::Completed;
                        torrent.seeding_status = SeedingStatus::Queued;
                    }
                }
            }
        }
        self.wait_at_peer_reconfiguration_test_pause().await;

        let reconstruction = if next_is_blocked {
            let has_live_tasks = !self.engine_handles.read().await.is_empty()
                || !self.seeder_registry.is_empty().await;
            if has_live_tasks {
                Err(CoreError::Internal(
                    "peer tasks remained live after blocked configuration install".into(),
                ))
            } else {
                Ok(())
            }
        } else if peer_limits_only {
            match self
                .reconstruct_live_peer_work_while_transition_locked(&live_work)
                .await
            {
                Ok(()) => self.verify_live_peer_work(&live_work).await,
                Err(error) => Err(error),
            }
        } else {
            self.reconstruct_eligible_peer_work_while_transition_locked()
                .await
        };
        if let Err(error) = reconstruction {
            let rollback = self
                .restore_peer_reconfiguration_while_transition_locked(
                    &previous,
                    &previous_permits,
                    &previous_health,
                    &previous_bind_failure,
                    &previous_lifecycle,
                    &live_work,
                )
                .await;
            drop(transition);
            let rollback = async {
                rollback?;
                self.apply_runtime_config_fields().await;
                self.verify_peer_permit_configuration_identity(&previous_permits)
                    .await?;
                self.verify_live_peer_work(&live_work).await
            }
            .await;
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return Err(CoreError::Internal(format!(
                "peer permit reconstruction failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
            )));
        }
        drop(transition);
        self.apply_runtime_config_fields_reversible().await;
        let post_reconcile_verification = if next_is_blocked {
            if !self.engine_handles.read().await.is_empty()
                || !self.seeder_registry.is_empty().await
            {
                Err(CoreError::Internal(
                    "peer tasks started after blocked configuration reconstruction".into(),
                ))
            } else {
                Ok(())
            }
        } else if peer_limits_only {
            self.verify_live_peer_work(&live_work).await
        } else {
            self.verify_eligible_peer_work().await
        };
        if let Err(error) = post_reconcile_verification {
            let rollback = self
                .restore_peer_reconfiguration(
                    &previous,
                    &previous_permits,
                    &previous_health,
                    &previous_bind_failure,
                    &previous_lifecycle,
                    &live_work,
                )
                .await;
            let file_rollback = match (persist_path, file_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => restore_config_file(path, snapshot),
                _ => Ok(()),
            };
            return Err(CoreError::Internal(format!(
                "peer permit post-reconcile verification failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
            )));
        }

        self.wait_at_peer_reconfiguration_persistence_test_pause()
            .await;
        if let Some(path) = persist_path {
            let persisted = if self.peer_reconfiguration_persistence_failure_injected() {
                Err(CoreError::Internal(
                    "injected peer permit configuration persistence failure".into(),
                ))
            } else {
                write_config_atomically(path, &next)
            };
            if let Err(error) = persisted {
                let rollback = self
                    .restore_peer_reconfiguration(
                        &previous,
                        &previous_permits,
                        &previous_health,
                        &previous_bind_failure,
                        &previous_lifecycle,
                        &live_work,
                    )
                    .await;
                let file_rollback = file_snapshot
                    .as_ref()
                    .map_or(Ok(()), |snapshot| restore_config_file(path, snapshot));
                return Err(CoreError::Internal(format!(
                    "peer permit configuration persistence failed: {error}; runtime rollback: {rollback:?}; configuration rollback: {file_rollback:?}"
                )));
            }
        }

        self.selfish_completion_enabled
            .store(next.torrent.selfish, Ordering::Release);
        // The candidate is now committed in memory and, for full PUT, on
        // disk. Irreversible policy effects must never run before this point.
        self.sweep_selfish_completed_torrents_best_effort("runtime_config_commit")
            .await;
        debug_assert_eq!(
            self.config.read().await.bandwidth.max_peers,
            self.peer_permit_snapshot().await.limit
        );
        Ok(())
    }

    async fn add_config_file_check(&self, checks: &mut Vec<DoctorCheck>) {
        let Some(path) = &self.config_path else {
            push_check(
                checks,
                "config_file",
                "Config file",
                DiagnosticLevel::Warning,
                "daemon was started without a config file, so full settings cannot be persisted",
                Some("start swarmotterd with --config to enable config.toml writes"),
            );
            return;
        };
        let level = if path.is_file() {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Warning
        };
        push_check(
            checks,
            "config_file",
            "Config file",
            level,
            format!("configured path: {}", path.display()),
            Some("create the config file or verify the daemon has write permissions"),
        );
    }

    async fn add_log_file_check(&self, checks: &mut Vec<DoctorCheck>) {
        let Some(path) = &self.log_file_path else {
            push_check(
                checks,
                "log_file",
                "Log file",
                DiagnosticLevel::Warning,
                "file logging is disabled; the Logs page can only show live events",
                Some("enable logging.file or configure logging.file_path"),
            );
            return;
        };
        let level = if path.is_file() {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Warning
        };
        push_check(
            checks,
            "log_file",
            "Log file",
            level,
            format!("log path: {}", path.display()),
            Some("verify the daemon can create and read the log file"),
        );
    }

    async fn add_storage_checks(&self, cfg: &Config, checks: &mut Vec<DoctorCheck>) {
        add_storage_check(
            checks,
            "download_dir",
            "Download directory",
            cfg.storage.download_dir.as_deref(),
        );
        add_storage_check(
            checks,
            "incomplete_dir",
            "Incomplete directory",
            cfg.storage.incomplete_dir.as_deref(),
        );
    }

    async fn add_watch_checks(&self, cfg: &Config, checks: &mut Vec<DoctorCheck>) {
        if cfg.watch.is_empty() {
            push_check(
                checks,
                "watch_folders",
                "Watch folders",
                DiagnosticLevel::Warning,
                "no watch folders are configured",
                Some("add [[watch]] entries if automatic .torrent import is desired"),
            );
            return;
        }
        let missing = cfg
            .watch
            .iter()
            .filter(|folder| !Path::new(&folder.path).is_dir())
            .count();
        push_check(
            checks,
            "watch_folders",
            "Watch folders",
            if missing == 0 {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Warning
            },
            format!(
                "{} configured, {} missing or unreadable",
                cfg.watch.len(),
                missing
            ),
            Some("verify watch folder paths and permissions"),
        );
    }

    async fn add_torrent_runtime_check(&self, checks: &mut Vec<DoctorCheck>) {
        let reg = self.registry.lock().await;
        let errors = reg
            .torrents
            .values()
            .filter(|torrent| torrent.error.is_some())
            .count();
        push_check(
            checks,
            "torrent_runtime",
            "Torrent runtime",
            if errors == 0 {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Warning
            },
            format!(
                "{} torrents loaded, {} with errors",
                reg.torrents.len(),
                errors
            ),
            Some("open torrent details or logs for the affected torrents"),
        );
    }
}

fn is_permanent_watch_error(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::Bencode(_)
            | CoreError::MalformedTorrent(_)
            | CoreError::InvalidInfoHash(_)
            | CoreError::Parse(_)
    )
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn redact_config(mut cfg: Config) -> Config {
    cfg.api.auth_token = None;
    cfg
}

fn capture_config_file(path: &Path) -> Result<ConfigFileSnapshot> {
    match fs::read(path) {
        Ok(bytes) => Ok(ConfigFileSnapshot::Bytes(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(ConfigFileSnapshot::Missing)
        }
        Err(error) => Err(CoreError::from(error)),
    }
}

fn restore_config_file(path: &Path, snapshot: &ConfigFileSnapshot) -> Result<()> {
    match snapshot {
        ConfigFileSnapshot::Bytes(bytes) => write_config_bytes_atomically(path, bytes),
        ConfigFileSnapshot::Missing => match fs::remove_file(path) {
            Ok(()) => {
                let parent = path.parent().unwrap_or_else(|| Path::new("."));
                fs::File::open(parent)
                    .and_then(|directory| directory.sync_all())
                    .map_err(CoreError::from)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CoreError::from(error)),
        },
    }
}

fn write_config_atomically(path: &Path, config: &Config) -> Result<()> {
    let toml = config.to_toml_string()?;
    write_config_bytes_atomically(path, toml.as_bytes())
}

fn write_config_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(CoreError::from)?;
    let sequence = CONFIG_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("swarmotter.toml");
    let tmp = path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), sequence));
    let result = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp).map_err(CoreError::from)?;
        file.write_all(bytes).map_err(CoreError::from)?;
        file.sync_all().map_err(CoreError::from)?;
        drop(file);
        fs::rename(&tmp, path).map_err(CoreError::from)?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(CoreError::from)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn restart_required_fields(previous: &Config, next: &Config) -> Vec<String> {
    let mut fields = Vec::new();
    if previous.api.bind_address != next.api.bind_address {
        fields.push("api.bind_address".into());
    }
    if previous.api.max_request_body_bytes != next.api.max_request_body_bytes {
        fields.push("api.max_request_body_bytes".into());
    }
    if previous.logging.level != next.logging.level {
        fields.push("logging.level".into());
    }
    if previous.logging.json != next.logging.json {
        fields.push("logging.json".into());
    }
    if previous.logging.file != next.logging.file {
        fields.push("logging.file".into());
    }
    if previous.logging.file_path != next.logging.file_path {
        fields.push("logging.file_path".into());
    }
    fields
}

fn data_plane_config_changed(previous: &Config, next: &Config) -> bool {
    previous.network != next.network
        || previous.torrent.listen_port != next.torrent.listen_port
        || previous.torrent.allow_ipv6 != next.torrent.allow_ipv6
        || previous.torrent.utp_enabled != next.torrent.utp_enabled
        || previous.torrent.utp_prefer_tcp != next.torrent.utp_prefer_tcp
        || previous.torrent.encryption_mode != next.torrent.encryption_mode
        || previous.dht != next.dht
        || previous.pex.enabled != next.pex.enabled
        || previous.pex.max_peers != next.pex.max_peers
        || peer_limits_changed(previous, next)
        || previous.storage.download_dir != next.storage.download_dir
        || previous.storage.incomplete_dir != next.storage.incomplete_dir
        || previous.storage.minimum_free_space_bytes != next.storage.minimum_free_space_bytes
        || previous.storage.minimum_free_space_percent != next.storage.minimum_free_space_percent
        || previous.storage.preallocate != next.storage.preallocate
        || previous.storage.sparse != next.storage.sparse
}

fn peer_limits_changed(previous: &Config, next: &Config) -> bool {
    previous.bandwidth.max_peers != next.bandwidth.max_peers
        || previous.bandwidth.max_peers_per_torrent != next.bandwidth.max_peers_per_torrent
}

fn configs_differ_only_in_peer_limits(previous: &Config, next: &Config) -> bool {
    if !peer_limits_changed(previous, next) {
        return false;
    }
    let mut normalized = next.clone();
    normalized.bandwidth.max_peers = previous.bandwidth.max_peers;
    normalized.bandwidth.max_peers_per_torrent = previous.bandwidth.max_peers_per_torrent;
    match (previous.to_toml_string(), normalized.to_toml_string()) {
        (Ok(previous), Ok(normalized)) => previous == normalized,
        _ => false,
    }
}

fn validate_storage_config_transition(
    previous: &Config,
    next: &Config,
    torrents: &[Torrent],
) -> Result<()> {
    if previous.storage.download_dir != next.storage.download_dir
        && torrents
            .iter()
            .any(|torrent| torrent.download_dir.is_none())
    {
        return Err(CoreError::InvalidConfig(
            "storage.download_dir cannot change while torrents still use the global download directory; move those torrents to explicit locations first"
                .into(),
        ));
    }
    if previous.storage.incomplete_dir != next.storage.incomplete_dir
        && torrents
            .iter()
            .any(|torrent| !torrent.progress.is_complete())
    {
        return Err(CoreError::InvalidConfig(
            "storage.incomplete_dir cannot change while torrents have incomplete payloads".into(),
        ));
    }
    validate_restored_storage_ownership(torrents.iter(), next)
}

fn push_check(
    checks: &mut Vec<DoctorCheck>,
    id: impl Into<String>,
    label: impl Into<String>,
    level: DiagnosticLevel,
    detail: impl Into<String>,
    remediation: Option<&str>,
) {
    checks.push(DoctorCheck {
        id: id.into(),
        label: label.into(),
        level,
        detail: detail.into(),
        remediation: remediation.map(str::to_string),
    });
}

fn containment_matrix(config: &Config, level: DiagnosticLevel) -> Vec<NetworkPathCheck> {
    let mut rows = vec![
        (
            "peer_tcp",
            "Peer TCP",
            "outbound peer TCP uses the contained NetworkBinder",
        ),
        (
            "peer_utp",
            "Peer uTP",
            "uTP uses contained UDP sockets with TCP fallback policy",
        ),
        (
            "dht_udp",
            "DHT UDP",
            "DHT packets use the same contained UDP socket layer",
        ),
        (
            "udp_tracker",
            "UDP trackers",
            "UDP tracker announces use contained UDP sockets",
        ),
        (
            "http_tracker",
            "HTTP(S) trackers",
            "tracker HTTP/TLS is performed over contained sockets",
        ),
        (
            "webseed",
            "Web seeds",
            "webseed range requests use contained HTTP/TLS sockets",
        ),
        (
            "dns",
            "DNS resolution",
            "hostname resolution is validated or blocked by containment policy",
        ),
    ];
    if !config.torrent.utp_enabled {
        rows.retain(|(id, _, _)| *id != "peer_utp");
    }
    rows.into_iter()
        .map(|(id, label, detail)| NetworkPathCheck {
            id: id.into(),
            label: label.into(),
            level,
            detail: detail.into(),
        })
        .collect()
}

fn add_storage_check(
    checks: &mut Vec<DoctorCheck>,
    id: &'static str,
    label: &'static str,
    path: Option<&str>,
) {
    let Some(path) = path else {
        push_check(
            checks,
            id,
            label,
            DiagnosticLevel::Warning,
            "not configured; daemon will use its default temporary directory behavior",
            Some("set an explicit storage path for predictable operations"),
        );
        return;
    };
    let path = Path::new(path);
    let existing = path.exists();
    let disk = free_space_bytes(path).or_else(|| path.parent().and_then(free_space_bytes));
    let level = match disk {
        Some(bytes) if bytes < 1024 * 1024 * 1024 => DiagnosticLevel::Invalid,
        Some(bytes) if bytes < 10 * 1024 * 1024 * 1024 => DiagnosticLevel::Warning,
        Some(_) if existing || path.parent().map(Path::exists).unwrap_or(false) => {
            DiagnosticLevel::Ok
        }
        Some(_) => DiagnosticLevel::Warning,
        None => DiagnosticLevel::Warning,
    };
    let detail = match disk {
        Some(bytes) => format!("{} available at {}", format_bytes(bytes), path.display()),
        None => format!("unable to inspect free space at {}", path.display()),
    };
    push_check(
        checks,
        id,
        label,
        level,
        detail,
        Some("ensure the path exists, is writable, and has enough free space"),
    );
}

#[cfg(unix)]
fn free_space_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    Some(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(not(unix))]
fn free_space_bytes(_path: &Path) -> Option<u64> {
    None
}

fn format_bytes(bytes: u64) -> String {
    let mut value = bytes as f64;
    let mut unit = "B";
    for next in ["KB", "MB", "GB", "TB"] {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next;
    }
    if unit == "B" {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {unit}")
    }
}

fn read_last_lines(path: &Path, max_lines: usize) -> std::io::Result<Vec<String>> {
    if max_lines == 0 {
        return Ok(Vec::new());
    }
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = Vec::new();
    for line in reader.lines() {
        lines.push(strip_ansi_controls(&line?));
        if lines.len() > max_lines {
            lines.remove(0);
        }
    }
    Ok(lines)
}

fn is_retryable_magnet_metadata_discovery_error(error: &CoreError) -> bool {
    let CoreError::Internal(message) = error else {
        return false;
    };
    message.contains("magnet metadata fetch failed after discovery retries")
        && message.contains("magnet metadata fetch: no peers discovered")
}

fn strip_ansi_controls(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
                continue;
            }
            continue;
        }
        out.push(ch);
    }
    out
}

/// Generate a process-unique peer id with the SwarmOtter client prefix.
fn make_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-SW0001-");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x5a17_cafe);
    let mut x = nanos ^ ((std::process::id() as u64) << 32);
    for byte in &mut id[8..] {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *byte = (x & 0xff) as u8;
    }
    id
}

fn apply_resolved_metadata(
    t: &mut Torrent,
    real: &swarmotter_core::meta::TorrentMeta,
    state: &EngineState,
) {
    let initialize_files = t.needs_metadata || t.meta.files.len() != real.files.len();
    t.meta = real.clone();
    t.needs_metadata = false;
    t.magnet_info_hash = None;
    t.progress
        .replace_from_bitfield(&state.pieces_have, real.piece_count());
    if initialize_files {
        t.files = real
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| TorrentFile {
                index: i,
                path: f.path.join("/"),
                length: f.length,
                bytes_completed: 0,
                priority: FilePriority::Normal,
                wanted: true,
            })
            .collect();
        t.priorities = vec![FilePriority::Normal; real.files.len()];
        t.wanted = vec![true; real.files.len()];
    }
    t.recompute_file_bytes_completed();
    if !t.progress.is_complete() {
        t.seeding_status = SeedingStatus::NotEligible;
    }
}

fn automatic_seeding_status(
    torrent: &Torrent,
    global: &swarmotter_core::ratio::SeedingPolicy,
    idle_seconds: u64,
) -> SeedingStatus {
    let accounting = TorrentAccounting {
        downloaded: torrent.downloaded,
        uploaded: torrent.uploaded,
        idle_seconds,
    };
    match ratio::evaluate_seeding(&accounting, global, &torrent.seeding) {
        SeedDecision::Continue => SeedingStatus::Queued,
        SeedDecision::StopOnRatio => SeedingStatus::StoppedRatio,
        SeedDecision::StopOnIdle => SeedingStatus::StoppedIdle,
    }
}

/// Normalize legacy/defaulted seeding fields before restored work is
/// scheduled. A blocked record retains what its pre-block lifecycle implied;
/// all other complete records are re-evaluated against effective targets.
fn recompute_restored_seeding_lifecycle(
    torrent: &mut Torrent,
    persisted_state: TorrentState,
    global: &swarmotter_core::ratio::SeedingPolicy,
    now_secs: u64,
) {
    if !torrent.progress.is_complete() {
        torrent.seeding_status = SeedingStatus::NotEligible;
        return;
    }

    if torrent.state == TorrentState::NetworkBlocked {
        torrent.seeding_status = match persisted_state {
            TorrentState::Seeding => SeedingStatus::Active,
            TorrentState::Paused => SeedingStatus::StoppedManual,
            _ if torrent.seeding_status != SeedingStatus::NotEligible => torrent.seeding_status,
            _ => automatic_seeding_status(
                torrent,
                global,
                now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added)),
            ),
        };
        return;
    }

    if torrent.state == TorrentState::Paused {
        torrent.seeding_status = SeedingStatus::StoppedManual;
        return;
    }

    if matches!(
        torrent.state,
        TorrentState::Completed | TorrentState::Seeding
    ) {
        torrent.state = TorrentState::Completed;
        torrent.seeding_status = automatic_seeding_status(
            torrent,
            global,
            now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added)),
        );
    } else {
        torrent.seeding_status = SeedingStatus::NotEligible;
    }
}

#[async_trait]
impl DaemonOps for DaemonRuntime {
    async fn list_torrents(&self) -> Vec<TorrentSummary> {
        let global_seeding = self.config.read().await.seeding.clone();
        let positions: HashMap<InfoHash, usize> = self
            .queue
            .lock()
            .await
            .order
            .iter()
            .enumerate()
            .map(|(i, hash)| (*hash, i + 1))
            .collect();
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        self.registry
            .lock()
            .await
            .list()
            .iter()
            .map(|t| {
                let mut summary = t.to_summary();
                summary.queue_position = positions.get(&t.info_hash()).copied();
                summary.effective_ratio_limit = t.seeding.effective_ratio_limit(&global_seeding);
                summary.effective_idle_limit = t.seeding.effective_idle_limit(&global_seeding);
                summary
            })
            .collect()
    }

    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        let global_seeding = self.config.read().await.seeding.clone();
        let position = self.queue.lock().await.position(hash);
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        self.registry.lock().await.get(hash).map(|t| {
            let mut summary = t.to_summary();
            summary.queue_position = position;
            summary.effective_ratio_limit = t.seeding.effective_ratio_limit(&global_seeding);
            summary.effective_idle_limit = t.seeding.effective_idle_limit(&global_seeding);
            summary
        })
    }

    async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        options: AddTorrentOptions,
    ) -> Result<InfoHash> {
        self.add_torrent_file_with_options(bytes, options).await
    }

    async fn add_magnet(&self, magnet: &str, options: AddTorrentOptions) -> Result<InfoHash> {
        self.add_magnet_with_options(magnet, options).await
    }

    async fn remove_torrent(&self, hash: &InfoHash, delete_data: bool) -> Result<()> {
        let removed = self
            .remove_torrents_with_single_reconcile(vec![*hash], delete_data)
            .await?;
        if removed.is_empty() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        Ok(())
    }

    async fn remove_torrents(
        &self,
        hashes: Vec<InfoHash>,
        delete_data: bool,
    ) -> Result<Vec<InfoHash>> {
        self.remove_torrents_with_single_reconcile(hashes, delete_data)
            .await
    }

    async fn pause(&self, hash: &InfoHash) -> Result<()> {
        // Stop the live engine; the torrent stays in the registry as paused.
        self.stop_engine(hash).await;
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.containment_recovery_intent = None;
                    t.state = TorrentState::Paused;
                    t.seeding_status = if t.progress.is_complete() {
                        SeedingStatus::StoppedManual
                    } else {
                        SeedingStatus::NotEligible
                    };
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        self.queue.lock().await.clear_bypass(hash);
        self.reconcile_queue().await;
        self.persist_state().await?;
        self.publish_torrent_event("torrent_changed", *hash, TorrentState::Paused);
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn resume(&self, hash: &InfoHash) -> Result<()> {
        self.engine_retry_after.write().await.remove(hash);
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.containment_recovery_intent = None;
                    if t.progress.is_complete() {
                        t.state = TorrentState::Completed;
                        t.seeding_status = SeedingStatus::Queued;
                    } else {
                        t.state = TorrentState::Queued;
                        t.seeding_status = SeedingStatus::NotEligible;
                    }
                    t.error = None;
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(*hash);
            queue.start_now(hash);
        }
        self.reconcile_queue().await;
        self.reconcile_seeders().await;
        self.persist_state().await?;
        let state = self
            .registry
            .lock()
            .await
            .get(hash)
            .map(|torrent| torrent.state)
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        self.publish_torrent_event("torrent_changed", *hash, state);
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn start_now(&self, hash: &InfoHash) -> Result<()> {
        let manually_stopped_complete =
            self.registry.lock().await.get(hash).is_some_and(|torrent| {
                torrent.progress.is_complete()
                    && (torrent.state == TorrentState::Paused
                        || torrent.seeding_status == SeedingStatus::StoppedManual)
            });
        if manually_stopped_complete {
            return self.resume(hash).await;
        }
        self.engine_retry_after.write().await.remove(hash);
        {
            let mut reg = self.registry.lock().await;
            if let Some(torrent) = reg.get_mut(hash) {
                torrent.containment_recovery_intent = None;
            } else {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(*hash);
            queue.start_now(hash);
        }
        self.reconcile_queue().await;
        self.persist_state().await?;
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn stop(&self, hash: &InfoHash) -> Result<()> {
        self.pause(hash).await
    }

    async fn recheck(&self, hash: &InfoHash) -> Result<()> {
        let was_completed = self
            .registry
            .lock()
            .await
            .get(hash)
            .map(|torrent| torrent.progress.is_complete())
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        self.stop_engine(hash).await;
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.containment_recovery_intent = None;
                    t.state = TorrentState::Checking;
                    t.seeding_status = SeedingStatus::NotEligible;
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        self.publish_torrent_event("torrent_changed", *hash, TorrentState::Checking);
        self.publish_event(stats_updated_event());
        // Run a real storage recheck on disk.
        let (meta, storage_dir) = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            let complete_dir = self.resolve_download_dir(t).await;
            let storage_dir = if was_completed {
                complete_dir
            } else {
                self.resolve_incomplete_dir(&complete_dir).await
            };
            (t.meta.clone(), storage_dir)
        };
        let storage = swarmotter_core::storage::StorageIo::new(
            meta.clone(),
            std::path::PathBuf::from(&storage_dir),
        );
        match storage.recheck().await {
            Ok(bf) => {
                let mut final_state = None;
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.progress.replace_from_bitfield(&bf, meta.piece_count());
                    t.recompute_file_bytes_completed();
                    if torrent_selection_complete(t, &bf)? {
                        t.state = TorrentState::Completed;
                        t.seeding_status = if t.progress.is_complete() {
                            SeedingStatus::Queued
                        } else {
                            SeedingStatus::NotEligible
                        };
                        t.date_completed = Some(now());
                        final_state = Some(TorrentState::Completed);
                    } else if t.state == TorrentState::Checking {
                        t.state = TorrentState::Paused;
                        t.seeding_status = SeedingStatus::NotEligible;
                        final_state = Some(TorrentState::Paused);
                    }
                }
                drop(reg);
                if let Some(state) = final_state {
                    self.publish_torrent_event("torrent_changed", *hash, state);
                    if state == TorrentState::Completed {
                        self.publish_torrent_event("torrent_completed", *hash, state);
                    }
                    self.publish_event(stats_updated_event());
                }
                self.persist_state().await?;
                self.reconcile_seeders().await;
            }
            Err(e) => {
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.state = TorrentState::StorageError;
                    t.error = Some(e.to_string());
                }
                drop(reg);
                self.publish_torrent_event("torrent_error", *hash, TorrentState::StorageError);
                self.publish_event(stats_updated_event());
                self.persist_state_best_effort("recheck_failed").await;
                return Err(e);
            }
        }
        Ok(())
    }

    async fn reannounce(&self, hash: &InfoHash) -> Result<()> {
        // If the engine is running, send a reannounce command; otherwise
        // restart the engine which announces on start.
        if let Some(tx) = self.engine_cmds.lock().await.get(hash) {
            let _ = tx.send(EngineCommand::Reannounce).await;
            Ok(())
        } else {
            self.resume(hash).await
        }
    }

    async fn move_data(&self, hash: &InfoHash, path: String) -> Result<()> {
        if path.trim().is_empty() {
            return Err(CoreError::Storage(
                "torrent data destination must not be empty".into(),
            ));
        }
        let storage_ownership = self.storage_ownership_lock.lock().await;
        let torrent = self
            .registry
            .lock()
            .await
            .get(hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        self.ensure_storage_paths_available_except(&torrent.meta, Some(&path), Some(*hash))
            .await?;
        let was_active = matches!(
            torrent.state,
            TorrentState::Downloading | TorrentState::DownloadingMetadata
        );
        let state_completed = matches!(
            torrent.state,
            TorrentState::Completed | TorrentState::Seeding
        );
        let payload_in_complete = torrent.progress.is_complete();
        self.stop_engine(hash).await;
        let cfg = self.config.read().await.clone();
        let old_complete = resolve_download_dir_from_config(torrent.download_dir.as_deref(), &cfg);
        let source = if payload_in_complete {
            old_complete
        } else {
            resolve_incomplete_dir_from_config(&old_complete, &cfg)
        };
        let destination = if payload_in_complete {
            path.clone()
        } else {
            resolve_incomplete_dir_from_config(&path, &cfg)
        };
        let source_path = PathBuf::from(source);
        let storage =
            swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), source_path.clone());
        let moved_storage = match storage.move_to(PathBuf::from(destination)).await {
            Ok(storage) => storage,
            Err(error) => {
                drop(storage_ownership);
                if was_active {
                    self.restart_engine_for_settings(hash).await;
                } else if state_completed {
                    self.reconcile_seeders().await;
                }
                return Err(error);
            }
        };
        if let Some(current) = self.registry.lock().await.get_mut(hash) {
            current.download_dir = Some(path);
        }
        let persist_result = self.persist_state().await;
        let result = if let Err(persist_error) = persist_result {
            match moved_storage.move_to(source_path).await {
                Ok(_) => {
                    if let Some(current) = self.registry.lock().await.get_mut(hash) {
                        current.download_dir = torrent.download_dir.clone();
                    }
                    Err(persist_error)
                }
                Err(rollback_error) => Err(CoreError::Storage(format!(
                    "{persist_error}; data move rollback also failed: {rollback_error}"
                ))),
            }
        } else {
            Ok(())
        };
        drop(storage_ownership);
        if was_active {
            self.restart_engine_for_settings(hash).await;
        } else if state_completed {
            self.reconcile_seeders().await;
        }
        result
    }

    async fn rename_path(
        &self,
        hash: &InfoHash,
        file_index: usize,
        new_path: String,
    ) -> Result<()> {
        let components = validated_relative_path(&new_path)?;
        let storage_ownership = self.storage_ownership_lock.lock().await;
        let torrent = self
            .registry
            .lock()
            .await
            .get(hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        if file_index >= torrent.meta.files.len() {
            return Err(CoreError::NotFound("torrent file".into()));
        }
        let mut renamed_meta = torrent.meta.clone();
        renamed_meta.files[file_index].path = components;
        self.ensure_storage_paths_available_except(
            &renamed_meta,
            torrent.download_dir.as_deref(),
            Some(*hash),
        )
        .await?;
        let was_active = matches!(
            torrent.state,
            TorrentState::Downloading | TorrentState::DownloadingMetadata
        );
        let state_completed = matches!(
            torrent.state,
            TorrentState::Completed | TorrentState::Seeding
        );
        let payload_in_complete = torrent.progress.is_complete();
        self.stop_engine(hash).await;
        let complete_dir = self.resolve_download_dir(&torrent).await;
        let storage_dir = if payload_in_complete {
            complete_dir
        } else {
            self.resolve_incomplete_dir(&complete_dir).await
        };
        let old_storage = swarmotter_core::storage::StorageIo::new(
            torrent.meta.clone(),
            PathBuf::from(&storage_dir),
        );
        let old_path = old_storage.file_path(file_index)?;
        let new_storage = swarmotter_core::storage::StorageIo::new(
            renamed_meta.clone(),
            PathBuf::from(storage_dir),
        );
        let new_file_path = new_storage.file_path(file_index)?;
        if old_path == new_file_path {
            drop(storage_ownership);
            if was_active {
                self.restart_engine_for_settings(hash).await;
            } else if state_completed {
                self.reconcile_seeders().await;
            }
            return Ok(());
        }
        let disk_outcome = match rename_payload_exclusive(&old_path, &new_file_path).await {
            Ok(outcome) => outcome,
            Err(error) => {
                drop(storage_ownership);
                if was_active {
                    self.restart_engine_for_settings(hash).await;
                } else if state_completed {
                    self.reconcile_seeders().await;
                }
                return Err(error);
            }
        };
        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
            torrent.meta = renamed_meta;
            torrent.files[file_index].path = new_path;
        }
        let result = if let Err(persist_error) = self.persist_state().await {
            match rollback_payload_rename(&old_path, &new_file_path, disk_outcome).await {
                Ok(()) => {
                    if let Some(current) = self.registry.lock().await.get_mut(hash) {
                        *current = torrent;
                    }
                    Err(persist_error)
                }
                Err(rollback_error) => Err(CoreError::Storage(format!(
                    "{persist_error}; payload rename rollback also failed: {rollback_error}"
                ))),
            }
        } else {
            Ok(())
        };
        drop(storage_ownership);
        if was_active {
            self.restart_engine_for_settings(hash).await;
        } else if state_completed {
            self.reconcile_seeders().await;
        }
        result
    }

    async fn set_labels(&self, hash: &InfoHash, labels: Vec<String>) -> Result<()> {
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                t.labels = labels;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
    }

    async fn set_torrent_limits(
        &self,
        hash: &InfoHash,
        limits: swarmotter_core::bandwidth::TorrentBandwidth,
    ) -> Result<()> {
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.download_limit = limits.download;
                    t.upload_limit = limits.upload;
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        // Apply live through the one retained Arc shared by the downloader and
        // active/queued seeder registration. No task restart is required.
        if let Some(rl) = self.torrent_limiters.read().await.get(hash).cloned() {
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Download,
                limits.download,
            );
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Upload,
                limits.upload,
            );
        }
        self.persist_state().await
    }

    async fn set_torrent_seeding(
        &self,
        hash: &InfoHash,
        seeding: swarmotter_core::ratio::TorrentSeeding,
    ) -> Result<TorrentSummary> {
        if seeding
            .ratio_limit
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(CoreError::InvalidArgument(
                "ratio_limit must be a finite non-negative number or null".into(),
            ));
        }

        // Keep tentative policy invisible to lifecycle reconciliation and API
        // readers until durable replacement succeeds or the prior value is
        // restored. Reconciliation reacquires this lock only after success.
        let lifecycle = self.seeder_lifecycle_lock.lock().await;
        let previous = {
            let mut reg = self.registry.lock().await;
            let torrent = reg
                .get_mut(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.seeding.clone();
            // Persist policy independently of runtime lifecycle. A live
            // registry entry remains Seeding+Active until synchronized
            // reconciliation stops it after the durable write succeeds.
            torrent.seeding = seeding;
            previous
        };

        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                torrent.seeding = previous;
            }
            return Err(error);
        }

        drop(lifecycle);
        self.reconcile_seeders().await;
        self.get_torrent(hash)
            .await
            .ok_or_else(|| CoreError::NotFound("torrent".into()))
    }

    async fn list_files(&self, hash: &InfoHash) -> Option<Vec<TorrentFile>> {
        self.registry
            .lock()
            .await
            .get(hash)
            .map(|t| t.files.clone())
    }

    async fn set_wanted(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        wanted: bool,
    ) -> Result<()> {
        let should_restart = {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            if file_indices.iter().any(|index| *index >= t.wanted.len()) {
                return Err(CoreError::NotFound("torrent file".into()));
            }
            for i in file_indices {
                t.wanted[i] = wanted;
                t.files[i].wanted = wanted;
            }
            matches!(
                t.state,
                TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
                    | TorrentState::Completed
            )
        };
        self.persist_state().await?;
        if should_restart {
            self.restart_engine_for_settings(hash).await;
        }
        Ok(())
    }

    async fn set_priority(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        priority: FilePriority,
    ) -> Result<()> {
        let should_restart = {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            if file_indices
                .iter()
                .any(|index| *index >= t.priorities.len())
            {
                return Err(CoreError::NotFound("torrent file".into()));
            }
            for i in file_indices {
                t.priorities[i] = priority;
                t.files[i].priority = priority;
            }
            matches!(
                t.state,
                TorrentState::Downloading
                    | TorrentState::DownloadingMetadata
                    | TorrentState::Completed
            )
        };
        self.persist_state().await?;
        if should_restart {
            self.restart_engine_for_settings(hash).await;
        }
        Ok(())
    }

    async fn list_trackers(&self, hash: &InfoHash) -> Option<Vec<TrackerInfo>> {
        // Reflect real per-tracker announce results from the live engine, if
        // present. Success text is kept separate from last_error so the UI and
        // Transmission emulation do not report successful announces as errors.
        let (engine_trackers, tracker_interval_seconds) = self
            .engine_states
            .read()
            .await
            .get(hash)
            .and_then(|s| s.try_lock().ok())
            .map(|s| (s.tracker_announces.clone(), s.tracker_interval_seconds))
            .unwrap_or_default();
        self.registry.lock().await.get(hash).map(|t| {
            let mut out = Vec::new();
            let tiers = tracker::announce_tiers(t.meta.announce.as_deref(), &t.meta.announce_list);
            for (tier, urls) in tiers.iter().enumerate() {
                for url in urls {
                    let mut info = make_tracker(url, tier);
                    if let Some(snapshot) = engine_trackers.get(url) {
                        info.status = snapshot.status;
                        info.seeders = snapshot.seeders;
                        info.leechers = snapshot.leechers;
                        info.downloads = snapshot.downloads;
                        info.last_error = snapshot.last_error.clone();
                        info.last_message = snapshot.last_message.clone();
                        info.last_announce = snapshot.last_announce;
                        info.next_announce = snapshot
                            .last_announce
                            .map(|last| last.saturating_add(tracker_interval_seconds.max(30)));
                    }
                    out.push(info);
                }
            }
            out
        })
    }

    async fn add_tracker(&self, hash: &InfoHash, url: String) -> Result<()> {
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.is_none() {
                    t.meta.announce = Some(url);
                } else {
                    t.meta.announce_list.push(vec![url]);
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
    }

    async fn remove_tracker(&self, hash: &InfoHash, url: String) -> Result<()> {
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.as_deref() == Some(&url) {
                    t.meta.announce = None;
                }
                t.meta.announce_list.retain_mut(|tier| {
                    tier.retain(|u| u != &url);
                    !tier.is_empty()
                });
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
    }

    async fn edit_tracker(&self, hash: &InfoHash, old_url: String, new_url: String) -> Result<()> {
        let result = match self.registry.lock().await.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.as_deref() == Some(&old_url) {
                    t.meta.announce = Some(new_url);
                } else {
                    for tier in t.meta.announce_list.iter_mut() {
                        for u in tier.iter_mut() {
                            if *u == old_url {
                                *u = new_url.clone();
                            }
                        }
                    }
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        };
        result?;
        self.persist_state().await
    }

    async fn list_peers(&self, hash: &InfoHash) -> Option<Vec<Peer>> {
        let states = self.engine_states.read().await;
        let state = states.get(hash)?;
        let s = state.lock().await;
        let peers = s
            .peers
            .iter()
            .map(|pa| Peer {
                address: pa.socket_addr().to_string(),
                ip: pa.ip,
                port: pa.port,
                direction: swarmotter_core::models::peer::PeerDirection::Outbound,
                client: None,
                progress: 0.0,
                rate_down: 0,
                rate_up: 0,
                flags: swarmotter_core::models::peer::PeerFlags::default(),
                banned: false,
            })
            .collect();
        Some(peers)
    }

    async fn queue_move_up(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_up(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }
    async fn queue_move_down(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_down(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }
    async fn queue_move_to_top(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_to_top(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }
    async fn queue_move_to_bottom(&self, hash: &InfoHash) -> Result<()> {
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        self.queue.lock().await.move_to_bottom(hash);
        self.reconcile_queue().await;
        self.persist_state().await
    }

    async fn get_config(&self) -> Config {
        self.config.read().await.clone()
    }

    async fn update_settings(&self, patch: swarmotter_api::state::SettingsPatch) -> Result<()> {
        let _config_transaction = self.config_write_lock.lock().await;
        let previous = self.config.read().await.clone();
        let mut next = previous.clone();
        if let Some(bandwidth) = patch.bandwidth {
            next.bandwidth = bandwidth;
        }
        if let Some(queue) = patch.queue {
            next.queue = queue;
        }
        if let Some(seeding) = patch.seeding {
            next.seeding = seeding;
        }
        if let Some(autopilot) = patch.autopilot {
            next.autopilot = autopilot;
        }
        next.validate()?;

        if peer_limits_changed(&previous, &next) {
            let peer_permits = self.build_peer_permit_configuration(&next).await?;
            self.apply_peer_budget_runtime_update(next, peer_permits, None, false)
                .await?;
        } else {
            *self.config.write().await = next;
            self.apply_runtime_config_fields().await;
        }
        self.publish_event(Event::new("settings_changed", json!({})));
        self.publish_event(stats_updated_event());
        Ok(())
    }

    async fn replace_config(&self, mut next: Config) -> Result<ConfigUpdateResult> {
        let _config_transaction = self.config_write_lock.lock().await;
        // A binder blocks the gate synchronously and queues teardown details.
        // Drain that report before replacement validation so stale listeners
        // cannot make an otherwise-correct explicit recovery look occupied.
        if !self.containment_gate.traffic_allowed() {
            self.network_health_tick().await;
        }
        let (previous, config_path) = {
            let cfg = self.config.read().await;
            (cfg.clone(), self.config_path.clone())
        };
        if next.api.auth_token.is_none() {
            next.api.auth_token = previous.api.auth_token.clone();
        }
        next.validate()?;
        let next_network_health = net::evaluate(&next.network, self.interface_probe.as_ref());
        let recovering_latched_failure = self.bind_failure_latched.read().await.is_some();
        if recovering_latched_failure {
            self.validate_replacement_bind_path(&next).await?;
        }
        if !next.api.require_auth
            && next
                .api
                .bind_address
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|bind| !bind.ip().is_loopback())
        {
            tracing::warn!(
                bind = %next.api.bind_address,
                "configuration update disables API and Web UI authentication on a non-loopback listener; every client that can reach this address can control SwarmOtter"
            );
        }
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        validate_storage_config_transition(&previous, &next, &torrents)?;

        let peer_limits_changed = peer_limits_changed(&previous, &next);
        let restart_required_fields = restart_required_fields(&previous, &next);
        if peer_limits_changed {
            let peer_permits = self.build_peer_permit_configuration(&next).await?;
            self.apply_peer_budget_runtime_update(
                next.clone(),
                peer_permits,
                config_path.as_deref(),
                recovering_latched_failure,
            )
            .await?;
        } else {
            if let Some(path) = &config_path {
                write_config_atomically(path, &next)?;
            }

            let rebuild_data_plane = data_plane_config_changed(&previous, &next);
            let data_plane_transition = if rebuild_data_plane {
                Some(self.data_plane_transition_lock.lock().await)
            } else {
                None
            };
            if rebuild_data_plane {
                // Snapshot progress before stopping every task created from the old
                // containment policy. No old binder, DHT runner, listener, tracker
                // sidecar, or accepted peer session may survive the config swap.
                self.reconcile_engine_progress_for_transition().await;
                let recovery_intents = if !next_network_health.traffic_allowed
                    && next_network_health.mode != NetworkContainmentMode::Disabled
                {
                    let intents = self.live_containment_recovery_intents().await;
                    self.containment_gate.block(
                        next_network_health.status,
                        next_network_health.detail.clone(),
                    );
                    intents
                } else {
                    HashMap::new()
                };
                let registry_hashes = self
                    .registry
                    .lock()
                    .await
                    .torrents
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                self.stop_all_torrent_tasks(&registry_hashes).await;
                *self.dht_runner.lock().await = None;
                if !recovery_intents.is_empty() {
                    let _lifecycle = self.seeder_lifecycle_lock.lock().await;
                    let mut registry = self.registry.lock().await;
                    for (hash, intent) in recovery_intents {
                        if let Some(torrent) = registry.get_mut(&hash) {
                            torrent.containment_recovery_intent = Some(intent);
                            torrent.state = TorrentState::NetworkBlocked;
                            torrent.error = Some(next_network_health.detail.clone());
                        }
                    }
                }
            }
            {
                let mut cfg = self.config.write().await;
                *cfg = next.clone();
            }
            self.selfish_completion_enabled
                .store(next.torrent.selfish, Ordering::Release);
            drop(data_plane_transition);
            if recovering_latched_failure {
                *self.bind_failure_latched.write().await = None;
                let health = net::evaluate(&next.network, self.interface_probe.as_ref());
                if health.traffic_allowed {
                    self.recover_containment_work(health).await;
                } else {
                    self.transition_data_plane_to_blocked(health.status, health.detail)
                        .await;
                }
            }
            self.apply_runtime_config_fields().await;
        }
        self.publish_event(Event::new("settings_changed", json!({})));
        self.publish_event(stats_updated_event());

        Ok(ConfigUpdateResult {
            persisted: config_path.is_some(),
            config_path: config_path.map(|p| p.display().to_string()),
            restart_required: !restart_required_fields.is_empty(),
            restart_required_fields,
            applied_runtime_fields: vec![
                "bandwidth".into(),
                "queue".into(),
                "seeding".into(),
                "network".into(),
                "torrent.allow_ipv6".into(),
                "torrent.utp_enabled".into(),
                "torrent.utp_prefer_tcp".into(),
                "torrent.listen_port".into(),
                "torrent.encryption_mode".into(),
                "torrent.selfish".into(),
                "dht".into(),
                "storage".into(),
                "watch".into(),
                "autopilot".into(),
            ],
            config: redact_config(next),
        })
    }

    async fn reset_downloads(&self) -> Result<ResetResult> {
        let torrents: Vec<Torrent> = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        tracing::warn!(
            torrents_requested = torrents.len(),
            "download state reset requested by API request"
        );
        let registry_hashes: Vec<InfoHash> = torrents.iter().map(Torrent::info_hash).collect();
        self.stop_all_torrent_tasks(&registry_hashes).await;
        self.clear_download_runtime_state().await;

        let mut storage_paths = Vec::new();
        for torrent in &torrents {
            let complete_dir = self.resolve_download_dir(torrent).await;
            let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
            for dir in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
                let storage =
                    swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), dir.clone());
                storage.remove_all().await?;
                push_display_path(&mut storage_paths, &dir);
            }
        }

        let cfg = self.config.read().await.clone();
        let download_dir = cfg
            .storage
            .download_dir
            .clone()
            .unwrap_or_else(default_download_dir_string);
        let incomplete_dir = cfg
            .storage
            .incomplete_dir
            .clone()
            .unwrap_or_else(|| download_dir.clone());
        let mut storage_entries_removed = 0usize;
        for dir in unique_pathbufs([PathBuf::from(incomplete_dir), PathBuf::from(download_dir)]) {
            storage_entries_removed =
                storage_entries_removed.saturating_add(remove_directory_contents(&dir).await?);
            push_display_path(&mut storage_paths, &dir);
        }

        let mut log_paths = Vec::new();
        let mut log_files_cleared = 0usize;
        if let Some(path) = &self.log_file_path {
            truncate_log_file(path).await?;
            log_files_cleared = 1;
            push_display_path(&mut log_paths, path);
        }

        self.clear_download_runtime_state().await;
        self.persist_state().await?;

        tracing::warn!(
            torrents_removed = torrents.len(),
            storage_entries_removed,
            log_files_cleared,
            storage_paths = ?storage_paths,
            log_paths = ?log_paths,
            "download state reset by API request"
        );
        for hash in registry_hashes {
            self.publish_event(torrent_removed_event(hash, true));
        }
        self.publish_event(stats_updated_event());

        Ok(ResetResult {
            torrents_removed: torrents.len(),
            storage_paths,
            storage_entries_removed,
            log_paths,
            log_files_cleared,
        })
    }

    async fn network_health(&self) -> NetworkHealth {
        self.network_health.read().await.clone()
    }

    async fn network_diagnostics(&self) -> NetworkDiagnostics {
        let cfg = self.config.read().await.clone();
        let health = self.network_health.read().await.clone();
        let probe = OsInterfaceProbe;
        let interfaces = probe
            .list()
            .into_iter()
            .map(|iface| {
                let has_ipv4 = iface.addresses.iter().any(std::net::IpAddr::is_ipv4);
                let has_ipv6 = iface.addresses.iter().any(std::net::IpAddr::is_ipv6);
                NetworkInterfaceDiagnostic {
                    selected: cfg.network.required_interface.as_deref()
                        == Some(iface.name.as_str()),
                    name: iface.name,
                    status: format!("{:?}", iface.status).to_ascii_lowercase(),
                    addresses: iface.addresses.iter().map(ToString::to_string).collect(),
                    has_ipv4,
                    has_ipv6,
                }
            })
            .collect();
        let traffic_level = if health.traffic_allowed {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Invalid
        };
        NetworkDiagnostics {
            health: health.clone(),
            listen_port: cfg.torrent.listen_port,
            dht_port: cfg.dht.port,
            torrent_allow_ipv6: cfg.torrent.allow_ipv6,
            utp_enabled: cfg.torrent.utp_enabled,
            utp_prefer_tcp: cfg.torrent.utp_prefer_tcp,
            peer_encryption_mode: cfg.torrent.encryption_mode,
            interfaces,
            checks: vec![
                NetworkPathCheck {
                    id: "containment_status".into(),
                    label: "Containment state".into(),
                    level: traffic_level,
                    detail: health.detail.clone(),
                },
                NetworkPathCheck {
                    id: "ipv6_policy".into(),
                    label: "IPv4/IPv6 policy".into(),
                    level: if cfg.network.allow_ipv6 && cfg.torrent.allow_ipv6 {
                        DiagnosticLevel::Ok
                    } else {
                        DiagnosticLevel::Warning
                    },
                    detail: format!(
                        "network.allow_ipv6={}, torrent.allow_ipv6={}",
                        cfg.network.allow_ipv6, cfg.torrent.allow_ipv6
                    ),
                },
                NetworkPathCheck {
                    id: "dns_validation".into(),
                    label: "DNS containment validation".into(),
                    level: if cfg.network.validate_dns {
                        traffic_level
                    } else {
                        DiagnosticLevel::Warning
                    },
                    detail: if cfg.network.validate_dns {
                        "DNS validation is enabled for the configured path".into()
                    } else {
                        "DNS validation is disabled; IP-literal peers and contained namespaces remain safest".into()
                    },
                },
                NetworkPathCheck {
                    id: "transport_selection".into(),
                    label: "Peer transport selection".into(),
                    level: DiagnosticLevel::Ok,
                    detail: format!(
                        "TCP is {}, uTP is {}, preference is {}, peer encryption is {:?}",
                        "enabled",
                        if cfg.torrent.utp_enabled {
                            "enabled"
                        } else {
                            "disabled"
                        },
                        if cfg.torrent.utp_prefer_tcp {
                            "tcp-first"
                        } else {
                            "utp-first"
                        },
                        cfg.torrent.encryption_mode.as_str()
                    ),
                },
            ],
            containment_matrix: containment_matrix(&cfg, traffic_level),
        }
    }

    async fn storage_roots(&self) -> StorageDiagnostics {
        let cfg = self.config.read().await.clone();
        let mut roots: HashMap<String, StorageRootAccumulator> = HashMap::new();

        let download_dir = resolve_download_dir_from_config(None, &cfg);
        add_storage_root_role(
            &mut roots,
            download_dir.clone(),
            if cfg.storage.download_dir.is_some() {
                StorageRootRole::Download
            } else {
                StorageRootRole::DefaultDownload
            },
        );
        let incomplete_dir = resolve_incomplete_dir_from_config(&download_dir, &cfg);
        add_storage_root_role(
            &mut roots,
            incomplete_dir.clone(),
            StorageRootRole::Incomplete,
        );

        for folder in &cfg.watch {
            if let Some(path) = folder.download_dir.as_ref() {
                add_storage_root_role(&mut roots, path.clone(), StorageRootRole::WatchDownload);
            }
        }

        {
            let reg = self.registry.lock().await;
            for torrent in reg.torrents.values() {
                let complete_dir =
                    resolve_download_dir_from_config(torrent.download_dir.as_deref(), &cfg);
                if torrent.download_dir.is_some() {
                    add_storage_root_role(
                        &mut roots,
                        complete_dir.clone(),
                        StorageRootRole::TorrentOverride,
                    );
                }
                add_storage_root_usage(&mut roots, complete_dir.clone(), torrent);
                let active_dir = resolve_incomplete_dir_from_config(&complete_dir, &cfg);
                add_storage_root_role(&mut roots, active_dir.clone(), StorageRootRole::Incomplete);
                if active_dir != complete_dir {
                    add_storage_root_usage(&mut roots, active_dir, torrent);
                }
            }
        }

        let mut roots = roots
            .into_iter()
            .map(|(path, acc)| {
                swarmotter_core::storage::inspect_storage_root(
                    Path::new(&path),
                    acc.roles,
                    &cfg.storage,
                    swarmotter_core::storage::StorageRootUsage {
                        torrent_count: acc.torrent_count,
                        active_torrents: acc.active_torrents,
                        active_write_rate: acc.active_write_rate,
                        active_recheck_rate: Some(0),
                    },
                )
            })
            .collect::<Vec<StorageRootDiagnostics>>();
        roots.sort_by(|a, b| a.path.cmp(&b.path));

        StorageDiagnostics {
            roots,
            minimum_free_space_bytes: cfg.storage.minimum_free_space_bytes,
            minimum_free_space_percent: cfg.storage.minimum_free_space_percent,
            generated_at: now(),
        }
    }

    async fn doctor_report(&self) -> DoctorReport {
        let cfg = self.config.read().await.clone();
        let network = self.network_health.read().await.clone();
        let mut checks = Vec::new();
        push_check(
            &mut checks,
            "config",
            "Configuration validation",
            if cfg.validate().is_ok() {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Invalid
            },
            "the active configuration parses and validates",
            None,
        );
        push_check(
            &mut checks,
            "network",
            "Network containment",
            if network.traffic_allowed {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Invalid
            },
            network.detail,
            Some(
                "fix the configured interface/source/namespace before torrent traffic can continue",
            ),
        );
        self.add_config_file_check(&mut checks).await;
        self.add_log_file_check(&mut checks).await;
        self.add_storage_checks(&cfg, &mut checks).await;
        self.add_watch_checks(&cfg, &mut checks).await;
        self.add_torrent_runtime_check(&mut checks).await;

        let level = checks.iter().fold(DiagnosticLevel::Ok, |level, check| {
            DiagnosticLevel::worst(level, check.level)
        });
        let summary = match level {
            DiagnosticLevel::Ok => "all doctor checks passed".into(),
            DiagnosticLevel::Warning => "one or more doctor checks need attention".into(),
            DiagnosticLevel::Invalid => "one or more doctor checks are invalid".into(),
        };
        DoctorReport {
            level,
            summary,
            checks,
        }
    }

    async fn recent_logs(&self, max_lines: usize) -> LogSnapshot {
        let Some(path) = self.log_file_path.clone() else {
            return LogSnapshot {
                enabled: false,
                path: None,
                lines: Vec::new(),
                truncated: false,
            };
        };
        let lines = read_last_lines(&path, max_lines).unwrap_or_default();
        LogSnapshot {
            enabled: true,
            path: Some(path.display().to_string()),
            truncated: lines.len() >= max_lines,
            lines,
        }
    }

    async fn global_stats(&self) -> GlobalStats {
        let desired = self.desired_download_hashes().await;
        let scheduler = self.scheduler_diagnostics(&desired).await;
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let active_seeds = self.seeder_registry.len().await;
        let reg = self.registry.lock().await;

        let mut active_downloads = 0;
        let mut paused = 0;
        let mut download_rate = 0;
        let mut upload_rate = 0;
        let mut total_downloaded = 0;
        let mut total_uploaded = 0;
        for t in reg.torrents.values() {
            match t.state {
                TorrentState::Downloading | TorrentState::DownloadingMetadata => {
                    active_downloads += 1;
                }
                TorrentState::Paused => {
                    paused += 1;
                }
                _ => {}
            }
            download_rate += t.rate_down;
            upload_rate += t.rate_up;
            total_downloaded += t.downloaded;
            total_uploaded += t.uploaded;
        }

        GlobalStats {
            download_rate,
            upload_rate,
            torrent_count: reg.torrents.len(),
            active_downloads,
            active_seeds,
            paused,
            total_downloaded,
            total_uploaded,
            scheduler,
            ..Default::default()
        }
    }

    async fn torrent_stats(&self, hash: &InfoHash) -> Option<TorrentDiagnostics> {
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let engine_state = self.engine_states.read().await.get(hash).cloned();
        let live = if let Some(state) = engine_state {
            let s = state.lock().await;
            Some(LiveTorrentDiagnostics::from_engine_state(
                &s,
                Instant::now(),
            ))
        } else {
            None
        };
        let reg = self.registry.lock().await;
        let t = reg.get(hash)?;
        let progress = if t.meta.total_length == 0 {
            0.0
        } else {
            t.bytes_completed() as f64 / t.meta.total_length as f64
        };
        let live = live.unwrap_or_default();
        Some(TorrentDiagnostics {
            info_hash: t.info_hash(),
            name: t.name().to_string(),
            state: t.state,
            total_length: t.meta.total_length,
            bytes_completed: t.bytes_completed(),
            downloaded: t.downloaded,
            uploaded: t.uploaded,
            piece_count: t.meta.piece_count(),
            pieces_have: t.pieces_have(),
            piece_length: t.meta.piece_length,
            progress,
            rate_down: t.rate_down,
            rate_up: t.rate_up,
            download_limit: t.download_limit,
            upload_limit: t.upload_limit,
            active_peer_workers: live.active_peer_workers,
            known_peers: live.known_peers,
            peer_scheduler: live.peer_scheduler,
            useful_peers: live.useful_peers,
            choked_peers: live.choked_peers,
            unchoked_peers: live.unchoked_peers,
            recent_peer_failures: live.recent_peer_failures,
            recent_tracker_failures: live.recent_tracker_failures,
            tracker_ok: live.tracker_ok,
            tracker_message: live.tracker_message,
            last_announce: live.last_announce,
            tracker_last_ok_seconds_ago: live.tracker_last_ok_seconds_ago,
            dht_discovery_ok: live.dht_discovery_ok,
            dht_last_seen_seconds_ago: live.dht_last_seen_seconds_ago,
            pex_discovery_ok: live.pex_discovery_ok,
            pex_last_seen_seconds_ago: live.pex_last_seen_seconds_ago,
            private: t.meta.is_private(),
        })
    }

    async fn autopilot_status(&self) -> AutopilotConfig {
        self.config.read().await.autopilot.clone()
    }

    async fn torrent_autopilot_decision(&self, hash: &InfoHash) -> Option<AutopilotDecision> {
        let torrent = self.registry.lock().await.get(hash).cloned()?;
        let cfg = self.config.read().await.clone();
        let network = self.network_health.read().await.clone();
        let mode = effective_autopilot_mode(cfg.autopilot.mode, torrent.autopilot_mode_override);
        let state = self.engine_states.read().await.get(hash).cloned();
        let state = match state {
            Some(state) => tokio::time::timeout(AUTOPILOT_STATE_LOCK_TIMEOUT, state.lock())
                .await
                .ok()
                .map(|guard| guard.clone()),
            None => None,
        };
        let input = build_autopilot_input(
            &torrent,
            state.as_ref(),
            self.rate_samples.read().await.get(hash).copied(),
            Instant::now(),
            &network,
        );
        let decision = AutopilotAnalyzer::new().analyze(&input, mode);
        self.autopilot_decisions
            .write()
            .await
            .insert(*hash, decision.clone());
        Some(decision)
    }

    async fn set_torrent_autopilot_mode_override(
        &self,
        hash: &InfoHash,
        mode: Option<AutopilotMode>,
    ) -> Result<()> {
        {
            let mut reg = self.registry.lock().await;
            let Some(t) = reg.get_mut(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            t.autopilot_mode_override = mode;
        }
        self.refresh_autopilot_decisions(false).await;
        self.persist_state().await
    }

    async fn watch_scan(&self) -> Result<()> {
        self.scan_watch_folders().await
    }

    async fn watch_status(&self) -> WatchStatus {
        let cfg = self.config.read().await.clone();
        let history = self
            .watch_imports
            .lock()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let observations = self.watch_observations.lock().await.clone();
        let enabled = !cfg.watch.is_empty();
        let mut folders = Vec::with_capacity(cfg.watch.len());
        for folder in cfg.watch {
            let scan_folder = folder.clone();
            let scan = tokio::task::spawn_blocking(move || watch::scan_watch_folder(&scan_folder))
                .await
                .ok()
                .and_then(|result| result.ok());
            let exists = scan.is_some();
            let pending_torrent_files = scan
                .as_ref()
                .map(|scan| {
                    scan.files
                        .iter()
                        .filter(|file| {
                            observations.get(&file.key).is_none_or(|observation| {
                                observation.fingerprint != file.fingerprint
                                    || observation.processed_fingerprint != Some(file.fingerprint)
                            })
                        })
                        .count()
                })
                .unwrap_or(0);
            let root = scan
                .as_ref()
                .map(|scan| scan.root.clone())
                .or_else(|| watch::lexical_absolute(Path::new(&folder.path)).ok());
            let last_result = history
                .iter()
                .rev()
                .find(|result| {
                    root.as_ref()
                        .is_some_and(|root| Path::new(&result.path).starts_with(root))
                })
                .cloned();
            folders.push(WatchFolderStatus {
                config: folder,
                exists,
                pending_torrent_files,
                last_result,
            });
        }
        WatchStatus {
            enabled,
            folders,
            recent_imports: history,
        }
    }

    async fn watch_history(&self) -> Vec<watch::ImportResult> {
        self.watch_imports.lock().await.iter().cloned().collect()
    }
}

fn make_tracker(url: &str, tier: usize) -> TrackerInfo {
    TrackerInfo {
        id: TrackerId(url.to_string()),
        url: url.to_string(),
        kind: TrackerKind::from_url(url).unwrap_or(TrackerKind::Http),
        tier,
        status: TrackerStatus::NotContacted,
        seeders: 0,
        leechers: 0,
        downloads: 0,
        last_error: None,
        last_message: None,
        next_announce: None,
        last_announce: None,
    }
}

fn validate_restored_storage_ownership<'a>(
    torrents: impl IntoIterator<Item = &'a Torrent>,
    config: &Config,
) -> Result<()> {
    let mut ownerships = Vec::new();
    for torrent in torrents {
        let complete_dir =
            resolve_download_dir_from_config(torrent.download_dir.as_deref(), config);
        let active_dir = resolve_incomplete_dir_from_config(&complete_dir, config);
        for root in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
            ownerships.push(
                swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), root)
                    .path_ownership()?,
            );
        }
    }
    for index in 0..ownerships.len() {
        for other in ownerships.iter().skip(index + 1) {
            ownerships[index].ensure_compatible_with(other)?;
        }
    }
    Ok(())
}

fn unique_pathbufs<I>(paths: I) -> Vec<PathBuf>
where
    I: IntoIterator<Item = PathBuf>,
{
    let mut out = Vec::new();
    for path in paths {
        if !out.contains(&path) {
            out.push(path);
        }
    }
    out
}

fn validated_relative_path(path: &str) -> Result<Vec<String>> {
    if path.trim().is_empty() {
        return Err(CoreError::Storage("renamed path must not be empty".into()));
    }
    let mut components = Vec::new();
    for component in Path::new(path).components() {
        match component {
            std::path::Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| CoreError::Storage("renamed path must be valid UTF-8".into()))?;
                components.push(value.to_string());
            }
            _ => {
                return Err(CoreError::Storage(
                    "renamed path must be relative and must not contain '.' or '..'".into(),
                ));
            }
        }
    }
    if components.is_empty() {
        return Err(CoreError::Storage("renamed path must not be empty".into()));
    }
    Ok(components)
}

#[derive(Debug, Clone, Copy)]
enum PayloadRenameOutcome {
    Moved,
    PlaceholderCreated,
}

async fn rename_payload_exclusive(
    source: &Path,
    destination: &Path,
) -> Result<PayloadRenameOutcome> {
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let source_metadata = match tokio::fs::symlink_metadata(source).await {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(CoreError::from(error)),
    };
    if source_metadata.is_none() {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .await
            .map_err(|error| {
                CoreError::Storage(format!(
                    "cannot reserve rename destination {}: {error}",
                    destination.display()
                ))
            })?;
        file.sync_all().await.map_err(CoreError::from)?;
        sync_parent_directory(destination).await?;
        return Ok(PayloadRenameOutcome::PlaceholderCreated);
    }
    if !source_metadata.is_some_and(|metadata| metadata.is_file()) {
        return Err(CoreError::Storage(format!(
            "rename source is not a regular file: {}",
            source.display()
        )));
    }

    let move_result: Result<()> = match tokio::fs::hard_link(source, destination).await {
        Ok(()) => Ok(()),
        Err(link_error) => {
            let mut input = tokio::fs::File::open(source)
                .await
                .map_err(CoreError::from)?;
            let mut output = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)
                .await
                .map_err(|error| {
                    CoreError::Storage(format!(
                        "cannot rename {} to {} without replacing data: hard link failed ({link_error}); exclusive copy failed ({error})",
                        source.display(),
                        destination.display()
                    ))
                })?;
            if let Err(error) = tokio::io::copy(&mut input, &mut output).await {
                let _ = tokio::fs::remove_file(destination).await;
                return Err(CoreError::from(error));
            }
            if let Err(error) = output.sync_all().await {
                let _ = tokio::fs::remove_file(destination).await;
                return Err(CoreError::from(error));
            }
            Ok(())
        }
    };
    move_result?;
    if let Err(error) = sync_parent_directory(destination).await {
        let cleanup = tokio::fs::remove_file(destination).await;
        return Err(CoreError::Storage(format!(
            "cannot sync rename destination {}: {error}{}",
            destination.display(),
            cleanup
                .err()
                .map(|cleanup| format!("; destination cleanup failed: {cleanup}"))
                .unwrap_or_default()
        )));
    }
    if let Err(error) = tokio::fs::remove_file(source).await {
        let cleanup = tokio::fs::remove_file(destination).await;
        return Err(CoreError::Storage(format!(
            "cannot remove rename source {}: {error}{}",
            source.display(),
            cleanup
                .err()
                .map(|cleanup| format!("; destination cleanup failed: {cleanup}"))
                .unwrap_or_default()
        )));
    }
    if let Err(error) = sync_parent_directory(source).await {
        let rollback = async {
            tokio::fs::hard_link(destination, source)
                .await
                .map_err(CoreError::from)?;
            tokio::fs::remove_file(destination)
                .await
                .map_err(CoreError::from)?;
            Ok::<(), CoreError>(())
        }
        .await;
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(CoreError::Storage(format!(
                "{error}; rename rollback also failed: {rollback_error}"
            ))),
        };
    }
    Ok(PayloadRenameOutcome::Moved)
}

async fn rollback_payload_rename(
    source: &Path,
    destination: &Path,
    outcome: PayloadRenameOutcome,
) -> Result<()> {
    match outcome {
        PayloadRenameOutcome::Moved => {
            if !matches!(
                rename_payload_exclusive(destination, source).await?,
                PayloadRenameOutcome::Moved
            ) {
                return Err(CoreError::Storage(
                    "rename rollback found a missing destination payload".into(),
                ));
            }
        }
        PayloadRenameOutcome::PlaceholderCreated => {
            tokio::fs::remove_file(destination)
                .await
                .map_err(CoreError::from)?;
            sync_parent_directory(destination).await?;
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(parent).and_then(|directory| directory.sync_all())
    })
    .await
    .map_err(|error| CoreError::Storage(format!("sync directory task failed: {error}")))?
    .map_err(CoreError::from)
}

#[cfg(not(unix))]
async fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn torrent_selection_complete(
    torrent: &Torrent,
    have: &swarmotter_core::storage::PieceBitfield,
) -> Result<bool> {
    for piece in 0..torrent.meta.piece_count() {
        let selected = swarmotter_core::storage::piece_file_ranges(&torrent.meta, piece)?
            .into_iter()
            .any(|slice| {
                torrent
                    .wanted
                    .get(slice.file_index)
                    .copied()
                    .unwrap_or(true)
                    && torrent
                        .priorities
                        .get(slice.file_index)
                        .copied()
                        .unwrap_or(FilePriority::Normal)
                        != FilePriority::Unwanted
            });
        if selected && !have.has(piece) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn add_storage_root_role(
    roots: &mut HashMap<String, StorageRootAccumulator>,
    path: String,
    role: StorageRootRole,
) {
    let entry = roots.entry(path).or_default();
    if !entry.roles.contains(&role) {
        entry.roles.push(role);
    }
}

fn add_storage_root_usage(
    roots: &mut HashMap<String, StorageRootAccumulator>,
    path: String,
    torrent: &Torrent,
) {
    let entry = roots.entry(path).or_default();
    entry.torrent_count += 1;
    if torrent.state.is_active() {
        entry.active_torrents += 1;
        entry.active_write_rate = entry.active_write_rate.saturating_add(torrent.rate_down);
    }
}

fn push_display_path(paths: &mut Vec<String>, path: &Path) {
    let value = path.display().to_string();
    if !paths.contains(&value) {
        paths.push(value);
    }
}

async fn remove_directory_contents(path: &Path) -> Result<usize> {
    match tokio::fs::metadata(path).await {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => {
            return Err(CoreError::Storage(format!(
                "reset path is not a directory: {}",
                path.display()
            )));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(path)
                .await
                .map_err(CoreError::from)?;
            return Ok(0);
        }
        Err(e) => return Err(CoreError::from(e)),
    }

    let mut entries = tokio::fs::read_dir(path).await.map_err(CoreError::from)?;
    let mut removed = 0usize;
    while let Some(entry) = entries.next_entry().await.map_err(CoreError::from)? {
        let entry_path = entry.path();
        let meta = tokio::fs::symlink_metadata(&entry_path)
            .await
            .map_err(CoreError::from)?;
        if meta.is_dir() && !meta.file_type().is_symlink() {
            tokio::fs::remove_dir_all(&entry_path)
                .await
                .map_err(CoreError::from)?;
        } else {
            tokio::fs::remove_file(&entry_path)
                .await
                .map_err(CoreError::from)?;
        }
        removed = removed.saturating_add(1);
    }
    Ok(removed)
}

async fn truncate_log_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(CoreError::from)?;
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(CoreError::from)?;
    Ok(())
}

/// Apply current network containment state to a torrent's lifecycle state.
async fn apply_network_state(t: &mut Torrent, health: &Arc<RwLock<NetworkHealth>>) {
    let h = health.read().await;
    if !h.traffic_allowed && h.mode != NetworkContainmentMode::Disabled {
        t.state = TorrentState::NetworkBlocked;
        t.error = Some(h.detail.clone());
    }
}

fn effective_autopilot_mode(
    global_mode: AutopilotMode,
    override_mode: Option<AutopilotMode>,
) -> AutopilotMode {
    if global_mode == AutopilotMode::Disabled {
        AutopilotMode::Disabled
    } else {
        override_mode.unwrap_or(global_mode)
    }
}

fn build_autopilot_input(
    torrent: &Torrent,
    state: Option<&EngineState>,
    sample: Option<RateSample>,
    now: Instant,
    network: &NetworkHealth,
) -> AutopilotInput {
    let rate_down = sample.map(|s| s.rate_down).unwrap_or(torrent.rate_down);
    let rate_up = sample.map(|s| s.rate_up).unwrap_or(torrent.rate_up);
    let rate_down_observed_peak = sample
        .map(|s| s.peak_rate_down)
        .unwrap_or(torrent.rate_down)
        .max(rate_down);
    let network_traffic_allowed =
        network.traffic_allowed || network.mode == NetworkContainmentMode::Disabled;

    let no_progress_seconds = latest_progress_instant(sample, state)
        .map(|seen| now.saturating_duration_since(seen).as_secs())
        .or_else(|| sample.map(|s| now.saturating_duration_since(s.at).as_secs()));

    let mut input = AutopilotInput {
        state: torrent.state,
        rate_down,
        rate_up,
        rate_down_observed_peak,
        download_limit: torrent.download_limit,
        piece_count: torrent.meta.piece_count(),
        pieces_have: torrent.pieces_have(),
        known_peers: torrent.known_peers,
        useful_peers: None,
        active_peer_workers: torrent.active_peer_workers,
        tracker_ok: torrent.state.is_active(),
        no_progress_seconds,
        network_traffic_allowed: Some(network_traffic_allowed),
        ..Default::default()
    };

    if let Some(state) = state {
        let piece_count = state.piece_count.max(torrent.meta.piece_count());
        input.piece_count = piece_count;
        input.pieces_have = if state.piece_count > 0 {
            state.pieces_have.count(state.piece_count)
        } else {
            torrent.pieces_have()
        };
        input.known_peers = state.peers.len();
        input.useful_peers = Some(useful_peer_count(&state.peer_health, now));
        input.active_peer_workers = state.active_peers;
        input.discovered_peers = Some(state.peer_scheduler.discovered_peers.max(state.peers.len()));
        input.eligible_peers = Some(state.peer_scheduler.eligible_peers);
        input.peer_worker_limit = Some(state.peer_scheduler.peer_worker_limit);
        input.backed_off_peers = Some(state.peer_scheduler.backed_off_peers);
        input.tracker_ok = state.tracker_ok;
        input.tracker_recent_ok_seconds_ago = instant_age_seconds(now, state.tracker_last_ok);
        input.tracker_failures_recent = state.tracker_failures_recent;
        input.dht_discovery_ok = Some(state.dht_discovery_ok);
        input.dht_last_seen_seconds_ago = instant_age_seconds(now, state.dht_last_seen);
        input.pex_discovery_ok = Some(state.pex_discovery_ok);
        input.pex_last_seen_seconds_ago = instant_age_seconds(now, state.pex_last_seen);
        input.peer_failures_recent = Some(
            state
                .peer_disconnects_recent
                .saturating_add(state.hash_failures)
                .saturating_add(state.timeout_failures),
        );
        input.serial_peer_active = state.peer_scheduler.serial_peer_active;
    }

    input
}

fn latest_progress_instant(
    sample: Option<RateSample>,
    state: Option<&EngineState>,
) -> Option<Instant> {
    let mut latest = sample.and_then(|sample| sample.last_download_at);
    if let Some(state) = state {
        for candidate in [
            state.last_valid_block,
            state.block_last_seen,
            state.webseed_last_seen,
        ] {
            if candidate > latest {
                latest = candidate;
            }
        }
    }
    latest
        .or_else(|| sample.and_then(|sample| sample.no_download_since))
        .or_else(|| sample.map(|sample| sample.at))
}

#[allow(clippy::too_many_arguments)]
fn log_torrent_throughput_peak(
    hash: &InfoHash,
    torrent: &Torrent,
    state: &EngineState,
    sample_rate_down: u64,
    sample_rate_up: u64,
    previous_peak_rate_down: u64,
    previous_peak_rate_up: u64,
    peak_rate_down: u64,
    peak_rate_up: u64,
    now: Instant,
) {
    tracing::info!(
        info_hash = %hash,
        name = %torrent.name(),
        state = %torrent.state,
        sample_rate_down_bps = sample_rate_down,
        sample_rate_down_mib_s = rate_mib_per_second(sample_rate_down),
        sample_rate_up_bps = sample_rate_up,
        sample_rate_up_mib_s = rate_mib_per_second(sample_rate_up),
        rate_down_bps = torrent.rate_down,
        rate_down_mib_s = rate_mib_per_second(torrent.rate_down),
        rate_up_bps = torrent.rate_up,
        rate_up_mib_s = rate_mib_per_second(torrent.rate_up),
        previous_peak_rate_down_bps = previous_peak_rate_down,
        previous_peak_rate_down_mib_s = rate_mib_per_second(previous_peak_rate_down),
        previous_peak_rate_up_bps = previous_peak_rate_up,
        previous_peak_rate_up_mib_s = rate_mib_per_second(previous_peak_rate_up),
        peak_rate_down_bps = peak_rate_down,
        peak_rate_down_mib_s = rate_mib_per_second(peak_rate_down),
        peak_rate_up_bps = peak_rate_up,
        peak_rate_up_mib_s = rate_mib_per_second(peak_rate_up),
        downloaded = state.downloaded,
        uploaded = state.uploaded,
        active_peer_workers = state.active_peers,
        known_peers = state.peers.len(),
        peer_worker_limit = state.peer_scheduler.peer_worker_limit,
        eligible_peers = state.peer_scheduler.eligible_peers,
        filtered_peers = state.peer_scheduler.filtered_peers,
        failed_peers = state.peer_scheduler.failed_peers,
        backed_off_peers = state.peer_scheduler.backed_off_peers,
        parallel_candidates = state.peer_scheduler.parallel_candidates,
        parallel_workers_started = state.peer_scheduler.parallel_workers_started,
        serial_peer_active = state.peer_scheduler.serial_peer_active,
        scheduler_reason = ?state.peer_scheduler.last_reason,
        useful_peers = useful_peer_count(&state.peer_health, now),
        tracker_ok = state.tracker_ok,
        dht_discovery_ok = state.dht_discovery_ok,
        pex_discovery_ok = state.pex_discovery_ok,
        tracker_last_ok_seconds_ago = ?instant_age_seconds(now, state.tracker_last_ok),
        dht_last_seen_seconds_ago = ?instant_age_seconds(now, state.dht_last_seen),
        pex_last_seen_seconds_ago = ?instant_age_seconds(now, state.pex_last_seen),
        webseed_last_seen_seconds_ago = ?instant_age_seconds(now, state.webseed_last_seen),
        "torrent throughput peak increased"
    );
}

fn rate_mib_per_second(bytes_per_second: u64) -> f64 {
    let mib = bytes_per_second as f64 / 1_048_576.0;
    (mib * 100.0).round() / 100.0
}

fn smooth_rate(
    previous_rate: u64,
    instantaneous_rate: u64,
    last_activity_at: Option<Instant>,
    now: Instant,
) -> u64 {
    if instantaneous_rate > 0 {
        if previous_rate == 0 {
            instantaneous_rate
        } else {
            ((previous_rate as f64 * 0.65) + (instantaneous_rate as f64 * 0.35)) as u64
        }
    } else if last_activity_at
        .map(|at| now.duration_since(at) <= Duration::from_secs(20))
        .unwrap_or(false)
    {
        ((previous_rate as f64) * 0.85) as u64
    } else {
        0
    }
}

/// Assemble a `HealthInput` from the live engine state and the torrent
/// record. Pulls out every signal the health calculator needs (piece
/// availability, per-peer usefulness, throughput, recent stability,
/// tracker/DHT/PEX freshness, and the network containment health) so that
/// the same scoring function is exercised in tests and in the daemon.
#[allow(clippy::too_many_arguments)]
fn build_health_input(
    t: &Torrent,
    piece_count: usize,
    pieces_have: &swarmotter_core::storage::resume::PieceBitfield,
    peer_health: &std::collections::HashMap<std::net::SocketAddr, EnginePeerHealth>,
    tracker_ok: &bool,
    dht_discovery_ok: bool,
    pex_discovery_ok: bool,
    tracker_failures_recent: u32,
    peer_disconnects_recent: u32,
    hash_failures: u32,
    timeout_failures: u32,
    last_valid_block: Option<std::time::Instant>,
    block_last_seen: Option<std::time::Instant>,
    webseed_last_seen: Option<std::time::Instant>,
    dht_last_seen: Option<std::time::Instant>,
    pex_last_seen: Option<std::time::Instant>,
    tracker_last_ok: Option<std::time::Instant>,
    known_peers: usize,
    _tracker_message: Option<&str>,
    rate_down_observed_peak: u64,
    global_download_limit: u64,
    network: NetworkHealth,
) -> HealthInput {
    use std::time::Duration;
    let now = std::time::Instant::now();
    // A "recent" signal is anything seen in the last ~90 seconds.
    let recent_window = Duration::from_secs(90);
    let peer_block_recent = peer_health.values().any(|p| {
        p.last_valid_block
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false)
    });
    let received_block_recently = last_valid_block
        .map(|t| now.duration_since(t) < recent_window)
        .unwrap_or(false)
        || block_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false)
        || webseed_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false)
        || peer_block_recent
        || t.rate_down > 0;
    let webseed_recent_ok = webseed_last_seen
        .map(|t| now.duration_since(t) < recent_window)
        .unwrap_or(false);
    let time_since_last_block = last_valid_block
        .or(block_last_seen)
        .or(webseed_last_seen)
        .map(|t| now.duration_since(t));
    let tracker_recent_ok = *tracker_ok
        || tracker_last_ok
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
    let dht_recent_ok = dht_discovery_ok
        || dht_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
    let pex_recent_ok = pex_discovery_ok
        || pex_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
    // The engine does not (yet) populate `EnginePeerHealth` automatically for
    // every candidate peer, so derive a coarse per-peer health from what the
    // engine has recorded: peers that have sent a valid block recently are
    // considered useful and unchoked, and peers that have only been seen but
    // not heard from are treated as having no missing pieces.
    let mut peers: Vec<EnginePeerHealth> = Vec::new();
    for p in peer_health.values() {
        let last_valid = p.last_valid_block;
        let last_seen = p.last_seen;
        let last_seen_recent = last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
        let useful_recently = (p.useful_recently && last_seen_recent)
            || last_valid
                .map(|t| now.duration_since(t) < recent_window)
                .unwrap_or(false);
        let unchoked = (p.unchoked && last_seen_recent) || useful_recently;
        let has_missing = (useful_recently || p.has_missing_pieces) && last_seen_recent;
        peers.push(EnginePeerHealth {
            piece_bitfield: p.piece_bitfield.clone(),
            has_missing_pieces: has_missing,
            unchoked,
            blocked: p.blocked,
            last_valid_block: last_valid,
            useful_recently,
            discovered_from_pex: p.discovered_from_pex,
            last_seen,
        });
    }
    let no_peers_discovered = known_peers == 0 && peers.is_empty() && t.rate_down == 0;
    HealthInput {
        state: t.state,
        private: t.meta.is_private(),
        piece_count,
        pieces_have: pieces_have.clone(),
        peers,
        rate_down: t.rate_down,
        rate_down_observed_peak,
        download_limit: t.download_limit,
        upload_limit: t.upload_limit,
        global_download_limit,
        network: Some(network),
        tracker_ok: *tracker_ok,
        tracker_recent_ok,
        tracker_failures_recent,
        dht_recent_ok,
        pex_recent_ok,
        peer_disconnects_recent,
        hash_failures,
        timeout_failures,
        received_block_recently,
        webseed_recent_ok,
        time_since_last_block,
        known_peers,
        no_peers_discovered,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt as _;
    use swarmotter_api::state::DaemonOps;

    fn unique_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "swarmotter-daemon-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    async fn add_complete_seed_fixture(
        runtime: &DaemonRuntime,
        name: &str,
        content: &[u8],
    ) -> (InfoHash, Arc<swarmotter_core::bandwidth::RateLimiter>) {
        let bytes = swarmotter_core::meta::build_single_file_torrent(name, content, 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let root = runtime
            .config
            .read()
            .await
            .storage
            .download_dir
            .clone()
            .unwrap();
        let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), PathBuf::from(root));
        for piece in 0..meta.piece_count() {
            let start = piece * meta.piece_length as usize;
            let end = (start + meta.piece_length as usize).min(content.len());
            storage
                .write_piece(piece, &content[start..end])
                .await
                .unwrap();
        }
        let mut torrent = Torrent::new(meta.clone(), now());
        torrent.state = TorrentState::Completed;
        torrent.downloaded = meta.total_length;
        torrent.date_completed = Some(now());
        torrent.seeding.seed_forever = true;
        for piece in 0..meta.piece_count() {
            torrent.progress.have_piece(piece);
        }
        torrent.recompute_file_bytes_completed();
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
        let limiter = runtime.ensure_torrent_limiter(hash, 0, 0).await;
        (hash, limiter)
    }

    async fn assert_seeder_state_registry_invariant(runtime: &DaemonRuntime) {
        let _lifecycle = runtime.seeder_lifecycle_lock.lock().await;
        let live = runtime.seeder_registry.info_hashes().await;
        let registry = runtime.registry.lock().await;
        for hash in &live {
            let torrent = registry.get(hash).expect("live seeder has a torrent");
            assert_eq!(torrent.state, TorrentState::Seeding);
            assert_eq!(torrent.seeding_status, SeedingStatus::Active);
        }
        for (hash, torrent) in &registry.torrents {
            if torrent.state != TorrentState::NetworkBlocked
                && (torrent.state == TorrentState::Seeding
                    || torrent.seeding_status == SeedingStatus::Active)
            {
                assert!(live.contains(hash), "modeled active seeder is not live");
            }
        }
    }

    async fn peer_reconfiguration_fixture(
        label: &str,
    ) -> (DaemonRuntime, InfoHash, PathBuf, PathBuf) {
        let root = unique_dir(label);
        let config_path = root.join("swarmotter.toml");
        let mut cfg = Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        cfg.bandwidth.max_peers = 3;
        cfg.bandwidth.max_peers_per_torrent = 2;
        cfg.queue.max_active_seeds = 1;
        cfg.seeding.global_ratio_limit = None;
        cfg.seeding.global_idle_limit = None;
        write_config_atomically(&config_path, &cfg).unwrap();
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_and_broker(
            cfg,
            health,
            Some(config_path.clone()),
            None,
            EventBroker::default(),
        );
        let (hash, _) = add_complete_seed_fixture(
            &runtime,
            "peer-reconfiguration-seed.bin",
            b"generated lawful peer reconfiguration fixture",
        )
        .await;
        runtime.reconcile_seeders().await;
        assert!(runtime.seeder_registry.contains(&hash).await);
        (runtime, hash, root, config_path)
    }

    async fn active_engine_reconfiguration_fixture(
        label: &str,
    ) -> (DaemonRuntime, InfoHash, PathBuf, PathBuf) {
        let root = unique_dir(label);
        let config_path = root.join("swarmotter.toml");
        let mut cfg = Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        cfg.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
        cfg.dht.enabled = false;
        cfg.pex.enabled = false;
        cfg.bandwidth.max_peers = 3;
        cfg.bandwidth.max_peers_per_torrent = 2;
        write_config_atomically(&config_path, &cfg).unwrap();
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_and_broker(
            cfg,
            health,
            Some(config_path.clone()),
            None,
            EventBroker::default(),
        );
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "active-peer-reconfiguration.bin",
            b"generated active engine peer reconfiguration fixture",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta, now());
        torrent.state = TorrentState::Downloading;
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
        runtime.ensure_torrent_peer_permit_pool(hash).await;
        runtime.start_engine(hash).await;
        assert!(runtime.engine_running_for_test(&hash).await);
        (runtime, hash, root, config_path)
    }

    fn scale_hash_bytes(n: u32) -> [u8; 20] {
        let mut bytes = [0u8; 20];
        bytes[..4].copy_from_slice(&n.to_be_bytes());
        bytes
    }

    #[tokio::test]
    async fn durable_state_restores_torrents_settings_and_queue() {
        let root = unique_dir("durable-state");
        let state_path = root.join("state.json");
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg.clone(),
            health.clone(),
            None,
            None,
            Some(state_path.clone()),
            EventBroker::default(),
        );
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "persisted.bin",
            b"durable daemon state",
            8,
            None,
            false,
        );
        let hash = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();
        runtime
            .set_labels(&hash, vec!["linux-release".into()])
            .await
            .unwrap();
        runtime
            .set_torrent_limits(
                &hash,
                swarmotter_core::bandwidth::TorrentBandwidth {
                    download: 111,
                    upload: 222,
                },
            )
            .await
            .unwrap();
        drop(runtime);

        let restored = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        assert_eq!(restored.restore_persisted_state().await.unwrap(), 1);
        let torrent = restored.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, TorrentState::Paused);
        assert_eq!(torrent.labels, vec!["linux-release"]);
        assert_eq!(restored.queue.lock().await.position(&hash), Some(1));
        let limiter = restored
            .torrent_limiters
            .read()
            .await
            .get(&hash)
            .cloned()
            .expect("paused restored torrents retain a limiter");
        assert_eq!(
            limiter.capacity(swarmotter_core::bandwidth::RateDirection::Download),
            111
        );
        assert_eq!(
            limiter.capacity(swarmotter_core::bandwidth::RateDirection::Upload),
            222
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn restart_reconstructs_eligible_seeder_and_preserves_automatic_and_manual_stops() {
        let root = unique_dir("seeding-restart-lifecycle");
        let state_path = root.join("state.json");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = 0;
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.seeding.global_ratio_limit = None;
        cfg.seeding.global_idle_limit = None;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg.clone(),
            health.clone(),
            None,
            None,
            Some(state_path.clone()),
            EventBroker::default(),
        );
        let (eligible, _) =
            add_complete_seed_fixture(&runtime, "restart-active.bin", b"restart active payload")
                .await;
        let (automatic, _) = add_complete_seed_fixture(
            &runtime,
            "restart-automatic.bin",
            b"restart automatic payload",
        )
        .await;
        let (manual, _) =
            add_complete_seed_fixture(&runtime, "restart-manual.bin", b"restart manual payload")
                .await;
        {
            let mut registry = runtime.registry.lock().await;
            let eligible_torrent = registry.get_mut(&eligible).unwrap();
            eligible_torrent.state = TorrentState::Seeding;
            eligible_torrent.seeding_status = SeedingStatus::Active;
            let automatic_torrent = registry.get_mut(&automatic).unwrap();
            automatic_torrent.state = TorrentState::Completed;
            automatic_torrent.seeding.seed_forever = false;
            automatic_torrent.seeding.ratio_limit = Some(0.0);
            automatic_torrent.seeding_status = SeedingStatus::StoppedRatio;
            let manual_torrent = registry.get_mut(&manual).unwrap();
            manual_torrent.state = TorrentState::Paused;
            manual_torrent.seeding_status = SeedingStatus::StoppedManual;
        }
        runtime.persist_state().await.unwrap();
        assert!(runtime.seeder_registry.is_empty().await);
        assert!(runtime.seeder_shutdowns.lock().await.is_empty());
        assert!(runtime.seeder_listener_handle.lock().await.is_none());
        // No task was started: dropping here deliberately models a process
        // crash after durable Active state, without detaching a live listener.
        drop(runtime);

        let restored = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        assert_eq!(restored.restore_persisted_state().await.unwrap(), 3);
        assert!(restored.seeder_registry.contains(&eligible).await);
        assert!(!restored.seeder_registry.contains(&automatic).await);
        assert!(!restored.seeder_registry.contains(&manual).await);
        let registry = restored.registry.lock().await;
        assert_eq!(
            registry.get(&eligible).unwrap().state,
            TorrentState::Seeding
        );
        assert_eq!(
            registry.get(&eligible).unwrap().seeding_status,
            SeedingStatus::Active
        );
        assert_eq!(
            registry.get(&automatic).unwrap().seeding_status,
            SeedingStatus::StoppedRatio
        );
        assert_eq!(registry.get(&manual).unwrap().state, TorrentState::Paused);
        assert_eq!(
            registry.get(&manual).unwrap().seeding_status,
            SeedingStatus::StoppedManual
        );
        drop(registry);
        assert_eq!(restored.torrent_limiters.read().await.len(), 3);
        assert_seeder_state_registry_invariant(&restored).await;
        restored.remove_torrent(&eligible, false).await.unwrap();
        restored.remove_torrent(&automatic, false).await.unwrap();
        restored.remove_torrent(&manual, false).await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn boundary_file_bytes_are_exact_after_restore_and_each_recheck() {
        let root = unique_dir("file-boundary-restore-recheck");
        let state_path = root.join("state.json");
        let payload_root = root.join("payload");
        let files = vec![
            (vec!["a.bin".into()], 3),
            (vec!["b.bin".into()], 4),
            (vec!["c.bin".into()], 2),
        ];
        let contents: [&[u8]; 3] = [b"abc", b"defg", b"hi"];
        let bytes =
            swarmotter_core::meta::build_multi_file_torrent("boundary", &files, &contents, 4, None);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), payload_root.clone());
        storage.write_piece(0, b"abcd").await.unwrap();
        storage.write_piece(2, b"i").await.unwrap();

        let mut torrent = Torrent::new(meta.clone(), now());
        torrent.state = TorrentState::Paused;
        torrent.progress.have_piece(0);
        torrent.progress.have_piece(2);
        torrent
            .files
            .iter_mut()
            .for_each(|file| file.bytes_completed = 0);
        torrent.seeding.idle_limit = Some(0);
        crate::state_store::save(
            &state_path,
            &crate::state_store::DaemonState::new(
                vec![torrent],
                QueueState::new(Config::default().queue),
            ),
        )
        .unwrap();

        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(payload_root.display().to_string());
        cfg.torrent.listen_port = 0;
        cfg.network.mode = NetworkContainmentMode::Disabled;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        runtime.restore_persisted_state().await.unwrap();
        let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(restored.bytes_completed(), 5);
        assert_eq!(
            restored
                .files
                .iter()
                .map(|file| file.bytes_completed)
                .collect::<Vec<_>>(),
            vec![3, 1, 1]
        );

        runtime.recheck(&hash).await.unwrap();
        let partial = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(partial.bytes_completed(), 5);
        assert_eq!(
            partial
                .files
                .iter()
                .map(|file| file.bytes_completed)
                .collect::<Vec<_>>(),
            vec![3, 1, 1]
        );

        storage.write_piece(1, b"efgh").await.unwrap();
        runtime.recheck(&hash).await.unwrap();
        let complete = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(complete.bytes_completed(), 9);
        assert_eq!(
            complete
                .files
                .iter()
                .map(|file| file.bytes_completed)
                .collect::<Vec<_>>(),
            vec![3, 4, 2]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn single_file_final_piece_bytes_are_exact_after_restore_and_recheck() {
        let root = unique_dir("single-file-boundary-restore-recheck");
        let state_path = root.join("state.json");
        let payload_root = root.join("payload");
        let content = b"123456789";
        let bytes =
            swarmotter_core::meta::build_single_file_torrent("nine.bin", content, 4, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), payload_root.clone());
        storage.write_piece(2, b"9").await.unwrap();
        let mut torrent = Torrent::new(meta.clone(), now());
        torrent.state = TorrentState::Paused;
        torrent.progress.have_piece(2);
        torrent.files[0].bytes_completed = 0;
        crate::state_store::save(
            &state_path,
            &crate::state_store::DaemonState::new(
                vec![torrent],
                QueueState::new(Config::default().queue),
            ),
        )
        .unwrap();

        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(payload_root.display().to_string());
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.seeding.global_idle_limit = None;
        cfg.seeding.global_ratio_limit = Some(0.0);
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        runtime.restore_persisted_state().await.unwrap();
        let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(restored.bytes_completed(), 1);
        assert_eq!(restored.files[0].bytes_completed, 1);
        runtime.recheck(&hash).await.unwrap();
        let rechecked = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(rechecked.bytes_completed(), 1);
        assert_eq!(rechecked.files[0].bytes_completed, 1);

        storage.write_piece(0, b"1234").await.unwrap();
        storage.write_piece(1, b"5678").await.unwrap();
        runtime.recheck(&hash).await.unwrap();
        let complete = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(complete.bytes_completed(), 9);
        assert_eq!(complete.files[0].bytes_completed, 9);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn torrent_add_rejects_cross_torrent_storage_path_collision() {
        let root = unique_dir("path-collision");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first = swarmotter_core::meta::build_single_file_torrent(
            "shared-name.bin",
            b"first lawful payload",
            8,
            None,
            false,
        );
        let second = swarmotter_core::meta::build_single_file_torrent(
            "shared-name.bin",
            b"different lawful payload",
            8,
            None,
            false,
        );

        runtime
            .add_torrent_file_with_options(first, AddTorrentOptions::new(None, true))
            .await
            .unwrap();
        let error = runtime
            .add_torrent_file_with_options(second, AddTorrentOptions::new(None, true))
            .await
            .unwrap_err();

        assert!(matches!(error, CoreError::Storage(_)));
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_torrent_adds_cannot_claim_the_same_storage_path() {
        let root = unique_dir("concurrent-path-collision");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first = swarmotter_core::meta::build_single_file_torrent(
            "concurrent.bin",
            b"first concurrent payload",
            8,
            None,
            false,
        );
        let second = swarmotter_core::meta::build_single_file_torrent(
            "concurrent.bin",
            b"second concurrent payload",
            8,
            None,
            false,
        );

        let (first, second) = tokio::join!(
            runtime.add_torrent_file_with_options(first, AddTorrentOptions::new(None, true)),
            runtime.add_torrent_file_with_options(second, AddTorrentOptions::new(None, true))
        );
        assert_ne!(first.is_ok(), second.is_ok());
        let error = first.err().or_else(|| second.err()).unwrap();
        assert!(matches!(error, CoreError::Storage(_)));
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn distinct_same_name_magnets_cannot_share_placeholder_paths() {
        let root = unique_dir("magnet-path-collision");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first = "magnet:?xt=urn:btih:0000000000000000000000000000000000000001&dn=shared.bin";
        let second = "magnet:?xt=urn:btih:0000000000000000000000000000000000000002&dn=shared.bin";

        runtime
            .add_magnet_with_options(first, AddTorrentOptions::new(None, true))
            .await
            .unwrap();
        let error = runtime
            .add_magnet_with_options(second, AddTorrentOptions::new(None, true))
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::Storage(_)));
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn durable_restore_rejects_colliding_paths_and_invalid_progress() {
        let root = unique_dir("restore-validation");
        let state_path = root.join("state.json");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.join("payload").display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let first_meta = swarmotter_core::meta::parse_torrent(
            &swarmotter_core::meta::build_single_file_torrent(
                "restored.bin",
                b"first restored payload",
                8,
                None,
                false,
            ),
        )
        .unwrap();
        let second_meta = swarmotter_core::meta::parse_torrent(
            &swarmotter_core::meta::build_single_file_torrent(
                "restored.bin",
                b"second restored payload",
                8,
                None,
                false,
            ),
        )
        .unwrap();
        let first = Torrent::new(first_meta, 1);
        let second = Torrent::new(second_meta, 2);
        crate::state_store::save(
            &state_path,
            &crate::state_store::DaemonState::new(
                vec![first.clone(), second],
                QueueState::new(cfg.queue.clone()),
            ),
        )
        .unwrap();
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg.clone(),
            health.clone(),
            None,
            None,
            Some(state_path.clone()),
            EventBroker::default(),
        );
        assert!(matches!(
            runtime.restore_persisted_state().await.unwrap_err(),
            CoreError::Storage(_)
        ));

        let mut invalid_progress = first;
        invalid_progress.progress.total += 1;
        crate::state_store::save(
            &state_path,
            &crate::state_store::DaemonState::new(
                vec![invalid_progress],
                QueueState::new(cfg.queue.clone()),
            ),
        )
        .unwrap();
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        assert!(matches!(
            runtime.restore_persisted_state().await.unwrap_err(),
            CoreError::Storage(_)
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn state_save_failure_rolls_back_move_and_rename() {
        let root = unique_dir("storage-state-rollback");
        let state_path = root.join("state-target");
        std::fs::create_dir_all(&state_path).unwrap();
        let old_root = root.join("old");
        let new_root = root.join("new");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(old_root.display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        let payload = b"rollback payload";
        let meta = swarmotter_core::meta::parse_torrent(
            &swarmotter_core::meta::build_single_file_torrent(
                "rollback.bin",
                payload,
                8,
                None,
                false,
            ),
        )
        .unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Paused;
        torrent.download_dir = Some(old_root.display().to_string());
        for piece in 0..meta.piece_count() {
            torrent.progress.have_piece(piece);
        }
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
        let before_policy = runtime
            .registry
            .lock()
            .await
            .get(&hash)
            .unwrap()
            .seeding
            .clone();
        let before_status = runtime
            .registry
            .lock()
            .await
            .get(&hash)
            .unwrap()
            .seeding_status;
        assert!(runtime
            .set_torrent_seeding(
                &hash,
                swarmotter_core::ratio::TorrentSeeding {
                    ratio_limit: Some(1.5),
                    idle_limit: Some(30),
                    seed_forever: true,
                },
            )
            .await
            .is_err());
        let after_failed_policy = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(after_failed_policy.seeding, before_policy);
        assert_eq!(after_failed_policy.seeding_status, before_status);
        tokio::fs::create_dir_all(&old_root).await.unwrap();
        tokio::fs::write(old_root.join("rollback.bin"), payload)
            .await
            .unwrap();

        assert!(runtime
            .move_data(&hash, new_root.display().to_string())
            .await
            .is_err());
        assert_eq!(
            tokio::fs::read(old_root.join("rollback.bin"))
                .await
                .unwrap(),
            payload
        );
        assert!(!new_root.join("rollback.bin").exists());
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&hash)
                .unwrap()
                .download_dir
                .as_deref(),
            old_root.to_str()
        );

        assert!(runtime
            .rename_path(&hash, 0, "renamed.bin".into())
            .await
            .is_err());
        assert_eq!(
            tokio::fs::read(old_root.join("rollback.bin"))
                .await
                .unwrap(),
            payload
        );
        assert!(!old_root.join("renamed.bin").exists());
        let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(restored.meta.files[0].path, vec!["rollback.bin"]);
        assert_eq!(restored.files[0].path, "rollback.bin");
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn state_save_failure_rolls_back_torrent_registration() {
        let root = unique_dir("add-state-rollback");
        let state_path = root.join("state-target");
        std::fs::create_dir_all(&state_path).unwrap();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            Config::default(),
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "registration-rollback.bin",
            b"registration rollback payload",
            8,
            None,
            false,
        );

        assert!(runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .is_err());
        assert!(runtime.registry.lock().await.torrents.is_empty());
        assert!(runtime.queue.lock().await.order.is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn seeding_policy_persistence_failure_restores_policy_status_and_state() {
        let root = unique_dir("seeding-policy-state-rollback");
        let state_path = root.join("state-target");
        std::fs::create_dir_all(&state_path).unwrap();
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = 0;
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.seeding.global_ratio_limit = None;
        cfg.seeding.global_idle_limit = None;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        let (hash, limiter) = add_complete_seed_fixture(
            &runtime,
            "policy-rollback.bin",
            b"generated rollback payload",
        )
        .await;
        runtime.reconcile_seeders().await;
        assert_seeder_state_registry_invariant(&runtime).await;
        let before = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(before.state, TorrentState::Seeding);
        assert_eq!(before.seeding_status, SeedingStatus::Active);
        let registered_limiter = runtime
            .seeder_registry
            .limiter_for_test(&hash)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&limiter, &registered_limiter));
        let shutdown = runtime
            .seeder_shutdowns
            .lock()
            .await
            .get(&hash)
            .cloned()
            .unwrap();
        let listener_task = runtime
            .seeder_listener_handle
            .lock()
            .await
            .as_ref()
            .unwrap()
            .id();

        let error = runtime
            .set_torrent_seeding(
                &hash,
                swarmotter_core::ratio::TorrentSeeding {
                    ratio_limit: Some(0.0),
                    idle_limit: None,
                    seed_forever: false,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::Storage(_)));
        let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(restored.seeding, before.seeding);
        assert_eq!(restored.seeding_status, SeedingStatus::Active);
        assert_eq!(restored.state, TorrentState::Seeding);
        assert!(runtime.seeder_registry.contains(&hash).await);
        assert!(runtime
            .seeder_shutdowns
            .lock()
            .await
            .get(&hash)
            .is_some_and(|current| current.same_channel(&shutdown)));
        assert_eq!(
            runtime
                .seeder_listener_handle
                .lock()
                .await
                .as_ref()
                .unwrap()
                .id(),
            listener_task
        );
        assert!(Arc::ptr_eq(
            runtime.torrent_limiters.read().await.get(&hash).unwrap(),
            &limiter
        ));
        assert!(Arc::ptr_eq(
            &runtime
                .seeder_registry
                .limiter_for_test(&hash)
                .await
                .unwrap(),
            &limiter
        ));
        assert_seeder_state_registry_invariant(&runtime).await;
        runtime.force_stop_seeder(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn durable_restore_rejects_invalid_per_torrent_ratio_policy_with_context() {
        let root = unique_dir("invalid-restored-seeding-policy");
        let state_path = root.join("state.json");
        let mut cfg = Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "invalid-policy.bin",
            b"generated invalid policy payload",
            8,
            None,
            false,
        );
        let mut torrent =
            Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), now());
        let hash = torrent.info_hash();
        torrent.seeding.ratio_limit = Some(-1.0);
        crate::state_store::save(
            &state_path,
            &crate::state_store::DaemonState::new(
                vec![torrent],
                QueueState::new(cfg.queue.clone()),
            ),
        )
        .unwrap();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg,
            health,
            None,
            None,
            Some(state_path),
            EventBroker::default(),
        );
        let error = runtime.restore_persisted_state().await.unwrap_err();
        assert!(matches!(error, CoreError::Storage(_)));
        assert!(error.to_string().contains(&hash.to_hex()));
        assert!(error.to_string().contains("seeding.ratio_limit"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn rename_rejects_the_torrents_own_resume_path() {
        let root = unique_dir("rename-resume-collision");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "resume-name.bin",
            b"resume collision payload",
            8,
            None,
            false,
        );
        let hash = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();

        let error = runtime
            .rename_path(&hash, 0, "resume-name.bin.swarmotter.resume".into())
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::Storage(_)));
        assert_eq!(
            runtime.registry.lock().await.get(&hash).unwrap().files[0].path,
            "resume-name.bin"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn recheck_preserves_selected_file_completion() {
        let root = unique_dir("selected-recheck");
        let complete_root = root.join("complete");
        let active_root = root.join("active");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(complete_root.display().to_string());
        cfg.storage.incomplete_dir = Some(active_root.display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first = b"aaaa".as_slice();
        let second = b"bbbb".as_slice();
        let bytes = swarmotter_core::meta::build_multi_file_torrent(
            "selection",
            &[
                (vec!["first.bin".into()], first.len() as u64),
                (vec!["second.bin".into()], second.len() as u64),
            ],
            &[first, second],
            4,
            None,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();
        {
            let mut registry = runtime.registry.lock().await;
            let torrent = registry.get_mut(&hash).unwrap();
            torrent.wanted[1] = false;
            torrent.priorities[1] = FilePriority::Unwanted;
            torrent.files[1].wanted = false;
            torrent.files[1].priority = FilePriority::Unwanted;
            torrent.progress.have_piece(0);
            torrent.state = TorrentState::Completed;
        }
        let storage = swarmotter_core::storage::StorageIo::new(meta, active_root);
        let first_path = storage.file_path(0).unwrap();
        tokio::fs::create_dir_all(first_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(first_path, first).await.unwrap();

        runtime.recheck(&hash).await.unwrap();
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, TorrentState::Completed);
        assert_eq!(torrent.progress.pieces_have(), 1);
        assert!(!torrent.progress.is_complete());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn move_and_rename_update_payload_and_registry_paths() {
        let root = unique_dir("move-rename");
        let old_root = root.join("old");
        let new_root = root.join("new");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(old_root.display().to_string());
        cfg.storage.incomplete_dir = None;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let payload = b"move and rename lawful payload";
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "original.bin",
            payload,
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();
        tokio::fs::create_dir_all(&old_root).await.unwrap();
        tokio::fs::write(old_root.join("original.bin"), payload)
            .await
            .unwrap();
        {
            let mut registry = runtime.registry.lock().await;
            let torrent = registry.get_mut(&hash).unwrap();
            for piece in 0..meta.piece_count() {
                torrent.progress.have_piece(piece);
            }
            torrent.state = TorrentState::Completed;
        }

        tokio::time::timeout(
            Duration::from_secs(5),
            runtime.move_data(&hash, new_root.display().to_string()),
        )
        .await
        .expect("move_data timed out")
        .unwrap();
        assert!(!old_root.join("original.bin").exists());
        assert_eq!(
            tokio::fs::read(new_root.join("original.bin"))
                .await
                .unwrap(),
            payload
        );

        tokio::time::timeout(
            Duration::from_secs(5),
            runtime.rename_path(&hash, 0, "renamed.bin".into()),
        )
        .await
        .expect("rename_path timed out")
        .unwrap();
        assert!(!new_root.join("original.bin").exists());
        assert_eq!(
            tokio::fs::read(new_root.join("renamed.bin")).await.unwrap(),
            payload
        );
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.download_dir.as_deref(), new_root.to_str());
        assert_eq!(torrent.files[0].path, "renamed.bin");
        assert_eq!(torrent.meta.files[0].path, vec!["renamed.bin"]);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_config_replacements_leave_runtime_and_disk_consistent() {
        let root = unique_dir("config-replacement");
        let config_path = root.join("swarmotter.toml");
        let mut cfg = Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::with_paths_and_broker(
            cfg.clone(),
            health,
            Some(config_path.clone()),
            None,
            EventBroker::default(),
        );
        let mut first = cfg.clone();
        first.queue.max_active_downloads = 2;
        let mut second = cfg;
        second.queue.max_active_downloads = 7;

        let (first_result, second_result) = tokio::join!(
            runtime.replace_config(first),
            runtime.replace_config(second)
        );
        first_result.unwrap();
        second_result.unwrap();

        let disk = Config::from_file(&config_path).unwrap();
        let live = runtime.config.read().await.clone();
        assert_eq!(
            disk.to_toml_string().unwrap(),
            live.to_toml_string().unwrap()
        );
        assert_eq!(live.queue.max_active_downloads, 7);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&config_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert!(std::fs::read_dir(&root).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn patch_peer_limits_commits_new_pools_and_reconstructs_live_seeder() {
        let (runtime, hash, root, _) = peer_reconfiguration_fixture("peer-patch-commit").await;
        let previous = runtime.current_peer_permit_configuration().await;
        let (queue_order, queue_bypass) = {
            let queue = runtime.queue.lock().await;
            (queue.order.clone(), queue.bypass.clone())
        };
        let mut bandwidth = runtime.config.read().await.bandwidth.clone();
        bandwidth.max_peers = 1;
        bandwidth.max_peers_per_torrent = 1;

        runtime
            .update_settings(swarmotter_api::state::SettingsPatch {
                bandwidth: Some(bandwidth),
                ..Default::default()
            })
            .await
            .unwrap();

        let current = runtime.current_peer_permit_configuration().await;
        assert_eq!(current.global.snapshot().limit, 1);
        assert_eq!(current.per_torrent[&hash].snapshot().limit, 1);
        assert!(!Arc::ptr_eq(&current.global, &previous.global));
        assert!(!Arc::ptr_eq(
            &current.per_torrent[&hash],
            &previous.per_torrent[&hash]
        ));
        assert!(runtime.seeder_registry.contains(&hash).await);
        let queue = runtime.queue.lock().await;
        assert_eq!(queue.order, queue_order);
        assert_eq!(queue.bypass, queue_bypass);
        drop(queue);
        runtime.force_stop_seeder(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn patch_peer_limits_failure_restores_exact_pools_lifecycle_and_queue() {
        let (runtime, hash, root, config_path) =
            peer_reconfiguration_fixture("peer-patch-rollback").await;
        let previous_config = runtime.config.read().await.clone();
        let previous_permits = runtime.current_peer_permit_configuration().await;
        let previous_file = std::fs::read(&config_path).unwrap();
        let previous_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        let (queue_order, queue_bypass) = {
            let queue = runtime.queue.lock().await;
            (queue.order.clone(), queue.bypass.clone())
        };
        let mut bandwidth = previous_config.bandwidth.clone();
        bandwidth.max_peers = 1;
        bandwidth.max_peers_per_torrent = 1;
        runtime.inject_peer_reconfiguration_failure_after_teardown();

        let error = runtime
            .update_settings(swarmotter_api::state::SettingsPatch {
                bandwidth: Some(bandwidth),
                ..Default::default()
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("provisional install"));
        assert_eq!(
            runtime.config.read().await.to_toml_string().unwrap(),
            previous_config.to_toml_string().unwrap()
        );
        runtime
            .verify_peer_permit_configuration_identity(&previous_permits)
            .await
            .unwrap();
        assert!(runtime.seeder_registry.contains(&hash).await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, previous_torrent.state);
        assert_eq!(torrent.seeding_status, previous_torrent.seeding_status);
        assert_eq!(torrent.error, previous_torrent.error);
        assert_eq!(
            torrent.containment_recovery_intent,
            previous_torrent.containment_recovery_intent
        );
        let queue = runtime.queue.lock().await;
        assert_eq!(queue.order, queue_order);
        assert_eq!(queue.bypass, queue_bypass);
        drop(queue);
        assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
        runtime.force_stop_seeder(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn put_peer_limits_persists_new_pools_and_reconstructs_live_seeder() {
        let (runtime, hash, root, config_path) =
            peer_reconfiguration_fixture("peer-put-commit").await;
        let previous = runtime.current_peer_permit_configuration().await;
        let mut next = runtime.config.read().await.clone();
        next.bandwidth.max_peers = 1;
        next.bandwidth.max_peers_per_torrent = 1;

        runtime.replace_config(next).await.unwrap();

        let current = runtime.current_peer_permit_configuration().await;
        assert_eq!(current.global.snapshot().limit, 1);
        assert_eq!(current.per_torrent[&hash].snapshot().limit, 1);
        assert!(!Arc::ptr_eq(&current.global, &previous.global));
        assert!(!Arc::ptr_eq(
            &current.per_torrent[&hash],
            &previous.per_torrent[&hash]
        ));
        assert_eq!(
            Config::from_file(&config_path).unwrap().bandwidth.max_peers,
            1
        );
        assert!(runtime.seeder_registry.contains(&hash).await);
        runtime.force_stop_seeder(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn put_peer_limits_failure_restores_runtime_file_and_live_ownership() {
        let (runtime, hash, root, config_path) =
            peer_reconfiguration_fixture("peer-put-rollback").await;
        let previous_config = runtime.config.read().await.clone();
        let previous_permits = runtime.current_peer_permit_configuration().await;
        let previous_file = std::fs::read(&config_path).unwrap();
        let previous_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        let mut next = previous_config.clone();
        next.bandwidth.max_peers = 1;
        next.bandwidth.max_peers_per_torrent = 1;
        runtime.inject_peer_reconfiguration_failure_after_teardown();

        let error = runtime.replace_config(next).await.unwrap_err();

        assert!(error.to_string().contains("provisional install"));
        assert_eq!(
            runtime.config.read().await.to_toml_string().unwrap(),
            previous_config.to_toml_string().unwrap()
        );
        runtime
            .verify_peer_permit_configuration_identity(&previous_permits)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
        assert!(runtime.seeder_registry.contains(&hash).await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, previous_torrent.state);
        assert_eq!(torrent.seeding_status, previous_torrent.seeding_status);
        assert_eq!(torrent.error, previous_torrent.error);
        assert_eq!(
            torrent.containment_recovery_intent,
            previous_torrent.containment_recovery_intent
        );
        runtime.force_stop_seeder(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn combined_peer_and_seeding_policy_update_commits_only_eligible_work() {
        let (runtime, hash, root, _) = peer_reconfiguration_fixture("peer-combined-seeding").await;
        runtime
            .registry
            .lock()
            .await
            .get_mut(&hash)
            .unwrap()
            .seeding
            .seed_forever = false;
        let mut next = runtime.config.read().await.clone();
        next.bandwidth.max_peers = 1;
        next.seeding.global_ratio_limit = Some(0.0);

        runtime.replace_config(next).await.unwrap();

        assert!(!runtime.seeder_registry.contains(&hash).await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, TorrentState::Completed);
        assert_eq!(torrent.seeding_status, SeedingStatus::StoppedRatio);
        assert_eq!(runtime.peer_permit_snapshot().await.limit, 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn late_persistence_failure_restores_candidate_only_queued_torrent() {
        let (runtime, first, root, config_path) =
            peer_reconfiguration_fixture("peer-candidate-queued-rollback").await;
        let (second, _) = add_complete_seed_fixture(
            &runtime,
            "candidate-only-seed.bin",
            b"generated candidate-only completed payload",
        )
        .await;
        runtime.reconcile_seeders().await;
        let prior_live = runtime
            .seeder_registry
            .info_hashes()
            .await
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(prior_live.len(), 1);
        let queued = [first, second]
            .into_iter()
            .find(|hash| !prior_live.contains(hash))
            .unwrap();
        let queued_before = runtime.registry.lock().await.get(&queued).cloned().unwrap();
        assert_eq!(queued_before.state, TorrentState::Completed);
        assert_eq!(queued_before.seeding_status, SeedingStatus::Queued);
        let previous_permits = runtime.current_peer_permit_configuration().await;
        let previous_file = std::fs::read(&config_path).unwrap();
        let (queue_order, queue_bypass) = {
            let queue = runtime.queue.lock().await;
            (queue.order.clone(), queue.bypass.clone())
        };
        let mut next = runtime.config.read().await.clone();
        next.bandwidth.max_peers = 1;
        next.queue.max_active_seeds = 2;
        runtime.inject_peer_reconfiguration_persistence_failure();

        assert!(runtime.replace_config(next).await.is_err());

        runtime
            .verify_peer_permit_configuration_identity(&previous_permits)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
        assert_eq!(
            runtime
                .seeder_registry
                .info_hashes()
                .await
                .into_iter()
                .collect::<HashSet<_>>(),
            prior_live
        );
        assert!(!runtime.seeder_registry.contains(&queued).await);
        let queued_after = runtime.registry.lock().await.get(&queued).cloned().unwrap();
        assert_eq!(queued_after.state, queued_before.state);
        assert_eq!(queued_after.seeding_status, queued_before.seeding_status);
        assert_eq!(queued_after.error, queued_before.error);
        assert_eq!(
            queued_after.containment_recovery_intent,
            queued_before.containment_recovery_intent
        );
        let queue = runtime.queue.lock().await;
        assert_eq!(queue.order, queue_order);
        assert_eq!(queue.bypass, queue_bypass);
        drop(queue);
        for hash in [first, second] {
            runtime.force_stop_seeder(&hash).await;
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn failed_candidate_seeder_ownership_does_not_survive_state_reload() {
        let root = unique_dir("peer-state-rollback-reload");
        let config_path = root.join("swarmotter.toml");
        let state_path = root.join("daemon-state.json");
        let mut config = Config::default();
        config.network.mode = NetworkContainmentMode::Disabled;
        config.storage.download_dir = Some(root.display().to_string());
        config.torrent.listen_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        config.queue.max_active_seeds = 1;
        config.seeding.global_ratio_limit = None;
        config.seeding.global_idle_limit = None;
        config.bandwidth.max_peers = 3;
        write_config_atomically(&config_path, &config).unwrap();
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            config.clone(),
            health.clone(),
            Some(config_path.clone()),
            None,
            Some(state_path.clone()),
            EventBroker::default(),
        );
        let (first, _) = add_complete_seed_fixture(
            &runtime,
            "state-rollback-one.bin",
            b"generated state rollback one",
        )
        .await;
        let (second, _) = add_complete_seed_fixture(
            &runtime,
            "state-rollback-two.bin",
            b"generated state rollback two",
        )
        .await;
        runtime.reconcile_seeders().await;
        runtime.persist_state().await.unwrap();
        assert_eq!(runtime.seeder_registry.len().await, 1);
        let prior_live = runtime.seeder_registry.info_hashes().await[0];
        let candidate_only = [first, second]
            .into_iter()
            .find(|hash| *hash != prior_live)
            .unwrap();
        let mut next = config.clone();
        next.bandwidth.max_peers = 1;
        next.queue.max_active_seeds = 2;
        runtime.inject_peer_reconfiguration_persistence_failure();
        assert!(runtime.replace_config(next).await.is_err());
        assert_eq!(runtime.seeder_registry.len().await, 1);
        let stored = crate::state_store::load(&state_path)
            .unwrap()
            .expect("rollback must retain the daemon state file");
        let stored_live = stored
            .torrents
            .iter()
            .find(|torrent| torrent.info_hash() == prior_live)
            .unwrap();
        let stored_candidate = stored
            .torrents
            .iter()
            .find(|torrent| torrent.info_hash() == candidate_only)
            .unwrap();
        assert_eq!(stored_live.state, TorrentState::Seeding);
        assert_eq!(stored_live.seeding_status, SeedingStatus::Active);
        assert_eq!(stored_candidate.state, TorrentState::Completed);
        assert_eq!(stored_candidate.seeding_status, SeedingStatus::Queued);
        for hash in [first, second] {
            runtime.force_stop_seeder(&hash).await;
        }

        let restored = DaemonRuntime::with_paths_broker_and_state(
            config,
            health,
            Some(config_path),
            None,
            Some(state_path),
            EventBroker::default(),
        );
        assert_eq!(restored.restore_persisted_state().await.unwrap(), 2);
        assert_eq!(restored.seeder_registry.len().await, 1);
        let torrents = restored
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            torrents
                .iter()
                .filter(|torrent| torrent.seeding_status == SeedingStatus::Active)
                .count(),
            1
        );
        assert_eq!(
            torrents
                .iter()
                .filter(|torrent| torrent.seeding_status == SeedingStatus::Queued)
                .count(),
            1
        );
        for hash in [first, second] {
            restored.force_stop_seeder(&hash).await;
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn fast_candidate_completion_cannot_selfish_remove_before_failed_persistence() {
        let root = unique_dir("peer-selfish-persistence-rollback");
        let config_path = root.join("swarmotter.toml");
        let mut config = Config::default();
        config.network.mode = NetworkContainmentMode::Disabled;
        config.storage.download_dir = Some(root.display().to_string());
        config.torrent.listen_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        config.torrent.selfish = false;
        config.queue.auto_start = false;
        config.dht.enabled = false;
        config.pex.enabled = false;
        config.bandwidth.max_peers = 3;
        write_config_atomically(&config_path, &config).unwrap();
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_and_broker(
            config.clone(),
            health,
            Some(config_path.clone()),
            None,
            EventBroker::default(),
        );
        let content = b"generated fast completion rollback payload";
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "fast-candidate.bin",
            content,
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), root.clone());
        for piece in 0..meta.piece_count() {
            let start = piece * meta.piece_length as usize;
            let end = (start + meta.piece_length as usize).min(content.len());
            storage
                .write_piece(piece, &content[start..end])
                .await
                .unwrap();
        }
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta, now()))
            .unwrap();
        runtime.queue.lock().await.add(hash);
        runtime.ensure_torrent_peer_permit_pool(hash).await;
        let previous_file = std::fs::read(&config_path).unwrap();
        let (persistence_reached, continue_persistence) = runtime
            .pause_peer_reconfiguration_before_persistence()
            .await;
        runtime.inject_peer_reconfiguration_persistence_failure();
        let mut next = config;
        next.bandwidth.max_peers = 1;
        next.queue.auto_start = true;
        next.torrent.selfish = true;
        let update_runtime = runtime.clone();
        let update = tokio::spawn(async move { update_runtime.replace_config(next).await });
        persistence_reached.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let complete = runtime
                    .registry
                    .lock()
                    .await
                    .get(&hash)
                    .is_some_and(|torrent| torrent.progress.is_complete());
                if complete {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(runtime.registry.lock().await.contains(&hash));
        assert!(!runtime.selfish_completion_enabled.load(Ordering::Acquire));
        continue_persistence.send(()).unwrap();
        assert!(update.await.unwrap().is_err());

        assert!(runtime.registry.lock().await.contains(&hash));
        assert!(!runtime.config.read().await.torrent.selfish);
        assert!(!runtime.selfish_completion_enabled.load(Ordering::Acquire));
        assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
        assert_eq!(
            runtime.registry.lock().await.get(&hash).unwrap().state,
            TorrentState::Queued
        );
        assert_eq!(
            tokio::fs::read(storage.file_path(0).unwrap())
                .await
                .unwrap(),
            content
        );
        runtime.force_stop_engine(&hash).await;
        runtime.force_stop_seeder(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn combined_peer_and_occupied_listener_update_rolls_back_live_seeder() {
        let (runtime, hash, root, config_path) =
            peer_reconfiguration_fixture("peer-combined-listener-rollback").await;
        let occupied = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
        let occupied_port = occupied.local_addr().unwrap().port();
        let previous_config = runtime.config.read().await.clone();
        let previous_permits = runtime.current_peer_permit_configuration().await;
        let previous_file = std::fs::read(&config_path).unwrap();
        let previous_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        let mut next = previous_config.clone();
        next.bandwidth.max_peers = 1;
        next.torrent.listen_port = occupied_port;

        let error = runtime.replace_config(next).await.unwrap_err();

        assert!(error.to_string().contains("reconstruction failed"));
        runtime
            .verify_peer_permit_configuration_identity(&previous_permits)
            .await
            .unwrap();
        assert_eq!(
            runtime.config.read().await.to_toml_string().unwrap(),
            previous_config.to_toml_string().unwrap()
        );
        assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
        assert!(runtime.seeder_registry.contains(&hash).await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, previous_torrent.state);
        assert_eq!(torrent.seeding_status, previous_torrent.seeding_status);
        assert_eq!(torrent.error, previous_torrent.error);
        assert_eq!(
            torrent.containment_recovery_intent,
            previous_torrent.containment_recovery_intent
        );
        runtime.force_stop_seeder(&hash).await;
        drop(occupied);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn active_engine_patch_reconstructs_on_commit_and_exactly_rolls_back_failure() {
        let (runtime, hash, root, _) =
            active_engine_reconfiguration_fixture("active-engine-patch").await;
        let initial = runtime.current_peer_permit_configuration().await;
        let (queue_order, queue_bypass) = {
            let queue = runtime.queue.lock().await;
            (queue.order.clone(), queue.bypass.clone())
        };
        let mut bandwidth = runtime.config.read().await.bandwidth.clone();
        bandwidth.max_peers = 1;
        bandwidth.max_peers_per_torrent = 1;
        runtime
            .update_settings(swarmotter_api::state::SettingsPatch {
                bandwidth: Some(bandwidth),
                ..Default::default()
            })
            .await
            .unwrap();
        let committed = runtime.current_peer_permit_configuration().await;
        assert!(!Arc::ptr_eq(&initial.global, &committed.global));
        assert_eq!(committed.global.snapshot().limit, 1);
        assert!(runtime.engine_running_for_test(&hash).await);

        let committed_config = runtime.config.read().await.clone();
        let committed_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        let mut rejected = committed_config.bandwidth.clone();
        rejected.max_peers = 2;
        rejected.max_peers_per_torrent = 2;
        runtime.inject_peer_reconfiguration_failure_after_teardown();
        assert!(runtime
            .update_settings(swarmotter_api::state::SettingsPatch {
                bandwidth: Some(rejected),
                ..Default::default()
            })
            .await
            .is_err());
        runtime
            .verify_peer_permit_configuration_identity(&committed)
            .await
            .unwrap();
        assert!(runtime.engine_running_for_test(&hash).await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, committed_torrent.state);
        assert_eq!(torrent.error, committed_torrent.error);
        assert_eq!(
            torrent.containment_recovery_intent,
            committed_torrent.containment_recovery_intent
        );
        let queue = runtime.queue.lock().await;
        assert_eq!(queue.order, queue_order);
        assert_eq!(queue.bypass, queue_bypass);
        drop(queue);
        runtime.force_stop_engine(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn unrelated_engine_start_cannot_enter_mid_peer_reconstruction() {
        let (runtime, active_hash, root, _) =
            active_engine_reconfiguration_fixture("peer-start-exclusion").await;
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "unrelated-reconfiguration-start.bin",
            b"generated unrelated queued torrent",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let unrelated_hash = meta.info_hash;
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta, now()))
            .unwrap();
        runtime.queue.lock().await.add(unrelated_hash);
        runtime
            .ensure_torrent_peer_permit_pool(unrelated_hash)
            .await;
        let (reconstruction_reached, continue_reconstruction) = runtime
            .pause_peer_reconfiguration_before_reconstruction()
            .await;
        let update_runtime = runtime.clone();
        let mut bandwidth = runtime.config.read().await.bandwidth.clone();
        bandwidth.max_peers = 1;
        let update = tokio::spawn(async move {
            update_runtime
                .update_settings(swarmotter_api::state::SettingsPatch {
                    bandwidth: Some(bandwidth),
                    ..Default::default()
                })
                .await
        });
        reconstruction_reached.await.unwrap();

        let start_runtime = runtime.clone();
        let unrelated_start =
            tokio::spawn(async move { start_runtime.start_engine(unrelated_hash).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!unrelated_start.is_finished());
        assert!(!runtime.engine_running_for_test(&unrelated_hash).await);
        continue_reconstruction.send(()).unwrap();
        update.await.unwrap().unwrap();
        unrelated_start.await.unwrap();
        assert!(runtime.engine_running_for_test(&active_hash).await);
        assert!(runtime.engine_running_for_test(&unrelated_hash).await);
        runtime.force_stop_engine(&active_hash).await;
        runtime.force_stop_engine(&unrelated_hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn active_engine_put_reconstructs_persists_and_rolls_back_failure() {
        let (runtime, hash, root, config_path) =
            active_engine_reconfiguration_fixture("active-engine-put").await;
        let mut next = runtime.config.read().await.clone();
        next.bandwidth.max_peers = 1;
        next.bandwidth.max_peers_per_torrent = 1;
        runtime.replace_config(next).await.unwrap();
        let committed = runtime.current_peer_permit_configuration().await;
        let committed_config = runtime.config.read().await.clone();
        let committed_file = std::fs::read(&config_path).unwrap();
        let committed_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(committed.global.snapshot().limit, 1);
        assert!(runtime.engine_running_for_test(&hash).await);
        assert_eq!(
            Config::from_file(&config_path).unwrap().bandwidth.max_peers,
            1
        );

        let mut rejected = committed_config.clone();
        rejected.bandwidth.max_peers = 2;
        rejected.bandwidth.max_peers_per_torrent = 2;
        runtime.inject_peer_reconfiguration_failure_after_teardown();
        assert!(runtime.replace_config(rejected).await.is_err());
        runtime
            .verify_peer_permit_configuration_identity(&committed)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&config_path).unwrap(), committed_file);
        assert!(runtime.engine_running_for_test(&hash).await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, committed_torrent.state);
        assert_eq!(torrent.error, committed_torrent.error);
        assert_eq!(
            torrent.containment_recovery_intent,
            committed_torrent.containment_recovery_intent
        );

        let mut persistence_rejected = committed_config.clone();
        persistence_rejected.bandwidth.max_peers = 2;
        persistence_rejected.bandwidth.max_peers_per_torrent = 2;
        runtime.inject_peer_reconfiguration_persistence_failure();
        let error = runtime
            .replace_config(persistence_rejected)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("persistence failed"));
        runtime
            .verify_peer_permit_configuration_identity(&committed)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&config_path).unwrap(), committed_file);
        assert!(runtime.engine_running_for_test(&hash).await);
        runtime.force_stop_engine(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn valid_blocked_peer_reconfiguration_commits_recovery_intent_without_live_tasks() {
        let (runtime, hash, root, config_path) =
            active_engine_reconfiguration_fixture("active-engine-blocked-put").await;
        let previous = runtime.current_peer_permit_configuration().await;
        let mut next = runtime.config.read().await.clone();
        next.bandwidth.max_peers = 1;
        next.network.mode = NetworkContainmentMode::Strict;
        next.network.required_interface = Some(format!(
            "swarmotter-missing-interface-{}",
            std::process::id()
        ));
        next.network.fail_closed = true;

        runtime.replace_config(next.clone()).await.unwrap();

        let current = runtime.current_peer_permit_configuration().await;
        assert!(!Arc::ptr_eq(&current.global, &previous.global));
        assert_eq!(current.global.snapshot().limit, 1);
        assert!(!runtime.engine_running_for_test(&hash).await);
        assert!(runtime.seeder_registry.is_empty().await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, TorrentState::NetworkBlocked);
        assert_eq!(
            torrent.containment_recovery_intent,
            Some(ContainmentRecoveryIntent::Downloading)
        );
        assert_eq!(
            Config::from_file(&config_path).unwrap().bandwidth.max_peers,
            1
        );
        assert!(!runtime.network_health.read().await.traffic_allowed);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn combined_peer_and_blocked_to_healthy_update_recovers_under_transition_lock() {
        let root = unique_dir("peer-blocked-to-healthy");
        let config_path = root.join("swarmotter.toml");
        let mut config = Config::default();
        config.network.mode = NetworkContainmentMode::Strict;
        config.network.required_interface = Some(format!(
            "swarmotter-missing-recovery-interface-{}",
            std::process::id()
        ));
        config.storage.download_dir = Some(root.display().to_string());
        config.torrent.listen_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        config.dht.enabled = false;
        config.pex.enabled = false;
        config.bandwidth.max_peers = 3;
        write_config_atomically(&config_path, &config).unwrap();
        let health = net::evaluate(&config.network, &OsInterfaceProbe);
        assert!(!health.traffic_allowed);
        let runtime = DaemonRuntime::with_paths_and_broker(
            config.clone(),
            health.clone(),
            Some(config_path.clone()),
            None,
            EventBroker::default(),
        );
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "blocked-recovery.bin",
            b"generated blocked recovery torrent",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta, now());
        torrent.state = TorrentState::NetworkBlocked;
        torrent.error = Some(health.detail);
        torrent.containment_recovery_intent = Some(ContainmentRecoveryIntent::Downloading);
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
        runtime.ensure_torrent_peer_permit_pool(hash).await;

        let mut next = config;
        next.network = swarmotter_core::net::NetworkConfig {
            mode: NetworkContainmentMode::Disabled,
            ..Default::default()
        };
        next.bandwidth.max_peers = 1;
        runtime.replace_config(next).await.unwrap();

        assert!(runtime.network_health.read().await.traffic_allowed);
        assert!(runtime.engine_running_for_test(&hash).await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, TorrentState::Downloading);
        assert_eq!(torrent.containment_recovery_intent, None);
        assert_eq!(runtime.peer_permit_snapshot().await.limit, 1);
        assert_eq!(
            Config::from_file(&config_path).unwrap().bandwidth.max_peers,
            1
        );
        runtime.force_stop_engine(&hash).await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_engine_starts_create_one_owned_task() {
        let root = unique_dir("concurrent-engine-start");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = 0;
        cfg.dht.enabled = false;
        cfg.pex.enabled = false;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "single-engine.bin",
            b"single owned engine",
            8,
            None,
            false,
        );
        let hash = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();
        runtime.registry.lock().await.get_mut(&hash).unwrap().state = TorrentState::Queued;

        tokio::join!(runtime.start_engine(hash), runtime.start_engine(hash));

        assert_eq!(runtime.engine_handles.read().await.len(), 1);
        assert_eq!(runtime.engine_cmds.lock().await.len(), 1);
        runtime.force_stop_engine(&hash).await;
        assert!(runtime.engine_handles.read().await.is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn failed_shared_listener_bind_does_not_register_or_announce_seeder() {
        let occupied = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
        let port = occupied.local_addr().unwrap().port();
        let root = unique_dir("seeder-bind-failure");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = port;
        cfg.network.mode = NetworkContainmentMode::Disabled;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "bind-failure.bin",
            b"bind failure payload",
            8,
            Some("http://127.0.0.1:1/announce"),
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Completed;
        torrent.seeding.seed_forever = true;
        for piece in 0..meta.piece_count() {
            torrent.progress.have_piece(piece);
        }
        runtime.registry.lock().await.add(torrent).unwrap();

        runtime.reconcile_seeders().await;

        assert!(!runtime.seeder_shutdowns.lock().await.contains_key(&hash));
        assert!(!runtime.seeder_handles.lock().await.contains_key(&hash));
        assert!(runtime.seeder_registry.is_empty().await);
        let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(torrent.state, TorrentState::Completed);
        assert_eq!(torrent.seeding_status, SeedingStatus::Queued);
        assert!(torrent.error.is_some());
        drop(occupied);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn complete_seeding_lifecycle_policy_slots_tasks_and_limiter_identity_are_truthful() {
        let root = unique_dir("phase4-seeding-lifecycle");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = 0;
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.queue.max_active_seeds = 1;
        cfg.seeding.global_ratio_limit = None;
        cfg.seeding.global_idle_limit = None;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::new(cfg, health);
        let (first, first_limiter) =
            add_complete_seed_fixture(&runtime, "seed-one.bin", b"first generated seed payload")
                .await;
        let (second, second_limiter) =
            add_complete_seed_fixture(&runtime, "seed-two.bin", b"second generated seed payload")
                .await;

        runtime.reconcile_seeders().await;
        assert_seeder_state_registry_invariant(&runtime).await;
        let first_status = runtime
            .registry
            .lock()
            .await
            .get(&first)
            .unwrap()
            .seeding_status;
        let second_status = runtime
            .registry
            .lock()
            .await
            .get(&second)
            .unwrap()
            .seeding_status;
        assert_eq!(
            [first_status, second_status]
                .into_iter()
                .filter(|status| *status == SeedingStatus::Active)
                .count(),
            1
        );
        assert_eq!(
            [first_status, second_status]
                .into_iter()
                .filter(|status| *status == SeedingStatus::Queued)
                .count(),
            1
        );
        assert_eq!(runtime.global_stats().await.active_seeds, 1);

        runtime.config.write().await.queue.max_active_seeds = 2;
        runtime.reconcile_seeders().await;
        assert_seeder_state_registry_invariant(&runtime).await;
        assert_eq!(runtime.global_stats().await.active_seeds, 2);
        let retained = runtime.torrent_limiters.read().await;
        assert!(Arc::ptr_eq(retained.get(&first).unwrap(), &first_limiter));
        assert!(Arc::ptr_eq(retained.get(&second).unwrap(), &second_limiter));
        drop(retained);

        // A complete imported/restored torrent may have no download counter.
        // Explicit zero is still an immediate target through the production
        // policy replacement path; it must not depend on ratio division.
        runtime
            .registry
            .lock()
            .await
            .get_mut(&first)
            .unwrap()
            .downloaded = 0;
        let mut policy_events = runtime.event_broker.subscribe();
        runtime
            .set_torrent_seeding(
                &first,
                swarmotter_core::ratio::TorrentSeeding {
                    ratio_limit: Some(0.0),
                    idle_limit: None,
                    seed_forever: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&first)
                .unwrap()
                .seeding_status,
            SeedingStatus::StoppedRatio
        );
        assert!(!runtime.seeder_registry.contains(&first).await);
        assert_seeder_state_registry_invariant(&runtime).await;
        let stopped_event = loop {
            let event = tokio::time::timeout(Duration::from_secs(1), policy_events.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if event.kind == "torrent_changed"
                && event.info_hash.as_deref() == Some(first.to_hex().as_str())
            {
                break event;
            }
        };
        let stopped_payload: serde_json::Value = serde_json::from_str(&stopped_event.json).unwrap();
        assert_eq!(stopped_payload["payload"]["state"], "completed");

        runtime
            .set_torrent_seeding(
                &first,
                swarmotter_core::ratio::TorrentSeeding {
                    ratio_limit: Some(2.0),
                    idle_limit: None,
                    seed_forever: false,
                },
            )
            .await
            .unwrap();
        assert!(runtime.seeder_registry.contains(&first).await);
        assert_seeder_state_registry_invariant(&runtime).await;

        runtime
            .set_torrent_seeding(
                &first,
                swarmotter_core::ratio::TorrentSeeding {
                    ratio_limit: Some(2.0),
                    idle_limit: Some(0),
                    seed_forever: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&first)
                .unwrap()
                .seeding_status,
            SeedingStatus::StoppedIdle
        );

        runtime
            .set_torrent_seeding(
                &first,
                swarmotter_core::ratio::TorrentSeeding {
                    ratio_limit: Some(0.0),
                    idle_limit: Some(0),
                    seed_forever: true,
                },
            )
            .await
            .unwrap();
        runtime.pause(&first).await.unwrap();
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&first)
                .unwrap()
                .seeding_status,
            SeedingStatus::StoppedManual
        );
        assert!(!runtime.seeder_registry.contains(&first).await);
        assert!(Arc::ptr_eq(
            runtime.torrent_limiters.read().await.get(&first).unwrap(),
            &first_limiter
        ));

        runtime
            .set_torrent_seeding(
                &first,
                swarmotter_core::ratio::TorrentSeeding {
                    ratio_limit: None,
                    idle_limit: None,
                    seed_forever: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&first)
                .unwrap()
                .seeding_status,
            SeedingStatus::StoppedManual,
            "policy updates must not auto-resume a manual pause"
        );
        let mut resume_events = runtime.event_broker.subscribe();
        runtime.resume(&first).await.unwrap();
        assert!(runtime.seeder_registry.contains(&first).await);
        assert_seeder_state_registry_invariant(&runtime).await;
        let resumed_event = loop {
            let event = tokio::time::timeout(Duration::from_secs(1), resume_events.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if event.kind == "torrent_changed" {
                break event;
            }
        };
        let resumed_payload: serde_json::Value = serde_json::from_str(&resumed_event.json).unwrap();
        assert_eq!(resumed_payload["payload"]["state"], "seeding");
        assert_eq!(
            runtime.get_torrent(&first).await.unwrap().state,
            TorrentState::Seeding
        );

        runtime.pause(&first).await.unwrap();
        runtime.start_now(&first).await.unwrap();
        assert!(runtime.seeder_registry.contains(&first).await);
        assert_eq!(
            runtime.get_torrent(&first).await.unwrap().state,
            TorrentState::Seeding
        );
        assert_seeder_state_registry_invariant(&runtime).await;

        runtime.force_stop_seeder(&first).await;
        assert!(!runtime.seeder_registry.contains(&first).await);
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&first)
                .unwrap()
                .seeding_status,
            SeedingStatus::Queued
        );
        runtime.reconcile_seeders().await;
        assert!(runtime.seeder_registry.contains(&first).await);
        assert_seeder_state_registry_invariant(&runtime).await;

        runtime.remove_torrent(&first, false).await.unwrap();
        assert!(!runtime.seeder_registry.contains(&first).await);
        assert!(!runtime.torrent_limiters.read().await.contains_key(&first));
        assert_seeder_state_registry_invariant(&runtime).await;
        runtime.remove_torrent(&second, false).await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn active_seeding_containment_block_preserves_status_and_recovery_rebuilds_task() {
        let root = unique_dir("seeding-containment-recovery");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = 0;
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.seeding.global_ratio_limit = None;
        cfg.seeding.global_idle_limit = None;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::new(cfg, health);
        let (hash, limiter) = add_complete_seed_fixture(
            &runtime,
            "containment-seed.bin",
            b"generated containment seed payload",
        )
        .await;
        runtime.reconcile_seeders().await;
        assert!(runtime.seeder_registry.contains(&hash).await);
        assert_seeder_state_registry_invariant(&runtime).await;

        let mut blocked_events = runtime.event_broker.subscribe();
        runtime
            .transition_data_plane_to_blocked(
                swarmotter_core::models::network::NetworkContainmentStatus::InterfaceMissing,
                "test interface disappeared".into(),
            )
            .await;
        assert!(!runtime.seeder_registry.contains(&hash).await);
        let blocked = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(blocked.state, TorrentState::NetworkBlocked);
        assert_eq!(blocked.seeding_status, SeedingStatus::Active);
        assert_eq!(
            blocked.containment_recovery_intent,
            Some(ContainmentRecoveryIntent::Seeding)
        );
        assert!(Arc::ptr_eq(
            runtime.torrent_limiters.read().await.get(&hash).unwrap(),
            &limiter
        ));
        let blocked_summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(blocked_summary.state, TorrentState::NetworkBlocked);
        assert_eq!(blocked_summary.seeding_status, SeedingStatus::Active);
        assert_eq!(
            runtime
                .list_torrents()
                .await
                .into_iter()
                .find(|summary| summary.info_hash == hash)
                .unwrap()
                .state,
            TorrentState::NetworkBlocked
        );
        assert_eq!(
            runtime.torrent_stats(&hash).await.unwrap().state,
            TorrentState::NetworkBlocked
        );
        assert_eq!(runtime.global_stats().await.active_seeds, 0);
        assert_seeder_state_registry_invariant(&runtime).await;
        let blocked_event = loop {
            let event = tokio::time::timeout(Duration::from_secs(1), blocked_events.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if event.kind == "torrent_changed"
                && event.info_hash.as_deref() == Some(hash.to_hex().as_str())
            {
                break event;
            }
        };
        let blocked_payload: serde_json::Value = serde_json::from_str(&blocked_event.json).unwrap();
        assert_eq!(blocked_payload["payload"]["state"], "network_blocked");

        let mut recovered_health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "recovered",
        );
        recovered_health.traffic_allowed = true;
        let mut recovery_events = runtime.event_broker.subscribe();
        runtime.recover_containment_work(recovered_health).await;
        assert!(runtime.seeder_registry.contains(&hash).await);
        let recovered = runtime.registry.lock().await.get(&hash).cloned().unwrap();
        assert_eq!(recovered.state, TorrentState::Seeding);
        assert_eq!(recovered.seeding_status, SeedingStatus::Active);
        assert!(recovered.containment_recovery_intent.is_none());
        assert!(Arc::ptr_eq(
            runtime.torrent_limiters.read().await.get(&hash).unwrap(),
            &limiter
        ));
        let recovered_summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(recovered_summary.state, TorrentState::Seeding);
        assert_eq!(recovered_summary.seeding_status, SeedingStatus::Active);
        assert_eq!(
            runtime.torrent_stats(&hash).await.unwrap().state,
            TorrentState::Seeding
        );
        assert_eq!(runtime.global_stats().await.active_seeds, 1);
        assert_seeder_state_registry_invariant(&runtime).await;
        let recovery_event = loop {
            let event = tokio::time::timeout(Duration::from_secs(1), recovery_events.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if event.kind == "torrent_changed"
                && event.info_hash.as_deref() == Some(hash.to_hex().as_str())
            {
                break event;
            }
        };
        let payload: serde_json::Value = serde_json::from_str(&recovery_event.json).unwrap();
        assert_eq!(payload["payload"]["state"], "seeding");
        runtime.remove_torrent(&hash, false).await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    /// End-to-end live shaping through the API-facing daemon operation. The
    /// first block consumes the retained limiter's initial 1 KiB burst. The
    /// second remains blocked at 400 ms under 1 KiB/s, then completes at the
    /// bounded 500 ms wake after `set_torrent_limits` raises the live rate.
    #[tokio::test(start_paused = true)]
    async fn daemon_limit_update_changes_active_registered_upload_without_replacement() {
        use swarmotter_core::bandwidth::{RateDirection, TorrentBandwidth};
        use swarmotter_core::peer::{self, Handshake, Message, PeerReader};

        let root = unique_dir("daemon-live-seed-limit");
        let state_path = root.join("state.json");
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.torrent.listen_port = port;
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.seeding.global_ratio_limit = None;
        cfg.seeding.global_idle_limit = None;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            cfg.clone(),
            health,
            None,
            None,
            Some(state_path.clone()),
            EventBroker::default(),
        );
        let content = vec![0x3cu8; 4096];
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "daemon-live-limit.bin",
            &content,
            4096,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), root.clone());
        storage.write_piece(0, &content).await.unwrap();
        let mut torrent = Torrent::new(meta.clone(), now());
        torrent.state = TorrentState::Completed;
        torrent.downloaded = meta.total_length;
        torrent.upload_limit = 1024;
        torrent.date_completed = Some(now());
        torrent.seeding.seed_forever = true;
        torrent.progress.have_piece(0);
        torrent.recompute_file_bytes_completed();
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
        let limiter = runtime.ensure_torrent_limiter(hash, 0, 1024).await;
        runtime.persist_state().await.unwrap();
        runtime.reconcile_seeders().await;
        assert_seeder_state_registry_invariant(&runtime).await;
        let live_state = runtime
            .engine_states
            .read()
            .await
            .get(&hash)
            .cloned()
            .expect("active seeder must retain its live engine state");
        let registered_limiter = runtime
            .seeder_registry
            .limiter_for_test(&hash)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&limiter, &registered_limiter));

        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let (read, mut write) = tokio::io::split(stream);
        peer::write_handshake(
            &mut write,
            &Handshake {
                info_hash: hash,
                peer_id: make_peer_id(),
                reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
            },
        )
        .await
        .unwrap();
        let mut reader = PeerReader::new(read);
        reader.read_handshake().await.unwrap();
        assert!(matches!(
            reader.read_message().await.unwrap(),
            Some(Message::Bitfield { .. })
        ));
        peer::write_message(&mut write, &Message::Interested)
            .await
            .unwrap();
        loop {
            if matches!(reader.read_message().await.unwrap(), Some(Message::Unchoke)) {
                break;
            }
        }

        for offset in [0u32, 1024] {
            peer::write_message(
                &mut write,
                &Message::Request {
                    piece: 0,
                    offset,
                    length: 1024,
                },
            )
            .await
            .unwrap();
            if offset == 0 {
                assert!(matches!(
                    reader.read_message().await.unwrap(),
                    Some(Message::Piece { block, .. }) if block.len() == 1024
                ));
            }
        }

        let second_block = tokio::spawn(async move { reader.read_message().await });
        let dispatch_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while live_state.lock().await.uploaded != 2048 {
            assert!(
                std::time::Instant::now() < dispatch_deadline,
                "second upload request did not reach the live limiter"
            );
            std::thread::yield_now();
            tokio::task::yield_now().await;
        }
        // Accounting occurs immediately before the limiter await. Yield once
        // more so the existing 500 ms sleep is armed before virtual time moves.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(400)).await;
        tokio::task::yield_now().await;
        assert!(!second_block.is_finished());
        runtime
            .set_torrent_limits(
                &hash,
                TorrentBandwidth {
                    download: 0,
                    upload: 4096,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&hash)
                .unwrap()
                .upload_limit,
            4096
        );
        let persisted = crate::state_store::load(&state_path)
            .unwrap()
            .unwrap()
            .torrents
            .into_iter()
            .find(|torrent| torrent.info_hash() == hash)
            .unwrap();
        assert_eq!(persisted.upload_limit, 4096);
        assert!(Arc::ptr_eq(
            runtime.torrent_limiters.read().await.get(&hash).unwrap(),
            &limiter
        ));
        assert!(Arc::ptr_eq(
            &runtime
                .seeder_registry
                .limiter_for_test(&hash)
                .await
                .unwrap(),
            &limiter
        ));
        tokio::time::advance(Duration::from_millis(100)).await;
        for _ in 0..100 {
            if second_block.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            second_block.is_finished(),
            "new 4 KiB/s window was not observed live"
        );
        assert!(matches!(
            second_block.await.unwrap().unwrap(),
            Some(Message::Piece { block, .. }) if block.len() == 1024
        ));
        assert_eq!(limiter.capacity(RateDirection::Upload), 4096);
        assert!(runtime.seeder_registry.contains(&hash).await);
        assert_seeder_state_registry_invariant(&runtime).await;

        runtime.remove_torrent(&hash, false).await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn torrent_add_publishes_event() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let mut events = runtime.event_broker.subscribe();
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "event-add.bin",
            b"event add payload",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();

        assert_eq!(hash, meta.info_hash);
        let event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.kind, "torrent_added");
        assert_eq!(event.info_hash.as_deref(), Some(hash.to_hex().as_str()));
        let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
        assert_eq!(payload["info_hash"], hash.to_hex());
        assert_eq!(payload["payload"]["info_hash"], hash.to_hex());
        assert_eq!(payload["payload"]["state"], "paused");
    }

    #[tokio::test]
    async fn reconcile_publishes_completion_events() {
        let mut cfg = Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.torrent.listen_port = 0;
        cfg.seeding.global_ratio_limit = None;
        cfg.seeding.global_idle_limit = None;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let runtime = DaemonRuntime::new(cfg, health);
        let mut events = runtime.event_broker.subscribe();
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "event-complete.bin",
            b"event complete payload",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Downloading;
        runtime.registry.lock().await.add(torrent).unwrap();
        let mut pieces_have =
            swarmotter_core::storage::resume::PieceBitfield::new(meta.piece_count());
        for piece in 0..meta.piece_count() {
            pieces_have.set(piece);
        }
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: meta.piece_count(),
                total_length: meta.total_length,
                downloaded: meta.total_length,
                pieces_have,
                finished: true,
                ..Default::default()
            })),
        );

        runtime.reconcile_engine_progress().await;

        let mut kinds = Vec::new();
        let mut final_state = None;
        for _ in 0..6 {
            let event = tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if event.kind == "torrent_changed" {
                let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
                if payload["payload"]["state"] == "seeding" {
                    final_state = Some(TorrentState::Seeding);
                }
            }
            kinds.push(event.kind);
            if final_state.is_some()
                && kinds.iter().any(|kind| kind == "torrent_completed")
                && kinds.iter().any(|kind| kind == "stats_updated")
            {
                break;
            }
        }
        assert!(kinds.iter().any(|kind| kind == "torrent_changed"));
        assert!(kinds.iter().any(|kind| kind == "torrent_completed"));
        assert!(kinds.iter().any(|kind| kind == "stats_updated"));
        assert_eq!(final_state, Some(TorrentState::Seeding));
        assert_eq!(
            runtime.get_torrent(&hash).await.unwrap().state,
            TorrentState::Seeding
        );
        assert!(runtime.seeder_registry.contains(&hash).await);
        runtime.force_stop_engine(&hash).await;
    }

    #[tokio::test]
    async fn reconcile_updates_transfer_rates_and_global_stats() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "rates.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;

        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta.clone(), 1))
            .unwrap();
        let state = Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            downloaded: 5_000,
            uploaded: 1_200,
            ..Default::default()
        }));
        runtime
            .engine_states
            .write()
            .await
            .insert(hash, state.clone());
        runtime.rate_samples.write().await.insert(
            hash,
            RateSample {
                downloaded: 1_000,
                uploaded: 200,
                rate_down: 100,
                rate_up: 100,
                last_download_at: None,
                last_upload_at: None,
                no_download_since: None,
                at: Instant::now() - Duration::from_secs(2),
                peak_rate_down: 0,
                peak_rate_up: 0,
            },
        );

        runtime.reconcile_engine_progress().await;
        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert!(summary.rate_down > 0);
        assert!(summary.rate_up > 0);
        assert_eq!(summary.downloaded, 5_000);
        assert_eq!(summary.uploaded, 1_200);
        let peak_sample = runtime
            .rate_samples
            .read()
            .await
            .get(&hash)
            .copied()
            .unwrap();
        assert!(peak_sample.peak_rate_down >= summary.rate_down);
        assert!(peak_sample.peak_rate_up >= summary.rate_up);
        assert!(
            peak_sample.peak_rate_down > summary.rate_down,
            "observed instantaneous peak should not be capped to the smoothed rate"
        );

        let stats = runtime.global_stats().await;
        assert_eq!(stats.download_rate, summary.rate_down);
        assert_eq!(stats.upload_rate, summary.rate_up);
        assert_eq!(stats.total_downloaded, 5_000);
        assert_eq!(stats.total_uploaded, 1_200);
    }

    #[tokio::test]
    async fn reconcile_applies_resolved_magnet_metadata_while_engine_runs() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let real_bytes = swarmotter_core::meta::build_single_file_torrent(
            "resolved-magnet.bin",
            b"resolved magnet payload",
            8,
            None,
            false,
        );
        let real_meta = swarmotter_core::meta::parse_torrent(&real_bytes).unwrap();
        let hash = real_meta.info_hash;
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "magnet placeholder",
            b"placeholder",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let mut torrent = Torrent::new(placeholder_meta, 1);
        torrent.state = TorrentState::DownloadingMetadata;
        torrent.needs_metadata = true;
        torrent.magnet_info_hash = Some(hash);
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.engine_handles.write().await.insert(
            hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );

        let mut pieces_have =
            swarmotter_core::storage::resume::PieceBitfield::new(real_meta.piece_count());
        pieces_have.set(0);
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                pieces_have,
                piece_count: real_meta.piece_count(),
                total_length: real_meta.total_length,
                resolved_meta: Some(real_meta.clone()),
                ..Default::default()
            })),
        );

        runtime.reconcile_engine_progress().await;
        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(summary.state, TorrentState::Downloading);
        assert_eq!(summary.name, "resolved-magnet.bin");
        assert_eq!(summary.total_length, real_meta.total_length);
        assert_eq!(summary.piece_count, real_meta.piece_count());
        assert_eq!(summary.pieces_have, 1);
        assert!(summary.bytes_completed <= summary.total_length);
        assert!(summary.progress() <= 1.0);

        let reg = runtime.registry.lock().await;
        let torrent = reg.get(&hash).unwrap();
        assert!(!torrent.needs_metadata);
        assert_eq!(torrent.progress.total, real_meta.piece_count());
        assert_eq!(torrent.files[0].path, "resolved-magnet.bin");
        drop(reg);
        runtime.force_stop_engine(&hash).await;
    }

    #[tokio::test]
    async fn reconcile_keeps_unresolved_magnet_in_metadata_state() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "magnet placeholder",
            b"placeholder",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let hash =
            swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
                .unwrap();
        let mut torrent = Torrent::new(placeholder_meta, 1);
        torrent.state = TorrentState::Queued;
        torrent.needs_metadata = true;
        torrent.magnet_info_hash = Some(hash);
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.engine_handles.write().await.insert(
            hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                tracker_message: Some("fetching metadata via BEP 9".into()),
                ..Default::default()
            })),
        );

        runtime.reconcile_engine_progress().await;
        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(summary.state, TorrentState::DownloadingMetadata);
        assert_eq!(summary.total_length, "placeholder".len() as u64);

        runtime.force_stop_engine(&hash).await;
    }

    #[tokio::test]
    async fn retryable_magnet_metadata_no_peers_stays_queued_after_progress_reconcile() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "magnet placeholder",
            b"placeholder",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let hash =
            swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
                .unwrap();
        let piece_count = placeholder_meta.piece_count();
        let total_length = placeholder_meta.total_length;
        let mut torrent = Torrent::new(placeholder_meta, 1);
        torrent.state = TorrentState::DownloadingMetadata;
        torrent.needs_metadata = true;
        torrent.magnet_info_hash = Some(hash);
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count,
                total_length,
                ..Default::default()
            })),
        );

        let retry = runtime
            .handle_engine_task_error(
                hash,
                true,
                CoreError::Internal(
                    "magnet metadata fetch failed after discovery retries: internal error: magnet metadata fetch: no peers discovered"
                        .into(),
                ),
            )
            .await;

        assert!(retry);
        {
            let reg = runtime.registry.lock().await;
            let torrent = reg.get(&hash).unwrap();
            assert_eq!(torrent.state, TorrentState::Queued);
            assert_eq!(
                torrent.error.as_deref(),
                Some(MAGNET_METADATA_NO_PEERS_RETRY_MESSAGE)
            );
        }
        assert!(runtime
            .engine_retry_after
            .read()
            .await
            .get(&hash)
            .is_some_and(|retry_at| *retry_at > Instant::now()));
        assert!(
            runtime.desired_download_hashes().await.is_empty(),
            "retry backoff should keep no-peer magnets out of active queue slots"
        );

        runtime.reconcile_engine_progress().await;

        let reg = runtime.registry.lock().await;
        let torrent = reg.get(&hash).unwrap();
        assert_eq!(
            torrent.state,
            TorrentState::Queued,
            "stale engine diagnostics must not reactivate a magnet queued for metadata retry"
        );
    }

    #[tokio::test]
    async fn unfinished_engine_exit_requeues_and_releases_active_slot() {
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 1;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first_bytes = swarmotter_core::meta::build_single_file_torrent(
            "unfinished-first.bin",
            b"unfinished first payload",
            8,
            None,
            false,
        );
        let second_bytes = swarmotter_core::meta::build_single_file_torrent(
            "unfinished-second.bin",
            b"unfinished second payload",
            8,
            None,
            false,
        );
        let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
        let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
        let first_hash = first.info_hash;
        let second_hash = second.info_hash;
        let mut first_torrent = Torrent::new(first, 1);
        first_torrent.state = TorrentState::Downloading;
        {
            let mut reg = runtime.registry.lock().await;
            reg.add(first_torrent).unwrap();
            reg.add(Torrent::new(second, 2)).unwrap();
        }
        {
            let mut queue = runtime.queue.lock().await;
            queue.add(first_hash);
            queue.add(second_hash);
        }

        let queued = runtime
            .queue_torrent_for_retry(
                first_hash,
                "engine stopped before completion; queued for retry",
                ENGINE_INCOMPLETE_RETRY_DELAY,
            )
            .await;

        assert!(queued);
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&first_hash)
                .unwrap()
                .state,
            TorrentState::Queued
        );
        assert_eq!(runtime.queue.lock().await.position(&second_hash), Some(1));
        assert_eq!(runtime.queue.lock().await.position(&first_hash), Some(2));
        assert!(runtime
            .engine_retry_after
            .read()
            .await
            .get(&first_hash)
            .is_some_and(|retry_at| *retry_at > Instant::now()));
        assert_eq!(runtime.desired_download_hashes().await, vec![second_hash]);
    }

    #[tokio::test]
    async fn stale_active_without_engine_is_requeued_and_releases_active_slot() {
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 1;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let stale_bytes = swarmotter_core::meta::build_single_file_torrent(
            "stale-active.bin",
            b"stale active payload",
            8,
            None,
            false,
        );
        let queued_bytes = swarmotter_core::meta::build_single_file_torrent(
            "queued-behind-stale.bin",
            b"queued behind stale payload",
            8,
            None,
            false,
        );
        let stale_meta = swarmotter_core::meta::parse_torrent(&stale_bytes).unwrap();
        let queued_meta = swarmotter_core::meta::parse_torrent(&queued_bytes).unwrap();
        let stale_hash = stale_meta.info_hash;
        let queued_hash = queued_meta.info_hash;
        let mut stale_torrent = Torrent::new(stale_meta, 1);
        stale_torrent.state = TorrentState::Downloading;
        {
            let mut reg = runtime.registry.lock().await;
            reg.add(stale_torrent).unwrap();
            reg.add(Torrent::new(queued_meta, 2)).unwrap();
        }
        {
            let mut queue = runtime.queue.lock().await;
            queue.add(stale_hash);
            queue.add(queued_hash);
        }

        let recovered = runtime.sweep_stale_active_torrents("test").await;

        assert_eq!(recovered, 1);
        {
            let reg = runtime.registry.lock().await;
            let torrent = reg.get(&stale_hash).unwrap();
            assert_eq!(torrent.state, TorrentState::Queued);
            assert_eq!(
                torrent.error.as_deref(),
                Some(STALE_ACTIVE_RECOVERY_MESSAGE)
            );
        }
        assert_eq!(runtime.queue.lock().await.position(&queued_hash), Some(1));
        assert_eq!(runtime.queue.lock().await.position(&stale_hash), Some(2));
        assert_eq!(runtime.desired_download_hashes().await, vec![queued_hash]);
    }

    #[tokio::test]
    async fn stale_metadata_progress_does_not_reactivate_large_queue_above_limit() {
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 50;
        cfg.queue.max_active_metadata_fetches = 50;
        cfg.queue.auto_start = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "magnet placeholder",
            b"placeholder",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();

        {
            let mut reg = runtime.registry.lock().await;
            let mut queue = runtime.queue.lock().await;
            let mut states = runtime.engine_states.write().await;
            for idx in 1..=100u8 {
                let hash = InfoHash::from_bytes([idx; 20]);
                let mut torrent = Torrent::new(placeholder_meta.clone(), idx as u64);
                torrent.state = TorrentState::DownloadingMetadata;
                torrent.needs_metadata = true;
                torrent.magnet_info_hash = Some(hash);
                reg.add(torrent).unwrap();
                queue.add(hash);
                states.insert(
                    hash,
                    Arc::new(Mutex::new(EngineState {
                        piece_count: placeholder_meta.piece_count(),
                        total_length: placeholder_meta.total_length,
                        ..Default::default()
                    })),
                );
            }
        }

        let recovered = runtime.sweep_stale_active_torrents("test").await;
        assert_eq!(recovered, 100);

        runtime.reconcile_engine_progress().await;

        let active_count = runtime
            .registry
            .lock()
            .await
            .torrents
            .values()
            .filter(|torrent| {
                matches!(
                    torrent.state,
                    TorrentState::Downloading | TorrentState::DownloadingMetadata
                )
            })
            .count();
        assert_eq!(
            active_count, 0,
            "retained metadata diagnostics must not bypass active queue limits"
        );
        assert_eq!(runtime.desired_download_hashes().await.len(), 50);
    }

    #[tokio::test]
    async fn ten_thousand_stale_metadata_records_recover_without_active_leak() {
        const TOTAL_TORRENTS: usize = 10_000;

        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 50;
        cfg.queue.max_active_metadata_fetches = 50;
        cfg.queue.auto_start = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "magnet placeholder",
            b"placeholder",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let hashes = (0..TOTAL_TORRENTS)
            .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
            .collect::<Vec<_>>();

        {
            let mut reg = runtime.registry.lock().await;
            for (idx, hash) in hashes.iter().copied().enumerate() {
                let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
                torrent.state = TorrentState::DownloadingMetadata;
                torrent.needs_metadata = true;
                torrent.magnet_info_hash = Some(hash);
                reg.add(torrent).unwrap();
            }
        }
        runtime.queue.lock().await.add_many(hashes.iter().copied());

        let recovered = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.sweep_stale_active_torrents("test"),
        )
        .await
        .expect("stale active recovery should be bounded for 10,000 records");

        assert_eq!(recovered, TOTAL_TORRENTS);
        let reg = runtime.registry.lock().await;
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| {
                    matches!(
                        torrent.state,
                        TorrentState::Downloading | TorrentState::DownloadingMetadata
                    )
                })
                .count(),
            0
        );
        drop(reg);
        assert_eq!(runtime.desired_download_hashes().await.len(), 50);
        assert_eq!(runtime.queue.lock().await.order.len(), TOTAL_TORRENTS);
    }

    #[tokio::test]
    async fn ten_thousand_metadata_retry_backoffs_leave_no_active_desired_slots() {
        const TOTAL_TORRENTS: usize = 10_000;

        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 50;
        cfg.queue.max_active_metadata_fetches = 50;
        cfg.queue.auto_start = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "magnet placeholder",
            b"placeholder",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let hashes = (0..TOTAL_TORRENTS)
            .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
            .collect::<Vec<_>>();

        {
            let mut reg = runtime.registry.lock().await;
            for (idx, hash) in hashes.iter().copied().enumerate() {
                let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
                torrent.state = TorrentState::Queued;
                torrent.needs_metadata = true;
                torrent.magnet_info_hash = Some(hash);
                reg.add(torrent).unwrap();
            }
        }
        runtime.queue.lock().await.add_many(hashes.iter().copied());
        {
            let mut retry_after = runtime.engine_retry_after.write().await;
            let retry_until = Instant::now() + MAGNET_METADATA_NO_PEERS_RETRY_DELAY;
            for hash in &hashes {
                retry_after.insert(*hash, retry_until);
            }
        }

        let desired =
            tokio::time::timeout(Duration::from_secs(5), runtime.desired_download_hashes())
                .await
                .expect("desired active planning should be bounded for 10,000 retrying magnets");

        assert!(desired.is_empty());
        assert_eq!(runtime.queue.lock().await.order.len(), TOTAL_TORRENTS);
    }

    #[tokio::test]
    #[ignore = "scale regression: mixed lifecycle states at 1k+ records"]
    async fn ignored_thousand_mixed_state_torrents_keep_scheduler_bounds() {
        const TOTAL_TORRENTS: usize = 1_200;
        const MAX_ACTIVE_DOWNLOADS: usize = 32;
        const MAX_ACTIVE_METADATA_FETCHES: usize = 24;
        const LIVE_DOWNLOAD_COUNT: usize = 20;
        const LIVE_METADATA_COUNT: usize = 16;
        const STALE_DOWNLOAD_COUNT: usize = 40;
        const STALE_METADATA_COUNT: usize = 44;
        const QUEUED_DOWNLOAD_COUNT: usize = 260;
        const QUEUED_METADATA_COUNT: usize = 220;
        const BACKOFF_METADATA_COUNT: usize = 150;
        const COMPLETED_COUNT: usize = 120;
        const PAUSED_COUNT: usize = 100;
        const SEEDING_COUNT: usize = 60;
        const CHECKING_COUNT: usize = 50;
        const ERROR_COUNT: usize = 40;
        const NETWORK_BLOCKED_COUNT: usize = 30;
        const STORAGE_ERROR_COUNT: usize = 25;
        const TRACKER_ERROR_COUNT: usize = 25;
        const LIVE_METADATA_START: usize = LIVE_DOWNLOAD_COUNT;
        const STALE_DOWNLOAD_START: usize = LIVE_METADATA_START + LIVE_METADATA_COUNT;
        const STALE_METADATA_START: usize = STALE_DOWNLOAD_START + STALE_DOWNLOAD_COUNT;
        const QUEUED_DOWNLOAD_START: usize = STALE_METADATA_START + STALE_METADATA_COUNT;
        const QUEUED_METADATA_START: usize = QUEUED_DOWNLOAD_START + QUEUED_DOWNLOAD_COUNT;
        const BACKOFF_METADATA_START: usize = QUEUED_METADATA_START + QUEUED_METADATA_COUNT;
        const COMPLETED_START: usize = BACKOFF_METADATA_START + BACKOFF_METADATA_COUNT;
        const PAUSED_START: usize = COMPLETED_START + COMPLETED_COUNT;
        const SEEDING_START: usize = PAUSED_START + PAUSED_COUNT;
        const CHECKING_START: usize = SEEDING_START + SEEDING_COUNT;
        const ERROR_START: usize = CHECKING_START + CHECKING_COUNT;
        const NETWORK_BLOCKED_START: usize = ERROR_START + ERROR_COUNT;
        const STORAGE_ERROR_START: usize = NETWORK_BLOCKED_START + NETWORK_BLOCKED_COUNT;
        const TRACKER_ERROR_START: usize = STORAGE_ERROR_START + STORAGE_ERROR_COUNT;

        assert_eq!(TRACKER_ERROR_START + TRACKER_ERROR_COUNT, TOTAL_TORRENTS);

        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = MAX_ACTIVE_DOWNLOADS;
        cfg.queue.max_active_metadata_fetches = MAX_ACTIVE_METADATA_FETCHES;
        cfg.queue.auto_start = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);

        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "mixed-state placeholder",
            b"placeholder payload",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let hashes = (0..TOTAL_TORRENTS)
            .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
            .collect::<Vec<_>>();
        let backoff_start = Instant::now();

        {
            let mut reg = runtime.registry.lock().await;
            for (idx, hash) in hashes.iter().copied().enumerate() {
                let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
                torrent.magnet_info_hash = Some(hash);
                if idx < LIVE_DOWNLOAD_COUNT {
                    torrent.state = TorrentState::Downloading;
                    torrent.needs_metadata = false;
                } else if idx < LIVE_METADATA_START + LIVE_METADATA_COUNT {
                    torrent.state = TorrentState::DownloadingMetadata;
                    torrent.needs_metadata = true;
                } else if idx < STALE_DOWNLOAD_START + STALE_DOWNLOAD_COUNT {
                    torrent.state = TorrentState::Downloading;
                    torrent.needs_metadata = false;
                } else if idx < STALE_METADATA_START + STALE_METADATA_COUNT {
                    torrent.state = TorrentState::DownloadingMetadata;
                    torrent.needs_metadata = true;
                } else if idx < QUEUED_DOWNLOAD_START + QUEUED_DOWNLOAD_COUNT {
                    torrent.state = TorrentState::Queued;
                    torrent.needs_metadata = false;
                } else if idx < BACKOFF_METADATA_START + BACKOFF_METADATA_COUNT {
                    torrent.state = TorrentState::Queued;
                    torrent.needs_metadata = true;
                } else if idx < COMPLETED_START + COMPLETED_COUNT {
                    torrent.state = TorrentState::Completed;
                    torrent.date_completed = Some((idx + 1) as u64);
                    torrent.needs_metadata = false;
                } else if idx < PAUSED_START + PAUSED_COUNT {
                    torrent.state = TorrentState::Paused;
                    torrent.needs_metadata = false;
                } else if idx < SEEDING_START + SEEDING_COUNT {
                    torrent.state = TorrentState::Seeding;
                    torrent.needs_metadata = false;
                } else if idx < CHECKING_START + CHECKING_COUNT {
                    torrent.state = TorrentState::Checking;
                    torrent.needs_metadata = false;
                } else {
                    torrent.state = if idx < ERROR_START + ERROR_COUNT {
                        TorrentState::Error
                    } else if idx < NETWORK_BLOCKED_START + NETWORK_BLOCKED_COUNT {
                        TorrentState::NetworkBlocked
                    } else if idx < STORAGE_ERROR_START + STORAGE_ERROR_COUNT {
                        TorrentState::StorageError
                    } else {
                        TorrentState::TrackerError
                    };
                    torrent.needs_metadata = false;
                    torrent.error = Some("mixed-state scale fixture error".to_string());
                }
                reg.add(torrent).unwrap();
            }
        }

        runtime.queue.lock().await.add_many(hashes.iter().copied());
        {
            let mut handles = runtime.engine_handles.write().await;
            for hash in hashes.iter().take(LIVE_DOWNLOAD_COUNT).chain(
                hashes
                    .iter()
                    .skip(LIVE_METADATA_START)
                    .take(LIVE_METADATA_COUNT),
            ) {
                handles.insert(
                    *hash,
                    tokio::spawn(async {
                        std::future::pending::<()>().await;
                    }),
                );
            }
        }
        {
            let mut retry_after = runtime.engine_retry_after.write().await;
            for hash in hashes
                .iter()
                .skip(BACKOFF_METADATA_START)
                .take(BACKOFF_METADATA_COUNT)
            {
                retry_after.insert(*hash, backoff_start + Duration::from_secs(60));
            }
        }

        let reg = runtime.registry.lock().await;
        assert_eq!(reg.torrents.len(), TOTAL_TORRENTS);
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Queued)
                .count(),
            QUEUED_DOWNLOAD_COUNT + QUEUED_METADATA_COUNT + BACKOFF_METADATA_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::DownloadingMetadata)
                .count(),
            LIVE_METADATA_COUNT + STALE_METADATA_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Downloading)
                .count(),
            LIVE_DOWNLOAD_COUNT + STALE_DOWNLOAD_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Completed)
                .count(),
            COMPLETED_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Paused)
                .count(),
            PAUSED_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Seeding)
                .count(),
            SEEDING_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Checking)
                .count(),
            CHECKING_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::Error)
                .count(),
            ERROR_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::NetworkBlocked)
                .count(),
            NETWORK_BLOCKED_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::StorageError)
                .count(),
            STORAGE_ERROR_COUNT
        );
        assert_eq!(
            reg.torrents
                .values()
                .filter(|torrent| torrent.state == TorrentState::TrackerError)
                .count(),
            TRACKER_ERROR_COUNT
        );
        drop(reg);

        let stale_recovered = runtime.sweep_stale_active_torrents("scale_test").await;
        assert_eq!(stale_recovered, STALE_DOWNLOAD_COUNT + STALE_METADATA_COUNT);

        let desired =
            tokio::time::timeout(Duration::from_secs(5), runtime.desired_download_hashes())
                .await
                .expect("mixed-state scheduler planning should remain bounded for 1,200 records");
        assert_eq!(
            desired.len(),
            MAX_ACTIVE_DOWNLOADS + MAX_ACTIVE_METADATA_FETCHES
        );
        let desired_backoff_hashes = hashes
            .iter()
            .skip(BACKOFF_METADATA_START)
            .take(BACKOFF_METADATA_COUNT)
            .copied()
            .collect::<Vec<_>>();
        assert!(desired
            .iter()
            .all(|hash| !desired_backoff_hashes.contains(hash)));
        {
            let reg = runtime.registry.lock().await;
            assert_eq!(
                desired
                    .iter()
                    .filter(|hash| reg.get(hash).is_some_and(|torrent| torrent.needs_metadata))
                    .count(),
                MAX_ACTIVE_METADATA_FETCHES
            );
        }

        let stats = runtime.global_stats().await;
        assert_eq!(
            stats.scheduler.requested_downloads,
            LIVE_DOWNLOAD_COUNT + STALE_DOWNLOAD_COUNT + QUEUED_DOWNLOAD_COUNT
        );
        assert_eq!(
            stats.scheduler.requested_metadata_fetches,
            LIVE_METADATA_COUNT + STALE_METADATA_COUNT + QUEUED_METADATA_COUNT
        );
        assert_eq!(stats.scheduler.granted_downloads, MAX_ACTIVE_DOWNLOADS);
        assert_eq!(
            stats.scheduler.granted_metadata_fetches,
            MAX_ACTIVE_METADATA_FETCHES
        );
        assert_eq!(
            stats.scheduler.retry_backoff_torrents,
            BACKOFF_METADATA_COUNT
        );
        assert_eq!(
            stats.scheduler.queued_torrents,
            QUEUED_DOWNLOAD_COUNT
                + QUEUED_METADATA_COUNT
                + BACKOFF_METADATA_COUNT
                + STALE_DOWNLOAD_COUNT
                + STALE_METADATA_COUNT
        );
        assert_eq!(
            stats.scheduler.running_engines,
            LIVE_DOWNLOAD_COUNT + LIVE_METADATA_COUNT
        );
        assert_eq!(stats.scheduler.running_downloads, LIVE_DOWNLOAD_COUNT);
        assert_eq!(
            stats.scheduler.running_metadata_fetches,
            LIVE_METADATA_COUNT
        );
        assert_eq!(stats.scheduler.active_download_limit, MAX_ACTIVE_DOWNLOADS);
        assert_eq!(
            stats.scheduler.active_metadata_fetch_limit,
            MAX_ACTIVE_METADATA_FETCHES
        );
        assert_eq!(
            runtime.active_download_hashes().await.len(),
            LIVE_DOWNLOAD_COUNT + LIVE_METADATA_COUNT
        );
        assert_eq!(
            runtime.engine_retry_after.read().await.len(),
            BACKOFF_METADATA_COUNT
        );
        assert!(stats.scheduler.download_slots_saturated);
        assert!(stats.scheduler.metadata_fetch_slots_saturated);

        for hash in hashes.iter().take(LIVE_DOWNLOAD_COUNT).chain(
            hashes
                .iter()
                .skip(LIVE_METADATA_START)
                .take(LIVE_METADATA_COUNT),
        ) {
            runtime.force_stop_engine(hash).await;
        }
    }

    #[tokio::test]
    async fn metadata_fetch_limit_is_separate_from_download_slot_limit() {
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 2;
        cfg.queue.max_active_metadata_fetches = 3;
        cfg.queue.auto_start = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "magnet placeholder",
            b"placeholder",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let mut metadata_hashes = Vec::new();
        let mut download_hashes = Vec::new();

        {
            let mut reg = runtime.registry.lock().await;
            let mut queue = runtime.queue.lock().await;
            for idx in 0..6u32 {
                let hash = InfoHash::from_bytes(scale_hash_bytes(idx));
                let mut torrent = Torrent::new(placeholder_meta.clone(), idx as u64 + 1);
                torrent.state = TorrentState::Queued;
                torrent.needs_metadata = true;
                torrent.magnet_info_hash = Some(hash);
                reg.add(torrent).unwrap();
                queue.add(hash);
                metadata_hashes.push(hash);
            }
            for idx in 0..5u32 {
                let name = format!("resolved-download-{idx}.bin");
                let payload = format!("resolved download payload {idx}");
                let bytes = swarmotter_core::meta::build_single_file_torrent(
                    &name,
                    payload.as_bytes(),
                    8,
                    None,
                    false,
                );
                let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
                let hash = meta.info_hash;
                reg.add(Torrent::new(meta, idx as u64 + 10)).unwrap();
                queue.add(hash);
                download_hashes.push(hash);
            }
        }

        let desired = runtime.desired_download_hashes().await;

        assert_eq!(
            desired
                .iter()
                .filter(|hash| metadata_hashes.contains(hash))
                .count(),
            3
        );
        assert_eq!(
            desired
                .iter()
                .filter(|hash| download_hashes.contains(hash))
                .count(),
            2
        );
        assert_eq!(desired.len(), 5);

        let stats = runtime.global_stats().await;
        assert_eq!(stats.scheduler.requested_metadata_fetches, 6);
        assert_eq!(stats.scheduler.granted_metadata_fetches, 3);
        assert_eq!(stats.scheduler.requested_downloads, 5);
        assert_eq!(stats.scheduler.granted_downloads, 2);
        assert_eq!(stats.scheduler.active_metadata_fetch_limit, 3);
        assert_eq!(stats.scheduler.active_download_limit, 2);
        assert!(stats.scheduler.metadata_fetch_slots_saturated);
        assert!(stats.scheduler.download_slots_saturated);
    }

    #[tokio::test]
    async fn queued_torrent_with_stale_engine_handle_is_cleared_for_restart() {
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 1;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "stale-queued-handle.bin",
            b"stale queued handle payload",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta, 1))
            .unwrap();
        runtime.queue.lock().await.add(hash);
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        runtime.engine_cmds.lock().await.insert(hash, tx);
        runtime.engine_handles.write().await.insert(
            hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
        runtime
            .engine_states
            .write()
            .await
            .insert(hash, Arc::new(Mutex::new(EngineState::default())));

        let recovered = tokio::time::timeout(
            Duration::from_millis(100),
            runtime.sweep_inactive_engine_handles("test"),
        )
        .await
        .expect("stale queued handles should be force-cleared promptly");

        assert_eq!(recovered, 1);
        assert!(!runtime.engine_handles.read().await.contains_key(&hash));
        assert!(!runtime.engine_cmds.lock().await.contains_key(&hash));
        assert!(!runtime.engine_states.read().await.contains_key(&hash));
        {
            let reg = runtime.registry.lock().await;
            let torrent = reg.get(&hash).unwrap();
            assert_eq!(torrent.state, TorrentState::Queued);
            assert_eq!(
                torrent.error.as_deref(),
                Some(STALE_INACTIVE_ENGINE_RECOVERY_MESSAGE)
            );
        }
        assert_eq!(runtime.desired_download_hashes().await, vec![hash]);
    }

    #[tokio::test]
    async fn reconcile_queue_force_clears_over_limit_active_engine() {
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 1;
        cfg.queue.auto_start = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first_bytes = swarmotter_core::meta::build_single_file_torrent(
            "active-slot-one.bin",
            b"active slot one payload",
            8,
            None,
            false,
        );
        let second_bytes = swarmotter_core::meta::build_single_file_torrent(
            "active-slot-two.bin",
            b"active slot two payload",
            8,
            None,
            false,
        );
        let first_meta = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
        let second_meta = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
        let first_hash = first_meta.info_hash;
        let second_hash = second_meta.info_hash;
        let mut first_torrent = Torrent::new(first_meta, 1);
        first_torrent.state = TorrentState::Downloading;
        let mut second_torrent = Torrent::new(second_meta, 2);
        second_torrent.state = TorrentState::Downloading;
        {
            let mut reg = runtime.registry.lock().await;
            reg.add(first_torrent).unwrap();
            reg.add(second_torrent).unwrap();
        }
        {
            let mut queue = runtime.queue.lock().await;
            queue.add(first_hash);
            queue.add(second_hash);
        }
        {
            let mut handles = runtime.engine_handles.write().await;
            handles.insert(
                first_hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
            handles.insert(
                second_hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
        }

        tokio::time::timeout(Duration::from_millis(100), runtime.reconcile_queue())
            .await
            .expect("queue reconciliation must not hang on over-limit active work");

        assert!(runtime
            .engine_handles
            .read()
            .await
            .contains_key(&first_hash));
        assert!(!runtime
            .engine_handles
            .read()
            .await
            .contains_key(&second_hash));
        {
            let reg = runtime.registry.lock().await;
            assert_eq!(
                reg.get(&first_hash).unwrap().state,
                TorrentState::Downloading
            );
            assert_eq!(reg.get(&second_hash).unwrap().state, TorrentState::Queued);
        }
        assert_eq!(runtime.active_download_hashes().await, vec![first_hash]);

        runtime.force_stop_engine(&first_hash).await;
    }

    #[tokio::test]
    async fn large_queue_recovery_keeps_configured_active_slots_startable() {
        assert_large_queue_recovery_keeps_configured_active_slots_startable(100).await;
    }

    #[tokio::test]
    async fn thousand_torrent_queue_recovery_keeps_configured_active_slots_startable() {
        assert_large_queue_recovery_keeps_configured_active_slots_startable(1_000).await;
    }

    async fn assert_large_queue_recovery_keeps_configured_active_slots_startable(
        total_torrents: usize,
    ) {
        assert!(total_torrents >= 50);
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 50;
        cfg.queue.auto_start = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let mut hashes = Vec::new();
        {
            let mut reg = runtime.registry.lock().await;
            let mut queue = runtime.queue.lock().await;
            for idx in 0..total_torrents {
                let name = format!("large-queue-{idx}.bin");
                let payload = format!("large queue payload {idx}");
                let bytes = swarmotter_core::meta::build_single_file_torrent(
                    &name,
                    payload.as_bytes(),
                    8,
                    None,
                    false,
                );
                let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
                let hash = meta.info_hash;
                let mut torrent = Torrent::new(meta, (idx + 1) as u64);
                if idx < 18 {
                    torrent.state = TorrentState::Downloading;
                }
                reg.add(torrent).unwrap();
                queue.add(hash);
                hashes.push(hash);
            }
        }

        {
            let mut handles = runtime.engine_handles.write().await;
            for hash in hashes.iter().take(18) {
                handles.insert(
                    *hash,
                    tokio::spawn(async {
                        std::future::pending::<()>().await;
                    }),
                );
            }
            for hash in hashes.iter().skip(18).take(32) {
                handles.insert(
                    *hash,
                    tokio::spawn(async {
                        std::future::pending::<()>().await;
                    }),
                );
            }
        }

        assert_eq!(runtime.active_download_hashes().await.len(), 18);
        let recovered = runtime.sweep_inactive_engine_handles("test").await;
        assert_eq!(recovered, 32);

        let desired = runtime.desired_download_hashes().await;
        assert_eq!(desired.len(), 50);
        assert_eq!(
            desired
                .iter()
                .filter(|hash| hashes[..18].contains(hash))
                .count(),
            18
        );
        let running = runtime.engine_handles.read().await;
        let blocked_startable = desired
            .iter()
            .filter(|hash| !hashes[..18].contains(hash) && running.contains_key(hash))
            .count();
        assert_eq!(
            blocked_startable, 0,
            "queued torrents selected to fill the configured active slots must not retain stale handles that make start_engine skip them"
        );
        drop(running);

        for hash in hashes.iter().take(18) {
            runtime.force_stop_engine(hash).await;
        }
    }

    #[tokio::test]
    async fn engine_task_finished_clears_restart_blocking_runtime_bookkeeping() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let hash =
            swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
                .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        runtime.engine_cmds.lock().await.insert(hash, tx);
        runtime
            .engine_handles
            .write()
            .await
            .insert(hash, tokio::spawn(async {}));
        runtime
            .engine_states
            .write()
            .await
            .insert(hash, Arc::new(Mutex::new(EngineState::default())));
        runtime.torrent_limiters.write().await.insert(
            hash,
            Arc::new(swarmotter_core::bandwidth::RateLimiter::new(0, 0)),
        );
        runtime.rate_samples.write().await.insert(
            hash,
            RateSample {
                downloaded: 1,
                uploaded: 1,
                rate_down: 1,
                rate_up: 1,
                last_download_at: Some(Instant::now()),
                last_upload_at: Some(Instant::now()),
                no_download_since: None,
                at: Instant::now(),
                peak_rate_down: 1,
                peak_rate_up: 1,
            },
        );

        runtime.engine_task_finished(hash).await;

        assert!(!runtime.engine_cmds.lock().await.contains_key(&hash));
        assert!(!runtime.engine_handles.read().await.contains_key(&hash));
        assert!(
            runtime.torrent_limiters.read().await.contains_key(&hash),
            "normal engine completion must retain the torrent limiter for queued seeding"
        );
        assert!(
            runtime.engine_states.read().await.contains_key(&hash),
            "diagnostic state should survive normal engine task exit"
        );
        assert!(
            runtime.rate_samples.read().await.contains_key(&hash),
            "rate samples should survive normal engine task exit"
        );
    }

    #[tokio::test]
    async fn runtime_config_sweeps_existing_completed_torrents_when_selfish() {
        let mut cfg = Config::default();
        cfg.torrent.selfish = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "selfish-sweep.bin",
            b"already complete payload",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Completed;
        torrent.date_completed = Some(2);
        for piece in 0..meta.piece_count() {
            torrent.progress.have_piece(piece);
        }
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: meta.piece_count(),
                total_length: meta.total_length,
                bytes_completed: meta.total_length,
                finished: true,
                ..Default::default()
            })),
        );
        runtime.rate_samples.write().await.insert(
            hash,
            RateSample {
                downloaded: 1,
                uploaded: 0,
                rate_down: 1,
                rate_up: 0,
                last_download_at: Some(Instant::now()),
                last_upload_at: None,
                no_download_since: None,
                at: Instant::now(),
                peak_rate_down: 1,
                peak_rate_up: 0,
            },
        );

        runtime.apply_runtime_config_fields().await;

        assert!(
            runtime.registry.lock().await.get(&hash).is_none(),
            "selfish mode should remove completed torrents already in the registry"
        );
        assert_eq!(runtime.queue.lock().await.position(&hash), None);
        assert!(!runtime.engine_states.read().await.contains_key(&hash));
        assert!(!runtime.rate_samples.read().await.contains_key(&hash));
    }

    #[tokio::test]
    async fn torrent_stats_includes_live_engine_diagnostics() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "diag.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Downloading;
        runtime.registry.lock().await.add(torrent).unwrap();
        let now = Instant::now();
        let mut peer_health = HashMap::new();
        peer_health.insert(
            "127.0.0.1:6881".parse().unwrap(),
            EnginePeerHealth {
                has_missing_pieces: true,
                unchoked: true,
                useful_recently: true,
                last_valid_block: Some(now),
                last_seen: Some(now),
                ..Default::default()
            },
        );
        peer_health.insert(
            "127.0.0.1:6882".parse().unwrap(),
            EnginePeerHealth {
                has_missing_pieces: true,
                last_seen: Some(now),
                ..Default::default()
            },
        );
        peer_health.insert(
            "127.0.0.1:6883".parse().unwrap(),
            EnginePeerHealth {
                has_missing_pieces: true,
                unchoked: true,
                useful_recently: true,
                last_seen: Some(now - Duration::from_secs(31)),
                ..Default::default()
            },
        );
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: meta.piece_count(),
                total_length: meta.total_length,
                active_peers: 4,
                peers: vec![
                    swarmotter_core::peer::PeerAddr::from_socket_addr(
                        "127.0.0.1:6881".parse().unwrap(),
                    ),
                    swarmotter_core::peer::PeerAddr::from_socket_addr(
                        "127.0.0.1:6882".parse().unwrap(),
                    ),
                ],
                peer_health,
                tracker_ok: true,
                tracker_message: Some("ok".into()),
                last_announce: Some(123),
                tracker_failures_recent: 3,
                dht_discovery_ok: true,
                pex_discovery_ok: true,
                peer_disconnects_recent: 2,
                dht_last_seen: Some(now - Duration::from_secs(11)),
                pex_last_seen: Some(now - Duration::from_secs(13)),
                tracker_last_ok: Some(now - Duration::from_secs(7)),
                peer_scheduler: PeerSchedulerDiagnostics {
                    discovered_peers: 2,
                    eligible_peers: 1,
                    failed_peers: 1,
                    peer_worker_limit: 8,
                    parallel_candidates: 1,
                    parallel_workers_started: 4,
                    serial_peer_active: true,
                    last_reason: Some("one eligible peer".into()),
                    ..Default::default()
                },
                ..Default::default()
            })),
        );

        runtime.reconcile_engine_progress().await;
        let stats = runtime.torrent_stats(&hash).await.unwrap();

        assert_eq!(stats.info_hash, hash);
        assert_eq!(stats.active_peer_workers, 4);
        assert_eq!(stats.known_peers, 2);
        let scheduler = stats.peer_scheduler.as_ref().unwrap();
        assert_eq!(scheduler.discovered_peers, 2);
        assert_eq!(scheduler.eligible_peers, 1);
        assert_eq!(scheduler.failed_peers, 1);
        assert_eq!(scheduler.peer_worker_limit, 8);
        assert_eq!(scheduler.parallel_candidates, 1);
        assert_eq!(scheduler.parallel_workers_started, 4);
        assert!(scheduler.serial_peer_active);
        assert_eq!(stats.useful_peers, Some(1));
        assert_eq!(stats.unchoked_peers, Some(1));
        assert_eq!(stats.choked_peers, None);
        assert_eq!(stats.recent_peer_failures, Some(2));
        assert_eq!(stats.recent_tracker_failures, Some(3));
        assert!(stats.tracker_ok);
        assert_eq!(stats.tracker_message.as_deref(), Some("ok"));
        assert_eq!(stats.last_announce, Some(123));
        assert_eq!(stats.dht_discovery_ok, Some(true));
        assert_eq!(stats.pex_discovery_ok, Some(true));
        assert!((7..=10).contains(&stats.tracker_last_ok_seconds_ago.unwrap()));
        assert!((11..=14).contains(&stats.dht_last_seen_seconds_ago.unwrap()));
        assert!((13..=16).contains(&stats.pex_last_seen_seconds_ago.unwrap()));

        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(summary.active_peer_workers, 4);
        assert_eq!(summary.known_peers, 2);
    }

    #[tokio::test]
    async fn autopilot_decision_uses_live_engine_telemetry() {
        let mut cfg = Config::default();
        cfg.autopilot.mode = AutopilotMode::Observe;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Downloading;
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: meta.piece_count(),
                total_length: meta.total_length,
                tracker_ok: false,
                dht_discovery_ok: false,
                pex_discovery_ok: false,
                dht_last_seen: Some(Instant::now() - Duration::from_secs(180)),
                pex_last_seen: Some(Instant::now() - Duration::from_secs(180)),
                ..Default::default()
            })),
        );
        runtime.rate_samples.write().await.insert(
            hash,
            RateSample {
                downloaded: 0,
                uploaded: 0,
                rate_down: 0,
                rate_up: 0,
                last_download_at: None,
                last_upload_at: None,
                no_download_since: Some(Instant::now() - Duration::from_secs(45)),
                at: Instant::now() - Duration::from_secs(45),
                peak_rate_down: 0,
                peak_rate_up: 0,
            },
        );

        let decision = runtime.torrent_autopilot_decision(&hash).await.unwrap();

        assert!(!decision.apply);
        assert!(decision.snapshot.is_slow());
        assert_eq!(decision.snapshot.network_traffic_allowed, Some(true));
        assert!(decision
            .snapshot
            .causes
            .contains(&swarmotter_core::models::stats::SlowCause::NoKnownPeers));
    }

    #[tokio::test]
    async fn torrent_autopilot_decision_does_not_refresh_unrelated_torrents() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first_bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-one.bin",
            b"autopilot one payload",
            8,
            None,
            false,
        );
        let second_bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-two.bin",
            b"autopilot two payload",
            8,
            None,
            false,
        );
        let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
        let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
        let first_hash = first.info_hash;
        let second_hash = second.info_hash;
        {
            let mut reg = runtime.registry.lock().await;
            reg.add(Torrent::new(first, 1)).unwrap();
            reg.add(Torrent::new(second, 2)).unwrap();
        }
        let blocked_state = Arc::new(Mutex::new(EngineState::default()));
        runtime
            .engine_states
            .write()
            .await
            .insert(second_hash, blocked_state.clone());
        let _unrelated_guard = blocked_state.lock().await;

        let decision = tokio::time::timeout(
            Duration::from_millis(100),
            runtime.torrent_autopilot_decision(&first_hash),
        )
        .await
        .expect("single-torrent autopilot decision should not wait on unrelated state")
        .expect("decision");

        assert_eq!(decision.snapshot.state, TorrentState::Queued);
    }

    #[tokio::test]
    async fn torrent_autopilot_decision_recomputes_stale_cached_snapshot() {
        let mut cfg = Config::default();
        cfg.autopilot.mode = AutopilotMode::Observe;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-current.bin",
            b"autopilot current payload",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Downloading;
        runtime.registry.lock().await.add(torrent).unwrap();
        let stale = AutopilotAnalyzer::new().analyze(
            &AutopilotInput {
                state: TorrentState::Queued,
                ..Default::default()
            },
            AutopilotMode::Observe,
        );
        runtime
            .autopilot_decisions
            .write()
            .await
            .insert(hash, stale);
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: meta.piece_count(),
                total_length: meta.total_length,
                active_peers: 2,
                peers: vec![
                    swarmotter_core::peer::PeerAddr::from_socket_addr(
                        "127.0.0.1:6881".parse().unwrap(),
                    ),
                    swarmotter_core::peer::PeerAddr::from_socket_addr(
                        "127.0.0.1:6882".parse().unwrap(),
                    ),
                ],
                peer_scheduler: PeerSchedulerDiagnostics {
                    discovered_peers: 2,
                    eligible_peers: 2,
                    peer_worker_limit: 8,
                    parallel_workers_started: 2,
                    ..Default::default()
                },
                ..Default::default()
            })),
        );

        let decision = runtime.torrent_autopilot_decision(&hash).await.unwrap();

        assert_eq!(decision.snapshot.state, TorrentState::Downloading);
        assert_eq!(decision.snapshot.known_peers, 2);
        assert_eq!(decision.snapshot.active_peer_workers, 2);
        let cached = runtime
            .autopilot_decisions
            .read()
            .await
            .get(&hash)
            .cloned()
            .unwrap();
        assert_eq!(cached.snapshot.state, TorrentState::Downloading);
    }

    #[tokio::test]
    async fn torrent_autopilot_override_is_persisted_and_used() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-override.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta, 1))
            .unwrap();

        runtime
            .set_torrent_autopilot_mode_override(&hash, Some(AutopilotMode::Disabled))
            .await
            .unwrap();

        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(
            summary.autopilot_mode_override,
            Some(AutopilotMode::Disabled)
        );
        let decision = runtime.torrent_autopilot_decision(&hash).await.unwrap();
        assert_eq!(decision.reasons[0].message, "autopilot disabled");
    }

    #[tokio::test]
    async fn autopilot_act_mode_expands_discovery_through_engine_command() {
        let mut cfg = Config::default();
        cfg.autopilot.mode = AutopilotMode::Act;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-act.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta.clone(), 1))
            .unwrap();
        runtime.engine_states.write().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: meta.piece_count(),
                total_length: meta.total_length,
                tracker_ok: false,
                dht_discovery_ok: false,
                pex_discovery_ok: false,
                ..Default::default()
            })),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        runtime.engine_cmds.lock().await.insert(hash, tx);
        runtime.engine_handles.write().await.insert(
            hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );

        runtime.refresh_autopilot_decisions(true).await;

        assert!(matches!(rx.try_recv().unwrap(), EngineCommand::Reannounce));
        let decision = runtime
            .autopilot_decisions
            .read()
            .await
            .get(&hash)
            .cloned()
            .unwrap();
        assert!(decision.apply);
        assert!(matches!(
            decision.action.unwrap().kind,
            AutopilotActionKind::ExpandDiscovery
        ));
        runtime.force_stop_engine(&hash).await;
    }

    #[tokio::test]
    async fn autopilot_act_mode_releases_stalled_active_queue_slot() {
        let mut cfg = Config::default();
        cfg.autopilot.mode = AutopilotMode::Act;
        cfg.queue.max_active_downloads = 1;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let stalled_bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-stalled.bin",
            b"stalled payload",
            8,
            None,
            false,
        );
        let queued_bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-queued.bin",
            b"queued payload",
            8,
            None,
            false,
        );
        let stalled_meta = swarmotter_core::meta::parse_torrent(&stalled_bytes).unwrap();
        let queued_meta = swarmotter_core::meta::parse_torrent(&queued_bytes).unwrap();
        let stalled_hash = stalled_meta.info_hash;
        let queued_hash = queued_meta.info_hash;
        let mut stalled = Torrent::new(stalled_meta.clone(), 1);
        stalled.state = TorrentState::Downloading;
        runtime.registry.lock().await.add(stalled).unwrap();
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(queued_meta, 2))
            .unwrap();
        {
            let mut queue = runtime.queue.lock().await;
            queue.add(stalled_hash);
            queue.add(queued_hash);
        }
        let stalled_since = Instant::now() - Duration::from_secs(45);
        runtime.engine_states.write().await.insert(
            stalled_hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: stalled_meta.piece_count(),
                total_length: stalled_meta.total_length,
                tracker_ok: false,
                dht_discovery_ok: false,
                pex_discovery_ok: false,
                peer_scheduler: PeerSchedulerDiagnostics {
                    peer_worker_limit: 1,
                    ..Default::default()
                },
                ..Default::default()
            })),
        );
        runtime.rate_samples.write().await.insert(
            stalled_hash,
            RateSample {
                downloaded: 0,
                uploaded: 0,
                rate_down: 0,
                rate_up: 0,
                last_download_at: None,
                last_upload_at: None,
                no_download_since: Some(stalled_since),
                at: stalled_since,
                peak_rate_down: 0,
                peak_rate_up: 0,
            },
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        runtime.engine_cmds.lock().await.insert(stalled_hash, tx);
        runtime.engine_handles.write().await.insert(
            stalled_hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
        runtime.queue_reconcile.lock().await.scheduled = true;

        tokio::time::timeout(
            Duration::from_millis(100),
            runtime.refresh_autopilot_decisions(true),
        )
        .await
        .expect("autopilot queue-slot release should not wait on a noncooperative engine task");

        let decision = runtime
            .autopilot_decisions
            .read()
            .await
            .get(&stalled_hash)
            .cloned()
            .unwrap();
        assert!(decision
            .snapshot
            .causes
            .contains(&swarmotter_core::models::stats::SlowCause::NoRecentProgress));
        assert!(matches!(
            decision.action.unwrap().kind,
            AutopilotActionKind::ReleaseQueueSlot
        ));
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&stalled_hash)
                .unwrap()
                .state,
            TorrentState::Queued
        );
        assert_eq!(runtime.queue.lock().await.position(&queued_hash), Some(1));
        assert_eq!(runtime.queue.lock().await.position(&stalled_hash), Some(2));
        assert!(runtime
            .engine_retry_after
            .read()
            .await
            .get(&stalled_hash)
            .is_some_and(|retry_at| *retry_at > Instant::now()));
        assert_eq!(runtime.desired_download_hashes().await, vec![queued_hash]);
    }

    #[tokio::test]
    async fn autopilot_act_mode_skips_queue_release_without_eligible_replacement() {
        let mut cfg = Config::default();
        cfg.autopilot.mode = AutopilotMode::Act;
        cfg.queue.max_active_downloads = 1;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let stalled_bytes = swarmotter_core::meta::build_single_file_torrent(
            "autopilot-stalled-alone.bin",
            b"stalled payload",
            8,
            None,
            false,
        );
        let stalled_meta = swarmotter_core::meta::parse_torrent(&stalled_bytes).unwrap();
        let stalled_hash = stalled_meta.info_hash;
        let mut stalled = Torrent::new(stalled_meta.clone(), 1);
        stalled.state = TorrentState::Downloading;
        runtime.registry.lock().await.add(stalled).unwrap();
        {
            let mut queue = runtime.queue.lock().await;
            queue.add(stalled_hash);
        }
        let stalled_since = Instant::now() - Duration::from_secs(45);
        runtime.engine_states.write().await.insert(
            stalled_hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: stalled_meta.piece_count(),
                total_length: stalled_meta.total_length,
                tracker_ok: false,
                dht_discovery_ok: false,
                pex_discovery_ok: false,
                peer_scheduler: PeerSchedulerDiagnostics {
                    peer_worker_limit: 1,
                    ..Default::default()
                },
                ..Default::default()
            })),
        );
        runtime.rate_samples.write().await.insert(
            stalled_hash,
            RateSample {
                downloaded: 0,
                uploaded: 0,
                rate_down: 0,
                rate_up: 0,
                last_download_at: None,
                last_upload_at: None,
                no_download_since: Some(stalled_since),
                at: stalled_since,
                peak_rate_down: 0,
                peak_rate_up: 0,
            },
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        runtime.engine_cmds.lock().await.insert(stalled_hash, tx);
        runtime.engine_handles.write().await.insert(
            stalled_hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );

        tokio::time::timeout(
            Duration::from_millis(100),
            runtime.refresh_autopilot_decisions(true),
        )
        .await
        .expect("autopilot queue-slot release should skip without a replacement candidate");

        let decision = runtime
            .autopilot_decisions
            .read()
            .await
            .get(&stalled_hash)
            .cloned()
            .unwrap();
        assert!(decision
            .snapshot
            .causes
            .contains(&swarmotter_core::models::stats::SlowCause::NoRecentProgress));
        assert!(matches!(
            decision.action.unwrap().kind,
            AutopilotActionKind::ReleaseQueueSlot
        ));
        assert_eq!(
            runtime
                .registry
                .lock()
                .await
                .get(&stalled_hash)
                .unwrap()
                .state,
            TorrentState::Downloading
        );
        assert_eq!(runtime.queue.lock().await.position(&stalled_hash), Some(1));
        assert!(runtime
            .engine_handles
            .read()
            .await
            .contains_key(&stalled_hash));
        assert!(runtime
            .engine_retry_after
            .read()
            .await
            .get(&stalled_hash)
            .is_none());
        assert_eq!(runtime.desired_download_hashes().await, vec![stalled_hash]);
    }

    #[tokio::test]
    async fn list_trackers_uses_per_tracker_live_announce_results() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let primary = "http://tracker.example/announce";
        let secondary = "udp://tracker.example:6969/announce";
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "trackers.bin",
            b"0123456789abcdef",
            8,
            Some(primary),
            false,
        );
        let mut meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        meta.announce_list = vec![vec![primary.into(), secondary.into()]];
        let hash = meta.info_hash;
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta, 1))
            .unwrap();

        let mut state = EngineState::default();
        state.tracker_announces.insert(
            primary.into(),
            crate::engine::TrackerAnnounceSnapshot {
                status: TrackerStatus::Ok,
                seeders: 256,
                leechers: 12,
                downloads: 0,
                last_error: None,
                last_message: Some("announce returned 64 peers".into()),
                last_announce: Some(1234),
            },
        );
        state.tracker_announces.insert(
            secondary.into(),
            crate::engine::TrackerAnnounceSnapshot {
                status: TrackerStatus::Error,
                seeders: 0,
                leechers: 0,
                downloads: 0,
                last_error: Some("tracker announce timed out".into()),
                last_message: None,
                last_announce: Some(1235),
            },
        );
        runtime
            .engine_states
            .write()
            .await
            .insert(hash, Arc::new(Mutex::new(state)));

        let trackers = runtime.list_trackers(&hash).await.unwrap();
        let primary_row = trackers.iter().find(|t| t.url == primary).unwrap();
        assert_eq!(primary_row.status, TrackerStatus::Ok);
        assert_eq!(primary_row.seeders, 256);
        assert_eq!(primary_row.leechers, 12);
        assert_eq!(primary_row.last_error, None);
        assert_eq!(
            primary_row.last_message.as_deref(),
            Some("announce returned 64 peers")
        );
        assert_eq!(primary_row.last_announce, Some(1234));
        assert_eq!(primary_row.tier, 0);

        let secondary_row = trackers.iter().find(|t| t.url == secondary).unwrap();
        assert_eq!(secondary_row.status, TrackerStatus::Error);
        assert_eq!(
            secondary_row.last_error.as_deref(),
            Some("tracker announce timed out")
        );
        assert_eq!(secondary_row.last_message, None);
        assert_eq!(secondary_row.tier, 0);
    }

    #[tokio::test]
    async fn storage_preflight_rejects_torrent_file_add_before_registration() {
        let root = unique_dir("storage-preflight");
        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(root.display().to_string());
        cfg.storage.minimum_free_space_bytes = u64::MAX;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "too-large.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );

        let err = runtime.add_torrent_file(bytes, None).await.unwrap_err();

        assert_eq!(err.code().as_str(), "storage_error");
        assert!(runtime.registry.lock().await.torrents.is_empty());
        assert!(runtime.queue.lock().await.order.is_empty());
    }

    #[tokio::test]
    async fn reset_downloads_clears_storage_roots_registry_and_logs() {
        let root = unique_dir("reset");
        let download_dir = root.join("downloads");
        let incomplete_dir = root.join("incomplete");
        let log_file = root.join("swarmotterd.log");
        tokio::fs::create_dir_all(download_dir.join("nested"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(&incomplete_dir).await.unwrap();
        tokio::fs::write(download_dir.join("nested").join("old.bin"), b"old")
            .await
            .unwrap();
        tokio::fs::write(incomplete_dir.join("partial.bin"), b"partial")
            .await
            .unwrap();
        tokio::fs::write(&log_file, b"old log line\n")
            .await
            .unwrap();

        let mut cfg = Config::default();
        cfg.storage.download_dir = Some(download_dir.display().to_string());
        cfg.storage.incomplete_dir = Some(incomplete_dir.display().to_string());
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::with_paths(cfg, health, None, Some(log_file.clone()));
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "reset.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let hash = meta.info_hash;
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta, 1))
            .unwrap();
        runtime.queue.lock().await.add(hash);
        runtime
            .engine_retry_after
            .write()
            .await
            .insert(hash, Instant::now() + Duration::from_secs(60));

        let result = runtime.reset_downloads().await.unwrap();

        assert_eq!(result.torrents_removed, 1);
        assert_eq!(result.log_files_cleared, 1);
        assert!(result
            .storage_paths
            .contains(&download_dir.display().to_string()));
        assert!(result
            .storage_paths
            .contains(&incomplete_dir.display().to_string()));
        assert!(runtime.registry.lock().await.torrents.is_empty());
        assert!(runtime.queue.lock().await.order.is_empty());
        assert!(runtime.engine_retry_after.read().await.is_empty());
        assert!(download_dir.is_dir());
        assert!(incomplete_dir.is_dir());
        assert!(tokio::fs::read_dir(&download_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none());
        assert!(tokio::fs::read_dir(&incomplete_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none());
        assert_eq!(tokio::fs::metadata(&log_file).await.unwrap().len(), 0);
    }

    #[test]
    fn per_torrent_worker_limit_is_independent_of_global_session_budget() {
        assert_eq!(
            DaemonRuntime::effective_per_torrent_peer_limit(0),
            DEFAULT_PER_TORRENT_PEER_LIMIT
        );
        assert_eq!(DaemonRuntime::effective_per_torrent_peer_limit(24), 24);
    }

    #[tokio::test]
    async fn peer_diagnostics_report_unlimited_observation_and_bounded_denial() {
        let mut unlimited_config = Config::default();
        unlimited_config.network.mode = NetworkContainmentMode::Disabled;
        unlimited_config.bandwidth.max_peers = 0;
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        health.traffic_allowed = true;
        let unlimited = DaemonRuntime::new(unlimited_config, health.clone());
        let unlimited_pool = unlimited.peer_permit_pool.read().await.clone();
        let permit = unlimited_pool.acquire().await.unwrap();
        let scheduler = unlimited.global_stats().await.scheduler;
        assert_eq!(scheduler.peer_limit, 0);
        assert_eq!(scheduler.peer_permits_in_use, 1);
        assert_eq!(scheduler.peer_permits_available, None);
        assert_eq!(scheduler.peer_sessions_denied, 0);
        drop(permit);
        assert_eq!(
            unlimited.global_stats().await.scheduler.peer_permits_in_use,
            0
        );

        let mut bounded_config = Config::default();
        bounded_config.network.mode = NetworkContainmentMode::Disabled;
        bounded_config.bandwidth.max_peers = 1;
        let bounded = DaemonRuntime::new(bounded_config, health);
        let bounded_pool = bounded.peer_permit_pool.read().await.clone();
        let permit = bounded_pool.try_acquire().unwrap();
        assert!(bounded_pool.try_acquire().is_none());
        let scheduler = bounded.global_stats().await.scheduler;
        assert_eq!(scheduler.peer_limit, 1);
        assert_eq!(scheduler.peer_permits_in_use, 1);
        assert_eq!(scheduler.peer_permits_available, Some(0));
        assert_eq!(scheduler.peer_sessions_denied, 1);
        drop(permit);
    }

    #[test]
    fn strip_ansi_controls_removes_terminal_sequences_from_logs() {
        let raw = "\u{1b}[2m2026-07-03T19:43:03Z\u{1b}[0m \u{1b}[32mINFO\u{1b}[0m message";
        assert_eq!(
            strip_ansi_controls(raw),
            "2026-07-03T19:43:03Z INFO message"
        );
    }

    #[test]
    fn encryption_mode_change_rebuilds_data_plane_without_process_restart() {
        let previous = Config::default();
        let mut next = previous.clone();
        next.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Required;

        assert!(data_plane_config_changed(&previous, &next));
        assert!(restart_required_fields(&previous, &next).is_empty());
    }

    #[test]
    fn storage_root_changes_reject_torrents_that_still_depend_on_old_roots() {
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "storage-transition.bin",
            b"storage transition payload",
            8,
            None,
            false,
        );
        let torrent = Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), 1);
        let previous = Config::default();
        let mut next = previous.clone();
        next.storage.download_dir = Some("/tmp/swarmotter-new-root".into());

        assert!(matches!(
            validate_storage_config_transition(&previous, &next, &[torrent]),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[tokio::test]
    async fn replace_config_preserves_and_redacts_auth_token() {
        let mut cfg = Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.api.auth_token = Some("existing-token".into());
        cfg.api.require_auth = true;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);

        let mut next = runtime.get_config().await;
        next.api.auth_token = None;
        next.api.require_auth = true;
        let result = runtime.replace_config(next).await.unwrap();

        assert_eq!(
            runtime.get_config().await.api.auth_token.as_deref(),
            Some("existing-token")
        );
        assert_eq!(result.config.api.auth_token, None);
    }

    #[tokio::test]
    async fn queue_scheduler_respects_auto_start_and_moves() {
        let mut cfg = Config::default();
        cfg.queue.max_active_downloads = 1;
        cfg.queue.auto_start = false;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let first_bytes = swarmotter_core::meta::build_single_file_torrent(
            "q1.bin",
            b"queue-one",
            4,
            None,
            false,
        );
        let second_bytes = swarmotter_core::meta::build_single_file_torrent(
            "q2.bin",
            b"queue-two",
            4,
            None,
            false,
        );
        let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
        let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
        let first_hash = first.info_hash;
        let second_hash = second.info_hash;

        {
            let mut reg = runtime.registry.lock().await;
            reg.add(Torrent::new(first, 1)).unwrap();
            reg.add(Torrent::new(second, 2)).unwrap();
        }
        {
            let mut queue = runtime.queue.lock().await;
            queue.add(first_hash);
            queue.add(second_hash);
        }

        assert!(runtime.desired_download_hashes().await.is_empty());

        runtime.queue.lock().await.start_now(&second_hash);
        assert_eq!(runtime.desired_download_hashes().await, vec![second_hash]);

        {
            let mut queue = runtime.queue.lock().await;
            queue.clear_bypass(&second_hash);
            queue.move_to_top(&first_hash);
        }
        runtime.config.write().await.queue.auto_start = true;
        assert_eq!(runtime.desired_download_hashes().await, vec![first_hash]);
    }

    #[tokio::test]
    async fn add_operations_mark_existing_queue_reconcile_dirty() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);

        {
            let mut state = runtime.queue_reconcile.lock().await;
            state.scheduled = true;
            state.dirty = false;
        }

        let magnet_hash = runtime
            .add_magnet(
                "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e&dn=bulk-one",
                None,
            )
            .await
            .unwrap();

        assert!(runtime.registry.lock().await.contains(&magnet_hash));
        assert_eq!(runtime.queue.lock().await.position(&magnet_hash), Some(1));
        assert!(runtime.engine_handles.read().await.is_empty());
        {
            let state = runtime.queue_reconcile.lock().await;
            assert!(state.scheduled);
            assert!(state.dirty);
        }

        {
            let mut state = runtime.queue_reconcile.lock().await;
            state.dirty = false;
        }

        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "bulk-two.bin",
            b"bulk torrent file payload",
            4,
            None,
            false,
        );
        let file_hash = runtime.add_torrent_file(bytes, None).await.unwrap();

        assert!(runtime.registry.lock().await.contains(&file_hash));
        assert_eq!(runtime.queue.lock().await.position(&file_hash), Some(2));
        assert!(runtime.engine_handles.read().await.is_empty());
        {
            let state = runtime.queue_reconcile.lock().await;
            assert!(state.scheduled);
            assert!(state.dirty);
        }
    }

    #[tokio::test]
    async fn runtime_queue_limit_update_marks_scheduled_reconcile_dirty() {
        let mut cfg = Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.queue.max_active_downloads = 25;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        {
            let mut state = runtime.queue_reconcile.lock().await;
            state.scheduled = true;
            state.dirty = false;
        }

        runtime
            .update_settings(swarmotter_api::state::SettingsPatch {
                queue: Some(swarmotter_core::queue::QueueLimits {
                    max_active_downloads: 50,
                    max_active_metadata_fetches: 100,
                    max_active_seeds: 5,
                    auto_start: true,
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(runtime.config.read().await.queue.max_active_downloads, 50);
        assert_eq!(runtime.queue.lock().await.limits.max_active_downloads, 50);
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(
            state.dirty,
            "runtime queue limit updates should schedule queue reconciliation instead of awaiting engine startup inline"
        );
    }

    #[tokio::test]
    async fn queue_reconcile_scheduler_clears_after_rapid_adds() {
        let mut cfg = Config::default();
        cfg.queue.auto_start = false;
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);

        let first_hash = runtime
            .add_magnet(
                "magnet:?xt=urn:btih:000000000000000000000000000000000000000a&dn=schedule-one",
                None,
            )
            .await
            .unwrap();
        {
            let state = runtime.queue_reconcile.lock().await;
            assert!(state.scheduled);
            assert!(!state.dirty);
        }

        for index in 1..3 {
            let magnet = format!(
                "magnet:?xt=urn:btih:{:040x}&dn=schedule-{index}",
                index + 10
            );
            runtime.add_magnet(&magnet, None).await.unwrap();
        }
        {
            let state = runtime.queue_reconcile.lock().await;
            assert!(state.scheduled);
            assert!(state.dirty);
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let complete = {
                    let state = runtime.queue_reconcile.lock().await;
                    !state.scheduled && !state.dirty
                };
                if complete {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(runtime.registry.lock().await.torrents.len(), 3);
        assert_eq!(runtime.queue.lock().await.order.len(), 3);
        assert_eq!(runtime.queue.lock().await.position(&first_hash), Some(1));
        assert!(runtime.engine_handles.read().await.is_empty());
    }

    #[tokio::test]
    async fn rapid_adds_queue_without_waiting_for_reconcile() {
        const ADD_COUNT: usize = 200;

        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);

        {
            let mut state = runtime.queue_reconcile.lock().await;
            state.scheduled = true;
            state.dirty = false;
        }

        for index in 0..ADD_COUNT {
            let magnet = format!("magnet:?xt=urn:btih:{:040x}&dn=rapid-{index}", index + 1);
            let hash = runtime.add_magnet(&magnet, None).await.unwrap();
            assert_eq!(runtime.queue.lock().await.position(&hash), Some(index + 1));
        }

        assert_eq!(runtime.registry.lock().await.torrents.len(), ADD_COUNT);
        assert_eq!(runtime.queue.lock().await.order.len(), ADD_COUNT);
        assert!(runtime.engine_handles.read().await.is_empty());
        {
            let state = runtime.queue_reconcile.lock().await;
            assert!(state.scheduled);
            assert!(state.dirty);
        }
    }

    #[tokio::test]
    async fn bulk_remove_clears_many_torrents_and_queue_entries() {
        const REMOVE_COUNT: usize = 98;

        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let mut hashes = Vec::with_capacity(REMOVE_COUNT);

        for index in 0..REMOVE_COUNT {
            let magnet = format!("magnet:?xt=urn:btih:{:040x}&dn=remove-{index}", index + 1);
            let hash = runtime.add_magnet(&magnet, None).await.unwrap();
            hashes.push(hash);
        }
        hashes.push(InfoHash::from_hex("ffffffffffffffffffffffffffffffffffffffff").unwrap());

        let removed = runtime.remove_torrents(hashes, false).await.unwrap();

        assert_eq!(removed.len(), REMOVE_COUNT);
        assert!(runtime.registry.lock().await.torrents.is_empty());
        assert!(runtime.queue.lock().await.order.is_empty());
        assert!(runtime.engine_handles.read().await.is_empty());
    }

    #[tokio::test]
    async fn bulk_remove_clears_ten_thousand_torrents_and_runtime_indexes() {
        const REMOVE_COUNT: usize = 10_000;

        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
            "managed placeholder",
            b"managed placeholder payload",
            8,
            None,
            false,
        );
        let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
        let hashes = (0..REMOVE_COUNT)
            .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
            .collect::<Vec<_>>();

        {
            let mut reg = runtime.registry.lock().await;
            for (idx, hash) in hashes.iter().copied().enumerate() {
                let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
                torrent.magnet_info_hash = Some(hash);
                reg.add(torrent).unwrap();
            }
        }
        runtime.queue.lock().await.add_many(hashes.iter().copied());
        runtime.rate_samples.write().await.insert(
            hashes[0],
            RateSample {
                downloaded: 1,
                uploaded: 0,
                rate_down: 1,
                rate_up: 0,
                last_download_at: Some(Instant::now()),
                last_upload_at: None,
                no_download_since: None,
                at: Instant::now(),
                peak_rate_down: 1,
                peak_rate_up: 0,
            },
        );
        runtime
            .engine_retry_after
            .write()
            .await
            .insert(hashes[1], Instant::now() + ENGINE_INCOMPLETE_RETRY_DELAY);

        let removed = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.remove_torrents(hashes.clone(), false),
        )
        .await
        .expect("bulk remove should be bounded for 10,000 records")
        .unwrap();

        assert_eq!(removed.len(), REMOVE_COUNT);
        assert!(runtime.registry.lock().await.torrents.is_empty());
        assert!(runtime.queue.lock().await.order.is_empty());
        assert!(runtime.queue.lock().await.bypass.is_empty());
        assert!(runtime.rate_samples.read().await.is_empty());
        assert!(runtime.engine_retry_after.read().await.is_empty());
        assert!(runtime.engine_handles.read().await.is_empty());
    }

    #[tokio::test]
    async fn paused_add_is_queued_without_reconcile_start() {
        let cfg = Config::default();
        let health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        );
        let runtime = DaemonRuntime::new(cfg, health);
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "paused-add.bin",
            b"paused add payload",
            4,
            None,
            false,
        );

        let hash = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();

        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(summary.state, TorrentState::Paused);
        assert_eq!(summary.queue_position, Some(1));
        assert!(runtime.desired_download_hashes().await.is_empty());
        assert!(runtime.engine_handles.read().await.is_empty());
        assert!(!runtime.queue_reconcile.lock().await.scheduled);
    }

    fn watch_test_config(
        root: &Path,
        start_behavior: swarmotter_core::config::StartBehavior,
    ) -> Config {
        let mut config = Config::default();
        config.network.mode = NetworkContainmentMode::Disabled;
        config.queue.auto_start = false;
        config.watch = vec![swarmotter_core::config::WatchFolderConfig {
            path: root.display().to_string(),
            recursive: false,
            download_dir: None,
            label: None,
            start_behavior,
            archive_dir: None,
            failure_dir: None,
            delete_after_import: false,
        }];
        config
    }

    fn disabled_health() -> NetworkHealth {
        NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        )
    }

    #[tokio::test]
    async fn watch_partial_copy_and_read_time_change_reset_without_terminal_result() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-partial-stability");
        let partial_path = root.join("a-partial.torrent");
        let first = swarmotter_core::meta::build_single_file_torrent(
            "partial-complete.bin",
            b"generated partial copy payload",
            8,
            None,
            false,
        );
        std::fs::write(&partial_path, &first[..first.len() / 2]).unwrap();
        let runtime = Arc::new(DaemonRuntime::new(
            watch_test_config(&root, StartBehavior::Paused),
            disabled_health(),
        ));

        runtime.watch_scan().await.unwrap();
        std::fs::write(&partial_path, &first).unwrap();
        runtime.watch_scan().await.unwrap();
        assert!(runtime.watch_history().await.is_empty());
        assert!(runtime.registry.lock().await.torrents.is_empty());
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_history().await.len(), 1);
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);

        let changing_path = root.join("z-changing.torrent");
        let before = swarmotter_core::meta::build_single_file_torrent(
            "before-read-change.bin",
            b"before read change",
            8,
            None,
            false,
        );
        let after = swarmotter_core::meta::build_single_file_torrent(
            "after-read-change.bin",
            b"after read change with a different length",
            8,
            None,
            false,
        );
        std::fs::write(&changing_path, before).unwrap();
        runtime.watch_scan().await.unwrap();
        let (read_reached, continue_read) = runtime.pause_watch_after_bounded_read().await;
        let scanning = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.watch_scan().await })
        };
        read_reached.await.unwrap();
        std::fs::write(&changing_path, &after).unwrap();
        continue_read.send(()).unwrap();
        scanning.await.unwrap().unwrap();
        assert_eq!(runtime.watch_history().await.len(), 1);
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);

        runtime.watch_scan().await.unwrap();
        let history = runtime.watch_history().await;
        assert_eq!(history.len(), 2);
        assert!(history.iter().all(|result| result.success));
        assert_eq!(runtime.registry.lock().await.torrents.len(), 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn watch_leave_processes_each_fingerprint_once_and_status_excludes_it() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-leave-once");
        let source = root.join("leave.torrent");
        let first = swarmotter_core::meta::build_single_file_torrent(
            "leave-first.bin",
            b"first generated leave payload",
            8,
            None,
            false,
        );
        std::fs::write(&source, first).unwrap();
        let runtime = DaemonRuntime::new(
            watch_test_config(&root, StartBehavior::Paused),
            disabled_health(),
        );
        runtime.watch_scan().await.unwrap();
        for _ in 0..2 {
            let status = runtime.watch_status().await;
            assert_eq!(status.folders[0].pending_torrent_files, 1);
            assert!(runtime.watch_history().await.is_empty());
            assert!(runtime.registry.lock().await.torrents.is_empty());
        }
        runtime.watch_scan().await.unwrap();
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_history().await.len(), 1);
        assert!(source.exists());
        assert_eq!(
            runtime.watch_status().await.folders[0].pending_torrent_files,
            0
        );

        let replacement = swarmotter_core::meta::build_single_file_torrent(
            "leave-replacement.bin",
            b"second generated leave payload with changed length",
            8,
            None,
            false,
        );
        std::fs::write(&source, replacement).unwrap();
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_history().await.len(), 1);
        assert_eq!(
            runtime.watch_status().await.folders[0].pending_torrent_files,
            1
        );
        runtime.watch_scan().await.unwrap();
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_history().await.len(), 2);
        assert_eq!(runtime.registry.lock().await.torrents.len(), 2);
        assert_eq!(
            runtime.watch_status().await.folders[0].pending_torrent_files,
            0
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn watch_restart_duplicate_runs_success_action_once_without_mutation() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-restart-duplicate");
        let state_path = root.join("state.json");
        let watch_root = root.join("watch");
        let archive = root.join("archive");
        std::fs::create_dir_all(&watch_root).unwrap();
        let source = watch_root.join("duplicate.torrent");
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "restart-duplicate.bin",
            b"generated restart duplicate payload",
            8,
            None,
            false,
        );
        std::fs::write(&source, &bytes).unwrap();
        let mut config = watch_test_config(&watch_root, StartBehavior::Paused);
        config.watch[0].archive_dir = Some(archive.display().to_string());
        config.watch[0].label = Some("must-not-apply-to-duplicate".into());

        let original = DaemonRuntime::with_paths_broker_and_state(
            config.clone(),
            disabled_health(),
            None,
            None,
            Some(state_path.clone()),
            EventBroker::default(),
        );
        let hash = original
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
            .await
            .unwrap();
        drop(original);

        let restart_broker = EventBroker::default();
        let restarted = DaemonRuntime::with_paths_broker_and_state(
            config,
            disabled_health(),
            None,
            None,
            Some(state_path),
            restart_broker.clone(),
        );
        restarted.restore_persisted_state().await.unwrap();
        let mut events = restart_broker.subscribe();
        let before =
            serde_json::to_value(restarted.registry.lock().await.get(&hash).cloned().unwrap())
                .unwrap();
        let before_order = restarted.queue.lock().await.order.clone();
        let before_bypass = restarted.queue.lock().await.bypass.clone();

        restarted.watch_scan().await.unwrap();
        assert!(source.exists());
        assert!(restarted.watch_history().await.is_empty());
        restarted.watch_scan().await.unwrap();
        assert!(!source.exists());
        assert!(archive.join("duplicate.torrent").exists());
        let history = restarted.watch_history().await;
        assert_eq!(history.len(), 1);
        assert!(history[0].success);
        assert!(history[0].duplicate);
        assert_eq!(history[0].outcome, watch::ImportOutcome::Duplicate);
        assert_eq!(
            history[0].info_hash_hex.as_deref(),
            Some(hash.to_hex().as_str())
        );
        let event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.kind, "watch_folder_imported");
        let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
        assert_eq!(payload["payload"]["outcome"], "duplicate");
        assert_eq!(payload["payload"]["duplicate"], true);
        assert_eq!(
            payload["payload"]["post_action_error"],
            serde_json::Value::Null
        );
        let after =
            serde_json::to_value(restarted.registry.lock().await.get(&hash).cloned().unwrap())
                .unwrap();
        assert_eq!(after, before);
        assert_eq!(restarted.queue.lock().await.order, before_order);
        assert_eq!(restarted.queue.lock().await.bypass, before_bypass);
        restarted.watch_scan().await.unwrap();
        assert_eq!(restarted.watch_history().await.len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn recursive_watch_excludes_in_root_archive_after_success() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-recursive-archive-exclusion");
        let archive = root.join("archive");
        let source = root.join("archive-once.torrent");
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "recursive-archive-once.bin",
            b"generated recursive archive exclusion payload",
            8,
            None,
            false,
        );
        std::fs::write(&source, bytes).unwrap();
        let mut config = watch_test_config(&root, StartBehavior::Paused);
        config.watch[0].recursive = true;
        config.watch[0].archive_dir = Some(archive.display().to_string());
        let runtime = DaemonRuntime::new(config, disabled_health());

        for _ in 0..5 {
            runtime.watch_scan().await.unwrap();
        }

        assert!(!source.exists());
        assert!(archive.join("archive-once.torrent").exists());
        let history = runtime.watch_history().await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, watch::ImportOutcome::Imported);
        assert!(history[0].post_action_error.is_none());
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        assert_eq!(
            runtime.watch_status().await.folders[0].pending_torrent_files,
            0
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn shared_add_persistence_failure_restores_exact_state_and_has_no_side_effects() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-add-rollback");
        let source = root.join("rollback.torrent");
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "watch-rollback.bin",
            b"generated watch rollback payload",
            8,
            None,
            false,
        );
        let hash = meta::parse_torrent(&bytes).unwrap().info_hash;
        std::fs::write(&source, bytes).unwrap();
        let broker = EventBroker::default();
        let runtime = DaemonRuntime::with_paths_broker_and_state(
            watch_test_config(&root, StartBehavior::Start),
            disabled_health(),
            None,
            None,
            None,
            broker.clone(),
        );
        let first = InfoHash::from_bytes([0x11; 20]);
        let last = InfoHash::from_bytes([0x22; 20]);
        {
            let mut queue = runtime.queue.lock().await;
            queue.add_many([first, hash, last]);
            queue.start_now(&hash);
        }
        let before_order = runtime.queue.lock().await.order.clone();
        let before_bypass = runtime.queue.lock().await.bypass.clone();
        runtime.watch_scan().await.unwrap();
        runtime.inject_add_mutation_persistence_failure();
        let mut events = broker.subscribe();
        runtime.watch_scan().await.unwrap();

        assert!(runtime.registry.lock().await.torrents.is_empty());
        assert_eq!(runtime.queue.lock().await.order, before_order);
        assert_eq!(runtime.queue.lock().await.bypass, before_bypass);
        assert!(!runtime.queue_reconcile.lock().await.scheduled);
        assert!(runtime.torrent_limiters.read().await.get(&hash).is_none());
        assert!(runtime
            .torrent_peer_permit_pools
            .read()
            .await
            .get(&hash)
            .is_none());
        assert!(source.exists());
        let history = runtime.watch_history().await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, watch::ImportOutcome::TransientFailure);
        let event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.kind, "watch_folder_failed");
        let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
        assert_eq!(payload["payload"]["outcome"], "transient_failure");
        assert!(
            tokio::time::timeout(Duration::from_millis(25), events.next())
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn api_add_uses_shared_injected_rollback_without_event_or_schedule() {
        let runtime = DaemonRuntime::new(Config::default(), disabled_health());
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "api-shared-rollback.bin",
            b"generated api rollback payload",
            8,
            None,
            false,
        );
        let hash = meta::parse_torrent(&bytes).unwrap().info_hash;
        let before_order = runtime.queue.lock().await.order.clone();
        let mut events = runtime.event_broker.subscribe();
        runtime.inject_add_mutation_persistence_failure();

        let error = runtime
            .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, false))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "storage_error");
        assert!(!runtime.registry.lock().await.contains(&hash));
        assert_eq!(runtime.queue.lock().await.order, before_order);
        assert!(!runtime.queue_reconcile.lock().await.scheduled);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), events.next())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn watch_permanent_failure_moves_while_transient_stays_and_retries() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-error-classification");
        let failure = root.join("failure");
        let bad = root.join("a-bad.torrent");
        let good = root.join("b-good.torrent");
        std::fs::write(&bad, b"not valid bencode").unwrap();
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "transient-retry.bin",
            b"generated transient retry payload",
            8,
            None,
            false,
        );
        std::fs::write(&good, bytes).unwrap();
        let mut config = watch_test_config(&root, StartBehavior::Paused);
        config.watch[0].failure_dir = Some(failure.display().to_string());
        let broker = EventBroker::default();
        let runtime = DaemonRuntime::with_paths_and_broker(
            config,
            disabled_health(),
            None,
            None,
            broker.clone(),
        );
        let mut events = broker.subscribe();
        runtime.watch_scan().await.unwrap();
        runtime.inject_add_mutation_persistence_failure();
        runtime.watch_scan().await.unwrap();

        let history = runtime.watch_history().await;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].outcome, watch::ImportOutcome::PermanentFailure);
        assert_eq!(history[1].outcome, watch::ImportOutcome::TransientFailure);
        assert!(!bad.exists());
        assert!(failure.join("a-bad.torrent").exists());
        assert!(good.exists());
        assert!(runtime.registry.lock().await.torrents.is_empty());
        let permanent_event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let transient_event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(permanent_event.kind, "watch_folder_failed");
        assert_eq!(transient_event.kind, "watch_folder_failed");
        let permanent_payload: serde_json::Value =
            serde_json::from_str(&permanent_event.json).unwrap();
        let transient_payload: serde_json::Value =
            serde_json::from_str(&transient_event.json).unwrap();
        assert_eq!(permanent_payload["payload"]["outcome"], "permanent_failure");
        assert_eq!(transient_payload["payload"]["outcome"], "transient_failure");

        runtime.watch_scan().await.unwrap();
        let history = runtime.watch_history().await;
        assert_eq!(history.len(), 3);
        assert_eq!(history[2].outcome, watch::ImportOutcome::Imported);
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn recursive_watch_excludes_in_root_failure_after_permanent_failure() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-recursive-failure-exclusion");
        let failure = root.join("failure");
        let source = root.join("fail-once.torrent");
        std::fs::write(&source, b"not valid bencode").unwrap();
        let mut config = watch_test_config(&root, StartBehavior::Paused);
        config.watch[0].recursive = true;
        config.watch[0].failure_dir = Some(failure.display().to_string());
        let runtime = DaemonRuntime::new(config, disabled_health());

        for _ in 0..5 {
            runtime.watch_scan().await.unwrap();
        }

        assert!(!source.exists());
        assert!(failure.join("fail-once.torrent").exists());
        let history = runtime.watch_history().await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, watch::ImportOutcome::PermanentFailure);
        assert!(history[0].post_action_error.is_none());
        assert!(runtime.registry.lock().await.torrents.is_empty());
        assert_eq!(
            runtime.watch_status().await.folders[0].pending_torrent_files,
            0
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn watch_error_classification_has_only_the_four_permanent_variants() {
        assert!(is_permanent_watch_error(&CoreError::Bencode("x".into())));
        assert!(is_permanent_watch_error(&CoreError::MalformedTorrent(
            "x".into()
        )));
        assert!(is_permanent_watch_error(&CoreError::InvalidInfoHash(
            "x".into()
        )));
        assert!(is_permanent_watch_error(&CoreError::Parse("x".into())));
        for transient in [
            CoreError::Storage("x".into()),
            CoreError::NetworkBlocked("x".into()),
            CoreError::Internal("x".into()),
            CoreError::InvalidConfig("x".into()),
        ] {
            assert!(!is_permanent_watch_error(&transient));
        }
    }

    #[tokio::test]
    async fn watch_destination_collision_preserves_both_files_and_processes_once() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-action-collision");
        let archive = root.join("archive");
        std::fs::create_dir_all(&archive).unwrap();
        let source = root.join("collision.torrent");
        let destination = archive.join("collision.torrent");
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "collision.bin",
            b"generated destination collision payload",
            8,
            None,
            false,
        );
        std::fs::write(&source, bytes).unwrap();
        std::fs::write(&destination, b"existing archive must survive").unwrap();
        let mut config = watch_test_config(&root, StartBehavior::Paused);
        config.watch[0].archive_dir = Some(archive.display().to_string());
        let broker = EventBroker::default();
        let runtime = DaemonRuntime::with_paths_and_broker(
            config,
            disabled_health(),
            None,
            None,
            broker.clone(),
        );
        let mut events = broker.subscribe();
        runtime.watch_scan().await.unwrap();
        runtime.watch_scan().await.unwrap();
        runtime.watch_scan().await.unwrap();

        let history = runtime.watch_history().await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, watch::ImportOutcome::Imported);
        assert!(history[0].post_action_error.is_some());
        assert!(source.exists());
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"existing archive must survive"
        );
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        let mut imported_event = None;
        for _ in 0..3 {
            let event = tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if event.kind == "watch_folder_imported" {
                imported_event = Some(event);
                break;
            }
        }
        let payload: serde_json::Value =
            serde_json::from_str(&imported_event.expect("watch success event").json).unwrap();
        assert_eq!(payload["payload"]["outcome"], "imported");
        assert!(payload["payload"]["post_action_error"].is_string());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn watch_observations_prune_disappeared_files_and_removed_roots() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-observation-prune");
        let source = root.join("observed.torrent");
        std::fs::write(&source, b"first observation only").unwrap();
        let runtime = DaemonRuntime::new(
            watch_test_config(&root, StartBehavior::Paused),
            disabled_health(),
        );
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_observations.lock().await.len(), 1);
        std::fs::remove_file(&source).unwrap();
        runtime.watch_scan().await.unwrap();
        assert!(runtime.watch_observations.lock().await.is_empty());

        std::fs::write(&source, b"second observation only").unwrap();
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_observations.lock().await.len(), 1);
        runtime.config.write().await.watch.clear();
        runtime.watch_scan().await.unwrap();
        assert!(runtime.watch_observations.lock().await.is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn overlapping_watch_roots_have_distinct_composite_observation_keys() {
        use swarmotter_core::config::{StartBehavior, WatchFolderConfig};

        let root = unique_dir("watch-overlap-keys");
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("shared.torrent"), b"observation only").unwrap();
        let mut config = watch_test_config(&root, StartBehavior::Paused);
        config.watch[0].recursive = true;
        config.watch.push(WatchFolderConfig {
            path: nested.display().to_string(),
            recursive: false,
            download_dir: None,
            label: None,
            start_behavior: StartBehavior::Paused,
            archive_dir: None,
            failure_dir: None,
            delete_after_import: false,
        });
        let runtime = DaemonRuntime::new(config, disabled_health());
        runtime.watch_scan().await.unwrap();
        let observations = runtime.watch_observations.lock().await;
        assert_eq!(observations.len(), 2);
        assert_eq!(
            observations
                .keys()
                .map(|key| key.root.clone())
                .collect::<HashSet<_>>()
                .len(),
            2
        );
        drop(observations);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn watch_action_exclusion_does_not_hide_separately_configured_overlapping_root() {
        use swarmotter_core::config::{StartBehavior, WatchFolderConfig};

        let root = unique_dir("watch-overlap-action-exclusion");
        let archive = root.join("archive");
        std::fs::create_dir_all(&archive).unwrap();
        std::fs::write(archive.join("shared.torrent"), b"observation only").unwrap();
        let mut config = watch_test_config(&root, StartBehavior::Paused);
        config.watch[0].recursive = true;
        config.watch[0].archive_dir = Some(archive.display().to_string());
        config.watch.push(WatchFolderConfig {
            path: archive.display().to_string(),
            recursive: false,
            download_dir: None,
            label: None,
            start_behavior: StartBehavior::Paused,
            archive_dir: None,
            failure_dir: None,
            delete_after_import: false,
        });
        let runtime = DaemonRuntime::new(config, disabled_health());

        runtime.watch_scan().await.unwrap();

        let observations = runtime.watch_observations.lock().await;
        assert_eq!(observations.len(), 1);
        let key = observations.keys().next().unwrap();
        assert_eq!(key.root, watch::lexical_absolute(&archive).unwrap());
        assert_eq!(key.relative_path, PathBuf::from("shared.torrent"));
        drop(observations);
        let status = runtime.watch_status().await;
        assert_eq!(status.folders[0].pending_torrent_files, 0);
        assert_eq!(status.folders[1].pending_torrent_files, 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_manual_watch_scans_produce_one_terminal_result() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-concurrent-scan");
        let source = root.join("single.torrent");
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "concurrent-watch.bin",
            b"generated concurrent watch payload",
            8,
            None,
            false,
        );
        std::fs::write(&source, bytes).unwrap();
        let runtime = Arc::new(DaemonRuntime::new(
            watch_test_config(&root, StartBehavior::Paused),
            disabled_health(),
        ));
        runtime.watch_scan().await.unwrap();
        let (read_reached, continue_read) = runtime.pause_watch_after_bounded_read().await;
        let first = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.watch_scan().await })
        };
        read_reached.await.unwrap();
        let second = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.watch_scan().await })
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !second.is_finished(),
            "scan B must wait while scan A owns the whole-scan lock"
        );
        continue_read.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(runtime.watch_history().await.len(), 1);
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        assert!(source.exists());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn incomplete_watch_root_scan_retains_prior_observations() {
        use swarmotter_core::config::StartBehavior;

        let root = unique_dir("watch-incomplete-root");
        let moved = root.with_extension("temporarily-moved");
        let source = root.join("retained.torrent");
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "retained-observation.bin",
            b"generated retained observation payload",
            8,
            None,
            false,
        );
        std::fs::write(&source, bytes).unwrap();
        let runtime = DaemonRuntime::new(
            watch_test_config(&root, StartBehavior::Paused),
            disabled_health(),
        );
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_observations.lock().await.len(), 1);

        std::fs::rename(&root, &moved).unwrap();
        assert!(runtime.watch_scan().await.is_err());
        assert_eq!(runtime.watch_observations.lock().await.len(), 1);
        std::fs::rename(&moved, &root).unwrap();
        runtime.watch_scan().await.unwrap();
        assert_eq!(runtime.watch_history().await.len(), 1);
        assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn watch_history_evicts_oldest_entry_at_ten_thousand_and_one() {
        let runtime = DaemonRuntime::new(Config::default(), disabled_health());
        for index in 0..=watch::MAX_IMPORT_HISTORY {
            runtime
                .record_watch_import(watch::ImportResult {
                    path: format!("/watch/{index}.torrent"),
                    success: false,
                    info_hash_hex: None,
                    error: Some("generated history entry".into()),
                    duplicate: false,
                    post_action_error: None,
                    outcome: watch::ImportOutcome::TransientFailure,
                })
                .await;
        }
        let history = runtime.watch_history().await;
        assert_eq!(history.len(), watch::MAX_IMPORT_HISTORY);
        assert_eq!(history.first().unwrap().path, "/watch/1.torrent");
        assert_eq!(
            history.last().unwrap().path,
            format!("/watch/{}.torrent", watch::MAX_IMPORT_HISTORY)
        );
    }

    #[test]
    fn health_input_uses_recent_peer_block_activity() {
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            "health.bin",
            b"0123456789abcdef",
            8,
            None,
            false,
        );
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let mut torrent = Torrent::new(meta.clone(), 1);
        torrent.state = TorrentState::Downloading;

        let mut peer_health = HashMap::new();
        peer_health.insert(
            "127.0.0.1:6881".parse().unwrap(),
            EnginePeerHealth {
                has_missing_pieces: true,
                unchoked: true,
                useful_recently: true,
                last_valid_block: Some(Instant::now()),
                last_seen: Some(Instant::now()),
                ..Default::default()
            },
        );

        let input = build_health_input(
            &torrent,
            meta.piece_count(),
            &swarmotter_core::storage::resume::PieceBitfield::new(meta.piece_count()),
            &peer_health,
            &true,
            false,
            false,
            0,
            0,
            0,
            0,
            None,
            None,
            None,
            None,
            None,
            Some(Instant::now()),
            1,
            None,
            0,
            0,
            NetworkHealth::blocked(
                NetworkContainmentMode::Disabled,
                swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
                "disabled",
            ),
        );

        assert!(input.received_block_recently);
        let health = HealthCalculator::new().compute(&input);
        assert!(
            health.score > 25,
            "recent peer blocks should avoid the stalled health cap"
        );
    }
}
