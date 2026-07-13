// SPDX-License-Identifier: Apache-2.0

//! Daemon runtime state implementing the API's `DaemonOps` trait.
//!
//! The runtime holds torrents, configuration, network health, and watch-
//! folder state. Torrent operations enforce network containment: in strict
//! fail-closed mode, torrent data-plane activity is blocked when the
//! configured path is unavailable, and torrents enter a `network_blocked`
//! state. The control plane (API/Web UI) remains available independently.

mod construction;
mod containment;
mod diagnostics;
mod lifecycle;
mod persistence;
mod policy_runtime;
mod scheduler;
mod seeding;
mod settings;
mod storage_controls;
#[path = "watch.rs"]
mod watch_runtime;

use diagnostics::*;

#[cfg(test)]
mod tests;

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
use swarmotter_core::models::tracker::{
    TrackerId, TrackerInfo, TrackerKind, TrackerScrapeStatus, TrackerStatus,
};
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
use storage_controls::{
    is_storage_work_cancelled, storage_root_admission_for_path, storage_work_cancelled_error,
    ExplicitRecheckOperation, StorageAdmissionController, StorageAdmissionPlan,
    StorageRecheckController, StorageRootAdmission, StorageWorkCancellation,
};

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
    /// One compiled immutable peer-admission policy shared by every active
    /// engine, magnet metadata fetch, and inbound listener generation.
    peer_filter: Arc<RwLock<Arc<swarmotter_core::peer_filter::PeerFilter>>>,
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
    /// Root-scoped active-engine reservations and shared write pressure
    /// limiters. These are local-storage controls only.
    storage_admissions: StorageAdmissionController,
    /// Root-scoped full-recheck permits. The RAII permit releases correctly if
    /// an API request is cancelled.
    storage_rechecks: StorageRecheckController,
    /// Cancellation signals for daemon-managed engine storage waits and
    /// startup verification. Lifecycle operations signal these before asking
    /// the engine to stop, so a saturated root cannot block command polling.
    engine_storage_cancellations: Arc<Mutex<HashMap<InfoHash, StorageWorkCancellation>>>,
    /// Explicit API rechecks have no engine task while their disk work runs.
    /// Track them separately so move/pause/stop can cancel and await cleanup.
    explicit_rechecks: Arc<Mutex<HashMap<InfoHash, ExplicitRecheckOperation>>>,
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
    /// Deterministic pause while an engine owns the transition lock immediately
    /// before it resolves a storage-root admission.
    #[cfg(test)]
    storage_admission_pause: AsyncTestPause,
    /// Deterministic pause after an explicit recheck has finalized its state
    /// but before its normal persistence write begins.
    #[cfg(test)]
    explicit_recheck_before_persist_pause: AsyncTestPause,
    /// Deterministic pause after a root-control-only replacement owns the
    /// transition lock and before it installs the new controls.
    #[cfg(test)]
    root_control_replacement_pause: AsyncTestPause,
    /// Deterministic post-rename configuration-write failure used to verify
    /// that generic/root-control replacements restore their file snapshot.
    #[cfg(test)]
    generic_config_fail_after_rename: Arc<AtomicBool>,
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
    active_bytes: u64,
    active_write_rate: u64,
}

#[derive(Debug, Clone)]
struct EngineStartSnapshot {
    meta: meta::TorrentMeta,
    complete_dir: String,
    active_dir: String,
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
    fn from_torrent(torrent: &Torrent, config: &Config) -> Self {
        let policy = DaemonRuntime::effective_policy_with_config(config, torrent);
        let complete_dir = policy.download_dir.value.unwrap_or_else(|| {
            std::env::temp_dir()
                .join("swarmotter-downloads")
                .display()
                .to_string()
        });
        let active_dir = policy
            .incomplete_dir
            .value
            .unwrap_or_else(|| complete_dir.clone());
        Self {
            meta: torrent.meta.clone(),
            complete_dir,
            active_dir,
            download_limit: policy.download_limit.value,
            upload_limit: policy.upload_limit.value,
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

/// Resolve the local storage-root control that owns a torrent's active write
/// directory. Controls intentionally do not affect network containment.
#[cfg(test)]
fn storage_root_admission_for_download(
    cfg: &Config,
    download_dir: Option<&str>,
) -> Option<StorageRootAdmission> {
    let complete_dir = resolve_download_dir_from_config(download_dir, cfg);
    let active_dir = resolve_incomplete_dir_from_config(&complete_dir, cfg);
    storage_root_admission_for_path(cfg, Path::new(&active_dir))
}

/// Profile storage selects a path; root controls remain globally configured
/// and are then chosen by that resolved active path.
fn storage_root_admission_for_torrent(
    cfg: &Config,
    torrent: &Torrent,
) -> Option<StorageRootAdmission> {
    let (_, active_dir) = DaemonRuntime::policy_storage_paths_with_config(cfg, torrent);
    storage_root_admission_for_path(cfg, Path::new(&active_dir))
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
