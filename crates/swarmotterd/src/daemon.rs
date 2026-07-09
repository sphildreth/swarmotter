// SPDX-License-Identifier: Apache-2.0

//! Daemon runtime state implementing the API's `DaemonOps` trait.
//!
//! The runtime holds torrents, configuration, network health, and watch-
//! folder state. Torrent operations enforce network containment: in strict
//! fail-closed mode, torrent data-plane activity is blocked when the
//! configured path is unavailable, and torrents enter a `network_blocked`
//! state. The control plane (API/Web UI) remains available independently.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use swarmotter_api::state::{AddTorrentOptions, DaemonOps};
use swarmotter_core::autopilot::{AutopilotAnalyzer, AutopilotConfig, AutopilotMode};
use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::meta;
use swarmotter_core::models::health::{HealthCalculator, HealthInput};
use swarmotter_core::models::network::{NetworkContainmentMode, NetworkHealth};
use swarmotter_core::models::peer::{EnginePeerHealth, Peer};
use swarmotter_core::models::stats::{
    AutopilotActionKind, AutopilotDecision, AutopilotInput, GlobalStats, PeerSchedulerDiagnostics,
    TorrentDiagnostics,
};
use swarmotter_core::models::storage::{
    StorageDiagnostics, StorageRootDiagnostics, StorageRootRole,
};
use swarmotter_core::models::torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
use swarmotter_core::models::tracker::{TrackerId, TrackerInfo, TrackerKind, TrackerStatus};
use swarmotter_core::models::{
    ConfigUpdateResult, DiagnosticLevel, DoctorCheck, DoctorReport, LogSnapshot,
    NetworkDiagnostics, NetworkInterfaceDiagnostic, NetworkPathCheck, ResetResult,
    WatchFolderStatus, WatchStatus,
};
use swarmotter_core::net::{self, InterfaceProbe, OsInterfaceProbe};
use swarmotter_core::queue::QueueState;
use swarmotter_core::ratio::{self, SeedDecision, TorrentAccounting, TorrentSeeding};
use swarmotter_core::torrent::{Torrent, TorrentRegistry};
use swarmotter_core::watch;

use crate::engine::{EngineCommand, EngineState, TorrentEngine};
use crate::netbinder::ContainedBinder;
use crate::seeder::Seeder;

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

#[derive(Clone)]
pub struct DaemonRuntime {
    pub registry: Arc<Mutex<TorrentRegistry>>,
    pub config: Arc<Mutex<Config>>,
    pub network_health: Arc<Mutex<NetworkHealth>>,
    pub watch_imports: Arc<Mutex<Vec<watch::ImportResult>>>,
    config_path: Option<PathBuf>,
    log_file_path: Option<PathBuf>,
    /// Live engine state per torrent, reconciled into summaries.
    engine_states: Arc<Mutex<HashMap<InfoHash, Arc<Mutex<EngineState>>>>>,
    /// Command channels to running engine tasks.
    engine_cmds: Arc<Mutex<HashMap<InfoHash, tokio::sync::mpsc::Sender<EngineCommand>>>>,
    /// Running engine task join handles.
    engine_handles: Arc<Mutex<HashMap<InfoHash, JoinHandle<()>>>>,
    /// Seeder shutdown signal senders per torrent (inbound peer listening).
    seeder_shutdowns: Arc<Mutex<HashMap<InfoHash, tokio::sync::watch::Sender<bool>>>>,
    /// Running seeder task join handles per torrent.
    seeder_handles: Arc<Mutex<HashMap<InfoHash, JoinHandle<()>>>>,
    /// Shared global download/upload rate limiter. Cloned into every engine
    /// and seeder so the configured global bandwidth cap is enforced as a true
    /// aggregate across all active torrents.
    global_limiter: swarmotter_core::bandwidth::RateLimiter,
    /// Per-torrent rate limiters for running engines, keyed by info hash. The
    /// daemon keeps a clone (cheap: buckets are shared) so per-torrent limit
    /// changes apply live to a running engine.
    engine_limiters: Arc<Mutex<HashMap<InfoHash, swarmotter_core::bandwidth::RateLimiter>>>,
    /// Last byte-counter samples used to calculate API/UI transfer rates.
    rate_samples: Arc<Mutex<HashMap<InfoHash, RateSample>>>,
    /// Per-torrent retry suppression for transient engine failures.
    engine_retry_after: Arc<Mutex<HashMap<InfoHash, Instant>>>,
    /// Latest computed autopilot decision per torrent, exposed through the API.
    autopilot_decisions: Arc<Mutex<HashMap<InfoHash, AutopilotDecision>>>,
    /// Last automatic action time per torrent, used to avoid repeated act-mode
    /// commands on every background pass.
    autopilot_last_action: Arc<Mutex<HashMap<InfoHash, Instant>>>,
    /// Runtime queue state backing queue positions and queue move operations.
    queue: Arc<Mutex<QueueState>>,
    /// Shared DHT runner so the configured DHT port is bound by one runner
    /// instead of once per active torrent.
    dht_runner: Arc<Mutex<Option<Arc<crate::dht::DhtRunner>>>>,
    /// Coalesces queue reconciliation requests triggered by rapid add/import
    /// bursts so API add calls do not wait for engine startup.
    queue_reconcile: Arc<Mutex<QueueReconcileState>>,
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
        let global_limiter = swarmotter_core::bandwidth::RateLimiter::new(
            config.bandwidth.effective_download(),
            config.bandwidth.effective_upload(),
        );
        Self {
            registry: Arc::new(Mutex::new(TorrentRegistry::default())),
            queue: Arc::new(Mutex::new(QueueState::new(config.queue.clone()))),
            config: Arc::new(Mutex::new(config)),
            network_health: Arc::new(Mutex::new(startup_health)),
            watch_imports: Arc::new(Mutex::new(Vec::new())),
            config_path,
            log_file_path,
            engine_states: Arc::new(Mutex::new(HashMap::new())),
            engine_cmds: Arc::new(Mutex::new(HashMap::new())),
            engine_handles: Arc::new(Mutex::new(HashMap::new())),
            seeder_shutdowns: Arc::new(Mutex::new(HashMap::new())),
            seeder_handles: Arc::new(Mutex::new(HashMap::new())),
            global_limiter,
            engine_limiters: Arc::new(Mutex::new(HashMap::new())),
            rate_samples: Arc::new(Mutex::new(HashMap::new())),
            engine_retry_after: Arc::new(Mutex::new(HashMap::new())),
            autopilot_decisions: Arc::new(Mutex::new(HashMap::new())),
            autopilot_last_action: Arc::new(Mutex::new(HashMap::new())),
            dht_runner: Arc::new(Mutex::new(None)),
            queue_reconcile: Arc::new(Mutex::new(QueueReconcileState::default())),
        }
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

    async fn add_torrent_file_with_options(
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
        if let Err(e) = self
            .preflight_storage_for_download(t.download_dir.as_deref(), t.meta.total_length)
            .await
        {
            tracing::warn!(
                info_hash = %hash,
                error = %e,
                error_code = %e.code(),
                "torrent file add rejected by storage preflight"
            );
            return Err(e);
        }
        apply_network_state(&mut t, &self.network_health).await;
        let blocked = t.state == TorrentState::NetworkBlocked;
        let start_paused = options.paused && !blocked;
        if start_paused {
            t.state = TorrentState::Paused;
        }
        {
            let mut reg = self.registry.lock().await;
            if reg.add(t).is_err() {
                tracing::warn!(
                    info_hash = %hash,
                    error_code = %CoreError::DuplicateTorrent(hash.to_hex()).code(),
                    "torrent file add rejected: duplicate"
                );
                return Err(CoreError::DuplicateTorrent(hash.to_hex()));
            }
        }
        self.queue.lock().await.add(hash);
        if !blocked && !start_paused {
            self.schedule_reconcile_queue("torrent_file_added").await;
        }
        tracing::info!(
            info_hash = %hash,
            network_blocked = blocked,
            paused = start_paused,
            "torrent file added"
        );
        Ok(hash)
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
        let parsed = meta::parse_torrent(&bytes)?;
        let mut t = Torrent::new(parsed, now());
        t.needs_metadata = true;
        t.magnet_info_hash = Some(hash);
        t.magnet_name = Some(name);
        t.magnet_trackers = m.trackers.clone();
        if let Some(d) = options.download_dir {
            t.download_dir = Some(d);
        }
        if let Err(e) = self
            .preflight_storage_for_download(t.download_dir.as_deref(), 0)
            .await
        {
            tracing::warn!(
                info_hash = %hash,
                error = %e,
                error_code = %e.code(),
                "magnet add rejected by storage reserve preflight"
            );
            return Err(e);
        }
        apply_network_state(&mut t, &self.network_health).await;
        let blocked = t.state == TorrentState::NetworkBlocked;
        let start_paused = options.paused && !blocked;
        if start_paused {
            t.state = TorrentState::Paused;
        }
        {
            let mut reg = self.registry.lock().await;
            reg.add(t)
                .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        }
        tracing::info!(
            info_hash = %hash,
            network_blocked = blocked,
            paused = start_paused,
            tracker_count = m.trackers.len(),
            "magnet added"
        );
        self.queue.lock().await.add(hash);
        if !blocked && !start_paused {
            self.schedule_reconcile_queue("magnet_added").await;
        }
        Ok(hash)
    }

    async fn remove_torrents_with_single_reconcile(
        &self,
        hashes: Vec<InfoHash>,
        delete_data: bool,
    ) -> Result<Vec<InfoHash>> {
        let mut unique_hashes = Vec::with_capacity(hashes.len());
        for hash in hashes {
            if !unique_hashes.contains(&hash) {
                unique_hashes.push(hash);
            }
        }

        let removed = {
            let mut reg = self.registry.lock().await;
            unique_hashes
                .into_iter()
                .filter_map(|hash| reg.remove(&hash).map(|torrent| (hash, torrent)))
                .collect::<Vec<_>>()
        };
        if removed.is_empty() {
            return Ok(Vec::new());
        }

        {
            let mut queue = self.queue.lock().await;
            for (hash, _) in &removed {
                queue.remove(hash);
            }
        }
        {
            let mut rate_samples = self.rate_samples.lock().await;
            for (hash, _) in &removed {
                rate_samples.remove(hash);
            }
        }
        {
            let mut decisions = self.autopilot_decisions.lock().await;
            let mut last_actions = self.autopilot_last_action.lock().await;
            for (hash, _) in &removed {
                decisions.remove(hash);
                last_actions.remove(hash);
            }
        }
        for (hash, _) in &removed {
            self.force_stop_engine(hash).await;
        }
        if delete_data {
            for (_, torrent) in &removed {
                let complete_dir = self.resolve_download_dir(torrent).await;
                let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
                let mut dirs = vec![active_dir, complete_dir];
                dirs.dedup();
                for dir in dirs {
                    let storage = swarmotter_core::storage::StorageIo::new(
                        torrent.meta.clone(),
                        std::path::PathBuf::from(&dir),
                    );
                    let _ = storage.remove_all().await;
                }
            }
        }
        self.reconcile_queue().await;
        Ok(removed.into_iter().map(|(hash, _)| hash).collect())
    }

    /// Resolve the download directory for a torrent: per-torrent override,
    /// then global config, then a default temp dir.
    async fn resolve_download_dir(&self, t: &Torrent) -> String {
        self.resolve_download_dir_override(t.download_dir.as_deref())
            .await
    }

    async fn resolve_download_dir_override(&self, download_dir: Option<&str>) -> String {
        let cfg = self.config.lock().await;
        resolve_download_dir_from_config(download_dir, &cfg)
    }

    /// Resolve the active write directory for a torrent. Incomplete downloads
    /// use the configured incomplete directory when present; otherwise they
    /// write directly to the final download directory.
    async fn resolve_incomplete_dir(&self, download_dir: &str) -> String {
        let cfg = self.config.lock().await;
        resolve_incomplete_dir_from_config(download_dir, &cfg)
    }

    async fn preflight_storage_for_download(
        &self,
        download_dir: Option<&str>,
        total_length: u64,
    ) -> Result<()> {
        let cfg = self.config.lock().await.clone();
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

    async fn configured_peer_worker_limit(&self, active_downloads: usize) -> usize {
        let cfg = self.config.lock().await;
        effective_peer_worker_limit(
            cfg.bandwidth.max_peers,
            cfg.bandwidth.max_peers_per_torrent,
            active_downloads,
        )
    }

    async fn apply_peer_worker_limits(&self) {
        let active_downloads = self.active_download_hashes().await.len().max(1);
        let limit = self.configured_peer_worker_limit(active_downloads).await;
        let senders: Vec<tokio::sync::mpsc::Sender<EngineCommand>> =
            self.engine_cmds.lock().await.values().cloned().collect();
        for tx in senders {
            let _ = tx.send(EngineCommand::UpdatePeerWorkerLimit(limit)).await;
        }
    }

    async fn active_download_hashes(&self) -> Vec<InfoHash> {
        let running: Vec<InfoHash> = self.engine_handles.lock().await.keys().copied().collect();
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
        let cfg = self.config.lock().await.clone();
        let mut queue = self.queue.lock().await;
        queue.limits = cfg.queue.clone();
        let reg = self.registry.lock().await;
        let retry_after = self.engine_retry_after.lock().await.clone();
        let now = Instant::now();
        queue.order.retain(|hash| reg.contains(hash));
        queue.bypass.retain(|hash| reg.contains(hash));

        let limit = queue.limits.max_active_downloads;
        let mut active = Vec::new();
        for hash in queue.bypass.iter().chain(queue.order.iter()) {
            if limit > 0 && active.len() >= limit {
                break;
            }
            if active.contains(hash) {
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
            let bypass = queue.bypass.contains(hash);
            let already_active = matches!(
                t.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            );
            let auto_startable = queue.limits.auto_start || bypass || already_active;
            if auto_startable
                && matches!(
                    t.state,
                    TorrentState::Queued
                        | TorrentState::Downloading
                        | TorrentState::DownloadingMetadata
                )
            {
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
                self.stop_engine(&hash).await;
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
        let running: HashSet<InfoHash> = self.engine_handles.lock().await.keys().copied().collect();
        let retry_after = self.engine_retry_after.lock().await.clone();
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
            for hash in &recovered {
                queue.add(*hash);
                queue.clear_bypass(hash);
                queue.move_to_bottom(hash);
            }
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
        let running: Vec<InfoHash> = self.engine_handles.lock().await.keys().copied().collect();
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
        self.engine_handles.lock().await.remove(&hash);
        self.engine_limiters.lock().await.remove(&hash);
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
            .lock()
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
        let mut reg = self.registry.lock().await;
        if let Some(t) = reg.get_mut(&hash) {
            t.state = state;
            t.error = Some(error.to_string());
        }
        false
    }

    async fn shared_dht_runner(
        &self,
        binder: Arc<dyn swarmotter_core::net::NetworkBinder>,
        peer_id: [u8; 20],
    ) -> Option<Arc<crate::dht::DhtRunner>> {
        let (dht_enabled, bootstrap_nodes, dht_port) = {
            let cfg = self.config.lock().await;
            (
                cfg.dht.enabled,
                cfg.dht.bootstrap_nodes.clone(),
                cfg.dht.port,
            )
        };
        if !dht_enabled || !self.network_health.lock().await.traffic_allowed {
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
        let health = self.network_health.lock().await.clone();
        if !health.traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
            // Network blocked: do not start the engine; mark torrent.
            let mut reg = self.registry.lock().await;
            if let Some(t) = reg.get_mut(&hash) {
                t.state = TorrentState::NetworkBlocked;
                t.error = Some(health.detail.clone());
            }
            return;
        }

        // Already running?
        if self.engine_handles.lock().await.contains_key(&hash) {
            return;
        }
        self.engine_retry_after.lock().await.remove(&hash);

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
            let cfg = self.config.lock().await;
            let preallocate = cfg.storage.preallocate;
            let sparse = cfg.storage.sparse;
            let allow_ipv6 = cfg.torrent.allow_ipv6 && cfg.network.allow_ipv6;
            let pex_enabled = cfg.pex.enabled;
            let pex_max_peers = cfg.pex.max_peers;
            let minimum_free_space_bytes = cfg.storage.minimum_free_space_bytes;
            let minimum_free_space_percent = cfg.storage.minimum_free_space_percent;
            let max_peer_workers = effective_peer_worker_limit(
                cfg.bandwidth.max_peers,
                cfg.bandwidth.max_peers_per_torrent,
                1,
            );
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
            let mut cfg = self.config.lock().await.storage.clone();
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
                    return;
                }
            }
        }

        let state = Arc::new(Mutex::new(EngineState::default()));
        self.engine_states.lock().await.insert(hash, state.clone());

        let binder: Arc<dyn swarmotter_core::net::NetworkBinder> = self.make_binder().await;
        let peer_id = make_peer_id();
        let (tx, rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
        self.engine_cmds.lock().await.insert(hash, tx);

        // Live bandwidth shaping: a per-torrent rate limiter built from the
        // torrent's own download/upload limits (0 = unlimited), plus the shared
        // global limiter so the configured global cap is also enforced. The
        // daemon keeps a clone so per-torrent limit changes apply live.
        let limiter = swarmotter_core::bandwidth::RateLimiter::new(
            snapshot.download_limit,
            snapshot.upload_limit,
        );
        self.engine_limiters
            .lock()
            .await
            .insert(hash, limiter.clone());
        // Peer transport selection (TCP/uTP) from config. All transports stay
        // on the contained binder; fail-closed blocks both.
        let (utp_enabled, utp_prefer_tcp, encryption_mode) = {
            let cfg = self.config.lock().await;
            (
                cfg.torrent.utp_enabled,
                cfg.torrent.utp_prefer_tcp,
                cfg.torrent.encryption_mode,
            )
        };

        let state_for_summary = state.clone();
        let hash_for_task = hash;
        let registry = self.registry.clone();
        // Clones needed by the engine task to perform selfish-mode removal
        // on completion without needing `&self` (the task owns only these
        // shared handles). Cheap `Arc` clones.
        let config = self.config.clone();
        let engine_cmds_arc = self.engine_cmds.clone();
        let engine_handles_arc = self.engine_handles.clone();
        let engine_states_arc = self.engine_states.clone();
        let engine_limiters_arc = self.engine_limiters.clone();
        let seeder_shutdowns_arc = self.seeder_shutdowns.clone();
        let seeder_handles_arc = self.seeder_handles.clone();
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
        .with_storage_reserve(minimum_free_space_bytes, minimum_free_space_percent)
        .with_peer_worker_limit(max_peer_workers)
        .with_allow_ipv6(allow_ipv6)
        .with_pex(pex_enabled, pex_max_peers);
        if let Some(dht) = dht_runner {
            engine = engine.with_dht(dht);
        }
        let handle = tokio::spawn(async move {
            match engine.run().await {
                Ok(final_state) => {
                    let finished = final_state.finished;
                    let stopped_by_command = final_state.stopped_by_command;
                    {
                        let mut reg = registry.lock().await;
                        if let Some(t) = reg.get_mut(&hash_for_task) {
                            // If metadata was fetched via BEP 9, replace the
                            // placeholder meta with the real one and rebuild the
                            // file/piece bookkeeping.
                            if let Some(real) = final_state.resolved_meta.as_ref() {
                                apply_resolved_metadata(t, real, &final_state);
                            }
                            t.downloaded = final_state.downloaded;
                            t.uploaded = final_state.uploaded;
                            t.progress.have = (0..final_state.piece_count)
                                .map(|i| final_state.pieces_have.has(i))
                                .collect();
                            if final_state.finished {
                                t.state = TorrentState::Completed;
                                t.date_completed = Some(now());
                            } else if t.state == TorrentState::DownloadingMetadata {
                                // Metadata fetched but download incomplete; mark
                                // downloading.
                                t.state = TorrentState::Downloading;
                            }
                        }
                    }
                    // Selfish completion policy: when enabled, immediately
                    // remove the finished torrent from the daemon (engine and
                    // seeder stopped, record removed) while preserving the
                    // downloaded data. This must run after the registry update
                    // above so final stats/name are captured before removal.
                    if finished && config.lock().await.torrent.selfish {
                        Self::selfish_remove_completed(
                            hash_for_task,
                            registry.clone(),
                            engine_cmds_arc.clone(),
                            engine_handles_arc.clone(),
                            engine_states_arc.clone(),
                            engine_limiters_arc.clone(),
                            seeder_shutdowns_arc.clone(),
                            seeder_handles_arc.clone(),
                        )
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
            let _ = state_for_summary;
        });
        self.engine_handles.lock().await.insert(hash, handle);

        if !self.registry.lock().await.contains(&hash) {
            self.force_stop_engine(&hash).await;
            return;
        }

        // Start the inbound peer listener / seeder alongside the download
        // engine, sharing the same live state. It serves verified pieces to
        // inbound peers (partial seeding during download, full seeding after
        // completion) through the contained listener. Skip for magnets until
        // metadata is resolved (the placeholder has no real pieces to serve).
        if !needs_metadata {
            self.start_seeder(
                hash,
                meta.clone(),
                active_dir.clone(),
                complete_dir.clone(),
                state.clone(),
            )
            .await;
        }

        // Mark the torrent as downloading.
        let mut reg = self.registry.lock().await;
        if let Some(t) = reg.get_mut(&hash) {
            if t.state == TorrentState::Queued || t.state == TorrentState::NetworkBlocked {
                t.state = if needs_metadata {
                    TorrentState::DownloadingMetadata
                } else {
                    TorrentState::Downloading
                };
                t.error = None;
            }
        }
    }

    async fn stop_engine(&self, hash: &InfoHash) {
        self.engine_retry_after.lock().await.remove(hash);
        if let Some(tx) = self.engine_cmds.lock().await.remove(hash) {
            let _ = tx.send(EngineCommand::Stop).await;
        }
        if let Some(handle) = self.engine_handles.lock().await.remove(hash) {
            let _ = handle.await;
        }
        // Stop the inbound peer listener / seeder too.
        self.stop_seeder(hash).await;
        self.engine_states.lock().await.remove(hash);
        self.engine_limiters.lock().await.remove(hash);
        self.rate_samples.lock().await.remove(hash);
    }

    async fn force_stop_engine(&self, hash: &InfoHash) {
        self.engine_retry_after.lock().await.remove(hash);
        if let Some(tx) = self.engine_cmds.lock().await.remove(hash) {
            let _ = tx.try_send(EngineCommand::Stop);
        }
        if let Some(handle) = self.engine_handles.lock().await.remove(hash) {
            handle.abort();
            let _ = handle.await;
        }
        self.force_stop_seeder(hash).await;
        self.engine_states.lock().await.remove(hash);
        self.engine_limiters.lock().await.remove(hash);
        self.rate_samples.lock().await.remove(hash);
    }

    async fn stop_all_torrent_tasks(&self, registry_hashes: &[InfoHash]) {
        let mut hashes = registry_hashes.to_vec();
        hashes.extend(self.engine_handles.lock().await.keys().copied());
        hashes.extend(self.seeder_handles.lock().await.keys().copied());
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
            queue.order.clear();
            queue.bypass.clear();
        }
        self.engine_states.lock().await.clear();
        self.engine_cmds.lock().await.clear();
        self.engine_handles.lock().await.clear();
        self.engine_limiters.lock().await.clear();
        self.seeder_shutdowns.lock().await.clear();
        self.seeder_handles.lock().await.clear();
        self.rate_samples.lock().await.clear();
        self.engine_retry_after.lock().await.clear();
        self.autopilot_decisions.lock().await.clear();
        self.autopilot_last_action.lock().await.clear();
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
    ) {
        if self.seeder_handles.lock().await.contains_key(&hash) {
            return;
        }
        let binder = self.make_binder().await;
        let peer_id = make_peer_id();
        let listen_port = self.config.lock().await.torrent.listen_port;
        // Per-torrent upload limit (0 = unlimited) plus the shared global cap.
        let (dl_limit, ul_limit) = {
            let reg = self.registry.lock().await;
            reg.get(&hash)
                .map(|t| (t.download_limit, t.upload_limit))
                .unwrap_or((0, 0))
        };
        let limiter = swarmotter_core::bandwidth::RateLimiter::new(dl_limit, ul_limit);
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
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let encryption_mode = self.config.lock().await.torrent.encryption_mode;
        let mut seeder = Seeder::with_limiter(
            meta,
            storage,
            state,
            binder,
            listen_port,
            peer_id,
            shutdown_rx,
            limiter,
        )
        .with_encryption_mode(encryption_mode)
        .with_global_limiter(Some(self.global_limiter.clone()));
        if let Some(complete_storage) = complete_storage {
            seeder = seeder.with_complete_storage(complete_storage);
        }
        self.seeder_shutdowns.lock().await.insert(hash, shutdown_tx);
        let hash_for_task = hash;
        let registry = self.registry.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = seeder.run().await {
                tracing::debug!(info_hash = %hash_for_task, error = %e, "seeder task ended");
            }
            // If the seeder exits (e.g. network blocked), clear its handle.
            let _ = registry;
        });
        self.seeder_handles.lock().await.insert(hash, handle);
    }

    async fn stop_seeder(&self, hash: &InfoHash) {
        if let Some(tx) = self.seeder_shutdowns.lock().await.remove(hash) {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.seeder_handles.lock().await.remove(hash) {
            let _ = handle.await;
        }
    }

    async fn force_stop_seeder(&self, hash: &InfoHash) {
        if let Some(tx) = self.seeder_shutdowns.lock().await.remove(hash) {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.seeder_handles.lock().await.remove(hash) {
            handle.abort();
            let _ = handle.await;
        }
    }

    async fn reconcile_seeders(&self) {
        let now_secs = now();
        let cfg = self.config.lock().await.clone();
        let seeding_limit = cfg.queue.max_active_seeds;
        let samples = self.rate_samples.lock().await.clone();
        let running_seeders: Vec<InfoHash> =
            self.seeder_handles.lock().await.keys().copied().collect();

        let completed: Vec<(
            InfoHash,
            swarmotter_core::meta::TorrentMeta,
            u64,
            u64,
            u64,
            u64,
        )> = {
            let reg = self.registry.lock().await;
            reg.torrents
                .iter()
                .filter_map(|(hash, t)| {
                    if t.state == TorrentState::Completed {
                        Some((
                            *hash,
                            t.meta.clone(),
                            t.downloaded,
                            t.uploaded,
                            t.date_completed.unwrap_or(t.date_added),
                            t.date_added,
                        ))
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut allowed = Vec::new();
        for (hash, _meta, downloaded, uploaded, completed_at, _date_added) in &completed {
            let idle_seconds = samples
                .get(hash)
                .and_then(|sample| sample.last_upload_at)
                .map(|at| Instant::now().saturating_duration_since(at).as_secs())
                .unwrap_or_else(|| now_secs.saturating_sub(*completed_at));
            let accounting = TorrentAccounting {
                downloaded: *downloaded,
                uploaded: *uploaded,
                idle_seconds,
            };
            if ratio::evaluate_seeding(&accounting, &cfg.seeding, &TorrentSeeding::default())
                != SeedDecision::Continue
            {
                continue;
            }
            if seeding_limit > 0 && allowed.len() >= seeding_limit {
                break;
            }
            allowed.push(*hash);
        }

        for hash in running_seeders {
            let completed_running = completed.iter().any(|(h, ..)| *h == hash);
            if completed_running && !allowed.contains(&hash) {
                self.stop_seeder(&hash).await;
            }
        }

        for hash in allowed {
            if self.seeder_handles.lock().await.contains_key(&hash) {
                continue;
            }
            let Some((_, meta, ..)) = completed.iter().find(|(h, ..)| *h == hash).cloned() else {
                continue;
            };
            let torrent_for_dir = {
                let reg = self.registry.lock().await;
                let Some(t) = reg.get(&hash) else {
                    continue;
                };
                t.clone()
            };
            let complete_dir = self.resolve_download_dir(&torrent_for_dir).await;
            let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
            let Some(state) = self.engine_states.lock().await.get(&hash).cloned() else {
                continue;
            };
            self.start_seeder(hash, meta, active_dir, complete_dir, state)
                .await;
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
        if !self.config.lock().await.torrent.selfish {
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
    ///
    /// This is an associated function taking the shared `Arc<Mutex<...>>`
    /// fields (rather than `&self`) precisely so the spawned engine task can
    /// invoke it with its captured clones.
    #[allow(clippy::too_many_arguments)]
    async fn selfish_remove_completed(
        hash: InfoHash,
        registry: Arc<Mutex<TorrentRegistry>>,
        engine_cmds: Arc<Mutex<HashMap<InfoHash, tokio::sync::mpsc::Sender<EngineCommand>>>>,
        engine_handles: Arc<Mutex<HashMap<InfoHash, JoinHandle<()>>>>,
        engine_states: Arc<Mutex<HashMap<InfoHash, Arc<Mutex<EngineState>>>>>,
        engine_limiters: Arc<Mutex<HashMap<InfoHash, swarmotter_core::bandwidth::RateLimiter>>>,
        seeder_shutdowns: Arc<Mutex<HashMap<InfoHash, tokio::sync::watch::Sender<bool>>>>,
        seeder_handles: Arc<Mutex<HashMap<InfoHash, JoinHandle<()>>>>,
    ) {
        let name = registry
            .lock()
            .await
            .get(&hash)
            .map(|t| t.name().to_string())
            .unwrap_or_default();
        // Stop the inbound seeder (a separate task; safe to await).
        if let Some(tx) = seeder_shutdowns.lock().await.remove(&hash) {
            let _ = tx.send(true);
        }
        if let Some(handle) = seeder_handles.lock().await.remove(&hash) {
            let _ = handle.await;
        }
        // Clear live engine bookkeeping. We deliberately do NOT await the
        // engine join handle: it belongs to the engine task that is calling
        // this method, so awaiting it would deadlock. Dropping the detached
        // handle is safe because the task is already returning.
        engine_cmds.lock().await.remove(&hash);
        engine_states.lock().await.remove(&hash);
        engine_limiters.lock().await.remove(&hash);
        if let Some(handle) = engine_handles.lock().await.remove(&hash) {
            drop(handle);
        }
        // Remove the torrent record; downloaded data is preserved (no
        // delete-data behavior is invoked).
        registry.lock().await.remove(&hash);
        tracing::info!(
            info_hash = %hash,
            name = %name,
            selfish = true,
            delete_data = false,
            "selfish mode removed completed torrent; downloaded data preserved"
        );
    }

    async fn make_binder(&self) -> Arc<dyn swarmotter_core::net::NetworkBinder> {
        let cfg = self.config.lock().await.clone();
        Arc::new(ContainedBinder::new(
            cfg.network.clone(),
            Arc::new(OsInterfaceProbe),
        ))
    }

    /// Periodically re-evaluate network containment health and flip torrent
    /// states between active and `network_blocked` as the path appears or
    /// disappears. Stop running engines when the path becomes unavailable.
    pub async fn network_health_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let cfg = self.config.lock().await.clone();
            let probe = OsInterfaceProbe;
            let health = net::evaluate(&cfg.network, &probe);
            let traffic_allowed = health.traffic_allowed;
            *self.network_health.lock().await = health.clone();

            // Reconcile live engine progress into torrent records.
            self.reconcile_engine_progress().await;

            if !traffic_allowed && health.mode != NetworkContainmentMode::Disabled {
                // Stop all running engines and mark torrents network_blocked.
                let hashes: Vec<InfoHash> =
                    self.engine_handles.lock().await.keys().copied().collect();
                for h in hashes {
                    self.stop_engine(&h).await;
                    let mut reg = self.registry.lock().await;
                    if let Some(t) = reg.get_mut(&h) {
                        t.state = TorrentState::NetworkBlocked;
                        t.error = Some(health.detail.clone());
                    }
                }
            } else {
                let mut reg = self.registry.lock().await;
                for t in reg.torrents.values_mut() {
                    if traffic_allowed && t.state == TorrentState::NetworkBlocked {
                        t.state = TorrentState::Queued;
                        t.error = None;
                    }
                }
                drop(reg);
                self.reconcile_queue().await;
            }
        }
    }

    /// Copy live engine state (pieces, byte counts) into the torrent records
    /// so API/UI summaries reflect real progress while downloading.
    async fn reconcile_engine_progress(&self) {
        let states = self.engine_states.lock().await.clone();
        let now = Instant::now();
        let mut samples = self.rate_samples.lock().await;
        let mut reg = self.registry.lock().await;
        let calc = HealthCalculator::new();
        for (hash, state) in &states {
            let s = state.lock().await;
            if let Some(t) = reg.get_mut(hash) {
                if let Some(real) = s.resolved_meta.as_ref() {
                    apply_resolved_metadata(t, real, &s);
                }
                let mut peak = samples.get(hash).map(|p| p.peak_rate_down).unwrap_or(0);
                if let Some(prev) = samples.get(hash).copied() {
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
                                &s,
                                inst_down,
                                inst_up,
                                previous_peak_down,
                                previous_peak_up,
                                peak,
                                peak_rate_up,
                                now,
                            );
                        }
                        samples.insert(
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
                        );
                    }
                } else {
                    samples.insert(
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
                    );
                }
                t.progress.have = (0..s.piece_count).map(|i| s.pieces_have.has(i)).collect();
                t.progress.total = s.piece_count;
                t.downloaded = s.downloaded;
                t.uploaded = s.uploaded;
                t.active_peer_workers = s.active_peers;
                t.known_peers = s.peers.len();
                if !t.state.is_error() && t.state != TorrentState::Paused {
                    if s.finished {
                        t.state = TorrentState::Completed;
                    } else if t.needs_metadata {
                        t.state = TorrentState::DownloadingMetadata;
                    } else if t.state == TorrentState::Queued
                        || t.state == TorrentState::DownloadingMetadata
                    {
                        t.state = TorrentState::Downloading;
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
                    self.config.lock().await.bandwidth.effective_download(),
                    self.network_health.lock().await.clone(),
                );
                t.health = calc.compute(&health_input);
            }
        }
        drop(reg);
        drop(samples);
        self.sweep_selfish_completed_torrents_best_effort("engine_progress")
            .await;
        self.reconcile_seeders().await;
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

        let cfg = self.config.lock().await.clone();
        let global_mode = cfg.autopilot.mode;
        let network = self.network_health.lock().await.clone();
        let states = self.engine_states.lock().await.clone();
        let samples = self.rate_samples.lock().await.clone();
        let active_downloads = self.active_download_hashes().await.len().max(1);
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
                self.apply_autopilot_decision(hash, &decision, &cfg, active_downloads)
                    .await;
            }
            decisions.insert(hash, decision);
        }

        *self.autopilot_decisions.lock().await = decisions;
    }

    async fn apply_autopilot_decision(
        &self,
        hash: InfoHash,
        decision: &AutopilotDecision,
        cfg: &Config,
        active_downloads: usize,
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
            .lock()
            .await
            .get(&hash)
            .is_some_and(|at| now.saturating_duration_since(*at) < AUTOPILOT_ACTION_COOLDOWN)
        {
            return;
        }

        let applied = match action.kind {
            AutopilotActionKind::IncreasePeerWorkers => {
                self.apply_autopilot_peer_worker_limit(hash, decision, cfg, active_downloads)
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
            self.autopilot_last_action.lock().await.insert(hash, now);
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
        active_downloads: usize,
    ) -> bool {
        let current = decision.snapshot.peer_worker_limit.max(1);
        let hard_limit = effective_peer_worker_limit(
            cfg.bandwidth.max_peers,
            cfg.bandwidth.max_peers_per_torrent,
            active_downloads,
        );
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
        if !self.engine_handles.lock().await.contains_key(&hash) {
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
            .lock()
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
        if let Some(rl) = self.engine_limiters.lock().await.get(&hash).cloned() {
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Download,
                download_limit,
            )
            .await;
        }
        true
    }

    /// Watch-folder scan loop: periodically scans configured folders and imports
    /// newly-stabilized `.torrent` files.
    pub async fn watch_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = self.scan_watch_folders().await;
        }
    }

    async fn scan_watch_folders(&self) -> Result<()> {
        let cfg = self.config.lock().await.clone();
        for folder in &cfg.watch {
            let path = std::path::Path::new(&folder.path);
            let files = watch::scan_torrent_files(path, folder.recursive);
            for file in files {
                let res = self
                    .import_one(&file, folder, cfg.storage.download_dir.as_deref())
                    .await;
                if res.is_err() {
                    move_failed_watch_file(folder, &file);
                }
                let info_hash_hex = res.as_ref().ok().map(|h| h.to_hex());
                let result = watch::ImportResult {
                    path: file.display().to_string(),
                    success: res.is_ok(),
                    info_hash_hex,
                    error: res.as_ref().err().map(|e| e.to_string()),
                    duplicate: matches!(res, Err(CoreError::DuplicateTorrent(_))),
                };
                self.watch_imports.lock().await.push(result);
            }
        }
        Ok(())
    }

    async fn import_one(
        &self,
        file: &std::path::Path,
        folder: &swarmotter_core::config::WatchFolderConfig,
        _global_dir: Option<&str>,
    ) -> Result<InfoHash> {
        let bytes = std::fs::read(file).map_err(CoreError::from)?;
        let parsed = meta::parse_torrent(&bytes)?;
        let hash = parsed.info_hash;
        let mut torrent = Torrent::new(parsed, now());
        watch::apply_folder_defaults(&mut torrent, folder);
        let mut reg = self.registry.lock().await;
        reg.add(torrent)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        // Post-import action for the source file.
        match watch::post_import_action(folder, file) {
            watch::PostImportAction::Delete => {
                let _ = std::fs::remove_file(file);
            }
            watch::PostImportAction::Archive(dest) => {
                let _ = std::fs::create_dir_all(dest.parent().unwrap_or(std::path::Path::new(".")));
                let _ = std::fs::rename(file, &dest);
            }
            watch::PostImportAction::Leave => {}
        }
        Ok(hash)
    }

    async fn apply_runtime_config_fields(&self) {
        let cfg = self.config.lock().await.clone();
        self.queue.lock().await.limits = cfg.queue.clone();
        self.global_limiter
            .set_capacity(
                swarmotter_core::bandwidth::RateDirection::Download,
                cfg.bandwidth.effective_download(),
            )
            .await;
        self.global_limiter
            .set_capacity(
                swarmotter_core::bandwidth::RateDirection::Upload,
                cfg.bandwidth.effective_upload(),
            )
            .await;
        let probe = OsInterfaceProbe;
        *self.network_health.lock().await = net::evaluate(&cfg.network, &probe);
        self.apply_peer_worker_limits().await;
        self.schedule_reconcile_queue("runtime_config").await;
        self.sweep_selfish_completed_torrents_best_effort("runtime_config")
            .await;
        self.reconcile_seeders().await;
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

fn move_failed_watch_file(
    folder: &swarmotter_core::config::WatchFolderConfig,
    file: &std::path::Path,
) {
    let Some(failure_dir) = &folder.failure_dir else {
        return;
    };
    let mut dest = std::path::PathBuf::from(failure_dir);
    dest.push(file.file_name().unwrap_or_default());
    let _ = std::fs::create_dir_all(dest.parent().unwrap_or(std::path::Path::new(".")));
    let _ = std::fs::rename(file, dest);
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

fn write_config_atomically(path: &Path, config: &Config) -> Result<()> {
    let toml = config.to_toml_string()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(CoreError::from)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, toml).map_err(CoreError::from)?;
    fs::rename(&tmp, path).map_err(CoreError::from)?;
    Ok(())
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
    if previous.torrent.listen_port != next.torrent.listen_port {
        fields.push("torrent.listen_port".into());
    }
    if previous.torrent.encryption_mode != next.torrent.encryption_mode {
        fields.push("torrent.encryption_mode".into());
    }
    if previous.dht.port != next.dht.port {
        fields.push("dht.port".into());
    }
    fields
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

fn effective_peer_worker_limit(
    global_max_peers: usize,
    max_peers_per_torrent: usize,
    active_downloads: usize,
) -> usize {
    let per_torrent = if max_peers_per_torrent == 0 {
        crate::engine::DEFAULT_PEER_WORKER_LIMIT
    } else {
        max_peers_per_torrent
    };
    if global_max_peers == 0 {
        return per_torrent.max(1);
    }
    let active = active_downloads.max(1);
    let global_share = global_max_peers.div_ceil(active);
    per_torrent.min(global_share).max(1)
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
    t.meta = real.clone();
    t.needs_metadata = false;
    t.magnet_info_hash = None;
    t.progress.have = (0..real.piece_count())
        .map(|i| state.pieces_have.has(i))
        .collect();
    t.progress.total = real.piece_count();
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

#[async_trait]
impl DaemonOps for DaemonRuntime {
    async fn list_torrents(&self) -> Vec<TorrentSummary> {
        self.reconcile_engine_progress().await;
        let positions: HashMap<InfoHash, usize> = self
            .queue
            .lock()
            .await
            .order
            .iter()
            .enumerate()
            .map(|(i, hash)| (*hash, i + 1))
            .collect();
        self.registry
            .lock()
            .await
            .list()
            .iter()
            .map(|t| {
                let mut summary = t.to_summary();
                summary.queue_position = positions.get(&t.info_hash()).copied();
                summary
            })
            .collect()
    }

    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        self.reconcile_engine_progress().await;
        let position = self.queue.lock().await.position(hash);
        self.registry.lock().await.get(hash).map(|t| {
            let mut summary = t.to_summary();
            summary.queue_position = position;
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
                    t.state = TorrentState::Paused;
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        self.queue.lock().await.clear_bypass(hash);
        self.reconcile_queue().await;
        Ok(())
    }

    async fn resume(&self, hash: &InfoHash) -> Result<()> {
        self.engine_retry_after.lock().await.remove(hash);
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.state = TorrentState::Queued;
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
        Ok(())
    }

    async fn start_now(&self, hash: &InfoHash) -> Result<()> {
        self.engine_retry_after.lock().await.remove(hash);
        {
            let reg = self.registry.lock().await;
            if reg.get(hash).is_none() {
                return Err(CoreError::NotFound("torrent".into()));
            }
        }
        {
            let mut queue = self.queue.lock().await;
            queue.add(*hash);
            queue.start_now(hash);
        }
        self.reconcile_queue().await;
        Ok(())
    }

    async fn stop(&self, hash: &InfoHash) -> Result<()> {
        self.pause(hash).await
    }

    async fn recheck(&self, hash: &InfoHash) -> Result<()> {
        self.stop_engine(hash).await;
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => t.state = TorrentState::Checking,
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        // Run a real storage recheck on disk.
        let (meta, storage_dir) = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            let complete_dir = self.resolve_download_dir(t).await;
            let storage_dir = if t.state == TorrentState::Completed {
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
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.progress.have = (0..meta.piece_count()).map(|i| bf.has(i)).collect();
                    if bf.count(meta.piece_count()) == meta.piece_count() {
                        t.state = TorrentState::Completed;
                        t.date_completed = Some(now());
                    } else if t.state == TorrentState::Checking {
                        t.state = TorrentState::Paused;
                    }
                }
            }
            Err(e) => {
                let mut reg = self.registry.lock().await;
                if let Some(t) = reg.get_mut(hash) {
                    t.state = TorrentState::StorageError;
                    t.error = Some(e.to_string());
                }
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
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.download_dir = Some(path);
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn rename_path(
        &self,
        hash: &InfoHash,
        file_index: usize,
        new_path: String,
    ) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                if file_index < t.files.len() {
                    t.files[file_index].path = new_path;
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn set_labels(&self, hash: &InfoHash, labels: Vec<String>) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.labels = labels;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
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
        // Apply live to a running engine (its per-torrent limiter shares the
        // buckets with the clone the daemon retains). The seeder reads limits
        // at start; a running seeder picks up the upload cap via the shared
        // global limiter and on its next start.
        if let Some(rl) = self.engine_limiters.lock().await.get(hash).cloned() {
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Download,
                limits.download,
            )
            .await;
            rl.set_capacity(
                swarmotter_core::bandwidth::RateDirection::Upload,
                limits.upload,
            )
            .await;
        }
        Ok(())
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
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                for i in file_indices {
                    if i < t.wanted.len() {
                        t.wanted[i] = wanted;
                        t.files[i].wanted = wanted;
                    }
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn set_priority(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        priority: FilePriority,
    ) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                for i in file_indices {
                    if i < t.priorities.len() {
                        t.priorities[i] = priority;
                        t.files[i].priority = priority;
                    }
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn list_trackers(&self, hash: &InfoHash) -> Option<Vec<TrackerInfo>> {
        // Reflect real per-tracker announce results from the live engine, if
        // present. Success text is kept separate from last_error so the UI and
        // Transmission emulation do not report successful announces as errors.
        let engine_trackers = self
            .engine_states
            .lock()
            .await
            .get(hash)
            .and_then(|s| s.try_lock().ok())
            .map(|s| s.tracker_announces.clone())
            .unwrap_or_default();
        self.registry.lock().await.get(hash).map(|t| {
            let mut out = Vec::new();
            let mut tier = 0usize;
            let mut urls = Vec::new();
            if let Some(a) = &t.meta.announce {
                urls.push(a.clone());
            }
            for tlist in &t.meta.announce_list {
                for url in tlist {
                    urls.push(url.clone());
                }
            }
            for url in &urls {
                let mut info = make_tracker(url, tier);
                if let Some(snapshot) = engine_trackers.get(url) {
                    info.status = snapshot.status;
                    info.seeders = snapshot.seeders;
                    info.leechers = snapshot.leechers;
                    info.downloads = snapshot.downloads;
                    info.last_error = snapshot.last_error.clone();
                    info.last_message = snapshot.last_message.clone();
                    info.last_announce = snapshot.last_announce;
                }
                out.push(info);
                tier += 1;
            }
            out
        })
    }

    async fn add_tracker(&self, hash: &InfoHash, url: String) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.is_none() {
                    t.meta.announce = Some(url);
                } else {
                    t.meta.announce_list.push(vec![url]);
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn remove_tracker(&self, hash: &InfoHash, url: String) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
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
        }
    }

    async fn edit_tracker(&self, hash: &InfoHash, old_url: String, new_url: String) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                if t.meta.announce.as_deref() == Some(&old_url) {
                    t.meta.announce = Some(new_url);
                    return Ok(());
                }
                for tier in t.meta.announce_list.iter_mut() {
                    for u in tier.iter_mut() {
                        if *u == old_url {
                            *u = new_url.clone();
                        }
                    }
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn list_peers(&self, hash: &InfoHash) -> Option<Vec<Peer>> {
        let states = self.engine_states.lock().await;
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
        Ok(())
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
        Ok(())
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
        Ok(())
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
        Ok(())
    }

    async fn get_config(&self) -> Config {
        self.config.lock().await.clone()
    }

    async fn update_settings(&self, patch: swarmotter_api::state::SettingsPatch) -> Result<()> {
        {
            let mut cfg = self.config.lock().await;
            if let Some(b) = patch.bandwidth {
                cfg.bandwidth = b;
            }
            if let Some(q) = patch.queue {
                cfg.queue = q;
            }
            if let Some(s) = patch.seeding {
                cfg.seeding = s;
            }
            if let Some(autopilot) = patch.autopilot {
                cfg.autopilot = autopilot;
            }
        }
        self.apply_runtime_config_fields().await;
        Ok(())
    }

    async fn replace_config(&self, mut next: Config) -> Result<ConfigUpdateResult> {
        let (previous, config_path) = {
            let cfg = self.config.lock().await;
            (cfg.clone(), self.config_path.clone())
        };
        if next.api.auth_token.is_none() {
            next.api.auth_token = previous.api.auth_token.clone();
        }
        next.validate()?;

        if let Some(path) = &config_path {
            write_config_atomically(path, &next)?;
        }

        let restart_required_fields = restart_required_fields(&previous, &next);
        {
            let mut cfg = self.config.lock().await;
            *cfg = next.clone();
        }
        self.apply_runtime_config_fields().await;

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
                "torrent.selfish".into(),
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

        let cfg = self.config.lock().await.clone();
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

        tracing::warn!(
            torrents_removed = torrents.len(),
            storage_entries_removed,
            log_files_cleared,
            storage_paths = ?storage_paths,
            log_paths = ?log_paths,
            "download state reset by API request"
        );

        Ok(ResetResult {
            torrents_removed: torrents.len(),
            storage_paths,
            storage_entries_removed,
            log_paths,
            log_files_cleared,
        })
    }

    async fn network_health(&self) -> NetworkHealth {
        self.network_health.lock().await.clone()
    }

    async fn network_diagnostics(&self) -> NetworkDiagnostics {
        let cfg = self.config.lock().await.clone();
        let health = self.network_health.lock().await.clone();
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
        self.reconcile_engine_progress().await;
        let cfg = self.config.lock().await.clone();
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
        let cfg = self.config.lock().await.clone();
        let network = self.network_health.lock().await.clone();
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
        self.reconcile_engine_progress().await;
        let reg = self.registry.lock().await;
        let active_downloads = reg
            .torrents
            .values()
            .filter(|t| {
                matches!(
                    t.state,
                    TorrentState::Downloading | TorrentState::DownloadingMetadata
                )
            })
            .count();
        let active_seeds = reg
            .torrents
            .values()
            .filter(|t| matches!(t.state, TorrentState::Seeding))
            .count();
        let paused = reg
            .torrents
            .values()
            .filter(|t| matches!(t.state, TorrentState::Paused))
            .count();
        GlobalStats {
            download_rate: reg.torrents.values().map(|t| t.rate_down).sum(),
            upload_rate: reg.torrents.values().map(|t| t.rate_up).sum(),
            torrent_count: reg.torrents.len(),
            active_downloads,
            active_seeds,
            paused,
            total_downloaded: reg.torrents.values().map(|t| t.downloaded).sum(),
            total_uploaded: reg.torrents.values().map(|t| t.uploaded).sum(),
            ..Default::default()
        }
    }

    async fn torrent_stats(&self, hash: &InfoHash) -> Option<TorrentDiagnostics> {
        self.reconcile_engine_progress().await;
        let engine_state = self.engine_states.lock().await.get(hash).cloned();
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
        self.config.lock().await.autopilot.clone()
    }

    async fn torrent_autopilot_decision(&self, hash: &InfoHash) -> Option<AutopilotDecision> {
        let torrent = self.registry.lock().await.get(hash).cloned()?;
        let cfg = self.config.lock().await.clone();
        let network = self.network_health.lock().await.clone();
        let mode = effective_autopilot_mode(cfg.autopilot.mode, torrent.autopilot_mode_override);
        let state = self.engine_states.lock().await.get(hash).cloned();
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
            self.rate_samples.lock().await.get(hash).copied(),
            Instant::now(),
            &network,
        );
        let decision = AutopilotAnalyzer::new().analyze(&input, mode);
        self.autopilot_decisions
            .lock()
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
        Ok(())
    }

    async fn watch_scan(&self) -> Result<()> {
        self.scan_watch_folders().await
    }

    async fn watch_status(&self) -> WatchStatus {
        let cfg = self.config.lock().await.clone();
        let history = self.watch_imports.lock().await.clone();
        let enabled = !cfg.watch.is_empty();
        let folders = cfg
            .watch
            .into_iter()
            .map(|folder| {
                let path = Path::new(&folder.path);
                let exists = path.is_dir();
                let pending_torrent_files = if exists {
                    watch::scan_torrent_files(path, folder.recursive).len()
                } else {
                    0
                };
                let last_result = history
                    .iter()
                    .rev()
                    .find(|result| result.path.starts_with(&folder.path))
                    .cloned();
                WatchFolderStatus {
                    config: folder,
                    exists,
                    pending_torrent_files,
                    last_result,
                }
            })
            .collect();
        WatchStatus {
            enabled,
            folders,
            recent_imports: history,
        }
    }

    async fn watch_history(&self) -> Vec<watch::ImportResult> {
        self.watch_imports.lock().await.clone()
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
async fn apply_network_state(t: &mut Torrent, health: &Arc<Mutex<NetworkHealth>>) {
    let h = health.lock().await;
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
    for (_addr, p) in peer_health.iter() {
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
            .lock()
            .await
            .insert(hash, state.clone());
        runtime.rate_samples.lock().await.insert(
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

        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert!(summary.rate_down > 0);
        assert!(summary.rate_up > 0);
        assert_eq!(summary.downloaded, 5_000);
        assert_eq!(summary.uploaded, 1_200);
        let peak_sample = runtime
            .rate_samples
            .lock()
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

        let mut pieces_have =
            swarmotter_core::storage::resume::PieceBitfield::new(real_meta.piece_count());
        pieces_have.set(0);
        runtime.engine_states.lock().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                pieces_have,
                piece_count: real_meta.piece_count(),
                total_length: real_meta.total_length,
                resolved_meta: Some(real_meta.clone()),
                ..Default::default()
            })),
        );

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
        runtime.engine_states.lock().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                tracker_message: Some("fetching metadata via BEP 9".into()),
                ..Default::default()
            })),
        );

        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert_eq!(summary.state, TorrentState::DownloadingMetadata);
        assert_eq!(summary.total_length, "placeholder".len() as u64);
    }

    #[tokio::test]
    async fn retryable_magnet_metadata_no_peers_stays_in_metadata_state() {
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
        torrent.state = TorrentState::DownloadingMetadata;
        torrent.needs_metadata = true;
        torrent.magnet_info_hash = Some(hash);
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);

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
            .lock()
            .await
            .get(&hash)
            .is_some_and(|retry_at| *retry_at > Instant::now()));
        assert!(
            runtime.desired_download_hashes().await.is_empty(),
            "retry backoff should keep no-peer magnets out of active queue slots"
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
            .lock()
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
        runtime.engine_handles.lock().await.insert(
            hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
        runtime
            .engine_states
            .lock()
            .await
            .insert(hash, Arc::new(Mutex::new(EngineState::default())));

        let recovered = tokio::time::timeout(
            Duration::from_millis(100),
            runtime.sweep_inactive_engine_handles("test"),
        )
        .await
        .expect("stale queued handles should be force-cleared promptly");

        assert_eq!(recovered, 1);
        assert!(!runtime.engine_handles.lock().await.contains_key(&hash));
        assert!(!runtime.engine_cmds.lock().await.contains_key(&hash));
        assert!(!runtime.engine_states.lock().await.contains_key(&hash));
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
            let mut handles = runtime.engine_handles.lock().await;
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
        let running = runtime.engine_handles.lock().await;
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
            .lock()
            .await
            .insert(hash, tokio::spawn(async {}));
        runtime
            .engine_states
            .lock()
            .await
            .insert(hash, Arc::new(Mutex::new(EngineState::default())));
        runtime
            .engine_limiters
            .lock()
            .await
            .insert(hash, swarmotter_core::bandwidth::RateLimiter::new(0, 0));
        runtime.rate_samples.lock().await.insert(
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
        assert!(!runtime.engine_handles.lock().await.contains_key(&hash));
        assert!(!runtime.engine_limiters.lock().await.contains_key(&hash));
        assert!(
            runtime.engine_states.lock().await.contains_key(&hash),
            "diagnostic state should survive normal engine task exit"
        );
        assert!(
            runtime.rate_samples.lock().await.contains_key(&hash),
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
        runtime.engine_states.lock().await.insert(
            hash,
            Arc::new(Mutex::new(EngineState {
                piece_count: meta.piece_count(),
                total_length: meta.total_length,
                bytes_completed: meta.total_length,
                finished: true,
                ..Default::default()
            })),
        );
        runtime.rate_samples.lock().await.insert(
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
        assert!(!runtime.engine_states.lock().await.contains_key(&hash));
        assert!(!runtime.rate_samples.lock().await.contains_key(&hash));
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
        runtime.engine_states.lock().await.insert(
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
        runtime.engine_states.lock().await.insert(
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
        runtime.rate_samples.lock().await.insert(
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
            .lock()
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
        runtime.autopilot_decisions.lock().await.insert(hash, stale);
        runtime.engine_states.lock().await.insert(
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
            .lock()
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
        runtime.engine_states.lock().await.insert(
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

        runtime.refresh_autopilot_decisions(true).await;

        assert!(matches!(rx.try_recv().unwrap(), EngineCommand::Reannounce));
        let decision = runtime
            .autopilot_decisions
            .lock()
            .await
            .get(&hash)
            .cloned()
            .unwrap();
        assert!(decision.apply);
        assert!(matches!(
            decision.action.unwrap().kind,
            AutopilotActionKind::ExpandDiscovery
        ));
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
        runtime.engine_states.lock().await.insert(
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
        runtime.rate_samples.lock().await.insert(
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
        runtime.engine_handles.lock().await.insert(
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
            .lock()
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
            .lock()
            .await
            .get(&stalled_hash)
            .is_some_and(|retry_at| *retry_at > Instant::now()));
        assert_eq!(runtime.desired_download_hashes().await, vec![queued_hash]);
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
        meta.announce_list.push(vec![secondary.into()]);
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
            .lock()
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

        let secondary_row = trackers.iter().find(|t| t.url == secondary).unwrap();
        assert_eq!(secondary_row.status, TrackerStatus::Error);
        assert_eq!(
            secondary_row.last_error.as_deref(),
            Some("tracker announce timed out")
        );
        assert_eq!(secondary_row.last_message, None);
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
            .lock()
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
        assert!(runtime.engine_retry_after.lock().await.is_empty());
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
    fn effective_peer_worker_limit_uses_global_and_per_torrent_caps() {
        assert_eq!(
            effective_peer_worker_limit(0, 0, 3),
            crate::engine::DEFAULT_PEER_WORKER_LIMIT
        );
        assert_eq!(effective_peer_worker_limit(120, 0, 3), 40);
        assert_eq!(effective_peer_worker_limit(120, 24, 3), 24);
        assert_eq!(effective_peer_worker_limit(2, 64, 5), 1);
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
    fn encryption_mode_change_requires_restart() {
        let previous = Config::default();
        let mut next = previous.clone();
        next.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Required;

        assert_eq!(
            restart_required_fields(&previous, &next),
            vec!["torrent.encryption_mode".to_string()]
        );
    }

    #[tokio::test]
    async fn replace_config_preserves_and_redacts_auth_token() {
        let mut cfg = Config::default();
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
        runtime.config.lock().await.queue.auto_start = true;
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
        assert!(runtime.engine_handles.lock().await.is_empty());
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
        assert!(runtime.engine_handles.lock().await.is_empty());
        {
            let state = runtime.queue_reconcile.lock().await;
            assert!(state.scheduled);
            assert!(state.dirty);
        }
    }

    #[tokio::test]
    async fn runtime_queue_limit_update_marks_scheduled_reconcile_dirty() {
        let mut cfg = Config::default();
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
                    max_active_seeds: 5,
                    auto_start: true,
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(runtime.config.lock().await.queue.max_active_downloads, 50);
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
        assert!(runtime.engine_handles.lock().await.is_empty());
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
        assert!(runtime.engine_handles.lock().await.is_empty());
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
        assert!(runtime.engine_handles.lock().await.is_empty());
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
        assert!(runtime.engine_handles.lock().await.is_empty());
        assert!(!runtime.queue_reconcile.lock().await.scheduled);
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
