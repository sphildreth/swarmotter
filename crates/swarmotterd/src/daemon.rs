// SPDX-License-Identifier: Apache-2.0

//! Daemon runtime state implementing the API's `DaemonOps` trait.
//!
//! The runtime holds torrents, configuration, network health, and watch-
//! folder state. Torrent operations enforce network containment: in strict
//! fail-closed mode, torrent data-plane activity is blocked when the
//! configured path is unavailable, and torrents enter a `network_blocked`
//! state. The control plane (API/Web UI) remains available independently.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use swarmotter_api::state::DaemonOps;
use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::meta;
use swarmotter_core::models::health::{HealthCalculator, HealthInput};
use swarmotter_core::models::network::{NetworkContainmentMode, NetworkHealth};
use swarmotter_core::models::peer::{EnginePeerHealth, Peer};
use swarmotter_core::models::stats::{GlobalStats, TorrentDiagnostics};
use swarmotter_core::models::torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
use swarmotter_core::models::tracker::{TrackerId, TrackerInfo, TrackerKind, TrackerStatus};
use swarmotter_core::models::{
    ConfigUpdateResult, DiagnosticLevel, DoctorCheck, DoctorReport, LogSnapshot,
    NetworkDiagnostics, NetworkInterfaceDiagnostic, NetworkPathCheck, WatchFolderStatus,
    WatchStatus,
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
    /// Runtime queue state backing queue positions and queue move operations.
    queue: Arc<Mutex<QueueState>>,
    /// Shared DHT runner so the configured DHT port is bound by one runner
    /// instead of once per active torrent.
    dht_runner: Arc<Mutex<Option<Arc<crate::dht::DhtRunner>>>>,
}

#[derive(Debug, Clone, Copy)]
struct RateSample {
    downloaded: u64,
    uploaded: u64,
    rate_down: u64,
    rate_up: u64,
    last_download_at: Option<Instant>,
    last_upload_at: Option<Instant>,
    at: Instant,
    /// Highest smoothed download rate observed for this torrent; used by the
    /// health calculator as a normalization reference when no bandwidth cap
    /// is set.
    peak_rate_down: u64,
    /// Highest smoothed upload rate observed for this torrent. This is
    /// recorded for operational troubleshooting and structured performance
    /// logs.
    peak_rate_up: u64,
}

#[derive(Debug, Clone, Default)]
struct LiveTorrentDiagnostics {
    active_peer_workers: usize,
    known_peers: usize,
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
            dht_runner: Arc::new(Mutex::new(None)),
        }
    }

    /// Resolve the download directory for a torrent: per-torrent override,
    /// then global config, then a default temp dir.
    async fn resolve_download_dir(&self, t: &Torrent) -> String {
        if let Some(d) = &t.download_dir {
            return d.clone();
        }
        let cfg = self.config.lock().await;
        cfg.storage.download_dir.clone().unwrap_or_else(|| {
            std::env::temp_dir()
                .join("swarmotter-downloads")
                .display()
                .to_string()
        })
    }

    /// Resolve the active write directory for a torrent. Incomplete downloads
    /// use the configured incomplete directory when present; otherwise they
    /// write directly to the final download directory.
    async fn resolve_incomplete_dir(&self, download_dir: &str) -> String {
        let cfg = self.config.lock().await;
        cfg.storage
            .incomplete_dir
            .clone()
            .unwrap_or_else(|| download_dir.to_string())
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
        let reg = self.registry.lock().await;
        reg.torrents
            .iter()
            .filter_map(|(hash, t)| {
                if matches!(
                    t.state,
                    TorrentState::Downloading | TorrentState::DownloadingMetadata
                ) {
                    Some(*hash)
                } else {
                    None
                }
            })
            .collect()
    }

    async fn desired_download_hashes(&self) -> Vec<InfoHash> {
        let cfg = self.config.lock().await.clone();
        let mut queue = self.queue.lock().await;
        queue.limits = cfg.queue.clone();
        let reg = self.registry.lock().await;
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
        let desired = self.desired_download_hashes().await;
        let current = self.active_download_hashes().await;

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
            magnet,
            needs_metadata,
        ) = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(&hash) else {
                return;
            };
            let complete_dir = self.resolve_download_dir(t).await;
            let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
            let magnet = if t.needs_metadata {
                Some(crate::engine::MagnetParams {
                    info_hash: t.magnet_info_hash.unwrap_or(t.meta.info_hash),
                    name: t.magnet_name.clone().unwrap_or_else(|| t.meta.name.clone()),
                    trackers: t.magnet_trackers.clone(),
                })
            } else {
                None
            };
            let cfg = self.config.lock().await;
            let preallocate = cfg.storage.preallocate;
            let sparse = cfg.storage.sparse;
            let allow_ipv6 = cfg.torrent.allow_ipv6 && cfg.network.allow_ipv6;
            let pex_enabled = cfg.pex.enabled;
            let pex_max_peers = cfg.pex.max_peers;
            let max_peer_workers = effective_peer_worker_limit(
                cfg.bandwidth.max_peers,
                cfg.bandwidth.max_peers_per_torrent,
                1,
            );
            (
                t.meta.clone(),
                active_dir,
                complete_dir,
                cfg.torrent.listen_port,
                preallocate,
                sparse,
                max_peer_workers,
                allow_ipv6,
                pex_enabled,
                pex_max_peers,
                magnet,
                t.needs_metadata,
            )
        };

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
        let limiter = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(&hash) else {
                return;
            };
            swarmotter_core::bandwidth::RateLimiter::new(t.download_limit, t.upload_limit)
        };
        self.engine_limiters
            .lock()
            .await
            .insert(hash, limiter.clone());
        // Peer transport selection (TCP/uTP) from config. All transports stay
        // on the contained binder; fail-closed blocks both.
        let (utp_enabled, utp_prefer_tcp) = {
            let cfg = self.config.lock().await;
            (cfg.torrent.utp_enabled, cfg.torrent.utp_prefer_tcp)
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
        .with_preallocate(preallocate)
        .with_sparse(sparse)
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
                    }
                }
                Err(e) => {
                    tracing::warn!(info_hash = %hash_for_task, error = %e, "engine task failed");
                    let mut reg = registry.lock().await;
                    if let Some(t) = reg.get_mut(&hash_for_task) {
                        t.state = TorrentState::Error;
                        t.error = Some(e.to_string());
                    }
                }
            }
            let _ = state_for_summary;
        });
        self.engine_handles.lock().await.insert(hash, handle);

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
                        let last_download_at = if down_delta > 0 {
                            Some(now)
                        } else {
                            prev.last_download_at
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
                        peak = previous_peak_down.max(t.rate_down);
                        let peak_rate_up = previous_peak_up.max(t.rate_up);
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
                    } else if t.state == TorrentState::Queued {
                        t.state = TorrentState::Downloading;
                    } else if t.state == TorrentState::DownloadingMetadata {
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
        self.reconcile_seeders().await;
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
        self.reconcile_queue().await;
        self.apply_peer_worker_limits().await;
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
        download_dir: Option<String>,
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
        if let Some(d) = download_dir {
            t.download_dir = Some(d);
        }
        apply_network_state(&mut t, &self.network_health).await;
        let blocked = t.state == TorrentState::NetworkBlocked;
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
        if !blocked {
            self.reconcile_queue().await;
        }
        tracing::info!(
            info_hash = %hash,
            network_blocked = blocked,
            "torrent file added"
        );
        Ok(hash)
    }

    async fn add_magnet(&self, magnet: &str, download_dir: Option<String>) -> Result<InfoHash> {
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
        if let Some(d) = download_dir {
            t.download_dir = Some(d);
        }
        apply_network_state(&mut t, &self.network_health).await;
        let blocked = t.state == TorrentState::NetworkBlocked;
        {
            let mut reg = self.registry.lock().await;
            reg.add(t)
                .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        }
        tracing::info!(
            info_hash = %hash,
            network_blocked = blocked,
            tracker_count = m.trackers.len(),
            "magnet added"
        );
        self.queue.lock().await.add(hash);
        if !blocked {
            self.reconcile_queue().await;
        }
        Ok(hash)
    }

    async fn remove_torrent(&self, hash: &InfoHash, delete_data: bool) -> Result<()> {
        let removed = {
            let mut reg = self.registry.lock().await;
            reg.remove(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?
        };
        self.queue.lock().await.remove(hash);
        self.rate_samples.lock().await.remove(hash);
        // Removal must not wait for an active peer session to reach its next
        // command poll. Abort the live data-plane task before deleting files.
        self.force_stop_engine(hash).await;
        if delete_data {
            let complete_dir = self.resolve_download_dir(&removed).await;
            let active_dir = self.resolve_incomplete_dir(&complete_dir).await;
            let mut dirs = vec![active_dir, complete_dir];
            dirs.dedup();
            for dir in dirs {
                let storage = swarmotter_core::storage::StorageIo::new(
                    removed.meta.clone(),
                    std::path::PathBuf::from(&dir),
                );
                let _ = storage.remove_all().await;
            }
        }
        self.reconcile_queue().await;
        Ok(())
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
        // Reflect real tracker status from the live engine, if present.
        let engine_tracker_ok = self
            .engine_states
            .lock()
            .await
            .get(hash)
            .and_then(|s| s.try_lock().ok())
            .map(|s| (s.tracker_ok, s.tracker_message.clone(), s.last_announce));
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
                if let Some((ok, msg, last)) = &engine_tracker_ok {
                    info.status = if *ok {
                        TrackerStatus::Ok
                    } else {
                        TrackerStatus::Error
                    };
                    info.last_error = msg.clone();
                    info.last_announce = *last;
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
            ],
            config: redact_config(next),
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
                        "TCP is {}, uTP is {}, preference is {}",
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
                        }
                    ),
                },
            ],
            containment_matrix: containment_matrix(&cfg, traffic_level),
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
        next_announce: None,
        last_announce: None,
    }
}

/// Apply current network containment state to a torrent's lifecycle state.
async fn apply_network_state(t: &mut Torrent, health: &Arc<Mutex<NetworkHealth>>) {
    let h = health.lock().await;
    if !h.traffic_allowed && h.mode != NetworkContainmentMode::Disabled {
        t.state = TorrentState::NetworkBlocked;
        t.error = Some(h.detail.clone());
    }
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
        let has_missing =
            (p.has_missing_pieces && last_seen_recent) || (useful_recently && last_seen_recent);
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
                rate_down: 0,
                rate_up: 0,
                last_download_at: None,
                last_upload_at: None,
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
        assert_eq!(peak_sample.peak_rate_down, summary.rate_down);
        assert_eq!(peak_sample.peak_rate_up, summary.rate_up);

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
        runtime
            .registry
            .lock()
            .await
            .add(Torrent::new(meta.clone(), 1))
            .unwrap();
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
                ..Default::default()
            })),
        );

        let stats = runtime.torrent_stats(&hash).await.unwrap();

        assert_eq!(stats.info_hash, hash);
        assert_eq!(stats.active_peer_workers, 4);
        assert_eq!(stats.known_peers, 2);
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
