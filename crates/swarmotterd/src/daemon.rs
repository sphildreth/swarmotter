// SPDX-License-Identifier: Apache-2.0

//! Daemon runtime state implementing the API's `DaemonOps` trait.
//!
//! The runtime holds torrents, configuration, network health, and watch-
//! folder state. Torrent operations enforce network containment: in strict
//! fail-closed mode, torrent data-plane activity is blocked when the
//! configured path is unavailable, and torrents enter a `network_blocked`
//! state. The control plane (API/Web UI) remains available independently.

use std::collections::HashMap;
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
use swarmotter_core::net::{self, OsInterfaceProbe};
use swarmotter_core::queue::QueueState;
use swarmotter_core::ratio::{self, SeedDecision, TorrentAccounting, TorrentSeeding};
use swarmotter_core::torrent::{Torrent, TorrentRegistry};
use swarmotter_core::watch;

use crate::engine::{EngineCommand, EngineState, TorrentEngine};
use crate::netbinder::ContainedBinder;
use crate::seeder::Seeder;

pub struct DaemonRuntime {
    pub registry: Arc<Mutex<TorrentRegistry>>,
    pub config: Arc<Mutex<Config>>,
    pub network_health: Arc<Mutex<NetworkHealth>>,
    pub watch_imports: Arc<Mutex<Vec<watch::ImportResult>>>,
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
}

impl DaemonRuntime {
    pub fn new(config: Config, startup_health: NetworkHealth) -> Self {
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
                            if let Some(real) = final_state.resolved_meta.clone() {
                                t.meta = real.clone();
                                t.needs_metadata = false;
                                t.progress.have = (0..real.piece_count())
                                    .map(|i| final_state.pieces_have.has(i))
                                    .collect();
                                t.files = real
                                    .files
                                    .iter()
                                    .enumerate()
                                    .map(|(i, f)| swarmotter_core::models::torrent::TorrentFile {
                                        index: i,
                                        path: f.path.join("/"),
                                        length: f.length,
                                        bytes_completed: 0,
                                        priority:
                                            swarmotter_core::models::torrent::FilePriority::Normal,
                                        wanted: true,
                                    })
                                    .collect();
                                t.priorities = vec![
                                    swarmotter_core::models::torrent::FilePriority::Normal;
                                    real.files.len()
                                ];
                                t.wanted = vec![true; real.files.len()];
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
                        peak = peak.max(t.rate_down);
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
                        },
                    );
                }
                t.progress.have = (0..s.piece_count).map(|i| s.pieces_have.has(i)).collect();
                t.downloaded = s.downloaded;
                t.uploaded = s.uploaded;
                t.active_peer_workers = s.active_peers;
                t.known_peers = s.peers.len();
                if !t.state.is_error() && t.state != TorrentState::Paused {
                    if s.finished {
                        t.state = TorrentState::Completed;
                    } else if t.state == TorrentState::Queued {
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

/// Generate a stable per-daemon peer id (`-SW0001-` + 12 bytes of zeros).
fn make_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-SW0001-");
    id
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
        let parsed = meta::parse_torrent(&bytes)?;
        let hash = parsed.info_hash;
        let mut t = Torrent::new(parsed, now());
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
        self.queue.lock().await.add(hash);
        if !blocked {
            self.reconcile_queue().await;
        }
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
        let eff_dl;
        let eff_ul;
        let queue_limits;
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
            eff_dl = cfg.bandwidth.effective_download();
            eff_ul = cfg.bandwidth.effective_upload();
            queue_limits = cfg.queue.clone();
        }
        self.queue.lock().await.limits = queue_limits;
        // Apply the new global limits live to the shared limiter (and therefore
        // to all running engines/seeders, which share its buckets).
        self.global_limiter
            .set_capacity(swarmotter_core::bandwidth::RateDirection::Download, eff_dl)
            .await;
        self.global_limiter
            .set_capacity(swarmotter_core::bandwidth::RateDirection::Upload, eff_ul)
            .await;
        self.reconcile_queue().await;
        self.apply_peer_worker_limits().await;
        self.reconcile_seeders().await;
        Ok(())
    }

    async fn network_health(&self) -> NetworkHealth {
        self.network_health.lock().await.clone()
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
            Some((
                s.active_peers,
                s.peers.len(),
                s.tracker_ok,
                s.tracker_message.clone(),
                s.last_announce,
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
        let (active_peer_workers, known_peers, tracker_ok, tracker_message, last_announce) =
            live.unwrap_or((0, 0, false, None, None));
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
            active_peer_workers,
            known_peers,
            tracker_ok,
            tracker_message,
            last_announce,
            private: t.meta.is_private(),
        })
    }

    async fn watch_scan(&self) -> Result<()> {
        self.scan_watch_folders().await
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
        || peer_block_recent
        || t.rate_down > 0;
    let time_since_last_block = last_valid_block
        .or(block_last_seen)
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
        let useful_recently = p.useful_recently
            || last_valid
                .map(|t| now.duration_since(t) < recent_window)
                .unwrap_or(false);
        let unchoked = p.unchoked || useful_recently;
        let last_seen_recent = last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
        let has_missing = p.has_missing_pieces || (useful_recently && last_seen_recent);
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
            },
        );

        let summary = runtime.get_torrent(&hash).await.unwrap();
        assert!(summary.rate_down > 0);
        assert!(summary.rate_up > 0);
        assert_eq!(summary.downloaded, 5_000);
        assert_eq!(summary.uploaded, 1_200);

        let stats = runtime.global_stats().await;
        assert_eq!(stats.download_rate, summary.rate_down);
        assert_eq!(stats.upload_rate, summary.rate_up);
        assert_eq!(stats.total_downloaded, 5_000);
        assert_eq!(stats.total_uploaded, 1_200);
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
                tracker_ok: true,
                tracker_message: Some("ok".into()),
                last_announce: Some(123),
                ..Default::default()
            })),
        );

        let stats = runtime.torrent_stats(&hash).await.unwrap();

        assert_eq!(stats.info_hash, hash);
        assert_eq!(stats.active_peer_workers, 4);
        assert_eq!(stats.known_peers, 2);
        assert!(stats.tracker_ok);
        assert_eq!(stats.tracker_message.as_deref(), Some("ok"));
        assert_eq!(stats.last_announce, Some(123));

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
