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
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use swarmotter_api::state::DaemonOps;
use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::meta;
use swarmotter_core::models::network::{NetworkContainmentMode, NetworkHealth};
use swarmotter_core::models::peer::Peer;
use swarmotter_core::models::stats::GlobalStats;
use swarmotter_core::models::torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
use swarmotter_core::models::tracker::{TrackerId, TrackerInfo, TrackerKind, TrackerStatus};
use swarmotter_core::net::{self, OsInterfaceProbe};
use swarmotter_core::torrent::{Torrent, TorrentRegistry};
use swarmotter_core::watch;

use crate::engine::{EngineCommand, EngineState, TorrentEngine};
use crate::netbinder::ContainedBinder;

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
}

impl DaemonRuntime {
    pub fn new(config: Config, startup_health: NetworkHealth) -> Self {
        Self {
            registry: Arc::new(Mutex::new(TorrentRegistry::default())),
            config: Arc::new(Mutex::new(config)),
            network_health: Arc::new(Mutex::new(startup_health)),
            watch_imports: Arc::new(Mutex::new(Vec::new())),
            engine_states: Arc::new(Mutex::new(HashMap::new())),
            engine_cmds: Arc::new(Mutex::new(HashMap::new())),
            engine_handles: Arc::new(Mutex::new(HashMap::new())),
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

        let (meta, download_dir, listen_port) = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(&hash) else {
                return;
            };
            let download_dir = self.resolve_download_dir(t).await;
            (
                t.meta.clone(),
                download_dir,
                self.config.lock().await.torrent.listen_port,
            )
        };

        let state = Arc::new(Mutex::new(EngineState::default()));
        self.engine_states.lock().await.insert(hash, state.clone());

        let binder: Arc<dyn swarmotter_core::net::NetworkBinder> = self.make_binder().await;
        let peer_id = make_peer_id();
        let (tx, rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
        self.engine_cmds.lock().await.insert(hash, tx);

        let state_for_summary = state.clone();
        let hash_for_task = hash;
        let registry = self.registry.clone();
        let engine = TorrentEngine::new(
            meta.clone(),
            download_dir.into(),
            peer_id,
            binder,
            state.clone(),
            rx,
            vec![],
            listen_port,
        );
        let handle = tokio::spawn(async move {
            match engine.run().await {
                Ok(final_state) => {
                    let mut reg = registry.lock().await;
                    if let Some(t) = reg.get_mut(&hash_for_task) {
                        t.downloaded = final_state.downloaded;
                        t.uploaded = final_state.uploaded;
                        t.progress.have = (0..final_state.piece_count)
                            .map(|i| final_state.pieces_have.has(i))
                            .collect();
                        if final_state.finished {
                            t.state = TorrentState::Completed;
                            t.date_completed = Some(now());
                        }
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

        // Mark the torrent as downloading.
        let mut reg = self.registry.lock().await;
        if let Some(t) = reg.get_mut(&hash) {
            if t.state == TorrentState::Queued || t.state == TorrentState::NetworkBlocked {
                t.state = TorrentState::Downloading;
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
        self.engine_states.lock().await.remove(hash);
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
            }
        }
    }

    /// Copy live engine state (pieces, byte counts) into the torrent records
    /// so API/UI summaries reflect real progress while downloading.
    async fn reconcile_engine_progress(&self) {
        let states = self.engine_states.lock().await.clone();
        let mut reg = self.registry.lock().await;
        for (hash, state) in &states {
            let s = state.lock().await;
            if let Some(t) = reg.get_mut(hash) {
                t.progress.have = (0..s.piece_count).map(|i| s.pieces_have.has(i)).collect();
                t.downloaded = s.downloaded;
                t.uploaded = s.uploaded;
                if !t.state.is_error() && t.state != TorrentState::Paused {
                    if s.finished {
                        t.state = TorrentState::Completed;
                    } else if t.state == TorrentState::Queued {
                        t.state = TorrentState::Downloading;
                    }
                }
            }
        }
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

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        self.registry
            .lock()
            .await
            .list()
            .iter()
            .map(|t| t.to_summary())
            .collect()
    }

    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        self.reconcile_engine_progress().await;
        self.registry
            .lock()
            .await
            .get(hash)
            .map(Torrent::to_summary)
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
        if !blocked {
            self.start_engine(hash).await;
        }
        Ok(hash)
    }

    async fn add_magnet(&self, magnet: &str, download_dir: Option<String>) -> Result<InfoHash> {
        let m = Magnet::parse(magnet)?;
        let hash = m.info_hash;
        let name = m.display_name.clone().unwrap_or_else(|| hash.to_hex());
        // Build a placeholder single-file torrent so the registry has a record;
        // real metadata is fetched via DHT/peers (metadata exchange) once the
        // peer protocol is active.
        let bytes = meta::build_single_file_torrent(
            &name,
            b"magnet placeholder data",
            16,
            m.trackers.first().map(|s| s.as_str()),
            false,
        );
        let parsed = meta::parse_torrent(&bytes)?;
        let mut t = Torrent::new(parsed, now());
        t.state = TorrentState::DownloadingMetadata;
        if let Some(d) = download_dir {
            t.download_dir = Some(d);
        }
        apply_network_state(&mut t, &self.network_health).await;
        let mut reg = self.registry.lock().await;
        reg.add(t)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        Ok(hash)
    }

    async fn remove_torrent(&self, hash: &InfoHash, delete_data: bool) -> Result<()> {
        // Stop the live engine and clean up its resources.
        self.stop_engine(hash).await;
        let removed = {
            let mut reg = self.registry.lock().await;
            reg.remove(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?
        };
        if delete_data {
            let dir = removed.download_dir.clone().unwrap_or_else(|| {
                std::env::temp_dir()
                    .join("swarmotter-downloads")
                    .display()
                    .to_string()
            });
            let storage = swarmotter_core::storage::StorageIo::new(
                removed.meta.clone(),
                std::path::PathBuf::from(&dir),
            );
            // Best-effort removal of data files and resume metadata.
            let _ = tokio::fs::remove_file(storage.resume_path()).await;
            for i in 0..removed.meta.files.len() {
                if let Ok(p) = storage.file_path(i) {
                    let _ = tokio::fs::remove_file(&p).await;
                }
            }
            if removed.meta.is_multi_file {
                let _ = tokio::fs::remove_dir(&dir).await;
            }
        }
        Ok(())
    }

    async fn pause(&self, hash: &InfoHash) -> Result<()> {
        // Stop the live engine; the torrent stays in the registry as paused.
        self.stop_engine(hash).await;
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.state = TorrentState::Paused;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn resume(&self, hash: &InfoHash) -> Result<()> {
        {
            let mut reg = self.registry.lock().await;
            match reg.get_mut(hash) {
                Some(t) => {
                    t.state = TorrentState::Downloading;
                    t.error = None;
                }
                None => return Err(CoreError::NotFound("torrent".into())),
            }
        }
        self.start_engine(*hash).await;
        Ok(())
    }

    async fn start_now(&self, hash: &InfoHash) -> Result<()> {
        self.resume(hash).await
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
        let (meta, download_dir) = {
            let reg = self.registry.lock().await;
            let Some(t) = reg.get(hash) else {
                return Err(CoreError::NotFound("torrent".into()));
            };
            let dir = t.download_dir.clone().unwrap_or_else(|| {
                std::env::temp_dir()
                    .join("swarmotter-downloads")
                    .display()
                    .to_string()
            });
            (t.meta.clone(), dir)
        };
        let storage = swarmotter_core::storage::StorageIo::new(
            meta.clone(),
            std::path::PathBuf::from(&download_dir),
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

    async fn queue_move_up(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn queue_move_down(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn queue_move_to_top(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn queue_move_to_bottom(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }

    async fn get_config(&self) -> Config {
        self.config.lock().await.clone()
    }

    async fn update_settings(&self, patch: swarmotter_api::state::SettingsPatch) -> Result<()> {
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
        Ok(())
    }

    async fn network_health(&self) -> NetworkHealth {
        self.network_health.lock().await.clone()
    }

    async fn global_stats(&self) -> GlobalStats {
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
            torrent_count: reg.torrents.len(),
            active_downloads,
            active_seeds,
            paused,
            ..Default::default()
        }
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
