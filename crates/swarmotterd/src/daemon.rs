// SPDX-License-Identifier: Apache-2.0

//! Daemon runtime state implementing the API's `DaemonOps` trait.
//!
//! The runtime holds torrents, configuration, network health, and watch-
//! folder state. Torrent operations enforce network containment: in strict
//! fail-closed mode, torrent data-plane activity is blocked when the
//! configured path is unavailable, and torrents enter a `network_blocked`
//! state. The control plane (API/Web UI) remains available independently.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

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

pub struct DaemonRuntime {
    pub registry: Arc<Mutex<TorrentRegistry>>,
    pub config: Arc<Mutex<Config>>,
    pub network_health: Arc<Mutex<NetworkHealth>>,
    pub watch_imports: Arc<Mutex<Vec<watch::ImportResult>>>,
}

impl DaemonRuntime {
    pub fn new(config: Config, startup_health: NetworkHealth) -> Self {
        Self {
            registry: Arc::new(Mutex::new(TorrentRegistry::default())),
            config: Arc::new(Mutex::new(config)),
            network_health: Arc::new(Mutex::new(startup_health)),
            watch_imports: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Periodically re-evaluate network containment health and flip torrent
    /// states between active and `network_blocked` as the path appears or
    /// disappears.
    pub async fn network_health_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let cfg = self.config.lock().await.clone();
            let probe = OsInterfaceProbe;
            let health = net::evaluate(&cfg.network, &probe);
            let traffic_allowed = health.traffic_allowed;
            *self.network_health.lock().await = health.clone();
            let mut reg = self.registry.lock().await;
            for t in reg.torrents.values_mut() {
                if traffic_allowed {
                    if t.state == TorrentState::NetworkBlocked {
                        t.state = TorrentState::Queued;
                    }
                } else if t.state.is_active() {
                    t.state = TorrentState::NetworkBlocked;
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

#[async_trait]
impl DaemonOps for DaemonRuntime {
    async fn list_torrents(&self) -> Vec<TorrentSummary> {
        self.registry
            .lock()
            .await
            .list()
            .iter()
            .map(|t| t.to_summary())
            .collect()
    }

    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary> {
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
        let mut reg = self.registry.lock().await;
        reg.add(t)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
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
        let mut reg = self.registry.lock().await;
        let removed = reg
            .remove(hash)
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        if delete_data {
            if let Some(dir) = &removed.download_dir {
                let path = std::path::Path::new(dir).join(removed.name());
                if path.exists() {
                    let _ = std::fs::remove_dir_all(&path);
                }
            }
        }
        Ok(())
    }

    async fn pause(&self, hash: &InfoHash) -> Result<()> {
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
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.state = TorrentState::Downloading;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn start_now(&self, hash: &InfoHash) -> Result<()> {
        self.resume(hash).await
    }

    async fn stop(&self, hash: &InfoHash) -> Result<()> {
        self.pause(hash).await
    }

    async fn recheck(&self, hash: &InfoHash) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.state = TorrentState::Checking;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }

    async fn reannounce(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
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
        self.registry.lock().await.get(hash).map(|t| {
            let mut out = Vec::new();
            let mut tier = 0usize;
            if let Some(a) = &t.meta.announce {
                out.push(make_tracker(a, tier));
                tier += 1;
            }
            for tlist in &t.meta.announce_list {
                for url in tlist {
                    out.push(make_tracker(url, tier));
                }
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

    async fn list_peers(&self, _hash: &InfoHash) -> Option<Vec<Peer>> {
        Some(Vec::new())
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
