// SPDX-License-Identifier: Apache-2.0

//! Test support: a fake in-memory `DaemonOps` implementation.
//!
//! Only compiled under `cfg(test)` / dev-dependents so it does not pollute the
//! public API surface.

use async_trait::async_trait;
use std::sync::Arc;
use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::meta::{self};
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::peer::Peer;
use swarmotter_core::models::stats::GlobalStats;
use swarmotter_core::models::torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
use swarmotter_core::models::tracker::{
    TrackerId, TrackerInfo, TrackerKind, TrackerStatus, TrackerTier,
};
use swarmotter_core::torrent::{Torrent, TorrentRegistry};
use swarmotter_core::watch::ImportResult;
use tokio::sync::Mutex;

pub struct FakeDaemon {
    registry: Arc<Mutex<TorrentRegistry>>,
    config: Arc<Mutex<Config>>,
    health: Arc<Mutex<NetworkHealth>>,
    pub watch_imports: Arc<Mutex<Vec<ImportResult>>>,
    #[allow(dead_code)]
    pub events: Arc<Mutex<Vec<String>>>,
}

impl FakeDaemon {
    pub fn new() -> Self {
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            NetworkContainmentStatus::Disabled,
            "disabled for tests",
        );
        health.traffic_allowed = true;
        Self {
            registry: Arc::new(Mutex::new(TorrentRegistry::default())),
            config: Arc::new(Mutex::new(Config::default())),
            health: Arc::new(Mutex::new(health)),
            events: Arc::new(Mutex::new(Vec::new())),
            watch_imports: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn insert(&self, torrent: Torrent) -> Result<InfoHash> {
        let hash = torrent.info_hash();
        let mut reg = self.registry.lock().await;
        reg.add(torrent)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        Ok(hash)
    }

    async fn summary(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        self.registry
            .lock()
            .await
            .get(hash)
            .map(Torrent::to_summary)
    }
}

impl Default for FakeDaemon {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl swarmotter_api::state::DaemonOps for FakeDaemon {
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
        self.summary(hash).await
    }
    async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        download_dir: Option<String>,
    ) -> Result<InfoHash> {
        let meta = meta::parse_torrent(&bytes)?;
        let mut t = Torrent::new(meta, now());
        t.download_dir = download_dir;
        self.insert(t).await
    }
    async fn add_magnet(&self, magnet: &str, download_dir: Option<String>) -> Result<InfoHash> {
        let m = Magnet::parse(magnet)?;
        // Build a minimal single-file meta from the magnet for testing.
        let bytes = meta::build_single_file_torrent(
            m.display_name.as_deref().unwrap_or("magnet"),
            b"magnet placeholder data",
            16,
            m.trackers.first().map(|s| s.as_str()),
            false,
        );
        let meta = meta::parse_torrent(&bytes)?;
        let mut t = Torrent::new(meta, now());
        t.state = TorrentState::DownloadingMetadata;
        t.download_dir = download_dir;
        self.insert(t).await
    }
    async fn remove_torrent(&self, hash: &InfoHash, _delete_data: bool) -> Result<()> {
        self.registry
            .lock()
            .await
            .remove(hash)
            .map(|_| ())
            .ok_or_else(|| CoreError::NotFound("torrent".into()))
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
        _hash: &InfoHash,
        _file_index: usize,
        _new_path: String,
    ) -> Result<()> {
        Ok(())
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
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.download_limit = limits.download;
                t.upload_limit = limits.upload;
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
            let mut tier = 0;
            if let Some(a) = &t.meta.announce {
                out.push(TrackerInfo {
                    id: TrackerId(a.clone()),
                    url: a.clone(),
                    kind: TrackerKind::from_url(a).unwrap_or(TrackerKind::Http),
                    tier,
                    status: TrackerStatus::NotContacted,
                    seeders: 0,
                    leechers: 0,
                    downloads: 0,
                    last_error: None,
                    next_announce: None,
                    last_announce: None,
                });
                tier += 1;
            }
            for t_list in &t.meta.announce_list {
                for url in t_list {
                    out.push(TrackerInfo {
                        id: TrackerId(url.clone()),
                        url: url.clone(),
                        kind: TrackerKind::from_url(url).unwrap_or(TrackerKind::Http),
                        tier,
                        status: TrackerStatus::NotContacted,
                        seeders: 0,
                        leechers: 0,
                        downloads: 0,
                        last_error: None,
                        next_announce: None,
                        last_announce: None,
                    });
                }
                tier += 1;
            }
            out
        })
    }
    async fn add_tracker(&self, _hash: &InfoHash, _url: String) -> Result<()> {
        Ok(())
    }
    async fn remove_tracker(&self, _hash: &InfoHash, _url: String) -> Result<()> {
        Ok(())
    }
    async fn edit_tracker(
        &self,
        _hash: &InfoHash,
        _old_url: String,
        _new_url: String,
    ) -> Result<()> {
        Ok(())
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
        self.health.lock().await.clone()
    }
    async fn global_stats(&self) -> GlobalStats {
        let reg = self.registry.lock().await;
        GlobalStats {
            torrent_count: reg.torrents.len(),
            ..Default::default()
        }
    }
    async fn watch_scan(&self) -> Result<()> {
        Ok(())
    }
    async fn watch_history(&self) -> Vec<ImportResult> {
        self.watch_imports.lock().await.clone()
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Build a shared state backed by a fresh fake daemon.
pub fn fake_state() -> swarmotter_api::state::SharedState {
    use swarmotter_api::state::{AppState, BuildInfo};
    Arc::new(AppState {
        daemon: Arc::new(FakeDaemon::new()),
        config: Arc::new(Mutex::new(Config::default())),
        build: BuildInfo::default(),
        broker: swarmotter_api::handlers::events::EventBroker::default(),
    })
}

// Suppress unused import warnings for models referenced only in trait impls.
#[allow(unused_imports)]
use TrackerTier as _;
