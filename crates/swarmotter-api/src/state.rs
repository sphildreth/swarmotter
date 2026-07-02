// SPDX-License-Identifier: Apache-2.0

//! Shared API state.
//!
//! The `AppState` holds a reference to the daemon's runtime state behind an
//! async-safe `Arc`. The daemon constructs the concrete state and passes it to
//! the API router. The API never creates torrent network sockets directly; it
//! issues commands to the daemon which enforces network containment.

use std::sync::Arc;
use swarmotter_core::config::Config;
use swarmotter_core::error::Result;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::models::network::NetworkHealth;
use swarmotter_core::models::peer::Peer;
use swarmotter_core::models::stats::GlobalStats;
use swarmotter_core::models::torrent::TorrentFile;
use swarmotter_core::models::torrent::TorrentSummary;
use swarmotter_core::models::tracker::TrackerInfo;
use tokio::sync::Mutex;

/// Operations the API requires from the daemon runtime.
///
/// The daemon implements this trait against its real state. Tests can provide
/// a fake implementation.
#[async_trait::async_trait]
pub trait DaemonOps: Send + Sync + 'static {
    /// List all torrents.
    async fn list_torrents(&self) -> Vec<TorrentSummary>;
    /// Get a single torrent's summary.
    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary>;
    /// Add a torrent from a `.torrent` file body.
    async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        download_dir: Option<String>,
    ) -> Result<InfoHash>;
    /// Add a torrent from a magnet URI.
    async fn add_magnet(&self, magnet: &str, download_dir: Option<String>) -> Result<InfoHash>;
    /// Remove a torrent, optionally deleting its data.
    async fn remove_torrent(&self, hash: &InfoHash, delete_data: bool) -> Result<()>;
    /// Pause a torrent.
    async fn pause(&self, hash: &InfoHash) -> Result<()>;
    /// Resume a torrent.
    async fn resume(&self, hash: &InfoHash) -> Result<()>;
    /// Start a torrent now (bypass queue).
    async fn start_now(&self, hash: &InfoHash) -> Result<()>;
    /// Stop a torrent.
    async fn stop(&self, hash: &InfoHash) -> Result<()>;
    /// Force a recheck.
    async fn recheck(&self, hash: &InfoHash) -> Result<()>;
    /// Reannounce to trackers.
    async fn reannounce(&self, hash: &InfoHash) -> Result<()>;
    /// Move torrent data.
    async fn move_data(&self, hash: &InfoHash, path: String) -> Result<()>;
    /// Rename a file/path.
    async fn rename_path(&self, hash: &InfoHash, file_index: usize, new_path: String)
        -> Result<()>;
    /// Update labels/categories.
    async fn set_labels(&self, hash: &InfoHash, labels: Vec<String>) -> Result<()>;
    /// Set per-torrent bandwidth limits (bytes/sec; 0 = unlimited). Applies
    /// live to a running engine/seeder.
    async fn set_torrent_limits(
        &self,
        hash: &InfoHash,
        limits: swarmotter_core::bandwidth::TorrentBandwidth,
    ) -> Result<()>;

    /// List files for a torrent.
    async fn list_files(&self, hash: &InfoHash) -> Option<Vec<TorrentFile>>;
    /// Set wanted/unwanted files.
    async fn set_wanted(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        wanted: bool,
    ) -> Result<()>;
    /// Set file priority.
    async fn set_priority(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        priority: swarmotter_core::models::torrent::FilePriority,
    ) -> Result<()>;

    /// List trackers for a torrent.
    async fn list_trackers(&self, hash: &InfoHash) -> Option<Vec<TrackerInfo>>;
    /// Add a tracker.
    async fn add_tracker(&self, hash: &InfoHash, url: String) -> Result<()>;
    /// Remove a tracker.
    async fn remove_tracker(&self, hash: &InfoHash, url: String) -> Result<()>;
    /// Edit a tracker.
    async fn edit_tracker(&self, hash: &InfoHash, old_url: String, new_url: String) -> Result<()>;

    /// List peers for a torrent.
    async fn list_peers(&self, hash: &InfoHash) -> Option<Vec<Peer>>;

    /// Queue: move up.
    async fn queue_move_up(&self, hash: &InfoHash) -> Result<()>;
    /// Queue: move down.
    async fn queue_move_down(&self, hash: &InfoHash) -> Result<()>;
    /// Queue: move to top.
    async fn queue_move_to_top(&self, hash: &InfoHash) -> Result<()>;
    /// Queue: move to bottom.
    async fn queue_move_to_bottom(&self, hash: &InfoHash) -> Result<()>;

    /// Get the current configuration (read-only view).
    async fn get_config(&self) -> Config;
    /// Update safe runtime settings (bandwidth/queue/seeding limits).
    async fn update_settings(&self, patch: SettingsPatch) -> Result<()>;

    /// Network containment health.
    async fn network_health(&self) -> NetworkHealth;
    /// Global stats.
    async fn global_stats(&self) -> GlobalStats;

    /// Trigger a watch-folder scan.
    async fn watch_scan(&self) -> Result<()>;
    /// Watch-folder import history.
    async fn watch_history(&self) -> Vec<swarmotter_core::watch::ImportResult>;
}

/// A patch of safe runtime settings.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SettingsPatch {
    pub bandwidth: Option<swarmotter_core::bandwidth::BandwidthLimits>,
    pub queue: Option<swarmotter_core::queue::QueueLimits>,
    pub seeding: Option<swarmotter_core::ratio::SeedingPolicy>,
}

/// Shared application state.
pub type SharedState = Arc<AppState>;

#[derive(Clone)]
pub struct AppState {
    pub daemon: Arc<dyn DaemonOps>,
    pub config: Arc<Mutex<Config>>,
    pub build: BuildInfo,
    pub broker: crate::handlers::events::EventBroker,
}

/// Build/version metadata.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BuildInfo {
    pub version: &'static str,
    pub commit: &'static str,
    pub target: &'static str,
}

impl Default for BuildInfo {
    fn default() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            commit: option_env!("SWARMOTTER_BUILD_COMMIT").unwrap_or("unknown"),
            target: std::env::consts::ARCH,
        }
    }
}

/// A helper to ignore an unused state field warning.
#[allow(dead_code)]
pub fn _state_used(s: &SharedState) -> bool {
    Arc::strong_count(s) > 0
}

// Suppress unused import in this module when some models aren't referenced yet.
#[allow(unused_imports)]
use swarmotter_core::models::torrent::TorrentState as _;
