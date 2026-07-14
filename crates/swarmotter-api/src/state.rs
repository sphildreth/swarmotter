// SPDX-License-Identifier: Apache-2.0

//! Shared API state.
//!
//! The `AppState` holds a reference to the daemon's runtime state behind an
//! async-safe `Arc`. The daemon constructs the concrete state and passes it to
//! the API router. The API never creates torrent network sockets directly; it
//! issues commands to the daemon which enforces network containment.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use swarmotter_core::autopilot::{AutopilotConfig, AutopilotMode};
use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::TorrentKey;
use swarmotter_core::models::diagnostics::{
    ConfigUpdateResult, DoctorReport, LogSnapshot, NetworkDiagnostics, ResetResult, WatchStatus,
};
use swarmotter_core::models::network::NetworkHealth;
use swarmotter_core::models::peer::Peer;
use swarmotter_core::models::stats::{AutopilotDecision, GlobalStats, TorrentDiagnostics};
use swarmotter_core::models::storage::StorageDiagnostics;
use swarmotter_core::models::torrent::TorrentFile;
use swarmotter_core::models::torrent::TorrentSummary;
use swarmotter_core::models::tracker::TrackerInfo;
use swarmotter_core::peer_filter::{ManualPeerBan, PeerFilterConfig, PeerFilterStatus};
use swarmotter_core::policy::PolicyFileExclusionRule;
use swarmotter_core::port_mapping::PortMappingStatus;
use swarmotter_core::port_test::PortTestStatus;
use tokio::sync::Mutex;

/// Options applied when registering a newly added torrent.
#[derive(Debug, Clone, Default)]
pub struct AddTorrentOptions {
    pub download_dir: Option<String>,
    /// Explicit active-data root for this newly registered torrent. This is
    /// independent from the completed-data root and wins over a profile's
    /// incomplete directory for the captured storage snapshot.
    pub incomplete_dir: Option<String>,
    pub paused: bool,
    /// Whether `paused` came from an explicit caller choice. If false, the
    /// daemon resolves the effective profile/global start behavior.
    pub start_behavior_explicit: bool,
    /// Explicit profile requested at add time.
    pub profile: Option<String>,
    /// Labels assigned before policy resolution so label mappings can select a
    /// profile deterministically.
    pub labels: Vec<String>,
    /// File indices that must be marked unwanted before payload transfer. For
    /// magnets the same captured selection is applied after contained BEP 9
    /// metadata retrieval reveals the real file list.
    pub unwanted_file_indices: Vec<usize>,
    /// Additional structured rules captured alongside the selected profile's
    /// intake rules. They are evaluated before payload work, including after
    /// a magnet's contained metadata resolution.
    pub file_exclusion_rules: Vec<PolicyFileExclusionRule>,
    /// Optional active-only filename suffix such as `.part`. When omitted,
    /// the selected profile's intake suffix is retained.
    pub partial_file_suffix: Option<String>,
    /// Register as a metadata-first preview. A `.torrent` remains paused; a
    /// magnet may retrieve metadata through the daemon's contained network
    /// path, then pauses before any payload storage or transfer begins.
    pub preview: bool,
}

/// A read-only proposal for resolving payload locations. It deliberately
/// performs no filesystem operation and never changes the torrent; callers
/// use it to inspect a move or profile assignment before applying it.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoragePathPreviewRequest {
    /// Proposed explicit completed-data root. Omit to retain the current
    /// resolved location.
    #[serde(default)]
    pub download_dir: Option<String>,
    /// Proposed explicit active-data root. Omit to retain the current
    /// resolved location.
    #[serde(default)]
    pub incomplete_dir: Option<String>,
    /// Proposed explicit profile assignment. Profile storage never silently
    /// moves existing data, so a response can make that invariant visible
    /// before a caller applies the assignment.
    #[serde(default)]
    pub profile: Option<String>,
}

/// Bounded, deterministic filesystem paths a storage operation would use.
/// Paths are strings for API/UI display only; this preview never creates,
/// opens, or moves a payload file.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StoragePathPreview {
    pub complete_dir: String,
    pub incomplete_dir: String,
    pub partial_file_suffix: Option<String>,
    pub complete_files: Vec<String>,
    pub incomplete_files: Vec<String>,
    pub file_count: usize,
    pub truncated: bool,
}

impl AddTorrentOptions {
    pub fn new(download_dir: Option<String>, paused: bool) -> Self {
        Self {
            download_dir,
            incomplete_dir: None,
            paused,
            // Preserve the historical programmatic-add behavior: `false`
            // delegates to the daemon's queue/profile policy, while `true`
            // remains an explicit pause request.
            start_behavior_explicit: paused,
            profile: None,
            labels: Vec::new(),
            unwanted_file_indices: Vec::new(),
            file_exclusion_rules: Vec::new(),
            partial_file_suffix: None,
            preview: false,
        }
    }

    /// Construct options from an API request where no paused/start value may
    /// have been supplied. Existing programmatic callers should use `new`.
    pub fn request(
        download_dir: Option<String>,
        paused: bool,
        start_behavior_explicit: bool,
        profile: Option<String>,
        labels: Vec<String>,
    ) -> Self {
        Self {
            download_dir,
            incomplete_dir: None,
            paused,
            start_behavior_explicit,
            profile,
            labels,
            unwanted_file_indices: Vec::new(),
            file_exclusion_rules: Vec::new(),
            partial_file_suffix: None,
            preview: false,
        }
    }
}

/// Operations the API requires from the daemon runtime.
///
/// The daemon implements this trait against its real state. Tests can provide
/// a fake implementation.
#[async_trait::async_trait]
pub trait DaemonOps: Send + Sync + 'static {
    /// List all torrents.
    async fn list_torrents(&self) -> Vec<TorrentSummary>;
    /// Get a single torrent's summary.
    async fn get_torrent(&self, hash: &TorrentKey) -> Option<TorrentSummary>;
    /// Return the exact original full `.torrent` document retained for an
    /// existing torrent. This never reconstructs a document from canonical
    /// metadata or performs any network retrieval.
    async fn original_metainfo(&self, _hash: &TorrentKey) -> Result<Vec<u8>> {
        Err(CoreError::NotFound(
            "original torrent metainfo is unavailable for this torrent".into(),
        ))
    }
    /// Add a torrent from a `.torrent` file body.
    async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        options: AddTorrentOptions,
    ) -> Result<TorrentKey>;
    /// Add a torrent from a magnet URI.
    async fn add_magnet(&self, magnet: &str, options: AddTorrentOptions) -> Result<TorrentKey>;
    /// Remove a torrent, optionally deleting its data.
    async fn remove_torrent(&self, hash: &TorrentKey, delete_data: bool) -> Result<()>;
    /// Remove multiple torrents, optionally deleting their data.
    async fn remove_torrents(
        &self,
        hashes: Vec<TorrentKey>,
        delete_data: bool,
    ) -> Result<Vec<TorrentKey>> {
        let mut removed = Vec::new();
        for hash in hashes {
            match self.remove_torrent(&hash, delete_data).await {
                Ok(()) => removed.push(hash),
                Err(CoreError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(removed)
    }
    /// Pause a torrent.
    async fn pause(&self, hash: &TorrentKey) -> Result<()>;
    /// Resume a torrent.
    async fn resume(&self, hash: &TorrentKey) -> Result<()>;
    /// Start a torrent now (bypass queue).
    async fn start_now(&self, hash: &TorrentKey) -> Result<()>;
    /// Stop a torrent.
    async fn stop(&self, hash: &TorrentKey) -> Result<()>;
    /// Force a recheck.
    async fn recheck(&self, hash: &TorrentKey) -> Result<()>;
    /// Reannounce to trackers.
    async fn reannounce(&self, hash: &TorrentKey) -> Result<()>;
    /// Move torrent data.
    async fn move_data(&self, hash: &TorrentKey, path: String) -> Result<()>;
    /// Rename a file/path.
    async fn rename_path(
        &self,
        hash: &TorrentKey,
        file_index: usize,
        new_path: String,
    ) -> Result<()>;
    /// Update labels/categories.
    async fn set_labels(&self, hash: &TorrentKey, labels: Vec<String>) -> Result<()>;
    /// Set per-torrent bandwidth limits (bytes/sec; 0 = unlimited). Applies
    /// live to a running engine/seeder.
    async fn set_torrent_limits(
        &self,
        hash: &TorrentKey,
        limits: swarmotter_core::bandwidth::TorrentBandwidth,
    ) -> Result<()>;
    /// Replace the complete persisted per-torrent seeding policy.
    async fn set_torrent_seeding(
        &self,
        hash: &TorrentKey,
        seeding: swarmotter_core::ratio::TorrentSeeding,
    ) -> Result<TorrentSummary>;

    /// Explain the profile-derived effective policy for a torrent.
    async fn torrent_policy(
        &self,
        _hash: &TorrentKey,
    ) -> Option<swarmotter_core::policy::EffectiveTorrentPolicy> {
        None
    }
    /// Preview the contained local filesystem paths a move or assignment
    /// would resolve to. This is intentionally read-only and has no network
    /// behavior.
    async fn preview_torrent_storage_paths(
        &self,
        _hash: &TorrentKey,
        _request: StoragePathPreviewRequest,
    ) -> Result<StoragePathPreview> {
        Err(CoreError::NotFound("torrent".into()))
    }
    /// Assign or clear an explicit profile for a torrent. Storage paths remain
    /// unchanged; moving data is a separate explicit operation.
    async fn set_torrent_profile(
        &self,
        _hash: &TorrentKey,
        _profile: Option<String>,
    ) -> Result<()> {
        Err(CoreError::NotFound("torrent".into()))
    }
    /// Set or clear a durable per-torrent peer-wire encryption override.
    /// `None` restores profile/label/global inheritance.
    async fn set_torrent_encryption_mode(
        &self,
        _hash: &TorrentKey,
        _encryption_mode: Option<swarmotter_core::config::PeerEncryptionMode>,
    ) -> Result<()> {
        Err(CoreError::NotFound("torrent".into()))
    }

    /// List files for a torrent.
    async fn list_files(&self, hash: &TorrentKey) -> Option<Vec<TorrentFile>>;
    /// Set wanted/unwanted files.
    async fn set_wanted(
        &self,
        hash: &TorrentKey,
        file_indices: Vec<usize>,
        wanted: bool,
    ) -> Result<()>;
    /// Set file priority.
    async fn set_priority(
        &self,
        hash: &TorrentKey,
        file_indices: Vec<usize>,
        priority: swarmotter_core::models::torrent::FilePriority,
    ) -> Result<()>;

    /// List trackers for a torrent.
    async fn list_trackers(&self, hash: &TorrentKey) -> Option<Vec<TrackerInfo>>;
    /// Add a tracker.
    async fn add_tracker(&self, hash: &TorrentKey, url: String) -> Result<()>;
    /// Remove a tracker.
    async fn remove_tracker(&self, hash: &TorrentKey, url: String) -> Result<()>;
    /// Edit a tracker.
    async fn edit_tracker(&self, hash: &TorrentKey, old_url: String, new_url: String)
        -> Result<()>;

    /// List peers for a torrent.
    async fn list_peers(&self, hash: &TorrentKey) -> Option<Vec<Peer>>;

    /// Report the compiled global peer-admission policy and its active-instance
    /// counters. This is distinct from network containment: admitted peers
    /// still use the contained data-plane binder.
    async fn peer_filter_status(&self) -> PeerFilterStatus;
    /// Replace the complete global peer-admission policy through the daemon's
    /// normal persistent configuration transaction.
    async fn replace_peer_filter(&self, peer_filter: PeerFilterConfig) -> Result<PeerFilterStatus>;
    /// Add or update a global manual IP ban after confirming the supplied
    /// torrent exists. The torrent hash scopes the UI action; the policy is
    /// deliberately global so every torrent receives the same protection.
    async fn ban_peer(&self, hash: &TorrentKey, ban: ManualPeerBan) -> Result<PeerFilterStatus>;
    /// Remove a global manual IP ban after confirming the supplied torrent
    /// exists. Removing an absent ban is idempotent.
    async fn unban_peer(&self, hash: &TorrentKey, ip: String) -> Result<PeerFilterStatus>;
    /// Remove a global manual IP ban without requiring a currently selected
    /// torrent. This supports the global peer-admission settings surface.
    /// Removing an absent ban is idempotent.
    async fn unban_global_peer(&self, ip: String) -> Result<PeerFilterStatus>;

    /// Queue: move up.
    async fn queue_move_up(&self, hash: &TorrentKey) -> Result<()>;
    /// Queue: move down.
    async fn queue_move_down(&self, hash: &TorrentKey) -> Result<()>;
    /// Queue: move to top.
    async fn queue_move_to_top(&self, hash: &TorrentKey) -> Result<()>;
    /// Queue: move to bottom.
    async fn queue_move_to_bottom(&self, hash: &TorrentKey) -> Result<()>;

    /// Get the current configuration (read-only view).
    async fn get_config(&self) -> Config;
    /// Update safe runtime settings (bandwidth/queue/seeding limits).
    async fn update_settings(&self, patch: SettingsPatch) -> Result<()>;
    /// Replace the full validated configuration.
    async fn replace_config(&self, config: Config) -> Result<ConfigUpdateResult>;
    /// Reset all download state, configured storage contents, and daemon logs.
    async fn reset_downloads(&self) -> Result<ResetResult>;

    /// Network containment health.
    async fn network_health(&self) -> NetworkHealth;
    /// Last known opt-in listen-port reachability result. This is
    /// informational and never changes torrent lifecycle behavior.
    async fn port_test_status(&self) -> PortTestStatus {
        let config = self.get_config().await;
        if !config.port_test.enabled {
            PortTestStatus::disabled(config.torrent.listen_port)
        } else if config.port_test.endpoint.is_none() {
            PortTestStatus::unconfigured(config.torrent.listen_port)
        } else {
            PortTestStatus::unknown(config.torrent.listen_port)
        }
    }
    /// Run an opt-in reachability test. Implementations must use the
    /// contained data-plane path; the default preserves a no-I/O fake daemon.
    async fn run_port_test(&self) -> PortTestStatus {
        self.port_test_status().await
    }
    /// Current opt-in router port-mapping lifecycle snapshot. It is
    /// informational: a mapping failure never changes torrent scheduling.
    async fn port_mapping_status(&self) -> PortMappingStatus {
        let config = self.get_config().await;
        if config.port_mapping.enabled {
            PortMappingStatus::pending(&config.port_mapping, config.torrent.listen_port)
        } else {
            PortMappingStatus::disabled(config.torrent.listen_port)
        }
    }
    /// Force an immediate contained router mapping reconciliation. The
    /// default remains no-I/O for API test fakes.
    async fn refresh_port_mapping(&self) -> PortMappingStatus {
        self.port_mapping_status().await
    }
    /// Rich network diagnostics for API dashboards.
    async fn network_diagnostics(&self) -> NetworkDiagnostics;
    /// Storage root diagnostics for API dashboards.
    async fn storage_roots(&self) -> StorageDiagnostics {
        let cfg = self.get_config().await;
        let download_root = cfg.storage.download_dir.clone().unwrap_or_else(|| {
            cfg.storage
                .temp_dir
                .as_ref()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(std::env::temp_dir)
                .join("swarmotter-downloads")
                .display()
                .to_string()
        });
        let mut configured_roots = BTreeMap::new();
        configured_roots.insert(
            download_root,
            vec![swarmotter_core::models::storage::StorageRootRole::Download],
        );
        for (path, role) in [
            (
                cfg.storage.resume_dir.as_ref(),
                swarmotter_core::models::storage::StorageRootRole::Resume,
            ),
            (
                cfg.storage.state_dir.as_ref(),
                swarmotter_core::models::storage::StorageRootRole::State,
            ),
            (
                cfg.storage.temp_dir.as_ref(),
                swarmotter_core::models::storage::StorageRootRole::Temporary,
            ),
        ] {
            if let Some(path) = path {
                configured_roots
                    .entry(path.clone())
                    .or_insert_with(Vec::new)
                    .push(role);
            }
        }
        if cfg.logging.file {
            if let Some(path) = cfg
                .logging
                .file_path
                .as_deref()
                .and_then(|path| std::path::Path::new(path).parent())
            {
                configured_roots
                    .entry(path.display().to_string())
                    .or_insert_with(Vec::new)
                    .push(swarmotter_core::models::storage::StorageRootRole::Log);
            }
        }
        let roots = configured_roots
            .into_iter()
            .map(|(root, roles)| {
                swarmotter_core::storage::inspect_storage_root(
                    std::path::Path::new(&root),
                    roles,
                    &cfg.storage,
                    swarmotter_core::storage::StorageRootUsage::default(),
                )
            })
            .collect();
        StorageDiagnostics {
            roots,
            minimum_free_space_bytes: cfg.storage.minimum_free_space_bytes,
            minimum_free_space_percent: cfg.storage.minimum_free_space_percent,
            generated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }
    /// Doctor/system health checks.
    async fn doctor_report(&self) -> DoctorReport;
    /// Recent daemon log lines.
    async fn recent_logs(&self, max_lines: usize) -> LogSnapshot;
    /// Global stats.
    async fn global_stats(&self) -> GlobalStats;
    /// Per-torrent diagnostics and stats.
    async fn torrent_stats(&self, hash: &TorrentKey) -> Option<TorrentDiagnostics>;
    /// Global autopilot status exposed through the API.
    async fn autopilot_status(&self) -> AutopilotConfig {
        self.get_config().await.autopilot
    }
    /// Per-torrent autopilot decision and snapshot.
    async fn torrent_autopilot_decision(&self, _hash: &TorrentKey) -> Option<AutopilotDecision> {
        None
    }
    /// Set or clear a per-torrent autopilot mode override.
    async fn set_torrent_autopilot_mode_override(
        &self,
        _hash: &TorrentKey,
        _mode: Option<AutopilotMode>,
    ) -> Result<()> {
        Err(CoreError::NotFound("torrent".into()))
    }

    /// Trigger a watch-folder scan.
    async fn watch_scan(&self) -> Result<()>;
    /// Watch-folder configured status.
    async fn watch_status(&self) -> WatchStatus;
    /// Watch-folder import history.
    async fn watch_history(&self) -> Vec<swarmotter_core::watch::ImportResult>;
}

/// A patch of safe runtime settings.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SettingsPatch {
    pub bandwidth: Option<swarmotter_core::bandwidth::BandwidthLimits>,
    pub queue: Option<swarmotter_core::queue::QueueLimits>,
    pub seeding: Option<swarmotter_core::ratio::SeedingPolicy>,
    pub autopilot: Option<AutopilotConfig>,
}

/// Shared application state.
pub type SharedState = Arc<AppState>;

#[derive(Clone)]
pub struct AppState {
    pub daemon: Arc<dyn DaemonOps>,
    pub config: Arc<Mutex<Config>>,
    pub build: BuildInfo,
    pub broker: crate::handlers::events::EventBroker,
    pub transmission: TransmissionCompatState,
    pub qbittorrent: QbittorrentCompatState,
}

/// Process-local state for the Transmission RPC compatibility adapter.
#[derive(Clone)]
pub struct TransmissionCompatState {
    pub(crate) session_id: Arc<String>,
    pub(crate) ids: Arc<Mutex<TransmissionIdCache>>,
}

impl TransmissionCompatState {
    pub fn new() -> Self {
        Self {
            session_id: Arc::new(generate_session_id()),
            ids: Arc::new(Mutex::new(TransmissionIdCache::default())),
        }
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }
}

impl Default for TransmissionCompatState {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-local state for the qBittorrent Web API compatibility adapter.
#[derive(Clone)]
pub struct QbittorrentCompatState {
    pub(crate) session_id: Arc<String>,
}

impl QbittorrentCompatState {
    pub fn new() -> Self {
        Self {
            session_id: Arc::new(generate_session_id()),
        }
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }
}

impl Default for QbittorrentCompatState {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-local Transmission integer ID mapping over SwarmOtter info hashes.
#[derive(Debug, Default)]
pub struct TransmissionIdCache {
    next_id: i64,
    hash_to_id: BTreeMap<TorrentKey, i64>,
    id_to_hash: BTreeMap<i64, TorrentKey>,
}

impl TransmissionIdCache {
    pub fn id_for(&mut self, hash: TorrentKey) -> i64 {
        if let Some(id) = self.hash_to_id.get(&hash) {
            return *id;
        }
        self.next_id += 1;
        let id = self.next_id;
        self.hash_to_id.insert(hash, id);
        self.id_to_hash.insert(id, hash);
        id
    }

    pub fn hash_for_id(&self, id: i64) -> Option<TorrentKey> {
        self.id_to_hash.get(&id).copied()
    }
}

fn generate_session_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let marker = 0u8;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    now.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    (&marker as *const u8 as usize).hash(&mut hasher);
    format!("swarmotter-{:016x}", hasher.finish())
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
