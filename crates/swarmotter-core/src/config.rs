// SPDX-License-Identifier: Apache-2.0

//! Configuration model: TOML file loading, environment variable overrides,
//! validation, and safe defaults.
//!
//! Environment variable overrides use the prefix `SWARMOTTER_` with nested
//! fields separated by double underscores, e.g. `SWARMOTTER_API__BIND_ADDRESS`.
//! Invalid required configuration produces clear startup errors.

use crate::autopilot::AutopilotConfig;
use crate::bandwidth::BandwidthLimits;
use crate::error::{CoreError, Result};
use crate::net::NetworkConfig;
use crate::peer_filter::PeerFilterConfig;
use crate::policy::{validate_profiles, PolicyProfilesConfig};
use crate::port_mapping::PortMappingConfig;
use crate::port_test::PortTestConfig;
use crate::queue::QueueLimits;
use crate::ratio::SeedingPolicy;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

/// Lexically normalize a path into an absolute path without resolving any
/// symlink. Parent components cannot escape the filesystem root.
pub fn lexical_absolute_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(CoreError::from)?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                }
            }
        }
    }
    Ok(normalized)
}

/// Top-level daemon configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub compatibility: CompatibilityConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub torrent: TorrentConfig,
    #[serde(default)]
    pub bandwidth: BandwidthLimits,
    #[serde(default)]
    pub queue: QueueLimits,
    #[serde(default)]
    pub seeding: SeedingPolicy,
    /// Named reusable policies and label assignments. Resolved storage and
    /// initial admission are selected when a torrent is created; queue,
    /// seeding, and per-torrent caps inherit live.
    #[serde(default)]
    pub profiles: PolicyProfilesConfig,
    #[serde(default)]
    pub autopilot: AutopilotConfig,
    #[serde(default)]
    pub dht: DhtConfig,
    #[serde(default)]
    pub pex: PexConfig,
    /// Global peer-admission filtering for abuse mitigation. This is separate
    /// from, and never relaxes, the contained torrent network path.
    #[serde(default)]
    pub peer_filter: PeerFilterConfig,
    /// Opt-in router forwarding for the contained TCP peer listener. Mapping
    /// traffic is never allowed to use an uncontained interface.
    #[serde(default)]
    pub port_mapping: PortMappingConfig,
    /// Opt-in, operator-configured diagnostic for the TCP peer listen port.
    /// Requests always use the contained data-plane binder at runtime.
    #[serde(default)]
    pub port_test: PortTestConfig,
    #[serde(default)]
    pub watch: Vec<WatchFolderConfig>,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CompatibilityConfig {
    #[serde(default)]
    pub transmission: TransmissionCompatibilityConfig,
    #[serde(default)]
    pub qbittorrent: QbittorrentCompatibilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TransmissionCompatibilityConfig {
    /// Enable the Transmission RPC compatibility adapter at `/transmission/rpc`.
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QbittorrentCompatibilityConfig {
    /// Enable the qBittorrent-compatible Web API adapter at `/api/v2`.
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    #[serde(default = "default_api_bind")]
    pub bind_address: String,
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Whether the API requires authentication.
    #[serde(default)]
    pub require_auth: bool,
    /// Maximum accepted request body size for API requests.
    #[serde(default = "default_max_request_body_bytes")]
    pub max_request_body_bytes: usize,
}

fn default_api_bind() -> String {
    "127.0.0.1:9091".to_string()
}

fn default_max_request_body_bytes() -> usize {
    16 * 1024 * 1024
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            bind_address: default_api_bind(),
            auth_token: None,
            require_auth: false,
            max_request_body_bytes: default_max_request_body_bytes(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub incomplete_dir: Option<String>,
    /// Optional directory for durable fast-resume metadata. When unset,
    /// resume files remain beside active payload data for compatibility.
    #[serde(default)]
    pub resume_dir: Option<String>,
    /// Optional directory for the daemon's durable state file when no
    /// explicit `--state-file` or `SWARMOTTER_STATE_FILE` is supplied.
    #[serde(default)]
    pub state_dir: Option<String>,
    /// Optional scratch root used for the daemon's fallback download layout
    /// when no download directory is configured. Atomic state and resume
    /// replacements intentionally continue to use same-directory temporary
    /// files so a cross-filesystem configuration cannot weaken durability.
    #[serde(default)]
    pub temp_dir: Option<String>,
    /// Minimum free bytes to keep available on storage roots after planned
    /// torrent writes. `0` disables byte-reserve enforcement.
    #[serde(default)]
    pub minimum_free_space_bytes: u64,
    /// Minimum percent of the storage root to keep available after planned
    /// torrent writes. `0` disables percent-reserve enforcement.
    #[serde(default)]
    pub minimum_free_space_percent: u8,
    /// Whether to preallocate files on disk.
    #[serde(default)]
    pub preallocate: bool,
    /// Use sparse files where supported.
    #[serde(default = "default_true")]
    pub sparse: bool,
    /// Explicit CoW strategy for newly created payload files. The default
    /// preserves filesystem defaults and never changes inode flags.
    #[serde(default)]
    pub cow_strategy: CowStrategy,
    /// Per-root admission, write-pressure, and recheck controls. A control
    /// applies to its lexical path and descendants; the most specific matching
    /// control wins.
    #[serde(default)]
    pub root_controls: Vec<StorageRootControl>,
}

/// Copy-on-write handling for newly created payload files.
///
/// This is deliberately conservative: opting into `disable_for_new_files`
/// only succeeds on supported Btrfs filesystems before data is written. It
/// never changes an existing file and never silently substitutes a different
/// write strategy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowStrategy {
    /// Preserve the filesystem's default CoW behavior.
    #[default]
    Conservative,
    /// Disable CoW for newly created payload files on supported Linux Btrfs
    /// roots. Unsupported or ineligible files fail explicitly.
    DisableForNewFiles,
}

impl CowStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conservative => "conservative",
            Self::DisableForNewFiles => "disable_for_new_files",
        }
    }
}

/// Disk-scheduling controls for one lexical storage-root boundary.
///
/// A value of `0` leaves the corresponding control unlimited. The daemon
/// applies controls to the active write directory, so an `incomplete_dir`
/// beneath this path shares one budget with all of its descendants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageRootControl {
    /// Lexical storage-root path. Relative paths are resolved from the daemon
    /// working directory during validation.
    pub path: String,
    /// Maximum active torrent engines using this root (0 = unlimited).
    #[serde(default)]
    pub max_active_downloads: usize,
    /// Maximum declared payload bytes across active engines (0 = unlimited).
    #[serde(default)]
    pub max_active_bytes: u64,
    /// Shared sustained payload-write ceiling for this root in bytes/sec
    /// (0 = unlimited).
    #[serde(default)]
    pub max_write_bytes_per_second: u64,
    /// Maximum simultaneous full rechecks using this root (0 = unlimited).
    #[serde(default)]
    pub max_concurrent_rechecks: usize,
}

impl StorageRootControl {
    /// Return the lexical absolute version of this configured root.
    pub fn normalized_path(&self) -> Result<PathBuf> {
        lexical_absolute_path(Path::new(&self.path))
    }
}

impl StorageConfig {
    /// Find the most-specific configured root containing `path`.
    ///
    /// Validation rejects duplicate normalized roots, while nested roots are
    /// intentional and deterministic: the longest lexical boundary wins.
    pub fn root_control_for_path(&self, path: &Path) -> Option<&StorageRootControl> {
        let path = lexical_absolute_path(path).ok()?;
        self.root_controls
            .iter()
            .filter_map(|control| {
                let root = control.normalized_path().ok()?;
                path.starts_with(&root).then_some((root, control))
            })
            .max_by_key(|(root, _)| root.components().count())
            .map(|(_, control)| control)
    }

    /// Return an optional durable state directory as a lexical absolute path.
    pub fn state_dir_path(&self) -> Result<Option<PathBuf>> {
        self.state_dir
            .as_deref()
            .map(|path| lexical_absolute_path(Path::new(path)))
            .transpose()
    }

    /// Return an optional fast-resume directory as a lexical absolute path.
    pub fn resume_dir_path(&self) -> Result<Option<PathBuf>> {
        self.resume_dir
            .as_deref()
            .map(|path| lexical_absolute_path(Path::new(path)))
            .transpose()
    }

    /// Return an optional scratch directory as a lexical absolute path.
    pub fn temp_dir_path(&self) -> Result<Option<PathBuf>> {
        self.temp_dir
            .as_deref()
            .map(|path| lexical_absolute_path(Path::new(path)))
            .transpose()
    }
}

fn default_true() -> bool {
    true
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            download_dir: None,
            incomplete_dir: None,
            resume_dir: None,
            state_dir: None,
            temp_dir: None,
            minimum_free_space_bytes: 0,
            minimum_free_space_percent: 0,
            preallocate: false,
            sparse: true,
            cow_strategy: CowStrategy::Conservative,
            root_controls: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TorrentConfig {
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    #[serde(default = "default_true")]
    pub allow_ipv6: bool,
    /// Whether uTP (BEP 29) peer connections are enabled. When true, the
    /// engine attempts uTP for peer candidates (with TCP fallback per
    /// `utp_prefer_tcp`). When false, only TCP is used. All uTP traffic goes
    /// through the contained UDP socket; fail-closed blocks it.
    #[serde(default = "default_utp_enabled")]
    pub utp_enabled: bool,
    /// When uTP is enabled, whether TCP is preferred (tried first, with uTP
    /// as a fallback if TCP fails). When false, uTP is preferred. Either way
    /// the other transport remains available.
    #[serde(default = "default_true")]
    pub utp_prefer_tcp: bool,
    /// Peer-wire encryption policy for contained TCP and uTP streams.
    /// `preferred` attempts MSE/PE first and falls back to plaintext on the
    /// selected contained transport; `required` refuses plaintext peer-wire
    /// sessions without silently changing transport.
    #[serde(default)]
    pub encryption_mode: PeerEncryptionMode,
    /// Selfish mode: when true, SwarmOtter removes a torrent from the daemon
    /// immediately after its download completes (all pieces verified). The
    /// downloaded files are kept, but SwarmOtter will not seed the torrent
    /// after completion. Default is false (normal completion/seeding).
    #[serde(default)]
    pub selfish: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerEncryptionMode {
    Disabled,
    #[default]
    Preferred,
    Required,
}

impl PeerEncryptionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Preferred => "preferred",
            Self::Required => "required",
        }
    }
}

fn default_utp_enabled() -> bool {
    true
}

fn default_listen_port() -> u16 {
    51413
}

impl Default for TorrentConfig {
    fn default() -> Self {
        Self {
            listen_port: default_listen_port(),
            allow_ipv6: true,
            utp_enabled: default_utp_enabled(),
            utp_prefer_tcp: true,
            encryption_mode: PeerEncryptionMode::default(),
            selfish: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DhtConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_dht_bootstrap_nodes")]
    pub bootstrap_nodes: Vec<String>,
    #[serde(default = "default_dht_port")]
    pub port: u16,
}

fn default_dht_port() -> u16 {
    51413
}

fn default_dht_bootstrap_nodes() -> Vec<String> {
    vec![
        "dht.transmissionbt.com:6881".into(),
        "router.bittorrent.com:6881".into(),
        "router.utorrent.com:6881".into(),
    ]
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bootstrap_nodes: default_dht_bootstrap_nodes(),
            port: default_dht_port(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PexConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub max_peers: usize,
}

impl Default for PexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_peers: 0,
        }
    }
}

/// A watch folder configuration entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchFolderConfig {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    /// Optional named profile assigned to imports from this folder.
    #[serde(default)]
    pub profile: Option<String>,
    /// "start" or "paused".
    #[serde(default)]
    pub start_behavior: StartBehavior,
    #[serde(default)]
    pub archive_dir: Option<String>,
    #[serde(default)]
    pub failure_dir: Option<String>,
    #[serde(default = "default_true")]
    pub delete_after_import: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartBehavior {
    #[default]
    Start,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub json: bool,
    #[serde(default = "default_true")]
    pub file: bool,
    #[serde(default)]
    pub file_path: Option<String>,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            json: false,
            file: true,
            file_path: None,
        }
    }
}

impl Config {
    fn parse_toml_str(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| CoreError::InvalidConfig(format!("TOML parse error: {e}")))
    }

    /// Load configuration from a TOML string.
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let cfg = Self::parse_toml_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Serialize the effective configuration as TOML after validation.
    pub fn to_toml_string(&self) -> Result<String> {
        self.validate()?;
        toml::to_string_pretty(self)
            .map_err(|e| CoreError::InvalidConfig(format!("TOML serialize error: {e}")))
    }

    /// Load from a TOML file path (sync; daemon reads before async runtime).
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let s = std::fs::read_to_string(path).map_err(|e| {
            CoreError::InvalidConfig(format!("failed to read config {}: {e}", path.display()))
        })?;
        Self::from_toml_str(&s)
    }

    /// Load a TOML file, apply environment overrides, then validate the final
    /// effective configuration.
    pub fn from_file_with_env_overrides(
        path: &std::path::Path,
        env: &[(String, String)],
    ) -> Result<Self> {
        let s = std::fs::read_to_string(path).map_err(|e| {
            CoreError::InvalidConfig(format!("failed to read config {}: {e}", path.display()))
        })?;
        Self::parse_toml_str(&s)?.apply_env_overrides(env)
    }

    /// Apply environment variable overrides using prefix `SWARMOTTER_`.
    /// Nested fields separated by `__`. Overrides are merged onto the parsed
    /// config via a TOML value tree, then re-deserialized and validated.
    pub fn apply_env_overrides(mut self, env: &[(String, String)]) -> Result<Self> {
        let mut toml_value: toml::Value =
            toml::Value::try_from(&self).map_err(|e| CoreError::InvalidConfig(e.to_string()))?;
        for (key, value) in env {
            if matches!(key.as_str(), "SWARMOTTER_CONFIG" | "SWARMOTTER_STATE_FILE") {
                continue;
            }
            let Some(rest) = key.strip_prefix("SWARMOTTER_") else {
                continue;
            };
            // Normalize: lowercase, replace "__" with path separator.
            let path: Vec<String> = rest
                .to_ascii_lowercase()
                .split("__")
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if path.is_empty() {
                continue;
            }
            let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
            apply_override(&mut toml_value, &path_refs, value);
        }
        self = toml::from_str(
            &toml::to_string(&toml_value).map_err(|e| CoreError::InvalidConfig(e.to_string()))?,
        )
        .map_err(|e| CoreError::InvalidConfig(format!("env override merge: {e}")))?;
        self.validate()?;
        Ok(self)
    }

    /// Validate the full configuration.
    pub fn validate(&self) -> Result<()> {
        self.network.validate()?;
        self.peer_filter.validate()?;
        self.port_mapping.validate()?;
        self.port_test.validate()?;
        if self.network.socks5.enabled && (self.torrent.utp_enabled || self.dht.enabled) {
            // SOCKS5 UDP ASSOCIATE is deliberately not implemented. Refuse an
            // ambiguous configuration rather than presenting a proxy as if it
            // covered UDP traffic or allowing a contained-direct fallback.
            return Err(CoreError::InvalidConfig(
                "network.socks5 is TCP CONNECT only; set torrent.utp_enabled = false and dht.enabled = false when enabling it (UDP tracker URLs are rejected at runtime)".into(),
            ));
        }
        if self.port_mapping.enabled
            && (self.network.mode != crate::models::network::NetworkContainmentMode::Strict
                || !self.network.fail_closed
                || self.network.required_interface.is_none())
        {
            // NAT-PMP gateway discovery and SSDP multicast must be scoped to
            // one concrete device. Source-only, preferred, and disabled modes
            // cannot prove that a router request avoids the default route.
            return Err(CoreError::InvalidConfig(
                "port_mapping.enabled requires network.mode = \"strict\", network.fail_closed = true, and network.required_interface".into(),
            ));
        }
        if self
            .seeding
            .global_ratio_limit
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(CoreError::InvalidConfig(
                "seeding.global_ratio_limit must be a finite non-negative number or omitted".into(),
            ));
        }
        if self.bandwidth.max_peers > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(CoreError::InvalidConfig(format!(
                "bandwidth.max_peers must be <= {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.bandwidth.max_peers_per_torrent > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(CoreError::InvalidConfig(format!(
                "bandwidth.max_peers_per_torrent must be <= {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.api.bind_address.is_empty() {
            return Err(CoreError::InvalidConfig(
                "api.bind_address must not be empty".into(),
            ));
        }
        self.api
            .bind_address
            .parse::<std::net::SocketAddr>()
            .map_err(|e| CoreError::InvalidConfig(format!("api.bind_address: {e}")))?;
        if self.api.require_auth
            && self
                .api
                .auth_token
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            return Err(CoreError::InvalidConfig(
                "api.auth_token must be set when api.require_auth is true".into(),
            ));
        }
        if self.api.max_request_body_bytes == 0 {
            return Err(CoreError::InvalidConfig(
                "api.max_request_body_bytes must be > 0".into(),
            ));
        }
        if self.logging.level.trim().is_empty() {
            return Err(CoreError::InvalidConfig(
                "logging.level must not be empty".into(),
            ));
        }
        if self
            .logging
            .file_path
            .as_deref()
            .map(|path| path.trim().is_empty())
            .unwrap_or(false)
        {
            return Err(CoreError::InvalidConfig(
                "logging.file_path must not be empty when set".into(),
            ));
        }
        if self.torrent.listen_port == 0 {
            return Err(CoreError::InvalidConfig(
                "torrent.listen_port must be > 0".into(),
            ));
        }
        if self.dht.port == 0 {
            return Err(CoreError::InvalidConfig("dht.port must be > 0".into()));
        }
        if self.storage.minimum_free_space_percent > 100 {
            return Err(CoreError::InvalidConfig(
                "storage.minimum_free_space_percent must be between 0 and 100".into(),
            ));
        }
        for (field, path) in [
            ("storage.resume_dir", self.storage.resume_dir.as_deref()),
            ("storage.state_dir", self.storage.state_dir.as_deref()),
            ("storage.temp_dir", self.storage.temp_dir.as_deref()),
        ] {
            if let Some(path) = path {
                if path.trim().is_empty() {
                    return Err(CoreError::InvalidConfig(format!(
                        "{field} must not be empty when set"
                    )));
                }
                lexical_absolute_path(Path::new(path)).map_err(|error| {
                    CoreError::InvalidConfig(format!("{field} could not be normalized: {error}"))
                })?;
            }
        }
        let mut normalized_storage_roots = BTreeSet::new();
        for control in &self.storage.root_controls {
            if control.path.trim().is_empty() {
                return Err(CoreError::InvalidConfig(
                    "storage.root_controls.path must not be empty".into(),
                ));
            }
            let path = control.normalized_path().map_err(|error| {
                CoreError::InvalidConfig(format!(
                    "storage.root_controls.path could not be normalized: {error}"
                ))
            })?;
            if !normalized_storage_roots.insert(path.clone()) {
                return Err(CoreError::InvalidConfig(format!(
                    "storage.root_controls contains duplicate path {}",
                    path.display()
                )));
            }
        }
        validate_profiles(&self.profiles).map_err(CoreError::InvalidConfig)?;
        for w in &self.watch {
            if w.path.trim().is_empty() {
                return Err(CoreError::InvalidConfig(
                    "watch folder path must not be empty".into(),
                ));
            }
            if let Some(profile) = w.profile.as_deref() {
                if profile.trim().is_empty() {
                    return Err(CoreError::InvalidConfig(
                        "watch folder profile must not be empty when set".into(),
                    ));
                }
                if !self.profiles.profiles.contains_key(profile) {
                    return Err(CoreError::InvalidConfig(format!(
                        "watch folder references unknown profile {profile}"
                    )));
                }
            }
            let root = lexical_absolute_path(Path::new(&w.path)).map_err(|error| {
                CoreError::InvalidConfig(format!(
                    "watch folder path could not be normalized: {error}"
                ))
            })?;
            for (field, configured_destination) in [
                ("archive_dir", w.archive_dir.as_deref()),
                ("failure_dir", w.failure_dir.as_deref()),
            ] {
                let Some(configured_destination) = configured_destination else {
                    continue;
                };
                if configured_destination.trim().is_empty() {
                    return Err(CoreError::InvalidConfig(format!(
                        "watch folder {field} must not be empty when set"
                    )));
                }
                let destination = lexical_absolute_path(Path::new(configured_destination))
                    .map_err(|error| {
                        CoreError::InvalidConfig(format!(
                            "watch folder {field} could not be normalized: {error}"
                        ))
                    })?;
                if destination == root {
                    return Err(CoreError::InvalidConfig(format!(
                        "watch folder {field} must not normalize to its watch root: {}",
                        root.display()
                    )));
                }
            }
        }
        Ok(())
    }
}

fn apply_override(root: &mut toml::Value, path: &[&str], value: &str) {
    if path.is_empty() {
        return;
    }
    let table = match root.as_table_mut() {
        Some(t) => t,
        None => return,
    };
    if path.len() == 1 {
        // Try to parse value as int/bool, else keep as string.
        let parsed: toml::Value = if let Ok(i) = value.parse::<i64>() {
            toml::Value::Integer(i)
        } else if let Ok(b) = value.parse::<bool>() {
            toml::Value::Boolean(b)
        } else {
            toml::Value::String(value.to_string())
        };
        table.insert(path[0].to_string(), parsed);
        return;
    }
    let entry = table
        .entry(path[0].to_string())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
    apply_override(entry, &path[1..], value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_strict_requires_path() {
        let cfg = Config::default();
        // The default is strict containment without a path, so validation
        // fails with invalid_config. See ADR-0051.
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.code().as_str(), "invalid_config");
        assert!(err.to_string().contains("strict network containment"));
        assert_eq!(cfg.torrent.listen_port, 51413);
        assert_eq!(cfg.api.bind_address, "127.0.0.1:9091");
        assert!(cfg.network.allow_ipv6);
        assert!(cfg.torrent.allow_ipv6);
        assert!(cfg.torrent.utp_enabled);
        assert_eq!(cfg.torrent.encryption_mode, PeerEncryptionMode::Preferred);
        assert_eq!(cfg.logging.level, "info");
        assert!(cfg.logging.file);
        assert!(!cfg.torrent.selfish);
        assert!(!cfg.compatibility.transmission.enabled);
        assert!(matches!(
            cfg.autopilot.mode,
            crate::autopilot::AutopilotMode::Act
        ));
    }

    #[test]
    fn default_config_with_disabled_mode_validates() {
        let mut cfg = Config::default();
        cfg.network.mode = crate::models::network::NetworkContainmentMode::Disabled;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn watch_paths_reject_whitespace_and_action_destination_equal_to_root() {
        let root = std::env::current_dir()
            .unwrap()
            .join("config-watch-validation-root");
        let mut cfg = Config::default();
        cfg.network.mode = crate::models::network::NetworkContainmentMode::Disabled;
        cfg.watch = vec![WatchFolderConfig {
            path: root.display().to_string(),
            recursive: true,
            download_dir: None,
            label: None,
            profile: None,
            start_behavior: StartBehavior::Paused,
            archive_dir: Some(root.join("archive").display().to_string()),
            failure_dir: Some(root.join("failure").display().to_string()),
            delete_after_import: false,
        }];
        assert!(cfg.validate().is_ok(), "distinct descendants are valid");

        let root_string = root.display().to_string();
        let equivalent_root = root.join("child").join("..").display().to_string();
        for (path, archive_dir, failure_dir, expected) in [
            (" \t".to_string(), None, None, "watch folder path"),
            (
                root_string.clone(),
                Some(" \t".to_string()),
                None,
                "archive_dir",
            ),
            (
                root_string.clone(),
                None,
                Some(" \t".to_string()),
                "failure_dir",
            ),
            (
                root_string.clone(),
                Some(equivalent_root),
                None,
                "archive_dir",
            ),
            (root_string.clone(), None, Some(root_string), "failure_dir"),
        ] {
            cfg.watch[0].path = path;
            cfg.watch[0].archive_dir = archive_dir;
            cfg.watch[0].failure_dir = failure_dir;
            let error = cfg.validate().unwrap_err();
            assert_eq!(error.code().as_str(), "invalid_config");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn rejects_negative_and_non_finite_global_ratio_limits() {
        for invalid in [-1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let mut cfg = Config::default();
            cfg.network.mode = crate::models::network::NetworkContainmentMode::Disabled;
            cfg.seeding.global_ratio_limit = Some(invalid);
            let error = cfg.validate().unwrap_err();
            assert_eq!(error.code().as_str(), "invalid_config");
            assert!(error.to_string().contains("global_ratio_limit"));
        }
    }

    #[test]
    fn peer_limits_accept_runtime_boundary_and_reject_one_over() {
        let mut cfg = Config::default();
        cfg.network.mode = crate::models::network::NetworkContainmentMode::Disabled;
        cfg.bandwidth.max_peers = tokio::sync::Semaphore::MAX_PERMITS;
        cfg.bandwidth.max_peers_per_torrent = tokio::sync::Semaphore::MAX_PERMITS;
        assert!(cfg.validate().is_ok());

        cfg.bandwidth.max_peers = tokio::sync::Semaphore::MAX_PERMITS + 1;
        assert!(matches!(cfg.validate(), Err(CoreError::InvalidConfig(_))));
        cfg.bandwidth.max_peers = 0;
        cfg.bandwidth.max_peers_per_torrent = tokio::sync::Semaphore::MAX_PERMITS + 1;
        assert!(matches!(cfg.validate(), Err(CoreError::InvalidConfig(_))));
    }

    #[test]
    fn default_config_with_strict_path_validates() {
        let mut cfg = Config::default();
        cfg.network.required_interface = Some("tun0".into());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn autopilot_config_defaults_to_act() {
        let toml = r#"
[network]
mode = "disabled"

[torrent]
listen_port = 51413
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(matches!(
            cfg.autopilot.mode,
            crate::autopilot::AutopilotMode::Act
        ));
    }

    #[test]
    fn autopilot_config_parses_and_env_override() {
        let toml = r#"
[network]
mode = "disabled"

[autopilot]
mode = "observe"
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(matches!(
            cfg.autopilot.mode,
            crate::autopilot::AutopilotMode::Observe
        ));

        let cfg = Config::default();
        let env = vec![
            ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
            ("SWARMOTTER_AUTOPILOT__MODE".into(), "disabled".into()),
        ];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert!(matches!(
            cfg.autopilot.mode,
            crate::autopilot::AutopilotMode::Disabled
        ));
    }

    #[test]
    fn partial_interface_network_config_defaults_to_strict_ipv6_enabled() {
        let toml = r#"
[api]
bind_address = "0.0.0.0:9091"
require_auth = true
auth_token = "test-token"

[storage]
download_dir = "/mnt/incoming/swarmotter/downloads"
incomplete_dir = "/mnt/incoming/swarmotter/incomplete"

[network]
required_interface = "br0"

[torrent]
listen_port = 51413
selfish = true
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(
            cfg.network.mode,
            crate::models::network::NetworkContainmentMode::Strict
        );
        assert_eq!(cfg.network.required_interface.as_deref(), Some("br0"));
        assert!(cfg.network.allow_ipv6);
        assert!(cfg.torrent.allow_ipv6);
        assert!(cfg.torrent.selfish);
    }

    #[test]
    fn partial_runtime_limit_tables_use_defaults() {
        let toml = r#"
[network]
mode = "disabled"

[bandwidth]
global_download = 1024

[queue]
max_active_downloads = 2

[seeding]
global_ratio_limit = 1.5
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(cfg.bandwidth.global_download, 1024);
        assert_eq!(cfg.bandwidth.global_upload, 0);
        assert_eq!(cfg.queue.max_active_downloads, 2);
        assert_eq!(cfg.queue.max_active_metadata_fetches, 100);
        assert_eq!(cfg.queue.max_active_seeds, 5);
        assert!(cfg.queue.auto_start);
        assert_eq!(cfg.seeding.global_ratio_limit, Some(1.5));
        assert_eq!(cfg.seeding.global_idle_limit, Some(1800));
    }

    #[test]
    fn parses_example_toml() {
        let toml = r#"
[api]
bind_address = "0.0.0.0:9091"
require_auth = true
auth_token = "test-token"

[storage]
download_dir = "/data/downloads"
incomplete_dir = "/data/incomplete"
minimum_free_space_bytes = 1048576
minimum_free_space_percent = 5

[network]
mode = "strict"
required_interface = "tun0"
required_source_ipv4 = "10.8.0.2"
allow_ipv6 = false
fail_closed = true
validate_route = true
validate_dns = true

[torrent]
listen_port = 51413
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(cfg.api.bind_address, "0.0.0.0:9091");
        assert_eq!(cfg.network.required_interface.as_deref(), Some("tun0"));
        assert!(cfg.storage.download_dir.as_deref() == Some("/data/downloads"));
        assert_eq!(cfg.storage.minimum_free_space_bytes, 1_048_576);
        assert_eq!(cfg.storage.minimum_free_space_percent, 5);
    }

    #[test]
    fn storage_free_space_percent_validates_range() {
        let err = Config::from_toml_str(
            r#"
[network]
mode = "disabled"

[storage]
minimum_free_space_percent = 101
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("minimum_free_space_percent"));
    }

    #[test]
    fn storage_reserve_env_overrides_apply() {
        let cfg = Config::default()
            .apply_env_overrides(&[
                ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
                (
                    "SWARMOTTER_STORAGE__MINIMUM_FREE_SPACE_BYTES".into(),
                    "4096".into(),
                ),
                (
                    "SWARMOTTER_STORAGE__MINIMUM_FREE_SPACE_PERCENT".into(),
                    "7".into(),
                ),
            ])
            .unwrap();
        assert_eq!(cfg.storage.minimum_free_space_bytes, 4096);
        assert_eq!(cfg.storage.minimum_free_space_percent, 7);
    }

    #[test]
    fn storage_state_placement_and_cow_strategy_parse_with_safe_defaults() {
        let cfg = Config::from_toml_str(
            r#"
[network]
mode = "disabled"

[storage]
resume_dir = "runtime/resume"
state_dir = "runtime/state"
temp_dir = "runtime/scratch"
cow_strategy = "disable_for_new_files"
"#,
        )
        .unwrap();
        assert_eq!(
            cfg.storage.resume_dir_path().unwrap().unwrap(),
            lexical_absolute_path(Path::new("runtime/resume")).unwrap()
        );
        assert_eq!(
            cfg.storage.state_dir_path().unwrap().unwrap(),
            lexical_absolute_path(Path::new("runtime/state")).unwrap()
        );
        assert_eq!(cfg.storage.cow_strategy, CowStrategy::DisableForNewFiles);

        let defaults = StorageConfig::default();
        assert_eq!(defaults.cow_strategy, CowStrategy::Conservative);
        assert!(defaults.resume_dir.is_none());
        assert!(defaults.state_dir.is_none());
        assert!(defaults.temp_dir.is_none());
    }

    #[test]
    fn storage_state_placement_rejects_blank_paths() {
        for field in ["resume_dir", "state_dir", "temp_dir"] {
            let error = Config::from_toml_str(&format!(
                "[network]\nmode = \"disabled\"\n\n[storage]\n{field} = \"  \"\n"
            ))
            .unwrap_err();
            assert_eq!(error.code().as_str(), "invalid_config");
            assert!(error.to_string().contains(field), "{error}");
        }
    }

    #[test]
    fn storage_root_controls_use_most_specific_lexical_root() {
        let cfg = Config::from_toml_str(
            r#"
[network]
mode = "disabled"

[[storage.root_controls]]
path = "/srv/torrents"
max_active_downloads = 2
max_active_bytes = 100

[[storage.root_controls]]
path = "/srv/torrents/ssd"
max_active_downloads = 1
max_write_bytes_per_second = 1024
"#,
        )
        .unwrap();

        let broad = cfg
            .storage
            .root_control_for_path(Path::new("/srv/torrents/hdd/incomplete"))
            .unwrap();
        assert_eq!(broad.max_active_downloads, 2);
        let nested = cfg
            .storage
            .root_control_for_path(Path::new("/srv/torrents/ssd/incomplete"))
            .unwrap();
        assert_eq!(nested.max_active_downloads, 1);
        assert_eq!(nested.max_write_bytes_per_second, 1024);
        assert!(cfg
            .storage
            .root_control_for_path(Path::new("/var/lib/swarmotter"))
            .is_none());
    }

    #[test]
    fn storage_root_controls_reject_empty_and_duplicate_paths() {
        for controls in [
            r#"
[[storage.root_controls]]
path = "  "
"#,
            r#"
[[storage.root_controls]]
path = "/srv/torrents"

[[storage.root_controls]]
path = "/srv/torrents/child/.."
"#,
        ] {
            let error =
                Config::from_toml_str(&format!("[network]\nmode = \"disabled\"\n\n{controls}"))
                    .unwrap_err();
            assert_eq!(error.code().as_str(), "invalid_config");
            assert!(error.to_string().contains("storage.root_controls"));
        }
    }

    #[test]
    fn parses_container_example_toml() {
        let toml = include_str!("../../../config/swarmotter.container.toml.example");
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(cfg.api.bind_address, "0.0.0.0:9091");
        assert!(cfg.api.require_auth);
        assert_eq!(
            cfg.network.mode,
            crate::models::network::NetworkContainmentMode::Disabled
        );
        assert_eq!(
            cfg.logging.file_path.as_deref(),
            Some("/var/lib/swarmotter/swarmotterd.log")
        );
    }

    #[test]
    fn compatibility_parses_and_env_overrides() {
        let toml = r#"
[network]
mode = "disabled"

[compatibility.transmission]
enabled = true

[compatibility.qbittorrent]
enabled = true
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(cfg.compatibility.transmission.enabled);
        assert!(cfg.compatibility.qbittorrent.enabled);

        let cfg = Config::default();
        let env = vec![
            ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
            (
                "SWARMOTTER_COMPATIBILITY__TRANSMISSION__ENABLED".into(),
                "true".into(),
            ),
            (
                "SWARMOTTER_COMPATIBILITY__QBITTORRENT__ENABLED".into(),
                "true".into(),
            ),
        ];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert!(cfg.compatibility.transmission.enabled);
        assert!(cfg.compatibility.qbittorrent.enabled);
    }

    #[test]
    fn rejects_invalid_listen_port() {
        let toml = r#"
[torrent]
listen_port = 0
"#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    #[test]
    fn torrent_selfish_defaults_false() {
        let toml = r#"
[network]
mode = "disabled"

[torrent]
listen_port = 51413
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(!cfg.torrent.selfish);
    }

    #[test]
    fn torrent_selfish_parses_true() {
        let toml = r#"
[network]
mode = "disabled"

[torrent]
selfish = true
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(cfg.torrent.selfish);
    }

    #[test]
    fn torrent_selfish_env_override() {
        let cfg = Config::default();
        let env = vec![
            ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
            ("SWARMOTTER_TORRENT__SELFISH".into(), "true".into()),
        ];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert!(cfg.torrent.selfish);
    }

    #[test]
    fn torrent_encryption_mode_parses_and_env_override() {
        let toml = r#"
[network]
mode = "disabled"

[torrent]
encryption_mode = "required"
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(cfg.torrent.encryption_mode, PeerEncryptionMode::Required);

        let cfg = Config::default();
        let env = vec![
            ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
            (
                "SWARMOTTER_TORRENT__ENCRYPTION_MODE".into(),
                "disabled".into(),
            ),
        ];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert_eq!(cfg.torrent.encryption_mode, PeerEncryptionMode::Disabled);
    }

    #[test]
    fn profile_encryption_mode_parses_as_an_optional_override() {
        let toml = r#"
[network]
mode = "disabled"

[torrent]
encryption_mode = "disabled"

[profiles.profiles.secure]
encryption_mode = "required"
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(
            cfg.profiles.profiles["secure"].encryption_mode,
            Some(PeerEncryptionMode::Required)
        );
    }

    #[test]
    fn env_overrides_apply() {
        let cfg = Config::default();
        let env = vec![
            ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
            ("SWARMOTTER_TORRENT__LISTEN_PORT".into(), "60000".into()),
            (
                "SWARMOTTER_API__BIND_ADDRESS".into(),
                "0.0.0.0:12345".into(),
            ),
            ("SWARMOTTER_API__REQUIRE_AUTH".into(), "true".into()),
            ("SWARMOTTER_API__AUTH_TOKEN".into(), "test-token".into()),
        ];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert_eq!(cfg.torrent.listen_port, 60000);
        assert_eq!(cfg.api.bind_address, "0.0.0.0:12345");
    }

    #[test]
    fn unauthenticated_api_can_use_a_non_loopback_bind() {
        let cfg = Config::from_toml_str(
            r#"
[network]
mode = "disabled"

[api]
bind_address = "0.0.0.0:9091"
require_auth = false
"#,
        )
        .unwrap();
        assert!(!cfg.api.require_auth);
        assert_eq!(cfg.api.bind_address, "0.0.0.0:9091");

        let cfg = Config::from_toml_str(
            r#"
[network]
mode = "disabled"

[api]
bind_address = "[::1]:9091"
"#,
        )
        .unwrap();
        assert!(!cfg.api.require_auth);
    }

    #[test]
    fn environment_overrides_are_applied_before_final_validation() {
        let cfg = Config::parse_toml_str(
            r#"
[network]
mode = "disabled"

[api]
require_auth = true
"#,
        )
        .unwrap()
        .apply_env_overrides(&[
            ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
            (
                "SWARMOTTER_API__AUTH_TOKEN".into(),
                "environment-token".into(),
            ),
        ])
        .unwrap();

        assert!(cfg.api.require_auth);
        assert_eq!(cfg.api.auth_token.as_deref(), Some("environment-token"));
    }

    #[test]
    fn command_environment_is_not_treated_as_config_fields() {
        let cfg = Config::default()
            .apply_env_overrides(&[
                ("SWARMOTTER_NETWORK__MODE".into(), "disabled".into()),
                ("SWARMOTTER_CONFIG".into(), "/tmp/swarmotter.toml".into()),
                ("SWARMOTTER_STATE_FILE".into(), "/tmp/state.json".into()),
            ])
            .unwrap();

        assert_eq!(cfg.api.bind_address, "127.0.0.1:9091");
    }

    #[test]
    fn auth_requires_token() {
        let toml = r#"
[network]
mode = "disabled"

[api]
require_auth = true
"#;
        assert!(Config::from_toml_str(toml).is_err());

        let toml = r#"
[network]
mode = "disabled"

[api]
require_auth = true
auth_token = "secret"
"#;
        assert!(Config::from_toml_str(toml).is_ok());
    }

    #[test]
    fn request_body_limit_must_be_positive() {
        let toml = r#"
[network]
mode = "disabled"

[api]
max_request_body_bytes = 0
"#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    #[test]
    fn dht_port_must_be_positive() {
        let toml = r#"
[network]
mode = "disabled"

[dht]
port = 0
"#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    #[test]
    fn dht_partial_config_uses_default_bootstrap_nodes() {
        let cfg = Config::from_toml_str(
            r#"
[network]
mode = "disabled"

[dht]
port = 55145
"#,
        )
        .unwrap();
        assert_eq!(cfg.dht.bootstrap_nodes, default_dht_bootstrap_nodes());
    }

    #[test]
    fn logging_defaults_to_file_enabled() {
        let cfg = Config::from_toml_str(
            r#"
[network]
mode = "disabled"

[logging]
json = true
"#,
        )
        .unwrap();
        assert_eq!(cfg.logging.level, "info");
        assert!(cfg.logging.json);
        assert!(cfg.logging.file);
        assert!(cfg.logging.file_path.is_none());
    }

    #[test]
    fn env_override_strict_network() {
        let toml = r#"
[network]
mode = "disabled"
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        let env = vec![
            ("SWARMOTTER_NETWORK__MODE".into(), "strict".into()),
            (
                "SWARMOTTER_NETWORK__REQUIRED_INTERFACE".into(),
                "tun0".into(),
            ),
            (
                "SWARMOTTER_NETWORK__REQUIRED_SOURCE_IPV4".into(),
                "10.8.0.2".into(),
            ),
        ];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert_eq!(cfg.network.required_interface.as_deref(), Some("tun0"));
        assert_eq!(
            cfg.network.required_source_ipv4.as_deref(),
            Some("10.8.0.2")
        );
    }

    #[test]
    fn rejects_unknown_top_level_and_nested_fields() {
        assert!(Config::from_toml_str("bandwith = {}\n").is_err());
        assert!(Config::from_toml_str("[network]\nvalidate_routes = true\n").is_err());
    }
}
