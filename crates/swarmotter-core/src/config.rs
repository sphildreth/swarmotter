// SPDX-License-Identifier: Apache-2.0

//! Configuration model: TOML file loading, environment variable overrides,
//! validation, and safe defaults.
//!
//! Environment variable overrides use the prefix `SWARMOTTER_` with nested
//! fields separated by double underscores, e.g. `SWARMOTTER_API__BIND_ADDRESS`.
//! Invalid required configuration produces clear startup errors.

use crate::bandwidth::BandwidthLimits;
use crate::error::{CoreError, Result};
use crate::net::NetworkConfig;
use crate::queue::QueueLimits;
use crate::ratio::SeedingPolicy;
use serde::{Deserialize, Serialize};

/// Top-level daemon configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub api: ApiConfig,
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
    #[serde(default)]
    pub dht: DhtConfig,
    #[serde(default)]
    pub pex: PexConfig,
    #[serde(default)]
    pub watch: Vec<WatchFolderConfig>,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct StorageConfig {
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub incomplete_dir: Option<String>,
    /// Whether to preallocate files on disk.
    #[serde(default)]
    pub preallocate: bool,
    /// Use sparse files where supported.
    #[serde(default = "default_true")]
    pub sparse: bool,
}

fn default_true() -> bool {
    true
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            download_dir: None,
            incomplete_dir: None,
            preallocate: false,
            sparse: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Selfish mode: when true, SwarmOtter removes a torrent from the daemon
    /// immediately after its download completes (all pieces verified). The
    /// downloaded files are kept, but SwarmOtter will not seed the torrent
    /// after completion. Default is false (normal completion/seeding).
    #[serde(default)]
    pub selfish: bool,
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
            selfish: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DhtConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub bootstrap_nodes: Vec<String>,
    #[serde(default = "default_dht_port")]
    pub port: u16,
}

fn default_dht_port() -> u16 {
    51413
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bootstrap_nodes: vec![
                "dht.transmissionbt.com:6881".into(),
                "router.bittorrent.com:6881".into(),
            ],
            port: default_dht_port(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct WatchFolderConfig {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartBehavior {
    #[default]
    Start,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Load configuration from a TOML string.
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let cfg: Config = toml::from_str(s)
            .map_err(|e| CoreError::InvalidConfig(format!("TOML parse error: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Load from a TOML file path (sync; daemon reads before async runtime).
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let s = std::fs::read_to_string(path).map_err(|e| {
            CoreError::InvalidConfig(format!("failed to read config {}: {e}", path.display()))
        })?;
        Self::from_toml_str(&s)
    }

    /// Apply environment variable overrides using prefix `SWARMOTTER_`.
    /// Nested fields separated by `__`. Overrides are merged onto the parsed
    /// config via a TOML value tree, then re-deserialized and validated.
    pub fn apply_env_overrides(mut self, env: &[(String, String)]) -> Result<Self> {
        let mut toml_value: toml::Value =
            toml::Value::try_from(&self).map_err(|e| CoreError::InvalidConfig(e.to_string()))?;
        for (key, value) in env {
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
        if self.api.bind_address.is_empty() {
            return Err(CoreError::InvalidConfig(
                "api.bind_address must not be empty".into(),
            ));
        }
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
        for w in &self.watch {
            if w.path.is_empty() {
                return Err(CoreError::InvalidConfig(
                    "watch folder path must not be empty".into(),
                ));
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
    fn default_config_validates() {
        let cfg = Config::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.torrent.listen_port, 51413);
        assert_eq!(cfg.api.bind_address, "127.0.0.1:9091");
        assert!(cfg.network.allow_ipv6);
        assert!(cfg.torrent.allow_ipv6);
        assert!(cfg.torrent.utp_enabled);
        assert_eq!(cfg.logging.level, "info");
        assert!(cfg.logging.file);
        assert!(!cfg.torrent.selfish);
    }

    #[test]
    fn partial_interface_network_config_defaults_to_strict_ipv6_enabled() {
        let toml = r#"
[api]
bind_address = "0.0.0.0:9091"

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

[storage]
download_dir = "/data/downloads"
incomplete_dir = "/data/incomplete"

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
[torrent]
listen_port = 51413
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(!cfg.torrent.selfish);
    }

    #[test]
    fn torrent_selfish_parses_true() {
        let toml = r#"
[torrent]
selfish = true
"#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(cfg.torrent.selfish);
    }

    #[test]
    fn torrent_selfish_env_override() {
        let cfg = Config::default();
        let env = vec![("SWARMOTTER_TORRENT__SELFISH".into(), "true".into())];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert!(cfg.torrent.selfish);
    }

    #[test]
    fn env_overrides_apply() {
        let cfg = Config::default();
        let env = vec![
            ("SWARMOTTER_TORRENT__LISTEN_PORT".into(), "60000".into()),
            (
                "SWARMOTTER_API__BIND_ADDRESS".into(),
                "0.0.0.0:12345".into(),
            ),
        ];
        let cfg = cfg.apply_env_overrides(&env).unwrap();
        assert_eq!(cfg.torrent.listen_port, 60000);
        assert_eq!(cfg.api.bind_address, "0.0.0.0:12345");
    }

    #[test]
    fn auth_requires_token() {
        let toml = r#"
[api]
require_auth = true
"#;
        assert!(Config::from_toml_str(toml).is_err());

        let toml = r#"
[api]
require_auth = true
auth_token = "secret"
"#;
        assert!(Config::from_toml_str(toml).is_ok());
    }

    #[test]
    fn request_body_limit_must_be_positive() {
        let toml = r#"
[api]
max_request_body_bytes = 0
"#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    #[test]
    fn dht_port_must_be_positive() {
        let toml = r#"
[dht]
port = 0
"#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    #[test]
    fn logging_defaults_to_file_enabled() {
        let cfg = Config::from_toml_str(
            r#"
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
}
