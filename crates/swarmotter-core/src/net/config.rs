// SPDX-License-Identifier: Apache-2.0

//! Network containment configuration.

use crate::error::{CoreError, Result};
use crate::models::network::NetworkContainmentMode;
use serde::{Deserialize, Serialize};

/// Default TCP port used by a SOCKS5 proxy when no explicit port is supplied.
pub const DEFAULT_SOCKS5_PROXY_PORT: u16 = 1080;

/// Opt-in SOCKS5 configuration for TCP torrent data-plane traffic.
///
/// The proxy is deliberately part of the network configuration rather than a
/// general HTTP-client setting. The daemon connects to the proxy only through
/// its contained binder, and uses SOCKS5 `CONNECT` with remote DNS for target
/// hostnames. SOCKS5 UDP ASSOCIATE is not implemented: enabling this feature
/// blocks UDP torrent operations instead of allowing a direct fallback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Socks5ProxyConfig {
    /// Explicit opt-in. When false, the contained binder connects directly to
    /// its contained TCP destination as before.
    #[serde(default)]
    pub enabled: bool,
    /// Proxy host name or IP literal. Its resolution, when necessary, goes
    /// through the contained binder before a TCP connection is opened.
    #[serde(default)]
    pub host: Option<String>,
    /// SOCKS5 TCP listener port.
    #[serde(default = "default_socks5_proxy_port")]
    pub port: u16,
    /// Optional RFC 1929 user name. It must be set together with `password`.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional RFC 1929 password. API read views redact this field and a
    /// missing value in a full settings replacement preserves the prior
    /// stored credential.
    #[serde(default)]
    pub password: Option<String>,
}

fn default_socks5_proxy_port() -> u16 {
    DEFAULT_SOCKS5_PROXY_PORT
}

impl Default for Socks5ProxyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: None,
            port: default_socks5_proxy_port(),
            username: None,
            password: None,
        }
    }
}

impl Socks5ProxyConfig {
    /// Return a canonical proxy host suitable for contained local resolution.
    /// Domain names are IDNA-normalized by `url`; IP literals are returned
    /// without URL brackets so `NetworkBinder::resolve_host` can accept them.
    pub fn normalized_host(&self) -> Result<String> {
        let raw = self.host.as_deref().ok_or_else(|| {
            CoreError::InvalidConfig("network.socks5.host is required when enabled".into())
        })?;
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(CoreError::InvalidConfig(
                "network.socks5.host must not be empty when set".into(),
            ));
        }
        match url::Host::parse(raw).map_err(|error| {
            CoreError::InvalidConfig(format!("network.socks5.host is invalid: {error}"))
        })? {
            url::Host::Domain(domain) => Ok(domain),
            url::Host::Ipv4(address) => Ok(address.to_string()),
            url::Host::Ipv6(address) => Ok(address.to_string()),
        }
    }

    /// Whether this configuration requires RFC 1929 username/password
    /// negotiation rather than SOCKS5 no-authentication.
    pub fn has_authentication(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }

    /// Validate syntax and SOCKS5 wire bounds without exposing credentials.
    pub fn validate(&self) -> Result<()> {
        if self.port == 0 {
            return Err(CoreError::InvalidConfig(
                "network.socks5.port must be greater than 0".into(),
            ));
        }
        if self.host.is_some() {
            let host = self.normalized_host()?;
            if host.len() > 255 {
                return Err(CoreError::InvalidConfig(
                    "network.socks5.host must be at most 255 bytes".into(),
                ));
            }
        } else if self.enabled {
            return Err(CoreError::InvalidConfig(
                "network.socks5.host is required when network.socks5.enabled is true".into(),
            ));
        }

        match (&self.username, &self.password) {
            (None, None) => {}
            (Some(username), Some(password)) => {
                if username.is_empty() || username.len() > u8::MAX as usize {
                    return Err(CoreError::InvalidConfig(
                        "network.socks5.username must contain 1 to 255 bytes".into(),
                    ));
                }
                if password.is_empty() || password.len() > u8::MAX as usize {
                    return Err(CoreError::InvalidConfig(
                        "network.socks5.password must contain 1 to 255 bytes".into(),
                    ));
                }
            }
            _ => {
                return Err(CoreError::InvalidConfig(
                    "network.socks5.username and network.socks5.password must be set together"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

/// Network containment configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    #[serde(default = "default_network_mode")]
    pub mode: NetworkContainmentMode,
    /// Required interface name (e.g. `tun0`).
    #[serde(default)]
    pub required_interface: Option<String>,
    /// Required source IPv4 address bound to the interface.
    #[serde(default)]
    pub required_source_ipv4: Option<String>,
    /// Required source IPv6 address bound to the interface.
    #[serde(default)]
    pub required_source_ipv6: Option<String>,
    /// Required Linux network namespace name.
    #[serde(default)]
    pub required_network_namespace: Option<String>,
    /// Whether IPv6 torrent traffic is allowed. Dual-stack is enabled by
    /// default for throughput; strict mode still requires an enforceable path.
    #[serde(default = "default_true")]
    pub allow_ipv6: bool,
    /// Fail closed in strict mode when the path is unavailable.
    #[serde(default = "default_true")]
    pub fail_closed: bool,
    /// Validate that a usable route exists through the configured path.
    #[serde(default)]
    pub validate_route: bool,
    /// Validate that DNS is constrained as configured.
    #[serde(default)]
    pub validate_dns: bool,
    /// Optional SOCKS5 TCP proxy for peer TCP, HTTP(S) tracker, and webseed
    /// traffic. The proxy itself remains subject to this network path.
    #[serde(default)]
    pub socks5: Socks5ProxyConfig,
}

fn default_true() -> bool {
    true
}

fn default_network_mode() -> NetworkContainmentMode {
    NetworkContainmentMode::Strict
}

impl Default for NetworkConfig {
    fn default() -> Self {
        // The Default impl matches the Serde default: strict containment.
        // An omitted `[network]` table therefore produces strict mode without a
        // path, which `Config::validate()` rejects with `invalid_config` before
        // the control listener or any background task starts. Disabled
        // containment is available only through the explicit setting
        // `[network] mode = "disabled"`. See ADR-0051.
        Self {
            mode: NetworkContainmentMode::Strict,
            required_interface: None,
            required_source_ipv4: None,
            required_source_ipv6: None,
            required_network_namespace: None,
            allow_ipv6: true,
            fail_closed: true,
            validate_route: false,
            validate_dns: false,
            socks5: Socks5ProxyConfig::default(),
        }
    }
}

impl NetworkConfig {
    /// Validate the network configuration. Returns an error for contradictory
    /// or invalid settings.
    pub fn validate(&self) -> Result<()> {
        self.socks5.validate()?;
        if self.mode == NetworkContainmentMode::Strict {
            // Strict mode requires a configured path. An interface name is
            // enforceable by the daemon binder on supported platforms via
            // device-bound sockets.
            let has_path = self.required_interface.is_some()
                || self.required_source_ipv4.is_some()
                || self.required_source_ipv6.is_some()
                || self.required_network_namespace.is_some();
            if !has_path {
                return Err(CoreError::InvalidConfig(
                    "strict network containment requires a configured network path".into(),
                ));
            }
            if !self.allow_ipv6 && self.required_source_ipv6.is_some() {
                return Err(CoreError::InvalidConfig(
                    "required_source_ipv6 set but allow_ipv6 is false".into(),
                ));
            }
        }
        if let Some(ip) = &self.required_source_ipv4 {
            if ip.parse::<std::net::Ipv4Addr>().is_err() {
                return Err(CoreError::InvalidConfig(format!(
                    "required_source_ipv4 is not a valid IPv4 address: {ip}"
                )));
            }
        }
        if let Some(ip) = &self.required_source_ipv6 {
            if ip.parse::<std::net::Ipv6Addr>().is_err() {
                return Err(CoreError::InvalidConfig(format!(
                    "required_source_ipv6 is not a valid IPv6 address: {ip}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_requires_path() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn strict_with_interface_ok() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            required_source_ipv4: Some("10.8.0.2".into()),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn strict_allows_interface_only() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn strict_mode_defaults_for_partial_network_table() {
        let cfg: NetworkConfig = toml::from_str(r#"required_interface = "br0""#).unwrap();
        assert_eq!(cfg.mode, NetworkContainmentMode::Strict);
        assert!(cfg.allow_ipv6);
        assert_eq!(cfg.required_interface.as_deref(), Some("br0"));
    }

    #[test]
    fn ipv6_source_requires_allow() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            required_source_ipv6: Some("fd00::1".into()),
            allow_ipv6: false,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
        let cfg2 = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            required_source_ipv6: Some("fd00::1".into()),
            allow_ipv6: true,
            ..Default::default()
        };
        assert!(cfg2.validate().is_ok());
    }

    #[test]
    fn invalid_source_ipv4() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            required_source_ipv4: Some("not-an-ip".into()),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn default_mode_is_strict() {
        // ADR-0051: the Default impl matches the Serde default (strict), so an
        // omitted [network] table produces strict mode without a path.
        let cfg = NetworkConfig::default();
        assert_eq!(cfg.mode, NetworkContainmentMode::Strict);
    }

    #[test]
    fn omitted_network_table_validates_strict_and_rejects_without_path() {
        // A config with no [network] table at all uses the strict default and
        // fails validation because there is no path.
        let cfg: crate::config::Config = crate::config::Config::default();
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.code().as_str(), "invalid_config");
        assert!(err.to_string().contains("strict network containment"));
    }

    #[test]
    fn explicit_disabled_mode_validates() {
        let cfg: NetworkConfig = toml::from_str(r#"mode = "disabled""#).unwrap();
        assert_eq!(cfg.mode, NetworkContainmentMode::Disabled);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn partial_network_table_defaults_to_strict() {
        // A partial [network] table with only an interface defaults mode to
        // strict and validates.
        let cfg: NetworkConfig = toml::from_str(r#"required_interface = "br0""#).unwrap();
        assert_eq!(cfg.mode, NetworkContainmentMode::Strict);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn env_override_provides_strict_path_validates() {
        let cfg = crate::config::Config::default();
        let cfg = cfg
            .apply_env_overrides(&[(
                "SWARMOTTER_NETWORK__REQUIRED_INTERFACE".into(),
                "tun0".into(),
            )])
            .unwrap();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.network.required_interface.as_deref(), Some("tun0"));
    }

    #[test]
    fn env_override_explicit_disabled_validates() {
        let cfg = crate::config::Config::default();
        let cfg = cfg
            .apply_env_overrides(&[("SWARMOTTER_NETWORK__MODE".into(), "disabled".into())])
            .unwrap();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.network.mode, NetworkContainmentMode::Disabled);
    }

    #[test]
    fn strict_mode_does_not_auto_change_to_preferred_or_disabled() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            ..Default::default()
        };
        assert_eq!(cfg.mode, NetworkContainmentMode::Strict);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn socks5_is_opt_in_and_normalizes_a_proxy_hostname() {
        let cfg = Socks5ProxyConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.port, DEFAULT_SOCKS5_PROXY_PORT);
        assert!(cfg.validate().is_ok());

        let cfg = Socks5ProxyConfig {
            enabled: true,
            host: Some("Proxy.Example".into()),
            ..Default::default()
        };
        assert_eq!(cfg.normalized_host().unwrap(), "proxy.example");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn socks5_validation_requires_safe_complete_configuration() {
        let mut cfg = Socks5ProxyConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(cfg.validate().unwrap_err().to_string().contains("host"));

        cfg.host = Some("proxy.example".into());
        cfg.username = Some("operator".into());
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("set together"));

        cfg.password = Some(String::new());
        assert!(cfg.validate().unwrap_err().to_string().contains("password"));
    }

    #[test]
    fn socks5_requires_udp_features_to_be_explicitly_disabled() {
        let mut cfg = crate::config::Config::default();
        cfg.network.mode = NetworkContainmentMode::Disabled;
        cfg.network.socks5.enabled = true;
        cfg.network.socks5.host = Some("proxy.example".into());
        let error = cfg.validate().unwrap_err();
        assert!(error.to_string().contains("TCP CONNECT only"));

        cfg.torrent.utp_enabled = false;
        cfg.dht.enabled = false;
        assert!(cfg.validate().is_ok());
    }
}
