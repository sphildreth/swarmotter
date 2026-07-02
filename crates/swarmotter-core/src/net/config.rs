// SPDX-License-Identifier: Apache-2.0

//! Network containment configuration.

use crate::error::{CoreError, Result};
use crate::models::network::NetworkContainmentMode;
use serde::{Deserialize, Serialize};

/// Network containment configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
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
    /// Whether IPv6 torrent traffic is allowed (default false to reduce leaks).
    #[serde(default)]
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
}

fn default_true() -> bool {
    true
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            mode: NetworkContainmentMode::Disabled,
            required_interface: None,
            required_source_ipv4: None,
            required_source_ipv6: None,
            required_network_namespace: None,
            allow_ipv6: false,
            fail_closed: true,
            validate_route: false,
            validate_dns: false,
        }
    }
}

impl NetworkConfig {
    /// Validate the network configuration. Returns an error for contradictory
    /// or invalid settings.
    pub fn validate(&self) -> Result<()> {
        if self.mode == NetworkContainmentMode::Strict {
            // Strict mode requires a configured path and an enforceable socket
            // binding strategy.
            let has_path = self.required_interface.is_some()
                || self.required_source_ipv4.is_some()
                || self.required_source_ipv6.is_some()
                || self.required_network_namespace.is_some();
            if !has_path {
                return Err(CoreError::InvalidConfig(
                    "strict network containment requires a configured network path".into(),
                ));
            }
            let has_enforceable_socket_path = self.required_source_ipv4.is_some()
                || self.required_source_ipv6.is_some()
                || self.required_network_namespace.is_some();
            if !has_enforceable_socket_path {
                return Err(CoreError::InvalidConfig(
                    "strict network containment requires a source address or network namespace; interface-only configuration cannot be enforced by socket binding".into(),
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
    fn strict_rejects_interface_only() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
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
}
