// SPDX-License-Identifier: Apache-2.0

//! Opt-in, contained NAT port-mapping configuration and public status.
//!
//! Port mapping is deliberately separate from torrent transport configuration:
//! it changes a router's inbound forwarding state and therefore must be
//! explicitly enabled. The daemon performs every NAT-PMP and UPnP request
//! through the configured [`crate::net::NetworkBinder`]; an unavailable
//! contained path is reported as blocked rather than falling back to a
//! default-route socket.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};

/// Default lease requested from a NAT-PMP or UPnP gateway.
pub const DEFAULT_PORT_MAPPING_LEASE_SECONDS: u32 = 60 * 60;
/// Default lead time before the daemon renews a successful mapping.
pub const DEFAULT_PORT_MAPPING_REFRESH_BEFORE_EXPIRY_SECONDS: u32 = 5 * 60;
/// Bound accidental long-lived mappings while allowing operators to use a
/// normal router-supported lease.
pub const MAX_PORT_MAPPING_LEASE_SECONDS: u32 = 7 * 24 * 60 * 60;

/// Router protocol used for an active mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortMappingProtocol {
    /// NAT Port Mapping Protocol (NAT-PMP), over contained UDP port 5351.
    NatPmp,
    /// UPnP Internet Gateway Device discovery and SOAP mapping actions.
    Upnp,
}

impl PortMappingProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NatPmp => "nat_pmp",
            Self::Upnp => "upnp",
        }
    }
}

/// Current lifecycle state of the configured listener mapping.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortMappingState {
    /// No router traffic is attempted unless the operator enables mapping.
    #[default]
    Disabled,
    /// A contained discovery, create, or renewal attempt is in progress.
    Pending,
    /// A router confirmed a leased forwarding rule for the listen port.
    Active,
    /// No configured router protocol was available or accepted the request.
    Unavailable,
    /// The contained data-plane path denied the operation. No fallback occurs.
    Blocked,
    /// A configured router protocol returned malformed or otherwise unusable
    /// data. This remains informational and never stops torrent operations.
    Error,
}

/// Opt-in configuration for forwarding the TCP peer listen port through a
/// router on the contained interface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortMappingConfig {
    /// Enable router discovery and mapping. The default is disabled, so the
    /// daemon never modifies gateway state without operator consent.
    #[serde(default)]
    pub enabled: bool,
    /// Protocols attempted in deterministic order until one confirms a
    /// mapping. The default tries NAT-PMP first, then UPnP IGD.
    #[serde(default = "default_port_mapping_protocols")]
    pub protocols: Vec<PortMappingProtocol>,
    /// Optional IPv4 NAT-PMP gateway. On Linux, when omitted, the daemon
    /// discovers only the default gateway associated with
    /// `network.required_interface`; it never queries an unrelated route.
    #[serde(default)]
    pub nat_pmp_gateway: Option<String>,
    /// Optional direct UPnP WANIP/WANPPP control URL. When omitted, the
    /// daemon discovers a gateway with contained SSDP multicast traffic.
    /// This is primarily useful for constrained router deployments.
    #[serde(default)]
    pub upnp_service_url: Option<String>,
    /// Requested router lease for one TCP forwarding rule.
    #[serde(default = "default_port_mapping_lease_seconds")]
    pub lease_seconds: u32,
    /// Start renewal this many seconds before the last accepted lease ends.
    #[serde(default = "default_port_mapping_refresh_before_expiry_seconds")]
    pub refresh_before_expiry_seconds: u32,
}

fn default_port_mapping_protocols() -> Vec<PortMappingProtocol> {
    vec![PortMappingProtocol::NatPmp, PortMappingProtocol::Upnp]
}

fn default_port_mapping_lease_seconds() -> u32 {
    DEFAULT_PORT_MAPPING_LEASE_SECONDS
}

fn default_port_mapping_refresh_before_expiry_seconds() -> u32 {
    DEFAULT_PORT_MAPPING_REFRESH_BEFORE_EXPIRY_SECONDS
}

impl Default for PortMappingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            protocols: default_port_mapping_protocols(),
            nat_pmp_gateway: None,
            upnp_service_url: None,
            lease_seconds: default_port_mapping_lease_seconds(),
            refresh_before_expiry_seconds: default_port_mapping_refresh_before_expiry_seconds(),
        }
    }
}

impl PortMappingConfig {
    /// Validate only local syntax and bounded timing values. Cross-field
    /// containment requirements are checked by [`crate::config::Config`].
    pub fn validate(&self) -> Result<()> {
        if self.lease_seconds == 0 || self.lease_seconds > MAX_PORT_MAPPING_LEASE_SECONDS {
            return Err(CoreError::InvalidConfig(format!(
                "port_mapping.lease_seconds must be between 1 and {MAX_PORT_MAPPING_LEASE_SECONDS}"
            )));
        }
        if self.refresh_before_expiry_seconds == 0
            || self.refresh_before_expiry_seconds >= self.lease_seconds
        {
            return Err(CoreError::InvalidConfig(
                "port_mapping.refresh_before_expiry_seconds must be greater than 0 and less than port_mapping.lease_seconds".into(),
            ));
        }
        if self.enabled && self.protocols.is_empty() {
            return Err(CoreError::InvalidConfig(
                "port_mapping.protocols must contain at least one protocol when port_mapping.enabled is true".into(),
            ));
        }
        let mut protocols = HashSet::new();
        for protocol in &self.protocols {
            if !protocols.insert(*protocol) {
                return Err(CoreError::InvalidConfig(format!(
                    "port_mapping.protocols contains duplicate protocol {}",
                    protocol.as_str()
                )));
            }
        }
        if let Some(gateway) = self.nat_pmp_gateway.as_deref() {
            let gateway = gateway.trim();
            let address = gateway.parse::<std::net::Ipv4Addr>().map_err(|error| {
                CoreError::InvalidConfig(format!(
                    "port_mapping.nat_pmp_gateway must be a valid IPv4 address: {error}"
                ))
            })?;
            if address.is_unspecified() || address.is_multicast() || address.is_broadcast() {
                return Err(CoreError::InvalidConfig(
                    "port_mapping.nat_pmp_gateway must be a unicast IPv4 address".into(),
                ));
            }
        }
        if let Some(url) = self.upnp_service_url.as_deref() {
            let url = url.trim();
            if url.is_empty() {
                return Err(CoreError::InvalidConfig(
                    "port_mapping.upnp_service_url must not be empty when set".into(),
                ));
            }
            let parsed = url::Url::parse(url).map_err(|error| {
                CoreError::InvalidConfig(format!(
                    "port_mapping.upnp_service_url is not a valid URL: {error}"
                ))
            })?;
            if parsed.scheme() != "http" || parsed.host_str().is_none() {
                return Err(CoreError::InvalidConfig(
                    "port_mapping.upnp_service_url must be an http URL with a host".into(),
                ));
            }
            if !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.fragment().is_some()
            {
                return Err(CoreError::InvalidConfig(
                    "port_mapping.upnp_service_url must not include credentials or a fragment"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

/// Public, non-sensitive snapshot of the port-mapping lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortMappingStatus {
    /// Whether mapping is opted in for the current configuration.
    pub enabled: bool,
    /// Protocols configured for this mapping attempt, in deterministic order.
    pub protocols: Vec<PortMappingProtocol>,
    pub state: PortMappingState,
    /// The protocol that last established an active lease, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_protocol: Option<PortMappingProtocol>,
    /// Configured local TCP listener port.
    pub listen_port: u16,
    /// Router-confirmed external TCP port, when a mapping is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_port: Option<u16>,
    /// NAT-PMP gateway address or the source address of an UPnP responder,
    /// when available. This is a local network diagnostic, not a public IP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    /// Unix timestamp for the most recent mapping attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempted_at: Option<u64>,
    /// Unix timestamp at which the active router lease expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<u64>,
    /// Bounded operator-facing detail. Mapping failures remain informational.
    pub detail: String,
}

impl PortMappingStatus {
    pub fn disabled(listen_port: u16) -> Self {
        Self {
            enabled: false,
            protocols: Vec::new(),
            state: PortMappingState::Disabled,
            active_protocol: None,
            listen_port,
            external_port: None,
            gateway: None,
            attempted_at: None,
            lease_expires_at: None,
            detail: "automatic router port mapping is disabled".into(),
        }
    }

    pub fn pending(config: &PortMappingConfig, listen_port: u16) -> Self {
        Self {
            enabled: config.enabled,
            protocols: config.protocols.clone(),
            state: PortMappingState::Pending,
            active_protocol: None,
            listen_port,
            external_port: None,
            gateway: None,
            attempted_at: None,
            lease_expires_at: None,
            detail: "awaiting a contained router mapping attempt".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::models::network::NetworkContainmentMode;

    #[test]
    fn mapping_defaults_are_opt_in_and_bounded() {
        let config = PortMappingConfig::default();
        assert!(!config.enabled);
        assert_eq!(
            config.protocols,
            vec![PortMappingProtocol::NatPmp, PortMappingProtocol::Upnp]
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn mapping_validation_rejects_invalid_protocol_and_timing_inputs() {
        let mut config = PortMappingConfig {
            enabled: true,
            protocols: Vec::new(),
            ..PortMappingConfig::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("protocols"));

        config.protocols = vec![PortMappingProtocol::NatPmp, PortMappingProtocol::NatPmp];
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("duplicate"));

        config.protocols = vec![PortMappingProtocol::Upnp];
        config.lease_seconds = 10;
        config.refresh_before_expiry_seconds = 10;
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("refresh_before_expiry"));

        config.lease_seconds = DEFAULT_PORT_MAPPING_LEASE_SECONDS;
        config.refresh_before_expiry_seconds = DEFAULT_PORT_MAPPING_REFRESH_BEFORE_EXPIRY_SECONDS;
        config.nat_pmp_gateway = Some("224.0.0.1".into());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("unicast"));
    }

    #[test]
    fn status_does_not_expose_an_upnp_control_url() {
        let status = PortMappingStatus::pending(&PortMappingConfig::default(), 51413);
        let json = serde_json::to_value(status).unwrap();
        assert!(json.get("upnp_service_url").is_none());
        assert_eq!(json["state"], "pending");
    }

    #[test]
    fn enabled_mapping_requires_a_strict_fail_closed_interface_path() {
        let mut config = Config::default();
        config.network.mode = NetworkContainmentMode::Disabled;
        config.port_mapping.enabled = true;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("port_mapping.enabled requires"));

        config.network.mode = NetworkContainmentMode::Strict;
        config.network.fail_closed = true;
        config.network.required_interface = Some("tun0".into());
        assert!(config.validate().is_ok());
    }
}
