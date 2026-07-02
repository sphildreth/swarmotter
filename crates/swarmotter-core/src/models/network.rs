// SPDX-License-Identifier: Apache-2.0

//! Network containment status models.
//!
//! These match the required health states in
//! `design/vpn-network-containment.md`: `healthy`, `disabled`,
//! `interface_missing`, `interface_down`, `no_interface_address`,
//! `source_address_missing`, `route_invalid`, `socket_bind_failed`,
//! `dns_not_constrained`, `network_namespace_unavailable`, and
//! `blocked_fail_closed`.

use serde::{Deserialize, Serialize};

/// Network containment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum NetworkContainmentMode {
    /// No containment; torrent traffic may use the default route.
    #[default]
    Disabled,
    /// Prefer the configured path but do not block if unavailable.
    Preferred,
    /// All torrent traffic must use the configured path; fail closed otherwise.
    Strict,
}

/// Network containment health state. Each variant maps to a stable snake_case
/// string for the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkContainmentStatus {
    Healthy,
    Disabled,
    InterfaceMissing,
    InterfaceDown,
    NoInterfaceAddress,
    SourceAddressMissing,
    RouteInvalid,
    SocketBindFailed,
    DnsNotConstrained,
    NetworkNamespaceUnavailable,
    BlockedFailClosed,
}

impl NetworkContainmentStatus {
    /// Stable API code string.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Disabled => "disabled",
            Self::InterfaceMissing => "interface_missing",
            Self::InterfaceDown => "interface_down",
            Self::NoInterfaceAddress => "no_interface_address",
            Self::SourceAddressMissing => "source_address_missing",
            Self::RouteInvalid => "route_invalid",
            Self::SocketBindFailed => "socket_bind_failed",
            Self::DnsNotConstrained => "dns_not_constrained",
            Self::NetworkNamespaceUnavailable => "network_namespace_unavailable",
            Self::BlockedFailClosed => "blocked_fail_closed",
        }
    }

    /// True if this state permits torrent data-plane traffic.
    pub fn traffic_allowed(self) -> bool {
        matches!(self, Self::Healthy | Self::Disabled)
    }

    /// True if this state indicates a fail-closed condition in strict mode.
    pub fn is_fail_closed(self) -> bool {
        !self.traffic_allowed()
    }
}

impl std::fmt::Display for NetworkContainmentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Full network health snapshot reported by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkHealth {
    pub mode: NetworkContainmentMode,
    pub status: NetworkContainmentStatus,
    pub required_interface: Option<String>,
    pub required_source_ipv4: Option<String>,
    pub required_source_ipv6: Option<String>,
    pub allow_ipv6: bool,
    pub fail_closed: bool,
    /// Human-readable detail about the current status.
    pub detail: String,
    /// Whether torrent data-plane traffic is currently permitted.
    pub traffic_allowed: bool,
}

impl NetworkHealth {
    pub fn blocked(
        mode: NetworkContainmentMode,
        status: NetworkContainmentStatus,
        detail: impl Into<String>,
    ) -> Self {
        NetworkHealth {
            mode,
            status,
            required_interface: None,
            required_source_ipv4: None,
            required_source_ipv6: None,
            allow_ipv6: false,
            fail_closed: matches!(mode, NetworkContainmentMode::Strict),
            detail: detail.into(),
            traffic_allowed: status.traffic_allowed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_required_states_present() {
        let required = [
            "healthy",
            "disabled",
            "interface_missing",
            "interface_down",
            "no_interface_address",
            "source_address_missing",
            "route_invalid",
            "socket_bind_failed",
            "dns_not_constrained",
            "network_namespace_unavailable",
            "blocked_fail_closed",
        ];
        let statuses = [
            NetworkContainmentStatus::Healthy,
            NetworkContainmentStatus::Disabled,
            NetworkContainmentStatus::InterfaceMissing,
            NetworkContainmentStatus::InterfaceDown,
            NetworkContainmentStatus::NoInterfaceAddress,
            NetworkContainmentStatus::SourceAddressMissing,
            NetworkContainmentStatus::RouteInvalid,
            NetworkContainmentStatus::SocketBindFailed,
            NetworkContainmentStatus::DnsNotConstrained,
            NetworkContainmentStatus::NetworkNamespaceUnavailable,
            NetworkContainmentStatus::BlockedFailClosed,
        ];
        assert_eq!(required.len(), statuses.len());
        for (s, code) in statuses.iter().zip(required.iter()) {
            assert_eq!(s.as_str(), *code);
        }
    }

    #[test]
    fn traffic_allowed_only_healthy_or_disabled() {
        assert!(NetworkContainmentStatus::Healthy.traffic_allowed());
        assert!(NetworkContainmentStatus::Disabled.traffic_allowed());
        assert!(!NetworkContainmentStatus::InterfaceMissing.traffic_allowed());
        assert!(!NetworkContainmentStatus::BlockedFailClosed.traffic_allowed());
    }

    #[test]
    fn serde_snake_case() {
        let s = serde_json::to_string(&NetworkContainmentMode::Strict).unwrap();
        assert_eq!(s, "\"strict\"");
        let st = serde_json::to_string(&NetworkContainmentStatus::SocketBindFailed).unwrap();
        assert_eq!(st, "\"socket_bind_failed\"");
    }
}
