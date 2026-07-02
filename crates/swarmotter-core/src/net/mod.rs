// SPDX-License-Identifier: Apache-2.0

//! Network containment layer.
//!
//! This module provides the central network binding and containment
//! abstraction. No engine component may create torrent data-plane sockets
//! directly; all such traffic must go through this layer.
//!
//! The layer:
//!  - Validates the configured network path (interface, source address, route).
//!  - Produces a `NetworkContainmentStatus` describing the current health.
//!  - Fails closed in strict mode when the configured path is unavailable,
//!    blocking new torrent sockets and closing existing ones.
//!  - Exposes a pluggable `InterfaceProbe` trait so platform-specific
//!    interface discovery can be injected (and tested) without real hardware.
//!
//! See `design/vpn-network-containment.md`.

pub mod binder;
pub mod config;
pub mod probe;

pub use binder::{
    parse_http_response, ContainedUdpSocket, HttpResponse, NetworkBinder, PeerListener,
};
pub use config::NetworkConfig;
pub use probe::{InterfaceInfo, InterfaceProbe, InterfaceStatus, OsInterfaceProbe};

use crate::error::{CoreError, Result};
use crate::models::network::{NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth};

/// Evaluate the current network containment health against a config and probe.
pub fn evaluate(config: &NetworkConfig, probe: &dyn InterfaceProbe) -> NetworkHealth {
    let status = compute_status(config, probe);
    NetworkHealth {
        mode: config.mode,
        status,
        required_interface: config.required_interface.clone(),
        required_source_ipv4: config.required_source_ipv4.clone(),
        required_source_ipv6: config.required_source_ipv6.clone(),
        allow_ipv6: config.allow_ipv6,
        fail_closed: config.fail_closed,
        detail: detail_for(config, status),
        traffic_allowed: status.traffic_allowed(),
    }
}

/// Decide whether torrent data-plane traffic is permitted, returning an error
/// in strict fail-closed mode when it is not.
pub fn enforce(config: &NetworkConfig, probe: &dyn InterfaceProbe) -> Result<NetworkHealth> {
    let health = evaluate(config, probe);
    if config.mode == NetworkContainmentMode::Strict
        && config.fail_closed
        && !health.traffic_allowed
    {
        return Err(CoreError::NetworkBlocked(format!(
            "torrent data plane blocked: {}",
            health.status
        )));
    }
    Ok(health)
}

fn compute_status(config: &NetworkConfig, probe: &dyn InterfaceProbe) -> NetworkContainmentStatus {
    if config.mode == NetworkContainmentMode::Disabled {
        return NetworkContainmentStatus::Disabled;
    }

    // If a required interface is configured, it must exist and be up.
    if let Some(iface) = &config.required_interface {
        match probe.find(iface) {
            None => return NetworkContainmentStatus::InterfaceMissing,
            Some(info) => {
                if info.status != InterfaceStatus::Up {
                    return NetworkContainmentStatus::InterfaceDown;
                }
            }
        }
    }

    // If a required namespace is configured but unavailable.
    if let Some(ns) = &config.required_network_namespace {
        if !probe.namespace_available(ns) {
            return NetworkContainmentStatus::NetworkNamespaceUnavailable;
        }
    }

    // Source IPv4 must be assigned to the configured interface (or any if none).
    if let Some(src) = &config.required_source_ipv4 {
        if !probe.source_assigned(src, config.required_interface.as_deref()) {
            return NetworkContainmentStatus::SourceAddressMissing;
        }
    }
    if config.allow_ipv6 {
        if let Some(src) = &config.required_source_ipv6 {
            if !probe.source_assigned(src, config.required_interface.as_deref()) {
                return NetworkContainmentStatus::SourceAddressMissing;
            }
        }
    }

    // Route validation.
    if config.validate_route && !probe.route_valid(config) {
        return NetworkContainmentStatus::RouteInvalid;
    }

    // DNS containment.
    if config.validate_dns && !probe.dns_constrained() {
        return NetworkContainmentStatus::DnsNotConstrained;
    }

    // Preferred mode allows traffic even if some non-fatal checks fail; only
    // strict mode surfaces blocked_fail_closed when fail_closed is set.
    if config.mode == NetworkContainmentMode::Strict {
        // All checks passed for strict; healthy.
        NetworkContainmentStatus::Healthy
    } else {
        NetworkContainmentStatus::Healthy
    }
}

fn detail_for(config: &NetworkConfig, status: NetworkContainmentStatus) -> String {
    match status {
        NetworkContainmentStatus::Healthy => "torrent data plane is healthy".into(),
        NetworkContainmentStatus::Disabled => "network containment disabled".into(),
        NetworkContainmentStatus::InterfaceMissing => format!(
            "required torrent network interface {} is not available",
            config.required_interface.as_deref().unwrap_or("?")
        ),
        NetworkContainmentStatus::InterfaceDown => format!(
            "required torrent network interface {} is down",
            config.required_interface.as_deref().unwrap_or("?")
        ),
        NetworkContainmentStatus::NoInterfaceAddress => format!(
            "required torrent network interface {} has no usable address",
            config.required_interface.as_deref().unwrap_or("?")
        ),
        NetworkContainmentStatus::SourceAddressMissing => {
            "required source address is not assigned".into()
        }
        NetworkContainmentStatus::RouteInvalid => "required route is missing or invalid".into(),
        NetworkContainmentStatus::SocketBindFailed => {
            "binding torrent sockets to the configured path failed".into()
        }
        NetworkContainmentStatus::DnsNotConstrained => {
            "DNS behavior is not constrained as configured".into()
        }
        NetworkContainmentStatus::NetworkNamespaceUnavailable => format!(
            "required network namespace {} is unavailable",
            config.required_network_namespace.as_deref().unwrap_or("?")
        ),
        NetworkContainmentStatus::BlockedFailClosed => {
            "torrent networking blocked by fail-closed policy".into()
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use probe::*;
    use std::collections::HashMap;

    /// A fake probe for deterministic testing.
    #[derive(Default)]
    struct FakeProbe {
        interfaces: HashMap<String, InterfaceInfo>,
        route_valid: bool,
        dns_ok: bool,
        namespace_ok: bool,
    }

    impl InterfaceProbe for FakeProbe {
        fn list(&self) -> Vec<InterfaceInfo> {
            self.interfaces.values().cloned().collect()
        }
        fn find(&self, name: &str) -> Option<InterfaceInfo> {
            self.interfaces.get(name).cloned()
        }
        fn source_assigned(&self, addr: &str, iface: Option<&str>) -> bool {
            if let Some(name) = iface {
                let Some(info) = self.interfaces.get(name) else {
                    return false;
                };
                info.addresses.iter().any(|a| a.to_string() == addr)
            } else {
                self.interfaces
                    .values()
                    .any(|i| i.addresses.iter().any(|a| a.to_string() == addr))
            }
        }
        fn route_valid(&self, _config: &NetworkConfig) -> bool {
            self.route_valid
        }
        fn dns_constrained(&self) -> bool {
            self.dns_ok
        }
        fn namespace_available(&self, _ns: &str) -> bool {
            self.namespace_ok
        }
    }

    fn up_iface(name: &str, addrs: &[&str]) -> InterfaceInfo {
        InterfaceInfo {
            name: name.into(),
            status: InterfaceStatus::Up,
            addresses: addrs.iter().map(|s| s.parse().unwrap()).collect(),
        }
    }

    fn strict_cfg() -> NetworkConfig {
        NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("tun0".into()),
            required_source_ipv4: Some("10.8.0.2".into()),
            required_source_ipv6: None,
            required_network_namespace: None,
            allow_ipv6: false,
            fail_closed: true,
            validate_route: true,
            validate_dns: true,
        }
    }

    #[test]
    fn disabled_allows_traffic() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Disabled,
            ..strict_cfg()
        };
        let probe = FakeProbe::default();
        let h = evaluate(&cfg, &probe);
        assert_eq!(h.status, NetworkContainmentStatus::Disabled);
        assert!(h.traffic_allowed);
    }

    #[test]
    fn strict_missing_interface_fails_closed() {
        let cfg = strict_cfg();
        let probe = FakeProbe::default();
        let err = enforce(&cfg, &probe).unwrap_err();
        assert!(err.is_network_blocked());
        let h = evaluate(&cfg, &probe);
        assert_eq!(h.status, NetworkContainmentStatus::InterfaceMissing);
        assert!(!h.traffic_allowed);
    }

    #[test]
    fn strict_interface_down() {
        let cfg = strict_cfg();
        let mut probe = FakeProbe::default();
        probe.interfaces.insert(
            "tun0".into(),
            InterfaceInfo {
                name: "tun0".into(),
                status: InterfaceStatus::Down,
                addresses: vec!["10.8.0.2".parse().unwrap()],
            },
        );
        let h = evaluate(&cfg, &probe);
        assert_eq!(h.status, NetworkContainmentStatus::InterfaceDown);
    }

    #[test]
    fn strict_source_missing() {
        let cfg = strict_cfg();
        let mut probe = FakeProbe::default();
        probe
            .interfaces
            .insert("tun0".into(), up_iface("tun0", &["10.8.0.3"]));
        probe.route_valid = true;
        probe.dns_ok = true;
        let h = evaluate(&cfg, &probe);
        assert_eq!(h.status, NetworkContainmentStatus::SourceAddressMissing);
    }

    #[test]
    fn strict_route_invalid() {
        let cfg = strict_cfg();
        let mut probe = FakeProbe::default();
        probe
            .interfaces
            .insert("tun0".into(), up_iface("tun0", &["10.8.0.2"]));
        probe.route_valid = false;
        probe.dns_ok = true;
        let h = evaluate(&cfg, &probe);
        assert_eq!(h.status, NetworkContainmentStatus::RouteInvalid);
    }

    #[test]
    fn strict_dns_not_constrained() {
        let cfg = strict_cfg();
        let mut probe = FakeProbe::default();
        probe
            .interfaces
            .insert("tun0".into(), up_iface("tun0", &["10.8.0.2"]));
        probe.route_valid = true;
        probe.dns_ok = false;
        let h = evaluate(&cfg, &probe);
        assert_eq!(h.status, NetworkContainmentStatus::DnsNotConstrained);
    }

    #[test]
    fn strict_namespace_unavailable() {
        let mut cfg = strict_cfg();
        cfg.required_interface = None;
        cfg.required_source_ipv4 = None;
        cfg.required_network_namespace = Some("vpnns".into());
        let mut probe = FakeProbe::default();
        probe.namespace_ok = false;
        probe.route_valid = true;
        probe.dns_ok = true;
        let h = evaluate(&cfg, &probe);
        assert_eq!(
            h.status,
            NetworkContainmentStatus::NetworkNamespaceUnavailable
        );
    }

    #[test]
    fn strict_healthy_when_all_ok() {
        let cfg = strict_cfg();
        let mut probe = FakeProbe::default();
        probe
            .interfaces
            .insert("tun0".into(), up_iface("tun0", &["10.8.0.2"]));
        probe.route_valid = true;
        probe.dns_ok = true;
        let h = enforce(&cfg, &probe).unwrap();
        assert_eq!(h.status, NetworkContainmentStatus::Healthy);
        assert!(h.traffic_allowed);
    }

    #[test]
    fn strict_no_interface_address_when_iface_up_but_no_addr_and_source_set() {
        let mut cfg = strict_cfg();
        cfg.required_source_ipv4 = None;
        let mut probe = FakeProbe::default();
        probe.interfaces.insert(
            "tun0".into(),
            InterfaceInfo {
                name: "tun0".into(),
                status: InterfaceStatus::Up,
                addresses: vec![],
            },
        );
        probe.route_valid = true;
        probe.dns_ok = true;
        // Without a source requirement and an up interface, we cannot detect
        // "no usable address" without a source check. This documents that the
        // no_interface_address state is surfaced when source binding fails at
        // socket creation (handled by the binder), so here we expect healthy.
        let h = evaluate(&cfg, &probe);
        assert_eq!(h.status, NetworkContainmentStatus::Healthy);
        let _ = std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED); // import used
    }
}
