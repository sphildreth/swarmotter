// SPDX-License-Identifier: Apache-2.0

//! Platform interface probing abstraction.
//!
//! The `InterfaceProbe` trait isolates platform-specific network interface
//! discovery so the containment logic can be tested deterministically and so
//! real socket creation stays centralized. The default `OsInterfaceProbe`
//! performs OS interface discovery where supported; full platform-specific
//! source/interface binding is implemented in the socket binder.

use std::collections::BTreeMap;
use std::net::IpAddr;

use crate::net::NetworkConfig;

/// Operational status of an interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterfaceStatus {
    Up,
    Down,
    Unknown,
}

/// Discovered network interface info.
#[derive(Debug, Clone)]
pub struct InterfaceInfo {
    pub name: String,
    pub status: InterfaceStatus,
    pub addresses: Vec<IpAddr>,
}

/// Abstraction over host network interface discovery.
pub trait InterfaceProbe {
    /// List all known interfaces.
    fn list(&self) -> Vec<InterfaceInfo>;
    /// Find a named interface.
    fn find(&self, name: &str) -> Option<InterfaceInfo>;
    /// Whether a source address is assigned to an interface (optionally named).
    fn source_assigned(&self, addr: &str, iface: Option<&str>) -> bool;
    /// Whether the configured route is valid.
    fn route_valid(&self, config: &NetworkConfig) -> bool;
    /// Whether DNS resolution is constrained as configured.
    fn dns_constrained(&self, config: &NetworkConfig) -> bool;
    /// Whether a given network namespace is available.
    fn namespace_available(&self, ns: &str) -> bool;
}

/// OS-backed probe for interface status and addresses.
pub struct OsInterfaceProbe;

impl InterfaceProbe for OsInterfaceProbe {
    fn list(&self) -> Vec<InterfaceInfo> {
        os_interfaces()
    }

    fn find(&self, name: &str) -> Option<InterfaceInfo> {
        self.list().into_iter().find(|iface| iface.name == name)
    }

    fn source_assigned(&self, addr: &str, iface: Option<&str>) -> bool {
        let target: IpAddr = match addr.parse() {
            Ok(a) => a,
            Err(_) => return false,
        };
        if let Some(iface) = iface {
            return self
                .find(iface)
                .map(|info| info.addresses.contains(&target))
                .unwrap_or(false);
        }
        std::net::TcpListener::bind(std::net::SocketAddr::new(target, 0)).is_ok()
    }

    fn route_valid(&self, _config: &NetworkConfig) -> bool {
        // Without a required interface/source, route is trivially valid.
        true
    }

    fn dns_constrained(&self, config: &NetworkConfig) -> bool {
        dns_constrained(config)
    }

    fn namespace_available(&self, ns: &str) -> bool {
        namespace_is_current(ns)
    }
}

#[cfg(target_os = "linux")]
fn os_interfaces() -> Vec<InterfaceInfo> {
    use std::ffi::CStr;
    use std::net::{Ipv4Addr, Ipv6Addr};

    let mut addrs: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut addrs) } != 0 {
        return Vec::new();
    }

    let mut interfaces: BTreeMap<String, InterfaceInfo> = BTreeMap::new();
    let mut cur = addrs;
    while !cur.is_null() {
        let ifa = unsafe { &*cur };
        if !ifa.ifa_name.is_null() {
            let name = unsafe { CStr::from_ptr(ifa.ifa_name) }
                .to_string_lossy()
                .into_owned();
            let is_up = (ifa.ifa_flags & libc::IFF_UP as u32) != 0;
            let entry = interfaces
                .entry(name.clone())
                .or_insert_with(|| InterfaceInfo {
                    name,
                    status: if is_up {
                        InterfaceStatus::Up
                    } else {
                        InterfaceStatus::Down
                    },
                    addresses: Vec::new(),
                });
            if is_up {
                entry.status = InterfaceStatus::Up;
            }
            if !ifa.ifa_addr.is_null() {
                let family = unsafe { (*ifa.ifa_addr).sa_family as i32 };
                match family {
                    libc::AF_INET => {
                        let sin = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
                        let octets = sin.sin_addr.s_addr.to_ne_bytes();
                        let ip = IpAddr::V4(Ipv4Addr::from(octets));
                        if !entry.addresses.contains(&ip) {
                            entry.addresses.push(ip);
                        }
                    }
                    libc::AF_INET6 => {
                        let sin6 = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in6) };
                        let ip = IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr));
                        if !entry.addresses.contains(&ip) {
                            entry.addresses.push(ip);
                        }
                    }
                    _ => {}
                }
            }
        }
        cur = unsafe { (*cur).ifa_next };
    }
    unsafe { libc::freeifaddrs(addrs) };

    interfaces.into_values().collect()
}

#[cfg(not(target_os = "linux"))]
fn os_interfaces() -> Vec<InterfaceInfo> {
    Vec::new()
}

#[cfg(target_os = "linux")]
fn dns_constrained(config: &NetworkConfig) -> bool {
    if config.required_network_namespace.is_some() {
        return true;
    }
    let Some(iface) = config.required_interface.as_deref() else {
        return false;
    };

    let resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
    let nameservers = nameservers_from_resolv_conf(&resolv);
    if nameservers.iter().any(IpAddr::is_loopback) {
        return systemd_resolved_interface_has_dns(iface);
    }
    !nameservers.is_empty()
        && nameservers
            .iter()
            .all(|ip| nameserver_route_uses_interface(*ip, iface))
}

#[cfg(not(target_os = "linux"))]
fn dns_constrained(config: &NetworkConfig) -> bool {
    config.required_network_namespace.is_some()
}

#[cfg(target_os = "linux")]
fn systemd_resolved_interface_has_dns(iface: &str) -> bool {
    let Ok(output) = std::process::Command::new("resolvectl")
        .arg("dns")
        .arg(iface)
        .output()
    else {
        return false;
    };
    output.status.success() && output_has_dns_address(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(target_os = "linux")]
fn nameserver_route_uses_interface(ip: IpAddr, iface: &str) -> bool {
    let mut cmd = std::process::Command::new("ip");
    if ip.is_ipv6() {
        cmd.args(["-6", "route", "get"]);
    } else {
        cmd.args(["-4", "route", "get"]);
    }
    let Ok(output) = cmd.arg(ip.to_string()).output() else {
        return false;
    };
    output.status.success()
        && route_output_uses_interface(&String::from_utf8_lossy(&output.stdout), iface)
}

fn nameservers_from_resolv_conf(contents: &str) -> Vec<IpAddr> {
    contents
        .lines()
        .filter_map(|line| {
            let line = line.split_once('#').map(|(head, _)| head).unwrap_or(line);
            let mut parts = line.split_whitespace();
            match (parts.next(), parts.next()) {
                (Some("nameserver"), Some(ip)) => ip.parse().ok(),
                _ => None,
            }
        })
        .collect()
}

fn output_has_dns_address(output: &str) -> bool {
    output
        .split_once(':')
        .map(|(_, servers)| {
            servers
                .split_whitespace()
                .any(|token| token.parse::<IpAddr>().is_ok())
        })
        .unwrap_or(false)
}

fn route_output_uses_interface(output: &str, iface: &str) -> bool {
    let mut parts = output.split_whitespace();
    while let Some(part) = parts.next() {
        if part == "dev" && parts.next() == Some(iface) {
            return true;
        }
    }
    false
}

#[cfg(target_os = "linux")]
fn namespace_is_current(ns: &str) -> bool {
    let configured = std::path::Path::new("/var/run/netns").join(ns);
    if !configured.exists() {
        return false;
    }
    let Ok(current) = std::fs::read_link("/proc/self/ns/net") else {
        return false;
    };
    let Ok(configured) = std::fs::read_link(configured) else {
        return false;
    };
    current == configured
}

#[cfg(not(target_os = "linux"))]
fn namespace_is_current(_ns: &str) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_probe_namespace_lookup_is_safe() {
        let probe = OsInterfaceProbe;
        // Should not panic for a clearly absent namespace.
        assert!(!probe.namespace_available("definitely-not-a-real-ns-xyz"));
    }

    #[test]
    fn source_assigned_bad_addr_false() {
        let probe = OsInterfaceProbe;
        assert!(!probe.source_assigned("not-an-ip", None));
    }

    #[test]
    fn os_probe_list_is_safe() {
        let probe = OsInterfaceProbe;
        let _ = probe.list();
    }

    #[test]
    fn resolv_conf_nameserver_parser_extracts_ips() {
        let ips = nameservers_from_resolv_conf(
            r#"
nameserver 127.0.0.53
nameserver 2605:a601:afdc:2300:184b:24ff:fea3:9d85 # router
search home.arpa
"#,
        );
        assert_eq!(ips.len(), 2);
        assert!(ips[0].is_loopback());
        assert!(ips[1].is_ipv6());
    }

    #[test]
    fn resolved_output_detects_link_dns() {
        assert!(output_has_dns_address(
            "Link 4 (br0): 192.168.1.1 2605:a601:afdc:2300::1"
        ));
        assert!(!output_has_dns_address("Link 4 (br0):"));
    }

    #[test]
    fn route_output_matches_interface_token() {
        assert!(route_output_uses_interface(
            "8.8.8.8 via 192.168.1.1 dev br0 src 192.168.8.36",
            "br0"
        ));
        assert!(!route_output_uses_interface(
            "8.8.8.8 via 192.168.1.1 dev eth0 src 192.168.8.36",
            "br0"
        ));
    }
}
