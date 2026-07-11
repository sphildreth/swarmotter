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

    fn route_valid(&self, config: &NetworkConfig) -> bool {
        #[cfg(target_os = "linux")]
        {
            linux_route_valid(config, &self.list())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = config;
            true
        }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteFamily {
    Ipv4,
    Ipv6,
}

#[cfg(target_os = "linux")]
impl RouteFamily {
    fn flag(self) -> &'static str {
        match self {
            Self::Ipv4 => "-4",
            Self::Ipv6 => "-6",
        }
    }

    fn destination(self) -> IpAddr {
        match self {
            Self::Ipv4 => IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1)),
            Self::Ipv6 => IpAddr::V6(std::net::Ipv6Addr::new(
                0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111,
            )),
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteProbe {
    family: RouteFamily,
    source: Option<IpAddr>,
    interface: Option<String>,
}

#[cfg(target_os = "linux")]
fn linux_route_valid(config: &NetworkConfig, interfaces: &[InterfaceInfo]) -> bool {
    linux_route_valid_with(config, interfaces, run_route_probe)
}

#[cfg(target_os = "linux")]
fn linux_route_valid_with<F>(
    config: &NetworkConfig,
    interfaces: &[InterfaceInfo],
    mut run_probe: F,
) -> bool
where
    F: FnMut(&RouteProbe) -> Option<String>,
{
    let Some(probes) = route_probes(config, interfaces) else {
        return false;
    };
    probes.iter().all(|probe| {
        run_probe(probe)
            .as_deref()
            .map(|output| route_output_matches_probe(output, probe))
            .unwrap_or(false)
    })
}

#[cfg(target_os = "linux")]
fn route_probes(config: &NetworkConfig, interfaces: &[InterfaceInfo]) -> Option<Vec<RouteProbe>> {
    let source_v4 = match config.required_source_ipv4.as_deref() {
        Some(source) => Some(IpAddr::V4(source.parse().ok()?)),
        None => None,
    };
    let source_v6 = match config.required_source_ipv6.as_deref() {
        Some(_) if !config.allow_ipv6 => return None,
        Some(source) => Some(IpAddr::V6(source.parse().ok()?)),
        None => None,
    };

    let required_interface = match config.required_interface.as_deref() {
        Some(name) => Some(interfaces.iter().find(|interface| interface.name == name)?),
        None => None,
    };

    let mut needs_v4 = source_v4.is_some();
    let mut needs_v6 = source_v6.is_some();
    if let Some(interface) = required_interface {
        add_route_families(
            interface.addresses.iter().copied(),
            config.allow_ipv6,
            &mut needs_v4,
            &mut needs_v6,
        );
    } else if config.required_network_namespace.is_some() {
        add_route_families(
            interfaces
                .iter()
                .filter(|interface| interface.status == InterfaceStatus::Up)
                .flat_map(|interface| interface.addresses.iter().copied()),
            config.allow_ipv6,
            &mut needs_v4,
            &mut needs_v6,
        );
    }

    let has_required_path = config.required_interface.is_some()
        || config.required_source_ipv4.is_some()
        || config.required_source_ipv6.is_some()
        || config.required_network_namespace.is_some();
    if has_required_path && !needs_v4 && !needs_v6 {
        return None;
    }

    let interface = config.required_interface.clone();
    let mut probes = Vec::with_capacity(2);
    if needs_v4 {
        probes.push(RouteProbe {
            family: RouteFamily::Ipv4,
            source: source_v4,
            interface: interface.clone(),
        });
    }
    if needs_v6 {
        probes.push(RouteProbe {
            family: RouteFamily::Ipv6,
            source: source_v6,
            interface,
        });
    }
    Some(probes)
}

#[cfg(target_os = "linux")]
fn add_route_families(
    addresses: impl IntoIterator<Item = IpAddr>,
    allow_ipv6: bool,
    needs_v4: &mut bool,
    needs_v6: &mut bool,
) {
    for address in addresses {
        match address {
            IpAddr::V4(address)
                if !address.is_unspecified()
                    && !address.is_loopback()
                    && !address.is_link_local() =>
            {
                *needs_v4 = true;
            }
            IpAddr::V6(address)
                if allow_ipv6
                    && !address.is_unspecified()
                    && !address.is_loopback()
                    && address.segments()[0] & 0xffc0 != 0xfe80 =>
            {
                *needs_v6 = true;
            }
            _ => {}
        }
    }
}

#[cfg(target_os = "linux")]
fn run_route_probe(probe: &RouteProbe) -> Option<String> {
    let mut command = std::process::Command::new("ip");
    command.env("LC_ALL", "C").args([
        probe.family.flag(),
        "route",
        "get",
        &probe.family.destination().to_string(),
    ]);
    if let Some(source) = probe.source {
        command.args(["from", &source.to_string()]);
    }
    if let Some(interface) = probe.interface.as_deref() {
        command.args(["oif", interface]);
    }

    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

#[cfg(target_os = "linux")]
fn route_output_matches_probe(output: &str, probe: &RouteProbe) -> bool {
    const INVALID_ROUTE_TYPES: [&str; 4] = ["blackhole", "prohibit", "throw", "unreachable"];

    let mut tokens = output.split_whitespace();
    if tokens.next().and_then(|token| token.parse::<IpAddr>().ok())
        != Some(probe.family.destination())
        || output
            .split_whitespace()
            .any(|token| INVALID_ROUTE_TYPES.contains(&token))
    {
        return false;
    }

    let device = route_output_value(output, "dev");
    if device.is_none()
        || probe
            .interface
            .as_deref()
            .is_some_and(|required| device != Some(required))
    {
        return false;
    }

    match probe.source {
        None => true,
        Some(required) => ["from", "src"].into_iter().any(|key| {
            route_output_value(output, key).and_then(|source| source.parse::<IpAddr>().ok())
                == Some(required)
        }),
    }
}

fn route_output_value<'a>(output: &'a str, key: &str) -> Option<&'a str> {
    let mut parts = output.split_whitespace();
    while let Some(part) = parts.next() {
        if part == key {
            return parts.next();
        }
    }
    None
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
    route_output_value(output, "dev") == Some(iface)
}

#[cfg(target_os = "linux")]
fn namespace_is_current(ns: &str) -> bool {
    if !valid_namespace_name(ns) {
        return false;
    }

    use std::path::Path;
    let configured = std::path::Path::new("/var/run/netns").join(ns);
    namespace_paths_match(Path::new("/proc/self/ns/net"), &configured)
}

#[cfg(target_os = "linux")]
fn valid_namespace_name(ns: &str) -> bool {
    use std::path::{Component, Path};

    let mut components = Path::new(ns).components();
    matches!(components.next(), Some(Component::Normal(name)) if name == std::ffi::OsStr::new(ns))
        && components.next().is_none()
}

#[cfg(target_os = "linux")]
fn namespace_paths_match(current: &std::path::Path, configured: &std::path::Path) -> bool {
    use std::os::unix::fs::MetadataExt;

    let Ok(current) = std::fs::metadata(current) else {
        return false;
    };
    let Ok(configured) = std::fs::metadata(configured) else {
        return false;
    };
    current.dev() == configured.dev() && current.ino() == configured.ino()
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

    #[cfg(target_os = "linux")]
    #[test]
    fn route_validation_checks_required_interface_and_source_families() {
        let config = NetworkConfig {
            required_interface: Some("tun0".into()),
            required_source_ipv4: Some("10.8.0.2".into()),
            required_source_ipv6: Some("fd00:0::2".into()),
            allow_ipv6: true,
            ..Default::default()
        };
        let interfaces = vec![InterfaceInfo {
            name: "tun0".into(),
            status: InterfaceStatus::Up,
            addresses: vec!["10.8.0.2".parse().unwrap(), "fd00::2".parse().unwrap()],
        }];
        let mut observed = Vec::new();

        let valid = linux_route_valid_with(&config, &interfaces, |probe| {
            observed.push(probe.clone());
            Some(match probe.family {
                RouteFamily::Ipv4 => {
                    "1.1.1.1 from 10.8.0.2 via 10.8.0.1 dev tun0 uid 1000\n cache\n".into()
                }
                RouteFamily::Ipv6 => {
                    "2606:4700:4700::1111 from fd00::2 dev tun0 metric 1024\n cache\n".into()
                }
            })
        });

        assert!(valid);
        assert_eq!(
            observed,
            vec![
                RouteProbe {
                    family: RouteFamily::Ipv4,
                    source: Some("10.8.0.2".parse().unwrap()),
                    interface: Some("tun0".into()),
                },
                RouteProbe {
                    family: RouteFamily::Ipv6,
                    source: Some("fd00::2".parse().unwrap()),
                    interface: Some("tun0".into()),
                },
            ]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn route_validation_ignores_unroutable_interface_families() {
        let config = NetworkConfig {
            required_interface: Some("tun0".into()),
            allow_ipv6: true,
            ..Default::default()
        };
        let interfaces = vec![InterfaceInfo {
            name: "tun0".into(),
            status: InterfaceStatus::Up,
            addresses: vec!["10.8.0.2".parse().unwrap(), "fe80::2".parse().unwrap()],
        }];
        let mut observed = Vec::new();

        assert!(linux_route_valid_with(&config, &interfaces, |probe| {
            observed.push(probe.family);
            Some("1.1.1.1 via 10.8.0.1 dev tun0 src 10.8.0.2\n".into())
        }));
        assert_eq!(observed, vec![RouteFamily::Ipv4]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn route_validation_fails_closed_on_probe_or_output_errors() {
        let config = NetworkConfig {
            required_interface: Some("tun0".into()),
            required_source_ipv4: Some("10.8.0.2".into()),
            ..Default::default()
        };
        let interfaces = vec![InterfaceInfo {
            name: "tun0".into(),
            status: InterfaceStatus::Up,
            addresses: vec!["10.8.0.2".parse().unwrap()],
        }];

        assert!(!linux_route_valid_with(&config, &interfaces, |_| None));
        assert!(!linux_route_valid_with(&config, &interfaces, |_| Some(
            "1.1.1.1 from 10.8.0.2 dev eth0\n".into()
        )));
        assert!(!linux_route_valid_with(&config, &interfaces, |_| Some(
            "1.1.1.1 dev tun0 src 10.8.0.99\n".into()
        )));
        assert!(!linux_route_valid_with(&config, &interfaces, |_| Some(
            "unreachable 1.1.1.1 dev tun0\n".into()
        )));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn route_validation_rejects_invalid_config_without_running_probe() {
        let config = NetworkConfig {
            required_source_ipv4: Some("not-an-ip".into()),
            ..Default::default()
        };
        let mut called = false;

        assert!(!linux_route_valid_with(&config, &[], |_| {
            called = true;
            Some(String::new())
        }));
        assert!(!called);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn namespace_identity_uses_device_and_inode() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "swarmotter-probe-namespace-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let original = directory.join("original");
        let same = directory.join("same");
        let different = directory.join("different");
        std::fs::write(&original, b"namespace-a").unwrap();
        std::fs::hard_link(&original, &same).unwrap();
        std::fs::write(&different, b"namespace-b").unwrap();

        assert!(namespace_paths_match(&original, &same));
        assert!(!namespace_paths_match(&original, &different));
        assert!(!namespace_paths_match(
            &original,
            &directory.join("missing")
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn namespace_name_must_be_a_single_path_component() {
        assert!(valid_namespace_name("vpn0"));
        assert!(!valid_namespace_name(""));
        assert!(!valid_namespace_name("."));
        assert!(!valid_namespace_name(".."));
        assert!(!valid_namespace_name("nested/vpn0"));
        assert!(!valid_namespace_name("/proc/self/ns/net"));
        assert!(!valid_namespace_name("vpn0/"));
    }
}
