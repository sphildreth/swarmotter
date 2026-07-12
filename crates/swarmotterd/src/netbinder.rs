// SPDX-License-Identifier: Apache-2.0

//! Real network binder implementation for the daemon.
//!
//! `ContainedBinder` opens torrent sockets bound to the configured source
//! address/interface and enforces fail-closed containment: in strict mode it
//! re-evaluates the network path before each connection and refuses to
//! create torrent traffic when the path is unavailable. All torrent
//! data-plane traffic (peers, trackers, DHT, webseeds) goes through here.
//!
//! See `design/vpn-network-containment.md` and ADR-0014 (network containment
//! integration).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use async_trait::async_trait;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::time::timeout;

use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::models::network::{NetworkContainmentMode, NetworkContainmentStatus};
use swarmotter_core::net::{
    self, ContainedUdpSocket, InterfaceProbe, NetworkBinder, NetworkConfig, PeerListener,
};

use crate::containment_gate::ContainmentGate;
use crate::daemon::HealthReport;

const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A real `NetworkBinder` that binds torrent sockets to the configured
/// source address and fails closed in strict mode.
pub struct ContainedBinder {
    config: Arc<RwLock<NetworkConfig>>,
    probe: Arc<dyn InterfaceProbe + Send + Sync>,
    /// Optional process-wide containment gate. When present, `guard()` and
    /// `traffic_allowed()` consult the gate in addition to the config/probe
    /// evaluation so a live block immediately denies new operations. See
    /// ADR-0051.
    gate: Option<Arc<ContainmentGate>>,
    /// Optional runtime health-report channel for bind/listen/source-bind
    /// failures. A failure sends a report that blocks the gate and exposes
    /// `socket_bind_failed`.
    health_report_tx: Option<tokio::sync::mpsc::UnboundedSender<HealthReport>>,
}

impl ContainedBinder {
    pub fn new(config: NetworkConfig, probe: Arc<dyn InterfaceProbe + Send + Sync>) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            probe,
            gate: None,
            health_report_tx: None,
        }
    }

    /// Attach the process-wide containment gate and health-report channel so
    /// the binder observes live blocks and reports bind failures. See
    /// ADR-0051.
    pub fn with_gate_and_health(
        mut self,
        gate: Arc<ContainmentGate>,
        health_report_tx: tokio::sync::mpsc::UnboundedSender<HealthReport>,
    ) -> Self {
        self.gate = Some(gate);
        self.health_report_tx = Some(health_report_tx);
        self
    }

    /// Report a bind/listen/source-bind failure through the runtime health
    /// channel so the gate blocks and `socket_bind_failed` is exposed.
    fn report_bind_failure(&self, detail: impl Into<String>) {
        self.report_health_status(NetworkContainmentStatus::SocketBindFailed, detail);
    }

    fn report_health_status(&self, status: NetworkContainmentStatus, detail: impl Into<String>) {
        let detail = detail.into();
        let fail_closed = self
            .config
            .read()
            .map(|config| config.mode == NetworkContainmentMode::Strict && config.fail_closed)
            .unwrap_or(true);
        if !fail_closed {
            // Disabled/preferred modes do not claim fail-closed containment.
            // An unavailable inbound port is an operation error there, not a
            // reason to cancel unrelated outbound traffic process-wide.
            return;
        }
        // A failed bind is itself proof that the required path cannot be
        // enforced. Block synchronously before reporting to the runtime so no
        // operation can slip through before the periodic health tick drains
        // the channel and performs centralized teardown/persistence.
        if let Some(gate) = &self.gate {
            gate.block(status, detail.clone());
        }
        if let Some(tx) = &self.health_report_tx {
            let _ = tx.send(HealthReport { status, detail });
        }
    }

    fn config_snapshot(&self) -> Result<NetworkConfig> {
        self.config
            .read()
            .map(|cfg| cfg.clone())
            .map_err(|_| CoreError::Internal("network binder config lock poisoned".into()))
    }

    async fn guard(&self) -> Result<()> {
        // The live gate is the authoritative check; if it is blocked, deny
        // immediately without touching sockets. See ADR-0051.
        if let Some(gate) = &self.gate {
            gate.enforce()?;
        }
        let cfg = self.config_snapshot()?;
        if cfg.mode == NetworkContainmentMode::Disabled {
            return Ok(());
        }
        if cfg.mode == NetworkContainmentMode::Strict
            && cfg.fail_closed
            && cfg.required_interface.is_none()
            && cfg.required_source_ipv4.is_none()
            && cfg.required_source_ipv6.is_none()
            && cfg.required_network_namespace.is_none()
        {
            let detail = "torrent data plane blocked: strict containment requires an interface, source binding, or a current network namespace";
            self.report_health_status(NetworkContainmentStatus::BlockedFailClosed, detail);
            return Err(CoreError::NetworkBlocked(detail.into()));
        }
        let health = net::evaluate(&cfg, self.probe.as_ref());
        if cfg.fail_closed && !health.traffic_allowed {
            return Err(CoreError::NetworkBlocked(format!(
                "torrent data plane blocked: {}",
                health.status
            )));
        }
        Ok(())
    }

    /// Update the network configuration at runtime (e.g. when the daemon
    /// reconfigures containment). Existing in-flight connections are not
    /// affected; new connections honor the updated config.
    #[allow(dead_code)]
    pub async fn update_config(&self, config: NetworkConfig) {
        if let Ok(mut cfg) = self.config.write() {
            *cfg = config;
        }
    }
}

#[async_trait]
impl NetworkBinder for ContainedBinder {
    async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        self.guard().await?;
        let cfg = self.config_snapshot()?;
        if let Err(error) = ensure_family_enforced(&cfg, addr) {
            if error.is_network_blocked() {
                self.report_health_status(
                    NetworkContainmentStatus::BlockedFailClosed,
                    format!("peer address {addr} denied by containment policy: {error}"),
                );
            }
            return Err(error);
        }
        let socket = tokio::net::TcpSocket::new_v4_or_v6_for(addr)?;
        if let Err(error) = bind_socket_to_interface(&socket, cfg.required_interface.as_deref()) {
            self.report_bind_failure(format!(
                "bind outbound peer socket for {addr} to configured interface: {error}"
            ));
            return Err(error);
        }
        let source = source_for_addr(&cfg, addr);
        if let Some(ip) = source {
            let bind = SocketAddr::new(ip, 0);
            if let Err(error) = socket.bind(bind) {
                self.report_bind_failure(format!(
                    "bind outbound peer socket to source {bind} for {addr}: {error}"
                ));
                return Err(CoreError::from(error));
            }
        }
        let stream = timeout(TCP_CONNECT_TIMEOUT, socket.connect(addr))
            .await
            .map_err(|_| CoreError::Internal(format!("tcp connect to {addr} timed out")))?
            .map_err(CoreError::from)?;
        stream.set_nodelay(true).ok();
        Ok(stream)
    }

    async fn resolve_host(&self, host: &str, port: u16) -> Result<SocketAddr> {
        self.guard().await?;
        if let Ok(ip) = host.parse() {
            return Ok(SocketAddr::new(ip, port));
        }
        let cfg = self.config_snapshot()?;
        if cfg.mode == NetworkContainmentMode::Strict
            && cfg.fail_closed
            && cfg.required_network_namespace.is_none()
            && !self.probe.dns_constrained(&cfg)
        {
            return Err(CoreError::NetworkBlocked(
                "torrent data plane blocked: hostname resolution requires DNS constrained to the configured network path or a current network namespace".into(),
            ));
        }
        let addrs: Vec<SocketAddr> = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))?
            .filter(|addr| cfg.allow_ipv6 || addr.is_ipv4())
            .collect();
        select_resolved_addr(&cfg, host, &addrs)
            .ok_or_else(|| CoreError::Internal(format!("host {host} has no usable address")))
    }

    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        self.udp_socket_for(None).await
    }

    async fn udp_socket_for(
        &self,
        remote: Option<SocketAddr>,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        self.guard().await?;
        let cfg = self.config_snapshot()?;
        if let Some(remote) = remote {
            if let Err(error) = ensure_family_enforced(&cfg, remote) {
                if error.is_network_blocked() {
                    self.report_health_status(
                        NetworkContainmentStatus::BlockedFailClosed,
                        format!("UDP address {remote} denied by containment policy: {error}"),
                    );
                }
                return Err(error);
            }
        }
        let bind = udp_bind_addr(&cfg, remote);
        let socket = match create_udp_socket(bind, cfg.required_interface.as_deref()) {
            Ok(socket) => socket,
            Err(error) => {
                self.report_bind_failure(format!("bind UDP socket on {bind}: {error}"));
                return Err(error);
            }
        };
        Ok(Box::new(ContainedUdpSocketImpl {
            socket,
            gate: self.gate.clone(),
        }))
    }

    async fn udp_socket_on(
        &self,
        remote: Option<SocketAddr>,
        local_port: u16,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        self.guard().await?;
        let cfg = self.config_snapshot()?;
        if let Some(remote) = remote {
            if let Err(error) = ensure_family_enforced(&cfg, remote) {
                if error.is_network_blocked() {
                    self.report_health_status(
                        NetworkContainmentStatus::BlockedFailClosed,
                        format!("UDP address {remote} denied by containment policy: {error}"),
                    );
                }
                return Err(error);
            }
        }
        let mut bind = udp_bind_addr(&cfg, remote);
        bind.set_port(local_port);
        let socket = match create_udp_socket(bind, cfg.required_interface.as_deref()) {
            Ok(socket) => socket,
            Err(error) => {
                self.report_bind_failure(format!("bind UDP socket on {bind}: {error}"));
                return Err(error);
            }
        };
        Ok(Box::new(ContainedUdpSocketImpl {
            socket,
            gate: self.gate.clone(),
        }))
    }

    async fn bind_peer_listener(&self, port: u16) -> Result<Box<dyn PeerListener>> {
        self.guard().await?;
        let cfg = self.config_snapshot()?;
        let iface = cfg.required_interface.as_deref();
        let v4_addr = cfg
            .required_source_ipv4
            .as_deref()
            .and_then(|s| s.parse::<Ipv4Addr>().ok())
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let v6_addr = cfg
            .required_source_ipv6
            .as_deref()
            .and_then(|s| s.parse::<Ipv6Addr>().ok())
            .map(IpAddr::V6)
            .unwrap_or(IpAddr::V6(Ipv6Addr::UNSPECIFIED));

        let namespace_only = cfg.required_network_namespace.is_some()
            && cfg.required_interface.is_none()
            && cfg.required_source_ipv4.is_none()
            && cfg.required_source_ipv6.is_none();
        let has_interface = cfg.required_interface.is_some();
        let v4 = if cfg.required_source_ipv6.is_some()
            && cfg.required_source_ipv4.is_none()
            && !has_interface
            && !namespace_only
        {
            None
        } else {
            match create_tcp_listener(SocketAddr::new(v4_addr, port), iface) {
                Ok(listener) => Some(listener),
                Err(error) => {
                    self.report_bind_failure(format!(
                        "bind peer listener v4 on port {port}: {error}"
                    ));
                    return Err(error);
                }
            }
        };
        let v6 = if cfg.allow_ipv6
            && (has_interface || namespace_only || cfg.required_source_ipv6.is_some())
        {
            match create_tcp_listener(SocketAddr::new(v6_addr, port), iface) {
                Ok(listener) => Some(listener),
                Err(error) => {
                    self.report_bind_failure(format!(
                        "bind peer listener v6 on port {port}: {error}"
                    ));
                    return Err(error);
                }
            }
        } else {
            None
        };
        Ok(Box::new(ContainedPeerListener {
            v4,
            v6,
            gate: self.gate.clone(),
        }))
    }

    fn traffic_allowed(&self) -> bool {
        // The live gate is authoritative when present. See ADR-0051.
        if let Some(gate) = &self.gate {
            if !gate.traffic_allowed() {
                return false;
            }
        }
        let Ok(cfg) = self.config.read() else {
            return false;
        };
        if cfg.mode == NetworkContainmentMode::Disabled {
            return true;
        }
        net::evaluate(&cfg, self.probe.as_ref()).traffic_allowed
    }
}

/// Real contained UDP socket backed by `tokio::net::UdpSocket`, source-bound
/// through the binder. Used by UDP trackers and DHT.
struct ContainedUdpSocketImpl {
    socket: tokio::net::UdpSocket,
    gate: Option<Arc<ContainmentGate>>,
}

#[async_trait]
impl ContainedUdpSocket for ContainedUdpSocketImpl {
    async fn send_to(&self, addr: SocketAddr, data: &[u8]) -> Result<()> {
        if let Some(gate) = &self.gate {
            gate.enforce()?;
            let generation = gate.generation();
            tokio::select! {
                biased;
                _ = gate.cancelled_since(generation) => return Err(gate_cancelled_error(gate)),
                result = self.socket.send_to(data, addr) => {
                    result.map_err(CoreError::from)?;
                }
            }
            return Ok(());
        }
        self.socket
            .send_to(data, addr)
            .await
            .map_err(CoreError::from)?;
        Ok(())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<(SocketAddr, usize)> {
        let (n, addr) = if let Some(gate) = &self.gate {
            gate.enforce()?;
            let generation = gate.generation();
            tokio::select! {
                biased;
                _ = gate.cancelled_since(generation) => return Err(gate_cancelled_error(gate)),
                result = self.socket.recv_from(buf) => result.map_err(CoreError::from)?,
            }
        } else {
            self.socket.recv_from(buf).await.map_err(CoreError::from)?
        };
        Ok((addr, n))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr().map_err(CoreError::from)
    }
}

/// Real contained inbound peer listener backed by `tokio::net::TcpListener`,
/// source-bound through the binder. Used for seeding/upload.
struct ContainedPeerListener {
    v4: Option<tokio::net::TcpListener>,
    v6: Option<tokio::net::TcpListener>,
    gate: Option<Arc<ContainmentGate>>,
}

impl ContainedPeerListener {
    async fn accept_inner(&self) -> Result<tokio::net::TcpStream> {
        match (&self.v4, &self.v6) {
            (Some(v4), Some(v6)) => tokio::select! {
                res = v4.accept() => res.map(|(stream, _)| stream),
                res = v6.accept() => res.map(|(stream, _)| stream),
            },
            (Some(v4), None) => v4.accept().await.map(|(stream, _)| stream),
            (None, Some(v6)) => v6.accept().await.map(|(stream, _)| stream),
            (None, None) => {
                return Err(CoreError::NetworkBlocked(
                    "torrent data plane blocked: no peer listener socket was bound".into(),
                ));
            }
        }
        .map_err(CoreError::from)
    }
}

#[async_trait]
impl PeerListener for ContainedPeerListener {
    async fn accept(&self) -> Result<tokio::net::TcpStream> {
        if let Some(gate) = &self.gate {
            gate.enforce()?;
            let generation = gate.generation();
            return tokio::select! {
                biased;
                _ = gate.cancelled_since(generation) => Err(gate_cancelled_error(gate)),
                result = self.accept_inner() => result,
            };
        }
        self.accept_inner().await
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        self.v4
            .as_ref()
            .or(self.v6.as_ref())
            .ok_or_else(|| {
                CoreError::NetworkBlocked(
                    "torrent data plane blocked: no peer listener socket was bound".into(),
                )
            })?
            .local_addr()
            .map_err(CoreError::from)
    }
}

fn gate_cancelled_error(gate: &ContainmentGate) -> CoreError {
    gate.enforce().err().unwrap_or_else(|| {
        CoreError::NetworkBlocked("torrent data plane cancelled by a containment transition".into())
    })
}

trait TcpSocketExt {
    fn new_v4_or_v6_for(addr: SocketAddr) -> Result<tokio::net::TcpSocket>;
}

impl TcpSocketExt for tokio::net::TcpSocket {
    fn new_v4_or_v6_for(addr: SocketAddr) -> Result<tokio::net::TcpSocket> {
        let socket = match addr {
            SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        Ok(socket)
    }
}

fn source_for_addr(cfg: &NetworkConfig, addr: SocketAddr) -> Option<IpAddr> {
    match addr {
        SocketAddr::V4(_) => cfg
            .required_source_ipv4
            .as_deref()
            .and_then(|s| s.parse::<Ipv4Addr>().ok())
            .map(IpAddr::V4),
        SocketAddr::V6(_) => cfg
            .required_source_ipv6
            .as_deref()
            .and_then(|s| s.parse::<Ipv6Addr>().ok())
            .map(IpAddr::V6),
    }
}

fn ensure_family_enforced(cfg: &NetworkConfig, addr: SocketAddr) -> Result<()> {
    if addr.is_ipv6() && !cfg.allow_ipv6 {
        return Err(CoreError::NetworkBlocked(
            "torrent data plane blocked: IPv6 traffic is disabled".into(),
        ));
    }
    if cfg.mode != NetworkContainmentMode::Strict || !cfg.fail_closed {
        return Ok(());
    }
    if cfg.required_interface.is_some() || cfg.required_network_namespace.is_some() {
        return Ok(());
    }
    if source_for_addr(cfg, addr).is_some() {
        return Ok(());
    }
    Err(CoreError::NetworkBlocked(format!(
        "torrent data plane blocked: no {} containment binding is configured",
        if addr.is_ipv6() { "IPv6" } else { "IPv4" }
    )))
}

fn select_resolved_addr(
    cfg: &NetworkConfig,
    host: &str,
    addrs: &[SocketAddr],
) -> Option<SocketAddr> {
    let prefer_ipv6 = cfg.allow_ipv6
        && (cfg.required_source_ipv6.is_some() && cfg.required_source_ipv4.is_none()
            || hostname_prefers_ipv6(host));
    if prefer_ipv6 {
        addrs
            .iter()
            .copied()
            .find(SocketAddr::is_ipv6)
            .or_else(|| addrs.iter().copied().find(SocketAddr::is_ipv4))
    } else {
        addrs
            .iter()
            .copied()
            .find(SocketAddr::is_ipv4)
            .or_else(|| addrs.iter().copied().find(SocketAddr::is_ipv6))
    }
}

fn hostname_prefers_ipv6(host: &str) -> bool {
    host.split('.')
        .next()
        .is_some_and(|label| label.eq_ignore_ascii_case("ipv6"))
}

fn udp_bind_addr(cfg: &NetworkConfig, remote: Option<SocketAddr>) -> SocketAddr {
    let use_ipv6 = remote.map(|addr| addr.is_ipv6()).unwrap_or_else(|| {
        cfg.required_source_ipv6.is_some() && cfg.required_source_ipv4.is_none()
    });
    if use_ipv6 {
        let ip = cfg
            .required_source_ipv6
            .as_deref()
            .and_then(|s| s.parse::<Ipv6Addr>().ok())
            .unwrap_or(Ipv6Addr::UNSPECIFIED);
        SocketAddr::new(IpAddr::V6(ip), 0)
    } else {
        let ip = cfg
            .required_source_ipv4
            .as_deref()
            .and_then(|s| s.parse::<Ipv4Addr>().ok())
            .unwrap_or(Ipv4Addr::UNSPECIFIED);
        SocketAddr::new(IpAddr::V4(ip), 0)
    }
}

fn create_udp_socket(bind: SocketAddr, iface: Option<&str>) -> Result<tokio::net::UdpSocket> {
    let socket = Socket::new(Domain::for_address(bind), Type::DGRAM, Some(Protocol::UDP))
        .map_err(CoreError::from)?;
    bind_socket_to_interface(&socket, iface)?;
    socket
        .bind(&SockAddr::from(bind))
        .map_err(CoreError::from)?;
    socket.set_nonblocking(true).map_err(CoreError::from)?;
    let socket: std::net::UdpSocket = socket.into();
    tokio::net::UdpSocket::from_std(socket).map_err(CoreError::from)
}

fn create_tcp_listener(bind: SocketAddr, iface: Option<&str>) -> Result<tokio::net::TcpListener> {
    let socket = Socket::new(Domain::for_address(bind), Type::STREAM, Some(Protocol::TCP))
        .map_err(CoreError::from)?;
    socket.set_reuse_address(true).map_err(CoreError::from)?;
    if bind.is_ipv6() {
        socket.set_only_v6(true).map_err(CoreError::from)?;
    }
    bind_socket_to_interface(&socket, iface)?;
    socket
        .bind(&SockAddr::from(bind))
        .map_err(CoreError::from)?;
    socket.listen(1024).map_err(CoreError::from)?;
    socket.set_nonblocking(true).map_err(CoreError::from)?;
    let listener: std::net::TcpListener = socket.into();
    tokio::net::TcpListener::from_std(listener).map_err(CoreError::from)
}

#[cfg(target_os = "linux")]
fn bind_socket_to_interface<S: std::os::fd::AsRawFd>(
    socket: &S,
    iface: Option<&str>,
) -> Result<()> {
    let Some(iface) = iface else {
        return Ok(());
    };
    let iface = std::ffi::CString::new(iface).map_err(|_| {
        CoreError::InvalidConfig("network.required_interface must not contain NUL bytes".into())
    })?;
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            iface.as_ptr().cast(),
            iface.as_bytes_with_nul().len() as libc::socklen_t,
        )
    };
    if ret != 0 {
        return Err(CoreError::NetworkBlocked(format!(
            "torrent data plane blocked: failed to bind socket to interface: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn bind_socket_to_interface<S>(_socket: &S, iface: Option<&str>) -> Result<()> {
    if iface.is_some() {
        return Err(CoreError::NetworkBlocked(
            "torrent data plane blocked: interface binding requires Linux SO_BINDTODEVICE".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use swarmotter_core::net::{InterfaceInfo, InterfaceProbe, NetworkBinder};

    struct FakeProbe {
        dns_ok: bool,
        source_ok: bool,
        namespace_ok: bool,
    }

    impl InterfaceProbe for FakeProbe {
        fn list(&self) -> Vec<InterfaceInfo> {
            Vec::new()
        }

        fn find(&self, _name: &str) -> Option<InterfaceInfo> {
            None
        }

        fn source_assigned(&self, _addr: &str, _iface: Option<&str>) -> bool {
            self.source_ok
        }

        fn route_valid(&self, _config: &NetworkConfig) -> bool {
            true
        }

        fn dns_constrained(&self, _config: &NetworkConfig) -> bool {
            self.dns_ok
        }

        fn namespace_available(&self, _ns: &str) -> bool {
            self.namespace_ok
        }
    }

    #[tokio::test]
    async fn blocked_binder_blocks_https_tracker() {
        use swarmotter_core::net::NetworkBinder;
        let binder = swarmotter_core::net::binder::BlockedBinder;
        let err = binder
            .http_get("https://tracker.example:443/announce?info_hash=x")
            .await
            .unwrap_err();
        assert!(err.is_network_blocked());
    }

    #[tokio::test]
    async fn strict_binder_requires_enforceable_socket_path() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            fail_closed: true,
            ..Default::default()
        };
        let binder = ContainedBinder::new(
            cfg,
            Arc::new(FakeProbe {
                dns_ok: false,
                source_ok: false,
                namespace_ok: false,
            }),
        );

        let err = match binder.udp_socket().await {
            Ok(_) => panic!("strict binder unexpectedly created a UDP socket"),
            Err(err) => err,
        };
        assert!(err.is_network_blocked());
    }

    #[test]
    fn traffic_allowed_uses_shared_config_reads() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_source_ipv4: Some("127.0.0.1".into()),
            fail_closed: true,
            validate_dns: false,
            ..Default::default()
        };
        let binder = ContainedBinder::new(
            cfg,
            Arc::new(FakeProbe {
                dns_ok: false,
                source_ok: true,
                namespace_ok: false,
            }),
        );

        let _read_guard = binder.config.read().unwrap();
        assert!(binder.traffic_allowed());
    }

    #[tokio::test]
    async fn strict_binder_blocks_webseed_range_without_socket_path() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            fail_closed: true,
            ..Default::default()
        };
        let binder = ContainedBinder::new(
            cfg,
            Arc::new(FakeProbe {
                dns_ok: false,
                source_ok: false,
                namespace_ok: false,
            }),
        );

        let err = binder
            .http_get_range("http://127.0.0.1:9/file.bin", 0, 16)
            .await
            .unwrap_err();
        assert!(err.is_network_blocked());
    }

    #[tokio::test]
    async fn strict_binder_blocks_unvalidated_hostname_dns() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_source_ipv4: Some("127.0.0.1".into()),
            fail_closed: true,
            validate_dns: false,
            ..Default::default()
        };
        let binder = ContainedBinder::new(
            cfg,
            Arc::new(FakeProbe {
                dns_ok: false,
                source_ok: true,
                namespace_ok: false,
            }),
        );

        let err = binder
            .resolve_host("tracker.example", 80)
            .await
            .unwrap_err();
        assert!(err.is_network_blocked());
        assert!(binder.resolve_host("127.0.0.1", 80).await.is_ok());
    }

    #[tokio::test]
    async fn strict_binder_allows_validated_hostname_dns() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_source_ipv4: Some("127.0.0.1".into()),
            fail_closed: true,
            validate_dns: false,
            ..Default::default()
        };
        let binder = ContainedBinder::new(
            cfg,
            Arc::new(FakeProbe {
                dns_ok: true,
                source_ok: true,
                namespace_ok: false,
            }),
        );

        assert!(binder.resolve_host("localhost", 80).await.is_ok());
    }

    #[test]
    fn interface_only_config_enforces_both_address_families() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("br0".into()),
            allow_ipv6: true,
            fail_closed: true,
            ..Default::default()
        };
        ensure_family_enforced(&cfg, "192.0.2.20:80".parse().unwrap()).unwrap();
        ensure_family_enforced(&cfg, "[2001:db8::20]:80".parse().unwrap()).unwrap();
    }

    #[test]
    fn ipv6_disabled_blocks_ipv6_even_with_interface_binding() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("br0".into()),
            allow_ipv6: false,
            fail_closed: true,
            ..Default::default()
        };

        let err = ensure_family_enforced(&cfg, "[2001:db8::20]:80".parse().unwrap()).unwrap_err();
        assert!(err.is_network_blocked());
        assert!(err.to_string().contains("IPv6 traffic is disabled"));
    }

    #[test]
    fn strict_source_config_blocks_unconfigured_family() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_source_ipv4: Some("192.0.2.10".into()),
            allow_ipv6: true,
            fail_closed: true,
            ..Default::default()
        };
        ensure_family_enforced(&cfg, "192.0.2.20:80".parse().unwrap()).unwrap();
        let err = ensure_family_enforced(&cfg, "[2001:db8::20]:80".parse().unwrap()).unwrap_err();
        assert!(err.is_network_blocked());
    }

    #[test]
    fn resolver_selection_prefers_ipv4_for_dual_stack_default() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("br0".into()),
            allow_ipv6: true,
            fail_closed: true,
            ..Default::default()
        };
        let addrs = [
            "[2001:db8::20]:443".parse().unwrap(),
            "192.0.2.20:443".parse().unwrap(),
        ];

        assert_eq!(
            select_resolved_addr(&cfg, "tracker.example", &addrs),
            Some("192.0.2.20:443".parse().unwrap())
        );
    }

    #[test]
    fn resolver_selection_honors_ipv6_only_source_preference() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_source_ipv6: Some("2001:db8::10".into()),
            allow_ipv6: true,
            fail_closed: true,
            ..Default::default()
        };
        let addrs = [
            "192.0.2.20:443".parse().unwrap(),
            "[2001:db8::20]:443".parse().unwrap(),
        ];

        assert_eq!(
            select_resolved_addr(&cfg, "tracker.example", &addrs),
            Some("[2001:db8::20]:443".parse().unwrap())
        );
    }

    #[test]
    fn resolver_selection_honors_explicit_ipv6_tracker_hostname() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("br0".into()),
            allow_ipv6: true,
            fail_closed: true,
            ..Default::default()
        };
        let addrs = [
            "192.0.2.20:443".parse().unwrap(),
            "[2001:db8::20]:443".parse().unwrap(),
        ];

        assert_eq!(
            select_resolved_addr(&cfg, "ipv6.tracker.example", &addrs),
            Some("[2001:db8::20]:443".parse().unwrap())
        );
    }

    #[test]
    fn udp_bind_addr_follows_remote_family() {
        let cfg = NetworkConfig {
            mode: NetworkContainmentMode::Strict,
            required_interface: Some("br0".into()),
            allow_ipv6: true,
            fail_closed: true,
            ..Default::default()
        };
        assert!(udp_bind_addr(&cfg, Some("192.0.2.20:80".parse().unwrap())).is_ipv4());
        assert!(udp_bind_addr(&cfg, Some("[2001:db8::20]:80".parse().unwrap())).is_ipv6());
    }

    #[tokio::test]
    async fn already_created_udp_socket_stops_sending_after_gate_block() {
        let gate = ContainmentGate::new(true);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let binder = ContainedBinder::new(
            NetworkConfig {
                mode: NetworkContainmentMode::Disabled,
                ..Default::default()
            },
            Arc::new(FakeProbe {
                dns_ok: true,
                source_ok: true,
                namespace_ok: true,
            }),
        )
        .with_gate_and_health(gate.clone(), tx);
        let spy = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let spy_addr = spy.local_addr().unwrap();
        let socket = binder.udp_socket_for(Some(spy_addr)).await.unwrap();

        socket.send_to(spy_addr, b"before").await.unwrap();
        let mut buf = [0u8; 16];
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), spy.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"before");

        gate.block(NetworkContainmentStatus::InterfaceDown, "path removed");
        let error = socket.send_to(spy_addr, b"after").await.unwrap_err();
        assert!(error.is_network_blocked());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), spy.recv_from(&mut buf))
                .await
                .is_err(),
            "blocked UDP socket emitted a datagram"
        );
    }

    #[tokio::test]
    async fn already_created_listener_cancels_pending_accept_after_gate_block() {
        let gate = ContainmentGate::new(true);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let binder = ContainedBinder::new(
            NetworkConfig {
                mode: NetworkContainmentMode::Disabled,
                allow_ipv6: false,
                ..Default::default()
            },
            Arc::new(FakeProbe {
                dns_ok: true,
                source_ok: true,
                namespace_ok: true,
            }),
        )
        .with_gate_and_health(gate.clone(), tx);
        let listener = binder.bind_peer_listener(0).await.unwrap();
        let generation = gate.generation();
        let accept = tokio::spawn(async move { listener.accept().await });
        tokio::task::yield_now().await;
        gate.block(NetworkContainmentStatus::InterfaceDown, "path removed");
        let error = tokio::time::timeout(Duration::from_secs(1), accept)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.is_network_blocked());
        assert_ne!(gate.generation(), generation);
    }

    #[tokio::test]
    async fn source_bind_failure_blocks_gate_immediately_and_reports_status() {
        let gate = ContainmentGate::new(true);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let binder = ContainedBinder::new(
            NetworkConfig {
                mode: NetworkContainmentMode::Strict,
                required_source_ipv4: Some("192.0.2.254".into()),
                allow_ipv6: false,
                fail_closed: true,
                validate_route: false,
                validate_dns: false,
                ..Default::default()
            },
            Arc::new(FakeProbe {
                dns_ok: true,
                source_ok: true,
                namespace_ok: false,
            }),
        )
        .with_gate_and_health(gate.clone(), tx);

        let error = match binder.udp_socket().await {
            Ok(_) => panic!("unassigned source address unexpectedly bound"),
            Err(error) => error,
        };
        assert!(
            !gate.traffic_allowed(),
            "bind report did not block immediately"
        );
        assert!(!error.to_string().is_empty());
        let report = rx.recv().await.unwrap();
        assert_eq!(report.status, NetworkContainmentStatus::SocketBindFailed);
    }

    #[tokio::test]
    async fn outbound_tcp_source_bind_failure_reports_before_connect() {
        let gate = ContainmentGate::new(true);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let binder = ContainedBinder::new(
            NetworkConfig {
                mode: NetworkContainmentMode::Strict,
                required_source_ipv4: Some("192.0.2.254".into()),
                allow_ipv6: false,
                fail_closed: true,
                validate_route: false,
                validate_dns: false,
                ..Default::default()
            },
            Arc::new(FakeProbe {
                dns_ok: true,
                source_ok: true,
                namespace_ok: false,
            }),
        )
        .with_gate_and_health(gate.clone(), tx);

        assert!(binder
            .connect_peer("127.0.0.1:9".parse().unwrap())
            .await
            .is_err());
        assert!(!gate.traffic_allowed());
        let report = rx.recv().await.unwrap();
        assert_eq!(report.status, NetworkContainmentStatus::SocketBindFailed);
        assert!(report.detail.contains("outbound peer socket to source"));
    }

    #[tokio::test]
    async fn peer_listener_bind_failure_blocks_and_reports_in_strict_mode() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = occupied.local_addr().unwrap().port();
        let gate = ContainmentGate::new(true);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let binder = ContainedBinder::new(
            NetworkConfig {
                mode: NetworkContainmentMode::Strict,
                required_source_ipv4: Some("127.0.0.1".into()),
                allow_ipv6: false,
                fail_closed: true,
                validate_route: false,
                validate_dns: false,
                ..Default::default()
            },
            Arc::new(FakeProbe {
                dns_ok: true,
                source_ok: true,
                namespace_ok: false,
            }),
        )
        .with_gate_and_health(gate.clone(), tx);

        assert!(binder.bind_peer_listener(port).await.is_err());
        assert!(!gate.traffic_allowed());
        let report = rx.recv().await.unwrap();
        assert_eq!(report.status, NetworkContainmentStatus::SocketBindFailed);
        assert!(report.detail.contains("peer listener"));
    }

    #[tokio::test]
    async fn family_policy_denial_blocks_gate_and_reports_fail_closed() {
        let gate = ContainmentGate::new(true);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let binder = ContainedBinder::new(
            NetworkConfig {
                mode: NetworkContainmentMode::Strict,
                required_source_ipv4: Some("127.0.0.1".into()),
                allow_ipv6: true,
                fail_closed: true,
                validate_route: false,
                validate_dns: false,
                ..Default::default()
            },
            Arc::new(FakeProbe {
                dns_ok: true,
                source_ok: true,
                namespace_ok: false,
            }),
        )
        .with_gate_and_health(gate.clone(), tx);

        let remote = "[::1]:9".parse().unwrap();
        let error = match binder.udp_socket_for(Some(remote)).await {
            Ok(_) => panic!("uncontained IPv6 socket unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.is_network_blocked());
        assert!(!gate.traffic_allowed());
        let report = rx.recv().await.unwrap();
        assert_eq!(report.status, NetworkContainmentStatus::BlockedFailClosed);
    }
}
