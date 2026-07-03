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
use std::time::Duration;

use async_trait::async_trait;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::models::network::NetworkContainmentMode;
use swarmotter_core::net::{
    self, parse_http_response, ContainedUdpSocket, HttpResponse, InterfaceProbe, NetworkBinder,
    NetworkConfig, PeerListener,
};

const MAX_TRACKER_HTTP_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const HTTP_TRACKER_IO_TIMEOUT: Duration = Duration::from_secs(30);
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Perform a TLS handshake over a contained TCP stream, validating the
/// certificate against the platform root trust store. The `server_name`
/// (SNI) is the tracker hostname. Returns the encrypted stream.
async fn tls_connect(
    stream: tokio::net::TcpStream,
    server_name: &str,
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let server_name: rustls::pki_types::ServerName<'static> = server_name
        .to_owned()
        .try_into()
        .map_err(|e| CoreError::Internal(format!("tls server name: {e}")))?;
    connector
        .connect(server_name, stream)
        .await
        .map_err(|e| CoreError::Internal(format!("tls handshake failed: {e}")))
}

/// Send an HTTP/1.1 GET request over any async read/write stream and parse
/// the response. Used for both plaintext (TCP) and TLS tracker connections.
async fn http_over_stream<S>(mut stream: S, req: &[u8]) -> Result<HttpResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    timeout(HTTP_TRACKER_IO_TIMEOUT, stream.write_all(req))
        .await
        .map_err(|_| CoreError::Internal("tracker request write timed out".into()))?
        .map_err(CoreError::from)?;

    let mut buf = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];
    loop {
        let n = timeout(HTTP_TRACKER_IO_TIMEOUT, stream.read(&mut chunk))
            .await
            .map_err(|_| CoreError::Internal("tracker response read timed out".into()))?
            .map_err(CoreError::from)?;
        if n == 0 {
            break;
        }
        if buf.len().saturating_add(n) > MAX_TRACKER_HTTP_RESPONSE_BYTES {
            return Err(CoreError::Internal(format!(
                "tracker response exceeded {} bytes",
                MAX_TRACKER_HTTP_RESPONSE_BYTES
            )));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    parse_http_response(&buf)
}

/// A real `NetworkBinder` that binds torrent sockets to the configured
/// source address and fails closed in strict mode.
pub struct ContainedBinder {
    config: Arc<Mutex<NetworkConfig>>,
    probe: Arc<dyn InterfaceProbe + Send + Sync>,
}

impl ContainedBinder {
    pub fn new(config: NetworkConfig, probe: Arc<dyn InterfaceProbe + Send + Sync>) -> Self {
        Self {
            config: Arc::new(Mutex::new(config)),
            probe,
        }
    }

    async fn guard(&self) -> Result<()> {
        let cfg = self.config.lock().await.clone();
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
            return Err(CoreError::NetworkBlocked(
                "torrent data plane blocked: strict containment requires an interface, source binding, or a current network namespace".into(),
            ));
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
        *self.config.lock().await = config;
    }
}

#[async_trait]
impl NetworkBinder for ContainedBinder {
    async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        self.guard().await?;
        let cfg = self.config.lock().await.clone();
        ensure_family_enforced(&cfg, addr)?;
        let socket = tokio::net::TcpSocket::new_v4_or_v6_for(addr)?;
        bind_socket_to_interface(&socket, cfg.required_interface.as_deref())?;
        let source = source_for_addr(&cfg, addr);
        let stream = timeout(TCP_CONNECT_TIMEOUT, async move {
            match source {
                Some(ip) => {
                    let bind = SocketAddr::new(ip, 0);
                    socket.bind(bind)?;
                    socket.connect(addr).await
                }
                None => socket.connect(addr).await,
            }
        })
        .await
        .map_err(|_| CoreError::Internal(format!("tcp connect to {addr} timed out")))?
        .map_err(CoreError::from)?;
        stream.set_nodelay(true).ok();
        Ok(stream)
    }

    async fn http_get(&self, url: &str) -> Result<HttpResponse> {
        self.guard().await?;
        let parsed = url::Url::parse(url)
            .map_err(|e| CoreError::Internal(format!("bad tracker url: {e}")))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| CoreError::Internal(format!("tracker url missing host: {url}")))?;
        let is_https = parsed.scheme() == "https";
        let port = parsed
            .port_or_known_default()
            .unwrap_or(if is_https { 443 } else { 80 });
        // Resolve through the binder so hostname lookup is gated by the same
        // containment and DNS policy as socket creation.
        let addr: SocketAddr = self.resolve_host(host, port).await?;
        let stream = self.connect_peer(addr).await?;
        let path = parsed.path();
        let query = parsed.query().map(|q| format!("?{q}")).unwrap_or_default();
        let req = format!(
            "GET {path}{query} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nUser-Agent: SwarmOtter/1.0\r\n\r\n"
        );
        if is_https {
            // TLS over the contained TCP connection. Certificate validation
            // uses the platform root trust store (webpki-roots); it stays
            // enabled unless a documented test-only path overrides it.
            let tls_stream = tls_connect(stream, host).await?;
            http_over_stream(tls_stream, req.as_bytes()).await
        } else {
            http_over_stream(stream, req.as_bytes()).await
        }
    }

    async fn resolve_host(&self, host: &str, port: u16) -> Result<SocketAddr> {
        self.guard().await?;
        if let Ok(ip) = host.parse() {
            return Ok(SocketAddr::new(ip, port));
        }
        let cfg = self.config.lock().await.clone();
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
        let cfg = self.config.lock().await.clone();
        if let Some(remote) = remote {
            ensure_family_enforced(&cfg, remote)?;
        }
        let bind = udp_bind_addr(&cfg, remote);
        let socket = create_udp_socket(bind, cfg.required_interface.as_deref())?;
        Ok(Box::new(ContainedUdpSocketImpl { socket }))
    }

    async fn udp_socket_on(
        &self,
        remote: Option<SocketAddr>,
        local_port: u16,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        self.guard().await?;
        let cfg = self.config.lock().await.clone();
        if let Some(remote) = remote {
            ensure_family_enforced(&cfg, remote)?;
        }
        let mut bind = udp_bind_addr(&cfg, remote);
        bind.set_port(local_port);
        let socket = create_udp_socket(bind, cfg.required_interface.as_deref())?;
        Ok(Box::new(ContainedUdpSocketImpl { socket }))
    }

    async fn bind_peer_listener(&self, port: u16) -> Result<Box<dyn PeerListener>> {
        self.guard().await?;
        let cfg = self.config.lock().await.clone();
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
            Some(create_tcp_listener(SocketAddr::new(v4_addr, port), iface)?)
        };
        let v6 = if cfg.allow_ipv6
            && (has_interface || namespace_only || cfg.required_source_ipv6.is_some())
        {
            Some(create_tcp_listener(SocketAddr::new(v6_addr, port), iface)?)
        } else {
            None
        };
        Ok(Box::new(ContainedPeerListener { v4, v6 }))
    }

    fn traffic_allowed(&self) -> bool {
        // Synchronous check: evaluate against a snapshot. For strictness we
        // re-evaluate; the async guard is the authoritative gate before any
        // socket is opened.
        let Ok(cfg) = self.config.try_lock() else {
            // If locked (in-flight reconfig), be conservative.
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
}

#[async_trait]
impl ContainedUdpSocket for ContainedUdpSocketImpl {
    async fn send_to(&self, addr: SocketAddr, data: &[u8]) -> Result<()> {
        self.socket
            .send_to(data, addr)
            .await
            .map_err(CoreError::from)?;
        Ok(())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<(SocketAddr, usize)> {
        let (n, addr) = self.socket.recv_from(buf).await.map_err(CoreError::from)?;
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
}

#[async_trait]
impl PeerListener for ContainedPeerListener {
    async fn accept(&self) -> Result<tokio::net::TcpStream> {
        let stream = match (&self.v4, &self.v6) {
            (Some(v4), Some(v6)) => {
                tokio::select! {
                    res = v4.accept() => res.map(|(stream, _)| stream),
                    res = v6.accept() => res.map(|(stream, _)| stream),
                }
            }
            (Some(v4), None) => v4.accept().await.map(|(stream, _)| stream),
            (None, Some(v6)) => v6.accept().await.map(|(stream, _)| stream),
            (None, None) => {
                return Err(CoreError::NetworkBlocked(
                    "torrent data plane blocked: no peer listener socket was bound".into(),
                ))
            }
        }
        .map_err(CoreError::from)?;
        Ok(stream)
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
    use swarmotter_core::net::{InterfaceInfo, InterfaceProbe};

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
    async fn http_over_stream_rejects_oversized_response() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            let mut req = [0u8; 256];
            let _ = server.read(&mut req).await;

            let header = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
            if server.write_all(header).await.is_err() {
                return;
            }
            let chunk = vec![b'x'; 64 * 1024];
            let mut sent = header.len();
            while sent <= MAX_TRACKER_HTTP_RESPONSE_BYTES + chunk.len() {
                if server.write_all(&chunk).await.is_err() {
                    return;
                }
                sent += chunk.len();
            }
            let _ = server.shutdown().await;
        });

        let err = http_over_stream(
            client,
            b"GET /announce HTTP/1.1\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("tracker response exceeded"));
        server_task.abort();
        let _ = server_task.await;
    }

    /// Real local TLS fixture: a self-signed HTTPS tracker over a contained
    /// TCP socket. Proves the HTTPS-over-contained-socket path with
    /// certificate validation against a configured root.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn https_get_over_contained_socket_validates_cert() {
        use swarmotter_core::net::NetworkBinder;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Install the ring crypto provider once for the process (rustls 0.23).
        use std::sync::OnceLock;
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });

        // Generate a self-signed cert for 127.0.0.1.
        let cert = rcgen::generate_simple_self_signed(vec![
            "127.0.0.1".to_string(),
            "localhost".to_string(),
        ])
        .unwrap();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let key_der =
            rustls::pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap();

        // Server config.
        let server_cfg = rustls::server::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_cfg));

        // Bencode tracker response body.
        let body = b"d8:intervali30e5:peers0:e".to_vec();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor_clone = acceptor.clone();
        let body_clone = body.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = acceptor_clone.accept(stream).await.unwrap();
            let mut buf = [0u8; 512];
            let _ = tokio::time::timeout(Duration::from_secs(2), tls.read(&mut buf)).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body_clone.len()
            );
            tls.write_all(resp.as_bytes()).await.unwrap();
            tls.write_all(&body_clone).await.unwrap();
            tls.shutdown().await.ok();
        });

        // Client: TLS over a contained TCP socket (LoopbackBinder provides the
        // contained TCP). The test adds the self-signed cert to the root store,
        // exercising the same machinery as production (http_over_stream + TLS).
        let binder = swarmotter_core::net::binder::LoopbackBinder;
        let tcp = binder.connect_peer(addr).await.unwrap();
        let mut client_root = rustls::RootCertStore::empty();
        client_root.add(cert_der).unwrap();
        let client_cfg = rustls::ClientConfig::builder()
            .with_root_certificates(client_root)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client_cfg));
        let server_name: rustls::pki_types::ServerName<'static> =
            "127.0.0.1".to_owned().try_into().unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let req =
            "GET /announce?info_hash=x HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
        let resp = http_over_stream(tls, req.as_bytes()).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, body);
    }
}
