// SPDX-License-Identifier: Apache-2.0

//! Network binding abstraction for the torrent data plane.
//!
//! This is the single choke point through which all torrent-related network
//! traffic must pass: peer TCP connections, tracker HTTP/UDP traffic, DHT,
//! PEX, webseeds, and magnet metadata fetching. No engine component may
//! create outbound sockets directly; it must obtain a connection from a
//! [`NetworkBinder`].
//!
//! The binder enforces fail-closed containment: before opening any torrent
//! socket it re-evaluates the configured network path and returns
//! [`CoreError::NetworkBlocked`] when strict mode is active and the path is
//! unavailable. This guarantees torrent traffic can never silently fall back
//! to the default route (see `design/vpn-network-containment.md`).
//!
//! The trait is defined in `swarmotter-core` (the contract) and implemented
//! concretely in the daemon against real `tokio` sockets with source or
//! interface binding. Tests inject [`LoopbackBinder`] (or a custom fake) so the
//! engine logic is exercised without real network hardware.

use std::net::SocketAddr;

use async_trait::async_trait;

use super::http::{ContainedHttpClient, HttpResponse};
#[cfg(any(test, feature = "test-binder"))]
use crate::error::CoreError;
use crate::error::Result;

/// A contained, fail-closed UDP datagram socket for torrent data-plane traffic
/// (UDP trackers, DHT, and future uTP). All send/receive goes through the
/// configured network path; the binder refuses to create it in strict
/// fail-closed mode when the path is unavailable.
///
/// Implementations are returned as a boxed trait object so the engine and
/// tracker logic remain independent of the concrete `tokio::net::UdpSocket`
/// and can be exercised in tests via the `LoopbackBinder`.
#[async_trait]
pub trait ContainedUdpSocket: Send + Sync {
    /// Send a datagram to `addr`.
    async fn send_to(&self, addr: SocketAddr, data: &[u8]) -> Result<()>;

    /// Receive a datagram, returning the source address and the payload. Waits
    /// up to the caller-chosen read loop; implementations should honor a
    /// reasonable bounded read via the surrounding `tokio::time::timeout`.
    async fn recv_from(&self, buf: &mut [u8]) -> Result<(SocketAddr, usize)>;

    /// The local address the socket is bound to (for DHT announce_peer).
    fn local_addr(&self) -> Result<SocketAddr>;
}

/// A contained, fail-closed TCP listener for inbound peer connections
/// (seeding). Accepts only through the configured network path; the binder
/// refuses to bind it in strict fail-closed mode when the path is unavailable.
#[async_trait]
pub trait PeerListener: Send + Sync {
    /// Accept the next inbound peer connection as a `tokio::net::TcpStream`.
    async fn accept(&self) -> Result<tokio::net::TcpStream>;

    /// The local address the listener is bound to.
    fn local_addr(&self) -> Result<SocketAddr>;
}

/// The network binding and containment abstraction for torrent traffic.
///
/// All methods must enforce containment: in strict fail-closed mode they
/// return [`CoreError::NetworkBlocked`] when the configured path is
/// unavailable, rather than creating a socket on the default route.
///
/// Peer connections are returned as concrete `tokio::net::TcpStream`s. Both
/// the real (source-bound) implementation and the test loopback
/// implementation produce real TCP streams, so the peer protocol code is
/// identical in production and tests.
#[async_trait]
pub trait NetworkBinder: Send + Sync {
    /// Open a TCP connection to a peer address through the contained path.
    async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream>;

    /// Issue an HTTP/1.1 GET to a tracker/announce URL through the contained
    /// path and return the response body. IP-literal hosts and localhost are
    /// supported; hostnames needing DNS resolution are handled by the binder
    /// implementation subject to DNS containment (see
    /// `vpn-network-containment.md`).
    async fn http_get(&self, url: &str) -> Result<HttpResponse> {
        ContainedHttpClient::new(self).get_tracker(url).await
    }

    /// Issue an HTTP/1.1 byte-range GET through the contained path. `start`
    /// is inclusive and `end_exclusive` is exclusive, matching torrent piece
    /// byte ranges; implementations convert this to an HTTP `Range:
    /// bytes=start-(end_exclusive - 1)` header.
    async fn http_get_range(
        &self,
        url: &str,
        start: u64,
        end_exclusive: u64,
    ) -> Result<HttpResponse> {
        ContainedHttpClient::new(self)
            .get_webseed_range(url, start, end_exclusive)
            .await
    }

    /// Resolve a torrent data-plane hostname through the contained path's DNS
    /// policy. IP literals return directly; hostnames must not be resolved
    /// before this binder has enforced containment.
    async fn resolve_host(&self, host: &str, port: u16) -> Result<SocketAddr>;

    /// Create a contained UDP socket bound to a local ephemeral port (and the
    /// configured source address/interface in the real binder). Used by UDP
    /// trackers and DHT. Never bypasses containment.
    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>>;

    /// Create a contained UDP socket suitable for traffic to `remote`. Binders
    /// that support multiple address families should use the remote address to
    /// choose IPv4 vs IPv6 binding. The default preserves older test binders.
    async fn udp_socket_for(
        &self,
        _remote: Option<SocketAddr>,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        self.udp_socket().await
    }

    /// Create a contained UDP socket bound to `local_port` when nonzero.
    /// Used by DHT so the configured DHT port is the actual local UDP port.
    async fn udp_socket_on(
        &self,
        remote: Option<SocketAddr>,
        local_port: u16,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        let _ = local_port;
        self.udp_socket_for(remote).await
    }

    /// Create a contained TCP listener for inbound peers on the given port
    /// (and the configured source address/interface in the real binder). Used
    /// for seeding/upload. Never bypasses containment.
    async fn bind_peer_listener(&self, port: u16) -> Result<Box<dyn PeerListener>>;

    /// Re-evaluate whether torrent data-plane traffic is currently permitted.
    /// Used by the engine to decide whether to start/continue peer activity.
    fn traffic_allowed(&self) -> bool;
}

/// A binder used in tests that permits localhost traffic and never blocks.
///
/// Real source binding is not performed; this connects to loopback peers and
/// serves tracker responses over plain TCP so engine logic can be exercised
/// deterministically without touching the default route.
#[cfg(any(test, feature = "test-binder"))]
pub struct LoopbackBinder;

#[cfg(any(test, feature = "test-binder"))]
#[async_trait]
impl NetworkBinder for LoopbackBinder {
    async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        tokio::net::TcpStream::connect(addr)
            .await
            .map_err(CoreError::from)
    }

    async fn resolve_host(&self, host: &str, port: u16) -> Result<SocketAddr> {
        match host.parse() {
            Ok(ip) => Ok(SocketAddr::new(ip, port)),
            Err(_) => {
                let mut iter = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))?;
                iter.next()
                    .ok_or_else(|| CoreError::Internal(format!("host {host} unresolvable")))
            }
        }
    }

    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .map_err(CoreError::from)?;
        Ok(Box::new(LoopbackUdpSocket { socket }))
    }

    async fn udp_socket_on(
        &self,
        remote: Option<SocketAddr>,
        local_port: u16,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        let bind = if remote.is_some_and(|addr| addr.is_ipv6()) {
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], local_port))
        } else {
            SocketAddr::from(([127, 0, 0, 1], local_port))
        };
        let socket = tokio::net::UdpSocket::bind(bind)
            .await
            .map_err(CoreError::from)?;
        Ok(Box::new(LoopbackUdpSocket { socket }))
    }

    async fn bind_peer_listener(&self, port: u16) -> Result<Box<dyn PeerListener>> {
        let addr: SocketAddr = format!("127.0.0.1:{port}")
            .parse()
            .map_err(|e| CoreError::Internal(format!("bad listener bind address: {e}")))?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(CoreError::from)?;
        Ok(Box::new(LoopbackPeerListener { listener }))
    }

    fn traffic_allowed(&self) -> bool {
        true
    }
}

#[cfg(any(test, feature = "test-binder"))]
struct LoopbackUdpSocket {
    socket: tokio::net::UdpSocket,
}

#[cfg(any(test, feature = "test-binder"))]
#[async_trait]
impl ContainedUdpSocket for LoopbackUdpSocket {
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

#[cfg(any(test, feature = "test-binder"))]
struct LoopbackPeerListener {
    listener: tokio::net::TcpListener,
}

#[cfg(any(test, feature = "test-binder"))]
#[async_trait]
impl PeerListener for LoopbackPeerListener {
    async fn accept(&self) -> Result<tokio::net::TcpStream> {
        let (stream, _addr) = self.listener.accept().await.map_err(CoreError::from)?;
        Ok(stream)
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(CoreError::from)
    }
}

/// A binder used in tests that models strict fail-closed containment: every
/// torrent data-plane operation (peer connect, tracker HTTP/UDP, inbound
/// listener) returns [`CoreError::NetworkBlocked`] and `traffic_allowed` is
/// false. The control plane is unaffected because this binder is only used by
/// torrent data-plane code.
#[cfg(any(test, feature = "test-binder"))]
pub struct BlockedBinder;

#[cfg(any(test, feature = "test-binder"))]
#[async_trait]
impl NetworkBinder for BlockedBinder {
    async fn connect_peer(&self, _addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        Err(CoreError::NetworkBlocked(
            "torrent data plane blocked".into(),
        ))
    }

    async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
        Err(CoreError::NetworkBlocked(
            "torrent data plane blocked".into(),
        ))
    }

    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        Err(CoreError::NetworkBlocked(
            "torrent data plane blocked".into(),
        ))
    }

    async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
        Err(CoreError::NetworkBlocked(
            "torrent data plane blocked".into(),
        ))
    }

    fn traffic_allowed(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loopback_udp_socket_send_recv_roundtrip() {
        let binder = LoopbackBinder;
        let sock = binder.udp_socket().await.unwrap();
        let local = sock.local_addr().unwrap();

        // Echo server: a second UDP socket that echoes datagrams back.
        let echo = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo.local_addr().unwrap();
        let echo_task = tokio::spawn(async move {
            let mut buf = [0u8; 32];
            let (n, peer) = echo.recv_from(&mut buf).await.unwrap();
            echo.send_to(&buf[..n], peer).await.unwrap();
        });

        sock.send_to(echo_addr, b"hello-udp").await.unwrap();
        let mut buf = [0u8; 32];
        let (from, n) = sock.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello-udp");
        assert_eq!(from, echo_addr);
        // Local addr is a loopback ephemeral port.
        assert!(local.is_ipv4());
        echo_task.await.unwrap();
    }

    #[tokio::test]
    async fn loopback_peer_listener_accepts_inbound() {
        let binder = LoopbackBinder;
        let listener = binder.bind_peer_listener(0).await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let stream = listener.accept().await.unwrap();
            stream.peer_addr().unwrap()
        });
        // Connect to the listener from loopback.
        let client = tokio::net::TcpStream::connect(listen_addr).await.unwrap();
        let client_local = client.local_addr().unwrap();
        let accepted_peer = accept_task.await.unwrap();
        // The accepted stream's peer addr is the client's local addr.
        assert_eq!(accepted_peer, client_local);
    }

    #[tokio::test]
    async fn blocked_binder_fail_closed_for_all_data_plane_ops() {
        let binder = BlockedBinder;
        assert!(!binder.traffic_allowed());
        assert!(binder
            .connect_peer("127.0.0.1:9".parse().unwrap())
            .await
            .is_err());
        assert!(binder
            .http_get("http://127.0.0.1:9/announce")
            .await
            .is_err());
        assert!(binder
            .http_get_range("http://127.0.0.1:9/file", 0, 4)
            .await
            .is_err());
        assert!(binder.udp_socket().await.is_err());
        assert!(binder.bind_peer_listener(0).await.is_err());
    }

    #[tokio::test]
    async fn loopback_http_get_range_sends_range_header() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 1024];
            let n = stream.read(&mut req).await.unwrap();
            let req = std::str::from_utf8(&req[..n]).unwrap();
            assert!(req.starts_with("GET /file.bin?token=local HTTP/1.1\r\n"));
            assert!(req
                .to_ascii_lowercase()
                .contains("\r\nrange: bytes=5-10\r\n"));

            stream
                .write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 5-10/100\r\n\r\nabcdef")
                .await
                .unwrap();
        });

        let binder = LoopbackBinder;
        let resp = binder
            .http_get_range(&format!("http://{addr}/file.bin?token=local"), 5, 11)
            .await
            .unwrap();

        assert_eq!(resp.status, 206);
        assert_eq!(resp.body, b"abcdef");
        server.await.unwrap();
    }
}
