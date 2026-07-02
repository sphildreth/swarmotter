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

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::models::network::NetworkContainmentMode;
use swarmotter_core::net::{
    self, parse_http_response, ContainedUdpSocket, HttpResponse, InterfaceProbe, NetworkBinder,
    NetworkConfig, PeerListener,
};

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
    S: AsyncWriteExt + AsyncReadExt + Unpin,
{
    stream.write_all(req).await.map_err(CoreError::from)?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(CoreError::from)?;
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
        let health = net::evaluate(&cfg, self.probe.as_ref());
        if cfg.fail_closed && !health.traffic_allowed {
            return Err(CoreError::NetworkBlocked(format!(
                "torrent data plane blocked: {}",
                health.status
            )));
        }
        Ok(())
    }

    async fn source(&self) -> Option<std::net::IpAddr> {
        let cfg = self.config.lock().await;
        cfg.required_source_ipv4
            .as_deref()
            .and_then(|s| s.parse().ok())
            .or_else(|| {
                cfg.required_source_ipv6
                    .as_deref()
                    .and_then(|s| s.parse().ok())
            })
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
        let source = self.source().await;
        let stream = match source {
            Some(ip) => {
                let bind = SocketAddr::new(ip, 0);
                let socket = tokio::net::TcpSocket::new_v4_or_v6_for(ip)?;
                socket.bind(bind)?;
                socket.connect(addr).await?
            }
            None => tokio::net::TcpStream::connect(addr).await?,
        };
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
        // Resolve hostname via std (subject to DNS containment validation at
        // the config layer). For IP-literal hosts this is a no-op.
        let addr: SocketAddr = match host.parse() {
            Ok(ip) => SocketAddr::new(ip, port),
            Err(_) => {
                let mut iter = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))?;
                iter.next().ok_or_else(|| {
                    CoreError::Internal(format!("tracker host {host} unresolvable"))
                })?
            }
        };
        let stream = self.connect_peer(addr).await?;
        let path = parsed.path();
        let query = parsed.query().map(|q| format!("?{q}")).unwrap_or_default();
        let req = format!(
            "GET {path}{query} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nUser-Agent: SwarmOtter/0.1\r\n\r\n"
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

    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        self.guard().await?;
        let source = self.source().await;
        let socket = match source {
            Some(ip) => {
                let bind = SocketAddr::new(ip, 0);
                tokio::net::UdpSocket::bind(bind).await?
            }
            None => tokio::net::UdpSocket::bind("0.0.0.0:0").await?,
        };
        Ok(Box::new(ContainedUdpSocketImpl { socket }))
    }

    async fn bind_peer_listener(&self, port: u16) -> Result<Box<dyn PeerListener>> {
        self.guard().await?;
        let source = self.source().await;
        let bind = match source {
            Some(ip) => SocketAddr::new(ip, port),
            None => SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), port),
        };
        let listener = tokio::net::TcpListener::bind(bind).await?;
        Ok(Box::new(ContainedPeerListener { listener }))
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
    listener: tokio::net::TcpListener,
}

#[async_trait]
impl PeerListener for ContainedPeerListener {
    async fn accept(&self) -> Result<tokio::net::TcpStream> {
        let (stream, _addr) = self.listener.accept().await.map_err(CoreError::from)?;
        Ok(stream)
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(CoreError::from)
    }
}

trait TcpSocketExt {
    fn new_v4_or_v6_for(ip: std::net::IpAddr) -> Result<tokio::net::TcpSocket>;
}

impl TcpSocketExt for tokio::net::TcpSocket {
    fn new_v4_or_v6_for(ip: std::net::IpAddr) -> Result<tokio::net::TcpSocket> {
        let socket = match ip {
            std::net::IpAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            std::net::IpAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        Ok(socket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
