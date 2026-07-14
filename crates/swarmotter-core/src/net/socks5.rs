// SPDX-License-Identifier: Apache-2.0

//! SOCKS5 TCP `CONNECT` transport layered over a contained binder.
//!
//! [`Socks5Binder`] never creates sockets itself. It resolves and connects to
//! the configured proxy through an inner [`NetworkBinder`], then performs a
//! SOCKS5 handshake on that contained stream. Target hostnames use the SOCKS5
//! domain address form, so HTTP(S) trackers and webseeds do not resolve their
//! target hostname locally. This is intentionally TCP-only: the wrapper
//! rejects UDP socket and direct-resolution requests rather than silently
//! using a non-proxied path.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{ContainedUdpSocket, NetworkBinder, PeerListener};
use crate::error::{CoreError, Result};
use crate::net::config::Socks5ProxyConfig;

const SOCKS5_VERSION: u8 = 0x05;
const SOCKS5_AUTH_VERSION: u8 = 0x01;
const SOCKS5_NO_AUTH: u8 = 0x00;
const SOCKS5_USERNAME_PASSWORD: u8 = 0x02;
const SOCKS5_NO_ACCEPTABLE_METHODS: u8 = 0xff;
const SOCKS5_COMMAND_CONNECT: u8 = 0x01;
const SOCKS5_ATYP_IPV4: u8 = 0x01;
const SOCKS5_ATYP_DOMAIN: u8 = 0x03;
const SOCKS5_ATYP_IPV6: u8 = 0x04;
const SOCKS5_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A contained SOCKS5 proxy layer for TCP torrent traffic.
///
/// The inner binder remains authoritative for containment checks, source or
/// interface binding, and proxy-host DNS. A failure to reach or negotiate
/// with the proxy returns an error and never retries the target directly.
pub struct Socks5Binder {
    inner: Arc<dyn NetworkBinder>,
    config: Socks5ProxyConfig,
}

impl Socks5Binder {
    /// Build a proxy layer from a validated explicit SOCKS5 configuration.
    /// The constructor does not open a socket; every operation reuses the
    /// inner binder's live containment policy.
    pub fn new(inner: Arc<dyn NetworkBinder>, config: Socks5ProxyConfig) -> Self {
        Self { inner, config }
    }

    async fn open_proxy_stream(&self) -> Result<tokio::net::TcpStream> {
        let host = self.config.normalized_host()?;
        let proxy = self.inner.resolve_host(&host, self.config.port).await?;
        self.inner.connect_peer(proxy).await
    }

    async fn connect_target(&self, target: Socks5Target<'_>) -> Result<tokio::net::TcpStream> {
        let stream = self.open_proxy_stream().await?;
        tokio::time::timeout(
            SOCKS5_HANDSHAKE_TIMEOUT,
            socks5_connect(stream, &self.config, target),
        )
        .await
        .map_err(|_| CoreError::Proxy("SOCKS5 proxy handshake timed out".into()))?
    }

    fn udp_blocked_error() -> CoreError {
        CoreError::Proxy(
            "SOCKS5 proxy support is TCP CONNECT only; UDP tracker, DHT, and uTP traffic is blocked rather than sent directly"
                .into(),
        )
    }

    fn target_resolution_blocked_error() -> CoreError {
        CoreError::Proxy(
            "SOCKS5 proxy support uses remote DNS only for TCP CONNECT targets; direct target hostname resolution is blocked"
                .into(),
        )
    }
}

#[async_trait]
impl NetworkBinder for Socks5Binder {
    /// Connect an IP-literal peer through the proxy. The only TCP connection
    /// opened by this layer is to the configured proxy through `inner`.
    async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        self.connect_target(Socks5Target::Socket(addr)).await
    }

    /// Connect an HTTP(S) hostname through the proxy's resolver. This is the
    /// hostname-capable seam used by `ContainedHttpClient`; it deliberately
    /// does not call `resolve_host` for the target.
    async fn connect_host(&self, host: &str, port: u16) -> Result<tokio::net::TcpStream> {
        self.connect_target(Socks5Target::Domain(host, port)).await
    }

    /// There is no SOCKS5 UDP ASSOCIATE implementation. Refuse every UDP
    /// path at the proxy layer so a configured proxy cannot leave a direct
    /// UDP escape hatch.
    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        Err(Self::udp_blocked_error())
    }

    async fn udp_socket_for(
        &self,
        _remote: Option<SocketAddr>,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        Err(Self::udp_blocked_error())
    }

    async fn udp_socket_on(
        &self,
        _remote: Option<SocketAddr>,
        _local_port: u16,
    ) -> Result<Box<dyn ContainedUdpSocket>> {
        Err(Self::udp_blocked_error())
    }

    /// Callers that need a local destination address are UDP-oriented. Block
    /// that seam rather than resolving a target outside SOCKS remote DNS.
    async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
        Err(Self::target_resolution_blocked_error())
    }

    /// Inbound listeners remain bound to the configured contained path; a
    /// SOCKS5 CONNECT proxy does not provide inbound listener forwarding.
    async fn bind_peer_listener(&self, port: u16) -> Result<Box<dyn PeerListener>> {
        self.inner.bind_peer_listener(port).await
    }

    async fn bind_diagnostic_listener(&self, port: u16) -> Result<Box<dyn PeerListener>> {
        self.inner.bind_diagnostic_listener(port).await
    }

    fn traffic_allowed(&self) -> bool {
        self.inner.traffic_allowed()
    }
}

enum Socks5Target<'a> {
    Socket(SocketAddr),
    Domain(&'a str, u16),
}

/// Negotiate SOCKS5 and issue one `CONNECT` request over an already-contained
/// TCP stream. Kept generic over async I/O so protocol framing has fully local
/// in-memory tests without a real network socket.
async fn socks5_connect<S>(
    mut stream: S,
    config: &Socks5ProxyConfig,
    target: Socks5Target<'_>,
) -> Result<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    config.validate()?;
    negotiate_authentication(&mut stream, config).await?;
    send_connect_request(&mut stream, target).await?;
    read_connect_response(&mut stream).await?;
    Ok(stream)
}

async fn negotiate_authentication<S>(stream: &mut S, config: &Socks5ProxyConfig) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let method = if config.has_authentication() {
        SOCKS5_USERNAME_PASSWORD
    } else {
        SOCKS5_NO_AUTH
    };
    write_proxy(stream, &[SOCKS5_VERSION, 1, method]).await?;
    let mut response = [0u8; 2];
    read_proxy(stream, &mut response).await?;
    if response[0] != SOCKS5_VERSION {
        return Err(CoreError::Proxy(
            "SOCKS5 proxy returned an unsupported protocol version".into(),
        ));
    }
    if response[1] == SOCKS5_NO_ACCEPTABLE_METHODS {
        return Err(CoreError::Proxy(
            "SOCKS5 proxy accepted no offered authentication method".into(),
        ));
    }
    if response[1] != method {
        return Err(CoreError::Proxy(
            "SOCKS5 proxy selected an authentication method not permitted by configuration".into(),
        ));
    }
    if method == SOCKS5_USERNAME_PASSWORD {
        authenticate_username_password(stream, config).await?;
    }
    Ok(())
}

async fn authenticate_username_password<S>(stream: &mut S, config: &Socks5ProxyConfig) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let username = config.username.as_deref().ok_or_else(|| {
        CoreError::InvalidConfig(
            "network.socks5 username/password configuration is incomplete".into(),
        )
    })?;
    let password = config.password.as_deref().ok_or_else(|| {
        CoreError::InvalidConfig(
            "network.socks5 username/password configuration is incomplete".into(),
        )
    })?;
    let username_len = u8::try_from(username.len()).map_err(|_| {
        CoreError::InvalidConfig("network.socks5.username must contain 1 to 255 bytes".into())
    })?;
    let password_len = u8::try_from(password.len()).map_err(|_| {
        CoreError::InvalidConfig("network.socks5.password must contain 1 to 255 bytes".into())
    })?;

    let mut request = Vec::with_capacity(3 + username.len() + password.len());
    request.push(SOCKS5_AUTH_VERSION);
    request.push(username_len);
    request.extend_from_slice(username.as_bytes());
    request.push(password_len);
    request.extend_from_slice(password.as_bytes());
    write_proxy(stream, &request).await?;

    let mut response = [0u8; 2];
    read_proxy(stream, &mut response).await?;
    if response[0] != SOCKS5_AUTH_VERSION || response[1] != 0 {
        return Err(CoreError::Proxy(
            "SOCKS5 username/password authentication was rejected".into(),
        ));
    }
    Ok(())
}

async fn send_connect_request<S>(stream: &mut S, target: Socks5Target<'_>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut request = vec![SOCKS5_VERSION, SOCKS5_COMMAND_CONNECT, 0];
    match target {
        Socks5Target::Socket(address) => push_socket_target(&mut request, address),
        Socks5Target::Domain(host, port) => {
            if let Ok(ip) = host.parse::<IpAddr>() {
                push_socket_target(&mut request, SocketAddr::new(ip, port));
            } else {
                let host = host.as_bytes();
                let length = u8::try_from(host.len()).map_err(|_| {
                    CoreError::Proxy(
                        "SOCKS5 target hostname exceeds the protocol's 255-byte limit".into(),
                    )
                })?;
                if length == 0 {
                    return Err(CoreError::Proxy(
                        "SOCKS5 target hostname must not be empty".into(),
                    ));
                }
                request.push(SOCKS5_ATYP_DOMAIN);
                request.push(length);
                request.extend_from_slice(host);
                request.extend_from_slice(&port.to_be_bytes());
            }
        }
    }
    write_proxy(stream, &request).await
}

fn push_socket_target(request: &mut Vec<u8>, address: SocketAddr) {
    match address.ip() {
        IpAddr::V4(ip) => {
            request.push(SOCKS5_ATYP_IPV4);
            request.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            request.push(SOCKS5_ATYP_IPV6);
            request.extend_from_slice(&ip.octets());
        }
    }
    request.extend_from_slice(&address.port().to_be_bytes());
}

async fn read_connect_response<S>(stream: &mut S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut header = [0u8; 4];
    read_proxy(stream, &mut header).await?;
    if header[0] != SOCKS5_VERSION {
        return Err(CoreError::Proxy(
            "SOCKS5 proxy returned an unsupported CONNECT response version".into(),
        ));
    }
    if header[2] != 0 {
        return Err(CoreError::Proxy(
            "SOCKS5 proxy returned a malformed CONNECT response".into(),
        ));
    }
    if header[1] != 0 {
        return Err(CoreError::Proxy(format!(
            "SOCKS5 proxy CONNECT was refused: {}",
            connect_reply_name(header[1])
        )));
    }
    consume_bound_address(stream, header[3]).await
}

async fn consume_bound_address<S>(stream: &mut S, atyp: u8) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match atyp {
        SOCKS5_ATYP_IPV4 => {
            let mut tail = [0u8; 6];
            read_proxy(stream, &mut tail).await?;
        }
        SOCKS5_ATYP_IPV6 => {
            let mut tail = [0u8; 18];
            read_proxy(stream, &mut tail).await?;
        }
        SOCKS5_ATYP_DOMAIN => {
            let mut length = [0u8; 1];
            read_proxy(stream, &mut length).await?;
            let mut tail = vec![0u8; usize::from(length[0]) + 2];
            read_proxy(stream, &mut tail).await?;
        }
        _ => {
            return Err(CoreError::Proxy(
                "SOCKS5 proxy returned an unsupported CONNECT address type".into(),
            ));
        }
    }
    Ok(())
}

fn connect_reply_name(reply: u8) -> &'static str {
    match reply {
        0x01 => "general failure",
        0x02 => "connection not allowed",
        0x03 => "network unreachable",
        0x04 => "host unreachable",
        0x05 => "connection refused",
        0x06 => "TTL expired",
        0x07 => "command unsupported",
        0x08 => "address type unsupported",
        _ => "unknown failure",
    }
}

async fn write_proxy<S>(stream: &mut S, bytes: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream
        .write_all(bytes)
        .await
        .map_err(|_| CoreError::Proxy("SOCKS5 proxy connection write failed".into()))
}

async fn read_proxy<S>(stream: &mut S, bytes: &mut [u8]) -> Result<()>
where
    S: AsyncRead + Unpin,
{
    stream
        .read_exact(bytes)
        .await
        .map(|_| ())
        .map_err(|_| CoreError::Proxy("SOCKS5 proxy response was truncated".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::PeerInfoHash;
    use crate::net::HttpResponse;
    use crate::tracker::{AnnounceEvent, AnnounceRequest};
    use crate::udp_tracker::udp_announce;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::DuplexStream;
    use tokio::sync::Mutex;

    fn proxy_config() -> Socks5ProxyConfig {
        Socks5ProxyConfig {
            enabled: true,
            host: Some("proxy.example".into()),
            port: 1080,
            username: None,
            password: None,
        }
    }

    async fn read_exact(stream: &mut DuplexStream, expected: &[u8]) {
        let mut received = vec![0u8; expected.len()];
        stream.read_exact(&mut received).await.unwrap();
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn no_auth_connect_uses_domain_form_for_remote_dns() {
        let (client, mut server) = tokio::io::duplex(1024);
        let server = tokio::spawn(async move {
            read_exact(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0]).await.unwrap();
            read_exact(
                &mut server,
                &[
                    5, 1, 0, 3, 15, b't', b'r', b'a', b'c', b'k', b'e', b'r', b'.', b'e', b'x',
                    b'a', b'm', b'p', b'l', b'e', 0x1a, 0xe1,
                ],
            )
            .await;
            server
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });

        let stream = socks5_connect(
            client,
            &proxy_config(),
            Socks5Target::Domain("tracker.example", 6881),
        )
        .await;
        assert!(stream.is_ok());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn username_password_authentication_is_negotiated_without_logging_credentials() {
        let (client, mut server) = tokio::io::duplex(1024);
        let server = tokio::spawn(async move {
            read_exact(&mut server, &[5, 1, 2]).await;
            server.write_all(&[5, 2]).await.unwrap();
            read_exact(
                &mut server,
                &[
                    1, 4, b'u', b's', b'e', b'r', 6, b's', b'e', b'c', b'r', b'e', b't',
                ],
            )
            .await;
            server.write_all(&[1, 0]).await.unwrap();
            read_exact(&mut server, &[5, 1, 0, 1, 192, 0, 2, 9, 0x13, 0x88]).await;
            server
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });
        let mut config = proxy_config();
        config.username = Some("user".into());
        config.password = Some("secret".into());

        assert!(socks5_connect(
            client,
            &config,
            Socks5Target::Socket("192.0.2.9:5000".parse().unwrap()),
        )
        .await
        .is_ok());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn malformed_and_refused_responses_are_proxy_errors() {
        let (client, mut server) = tokio::io::duplex(256);
        let server = tokio::spawn(async move {
            read_exact(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0]).await.unwrap();
            let mut request = [0u8; 10];
            server.read_exact(&mut request).await.unwrap();
            server
                .write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });
        let error = socks5_connect(
            client,
            &proxy_config(),
            Socks5Target::Socket("192.0.2.9:80".parse().unwrap()),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code().as_str(), "proxy_error");
        assert!(error.to_string().contains("connection refused"));
        server.await.unwrap();
    }

    struct RecordingBinder {
        proxy_addr: SocketAddr,
        resolves: Mutex<Vec<(String, u16)>>,
        connects: Mutex<Vec<SocketAddr>>,
        udp_calls: AtomicUsize,
    }

    impl RecordingBinder {
        fn new(proxy_addr: SocketAddr) -> Self {
            Self {
                proxy_addr,
                resolves: Mutex::new(Vec::new()),
                connects: Mutex::new(Vec::new()),
                udp_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl NetworkBinder for RecordingBinder {
        async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
            self.connects.lock().await.push(addr);
            tokio::net::TcpStream::connect(addr)
                .await
                .map_err(CoreError::from)
        }

        async fn resolve_host(&self, host: &str, port: u16) -> Result<SocketAddr> {
            self.resolves.lock().await.push((host.into(), port));
            if host == "proxy.example" && port == 1080 {
                Ok(self.proxy_addr)
            } else {
                Err(CoreError::Internal(
                    "target hostname must not be resolved locally".into(),
                ))
            }
        }

        async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
            self.udp_calls.fetch_add(1, Ordering::Relaxed);
            Err(CoreError::Internal("inner UDP must not be called".into()))
        }

        async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
            Err(CoreError::Internal("not used in test".into()))
        }

        fn traffic_allowed(&self) -> bool {
            true
        }

        async fn http_get(&self, _url: &str) -> Result<HttpResponse> {
            Err(CoreError::Internal("not used in test".into()))
        }
    }

    #[tokio::test]
    async fn wrapper_resolves_only_proxy_and_never_falls_back_to_target() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 3];
            stream.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, 0]);
            stream.write_all(&[5, 0]).await.unwrap();
            let mut head = [0u8; 5];
            stream.read_exact(&mut head).await.unwrap();
            assert_eq!(&head[..4], &[5, 1, 0, 3]);
            let mut rest = vec![0u8; usize::from(head[4]) + 2];
            stream.read_exact(&mut rest).await.unwrap();
            assert_eq!(&rest[..head[4] as usize], b"tracker.example");
            assert_eq!(&rest[head[4] as usize..], &443u16.to_be_bytes());
            stream
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });
        let inner = Arc::new(RecordingBinder::new(proxy_addr));
        let binder = Socks5Binder::new(inner.clone(), proxy_config());

        assert!(binder.connect_host("tracker.example", 443).await.is_ok());
        assert_eq!(
            *inner.resolves.lock().await,
            vec![("proxy.example".into(), 1080)]
        );
        assert_eq!(*inner.connects.lock().await, vec![proxy_addr]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wrapper_blocks_udp_and_local_target_resolution() {
        let inner = Arc::new(RecordingBinder::new("127.0.0.1:1080".parse().unwrap()));
        let binder = Socks5Binder::new(inner.clone(), proxy_config());
        let udp_error = match binder.udp_socket().await {
            Ok(_) => panic!("SOCKS5 wrapper unexpectedly opened a UDP socket"),
            Err(error) => error,
        };
        assert_eq!(udp_error.code().as_str(), "proxy_error");
        assert!(udp_error.to_string().contains("UDP tracker, DHT, and uTP"));
        let udp_for_error = match binder.udp_socket_for(None).await {
            Ok(_) => panic!("SOCKS5 wrapper unexpectedly opened a UDP socket"),
            Err(error) => error,
        };
        let udp_on_error = match binder.udp_socket_on(None, 0).await {
            Ok(_) => panic!("SOCKS5 wrapper unexpectedly opened a UDP socket"),
            Err(error) => error,
        };
        for error in [udp_for_error, udp_on_error] {
            assert_eq!(error.code().as_str(), "proxy_error");
            assert!(error.to_string().contains("UDP tracker, DHT, and uTP"));
        }
        let dns_error = binder
            .resolve_host("tracker.example", 443)
            .await
            .unwrap_err();
        assert_eq!(dns_error.code().as_str(), "proxy_error");
        let udp_tracker_error = udp_announce(
            &binder,
            &AnnounceRequest {
                tracker_url: "udp://tracker.example:6969/announce".into(),
                info_hash: PeerInfoHash::from_bytes([0x42; 20]),
                peer_id: *b"-SW0010-socks5udp001",
                port: 6881,
                uploaded: 0,
                downloaded: 0,
                left: 1,
                event: AnnounceEvent::Started,
                numwant: Some(1),
                compact: true,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(udp_tracker_error.code().as_str(), "proxy_error");
        assert_eq!(inner.udp_calls.load(Ordering::Relaxed), 0);
        assert!(inner.resolves.lock().await.is_empty());
        assert!(inner.connects.lock().await.is_empty());
    }
}
