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
    self, parse_http_response, HttpResponse, InterfaceProbe, NetworkBinder, NetworkConfig,
};

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
        let port = parsed.port_or_known_default().unwrap_or(80);
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
        let mut stream = self.connect_peer(addr).await?;
        let path = parsed.path();
        let query = parsed.query().map(|q| format!("?{q}")).unwrap_or_default();
        let req = format!(
            "GET {path}{query} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nUser-Agent: SwarmOtter/0.1\r\n\r\n"
        );
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(CoreError::from)?;
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .map_err(CoreError::from)?;
        parse_http_response(&buf)
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
