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
//! concretely in the daemon against real `tokio` sockets with source binding.
//! Tests inject [`LoopbackBinder`] (or a custom fake) so the engine logic is
//! exercised without real network hardware.

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::error::{CoreError, Result};

/// A minimal HTTP response from a tracker announce.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
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
    async fn http_get(&self, url: &str) -> Result<HttpResponse>;

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

    async fn http_get(&self, url: &str) -> Result<HttpResponse> {
        let parsed = url::Url::parse(url)
            .map_err(|e| CoreError::Internal(format!("bad tracker url: {e}")))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| CoreError::Internal(format!("tracker url missing host: {url}")))?;
        let port = parsed.port_or_known_default().unwrap_or(80);
        let addr: SocketAddr = format!("{}:{}", host, port)
            .parse()
            .map_err(|e| CoreError::Internal(format!("bad tracker addr: {e}")))?;
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(CoreError::from)?;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
        true
    }
}

/// Parse a raw HTTP/1.x response into a [`HttpResponse`].
pub fn parse_http_response(raw: &[u8]) -> Result<HttpResponse> {
    let sep = find_subslice(raw, b"\r\n\r\n").ok_or_else(|| {
        CoreError::Internal("tracker response has no header/body separator".into())
    })?;
    let head = &raw[..sep];
    let body = raw[sep + 4..].to_vec();
    let head_str = std::str::from_utf8(head)
        .map_err(|e| CoreError::Internal(format!("tracker response non-utf8 header: {e}")))?;
    let mut lines = head_str.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| CoreError::Internal("tracker response empty status line".into()))?;
    let mut parts = status_line.split_whitespace();
    let _version = parts.next();
    let status: u16 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    Ok(HttpResponse { status, body })
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    (0..=haystack.len() - needle.len()).find(|&i| &haystack[i..i + needle.len()] == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_response_body() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nbody bytes here";
        let r = parse_http_response(raw).unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, b"body bytes here");
    }

    #[test]
    fn parse_http_response_rejects_malformed() {
        assert!(parse_http_response(b"no headers here").is_err());
    }
}
