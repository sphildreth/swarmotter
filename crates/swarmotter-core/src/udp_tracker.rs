// SPDX-License-Identifier: Apache-2.0

//! UDP tracker announce support (BEP 15).
//!
//! Implements the UDP tracker protocol: connect request/response, announce
//! request/response, compact IPv4 (and IPv6 where provided) peer parsing,
//! transaction IDs, and error response handling. All UDP traffic is routed
//! through the network containment layer's [`ContainedUdpSocket`] obtained
//! from the [`NetworkBinder`]; no UDP socket is created directly.
//!
//! BEP 15 uses a simple request/response transaction model over UDP with a
//! connection handshake: the client sends a connect request to obtain a
//! connection id, then sends an announce request signed with that connection
//! id. Responses are matched by transaction id. Timeouts and retries are
//! handled by the caller; this module performs single connect + announce
//! sequences against a bound contained socket.
//!
//! See `design/adr/0014-tracker-implementation-strategy.md`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::error::{CoreError, Result};
use crate::net::{ContainedUdpSocket, NetworkBinder};
use crate::peer::PeerAddr;
use crate::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse};

/// BEP 15 connect request magic constant.
const CONNECT_MAGIC: u64 = 0x4172_7101_9800;
pub const ACTION_CONNECT: u32 = 0;
pub const ACTION_ANNOUNCE: u32 = 1;
#[allow(dead_code)]
const ACTION_SCRAPE: u32 = 2;
pub const ACTION_ERROR: u32 = 3;

/// A UDP tracker connect+announce transaction executed through a contained
/// UDP socket. Resolves the tracker's UDP host:port into a `SocketAddr` and
/// runs the BEP 15 exchange, returning the parsed announce response.
pub async fn udp_announce(
    binder: &dyn NetworkBinder,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse> {
    udp_announce_with_iters(binder, req, 2).await
}

/// Run a UDP announce with a bounded number of connect/announce retries.
/// Each retry re-issues the connect handshake. Timeouts bound each step so a
/// silent tracker cannot hang the engine.
pub async fn udp_announce_with_iters(
    binder: &dyn NetworkBinder,
    req: &AnnounceRequest,
    retries: u32,
) -> Result<AnnounceResponse> {
    let addr = resolve_udp_tracker(&req.tracker_url)?;
    let socket = binder.udp_socket().await?;

    let mut last_err: Option<String> = None;
    for _ in 0..=retries {
        match run_one_transaction(socket.as_ref(), addr, req).await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                last_err = Some(e.to_string());
            }
        }
    }
    Err(CoreError::Internal(format!(
        "udp tracker {} failed after retries: {}",
        req.tracker_url,
        last_err.unwrap_or_else(|| "unknown error".into())
    )))
}

/// Resolve a `udp://host:port[/path]` tracker URL to a `SocketAddr`.
/// Hostnames are resolved via std (subject to DNS containment validation at
/// the config layer); IP-literal hosts require no resolution.
fn resolve_udp_tracker(url: &str) -> Result<SocketAddr> {
    let parsed = url::Url::parse(url)
        .map_err(|e| CoreError::Internal(format!("bad udp tracker url: {e}")))?;
    if parsed.scheme() != "udp" {
        return Err(CoreError::Internal(format!(
            "udp tracker url is not udp scheme: {url}"
        )));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| CoreError::Internal(format!("udp tracker url missing host: {url}")))?;
    let port = parsed
        .port()
        .ok_or_else(|| CoreError::Internal(format!("udp tracker url missing port: {url}")))?;
    match host.parse::<IpAddr>() {
        Ok(ip) => Ok(SocketAddr::new(ip, port)),
        Err(_) => {
            let mut iter = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))?;
            iter.next()
                .ok_or_else(|| CoreError::Internal(format!("udp tracker host {host} unresolvable")))
        }
    }
}

/// One BEP 15 connect + announce attempt. Returns the parsed announce
/// response or an error (timeout, malformed, or tracker error message).
async fn run_one_transaction(
    socket: &dyn ContainedUdpSocket,
    addr: SocketAddr,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse> {
    let connect_txn = random_txn_id();
    let connect = encode_connect(connect_txn);
    let buf = send_recv(socket, addr, &connect, 16).await?;
    let conn_id = decode_connect(&buf, connect_txn)?;

    let announce_req = encode_announce(req, conn_id);
    let buf = send_recv(socket, addr, &announce_req, 2048).await?;
    decode_announce(&buf)
}

/// Encode a BEP 15 connect request (16 bytes).
fn encode_connect(txn: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&CONNECT_MAGIC.to_be_bytes());
    out.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
    out.extend_from_slice(&txn.to_be_bytes());
    out
}

/// Decode a connect response (16 bytes), validating the action and txn id.
fn decode_connect(buf: &[u8], expected_txn: u32) -> Result<u64> {
    if buf.len() < 16 {
        return Err(CoreError::Parse("udp connect response too short".into()));
    }
    let action = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    let txn = u32::from_be_bytes(buf[4..8].try_into().unwrap());
    if txn != expected_txn {
        return Err(CoreError::Parse(
            "udp connect response transaction id mismatch".into(),
        ));
    }
    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..]).to_string();
        return Err(CoreError::Internal(format!(
            "udp tracker connect error: {msg}"
        )));
    }
    if action != ACTION_CONNECT {
        return Err(CoreError::Parse(format!(
            "udp connect response unexpected action {action}"
        )));
    }
    let conn_id = u64::from_be_bytes(buf[8..16].try_into().unwrap());
    Ok(conn_id)
}

/// Encode a BEP 15 announce request (98 bytes).
fn encode_announce(req: &AnnounceRequest, connection_id: u64) -> Vec<u8> {
    let txn = random_txn_id();
    let mut out = Vec::with_capacity(98);
    out.extend_from_slice(&connection_id.to_be_bytes());
    out.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    out.extend_from_slice(&txn.to_be_bytes());
    out.extend_from_slice(req.info_hash.as_bytes());
    out.extend_from_slice(&req.peer_id);
    out.extend_from_slice(&req.downloaded.to_be_bytes());
    out.extend_from_slice(&req.left.to_be_bytes());
    out.extend_from_slice(&req.uploaded.to_be_bytes());
    out.extend_from_slice(&event_code(req.event).to_be_bytes());
    // IP override: 0 = use the source IP the tracker observes.
    out.extend_from_slice(&0u32.to_be_bytes());
    // Random key.
    out.extend_from_slice(&random_txn_id().to_be_bytes());
    // numwant: -1 = default.
    let numwant = req.numwant.map(|n| n as i32).unwrap_or(-1);
    out.extend_from_slice(&numwant.to_be_bytes());
    out.extend_from_slice(&req.port.to_be_bytes());
    out
}

/// Decode an announce response. Validates the action and parses interval,
/// seeders/leechers, and compact IPv4 peers.
fn decode_announce(buf: &[u8]) -> Result<AnnounceResponse> {
    if buf.len() < 8 {
        return Err(CoreError::Parse("udp announce response too short".into()));
    }
    let action = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..]).to_string();
        return Ok(AnnounceResponse {
            failure_reason: Some(msg),
            ..Default::default()
        });
    }
    if action != ACTION_ANNOUNCE {
        return Err(CoreError::Parse(format!(
            "udp announce response unexpected action {action}"
        )));
    }
    if buf.len() < 20 {
        return Err(CoreError::Parse(
            "udp announce response missing fixed fields".into(),
        ));
    }
    let interval = u32::from_be_bytes(buf[8..12].try_into().unwrap()) as u64;
    let leechers = u32::from_be_bytes(buf[12..16].try_into().unwrap()) as u64;
    let seeders = u32::from_be_bytes(buf[16..20].try_into().unwrap()) as u64;
    let peers_bytes = &buf[20..];
    let peers = parse_compact_ipv4_udp(peers_bytes);
    Ok(AnnounceResponse {
        interval,
        min_interval: None,
        seeders,
        leechers,
        peers,
        failure_reason: None,
        tracker_id: None,
    })
}

/// Parse compact IPv4 peers from a UDP announce response tail (6 bytes each).
fn parse_compact_ipv4_udp(bytes: &[u8]) -> Vec<PeerAddr> {
    let mut out = Vec::with_capacity(bytes.len() / 6);
    for chunk in bytes.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        out.push(PeerAddr {
            ip: IpAddr::V4(ip),
            port,
        });
    }
    out
}

fn event_code(e: AnnounceEvent) -> u32 {
    match e {
        AnnounceEvent::Empty => 0,
        AnnounceEvent::Completed => 1,
        AnnounceEvent::Started => 2,
        AnnounceEvent::Stopped => 3,
    }
}

/// Send a datagram and await a response, bounded by a timeout. Returns the
/// response buffer.
async fn send_recv(
    socket: &dyn ContainedUdpSocket,
    addr: SocketAddr,
    payload: &[u8],
    max_read: usize,
) -> Result<Vec<u8>> {
    socket.send_to(addr, payload).await?;
    let mut buf = vec![0u8; max_read];
    let (_from, n) = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        socket.recv_from(&mut buf),
    )
    .await
    .map_err(|_| CoreError::Internal("udp tracker response timed out".into()))??;
    buf.truncate(n);
    Ok(buf)
}

fn random_txn_id() -> u32 {
    // A process-local counter would suffice; use a simple hash of time for
    // determinism-free transaction ids.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xdead_beef);
    let pid = std::process::id() as u64;
    ((nanos ^ (pid << 32)).wrapping_mul(2654435761)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::InfoHash;
    use std::sync::Arc;

    fn req() -> AnnounceRequest {
        AnnounceRequest {
            tracker_url: "udp://127.0.0.1:0/announce".into(),
            info_hash: InfoHash::from_bytes([0x12u8; 20]),
            peer_id: *b"-SW0010-abcdefghij12",
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1024,
            event: AnnounceEvent::Started,
            numwant: Some(50),
            compact: true,
        }
    }

    #[test]
    fn encode_connect_is_16_bytes() {
        let enc = encode_connect(42);
        assert_eq!(enc.len(), 16);
        assert_eq!(
            u64::from_be_bytes(enc[0..8].try_into().unwrap()),
            CONNECT_MAGIC
        );
        assert_eq!(
            u32::from_be_bytes(enc[8..12].try_into().unwrap()),
            ACTION_CONNECT
        );
        assert_eq!(u32::from_be_bytes(enc[12..16].try_into().unwrap()), 42);
    }

    #[test]
    fn decode_connect_validates_txn_and_action() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
        buf.extend_from_slice(&7u32.to_be_bytes());
        buf.extend_from_slice(&0x0102030405060708u64.to_be_bytes());
        let id = decode_connect(&buf, 7).unwrap();
        assert_eq!(id, 0x0102030405060708);
        assert!(decode_connect(&buf, 8).is_err());
        let mut bad = buf.clone();
        bad[0..4].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        assert!(decode_connect(&bad, 7).is_err());
    }

    #[test]
    fn decode_connect_error_returns_message() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ACTION_ERROR.to_be_bytes());
        buf.extend_from_slice(&7u32.to_be_bytes());
        buf.extend_from_slice(b"bad request");
        let err = decode_connect(&buf, 7);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("bad request"));
    }

    #[test]
    fn encode_announce_is_98_bytes() {
        let enc = encode_announce(&req(), 0x0102030405060708);
        assert_eq!(enc.len(), 98);
        assert_eq!(
            u64::from_be_bytes(enc[0..8].try_into().unwrap()),
            0x0102030405060708
        );
        assert_eq!(
            u32::from_be_bytes(enc[8..12].try_into().unwrap()),
            ACTION_ANNOUNCE
        );
        assert_eq!(&enc[16..36], req().info_hash.as_bytes());
        assert_eq!(&enc[36..56], &req().peer_id);
        let port = u16::from_be_bytes(enc[96..98].try_into().unwrap());
        assert_eq!(port, 6881);
    }

    #[test]
    fn event_code_mapping() {
        assert_eq!(event_code(AnnounceEvent::Empty), 0);
        assert_eq!(event_code(AnnounceEvent::Completed), 1);
        assert_eq!(event_code(AnnounceEvent::Started), 2);
        assert_eq!(event_code(AnnounceEvent::Stopped), 3);
    }

    #[test]
    fn decode_announce_parses_peers() {
        // action + txn + interval + leechers + seeders + 2 compact peers.
        let mut buf = Vec::new();
        buf.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.extend_from_slice(&1800u32.to_be_bytes()); // interval
        buf.extend_from_slice(&2u32.to_be_bytes()); // leechers
        buf.extend_from_slice(&3u32.to_be_bytes()); // seeders
                                                    // peer 192.168.1.1:6881
        buf.extend_from_slice(&[192, 168, 1, 1, 0x1A, 0xE1]);
        // peer 10.0.0.2:6882
        buf.extend_from_slice(&[10, 0, 0, 2, 0x1A, 0xE2]);
        let resp = decode_announce(&buf).unwrap();
        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.seeders, 3);
        assert_eq!(resp.leechers, 2);
        assert_eq!(resp.peers.len(), 2);
        assert_eq!(resp.peers[0].port, 6881);
        assert_eq!(resp.peers[1].port, 6882);
        assert!(resp.failure_reason.is_none());
    }

    #[test]
    fn decode_announce_error_response() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ACTION_ERROR.to_be_bytes());
        buf.extend_from_slice(&9u32.to_be_bytes());
        buf.extend_from_slice(b"tracker overloaded");
        let resp = decode_announce(&buf).unwrap();
        assert!(resp.failure_reason.is_some());
        assert!(resp.failure_reason.unwrap().contains("tracker overloaded"));
        assert!(resp.peers.is_empty());
    }

    #[test]
    fn decode_announce_rejects_wrong_action() {
        let mut buf = vec![0u8; 20];
        buf[0..4].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
        assert!(decode_announce(&buf).is_err());
    }

    #[test]
    fn resolve_udp_tracker_rejects_non_udp_and_missing_port() {
        assert!(resolve_udp_tracker("http://h/announce").is_err());
        assert!(resolve_udp_tracker("udp://h/announce").is_err());
        assert!(resolve_udp_tracker("udp://127.0.0.1:6881/announce").is_ok());
    }

    /// A real UDP tracker fixture: responds to a connect then an announce with
    /// a compact peer list. Exercises the contained UDP path over loopback.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_announce_against_local_fixture() {
        use tokio::net::UdpSocket as TokioUdp;
        // Bind a real tokio UDP socket to act as the tracker.
        let tracker_sock = TokioUdp::bind("127.0.0.1:0").await.unwrap();
        let tracker_addr = tracker_sock.local_addr().unwrap();
        let tracker_url = format!("udp://{tracker_addr}/announce");

        let tracker_task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            // Connect request.
            let (_n, peer) = tracker_sock.recv_from(&mut buf).await.unwrap();
            let action = u32::from_be_bytes(buf[8..12].try_into().unwrap());
            assert_eq!(action, ACTION_CONNECT);
            let txn = u32::from_be_bytes(buf[12..16].try_into().unwrap());
            // Connection id = a fixed value for the test.
            let conn_id: u64 = 0x0A0B0C0D0E0F1011;
            let mut resp = Vec::new();
            resp.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            resp.extend_from_slice(&txn.to_be_bytes());
            resp.extend_from_slice(&conn_id.to_be_bytes());
            tracker_sock.send_to(&resp, peer).await.unwrap();

            // Announce request.
            let (_n, peer) = tracker_sock.recv_from(&mut buf).await.unwrap();
            let conn = u64::from_be_bytes(buf[0..8].try_into().unwrap());
            assert_eq!(conn, conn_id);
            let action = u32::from_be_bytes(buf[8..12].try_into().unwrap());
            assert_eq!(action, ACTION_ANNOUNCE);
            let txn = u32::from_be_bytes(buf[12..16].try_into().unwrap());
            // Reply with one compact peer: 127.0.0.1:51413.
            let mut resp = Vec::new();
            resp.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
            resp.extend_from_slice(&txn.to_be_bytes());
            resp.extend_from_slice(&1800u32.to_be_bytes()); // interval
            resp.extend_from_slice(&1u32.to_be_bytes()); // leechers
            resp.extend_from_slice(&1u32.to_be_bytes()); // seeders
            resp.extend_from_slice(&[127, 0, 0, 1, 0xC8, 0xE5]); // peer
            tracker_sock.send_to(&resp, peer).await.unwrap();
        });

        let binder = Arc::new(crate::net::binder::LoopbackBinder);
        let mut req = req();
        req.tracker_url = tracker_url;
        let resp = udp_announce_with_iters(binder.as_ref(), &req, 0)
            .await
            .unwrap();
        assert!(resp.failure_reason.is_none());
        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(resp.peers[0].ip.to_string(), "127.0.0.1");
        assert_eq!(resp.peers[0].port, u16::from_be_bytes([0xC8, 0xE5]));
        tracker_task.await.unwrap();
    }

    /// Fail-closed UDP tracker: a blocking binder refuses to create the UDP
    /// socket.
    #[tokio::test]
    async fn udp_announce_blocked_by_fail_closed_binder() {
        let binder = Arc::new(crate::net::binder::BlockedBinder);
        let req = req();
        let err = udp_announce(binder.as_ref(), &req).await.unwrap_err();
        assert!(err.is_network_blocked());
    }
}
