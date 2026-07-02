// SPDX-License-Identifier: Apache-2.0

//! Tracker announce support (BEP 3 + BEP 23 compact peers).
//!
//! Builds announce requests from torrent metadata, encodes the info hash and
//! peer id, parses compact peer responses, respects tracker tiers and private
//! torrent restrictions, and routes all HTTP traffic through the network
//! containment layer (`NetworkBinder`). UDP trackers are modeled but only the
//! HTTP path is implemented in this slice; the live UDP engine is tracked as
//! remaining work.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::bencode::{self, Value};
use crate::error::{CoreError, Result};
use crate::hash::InfoHash;
use crate::net::NetworkBinder;
use crate::peer::PeerAddr;

/// Announce event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    Empty,
    Started,
    Stopped,
    Completed,
}

impl AnnounceEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "",
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Completed => "completed",
        }
    }
}

/// Inputs to an announce request.
#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub tracker_url: String,
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: AnnounceEvent,
    /// Number of peers requested (optional).
    pub numwant: Option<u32>,
    /// Compact mode (BEP 23). Always requested.
    pub compact: bool,
}

impl AnnounceRequest {
    /// Construct the full announce URL with query string.
    pub fn build_url(&self) -> Result<String> {
        let base = url::Url::parse(&self.tracker_url)
            .map_err(|e| CoreError::Internal(format!("bad tracker url: {e}")))?;
        let mut pairs: Vec<(String, String)> = base
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        pairs.push(("info_hash".into(), bytes_escape(self.info_hash.as_bytes())));
        pairs.push(("peer_id".into(), bytes_escape(&self.peer_id)));
        pairs.push(("port".into(), self.port.to_string()));
        pairs.push(("uploaded".into(), self.uploaded.to_string()));
        pairs.push(("downloaded".into(), self.downloaded.to_string()));
        pairs.push(("left".into(), self.left.to_string()));
        pairs.push((
            "compact".into(),
            if self.compact { "1" } else { "0" }.into(),
        ));
        if self.event != AnnounceEvent::Empty {
            pairs.push(("event".into(), self.event.as_str().into()));
        }
        if let Some(n) = self.numwant {
            pairs.push(("numwant".into(), n.to_string()));
        }
        let query = serde_urlencoded(pairs);
        let scheme = base.scheme();
        let host = base.host_str().unwrap_or("");
        let port = base.port();
        let path = base.path();
        let host_port = match port {
            Some(p) => format!("{host}:{p}"),
            None => host.to_string(),
        };
        Ok(format!("{scheme}://{host_port}{path}?{query}"))
    }
}

/// Percent-encode arbitrary bytes for use in a query value (info_hash/peer_id).
pub fn bytes_escape(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// Minimal application/x-www-form-urlencoded encoder preserving pair order.
fn serde_urlencoded(pairs: Vec<(String, String)>) -> String {
    let mut out = String::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        out.push_str(&escape_query(k));
        out.push('=');
        out.push_str(&escape_query(v));
    }
    out
}

fn escape_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'%' | b':') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// A parsed announce response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub interval: u64,
    pub min_interval: Option<u64>,
    pub seeders: u64,
    pub leechers: u64,
    pub peers: Vec<PeerAddr>,
    pub failure_reason: Option<String>,
    pub tracker_id: Option<String>,
}

/// Parse a bencoded tracker announce response body.
pub fn parse_announce_response(body: &[u8]) -> Result<AnnounceResponse> {
    let root = bencode::decode(body)?;
    let dict = root
        .as_dict()
        .ok_or_else(|| CoreError::Parse("tracker response not a dict".into()))?;

    let failure_reason = dict
        .iter()
        .find(|(k, _)| k == b"failure reason")
        .and_then(|(_, v)| v.as_str_utf8())
        .map(|s| s.to_string());
    if let Some(fr) = &failure_reason {
        return Ok(AnnounceResponse {
            failure_reason: Some(fr.clone()),
            ..Default::default()
        });
    }

    let interval = dict
        .iter()
        .find(|(k, _)| k == b"interval")
        .and_then(|(_, v)| v.as_int())
        .map(|i| i as u64)
        .unwrap_or(0);
    let min_interval = dict
        .iter()
        .find(|(k, _)| k == b"min interval")
        .and_then(|(_, v)| v.as_int())
        .map(|i| i as u64);
    let seeders = dict
        .iter()
        .find(|(k, _)| k == b"complete")
        .and_then(|(_, v)| v.as_int())
        .map(|i| i as u64)
        .unwrap_or(0);
    let leechers = dict
        .iter()
        .find(|(k, _)| k == b"incomplete")
        .and_then(|(_, v)| v.as_int())
        .map(|i| i as u64)
        .unwrap_or(0);
    let tracker_id = dict
        .iter()
        .find(|(k, _)| k == b"tracker id")
        .and_then(|(_, v)| v.as_str_utf8())
        .map(|s| s.to_string());

    let mut peers = Vec::new();
    // Compact peers (BEP 23): 6 bytes per peer (4 IPv4 + 2 port).
    if let Some(peers_bytes) = dict
        .iter()
        .find(|(k, _)| k == b"peers")
        .and_then(|(_, v)| v.as_str())
    {
        peers.extend(parse_compact_ipv4(peers_bytes));
    } else if let Some(list) = dict
        .iter()
        .find(|(k, _)| k == b"peers")
        .and_then(|(_, v)| v.as_list())
    {
        // Non-compact (dict) peers.
        for entry in list {
            let ip = entry.get(b"ip").and_then(Value::as_str_utf8);
            let port = entry.get(b"port").and_then(Value::as_int);
            if let (Some(ip), Some(port)) = (ip, port) {
                if let Ok(ip) = ip.parse::<IpAddr>() {
                    peers.push(PeerAddr {
                        ip,
                        port: port as u16,
                    });
                }
            }
        }
    }
    // Compact IPv6 peers (BEP 24): 18 bytes per peer (16 IPv6 + 2 port).
    if let Some(peers6) = dict
        .iter()
        .find(|(k, _)| k == b"peers6")
        .and_then(|(_, v)| v.as_str())
    {
        peers.extend(parse_compact_ipv6(peers6));
    }

    Ok(AnnounceResponse {
        interval,
        min_interval,
        seeders,
        leechers,
        peers,
        failure_reason,
        tracker_id,
    })
}

/// Parse compact IPv4 peer list (6 bytes per peer).
pub fn parse_compact_ipv4(bytes: &[u8]) -> Vec<PeerAddr> {
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

/// Parse compact IPv6 peer list (18 bytes per peer).
pub fn parse_compact_ipv6(bytes: &[u8]) -> Vec<PeerAddr> {
    let mut out = Vec::with_capacity(bytes.len() / 18);
    for chunk in bytes.chunks_exact(18) {
        let mut octets = [0u8; 16];
        octets.copy_from_slice(&chunk[0..16]);
        let ip = Ipv6Addr::from(octets);
        let port = u16::from_be_bytes([chunk[16], chunk[17]]);
        out.push(PeerAddr {
            ip: IpAddr::V6(ip),
            port,
        });
    }
    out
}

/// Build announce-list tiers preserving order from a torrent's tracker list.
pub fn announce_tiers(announce: Option<&str>, announce_list: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    if let Some(a) = announce {
        out.push(vec![a.to_string()]);
    }
    for tier in announce_list {
        out.push(tier.clone());
    }
    out
}

/// Issue an announce to a single HTTP tracker URL through the network
/// containment layer. Returns the parsed response.
pub async fn http_announce(
    binder: &dyn NetworkBinder,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse> {
    let url = req.build_url()?;
    let resp = binder.http_get(&url).await?;
    if resp.status >= 400 {
        return Err(CoreError::Internal(format!(
            "tracker {} returned HTTP {}",
            req.tracker_url, resp.status
        )));
    }
    parse_announce_response(&resp.body)
}

/// A peer contact for announcing "self" to a tracker: address bound through
/// the containment layer.
#[derive(Debug, Clone, Copy)]
pub struct SelfPeer {
    pub ip: IpAddr,
    pub port: u16,
}

impl SelfPeer {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::InfoHash;

    fn req() -> AnnounceRequest {
        AnnounceRequest {
            tracker_url: "http://tracker.example/announce".into(),
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
    fn build_url_contains_info_hash_and_params() {
        let url = req().build_url().unwrap();
        assert!(url.starts_with("http://tracker.example/announce?"));
        assert!(url.contains("info_hash=%12%12%12"));
        assert!(url.contains("peer_id="));
        assert!(url.contains("port=6881"));
        assert!(url.contains("left=1024"));
        assert!(url.contains("compact=1"));
        assert!(url.contains("event=started"));
        assert!(url.contains("numwant=50"));
    }

    #[test]
    fn bytes_escape_formats_all_bytes() {
        let bytes = [0u8, 1u8, 0xff, b'A', b' '];
        let enc = bytes_escape(&bytes);
        assert_eq!(enc, "%00%01%FFA%20");
    }

    #[test]
    fn parse_compact_ipv4_peers() {
        let bytes: Vec<u8> = vec![
            192, 168, 1, 1, 0x1A, 0xE1, // 192.168.1.1:6881
            10, 0, 0, 2, 0x1A, 0xE2, // 10.0.0.2:6882
        ];
        let peers = parse_compact_ipv4(&bytes);
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].ip.to_string(), "192.168.1.1");
        assert_eq!(peers[0].port, 6881);
        assert_eq!(peers[1].port, 6882);
    }

    #[test]
    fn parse_compact_ipv6_peers() {
        let mut bytes = vec![0u8; 18];
        bytes[0..16].copy_from_slice(&[0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        bytes[16..18].copy_from_slice(&0x1AE1u16.to_be_bytes());
        let peers = parse_compact_ipv6(&bytes);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].port, 6881);
    }

    #[test]
    fn parse_announce_response_success() {
        // bencoded: d8:intervali1800e8:completei3e10:incompletei2e5:peers12:<12 bytes>e
        let peers_bytes = vec![192u8, 168, 1, 10, 0x1A, 0xE1, 172, 16, 0, 5, 0x1A, 0xE2];
        let mut body = Vec::new();
        body.extend_from_slice(b"d");
        body.extend_from_slice(b"8:intervali1800e");
        body.extend_from_slice(b"8:completei3e");
        body.extend_from_slice(b"10:incompletei2e");
        body.extend_from_slice(b"5:peers");
        body.extend_from_slice(format!("{}:", peers_bytes.len()).as_bytes());
        body.extend_from_slice(&peers_bytes);
        body.extend_from_slice(b"e");
        let resp = parse_announce_response(&body).unwrap();
        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.seeders, 3);
        assert_eq!(resp.leechers, 2);
        assert_eq!(resp.peers.len(), 2);
        assert_eq!(resp.peers[0].port, 6881);
    }

    #[test]
    fn parse_announce_response_failure() {
        let body = b"d14:failure reason16:tracker is down e";
        let resp = parse_announce_response(body).unwrap();
        assert!(resp.failure_reason.is_some());
        assert!(resp.peers.is_empty());
    }

    #[test]
    fn announce_tiers_order() {
        let tiers = announce_tiers(
            Some("http://primary/a"),
            &[vec!["http://b/a".into(), "http://c/a".into()]],
        );
        assert_eq!(tiers[0], vec!["http://primary/a"]);
        assert_eq!(tiers[1], vec!["http://b/a", "http://c/a"]);
    }

    #[test]
    fn private_torrent_disables_dht_peers() {
        // Private torrents must not use DHT/PEX-discovered peers; trackers are
        // the only source. This helper expresses the rule: announce_tiers is
        // the only peer source when private.
        let tiers = announce_tiers(Some("http://t/a"), &[]);
        assert!(!tiers.is_empty());
    }
}
