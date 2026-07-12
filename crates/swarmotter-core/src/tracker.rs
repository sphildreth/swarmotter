// SPDX-License-Identifier: Apache-2.0

//! Tracker announce support (BEP 3 + BEP 23 compact peers).
//!
//! Builds announce requests from torrent metadata, encodes the info hash and
//! peer id, parses compact peer responses, respects tracker tiers and private
//! torrent restrictions, and routes HTTP announce/scrape through the contained
//! HTTP client over `NetworkBinder`. Live UDP announce is implemented in
//! `udp_tracker`; UDP scrape is explicitly unsupported.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::bencode::{self, Value};
use crate::error::{CoreError, Result};
use crate::hash::InfoHash;
use crate::net::{ContainedHttpClient, NetworkBinder};
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

/// Last-success counts returned by a BEP 48 tracker scrape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrapeCounts {
    pub seeders: u64,
    pub leechers: u64,
    pub downloads: u64,
}

/// A scrape URL is not universally derivable. Unsupported tracker schemes or
/// path shapes are distinct from a failed supported request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScrapeOutcome {
    Unsupported,
    Success(HashMap<InfoHash, ScrapeCounts>),
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
    if !announce_list.is_empty() {
        return announce_list
            .iter()
            .filter(|tier| !tier.is_empty())
            .cloned()
            .collect();
    }
    announce
        .map(|url| vec![vec![url.to_string()]])
        .unwrap_or_default()
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

/// Derive a BEP 48 HTTP/HTTPS scrape URL and append exactly one binary
/// `info_hash` query pair for each distinct requested hash, in input order.
/// Existing form-decoded `info_hash` pairs are removed while unrelated raw
/// query components retain their spelling and order.
pub fn build_scrape_url(tracker_url: &str, info_hashes: &[InfoHash]) -> Result<Option<String>> {
    if info_hashes.is_empty() {
        return Err(CoreError::InvalidArgument(
            "tracker scrape requires at least one info hash".into(),
        ));
    }
    let mut distinct = HashSet::with_capacity(info_hashes.len());
    for hash in info_hashes {
        if !distinct.insert(*hash) {
            return Err(CoreError::InvalidArgument(format!(
                "tracker scrape contains duplicate info hash {}",
                hash.to_hex()
            )));
        }
    }

    let mut url = url::Url::parse(tracker_url)
        .map_err(|error| CoreError::InvalidArgument(format!("bad tracker URL: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Ok(None);
    }
    let path = url.path();
    let (directory, final_segment) = path.rsplit_once('/').unwrap_or(("", path));
    let Some(suffix) = final_segment.strip_prefix("announce") else {
        return Ok(None);
    };
    let scrape_path = format!("{directory}/scrape{suffix}");
    url.set_path(&scrape_path);
    url.set_fragment(None);

    let mut query_components = url
        .query()
        .map(|query| {
            query
                .split('&')
                .filter(|component| !raw_query_key_is_info_hash(component))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    query_components.extend(
        info_hashes
            .iter()
            .map(|hash| format!("info_hash={}", bytes_escape(hash.as_bytes()))),
    );
    url.set_query(Some(&query_components.join("&")));
    Ok(Some(url.to_string()))
}

fn raw_query_key_is_info_hash(component: &str) -> bool {
    let raw_key = component
        .split_once('=')
        .map(|(key, _)| key)
        .unwrap_or(component);
    url::form_urlencoded::parse(raw_key.as_bytes())
        .next()
        .is_some_and(|(key, _)| key == "info_hash")
}

/// Parse a bounded BEP 48 response for every requested hash. The entire
/// attempt fails before returning any counts if one requested entry is absent
/// or malformed.
pub fn parse_scrape_response(
    body: &[u8],
    info_hashes: &[InfoHash],
) -> Result<HashMap<InfoHash, ScrapeCounts>> {
    if info_hashes.is_empty() {
        return Err(CoreError::InvalidArgument(
            "tracker scrape requires at least one info hash".into(),
        ));
    }
    let root = bencode::decode(body)?;
    let root = root
        .as_dict()
        .ok_or_else(|| CoreError::Parse("tracker scrape response is not a dictionary".into()))?;
    if let Some(reason) = root
        .iter()
        .find(|(key, _)| key == b"failure reason")
        .and_then(|(_, value)| value.as_str_utf8())
    {
        return Err(CoreError::Parse(format!(
            "tracker scrape failure: {reason}"
        )));
    }
    let files = root
        .iter()
        .find(|(key, _)| key == b"files")
        .and_then(|(_, value)| value.as_dict())
        .ok_or_else(|| {
            CoreError::Parse("tracker scrape response has no files dictionary".into())
        })?;

    let mut parsed = HashMap::with_capacity(info_hashes.len());
    for hash in info_hashes {
        let entry = files
            .iter()
            .find(|(key, _)| key.as_slice() == hash.as_bytes())
            .and_then(|(_, value)| value.as_dict())
            .ok_or_else(|| {
                CoreError::Parse(format!(
                    "tracker scrape response has no dictionary for info hash {}",
                    hash.to_hex()
                ))
            })?;
        let counts = ScrapeCounts {
            seeders: scrape_nonnegative_count(entry, b"complete", hash)?,
            leechers: scrape_nonnegative_count(entry, b"incomplete", hash)?,
            downloads: scrape_nonnegative_count(entry, b"downloaded", hash)?,
        };
        parsed.insert(*hash, counts);
    }
    Ok(parsed)
}

fn scrape_nonnegative_count(
    entry: &[(Vec<u8>, Value)],
    field: &[u8],
    hash: &InfoHash,
) -> Result<u64> {
    let value = entry
        .iter()
        .find(|(key, _)| key.as_slice() == field)
        .and_then(|(_, value)| value.as_int())
        .ok_or_else(|| {
            CoreError::Parse(format!(
                "tracker scrape field {} is missing or not an integer for info hash {}",
                String::from_utf8_lossy(field),
                hash.to_hex()
            ))
        })?;
    u64::try_from(value).map_err(|_| {
        CoreError::Parse(format!(
            "tracker scrape field {} is negative for info hash {}",
            String::from_utf8_lossy(field),
            hash.to_hex()
        ))
    })
}

/// Execute a supported HTTP/HTTPS scrape through the contained client.
pub async fn http_scrape(
    binder: &dyn NetworkBinder,
    tracker_url: &str,
    info_hashes: &[InfoHash],
) -> Result<ScrapeOutcome> {
    let client = ContainedHttpClient::new(binder);
    http_scrape_with_client(&client, tracker_url, info_hashes).await
}

async fn http_scrape_with_client<B: NetworkBinder + ?Sized>(
    client: &ContainedHttpClient<'_, B>,
    tracker_url: &str,
    info_hashes: &[InfoHash],
) -> Result<ScrapeOutcome> {
    let Some(scrape_url) = build_scrape_url(tracker_url, info_hashes)? else {
        return Ok(ScrapeOutcome::Unsupported);
    };
    let response = client.get_tracker(&scrape_url).await?;
    let counts = parse_scrape_response(&response.body, info_hashes)?;
    Ok(ScrapeOutcome::Success(counts))
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
    use crate::net::{ContainedUdpSocket, PeerListener};
    use async_trait::async_trait;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    fn build_url_https_scheme_preserved() {
        let mut r = req();
        r.tracker_url = "https://tracker.example:8443/announce".into();
        let url = r.build_url().unwrap();
        assert!(url.starts_with("https://tracker.example:8443/announce?"));
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
    fn announce_list_tiers_take_precedence_over_announce() {
        let tiers = announce_tiers(
            Some("http://primary/a"),
            &[vec!["http://b/a".into(), "http://c/a".into()]],
        );
        assert_eq!(tiers, vec![vec!["http://b/a", "http://c/a"]]);
    }

    #[test]
    fn announce_is_used_when_announce_list_is_absent() {
        assert_eq!(
            announce_tiers(Some("http://primary/a"), &[]),
            vec![vec!["http://primary/a"]]
        );
    }

    #[test]
    fn private_torrent_disables_dht_peers() {
        // Private torrents must not use DHT/PEX-discovered peers; trackers are
        // the only source. This helper expresses the rule: announce_tiers is
        // the only peer source when private.
        let tiers = announce_tiers(Some("http://t/a"), &[]);
        assert!(!tiers.is_empty());
    }

    fn push_bstring(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(value.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(value);
    }

    fn scrape_body(entries: &[(InfoHash, i64, i64, i64)]) -> Vec<u8> {
        let mut body = b"d5:filesd".to_vec();
        for (hash, complete, incomplete, downloaded) in entries {
            push_bstring(&mut body, hash.as_bytes());
            body.push(b'd');
            body.extend_from_slice(format!("8:completei{complete}e").as_bytes());
            body.extend_from_slice(format!("10:downloadedi{downloaded}e").as_bytes());
            body.extend_from_slice(format!("10:incompletei{incomplete}e").as_bytes());
            body.push(b'e');
        }
        body.extend_from_slice(b"ee");
        body
    }

    #[test]
    fn scrape_url_derivation_preserves_raw_query_and_orders_binary_hashes() {
        let first = InfoHash::from_bytes([
            0x00, 0x20, 0x2f, 0x3f, 0xff, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ]);
        let second = InfoHash::from_bytes([0x22; 20]);
        let url = build_scrape_url(
            "https://tracker.test/nested/path/announce.php?pass=%2f+Keep&info_hash=old&%69nfo_hash=old2&x=1#ignored",
            &[first, second],
        )
        .unwrap()
        .unwrap();
        assert!(url.starts_with("https://tracker.test/nested/path/scrape.php?"));
        assert!(!url.contains('#'));
        let query = url::Url::parse(&url).unwrap().query().unwrap().to_string();
        assert_eq!(
            query,
            format!(
                "pass=%2f+Keep&x=1&info_hash={}&info_hash={}",
                bytes_escape(first.as_bytes()),
                bytes_escape(second.as_bytes())
            )
        );
    }

    #[test]
    fn scrape_url_derivation_handles_exact_suffix_relative_directory_and_unsupported() {
        let hash = InfoHash::from_bytes([0x11; 20]);
        assert_eq!(
            build_scrape_url("http://tracker.test/announce", &[hash])
                .unwrap()
                .unwrap(),
            format!(
                "http://tracker.test/scrape?info_hash={}",
                bytes_escape(hash.as_bytes())
            )
        );
        assert!(
            build_scrape_url("http://tracker.test/a/b/announce-v2", &[hash])
                .unwrap()
                .unwrap()
                .contains("/a/b/scrape-v2?")
        );
        for unsupported in [
            "http://tracker.test/a/not-announce",
            "http://tracker.test/a/Announce",
            "udp://tracker.test:6969/announce",
        ] {
            assert_eq!(
                build_scrape_url(unsupported, &[hash]).unwrap(),
                None,
                "{unsupported}"
            );
        }
        assert!(build_scrape_url("http://tracker.test/announce", &[]).is_err());
        assert!(build_scrape_url("http://tracker.test/announce", &[hash, hash]).is_err());
    }

    #[test]
    fn scrape_parser_requires_every_exact_hash_and_all_nonnegative_counts() {
        let first = InfoHash::from_bytes([0x31; 20]);
        let second = InfoHash::from_bytes([0x32; 20]);
        let unrelated = InfoHash::from_bytes([0x77; 20]);
        let parsed = parse_scrape_response(
            &scrape_body(&[(unrelated, 9, 8, 7), (first, 1, 2, 3), (second, 4, 5, 6)]),
            &[first, second],
        )
        .unwrap();
        assert_eq!(
            parsed[&first],
            ScrapeCounts {
                seeders: 1,
                leechers: 2,
                downloads: 3
            }
        );
        assert_eq!(parsed[&second].downloads, 6);

        assert!(
            parse_scrape_response(&scrape_body(&[(first, 1, 2, 3)]), &[first, second]).is_err()
        );
        assert!(parse_scrape_response(&scrape_body(&[(first, -1, 2, 3)]), &[first]).is_err());
        assert!(parse_scrape_response(b"d5:filesd20:11111111111111111111i1eee", &[first]).is_err());
        assert!(parse_scrape_response(b"le", &[first]).is_err());
        assert!(parse_scrape_response(b"d5:filesdee", &[first]).is_err());
        assert!(parse_scrape_response(b"d14:failure reason13:not availablee", &[first]).is_err());
    }

    #[test]
    fn scrape_parser_rejects_missing_wrong_and_out_of_range_matching_fields() {
        let hash = InfoHash::from_bytes([0x41; 20]);
        let mut missing = b"d5:filesd".to_vec();
        push_bstring(&mut missing, hash.as_bytes());
        missing.extend_from_slice(b"d8:completei1e10:incompletei2eeee");
        assert!(parse_scrape_response(&missing, &[hash]).is_err());

        let mut wrong = b"d5:filesd".to_vec();
        push_bstring(&mut wrong, hash.as_bytes());
        wrong.extend_from_slice(b"d8:complete3:bad10:downloadedi3e10:incompletei2eeee");
        assert!(parse_scrape_response(&wrong, &[hash]).is_err());

        let mut overflow = b"d5:filesd".to_vec();
        push_bstring(&mut overflow, hash.as_bytes());
        overflow.extend_from_slice(
            b"d8:completei9223372036854775808e10:downloadedi3e10:incompletei2eeee",
        );
        assert!(parse_scrape_response(&overflow, &[hash]).is_err());
    }

    struct ScrapeBinder {
        address: SocketAddr,
        resolves: AtomicUsize,
        connects: AtomicUsize,
    }

    impl ScrapeBinder {
        fn new(address: SocketAddr) -> Self {
            Self {
                address,
                resolves: AtomicUsize::new(0),
                connects: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl NetworkBinder for ScrapeBinder {
        async fn connect_peer(&self, _addr: SocketAddr) -> Result<tokio::net::TcpStream> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            tokio::net::TcpStream::connect(self.address)
                .await
                .map_err(CoreError::from)
        }

        async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
            self.resolves.fetch_add(1, Ordering::SeqCst);
            Ok(self.address)
        }

        async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
            Err(CoreError::Internal("unused in scrape fixture".into()))
        }

        async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
            Err(CoreError::Internal("unused in scrape fixture".into()))
        }

        fn traffic_allowed(&self) -> bool {
            true
        }
    }

    async fn read_http_request<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while request.windows(4).all(|window| window != b"\r\n\r\n") {
            let count = stream.read(&mut chunk).await.unwrap();
            if count == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..count]);
        }
        request
    }

    #[tokio::test]
    async fn scrape_unsupported_and_udp_make_no_binder_calls() {
        let hash = InfoHash::from_bytes([0x51; 20]);
        let binder = ScrapeBinder::new("127.0.0.1:9".parse().unwrap());
        for tracker_url in [
            "http://tracker.test/not-supported",
            "udp://tracker.test:6969/announce",
        ] {
            assert_eq!(
                http_scrape(&binder, tracker_url, &[hash]).await.unwrap(),
                ScrapeOutcome::Unsupported
            );
        }
        assert_eq!(binder.resolves.load(Ordering::SeqCst), 0);
        assert_eq!(binder.connects.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn contained_http_scrape_returns_only_exact_matching_counts() {
        let hash = InfoHash::from_bytes([0x52; 20]);
        let body = scrape_body(&[(hash, 7, 8, 9)]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let expected_body = body.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = String::from_utf8(read_http_request(&mut stream).await).unwrap();
            assert!(request.starts_with("GET /scrape?info_hash="));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                expected_body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&expected_body).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let binder = ScrapeBinder::new(address);
        let outcome = http_scrape(&binder, "http://tracker.test/announce", &[hash])
            .await
            .unwrap();
        let ScrapeOutcome::Success(counts) = outcome else {
            panic!("supported HTTP scrape was reported unsupported");
        };
        assert_eq!(counts[&hash].seeders, 7);
        assert_eq!(counts[&hash].leechers, 8);
        assert_eq!(counts[&hash].downloads, 9);
        assert_eq!(binder.resolves.load(Ordering::SeqCst), 1);
        assert_eq!(binder.connects.load(Ordering::SeqCst), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn injected_trust_https_scrape_uses_the_same_contained_client() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let hash = InfoHash::from_bytes([0x53; 20]);
        let body = scrape_body(&[(hash, 10, 11, 12)]);
        let certified = rcgen::generate_simple_self_signed(vec!["secure.test".into()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key =
            rustls::pki_types::PrivateKeyDer::try_from(certified.key_pair.serialize_der()).unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let expected_body = body.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let request = String::from_utf8(read_http_request(&mut stream).await).unwrap();
            assert!(request.starts_with("GET /scrape?info_hash="));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                expected_body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&expected_body).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let tls_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let binder = ScrapeBinder::new(address);
        let client = ContainedHttpClient::with_tls_config(&binder, tls_config);
        let outcome = http_scrape_with_client(&client, "https://secure.test:443/announce", &[hash])
            .await
            .unwrap();
        let ScrapeOutcome::Success(counts) = outcome else {
            panic!("supported HTTPS scrape was reported unsupported");
        };
        assert_eq!(counts[&hash].downloads, 12);
        assert_eq!(binder.resolves.load(Ordering::SeqCst), 1);
        assert_eq!(binder.connects.load(Ordering::SeqCst), 1);
        server.await.unwrap();
    }
}
