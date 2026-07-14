// SPDX-License-Identifier: Apache-2.0

//! Magnet URI parsing.
//!
//! Supports the `magnet:` scheme with v1 `btih` and BEP 52 v2 `btmh` exact
//! topics, plus `dn` (display name), `tr` (trackers), `xl`/`ws`, BEP 53 `so`
//! file selection, and literal `x.pe` direct peers where present. A v2 exact
//! topic is represented by a full [`V2InfoHash`], never coerced into a v1
//! [`InfoHash`]. Malformed or ambiguous magnets produce typed errors.

use crate::error::{CoreError, Result};
use crate::hash::{InfoHash, TorrentIdentity, V2InfoHash};
use crate::meta::MAX_TORRENT_FILES;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use url::Url;

/// The largest number of distinct file indices accepted from BEP 53 `so`.
///
/// A valid metainfo file tree is bounded by the same limit, so this prevents a
/// magnet URI from allocating more selection state than a torrent can use.
pub const MAX_MAGNET_SELECT_ONLY_INDICES: usize = MAX_TORRENT_FILES;

/// The largest number of direct peer endpoints accepted from `x.pe` fields.
///
/// Direct peers are hints, not an alternative network path. Keeping the set
/// bounded makes malformed or hostile magnet URIs cheap to reject before any
/// torrent traffic is attempted.
pub const MAX_MAGNET_DIRECT_PEERS: usize = 256;

/// An IP-literal direct peer supplied by a magnet `x.pe` field.
///
/// Hostnames are intentionally not represented: resolving one here could
/// create an unconstrained DNS path. The daemon converts this value to a peer
/// candidate only after its contained network binder is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MagnetDirectPeer {
    pub ip: IpAddr,
    pub port: u16,
}

impl MagnetDirectPeer {
    /// Return the concrete socket endpoint represented by this magnet hint.
    pub fn socket_addr(self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
}

/// A parsed magnet link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Magnet {
    /// Authoritative v1, v2, or hybrid identity from the magnet exact topics.
    ///
    /// New magnets always have an explicit value. `Unknown` remains
    /// deserializable only for a legacy persisted value that predates v2
    /// support and should be rejected before it is used for torrent traffic.
    #[serde(default)]
    pub identity: TorrentIdentity,
    /// Display name from `dn` if present.
    pub display_name: Option<String>,
    /// Tracker URLs from `tr` (order preserved).
    pub trackers: Vec<String>,
    /// Exact length (bytes) from `xl` if present and parseable.
    pub exact_length: Option<u64>,
    /// Webseed URLs from `ws` if present.
    pub webseeds: Vec<String>,
    /// Zero-based files explicitly selected by BEP 53 `so`.
    ///
    /// Parsed values are sorted, unique, and bounded. The selection is
    /// validated against the real file list after metadata retrieval.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub select_only_file_indices: Vec<usize>,
    /// IP-literal direct peer hints from `x.pe`.
    ///
    /// Each candidate remains subject to the central contained network path
    /// and normal peer filtering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub direct_peers: Vec<MagnetDirectPeer>,
    /// Raw source magnet string.
    pub raw: String,
}

impl Magnet {
    /// Parse a magnet URI string.
    pub fn parse(s: &str) -> Result<Self> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(CoreError::MalformedMagnet("empty magnet".into()));
        }

        // Parse as a URL to split query pairs robustly. Do not infer a magnet
        // from a different scheme: tracker and peer discovery must only begin
        // from a validated torrent identity.
        let url = Url::parse(trimmed)
            .map_err(|e| CoreError::MalformedMagnet(format!("url parse: {e}")))?;
        if !url.scheme().eq_ignore_ascii_case("magnet") {
            return Err(CoreError::MalformedMagnet(
                "must use the 'magnet:' scheme".into(),
            ));
        }
        if url.query().is_none() {
            return Err(CoreError::MalformedMagnet(
                "magnet URI is missing a query string".into(),
            ));
        }

        let mut v1: Option<InfoHash> = None;
        let mut v2: Option<V2InfoHash> = None;
        let mut display_name: Option<String> = None;
        let mut trackers: Vec<String> = Vec::new();
        let mut exact_length: Option<u64> = None;
        let mut webseeds: Vec<String> = Vec::new();
        let mut select_only_file_indices = BTreeSet::new();
        let mut direct_peers = Vec::new();

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "xt" => match parse_xt(&value)? {
                    ExactTopic::V1(hash) => set_unique_v1(&mut v1, hash)?,
                    ExactTopic::V2(hash) => set_unique_v2(&mut v2, hash)?,
                    ExactTopic::Other => {}
                },
                "dn" => {
                    display_name = Some(value.into_owned());
                }
                "tr" => {
                    trackers.push(value.into_owned());
                }
                "xl" => {
                    exact_length = value.parse::<u64>().ok();
                }
                "ws" => {
                    webseeds.push(value.into_owned());
                }
                "so" => {
                    parse_select_only(&value, &mut select_only_file_indices)?;
                }
                "x.pe" => {
                    let peer = parse_direct_peer(&value)?;
                    if !direct_peers.contains(&peer) {
                        if direct_peers.len() == MAX_MAGNET_DIRECT_PEERS {
                            return Err(CoreError::MalformedMagnet(format!(
                                "magnet contains more than {MAX_MAGNET_DIRECT_PEERS} x.pe direct peers"
                            )));
                        }
                        direct_peers.push(peer);
                    }
                }
                _ => {}
            }
        }

        let identity = match (v1, v2) {
            (Some(v1), Some(v2)) => TorrentIdentity::hybrid(v1, v2),
            (Some(v1), None) => TorrentIdentity::v1(v1),
            (None, Some(v2)) => TorrentIdentity::v2(v2),
            (None, None) => {
                return Err(CoreError::MalformedMagnet(
                    "missing a supported 'xt' torrent identity (urn:btih or urn:btmh)".into(),
                ));
            }
        };

        Ok(Magnet {
            identity,
            display_name,
            trackers,
            exact_length,
            webseeds,
            select_only_file_indices: select_only_file_indices.into_iter().collect(),
            direct_peers,
            raw: trimmed.to_string(),
        })
    }

    /// The v1 SHA-1 identity, when the magnet is v1 or hybrid.
    pub const fn v1_info_hash(&self) -> Option<InfoHash> {
        self.identity.v1_info_hash()
    }

    /// The full v2 SHA-256 identity, when the magnet is v2 or hybrid.
    pub const fn v2_info_hash(&self) -> Option<V2InfoHash> {
        self.identity.v2_info_hash()
    }

    /// True when this magnet requires the BEP 52 SHA-256 piece-layer data
    /// plane because it does not contain a v1 compatibility identity.
    pub const fn requires_v2_data_plane(&self) -> bool {
        !self.identity.supports_v1_data_plane()
    }

    /// Construct a canonical magnet URI preserving all explicit identity
    /// components. Hybrid magnets emit both `btih` and `btmh` exact topics.
    pub fn to_uri(&self) -> String {
        let mut topics = Vec::with_capacity(2);
        if let Some(v1) = self.identity.v1_info_hash() {
            topics.push(format!("xt=urn:btih:{}", v1.to_hex()));
        }
        if let Some(v2) = self.identity.v2_info_hash() {
            topics.push(format!("xt=urn:btmh:{}", v2.to_magnet_multihash()));
        }
        let mut s = format!("magnet:?{}", topics.join("&"));
        if let Some(name) = &self.display_name {
            s.push_str(&format!("&dn={}", url_encode(name)));
        }
        if !self.select_only_file_indices.is_empty() {
            s.push_str(&format!(
                "&so={}",
                url_encode(&format_select_only(&self.select_only_file_indices))
            ));
        }
        for tr in &self.trackers {
            s.push_str(&format!("&tr={}", url_encode(tr)));
        }
        for ws in &self.webseeds {
            s.push_str(&format!("&ws={}", url_encode(ws)));
        }
        for peer in &self.direct_peers {
            s.push_str(&format!(
                "&x.pe={}",
                url_encode(&peer.socket_addr().to_string())
            ));
        }
        s
    }
}

/// Validate a persisted or manually-constructed BEP 53 selection once the
/// authoritative file count is known.
///
/// The parser produces sorted, unique indices. Keeping that invariant for
/// durable state makes repeated metadata resolution and canonical URI output
/// deterministic rather than depending on insertion order.
pub fn validate_select_only_file_indices(
    indices: &[usize],
    file_count: usize,
) -> std::result::Result<(), String> {
    if indices.len() > MAX_MAGNET_SELECT_ONLY_INDICES {
        return Err(format!(
            "magnet select-only list exceeds maximum of {MAX_MAGNET_SELECT_ONLY_INDICES} files"
        ));
    }
    if indices.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("magnet select-only file indices must be sorted and unique".into());
    }
    if let Some(index) = indices.iter().copied().find(|index| *index >= file_count) {
        return Err(format!(
            "magnet select-only list contains index {index} outside this torrent's file list"
        ));
    }
    Ok(())
}

/// Validate durable literal direct-peer hints before they enter discovery.
pub fn validate_direct_peers(peers: &[MagnetDirectPeer]) -> std::result::Result<(), String> {
    if peers.len() > MAX_MAGNET_DIRECT_PEERS {
        return Err(format!(
            "magnet direct-peer list exceeds maximum of {MAX_MAGNET_DIRECT_PEERS} peers"
        ));
    }
    if let Some(peer) = peers
        .iter()
        .find(|peer| peer.port == 0 || peer.ip.is_unspecified())
    {
        return Err(format!("invalid magnet direct peer {}", peer.socket_addr()));
    }
    if peers
        .iter()
        .enumerate()
        .any(|(index, peer)| peers[..index].contains(peer))
    {
        return Err("magnet direct-peer list contains duplicates".into());
    }
    Ok(())
}

/// Apply BEP 53's positive selection without overriding a local exclusion.
///
/// A magnet `so` is an explicit allow-list, so files outside it are made
/// unwanted. Existing `false` entries are deliberately retained: an
/// operator's API selection or intake-policy exclusion has higher precedence
/// and must never be re-enabled by an untrusted URI.
pub fn apply_select_only_file_indices(
    priorities: &mut [crate::models::torrent::FilePriority],
    wanted: &mut [bool],
    selected: &[usize],
) {
    for (index, wanted) in wanted.iter_mut().enumerate() {
        if selected.binary_search(&index).is_err() {
            *wanted = false;
            if let Some(priority) = priorities.get_mut(index) {
                *priority = crate::models::torrent::FilePriority::Unwanted;
            }
        }
    }
}

fn parse_select_only(value: &str, selected: &mut BTreeSet<usize>) -> Result<()> {
    if value.is_empty() {
        return Err(CoreError::MalformedMagnet(
            "empty BEP 53 so file selection".into(),
        ));
    }

    for term in value.split(',') {
        if term.is_empty() {
            return Err(CoreError::MalformedMagnet(
                "empty item in BEP 53 so file selection".into(),
            ));
        }
        let (first, last) = match term.split_once('-') {
            Some((start, end)) if !end.contains('-') => (
                parse_select_only_index(start)?,
                parse_select_only_index(end)?,
            ),
            Some(_) => {
                return Err(CoreError::MalformedMagnet(format!(
                    "invalid BEP 53 so range '{term}'"
                )));
            }
            None => {
                let index = parse_select_only_index(term)?;
                (index, index)
            }
        };
        if first > last {
            return Err(CoreError::MalformedMagnet(format!(
                "descending BEP 53 so range '{term}'"
            )));
        }
        let range_len = last
            .checked_sub(first)
            .and_then(|length| length.checked_add(1))
            .ok_or_else(|| CoreError::MalformedMagnet("BEP 53 so range is too large".into()))?;
        if range_len > MAX_MAGNET_SELECT_ONLY_INDICES {
            return Err(CoreError::MalformedMagnet(format!(
                "BEP 53 so range exceeds maximum of {MAX_MAGNET_SELECT_ONLY_INDICES} files"
            )));
        }
        for index in first..=last {
            selected.insert(index);
            if selected.len() > MAX_MAGNET_SELECT_ONLY_INDICES {
                return Err(CoreError::MalformedMagnet(format!(
                    "BEP 53 so selection exceeds maximum of {MAX_MAGNET_SELECT_ONLY_INDICES} files"
                )));
            }
        }
    }
    Ok(())
}

fn parse_select_only_index(value: &str) -> Result<usize> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CoreError::MalformedMagnet(format!(
            "invalid BEP 53 so file index '{value}'"
        )));
    }
    value.parse::<usize>().map_err(|_| {
        CoreError::MalformedMagnet(format!("BEP 53 so file index '{value}' is too large"))
    })
}

fn parse_direct_peer(value: &str) -> Result<MagnetDirectPeer> {
    let endpoint = value.parse::<SocketAddr>().map_err(|_| {
        CoreError::MalformedMagnet(
            "invalid x.pe direct peer; expected an IPv4 literal or [IPv6]:port".into(),
        )
    })?;
    if endpoint.port() == 0 || endpoint.ip().is_unspecified() {
        return Err(CoreError::MalformedMagnet(
            "invalid x.pe direct peer endpoint".into(),
        ));
    }
    Ok(MagnetDirectPeer {
        ip: endpoint.ip(),
        port: endpoint.port(),
    })
}

fn format_select_only(indices: &[usize]) -> String {
    let mut sorted = indices.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut terms = Vec::new();
    let mut start = 0usize;
    while start < sorted.len() {
        let mut end = start;
        while end + 1 < sorted.len() && sorted[end + 1] == sorted[end].saturating_add(1) {
            end += 1;
        }
        if start == end {
            terms.push(sorted[start].to_string());
        } else {
            terms.push(format!("{}-{}", sorted[start], sorted[end]));
        }
        start = end + 1;
    }
    terms.join(",")
}

enum ExactTopic {
    V1(InfoHash),
    V2(V2InfoHash),
    Other,
}

fn parse_xt(value: &str) -> Result<ExactTopic> {
    let mut parts = value.splitn(3, ':');
    let Some(urn) = parts.next() else {
        return Ok(ExactTopic::Other);
    };
    let Some(kind) = parts.next() else {
        return Ok(ExactTopic::Other);
    };
    let Some(body) = parts.next() else {
        return Ok(ExactTopic::Other);
    };
    if !urn.eq_ignore_ascii_case("urn") {
        return Ok(ExactTopic::Other);
    }
    if kind.eq_ignore_ascii_case("btih") {
        // Expected: 40-hex or 32-base32 v1 SHA-1 identity.
        return if body.len() == 40 {
            InfoHash::from_hex(body).map(ExactTopic::V1)
        } else if body.len() == 32 {
            InfoHash::from_base32(body).map(ExactTopic::V1)
        } else {
            Err(CoreError::MalformedMagnet(format!(
                "invalid btih hash length {} in xt",
                body.len()
            )))
        };
    }
    if kind.eq_ignore_ascii_case("btmh") {
        return V2InfoHash::from_magnet_multihash(body)
            .map(ExactTopic::V2)
            .map_err(|error| CoreError::MalformedMagnet(error.to_string()));
    }
    Ok(ExactTopic::Other)
}

fn set_unique_v1(slot: &mut Option<InfoHash>, hash: InfoHash) -> Result<()> {
    match slot {
        Some(existing) if *existing != hash => Err(CoreError::MalformedMagnet(
            "magnet contains conflicting v1 btih identities".into(),
        )),
        Some(_) => Ok(()),
        None => {
            *slot = Some(hash);
            Ok(())
        }
    }
}

fn set_unique_v2(slot: &mut Option<V2InfoHash>, hash: V2InfoHash) -> Result<()> {
    match slot {
        Some(existing) if *existing != hash => Err(CoreError::MalformedMagnet(
            "magnet contains conflicting v2 btmh identities".into(),
        )),
        Some(_) => Ok(()),
        None => {
            *slot = Some(hash);
            Ok(())
        }
    }
}

fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        // Encode reserved/unsafe chars per RFC 3986.
        if b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b':'
                    | b'/'
                    | b'?'
                    | b'#'
                    | b'['
                    | b']'
                    | b'@'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
            )
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known_hex() -> &'static str {
        "dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e"
    }

    fn known_v2_hex() -> &'static str {
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }

    #[test]
    fn parses_minimal_v1_magnet() {
        let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", known_hex())).unwrap();
        assert_eq!(m.v1_info_hash().unwrap().to_hex(), known_hex());
        assert!(m.v2_info_hash().is_none());
        assert!(!m.requires_v2_data_plane());
        assert!(m.display_name.is_none());
        assert!(m.trackers.is_empty());
    }

    #[test]
    fn parses_full_v1_magnet() {
        let uri = format!(
            "magnet:?xt=urn:btih:{}&dn=test%20file&tr=http%3A%2F%2Ftracker.example%2Fannounce&tr=udp%3A%2F%2Ftracker.example%3A1337&xl=1024&ws=http%3A%2F%2Fwebseed.example%2Ffile",
            known_hex()
        );
        let m = Magnet::parse(&uri).unwrap();
        assert_eq!(m.v1_info_hash().unwrap().to_hex(), known_hex());
        assert_eq!(m.display_name.as_deref(), Some("test file"));
        assert_eq!(m.trackers.len(), 2);
        assert_eq!(m.trackers[0], "http://tracker.example/announce");
        assert_eq!(m.trackers[1], "udp://tracker.example:1337");
        assert_eq!(m.exact_length, Some(1024));
        assert_eq!(m.webseeds.len(), 1);
    }

    #[test]
    fn parses_bep53_select_only_and_literal_direct_peers() {
        let uri = format!(
            "magnet:?xt=urn:btih:{}&so=8,0,2,4,6-7&so=7,10&x.pe=192.0.2.25:51413&x.pe=%5B2001:db8::25%5D:51414&x.pe=192.0.2.25:51413",
            known_hex()
        );
        let magnet = Magnet::parse(&uri).unwrap();

        assert_eq!(magnet.select_only_file_indices, vec![0, 2, 4, 6, 7, 8, 10]);
        assert_eq!(magnet.direct_peers.len(), 2);
        assert_eq!(
            magnet.direct_peers[0].socket_addr().to_string(),
            "192.0.2.25:51413"
        );
        assert_eq!(
            magnet.direct_peers[1].socket_addr().to_string(),
            "[2001:db8::25]:51414"
        );
        assert_eq!(
            magnet.to_uri(),
            format!(
                "magnet:?xt=urn:btih:{}&so=0,2,4,6-8,10&x.pe=192.0.2.25:51413&x.pe=[2001:db8::25]:51414",
                known_hex()
            )
        );
    }

    #[test]
    fn rejects_invalid_select_only_and_non_literal_direct_peers() {
        let prefix = format!("magnet:?xt=urn:btih:{}", known_hex());
        for value in ["", "0,,2", "4-2", "0-1-2", "zero", "0-100000"] {
            let uri = format!("{prefix}&so={value}");
            assert!(Magnet::parse(&uri).is_err(), "must reject so={value:?}");
        }
        for value in [
            "peer.example:51413",
            "2001:db8::1:51413",
            "0.0.0.0:51413",
            "192.0.2.25:0",
        ] {
            let uri = format!("{prefix}&x.pe={value}");
            assert!(Magnet::parse(&uri).is_err(), "must reject x.pe={value:?}");
        }
    }

    #[test]
    fn select_only_application_keeps_local_exclusions_authoritative() {
        use crate::models::torrent::FilePriority;

        let mut priorities = vec![
            FilePriority::Normal,
            FilePriority::Unwanted,
            FilePriority::Normal,
            FilePriority::Normal,
        ];
        let mut wanted = vec![true, false, true, true];
        apply_select_only_file_indices(&mut priorities, &mut wanted, &[0, 1, 3]);

        assert_eq!(wanted, vec![true, false, false, true]);
        assert_eq!(
            priorities,
            vec![
                FilePriority::Normal,
                FilePriority::Unwanted,
                FilePriority::Unwanted,
                FilePriority::Normal,
            ]
        );
    }

    #[test]
    fn validates_durable_select_only_indices_against_real_metadata() {
        assert!(validate_select_only_file_indices(&[0, 2], 3).is_ok());
        assert!(validate_select_only_file_indices(&[2, 0], 3).is_err());
        assert!(validate_select_only_file_indices(&[3], 3)
            .unwrap_err()
            .contains("index 3"));
    }

    #[test]
    fn parses_base32_v1_hash() {
        // base32 of known_hex
        let bytes = hex::decode(known_hex()).unwrap();
        let mut b32 = String::new();
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let mut buf: u64 = 0;
        let mut bits: u32 = 0;
        for &b in &bytes {
            buf = (buf << 8) | (b as u64);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                b32.push(A[((buf >> bits) & 0x1f) as usize] as char);
                buf &= (1u64 << bits) - 1;
            }
        }
        if bits > 0 {
            b32.push(A[((buf << (5 - bits)) & 0x1f) as usize] as char);
        }
        let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", b32)).unwrap();
        assert_eq!(m.v1_info_hash().unwrap().to_hex(), known_hex());
    }

    #[test]
    fn parses_v2_btmh_without_coercing_it_to_v1() {
        let uri = format!("magnet:?xt=urn:btmh:1220{}", known_v2_hex());
        let m = Magnet::parse(&uri).unwrap();
        assert!(m.v1_info_hash().is_none());
        assert_eq!(m.v2_info_hash().unwrap().to_hex(), known_v2_hex());
        assert!(m.requires_v2_data_plane());
        assert_eq!(m.to_uri(), uri);
    }

    #[test]
    fn parses_hybrid_magnet_and_preserves_both_exact_topics() {
        let uri = format!(
            "magnet:?xt=urn:btih:{}&xt=urn:btmh:1220{}&dn=hybrid",
            known_hex(),
            known_v2_hex()
        );
        let m = Magnet::parse(&uri).unwrap();
        assert_eq!(m.v1_info_hash().unwrap().to_hex(), known_hex());
        assert_eq!(m.v2_info_hash().unwrap().to_hex(), known_v2_hex());
        assert!(matches!(m.identity, TorrentIdentity::Hybrid { .. }));
        assert_eq!(Magnet::parse(&m.to_uri()).unwrap(), m);
    }

    #[test]
    fn rejects_malformed_or_ambiguous_exact_topics() {
        assert!(Magnet::parse("").is_err());
        assert!(Magnet::parse("http://example.com").is_err());
        assert!(Magnet::parse("magnet:?dn=foo").is_err()); // no xt
        assert!(Magnet::parse("magnet:?xt=urn:btih:tooShort").is_err());
        assert!(Magnet::parse("magnet:?xt=urn:btmh:1220deadbeef").is_err());
        assert!(Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{}&xt=urn:btih:{}",
            known_hex(),
            "00112233445566778899aabbccddeeff00112233"
        ))
        .is_err());
    }

    #[test]
    fn legacy_deserialization_defaults_identity_to_unknown() {
        let magnet: Magnet = serde_json::from_value(serde_json::json!({
            "display_name": "legacy",
            "trackers": [],
            "exact_length": null,
            "webseeds": [],
            "raw": "magnet:?"
        }))
        .unwrap();
        assert_eq!(magnet.identity, TorrentIdentity::Unknown);
    }
}
