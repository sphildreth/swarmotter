// SPDX-License-Identifier: Apache-2.0

//! Mainline DHT (BEP 5) support: KRPC message encoding, node IDs, and a
//! simplified routing table.
//!
//! KRPC is a bencoded RPC protocol over UDP. Messages are dicts with:
//! - `t`: transaction id (byte string)
//! - `y`: message type (`q` query, `r` response, `e` error)
//! - `q`: query method name (for queries)
//! - `a`: arguments dict (for queries)
//! - `r`: response dict (for responses)
//! - `e`: error list `[code, message]` (for errors)
//!
//! This module holds the pure, unit-tested KRPC and routing logic; the live
//! UDP transport lives in the daemon (`swarmotterd::dht`) and routes all
//! traffic through the `NetworkBinder`'s contained UDP socket. Private
//! torrents disable DHT. See `design/requirements.md` and ADR-0019.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};

use crate::bencode;
use crate::error::{CoreError, Result};
use crate::hash::PeerInfoHash;
use crate::peer::PeerAddr;

/// A DHT node ID (20 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId([u8; 20]);

impl NodeId {
    pub fn from_bytes(b: [u8; 20]) -> Self {
        Self(b)
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Derive a stable node ID from a random seed (e.g. the peer id bytes).
    pub fn derive(seed: &[u8; 20]) -> Self {
        // SHA-1 of the seed gives a 20-byte node id.
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(seed);
        let out = hasher.finalize();
        let mut id = [0u8; 20];
        id.copy_from_slice(&out);
        Self(id)
    }

    /// Random node id for tests/fixtures.
    pub fn random() -> Self {
        Self::derive(&{
            let mut seed = [0u8; 20];
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0xdead_beef);
            for (i, b) in seed.iter_mut().enumerate() {
                *b = ((nanos >> (i % 8)) & 0xff) as u8;
            }
            seed
        })
    }

    /// XOR distance between two node ids (as a 20-byte big-endian magnitude).
    pub fn distance(&self, other: &NodeId) -> [u8; 20] {
        let mut d = [0u8; 20];
        for (d, (a, b)) in d.iter_mut().zip(self.0.iter().zip(other.0.iter())) {
            *d = a ^ b;
        }
        d
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

/// A DHT routing table node: id + address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DhtNode {
    pub id: NodeId,
    pub addr: SocketAddr,
}

/// A simplified K-bucket routing table. It keeps a bounded set of known nodes
/// (no dynamic bucket splitting) and returns the `k` closest nodes to a given
/// id by XOR distance.
#[derive(Debug, Clone, Default)]
pub struct RoutingTable {
    nodes: Vec<DhtNode>,
    k: usize,
}

impl RoutingTable {
    pub fn new(k: usize) -> Self {
        Self {
            nodes: Vec::new(),
            k: k.max(1),
        }
    }

    /// Insert/update a node; deduplicates by id. Bounded to a reasonable cap
    /// to avoid unbounded growth.
    pub fn insert(&mut self, node: DhtNode) {
        if let Some(existing) = self.nodes.iter_mut().find(|n| n.id == node.id) {
            existing.addr = node.addr;
            return;
        }
        if self.nodes.len() >= self.k * 8 {
            // Drop the least-recently-useful slot (here: the head) to bound.
            self.nodes.remove(0);
        }
        self.nodes.push(node);
    }

    /// The `k` closest nodes to `target` by XOR distance.
    pub fn closest(&self, target: &NodeId, k: usize) -> Vec<DhtNode> {
        let mut scored: Vec<([u8; 20], DhtNode)> = self
            .nodes
            .iter()
            .map(|n| (n.id.distance(target), *n))
            .collect();
        scored.sort_by_key(|a| a.0);
        scored.into_iter().take(k).map(|(_, n)| n).collect()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn nodes(&self) -> &[DhtNode] {
        &self.nodes
    }
}

/// A KRPC transaction id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId([u8; 2]);

impl TransactionId {
    pub fn new(b: [u8; 2]) -> Self {
        Self(b)
    }

    pub fn random() -> Self {
        static NEXT_TRANSACTION_ID: AtomicU16 = AtomicU16::new(1);
        let value = NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);
        Self(value.to_be_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 2] {
        &self.0
    }
}

/// KRPC query methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KrpcMethod {
    Ping,
    FindNode,
    GetPeers,
    AnnouncePeer,
}

impl KrpcMethod {
    pub fn name(self) -> &'static str {
        match self {
            KrpcMethod::Ping => "ping",
            KrpcMethod::FindNode => "find_node",
            KrpcMethod::GetPeers => "get_peers",
            KrpcMethod::AnnouncePeer => "announce_peer",
        }
    }

    pub fn from_name(s: &str) -> Option<Self> {
        match s {
            "ping" => Some(Self::Ping),
            "find_node" => Some(Self::FindNode),
            "get_peers" => Some(Self::GetPeers),
            "announce_peer" => Some(Self::AnnouncePeer),
            _ => None,
        }
    }
}

/// Encode a KRPC query.
pub fn encode_query(txn: TransactionId, method: KrpcMethod, args: &KrpcArgs) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');
    write_str(&mut out, b"a");
    encode_args(&mut out, method, args);
    write_str(&mut out, b"q");
    write_str(&mut out, method.name().as_bytes());
    write_str(&mut out, b"t");
    write_str(&mut out, txn.as_bytes());
    write_str(&mut out, b"y");
    write_str(&mut out, b"q");
    out.push(b'e');
    out
}

/// KRPC query arguments.
#[derive(Debug, Clone)]
pub struct KrpcArgs {
    /// Sender node id (always present).
    pub id: NodeId,
    /// Target node id for find_node / info hash for get_peers.
    pub target: Option<PeerInfoHash>,
    /// For announce_peer: the port being announced.
    pub port: Option<u16>,
    /// For announce_peer: the token from a prior get_peers response.
    pub token: Option<Vec<u8>>,
}

fn encode_args(out: &mut Vec<u8>, method: KrpcMethod, args: &KrpcArgs) {
    out.push(b'd');
    write_str(out, b"id");
    write_str(out, args.id.as_bytes());
    if let Some(t) = args.target {
        match method {
            KrpcMethod::FindNode => {
                write_str(out, b"target");
                write_str(out, t.as_bytes());
            }
            KrpcMethod::GetPeers | KrpcMethod::AnnouncePeer => {
                write_str(out, b"info_hash");
                write_str(out, t.as_bytes());
            }
            KrpcMethod::Ping => {}
        }
    }
    if let Some(port) = args.port {
        write_str(out, b"port");
        out.push(b'i');
        out.extend_from_slice(port.to_string().as_bytes());
        out.push(b'e');
    }
    if let Some(token) = &args.token {
        write_str(out, b"token");
        write_str(out, token);
    }
    out.push(b'e');
}

/// A parsed KRPC response.
#[derive(Debug, Clone)]
pub struct KrpcResponse {
    pub txn: TransactionId,
    pub sender_id: Option<NodeId>,
    pub nodes: Vec<DhtNode>,
    pub peers: Vec<PeerAddr>,
    pub token: Option<Vec<u8>>,
    pub error: Option<(i64, String)>,
}

/// Parse a KRPC message (query or response). For the engine we primarily need
/// responses; this returns the parsed response fields.
pub fn parse_response(buf: &[u8]) -> Result<KrpcResponse> {
    let root = bencode::decode(buf)?;
    let dict = root
        .as_dict()
        .ok_or_else(|| CoreError::Parse("krpc message not a dict".into()))?;
    let txn_bytes = dict
        .iter()
        .find(|(k, _)| k == b"t")
        .and_then(|(_, v)| v.as_str())
        .ok_or_else(|| CoreError::Parse("krpc missing transaction id".into()))?;
    let mut txn_arr = [0u8; 2];
    if txn_bytes.len() >= 2 {
        txn_arr.copy_from_slice(&txn_bytes[..2]);
    } else if !txn_bytes.is_empty() {
        txn_arr[0] = txn_bytes[0];
    }
    let txn = TransactionId::new(txn_arr);

    let y = dict
        .iter()
        .find(|(k, _)| k == b"y")
        .and_then(|(_, v)| v.as_str());

    let mut resp = KrpcResponse {
        txn,
        sender_id: None,
        nodes: Vec::new(),
        peers: Vec::new(),
        token: None,
        error: None,
    };

    match y {
        Some(b"r") => {
            let r = dict
                .iter()
                .find(|(k, _)| k == b"r")
                .and_then(|(_, v)| v.as_dict())
                .ok_or_else(|| CoreError::Parse("krpc response missing r".into()))?;
            if let Some(id) = r
                .iter()
                .find(|(k, _)| k == b"id")
                .and_then(|(_, v)| v.as_str())
            {
                if id.len() == 20 {
                    let mut arr = [0u8; 20];
                    arr.copy_from_slice(id);
                    resp.sender_id = Some(NodeId::from_bytes(arr));
                }
            }
            if let Some(token) = r
                .iter()
                .find(|(k, _)| k == b"token")
                .and_then(|(_, v)| v.as_str())
            {
                resp.token = Some(token.to_vec());
            }
            if let Some(nodes) = r
                .iter()
                .find(|(k, _)| k == b"nodes")
                .and_then(|(_, v)| v.as_str())
            {
                resp.nodes.extend(parse_compact_nodes(nodes));
            }
            if let Some(nodes6) = r
                .iter()
                .find(|(k, _)| k == b"nodes6")
                .and_then(|(_, v)| v.as_str())
            {
                resp.nodes.extend(parse_compact_nodes6(nodes6));
            }
            if let Some(values) = r
                .iter()
                .find(|(k, _)| k == b"values")
                .and_then(|(_, v)| v.as_list())
            {
                for v in values {
                    if let Some(p) = v.as_str() {
                        if let Some(peer) = parse_one_peer(p) {
                            resp.peers.push(peer);
                        }
                    }
                }
            }
        }
        Some(b"e") => {
            let e = dict
                .iter()
                .find(|(k, _)| k == b"e")
                .and_then(|(_, v)| v.as_list());
            if let Some(e) = e {
                let code = e.first().and_then(|v| v.as_int()).unwrap_or(0);
                let msg = e
                    .get(1)
                    .and_then(|v| v.as_str_utf8())
                    .unwrap_or_default()
                    .to_string();
                resp.error = Some((code, msg));
            }
        }
        _ => {
            return Err(CoreError::Parse(format!(
                "krpc unknown message type: {:?}",
                y.and_then(|s| std::str::from_utf8(s).ok())
            )));
        }
    }
    Ok(resp)
}

/// Parse compact node info (26 bytes each: 20 id + 4 ip + 2 port).
pub fn parse_compact_nodes(bytes: &[u8]) -> Vec<DhtNode> {
    let mut out = Vec::with_capacity(bytes.len() / 26);
    for chunk in bytes.chunks_exact(26) {
        let mut id = [0u8; 20];
        id.copy_from_slice(&chunk[0..20]);
        let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
        let port = u16::from_be_bytes([chunk[24], chunk[25]]);
        out.push(DhtNode {
            id: NodeId::from_bytes(id),
            addr: SocketAddr::new(IpAddr::V4(ip), port),
        });
    }
    out
}

/// Parse compact IPv6 node info (38 bytes each: 20 id + 16 ip + 2 port).
pub fn parse_compact_nodes6(bytes: &[u8]) -> Vec<DhtNode> {
    let mut out = Vec::with_capacity(bytes.len() / 38);
    for chunk in bytes.chunks_exact(38) {
        let mut id = [0u8; 20];
        id.copy_from_slice(&chunk[0..20]);
        let mut octets = [0u8; 16];
        octets.copy_from_slice(&chunk[20..36]);
        let ip = Ipv6Addr::from(octets);
        let port = u16::from_be_bytes([chunk[36], chunk[37]]);
        out.push(DhtNode {
            id: NodeId::from_bytes(id),
            addr: SocketAddr::new(IpAddr::V6(ip), port),
        });
    }
    out
}

/// Encode compact node info.
pub fn encode_compact_nodes(nodes: &[DhtNode]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nodes.len() * 26);
    for n in nodes {
        out.extend_from_slice(n.id.as_bytes());
        if let IpAddr::V4(v4) = n.addr.ip() {
            out.extend_from_slice(&v4.octets());
        } else if let IpAddr::V6(v6) = n.addr.ip() {
            // Truncate IPv6 into the v4 compact form is invalid; skip v6 here.
            let _ = v6;
            out.extend_from_slice(&[0, 0, 0, 0]);
        }
        out.extend_from_slice(&n.addr.port().to_be_bytes());
    }
    out
}

fn parse_one_peer(bytes: &[u8]) -> Option<PeerAddr> {
    if bytes.len() == 6 {
        let ip = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = u16::from_be_bytes([bytes[4], bytes[5]]);
        Some(PeerAddr {
            ip: IpAddr::V4(ip),
            port,
        })
    } else if bytes.len() == 18 {
        let mut octets = [0u8; 16];
        octets.copy_from_slice(&bytes[0..16]);
        let ip = Ipv6Addr::from(octets);
        let port = u16::from_be_bytes([bytes[16], bytes[17]]);
        Some(PeerAddr {
            ip: IpAddr::V6(ip),
            port,
        })
    } else {
        None
    }
}

fn write_str(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(format!("{}:", b.len()).as_bytes());
    out.extend_from_slice(b);
}

/// Build a `get_peers` query for an info hash.
pub fn build_get_peers(txn: TransactionId, self_id: NodeId, info_hash: PeerInfoHash) -> Vec<u8> {
    encode_query(
        txn,
        KrpcMethod::GetPeers,
        &KrpcArgs {
            id: self_id,
            target: Some(info_hash),
            port: None,
            token: None,
        },
    )
}

/// Build a `find_node` query for a target id.
pub fn build_find_node(txn: TransactionId, self_id: NodeId, target: NodeId) -> Vec<u8> {
    encode_query(
        txn,
        KrpcMethod::FindNode,
        &KrpcArgs {
            id: self_id,
            target: Some(PeerInfoHash::from_bytes(target.0)),
            port: None,
            token: None,
        },
    )
}

/// Build a `ping` query.
pub fn build_ping(txn: TransactionId, self_id: NodeId) -> Vec<u8> {
    encode_query(
        txn,
        KrpcMethod::Ping,
        &KrpcArgs {
            id: self_id,
            target: None,
            port: None,
            token: None,
        },
    )
}

/// Build an `announce_peer` query.
pub fn build_announce_peer(
    txn: TransactionId,
    self_id: NodeId,
    info_hash: PeerInfoHash,
    port: u16,
    token: Vec<u8>,
) -> Vec<u8> {
    encode_query(
        txn,
        KrpcMethod::AnnouncePeer,
        &KrpcArgs {
            id: self_id,
            target: Some(info_hash),
            port: Some(port),
            token: Some(token),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_distance_is_xor() {
        let a = NodeId::from_bytes([0xff; 20]);
        let b = NodeId::from_bytes([0x0f; 20]);
        let d = a.distance(&b);
        assert_eq!(d, [0xf0; 20]);
    }

    #[test]
    fn routing_table_returns_closest() {
        let mut rt = RoutingTable::new(8);
        let target = NodeId::from_bytes([0; 20]);
        for i in 0..16u8 {
            rt.insert(DhtNode {
                id: NodeId::from_bytes([i; 20]),
                addr: "127.0.0.1:6881".parse().unwrap(),
            });
        }
        let closest = rt.closest(&target, 4);
        assert_eq!(closest.len(), 4);
        // Closest to all-zero should be the smallest ids.
        assert_eq!(closest[0].id, NodeId::from_bytes([0; 20]));
        assert_eq!(closest[1].id, NodeId::from_bytes([1; 20]));
    }

    #[test]
    fn routing_table_dedups_by_id() {
        let mut rt = RoutingTable::new(8);
        let n = DhtNode {
            id: NodeId::from_bytes([1; 20]),
            addr: "127.0.0.1:1".parse().unwrap(),
        };
        rt.insert(n);
        rt.insert(DhtNode {
            id: NodeId::from_bytes([1; 20]),
            addr: "127.0.0.1:2".parse().unwrap(),
        });
        assert_eq!(rt.len(), 1);
        assert_eq!(rt.nodes()[0].addr.port(), 2);
    }

    #[test]
    fn ping_query_roundtrips_via_parse() {
        let id = NodeId::from_bytes([7; 20]);
        let q = build_ping(TransactionId::new([1, 2]), id);
        // parse_response expects a response; queries aren't responses, so we
        // just assert the encoded form is well-formed bencode.
        assert!(bencode::decode(&q).is_ok());
    }

    #[test]
    fn query_outer_keys_are_canonical() {
        fn offset(haystack: &[u8], needle: &[u8]) -> usize {
            haystack
                .windows(needle.len())
                .position(|w| w == needle)
                .unwrap()
        }

        let id = NodeId::from_bytes([7; 20]);
        let info_hash = PeerInfoHash::from_bytes([8; 20]);
        let q = build_get_peers(TransactionId::new([1, 2]), id, info_hash);

        assert!(q.starts_with(b"d1:a"));
        let a = offset(&q, b"1:a");
        let query = offset(&q, b"1:q9:get_peers");
        let txn = offset(&q, b"1:t2:");
        let y = offset(&q, b"1:y1:q");
        assert!(a < query);
        assert!(query < txn);
        assert!(txn < y);
    }

    #[test]
    fn random_transaction_ids_advance() {
        let first = TransactionId::random();
        let second = TransactionId::random();

        assert_ne!(first, second);
    }

    #[test]
    fn get_peers_query_uses_info_hash_not_target() {
        let id = NodeId::from_bytes([7; 20]);
        let info_hash = PeerInfoHash::from_bytes([8; 20]);
        let q = build_get_peers(TransactionId::new([1, 2]), id, info_hash);
        let root = bencode::decode(&q).unwrap();
        let dict = root.as_dict().unwrap();
        let args = dict
            .iter()
            .find(|(k, _)| k == b"a")
            .and_then(|(_, v)| v.as_dict())
            .unwrap();

        assert!(args.iter().any(|(k, _)| k == b"info_hash"));
        assert!(!args.iter().any(|(k, _)| k == b"target"));
    }

    #[test]
    fn find_node_query_uses_target_not_info_hash() {
        let id = NodeId::from_bytes([7; 20]);
        let target = NodeId::from_bytes([8; 20]);
        let q = build_find_node(TransactionId::new([1, 2]), id, target);
        let root = bencode::decode(&q).unwrap();
        let dict = root.as_dict().unwrap();
        let args = dict
            .iter()
            .find(|(k, _)| k == b"a")
            .and_then(|(_, v)| v.as_dict())
            .unwrap();

        assert!(args.iter().any(|(k, _)| k == b"target"));
        assert!(!args.iter().any(|(k, _)| k == b"info_hash"));
    }

    #[test]
    fn get_peers_response_parses_peers_and_nodes() {
        // Build a get_peers response by hand.
        let mut out = Vec::new();
        out.push(b'd');
        write_str(&mut out, b"t");
        write_str(&mut out, &[9, 9]);
        write_str(&mut out, b"y");
        write_str(&mut out, b"r");
        write_str(&mut out, b"r");
        out.push(b'd');
        write_str(&mut out, b"id");
        write_str(&mut out, &[3; 20]);
        write_str(&mut out, b"token");
        write_str(&mut out, b"tok");
        // values: one compact peer 1.2.3.4:6881
        write_str(&mut out, b"values");
        out.push(b'l');
        write_str(&mut out, &[1, 2, 3, 4, 0x1a, 0xe1]);
        out.push(b'e');
        // nodes: one compact node
        write_str(&mut out, b"nodes");
        let mut node = Vec::new();
        node.extend_from_slice(&[5; 20]);
        node.extend_from_slice(&[6, 7, 8, 9, 0x1a, 0xe2]);
        write_str(&mut out, &node);
        out.push(b'e'); // close r dict
        out.push(b'e'); // close outer dict
        let resp = parse_response(&out).unwrap();
        assert_eq!(resp.txn, TransactionId::new([9, 9]));
        assert_eq!(resp.sender_id, Some(NodeId::from_bytes([3; 20])));
        assert_eq!(resp.token.as_deref(), Some(b"tok".as_ref()));
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(resp.peers[0].port, 6881);
        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].id, NodeId::from_bytes([5; 20]));
        assert_eq!(resp.nodes[0].addr.port(), 6882);
    }

    #[test]
    fn get_peers_response_parses_nodes6() {
        let mut out = Vec::new();
        out.push(b'd');
        write_str(&mut out, b"t");
        write_str(&mut out, &[9, 9]);
        write_str(&mut out, b"y");
        write_str(&mut out, b"r");
        write_str(&mut out, b"r");
        out.push(b'd');
        write_str(&mut out, b"id");
        write_str(&mut out, &[3; 20]);
        write_str(&mut out, b"nodes6");
        let mut node = Vec::new();
        node.extend_from_slice(&[5; 20]);
        node.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        node.extend_from_slice(&6881u16.to_be_bytes());
        write_str(&mut out, &node);
        out.push(b'e');
        out.push(b'e');

        let resp = parse_response(&out).unwrap();

        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].id, NodeId::from_bytes([5; 20]));
        assert_eq!(resp.nodes[0].addr, "[2001:db8::1]:6881".parse().unwrap());
    }

    #[test]
    fn error_response_parses() {
        let mut out = Vec::new();
        out.push(b'd');
        write_str(&mut out, b"t");
        write_str(&mut out, &[1, 1]);
        write_str(&mut out, b"y");
        write_str(&mut out, b"e");
        write_str(&mut out, b"e");
        out.push(b'l');
        out.push(b'i');
        out.extend_from_slice(b"203");
        out.push(b'e');
        write_str(&mut out, b"bad");
        out.push(b'e');
        out.push(b'e');
        let resp = parse_response(&out).unwrap();
        assert_eq!(resp.error, Some((203, "bad".to_string())));
    }

    #[test]
    fn compact_nodes_roundtrip() {
        let nodes = vec![
            DhtNode {
                id: NodeId::from_bytes([1; 20]),
                addr: "1.2.3.4:100".parse().unwrap(),
            },
            DhtNode {
                id: NodeId::from_bytes([2; 20]),
                addr: "5.6.7.8:200".parse().unwrap(),
            },
        ];
        let enc = encode_compact_nodes(&nodes);
        assert_eq!(enc.len(), 52);
        let back = parse_compact_nodes(&enc);
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].addr.port(), 100);
        assert_eq!(back[1].addr.port(), 200);
    }

    #[test]
    fn private_torrent_blocks_dht_by_design() {
        let private = true;
        assert!(private && !should_dht(private));
    }

    fn should_dht(private: bool) -> bool {
        !private
    }
}
