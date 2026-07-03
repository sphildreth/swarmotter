// SPDX-License-Identifier: Apache-2.0

//! Live mainline DHT (BEP 5) runner.
//!
//! Drives KRPC over a contained UDP socket obtained from the `NetworkBinder`.
//! Bootstraps from configured nodes, then performs iterative `get_peers` for a
//! torrent's info hash to discover peers, and `announce_peer` to publish the
//! local torrent port. Private torrents disable DHT.
//!
//! All UDP traffic goes through the binder; fail-closed blocks DHT entirely.
//! The pure KRPC/routing logic lives in `swarmotter-core::dht`. See
//! `design/requirements.md` and ADR-0019.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::dht::{
    self, build_get_peers, build_ping, KrpcResponse, NodeId, RoutingTable, TransactionId,
};
use swarmotter_core::error::Result;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::net::{ContainedUdpSocket, NetworkBinder};
use swarmotter_core::peer::PeerAddr;

const DHT_ALPHA: usize = 8;
const DHT_QUERY_WINDOW: Duration = Duration::from_millis(1_200);
const DHT_MAX_QUERIES: usize = 96;
const DHT_MAX_PEERS: usize = 256;

/// Summary of a bounded DHT lookup.
#[derive(Debug, Clone, Default)]
pub struct DhtLookupResult {
    pub peers: Vec<PeerAddr>,
    pub queried_nodes: usize,
    pub responding_nodes: usize,
}

#[derive(Debug, Clone, Copy)]
struct DhtCandidate {
    id: Option<NodeId>,
    addr: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DhtAddressFamily {
    V4,
    V6,
}

impl DhtAddressFamily {
    fn for_addr(addr: SocketAddr) -> Self {
        if addr.is_ipv6() {
            Self::V6
        } else {
            Self::V4
        }
    }

    fn matches(self, addr: SocketAddr) -> bool {
        matches!(
            (self, addr),
            (Self::V4, SocketAddr::V4(_)) | (Self::V6, SocketAddr::V6(_))
        )
    }
}

/// A live DHT runner bound to a contained UDP socket.
pub struct DhtRunner {
    self_id: NodeId,
    binder: Arc<dyn NetworkBinder>,
    table: Arc<Mutex<RoutingTable>>,
    bootstrap: Vec<SocketAddr>,
    port: u16,
    socket_lock: Mutex<()>,
}

impl DhtRunner {
    pub fn new(
        self_id: NodeId,
        binder: Arc<dyn NetworkBinder>,
        bootstrap: Vec<SocketAddr>,
        port: u16,
    ) -> Self {
        Self {
            self_id,
            binder,
            table: Arc::new(Mutex::new(RoutingTable::new(16))),
            bootstrap,
            port,
            socket_lock: Mutex::new(()),
        }
    }

    /// Derive a node id from a peer id seed.
    pub fn derive_from_peer_id(peer_id: &[u8; 20]) -> NodeId {
        NodeId::derive(peer_id)
    }

    /// Open the contained UDP socket. Returns an error in fail-closed mode.
    #[allow(dead_code)]
    pub async fn socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        self.socket_for(self.bootstrap.first().copied()).await
    }

    async fn socket_for(&self, remote: Option<SocketAddr>) -> Result<Box<dyn ContainedUdpSocket>> {
        self.binder.udp_socket_on(remote, self.port).await
    }

    /// Bootstrap: ping each configured bootstrap node so it learns about us
    /// and we learn its id, inserting it into the routing table.
    #[allow(dead_code)]
    pub async fn bootstrap(&self) -> Result<()> {
        let _socket_guard = self.socket_lock.lock().await;
        for addr in &self.bootstrap {
            let socket = self.socket_for(Some(*addr)).await?;
            let txn = TransactionId::random();
            let q = build_ping(txn, self.self_id);
            let _ = socket.send_to(*addr, &q).await;
            // Best-effort: read a response with a short timeout.
            let mut buf = vec![0u8; 2048];
            if let Ok(Ok((from, n))) =
                timeout(Duration::from_secs(3), socket.recv_from(&mut buf)).await
            {
                if let Ok(resp) = dht::parse_response(&buf[..n]) {
                    if let Some(id) = resp.sender_id {
                        self.table
                            .lock()
                            .await
                            .insert(dht::DhtNode { id, addr: from });
                    }
                }
            }
        }
        Ok(())
    }

    /// Iterative `get_peers` with basic reachability counters for diagnostics.
    pub async fn get_peers_with_stats(
        &self,
        info_hash: InfoHash,
        max_rounds: usize,
    ) -> Result<DhtLookupResult> {
        let _socket_guard = self.socket_lock.lock().await;
        let target = NodeId::from_bytes(*info_hash.as_bytes());
        let mut hints = self.lookup_family_hints(target).await;
        let mut result = DhtLookupResult::default();
        let mut seen_peers: HashSet<PeerAddr> = HashSet::new();
        let mut attempted_families: HashSet<DhtAddressFamily> = HashSet::new();
        let mut opened_socket = false;
        let mut first_error = None;
        let mut index = 0usize;

        while index < hints.len() {
            let hint = hints[index];
            index += 1;
            let family = DhtAddressFamily::for_addr(hint);
            if !attempted_families.insert(family) {
                continue;
            }

            let socket: Arc<dyn ContainedUdpSocket> = match self.socket_for(Some(hint)).await {
                Ok(socket) => {
                    opened_socket = true;
                    socket.into()
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                    continue;
                }
            };

            let (partial, deferred_hints) = self
                .get_peers_with_socket(info_hash, max_rounds, socket, family, hint)
                .await;
            result.queried_nodes += partial.queried_nodes;
            result.responding_nodes += partial.responding_nodes;
            for peer in partial.peers {
                if seen_peers.insert(peer) {
                    result.peers.push(peer);
                }
            }
            for hint in deferred_hints {
                add_family_hint(&mut hints, hint);
            }
        }

        if !opened_socket {
            if let Some(e) = first_error {
                return Err(e);
            }
        }

        Ok(result)
    }

    async fn get_peers_with_socket(
        &self,
        info_hash: InfoHash,
        max_rounds: usize,
        socket: Arc<dyn ContainedUdpSocket>,
        family: DhtAddressFamily,
        seed_hint: SocketAddr,
    ) -> (DhtLookupResult, Vec<SocketAddr>) {
        let mut result = DhtLookupResult::default();
        let mut deferred_hints = Vec::new();
        let mut queried: HashSet<SocketAddr> = HashSet::new();
        let mut queued: HashSet<SocketAddr> = HashSet::new();
        let mut pending: Vec<DhtCandidate> = self
            .bootstrap
            .iter()
            .copied()
            .filter(|addr| family.matches(*addr))
            .map(|addr| DhtCandidate { id: None, addr })
            .collect();
        let target = NodeId::from_bytes(*info_hash.as_bytes());
        queued.extend(pending.iter().map(|candidate| candidate.addr));
        if family.matches(seed_hint) && queued.insert(seed_hint) {
            pending.push(DhtCandidate {
                id: None,
                addr: seed_hint,
            });
        }
        // Seed with any known routing-table nodes.
        {
            let table = self.table.lock().await;
            for n in table.closest(&target, 32) {
                if family.matches(n.addr) && queued.insert(n.addr) {
                    pending.push(DhtCandidate {
                        id: Some(n.id),
                        addr: n.addr,
                    });
                }
            }
        }

        for _ in 0..max_rounds.max(1) {
            if queried.len() >= DHT_MAX_QUERIES || result.peers.len() >= DHT_MAX_PEERS {
                break;
            }
            pending.retain(|candidate| !queried.contains(&candidate.addr));
            sort_candidates_by_distance(&mut pending, target);
            if pending.is_empty() {
                break;
            }

            let remaining_budget = DHT_MAX_QUERIES.saturating_sub(queried.len());
            let batch_len = pending.len().min(DHT_ALPHA).min(remaining_budget);
            let batch: Vec<DhtCandidate> = pending.drain(..batch_len).collect();
            let mut transactions: HashMap<TransactionId, SocketAddr> = HashMap::new();
            for candidate in batch {
                let addr = candidate.addr;
                if !queried.insert(addr) {
                    continue;
                }
                let txn = unique_transaction_id(&transactions);
                let q = build_get_peers(txn, self.self_id, info_hash);
                if socket.send_to(addr, &q).await.is_err() {
                    continue;
                }
                result.queried_nodes += 1;
                transactions.insert(txn, addr);
            }

            let deadline = Instant::now() + DHT_QUERY_WINDOW;
            while !transactions.is_empty() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let mut buf = vec![0u8; 2048];
                let Ok(Ok((from, n))) = timeout(remaining, socket.recv_from(&mut buf)).await else {
                    break;
                };
                let Ok(resp) = dht::parse_response(&buf[..n]) else {
                    continue;
                };
                let Some(expected_from) = transactions.get(&resp.txn).copied() else {
                    continue;
                };
                if expected_from != from {
                    continue;
                }
                transactions.remove(&resp.txn);
                result.responding_nodes += 1;
                self.handle_response(
                    from,
                    &resp,
                    &mut result.peers,
                    &mut pending,
                    &mut queued,
                    &mut deferred_hints,
                    family,
                )
                .await;
                if result.peers.len() >= DHT_MAX_PEERS {
                    result.peers.truncate(DHT_MAX_PEERS);
                    break;
                }
            }
        }
        (result, deferred_hints)
    }

    async fn lookup_family_hints(&self, target: NodeId) -> Vec<SocketAddr> {
        let mut hints = Vec::new();
        for addr in &self.bootstrap {
            add_family_hint(&mut hints, *addr);
        }
        let table = self.table.lock().await;
        for n in table.closest(&target, 64) {
            add_family_hint(&mut hints, n.addr);
        }
        hints
    }

    async fn handle_response(
        &self,
        from: SocketAddr,
        resp: &KrpcResponse,
        discovered: &mut Vec<PeerAddr>,
        pending: &mut Vec<DhtCandidate>,
        queued: &mut HashSet<SocketAddr>,
        deferred_hints: &mut Vec<SocketAddr>,
        family: DhtAddressFamily,
    ) {
        if let Some(id) = resp.sender_id {
            self.table
                .lock()
                .await
                .insert(dht::DhtNode { id, addr: from });
        }
        for p in &resp.peers {
            if !discovered.contains(p) {
                discovered.push(*p);
            }
        }
        for n in &resp.nodes {
            if !family.matches(n.addr) {
                add_family_hint(deferred_hints, n.addr);
            } else if queued.insert(n.addr) {
                pending.push(DhtCandidate {
                    id: Some(n.id),
                    addr: n.addr,
                });
            }
        }
    }

    /// Announce this peer for an info hash to the closest known nodes that
    /// previously returned a token. Best-effort.
    #[allow(dead_code)]
    pub async fn announce_peer(
        &self,
        info_hash: InfoHash,
        port: u16,
        tokens: &[(SocketAddr, Vec<u8>)],
    ) -> Result<()> {
        let _socket_guard = self.socket_lock.lock().await;
        for (addr, token) in tokens {
            let socket = self.socket_for(Some(*addr)).await?;
            let txn = TransactionId::random();
            let q = dht::build_announce_peer(txn, self.self_id, info_hash, port, token.clone());
            let _ = socket.send_to(*addr, &q).await;
            let mut buf = vec![0u8; 2048];
            let _ = timeout(Duration::from_secs(3), socket.recv_from(&mut buf)).await;
        }
        Ok(())
    }

    /// Number of known nodes in the routing table (for API/UI status).
    #[allow(dead_code)]
    pub async fn node_count(&self) -> usize {
        self.table.lock().await.len()
    }
}

/// Resolve configured bootstrap node strings ("host:port") to SocketAddrs.
#[cfg(test)]
pub fn resolve_bootstrap(specs: &[String]) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for s in specs {
        if let Ok(addr) = s.parse::<SocketAddr>() {
            out.push(addr);
            continue;
        }
        if let Some((host, port)) = s.rsplit_once(':') {
            if let Ok(port) = port.parse::<u16>() {
                if let Ok(mut iter) = std::net::ToSocketAddrs::to_socket_addrs(&(host, port)) {
                    if let Some(addr) = iter.next() {
                        out.push(addr);
                    }
                }
            }
        }
    }
    out
}

/// Resolve configured bootstrap node strings through the containment binder.
/// IP literals are accepted directly; hostnames must pass the binder's DNS
/// policy so DHT bootstrap cannot resolve outside fail-closed containment.
pub async fn resolve_bootstrap_with_binder(
    binder: &dyn NetworkBinder,
    specs: &[String],
) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for s in specs {
        if let Ok(addr) = s.parse::<SocketAddr>() {
            out.push(addr);
            continue;
        }
        if let Some((host, port)) = s.rsplit_once(':') {
            if let Ok(port) = port.parse::<u16>() {
                if let Ok(addr) = binder.resolve_host(host, port).await {
                    out.push(addr);
                }
            }
        }
    }
    out
}

fn sort_candidates_by_distance(candidates: &mut [DhtCandidate], target: NodeId) {
    candidates.sort_by_key(|candidate| {
        candidate
            .id
            .map(|id| id.distance(&target))
            .unwrap_or([0u8; 20])
    });
}

fn add_family_hint(hints: &mut Vec<SocketAddr>, addr: SocketAddr) {
    let family = DhtAddressFamily::for_addr(addr);
    if !hints
        .iter()
        .any(|existing| DhtAddressFamily::for_addr(*existing) == family)
    {
        hints.push(addr);
    }
}

fn unique_transaction_id(transactions: &HashMap<TransactionId, SocketAddr>) -> TransactionId {
    loop {
        let txn = TransactionId::random();
        if !transactions.contains_key(&txn) {
            return txn;
        }
    }
}

#[cfg(test)]
mod utp_transport_tests {
    use super::*;
    use swarmotter_core::net::binder::BlockedBinder;
    use swarmotter_core::utp::UtpConnection;

    /// Fail-closed containment blocks uTP: connecting over a `BlockedBinder`
    /// must surface `NetworkBlocked` (the contained UDP socket is refused),
    /// proving uTP cannot bypass the network path.
    #[tokio::test]
    async fn utp_transport_fail_closed_blocks_connect() {
        let binder = Arc::new(BlockedBinder);
        match UtpConnection::connect(binder.as_ref(), "127.0.0.1:9".parse().unwrap()).await {
            Ok(_) => panic!("expected fail-closed to block uTP connect"),
            Err(e) => assert!(e.is_network_blocked()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::net::binder::LoopbackBinder;
    use swarmotter_core::peer::PeerAddr;

    fn write_bstr(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(bytes.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(bytes);
    }

    fn txn_from_query(buf: &[u8]) -> Vec<u8> {
        swarmotter_core::bencode::decode(buf)
            .ok()
            .and_then(|root| {
                root.as_dict()
                    .and_then(|d| d.iter().find(|(k, _)| k == b"t"))
                    .and_then(|(_, v)| v.as_str())
                    .map(Vec::from)
            })
            .unwrap_or_else(|| vec![0, 0])
    }

    fn response_start(responder_id: [u8; 20]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(b'd');
        write_bstr(&mut out, b"r");
        out.push(b'd');
        write_bstr(&mut out, b"id");
        write_bstr(&mut out, &responder_id);
        out
    }

    fn response_finish(out: &mut Vec<u8>, txn: &[u8]) {
        out.push(b'e');
        write_bstr(out, b"t");
        write_bstr(out, txn);
        write_bstr(out, b"y");
        write_bstr(out, b"r");
        out.push(b'e');
    }

    /// A minimal local DHT node that responds to get_peers with a peer and a
    /// node, exercising the contained UDP path over loopback.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dht_get_peers_discovers_peer_from_local_node() {
        // Local "DHT node" UDP socket.
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let node_addr = sock.local_addr().unwrap();
        let info_hash = InfoHash::from_bytes([0xab; 20]);
        let info_hash_for_task = info_hash;
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (_n, peer) = sock.recv_from(&mut buf).await.unwrap();
            // Build a get_peers response with one peer (1.2.3.4:6881) and one
            // node, plus a token.
            let mut out = Vec::new();
            out.push(b'd');
            // Copy the transaction id from the request.
            let txn = &buf[3..5]; // after "2:t" prefix in our encoding
                                  // Find the transaction id value robustly: re-decode.
            if let Ok(root) = swarmotter_core::bencode::decode(&buf[.._n]) {
                if let Some(t) = root
                    .as_dict()
                    .and_then(|d| d.iter().find(|(k, _)| k == b"t"))
                    .and_then(|(_, v)| v.as_str())
                {
                    out.extend_from_slice(b"1:t");
                    out.extend_from_slice(format!("{}:", t.len()).as_bytes());
                    out.extend_from_slice(t);
                }
            } else {
                let _ = txn;
            }
            out.extend_from_slice(b"1:y1:r1:rd");
            out.extend_from_slice(b"2:id20:");
            out.extend_from_slice(&[9u8; 20]);
            out.extend_from_slice(b"5:token3:tok");
            // values list with one compact peer.
            out.extend_from_slice(b"6:valuesl6:");
            out.extend_from_slice(&[1, 2, 3, 4, 0x1a, 0xe1]);
            out.extend_from_slice(b"e");
            // nodes with one compact node.
            out.extend_from_slice(b"5:nodes26:");
            let mut node = Vec::new();
            node.extend_from_slice(&[8u8; 20]);
            node.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe3]);
            out.extend_from_slice(&node);
            out.extend_from_slice(b"ee");
            sock.send_to(&out, peer).await.unwrap();
            let _ = info_hash_for_task;
        });

        let binder = Arc::new(LoopbackBinder);
        let runner = DhtRunner::new(NodeId::random(), binder, vec![node_addr], 0);
        let peers = runner
            .get_peers_with_stats(info_hash, 2)
            .await
            .unwrap()
            .peers;
        assert!(!peers.is_empty(), "should have discovered a peer");
        assert_eq!(peers[0].port, 6881);
        assert_eq!(peers[0].ip.to_string(), "1.2.3.4");
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dht_get_peers_follows_nodes6_from_ipv4_response() {
        let v4_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let v4_addr = v4_sock.local_addr().unwrap();
        let Ok(v6_sock) = tokio::net::UdpSocket::bind("[::1]:0").await else {
            return;
        };
        let v6_addr = v6_sock.local_addr().unwrap();
        let info_hash = InfoHash::from_bytes([0xcd; 20]);

        let v4_task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = v4_sock.recv_from(&mut buf).await.unwrap();
            let txn = txn_from_query(&buf[..n]);
            let mut out = response_start([9u8; 20]);
            write_bstr(&mut out, b"nodes6");
            let mut node = Vec::new();
            node.extend_from_slice(&[8u8; 20]);
            if let SocketAddr::V6(addr) = v6_addr {
                node.extend_from_slice(&addr.ip().octets());
                node.extend_from_slice(&addr.port().to_be_bytes());
            }
            write_bstr(&mut out, &node);
            response_finish(&mut out, &txn);
            v4_sock.send_to(&out, peer).await.unwrap();
        });

        let v6_task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = v6_sock.recv_from(&mut buf).await.unwrap();
            let txn = txn_from_query(&buf[..n]);
            let mut out = response_start([8u8; 20]);
            write_bstr(&mut out, b"values");
            out.push(b'l');
            let mut compact_peer = Vec::new();
            compact_peer.extend_from_slice(&std::net::Ipv6Addr::LOCALHOST.octets());
            compact_peer.extend_from_slice(&6881u16.to_be_bytes());
            write_bstr(&mut out, &compact_peer);
            out.push(b'e');
            response_finish(&mut out, &txn);
            v6_sock.send_to(&out, peer).await.unwrap();
        });

        let binder = Arc::new(LoopbackBinder);
        let runner = DhtRunner::new(NodeId::random(), binder, vec![v4_addr], 0);
        let peers = runner
            .get_peers_with_stats(info_hash, 4)
            .await
            .unwrap()
            .peers;

        assert!(peers
            .iter()
            .any(|peer| peer.ip.is_ipv6() && peer.port == 6881));
        v4_task.await.unwrap();
        v6_task.await.unwrap();
    }

    #[tokio::test]
    async fn dht_fail_closed_blocks_socket() {
        let binder = Arc::new(swarmotter_core::net::binder::BlockedBinder);
        let runner = DhtRunner::new(NodeId::random(), binder, vec![], 0);
        match runner.socket().await {
            Ok(_) => panic!("expected fail-closed to block DHT socket"),
            Err(e) => assert!(e.is_network_blocked()),
        }
    }

    #[tokio::test]
    async fn dht_socket_uses_configured_port() {
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let binder = Arc::new(LoopbackBinder);
        let runner = DhtRunner::new(NodeId::random(), binder, vec![], port);
        let socket = runner.socket().await.unwrap();

        assert_eq!(socket.local_addr().unwrap().port(), port);
    }

    #[test]
    fn resolve_bootstrap_parses_addrs() {
        let specs = vec!["127.0.0.1:6881".to_string(), "1.2.3.4:6882".to_string()];
        let addrs = resolve_bootstrap(&specs);
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0].port(), 6881);
    }

    #[test]
    fn resolve_bootstrap_skips_invalid() {
        let addrs = resolve_bootstrap(&["not-an-addr".to_string()]);
        assert!(addrs.is_empty());
    }

    #[test]
    fn node_count_starts_empty() {
        let binder: Arc<dyn NetworkBinder> = Arc::new(LoopbackBinder);
        let runner = DhtRunner::new(NodeId::random(), binder, vec![], 0);
        let _ = PeerAddr::from_socket_addr("127.0.0.1:1".parse().unwrap());
        // node_count is async; assert via a minimal runtime.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let count = rt.block_on(runner.node_count());
        assert_eq!(count, 0);
    }
}
