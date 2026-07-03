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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::dht::{
    self, build_get_peers, build_ping, KrpcResponse, NodeId, RoutingTable, TransactionId,
};
use swarmotter_core::error::Result;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::net::{ContainedUdpSocket, NetworkBinder};
use swarmotter_core::peer::PeerAddr;

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
    pub async fn socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        self.binder
            .udp_socket_on(self.bootstrap.first().copied(), self.port)
            .await
    }

    /// Bootstrap: ping each configured bootstrap node so it learns about us
    /// and we learn its id, inserting it into the routing table.
    #[allow(dead_code)]
    pub async fn bootstrap(&self) -> Result<()> {
        let _socket_guard = self.socket_lock.lock().await;
        let socket = self.socket().await?;
        for addr in &self.bootstrap {
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

    /// Iterative `get_peers` for an info hash. Returns discovered peers and
    /// populates the routing table with nodes that responded. Performs a
    /// bounded number of rounds against the closest known nodes, with short
    /// per-node timeouts so unreachable bootstrap nodes cannot stall the
    /// caller.
    pub async fn get_peers(&self, info_hash: InfoHash, max_rounds: usize) -> Result<Vec<PeerAddr>> {
        let _socket_guard = self.socket_lock.lock().await;
        let socket = self.socket().await?;
        let mut discovered: Vec<PeerAddr> = Vec::new();
        let mut queried: Vec<SocketAddr> = Vec::new();
        let mut pending: Vec<SocketAddr> = self.bootstrap.clone();
        // Seed with any known routing-table nodes.
        {
            let table = self.table.lock().await;
            for n in table.nodes() {
                pending.push(n.addr);
            }
        }

        for _ in 0..max_rounds.max(1) {
            if pending.is_empty() {
                break;
            }
            let batch: Vec<SocketAddr> = pending.drain(..pending.len().min(8)).collect();
            let mut next: Vec<SocketAddr> = Vec::new();
            for addr in batch {
                if queried.contains(&addr) {
                    continue;
                }
                queried.push(addr);
                let txn = TransactionId::random();
                let q = build_get_peers(txn, self.self_id, info_hash);
                if socket.send_to(addr, &q).await.is_err() {
                    continue;
                }
                let mut buf = vec![0u8; 2048];
                let resp =
                    match timeout(Duration::from_millis(800), socket.recv_from(&mut buf)).await {
                        Ok(Ok((from, n))) => match dht::parse_response(&buf[..n]) {
                            Ok(r) => Some((from, r)),
                            Err(_) => None,
                        },
                        _ => None,
                    };
                if let Some((from, resp)) = resp {
                    self.handle_response(from, &resp, &mut discovered, &mut next, info_hash);
                }
            }
            pending.extend(next);
        }
        Ok(discovered)
    }

    fn handle_response(
        &self,
        from: SocketAddr,
        resp: &KrpcResponse,
        discovered: &mut Vec<PeerAddr>,
        next: &mut Vec<SocketAddr>,
        _info_hash: InfoHash,
    ) {
        if let Some(id) = resp.sender_id {
            if let Ok(mut t) = self.table.try_lock() {
                t.insert(dht::DhtNode { id, addr: from });
            }
        }
        for p in &resp.peers {
            if !discovered.contains(p) {
                discovered.push(*p);
            }
        }
        for n in &resp.nodes {
            next.push(n.addr);
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
        let socket = self.socket().await?;
        for (addr, token) in tokens {
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
        let peers = runner.get_peers(info_hash, 2).await.unwrap();
        assert!(!peers.is_empty(), "should have discovered a peer");
        assert_eq!(peers[0].port, 6881);
        assert_eq!(peers[0].ip.to_string(), "1.2.3.4");
        task.await.unwrap();
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
