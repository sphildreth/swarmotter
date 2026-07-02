use std::collections::HashMap;
use tokio::sync::Mutex;
use crate::models::PeerInfo;

pub struct PeerManager {
    peers: Mutex<HashMap<String, PeerInfo>>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn add_peer(&self, peer: PeerInfo) {
        let mut peers = self.peers.lock().await;
        peers.insert(peer.id.clone(), peer);
    }

    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.lock().await;
        peers.values().cloned().collect()
    }
}