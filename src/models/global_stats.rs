use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStats {
    pub active_peers: usize,
    pub download_speed: f64,
    pub upload_speed: f64,
    pub torrents: usize,
    pub completed_torrents: usize,
}