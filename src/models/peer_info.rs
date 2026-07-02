use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub ip: String,
    pub client: Option<String>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub progress: f32,
    pub is_seed: bool,
}