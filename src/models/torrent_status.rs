use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentStatus {
    pub state: String,
    pub progress: f32,
    pub download_speed: f64,
    pub upload_speed: f64,
    pub eta: u64,
}