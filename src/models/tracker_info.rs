use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerInfo {
    pub url: String,
    pub last_announce: Option<String>,
}