use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Torrent {
    pub id: String,
    pub name: String,
    pub info_hash: Vec<u8>,
    pub size: u64,
    pub files: Vec<FileInfo>,
    pub trackers: Vec<TrackerInfo>,
    pub status: TorrentStatus,
    pub added_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TorrentStatus {
    Queued,
    Downloading,
    Seeding,
    Paused,
    Error,
}