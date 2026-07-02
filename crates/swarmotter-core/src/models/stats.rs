// SPDX-License-Identifier: Apache-2.0

//! Statistics models.

use crate::hash::InfoHash;
use crate::models::torrent::TorrentState;
use serde::{Deserialize, Serialize};

/// Per-torrent statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TorrentStats {
    pub downloaded: u64,
    pub uploaded: u64,
    pub bytes_completed: u64,
    pub total_length: u64,
    pub rate_down: u64,
    pub rate_up: u64,
    pub seeders: u64,
    pub leechers: u64,
    pub pieces_have: usize,
    pub piece_count: usize,
    pub date_added: u64,
    pub date_completed: Option<u64>,
}

impl TorrentStats {
    pub fn ratio(&self) -> f64 {
        if self.downloaded == 0 {
            return 0.0;
        }
        (self.uploaded as f64) / (self.downloaded as f64)
    }

    pub fn progress(&self) -> f64 {
        if self.total_length == 0 {
            return 0.0;
        }
        (self.bytes_completed as f64) / (self.total_length as f64)
    }
}

/// Global daemon statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalStats {
    pub download_rate: u64,
    pub upload_rate: u64,
    pub torrent_count: usize,
    pub active_downloads: usize,
    pub active_seeds: usize,
    pub paused: usize,
    pub total_downloaded: u64,
    pub total_uploaded: u64,
    pub free_space: Option<u64>,
    pub uptime_seconds: u64,
}

/// Per-torrent operational diagnostics for API/UI troubleshooting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentDiagnostics {
    pub info_hash: InfoHash,
    pub name: String,
    pub state: TorrentState,
    pub total_length: u64,
    pub bytes_completed: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub piece_count: usize,
    pub pieces_have: usize,
    pub piece_length: u64,
    pub progress: f64,
    pub rate_down: u64,
    pub rate_up: u64,
    pub download_limit: u64,
    pub upload_limit: u64,
    pub active_peer_workers: usize,
    pub known_peers: usize,
    pub tracker_ok: bool,
    pub tracker_message: Option<String>,
    pub last_announce: Option<u64>,
    pub private: bool,
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn ratio_calc() {
        let mut s = TorrentStats::default();
        s.downloaded = 1000;
        s.uploaded = 500;
        assert_eq!(s.ratio(), 0.5);
        s.downloaded = 0;
        assert_eq!(s.ratio(), 0.0);
    }

    #[test]
    fn progress_calc() {
        let mut s = TorrentStats::default();
        s.total_length = 1000;
        s.bytes_completed = 250;
        assert!((s.progress() - 0.25).abs() < f64::EPSILON);
    }
}
