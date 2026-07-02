// SPDX-License-Identifier: Apache-2.0

//! Torrent state, summary, and file models.

use crate::hash::InfoHash;
use serde::{Deserialize, Serialize};

/// Torrent lifecycle state.
///
/// Matches the required states in `design/PRD.md`:
/// `queued`, `checking`, `downloading_metadata`, `downloading`, `seeding`,
/// `paused`, `completed`, `error`, `network_blocked`, `storage_error`,
/// `tracker_error`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentState {
    Queued,
    Checking,
    DownloadingMetadata,
    Downloading,
    Seeding,
    Paused,
    Completed,
    Error,
    NetworkBlocked,
    StorageError,
    TrackerError,
}

impl TorrentState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Checking => "checking",
            Self::DownloadingMetadata => "downloading_metadata",
            Self::Downloading => "downloading",
            Self::Seeding => "seeding",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Error => "error",
            Self::NetworkBlocked => "network_blocked",
            Self::StorageError => "storage_error",
            Self::TrackerError => "tracker_error",
        }
    }

    /// True if this is an active (non-stopped) downloading or seeding state.
    pub fn is_active(self) -> bool {
        matches!(
            self,
            Self::DownloadingMetadata | Self::Downloading | Self::Seeding | Self::Checking
        )
    }

    /// True if this state indicates an error condition.
    pub fn is_error(self) -> bool {
        matches!(
            self,
            Self::Error | Self::NetworkBlocked | Self::StorageError | Self::TrackerError
        )
    }
}

impl std::fmt::Display for TorrentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// File priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum FilePriority {
    /// Do not download.
    Unwanted,
    Low,
    #[default]
    Normal,
    High,
}

impl FilePriority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unwanted => "unwanted",
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
        }
    }

    /// Numeric weight for scheduling (higher = more priority).
    pub fn weight(self) -> i32 {
        match self {
            Self::Unwanted => -1,
            Self::Low => 0,
            Self::Normal => 1,
            Self::High => 2,
        }
    }
}

/// A file within a torrent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentFile {
    pub index: usize,
    pub path: String,
    pub length: u64,
    pub bytes_completed: u64,
    pub priority: FilePriority,
    pub wanted: bool,
}

/// Per-torrent health signal exposed in summaries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct TorrentHealth {
    pub score: u8,
    pub bars: u8,
    pub label: HealthLabel,
    pub availability_score: u8,
    pub throughput_score: u8,
    pub peer_score: u8,
    pub stability_score: u8,
    pub discovery_score: u8,
    pub reasons: Vec<String>,
}

impl TorrentHealth {
    pub fn complete() -> Self {
        Self {
            score: 100,
            bars: 5,
            label: HealthLabel::Complete,
            availability_score: 100,
            throughput_score: 100,
            peer_score: 100,
            stability_score: 100,
            discovery_score: 100,
            reasons: vec!["torrent is complete".to_string()],
        }
    }

    pub fn unknown() -> Self {
        Self {
            score: 0,
            bars: 0,
            label: HealthLabel::Unknown,
            availability_score: 0,
            throughput_score: 0,
            peer_score: 0,
            stability_score: 0,
            discovery_score: 0,
            reasons: vec!["health not yet measured".to_string()],
        }
    }
}

/// Human-readable health label used by the UI and API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthLabel {
    Unknown,
    NetworkBlocked,
    Stalled,
    Critical,
    Poor,
    Fair,
    Good,
    Excellent,
    Paused,
    Complete,
}

/// Summary of a torrent exposed in the torrent list and details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentSummary {
    pub info_hash: InfoHash,
    pub name: String,
    pub state: TorrentState,
    pub total_length: u64,
    pub bytes_completed: u64,
    pub uploaded: u64,
    pub downloaded: u64,
    pub piece_count: usize,
    pub pieces_have: usize,
    pub piece_length: u64,
    pub private: bool,
    pub labels: Vec<String>,
    pub download_dir: Option<String>,
    /// Per-torrent download limit in bytes/sec (0 = unlimited).
    pub download_limit: u64,
    /// Per-torrent upload limit in bytes/sec (0 = unlimited).
    pub upload_limit: u64,
    pub rate_down: u64,
    pub rate_up: u64,
    /// Number of peer workers currently active for this torrent.
    pub active_peer_workers: usize,
    /// Number of currently known peer candidates for this torrent.
    pub known_peers: usize,
    pub ratio: f64,
    pub queue_position: Option<usize>,
    pub date_added: u64,
    pub date_completed: Option<u64>,
    pub health: TorrentHealth,
}

impl TorrentSummary {
    pub fn progress(&self) -> f64 {
        if self.total_length == 0 {
            return 0.0;
        }
        (self.bytes_completed as f64) / (self.total_length as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&TorrentState::DownloadingMetadata).unwrap(),
            "\"downloading_metadata\""
        );
        assert_eq!(
            serde_json::to_string(&TorrentState::NetworkBlocked).unwrap(),
            "\"network_blocked\""
        );
    }

    #[test]
    fn priority_weights() {
        assert!(FilePriority::High.weight() > FilePriority::Normal.weight());
        assert_eq!(FilePriority::Unwanted.weight(), -1);
    }
}
