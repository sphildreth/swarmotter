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

/// Live peer scheduling diagnostics for understanding why discovered peers
/// are or are not being used for download work.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerSchedulerDiagnostics {
    pub discovered_peers: usize,
    pub eligible_peers: usize,
    pub filtered_peers: usize,
    pub failed_peers: usize,
    pub backed_off_peers: usize,
    pub peer_worker_limit: usize,
    pub parallel_candidates: usize,
    pub parallel_workers_started: usize,
    pub serial_peer_active: bool,
    pub last_reason: Option<String>,
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
    pub peer_scheduler: Option<PeerSchedulerDiagnostics>,
    pub useful_peers: Option<usize>,
    pub choked_peers: Option<usize>,
    pub unchoked_peers: Option<usize>,
    pub recent_peer_failures: Option<u32>,
    pub recent_tracker_failures: Option<u32>,
    pub tracker_ok: bool,
    pub tracker_message: Option<String>,
    pub last_announce: Option<u64>,
    pub tracker_last_ok_seconds_ago: Option<u64>,
    pub dht_discovery_ok: Option<bool>,
    pub dht_last_seen_seconds_ago: Option<u64>,
    pub pex_discovery_ok: Option<bool>,
    pub pex_last_seen_seconds_ago: Option<u64>,
    pub private: bool,
}

/// Telemetry-like input consumed by the autopilot analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotInput {
    #[serde(default)]
    pub state: TorrentState,
    #[serde(default)]
    pub rate_down: u64,
    #[serde(default)]
    pub rate_up: u64,
    #[serde(default)]
    pub rate_down_observed_peak: u64,
    #[serde(default)]
    pub download_limit: u64,
    #[serde(default)]
    pub piece_count: usize,
    #[serde(default)]
    pub pieces_have: usize,
    #[serde(default)]
    pub known_peers: usize,
    #[serde(default)]
    pub useful_peers: Option<usize>,
    #[serde(default)]
    pub active_peer_workers: usize,
    #[serde(default)]
    pub discovered_peers: Option<usize>,
    #[serde(default)]
    pub eligible_peers: Option<usize>,
    #[serde(default)]
    pub peer_worker_limit: Option<usize>,
    #[serde(default)]
    pub backed_off_peers: Option<usize>,
    #[serde(default)]
    pub tracker_ok: bool,
    #[serde(default)]
    pub tracker_recent_ok_seconds_ago: Option<u64>,
    #[serde(default)]
    pub tracker_failures_recent: u32,
    #[serde(default)]
    pub dht_discovery_ok: Option<bool>,
    #[serde(default)]
    pub dht_last_seen_seconds_ago: Option<u64>,
    #[serde(default)]
    pub pex_discovery_ok: Option<bool>,
    #[serde(default)]
    pub pex_last_seen_seconds_ago: Option<u64>,
    #[serde(default)]
    pub no_progress_seconds: Option<u64>,
    #[serde(default)]
    pub peer_failures_recent: Option<u32>,
    #[serde(default)]
    pub serial_peer_active: bool,
    #[serde(default)]
    pub network_traffic_allowed: Option<bool>,
}

impl Default for AutopilotInput {
    fn default() -> Self {
        Self {
            state: TorrentState::Queued,
            rate_down: 0,
            rate_up: 0,
            rate_down_observed_peak: 0,
            download_limit: 0,
            piece_count: 0,
            pieces_have: 0,
            known_peers: 0,
            useful_peers: None,
            active_peer_workers: 0,
            discovered_peers: None,
            eligible_peers: None,
            peer_worker_limit: None,
            backed_off_peers: None,
            tracker_ok: false,
            tracker_recent_ok_seconds_ago: None,
            tracker_failures_recent: 0,
            dht_discovery_ok: None,
            dht_last_seen_seconds_ago: None,
            pex_discovery_ok: None,
            pex_last_seen_seconds_ago: None,
            no_progress_seconds: None,
            peer_failures_recent: None,
            serial_peer_active: false,
            network_traffic_allowed: None,
        }
    }
}

impl AutopilotInput {
    pub fn is_download_active(&self) -> bool {
        self.state.is_active() && self.piece_count > self.pieces_have
    }
}

/// A computed snapshot of conditions seen by the autopilot for one torrent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotSnapshot {
    pub slow: bool,
    pub causes: Vec<SlowCause>,
    pub state: TorrentState,
    pub rate_down: u64,
    pub rate_up: u64,
    pub rate_down_observed_peak: u64,
    pub download_limit: u64,
    pub known_peers: usize,
    pub useful_peers: usize,
    pub active_peer_workers: usize,
    pub discovered_peers: usize,
    pub eligible_peers: usize,
    pub peer_worker_limit: usize,
    pub backed_off_peers: usize,
    pub tracker_ok: bool,
    pub tracker_recent_ok_seconds_ago: Option<u64>,
    pub tracker_failures_recent: u32,
    pub discovery_ok: bool,
    pub no_progress_seconds: Option<u64>,
    pub peer_failures_recent: Option<u32>,
    pub serial_peer_active: bool,
    pub network_traffic_allowed: Option<bool>,
}

/// A serializable action decision returned from the analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotDecision {
    pub apply: bool,
    pub action: Option<AutopilotAction>,
    pub reasons: Vec<AutopilotReason>,
    pub snapshot: AutopilotSnapshot,
}

/// A single decision for an observed cause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotAction {
    pub kind: AutopilotActionKind,
    pub rationale: String,
    #[serde(default)]
    pub suggested_peer_workers: Option<usize>,
    #[serde(default)]
    pub suggested_download_limit: Option<u64>,
}

/// Kinds of safe autopilot actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutopilotActionKind {
    IncreasePeerWorkers,
    ExpandDiscovery,
    RelaxPeerBackoff,
    ReleaseQueueSlot,
    RaiseDownloadCeiling,
}

/// Structured reason for why a torrent is slow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutopilotReason {
    #[serde(default)]
    pub cause: Option<SlowCause>,
    pub message: String,
}

/// Canonical slow causes for explainability tooling.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SlowCause {
    NetworkContainmentBlocked,
    NoKnownPeers,
    NoUsefulPeers,
    PeerWorkersAtCap,
    ThroughputBelowReference,
    DiscoveryBlackout,
    NoRecentProgress,
    TrackerIssues,
    PeerFailureStorm,
    PeerBackoffSaturation,
}

impl AutopilotSnapshot {
    pub fn is_slow(&self) -> bool {
        self.slow || !self.causes.is_empty()
    }
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
