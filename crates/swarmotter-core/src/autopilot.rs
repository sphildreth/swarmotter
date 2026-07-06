// SPDX-License-Identifier: Apache-2.0

//! Pure, deterministic autopilot analyzer for torrent throughput diagnosis.
//!
//! This module is intentionally free of network probing and side effects.
//! It evaluates telemetry-like input and decides why a torrent is slow and
//! whether to emit an apply-ready tuning action.

use serde::{Deserialize, Serialize};

use crate::models::stats::{
    AutopilotAction, AutopilotActionKind, AutopilotDecision, AutopilotInput, AutopilotReason,
    AutopilotSnapshot, SlowCause,
};
use crate::models::torrent::TorrentState;

const NO_PROGRESS_SECONDS: u64 = 30;
const DISCOVERY_STALE_SECONDS: u64 = 120;
const PEER_FAILURE_STORM: u32 = 4;
const LOW_THROUGHPUT_FLOOR_BPS: u64 = 8 * 1024;

/// Autopilot operating mode.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum AutopilotMode {
    /// Autopilot decisions are disabled.
    Disabled,
    /// Observe and report causes without applying recommendations.
    #[default]
    Observe,
    /// Return apply-ready recommendations from deterministic heuristics.
    Act,
}

/// Top-level autopilot settings exposed from config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AutopilotConfig {
    #[serde(default)]
    pub mode: AutopilotMode,
}

impl Default for AutopilotConfig {
    fn default() -> Self {
        Self {
            mode: AutopilotMode::Observe,
        }
    }
}

impl AutopilotConfig {
    pub fn is_observe_or_act(&self) -> bool {
        matches!(self.mode, AutopilotMode::Observe | AutopilotMode::Act)
    }
}

/// Deterministic analyzer used by core components and tests.
#[derive(Debug, Default)]
pub struct AutopilotAnalyzer;

impl AutopilotAnalyzer {
    pub fn new() -> Self {
        Self
    }

    pub fn analyze(&self, input: &AutopilotInput, mode: AutopilotMode) -> AutopilotDecision {
        if mode == AutopilotMode::Disabled {
            return AutopilotDecision {
                apply: false,
                action: None,
                reasons: vec![AutopilotReason {
                    cause: None,
                    message: "autopilot disabled".to_string(),
                }],
                snapshot: Self::snapshot(input, Vec::new()),
            };
        }

        if input.state == TorrentState::NetworkBlocked
            || input.network_traffic_allowed == Some(false)
        {
            let causes = vec![SlowCause::NetworkContainmentBlocked];
            return AutopilotDecision {
                apply: false,
                action: None,
                reasons: vec![AutopilotReason {
                    cause: Some(SlowCause::NetworkContainmentBlocked),
                    message: "network containment is blocking torrent data-plane traffic"
                        .to_string(),
                }],
                snapshot: Self::snapshot(input, causes),
            };
        }

        if !input.is_download_active() {
            return AutopilotDecision {
                apply: false,
                action: None,
                reasons: vec![AutopilotReason {
                    cause: None,
                    message: "torrent is not in active download path".to_string(),
                }],
                snapshot: Self::snapshot(input, Vec::new()),
            };
        }

        let causes = Self::detect_causes(input);
        if causes.is_empty() {
            return AutopilotDecision {
                apply: false,
                action: None,
                reasons: vec![AutopilotReason {
                    cause: None,
                    message: "no slow conditions detected".to_string(),
                }],
                snapshot: Self::snapshot(input, Vec::new()),
            };
        }

        let snapshot = Self::snapshot(input, causes.clone());
        let reasons = causes
            .iter()
            .map(|cause| AutopilotReason {
                cause: Some(*cause),
                message: match cause {
                    SlowCause::NetworkContainmentBlocked => {
                        "network containment is blocking torrent data-plane traffic".to_string()
                    }
                    SlowCause::NoKnownPeers => "no known peers available".to_string(),
                    SlowCause::NoUsefulPeers => "no useful peers currently available".to_string(),
                    SlowCause::PeerWorkersAtCap => "peer workers are capped".to_string(),
                    SlowCause::ThroughputBelowReference => {
                        "download rate is low relative to observed cap".to_string()
                    }
                    SlowCause::DiscoveryBlackout => {
                        "discovery channels are not producing fresh peer candidates".to_string()
                    }
                    SlowCause::NoRecentProgress => "no recent block progress".to_string(),
                    SlowCause::TrackerIssues => "tracker health is degraded".to_string(),
                    SlowCause::PeerFailureStorm => {
                        "peer failures suggest unstable candidates".to_string()
                    }
                    SlowCause::PeerBackoffSaturation => {
                        "peer backoff is saturating the candidate pool".to_string()
                    }
                },
            })
            .collect();

        if mode == AutopilotMode::Observe {
            return AutopilotDecision {
                apply: false,
                action: None,
                reasons,
                snapshot,
            };
        }

        let action = causes
            .iter()
            .find_map(|cause| Self::action_for_cause(*cause, input))
            .map(|mut action| {
                action.rationale = format!("autopilot recommend: {}", action.rationale);
                action
            });
        let apply = action.is_some();

        AutopilotDecision {
            apply,
            action,
            reasons,
            snapshot,
        }
    }

    fn detect_causes(input: &AutopilotInput) -> Vec<SlowCause> {
        let discovered_peers = input.discovered_peers.unwrap_or_default();
        let peer_worker_limit = input.peer_worker_limit.unwrap_or_default();
        let backed_off_peers = input.backed_off_peers.unwrap_or_default();
        let peer_failures = input.peer_failures_recent.unwrap_or_default();
        let observed_peak = input.rate_down_observed_peak.max(LOW_THROUGHPUT_FLOOR_BPS);
        let reference_throughput = if input.download_limit > 0 {
            input.download_limit
        } else {
            observed_peak
        };

        let mut causes = Vec::new();

        if input.known_peers == 0 {
            causes.push(SlowCause::NoKnownPeers);
        }

        if input.useful_peers.unwrap_or_default() == 0 && input.known_peers > 0 {
            causes.push(SlowCause::NoUsefulPeers);
        }

        if peer_worker_limit > 0
            && input.active_peer_workers >= peer_worker_limit
            && discovered_peers > peer_worker_limit
        {
            causes.push(SlowCause::PeerWorkersAtCap);
        }

        if no_recent_progress(input) {
            causes.push(SlowCause::NoRecentProgress);
        } else if input.rate_down < reference_throughput / 2 && input.rate_down > 0 {
            causes.push(SlowCause::ThroughputBelowReference);
        }

        if discovery_is_stale(input) {
            causes.push(SlowCause::DiscoveryBlackout);
        }

        if tracker_is_degraded(input) {
            causes.push(SlowCause::TrackerIssues);
        }

        if peer_failures >= PEER_FAILURE_STORM {
            causes.push(SlowCause::PeerFailureStorm);
        }

        if peer_worker_limit > 0 && backed_off_peers >= peer_worker_limit / 2 {
            causes.push(SlowCause::PeerBackoffSaturation);
        }

        causes.sort_unstable();
        causes.dedup();
        causes
    }

    fn action_for_cause(cause: SlowCause, input: &AutopilotInput) -> Option<AutopilotAction> {
        match cause {
            SlowCause::NoKnownPeers => Some(AutopilotAction {
                kind: AutopilotActionKind::ExpandDiscovery,
                rationale: "broaden discovery sources while keeping active constraints".into(),
                suggested_peer_workers: Some(input.active_peer_workers.saturating_add(1)),
                suggested_download_limit: None,
            }),
            SlowCause::NoUsefulPeers | SlowCause::PeerWorkersAtCap => Some(AutopilotAction {
                kind: AutopilotActionKind::IncreasePeerWorkers,
                rationale: "increase peer worker capacity to improve candidate utilization".into(),
                suggested_peer_workers: Some(input.active_peer_workers.saturating_add(1)),
                suggested_download_limit: None,
            }),
            SlowCause::NoRecentProgress => Some(AutopilotAction {
                kind: AutopilotActionKind::ReleaseQueueSlot,
                rationale: "temporarily release the active queue slot for another eligible torrent"
                    .into(),
                suggested_peer_workers: None,
                suggested_download_limit: None,
            }),
            SlowCause::PeerFailureStorm => Some(AutopilotAction {
                kind: AutopilotActionKind::RelaxPeerBackoff,
                rationale: "reduce backoff pressure on unstable peers".into(),
                suggested_peer_workers: None,
                suggested_download_limit: None,
            }),
            SlowCause::ThroughputBelowReference => Some(AutopilotAction {
                kind: AutopilotActionKind::RaiseDownloadCeiling,
                rationale: "raise per-torrent ceiling when no configured cap exists".into(),
                suggested_peer_workers: None,
                suggested_download_limit: None,
            }),
            SlowCause::DiscoveryBlackout | SlowCause::TrackerIssues => Some(AutopilotAction {
                kind: AutopilotActionKind::ExpandDiscovery,
                rationale: "favor non-tracker discovery paths".into(),
                suggested_peer_workers: None,
                suggested_download_limit: None,
            }),
            SlowCause::PeerBackoffSaturation => Some(AutopilotAction {
                kind: AutopilotActionKind::RelaxPeerBackoff,
                rationale: "decrease candidate backoff saturation".into(),
                suggested_peer_workers: Some(input.active_peer_workers.saturating_add(1)),
                suggested_download_limit: None,
            }),
            SlowCause::NetworkContainmentBlocked => None,
        }
    }

    fn snapshot(input: &AutopilotInput, causes: Vec<SlowCause>) -> AutopilotSnapshot {
        let discovered = input.discovered_peers.unwrap_or_default();
        let eligible = input.eligible_peers.unwrap_or_default();
        let peer_worker_limit = input.peer_worker_limit.unwrap_or_default();
        let backed_off = input.backed_off_peers.unwrap_or_default();
        let discovery_ok = matches!(
            (input.dht_discovery_ok, input.pex_discovery_ok),
            (Some(true), _) | (_, Some(true))
        );

        AutopilotSnapshot {
            slow: !causes.is_empty(),
            causes,
            state: input.state,
            rate_down: input.rate_down,
            rate_up: input.rate_up,
            rate_down_observed_peak: input.rate_down_observed_peak,
            download_limit: input.download_limit,
            known_peers: input.known_peers,
            useful_peers: input.useful_peers.unwrap_or_default(),
            active_peer_workers: input.active_peer_workers,
            discovered_peers: discovered,
            eligible_peers: eligible,
            peer_worker_limit,
            backed_off_peers: backed_off,
            tracker_ok: input.tracker_ok,
            tracker_recent_ok_seconds_ago: input.tracker_recent_ok_seconds_ago,
            tracker_failures_recent: input.tracker_failures_recent,
            discovery_ok,
            no_progress_seconds: input.no_progress_seconds,
            peer_failures_recent: input.peer_failures_recent,
            serial_peer_active: input.serial_peer_active,
            network_traffic_allowed: input.network_traffic_allowed,
        }
    }
}

fn no_progress(input: &AutopilotInput) -> bool {
    input.no_progress_seconds.unwrap_or_default() >= NO_PROGRESS_SECONDS
}

fn no_recent_progress(input: &AutopilotInput) -> bool {
    no_progress(input) && input.rate_down == 0
}

fn discovery_is_stale(input: &AutopilotInput) -> bool {
    let has_no_discovery_channels = input.dht_discovery_ok == Some(false)
        && input.pex_discovery_ok == Some(false)
        && !input.tracker_ok;
    let discovery_last_seen_stale = match input
        .dht_last_seen_seconds_ago
        .or(input.pex_last_seen_seconds_ago)
    {
        Some(age) => age >= DISCOVERY_STALE_SECONDS,
        None => false,
    };
    has_no_discovery_channels && discovery_last_seen_stale
}

fn tracker_is_degraded(input: &AutopilotInput) -> bool {
    let tracker_aging = match input.tracker_recent_ok_seconds_ago {
        Some(age) => age >= DISCOVERY_STALE_SECONDS,
        None => false,
    };
    let tracker_error_rate = input.tracker_failures_recent >= PEER_FAILURE_STORM;
    tracker_aging || tracker_error_rate
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disabled_mode_is_noop() {
        let input = AutopilotInput {
            state: TorrentState::Downloading,
            piece_count: 1,
            known_peers: 12,
            rate_down: 0,
            ..Default::default()
        };
        let decision = AutopilotAnalyzer::new().analyze(&input, AutopilotMode::Disabled);
        assert!(!decision.apply);
        assert!(decision.action.is_none());
        assert_eq!(decision.reasons[0].message, "autopilot disabled");
    }

    #[test]
    fn observe_mode_only_reports_causes() {
        let input = AutopilotInput {
            state: TorrentState::Downloading,
            piece_count: 1,
            rate_down: 10,
            rate_down_observed_peak: 1000,
            known_peers: 0,
            ..Default::default()
        };
        let decision = AutopilotAnalyzer::new().analyze(&input, AutopilotMode::Observe);
        assert!(!decision.apply);
        assert!(decision.action.is_none());
        assert!(!decision.snapshot.causes.is_empty());
    }

    #[test]
    fn act_mode_returns_action_for_known_cause() {
        let input = AutopilotInput {
            state: TorrentState::Downloading,
            piece_count: 10,
            known_peers: 4,
            useful_peers: Some(0),
            active_peer_workers: 2,
            ..Default::default()
        };
        let decision = AutopilotAnalyzer::new().analyze(&input, AutopilotMode::Act);
        assert!(decision.apply);
        assert!(matches!(
            decision.action.as_ref().expect("action").kind,
            AutopilotActionKind::IncreasePeerWorkers
                | AutopilotActionKind::RelaxPeerBackoff
                | AutopilotActionKind::ReleaseQueueSlot
                | AutopilotActionKind::ExpandDiscovery
                | AutopilotActionKind::RaiseDownloadCeiling
        ));
    }

    #[test]
    fn no_progress_without_download_activity_is_noop() {
        let input = AutopilotInput {
            rate_down: 0,
            state: TorrentState::Paused,
            ..Default::default()
        };
        let decision = AutopilotAnalyzer::new().analyze(&input, AutopilotMode::Act);
        assert!(!decision.apply);
        assert_eq!(
            decision.reasons[0].message,
            "torrent is not in active download path"
        );
    }

    #[test]
    fn no_peers_prefers_discovery_action() {
        let input = AutopilotInput {
            state: TorrentState::Downloading,
            piece_count: 1,
            known_peers: 0,
            ..Default::default()
        };
        let decision = AutopilotAnalyzer::new().analyze(&input, AutopilotMode::Act);
        assert_eq!(
            decision.action.as_ref().expect("action").kind,
            AutopilotActionKind::ExpandDiscovery
        );
    }
}
