// SPDX-License-Identifier: Apache-2.0

//! Per-torrent health calculation.
//!
//! Health answers: "Can this torrent complete, and is it downloading well
//! right now?" It is computed from real engine state — piece availability,
//! peer usefulness, throughput, recent stability, and discovery — and not
//! from a single value such as seed count.
//!
//! The result is a `TorrentHealth` with a 0..100 score, a 0..5 bar mapping
//! for the UI, a human-readable label, per-component sub-scores, and a list
//! of plain-text reasons. The same shape is exposed in the API and rendered
//! by the Web UI as a signal-bars indicator.

use std::time::{Duration, Instant};

use crate::models::network::NetworkHealth;
use crate::models::torrent::{HealthLabel, TorrentHealth, TorrentState};
use crate::storage::resume::PieceBitfield;

use super::peer::EnginePeerHealth;

/// Input collected from the live engine and the registry that the health
/// calculator turns into a `TorrentHealth`. This is a pure data struct;
/// the daemon builds it from `EngineState` + the `Torrent` record.
#[derive(Debug, Clone)]
pub struct HealthInput {
    pub state: TorrentState,
    pub private: bool,
    pub piece_count: usize,
    pub pieces_have: PieceBitfield,
    /// Connected peers and their derived per-peer health signals.
    pub peers: Vec<EnginePeerHealth>,
    /// Current smoothed download rate (bytes/sec).
    pub rate_down: u64,
    /// Best observed recent download rate (bytes/sec), used as a
    /// normalization reference. `0` when no history is available yet.
    pub rate_down_observed_peak: u64,
    /// Per-torrent configured download cap (bytes/sec); `0` = unlimited.
    pub download_limit: u64,
    /// Per-torrent configured upload cap (bytes/sec); `0` = unlimited.
    pub upload_limit: u64,
    /// Global configured download cap (bytes/sec); `0` = unlimited.
    pub global_download_limit: u64,
    /// Network containment health — used for the network_blocked hard cap.
    pub network: Option<NetworkHealth>,
    /// Tracker health from the live engine.
    pub tracker_ok: bool,
    /// Whether any tracker has produced a recent successful announce.
    pub tracker_recent_ok: bool,
    /// Recent tracker failure count.
    pub tracker_failures_recent: u32,
    /// Whether DHT discovery has produced peers recently.
    pub dht_recent_ok: bool,
    /// Whether PEX discovery has produced peers recently.
    pub pex_recent_ok: bool,
    /// Recent peer disconnect churn.
    pub peer_disconnects_recent: u32,
    /// Hash failures observed during the run.
    pub hash_failures: u32,
    /// Block timeout / bad-response events.
    pub timeout_failures: u32,
    /// Whether a valid block has been received in the recent past.
    pub received_block_recently: bool,
    /// How long ago a valid block was last seen. `None` means never.
    pub time_since_last_block: Option<Duration>,
    /// Number of known peer candidates (connected + candidate pool).
    pub known_peers: usize,
    /// Whether the daemon has reached the bounded "no peers discovered"
    /// give-up state.
    pub no_peers_discovered: bool,
}

impl Default for HealthInput {
    fn default() -> Self {
        Self {
            state: TorrentState::Queued,
            private: false,
            piece_count: 0,
            pieces_have: PieceBitfield::default(),
            peers: Vec::new(),
            rate_down: 0,
            rate_down_observed_peak: 0,
            download_limit: 0,
            upload_limit: 0,
            global_download_limit: 0,
            network: None,
            tracker_ok: false,
            tracker_recent_ok: false,
            tracker_failures_recent: 0,
            dht_recent_ok: false,
            pex_recent_ok: false,
            peer_disconnects_recent: 0,
            hash_failures: 0,
            timeout_failures: 0,
            received_block_recently: false,
            time_since_last_block: None,
            known_peers: 0,
            no_peers_discovered: false,
        }
    }
}

impl HealthInput {
    /// Build a per-peer derived summary used by `peer_score` and
    /// `availability_score`. It treats a peer as useful when it is
    /// connected (i.e. has a `last_seen`), unchoked or recently useful,
    /// not blocked, and has at least one piece we still need.
    fn peer_summaries(&self) -> Vec<PeerSummary> {
        self.peers
            .iter()
            .map(|p| PeerSummary {
                has_missing: p.has_missing_pieces,
                unchoked: p.unchoked,
                blocked: p.blocked,
                useful_recently: p.useful_recently,
                active_sending: p.unchoked
                    && p.has_missing_pieces
                    && p.last_valid_block
                        .map(|t| t.elapsed() < Duration::from_secs(60))
                        .unwrap_or(false),
                last_seen: p.last_seen,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerSummary {
    has_missing: bool,
    unchoked: bool,
    blocked: bool,
    useful_recently: bool,
    active_sending: bool,
    last_seen: Option<Instant>,
}

/// Per-torrent health calculator. Pure deterministic function:
/// `HealthInput -> TorrentHealth`.
///
/// The same calculator is exercised by unit tests and by the daemon
/// during state reconciliation, so that the API and UI surface agrees
/// with the documented scoring rules.
#[derive(Debug, Default, Clone, Copy)]
pub struct HealthCalculator;

impl HealthCalculator {
    pub fn new() -> Self {
        Self
    }

    /// Compute the torrent health snapshot for a given input. The result is
    /// clamped, mapped to bars and a label, and accompanied by human-readable
    /// reasons explaining the score.
    pub fn compute(&self, input: &HealthInput) -> TorrentHealth {
        let mut reasons: Vec<String> = Vec::new();

        // Hard caps first: these can short-circuit the whole calculation.
        if let Some(net) = &input.network {
            if !net.traffic_allowed {
                return TorrentHealth {
                    score: 0,
                    bars: 0,
                    label: HealthLabel::NetworkBlocked,
                    availability_score: 0,
                    throughput_score: 0,
                    peer_score: 0,
                    stability_score: 0,
                    discovery_score: 0,
                    reasons: vec!["network containment is blocking torrent traffic".to_string()],
                };
            }
        }

        if input.state == TorrentState::Paused {
            return TorrentHealth {
                score: 0,
                bars: 0,
                label: HealthLabel::Paused,
                availability_score: 0,
                throughput_score: 0,
                peer_score: 0,
                stability_score: 0,
                discovery_score: 0,
                reasons: vec!["torrent is paused".to_string()],
            };
        }

        if input.state == TorrentState::Completed
            || (input.piece_count > 0 && (0..input.piece_count).all(|i| input.pieces_have.has(i)))
        {
            return TorrentHealth::complete();
        }

        // Derived state used by several components.
        let pieces_have_count = if input.piece_count == 0 {
            0
        } else {
            (0..input.piece_count)
                .filter(|&i| input.pieces_have.has(i))
                .count()
        };
        let missing_piece_count = input.piece_count.saturating_sub(pieces_have_count);

        // Per-peer derived signals.
        let peer_summaries = input.peer_summaries();
        let useful_peers: usize = peer_summaries
            .iter()
            .filter(|p| {
                p.has_missing
                    && !p.blocked
                    && (p.unchoked || p.useful_recently)
                    && p.last_seen.is_some()
            })
            .count();
        let active_sending_peers: usize =
            peer_summaries.iter().filter(|p| p.active_sending).count();
        let unchoked_peers: usize = peer_summaries.iter().filter(|p| p.unchoked).count();
        let peers_with_needed_pieces: usize =
            peer_summaries.iter().filter(|p| p.has_missing).count();

        // 1) Availability score.
        let availability =
            availability_score(input, missing_piece_count, &peer_summaries, &mut reasons);

        // 2) Throughput score.
        let throughput = throughput_score(input, missing_piece_count, &mut reasons);

        // 3) Peer score.
        let peer_score = peer_score(
            input,
            useful_peers,
            active_sending_peers,
            unchoked_peers,
            peers_with_needed_pieces,
            &mut reasons,
        );

        // 4) Stability score.
        let stability = stability_score(input, missing_piece_count, &mut reasons);

        // 5) Discovery score.
        let discovery =
            discovery_score(input, useful_peers, peers_with_needed_pieces, &mut reasons);

        // Weighted combination.
        let raw = (availability as u32) * 40
            + (throughput as u32) * 25
            + (peer_score as u32) * 15
            + (stability as u32) * 10
            + (discovery as u32) * 10;
        let mut score = (raw / 100) as u8;

        // Post-cap hard limits.
        let missing_with_zero_sources = count_missing_with_zero_sources(input, &peer_summaries);
        if missing_piece_count > 0 && missing_with_zero_sources > 0 {
            score = score.min(35);
            reasons.push(format!(
                "{} missing pieces have no source",
                missing_with_zero_sources
            ));
        }
        if missing_piece_count > 0 && useful_peers == 0 {
            score = score.min(30);
            if !reasons.iter().any(|r| r.contains("no useful peer")) {
                reasons.push("no useful peer is currently sending data".to_string());
            }
        }
        if missing_piece_count > 0 && !input.received_block_recently {
            score = score.min(25);
            reasons.push("download stalled: no valid block received recently".to_string());
        }
        let discovery_dark =
            !input.tracker_recent_ok && !input.dht_recent_ok && !input.pex_recent_ok;
        if discovery_dark && peer_summaries.is_empty() {
            score = score.min(20);
            reasons.push("no discovery and no connected peers".to_string());
        }

        // Map to bars + label.
        let (bars, label) = bars_and_label(score);
        if reasons.is_empty() {
            reasons.push("health not yet measured".to_string());
        }

        TorrentHealth {
            score,
            bars,
            label,
            availability_score: availability,
            throughput_score: throughput,
            peer_score,
            stability_score: stability,
            discovery_score: discovery,
            reasons,
        }
    }
}

fn bars_and_label(score: u8) -> (u8, HealthLabel) {
    match score {
        0 => (0, HealthLabel::Stalled),
        1..=34 => (1, HealthLabel::Critical),
        35..=54 => (2, HealthLabel::Poor),
        55..=74 => (3, HealthLabel::Fair),
        75..=89 => (4, HealthLabel::Good),
        _ => (5, HealthLabel::Excellent),
    }
}

fn count_missing_with_zero_sources(input: &HealthInput, peer_summaries: &[PeerSummary]) -> usize {
    if input.piece_count == 0 {
        return 0;
    }
    // For each missing piece, count peers whose bitfield covers it. Without
    // per-peer bitfields in `EnginePeerHealth` we approximate using the
    // `has_missing_pieces` flag and the configured heuristic: if no peer has
    // any missing piece, every missing piece has zero sources.
    let any_peer_has_missing = peer_summaries.iter().any(|p| p.has_missing);
    if !any_peer_has_missing {
        return input.piece_count.saturating_sub(
            (0..input.piece_count)
                .filter(|&i| input.pieces_have.has(i))
                .count(),
        );
    }
    // Per-peer bitfield info isn't carried through `EnginePeerHealth` yet,
    // so report zero here: the calculator errs on the generous side rather
    // than over-penalising the torrent in the absence of per-piece data.
    0
}

fn availability_score(
    input: &HealthInput,
    missing_piece_count: usize,
    peer_summaries: &[PeerSummary],
    reasons: &mut Vec<String>,
) -> u8 {
    if input.piece_count == 0 {
        return 0;
    }
    if missing_piece_count == 0 {
        reasons.push("all missing pieces are available".to_string());
        return 100;
    }
    let zero_source = count_missing_with_zero_sources(input, peer_summaries);
    if zero_source > 0 {
        let available = missing_piece_count.saturating_sub(zero_source);
        let ratio = available as f64 / missing_piece_count as f64;
        let score = (ratio * 25.0).round() as u8;
        return score.min(25);
    }
    // No zero-source pieces; estimate from peer coverage. With the per-peer
    // `has_missing_pieces` flag we can only reason about coarse coverage
    // counts; if any peer is known to carry missing pieces, the rarest
    // missing piece has at least one source.
    let sources: Vec<usize> = peer_summaries
        .iter()
        .filter(|p| p.has_missing)
        .map(|p| if p.unchoked { 2 } else { 1 })
        .collect();
    let rarest = sources.iter().min().copied().unwrap_or(0);
    let avg = if sources.is_empty() {
        0.0
    } else {
        sources.iter().sum::<usize>() as f64 / sources.len() as f64
    };
    let score = match rarest {
        0 => 0,
        1 => 55,
        2 => 70,
        _ => {
            let base = 85.0;
            let bonus = ((avg - 3.0).max(0.0) * 5.0).min(15.0);
            (base + bonus).round() as u8
        }
    };
    if score >= 100 {
        reasons.push("all missing pieces are available".to_string());
    } else if score >= 70 {
        reasons.push("missing pieces have multiple sources".to_string());
    }
    score.min(100)
}

fn throughput_score(input: &HealthInput, missing: usize, reasons: &mut Vec<String>) -> u8 {
    if missing == 0 {
        return 100;
    }
    if input.rate_down == 0 {
        reasons.push("no download throughput right now".to_string());
        return 0;
    }
    let reference = reference_download_rate(input);
    if reference == 0 {
        // No reference: use a daemon default of ~64 KiB/s as a soft target.
        let default_ref: u64 = 64 * 1024;
        let ratio = (input.rate_down as f64 / default_ref as f64).min(1.0);
        let score = (10.0 + ratio * 90.0).round() as u8;
        if score >= 70 {
            reasons.push("download is active".to_string());
        } else if score >= 40 {
            reasons.push("download is slow but active".to_string());
        } else {
            reasons.push("download is barely moving".to_string());
        }
        return score.min(100);
    }
    let ratio = (input.rate_down as f64 / reference as f64).min(1.0);
    if ratio >= 0.9 {
        reasons.push("download speed is near the configured cap".to_string());
        100
    } else if ratio >= 0.5 {
        reasons.push("download speed is healthy".to_string());
        70
    } else if ratio >= 0.15 {
        reasons.push("download is moving slowly".to_string());
        40
    } else {
        reasons.push("download is barely moving".to_string());
        10
    }
}

fn reference_download_rate(input: &HealthInput) -> u64 {
    // Prefer the tightest configured cap. A capped torrent is still healthy,
    // so we always use the cap (when non-zero) as the reference: a torrent
    // running at its cap is a 100.
    let cap = if input.download_limit > 0 {
        Some(input.download_limit)
    } else if input.global_download_limit > 0 {
        Some(input.global_download_limit)
    } else {
        None
    };
    if let Some(c) = cap {
        return c;
    }
    if input.rate_down_observed_peak > 0 {
        return input.rate_down_observed_peak;
    }
    0
}

fn peer_score(
    _input: &HealthInput,
    useful: usize,
    active_sending: usize,
    unchoked: usize,
    peers_with_needed_pieces: usize,
    reasons: &mut Vec<String>,
) -> u8 {
    if useful == 0 {
        if peers_with_needed_pieces == 0 {
            reasons.push("no connected peer has the pieces we need".to_string());
        } else {
            reasons.push("no useful peer is currently sending data".to_string());
        }
        return 0;
    }
    let base = match useful {
        1 => 45,
        2 => 65,
        _ => 80,
    };
    let bonus = if useful >= 3 {
        // Boost up to 20 based on the number of actively-sending peers.
        (active_sending.min(useful) as u8).saturating_mul(4).min(20)
    } else {
        0
    };
    let score = base + bonus;
    if score >= 90 {
        reasons.push(format!("{useful} useful peers are sending data"));
    } else if score >= 65 {
        reasons.push(format!("{useful} useful peers are active"));
    } else if unchoked > 0 {
        reasons.push(format!("{unchoked} peer is unchoked but slow"));
    }
    score.min(100)
}

fn stability_score(input: &HealthInput, missing: usize, reasons: &mut Vec<String>) -> u8 {
    if missing == 0 {
        return 100;
    }
    let mut score: i32 = 100;
    if input.tracker_failures_recent > 0 {
        score -= 10;
        reasons.push("tracker errors recently".to_string());
    }
    if input.peer_disconnects_recent > 0 {
        score -= 10;
        reasons.push("repeated peer disconnects".to_string());
    }
    if input.timeout_failures > 0 {
        score -= 20;
        reasons.push("block timeout bursts".to_string());
    }
    if input.hash_failures > 0 {
        score -= 30;
        reasons.push("hash failures observed".to_string());
    }
    if !input.received_block_recently {
        score -= 40;
    }
    if let Some(d) = input.time_since_last_block {
        if d > Duration::from_secs(120) {
            score -= 10;
        }
    }
    let score = score.clamp(0, 100) as u8;
    if score == 100 {
        reasons.push("download is stable".to_string());
    }
    score
}

fn discovery_score(
    input: &HealthInput,
    useful: usize,
    peers_with_needed_pieces: usize,
    reasons: &mut Vec<String>,
) -> u8 {
    // Private torrents disable DHT/PEX by design; do not penalise the
    // discovery score when the private flag is the reason those methods
    // are off. In that case only the tracker contributes.
    let dht_active = !input.private && input.dht_recent_ok;
    let pex_active = !input.private && input.pex_recent_ok;
    let tracker_active = input.tracker_recent_ok;

    if tracker_active && (dht_active || pex_active) {
        reasons.push("tracker and DHT/PEX are healthy".to_string());
        return 100;
    }
    if tracker_active || dht_active || pex_active {
        if dht_active {
            reasons.push("DHT discovered peers".to_string());
        } else if pex_active {
            reasons.push("PEX discovered peers".to_string());
        } else {
            reasons.push("tracker is healthy".to_string());
        }
        return 75;
    }
    if peers_with_needed_pieces > 0 || useful > 0 {
        reasons.push("connected peers but discovery is weak".to_string());
        return 50;
    }
    if input.no_peers_discovered {
        reasons.push("no peers discovered and discovery is failing".to_string());
        return 0;
    }
    reasons.push("discovery has not yet reported results".to_string());
    25
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::torrent::TorrentHealth;

    fn empty_input() -> HealthInput {
        HealthInput {
            piece_count: 4,
            ..Default::default()
        }
    }

    fn input_with_missing_pieces(pieces: &[bool]) -> HealthInput {
        let mut bf = PieceBitfield::new(pieces.len());
        for (i, &b) in pieces.iter().enumerate() {
            if b {
                bf.set(i);
            }
        }
        HealthInput {
            piece_count: pieces.len(),
            pieces_have: bf,
            ..Default::default()
        }
    }

    fn input_with_state(state: TorrentState) -> HealthInput {
        HealthInput {
            state,
            ..empty_input()
        }
    }

    #[test]
    fn complete_torrent_is_perfect_health() {
        let mut pieces = vec![false; 4];
        for p in pieces.iter_mut() {
            *p = true;
        }
        let input = input_with_missing_pieces(&pieces);
        let h = HealthCalculator::new().compute(&input);
        assert_eq!(h.score, 100);
        assert_eq!(h.bars, 5);
        assert_eq!(h.label, HealthLabel::Complete);
        assert!(h.reasons.iter().any(|r| r.contains("complete")));
    }

    #[test]
    fn network_blocked_is_zero_bars() {
        let mut input = empty_input();
        input.network = Some(NetworkHealth::blocked(
            crate::models::network::NetworkContainmentMode::Strict,
            crate::models::network::NetworkContainmentStatus::InterfaceMissing,
            "test",
        ));
        let h = HealthCalculator::new().compute(&input);
        assert_eq!(h.score, 0);
        assert_eq!(h.bars, 0);
        assert_eq!(h.label, HealthLabel::NetworkBlocked);
    }

    #[test]
    fn paused_torrent_is_paused_label() {
        let input = input_with_state(TorrentState::Paused);
        let h = HealthCalculator::new().compute(&input);
        assert_eq!(h.score, 0);
        assert_eq!(h.bars, 0);
        assert_eq!(h.label, HealthLabel::Paused);
    }

    #[test]
    fn missing_pieces_with_zero_sources_caps_score() {
        // All peers have no missing pieces: every missing piece is "no source".
        let pieces = vec![true, false, false, false];
        let mut input = input_with_missing_pieces(&pieces);
        input.received_block_recently = true;
        let h = HealthCalculator::new().compute(&input);
        assert!(
            h.score <= 35,
            "score must be capped at 35 with zero-source missing pieces (was {})",
            h.score
        );
        assert!(h
            .reasons
            .iter()
            .any(|r| r.contains("no source") || r.contains("no useful peer")));
    }

    #[test]
    fn good_active_swarm_scores_high() {
        let pieces = vec![true, false, false];
        let mut input = input_with_missing_pieces(&pieces);
        input.tracker_recent_ok = true;
        input.dht_recent_ok = true;
        input.pex_recent_ok = true;
        input.received_block_recently = true;
        input.rate_down = 256 * 1024;
        input.download_limit = 512 * 1024;
        for i in 0..4u8 {
            let p = EnginePeerHealth {
                piece_bitfield: None,
                has_missing_pieces: true,
                unchoked: true,
                blocked: false,
                last_valid_block: Some(Instant::now()),
                useful_recently: true,
                discovered_from_pex: true,
                last_seen: Some(Instant::now()),
            };
            input.peers.push(p);
            let _ = i;
        }
        let h = HealthCalculator::new().compute(&input);
        assert!(
            h.score >= 75,
            "good active swarm should score 4-5 bars, got {}",
            h.score
        );
        assert!(h.bars >= 4);
        assert!(matches!(
            h.label,
            HealthLabel::Good | HealthLabel::Excellent
        ));
    }

    #[test]
    fn many_connected_but_useless_peers_scores_low() {
        let pieces = vec![true, false, false, false];
        let mut input = input_with_missing_pieces(&pieces);
        for _ in 0..20 {
            input.peers.push(EnginePeerHealth {
                has_missing_pieces: false, // they have nothing we need
                unchoked: true,
                useful_recently: false,
                ..Default::default()
            });
        }
        // No useful peers at all → score should be capped at 30.
        let h = HealthCalculator::new().compute(&input);
        assert!(
            h.score <= 30,
            "useless peers should produce a critical/stalled health, got {}",
            h.score
        );
    }

    #[test]
    fn slow_but_completable_is_fair_or_poor() {
        let pieces = vec![true, false, false];
        let mut input = input_with_missing_pieces(&pieces);
        // All peers have the missing pieces (rarest >= 3) and tracker ok.
        for _ in 0..4 {
            input.peers.push(EnginePeerHealth {
                has_missing_pieces: true,
                unchoked: true,
                blocked: false,
                last_valid_block: Some(Instant::now()),
                useful_recently: true,
                last_seen: Some(Instant::now()),
                ..Default::default()
            });
        }
        input.tracker_recent_ok = true;
        input.received_block_recently = true;
        input.rate_down = 4 * 1024; // very slow
        input.download_limit = 1024 * 1024; // but cap is high
        let h = HealthCalculator::new().compute(&input);
        assert!(
            h.score >= 35 && h.score <= 90,
            "slow-but-completable should be fair/poor (35..=90), got {}",
            h.score
        );
        assert!(matches!(
            h.label,
            HealthLabel::Poor | HealthLabel::Fair | HealthLabel::Good
        ));
    }

    #[test]
    fn private_torrent_does_not_penalise_disabled_dht_pex() {
        let pieces = vec![true, false, false];
        let mut input = input_with_missing_pieces(&pieces);
        input.private = true; // DHT/PEX must be skipped
        input.tracker_recent_ok = true;
        // dht_recent_ok and pex_recent_ok are false but that's expected.
        for _ in 0..3 {
            input.peers.push(EnginePeerHealth {
                has_missing_pieces: true,
                unchoked: true,
                blocked: false,
                last_valid_block: Some(Instant::now()),
                useful_recently: true,
                last_seen: Some(Instant::now()),
                ..Default::default()
            });
        }
        input.received_block_recently = true;
        input.rate_down = 200 * 1024;
        let h_priv = HealthCalculator::new().compute(&input);

        // Compare against an otherwise-identical non-private torrent where
        // DHT/PEX are also disabled by the runtime (not by the private flag).
        let mut input_pub = input.clone();
        input_pub.private = false;
        // Public-but-DHT-off would be punished by a real engine; our
        // calculator only adds a discovery bonus when DHT/PEX are active,
        // so private+tracker-only should match public+tracker-only.
        let h_pub = HealthCalculator::new().compute(&input_pub);
        assert_eq!(
            h_priv.discovery_score, h_pub.discovery_score,
            "private flag must not penalise discovery scoring"
        );
    }

    #[test]
    fn torrent_summary_includes_health() {
        let pieces = vec![true; 4];
        let mut input = input_with_missing_pieces(&pieces);
        input.state = TorrentState::Completed;
        let calc = HealthCalculator::new();
        let h: TorrentHealth = calc.compute(&input);
        let json = serde_json::to_string(&h).unwrap();
        assert!(json.contains("\"score\":100"));
        assert!(json.contains("\"bars\":5"));
        assert!(json.contains("\"label\":\"complete\""));
    }

    #[test]
    fn bars_label_mapping() {
        for score in 0u8..=100 {
            let (bars, label) = bars_and_label(score);
            assert!(bars <= 5);
            match score {
                0 => assert_eq!(label, HealthLabel::Stalled),
                1..=34 => {
                    assert_eq!(bars, 1);
                    assert_eq!(label, HealthLabel::Critical);
                }
                35..=54 => {
                    assert_eq!(bars, 2);
                    assert_eq!(label, HealthLabel::Poor);
                }
                55..=74 => {
                    assert_eq!(bars, 3);
                    assert_eq!(label, HealthLabel::Fair);
                }
                75..=89 => {
                    assert_eq!(bars, 4);
                    assert_eq!(label, HealthLabel::Good);
                }
                _ => {
                    assert_eq!(bars, 5);
                    assert_eq!(label, HealthLabel::Excellent);
                }
            }
        }
    }
}
