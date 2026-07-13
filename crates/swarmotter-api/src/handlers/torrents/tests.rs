// SPDX-License-Identifier: Apache-2.0

use super::*;
use super::{add::*, bulk::*, query::*};
use std::collections::BTreeMap;
use swarmotter_core::models::torrent::TorrentHealth;

fn hash(seed: u8) -> InfoHash {
    // Build a distinct 40-char hex string per seed.
    let mut hex = String::with_capacity(40);
    for i in 0..40 {
        hex.push(std::char::from_digit(((seed.wrapping_add(i as u8)) % 16) as u32, 16).unwrap());
    }
    InfoHash::from_hex(&hex).expect("valid hex")
}

#[allow(clippy::too_many_arguments)]
fn summary_with(
    seed: u8,
    name: &str,
    state: TorrentState,
    health_score: u8,
    health_label: HealthLabel,
    rate_down: u64,
    rate_up: u64,
    known_peers: usize,
    active_peer_workers: usize,
    labels: Vec<&str>,
    download_dir: Option<&str>,
) -> TorrentSummary {
    TorrentSummary {
        info_hash: hash(seed),
        name: name.to_string(),
        state,
        error: None,
        total_length: 1000,
        bytes_completed: 500,
        uploaded: 0,
        downloaded: 0,
        seeding: swarmotter_core::ratio::TorrentSeeding::default(),
        seeding_status: swarmotter_core::models::torrent::SeedingStatus::NotEligible,
        effective_ratio_limit: None,
        effective_idle_limit: None,
        piece_count: 1,
        pieces_have: 1,
        piece_length: 1000,
        private: false,
        labels: labels.into_iter().map(String::from).collect(),
        download_dir: download_dir.map(String::from),
        download_limit: 0,
        upload_limit: 0,
        autopilot_mode_override: None,
        rate_down,
        rate_up,
        active_peer_workers,
        known_peers,
        ratio: 0.0,
        queue_position: None,
        date_added: 0,
        date_completed: None,
        health: TorrentHealth {
            score: health_score,
            bars: 3,
            label: health_label,
            availability_score: 50,
            throughput_score: 50,
            peer_score: 50,
            stability_score: 50,
            discovery_score: 50,
            reasons: Vec::new(),
        },
    }
}

fn minimal_summary() -> TorrentSummary {
    summary_with(
        1,
        "alpha.iso",
        TorrentState::Downloading,
        80,
        HealthLabel::Good,
        0,
        0,
        0,
        0,
        vec![],
        None,
    )
}

#[test]
fn normalize_filter_text_trims_and_lowercases() {
    assert_eq!(normalize_filter_text("  HELLO World "), "hello world");
    assert_eq!(normalize_filter_text(""), "");
}

#[test]
fn compare_strings_ignores_case_and_whitespace() {
    assert_eq!(compare_strings("Alpha", "alpha"), Ordering::Equal);
    assert_eq!(compare_strings("  beta", "Beta"), Ordering::Equal);
    assert!(compare_strings("alpha", "beta").is_lt());
}

#[test]
fn compare_f64_handles_nan_as_equal() {
    assert_eq!(compare_f64(1.0, 2.0), Ordering::Less);
    assert_eq!(compare_f64(2.0, 1.0), Ordering::Greater);
    assert_eq!(compare_f64(1.0, 1.0), Ordering::Equal);
    assert_eq!(compare_f64(f64::NAN, 1.0), Ordering::Equal);
}

#[test]
fn token_set_splits_trims_lowercases_and_drops_empty() {
    let s = token_set(Some(" Alpha , beta , , gamma "));
    assert_eq!(s.len(), 3);
    assert!(s.contains("alpha"));
    assert!(s.contains("beta"));
    assert!(s.contains("gamma"));
}

#[test]
fn token_set_none_yields_empty_set() {
    assert!(token_set(None).is_empty());
    assert!(token_set(Some("")).is_empty());
}

#[test]
fn health_label_key_covers_all_variants() {
    assert_eq!(health_label_key(&HealthLabel::Unknown), "unknown");
    assert_eq!(
        health_label_key(&HealthLabel::NetworkBlocked),
        "network_blocked"
    );
    assert_eq!(health_label_key(&HealthLabel::Stalled), "stalled");
    assert_eq!(health_label_key(&HealthLabel::Critical), "critical");
    assert_eq!(health_label_key(&HealthLabel::Poor), "poor");
    assert_eq!(health_label_key(&HealthLabel::Fair), "fair");
    assert_eq!(health_label_key(&HealthLabel::Good), "good");
    assert_eq!(health_label_key(&HealthLabel::Excellent), "excellent");
    assert_eq!(health_label_key(&HealthLabel::Paused), "paused");
    assert_eq!(health_label_key(&HealthLabel::Complete), "complete");
}

#[test]
fn storage_root_key_falls_back_to_default_for_missing_or_empty() {
    let mut s = minimal_summary();
    assert_eq!(storage_root_key(&s), "default");
    s.download_dir = Some("".into());
    assert_eq!(storage_root_key(&s), "default");
    s.download_dir = Some("   ".into());
    assert_eq!(storage_root_key(&s), "default");
    s.download_dir = Some("/data/torrents".into());
    assert_eq!(storage_root_key(&s), "/data/torrents");
}

#[test]
fn label_keys_yields_unlabeled_when_empty() {
    let s = minimal_summary();
    assert_eq!(label_keys(&s), vec!["unlabeled"]);
}

#[test]
fn label_keys_normalizes_and_filters_empty() {
    let mut s = minimal_summary();
    s.labels = vec!["  Linux  ".into(), "ISO".into(), "   ".into()];
    let keys = label_keys(&s);
    assert_eq!(keys, vec!["linux", "iso"]);
}

#[test]
fn peer_count_uses_max_of_active_and_known() {
    let mut s = minimal_summary();
    s.active_peer_workers = 3;
    s.known_peers = 5;
    assert_eq!(peer_count(&s), 5);
    s.active_peer_workers = 7;
    s.known_peers = 5;
    assert_eq!(peer_count(&s), 7);
    s.active_peer_workers = 0;
    s.known_peers = 0;
    assert_eq!(peer_count(&s), 0);
}

#[test]
fn performance_keys_includes_active_when_state_is_active() {
    let s = summary_with(
        1,
        "a",
        TorrentState::Downloading,
        90,
        HealthLabel::Good,
        1000,
        0,
        2,
        2,
        vec![],
        None,
    );
    let keys = performance_keys(&s);
    assert!(keys.contains(&"active"));
    assert!(keys.contains(&"transferring"));
    assert!(keys.contains(&"has_peers"));
}

#[test]
fn performance_keys_flags_stalled_downloading_with_zero_rate() {
    let s = summary_with(
        1,
        "a",
        TorrentState::Downloading,
        90,
        HealthLabel::Good,
        0,
        0,
        0,
        0,
        vec![],
        None,
    );
    let keys = performance_keys(&s);
    assert!(keys.contains(&"stalled"));
    assert!(keys.contains(&"no_peers"));
}

#[test]
fn performance_keys_flags_unhealthy_low_score_non_complete() {
    let s = summary_with(
        1,
        "a",
        TorrentState::Downloading,
        10,
        HealthLabel::Critical,
        0,
        0,
        0,
        0,
        vec![],
        None,
    );
    let keys = performance_keys(&s);
    assert!(keys.contains(&"unhealthy"));
}

#[test]
fn performance_keys_complete_state_does_not_flag_unhealthy() {
    let s = summary_with(
        1,
        "a",
        TorrentState::Seeding,
        10,
        HealthLabel::Complete,
        0,
        0,
        0,
        0,
        vec![],
        None,
    );
    let keys = performance_keys(&s);
    assert!(keys.contains(&"complete"));
    assert!(!keys.contains(&"unhealthy"));
}

#[test]
fn increment_count_adds_and_aggregates() {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    increment_count(&mut counts, "alpha");
    increment_count(&mut counts, "alpha");
    increment_count(&mut counts, "beta");
    assert_eq!(counts.get("alpha"), Some(&2));
    assert_eq!(counts.get("beta"), Some(&1));
    assert_eq!(counts.len(), 2);
}

#[test]
fn display_group_label_titlecases_underscore_parts() {
    assert_eq!(display_group_label("network_blocked"), "Network Blocked");
    assert_eq!(display_group_label("stalled"), "Stalled");
    assert_eq!(display_group_label(""), "");
}

#[test]
fn torrent_matches_search_matches_name_hash_state_health_label_storage_and_labels() {
    let s = summary_with(
        42,
        "Linux Distro",
        TorrentState::Downloading,
        80,
        HealthLabel::Good,
        0,
        0,
        0,
        0,
        vec!["alpha", "beta"],
        Some("/data/dl"),
    );
    assert!(torrent_matches_search(&s, "linux distro"));
    assert!(torrent_matches_search(&s, "alpha"));
    assert!(torrent_matches_search(&s, "downloading"));
    assert!(torrent_matches_search(&s, "good"));
    assert!(torrent_matches_search(&s, "data"));
    // Hash lookup
    let hex = s.info_hash.to_hex();
    assert!(torrent_matches_search(&s, &hex[..8]));
    // Mismatch
    assert!(!torrent_matches_search(&s, "nonexistent"));
}

#[test]
fn torrent_list_groups_returns_empty_when_group_by_is_none() {
    let s = minimal_summary();
    assert!(torrent_list_groups(&[s], None).is_empty());
}

#[test]
fn torrent_list_groups_groups_by_state() {
    let rows = vec![
        summary_with(
            1,
            "a",
            TorrentState::Downloading,
            80,
            HealthLabel::Good,
            0,
            0,
            0,
            0,
            vec![],
            None,
        ),
        summary_with(
            2,
            "b",
            TorrentState::Downloading,
            80,
            HealthLabel::Good,
            0,
            0,
            0,
            0,
            vec![],
            None,
        ),
        summary_with(
            3,
            "c",
            TorrentState::Seeding,
            100,
            HealthLabel::Complete,
            0,
            0,
            0,
            0,
            vec![],
            None,
        ),
    ];
    let groups = torrent_list_groups(&rows, Some(TorrentListGroupBy::State));
    assert_eq!(groups.len(), 2);
    let by_key: BTreeMap<String, usize> = groups.iter().map(|g| (g.key.clone(), g.count)).collect();
    assert_eq!(by_key.get("downloading").copied().unwrap(), 2);
    assert_eq!(by_key.get("seeding").copied().unwrap(), 1);
}

#[test]
fn torrent_list_groups_groups_by_health() {
    let rows = vec![
        summary_with(
            1,
            "a",
            TorrentState::Downloading,
            80,
            HealthLabel::Good,
            0,
            0,
            0,
            0,
            vec![],
            None,
        ),
        summary_with(
            2,
            "b",
            TorrentState::Downloading,
            10,
            HealthLabel::Critical,
            0,
            0,
            0,
            0,
            vec![],
            None,
        ),
    ];
    let groups = torrent_list_groups(&rows, Some(TorrentListGroupBy::Health));
    let by_key: BTreeMap<String, usize> = groups.iter().map(|g| (g.key.clone(), g.count)).collect();
    assert_eq!(by_key.get("good").copied().unwrap(), 1);
    assert_eq!(by_key.get("critical").copied().unwrap(), 1);
}

#[test]
fn torrent_list_groups_groups_by_label_including_unlabeled() {
    let mut a = minimal_summary();
    a.labels = vec!["Linux".into(), "ISO".into()];
    let mut b = minimal_summary();
    b.labels = vec!["Linux".into()];
    let c = minimal_summary();
    let groups = torrent_list_groups(&[a, b, c], Some(TorrentListGroupBy::Label));
    let by_key: BTreeMap<String, usize> = groups.iter().map(|g| (g.key.clone(), g.count)).collect();
    assert_eq!(by_key.get("linux").copied().unwrap(), 2);
    assert_eq!(by_key.get("iso").copied().unwrap(), 1);
    assert_eq!(by_key.get("unlabeled").copied().unwrap(), 1);
}

#[test]
fn torrent_list_groups_groups_by_storage_root() {
    let mut a = minimal_summary();
    a.download_dir = Some("/data/a".into());
    let mut b = minimal_summary();
    b.download_dir = Some("/data/a".into());
    let c = minimal_summary();
    let groups = torrent_list_groups(&[a, b, c], Some(TorrentListGroupBy::StorageRoot));
    let by_key: BTreeMap<String, usize> = groups.iter().map(|g| (g.key.clone(), g.count)).collect();
    assert_eq!(by_key.get("/data/a").copied().unwrap(), 2);
    assert_eq!(by_key.get("default").copied().unwrap(), 1);
}

#[test]
fn torrent_list_groups_groups_by_performance() {
    // Active + transferring torrent.
    let active = summary_with(
        1,
        "a",
        TorrentState::Downloading,
        90,
        HealthLabel::Good,
        1024,
        0,
        2,
        2,
        vec![],
        None,
    );
    // Paused torrent: not active, not transferring, no peers, not stalled.
    let paused = summary_with(
        2,
        "b",
        TorrentState::Paused,
        90,
        HealthLabel::Paused,
        0,
        0,
        0,
        0,
        vec![],
        None,
    );
    let groups = torrent_list_groups(&[active, paused], Some(TorrentListGroupBy::Performance));
    let by_key: BTreeMap<String, usize> = groups.iter().map(|g| (g.key.clone(), g.count)).collect();
    assert_eq!(by_key.get("active").copied().unwrap(), 1);
    assert_eq!(by_key.get("transferring").copied().unwrap(), 1);
    assert_eq!(by_key.get("has_peers").copied().unwrap(), 1);
    assert_eq!(by_key.get("no_peers").copied().unwrap(), 1);
}

#[test]
fn torrent_list_counts_includes_all_dimensions() {
    let rows = vec![
        summary_with(
            1,
            "a",
            TorrentState::Downloading,
            80,
            HealthLabel::Good,
            0,
            0,
            1,
            1,
            vec!["Linux"],
            Some("/data"),
        ),
        summary_with(
            2,
            "b",
            TorrentState::Seeding,
            100,
            HealthLabel::Complete,
            0,
            0,
            0,
            0,
            vec!["Linux"],
            Some("/data"),
        ),
    ];
    let counts = torrent_list_counts(&rows);
    assert_eq!(counts.states.get("downloading"), Some(&1));
    assert_eq!(counts.states.get("seeding"), Some(&1));
    assert_eq!(counts.health.get("good"), Some(&1));
    assert_eq!(counts.health.get("complete"), Some(&1));
    assert_eq!(counts.labels.get("linux"), Some(&2));
    assert_eq!(counts.storage_roots.get("/data"), Some(&2));
}

#[test]
fn sort_torrent_rows_uses_stable_secondary_name_order() {
    let mut a = minimal_summary();
    a.name = "charlie".into();
    a.rate_down = 100;
    let mut b = minimal_summary();
    b.name = "ALPHA".into();
    b.rate_down = 100;
    let mut rows = [a, b];
    sort_torrent_rows(
        &mut rows,
        TorrentListSort::DownRate,
        TorrentListDirection::Asc,
    );
    // Equal rate; secondary sort is case-insensitive by name.
    assert_eq!(rows[0].name, "ALPHA");
    assert_eq!(rows[1].name, "charlie");
}

#[test]
fn sort_torrent_rows_desc_reverses() {
    let mut rows = vec![
        summary_with(
            1,
            "alpha",
            TorrentState::Downloading,
            80,
            HealthLabel::Good,
            0,
            0,
            0,
            0,
            vec![],
            None,
        ),
        summary_with(
            2,
            "beta",
            TorrentState::Downloading,
            80,
            HealthLabel::Good,
            0,
            0,
            0,
            0,
            vec![],
            None,
        ),
    ];
    sort_torrent_rows(&mut rows, TorrentListSort::Name, TorrentListDirection::Desc);
    assert_eq!(rows[0].name, "beta");
    assert_eq!(rows[1].name, "alpha");
}

#[test]
fn compare_torrent_rows_covers_all_sort_keys() {
    let mut a = minimal_summary();
    a.name = "alpha".into();
    a.state = TorrentState::Downloading;
    a.total_length = 100;
    a.bytes_completed = 50;
    a.rate_down = 1000;
    a.rate_up = 200;
    a.ratio = 1.0;
    a.date_added = 10;
    a.date_completed = Some(20);
    a.queue_position = Some(0);
    a.active_peer_workers = 3;
    a.known_peers = 5;
    a.health.score = 75;
    a.health.label = HealthLabel::Good;

    let mut b = a.clone();
    b.name = "Beta".into();
    b.state = TorrentState::Seeding;
    b.total_length = 200;
    b.bytes_completed = 150; // progress 0.75 > 0.5
    b.rate_down = 2000;
    b.rate_up = 400;
    b.ratio = 2.0;
    b.date_added = 20;
    b.date_completed = Some(10);
    b.queue_position = Some(1);
    b.active_peer_workers = 5;
    b.known_peers = 3;
    b.health.score = 25;
    b.health.label = HealthLabel::Critical;

    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Name).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::State).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Health).is_ne());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::HealthScore).is_gt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Progress).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Size).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::DownRate).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::UpRate).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Ratio).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Peers).is_eq());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Added).is_lt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Completed).is_gt());
    assert!(compare_torrent_rows(&a, &b, TorrentListSort::Queue).is_lt());
}

#[test]
fn add_options_resolves_paused_only() {
    let opts = add_options(None, Some(true), None, None).unwrap();
    assert!(opts.paused);
}

#[test]
fn add_options_resolves_start_behavior_only() {
    let opts = add_options(None, None, Some(StartBehavior::Paused), None).unwrap();
    assert!(opts.paused);
    let opts = add_options(None, None, Some(StartBehavior::Start), None).unwrap();
    assert!(!opts.paused);
}

#[test]
fn add_options_picks_query_value_when_body_absent() {
    let q = AddTorrentQuery {
        paused: Some(false),
        start_behavior: None,
    };
    let opts = add_options(None, None, None, Some(&q)).unwrap();
    assert!(!opts.paused);
}

#[test]
fn add_options_rejects_conflicting_paused() {
    let q = AddTorrentQuery {
        paused: Some(true),
        start_behavior: None,
    };
    let err = add_options(None, Some(false), None, Some(&q)).unwrap_err();
    assert!(err.to_string().contains("conflict"));
}

#[test]
fn add_options_rejects_conflicting_start_behavior() {
    let q = AddTorrentQuery {
        paused: None,
        start_behavior: Some(StartBehavior::Start),
    };
    let err = add_options(None, None, Some(StartBehavior::Paused), Some(&q)).unwrap_err();
    assert!(err.to_string().contains("conflict"));
}

#[test]
fn add_options_rejects_paused_vs_start_behavior_mismatch() {
    let err = add_options(None, Some(true), Some(StartBehavior::Start), None).unwrap_err();
    assert!(err.to_string().contains("conflict"));
}

#[test]
fn add_failure_captures_code_and_message() {
    let f = add_failure("magnet", 3, CoreError::InvalidArgument("bad magnet".into()));
    assert_eq!(f.kind, "magnet");
    assert_eq!(f.index, 3);
    assert_eq!(f.code, "invalid_argument");
    assert!(f.message.contains("bad magnet"));
}
