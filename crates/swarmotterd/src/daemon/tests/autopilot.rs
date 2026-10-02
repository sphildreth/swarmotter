// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn runtime_config_sweeps_existing_completed_torrents_when_selfish() {
    let mut cfg = Config::default();
    cfg.torrent.selfish = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "selfish-sweep.bin",
        b"already complete payload",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Completed;
    torrent.date_completed = Some(2);
    for piece in 0..meta.piece_count() {
        torrent.progress.have_piece(piece);
    }
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            bytes_completed: meta.total_length,
            finished: true,
            ..Default::default()
        })),
    );
    runtime.rate_samples.write().await.insert(
        hash,
        RateSample {
            downloaded: 1,
            uploaded: 0,
            rate_down: 1,
            rate_up: 0,
            last_download_at: Some(Instant::now()),
            last_upload_at: None,
            no_download_since: None,
            at: Instant::now(),
            peak_rate_down: 1,
            peak_rate_up: 0,
        },
    );

    runtime.apply_runtime_config_fields().await;

    assert!(
        runtime.registry.lock().await.get(&hash).is_none(),
        "selfish mode should remove completed torrents already in the registry"
    );
    assert_eq!(runtime.queue.lock().await.position(&hash), None);
    assert!(!runtime.engine_states.read().await.contains_key(&hash));
    assert!(!runtime.rate_samples.read().await.contains_key(&hash));
}

#[tokio::test]
async fn torrent_stats_includes_live_engine_diagnostics() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "diag.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(torrent).unwrap();
    let now = Instant::now();
    let mut peer_health = HashMap::new();
    peer_health.insert(
        "127.0.0.1:6881".parse().unwrap(),
        EnginePeerHealth {
            has_missing_pieces: true,
            unchoked: true,
            useful_recently: true,
            last_valid_block: Some(now),
            last_seen: Some(now),
            ..Default::default()
        },
    );
    peer_health.insert(
        "127.0.0.1:6882".parse().unwrap(),
        EnginePeerHealth {
            has_missing_pieces: true,
            last_seen: Some(now),
            ..Default::default()
        },
    );
    peer_health.insert(
        "127.0.0.1:6883".parse().unwrap(),
        EnginePeerHealth {
            has_missing_pieces: true,
            unchoked: true,
            useful_recently: true,
            last_seen: Some(now - Duration::from_secs(31)),
            ..Default::default()
        },
    );
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            active_peers: 4,
            peers: vec![
                swarmotter_core::peer::PeerAddr::from_socket_addr(
                    "127.0.0.1:6881".parse().unwrap(),
                ),
                swarmotter_core::peer::PeerAddr::from_socket_addr(
                    "127.0.0.1:6882".parse().unwrap(),
                ),
            ],
            peer_health,
            tracker_ok: true,
            tracker_message: Some("ok".into()),
            last_announce: Some(123),
            tracker_failures_recent: 3,
            dht_discovery_ok: true,
            pex_discovery_ok: true,
            peer_disconnects_recent: 2,
            dht_last_seen: Some(now - Duration::from_secs(11)),
            pex_last_seen: Some(now - Duration::from_secs(13)),
            tracker_last_ok: Some(now - Duration::from_secs(7)),
            peer_scheduler: PeerSchedulerDiagnostics {
                discovered_peers: 2,
                eligible_peers: 1,
                failed_peers: 1,
                peer_worker_limit: 8,
                parallel_candidates: 1,
                parallel_workers_started: 4,
                serial_peer_active: true,
                last_reason: Some("one eligible peer".into()),
                ..Default::default()
            },
            ..Default::default()
        })),
    );

    runtime.reconcile_engine_progress().await;
    let stats = runtime.torrent_stats(&hash).await.unwrap();

    assert_eq!(stats.info_hash, hash);
    assert_eq!(stats.active_peer_workers, 4);
    assert_eq!(stats.known_peers, 2);
    let scheduler = stats.peer_scheduler.as_ref().unwrap();
    assert_eq!(scheduler.discovered_peers, 2);
    assert_eq!(scheduler.eligible_peers, 1);
    assert_eq!(scheduler.failed_peers, 1);
    assert_eq!(scheduler.peer_worker_limit, 8);
    assert_eq!(scheduler.parallel_candidates, 1);
    assert_eq!(scheduler.parallel_workers_started, 4);
    assert!(scheduler.serial_peer_active);
    assert_eq!(stats.useful_peers, Some(1));
    assert_eq!(stats.unchoked_peers, Some(1));
    assert_eq!(stats.choked_peers, None);
    assert_eq!(stats.recent_peer_failures, Some(2));
    assert_eq!(stats.recent_tracker_failures, Some(3));
    assert!(stats.tracker_ok);
    assert_eq!(stats.tracker_message.as_deref(), Some("ok"));
    assert_eq!(stats.last_announce, Some(123));
    assert_eq!(stats.dht_discovery_ok, Some(true));
    assert_eq!(stats.pex_discovery_ok, Some(true));
    assert!((7..=10).contains(&stats.tracker_last_ok_seconds_ago.unwrap()));
    assert!((11..=14).contains(&stats.dht_last_seen_seconds_ago.unwrap()));
    assert!((13..=16).contains(&stats.pex_last_seen_seconds_ago.unwrap()));

    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.active_peer_workers, 4);
    assert_eq!(summary.known_peers, 2);
}

#[tokio::test]
async fn autopilot_decision_uses_live_engine_telemetry() {
    let mut cfg = Config::default();
    cfg.autopilot.mode = AutopilotMode::Observe;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            tracker_ok: false,
            dht_discovery_ok: false,
            pex_discovery_ok: false,
            dht_last_seen: Some(Instant::now() - Duration::from_secs(180)),
            pex_last_seen: Some(Instant::now() - Duration::from_secs(180)),
            ..Default::default()
        })),
    );
    runtime.rate_samples.write().await.insert(
        hash,
        RateSample {
            downloaded: 0,
            uploaded: 0,
            rate_down: 0,
            rate_up: 0,
            last_download_at: None,
            last_upload_at: None,
            no_download_since: Some(Instant::now() - Duration::from_secs(45)),
            at: Instant::now() - Duration::from_secs(45),
            peak_rate_down: 0,
            peak_rate_up: 0,
        },
    );

    let decision = runtime.torrent_autopilot_decision(&hash).await.unwrap();

    assert!(!decision.apply);
    assert!(decision.snapshot.is_slow());
    assert_eq!(decision.snapshot.network_traffic_allowed, Some(true));
    assert!(decision
        .snapshot
        .causes
        .contains(&swarmotter_core::models::stats::SlowCause::NoKnownPeers));
}

#[tokio::test]
async fn torrent_autopilot_decision_does_not_refresh_unrelated_torrents() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first_bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-one.bin",
        b"autopilot one payload",
        8,
        None,
        false,
    );
    let second_bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-two.bin",
        b"autopilot two payload",
        8,
        None,
        false,
    );
    let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
    let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
    let first_hash = TorrentKey::v1(first.info_hash);
    let second_hash = TorrentKey::v1(second.info_hash);
    {
        let mut reg = runtime.registry.lock().await;
        reg.add(Torrent::new(first, 1)).unwrap();
        reg.add(Torrent::new(second, 2)).unwrap();
    }
    let blocked_state = Arc::new(Mutex::new(EngineState::default()));
    runtime
        .engine_states
        .write()
        .await
        .insert(second_hash, blocked_state.clone());
    let _unrelated_guard = blocked_state.lock().await;

    let decision = tokio::time::timeout(
        Duration::from_millis(100),
        runtime.torrent_autopilot_decision(&first_hash),
    )
    .await
    .expect("single-torrent autopilot decision should not wait on unrelated state")
    .expect("decision");

    assert_eq!(decision.snapshot.state, TorrentState::Queued);
}

#[tokio::test]
async fn torrent_autopilot_decision_recomputes_stale_cached_snapshot() {
    let mut cfg = Config::default();
    cfg.autopilot.mode = AutopilotMode::Observe;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-current.bin",
        b"autopilot current payload",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(torrent).unwrap();
    let stale = AutopilotAnalyzer::new().analyze(
        &AutopilotInput {
            state: TorrentState::Queued,
            ..Default::default()
        },
        AutopilotMode::Observe,
    );
    runtime
        .autopilot_decisions
        .write()
        .await
        .insert(hash, stale);
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            active_peers: 2,
            peers: vec![
                swarmotter_core::peer::PeerAddr::from_socket_addr(
                    "127.0.0.1:6881".parse().unwrap(),
                ),
                swarmotter_core::peer::PeerAddr::from_socket_addr(
                    "127.0.0.1:6882".parse().unwrap(),
                ),
            ],
            peer_scheduler: PeerSchedulerDiagnostics {
                discovered_peers: 2,
                eligible_peers: 2,
                peer_worker_limit: 8,
                parallel_workers_started: 2,
                ..Default::default()
            },
            ..Default::default()
        })),
    );

    let decision = runtime.torrent_autopilot_decision(&hash).await.unwrap();

    assert_eq!(decision.snapshot.state, TorrentState::Downloading);
    assert_eq!(decision.snapshot.known_peers, 2);
    assert_eq!(decision.snapshot.active_peer_workers, 2);
    let cached = runtime
        .autopilot_decisions
        .read()
        .await
        .get(&hash)
        .cloned()
        .unwrap();
    assert_eq!(cached.snapshot.state, TorrentState::Downloading);
}

#[tokio::test]
async fn torrent_autopilot_override_is_persisted_and_used() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-override.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();

    runtime
        .set_torrent_autopilot_mode_override(&hash, Some(AutopilotMode::Disabled))
        .await
        .unwrap();

    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(
        summary.autopilot_mode_override,
        Some(AutopilotMode::Disabled)
    );
    let decision = runtime.torrent_autopilot_decision(&hash).await.unwrap();
    assert_eq!(decision.reasons[0].message, "autopilot disabled");
}

#[tokio::test]
async fn autopilot_act_mode_expands_discovery_through_engine_command() {
    let mut cfg = Config::default();
    cfg.autopilot.mode = AutopilotMode::Act;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-act.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta.clone(), 1))
        .unwrap();
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            tracker_ok: false,
            dht_discovery_ok: false,
            pex_discovery_ok: false,
            ..Default::default()
        })),
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(hash, tx);
    runtime.engine_handles.write().await.insert(
        hash,
        tokio::spawn(async {
            std::future::pending::<()>().await;
        }),
    );

    runtime.refresh_autopilot_decisions(true).await;

    assert!(matches!(rx.try_recv().unwrap(), EngineCommand::Reannounce));
    let decision = runtime
        .autopilot_decisions
        .read()
        .await
        .get(&hash)
        .cloned()
        .unwrap();
    assert!(decision.apply);
    assert!(matches!(
        decision.action.unwrap().kind,
        AutopilotActionKind::ExpandDiscovery
    ));
    runtime.force_stop_engine(&hash).await;
}

#[tokio::test]
async fn autopilot_act_mode_releases_stalled_active_queue_slot() {
    let mut cfg = Config::default();
    cfg.autopilot.mode = AutopilotMode::Act;
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let stalled_bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-stalled.bin",
        b"stalled payload",
        8,
        None,
        false,
    );
    let queued_bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-queued.bin",
        b"queued payload",
        8,
        None,
        false,
    );
    let stalled_meta = swarmotter_core::meta::parse_torrent(&stalled_bytes).unwrap();
    let queued_meta = swarmotter_core::meta::parse_torrent(&queued_bytes).unwrap();
    let stalled_hash = TorrentKey::v1(stalled_meta.info_hash);
    let queued_hash = TorrentKey::v1(queued_meta.info_hash);
    let mut stalled = Torrent::new(stalled_meta.clone(), 1);
    stalled.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(stalled).unwrap();
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(queued_meta, 2))
        .unwrap();
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(stalled_hash);
        queue.add(queued_hash);
    }
    let stalled_since = Instant::now() - Duration::from_secs(45);
    runtime.engine_states.write().await.insert(
        stalled_hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: stalled_meta.piece_count(),
            total_length: stalled_meta.total_length,
            tracker_ok: false,
            dht_discovery_ok: false,
            pex_discovery_ok: false,
            peer_scheduler: PeerSchedulerDiagnostics {
                peer_worker_limit: 1,
                ..Default::default()
            },
            ..Default::default()
        })),
    );
    runtime.rate_samples.write().await.insert(
        stalled_hash,
        RateSample {
            downloaded: 0,
            uploaded: 0,
            rate_down: 0,
            rate_up: 0,
            last_download_at: None,
            last_upload_at: None,
            no_download_since: Some(stalled_since),
            at: stalled_since,
            peak_rate_down: 0,
            peak_rate_up: 0,
        },
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(stalled_hash, tx);
    runtime.engine_handles.write().await.insert(
        stalled_hash,
        tokio::spawn(async {
            std::future::pending::<()>().await;
        }),
    );
    runtime.queue_reconcile.lock().await.scheduled = true;

    tokio::time::timeout(
        Duration::from_millis(100),
        runtime.refresh_autopilot_decisions(true),
    )
    .await
    .expect("autopilot queue-slot release should not wait on a noncooperative engine task");

    let decision = runtime
        .autopilot_decisions
        .read()
        .await
        .get(&stalled_hash)
        .cloned()
        .unwrap();
    assert!(decision
        .snapshot
        .causes
        .contains(&swarmotter_core::models::stats::SlowCause::NoRecentProgress));
    assert!(matches!(
        decision.action.unwrap().kind,
        AutopilotActionKind::ReleaseQueueSlot
    ));
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&stalled_hash)
            .unwrap()
            .state,
        TorrentState::Queued
    );
    assert_eq!(runtime.queue.lock().await.position(&queued_hash), Some(1));
    assert_eq!(runtime.queue.lock().await.position(&stalled_hash), Some(2));
    assert!(runtime
        .engine_retry_after
        .read()
        .await
        .get(&stalled_hash)
        .is_some_and(|retry_at| *retry_at > Instant::now()));
    assert_eq!(runtime.desired_download_hashes().await, vec![queued_hash]);
}

#[tokio::test]
async fn autopilot_act_mode_skips_queue_release_without_eligible_replacement() {
    let mut cfg = Config::default();
    cfg.autopilot.mode = AutopilotMode::Act;
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let stalled_bytes = swarmotter_core::meta::build_single_file_torrent(
        "autopilot-stalled-alone.bin",
        b"stalled payload",
        8,
        None,
        false,
    );
    let stalled_meta = swarmotter_core::meta::parse_torrent(&stalled_bytes).unwrap();
    let stalled_hash = TorrentKey::v1(stalled_meta.info_hash);
    let mut stalled = Torrent::new(stalled_meta.clone(), 1);
    stalled.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(stalled).unwrap();
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(stalled_hash);
    }
    let stalled_since = Instant::now() - Duration::from_secs(45);
    runtime.engine_states.write().await.insert(
        stalled_hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: stalled_meta.piece_count(),
            total_length: stalled_meta.total_length,
            tracker_ok: false,
            dht_discovery_ok: false,
            pex_discovery_ok: false,
            peer_scheduler: PeerSchedulerDiagnostics {
                peer_worker_limit: 1,
                ..Default::default()
            },
            ..Default::default()
        })),
    );
    runtime.rate_samples.write().await.insert(
        stalled_hash,
        RateSample {
            downloaded: 0,
            uploaded: 0,
            rate_down: 0,
            rate_up: 0,
            last_download_at: None,
            last_upload_at: None,
            no_download_since: Some(stalled_since),
            at: stalled_since,
            peak_rate_down: 0,
            peak_rate_up: 0,
        },
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(stalled_hash, tx);
    runtime.engine_handles.write().await.insert(
        stalled_hash,
        tokio::spawn(async {
            std::future::pending::<()>().await;
        }),
    );

    tokio::time::timeout(
        Duration::from_millis(100),
        runtime.refresh_autopilot_decisions(true),
    )
    .await
    .expect("autopilot queue-slot release should skip without a replacement candidate");

    let decision = runtime
        .autopilot_decisions
        .read()
        .await
        .get(&stalled_hash)
        .cloned()
        .unwrap();
    assert!(decision
        .snapshot
        .causes
        .contains(&swarmotter_core::models::stats::SlowCause::NoRecentProgress));
    assert!(matches!(
        decision.action.unwrap().kind,
        AutopilotActionKind::ReleaseQueueSlot
    ));
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&stalled_hash)
            .unwrap()
            .state,
        TorrentState::Downloading
    );
    assert_eq!(runtime.queue.lock().await.position(&stalled_hash), Some(1));
    assert!(runtime
        .engine_handles
        .read()
        .await
        .contains_key(&stalled_hash));
    assert!(runtime
        .engine_retry_after
        .read()
        .await
        .get(&stalled_hash)
        .is_none());
    assert_eq!(runtime.desired_download_hashes().await, vec![stalled_hash]);
}
