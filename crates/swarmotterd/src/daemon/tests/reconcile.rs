// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn torrent_add_publishes_event() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let mut events = runtime.event_broker.subscribe();
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "event-add.bin",
        b"event add payload",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();

    assert_eq!(hash, TorrentKey::v1(meta.info_hash));
    let event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, "torrent_added");
    assert_eq!(event.info_hash.as_deref(), Some(hash.to_locator().as_str()));
    let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
    assert_eq!(payload["info_hash"], hash.to_locator());
    assert_eq!(payload["payload"]["info_hash"], hash.to_locator());
    assert_eq!(payload["payload"]["state"], "paused");
}

#[tokio::test]
async fn reconcile_publishes_completion_events() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.torrent.listen_port = 0;
    cfg.seeding.global_ratio_limit = None;
    cfg.seeding.global_idle_limit = None;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);
    let mut events = runtime.event_broker.subscribe();
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "event-complete.bin",
        b"event complete payload",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(torrent).unwrap();
    let mut pieces_have = swarmotter_core::storage::resume::PieceBitfield::new(meta.piece_count());
    for piece in 0..meta.piece_count() {
        pieces_have.set(piece);
    }
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            piece_count: meta.piece_count(),
            total_length: meta.total_length,
            downloaded: meta.total_length,
            pieces_have,
            finished: true,
            ..Default::default()
        })),
    );

    runtime.reconcile_engine_progress().await;

    let mut kinds = Vec::new();
    let mut final_state = None;
    for _ in 0..6 {
        let event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if event.kind == "torrent_changed" {
            let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
            if payload["payload"]["state"] == "seeding" {
                final_state = Some(TorrentState::Seeding);
            }
        }
        kinds.push(event.kind);
        if final_state.is_some()
            && kinds.iter().any(|kind| kind == "torrent_completed")
            && kinds.iter().any(|kind| kind == "stats_updated")
        {
            break;
        }
    }
    assert!(kinds.iter().any(|kind| kind == "torrent_changed"));
    assert!(kinds.iter().any(|kind| kind == "torrent_completed"));
    assert!(kinds.iter().any(|kind| kind == "stats_updated"));
    assert_eq!(final_state, Some(TorrentState::Seeding));
    assert_eq!(
        runtime.get_torrent(&hash).await.unwrap().state,
        TorrentState::Seeding
    );
    assert!(runtime.seeder_registry.contains(&hash).await);
    runtime.force_stop_engine(&hash).await;
}

#[tokio::test]
async fn reconcile_updates_transfer_rates_and_global_stats() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "rates.bin",
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
    let state = Arc::new(Mutex::new(EngineState {
        piece_count: meta.piece_count(),
        total_length: meta.total_length,
        downloaded: 5_000,
        uploaded: 1_200,
        ..Default::default()
    }));
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, state.clone());
    runtime.rate_samples.write().await.insert(
        hash,
        RateSample {
            downloaded: 1_000,
            uploaded: 200,
            rate_down: 100,
            rate_up: 100,
            last_download_at: None,
            last_upload_at: None,
            no_download_since: None,
            at: Instant::now() - Duration::from_secs(2),
            peak_rate_down: 0,
            peak_rate_up: 0,
        },
    );

    runtime.reconcile_engine_progress().await;
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert!(summary.rate_down > 0);
    assert!(summary.rate_up > 0);
    assert_eq!(summary.downloaded, 5_000);
    assert_eq!(summary.uploaded, 1_200);
    let peak_sample = runtime
        .rate_samples
        .read()
        .await
        .get(&hash)
        .copied()
        .unwrap();
    assert!(peak_sample.peak_rate_down >= summary.rate_down);
    assert!(peak_sample.peak_rate_up >= summary.rate_up);
    assert!(
        peak_sample.peak_rate_down > summary.rate_down,
        "observed instantaneous peak should not be capped to the smoothed rate"
    );

    let stats = runtime.global_stats().await;
    assert_eq!(stats.download_rate, summary.rate_down);
    assert_eq!(stats.upload_rate, summary.rate_up);
    assert_eq!(stats.total_downloaded, 5_000);
    assert_eq!(stats.total_uploaded, 1_200);
}

#[tokio::test]
async fn reconcile_applies_resolved_magnet_metadata_while_engine_runs() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let real_bytes = swarmotter_core::meta::build_single_file_torrent(
        "resolved-magnet.bin",
        b"resolved magnet payload",
        8,
        None,
        false,
    );
    let real_meta = swarmotter_core::meta::parse_torrent(&real_bytes).unwrap();
    let hash = TorrentKey::v1(real_meta.info_hash);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "magnet placeholder",
        b"placeholder",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let mut torrent = Torrent::new(placeholder_meta, 1);
    torrent.state = TorrentState::DownloadingMetadata;
    torrent.needs_metadata = true;
    set_test_v1_magnet_identity(&mut torrent, hash);
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.engine_handles.write().await.insert(
        hash,
        tokio::spawn(async {
            std::future::pending::<()>().await;
        }),
    );

    let mut pieces_have =
        swarmotter_core::storage::resume::PieceBitfield::new(real_meta.piece_count());
    pieces_have.set(0);
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            pieces_have,
            piece_count: real_meta.piece_count(),
            total_length: real_meta.total_length,
            resolved_meta: Some(real_meta.clone()),
            ..Default::default()
        })),
    );

    runtime.reconcile_engine_progress().await;
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::Downloading);
    assert_eq!(summary.name, "resolved-magnet.bin");
    assert_eq!(summary.total_length, real_meta.total_length);
    assert_eq!(summary.piece_count, real_meta.piece_count());
    assert_eq!(summary.pieces_have, 1);
    assert!(summary.bytes_completed <= summary.total_length);
    assert!(summary.progress() <= 1.0);

    let reg = runtime.registry.lock().await;
    let torrent = reg.get(&hash).unwrap();
    assert!(!torrent.needs_metadata);
    assert_eq!(torrent.progress.total, real_meta.piece_count());
    assert_eq!(torrent.files[0].path, "resolved-magnet.bin");
    drop(reg);
    runtime.force_stop_engine(&hash).await;
}

#[tokio::test]
async fn reconcile_keeps_unresolved_magnet_in_metadata_state() {
    let root = unique_dir("unresolved-magnet-progress-restart");
    let state_path = root.join("state.sqlite");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.queue.auto_start = false;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        disabled_health(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "magnet placeholder",
        b"placeholder",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let placeholder_piece_count = placeholder_meta.piece_count();
    let magnet_info_hash =
        swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
            .unwrap();
    let hash = TorrentKey::v1(magnet_info_hash);
    let mut torrent = Torrent::new(placeholder_meta, 1);
    torrent.state = TorrentState::Queued;
    torrent.needs_metadata = true;
    set_test_v1_magnet_identity(&mut torrent, hash);
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.engine_handles.write().await.insert(
        hash,
        tokio::spawn(async {
            std::future::pending::<()>().await;
        }),
    );
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            tracker_message: Some("fetching metadata via BEP 9".into()),
            ..Default::default()
        })),
    );

    runtime.reconcile_engine_progress().await;
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::DownloadingMetadata);
    assert_eq!(summary.total_length, "placeholder".len() as u64);
    let unresolved = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(unresolved.progress.total, placeholder_piece_count);
    assert_eq!(unresolved.progress.pieces_have(), 0);
    assert_eq!(
        unresolved.progress.bitfield().as_bytes().len(),
        placeholder_piece_count.div_ceil(8)
    );

    runtime.force_stop_engine(&hash).await;
    drop(runtime);

    let restarted = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        disabled_health(),
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restarted.restore_persisted_state().await.unwrap(), 1);
    let restored = restarted.registry.lock().await.get(&hash).cloned().unwrap();
    assert!(restored.needs_metadata);
    assert_eq!(restored.progress.total, placeholder_piece_count);
    assert_eq!(restored.progress.pieces_have(), 0);
    assert_eq!(
        restored.progress.bitfield().as_bytes().len(),
        placeholder_piece_count.div_ceil(8)
    );
    drop(restarted);
    std::fs::remove_dir_all(root).ok();
}
