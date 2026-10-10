// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn full_persistence_and_queue_planning_remain_responsive() {
    persistence_and_queue_planning_remain_responsive(false).await;
}

#[tokio::test]
async fn incremental_persistence_and_queue_planning_remain_responsive() {
    persistence_and_queue_planning_remain_responsive(true).await;
}

async fn persistence_and_queue_planning_remain_responsive(incremental: bool) {
    let root = unique_dir("persistence-queue-lock-order");
    let state_path = root.join("state.sqlite");
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        Config::default(),
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "lock-order.bin",
        b"generated local payload",
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
    runtime.queue.lock().await.add(hash);
    if incremental {
        // The first save creates SQLite; the next detects the existing
        // database and enables changed-record persistence.
        runtime.persist_state().await.unwrap();
        runtime.persist_state().await.unwrap();
    }
    assert_eq!(
        runtime
            .incremental_persistence_ready
            .load(Ordering::Relaxed),
        incremental
    );
    runtime
        .registry
        .lock()
        .await
        .get_mut(&hash)
        .unwrap()
        .uploaded = 17;

    // Enqueue persistence first at the registry, then poll the scheduler.
    // The old scheduler held the queue while waiting here. Once persistence
    // acquired the registry, each operation waited forever for the other.
    // Explicit polling makes the interleaving deterministic without sleeps.
    let registry_guard = runtime.registry.lock().await;
    let mut save = Box::pin(runtime.persist_state());
    assert!(futures_util::poll!(&mut save).is_pending());
    let mut plan = Box::pin(runtime.desired_download_hashes());
    assert!(futures_util::poll!(&mut plan).is_pending());
    drop(registry_guard);

    let (saved, planned, torrents, stats) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(save, plan, runtime.list_torrents(), runtime.global_stats())
    })
    .await
    .expect("persistence, queue planning, and control-plane reads must not deadlock");
    saved.unwrap();
    assert_eq!(planned, vec![hash]);
    assert_eq!(torrents.len(), 1);
    assert_eq!(stats.torrent_count, 1);
    let persisted = crate::state_store::load(&state_path).unwrap().unwrap();
    assert_eq!(persisted.torrents.len(), 1);
    assert_eq!(persisted.torrents[0].uploaded, 17);
    assert_eq!(persisted.queue.position(&hash), Some(1));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn durable_state_restores_torrents_settings_and_queue() {
    let root = unique_dir("durable-state");
    let state_path = root.join("state.json");
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        health.clone(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "persisted.bin",
        b"durable daemon state",
        8,
        None,
        false,
    );
    let hash = runtime
        .add_torrent_file_with_options(bytes.clone(), AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    runtime
        .set_labels(&hash, vec!["linux-release".into()])
        .await
        .unwrap();
    runtime
        .set_torrent_limits(
            &hash,
            swarmotter_core::bandwidth::TorrentBandwidth {
                download: 111,
                upload: 222,
            },
        )
        .await
        .unwrap();
    drop(runtime);

    let connection = rusqlite::Connection::open(&state_path).unwrap();
    let original: Vec<u8> = connection
        .query_row(
            "SELECT metainfo FROM torrent_metainfo
             WHERE info_hash = ?1 AND representation = 'original_torrent'",
            rusqlite::params![hash.to_locator()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(original, bytes);
    drop(connection);

    let restored = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 1);
    let torrent = restored.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, TorrentState::Paused);
    assert_eq!(torrent.labels, vec!["linux-release"]);
    assert_eq!(restored.queue.lock().await.position(&hash), Some(1));
    let limiter = restored
        .torrent_limiters
        .read()
        .await
        .get(&hash)
        .cloned()
        .expect("paused restored torrents retain a limiter");
    assert_eq!(
        limiter.capacity(swarmotter_core::bandwidth::RateDirection::Download),
        111
    );
    assert_eq!(
        limiter.capacity(swarmotter_core::bandwidth::RateDirection::Upload),
        222
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn restart_reconstructs_eligible_seeder_and_preserves_automatic_and_manual_stops() {
    let root = unique_dir("seeding-restart-lifecycle");
    let state_path = root.join("state.json");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = 0;
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.seeding.global_ratio_limit = None;
    cfg.seeding.global_idle_limit = None;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        health.clone(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let (eligible, _) =
        add_complete_seed_fixture(&runtime, "restart-active.bin", b"restart active payload").await;
    let (automatic, _) = add_complete_seed_fixture(
        &runtime,
        "restart-automatic.bin",
        b"restart automatic payload",
    )
    .await;
    let (manual, _) =
        add_complete_seed_fixture(&runtime, "restart-manual.bin", b"restart manual payload").await;
    {
        let mut registry = runtime.registry.lock().await;
        let eligible_torrent = registry.get_mut(&eligible).unwrap();
        eligible_torrent.state = TorrentState::Seeding;
        eligible_torrent.seeding_status = SeedingStatus::Active;
        let automatic_torrent = registry.get_mut(&automatic).unwrap();
        automatic_torrent.state = TorrentState::Completed;
        automatic_torrent.seeding.seed_forever = false;
        automatic_torrent.seeding.ratio_limit = Some(0.0);
        automatic_torrent.seeding_status = SeedingStatus::StoppedRatio;
        let manual_torrent = registry.get_mut(&manual).unwrap();
        manual_torrent.state = TorrentState::Paused;
        manual_torrent.seeding_status = SeedingStatus::StoppedManual;
    }
    runtime.persist_state().await.unwrap();
    assert!(runtime.seeder_registry.is_empty().await);
    assert!(runtime.seeder_shutdowns.lock().await.is_empty());
    assert!(runtime.seeder_listener_handle.lock().await.is_none());
    // No task was started: dropping here deliberately models a process
    // crash after durable Active state, without detaching a live listener.
    drop(runtime);

    let restored = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 3);
    assert!(restored.seeder_registry.contains(&eligible).await);
    assert!(!restored.seeder_registry.contains(&automatic).await);
    assert!(!restored.seeder_registry.contains(&manual).await);
    let registry = restored.registry.lock().await;
    assert_eq!(
        registry.get(&eligible).unwrap().state,
        TorrentState::Seeding
    );
    assert_eq!(
        registry.get(&eligible).unwrap().seeding_status,
        SeedingStatus::Active
    );
    assert_eq!(
        registry.get(&automatic).unwrap().seeding_status,
        SeedingStatus::StoppedRatio
    );
    assert_eq!(registry.get(&manual).unwrap().state, TorrentState::Paused);
    assert_eq!(
        registry.get(&manual).unwrap().seeding_status,
        SeedingStatus::StoppedManual
    );
    drop(registry);
    assert_eq!(restored.torrent_limiters.read().await.len(), 3);
    assert_seeder_state_registry_invariant(&restored).await;
    restored.remove_torrent(&eligible, false).await.unwrap();
    restored.remove_torrent(&automatic, false).await.unwrap();
    restored.remove_torrent(&manual, false).await.unwrap();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn boundary_file_bytes_are_exact_after_restore_and_each_recheck() {
    let root = unique_dir("file-boundary-restore-recheck");
    let state_path = root.join("state.json");
    let payload_root = root.join("payload");
    let files = vec![
        (vec!["a.bin".into()], 3),
        (vec!["b.bin".into()], 4),
        (vec!["c.bin".into()], 2),
    ];
    let contents: [&[u8]; 3] = [b"abc", b"defg", b"hi"];
    let bytes =
        swarmotter_core::meta::build_multi_file_torrent("boundary", &files, &contents, 4, None);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), payload_root.clone());
    storage.write_piece(0, b"abcd").await.unwrap();
    storage.write_piece(2, b"i").await.unwrap();

    let mut torrent = Torrent::new(meta.clone(), now());
    torrent.state = TorrentState::Paused;
    torrent.progress.have_piece(0);
    torrent.progress.have_piece(2);
    torrent
        .files
        .iter_mut()
        .for_each(|file| file.bytes_completed = 0);
    torrent.seeding.idle_limit = Some(0);
    crate::state_store::save(
        &state_path,
        &crate::state_store::DaemonState::new(
            vec![torrent],
            QueueState::new(Config::default().queue),
        ),
    )
    .unwrap();

    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(payload_root.display().to_string());
    cfg.torrent.listen_port = 0;
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    runtime.restore_persisted_state().await.unwrap();
    let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(restored.bytes_completed(), 5);
    assert_eq!(
        restored
            .files
            .iter()
            .map(|file| file.bytes_completed)
            .collect::<Vec<_>>(),
        vec![3, 1, 1]
    );

    runtime.recheck(&hash).await.unwrap();
    let partial = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(partial.bytes_completed(), 5);
    assert_eq!(
        partial
            .files
            .iter()
            .map(|file| file.bytes_completed)
            .collect::<Vec<_>>(),
        vec![3, 1, 1]
    );

    storage.write_piece(1, b"efgh").await.unwrap();
    runtime.recheck(&hash).await.unwrap();
    let complete = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(complete.bytes_completed(), 9);
    assert_eq!(
        complete
            .files
            .iter()
            .map(|file| file.bytes_completed)
            .collect::<Vec<_>>(),
        vec![3, 4, 2]
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn single_file_final_piece_bytes_are_exact_after_restore_and_recheck() {
    let root = unique_dir("single-file-boundary-restore-recheck");
    let state_path = root.join("state.json");
    let payload_root = root.join("payload");
    let content = b"123456789";
    let bytes =
        swarmotter_core::meta::build_single_file_torrent("nine.bin", content, 4, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), payload_root.clone());
    storage.write_piece(2, b"9").await.unwrap();
    let mut torrent = Torrent::new(meta.clone(), now());
    torrent.state = TorrentState::Paused;
    torrent.progress.have_piece(2);
    torrent.files[0].bytes_completed = 0;
    crate::state_store::save(
        &state_path,
        &crate::state_store::DaemonState::new(
            vec![torrent],
            QueueState::new(Config::default().queue),
        ),
    )
    .unwrap();

    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(payload_root.display().to_string());
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.seeding.global_idle_limit = None;
    cfg.seeding.global_ratio_limit = Some(0.0);
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    runtime.restore_persisted_state().await.unwrap();
    let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(restored.bytes_completed(), 1);
    assert_eq!(restored.files[0].bytes_completed, 1);
    runtime.recheck(&hash).await.unwrap();
    let rechecked = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(rechecked.bytes_completed(), 1);
    assert_eq!(rechecked.files[0].bytes_completed, 1);

    storage.write_piece(0, b"1234").await.unwrap();
    storage.write_piece(1, b"5678").await.unwrap();
    runtime.recheck(&hash).await.unwrap();
    let complete = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(complete.bytes_completed(), 9);
    assert_eq!(complete.files[0].bytes_completed, 9);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn torrent_add_rejects_cross_torrent_storage_path_collision() {
    let root = unique_dir("path-collision");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first = swarmotter_core::meta::build_single_file_torrent(
        "shared-name.bin",
        b"first lawful payload",
        8,
        None,
        false,
    );
    let second = swarmotter_core::meta::build_single_file_torrent(
        "shared-name.bin",
        b"different lawful payload",
        8,
        None,
        false,
    );

    runtime
        .add_torrent_file_with_options(first, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    let error = runtime
        .add_torrent_file_with_options(second, AddTorrentOptions::new(None, true))
        .await
        .unwrap_err();

    assert!(matches!(error, CoreError::Storage(_)));
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn concurrent_torrent_adds_cannot_claim_the_same_storage_path() {
    let root = unique_dir("concurrent-path-collision");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first = swarmotter_core::meta::build_single_file_torrent(
        "concurrent.bin",
        b"first concurrent payload",
        8,
        None,
        false,
    );
    let second = swarmotter_core::meta::build_single_file_torrent(
        "concurrent.bin",
        b"second concurrent payload",
        8,
        None,
        false,
    );

    let (first, second) = tokio::join!(
        runtime.add_torrent_file_with_options(first, AddTorrentOptions::new(None, true)),
        runtime.add_torrent_file_with_options(second, AddTorrentOptions::new(None, true))
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let error = first.err().or_else(|| second.err()).unwrap();
    assert!(matches!(error, CoreError::Storage(_)));
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn distinct_same_name_magnets_can_coexist_without_placeholder_payload_paths() {
    let root = unique_dir("magnet-path-collision");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first = "magnet:?xt=urn:btih:0000000000000000000000000000000000000001&dn=shared.bin";
    let second = "magnet:?xt=urn:btih:0000000000000000000000000000000000000002&dn=shared.bin";

    let first_key = runtime
        .add_magnet_with_options(first, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    let second_key = runtime
        .add_magnet_with_options(second, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    assert_ne!(first_key, second_key);
    let registry = runtime.registry.lock().await;
    assert_eq!(registry.torrents.len(), 2);
    assert!(registry.get(&first_key).unwrap().needs_metadata);
    assert!(registry.get(&second_key).unwrap().needs_metadata);
    drop(registry);
    assert!(
        !root.join("shared.bin").exists(),
        "unresolved metadata previews must not reserve or create payload paths"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn durable_restore_rejects_colliding_paths_and_invalid_progress() {
    let root = unique_dir("restore-validation");
    let state_path = root.join("state.json");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.join("payload").display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let first_meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "restored.bin",
            b"first restored payload",
            8,
            None,
            false,
        ))
        .unwrap();
    let second_meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "restored.bin",
            b"second restored payload",
            8,
            None,
            false,
        ))
        .unwrap();
    let first = Torrent::new(first_meta, 1);
    let second = Torrent::new(second_meta, 2);
    crate::state_store::save(
        &state_path,
        &crate::state_store::DaemonState::new(
            vec![first.clone(), second],
            QueueState::new(cfg.queue.clone()),
        ),
    )
    .unwrap();
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        health.clone(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    assert!(matches!(
        runtime.restore_persisted_state().await.unwrap_err(),
        CoreError::Storage(_)
    ));

    let mut invalid_progress = first;
    invalid_progress.progress.total += 1;
    crate::state_store::save(
        &state_path,
        &crate::state_store::DaemonState::new(
            vec![invalid_progress],
            QueueState::new(cfg.queue.clone()),
        ),
    )
    .unwrap();
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert!(matches!(
        runtime.restore_persisted_state().await.unwrap_err(),
        CoreError::Storage(_)
    ));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn durable_restore_normalizes_legacy_zero_progress_unresolved_magnet() {
    let root = unique_dir("restore-legacy-unresolved-progress");
    let state_path = root.join("state.sqlite");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.queue.auto_start = false;
    let placeholder =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "generated-metadata-placeholder.bin",
            b"generated metadata placeholder payload",
            16,
            None,
            false,
        ))
        .unwrap();
    let piece_count = placeholder.piece_count();
    assert!(piece_count > 0);
    let key =
        TorrentKey::v1(InfoHash::from_hex("8a15da1c44b064473f826a02aa01c39ae577da16").unwrap());
    let mut unresolved = Torrent::new(placeholder, now());
    unresolved.needs_metadata = true;
    set_test_v1_magnet_identity(&mut unresolved, key);
    unresolved.state = TorrentState::Error;
    unresolved.error = Some("generated metadata discovery failure".into());
    unresolved.progress = swarmotter_core::storage::PieceProgress::new(0);
    crate::state_store::save(
        &state_path,
        &crate::state_store::DaemonState::new(vec![unresolved], QueueState::new(cfg.queue.clone())),
    )
    .unwrap();

    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        disabled_health(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    assert_eq!(runtime.restore_persisted_state().await.unwrap(), 1);
    let restored = runtime.registry.lock().await.get(&key).cloned().unwrap();
    assert!(restored.needs_metadata);
    assert_eq!(restored.progress.total, piece_count);
    assert_eq!(restored.progress.pieces_have(), 0);
    assert_eq!(
        restored.progress.bitfield().as_bytes().len(),
        piece_count.div_ceil(8)
    );
    drop(runtime);

    let persisted = crate::state_store::load(&state_path)
        .unwrap()
        .unwrap()
        .torrents
        .into_iter()
        .find(|torrent| torrent.key() == key)
        .unwrap();
    assert_eq!(persisted.progress.total, piece_count);
    assert_eq!(persisted.progress.pieces_have(), 0);
    assert_eq!(
        persisted.progress.bitfield().as_bytes().len(),
        piece_count.div_ceil(8)
    );

    let restarted = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        disabled_health(),
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restarted.restore_persisted_state().await.unwrap(), 1);
    assert_eq!(
        restarted
            .registry
            .lock()
            .await
            .get(&key)
            .unwrap()
            .progress
            .total,
        piece_count
    );
    drop(restarted);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn persistence_rejects_inconsistent_piece_progress_before_commit() {
    let root = unique_dir("persist-invalid-progress");
    let state_path = root.join("state.sqlite");
    let cfg = Config::default();
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        disabled_health(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "generated-persistence-fixture.bin",
            b"generated persistence validation payload",
            8,
            None,
            false,
        ))
        .unwrap();
    let key = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, now()))
        .unwrap();
    runtime.queue.lock().await.add(key);
    runtime.persist_state().await.unwrap();
    let valid_generation = std::fs::read(&state_path).unwrap();

    runtime
        .registry
        .lock()
        .await
        .get_mut(&key)
        .unwrap()
        .progress
        .total += 1;
    let error = runtime.persist_state().await.unwrap_err();
    assert!(error.to_string().contains("inconsistent piece progress"));
    assert_eq!(std::fs::read(&state_path).unwrap(), valid_generation);

    drop(runtime);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn durable_restore_rejects_legacy_zero_v1_magnet_identity() {
    let root = unique_dir("restore-zero-v1-magnet");
    let state_path = root.join("state.sqlite");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let placeholder =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "magnet-placeholder.bin",
            b"generated legacy magnet placeholder",
            8,
            None,
            false,
        ))
        .unwrap();
    let mut unresolved = Torrent::new(placeholder, now());
    let key = unresolved.key();
    unresolved.needs_metadata = true;
    // This represents a malformed legacy record: it has neither a full
    // identity nor a usable v1 hash. Restore must fail closed rather than
    // issuing metadata discovery on an all-zero swarm identity.
    unresolved.magnet_info_hash = Some(InfoHash::ZERO);
    unresolved.magnet_identity = None;
    crate::state_store::save(
        &state_path,
        &crate::state_store::DaemonState::new(vec![unresolved], QueueState::new(cfg.queue.clone())),
    )
    .unwrap();

    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    let error = runtime.restore_persisted_state().await.unwrap_err();
    assert!(matches!(error, CoreError::Storage(_)));
    assert!(error.to_string().contains(&key.to_locator()));
    assert!(error.to_string().contains("inconsistent magnet identity"));
    assert!(runtime.registry.lock().await.torrents.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn state_save_failure_rolls_back_move_and_rename() {
    let root = unique_dir("storage-state-rollback");
    let state_path = root.join("state-target");
    std::fs::create_dir_all(&state_path).unwrap();
    let old_root = root.join("old");
    let new_root = root.join("new");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(old_root.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    let payload = b"rollback payload";
    let meta = swarmotter_core::meta::parse_torrent(
        &swarmotter_core::meta::build_single_file_torrent("rollback.bin", payload, 8, None, false),
    )
    .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Paused;
    torrent.download_dir = Some(old_root.display().to_string());
    for piece in 0..meta.piece_count() {
        torrent.progress.have_piece(piece);
    }
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    let before_policy = runtime
        .registry
        .lock()
        .await
        .get(&hash)
        .unwrap()
        .seeding
        .clone();
    let before_status = runtime
        .registry
        .lock()
        .await
        .get(&hash)
        .unwrap()
        .seeding_status;
    assert!(runtime
        .set_torrent_seeding(
            &hash,
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: Some(1.5),
                idle_limit: Some(30),
                seed_forever: true,
            },
        )
        .await
        .is_err());
    let after_failed_policy = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(after_failed_policy.seeding, before_policy);
    assert_eq!(after_failed_policy.seeding_status, before_status);
    tokio::fs::create_dir_all(&old_root).await.unwrap();
    tokio::fs::write(old_root.join("rollback.bin"), payload)
        .await
        .unwrap();

    assert!(runtime
        .move_data(&hash, new_root.display().to_string())
        .await
        .is_err());
    assert_eq!(
        tokio::fs::read(old_root.join("rollback.bin"))
            .await
            .unwrap(),
        payload
    );
    assert!(!new_root.join("rollback.bin").exists());
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&hash)
            .unwrap()
            .download_dir
            .as_deref(),
        old_root.to_str()
    );

    assert!(runtime
        .rename_path(&hash, 0, "renamed.bin".into())
        .await
        .is_err());
    assert_eq!(
        tokio::fs::read(old_root.join("rollback.bin"))
            .await
            .unwrap(),
        payload
    );
    assert!(!old_root.join("renamed.bin").exists());
    let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(restored.meta.files[0].path, vec!["rollback.bin"]);
    assert_eq!(restored.files[0].path, "rollback.bin");
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn state_save_failure_rolls_back_torrent_registration() {
    let root = unique_dir("add-state-rollback");
    let state_path = root.join("state-target");
    std::fs::create_dir_all(&state_path).unwrap();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        Config::default(),
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "registration-rollback.bin",
        b"registration rollback payload",
        8,
        None,
        false,
    );

    assert!(runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .is_err());
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn seeding_policy_persistence_failure_restores_policy_status_and_state() {
    let root = unique_dir("seeding-policy-state-rollback");
    let state_path = root.join("state-target");
    std::fs::create_dir_all(&state_path).unwrap();
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = 0;
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.seeding.global_ratio_limit = None;
    cfg.seeding.global_idle_limit = None;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    let (hash, limiter) = add_complete_seed_fixture(
        &runtime,
        "policy-rollback.bin",
        b"generated rollback payload",
    )
    .await;
    runtime.reconcile_seeders().await;
    assert_seeder_state_registry_invariant(&runtime).await;
    let before = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(before.state, TorrentState::Seeding);
    assert_eq!(before.seeding_status, SeedingStatus::Active);
    let registered_limiter = runtime
        .seeder_registry
        .limiter_for_test(&hash)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&limiter, &registered_limiter));
    let shutdown = runtime
        .seeder_shutdowns
        .lock()
        .await
        .get(&hash)
        .cloned()
        .unwrap();
    let listener_task = runtime
        .seeder_listener_handle
        .lock()
        .await
        .as_ref()
        .unwrap()
        .id();

    let error = runtime
        .set_torrent_seeding(
            &hash,
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: Some(0.0),
                idle_limit: None,
                seed_forever: false,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::Storage(_)));
    let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(restored.seeding, before.seeding);
    assert_eq!(restored.seeding_status, SeedingStatus::Active);
    assert_eq!(restored.state, TorrentState::Seeding);
    assert!(runtime.seeder_registry.contains(&hash).await);
    assert!(runtime
        .seeder_shutdowns
        .lock()
        .await
        .get(&hash)
        .is_some_and(|current| current.same_channel(&shutdown)));
    assert_eq!(
        runtime
            .seeder_listener_handle
            .lock()
            .await
            .as_ref()
            .unwrap()
            .id(),
        listener_task
    );
    assert!(Arc::ptr_eq(
        runtime.torrent_limiters.read().await.get(&hash).unwrap(),
        &limiter
    ));
    assert!(Arc::ptr_eq(
        &runtime
            .seeder_registry
            .limiter_for_test(&hash)
            .await
            .unwrap(),
        &limiter
    ));
    assert_seeder_state_registry_invariant(&runtime).await;
    runtime.force_stop_seeder(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn durable_restore_rejects_invalid_per_torrent_ratio_policy_with_context() {
    let root = unique_dir("invalid-restored-seeding-policy");
    let state_path = root.join("state.json");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "invalid-policy.bin",
        b"generated invalid policy payload",
        8,
        None,
        false,
    );
    let mut torrent = Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), now());
    let hash = torrent.key();
    torrent.seeding.ratio_limit = Some(-1.0);
    crate::state_store::save(
        &state_path,
        &crate::state_store::DaemonState::new(vec![torrent], QueueState::new(cfg.queue.clone())),
    )
    .unwrap();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    let error = runtime.restore_persisted_state().await.unwrap_err();
    assert!(matches!(error, CoreError::Storage(_)));
    assert!(error.to_string().contains(&hash.to_locator()));
    assert!(error.to_string().contains("seeding.ratio_limit"));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn rename_rejects_the_torrents_own_torrent_key_resume_path() {
    let root = unique_dir("rename-resume-collision");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "resume-name.bin",
        b"resume collision payload",
        8,
        None,
        false,
    );
    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();

    let error = runtime
        .rename_path(&hash, 0, format!("{hash}.swarmotter.resume"))
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::Storage(_)));
    assert_eq!(
        runtime.registry.lock().await.get(&hash).unwrap().files[0].path,
        "resume-name.bin"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn recheck_preserves_selected_file_completion() {
    let root = unique_dir("selected-recheck");
    let complete_root = root.join("complete");
    let active_root = root.join("active");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(complete_root.display().to_string());
    cfg.storage.incomplete_dir = Some(active_root.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first = b"aaaa".as_slice();
    let second = b"bbbb".as_slice();
    let bytes = swarmotter_core::meta::build_multi_file_torrent(
        "selection",
        &[
            (vec!["first.bin".into()], first.len() as u64),
            (vec!["second.bin".into()], second.len() as u64),
        ],
        &[first, second],
        4,
        None,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    {
        let mut registry = runtime.registry.lock().await;
        let torrent = registry.get_mut(&hash).unwrap();
        torrent.wanted[1] = false;
        torrent.priorities[1] = FilePriority::Unwanted;
        torrent.files[1].wanted = false;
        torrent.files[1].priority = FilePriority::Unwanted;
        torrent.progress.have_piece(0);
        torrent.state = TorrentState::Completed;
    }
    let storage = swarmotter_core::storage::StorageIo::new(meta, active_root);
    let first_path = storage.file_path(0).unwrap();
    tokio::fs::create_dir_all(first_path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(first_path, first).await.unwrap();

    runtime.recheck(&hash).await.unwrap();
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, TorrentState::Completed);
    assert_eq!(torrent.progress.pieces_have(), 1);
    assert!(!torrent.progress.is_complete());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn move_and_rename_update_payload_and_registry_paths() {
    let root = unique_dir("move-rename");
    let old_root = root.join("old");
    let new_root = root.join("new");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(old_root.display().to_string());
    cfg.storage.incomplete_dir = None;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let payload = b"move and rename lawful payload";
    let bytes =
        swarmotter_core::meta::build_single_file_torrent("original.bin", payload, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    tokio::fs::create_dir_all(&old_root).await.unwrap();
    tokio::fs::write(old_root.join("original.bin"), payload)
        .await
        .unwrap();
    {
        let mut registry = runtime.registry.lock().await;
        let torrent = registry.get_mut(&hash).unwrap();
        for piece in 0..meta.piece_count() {
            torrent.progress.have_piece(piece);
        }
        torrent.state = TorrentState::Completed;
    }

    tokio::time::timeout(
        Duration::from_secs(5),
        runtime.move_data(&hash, new_root.display().to_string()),
    )
    .await
    .expect("move_data timed out")
    .unwrap();
    assert!(!old_root.join("original.bin").exists());
    assert_eq!(
        tokio::fs::read(new_root.join("original.bin"))
            .await
            .unwrap(),
        payload
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        runtime.rename_path(&hash, 0, "renamed.bin".into()),
    )
    .await
    .expect("rename_path timed out")
    .unwrap();
    assert!(!new_root.join("original.bin").exists());
    assert_eq!(
        tokio::fs::read(new_root.join("renamed.bin")).await.unwrap(),
        payload
    );
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.download_dir.as_deref(), new_root.to_str());
    assert_eq!(torrent.files[0].path, "renamed.bin");
    assert_eq!(torrent.meta.files[0].path, vec!["renamed.bin"]);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn concurrent_config_replacements_leave_runtime_and_disk_consistent() {
    let root = unique_dir("config-replacement");
    let config_path = root.join("swarmotter.toml");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_and_broker(
        cfg.clone(),
        health,
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );
    let mut first = cfg.clone();
    first.queue.max_active_downloads = 2;
    let mut second = cfg;
    second.queue.max_active_downloads = 7;

    let (first_result, second_result) = tokio::join!(
        runtime.replace_config(first),
        runtime.replace_config(second)
    );
    first_result.unwrap();
    second_result.unwrap();

    let disk = Config::from_file(&config_path).unwrap();
    let live = runtime.config.read().await.clone();
    assert_eq!(
        disk.to_toml_string().unwrap(),
        live.to_toml_string().unwrap()
    );
    assert_eq!(live.queue.max_active_downloads, 7);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&config_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
    assert!(std::fs::read_dir(&root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".tmp")));
    std::fs::remove_dir_all(root).ok();
}

// --- Proportional changed-record persistence (ADR-0073) ---

fn bulk_registry_torrent(label: &str, index: usize) -> Torrent {
    let payload = format!("generated lawful library payload {label} {index}");
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        &format!("{label}-{index}.bin"),
        payload.as_bytes(),
        64,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    Torrent::new(meta, now())
}

#[tokio::test]
async fn progress_persistence_rewrites_only_changed_records() {
    use crate::state_store::CHANGED_SAVE_RECORDS_WRITTEN;

    let root = unique_dir("incremental-persistence");
    let state_path = root.join("state.sqlite");
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        health.clone(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );

    // First save: legacy/absent state must take the full migration path.
    // This runtime-local counter isolates the assertion from other tests in
    // the same process that legitimately perform changed-record saves.
    runtime.persist_state().await.unwrap();
    assert_eq!(
        runtime.changed_record_save_count(),
        0,
        "full saves are not changed-record saves"
    );

    // Build a library of paused records plus one record with engine progress.
    let mut active_hash = None;
    for index in 0..300 {
        let torrent = bulk_registry_torrent("library", index);
        let hash = torrent.key();
        let mut torrent = torrent;
        torrent.state = TorrentState::Paused;
        if index == 0 {
            torrent.state = TorrentState::Downloading;
            torrent.downloaded = 4096;
            torrent.uploaded = 512;
            active_hash = Some(hash);
        }
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
    }
    let active_hash = active_hash.unwrap();

    // Prime the fingerprint generation with a full save.
    runtime.persist_state().await.unwrap();

    // Simulate one engine progress sample: exactly one record must be
    // serialized and rewritten.
    {
        let mut reg = runtime.registry.lock().await;
        let torrent = reg.get_mut(&active_hash).unwrap();
        torrent.downloaded += 65536;
        torrent.uploaded += 1024;
    }
    let before = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    runtime.persist_state().await.unwrap();
    let after = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        after - before,
        1,
        "a one-torrent progress change must rewrite exactly one record"
    );

    // Growing the inactive library must not increase writes for a fixed
    // active set.
    for index in 300..1_300 {
        let torrent = bulk_registry_torrent("library", index);
        let hash = torrent.key();
        let mut torrent = torrent;
        torrent.state = TorrentState::Paused;
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
    }
    // Newly registered records are written exactly once (their first
    // durable generation), then never again while unchanged.
    let before = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    runtime.persist_state().await.unwrap();
    let after = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        after - before,
        1_000,
        "newly registered library records are written once"
    );
    let before = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    runtime.persist_state().await.unwrap();
    let after = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        after - before,
        0,
        "an unchanged 10k inactive library must not be rewritten on a no-op save"
    );

    // One further active change still rewrites exactly one record despite the
    // larger library.
    {
        let mut reg = runtime.registry.lock().await;
        let torrent = reg.get_mut(&active_hash).unwrap();
        torrent.downloaded += 65536;
    }
    let before = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    runtime.persist_state().await.unwrap();
    let after = CHANGED_SAVE_RECORDS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        after - before,
        1,
        "changed-record saves stay proportional to changes as the library grows"
    );

    // Durable state remains loadable and coherent after incremental saves.
    // Assert against the durable record itself so the restored runtime's
    // engine tasks (which legitimately re-derive live counters after their
    // first discovery pass) cannot race these assertions.
    let connection = rusqlite::Connection::open(&state_path).unwrap();
    let json: Vec<u8> = connection
        .query_row(
            "SELECT torrent_json FROM torrent_records WHERE info_hash = ?1",
            rusqlite::params![active_hash.to_locator()],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);
    let durable: Torrent = serde_json::from_slice(&json).unwrap();
    assert_eq!(durable.downloaded, 4096 + 65536 + 65536);
    assert_eq!(durable.state, TorrentState::Downloading);
    let count: i64 = rusqlite::Connection::open(&state_path)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM torrent_records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1_300);

    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn full_command_channel_does_not_hold_global_map() {
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(Config::default(), health);
    let hash = TorrentKey::v1(InfoHash::from_bytes([0x11; 20]));
    let other = TorrentKey::v1(InfoHash::from_bytes([0x22; 20]));
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    tx.try_send(EngineCommand::Reannounce).unwrap();
    runtime.engine_cmds.lock().await.insert(hash, tx);
    let (other_tx, mut other_rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(other, other_tx);
    let (finished, completion) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _finished = finished;
        std::future::pending::<()>().await;
    });
    runtime.engine_handles.write().await.insert(hash, handle);
    let mut stopping = Box::pin(runtime.stop_engine(&hash));
    assert!(futures_util::poll!(&mut stopping).is_pending());
    assert!(runtime.engine_cmds.try_lock().is_ok());
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        runtime.send_engine_command(other, EngineCommand::Reannounce)
    )
    .await
    .unwrap());
    assert!(other_rx.recv().await.is_some());
    tokio::time::timeout(Duration::from_secs(7), &mut stopping)
        .await
        .unwrap();
    drop(stopping);
    assert!(
        completion.await.is_err(),
        "cancelled engine must drop its completion sender"
    );
    assert!(runtime.engine_cmds.try_lock().is_ok());
}

#[tokio::test]
async fn rollback_save_adopts_only_committed_snapshot_fingerprint() {
    let root = unique_dir("review-fingerprint-race");
    let state_path = root.join("state.sqlite");
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        Config::default(),
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "review.bin",
        b"local payload",
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
    runtime.queue.lock().await.add(hash);
    runtime.persist_state().await.unwrap();
    runtime.persist_state().await.unwrap();
    runtime
        .registry
        .lock()
        .await
        .get_mut(&hash)
        .unwrap()
        .uploaded = 1;
    // Delay post-save fingerprint adoption until the on-disk snapshot is visible.
    let fingerprint_guard = runtime.meta_fingerprints.lock().await;
    let saving = runtime.clone();
    let save = tokio::spawn(async move { saving.persist_state_with_file_rollback().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let uploaded = {
                let connection = rusqlite::Connection::open_with_flags(
                    &state_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .unwrap();
                let bytes: Vec<u8> = connection
                    .query_row("SELECT torrent_json FROM torrent_records", [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["uploaded"]
                    .as_u64()
                    .unwrap()
            };
            if uploaded == 1 {
                break;
            }
            assert!(
                !save.is_finished(),
                "save unexpectedly ended before writing the snapshot"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    runtime
        .registry
        .lock()
        .await
        .get_mut(&hash)
        .unwrap()
        .uploaded = 2;
    drop(fingerprint_guard);
    save.await.unwrap().unwrap();
    runtime.persist_state().await.unwrap();
    let disk = crate::state_store::load(&state_path)
        .unwrap()
        .unwrap()
        .torrents[0]
        .uploaded;
    assert_eq!(disk, 2);
    assert_eq!(
        runtime.registry.lock().await.get(&hash).unwrap().uploaded,
        2
    );

    std::fs::remove_dir_all(root).unwrap();
}

/// Reproducible local profile; no network or external content. Run alone with
/// --ignored --nocapture --test-threads=1 to compare commits on the same host.
#[tokio::test]
#[ignore = "generated large-metadata persistence profile"]
async fn persistence_large_metadata_profile() {
    let root = unique_dir("persistence-profile");
    let path = root.join("state.sqlite");
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        Config::default(),
        NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            NetworkContainmentStatus::Disabled,
            "profile",
        ),
        None,
        None,
        Some(path.clone()),
        EventBroker::default(),
    );
    let mut active = Vec::new();
    for index in 0..308 {
        let mut torrent = if index < 8 {
            let bytes = swarmotter_core::meta::build_single_file_torrent(
                &format!("generated-{index}.bin"),
                &vec![0x42; 4 * 1024 * 1024],
                64,
                None,
                false,
            );
            Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), now())
        } else {
            bulk_registry_torrent("profile", index)
        };
        torrent.state = TorrentState::Paused;
        let hash = torrent.key();
        if index < 8 {
            active.push(hash);
        }
        runtime.registry.lock().await.add(torrent).unwrap();
        runtime.queue.lock().await.add(hash);
    }
    runtime.persist_state().await.unwrap();
    runtime.persist_state().await.unwrap();
    let started = Instant::now();
    for _ in 0..5 {
        runtime.persist_state().await.unwrap();
    }
    println!("idle_five_ms={}", started.elapsed().as_millis());
    let started = Instant::now();
    for sample in 1..=5 {
        for hash in &active {
            runtime
                .registry
                .lock()
                .await
                .get_mut(hash)
                .unwrap()
                .uploaded = sample;
        }
        runtime.persist_state().await.unwrap();
    }
    println!(
        "eight_active_five_ms={} sqlite_bytes={}",
        started.elapsed().as_millis(),
        std::fs::metadata(&path).unwrap().len()
    );
    std::fs::remove_dir_all(root).unwrap();
}
