// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn root_scoped_recheck_cancellation_releases_a_running_permit() {
    let root = unique_dir("root-recheck-cancellation");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 0,
        max_active_bytes: 0,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 1,
    }];
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let cancellation = StorageWorkCancellation::new();
    let worker_runtime = runtime.clone();
    let worker_root = root.clone();
    let worker_cancellation = cancellation.clone();
    let worker = tokio::spawn(async move {
        worker_runtime
            .run_root_scoped_recheck(
                &worker_root,
                Some(&worker_cancellation),
                std::future::pending::<Result<()>>(),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.storage_rechecks.active_counts().len() == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recheck should acquire its root permit before cancellation");

    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("cancelled recheck should complete")
        .expect("recheck task should not panic")
        .expect_err("cancelled recheck should report cancellation");
    assert!(is_storage_work_cancelled(&error));
    assert!(runtime.storage_rechecks.active_counts().is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn metadata_root_admission_wait_observes_lifecycle_cancellation() {
    let root = unique_dir("metadata-admission-cancellation");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 1,
        max_active_bytes: 0,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 0,
    }];
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg.clone(), health);
    let resolved =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "metadata-cancellation.bin",
            b"generated metadata admission fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(resolved.info_hash);
    let mut torrent = Torrent::new(Arc::new(resolved.clone()), now());
    torrent.state = TorrentState::DownloadingMetadata;
    torrent.needs_metadata = true;
    runtime.registry.lock().await.add(torrent).unwrap();
    let admission = storage_root_admission_for_path(&cfg, &root).unwrap();
    let blocker = TorrentKey::v1(InfoHash::from_bytes([0x42; 20]));
    runtime
        .storage_admissions
        .reserve(blocker, &admission, 0)
        .await
        .unwrap();

    let cancellation = StorageWorkCancellation::new();
    let waiting_runtime = runtime.clone();
    let waiting_cancellation = cancellation.clone();
    let complete_dir = root.display().to_string();
    let active_dir = complete_dir.clone();
    let waiter = tokio::spawn(async move {
        waiting_runtime
            .reserve_resolved_magnet_metadata(
                hash,
                std::sync::Arc::new(resolved),
                complete_dir,
                active_dir,
                waiting_cancellation,
            )
            .await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();

    let error = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("metadata admission cancellation should complete the engine preflight")
        .expect("metadata admission task should not panic")
        .expect_err("cancelled metadata admission must not proceed");
    assert!(is_storage_work_cancelled(&error));
    assert_eq!(runtime.storage_admissions.records().await.len(), 1);
    runtime.storage_admissions.release(&blocker).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn dropped_explicit_recheck_restores_and_persists_incomplete_state() {
    let root = unique_dir("dropped-explicit-recheck");
    let state_path = root.join("state.json");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 0,
        max_active_bytes: 0,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 1,
    }];
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "cancelled-incomplete.bin",
            b"generated incomplete recheck fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::Paused;
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    let admission = storage_root_admission_for_path(&cfg, &root).unwrap();
    let held_permit = runtime.storage_rechecks.try_acquire(&admission).unwrap();

    let recheck_runtime = runtime.clone();
    let recheck = tokio::spawn(async move { recheck_runtime.recheck(&hash).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime
                .registry
                .lock()
                .await
                .get(&hash)
                .is_some_and(|torrent| torrent.state == TorrentState::Checking)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("explicit recheck should wait behind the held root permit");

    recheck.abort();
    let _ = recheck.await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let restored = runtime
                .registry
                .lock()
                .await
                .get(&hash)
                .is_some_and(|torrent| {
                    torrent.state == TorrentState::Paused
                        && torrent.seeding_status == SeedingStatus::NotEligible
                });
            let finished = !runtime.explicit_rechecks.lock().await.contains_key(&hash);
            if restored && finished {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped explicit recheck should restore a non-checking state");

    let persisted = crate::state_store::load(&state_path)
        .unwrap()
        .expect("cancelled recheck should persist its restored state");
    assert_eq!(
        persisted
            .torrents
            .iter()
            .find(|torrent| torrent.key() == hash)
            .map(|torrent| torrent.state),
        Some(TorrentState::Paused)
    );
    assert_eq!(runtime.storage_rechecks.active_counts().len(), 1);
    drop(held_permit);
    assert!(runtime.storage_rechecks.active_counts().is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cancelled_explicit_recheck_restores_completed_torrent_to_seeding_queue() {
    let root = unique_dir("cancelled-completed-recheck");
    let state_path = root.join("state.json");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
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
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "cancelled-completed.bin",
            b"generated completed recheck fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), now());
    for piece in 0..meta.piece_count() {
        torrent.progress.have_piece(piece);
    }
    torrent.recompute_file_bytes_completed();
    torrent.state = TorrentState::Checking;
    runtime.registry.lock().await.add(torrent).unwrap();
    let operation = ExplicitRecheckOperation::new();
    runtime
        .explicit_rechecks
        .lock()
        .await
        .insert(hash, operation.clone());

    runtime
        .finish_cancelled_explicit_recheck(
            hash,
            operation,
            ExplicitRecheckRestoreState {
                was_completed: true,
                was_manually_paused: false,
            },
        )
        .await;

    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert!(matches!(
        torrent.state,
        TorrentState::Completed | TorrentState::Seeding
    ));
    assert!(matches!(
        torrent.seeding_status,
        SeedingStatus::Queued | SeedingStatus::Active
    ));
    assert!(!runtime.explicit_rechecks.lock().await.contains_key(&hash));
    let persisted = crate::state_store::load(&state_path)
        .unwrap()
        .expect("completed cancellation restoration should persist");
    assert!(matches!(
        persisted
            .torrents
            .iter()
            .find(|torrent| torrent.key() == hash)
            .map(|torrent| torrent.state),
        Some(TorrentState::Completed | TorrentState::Seeding)
    ));
    runtime.force_stop_engine(&hash).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn dropped_finalized_explicit_recheck_persists_after_the_normal_write_barrier() {
    let root = unique_dir("dropped-finalized-recheck");
    let state_path = root.join("state.json");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
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
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let payload = b"generated finalized explicit recheck fixture";
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "finalized-recheck.bin",
            payload,
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), root.clone());
    for piece in 0..meta.piece_count() {
        let start = piece * meta.piece_length as usize;
        let end = (start + meta.piece_length as usize).min(payload.len());
        storage
            .write_piece(piece, &payload[start..end])
            .await
            .unwrap();
    }
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::Paused;
    runtime.registry.lock().await.add(torrent).unwrap();

    let (persist_reached, persist_continue) = runtime.pause_explicit_recheck_before_persist().await;
    let recheck_runtime = runtime.clone();
    let recheck = tokio::spawn(async move { recheck_runtime.recheck(&hash).await });
    tokio::time::timeout(Duration::from_secs(1), persist_reached)
        .await
        .expect("verification should finalize before the normal persistence barrier")
        .expect("recheck persistence pause should remain reachable");
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&hash)
            .map(|torrent| torrent.state),
        Some(TorrentState::Completed)
    );

    recheck.abort();
    let _ = recheck.await;
    drop(persist_continue);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !runtime.explicit_rechecks.lock().await.contains_key(&hash) && state_path.exists() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop cleanup should persist the already-finalized recheck state");

    let persisted = crate::state_store::load(&state_path)
        .unwrap()
        .expect("drop cleanup should write daemon state");
    assert_eq!(
        persisted
            .torrents
            .iter()
            .find(|torrent| torrent.key() == hash)
            .map(|torrent| torrent.state),
        Some(TorrentState::Completed)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn root_control_replacement_keeps_active_engine_and_wakes_new_admission() {
    let root = unique_dir("root-control-replacement");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.queue.max_active_downloads = 0;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 1,
        max_active_bytes: 0,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 0,
    }];
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg.clone(), health);
    let first =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "grandfathered-active.bin",
            b"first active root-control fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let second =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "replacement-admission.bin",
            b"second root-control fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let first_hash = TorrentKey::v1(first.info_hash);
    let second_hash = TorrentKey::v1(second.info_hash);
    let mut active = Torrent::new(first.clone(), now());
    active.state = TorrentState::Downloading;
    {
        let mut registry = runtime.registry.lock().await;
        registry.add(active).unwrap();
        registry.add(Torrent::new(second, now())).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
        queue.start_now(&second_hash);
    }
    let old_admission = storage_root_admission_for_path(&cfg, &root).unwrap();
    runtime
        .storage_admissions
        .reserve(first_hash, &old_admission, first.total_length)
        .await
        .unwrap();
    let (engine_tx, mut engine_rx) = tokio::sync::mpsc::channel(1);
    let fake_engine = tokio::spawn(async move { while engine_rx.recv().await.is_some() {} });
    runtime
        .engine_cmds
        .lock()
        .await
        .insert(first_hash, engine_tx);
    runtime
        .engine_handles
        .write()
        .await
        .insert(first_hash, fake_engine);

    assert_eq!(runtime.desired_download_hashes().await, vec![first_hash]);
    let admissions = runtime.storage_admissions.clone();
    let (woken_tx, woken_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        admissions.changed().await;
        let _ = woken_tx.send(());
    });
    tokio::task::yield_now().await;

    let mut replacement = cfg.clone();
    replacement.storage.root_controls[0].max_active_downloads = 2;
    assert!(!data_plane_config_changed(&cfg, &replacement));
    runtime.replace_config(replacement).await.unwrap();

    tokio::time::timeout(Duration::from_secs(1), woken_rx)
        .await
        .expect("root-control replacement should wake admission waiters")
        .expect("admission wake task should remain alive");
    assert!(
        runtime
            .engine_handles
            .read()
            .await
            .contains_key(&first_hash),
        "root-control-only replacement must not tear down active engines"
    );
    let desired = runtime.desired_download_hashes().await;
    assert!(
        desired.contains(&first_hash) && desired.contains(&second_hash),
        "the replacement capacity should admit the waiting queued torrent"
    );

    runtime.force_stop_engine(&first_hash).await;
    runtime.force_stop_engine(&second_hash).await;
    runtime.storage_admissions.release(&first_hash).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn root_control_replace_config_restores_file_after_post_rename_sync_failure() {
    let root = unique_dir("root-control-config-rollback");
    let config_path = root.join("swarmotter.toml");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.display().to_string());
    config.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 1,
        max_active_bytes: 0,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 0,
    }];
    write_config_atomically(&config_path, &config).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_and_broker(
        config.clone(),
        health,
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );
    let previous_file = std::fs::read(&config_path).unwrap();
    let mut replacement = config.clone();
    replacement.storage.root_controls[0].max_active_downloads = 2;

    runtime.inject_generic_config_persistence_failure_after_rename();
    let error = runtime
        .replace_config(replacement.clone())
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("configuration persistence failed"));
    assert!(error.to_string().contains("after rename"));
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
    assert_eq!(
        runtime.config.read().await.storage.root_controls[0].max_active_downloads,
        1
    );
    assert_eq!(
        Config::from_file(&config_path)
            .unwrap()
            .storage
            .root_controls[0]
            .max_active_downloads,
        1
    );

    // A second update demonstrates both configuration locks were released
    // after the failed persistence attempt.
    runtime.replace_config(replacement).await.unwrap();
    assert_eq!(
        runtime.config.read().await.storage.root_controls[0].max_active_downloads,
        2
    );
    assert_eq!(
        Config::from_file(&config_path)
            .unwrap()
            .storage
            .root_controls[0]
            .max_active_downloads,
        2
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn tightening_root_controls_serializes_an_inflight_engine_admission() {
    let root = unique_dir("root-control-admission-race");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.dht.enabled = false;
    cfg.pex.enabled = false;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 2,
        max_active_bytes: 0,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 0,
    }];
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg.clone(), health);
    let first =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "race-first.bin",
            b"first root admission race fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let second =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "race-second.bin",
            b"second root admission race fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let third =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "race-third.bin",
            b"third root admission race fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let first_hash = TorrentKey::v1(first.info_hash);
    let second_hash = TorrentKey::v1(second.info_hash);
    let third_hash = TorrentKey::v1(third.info_hash);
    let mut active = Torrent::new(first.clone(), now());
    active.state = TorrentState::Downloading;
    {
        let mut registry = runtime.registry.lock().await;
        registry.add(active).unwrap();
        registry.add(Torrent::new(second, now())).unwrap();
        registry.add(Torrent::new(third, now())).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
        queue.add(third_hash);
    }
    let old_admission = storage_root_admission_for_path(&cfg, &root).unwrap();
    runtime
        .storage_admissions
        .reserve(first_hash, &old_admission, first.total_length)
        .await
        .unwrap();
    let (first_tx, mut first_rx) = tokio::sync::mpsc::channel(1);
    let first_handle = tokio::spawn(async move { while first_rx.recv().await.is_some() {} });
    runtime
        .engine_cmds
        .lock()
        .await
        .insert(first_hash, first_tx);
    runtime
        .engine_handles
        .write()
        .await
        .insert(first_hash, first_handle);

    let (start_reached, start_continue) =
        runtime.pause_engine_start_before_storage_admission().await;
    let starting_runtime = runtime.clone();
    let start = tokio::spawn(async move {
        starting_runtime.start_engine(second_hash).await;
    });
    tokio::time::timeout(Duration::from_secs(1), start_reached)
        .await
        .expect("second engine should own the transition lock before admission")
        .expect("engine-start pause should remain reachable");

    let (mut replacement_reached, replacement_continue) = runtime
        .pause_root_control_replacement_after_transition_lock()
        .await;
    let mut tightening = cfg.clone();
    tightening.storage.root_controls[0].max_active_downloads = 1;
    let replacing_runtime = runtime.clone();
    let replacement =
        tokio::spawn(async move { replacing_runtime.replace_config(tightening).await });

    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut replacement_reached)
            .await
            .is_err(),
        "a root-control PUT must wait for the in-flight engine admission lock"
    );
    assert_eq!(
        runtime.config.read().await.storage.root_controls[0].max_active_downloads,
        2
    );

    let _ = start_continue.send(());
    tokio::time::timeout(Duration::from_secs(1), start)
        .await
        .expect("engine start should finish after admission pause releases")
        .expect("engine-start task should not panic");
    tokio::time::timeout(Duration::from_secs(1), &mut replacement_reached)
        .await
        .expect("root-control PUT should take the transition lock after start")
        .expect("root-control replacement pause should remain reachable");
    assert!(
        runtime
            .storage_admissions
            .records()
            .await
            .iter()
            .any(|record| record.key == second_hash),
        "the already-started engine is grandfathered under the old admission"
    );

    let _ = replacement_continue.send(());
    tokio::time::timeout(Duration::from_secs(1), replacement)
        .await
        .expect("root-control replacement should complete")
        .expect("root-control replacement task should not panic")
        .expect("root-control replacement should be valid");
    assert_eq!(
        runtime.config.read().await.storage.root_controls[0].max_active_downloads,
        1
    );

    runtime.start_engine(third_hash).await;
    assert!(
        !runtime
            .storage_admissions
            .records()
            .await
            .iter()
            .any(|record| record.key == third_hash),
        "a post-PUT engine start must use the tightened root limit"
    );

    runtime.force_stop_engine(&first_hash).await;
    runtime.force_stop_engine(&second_hash).await;
    runtime.force_stop_engine(&third_hash).await;
    runtime.storage_admissions.release(&first_hash).await;
    let _ = std::fs::remove_dir_all(root);
}
