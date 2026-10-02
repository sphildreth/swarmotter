// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn patch_peer_limits_commits_new_pools_and_reconstructs_live_seeder() {
    let (runtime, hash, root, _) = peer_reconfiguration_fixture("peer-patch-commit").await;
    let previous = runtime.current_peer_permit_configuration().await;
    let (queue_order, queue_bypass) = {
        let queue = runtime.queue.lock().await;
        (queue.order.clone(), queue.bypass.clone())
    };
    let mut bandwidth = runtime.config.read().await.bandwidth.clone();
    bandwidth.max_peers = 1;
    bandwidth.max_peers_per_torrent = 1;

    runtime
        .update_settings(swarmotter_api::state::SettingsPatch {
            bandwidth: Some(bandwidth),
            ..Default::default()
        })
        .await
        .unwrap();

    let current = runtime.current_peer_permit_configuration().await;
    assert_eq!(current.global.snapshot().limit, 1);
    assert_eq!(current.per_torrent[&hash].snapshot().limit, 1);
    assert!(!Arc::ptr_eq(&current.global, &previous.global));
    assert!(!Arc::ptr_eq(
        &current.per_torrent[&hash],
        &previous.per_torrent[&hash]
    ));
    assert!(runtime.seeder_registry.contains(&hash).await);
    let queue = runtime.queue.lock().await;
    assert_eq!(queue.order, queue_order);
    assert_eq!(queue.bypass, queue_bypass);
    drop(queue);
    runtime.force_stop_seeder(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn patch_peer_limits_failure_restores_exact_pools_lifecycle_and_queue() {
    let (runtime, hash, root, config_path) =
        peer_reconfiguration_fixture("peer-patch-rollback").await;
    let previous_config = runtime.config.read().await.clone();
    let previous_permits = runtime.current_peer_permit_configuration().await;
    let previous_file = std::fs::read(&config_path).unwrap();
    let previous_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    let (queue_order, queue_bypass) = {
        let queue = runtime.queue.lock().await;
        (queue.order.clone(), queue.bypass.clone())
    };
    let mut bandwidth = previous_config.bandwidth.clone();
    bandwidth.max_peers = 1;
    bandwidth.max_peers_per_torrent = 1;
    runtime.inject_peer_reconfiguration_failure_after_teardown();

    let error = runtime
        .update_settings(swarmotter_api::state::SettingsPatch {
            bandwidth: Some(bandwidth),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert!(error.to_string().contains("provisional install"));
    assert_eq!(
        runtime.config.read().await.to_toml_string().unwrap(),
        previous_config.to_toml_string().unwrap()
    );
    runtime
        .verify_peer_permit_configuration_identity(&previous_permits)
        .await
        .unwrap();
    assert!(runtime.seeder_registry.contains(&hash).await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, previous_torrent.state);
    assert_eq!(torrent.seeding_status, previous_torrent.seeding_status);
    assert_eq!(torrent.error, previous_torrent.error);
    assert_eq!(
        torrent.containment_recovery_intent,
        previous_torrent.containment_recovery_intent
    );
    let queue = runtime.queue.lock().await;
    assert_eq!(queue.order, queue_order);
    assert_eq!(queue.bypass, queue_bypass);
    drop(queue);
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
    runtime.force_stop_seeder(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn put_peer_limits_persists_new_pools_and_reconstructs_live_seeder() {
    let (runtime, hash, root, config_path) = peer_reconfiguration_fixture("peer-put-commit").await;
    let previous = runtime.current_peer_permit_configuration().await;
    let mut next = runtime.config.read().await.clone();
    next.bandwidth.max_peers = 1;
    next.bandwidth.max_peers_per_torrent = 1;

    runtime.replace_config(next).await.unwrap();

    let current = runtime.current_peer_permit_configuration().await;
    assert_eq!(current.global.snapshot().limit, 1);
    assert_eq!(current.per_torrent[&hash].snapshot().limit, 1);
    assert!(!Arc::ptr_eq(&current.global, &previous.global));
    assert!(!Arc::ptr_eq(
        &current.per_torrent[&hash],
        &previous.per_torrent[&hash]
    ));
    assert_eq!(
        Config::from_file(&config_path).unwrap().bandwidth.max_peers,
        1
    );
    assert!(runtime.seeder_registry.contains(&hash).await);
    runtime.force_stop_seeder(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn put_peer_limits_failure_restores_runtime_file_and_live_ownership() {
    let (runtime, hash, root, config_path) =
        peer_reconfiguration_fixture("peer-put-rollback").await;
    let previous_config = runtime.config.read().await.clone();
    let previous_permits = runtime.current_peer_permit_configuration().await;
    let previous_file = std::fs::read(&config_path).unwrap();
    let previous_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    let mut next = previous_config.clone();
    next.bandwidth.max_peers = 1;
    next.bandwidth.max_peers_per_torrent = 1;
    runtime.inject_peer_reconfiguration_failure_after_teardown();

    let error = runtime.replace_config(next).await.unwrap_err();

    assert!(error.to_string().contains("provisional install"));
    assert_eq!(
        runtime.config.read().await.to_toml_string().unwrap(),
        previous_config.to_toml_string().unwrap()
    );
    runtime
        .verify_peer_permit_configuration_identity(&previous_permits)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
    assert!(runtime.seeder_registry.contains(&hash).await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, previous_torrent.state);
    assert_eq!(torrent.seeding_status, previous_torrent.seeding_status);
    assert_eq!(torrent.error, previous_torrent.error);
    assert_eq!(
        torrent.containment_recovery_intent,
        previous_torrent.containment_recovery_intent
    );
    runtime.force_stop_seeder(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn manual_peer_ban_persistence_failure_restores_prior_policy_and_live_sessions() {
    let (runtime, hash, root, config_path) =
        peer_reconfiguration_fixture("manual-peer-ban-persistence-rollback").await;
    let previous_filter = runtime.peer_filter.read().await.clone();
    let previous_file = std::fs::read(&config_path).unwrap();
    let previous_config = runtime.config.read().await.clone();
    runtime.inject_peer_reconfiguration_persistence_failure();

    let error = runtime
        .ban_peer(
            &hash,
            swarmotter_core::peer_filter::ManualPeerBan {
                ip: "203.0.113.7".into(),
                reason: Some("test rollback".into()),
            },
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("persistence failed"));
    let current_filter = runtime.peer_filter.read().await.clone();
    assert!(Arc::ptr_eq(&current_filter, &previous_filter));
    assert_eq!(
        runtime.config.read().await.to_toml_string().unwrap(),
        previous_config.to_toml_string().unwrap()
    );
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
    assert!(runtime.seeder_registry.contains(&hash).await);

    runtime.force_stop_seeder(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn peer_rows_mark_only_manual_bans_without_recording_admission_checks() {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.peer_filter.enabled = true;
    config.peer_filter.rules = vec!["198.51.100.0/24".into()];
    config.peer_filter.manual_bans = vec![swarmotter_core::peer_filter::ManualPeerBan {
        ip: "203.0.113.7".into(),
        reason: Some("operator ban".into()),
    }];
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(config, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "peer-row-ban-state.bin",
        b"peer row ban state",
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
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            peers: vec![
                swarmotter_core::peer::PeerAddr::from_socket_addr(
                    "203.0.113.7:6881".parse().unwrap(),
                ),
                swarmotter_core::peer::PeerAddr::from_socket_addr(
                    "198.51.100.9:6881".parse().unwrap(),
                ),
            ],
            ..Default::default()
        })),
    );
    let before = runtime.peer_filter.read().await.status().rejections;

    let peers = runtime.list_peers(&hash).await.unwrap();

    assert!(
        peers
            .iter()
            .find(|peer| peer.ip.to_string() == "203.0.113.7")
            .unwrap()
            .banned
    );
    assert!(
        !peers
            .iter()
            .find(|peer| peer.ip.to_string() == "198.51.100.9")
            .unwrap()
            .banned
    );
    let after = runtime.peer_filter.read().await.status().rejections;
    assert_eq!(after.ip_checks, before.ip_checks);
    assert_eq!(after.manual_bans, before.manual_bans);
    assert_eq!(after.configured_rules, before.configured_rules);
}

#[tokio::test]
async fn global_peer_unban_removes_a_manual_ban_without_a_torrent_scope() {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.peer_filter.enabled = true;
    config.peer_filter.manual_bans = vec![swarmotter_core::peer_filter::ManualPeerBan {
        ip: "203.0.113.7".into(),
        reason: Some("operator ban".into()),
    }];
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(config, health);

    let status = runtime
        .unban_global_peer("203.0.113.7".into())
        .await
        .unwrap();

    assert!(status.manual_bans.is_empty());
    assert!(runtime
        .config
        .read()
        .await
        .peer_filter
        .manual_bans
        .is_empty());
}

#[tokio::test]
async fn combined_peer_and_seeding_policy_update_commits_only_eligible_work() {
    let (runtime, hash, root, _) = peer_reconfiguration_fixture("peer-combined-seeding").await;
    runtime
        .registry
        .lock()
        .await
        .get_mut(&hash)
        .unwrap()
        .seeding
        .seed_forever = false;
    let mut next = runtime.config.read().await.clone();
    next.bandwidth.max_peers = 1;
    next.seeding.global_ratio_limit = Some(0.0);

    runtime.replace_config(next).await.unwrap();

    assert!(!runtime.seeder_registry.contains(&hash).await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, TorrentState::Completed);
    assert_eq!(torrent.seeding_status, SeedingStatus::StoppedRatio);
    assert_eq!(runtime.peer_permit_snapshot().await.limit, 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn late_persistence_failure_restores_candidate_only_queued_torrent() {
    let (runtime, first, root, config_path) =
        peer_reconfiguration_fixture("peer-candidate-queued-rollback").await;
    let (second, _) = add_complete_seed_fixture(
        &runtime,
        "candidate-only-seed.bin",
        b"generated candidate-only completed payload",
    )
    .await;
    runtime.reconcile_seeders().await;
    let prior_live = runtime
        .seeder_registry
        .keys()
        .await
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(prior_live.len(), 1);
    let queued = [first, second]
        .into_iter()
        .find(|hash| !prior_live.contains(hash))
        .unwrap();
    let queued_before = runtime.registry.lock().await.get(&queued).cloned().unwrap();
    assert_eq!(queued_before.state, TorrentState::Completed);
    assert_eq!(queued_before.seeding_status, SeedingStatus::Queued);
    let previous_permits = runtime.current_peer_permit_configuration().await;
    let previous_file = std::fs::read(&config_path).unwrap();
    let (queue_order, queue_bypass) = {
        let queue = runtime.queue.lock().await;
        (queue.order.clone(), queue.bypass.clone())
    };
    let mut next = runtime.config.read().await.clone();
    next.bandwidth.max_peers = 1;
    next.queue.max_active_seeds = 2;
    runtime.inject_peer_reconfiguration_persistence_failure();

    assert!(runtime.replace_config(next).await.is_err());

    runtime
        .verify_peer_permit_configuration_identity(&previous_permits)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
    assert_eq!(
        runtime
            .seeder_registry
            .keys()
            .await
            .into_iter()
            .collect::<HashSet<_>>(),
        prior_live
    );
    assert!(!runtime.seeder_registry.contains(&queued).await);
    let queued_after = runtime.registry.lock().await.get(&queued).cloned().unwrap();
    assert_eq!(queued_after.state, queued_before.state);
    assert_eq!(queued_after.seeding_status, queued_before.seeding_status);
    assert_eq!(queued_after.error, queued_before.error);
    assert_eq!(
        queued_after.containment_recovery_intent,
        queued_before.containment_recovery_intent
    );
    let queue = runtime.queue.lock().await;
    assert_eq!(queue.order, queue_order);
    assert_eq!(queue.bypass, queue_bypass);
    drop(queue);
    for hash in [first, second] {
        runtime.force_stop_seeder(&hash).await;
    }
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn failed_candidate_seeder_ownership_does_not_survive_state_reload() {
    let root = unique_dir("peer-state-rollback-reload");
    let config_path = root.join("swarmotter.toml");
    let state_path = root.join("daemon-state.json");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.display().to_string());
    config.torrent.listen_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    config.queue.max_active_seeds = 1;
    config.seeding.global_ratio_limit = None;
    config.seeding.global_idle_limit = None;
    config.bandwidth.max_peers = 3;
    write_config_atomically(&config_path, &config).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health.clone(),
        Some(config_path.clone()),
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let (first, _) = add_complete_seed_fixture(
        &runtime,
        "state-rollback-one.bin",
        b"generated state rollback one",
    )
    .await;
    let (second, _) = add_complete_seed_fixture(
        &runtime,
        "state-rollback-two.bin",
        b"generated state rollback two",
    )
    .await;
    runtime.reconcile_seeders().await;
    runtime.persist_state().await.unwrap();
    assert_eq!(runtime.seeder_registry.len().await, 1);
    let prior_live = runtime.seeder_registry.keys().await[0];
    let candidate_only = [first, second]
        .into_iter()
        .find(|hash| *hash != prior_live)
        .unwrap();
    let mut next = config.clone();
    next.bandwidth.max_peers = 1;
    next.queue.max_active_seeds = 2;
    runtime.inject_peer_reconfiguration_persistence_failure();
    assert!(runtime.replace_config(next).await.is_err());
    assert_eq!(runtime.seeder_registry.len().await, 1);
    let stored = crate::state_store::load(&state_path)
        .unwrap()
        .expect("rollback must retain the daemon state file");
    let stored_live = stored
        .torrents
        .iter()
        .find(|torrent| torrent.key() == prior_live)
        .unwrap();
    let stored_candidate = stored
        .torrents
        .iter()
        .find(|torrent| torrent.key() == candidate_only)
        .unwrap();
    assert_eq!(stored_live.state, TorrentState::Seeding);
    assert_eq!(stored_live.seeding_status, SeedingStatus::Active);
    assert_eq!(stored_candidate.state, TorrentState::Completed);
    assert_eq!(stored_candidate.seeding_status, SeedingStatus::Queued);
    for hash in [first, second] {
        runtime.force_stop_seeder(&hash).await;
    }

    let restored = DaemonRuntime::with_paths_broker_and_state(
        config,
        health,
        Some(config_path),
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 2);
    assert_eq!(restored.seeder_registry.len().await, 1);
    let torrents = restored
        .registry
        .lock()
        .await
        .torrents
        .values()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        torrents
            .iter()
            .filter(|torrent| torrent.seeding_status == SeedingStatus::Active)
            .count(),
        1
    );
    assert_eq!(
        torrents
            .iter()
            .filter(|torrent| torrent.seeding_status == SeedingStatus::Queued)
            .count(),
        1
    );
    for hash in [first, second] {
        restored.force_stop_seeder(&hash).await;
    }
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn fast_candidate_completion_cannot_selfish_remove_before_failed_persistence() {
    let root = unique_dir("peer-selfish-persistence-rollback");
    let config_path = root.join("swarmotter.toml");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.display().to_string());
    config.torrent.listen_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    config.torrent.selfish = false;
    config.queue.auto_start = false;
    config.dht.enabled = false;
    config.pex.enabled = false;
    config.bandwidth.max_peers = 3;
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
    let content = b"generated fast completion rollback payload";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "fast-candidate.bin",
        content,
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), root.clone());
    for piece in 0..meta.piece_count() {
        let start = piece * meta.piece_length as usize;
        let end = (start + meta.piece_length as usize).min(content.len());
        storage
            .write_piece(piece, &content[start..end])
            .await
            .unwrap();
    }
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, now()))
        .unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.ensure_torrent_peer_permit_pool(hash).await;
    let previous_file = std::fs::read(&config_path).unwrap();
    let (persistence_reached, continue_persistence) = runtime
        .pause_peer_reconfiguration_before_persistence()
        .await;
    runtime.inject_peer_reconfiguration_persistence_failure();
    let mut next = config;
    next.bandwidth.max_peers = 1;
    next.queue.auto_start = true;
    next.torrent.selfish = true;
    let update_runtime = runtime.clone();
    let update = tokio::spawn(async move { update_runtime.replace_config(next).await });
    persistence_reached.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let complete = runtime
                .registry
                .lock()
                .await
                .get(&hash)
                .is_some_and(|torrent| torrent.progress.is_complete());
            if complete {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(runtime.registry.lock().await.contains(&hash));
    assert!(!runtime.selfish_completion_enabled.load(Ordering::Acquire));
    continue_persistence.send(()).unwrap();
    assert!(update.await.unwrap().is_err());

    assert!(runtime.registry.lock().await.contains(&hash));
    assert!(!runtime.config.read().await.torrent.selfish);
    assert!(!runtime.selfish_completion_enabled.load(Ordering::Acquire));
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
    assert_eq!(
        runtime.registry.lock().await.get(&hash).unwrap().state,
        TorrentState::Queued
    );
    assert_eq!(
        tokio::fs::read(storage.file_path(0).unwrap())
            .await
            .unwrap(),
        content
    );
    runtime.force_stop_engine(&hash).await;
    runtime.force_stop_seeder(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn combined_peer_and_occupied_listener_update_rolls_back_live_seeder() {
    let (runtime, hash, root, config_path) =
        peer_reconfiguration_fixture("peer-combined-listener-rollback").await;
    let occupied = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    let previous_config = runtime.config.read().await.clone();
    let previous_permits = runtime.current_peer_permit_configuration().await;
    let previous_file = std::fs::read(&config_path).unwrap();
    let previous_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    let mut next = previous_config.clone();
    next.bandwidth.max_peers = 1;
    next.torrent.listen_port = occupied_port;

    let error = runtime.replace_config(next).await.unwrap_err();

    assert!(error.to_string().contains("reconstruction failed"));
    runtime
        .verify_peer_permit_configuration_identity(&previous_permits)
        .await
        .unwrap();
    assert_eq!(
        runtime.config.read().await.to_toml_string().unwrap(),
        previous_config.to_toml_string().unwrap()
    );
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_file);
    assert!(runtime.seeder_registry.contains(&hash).await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, previous_torrent.state);
    assert_eq!(torrent.seeding_status, previous_torrent.seeding_status);
    assert_eq!(torrent.error, previous_torrent.error);
    assert_eq!(
        torrent.containment_recovery_intent,
        previous_torrent.containment_recovery_intent
    );
    runtime.force_stop_seeder(&hash).await;
    drop(occupied);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn active_engine_patch_reconstructs_on_commit_and_exactly_rolls_back_failure() {
    let (runtime, hash, root, _) =
        active_engine_reconfiguration_fixture("active-engine-patch").await;
    let initial = runtime.current_peer_permit_configuration().await;
    let (queue_order, queue_bypass) = {
        let queue = runtime.queue.lock().await;
        (queue.order.clone(), queue.bypass.clone())
    };
    let mut bandwidth = runtime.config.read().await.bandwidth.clone();
    bandwidth.max_peers = 1;
    bandwidth.max_peers_per_torrent = 1;
    runtime
        .update_settings(swarmotter_api::state::SettingsPatch {
            bandwidth: Some(bandwidth),
            ..Default::default()
        })
        .await
        .unwrap();
    let committed = runtime.current_peer_permit_configuration().await;
    assert!(!Arc::ptr_eq(&initial.global, &committed.global));
    assert_eq!(committed.global.snapshot().limit, 1);
    assert!(runtime.engine_running_for_key_for_test(hash).await);

    let committed_config = runtime.config.read().await.clone();
    let committed_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    let mut rejected = committed_config.bandwidth.clone();
    rejected.max_peers = 2;
    rejected.max_peers_per_torrent = 2;
    runtime.inject_peer_reconfiguration_failure_after_teardown();
    assert!(runtime
        .update_settings(swarmotter_api::state::SettingsPatch {
            bandwidth: Some(rejected),
            ..Default::default()
        })
        .await
        .is_err());
    runtime
        .verify_peer_permit_configuration_identity(&committed)
        .await
        .unwrap();
    assert!(runtime.engine_running_for_key_for_test(hash).await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, committed_torrent.state);
    assert_eq!(torrent.error, committed_torrent.error);
    assert_eq!(
        torrent.containment_recovery_intent,
        committed_torrent.containment_recovery_intent
    );
    let queue = runtime.queue.lock().await;
    assert_eq!(queue.order, queue_order);
    assert_eq!(queue.bypass, queue_bypass);
    drop(queue);
    runtime.force_stop_engine(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn unrelated_engine_start_cannot_enter_mid_peer_reconstruction() {
    let (runtime, active_hash, root, _) =
        active_engine_reconfiguration_fixture("peer-start-exclusion").await;
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "unrelated-reconfiguration-start.bin",
        b"generated unrelated queued torrent",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let unrelated_hash = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, now()))
        .unwrap();
    runtime.queue.lock().await.add(unrelated_hash);
    runtime
        .ensure_torrent_peer_permit_pool(unrelated_hash)
        .await;
    let (reconstruction_reached, continue_reconstruction) = runtime
        .pause_peer_reconfiguration_before_reconstruction()
        .await;
    let update_runtime = runtime.clone();
    let mut bandwidth = runtime.config.read().await.bandwidth.clone();
    bandwidth.max_peers = 1;
    let update = tokio::spawn(async move {
        update_runtime
            .update_settings(swarmotter_api::state::SettingsPatch {
                bandwidth: Some(bandwidth),
                ..Default::default()
            })
            .await
    });
    reconstruction_reached.await.unwrap();

    let start_runtime = runtime.clone();
    let unrelated_start =
        tokio::spawn(async move { start_runtime.start_engine(unrelated_hash).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!unrelated_start.is_finished());
    assert!(
        !runtime
            .engine_running_for_key_for_test(unrelated_hash)
            .await
    );
    continue_reconstruction.send(()).unwrap();
    update.await.unwrap().unwrap();
    unrelated_start.await.unwrap();
    assert!(runtime.engine_running_for_key_for_test(active_hash).await);
    assert!(
        runtime
            .engine_running_for_key_for_test(unrelated_hash)
            .await
    );
    runtime.force_stop_engine(&active_hash).await;
    runtime.force_stop_engine(&unrelated_hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn active_engine_put_reconstructs_persists_and_rolls_back_failure() {
    let (runtime, hash, root, config_path) =
        active_engine_reconfiguration_fixture("active-engine-put").await;
    let mut next = runtime.config.read().await.clone();
    next.bandwidth.max_peers = 1;
    next.bandwidth.max_peers_per_torrent = 1;
    runtime.replace_config(next).await.unwrap();
    let committed = runtime.current_peer_permit_configuration().await;
    let committed_config = runtime.config.read().await.clone();
    let committed_file = std::fs::read(&config_path).unwrap();
    let committed_torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(committed.global.snapshot().limit, 1);
    assert!(runtime.engine_running_for_key_for_test(hash).await);
    assert_eq!(
        Config::from_file(&config_path).unwrap().bandwidth.max_peers,
        1
    );

    let mut rejected = committed_config.clone();
    rejected.bandwidth.max_peers = 2;
    rejected.bandwidth.max_peers_per_torrent = 2;
    runtime.inject_peer_reconfiguration_failure_after_teardown();
    assert!(runtime.replace_config(rejected).await.is_err());
    runtime
        .verify_peer_permit_configuration_identity(&committed)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&config_path).unwrap(), committed_file);
    assert!(runtime.engine_running_for_key_for_test(hash).await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, committed_torrent.state);
    assert_eq!(torrent.error, committed_torrent.error);
    assert_eq!(
        torrent.containment_recovery_intent,
        committed_torrent.containment_recovery_intent
    );

    let mut persistence_rejected = committed_config.clone();
    persistence_rejected.bandwidth.max_peers = 2;
    persistence_rejected.bandwidth.max_peers_per_torrent = 2;
    runtime.inject_peer_reconfiguration_persistence_failure();
    let error = runtime
        .replace_config(persistence_rejected)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("persistence failed"));
    runtime
        .verify_peer_permit_configuration_identity(&committed)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&config_path).unwrap(), committed_file);
    assert!(runtime.engine_running_for_key_for_test(hash).await);
    runtime.force_stop_engine(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn valid_blocked_peer_reconfiguration_commits_recovery_intent_without_live_tasks() {
    let (runtime, hash, root, config_path) =
        active_engine_reconfiguration_fixture("active-engine-blocked-put").await;
    let previous = runtime.current_peer_permit_configuration().await;
    let mut next = runtime.config.read().await.clone();
    next.bandwidth.max_peers = 1;
    next.network.mode = NetworkContainmentMode::Strict;
    next.network.required_interface = Some(format!(
        "swarmotter-missing-interface-{}",
        std::process::id()
    ));
    next.network.fail_closed = true;

    runtime.replace_config(next.clone()).await.unwrap();

    let current = runtime.current_peer_permit_configuration().await;
    assert!(!Arc::ptr_eq(&current.global, &previous.global));
    assert_eq!(current.global.snapshot().limit, 1);
    assert!(!runtime.engine_running_for_key_for_test(hash).await);
    assert!(runtime.seeder_registry.is_empty().await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, TorrentState::NetworkBlocked);
    assert_eq!(
        torrent.containment_recovery_intent,
        Some(ContainmentRecoveryIntent::Downloading)
    );
    assert_eq!(
        Config::from_file(&config_path).unwrap().bandwidth.max_peers,
        1
    );
    assert!(!runtime.network_health.read().await.traffic_allowed);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn combined_peer_and_blocked_to_healthy_update_recovers_under_transition_lock() {
    let root = unique_dir("peer-blocked-to-healthy");
    let config_path = root.join("swarmotter.toml");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Strict;
    config.network.required_interface = Some(format!(
        "swarmotter-missing-recovery-interface-{}",
        std::process::id()
    ));
    config.storage.download_dir = Some(root.display().to_string());
    config.torrent.listen_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    config.dht.enabled = false;
    config.pex.enabled = false;
    config.bandwidth.max_peers = 3;
    write_config_atomically(&config_path, &config).unwrap();
    let health = net::evaluate(&config.network, &OsInterfaceProbe);
    assert!(!health.traffic_allowed);
    let runtime = DaemonRuntime::with_paths_and_broker(
        config.clone(),
        health.clone(),
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "blocked-recovery.bin",
        b"generated blocked recovery torrent",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::NetworkBlocked;
    torrent.error = Some(health.detail);
    torrent.containment_recovery_intent = Some(ContainmentRecoveryIntent::Downloading);
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.ensure_torrent_peer_permit_pool(hash).await;

    let mut next = config;
    next.network = swarmotter_core::net::NetworkConfig {
        mode: NetworkContainmentMode::Disabled,
        ..Default::default()
    };
    next.bandwidth.max_peers = 1;
    runtime.replace_config(next).await.unwrap();

    assert!(runtime.network_health.read().await.traffic_allowed);
    assert!(runtime.engine_running_for_key_for_test(hash).await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, TorrentState::Downloading);
    assert_eq!(torrent.containment_recovery_intent, None);
    assert_eq!(runtime.peer_permit_snapshot().await.limit, 1);
    assert_eq!(
        Config::from_file(&config_path).unwrap().bandwidth.max_peers,
        1
    );
    runtime.force_stop_engine(&hash).await;
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn concurrent_engine_starts_create_one_owned_task() {
    let root = unique_dir("concurrent-engine-start");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = 0;
    cfg.dht.enabled = false;
    cfg.pex.enabled = false;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "single-engine.bin",
        b"single owned engine",
        8,
        None,
        false,
    );
    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    runtime.registry.lock().await.get_mut(&hash).unwrap().state = TorrentState::Queued;

    tokio::join!(runtime.start_engine(hash), runtime.start_engine(hash));

    assert_eq!(runtime.engine_handles.read().await.len(), 1);
    assert_eq!(runtime.engine_cmds.lock().await.len(), 1);
    runtime.force_stop_engine(&hash).await;
    assert!(runtime.engine_handles.read().await.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn failed_shared_listener_bind_does_not_register_or_announce_seeder() {
    let occupied = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    let root = unique_dir("seeder-bind-failure");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = port;
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "bind-failure.bin",
        b"bind failure payload",
        8,
        Some("http://127.0.0.1:1/announce"),
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Completed;
    torrent.seeding.seed_forever = true;
    for piece in 0..meta.piece_count() {
        torrent.progress.have_piece(piece);
    }
    runtime.registry.lock().await.add(torrent).unwrap();

    runtime.reconcile_seeders().await;

    assert!(!runtime.seeder_shutdowns.lock().await.contains_key(&hash));
    assert!(!runtime.seeder_handles.lock().await.contains_key(&hash));
    assert!(runtime.seeder_registry.is_empty().await);
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert!(matches!(
        torrent.state,
        TorrentState::Completed | TorrentState::Seeding
    ));
    assert!(matches!(
        torrent.seeding_status,
        SeedingStatus::Queued | SeedingStatus::Active
    ));
    assert!(torrent.error.is_some());
    drop(occupied);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn complete_seeding_lifecycle_policy_slots_tasks_and_limiter_identity_are_truthful() {
    let root = unique_dir("phase4-seeding-lifecycle");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = 0;
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.queue.max_active_seeds = 1;
    cfg.seeding.global_ratio_limit = None;
    cfg.seeding.global_idle_limit = None;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);
    let (first, first_limiter) =
        add_complete_seed_fixture(&runtime, "seed-one.bin", b"first generated seed payload").await;
    let (second, second_limiter) =
        add_complete_seed_fixture(&runtime, "seed-two.bin", b"second generated seed payload").await;

    runtime.reconcile_seeders().await;
    assert_seeder_state_registry_invariant(&runtime).await;
    let first_status = runtime
        .registry
        .lock()
        .await
        .get(&first)
        .unwrap()
        .seeding_status;
    let second_status = runtime
        .registry
        .lock()
        .await
        .get(&second)
        .unwrap()
        .seeding_status;
    assert_eq!(
        [first_status, second_status]
            .into_iter()
            .filter(|status| *status == SeedingStatus::Active)
            .count(),
        1
    );
    assert_eq!(
        [first_status, second_status]
            .into_iter()
            .filter(|status| *status == SeedingStatus::Queued)
            .count(),
        1
    );
    assert_eq!(runtime.global_stats().await.active_seeds, 1);

    runtime.config.write().await.queue.max_active_seeds = 2;
    runtime.reconcile_seeders().await;
    assert_seeder_state_registry_invariant(&runtime).await;
    assert_eq!(runtime.global_stats().await.active_seeds, 2);
    let retained = runtime.torrent_limiters.read().await;
    assert!(Arc::ptr_eq(retained.get(&first).unwrap(), &first_limiter));
    assert!(Arc::ptr_eq(retained.get(&second).unwrap(), &second_limiter));
    drop(retained);

    // A complete imported/restored torrent may have no download counter.
    // Explicit zero is still an immediate target through the production
    // policy replacement path; it must not depend on ratio division.
    runtime
        .registry
        .lock()
        .await
        .get_mut(&first)
        .unwrap()
        .downloaded = 0;
    let mut policy_events = runtime.event_broker.subscribe();
    runtime
        .set_torrent_seeding(
            &first,
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: Some(0.0),
                idle_limit: None,
                seed_forever: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&first)
            .unwrap()
            .seeding_status,
        SeedingStatus::StoppedRatio
    );
    assert!(!runtime.seeder_registry.contains(&first).await);
    assert_seeder_state_registry_invariant(&runtime).await;
    let stopped_event = loop {
        let event = tokio::time::timeout(Duration::from_secs(1), policy_events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if event.kind == "torrent_changed"
            && event.info_hash.as_deref() == Some(first.to_locator().as_str())
        {
            break event;
        }
    };
    let stopped_payload: serde_json::Value = serde_json::from_str(&stopped_event.json).unwrap();
    assert_eq!(stopped_payload["payload"]["state"], "completed");

    runtime
        .set_torrent_seeding(
            &first,
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: Some(2.0),
                idle_limit: None,
                seed_forever: false,
            },
        )
        .await
        .unwrap();
    assert!(runtime.seeder_registry.contains(&first).await);
    assert_seeder_state_registry_invariant(&runtime).await;

    runtime
        .set_torrent_seeding(
            &first,
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: Some(2.0),
                idle_limit: Some(0),
                seed_forever: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&first)
            .unwrap()
            .seeding_status,
        SeedingStatus::StoppedIdle
    );

    runtime
        .set_torrent_seeding(
            &first,
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: Some(0.0),
                idle_limit: Some(0),
                seed_forever: true,
            },
        )
        .await
        .unwrap();
    runtime.pause(&first).await.unwrap();
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&first)
            .unwrap()
            .seeding_status,
        SeedingStatus::StoppedManual
    );
    assert!(!runtime.seeder_registry.contains(&first).await);
    assert!(Arc::ptr_eq(
        runtime.torrent_limiters.read().await.get(&first).unwrap(),
        &first_limiter
    ));

    runtime
        .set_torrent_seeding(
            &first,
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: None,
                idle_limit: None,
                seed_forever: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&first)
            .unwrap()
            .seeding_status,
        SeedingStatus::StoppedManual,
        "policy updates must not auto-resume a manual pause"
    );
    let mut resume_events = runtime.event_broker.subscribe();
    runtime.resume(&first).await.unwrap();
    assert!(runtime.seeder_registry.contains(&first).await);
    assert_seeder_state_registry_invariant(&runtime).await;
    let resumed_event = loop {
        let event = tokio::time::timeout(Duration::from_secs(1), resume_events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if event.kind == "torrent_changed" {
            break event;
        }
    };
    let resumed_payload: serde_json::Value = serde_json::from_str(&resumed_event.json).unwrap();
    assert_eq!(resumed_payload["payload"]["state"], "seeding");
    assert_eq!(
        runtime.get_torrent(&first).await.unwrap().state,
        TorrentState::Seeding
    );

    runtime.pause(&first).await.unwrap();
    runtime.start_now(&first).await.unwrap();
    assert!(runtime.seeder_registry.contains(&first).await);
    assert_eq!(
        runtime.get_torrent(&first).await.unwrap().state,
        TorrentState::Seeding
    );
    assert_seeder_state_registry_invariant(&runtime).await;

    runtime.force_stop_seeder(&first).await;
    assert!(!runtime.seeder_registry.contains(&first).await);
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&first)
            .unwrap()
            .seeding_status,
        SeedingStatus::Queued
    );
    runtime.reconcile_seeders().await;
    assert!(runtime.seeder_registry.contains(&first).await);
    assert_seeder_state_registry_invariant(&runtime).await;

    runtime.remove_torrent(&first, false).await.unwrap();
    assert!(!runtime.seeder_registry.contains(&first).await);
    assert!(!runtime.torrent_limiters.read().await.contains_key(&first));
    assert_seeder_state_registry_invariant(&runtime).await;
    runtime.remove_torrent(&second, false).await.unwrap();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn active_seeding_containment_block_preserves_status_and_recovery_rebuilds_task() {
    let root = unique_dir("seeding-containment-recovery");
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
    let runtime = DaemonRuntime::new(cfg, health);
    let (hash, limiter) = add_complete_seed_fixture(
        &runtime,
        "containment-seed.bin",
        b"generated containment seed payload",
    )
    .await;
    runtime.reconcile_seeders().await;
    assert!(runtime.seeder_registry.contains(&hash).await);
    assert_seeder_state_registry_invariant(&runtime).await;

    let mut blocked_events = runtime.event_broker.subscribe();
    runtime
        .transition_data_plane_to_blocked(
            swarmotter_core::models::network::NetworkContainmentStatus::InterfaceMissing,
            "test interface disappeared".into(),
        )
        .await;
    assert!(!runtime.seeder_registry.contains(&hash).await);
    let blocked = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(blocked.state, TorrentState::NetworkBlocked);
    assert_eq!(blocked.seeding_status, SeedingStatus::Active);
    assert_eq!(
        blocked.containment_recovery_intent,
        Some(ContainmentRecoveryIntent::Seeding)
    );
    assert!(Arc::ptr_eq(
        runtime.torrent_limiters.read().await.get(&hash).unwrap(),
        &limiter
    ));
    let blocked_summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(blocked_summary.state, TorrentState::NetworkBlocked);
    assert_eq!(blocked_summary.seeding_status, SeedingStatus::Active);
    assert_eq!(
        runtime
            .list_torrents()
            .await
            .into_iter()
            .find(|summary| summary.info_hash == hash)
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );
    assert_eq!(
        runtime.torrent_stats(&hash).await.unwrap().state,
        TorrentState::NetworkBlocked
    );
    assert_eq!(runtime.global_stats().await.active_seeds, 0);
    assert_seeder_state_registry_invariant(&runtime).await;
    let blocked_event = loop {
        let event = tokio::time::timeout(Duration::from_secs(1), blocked_events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if event.kind == "torrent_changed"
            && event.info_hash.as_deref() == Some(hash.to_locator().as_str())
        {
            break event;
        }
    };
    let blocked_payload: serde_json::Value = serde_json::from_str(&blocked_event.json).unwrap();
    assert_eq!(blocked_payload["payload"]["state"], "network_blocked");

    let mut recovered_health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "recovered",
    );
    recovered_health.traffic_allowed = true;
    let mut recovery_events = runtime.event_broker.subscribe();
    runtime.recover_containment_work(recovered_health).await;
    assert!(runtime.seeder_registry.contains(&hash).await);
    let recovered = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(recovered.state, TorrentState::Seeding);
    assert_eq!(recovered.seeding_status, SeedingStatus::Active);
    assert!(recovered.containment_recovery_intent.is_none());
    assert!(Arc::ptr_eq(
        runtime.torrent_limiters.read().await.get(&hash).unwrap(),
        &limiter
    ));
    let recovered_summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(recovered_summary.state, TorrentState::Seeding);
    assert_eq!(recovered_summary.seeding_status, SeedingStatus::Active);
    assert_eq!(
        runtime.torrent_stats(&hash).await.unwrap().state,
        TorrentState::Seeding
    );
    assert_eq!(runtime.global_stats().await.active_seeds, 1);
    assert_seeder_state_registry_invariant(&runtime).await;
    let recovery_event = loop {
        let event = tokio::time::timeout(Duration::from_secs(1), recovery_events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if event.kind == "torrent_changed"
            && event.info_hash.as_deref() == Some(hash.to_locator().as_str())
        {
            break event;
        }
    };
    let payload: serde_json::Value = serde_json::from_str(&recovery_event.json).unwrap();
    assert_eq!(payload["payload"]["state"], "seeding");
    runtime.remove_torrent(&hash, false).await.unwrap();
    std::fs::remove_dir_all(root).ok();
}

/// End-to-end live shaping through the API-facing daemon operation. The
/// first block consumes the retained limiter's initial 1 KiB burst. The
/// second remains blocked at 400 ms under 1 KiB/s, then completes at the
/// bounded 500 ms wake after `set_torrent_limits` raises the live rate.
#[tokio::test(start_paused = true)]
async fn daemon_limit_update_changes_active_registered_upload_without_replacement() {
    use swarmotter_core::bandwidth::{RateDirection, TorrentBandwidth};
    use swarmotter_core::peer::{self, Handshake, Message, PeerReader};

    let root = unique_dir("daemon-live-seed-limit");
    let state_path = root.join("state.json");
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = port;
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
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let content = vec![0x3cu8; 4096];
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "daemon-live-limit.bin",
        &content,
        4096,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), root.clone());
    storage.write_piece(0, &content).await.unwrap();
    let mut torrent = Torrent::new(meta.clone(), now());
    torrent.state = TorrentState::Completed;
    torrent.downloaded = meta.total_length;
    torrent.upload_limit = 1024;
    torrent.date_completed = Some(now());
    torrent.seeding.seed_forever = true;
    torrent.progress.have_piece(0);
    torrent.recompute_file_bytes_completed();
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    let limiter = runtime.ensure_torrent_limiter(hash, 0, 1024).await;
    runtime.persist_state().await.unwrap();
    runtime.reconcile_seeders().await;
    assert_seeder_state_registry_invariant(&runtime).await;
    let live_state = runtime
        .engine_states
        .read()
        .await
        .get(&hash)
        .cloned()
        .expect("active seeder must retain its live engine state");
    let registered_limiter = runtime
        .seeder_registry
        .limiter_for_test(&hash)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&limiter, &registered_limiter));

    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let (read, mut write) = tokio::io::split(stream);
    peer::write_handshake(
        &mut write,
        &Handshake {
            info_hash: hash.as_v1().unwrap(),
            peer_id: make_peer_id(),
            reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
        },
    )
    .await
    .unwrap();
    let mut reader = PeerReader::new(read);
    reader.read_handshake().await.unwrap();
    assert!(matches!(
        reader.read_message().await.unwrap(),
        Some(Message::Bitfield { .. })
    ));
    peer::write_message(&mut write, &Message::Interested)
        .await
        .unwrap();
    loop {
        if matches!(reader.read_message().await.unwrap(), Some(Message::Unchoke)) {
            break;
        }
    }

    for offset in [0u32, 1024] {
        peer::write_message(
            &mut write,
            &Message::Request {
                piece: 0,
                offset,
                length: 1024,
            },
        )
        .await
        .unwrap();
        if offset == 0 {
            assert!(matches!(
                reader.read_message().await.unwrap(),
                Some(Message::Piece { block, .. }) if block.len() == 1024
            ));
        }
    }

    let second_block = tokio::spawn(async move { reader.read_message().await });
    let dispatch_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while live_state.lock().await.uploaded != 2048 {
        assert!(
            std::time::Instant::now() < dispatch_deadline,
            "second upload request did not reach the live limiter"
        );
        std::thread::yield_now();
        tokio::task::yield_now().await;
    }
    // Accounting occurs immediately before the limiter await. Yield once
    // more so the existing 500 ms sleep is armed before virtual time moves.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(400)).await;
    tokio::task::yield_now().await;
    assert!(!second_block.is_finished());
    runtime
        .set_torrent_limits(
            &hash,
            TorrentBandwidth {
                download: 0,
                upload: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&hash)
            .unwrap()
            .upload_limit,
        4096
    );
    let persisted = crate::state_store::load(&state_path)
        .unwrap()
        .unwrap()
        .torrents
        .into_iter()
        .find(|torrent| torrent.key() == hash)
        .unwrap();
    assert_eq!(persisted.upload_limit, 4096);
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
    tokio::time::advance(Duration::from_millis(100)).await;
    for _ in 0..100 {
        if second_block.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        second_block.is_finished(),
        "new 4 KiB/s window was not observed live"
    );
    assert!(matches!(
        second_block.await.unwrap().unwrap(),
        Some(Message::Piece { block, .. }) if block.len() == 1024
    ));
    assert_eq!(limiter.capacity(RateDirection::Upload), 4096);
    assert!(runtime.seeder_registry.contains(&hash).await);
    assert_seeder_state_registry_invariant(&runtime).await;

    runtime.remove_torrent(&hash, false).await.unwrap();
    std::fs::remove_dir_all(root).ok();
}
