// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn watch_profile_captures_storage_before_registration() {
    use swarmotter_core::config::StartBehavior;
    use swarmotter_core::policy::{PolicyProfile, PolicyStorage};

    let root = unique_dir("watch-profile-storage");
    let complete = root.join("profile-complete");
    let incomplete = root.join("profile-incomplete");
    std::fs::create_dir_all(&complete).unwrap();
    std::fs::create_dir_all(&incomplete).unwrap();
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "watch-profile.bin",
        b"watch profile payload",
        8,
        None,
        false,
    );
    std::fs::write(root.join("watch-profile.torrent"), bytes).unwrap();
    let mut config = watch_test_config(&root, StartBehavior::Paused);
    config.profiles.profiles.insert(
        "archive".into(),
        PolicyProfile {
            storage: PolicyStorage {
                download_dir: Some(complete.display().to_string()),
                incomplete_dir: Some(incomplete.display().to_string()),
            },
            ..Default::default()
        },
    );
    config.watch[0].profile = Some("archive".into());
    let runtime = DaemonRuntime::new(config, disabled_health());

    runtime.watch_scan().await.unwrap();
    runtime.watch_scan().await.unwrap();
    let torrent = {
        let registry = runtime.registry.lock().await;
        registry
            .list()
            .first()
            .map(|torrent| (*torrent).clone())
            .unwrap()
    };
    assert_eq!(torrent.policy.profile.as_deref(), Some("archive"));
    assert_eq!(
        torrent.policy.profile_origin,
        Some(swarmotter_core::policy::PolicyProfileOrigin::WatchFolder)
    );
    assert_eq!(
        runtime.policy_storage_paths(&torrent).await,
        (
            complete.display().to_string(),
            incomplete.display().to_string(),
        )
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn watch_partial_copy_and_read_time_change_reset_without_terminal_result() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-partial-stability");
    let partial_path = root.join("a-partial.torrent");
    let first = swarmotter_core::meta::build_single_file_torrent(
        "partial-complete.bin",
        b"generated partial copy payload",
        8,
        None,
        false,
    );
    std::fs::write(&partial_path, &first[..first.len() / 2]).unwrap();
    let runtime = Arc::new(DaemonRuntime::new(
        watch_test_config(&root, StartBehavior::Paused),
        disabled_health(),
    ));

    runtime.watch_scan().await.unwrap();
    std::fs::write(&partial_path, &first).unwrap();
    runtime.watch_scan().await.unwrap();
    assert!(runtime.watch_history().await.is_empty());
    assert!(runtime.registry.lock().await.torrents.is_empty());
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_history().await.len(), 1);
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);

    let changing_path = root.join("z-changing.torrent");
    let before = swarmotter_core::meta::build_single_file_torrent(
        "before-read-change.bin",
        b"before read change",
        8,
        None,
        false,
    );
    let after = swarmotter_core::meta::build_single_file_torrent(
        "after-read-change.bin",
        b"after read change with a different length",
        8,
        None,
        false,
    );
    std::fs::write(&changing_path, before).unwrap();
    runtime.watch_scan().await.unwrap();
    let (read_reached, continue_read) = runtime.pause_watch_after_bounded_read().await;
    let scanning = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.watch_scan().await })
    };
    read_reached.await.unwrap();
    std::fs::write(&changing_path, &after).unwrap();
    continue_read.send(()).unwrap();
    scanning.await.unwrap().unwrap();
    assert_eq!(runtime.watch_history().await.len(), 1);
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);

    runtime.watch_scan().await.unwrap();
    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 2);
    assert!(history.iter().all(|result| result.success));
    assert_eq!(runtime.registry.lock().await.torrents.len(), 2);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn watch_leave_processes_each_fingerprint_once_and_status_excludes_it() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-leave-once");
    let source = root.join("leave.torrent");
    let first = swarmotter_core::meta::build_single_file_torrent(
        "leave-first.bin",
        b"first generated leave payload",
        8,
        None,
        false,
    );
    std::fs::write(&source, first).unwrap();
    let runtime = DaemonRuntime::new(
        watch_test_config(&root, StartBehavior::Paused),
        disabled_health(),
    );
    runtime.watch_scan().await.unwrap();
    for _ in 0..2 {
        let status = runtime.watch_status().await;
        assert_eq!(status.folders[0].pending_torrent_files, 1);
        assert!(runtime.watch_history().await.is_empty());
        assert!(runtime.registry.lock().await.torrents.is_empty());
    }
    runtime.watch_scan().await.unwrap();
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_history().await.len(), 1);
    assert!(source.exists());
    assert_eq!(
        runtime.watch_status().await.folders[0].pending_torrent_files,
        0
    );

    let replacement = swarmotter_core::meta::build_single_file_torrent(
        "leave-replacement.bin",
        b"second generated leave payload with changed length",
        8,
        None,
        false,
    );
    std::fs::write(&source, replacement).unwrap();
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_history().await.len(), 1);
    assert_eq!(
        runtime.watch_status().await.folders[0].pending_torrent_files,
        1
    );
    runtime.watch_scan().await.unwrap();
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_history().await.len(), 2);
    assert_eq!(runtime.registry.lock().await.torrents.len(), 2);
    assert_eq!(
        runtime.watch_status().await.folders[0].pending_torrent_files,
        0
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn watch_restart_duplicate_runs_success_action_once_without_mutation() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-restart-duplicate");
    let state_path = root.join("state.json");
    let watch_root = root.join("watch");
    let archive = root.join("archive");
    std::fs::create_dir_all(&watch_root).unwrap();
    let source = watch_root.join("duplicate.torrent");
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "restart-duplicate.bin",
        b"generated restart duplicate payload",
        8,
        None,
        false,
    );
    std::fs::write(&source, &bytes).unwrap();
    let mut config = watch_test_config(&watch_root, StartBehavior::Paused);
    config.watch[0].archive_dir = Some(archive.display().to_string());
    config.watch[0].label = Some("must-not-apply-to-duplicate".into());

    let original = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        disabled_health(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let hash = original
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    drop(original);

    let restart_broker = EventBroker::default();
    let restarted = DaemonRuntime::with_paths_broker_and_state(
        config,
        disabled_health(),
        None,
        None,
        Some(state_path),
        restart_broker.clone(),
    );
    restarted.restore_persisted_state().await.unwrap();
    let mut events = restart_broker.subscribe();
    let before =
        serde_json::to_value(restarted.registry.lock().await.get(&hash).cloned().unwrap()).unwrap();
    let before_order = restarted.queue.lock().await.order.clone();
    let before_bypass = restarted.queue.lock().await.bypass.clone();

    restarted.watch_scan().await.unwrap();
    assert!(source.exists());
    assert!(restarted.watch_history().await.is_empty());
    restarted.watch_scan().await.unwrap();
    assert!(!source.exists());
    assert!(archive.join("duplicate.torrent").exists());
    let history = restarted.watch_history().await;
    assert_eq!(history.len(), 1);
    assert!(history[0].success);
    assert!(history[0].duplicate);
    assert_eq!(
        history[0].outcome,
        crate::daemon::watch::ImportOutcome::Duplicate
    );
    assert_eq!(
        history[0].info_hash_hex.as_deref(),
        Some(hash.to_locator().as_str())
    );
    let event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, "watch_folder_imported");
    let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
    assert_eq!(payload["payload"]["outcome"], "duplicate");
    assert_eq!(payload["payload"]["duplicate"], true);
    assert_eq!(
        payload["payload"]["post_action_error"],
        serde_json::Value::Null
    );
    let after =
        serde_json::to_value(restarted.registry.lock().await.get(&hash).cloned().unwrap()).unwrap();
    assert_eq!(after, before);
    assert_eq!(restarted.queue.lock().await.order, before_order);
    assert_eq!(restarted.queue.lock().await.bypass, before_bypass);
    restarted.watch_scan().await.unwrap();
    assert_eq!(restarted.watch_history().await.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn recursive_watch_excludes_in_root_archive_after_success() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-recursive-archive-exclusion");
    let archive = root.join("archive");
    let source = root.join("archive-once.torrent");
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "recursive-archive-once.bin",
        b"generated recursive archive exclusion payload",
        8,
        None,
        false,
    );
    std::fs::write(&source, bytes).unwrap();
    let mut config = watch_test_config(&root, StartBehavior::Paused);
    config.watch[0].recursive = true;
    config.watch[0].archive_dir = Some(archive.display().to_string());
    let runtime = DaemonRuntime::new(config, disabled_health());

    for _ in 0..5 {
        runtime.watch_scan().await.unwrap();
    }

    assert!(!source.exists());
    assert!(archive.join("archive-once.torrent").exists());
    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].outcome,
        crate::daemon::watch::ImportOutcome::Imported
    );
    assert!(history[0].post_action_error.is_none());
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    assert_eq!(
        runtime.watch_status().await.folders[0].pending_torrent_files,
        0
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn shared_add_persistence_failure_restores_exact_state_and_has_no_side_effects() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-add-rollback");
    let source = root.join("rollback.torrent");
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "watch-rollback.bin",
        b"generated watch rollback payload",
        8,
        None,
        false,
    );
    let hash = TorrentKey::v1(meta::parse_torrent(&bytes).unwrap().info_hash);
    std::fs::write(&source, bytes).unwrap();
    let broker = EventBroker::default();
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        watch_test_config(&root, StartBehavior::Start),
        disabled_health(),
        None,
        None,
        None,
        broker.clone(),
    );
    let first = TorrentKey::v1(InfoHash::from_bytes([0x11; 20]));
    let last = TorrentKey::v1(InfoHash::from_bytes([0x22; 20]));
    {
        let mut queue = runtime.queue.lock().await;
        queue.add_many([first, hash, last]);
        queue.start_now(&hash);
    }
    let before_order = runtime.queue.lock().await.order.clone();
    let before_bypass = runtime.queue.lock().await.bypass.clone();
    runtime.watch_scan().await.unwrap();
    runtime.inject_add_mutation_persistence_failure();
    let mut events = broker.subscribe();
    runtime.watch_scan().await.unwrap();

    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert_eq!(runtime.queue.lock().await.order, before_order);
    assert_eq!(runtime.queue.lock().await.bypass, before_bypass);
    assert!(!runtime.queue_reconcile.lock().await.scheduled);
    assert!(runtime.torrent_limiters.read().await.get(&hash).is_none());
    assert!(runtime
        .torrent_peer_permit_pools
        .read()
        .await
        .get(&hash)
        .is_none());
    assert!(source.exists());
    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].outcome,
        crate::daemon::watch::ImportOutcome::TransientFailure
    );
    let event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, "watch_folder_failed");
    let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
    assert_eq!(payload["payload"]["outcome"], "transient_failure");
    assert!(
        tokio::time::timeout(Duration::from_millis(25), events.next())
            .await
            .is_err()
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn api_add_uses_shared_injected_rollback_without_event_or_schedule() {
    let runtime = DaemonRuntime::new(Config::default(), disabled_health());
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "api-shared-rollback.bin",
        b"generated api rollback payload",
        8,
        None,
        false,
    );
    let hash = TorrentKey::v1(meta::parse_torrent(&bytes).unwrap().info_hash);
    let before_order = runtime.queue.lock().await.order.clone();
    let mut events = runtime.event_broker.subscribe();
    runtime.inject_add_mutation_persistence_failure();

    let error = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, false))
        .await
        .unwrap_err();
    assert_eq!(error.code().as_str(), "storage_error");
    assert!(!runtime.registry.lock().await.contains(&hash));
    assert_eq!(runtime.queue.lock().await.order, before_order);
    assert!(!runtime.queue_reconcile.lock().await.scheduled);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), events.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn watch_permanent_failure_moves_while_transient_stays_and_retries() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-error-classification");
    let failure = root.join("failure");
    let bad = root.join("a-bad.torrent");
    let good = root.join("b-good.torrent");
    std::fs::write(&bad, b"not valid bencode").unwrap();
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "transient-retry.bin",
        b"generated transient retry payload",
        8,
        None,
        false,
    );
    std::fs::write(&good, bytes).unwrap();
    let mut config = watch_test_config(&root, StartBehavior::Paused);
    config.watch[0].failure_dir = Some(failure.display().to_string());
    let broker = EventBroker::default();
    let runtime =
        DaemonRuntime::with_paths_and_broker(config, disabled_health(), None, None, broker.clone());
    let mut events = broker.subscribe();
    runtime.watch_scan().await.unwrap();
    runtime.inject_add_mutation_persistence_failure();
    runtime.watch_scan().await.unwrap();

    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[0].outcome,
        crate::daemon::watch::ImportOutcome::PermanentFailure
    );
    assert_eq!(
        history[1].outcome,
        crate::daemon::watch::ImportOutcome::TransientFailure
    );
    assert!(!bad.exists());
    assert!(failure.join("a-bad.torrent").exists());
    assert!(good.exists());
    assert!(runtime.registry.lock().await.torrents.is_empty());
    let permanent_event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let transient_event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(permanent_event.kind, "watch_folder_failed");
    assert_eq!(transient_event.kind, "watch_folder_failed");
    let permanent_payload: serde_json::Value = serde_json::from_str(&permanent_event.json).unwrap();
    let transient_payload: serde_json::Value = serde_json::from_str(&transient_event.json).unwrap();
    assert_eq!(permanent_payload["payload"]["outcome"], "permanent_failure");
    assert_eq!(transient_payload["payload"]["outcome"], "transient_failure");

    runtime.watch_scan().await.unwrap();
    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 3);
    assert_eq!(
        history[2].outcome,
        crate::daemon::watch::ImportOutcome::Imported
    );
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn recursive_watch_excludes_in_root_failure_after_permanent_failure() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-recursive-failure-exclusion");
    let failure = root.join("failure");
    let source = root.join("fail-once.torrent");
    std::fs::write(&source, b"not valid bencode").unwrap();
    let mut config = watch_test_config(&root, StartBehavior::Paused);
    config.watch[0].recursive = true;
    config.watch[0].failure_dir = Some(failure.display().to_string());
    let runtime = DaemonRuntime::new(config, disabled_health());

    for _ in 0..5 {
        runtime.watch_scan().await.unwrap();
    }

    assert!(!source.exists());
    assert!(failure.join("fail-once.torrent").exists());
    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].outcome,
        crate::daemon::watch::ImportOutcome::PermanentFailure
    );
    assert!(history[0].post_action_error.is_none());
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert_eq!(
        runtime.watch_status().await.folders[0].pending_torrent_files,
        0
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn watch_error_classification_has_only_the_four_permanent_variants() {
    assert!(is_permanent_watch_error(&CoreError::Bencode("x".into())));
    assert!(is_permanent_watch_error(&CoreError::MalformedTorrent(
        "x".into()
    )));
    assert!(is_permanent_watch_error(&CoreError::InvalidInfoHash(
        "x".into()
    )));
    assert!(is_permanent_watch_error(&CoreError::Parse("x".into())));
    for transient in [
        CoreError::Storage("x".into()),
        CoreError::NetworkBlocked("x".into()),
        CoreError::Internal("x".into()),
        CoreError::InvalidConfig("x".into()),
    ] {
        assert!(!is_permanent_watch_error(&transient));
    }
}

#[tokio::test]
async fn watch_destination_collision_preserves_both_files_and_processes_once() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-action-collision");
    let archive = root.join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    let source = root.join("collision.torrent");
    let destination = archive.join("collision.torrent");
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "collision.bin",
        b"generated destination collision payload",
        8,
        None,
        false,
    );
    std::fs::write(&source, bytes).unwrap();
    std::fs::write(&destination, b"existing archive must survive").unwrap();
    let mut config = watch_test_config(&root, StartBehavior::Paused);
    config.watch[0].archive_dir = Some(archive.display().to_string());
    let broker = EventBroker::default();
    let runtime =
        DaemonRuntime::with_paths_and_broker(config, disabled_health(), None, None, broker.clone());
    let mut events = broker.subscribe();
    runtime.watch_scan().await.unwrap();
    runtime.watch_scan().await.unwrap();
    runtime.watch_scan().await.unwrap();

    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].outcome,
        crate::daemon::watch::ImportOutcome::Imported
    );
    assert!(history[0].post_action_error.is_some());
    assert!(source.exists());
    assert_eq!(
        std::fs::read(&destination).unwrap(),
        b"existing archive must survive"
    );
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    let mut imported_event = None;
    for _ in 0..3 {
        let event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if event.kind == "watch_folder_imported" {
            imported_event = Some(event);
            break;
        }
    }
    let payload: serde_json::Value =
        serde_json::from_str(&imported_event.expect("watch success event").json).unwrap();
    assert_eq!(payload["payload"]["outcome"], "imported");
    assert!(payload["payload"]["post_action_error"].is_string());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn watch_observations_prune_disappeared_files_and_removed_roots() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-observation-prune");
    let source = root.join("observed.torrent");
    std::fs::write(&source, b"first observation only").unwrap();
    let runtime = DaemonRuntime::new(
        watch_test_config(&root, StartBehavior::Paused),
        disabled_health(),
    );
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_observations.lock().await.len(), 1);
    std::fs::remove_file(&source).unwrap();
    runtime.watch_scan().await.unwrap();
    assert!(runtime.watch_observations.lock().await.is_empty());

    std::fs::write(&source, b"second observation only").unwrap();
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_observations.lock().await.len(), 1);
    runtime.config.write().await.watch.clear();
    runtime.watch_scan().await.unwrap();
    assert!(runtime.watch_observations.lock().await.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn overlapping_watch_roots_have_distinct_composite_observation_keys() {
    use swarmotter_core::config::{StartBehavior, WatchFolderConfig};

    let root = unique_dir("watch-overlap-keys");
    let nested = root.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("shared.torrent"), b"observation only").unwrap();
    let mut config = watch_test_config(&root, StartBehavior::Paused);
    config.watch[0].recursive = true;
    config.watch.push(WatchFolderConfig {
        path: nested.display().to_string(),
        recursive: false,
        download_dir: None,
        label: None,
        profile: None,
        start_behavior: StartBehavior::Paused,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: false,
    });
    let runtime = DaemonRuntime::new(config, disabled_health());
    runtime.watch_scan().await.unwrap();
    let observations = runtime.watch_observations.lock().await;
    assert_eq!(observations.len(), 2);
    assert_eq!(
        observations
            .keys()
            .map(|key| key.root.clone())
            .collect::<HashSet<_>>()
            .len(),
        2
    );
    drop(observations);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn watch_action_exclusion_does_not_hide_separately_configured_overlapping_root() {
    use swarmotter_core::config::{StartBehavior, WatchFolderConfig};

    let root = unique_dir("watch-overlap-action-exclusion");
    let archive = root.join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    std::fs::write(archive.join("shared.torrent"), b"observation only").unwrap();
    let mut config = watch_test_config(&root, StartBehavior::Paused);
    config.watch[0].recursive = true;
    config.watch[0].archive_dir = Some(archive.display().to_string());
    config.watch.push(WatchFolderConfig {
        path: archive.display().to_string(),
        recursive: false,
        download_dir: None,
        label: None,
        profile: None,
        start_behavior: StartBehavior::Paused,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: false,
    });
    let runtime = DaemonRuntime::new(config, disabled_health());

    runtime.watch_scan().await.unwrap();

    let observations = runtime.watch_observations.lock().await;
    assert_eq!(observations.len(), 1);
    let key = observations.keys().next().unwrap();
    assert_eq!(
        key.root,
        crate::daemon::watch::lexical_absolute(&archive).unwrap()
    );
    assert_eq!(key.relative_path, PathBuf::from("shared.torrent"));
    drop(observations);
    let status = runtime.watch_status().await;
    assert_eq!(status.folders[0].pending_torrent_files, 0);
    assert_eq!(status.folders[1].pending_torrent_files, 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn concurrent_manual_watch_scans_produce_one_terminal_result() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-concurrent-scan");
    let source = root.join("single.torrent");
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "concurrent-watch.bin",
        b"generated concurrent watch payload",
        8,
        None,
        false,
    );
    std::fs::write(&source, bytes).unwrap();
    let runtime = Arc::new(DaemonRuntime::new(
        watch_test_config(&root, StartBehavior::Paused),
        disabled_health(),
    ));
    runtime.watch_scan().await.unwrap();
    let (read_reached, continue_read) = runtime.pause_watch_after_bounded_read().await;
    let first = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.watch_scan().await })
    };
    read_reached.await.unwrap();
    let second = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.watch_scan().await })
    };
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !second.is_finished(),
        "scan B must wait while scan A owns the whole-scan lock"
    );
    continue_read.send(()).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(runtime.watch_history().await.len(), 1);
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    assert!(source.exists());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn incomplete_watch_root_scan_retains_prior_observations() {
    use swarmotter_core::config::StartBehavior;

    let root = unique_dir("watch-incomplete-root");
    let moved = root.with_extension("temporarily-moved");
    let source = root.join("retained.torrent");
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "retained-observation.bin",
        b"generated retained observation payload",
        8,
        None,
        false,
    );
    std::fs::write(&source, bytes).unwrap();
    let runtime = DaemonRuntime::new(
        watch_test_config(&root, StartBehavior::Paused),
        disabled_health(),
    );
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_observations.lock().await.len(), 1);

    std::fs::rename(&root, &moved).unwrap();
    assert!(runtime.watch_scan().await.is_err());
    assert_eq!(runtime.watch_observations.lock().await.len(), 1);
    std::fs::rename(&moved, &root).unwrap();
    runtime.watch_scan().await.unwrap();
    assert_eq!(runtime.watch_history().await.len(), 1);
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn watch_history_evicts_oldest_entry_at_ten_thousand_and_one() {
    let runtime = DaemonRuntime::new(Config::default(), disabled_health());
    for index in 0..=crate::daemon::watch::MAX_IMPORT_HISTORY {
        runtime
            .record_watch_import(crate::daemon::watch::ImportResult {
                path: format!("/watch/{index}.torrent"),
                success: false,
                info_hash_hex: None,
                error: Some("generated history entry".into()),
                duplicate: false,
                post_action_error: None,
                outcome: crate::daemon::watch::ImportOutcome::TransientFailure,
            })
            .await;
    }
    let history = runtime.watch_history().await;
    assert_eq!(history.len(), crate::daemon::watch::MAX_IMPORT_HISTORY);
    assert_eq!(history.first().unwrap().path, "/watch/1.torrent");
    assert_eq!(
        history.last().unwrap().path,
        format!(
            "/watch/{}.torrent",
            crate::daemon::watch::MAX_IMPORT_HISTORY
        )
    );
}
