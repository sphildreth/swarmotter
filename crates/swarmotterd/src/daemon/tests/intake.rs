// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn profile_intake_preview_snapshots_selection_and_organization_before_payload() {
    use swarmotter_core::policy::{PolicyIntake, PolicyProfile};

    let root = unique_dir("profile-intake-preview");
    let complete = root.join("complete");
    let incomplete = root.join("incomplete");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(complete.display().to_string());
    cfg.storage.incomplete_dir = Some(incomplete.display().to_string());
    cfg.profiles.profiles.insert(
        "review".into(),
        PolicyProfile {
            intake: PolicyIntake {
                excluded_file_patterns: vec!["samples/*".into(), "*.nfo".into()],
                excluded_file_rules: Vec::new(),
                organization_subdirectory: Some("lawful/releases".into()),
                incomplete_subdirectory: Some("staging/review".into()),
                force_top_level_folder: false,
                partial_file_suffix: Some(".part".into()),
            },
            ..Default::default()
        },
    );
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let sample = b"sample".as_slice();
    let release = b"release".as_slice();
    let notes = b"notes".as_slice();
    let bytes = swarmotter_core::meta::build_multi_file_torrent(
        "lawful-release",
        &[
            (
                vec!["samples".into(), "clip.bin".into()],
                sample.len() as u64,
            ),
            (vec!["release.bin".into()], release.len() as u64),
            (vec!["readme.nfo".into()], notes.len() as u64),
        ],
        &[sample, release, notes],
        8,
        None,
    );
    let mut options =
        AddTorrentOptions::request(None, false, false, Some("review".into()), Vec::new());
    options.preview = true;
    options.unwanted_file_indices = vec![1];
    let hash = runtime
        .add_torrent_file_with_options(bytes, options)
        .await
        .unwrap();

    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, TorrentState::Paused);
    assert!(torrent.policy.preview_until_started);
    let intake = torrent.policy.intake_snapshot.as_ref().unwrap();
    assert_eq!(intake.profile, "review");
    assert_eq!(intake.unwanted_file_indices, vec![1]);
    assert_eq!(
        intake.organization_subdirectory.as_deref(),
        Some("lawful/releases")
    );
    assert_eq!(
        torrent.priorities,
        vec![
            FilePriority::Unwanted,
            FilePriority::Unwanted,
            FilePriority::Unwanted,
        ]
    );
    assert_eq!(torrent.wanted, vec![false, false, false]);
    assert_eq!(
        runtime.policy_storage_paths(&torrent).await,
        (
            complete.join("lawful/releases").display().to_string(),
            incomplete.join("staging/review").display().to_string(),
        )
    );
    assert!(
        runtime.desired_download_hashes().await.is_empty(),
        "a known .torrent preview must not admit payload work before Start"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn request_structured_intake_rules_apply_before_known_payload_admission() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let runtime = DaemonRuntime::new(
        cfg,
        NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        ),
    );
    let release = b"lawful release".as_slice();
    let proof = b"checksum proof".as_slice();
    let bytes = swarmotter_core::meta::build_multi_file_torrent(
        "structured-intake-release",
        &[
            (vec!["release.bin".into()], release.len() as u64),
            (
                vec!["proof".into(), "checksum.txt".into()],
                proof.len() as u64,
            ),
        ],
        &[release, proof],
        8,
        None,
    );
    let mut options = AddTorrentOptions::request(None, false, false, None, Vec::new());
    options.preview = true;
    options.file_exclusion_rules = vec![swarmotter_core::policy::PolicyFileExclusionRule {
        path_segment: Some("proof".into()),
        ..Default::default()
    }];
    let hash = runtime
        .add_torrent_file_with_options(bytes, options)
        .await
        .unwrap();

    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.state, TorrentState::Paused);
    assert_eq!(
        torrent.priorities,
        vec![FilePriority::Normal, FilePriority::Unwanted]
    );
    assert_eq!(torrent.wanted, vec![true, false]);
    assert_eq!(
        torrent
            .policy
            .intake_snapshot
            .as_ref()
            .map(|snapshot| snapshot.excluded_file_rules.len()),
        Some(1)
    );
    assert!(runtime.desired_download_hashes().await.is_empty());
}

#[tokio::test]
async fn metadata_preview_resolution_commits_selection_before_public_state() {
    use swarmotter_core::policy::IntakePolicySnapshot;

    let root = unique_dir("metadata-preview-commit");
    let state_path = root.join("state.sqlite");
    let complete = root.join("complete");
    let incomplete = root.join("incomplete");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(complete.display().to_string());
    cfg.storage.incomplete_dir = Some(incomplete.display().to_string());
    cfg.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: complete.display().to_string(),
        max_active_downloads: 1,
        max_active_bytes: 1,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 0,
    }];
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
    let content = b"generated lawful metadata preview content".as_slice();
    let resolved =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_multi_file_torrent(
            "preview-release",
            &[
                (vec!["release.bin".into()], content.len() as u64),
                (vec!["samples".into(), "clip.bin".into()], 1),
            ],
            &[content, b"x"],
            8,
            None,
        ))
        .unwrap();
    let hash = TorrentKey::v1(resolved.info_hash);
    let placeholder =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "metadata-placeholder",
            b"placeholder",
            8,
            None,
            false,
        ))
        .unwrap();
    let mut torrent = Torrent::new(placeholder, now());
    torrent.state = TorrentState::DownloadingMetadata;
    torrent.needs_metadata = true;
    set_test_v1_magnet_identity(&mut torrent, hash);
    // `so=0` requests the first file, while the local add-time exclusion for
    // that same index remains authoritative. The second file is removed by
    // the magnet allow-list, proving the precedence is deterministic.
    torrent.magnet_select_only_file_indices = vec![0];
    torrent.magnet_direct_peers = vec![swarmotter_core::magnet::MagnetDirectPeer {
        ip: "192.0.2.25".parse().unwrap(),
        port: 51413,
    }];
    torrent.policy.preview_until_started = true;
    torrent.policy.intake_snapshot = Some(IntakePolicySnapshot {
        profile: "review".into(),
        excluded_file_patterns: Vec::new(),
        excluded_file_rules: Vec::new(),
        organization_subdirectory: Some("lawful/releases".into()),
        incomplete_subdirectory: Some("staging/review".into()),
        force_top_level_folder: false,
        partial_file_suffix: Some(".part".into()),
        unwanted_file_indices: vec![0],
    });
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);

    assert_eq!(
        runtime.desired_download_hashes().await,
        vec![hash],
        "metadata-only preview discovery must not consume payload-root admission"
    );

    runtime
        .commit_metadata_preview_resolution(hash, Arc::new(resolved.clone()))
        .await
        .unwrap();
    let committed = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(committed.state, TorrentState::Paused);
    assert!(!committed.needs_metadata);
    assert!(committed.magnet_select_only_file_indices.is_empty());
    assert_eq!(committed.magnet_direct_peers.len(), 1);
    assert!(committed.policy.preview_until_started);
    assert_eq!(committed.files.len(), 2);
    assert_eq!(
        committed.priorities,
        vec![FilePriority::Unwanted, FilePriority::Unwanted]
    );
    assert_eq!(committed.wanted, vec![false, false]);
    assert!(
        runtime.storage_admissions.records().await.is_empty(),
        "metadata-only preview resolution must not reserve payload storage"
    );
    let persisted = crate::state_store::load(&state_path)
        .unwrap()
        .unwrap()
        .torrents
        .into_iter()
        .find(|torrent| torrent.key() == hash)
        .unwrap();
    assert_eq!(persisted.state, TorrentState::Paused);
    assert!(!persisted.needs_metadata);
    assert!(persisted.magnet_select_only_file_indices.is_empty());
    assert_eq!(persisted.magnet_direct_peers, committed.magnet_direct_peers);
    assert_eq!(persisted.priorities, committed.priorities);
    assert_eq!(
        runtime.policy_storage_paths(&committed).await,
        (
            complete.join("lawful/releases").display().to_string(),
            incomplete.join("staging/review").display().to_string(),
        )
    );
    assert_eq!(
        DaemonRuntime::partial_file_suffix_for_active_storage(&committed),
        Some(".part".into())
    );

    // Metadata resolution freezes the intake decision before it is exposed
    // publicly. A restart must retain both the organized complete root and
    // the distinct incomplete `.part` path rather than recomputing either.
    drop(runtime);
    let restarted = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    assert_eq!(restarted.restore_persisted_state().await.unwrap(), 1);
    let restored = restarted.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(
        restarted.policy_storage_paths(&restored).await,
        (
            complete.join("lawful/releases").display().to_string(),
            incomplete.join("staging/review").display().to_string(),
        )
    );
    assert_eq!(
        DaemonRuntime::partial_file_suffix_for_active_storage(&restored),
        Some(".part".into())
    );
    let active_storage = storage_io_with_config(
        restored.meta.clone(),
        incomplete.join("staging/review"),
        &cfg,
    )
    .with_partial_file_suffix(DaemonRuntime::partial_file_suffix_for_active_storage(
        &restored,
    ));
    assert_eq!(
        active_storage.file_path(0).unwrap(),
        incomplete.join("staging/review/preview-release/release.bin.part")
    );
    drop(restarted);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn metadata_preview_rejects_out_of_range_deferred_selection_durably() {
    use swarmotter_core::policy::IntakePolicySnapshot;

    let root = unique_dir("metadata-preview-invalid-selection");
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
    let resolved =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "one-file-release.bin",
            b"generated lawful metadata fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(resolved.info_hash);
    let placeholder =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "metadata-placeholder",
            b"placeholder",
            8,
            None,
            false,
        ))
        .unwrap();
    let mut torrent = Torrent::new(placeholder, now());
    torrent.state = TorrentState::DownloadingMetadata;
    torrent.needs_metadata = true;
    set_test_v1_magnet_identity(&mut torrent, hash);
    torrent.policy.preview_until_started = true;
    torrent.policy.intake_snapshot = Some(IntakePolicySnapshot {
        unwanted_file_indices: vec![4],
        ..Default::default()
    });
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);

    let error = runtime
        .commit_metadata_preview_resolution(hash, std::sync::Arc::new(resolved))
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::InvalidArgument(_)));
    let rejected = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(rejected.state, TorrentState::Error);
    assert!(rejected.needs_metadata);
    assert!(rejected.policy.preview_until_started);
    assert!(rejected
        .error
        .as_deref()
        .is_some_and(|message| message.contains("index 4")));
    let persisted = crate::state_store::load(&state_path)
        .unwrap()
        .unwrap()
        .torrents
        .into_iter()
        .find(|torrent| torrent.key() == hash)
        .unwrap();
    assert_eq!(persisted.state, TorrentState::Error);
    assert!(persisted.needs_metadata);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn preview_start_rolls_back_gate_when_state_persistence_fails() {
    let root = unique_dir("preview-start-state-rollback");
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
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "preview-start.bin",
            b"generated lawful preview-start fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::Paused;
    torrent.policy.preview_until_started = true;
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);

    assert!(runtime.resume(&hash).await.is_err());
    assert!(runtime.start_now(&hash).await.is_err());
    let restored = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert!(restored.policy.preview_until_started);
    assert_eq!(restored.state, TorrentState::Paused);
    assert!(runtime.engine_handles_empty().await);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn retryable_magnet_metadata_no_peers_stays_queued_after_progress_reconcile() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "magnet placeholder",
        b"placeholder",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let magnet_info_hash =
        swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
            .unwrap();
    let hash = TorrentKey::v1(magnet_info_hash);
    let piece_count = placeholder_meta.piece_count();
    let total_length = placeholder_meta.total_length;
    let mut torrent = Torrent::new(placeholder_meta, 1);
    torrent.state = TorrentState::DownloadingMetadata;
    torrent.needs_metadata = true;
    set_test_v1_magnet_identity(&mut torrent, hash);
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.engine_states.write().await.insert(
        hash,
        Arc::new(Mutex::new(EngineState {
            piece_count,
            total_length,
            ..Default::default()
        })),
    );

    let retry = runtime
            .handle_engine_task_error(
                hash,
                true,
                CoreError::Internal(
                    "magnet metadata fetch failed after discovery retries: internal error: magnet metadata fetch: no peers discovered"
                        .into(),
                ),
            )
            .await;

    assert!(retry);
    {
        let reg = runtime.registry.lock().await;
        let torrent = reg.get(&hash).unwrap();
        assert_eq!(torrent.state, TorrentState::Queued);
        assert_eq!(
            torrent.error.as_deref(),
            Some(MAGNET_METADATA_NO_PEERS_RETRY_MESSAGE)
        );
    }
    assert!(runtime
        .engine_retry_after
        .read()
        .await
        .get(&hash)
        .is_some_and(|retry_at| *retry_at > Instant::now()));
    assert!(
        runtime.desired_download_hashes().await.is_empty(),
        "retry backoff should keep no-peer magnets out of active queue slots"
    );

    runtime.reconcile_engine_progress().await;

    let reg = runtime.registry.lock().await;
    let torrent = reg.get(&hash).unwrap();
    assert_eq!(
        torrent.state,
        TorrentState::Queued,
        "stale engine diagnostics must not reactivate a magnet queued for metadata retry"
    );
}

#[tokio::test]
async fn storage_root_declared_byte_control_defers_only_the_over_budget_queue_entry() {
    let root = unique_dir("storage-root-admission");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.queue.max_active_downloads = 0;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 0,
        max_active_bytes: 10,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 0,
    }];
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg.clone(), health);
    let first =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "root-active.bin",
            b"12345678",
            8,
            None,
            false,
        ))
        .unwrap();
    let blocked =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "root-blocked.bin",
            b"123456",
            8,
            None,
            false,
        ))
        .unwrap();
    let fitting =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "root-fitting.bin",
            b"12",
            8,
            None,
            false,
        ))
        .unwrap();
    let first_hash = TorrentKey::v1(first.info_hash);
    let blocked_hash = TorrentKey::v1(blocked.info_hash);
    let fitting_hash = TorrentKey::v1(fitting.info_hash);
    let mut first_torrent = Torrent::new(first.clone(), 1);
    first_torrent.state = TorrentState::Downloading;
    {
        let mut registry = runtime.registry.lock().await;
        registry.add(first_torrent).unwrap();
        registry.add(Torrent::new(blocked, 2)).unwrap();
        registry.add(Torrent::new(fitting, 3)).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(blocked_hash);
        queue.add(fitting_hash);
    }
    let admission = storage_root_admission_for_download(&cfg, None).unwrap();
    runtime
        .storage_admissions
        .reserve(first_hash, &admission, first.total_length)
        .await
        .unwrap();

    assert_eq!(
        runtime.desired_download_hashes().await,
        vec![first_hash, fitting_hash]
    );
    runtime.storage_admissions.release(&first_hash).await;
    let _ = std::fs::remove_dir_all(root);
}
