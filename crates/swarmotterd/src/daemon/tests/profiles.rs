// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn profile_assignment_preserves_existing_storage_and_rolls_back_on_persistence_failure() {
    use swarmotter_core::policy::{PolicyProfile, PolicyStorage};

    let root = unique_dir("policy-profile-assignment");
    let state_path = root.join("state.json");
    let complete = root.join("complete");
    let incomplete = root.join("incomplete");
    let profile_complete = root.join("profile-complete");
    let profile_incomplete = root.join("profile-incomplete");
    let other_complete = root.join("other-complete");
    let other_incomplete = root.join("other-incomplete");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(complete.display().to_string());
    cfg.storage.incomplete_dir = Some(incomplete.display().to_string());
    cfg.profiles.profiles.insert(
        "archive".into(),
        PolicyProfile {
            storage: PolicyStorage {
                download_dir: Some(profile_complete.display().to_string()),
                incomplete_dir: Some(profile_incomplete.display().to_string()),
            },
            ..Default::default()
        },
    );
    cfg.profiles.profiles.insert(
        "other".into(),
        PolicyProfile {
            storage: PolicyStorage {
                download_dir: Some(other_complete.display().to_string()),
                incomplete_dir: Some(other_incomplete.display().to_string()),
            },
            ..Default::default()
        },
    );
    cfg.profiles.labels.insert("linux".into(), "archive".into());
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
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "existing-storage.bin",
            b"existing storage payload",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::Paused;
    // This models a legacy explicit completed-data path with an inherited
    // incomplete path. Profile reassignment must retain both locations.
    torrent.download_dir = Some(complete.display().to_string());
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);

    // A label can select a profile after registration, but must not redirect
    // existing data into the profile's storage root.
    runtime
        .set_labels(&hash, vec!["linux".into()])
        .await
        .unwrap();
    let labelled = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    let label_policy = runtime.effective_policy(&labelled).await;
    assert!(matches!(
        label_policy.profile.unwrap().source,
        swarmotter_core::policy::PolicyValueSource::Label { .. }
    ));
    assert_eq!(
        runtime.policy_storage_paths(&labelled).await,
        (
            complete.display().to_string(),
            incomplete.display().to_string(),
        )
    );

    runtime
        .assign_torrent_profile(&hash, Some("archive".into()))
        .await
        .unwrap();
    let assigned = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    let snapshot = assigned.policy.storage_snapshot.as_ref().unwrap();
    assert!(snapshot.preserve_existing_storage);
    assert_eq!(snapshot.download_dir, None);
    assert_eq!(
        snapshot.incomplete_dir.as_deref(),
        Some(incomplete.to_string_lossy().as_ref())
    );
    assert_eq!(
        runtime.policy_storage_paths(&assigned).await,
        (
            complete.display().to_string(),
            incomplete.display().to_string(),
        )
    );
    let persisted = crate::state_store::load(&state_path).unwrap().unwrap();
    assert_eq!(
        persisted.torrents[0].policy.profile.as_deref(),
        Some("archive")
    );

    // A restart reads the durable snapshot rather than re-resolving the new
    // assignment's profile storage paths.
    let restored = DaemonRuntime::with_paths_broker_and_state(
        cfg,
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 1);
    let restored_torrent = restored.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(
        restored.policy_storage_paths(&restored_torrent).await,
        (
            complete.display().to_string(),
            incomplete.display().to_string(),
        )
    );
    drop(restored);

    // Turn the durable-state target into a directory so the next transactional
    // write fails. The prior profile and storage snapshot must remain intact.
    std::fs::remove_file(&state_path).unwrap();
    std::fs::create_dir_all(&state_path).unwrap();
    assert!(runtime
        .assign_torrent_profile(&hash, Some("other".into()))
        .await
        .is_err());
    let rolled_back = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(rolled_back.policy.profile.as_deref(), Some("archive"));
    assert_eq!(
        runtime.policy_storage_paths(&rolled_back).await,
        (
            complete.display().to_string(),
            incomplete.display().to_string(),
        )
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn profile_encryption_mode_changes_only_select_affected_effective_torrents() {
    use swarmotter_core::config::PeerEncryptionMode;
    use swarmotter_core::policy::{PolicyBandwidth, PolicyProfile};

    let mut previous = Config::default();
    previous.torrent.encryption_mode = PeerEncryptionMode::Preferred;
    previous.profiles.profiles.insert(
        "encrypted".into(),
        PolicyProfile {
            encryption_mode: Some(PeerEncryptionMode::Required),
            ..Default::default()
        },
    );
    previous
        .profiles
        .labels
        .insert("encrypted".into(), "encrypted".into());

    let make_torrent = |name: &str| {
        let bytes = swarmotter_core::meta::build_single_file_torrent(
            name,
            b"generated encryption policy fixture",
            8,
            None,
            false,
        );
        Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), now())
    };
    let mut inherited = make_torrent("inherited-encryption.bin");
    inherited.labels = vec!["encrypted".into()];
    let unchanged = make_torrent("global-encryption.bin");
    let mut explicit = make_torrent("explicit-encryption.bin");
    explicit.labels = vec!["encrypted".into()];
    explicit.policy.overrides.encryption_mode = Some(PeerEncryptionMode::Disabled);
    let torrents = vec![inherited.clone(), unchanged, explicit];

    let mut bandwidth_only = previous.clone();
    bandwidth_only
        .profiles
        .profiles
        .get_mut("encrypted")
        .unwrap()
        .bandwidth = PolicyBandwidth {
        download_limit: Some(123),
        upload_limit: None,
    };
    assert!(DaemonRuntime::effective_encryption_mode_changes(
        &previous,
        &bandwidth_only,
        &torrents,
    )
    .is_empty());

    let mut next = previous.clone();
    next.profiles
        .profiles
        .get_mut("encrypted")
        .unwrap()
        .encryption_mode = Some(PeerEncryptionMode::Preferred);
    assert_eq!(
        DaemonRuntime::effective_encryption_mode_changes(&previous, &next, &torrents),
        vec![inherited.key()],
    );
}

#[tokio::test]
async fn torrent_encryption_override_is_durable_and_rolls_back_with_state_write_failure() {
    use swarmotter_core::config::PeerEncryptionMode;
    use swarmotter_core::policy::PolicyProfile;

    let root = unique_dir("torrent-encryption-override");
    let state_path = root.join("state.json");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.profiles.profiles.insert(
        "encrypted".into(),
        PolicyProfile {
            encryption_mode: Some(PeerEncryptionMode::Required),
            ..Default::default()
        },
    );
    config
        .profiles
        .labels
        .insert("encrypted".into(), "encrypted".into());
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        disabled_health(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "durable-encryption-override.bin",
            b"generated durable encryption override fixture",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::Paused;
    torrent.labels = vec!["encrypted".into()];
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);

    runtime
        .assign_torrent_encryption_mode(&hash, Some(PeerEncryptionMode::Disabled))
        .await
        .unwrap();
    let persisted = crate::state_store::load(&state_path).unwrap().unwrap();
    assert_eq!(
        persisted.torrents[0].policy.overrides.encryption_mode,
        Some(PeerEncryptionMode::Disabled)
    );

    drop(runtime);
    let restored = DaemonRuntime::with_paths_broker_and_state(
        config,
        disabled_health(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 1);
    let restored_torrent = restored.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(
        restored_torrent.policy.overrides.encryption_mode,
        Some(PeerEncryptionMode::Disabled)
    );

    // A failed durable write must restore the old override before any
    // effective policy or live session is changed.
    std::fs::remove_file(&state_path).unwrap();
    std::fs::create_dir_all(&state_path).unwrap();
    assert!(restored
        .assign_torrent_encryption_mode(&hash, Some(PeerEncryptionMode::Preferred))
        .await
        .is_err());
    assert_eq!(
        restored
            .registry
            .lock()
            .await
            .get(&hash)
            .unwrap()
            .policy
            .overrides
            .encryption_mode,
        Some(PeerEncryptionMode::Disabled)
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn profile_replacement_migrates_legacy_label_storage_and_initial_admission() {
    use swarmotter_core::config::StartBehavior;
    use swarmotter_core::policy::{PolicyProfile, PolicyQueue, PolicyStorage, PolicyValueSource};

    let root = unique_dir("legacy-profile-config-migration");
    let state_path = root.join("state.json");
    let global_complete = root.join("global-complete");
    let global_incomplete = root.join("global-incomplete");
    let profile_complete = root.join("profile-complete");
    let profile_incomplete = root.join("profile-incomplete");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.storage.download_dir = Some(global_complete.display().to_string());
    config.storage.incomplete_dir = Some(global_incomplete.display().to_string());
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        disabled_health(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "legacy-profile-migration.bin",
            b"generated lawful legacy profile migration payload",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut legacy = Torrent::new(meta, now());
    legacy.state = TorrentState::Queued;
    legacy.labels = vec!["linux".into()];
    runtime.registry.lock().await.add(legacy).unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.persist_state().await.unwrap();

    let mut replacement = config.clone();
    replacement.profiles.profiles.insert(
        "archive".into(),
        PolicyProfile {
            storage: PolicyStorage {
                download_dir: Some(profile_complete.display().to_string()),
                incomplete_dir: Some(profile_incomplete.display().to_string()),
            },
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Start),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    replacement
        .profiles
        .labels
        .insert("linux".into(), "archive".into());
    runtime.replace_config(replacement.clone()).await.unwrap();

    let migrated = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert!(migrated
        .policy
        .storage_snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.preserve_existing_storage));
    assert_eq!(
        migrated.policy.initial_start_behavior,
        Some(StartBehavior::Paused),
        "the legacy record keeps the admission decision from before the profile PUT"
    );
    let effective = runtime.effective_policy(&migrated).await;
    assert!(matches!(
        effective.profile.unwrap().source,
        PolicyValueSource::Label { .. }
    ));
    assert!(matches!(
        effective.download_dir.source,
        PolicyValueSource::ExistingStorageSnapshot
    ));
    assert!(matches!(
        effective.start_behavior.source,
        PolicyValueSource::InitialAdmissionSnapshot
    ));
    assert_eq!(
        runtime.policy_storage_paths(&migrated).await,
        (
            global_complete.display().to_string(),
            global_incomplete.display().to_string(),
        )
    );

    let persisted = crate::state_store::load(&state_path).unwrap().unwrap();
    let persisted = persisted
        .torrents
        .into_iter()
        .find(|torrent| torrent.key() == hash)
        .unwrap();
    assert!(persisted.policy.storage_snapshot.is_some());
    assert_eq!(
        persisted.policy.initial_start_behavior,
        Some(StartBehavior::Paused)
    );

    let restarted = DaemonRuntime::with_paths_broker_and_state(
        replacement,
        disabled_health(),
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restarted.restore_persisted_state().await.unwrap(), 1);
    let restored = restarted.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(
        restarted.policy_storage_paths(&restored).await,
        (
            global_complete.display().to_string(),
            global_incomplete.display().to_string(),
        )
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn failed_profile_replacement_restores_legacy_policy_state_and_config() {
    use swarmotter_core::config::StartBehavior;
    use swarmotter_core::policy::{PolicyProfile, PolicyQueue};

    let root = unique_dir("legacy-profile-config-rollback");
    let config_path = root.join("swarmotter.toml");
    let state_path = root.join("state.json");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.join("global-complete").display().to_string());
    config.storage.incomplete_dir = Some(root.join("global-incomplete").display().to_string());
    write_config_atomically(&config_path, &config).unwrap();
    let previous_config_file = std::fs::read(&config_path).unwrap();
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        disabled_health(),
        Some(config_path.clone()),
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let meta =
        swarmotter_core::meta::parse_torrent(&swarmotter_core::meta::build_single_file_torrent(
            "legacy-profile-config-rollback.bin",
            b"generated lawful legacy profile rollback payload",
            8,
            None,
            false,
        ))
        .unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut legacy = Torrent::new(meta, now());
    legacy.state = TorrentState::Paused;
    legacy.labels = vec!["linux".into()];
    runtime.registry.lock().await.add(legacy).unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.persist_state().await.unwrap();

    let mut replacement = config.clone();
    replacement.profiles.profiles.insert(
        "archive".into(),
        PolicyProfile {
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Start),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    replacement
        .profiles
        .labels
        .insert("linux".into(), "archive".into());
    runtime.inject_generic_config_persistence_failure_after_rename();
    assert!(runtime.replace_config(replacement).await.is_err());

    let restored_live = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert!(restored_live.policy.storage_snapshot.is_none());
    assert!(restored_live.policy.initial_start_behavior.is_none());
    let restored_disk = crate::state_store::load(&state_path).unwrap().unwrap();
    let restored_disk = restored_disk
        .torrents
        .iter()
        .find(|torrent| torrent.key() == hash)
        .unwrap();
    assert!(restored_disk.policy.storage_snapshot.is_none());
    assert!(restored_disk.policy.initial_start_behavior.is_none());
    assert_eq!(std::fs::read(&config_path).unwrap(), previous_config_file);
    assert!(runtime.config.read().await.profiles.profiles.is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn profile_start_behavior_is_fixed_for_queued_torrents_after_edit_and_assignment() {
    use swarmotter_core::config::StartBehavior;
    use swarmotter_core::policy::{PolicyProfile, PolicyQueue, PolicyValueSource};

    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.profiles.profiles.insert(
        "launch".into(),
        PolicyProfile {
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Start),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    config.profiles.profiles.insert(
        "hold".into(),
        PolicyProfile {
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Paused),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    config
        .profiles
        .labels
        .insert("launch".into(), "launch".into());
    let runtime = DaemonRuntime::new(config.clone(), disabled_health());
    let hash = runtime
        .add_torrent_file_with_options(
            swarmotter_core::meta::build_single_file_torrent(
                "initial-admission-snapshot.bin",
                b"generated lawful initial admission snapshot payload",
                8,
                None,
                false,
            ),
            AddTorrentOptions::request(None, false, false, None, vec!["launch".into()]),
        )
        .await
        .unwrap();
    assert_eq!(runtime.desired_download_hashes().await, vec![hash]);

    let mut replacement = config.clone();
    replacement.profiles.profiles.insert(
        "launch".into(),
        PolicyProfile {
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Paused),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    runtime.replace_config(replacement).await.unwrap();
    assert_eq!(
        runtime.desired_download_hashes().await,
        vec![hash],
        "editing a profile cannot revoke an existing queued torrent's initial admission"
    );

    runtime
        .assign_torrent_profile(&hash, Some("hold".into()))
        .await
        .unwrap();
    assert_eq!(
        runtime.desired_download_hashes().await,
        vec![hash],
        "reassignment cannot retroactively pause a queued torrent admitted at creation"
    );
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    let policy = runtime.effective_policy(&torrent).await;
    assert!(matches!(
        policy.start_behavior.source,
        PolicyValueSource::InitialAdmissionSnapshot
    ));
    assert_eq!(policy.start_behavior.value, StartBehavior::Start);
}
