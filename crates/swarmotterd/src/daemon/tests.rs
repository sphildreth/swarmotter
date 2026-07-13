// SPDX-License-Identifier: Apache-2.0

use super::lifecycle::ExplicitRecheckRestoreState;
use super::*;
use futures_util::StreamExt as _;
use swarmotter_api::state::DaemonOps;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn unique_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-daemon-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn add_complete_seed_fixture(
    runtime: &DaemonRuntime,
    name: &str,
    content: &[u8],
) -> (InfoHash, Arc<swarmotter_core::bandwidth::RateLimiter>) {
    let bytes = swarmotter_core::meta::build_single_file_torrent(name, content, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = meta.info_hash;
    let root = runtime
        .config
        .read()
        .await
        .storage
        .download_dir
        .clone()
        .unwrap();
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), PathBuf::from(root));
    for piece in 0..meta.piece_count() {
        let start = piece * meta.piece_length as usize;
        let end = (start + meta.piece_length as usize).min(content.len());
        storage
            .write_piece(piece, &content[start..end])
            .await
            .unwrap();
    }
    let mut torrent = Torrent::new(meta.clone(), now());
    torrent.state = TorrentState::Completed;
    torrent.downloaded = meta.total_length;
    torrent.date_completed = Some(now());
    torrent.seeding.seed_forever = true;
    for piece in 0..meta.piece_count() {
        torrent.progress.have_piece(piece);
    }
    torrent.recompute_file_bytes_completed();
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    let limiter = runtime.ensure_torrent_limiter(hash, 0, 0).await;
    (hash, limiter)
}

async fn assert_seeder_state_registry_invariant(runtime: &DaemonRuntime) {
    let _lifecycle = runtime.seeder_lifecycle_lock.lock().await;
    let live = runtime.seeder_registry.info_hashes().await;
    let registry = runtime.registry.lock().await;
    for hash in &live {
        let torrent = registry.get(hash).expect("live seeder has a torrent");
        assert_eq!(torrent.state, TorrentState::Seeding);
        assert_eq!(torrent.seeding_status, SeedingStatus::Active);
    }
    for (hash, torrent) in &registry.torrents {
        if torrent.state != TorrentState::NetworkBlocked
            && (torrent.state == TorrentState::Seeding
                || torrent.seeding_status == SeedingStatus::Active)
        {
            assert!(live.contains(hash), "modeled active seeder is not live");
        }
    }
}

async fn peer_reconfiguration_fixture(label: &str) -> (DaemonRuntime, InfoHash, PathBuf, PathBuf) {
    let root = unique_dir(label);
    let config_path = root.join("swarmotter.toml");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    cfg.bandwidth.max_peers = 3;
    cfg.bandwidth.max_peers_per_torrent = 2;
    cfg.queue.max_active_seeds = 1;
    cfg.seeding.global_ratio_limit = None;
    cfg.seeding.global_idle_limit = None;
    write_config_atomically(&config_path, &cfg).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_and_broker(
        cfg,
        health,
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );
    let (hash, _) = add_complete_seed_fixture(
        &runtime,
        "peer-reconfiguration-seed.bin",
        b"generated lawful peer reconfiguration fixture",
    )
    .await;
    runtime.reconcile_seeders().await;
    assert!(runtime.seeder_registry.contains(&hash).await);
    (runtime, hash, root, config_path)
}

async fn active_engine_reconfiguration_fixture(
    label: &str,
) -> (DaemonRuntime, InfoHash, PathBuf, PathBuf) {
    let root = unique_dir(label);
    let config_path = root.join("swarmotter.toml");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    cfg.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    cfg.dht.enabled = false;
    cfg.pex.enabled = false;
    cfg.bandwidth.max_peers = 3;
    cfg.bandwidth.max_peers_per_torrent = 2;
    write_config_atomically(&config_path, &cfg).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_and_broker(
        cfg,
        health,
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "active-peer-reconfiguration.bin",
        b"generated active engine peer reconfiguration fixture",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = meta.info_hash;
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.ensure_torrent_peer_permit_pool(hash).await;
    runtime.start_engine(hash).await;
    assert!(runtime.engine_running_for_test(&hash).await);
    (runtime, hash, root, config_path)
}

fn scale_hash_bytes(n: u32) -> [u8; 20] {
    let mut bytes = [0u8; 20];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    bytes
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
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
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
    let hash = meta.info_hash;
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
        vec![inherited.info_hash()],
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
    let hash = meta.info_hash;
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
    let hash = meta.info_hash;
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
        .find(|torrent| torrent.info_hash() == hash)
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
    let hash = meta.info_hash;
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
        .find(|torrent| torrent.info_hash() == hash)
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
    let hash = meta.info_hash;
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
    let hash = meta.info_hash;
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
async fn distinct_same_name_magnets_cannot_share_placeholder_paths() {
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

    runtime
        .add_magnet_with_options(first, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    let error = runtime
        .add_magnet_with_options(second, AddTorrentOptions::new(None, true))
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::Storage(_)));
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
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
    let hash = meta.info_hash;
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
    let hash = torrent.info_hash();
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
    assert!(error.to_string().contains(&hash.to_hex()));
    assert!(error.to_string().contains("seeding.ratio_limit"));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn rename_rejects_the_torrents_own_resume_path() {
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
        .rename_path(&hash, 0, "resume-name.bin.swarmotter.resume".into())
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
    let hash = meta.info_hash;
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
        .info_hashes()
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
            .info_hashes()
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
    let prior_live = runtime.seeder_registry.info_hashes().await[0];
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
        .find(|torrent| torrent.info_hash() == prior_live)
        .unwrap();
    let stored_candidate = stored
        .torrents
        .iter()
        .find(|torrent| torrent.info_hash() == candidate_only)
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
    let hash = meta.info_hash;
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
    assert!(runtime.engine_running_for_test(&hash).await);

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
    assert!(runtime.engine_running_for_test(&hash).await);
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
    let unrelated_hash = meta.info_hash;
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
    assert!(!runtime.engine_running_for_test(&unrelated_hash).await);
    continue_reconstruction.send(()).unwrap();
    update.await.unwrap().unwrap();
    unrelated_start.await.unwrap();
    assert!(runtime.engine_running_for_test(&active_hash).await);
    assert!(runtime.engine_running_for_test(&unrelated_hash).await);
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
    assert!(runtime.engine_running_for_test(&hash).await);
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
    assert!(runtime.engine_running_for_test(&hash).await);
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
    assert!(runtime.engine_running_for_test(&hash).await);
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
    assert!(!runtime.engine_running_for_test(&hash).await);
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
    let hash = meta.info_hash;
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
    assert!(runtime.engine_running_for_test(&hash).await);
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
    let hash = meta.info_hash;
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
            && event.info_hash.as_deref() == Some(first.to_hex().as_str())
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
            && event.info_hash.as_deref() == Some(hash.to_hex().as_str())
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
            && event.info_hash.as_deref() == Some(hash.to_hex().as_str())
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
    let hash = meta.info_hash;
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
            info_hash: hash,
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
        .find(|torrent| torrent.info_hash() == hash)
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

    assert_eq!(hash, meta.info_hash);
    let event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, "torrent_added");
    assert_eq!(event.info_hash.as_deref(), Some(hash.to_hex().as_str()));
    let payload: serde_json::Value = serde_json::from_str(&event.json).unwrap();
    assert_eq!(payload["info_hash"], hash.to_hex());
    assert_eq!(payload["payload"]["info_hash"], hash.to_hex());
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
    let hash = meta.info_hash;
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
    let hash = meta.info_hash;

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
    let hash = real_meta.info_hash;
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
    torrent.magnet_info_hash = Some(hash);
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
    let hash =
        swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
            .unwrap();
    let mut torrent = Torrent::new(placeholder_meta, 1);
    torrent.state = TorrentState::Queued;
    torrent.needs_metadata = true;
    torrent.magnet_info_hash = Some(hash);
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

    runtime.force_stop_engine(&hash).await;
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
    let hash =
        swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
            .unwrap();
    let piece_count = placeholder_meta.piece_count();
    let total_length = placeholder_meta.total_length;
    let mut torrent = Torrent::new(placeholder_meta, 1);
    torrent.state = TorrentState::DownloadingMetadata;
    torrent.needs_metadata = true;
    torrent.magnet_info_hash = Some(hash);
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
    let first_hash = first.info_hash;
    let blocked_hash = blocked.info_hash;
    let fitting_hash = fitting.info_hash;
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
    let hash = resolved.info_hash;
    let mut torrent = Torrent::new(resolved.clone(), now());
    torrent.state = TorrentState::DownloadingMetadata;
    torrent.needs_metadata = true;
    runtime.registry.lock().await.add(torrent).unwrap();
    let admission = storage_root_admission_for_path(&cfg, &root).unwrap();
    let blocker = InfoHash::from_bytes([0x42; 20]);
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
                resolved,
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
    let hash = meta.info_hash;
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
            .find(|torrent| torrent.info_hash() == hash)
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
    let hash = meta.info_hash;
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
            .find(|torrent| torrent.info_hash() == hash)
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
    let hash = meta.info_hash;
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
            .find(|torrent| torrent.info_hash() == hash)
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
    let first_hash = first.info_hash;
    let second_hash = second.info_hash;
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
    let first_hash = first.info_hash;
    let second_hash = second.info_hash;
    let third_hash = third.info_hash;
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
            .any(|record| record.hash == second_hash),
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
            .any(|record| record.hash == third_hash),
        "a post-PUT engine start must use the tightened root limit"
    );

    runtime.force_stop_engine(&first_hash).await;
    runtime.force_stop_engine(&second_hash).await;
    runtime.force_stop_engine(&third_hash).await;
    runtime.storage_admissions.release(&first_hash).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn unfinished_engine_exit_requeues_and_releases_active_slot() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first_bytes = swarmotter_core::meta::build_single_file_torrent(
        "unfinished-first.bin",
        b"unfinished first payload",
        8,
        None,
        false,
    );
    let second_bytes = swarmotter_core::meta::build_single_file_torrent(
        "unfinished-second.bin",
        b"unfinished second payload",
        8,
        None,
        false,
    );
    let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
    let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
    let first_hash = first.info_hash;
    let second_hash = second.info_hash;
    let mut first_torrent = Torrent::new(first, 1);
    first_torrent.state = TorrentState::Downloading;
    {
        let mut reg = runtime.registry.lock().await;
        reg.add(first_torrent).unwrap();
        reg.add(Torrent::new(second, 2)).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
    }

    let queued = runtime
        .queue_torrent_for_retry(
            first_hash,
            "engine stopped before completion; queued for retry",
            ENGINE_INCOMPLETE_RETRY_DELAY,
        )
        .await;

    assert!(queued);
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&first_hash)
            .unwrap()
            .state,
        TorrentState::Queued
    );
    assert_eq!(runtime.queue.lock().await.position(&second_hash), Some(1));
    assert_eq!(runtime.queue.lock().await.position(&first_hash), Some(2));
    assert!(runtime
        .engine_retry_after
        .read()
        .await
        .get(&first_hash)
        .is_some_and(|retry_at| *retry_at > Instant::now()));
    assert_eq!(runtime.desired_download_hashes().await, vec![second_hash]);
}

#[tokio::test]
async fn stale_active_without_engine_is_requeued_and_releases_active_slot() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let stale_bytes = swarmotter_core::meta::build_single_file_torrent(
        "stale-active.bin",
        b"stale active payload",
        8,
        None,
        false,
    );
    let queued_bytes = swarmotter_core::meta::build_single_file_torrent(
        "queued-behind-stale.bin",
        b"queued behind stale payload",
        8,
        None,
        false,
    );
    let stale_meta = swarmotter_core::meta::parse_torrent(&stale_bytes).unwrap();
    let queued_meta = swarmotter_core::meta::parse_torrent(&queued_bytes).unwrap();
    let stale_hash = stale_meta.info_hash;
    let queued_hash = queued_meta.info_hash;
    let mut stale_torrent = Torrent::new(stale_meta, 1);
    stale_torrent.state = TorrentState::Downloading;
    {
        let mut reg = runtime.registry.lock().await;
        reg.add(stale_torrent).unwrap();
        reg.add(Torrent::new(queued_meta, 2)).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(stale_hash);
        queue.add(queued_hash);
    }

    let recovered = runtime.sweep_stale_active_torrents("test").await;

    assert_eq!(recovered, 1);
    {
        let reg = runtime.registry.lock().await;
        let torrent = reg.get(&stale_hash).unwrap();
        assert_eq!(torrent.state, TorrentState::Queued);
        assert_eq!(
            torrent.error.as_deref(),
            Some(STALE_ACTIVE_RECOVERY_MESSAGE)
        );
    }
    assert_eq!(runtime.queue.lock().await.position(&queued_hash), Some(1));
    assert_eq!(runtime.queue.lock().await.position(&stale_hash), Some(2));
    assert_eq!(runtime.desired_download_hashes().await, vec![queued_hash]);
}

#[tokio::test]
async fn stale_metadata_progress_does_not_reactivate_large_queue_above_limit() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.max_active_metadata_fetches = 50;
    cfg.queue.auto_start = true;
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

    {
        let mut reg = runtime.registry.lock().await;
        let mut queue = runtime.queue.lock().await;
        let mut states = runtime.engine_states.write().await;
        for idx in 1..=100u8 {
            let hash = InfoHash::from_bytes([idx; 20]);
            let mut torrent = Torrent::new(placeholder_meta.clone(), idx as u64);
            torrent.state = TorrentState::DownloadingMetadata;
            torrent.needs_metadata = true;
            torrent.magnet_info_hash = Some(hash);
            reg.add(torrent).unwrap();
            queue.add(hash);
            states.insert(
                hash,
                Arc::new(Mutex::new(EngineState {
                    piece_count: placeholder_meta.piece_count(),
                    total_length: placeholder_meta.total_length,
                    ..Default::default()
                })),
            );
        }
    }

    let recovered = runtime.sweep_stale_active_torrents("test").await;
    assert_eq!(recovered, 100);

    runtime.reconcile_engine_progress().await;

    let active_count = runtime
        .registry
        .lock()
        .await
        .torrents
        .values()
        .filter(|torrent| {
            matches!(
                torrent.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            )
        })
        .count();
    assert_eq!(
        active_count, 0,
        "retained metadata diagnostics must not bypass active queue limits"
    );
    assert_eq!(runtime.desired_download_hashes().await.len(), 50);
}

#[tokio::test]
async fn ten_thousand_stale_metadata_records_recover_without_active_leak() {
    const TOTAL_TORRENTS: usize = 10_000;

    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.max_active_metadata_fetches = 50;
    cfg.queue.auto_start = true;
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
    let hashes = (0..TOTAL_TORRENTS)
        .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
        .collect::<Vec<_>>();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            torrent.state = TorrentState::DownloadingMetadata;
            torrent.needs_metadata = true;
            torrent.magnet_info_hash = Some(hash);
            reg.add(torrent).unwrap();
        }
    }
    runtime.queue.lock().await.add_many(hashes.iter().copied());

    let recovered = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.sweep_stale_active_torrents("test"),
    )
    .await
    .expect("stale active recovery should be bounded for 10,000 records");

    assert_eq!(recovered, TOTAL_TORRENTS);
    let reg = runtime.registry.lock().await;
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| {
                matches!(
                    torrent.state,
                    TorrentState::Downloading | TorrentState::DownloadingMetadata
                )
            })
            .count(),
        0
    );
    drop(reg);
    assert_eq!(runtime.desired_download_hashes().await.len(), 50);
    assert_eq!(runtime.queue.lock().await.order.len(), TOTAL_TORRENTS);
}

#[tokio::test]
async fn ten_thousand_metadata_retry_backoffs_leave_no_active_desired_slots() {
    const TOTAL_TORRENTS: usize = 10_000;

    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.max_active_metadata_fetches = 50;
    cfg.queue.auto_start = true;
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
    let hashes = (0..TOTAL_TORRENTS)
        .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
        .collect::<Vec<_>>();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            torrent.state = TorrentState::Queued;
            torrent.needs_metadata = true;
            torrent.magnet_info_hash = Some(hash);
            reg.add(torrent).unwrap();
        }
    }
    runtime.queue.lock().await.add_many(hashes.iter().copied());
    {
        let mut retry_after = runtime.engine_retry_after.write().await;
        let retry_until = Instant::now() + MAGNET_METADATA_NO_PEERS_RETRY_DELAY;
        for hash in &hashes {
            retry_after.insert(*hash, retry_until);
        }
    }

    let desired = tokio::time::timeout(Duration::from_secs(5), runtime.desired_download_hashes())
        .await
        .expect("desired active planning should be bounded for 10,000 retrying magnets");

    assert!(desired.is_empty());
    assert_eq!(runtime.queue.lock().await.order.len(), TOTAL_TORRENTS);
}

#[tokio::test]
#[ignore = "scale regression: mixed lifecycle states at 1k+ records"]
async fn ignored_thousand_mixed_state_torrents_keep_scheduler_bounds() {
    const TOTAL_TORRENTS: usize = 1_200;
    const MAX_ACTIVE_DOWNLOADS: usize = 32;
    const MAX_ACTIVE_METADATA_FETCHES: usize = 24;
    const LIVE_DOWNLOAD_COUNT: usize = 20;
    const LIVE_METADATA_COUNT: usize = 16;
    const STALE_DOWNLOAD_COUNT: usize = 40;
    const STALE_METADATA_COUNT: usize = 44;
    const QUEUED_DOWNLOAD_COUNT: usize = 260;
    const QUEUED_METADATA_COUNT: usize = 220;
    const BACKOFF_METADATA_COUNT: usize = 150;
    const COMPLETED_COUNT: usize = 120;
    const PAUSED_COUNT: usize = 100;
    const SEEDING_COUNT: usize = 60;
    const CHECKING_COUNT: usize = 50;
    const ERROR_COUNT: usize = 40;
    const NETWORK_BLOCKED_COUNT: usize = 30;
    const STORAGE_ERROR_COUNT: usize = 25;
    const TRACKER_ERROR_COUNT: usize = 25;
    const LIVE_METADATA_START: usize = LIVE_DOWNLOAD_COUNT;
    const STALE_DOWNLOAD_START: usize = LIVE_METADATA_START + LIVE_METADATA_COUNT;
    const STALE_METADATA_START: usize = STALE_DOWNLOAD_START + STALE_DOWNLOAD_COUNT;
    const QUEUED_DOWNLOAD_START: usize = STALE_METADATA_START + STALE_METADATA_COUNT;
    const QUEUED_METADATA_START: usize = QUEUED_DOWNLOAD_START + QUEUED_DOWNLOAD_COUNT;
    const BACKOFF_METADATA_START: usize = QUEUED_METADATA_START + QUEUED_METADATA_COUNT;
    const COMPLETED_START: usize = BACKOFF_METADATA_START + BACKOFF_METADATA_COUNT;
    const PAUSED_START: usize = COMPLETED_START + COMPLETED_COUNT;
    const SEEDING_START: usize = PAUSED_START + PAUSED_COUNT;
    const CHECKING_START: usize = SEEDING_START + SEEDING_COUNT;
    const ERROR_START: usize = CHECKING_START + CHECKING_COUNT;
    const NETWORK_BLOCKED_START: usize = ERROR_START + ERROR_COUNT;
    const STORAGE_ERROR_START: usize = NETWORK_BLOCKED_START + NETWORK_BLOCKED_COUNT;
    const TRACKER_ERROR_START: usize = STORAGE_ERROR_START + STORAGE_ERROR_COUNT;

    assert_eq!(TRACKER_ERROR_START + TRACKER_ERROR_COUNT, TOTAL_TORRENTS);

    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = MAX_ACTIVE_DOWNLOADS;
    cfg.queue.max_active_metadata_fetches = MAX_ACTIVE_METADATA_FETCHES;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "mixed-state placeholder",
        b"placeholder payload",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let hashes = (0..TOTAL_TORRENTS)
        .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
        .collect::<Vec<_>>();
    let backoff_start = Instant::now();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            torrent.magnet_info_hash = Some(hash);
            if idx < LIVE_DOWNLOAD_COUNT {
                torrent.state = TorrentState::Downloading;
                torrent.needs_metadata = false;
            } else if idx < LIVE_METADATA_START + LIVE_METADATA_COUNT {
                torrent.state = TorrentState::DownloadingMetadata;
                torrent.needs_metadata = true;
            } else if idx < STALE_DOWNLOAD_START + STALE_DOWNLOAD_COUNT {
                torrent.state = TorrentState::Downloading;
                torrent.needs_metadata = false;
            } else if idx < STALE_METADATA_START + STALE_METADATA_COUNT {
                torrent.state = TorrentState::DownloadingMetadata;
                torrent.needs_metadata = true;
            } else if idx < QUEUED_DOWNLOAD_START + QUEUED_DOWNLOAD_COUNT {
                torrent.state = TorrentState::Queued;
                torrent.needs_metadata = false;
            } else if idx < BACKOFF_METADATA_START + BACKOFF_METADATA_COUNT {
                torrent.state = TorrentState::Queued;
                torrent.needs_metadata = true;
            } else if idx < COMPLETED_START + COMPLETED_COUNT {
                torrent.state = TorrentState::Completed;
                torrent.date_completed = Some((idx + 1) as u64);
                torrent.needs_metadata = false;
            } else if idx < PAUSED_START + PAUSED_COUNT {
                torrent.state = TorrentState::Paused;
                torrent.needs_metadata = false;
            } else if idx < SEEDING_START + SEEDING_COUNT {
                torrent.state = TorrentState::Seeding;
                torrent.needs_metadata = false;
            } else if idx < CHECKING_START + CHECKING_COUNT {
                torrent.state = TorrentState::Checking;
                torrent.needs_metadata = false;
            } else {
                torrent.state = if idx < ERROR_START + ERROR_COUNT {
                    TorrentState::Error
                } else if idx < NETWORK_BLOCKED_START + NETWORK_BLOCKED_COUNT {
                    TorrentState::NetworkBlocked
                } else if idx < STORAGE_ERROR_START + STORAGE_ERROR_COUNT {
                    TorrentState::StorageError
                } else {
                    TorrentState::TrackerError
                };
                torrent.needs_metadata = false;
                torrent.error = Some("mixed-state scale fixture error".to_string());
            }
            reg.add(torrent).unwrap();
        }
    }

    runtime.queue.lock().await.add_many(hashes.iter().copied());
    {
        let mut handles = runtime.engine_handles.write().await;
        for hash in hashes.iter().take(LIVE_DOWNLOAD_COUNT).chain(
            hashes
                .iter()
                .skip(LIVE_METADATA_START)
                .take(LIVE_METADATA_COUNT),
        ) {
            handles.insert(
                *hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
        }
    }
    {
        let mut retry_after = runtime.engine_retry_after.write().await;
        for hash in hashes
            .iter()
            .skip(BACKOFF_METADATA_START)
            .take(BACKOFF_METADATA_COUNT)
        {
            retry_after.insert(*hash, backoff_start + Duration::from_secs(60));
        }
    }

    let reg = runtime.registry.lock().await;
    assert_eq!(reg.torrents.len(), TOTAL_TORRENTS);
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Queued)
            .count(),
        QUEUED_DOWNLOAD_COUNT + QUEUED_METADATA_COUNT + BACKOFF_METADATA_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::DownloadingMetadata)
            .count(),
        LIVE_METADATA_COUNT + STALE_METADATA_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Downloading)
            .count(),
        LIVE_DOWNLOAD_COUNT + STALE_DOWNLOAD_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Completed)
            .count(),
        COMPLETED_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Paused)
            .count(),
        PAUSED_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Seeding)
            .count(),
        SEEDING_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Checking)
            .count(),
        CHECKING_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Error)
            .count(),
        ERROR_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::NetworkBlocked)
            .count(),
        NETWORK_BLOCKED_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::StorageError)
            .count(),
        STORAGE_ERROR_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::TrackerError)
            .count(),
        TRACKER_ERROR_COUNT
    );
    drop(reg);

    let stale_recovered = runtime.sweep_stale_active_torrents("scale_test").await;
    assert_eq!(stale_recovered, STALE_DOWNLOAD_COUNT + STALE_METADATA_COUNT);

    let desired = tokio::time::timeout(Duration::from_secs(5), runtime.desired_download_hashes())
        .await
        .expect("mixed-state scheduler planning should remain bounded for 1,200 records");
    assert_eq!(
        desired.len(),
        MAX_ACTIVE_DOWNLOADS + MAX_ACTIVE_METADATA_FETCHES
    );
    let desired_backoff_hashes = hashes
        .iter()
        .skip(BACKOFF_METADATA_START)
        .take(BACKOFF_METADATA_COUNT)
        .copied()
        .collect::<Vec<_>>();
    assert!(desired
        .iter()
        .all(|hash| !desired_backoff_hashes.contains(hash)));
    {
        let reg = runtime.registry.lock().await;
        assert_eq!(
            desired
                .iter()
                .filter(|hash| reg.get(hash).is_some_and(|torrent| torrent.needs_metadata))
                .count(),
            MAX_ACTIVE_METADATA_FETCHES
        );
    }

    let stats = runtime.global_stats().await;
    assert_eq!(
        stats.scheduler.requested_downloads,
        LIVE_DOWNLOAD_COUNT + STALE_DOWNLOAD_COUNT + QUEUED_DOWNLOAD_COUNT
    );
    assert_eq!(
        stats.scheduler.requested_metadata_fetches,
        LIVE_METADATA_COUNT + STALE_METADATA_COUNT + QUEUED_METADATA_COUNT
    );
    assert_eq!(stats.scheduler.granted_downloads, MAX_ACTIVE_DOWNLOADS);
    assert_eq!(
        stats.scheduler.granted_metadata_fetches,
        MAX_ACTIVE_METADATA_FETCHES
    );
    assert_eq!(
        stats.scheduler.retry_backoff_torrents,
        BACKOFF_METADATA_COUNT
    );
    assert_eq!(
        stats.scheduler.queued_torrents,
        QUEUED_DOWNLOAD_COUNT
            + QUEUED_METADATA_COUNT
            + BACKOFF_METADATA_COUNT
            + STALE_DOWNLOAD_COUNT
            + STALE_METADATA_COUNT
    );
    assert_eq!(
        stats.scheduler.running_engines,
        LIVE_DOWNLOAD_COUNT + LIVE_METADATA_COUNT
    );
    assert_eq!(stats.scheduler.running_downloads, LIVE_DOWNLOAD_COUNT);
    assert_eq!(
        stats.scheduler.running_metadata_fetches,
        LIVE_METADATA_COUNT
    );
    assert_eq!(stats.scheduler.active_download_limit, MAX_ACTIVE_DOWNLOADS);
    assert_eq!(
        stats.scheduler.active_metadata_fetch_limit,
        MAX_ACTIVE_METADATA_FETCHES
    );
    assert_eq!(
        runtime.active_download_hashes().await.len(),
        LIVE_DOWNLOAD_COUNT + LIVE_METADATA_COUNT
    );
    assert_eq!(
        runtime.engine_retry_after.read().await.len(),
        BACKOFF_METADATA_COUNT
    );
    assert!(stats.scheduler.download_slots_saturated);
    assert!(stats.scheduler.metadata_fetch_slots_saturated);

    for hash in hashes.iter().take(LIVE_DOWNLOAD_COUNT).chain(
        hashes
            .iter()
            .skip(LIVE_METADATA_START)
            .take(LIVE_METADATA_COUNT),
    ) {
        runtime.force_stop_engine(hash).await;
    }
}

#[tokio::test]
async fn metadata_fetch_limit_is_separate_from_download_slot_limit() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 2;
    cfg.queue.max_active_metadata_fetches = 3;
    cfg.queue.auto_start = true;
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
    let mut metadata_hashes = Vec::new();
    let mut download_hashes = Vec::new();

    {
        let mut reg = runtime.registry.lock().await;
        let mut queue = runtime.queue.lock().await;
        for idx in 0..6u32 {
            let hash = InfoHash::from_bytes(scale_hash_bytes(idx));
            let mut torrent = Torrent::new(placeholder_meta.clone(), idx as u64 + 1);
            torrent.state = TorrentState::Queued;
            torrent.needs_metadata = true;
            torrent.magnet_info_hash = Some(hash);
            reg.add(torrent).unwrap();
            queue.add(hash);
            metadata_hashes.push(hash);
        }
        for idx in 0..5u32 {
            let name = format!("resolved-download-{idx}.bin");
            let payload = format!("resolved download payload {idx}");
            let bytes = swarmotter_core::meta::build_single_file_torrent(
                &name,
                payload.as_bytes(),
                8,
                None,
                false,
            );
            let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
            let hash = meta.info_hash;
            reg.add(Torrent::new(meta, idx as u64 + 10)).unwrap();
            queue.add(hash);
            download_hashes.push(hash);
        }
    }

    let desired = runtime.desired_download_hashes().await;

    assert_eq!(
        desired
            .iter()
            .filter(|hash| metadata_hashes.contains(hash))
            .count(),
        3
    );
    assert_eq!(
        desired
            .iter()
            .filter(|hash| download_hashes.contains(hash))
            .count(),
        2
    );
    assert_eq!(desired.len(), 5);

    let stats = runtime.global_stats().await;
    assert_eq!(stats.scheduler.requested_metadata_fetches, 6);
    assert_eq!(stats.scheduler.granted_metadata_fetches, 3);
    assert_eq!(stats.scheduler.requested_downloads, 5);
    assert_eq!(stats.scheduler.granted_downloads, 2);
    assert_eq!(stats.scheduler.active_metadata_fetch_limit, 3);
    assert_eq!(stats.scheduler.active_download_limit, 2);
    assert!(stats.scheduler.metadata_fetch_slots_saturated);
    assert!(stats.scheduler.download_slots_saturated);
}

#[tokio::test]
async fn queued_torrent_with_stale_engine_handle_is_cleared_for_restart() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "stale-queued-handle.bin",
        b"stale queued handle payload",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = meta.info_hash;
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();
    runtime.queue.lock().await.add(hash);
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(hash, tx);
    runtime.engine_handles.write().await.insert(
        hash,
        tokio::spawn(async {
            std::future::pending::<()>().await;
        }),
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(EngineState::default())));

    let recovered = tokio::time::timeout(
        Duration::from_millis(100),
        runtime.sweep_inactive_engine_handles("test"),
    )
    .await
    .expect("stale queued handles should be force-cleared promptly");

    assert_eq!(recovered, 1);
    assert!(!runtime.engine_handles.read().await.contains_key(&hash));
    assert!(!runtime.engine_cmds.lock().await.contains_key(&hash));
    assert!(!runtime.engine_states.read().await.contains_key(&hash));
    {
        let reg = runtime.registry.lock().await;
        let torrent = reg.get(&hash).unwrap();
        assert_eq!(torrent.state, TorrentState::Queued);
        assert_eq!(
            torrent.error.as_deref(),
            Some(STALE_INACTIVE_ENGINE_RECOVERY_MESSAGE)
        );
    }
    assert_eq!(runtime.desired_download_hashes().await, vec![hash]);
}

#[tokio::test]
async fn reconcile_queue_force_clears_over_limit_active_engine() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first_bytes = swarmotter_core::meta::build_single_file_torrent(
        "active-slot-one.bin",
        b"active slot one payload",
        8,
        None,
        false,
    );
    let second_bytes = swarmotter_core::meta::build_single_file_torrent(
        "active-slot-two.bin",
        b"active slot two payload",
        8,
        None,
        false,
    );
    let first_meta = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
    let second_meta = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
    let first_hash = first_meta.info_hash;
    let second_hash = second_meta.info_hash;
    let mut first_torrent = Torrent::new(first_meta, 1);
    first_torrent.state = TorrentState::Downloading;
    let mut second_torrent = Torrent::new(second_meta, 2);
    second_torrent.state = TorrentState::Downloading;
    {
        let mut reg = runtime.registry.lock().await;
        reg.add(first_torrent).unwrap();
        reg.add(second_torrent).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
    }
    {
        let mut handles = runtime.engine_handles.write().await;
        handles.insert(
            first_hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
        handles.insert(
            second_hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
    }

    tokio::time::timeout(Duration::from_millis(100), runtime.reconcile_queue())
        .await
        .expect("queue reconciliation must not hang on over-limit active work");

    assert!(runtime
        .engine_handles
        .read()
        .await
        .contains_key(&first_hash));
    assert!(!runtime
        .engine_handles
        .read()
        .await
        .contains_key(&second_hash));
    {
        let reg = runtime.registry.lock().await;
        assert_eq!(
            reg.get(&first_hash).unwrap().state,
            TorrentState::Downloading
        );
        assert_eq!(reg.get(&second_hash).unwrap().state, TorrentState::Queued);
    }
    assert_eq!(runtime.active_download_hashes().await, vec![first_hash]);

    runtime.force_stop_engine(&first_hash).await;
}

#[tokio::test]
async fn large_queue_recovery_keeps_configured_active_slots_startable() {
    assert_large_queue_recovery_keeps_configured_active_slots_startable(100).await;
}

#[tokio::test]
async fn thousand_torrent_queue_recovery_keeps_configured_active_slots_startable() {
    assert_large_queue_recovery_keeps_configured_active_slots_startable(1_000).await;
}

async fn assert_large_queue_recovery_keeps_configured_active_slots_startable(
    total_torrents: usize,
) {
    assert!(total_torrents >= 50);
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let mut hashes = Vec::new();
    {
        let mut reg = runtime.registry.lock().await;
        let mut queue = runtime.queue.lock().await;
        for idx in 0..total_torrents {
            let name = format!("large-queue-{idx}.bin");
            let payload = format!("large queue payload {idx}");
            let bytes = swarmotter_core::meta::build_single_file_torrent(
                &name,
                payload.as_bytes(),
                8,
                None,
                false,
            );
            let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
            let hash = meta.info_hash;
            let mut torrent = Torrent::new(meta, (idx + 1) as u64);
            if idx < 18 {
                torrent.state = TorrentState::Downloading;
            }
            reg.add(torrent).unwrap();
            queue.add(hash);
            hashes.push(hash);
        }
    }

    {
        let mut handles = runtime.engine_handles.write().await;
        for hash in hashes.iter().take(18) {
            handles.insert(
                *hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
        }
        for hash in hashes.iter().skip(18).take(32) {
            handles.insert(
                *hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
        }
    }

    assert_eq!(runtime.active_download_hashes().await.len(), 18);
    let recovered = runtime.sweep_inactive_engine_handles("test").await;
    assert_eq!(recovered, 32);

    let desired = runtime.desired_download_hashes().await;
    assert_eq!(desired.len(), 50);
    assert_eq!(
        desired
            .iter()
            .filter(|hash| hashes[..18].contains(hash))
            .count(),
        18
    );
    let running = runtime.engine_handles.read().await;
    let blocked_startable = desired
        .iter()
        .filter(|hash| !hashes[..18].contains(hash) && running.contains_key(hash))
        .count();
    assert_eq!(
            blocked_startable, 0,
            "queued torrents selected to fill the configured active slots must not retain stale handles that make start_engine skip them"
        );
    drop(running);

    for hash in hashes.iter().take(18) {
        runtime.force_stop_engine(hash).await;
    }
}

#[tokio::test]
async fn engine_task_finished_clears_restart_blocking_runtime_bookkeeping() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let hash =
        swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
            .unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(hash, tx);
    runtime
        .engine_handles
        .write()
        .await
        .insert(hash, tokio::spawn(async {}));
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(EngineState::default())));
    runtime.torrent_limiters.write().await.insert(
        hash,
        Arc::new(swarmotter_core::bandwidth::RateLimiter::new(0, 0)),
    );
    runtime.rate_samples.write().await.insert(
        hash,
        RateSample {
            downloaded: 1,
            uploaded: 1,
            rate_down: 1,
            rate_up: 1,
            last_download_at: Some(Instant::now()),
            last_upload_at: Some(Instant::now()),
            no_download_since: None,
            at: Instant::now(),
            peak_rate_down: 1,
            peak_rate_up: 1,
        },
    );

    runtime.engine_task_finished(hash).await;

    assert!(!runtime.engine_cmds.lock().await.contains_key(&hash));
    assert!(!runtime.engine_handles.read().await.contains_key(&hash));
    assert!(
        runtime.torrent_limiters.read().await.contains_key(&hash),
        "normal engine completion must retain the torrent limiter for queued seeding"
    );
    assert!(
        runtime.engine_states.read().await.contains_key(&hash),
        "diagnostic state should survive normal engine task exit"
    );
    assert!(
        runtime.rate_samples.read().await.contains_key(&hash),
        "rate samples should survive normal engine task exit"
    );
}

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
    let hash = meta.info_hash;
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
    let hash = meta.info_hash;
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
    let hash = meta.info_hash;
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
    let first_hash = first.info_hash;
    let second_hash = second.info_hash;
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
    let hash = meta.info_hash;
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
    let hash = meta.info_hash;
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
    let hash = meta.info_hash;
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
    let stalled_hash = stalled_meta.info_hash;
    let queued_hash = queued_meta.info_hash;
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
    let stalled_hash = stalled_meta.info_hash;
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

#[tokio::test]
async fn list_trackers_exposes_scrape_state_and_falls_back_without_announce_success() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let primary = "http://tracker.example/announce";
    let secondary = "http://backup.example/announce.php";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "trackers.bin",
        b"0123456789abcdef",
        8,
        Some(primary),
        false,
    );
    let mut meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    meta.announce_list = vec![vec![primary.into(), secondary.into()]];
    let hash = meta.info_hash;
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();

    let mut state = EngineState::default();
    state.tracker_announces.insert(
        primary.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Ok,
            seeders: 256,
            leechers: 12,
            downloads: 0,
            last_error: None,
            last_message: Some("announce returned 64 peers".into()),
            last_announce: Some(1234),
        },
    );
    state.tracker_announces.insert(
        secondary.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Error,
            seeders: 0,
            leechers: 0,
            downloads: 0,
            last_error: Some("tracker announce timed out".into()),
            last_message: None,
            last_announce: Some(1235),
        },
    );
    state.tracker_scrapes.insert(
        primary.into(),
        crate::engine::TrackerScrapeSnapshot {
            status: TrackerScrapeStatus::Ok,
            seeders: Some(300),
            leechers: Some(20),
            downloads: Some(99),
            last_error: None,
            last_scrape: Some(1240),
        },
    );
    state.tracker_scrapes.insert(
        secondary.into(),
        crate::engine::TrackerScrapeSnapshot {
            status: TrackerScrapeStatus::Error,
            seeders: Some(40),
            leechers: Some(5),
            downloads: Some(6),
            last_error: Some("latest scrape was malformed".into()),
            last_scrape: Some(1241),
        },
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(state)));

    let trackers = runtime.list_trackers(&hash).await.unwrap();
    let primary_row = trackers.iter().find(|t| t.url == primary).unwrap();
    assert_eq!(primary_row.status, TrackerStatus::Ok);
    assert_eq!(primary_row.seeders, 256);
    assert_eq!(primary_row.leechers, 12);
    assert_eq!(primary_row.downloads, 99);
    assert_eq!(primary_row.last_error, None);
    assert_eq!(
        primary_row.last_message.as_deref(),
        Some("announce returned 64 peers")
    );
    assert_eq!(primary_row.last_announce, Some(1234));
    assert_eq!(primary_row.scrape_status, TrackerScrapeStatus::Ok);
    assert_eq!(primary_row.last_scrape, Some(1240));
    assert_eq!(primary_row.scrape_seeders, Some(300));
    assert_eq!(primary_row.scrape_leechers, Some(20));
    assert_eq!(primary_row.scrape_downloads, Some(99));
    assert_eq!(primary_row.tier, 0);

    let secondary_row = trackers.iter().find(|t| t.url == secondary).unwrap();
    assert_eq!(secondary_row.status, TrackerStatus::Error);
    assert_eq!(
        secondary_row.last_error.as_deref(),
        Some("tracker announce timed out")
    );
    assert_eq!(secondary_row.last_message, None);
    assert_eq!(secondary_row.seeders, 40);
    assert_eq!(secondary_row.leechers, 5);
    assert_eq!(secondary_row.downloads, 6);
    assert_eq!(secondary_row.scrape_status, TrackerScrapeStatus::Error);
    assert_eq!(secondary_row.last_scrape, Some(1241));
    assert_eq!(secondary_row.scrape_seeders, Some(40));
    assert_eq!(secondary_row.scrape_leechers, Some(5));
    assert_eq!(secondary_row.scrape_downloads, Some(6));
    assert_eq!(
        secondary_row.last_scrape_error.as_deref(),
        Some("latest scrape was malformed")
    );
    assert_eq!(secondary_row.tier, 0);
}

#[tokio::test]
async fn seeder_announce_schedules_scrape_into_the_shared_engine_state() {
    let hash = InfoHash::from_bytes([0x73; 20]);
    let announce_body = b"d8:completei5e10:incompletei6e8:intervali30e5:peers0:e".to_vec();
    let mut scrape_body = b"d5:filesd20:".to_vec();
    scrape_body.extend_from_slice(hash.as_bytes());
    scrape_body.extend_from_slice(b"d8:completei15e10:downloadedi17e10:incompletei16eeee");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            let body = if request.starts_with("GET /scrape?") {
                &scrape_body
            } else {
                assert!(request.starts_with("GET /announce?"));
                &announce_body
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        }
    });

    let url = format!("http://{address}/announce");
    let state = Arc::new(Mutex::new(EngineState::default()));
    let interval = DaemonRuntime::seeder_announce_once(
        &[vec![url.clone()]],
        hash,
        [0u8; 20],
        6881,
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        state.clone(),
        AnnounceEvent::Started,
    )
    .await;
    server.await.unwrap();

    assert_eq!(interval, 30);
    let engine = state.lock().await;
    assert_eq!(
        engine.tracker_announces.get(&url).unwrap().status,
        TrackerStatus::Ok
    );
    let scrape = engine.tracker_scrapes.get(&url).unwrap();
    assert_eq!(scrape.status, TrackerScrapeStatus::Ok);
    assert_eq!(scrape.seeders, Some(15));
    assert_eq!(scrape.leechers, Some(16));
    assert_eq!(scrape.downloads, Some(17));
}

#[tokio::test]
async fn tracker_scrape_snapshot_serializes_through_the_real_native_router() {
    use axum::body::Body;
    use swarmotter_api::state::{
        AppState, BuildInfo, QbittorrentCompatState, TransmissionCompatState,
    };
    use tower::ServiceExt as _;

    let config = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = Arc::new(DaemonRuntime::new(config.clone(), health));
    let tracker_url = "http://tracker.example/announce";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "router-scrape.bin",
        b"generated router scrape payload",
        8,
        Some(tracker_url),
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = meta.info_hash;
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();
    let mut engine = EngineState::default();
    engine.tracker_scrapes.insert(
        tracker_url.into(),
        crate::engine::TrackerScrapeSnapshot {
            status: TrackerScrapeStatus::Ok,
            seeders: Some(31),
            leechers: Some(32),
            downloads: Some(33),
            last_error: None,
            last_scrape: Some(34),
        },
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(engine)));

    let app_state = Arc::new(AppState {
        daemon: runtime,
        config: Arc::new(Mutex::new(config)),
        build: BuildInfo::default(),
        broker: EventBroker::default(),
        transmission: TransmissionCompatState::default(),
        qbittorrent: QbittorrentCompatState::default(),
    });
    let response = swarmotter_api::app_router(app_state)
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/api/v1/torrents/{hash}/trackers"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let row = &envelope["data"][0];
    assert_eq!(row["scrape_status"], "ok");
    assert_eq!(row["last_scrape"], 34);
    assert_eq!(row["scrape_seeders"], 31);
    assert_eq!(row["scrape_leechers"], 32);
    assert_eq!(row["scrape_downloads"], 33);
    assert_eq!(row["last_scrape_error"], serde_json::Value::Null);
    assert_eq!(row["seeders"], 31);
    assert_eq!(row["leechers"], 32);
    assert_eq!(row["downloads"], 33);
}

#[tokio::test]
async fn storage_preflight_rejects_torrent_file_add_before_registration() {
    let root = unique_dir("storage-preflight");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.minimum_free_space_bytes = u64::MAX;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "too-large.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );

    let err = runtime.add_torrent_file(bytes, None).await.unwrap_err();

    assert_eq!(err.code().as_str(), "storage_error");
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
}

#[tokio::test]
async fn reset_downloads_clears_storage_roots_registry_and_logs() {
    let root = unique_dir("reset");
    let download_dir = root.join("downloads");
    let incomplete_dir = root.join("incomplete");
    let log_file = root.join("swarmotterd.log");
    tokio::fs::create_dir_all(download_dir.join("nested"))
        .await
        .unwrap();
    tokio::fs::create_dir_all(&incomplete_dir).await.unwrap();
    tokio::fs::write(download_dir.join("nested").join("old.bin"), b"old")
        .await
        .unwrap();
    tokio::fs::write(incomplete_dir.join("partial.bin"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(&log_file, b"old log line\n")
        .await
        .unwrap();

    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(download_dir.display().to_string());
    cfg.storage.incomplete_dir = Some(incomplete_dir.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths(cfg, health, None, Some(log_file.clone()));
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "reset.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = meta.info_hash;
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();
    runtime.queue.lock().await.add(hash);
    runtime
        .engine_retry_after
        .write()
        .await
        .insert(hash, Instant::now() + Duration::from_secs(60));

    let result = runtime.reset_downloads().await.unwrap();

    assert_eq!(result.torrents_removed, 1);
    assert_eq!(result.log_files_cleared, 1);
    assert!(result
        .storage_paths
        .contains(&download_dir.display().to_string()));
    assert!(result
        .storage_paths
        .contains(&incomplete_dir.display().to_string()));
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
    assert!(runtime.engine_retry_after.read().await.is_empty());
    assert!(download_dir.is_dir());
    assert!(incomplete_dir.is_dir());
    assert!(tokio::fs::read_dir(&download_dir)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_none());
    assert!(tokio::fs::read_dir(&incomplete_dir)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_none());
    assert_eq!(tokio::fs::metadata(&log_file).await.unwrap().len(), 0);
}

#[test]
fn per_torrent_worker_limit_is_independent_of_global_session_budget() {
    assert_eq!(
        DaemonRuntime::effective_per_torrent_peer_limit(0),
        DEFAULT_PER_TORRENT_PEER_LIMIT
    );
    assert_eq!(DaemonRuntime::effective_per_torrent_peer_limit(24), 24);
}

#[tokio::test]
async fn peer_diagnostics_report_unlimited_observation_and_bounded_denial() {
    let mut unlimited_config = Config::default();
    unlimited_config.network.mode = NetworkContainmentMode::Disabled;
    unlimited_config.bandwidth.max_peers = 0;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let unlimited = DaemonRuntime::new(unlimited_config, health.clone());
    let unlimited_pool = unlimited.peer_permit_pool.read().await.clone();
    let permit = unlimited_pool.acquire().await.unwrap();
    let scheduler = unlimited.global_stats().await.scheduler;
    assert_eq!(scheduler.peer_limit, 0);
    assert_eq!(scheduler.peer_permits_in_use, 1);
    assert_eq!(scheduler.peer_permits_available, None);
    assert_eq!(scheduler.peer_sessions_denied, 0);
    drop(permit);
    assert_eq!(
        unlimited.global_stats().await.scheduler.peer_permits_in_use,
        0
    );

    let mut bounded_config = Config::default();
    bounded_config.network.mode = NetworkContainmentMode::Disabled;
    bounded_config.bandwidth.max_peers = 1;
    let bounded = DaemonRuntime::new(bounded_config, health);
    let bounded_pool = bounded.peer_permit_pool.read().await.clone();
    let permit = bounded_pool.try_acquire().unwrap();
    assert!(bounded_pool.try_acquire().is_none());
    let scheduler = bounded.global_stats().await.scheduler;
    assert_eq!(scheduler.peer_limit, 1);
    assert_eq!(scheduler.peer_permits_in_use, 1);
    assert_eq!(scheduler.peer_permits_available, Some(0));
    assert_eq!(scheduler.peer_sessions_denied, 1);
    drop(permit);
}

#[test]
fn strip_ansi_controls_removes_terminal_sequences_from_logs() {
    let raw = "\u{1b}[2m2026-07-03T19:43:03Z\u{1b}[0m \u{1b}[32mINFO\u{1b}[0m message";
    assert_eq!(
        strip_ansi_controls(raw),
        "2026-07-03T19:43:03Z INFO message"
    );
}

#[test]
fn encryption_mode_change_rebuilds_data_plane_without_process_restart() {
    let previous = Config::default();
    let mut next = previous.clone();
    next.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Required;

    assert!(data_plane_config_changed(&previous, &next));
    assert!(restart_required_fields(&previous, &next).is_empty());
}

#[test]
fn cow_strategy_change_rebuilds_data_plane_without_process_restart() {
    let previous = Config::default();
    let mut next = previous.clone();
    next.storage.cow_strategy = swarmotter_core::config::CowStrategy::DisableForNewFiles;

    assert!(data_plane_config_changed(&previous, &next));
    assert!(restart_required_fields(&previous, &next).is_empty());
}

#[test]
fn storage_root_changes_reject_torrents_that_still_depend_on_old_roots() {
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "storage-transition.bin",
        b"storage transition payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), 1);
    let previous = Config::default();
    let mut next = previous.clone();
    next.storage.download_dir = Some("/tmp/swarmotter-new-root".into());

    assert!(matches!(
        validate_storage_config_transition(&previous, &next, &[torrent]),
        Err(CoreError::InvalidConfig(_))
    ));
}

#[test]
fn storage_resume_and_fallback_root_changes_preserve_existing_torrent_placement() {
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "storage-resume-transition.bin",
        b"storage resume transition payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), 1);
    let previous = Config::default();

    let mut changed_resume = previous.clone();
    changed_resume.storage.resume_dir = Some("/tmp/swarmotter-new-resume".into());
    assert!(matches!(
        validate_storage_config_transition(
            &previous,
            &changed_resume,
            std::slice::from_ref(&torrent),
        ),
        Err(CoreError::InvalidConfig(message)) if message.contains("storage.resume_dir")
    ));

    let mut changed_temp = previous.clone();
    changed_temp.storage.temp_dir = Some("/tmp/swarmotter-new-scratch".into());
    assert!(matches!(
        validate_storage_config_transition(&previous, &changed_temp, &[torrent]),
        Err(CoreError::InvalidConfig(message)) if message.contains("storage.temp_dir")
    ));
}

#[tokio::test]
async fn state_directory_change_requires_restart_and_retains_active_state_path() {
    let root = unique_dir("state-directory-transition");
    let active_state_path = root.join("active-state.json");
    let configured_state_dir = root.join("configured-next-state");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        disabled_health(),
        None,
        None,
        Some(active_state_path.clone()),
        EventBroker::default(),
    );
    let mut next = cfg;
    next.storage.state_dir = Some(configured_state_dir.display().to_string());

    let result = runtime.replace_config(next).await.unwrap();

    assert!(result.restart_required);
    assert_eq!(result.restart_required_fields, vec!["storage.state_dir"]);
    assert_eq!(
        runtime.state_path.as_deref(),
        Some(active_state_path.as_path())
    );
    assert_eq!(
        runtime.get_config().await.storage.state_dir.as_deref(),
        Some(configured_state_dir.to_string_lossy().as_ref())
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn replace_config_preserves_and_redacts_auth_token() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.api.auth_token = Some("existing-token".into());
    cfg.api.require_auth = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    let mut next = runtime.get_config().await;
    next.api.auth_token = None;
    next.api.require_auth = true;
    let result = runtime.replace_config(next).await.unwrap();

    assert_eq!(
        runtime.get_config().await.api.auth_token.as_deref(),
        Some("existing-token")
    );
    assert_eq!(result.config.api.auth_token, None);
}

#[tokio::test]
async fn socks5_data_plane_binder_proxies_tracker_and_webseed_http() {
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = proxy_listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        for (expected_host, expected_path, response) in [
            (
                "tracker.example",
                "GET /announce HTTP/1.1",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            ),
            (
                "webseed.example",
                "GET /payload HTTP/1.1",
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/2\r\nConnection: close\r\n\r\nok",
            ),
        ] {
            let (mut stream, _) = proxy_listener.accept().await.unwrap();
            let mut greeting = [0u8; 3];
            stream.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, 0]);
            stream.write_all(&[5, 0]).await.unwrap();

            let mut request_head = [0u8; 5];
            stream.read_exact(&mut request_head).await.unwrap();
            assert_eq!(&request_head[..4], &[5, 1, 0, 3]);
            let mut target = vec![0u8; usize::from(request_head[4]) + 2];
            stream.read_exact(&mut target).await.unwrap();
            assert_eq!(&target[..request_head[4] as usize], expected_host.as_bytes());
            assert_eq!(&target[request_head[4] as usize..], &80u16.to_be_bytes());
            stream
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();

            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0, "HTTP request ended before its headers");
                request.extend_from_slice(&chunk[..read]);
                assert!(request.len() <= 16 * 1024, "HTTP request headers exceeded cap");
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            assert!(request.starts_with(expected_path));
            let request_lower = request.to_ascii_lowercase();
            assert!(request_lower.contains(&format!("host: {expected_host}")));
            if expected_host == "webseed.example" {
                assert!(request_lower.contains("range: bytes=0-1"));
            }
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.network.socks5.enabled = true;
    cfg.network.socks5.host = Some("127.0.0.1".into());
    cfg.network.socks5.port = proxy_port;
    cfg.torrent.utp_enabled = false;
    cfg.dht.enabled = false;
    cfg.validate().unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);
    let binder = runtime.data_plane_binder_for_test().await;

    let tracker = binder
        .http_get("http://tracker.example/announce")
        .await
        .unwrap();
    assert_eq!(tracker.body, b"ok");
    let webseed = binder
        .http_get_range("http://webseed.example/payload", 0, 2)
        .await
        .unwrap();
    assert_eq!(webseed.body, b"ok");
    proxy.await.unwrap();
}

#[tokio::test]
async fn replace_config_preserves_and_redacts_socks5_password() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.network.socks5.enabled = true;
    cfg.network.socks5.host = Some("proxy.example".into());
    cfg.network.socks5.username = Some("operator".into());
    cfg.network.socks5.password = Some("proxy-secret".into());
    cfg.torrent.utp_enabled = false;
    cfg.dht.enabled = false;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);

    let mut next = runtime.get_config().await;
    next.network.socks5.password = None;
    let result = runtime.replace_config(next).await.unwrap();

    assert_eq!(
        runtime
            .get_config()
            .await
            .network
            .socks5
            .password
            .as_deref(),
        Some("proxy-secret")
    );
    assert_eq!(result.config.network.socks5.password, None);
}

#[tokio::test]
async fn socks5_network_diagnostics_are_auditable_without_proxy_secrets() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.network.socks5.enabled = true;
    cfg.network.socks5.host = Some("proxy.example".into());
    cfg.network.socks5.username = Some("operator".into());
    cfg.network.socks5.password = Some("proxy-secret".into());
    cfg.torrent.utp_enabled = false;
    cfg.dht.enabled = false;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);

    let diagnostics = runtime.network_diagnostics().await;
    assert!(diagnostics.socks5_enabled);
    assert!(diagnostics.socks5_udp_blocked);
    assert!(diagnostics.checks.iter().any(|check| {
        check.id == "socks5_proxy"
            && check.detail.contains("target DNS is remote")
            && check
                .detail
                .contains("UDP tracker, DHT, and uTP are blocked")
    }));
    let serialized = serde_json::to_string(&diagnostics).unwrap();
    assert!(!serialized.contains("proxy.example"));
    assert!(!serialized.contains("proxy-secret"));
}

#[tokio::test]
async fn queue_scheduler_respects_auto_start_and_moves() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    cfg.queue.auto_start = false;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first_bytes =
        swarmotter_core::meta::build_single_file_torrent("q1.bin", b"queue-one", 4, None, false);
    let second_bytes =
        swarmotter_core::meta::build_single_file_torrent("q2.bin", b"queue-two", 4, None, false);
    let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
    let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
    let first_hash = first.info_hash;
    let second_hash = second.info_hash;

    {
        let mut reg = runtime.registry.lock().await;
        reg.add(Torrent::new(first, 1)).unwrap();
        reg.add(Torrent::new(second, 2)).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
    }

    assert!(runtime.desired_download_hashes().await.is_empty());

    runtime.queue.lock().await.start_now(&second_hash);
    assert_eq!(runtime.desired_download_hashes().await, vec![second_hash]);

    {
        let mut queue = runtime.queue.lock().await;
        queue.clear_bypass(&second_hash);
        queue.move_to_top(&first_hash);
    }
    runtime.config.write().await.queue.auto_start = true;
    assert_eq!(runtime.desired_download_hashes().await, vec![first_hash]);
}

#[tokio::test]
async fn add_operations_mark_existing_queue_reconcile_dirty() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.scheduled = true;
        state.dirty = false;
    }

    let magnet_hash = runtime
        .add_magnet(
            "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e&dn=bulk-one",
            None,
        )
        .await
        .unwrap();

    assert!(runtime.registry.lock().await.contains(&magnet_hash));
    assert_eq!(runtime.queue.lock().await.position(&magnet_hash), Some(1));
    assert!(runtime.engine_handles.read().await.is_empty());
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }

    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.dirty = false;
    }

    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "bulk-two.bin",
        b"bulk torrent file payload",
        4,
        None,
        false,
    );
    let file_hash = runtime.add_torrent_file(bytes, None).await.unwrap();

    assert!(runtime.registry.lock().await.contains(&file_hash));
    assert_eq!(runtime.queue.lock().await.position(&file_hash), Some(2));
    assert!(runtime.engine_handles.read().await.is_empty());
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }
}

#[tokio::test]
async fn runtime_queue_limit_update_marks_scheduled_reconcile_dirty() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.queue.max_active_downloads = 25;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.scheduled = true;
        state.dirty = false;
    }

    runtime
        .update_settings(swarmotter_api::state::SettingsPatch {
            queue: Some(swarmotter_core::queue::QueueLimits {
                max_active_downloads: 50,
                max_active_metadata_fetches: 100,
                max_active_seeds: 5,
                auto_start: true,
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(runtime.config.read().await.queue.max_active_downloads, 50);
    assert_eq!(runtime.queue.lock().await.limits.max_active_downloads, 50);
    let state = runtime.queue_reconcile.lock().await;
    assert!(state.scheduled);
    assert!(
            state.dirty,
            "runtime queue limit updates should schedule queue reconciliation instead of awaiting engine startup inline"
        );
}

#[tokio::test]
async fn queue_reconcile_scheduler_clears_after_rapid_adds() {
    let mut cfg = Config::default();
    cfg.queue.auto_start = false;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    let first_hash = runtime
        .add_magnet(
            "magnet:?xt=urn:btih:000000000000000000000000000000000000000a&dn=schedule-one",
            None,
        )
        .await
        .unwrap();
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(!state.dirty);
    }

    for index in 1..3 {
        let magnet = format!(
            "magnet:?xt=urn:btih:{:040x}&dn=schedule-{index}",
            index + 10
        );
        runtime.add_magnet(&magnet, None).await.unwrap();
    }
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let complete = {
                let state = runtime.queue_reconcile.lock().await;
                !state.scheduled && !state.dirty
            };
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    assert_eq!(runtime.registry.lock().await.torrents.len(), 3);
    assert_eq!(runtime.queue.lock().await.order.len(), 3);
    assert_eq!(runtime.queue.lock().await.position(&first_hash), Some(1));
    assert!(runtime.engine_handles.read().await.is_empty());
}

#[tokio::test]
async fn rapid_adds_queue_without_waiting_for_reconcile() {
    const ADD_COUNT: usize = 200;

    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.scheduled = true;
        state.dirty = false;
    }

    for index in 0..ADD_COUNT {
        let magnet = format!("magnet:?xt=urn:btih:{:040x}&dn=rapid-{index}", index + 1);
        let hash = runtime.add_magnet(&magnet, None).await.unwrap();
        assert_eq!(runtime.queue.lock().await.position(&hash), Some(index + 1));
    }

    assert_eq!(runtime.registry.lock().await.torrents.len(), ADD_COUNT);
    assert_eq!(runtime.queue.lock().await.order.len(), ADD_COUNT);
    assert!(runtime.engine_handles.read().await.is_empty());
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }
}

#[tokio::test]
async fn bulk_remove_clears_many_torrents_and_queue_entries() {
    const REMOVE_COUNT: usize = 98;

    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let mut hashes = Vec::with_capacity(REMOVE_COUNT);

    for index in 0..REMOVE_COUNT {
        let magnet = format!("magnet:?xt=urn:btih:{:040x}&dn=remove-{index}", index + 1);
        let hash = runtime.add_magnet(&magnet, None).await.unwrap();
        hashes.push(hash);
    }
    hashes.push(InfoHash::from_hex("ffffffffffffffffffffffffffffffffffffffff").unwrap());

    let removed = runtime.remove_torrents(hashes, false).await.unwrap();

    assert_eq!(removed.len(), REMOVE_COUNT);
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
    assert!(runtime.engine_handles.read().await.is_empty());
}

#[tokio::test]
async fn bulk_remove_clears_ten_thousand_torrents_and_runtime_indexes() {
    const REMOVE_COUNT: usize = 10_000;

    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "managed placeholder",
        b"managed placeholder payload",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let hashes = (0..REMOVE_COUNT)
        .map(|idx| InfoHash::from_bytes(scale_hash_bytes(idx as u32)))
        .collect::<Vec<_>>();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            torrent.magnet_info_hash = Some(hash);
            reg.add(torrent).unwrap();
        }
    }
    runtime.queue.lock().await.add_many(hashes.iter().copied());
    runtime.rate_samples.write().await.insert(
        hashes[0],
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
    runtime
        .engine_retry_after
        .write()
        .await
        .insert(hashes[1], Instant::now() + ENGINE_INCOMPLETE_RETRY_DELAY);

    let removed = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.remove_torrents(hashes.clone(), false),
    )
    .await
    .expect("bulk remove should be bounded for 10,000 records")
    .unwrap();

    assert_eq!(removed.len(), REMOVE_COUNT);
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
    assert!(runtime.queue.lock().await.bypass.is_empty());
    assert!(runtime.rate_samples.read().await.is_empty());
    assert!(runtime.engine_retry_after.read().await.is_empty());
    assert!(runtime.engine_handles.read().await.is_empty());
}

#[tokio::test]
async fn paused_add_is_queued_without_reconcile_start() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "paused-add.bin",
        b"paused add payload",
        4,
        None,
        false,
    );

    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();

    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::Paused);
    assert_eq!(summary.queue_position, Some(1));
    assert!(runtime.desired_download_hashes().await.is_empty());
    assert!(runtime.engine_handles.read().await.is_empty());
    assert!(!runtime.queue_reconcile.lock().await.scheduled);
}

fn watch_test_config(
    root: &Path,
    start_behavior: swarmotter_core::config::StartBehavior,
) -> Config {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.watch = vec![swarmotter_core::config::WatchFolderConfig {
        path: root.display().to_string(),
        recursive: false,
        download_dir: None,
        label: None,
        profile: None,
        start_behavior,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: false,
    }];
    config
}

fn disabled_health() -> NetworkHealth {
    NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    )
}

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
    assert_eq!(history[0].outcome, watch::ImportOutcome::Duplicate);
    assert_eq!(
        history[0].info_hash_hex.as_deref(),
        Some(hash.to_hex().as_str())
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
    assert_eq!(history[0].outcome, watch::ImportOutcome::Imported);
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
    let hash = meta::parse_torrent(&bytes).unwrap().info_hash;
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
    let first = InfoHash::from_bytes([0x11; 20]);
    let last = InfoHash::from_bytes([0x22; 20]);
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
    assert_eq!(history[0].outcome, watch::ImportOutcome::TransientFailure);
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
    let hash = meta::parse_torrent(&bytes).unwrap().info_hash;
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
    assert_eq!(history[0].outcome, watch::ImportOutcome::PermanentFailure);
    assert_eq!(history[1].outcome, watch::ImportOutcome::TransientFailure);
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
    assert_eq!(history[2].outcome, watch::ImportOutcome::Imported);
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
    assert_eq!(history[0].outcome, watch::ImportOutcome::PermanentFailure);
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
    assert_eq!(history[0].outcome, watch::ImportOutcome::Imported);
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
    assert_eq!(key.root, watch::lexical_absolute(&archive).unwrap());
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
    for index in 0..=watch::MAX_IMPORT_HISTORY {
        runtime
            .record_watch_import(watch::ImportResult {
                path: format!("/watch/{index}.torrent"),
                success: false,
                info_hash_hex: None,
                error: Some("generated history entry".into()),
                duplicate: false,
                post_action_error: None,
                outcome: watch::ImportOutcome::TransientFailure,
            })
            .await;
    }
    let history = runtime.watch_history().await;
    assert_eq!(history.len(), watch::MAX_IMPORT_HISTORY);
    assert_eq!(history.first().unwrap().path, "/watch/1.torrent");
    assert_eq!(
        history.last().unwrap().path,
        format!("/watch/{}.torrent", watch::MAX_IMPORT_HISTORY)
    );
}

#[test]
fn health_input_uses_recent_peer_block_activity() {
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "health.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Downloading;

    let mut peer_health = HashMap::new();
    peer_health.insert(
        "127.0.0.1:6881".parse().unwrap(),
        EnginePeerHealth {
            has_missing_pieces: true,
            unchoked: true,
            useful_recently: true,
            last_valid_block: Some(Instant::now()),
            last_seen: Some(Instant::now()),
            ..Default::default()
        },
    );

    let input = build_health_input(
        &torrent,
        meta.piece_count(),
        &swarmotter_core::storage::resume::PieceBitfield::new(meta.piece_count()),
        &peer_health,
        &true,
        false,
        false,
        0,
        0,
        0,
        0,
        None,
        None,
        None,
        None,
        None,
        Some(Instant::now()),
        1,
        None,
        0,
        0,
        NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        ),
    );

    assert!(input.received_block_recently);
    let health = HealthCalculator::new().compute(&input);
    assert!(
        health.score > 25,
        "recent peer blocks should avoid the stalled health cap"
    );
}
