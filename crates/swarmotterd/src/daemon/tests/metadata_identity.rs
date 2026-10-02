// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn pure_v2_inputs_register_with_their_full_v2_identity() {
    let runtime = DaemonRuntime::new(Config::default(), disabled_health());
    let torrent = pure_v2_single_file_fixture();
    let parsed = swarmotter_core::meta::parse_torrent(&torrent).unwrap();
    assert!(parsed.requires_v2_data_plane());
    let expected_file_key = TorrentKey::v2(parsed.identity.v2_info_hash().unwrap());

    let file_key = runtime
        .add_torrent_file_with_options(torrent, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    assert_eq!(file_key, expected_file_key);
    assert_eq!(
        runtime.get_torrent(&file_key).await.unwrap().info_hash,
        expected_file_key
    );

    let v2_magnet = format!("magnet:?xt=urn:btmh:1220{}", "a".repeat(64));
    let expected_magnet_key = TorrentKey::v2(
        swarmotter_core::magnet::Magnet::parse(&v2_magnet)
            .unwrap()
            .v2_info_hash()
            .unwrap(),
    );
    let magnet_key = runtime
        .add_magnet_with_options(&v2_magnet, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    assert_eq!(magnet_key, expected_magnet_key);
    assert_eq!(
        runtime.get_torrent(&magnet_key).await.unwrap().info_hash,
        expected_magnet_key
    );
    assert_eq!(runtime.registry.lock().await.torrents.len(), 2);
    assert!(runtime.engine_handles.read().await.is_empty());
}

#[tokio::test]
async fn hybrid_metainfo_preserves_v1_and_v2_identity_through_registration_and_restore() {
    let root = unique_dir("hybrid-identity-state");
    let state_path = root.join("state.sqlite");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.join("payload").display().to_string());
    let mut health = disabled_health();
    health.traffic_allowed = true;

    let bytes = hybrid_v1_compatible_fixture();
    let expected = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    assert!(expected.identity.supports_v1_data_plane());
    assert!(matches!(
        expected.identity,
        swarmotter_core::hash::TorrentIdentity::Hybrid { .. }
    ));
    let expected_identity = expected.identity.clone();
    let expected_hash = TorrentKey::v1(expected.info_hash);
    let v2_alias = TorrentKey::v2(expected_identity.v2_info_hash().unwrap());

    let runtime = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health.clone(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    assert_eq!(hash, expected_hash);
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.identity, expected_identity);
    assert_eq!(summary.identity.v1_info_hash(), hash.as_v1());
    assert!(summary.identity.v2_info_hash().is_some());
    assert_eq!(
        runtime.get_torrent(&v2_alias).await.unwrap().info_hash,
        expected_hash,
        "a hybrid v2 locator must resolve to its canonical v1 owner"
    );
    runtime
        .set_labels(&v2_alias, vec!["hybrid-alias".into()])
        .await
        .unwrap();
    assert_eq!(
        runtime.get_torrent(&hash).await.unwrap().labels,
        vec!["hybrid-alias"]
    );
    drop(runtime);

    let restored = DaemonRuntime::with_paths_broker_and_state(
        config,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 1);
    let summary = restored.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.identity, expected_identity);
    assert_eq!(summary.identity.v1_info_hash(), hash.as_v1());
    assert!(summary.identity.v2_info_hash().is_some());
    assert_eq!(
        restored.get_torrent(&v2_alias).await.unwrap().labels,
        vec!["hybrid-alias"]
    );
    restored.remove_torrent(&v2_alias, false).await.unwrap();
    assert!(restored.get_torrent(&hash).await.is_none());
    drop(restored);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn hybrid_magnet_keeps_claimed_identity_separate_from_placeholder_metainfo() {
    let root = unique_dir("hybrid-magnet-identity-state");
    let state_path = root.join("state.sqlite");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.join("payload").display().to_string());
    let mut health = disabled_health();
    health.traffic_allowed = true;
    let magnet_uri = format!(
        "magnet:?xt=urn:btih:{}&xt=urn:btmh:1220{}&dn=hybrid-magnet.bin&so=0&x.pe=192.0.2.25:51413",
        "1".repeat(40),
        "2".repeat(64),
    );
    let expected = swarmotter_core::magnet::Magnet::parse(&magnet_uri).unwrap();
    let hash = TorrentKey::v1(expected.v1_info_hash().unwrap());
    let expected_identity = expected.identity.clone();

    let runtime = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health.clone(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    assert_eq!(
        runtime
            .add_magnet_with_options(&magnet_uri, AddTorrentOptions::new(None, true))
            .await
            .unwrap(),
        hash
    );
    let torrent = runtime.registry.lock().await.get(&hash).cloned().unwrap();
    assert!(torrent.needs_metadata);
    assert_eq!(torrent.magnet_identity, Some(expected_identity.clone()));
    assert_eq!(torrent.magnet_select_only_file_indices, vec![0]);
    assert_eq!(torrent.magnet_direct_peers, expected.direct_peers);
    assert_eq!(torrent.key(), hash);
    assert_ne!(torrent.meta.identity, expected_identity);
    assert_eq!(
        InfoHash::from_info_bencoded(torrent.meta.raw_info.as_deref().unwrap()),
        torrent.meta.info_hash
    );
    assert_eq!(
        runtime.get_torrent(&hash).await.unwrap().identity,
        expected_identity
    );
    drop(runtime);

    let restored = DaemonRuntime::with_paths_broker_and_state(
        config,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 1);
    let torrent = restored.registry.lock().await.get(&hash).cloned().unwrap();
    assert_eq!(torrent.magnet_identity, Some(expected_identity.clone()));
    assert_eq!(torrent.magnet_select_only_file_indices, vec![0]);
    assert_eq!(torrent.magnet_direct_peers, expected.direct_peers);
    assert_eq!(
        InfoHash::from_info_bencoded(torrent.meta.raw_info.as_deref().unwrap()),
        torrent.meta.info_hash
    );
    assert_eq!(
        restored.get_torrent(&hash).await.unwrap().identity,
        expected_identity
    );
    drop(restored);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn retained_original_metainfo_is_exact_for_file_adds_and_unavailable_for_magnets() {
    let root = unique_dir("retained-original-metainfo");
    let state_path = root.join("state.sqlite");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.join("payload").display().to_string());
    let mut health = disabled_health();
    health.traffic_allowed = true;
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "original-export.bin",
        b"generated original metainfo export fixture",
        8,
        None,
        false,
    );
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health.clone(),
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    );
    let file_hash = runtime
        .add_torrent_file_with_options(bytes.clone(), AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    assert_eq!(
        runtime.retained_original_metainfo(file_hash).await.unwrap(),
        bytes
    );

    let magnet_hash = "3".repeat(40);
    let magnet = format!("magnet:?xt=urn:btih:{magnet_hash}&dn=magnet-only.bin");
    let magnet_hash = runtime
        .add_magnet_with_options(&magnet, AddTorrentOptions::new(None, true))
        .await
        .unwrap();
    let error = runtime
        .retained_original_metainfo(magnet_hash)
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::NotFound(_)));
    assert!(error.to_string().contains("unavailable"));
    drop(runtime);

    let restored = DaemonRuntime::with_paths_broker_and_state(
        config,
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    );
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 2);
    assert_eq!(
        restored
            .retained_original_metainfo(file_hash)
            .await
            .unwrap(),
        bytes
    );
    drop(restored);
    std::fs::remove_dir_all(root).ok();
}
