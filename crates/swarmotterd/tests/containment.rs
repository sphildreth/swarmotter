// SPDX-License-Identifier: Apache-2.0

//! Daemon integration tests: network containment fail-closed behavior and
//! watch-folder import exercised against the real `DaemonRuntime`.
//!
//! These tests use the `DaemonRuntime` directly (not the HTTP server) to
//! validate that strict containment blocks torrent activity and surfaces
//! `network_blocked` state.

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use swarmotter_api::state::DaemonOps;
use swarmotter_core::config::Config;
use swarmotter_core::meta::build_single_file_torrent;
use swarmotter_core::models::network::{
    NetworkContainmentMode as Mode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::torrent::TorrentState;
use swarmotter_core::net::NetworkConfig;
use swarmotterd::daemon::DaemonRuntime;

fn strict_missing_interface_health() -> NetworkHealth {
    NetworkHealth::blocked(
        Mode::Strict,
        NetworkContainmentStatus::InterfaceMissing,
        "required torrent network interface tun0 is not available",
    )
}

fn strict_config() -> Config {
    let mut cfg = Config::default();
    cfg.network = NetworkConfig {
        mode: Mode::Strict,
        required_interface: Some("tun0".into()),
        required_source_ipv4: Some("10.8.0.2".into()),
        required_source_ipv6: None,
        required_network_namespace: None,
        allow_ipv6: false,
        fail_closed: true,
        validate_route: false,
        validate_dns: false,
    };
    cfg
}

#[tokio::test]
async fn torrent_added_under_strict_missing_interface_is_network_blocked() {
    let runtime = Arc::new(DaemonRuntime::new(
        strict_config(),
        strict_missing_interface_health(),
    ));
    let bytes = build_single_file_torrent("f", b"data payload bytes", 8, None, false);
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::NetworkBlocked);
}

#[tokio::test]
async fn network_health_reported_strict_blocked() {
    let runtime = Arc::new(DaemonRuntime::new(
        strict_config(),
        strict_missing_interface_health(),
    ));
    let h = runtime.network_health().await;
    assert_eq!(h.status, NetworkContainmentStatus::InterfaceMissing);
    assert!(!h.traffic_allowed);
}

#[tokio::test]
async fn torrent_allowed_when_disabled() {
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    // This test asserts the torrent is not network-blocked under disabled
    // containment. It must not depend on third-party DHT bootstrap nodes or
    // real network access, so disable DHT; the engine still starts and, with
    // no trackers/peers, terminates after bounded no-peer retries.
    cfg.dht.enabled = false;
    let healthy = NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = Arc::new(DaemonRuntime::new(cfg, healthy));
    let bytes = build_single_file_torrent("f", b"allowed payload bytes", 8, None, false);
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_ne!(summary.state, TorrentState::NetworkBlocked);
}

#[tokio::test]
async fn watch_folder_imports_torrent() {
    use swarmotter_core::config::{StartBehavior, WatchFolderConfig};
    use swarmotter_core::meta::build_single_file_torrent;
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    let healthy = NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    // Create a temp watch folder with a .torrent file.
    let dir = std::env::temp_dir().join(format!(
        "swarmotter-watch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let bytes = build_single_file_torrent("watched", b"watched payload data here", 8, None, false);
    std::fs::write(dir.join("sample.torrent"), &bytes).unwrap();

    cfg.watch = vec![WatchFolderConfig {
        path: dir.display().to_string(),
        recursive: false,
        download_dir: None,
        label: Some("watched".into()),
        profile: None,
        start_behavior: StartBehavior::Paused,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: true,
    }];

    let runtime = Arc::new(DaemonRuntime::new(cfg, healthy));
    runtime.watch_scan().await.unwrap();
    assert!(runtime.list_torrents().await.is_empty());
    assert!(runtime.watch_history().await.is_empty());
    runtime.watch_scan().await.unwrap();

    let list = runtime.list_torrents().await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "watched");
    assert_eq!(list[0].state, TorrentState::Paused);
    assert_eq!(list[0].queue_position, Some(1));
    assert!(list[0].labels.contains(&"watched".to_string()));

    // The source file should have been deleted after import.
    assert!(!dir.join("sample.torrent").exists());

    // Import history recorded.
    let hist = runtime.watch_history().await;
    assert_eq!(hist.len(), 1);
    assert!(hist[0].success);
    assert_eq!(
        hist[0].outcome,
        swarmotter_core::watch::ImportOutcome::Imported
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn watch_folder_start_import_is_queued_for_scheduler() {
    use swarmotter_core::config::{StartBehavior, WatchFolderConfig};
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    let healthy = NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let dir = std::env::temp_dir().join(format!(
        "swarmotter-watch-start-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let bytes = build_single_file_torrent(
        "watched-start",
        b"watched start payload data here",
        8,
        None,
        false,
    );
    std::fs::write(dir.join("start.torrent"), &bytes).unwrap();

    cfg.watch = vec![WatchFolderConfig {
        path: dir.display().to_string(),
        recursive: false,
        download_dir: None,
        label: Some("watched".into()),
        profile: None,
        start_behavior: StartBehavior::Start,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: true,
    }];

    let runtime = Arc::new(DaemonRuntime::new(cfg, healthy));
    runtime.watch_scan().await.unwrap();
    assert!(runtime.list_torrents().await.is_empty());
    runtime.watch_scan().await.unwrap();

    let list = runtime.list_torrents().await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "watched-start");
    assert_eq!(list[0].state, TorrentState::Queued);
    assert_eq!(list[0].queue_position, Some(1));

    let stats = runtime.global_stats().await;
    assert_eq!(stats.scheduler.queued_torrents, 1);
    assert_eq!(stats.scheduler.requested_downloads, 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn watch_folder_moves_failed_import_to_failure_dir() {
    use swarmotter_core::config::{StartBehavior, WatchFolderConfig};
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    let healthy = NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let dir = std::env::temp_dir().join(format!(
        "swarmotter-watch-fail-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let failure_dir = dir.join("failed");
    std::fs::create_dir_all(&dir).unwrap();
    let bad_file = dir.join("bad.torrent");
    std::fs::write(&bad_file, b"not bencoded torrent data").unwrap();

    cfg.watch = vec![WatchFolderConfig {
        path: dir.display().to_string(),
        recursive: false,
        download_dir: None,
        label: None,
        profile: None,
        start_behavior: StartBehavior::Start,
        archive_dir: None,
        failure_dir: Some(failure_dir.display().to_string()),
        delete_after_import: true,
    }];

    let runtime = Arc::new(DaemonRuntime::new(cfg, healthy));
    runtime.watch_scan().await.unwrap();
    assert!(runtime.watch_history().await.is_empty());
    runtime.watch_scan().await.unwrap();

    assert!(!bad_file.exists());
    assert!(failure_dir.join("bad.torrent").exists());
    let hist = runtime.watch_history().await;
    assert_eq!(hist.len(), 1);
    assert!(!hist[0].success);
    assert_eq!(
        hist[0].outcome,
        swarmotter_core::watch::ImportOutcome::PermanentFailure
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn watch_folder_rejects_oversized_metadata_file() {
    use swarmotter_core::config::{StartBehavior, WatchFolderConfig};
    use swarmotter_core::meta::MAX_TORRENT_METADATA_BYTES;
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    let healthy = NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let dir = std::env::temp_dir().join(format!(
        "swarmotter-watch-oversize-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let big_file = dir.join("oversize.torrent");
    // Write a file one byte over the metadata limit. The bounded read rejects
    // it before parsing and before any allocation sized to the attacker input.
    std::fs::write(&big_file, vec![b'x'; MAX_TORRENT_METADATA_BYTES + 1]).unwrap();

    cfg.watch = vec![WatchFolderConfig {
        path: dir.display().to_string(),
        recursive: false,
        download_dir: None,
        label: None,
        profile: None,
        start_behavior: StartBehavior::Start,
        archive_dir: None,
        failure_dir: Some(dir.join("failed").display().to_string()),
        delete_after_import: false,
    }];

    let runtime = Arc::new(DaemonRuntime::new(cfg, healthy));
    runtime.watch_scan().await.unwrap();
    assert!(runtime.watch_history().await.is_empty());
    runtime.watch_scan().await.unwrap();

    // No torrent should have been imported.
    let list = runtime.list_torrents().await;
    assert!(list.is_empty(), "oversized metadata must not be imported");

    // Import history records the failure.
    let hist = runtime.watch_history().await;
    assert_eq!(hist.len(), 1);
    assert!(!hist[0].success);
    assert!(
        hist[0]
            .error
            .as_deref()
            .is_some_and(|e| e.contains("exceeds maximum")),
        "expected size-limit error, got: {:?}",
        hist[0].error
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn api_add_rejects_oversized_metadata_bytes() {
    use swarmotter_core::meta::MAX_TORRENT_METADATA_BYTES;
    let cfg = Config::default();
    let healthy = NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = Arc::new(DaemonRuntime::new(cfg, healthy));
    // Feed bytes one byte over the metadata limit directly through the same
    // production add path used by the HTTP API. The bencode byte limit inside
    // parse_torrent rejects it before any piece-sized allocation.
    let oversize = vec![b'd'; MAX_TORRENT_METADATA_BYTES + 1];
    let err = runtime.add_torrent_file(oversize, None).await.unwrap_err();
    assert!(err.to_string().contains("exceeds maximum"));
    assert!(runtime.list_torrents().await.is_empty());
}
