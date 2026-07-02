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
        start_behavior: StartBehavior::Paused,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: true,
    }];

    let runtime = Arc::new(DaemonRuntime::new(cfg, healthy));
    runtime.watch_scan().await.unwrap();

    let list = runtime.list_torrents().await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "watched");
    assert_eq!(list[0].state, TorrentState::Paused);
    assert!(list[0].labels.contains(&"watched".to_string()));

    // The source file should have been deleted after import.
    assert!(!dir.join("sample.torrent").exists());

    // Import history recorded.
    let hist = runtime.watch_history().await;
    assert_eq!(hist.len(), 1);
    assert!(hist[0].success);

    std::fs::remove_dir_all(&dir).ok();
}
