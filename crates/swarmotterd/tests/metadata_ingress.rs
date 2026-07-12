// SPDX-License-Identifier: Apache-2.0

//! Phase 1 production-ingress coverage for the shared metainfo byte limit.

#![allow(clippy::field_reassign_with_default)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use swarmotter_api::state::{AppState, BuildInfo, DaemonOps};
use swarmotter_core::config::{Config, StartBehavior, WatchFolderConfig};
use swarmotter_core::meta::{build_single_file_torrent, parse_torrent, MAX_TORRENT_METADATA_BYTES};
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::torrent::TorrentState;
use swarmotter_core::watch::ImportOutcome;
use swarmotterd::daemon::DaemonRuntime;
use tokio::sync::Mutex;
use tower::ServiceExt;

struct TestDir(PathBuf);

impl TestDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "swarmotter-metadata-ingress-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock must be after the Unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).expect("create isolated test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

fn torrent_padded_to_size(name: &str, target: usize) -> Vec<u8> {
    let mut bytes = build_single_file_torrent(
        name,
        b"generated lawful metadata-ingress payload",
        8,
        None,
        false,
    );
    assert_eq!(bytes.pop(), Some(b'e'));
    bytes.extend_from_slice(b"7:padding");

    let mut padding_len = target.saturating_sub(bytes.len() + 2);
    for _ in 0..32 {
        let encoded_len = bytes.len() + padding_len.to_string().len() + 1 + padding_len + 1;
        if encoded_len == target {
            bytes.extend_from_slice(padding_len.to_string().as_bytes());
            bytes.push(b':');
            bytes.extend(std::iter::repeat_n(b'x', padding_len));
            bytes.push(b'e');
            assert_eq!(bytes.len(), target);
            return bytes;
        }
        padding_len = target
            .checked_sub(bytes.len() + padding_len.to_string().len() + 2)
            .expect("target must accommodate generated metainfo");
    }
    panic!("could not generate metainfo at exact size {target}");
}

fn isolated_disabled_config(root: &Path) -> Config {
    let download = root.join("downloads");
    let incomplete = root.join("incomplete");
    std::fs::create_dir_all(&download).expect("create download directory");
    std::fs::create_dir_all(&incomplete).expect("create incomplete directory");

    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.dht.enabled = false;
    config.storage.download_dir = Some(download.display().to_string());
    config.storage.incomplete_dir = Some(incomplete.display().to_string());
    config.api.max_request_body_bytes = MAX_TORRENT_METADATA_BYTES + 1024;
    config
}

fn disabled_health() -> NetworkHealth {
    NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "containment explicitly disabled for isolated paused-ingress test",
    )
}

fn app_state(runtime: Arc<DaemonRuntime>, config: Config) -> swarmotter_api::state::SharedState {
    let daemon: Arc<dyn DaemonOps> = runtime;
    Arc::new(AppState {
        daemon,
        config: Arc::new(Mutex::new(config)),
        build: BuildInfo::default(),
        broker: swarmotter_api::handlers::events::EventBroker::default(),
        transmission: swarmotter_api::state::TransmissionCompatState::default(),
        qbittorrent: swarmotter_api::state::QbittorrentCompatState::default(),
    })
}

#[tokio::test]
async fn real_daemon_api_accepts_exact_metadata_limit_and_rejects_one_over() {
    let root = TestDir::new("api");
    let config = isolated_disabled_config(root.path());
    let runtime = Arc::new(DaemonRuntime::new(config.clone(), disabled_health()));
    let app = swarmotter_api::routes::app_router_with_body_limit(
        app_state(runtime.clone(), config.clone()),
        config.api.max_request_body_bytes,
    );
    let exact = torrent_padded_to_size("api-limit.bin", MAX_TORRENT_METADATA_BYTES);
    let expected_hash = parse_torrent(&exact)
        .expect("generated exact-limit metainfo must parse")
        .info_hash;

    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file?paused=true")
                .header("content-type", "application/x-bittorrent")
                .body(Body::from(exact.clone()))
                .expect("build exact-limit request"),
        )
        .await
        .expect("exact-limit API request must complete");
    assert_eq!(accepted.status(), StatusCode::OK);
    let body = axum::body::to_bytes(accepted.into_body(), usize::MAX)
        .await
        .expect("read exact-limit response body");
    let envelope: serde_json::Value =
        serde_json::from_slice(&body).expect("decode exact-limit response envelope");
    assert_eq!(envelope["success"], true);
    assert_eq!(envelope["data"], expected_hash.to_hex());
    let registered = runtime.list_torrents().await;
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].info_hash, expected_hash);
    assert_eq!(registered[0].state, TorrentState::Paused);

    let mut one_over = exact;
    one_over.push(b'X');
    assert_eq!(one_over.len(), MAX_TORRENT_METADATA_BYTES + 1);
    let rejected = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file?paused=true")
                .header("content-type", "application/x-bittorrent")
                .body(Body::from(one_over))
                .expect("build one-over request"),
        )
        .await
        .expect("one-over API request must complete");
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(rejected.into_body(), usize::MAX)
        .await
        .expect("read one-over response body");
    let envelope: serde_json::Value =
        serde_json::from_slice(&body).expect("decode one-over response envelope");
    assert_eq!(envelope["success"], false);
    assert_eq!(envelope["data"], serde_json::Value::Null);
    assert_eq!(envelope["error"]["code"], "malformed_torrent");
    assert!(envelope["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("exceeds maximum")));
    assert_eq!(runtime.list_torrents().await.len(), 1);
}

#[tokio::test]
async fn real_daemon_watch_accepts_exact_metadata_limit_and_rejects_one_over() {
    let root = TestDir::new("watch");
    let watch = root.path().join("watch");
    std::fs::create_dir_all(&watch).expect("create watch directory");
    let exact = torrent_padded_to_size("watch-limit.bin", MAX_TORRENT_METADATA_BYTES);
    let expected_hash = parse_torrent(&exact)
        .expect("generated exact-limit metainfo must parse")
        .info_hash;
    std::fs::write(watch.join("exact.torrent"), &exact).expect("write exact-limit watch metainfo");
    let oversized = std::fs::File::create(watch.join("one-over.torrent"))
        .expect("create one-over watch metainfo");
    oversized
        .set_len((MAX_TORRENT_METADATA_BYTES + 1) as u64)
        .expect("size one-over watch metainfo");
    drop(oversized);

    let mut config = isolated_disabled_config(root.path());
    config.watch = vec![WatchFolderConfig {
        path: watch.display().to_string(),
        recursive: false,
        download_dir: Some(root.path().join("downloads").display().to_string()),
        label: Some("phase-1-limit".into()),
        start_behavior: StartBehavior::Paused,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: false,
    }];
    let runtime = Arc::new(DaemonRuntime::new(config, disabled_health()));

    runtime
        .watch_scan()
        .await
        .expect("first watch observation must complete");
    assert!(runtime.list_torrents().await.is_empty());
    assert!(runtime.watch_history().await.is_empty());

    runtime
        .watch_scan()
        .await
        .expect("second watch observation must process stable files");
    let registered = runtime.list_torrents().await;
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].info_hash, expected_hash);
    assert_eq!(registered[0].state, TorrentState::Paused);
    assert_eq!(registered[0].labels, vec!["phase-1-limit"]);

    let history = runtime.watch_history().await;
    assert_eq!(history.len(), 2);
    let imported = history
        .iter()
        .find(|result| result.path.ends_with("exact.torrent"))
        .expect("exact-limit terminal watch result");
    assert!(imported.success);
    assert_eq!(imported.outcome, ImportOutcome::Imported);
    assert_eq!(
        imported.info_hash_hex.as_deref(),
        Some(expected_hash.to_hex().as_str())
    );
    assert!(imported.error.is_none());

    let rejected = history
        .iter()
        .find(|result| result.path.ends_with("one-over.torrent"))
        .expect("one-over terminal watch result");
    assert!(!rejected.success);
    assert_eq!(rejected.outcome, ImportOutcome::PermanentFailure);
    assert!(rejected.info_hash_hex.is_none());
    assert!(rejected
        .error
        .as_deref()
        .is_some_and(|error| error.contains("exceeds maximum")));
}
