// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use serde_json::Value;
use swarmotter_api::handlers::events::EventBroker;
use swarmotter_api::state::{AppState, BuildInfo, DaemonOps, SharedState};
use swarmotter_core::config::Config;
use swarmotter_core::meta::build_single_file_torrent;
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotterd::daemon::DaemonRuntime;

fn unique_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "swarmotterctl-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn state(runtime: Arc<DaemonRuntime>, config: Config) -> SharedState {
    let daemon: Arc<dyn DaemonOps> = runtime;
    Arc::new(AppState {
        daemon,
        config: Arc::new(tokio::sync::Mutex::new(config)),
        build: BuildInfo::default(),
        broker: EventBroker::default(),
        transmission: swarmotter_api::state::TransmissionCompatState::default(),
        qbittorrent: swarmotter_api::state::QbittorrentCompatState::default(),
    })
}

async fn run_cli(url: String, token: Option<String>, args: Vec<String>) -> std::process::Output {
    tokio::task::spawn_blocking(move || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmotterctl"));
        command.env("SWARMOTTER_API_URL", url).args(args);
        if let Some(token) = token {
            command.env("SWARMOTTER_API_TOKEN", token);
        } else {
            command.env_remove("SWARMOTTER_API_TOKEN");
            command.env_remove("SWARMOTTER_API_TOKEN_FILE");
        }
        command.output().expect("run swarmotterctl")
    })
    .await
    .unwrap()
}

fn json_output(output: &std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("valid CLI JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_daemon_add_inspect_operate_and_remove_in_both_output_modes() {
    let root = unique_dir("e2e");
    let state_path = root.join("state.json");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.storage.download_dir = Some(root.join("downloads").display().to_string());
    config.storage.incomplete_dir = Some(root.join("incomplete").display().to_string());
    config.api.require_auth = true;
    config.api.auth_token = Some("test-token".into());
    std::fs::create_dir_all(root.join("downloads")).unwrap();
    std::fs::create_dir_all(root.join("incomplete")).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled for tests",
    );
    health.traffic_allowed = true;
    let runtime = Arc::new(DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health,
        None,
        None,
        Some(state_path),
        EventBroker::default(),
    ));
    runtime.restore_persisted_state().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let url = format!("http://{address}");
    let server = tokio::spawn(async move {
        axum::serve(listener, swarmotter_api::app_router(state(runtime, config)))
            .await
            .unwrap();
    });

    let torrent_path = root.join("fixture.torrent");
    let content = b"swarmotterctl integration fixture payload";
    let bytes = build_single_file_torrent("fixture.bin", content, 16, None, false);
    std::fs::write(&torrent_path, &bytes).unwrap();

    let added = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec![
                "--json".into(),
                "add".into(),
                "file".into(),
                torrent_path.display().to_string(),
                "--paused".into(),
            ],
        )
        .await,
    );
    assert_eq!(added["format_version"], 1);
    assert_eq!(added["success"], true);
    let hash = added["data"]["info_hash"].as_str().unwrap().to_string();
    assert_eq!(hash.len(), 40);

    let magnet_hash = "0123456789abcdef0123456789abcdef01234567";
    let added_magnet = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec![
                "--json".into(),
                "add".into(),
                "magnet".into(),
                format!("magnet:?xt=urn:btih:{magnet_hash}&dn=cli-magnet"),
                "--paused".into(),
            ],
        )
        .await,
    );
    assert_eq!(added_magnet["data"]["info_hash"], magnet_hash);

    let listed = run_cli(url.clone(), Some("test-token".into()), vec!["list".into()]).await;
    assert!(listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stdout).contains("fixture.bin"));

    let paged = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec![
                "--json".into(),
                "list".into(),
                "--per-page".into(),
                "1".into(),
            ],
        )
        .await,
    );
    assert_eq!(paged["data"]["page_count"], 2);
    assert_eq!(paged["data"]["rows"].as_array().unwrap().len(), 1);
    let all_pages = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec![
                "--json".into(),
                "list".into(),
                "--per-page".into(),
                "1".into(),
                "--all".into(),
            ],
        )
        .await,
    );
    assert_eq!(all_pages["data"]["rows"].as_array().unwrap().len(), 2);

    let shown = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec!["--json".into(), "show".into(), hash.clone()],
        )
        .await,
    );
    assert_eq!(shown["data"]["name"], "fixture.bin");
    assert_eq!(shown["data"]["state"], "paused");

    let paused = run_cli(
        url.clone(),
        Some("test-token".into()),
        vec!["pause".into(), hash.clone()],
    )
    .await;
    assert!(paused.status.success());
    let resumed = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec!["--json".into(), "resume".into(), hash.clone()],
        )
        .await,
    );
    assert_eq!(resumed["data"]["action"], "resume");
    let stopped = run_cli(
        url.clone(),
        Some("test-token".into()),
        vec!["stop".into(), hash.clone()],
    )
    .await;
    assert!(stopped.status.success());
    for action in ["recheck", "reannounce"] {
        let output = run_cli(
            url.clone(),
            Some("test-token".into()),
            vec![action.into(), hash.clone()],
        )
        .await;
        assert!(
            output.status.success(),
            "{action} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let limits = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec![
                "--json".into(),
                "limits".into(),
                hash.clone(),
                "--download".into(),
                "1234".into(),
                "--upload".into(),
                "5678".into(),
            ],
        )
        .await,
    );
    assert_eq!(limits["data"]["download_limit"], 1234);
    let health = run_cli(
        url.clone(),
        Some("test-token".into()),
        vec!["health".into(), hash.clone()],
    )
    .await;
    assert!(health.status.success());

    let removed = json_output(
        &run_cli(
            url.clone(),
            Some("test-token".into()),
            vec!["--json".into(), "remove".into(), hash.clone()],
        )
        .await,
    );
    assert_eq!(removed["data"]["info_hash"], hash);
    let missing = run_cli(
        url.clone(),
        Some("test-token".into()),
        vec!["show".into(), hash],
    )
    .await;
    assert_eq!(missing.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("not found"));
    let removed_magnet = run_cli(
        url.clone(),
        Some("test-token".into()),
        vec!["remove".into(), magnet_hash.into()],
    )
    .await;
    assert!(removed_magnet.status.success());

    server.abort();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authentication_and_connection_failures_have_distinct_exit_codes() {
    let root = unique_dir("failures");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.storage.download_dir = Some(root.join("downloads").display().to_string());
    config.api.require_auth = true;
    config.api.auth_token = Some("right-token".into());
    std::fs::create_dir_all(root.join("downloads")).unwrap();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled for tests",
    );
    let runtime = Arc::new(DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health,
        None,
        None,
        Some(root.join("state.json")),
        EventBroker::default(),
    ));
    runtime.restore_persisted_state().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let url = format!("http://{address}");
    let server = tokio::spawn(async move {
        axum::serve(listener, swarmotter_api::app_router(state(runtime, config)))
            .await
            .unwrap();
    });
    let auth = run_cli(url.clone(), Some("wrong-token".into()), vec!["list".into()]).await;
    assert_eq!(auth.status.code(), Some(3));
    let json_auth_output = run_cli(
        url,
        Some("wrong-token".into()),
        vec!["--json".into(), "list".into()],
    )
    .await;
    let json_auth: Value =
        serde_json::from_slice(&json_auth_output.stdout).expect("valid error JSON");
    assert_eq!(json_auth["error"]["code"], "authentication_failed");
    server.abort();
    let connection = run_cli("http://127.0.0.1:1".into(), None, vec!["list".into()]).await;
    assert_eq!(connection.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&connection.stderr).contains("connection failed"));
    std::fs::remove_dir_all(root).ok();
}
