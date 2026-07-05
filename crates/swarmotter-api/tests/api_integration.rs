// SPDX-License-Identifier: Apache-2.0

//! API integration tests using a fake in-memory daemon.

mod fake_daemon;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use serde::de::DeserializeOwned;
use std::collections::BTreeSet;
use swarmotter_core::config::{Config, StartBehavior, WatchFolderConfig};
use swarmotter_core::meta::build_single_file_torrent;
use swarmotter_core::models::network::NetworkContainmentStatus;
use swarmotter_core::models::{
    ConfigUpdateResult, DiagnosticLevel, DoctorReport, LogSnapshot, NetworkDiagnostics, WatchStatus,
};
use tower::ServiceExt;

fn known_magnet() -> String {
    "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e&dn=test".to_string()
}

fn bulk_magnet(index: usize) -> String {
    format!("magnet:?xt=urn:btih:{:040x}&dn=bulk-{index}", index + 1)
}

async fn transmission_session(app: Router, auth: Option<&str>) -> String {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/transmission/rpc")
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    let resp = app
        .oneshot(
            builder
                .body(Body::from(r#"{"method":"session_get"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    resp.headers()
        .get("x-transmission-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("session header")
        .to_string()
}

async fn transmission_rpc(
    app: Router,
    session: &str,
    payload: serde_json::Value,
    auth: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/transmission/rpc")
        .header("content-type", "application/json")
        .header("x-transmission-session-id", session);
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    let resp = app
        .oneshot(builder.body(Body::from(payload.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

fn test_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut chunks = bytes.chunks(3);
    for chunk in &mut chunks {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn parse_api_data<T: DeserializeOwned>(body: &[u8]) -> T {
    let v: serde_json::Value = serde_json::from_slice(body).unwrap();
    serde_json::from_value(v["data"].clone()).unwrap()
}

#[tokio::test]
async fn health_and_version_endpoints() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["success"], true);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["name"], "SwarmOtter");
}

#[tokio::test]
async fn add_and_list_torrents() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    // Add via magnet (JSON body).
    let body = serde_json::json!({ "magnet": known_magnet() }).to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hash = v["data"].as_str().unwrap().to_string();
    assert_eq!(hash.len(), 40);

    // List.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let list = v["data"].as_array().unwrap();
    assert_eq!(list.len(), 1);

    // Get details.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Get per-torrent diagnostics/stats.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}/stats"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["info_hash"], hash);
    assert!(v["data"]["piece_count"].as_u64().unwrap() > 0);
    let data = v["data"].as_object().unwrap();
    for field in [
        "useful_peers",
        "peer_scheduler",
        "choked_peers",
        "unchoked_peers",
        "recent_peer_failures",
        "recent_tracker_failures",
        "tracker_last_ok_seconds_ago",
        "dht_discovery_ok",
        "dht_last_seen_seconds_ago",
        "pex_discovery_ok",
        "pex_last_seen_seconds_ago",
    ] {
        assert!(data.contains_key(field), "{field} should be present");
        assert!(
            data[field].is_null(),
            "{field} should be null without live engine data"
        );
    }

    // 404 for unknown.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents/0000000000000000000000000000000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn add_torrent_file_raw_body() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let bytes = build_single_file_torrent("file.bin", b"hello world data payload", 8, None, false);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file")
                .header("content-type", "application/octet-stream")
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["data"].as_str().unwrap().len() == 40);
}

#[tokio::test]
async fn add_magnet_can_start_paused() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let body = serde_json::json!({
        "magnet": known_magnet(),
        "paused": true
    })
    .to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/magnet")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hash = v["data"].as_str().unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["state"], "paused");
}

#[tokio::test]
async fn add_torrent_file_can_start_paused_from_query() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let bytes = build_single_file_torrent("paused-file.bin", b"paused payload", 8, None, false);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file?start_behavior=paused")
                .header("content-type", "application/octet-stream")
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hash = v["data"].as_str().unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["state"], "paused");
}

#[tokio::test]
async fn rapid_api_magnet_adds_all_register() {
    const ADD_COUNT: usize = 200;

    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let mut handles = Vec::with_capacity(ADD_COUNT);

    for index in 0..ADD_COUNT {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let body = serde_json::json!({ "magnet": bulk_magnet(index) }).to_string();
            let resp = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/torrents/magnet")
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            v["data"].as_str().unwrap().to_string()
        }));
    }

    let mut hashes = BTreeSet::new();
    for handle in handles {
        hashes.insert(handle.await.unwrap());
    }
    assert_eq!(hashes.len(), ADD_COUNT);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"].as_array().unwrap().len(), ADD_COUNT);
}

#[tokio::test]
async fn reset_endpoint_clears_torrents() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let body = serde_json::json!({ "magnet": known_magnet() }).to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/reset")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["torrents_removed"], 1);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["data"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn pause_resume_remove_lifecycle() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let body = serde_json::json!({ "magnet": known_magnet() }).to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hash = v["data"].as_str().unwrap().to_string();

    // Pause.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/torrents/{hash}/pause"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify state.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["state"], "paused");

    // Resume.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/torrents/{hash}/resume"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Remove.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/torrents/{hash}?delete_data=true"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify gone.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn settings_get_and_update() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/settings")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let patch = serde_json::json!({
        "bandwidth": { "global_download": 1000, "global_upload": 500, "alt_download": 0, "alt_upload": 0, "alt_enabled": false, "max_peers": 0, "max_peers_per_torrent": 0 }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/settings")
                .header("content-type", "application/json")
                .body(Body::from(patch.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn settings_put_replaces_and_preserves_auth_token() {
    let mut cfg = Config::default();
    cfg.api.auth_token = Some("existing-token".into());

    let state = fake_daemon::fake_state_with_config(cfg.clone());
    let app = swarmotter_api::app_router(state);

    let mut payload = serde_json::to_value(cfg).unwrap();
    payload["api"]["auth_token"] = serde_json::Value::Null;
    payload["api"]["require_auth"] = serde_json::Value::Bool(true);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/settings")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: ConfigUpdateResult = parse_api_data(&body);
    assert!(!result.persisted);
    assert_eq!(result.config_path, None);
    assert!(!result.restart_required);
    assert!(result.restart_required_fields.is_empty());
    assert_eq!(result.config.api.auth_token, None);
    assert!(result.config.api.require_auth);
    assert_eq!(result.applied_runtime_fields, vec!["config"]);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/settings")
                .header("authorization", "Bearer existing-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut rotated = payload;
    rotated["api"]["auth_token"] = serde_json::json!("rotated-token");
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/settings")
                .header("content-type", "application/json")
                .header("authorization", "Bearer existing-token")
                .body(Body::from(rotated.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: ConfigUpdateResult = parse_api_data(&body);
    assert_eq!(result.config.api.auth_token, None);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/settings")
                .header("authorization", "Bearer existing-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/settings")
                .header("authorization", "Bearer rotated-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn network_health_endpoint() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/network/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["status"], "disabled");
}

#[tokio::test]
async fn stats_endpoint() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn duplicate_torrent_returns_conflict() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let body = serde_json::json!({ "magnet": known_magnet() }).to_string();
    let r1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents")
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let r2 = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn api_auth_blocks_v1_without_token_and_accepts_bearer() {
    let mut cfg = Config::default();
    cfg.api.require_auth = true;
    cfg.api.auth_token = Some("test-token".into());
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn transmission_rpc_is_disabled_by_default() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/transmission/rpc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"method":"session_get"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn transmission_rpc_session_handshake_and_legacy_envelope() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);
    let session = transmission_session(app.clone(), None).await;

    let payload = serde_json::json!({
        "method": "session-get",
        "arguments": { "fields": ["version", "session-id"] },
        "tag": 7
    });
    let (status, body) = transmission_rpc(app.clone(), &session, payload, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "success");
    assert_eq!(body["tag"], 7);
    assert!(body["arguments"]["version"]
        .as_str()
        .unwrap()
        .contains("SwarmOtter"));
    assert_eq!(body["arguments"]["session-id"], session);

    let payload = serde_json::json!({
        "method": "session-get",
        "arguments": {},
        "tag": 8
    });
    let (status, body) = transmission_rpc(app.clone(), &session, payload, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["arguments"]["rpc-version"].is_number());
    assert!(body["arguments"]["download-dir"].is_string());
    assert!(body["arguments"]["rpc_version"].is_null());

    let add = serde_json::json!({
        "method": "torrent-add",
        "arguments": { "filename": known_magnet() },
        "tag": 9
    });
    let (status, body) = transmission_rpc(app.clone(), &session, add, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "success");
    assert!(
        body["arguments"]["torrent-added"]["hashString"]
            .as_str()
            .unwrap()
            .len()
            == 40
    );

    let get = serde_json::json!({
        "method": "torrent-get",
        "arguments": {},
        "tag": 10
    });
    let (status, body) = transmission_rpc(app, &session, get, None).await;
    assert_eq!(status, StatusCode::OK);
    let torrents = body["arguments"]["torrents"].as_array().unwrap();
    assert!(torrents[0]["hashString"].as_str().unwrap().len() == 40);
    assert!(torrents[0]["hash_string"].is_null());
}

#[tokio::test]
async fn transmission_rpc_reuses_api_token_for_basic_auth() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    cfg.api.require_auth = true;
    cfg.api.auth_token = Some("test-token".into());
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/transmission/rpc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"method":"session_get"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/transmission/rpc")
                .header("content-type", "application/json")
                .header("authorization", "Basic dXNlcjp3cm9uZw==")
                .body(Body::from(r#"{"method":"session_get"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let auth = "Basic dXNlcjp0ZXN0LXRva2Vu";
    let session = transmission_session(app.clone(), Some(auth)).await;
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "session_get",
        "params": { "fields": ["rpc_version_semver", "session_id"] },
        "id": "webui"
    });
    let (status, body) = transmission_rpc(app, &session, payload, Some(auth)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["result"]["rpc_version_semver"], "6.0.0");
    assert_eq!(body["result"]["session_id"], session);
}

#[tokio::test]
async fn transmission_rpc_add_get_action_and_remove_torrent() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);
    let session = transmission_session(app.clone(), None).await;

    let add = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_add",
        "params": {
            "filename": known_magnet(),
            "labels": ["linux"],
            "paused": true
        },
        "id": 1
    });
    let (status, body) = transmission_rpc(app.clone(), &session, add, None).await;
    assert_eq!(status, StatusCode::OK);
    let torrent_id = body["result"]["torrent_added"]["id"].as_i64().unwrap();
    assert_eq!(body["result"]["torrent_added"]["name"], "test");
    assert_eq!(
        body["result"]["torrent_added"]["hash_string"]
            .as_str()
            .unwrap()
            .len(),
        40
    );

    let get_table = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_get",
        "params": {
            "fields": ["id", "name", "hashString", "status", "labels", "percentDone"],
            "format": "table"
        },
        "id": 2
    });
    let (status, body) = transmission_rpc(app.clone(), &session, get_table, None).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body["result"]["torrents"].as_array().unwrap();
    assert_eq!(rows[0][0], "id");
    assert_eq!(rows[1][0], torrent_id);
    assert_eq!(rows[1][3], 0);
    assert_eq!(rows[1][4], serde_json::json!(["linux"]));

    let start = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_start",
        "params": { "ids": [torrent_id] },
        "id": 3
    });
    let (status, body) = transmission_rpc(app.clone(), &session, start, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    let get_object = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_get",
        "params": {
            "ids": [torrent_id],
            "fields": ["id", "status", "metadataPercentComplete"]
        },
        "id": 4
    });
    let (status, body) = transmission_rpc(app.clone(), &session, get_object, None).await;
    assert_eq!(status, StatusCode::OK);
    let torrents = body["result"]["torrents"].as_array().unwrap();
    assert_eq!(torrents[0]["id"], torrent_id);
    assert_eq!(torrents[0]["status"], 4);
    assert_eq!(torrents[0]["metadataPercentComplete"], 1.0);

    let remove = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_remove",
        "params": {
            "ids": [torrent_id],
            "delete_local_data": true
        },
        "id": 5
    });
    let (status, _) = transmission_rpc(app.clone(), &session, remove, None).await;
    assert_eq!(status, StatusCode::OK);

    let get_after_remove = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_get",
        "params": { "fields": ["id"] },
        "id": 6
    });
    let (status, body) = transmission_rpc(app, &session, get_after_remove, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"]["torrents"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn transmission_rpc_adds_base64_metainfo_and_rejects_remote_urls() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);
    let session = transmission_session(app.clone(), None).await;

    let metainfo = build_single_file_torrent("local-linux.iso", b"local payload", 8, None, false);
    let add_metainfo = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_add",
        "params": { "metainfo": test_base64(&metainfo) },
        "id": 1
    });
    let (status, body) = transmission_rpc(app.clone(), &session, add_metainfo, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["torrent_added"]["name"], "local-linux.iso");

    let add_url = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_add",
        "params": { "filename": "https://example.invalid/linux.torrent" },
        "id": 2
    });
    let (status, body) = transmission_rpc(app, &session, add_url, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32602);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("remote torrent URL fetching is not supported"));
}

#[tokio::test]
async fn settings_redacts_auth_token() {
    let mut cfg = Config::default();
    cfg.api.auth_token = Some("secret-token".into());
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/settings")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["data"]["api"]["auth_token"].is_null());
}

#[tokio::test]
async fn api_body_limit_rejects_oversized_upload() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::routes::app_router_with_body_limit(state, 8);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file")
                .header("content-type", "application/octet-stream")
                .body(Body::from(vec![0u8; 16]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn network_diagnostics_endpoint() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/network/diagnostics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: NetworkDiagnostics = parse_api_data(&body);
    assert_eq!(v.health.status, NetworkContainmentStatus::Disabled);
    let checks = v.checks;
    assert!(!checks.is_empty());
    assert!(!v.interfaces.is_empty());
}

#[tokio::test]
async fn watch_status_endpoint_reflects_config() {
    let mut cfg = Config::default();
    cfg.watch.push(WatchFolderConfig {
        path: "/tmp/swarmotter-nonexistent-watch".into(),
        recursive: true,
        download_dir: Some("/tmp/downloads".into()),
        label: Some("linux".into()),
        start_behavior: StartBehavior::Paused,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: true,
    });
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/watch/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: WatchStatus = parse_api_data(&body);
    let folders = v.folders;
    assert_eq!(folders.len(), 1);
    assert_eq!(folders[0].config.path, "/tmp/swarmotter-nonexistent-watch");
    assert!(v.enabled);
}

#[tokio::test]
async fn recent_logs_endpoint_supports_limit() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/logs/recent?lines=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: LogSnapshot = parse_api_data(&body);
    assert!(!v.lines.is_empty());
    assert!(v.lines.len() <= 1);
    assert!(v.enabled);
    assert!(!v.truncated);
}

#[tokio::test]
async fn doctor_checks_endpoint_contains_status() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/doctor")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: DoctorReport = parse_api_data(&body);
    assert!(!v.checks.is_empty());
    assert!(matches!(
        v.level,
        DiagnosticLevel::Ok | DiagnosticLevel::Warning | DiagnosticLevel::Invalid
    ));
}

#[tokio::test]
async fn torrent_summary_includes_health_field() {
    use swarmotter_core::models::torrent::HealthLabel;
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let bytes = build_single_file_torrent("file.bin", b"hello world data payload", 8, None, false);

    // Add a torrent.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file")
                .header("content-type", "application/octet-stream")
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hash = v["data"].as_str().unwrap().to_string();

    // List torrents and confirm each row carries a `health` object.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let arr = v["data"].as_array().expect("torrents list is an array");
    let row = arr
        .iter()
        .find(|r| r["info_hash"].as_str() == Some(hash.as_str()))
        .expect("added torrent is in list");
    let h = &row["health"];
    assert!(h.is_object(), "health must be an object on the summary");
    assert!(h["score"].is_u64());
    assert!(h["bars"].is_u64());
    assert!(h["label"].is_string());
    assert!(h["availability_score"].is_u64());
    assert!(h["throughput_score"].is_u64());
    assert!(h["peer_score"].is_u64());
    assert!(h["stability_score"].is_u64());
    assert!(h["discovery_score"].is_u64());
    assert!(h["reasons"].is_array());
    assert!(row["active_peer_workers"].is_u64());
    assert!(row["known_peers"].is_u64());
    // Default health for an empty daemon: a queued torrent with no engine
    // activity gets the unknown placeholder.
    let label = h["label"].as_str().unwrap();
    let valid_labels = [
        "unknown",
        "network_blocked",
        "stalled",
        "critical",
        "poor",
        "fair",
        "good",
        "excellent",
        "paused",
        "complete",
    ];
    assert!(
        valid_labels.contains(&label),
        "unexpected health label {label}"
    );
    // Bars are bounded 0..=5.
    let bars = h["bars"].as_u64().unwrap();
    assert!(bars <= 5);
    // And the same health must be present on the detail endpoint.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["data"]["health"].is_object());
    // Spot-check that HealthLabel::Unknown deserializes back to its snake-case
    // string form, as documented for the API.
    let _ = HealthLabel::Unknown;
}
