// SPDX-License-Identifier: Apache-2.0

//! API integration tests using a fake in-memory daemon.

mod fake_daemon;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use swarmotter_core::config::Config;
use swarmotter_core::meta::build_single_file_torrent;
use tower::ServiceExt;

fn known_magnet() -> String {
    "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e&dn=test".to_string()
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
