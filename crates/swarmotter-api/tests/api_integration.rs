// SPDX-License-Identifier: Apache-2.0

//! API integration tests using a fake in-memory daemon.

mod fake_daemon;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use serde::de::DeserializeOwned;
use std::collections::BTreeSet;
use swarmotter_api::state::DaemonOps;
use swarmotter_core::config::{
    Config, PeerEncryptionMode, StartBehavior, StorageRootControl, WatchFolderConfig,
};
use swarmotter_core::meta::{build_single_file_torrent, MAX_TORRENT_METADATA_BYTES};
use swarmotter_core::models::network::{NetworkContainmentMode, NetworkContainmentStatus};
use swarmotter_core::models::{
    ConfigUpdateResult, DiagnosticLevel, DoctorReport, LogSnapshot, NetworkDiagnostics, WatchStatus,
};
use swarmotter_core::policy::{
    PolicyBandwidth, PolicyFileExclusionRule, PolicyIntake, PolicyProfile, PolicyQueue,
    PolicySeeding, PolicyStorage, PolicyTracker, QueuePriority, TrackerHostRule,
};
use tower::ServiceExt;

fn known_magnet() -> String {
    "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e&dn=test".to_string()
}

fn bulk_magnet(index: usize) -> String {
    format!("magnet:?xt=urn:btih:{:040x}&dn=bulk-{index}", index + 1)
}

fn named_magnet(index: usize, name: &str) -> String {
    format!("magnet:?xt=urn:btih:{:040x}&dn={name}", index + 1)
}

fn torrent_padded_to_size(name: &str, target: usize) -> Vec<u8> {
    let mut bytes = build_single_file_torrent(name, b"bounded API payload", 8, None, false);
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
            .expect("target must accommodate the generated torrent");
    }
    panic!("could not solve bencode padding for target size {target}");
}

async fn get_json(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

async fn post_json(
    app: &Router,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

async fn put_raw(app: &Router, uri: &str, body: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

async fn put_json(
    app: &Router,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    put_raw(app, uri, &body.to_string()).await
}

async fn add_named_test_magnet(
    app: &Router,
    index: usize,
    name: &str,
    paused: bool,
    download_dir: &str,
) -> String {
    let (status, value) = post_json(
        app,
        "/api/v1/torrents/magnet",
        serde_json::json!({
            "magnet": named_magnet(index, name),
            "paused": paused,
            "download_dir": download_dir,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    value["data"].as_str().unwrap().to_string()
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

async fn qb_get(
    app: Router,
    uri: &str,
    auth: Option<&str>,
    cookie: Option<&str>,
) -> (StatusCode, String) {
    let mut builder = Request::builder().uri(uri);
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    if let Some(cookie) = cookie {
        builder = builder.header("cookie", cookie);
    }
    let resp = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

async fn qb_post_form(
    app: Router,
    uri: &str,
    body: &str,
    auth: Option<&str>,
    cookie: Option<&str>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/x-www-form-urlencoded");
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    if let Some(cookie) = cookie {
        builder = builder.header("cookie", cookie);
    }
    let resp = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, String::from_utf8(body.to_vec()).unwrap())
}

async fn qb_login(app: Router, password: &str) -> String {
    let (status, headers, body) = qb_post_form(
        app,
        "/api/v2/auth/login",
        &format!("username=admin&password={password}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    headers
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .expect("SID cookie")
        .split(';')
        .next()
        .unwrap()
        .to_string()
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

fn pure_v2_single_file_fixture() -> Vec<u8> {
    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d4:infod9:file treed10:lawful.bind0:d6:lengthi1e11:pieces root32:");
    torrent.extend_from_slice(&[0x42; 32]);
    torrent.extend_from_slice(
        b"eee12:meta versioni2e4:name10:lawful.bin12:piece lengthi16384ee12:piece layersdee",
    );
    torrent
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
    assert_eq!(v["data"]["version"], env!("CARGO_PKG_VERSION"));
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
async fn native_seeding_put_replaces_policy_and_list_detail_are_truthful() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let hash = add_named_test_magnet(&app, 90, "seeding-policy", false, "/data").await;
    let uri = format!("/api/v1/torrents/{hash}/seeding");

    let (status, body) = put_json(
        &app,
        &uri,
        serde_json::json!({
            "ratio_limit": 0.0,
            "idle_limit": 0,
            "seed_forever": false,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["seeding"]["ratio_limit"], 0.0);
    assert_eq!(body["data"]["seeding"]["idle_limit"], 0);

    let (status, body) = put_json(
        &app,
        &uri,
        serde_json::json!({
            "ratio_limit": null,
            "idle_limit": null,
            "seed_forever": false,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["data"]["seeding"]["ratio_limit"],
        serde_json::Value::Null
    );
    assert_eq!(body["data"]["effective_ratio_limit"], 2.0);
    assert_eq!(body["data"]["effective_idle_limit"], 1800);

    let (status, body) = put_json(
        &app,
        &uri,
        serde_json::json!({
            "ratio_limit": 1.5,
            "idle_limit": 77,
            "seed_forever": true,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["seeding"]["ratio_limit"], 1.5);
    assert_eq!(body["data"]["seeding"]["idle_limit"], 77);
    assert!(body["data"]["effective_ratio_limit"].is_null());
    assert!(body["data"]["effective_idle_limit"].is_null());

    let (_, detail) = get_json(&app, &format!("/api/v1/torrents/{hash}")).await;
    assert_eq!(detail["data"]["seeding"]["seed_forever"], true);
    let (_, list) = get_json(&app, "/api/v1/torrents").await;
    assert_eq!(list["data"][0]["seeding"]["seed_forever"], true);
    assert_eq!(list["data"][0]["seeding_status"], "not_eligible");
}

#[tokio::test]
async fn native_seeding_put_rejects_non_replacement_and_invalid_values() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let hash = add_named_test_magnet(&app, 91, "seeding-validation", false, "/data").await;
    let uri = format!("/api/v1/torrents/{hash}/seeding");
    let invalid = [
        r#"{"ratio_limit":null,"idle_limit":null}"#,
        r#"{"ratio_limit":null,"seed_forever":false}"#,
        r#"{"idle_limit":null,"seed_forever":false}"#,
        r#"{"ratio_limit":-1,"idle_limit":null,"seed_forever":false}"#,
        r#"{"ratio_limit":null,"idle_limit":-1,"seed_forever":false}"#,
        r#"{"ratio_limit":null,"idle_limit":1.5,"seed_forever":false}"#,
        r#"{"ratio_limit":1e999,"idle_limit":null,"seed_forever":false}"#,
        r#"{"ratio_limit":null,"idle_limit":18446744073709551616,"seed_forever":false}"#,
        r#"{"ratio_limit":"1.5","idle_limit":null,"seed_forever":false}"#,
        r#"{"ratio_limit":true,"idle_limit":null,"seed_forever":false}"#,
        r#"{"ratio_limit":null,"idle_limit":"1800","seed_forever":false}"#,
        r#"{"ratio_limit":null,"idle_limit":true,"seed_forever":false}"#,
        r#"{"ratio_limit":null,"idle_limit":null,"seed_forever":null}"#,
        r#"{"ratio_limit":null,"idle_limit":null,"seed_forever":"false"}"#,
        r#"{"ratio_limit":null,"idle_limit":null,"seed_forever":false,"extra":1}"#,
        r#"[]"#,
    ];
    for body in invalid {
        let (status, response) = put_raw(&app, &uri, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
        assert_eq!(
            response["error"]["code"], "invalid_argument",
            "body: {body}"
        );
    }
}

#[tokio::test]
async fn torrent_query_filters_sorts_paginates_counts_and_groups() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let alpha = add_named_test_magnet(&app, 100, "alpha-linux", false, "/data/linux").await;
    let beta = add_named_test_magnet(&app, 101, "beta-archive", true, "/data/archive").await;
    let gamma = add_named_test_magnet(&app, 102, "gamma-linux", false, "/data/linux").await;

    for (hash, labels) in [
        (
            &alpha,
            serde_json::json!({ "labels": ["linux", "release"] }),
        ),
        (&beta, serde_json::json!({ "labels": ["archive"] })),
        (&gamma, serde_json::json!({ "labels": ["linux"] })),
    ] {
        let (status, _value) =
            post_json(&app, &format!("/api/v1/torrents/{hash}/labels"), labels).await;
        assert_eq!(status, StatusCode::OK);
    }

    let (status, value) = get_json(
        &app,
        "/api/v1/torrents/query?label=linux&storage_root=/data/linux&sort=name&dir=desc&page=1&per_page=1&group_by=label",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let data = &value["data"];
    assert_eq!(data["total"], 3);
    assert_eq!(data["filtered"], 2);
    assert_eq!(data["page"], 1);
    assert_eq!(data["per_page"], 1);
    assert_eq!(data["page_count"], 2);
    assert_eq!(data["rows"].as_array().unwrap().len(), 1);
    assert_eq!(data["rows"][0]["name"], "gamma-linux");
    assert_eq!(data["counts"]["labels"]["linux"], 2);
    assert_eq!(data["counts"]["storage_roots"]["/data/linux"], 2);
    assert_eq!(data["counts"]["states"]["downloading_metadata"], 2);
    assert!(data["groups"]
        .as_array()
        .unwrap()
        .iter()
        .any(|group| group["key"] == "linux" && group["count"] == 2));

    let (status, value) = get_json(&app, "/api/v1/torrents/query?state=paused&sort=name").await;
    assert_eq!(status, StatusCode::OK);
    let data = &value["data"];
    assert_eq!(data["filtered"], 1);
    assert_eq!(data["rows"][0]["name"], "beta-archive");

    let (status, value) = get_json(
        &app,
        "/api/v1/torrents/query?q=alpha&per_page=0&group_by=state",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let data = &value["data"];
    assert_eq!(data["filtered"], 1);
    assert_eq!(data["rows"].as_array().unwrap().len(), 0);
    assert_eq!(data["counts"]["labels"]["linux"], 1);
    assert!(data["groups"]
        .as_array()
        .unwrap()
        .iter()
        .any(|group| group["key"] == "downloading_metadata" && group["count"] == 1));
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
async fn pure_v2_adds_preserve_full_identity_and_register() {
    let (state, daemon) = fake_daemon::fake_state_with_config_and_daemon(Config::default());
    let app = swarmotter_api::app_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file")
                .header("content-type", "application/octet-stream")
                .body(Body::from(pure_v2_single_file_fixture()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let file_key = value["data"]
        .as_str()
        .expect("pure-v2 file add returns a canonical locator")
        .to_string();
    assert_eq!(file_key.len(), 64);

    let (status, value) = post_json(
        &app,
        "/api/v1/torrents/magnet",
        serde_json::json!({
            "magnet": format!("magnet:?xt=urn:btmh:1220{}", "a".repeat(64)),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let magnet_key = value["data"]
        .as_str()
        .expect("pure-v2 magnet add returns a canonical locator")
        .to_string();
    assert_eq!(magnet_key, "a".repeat(64));
    let snapshot = daemon.state_snapshot().await;
    let torrents = snapshot["torrents"]
        .as_array()
        .expect("registered torrent summaries");
    assert_eq!(torrents.len(), 2);
    assert!(torrents
        .iter()
        .any(|torrent| torrent["info_hash"] == file_key));
    assert!(torrents
        .iter()
        .any(|torrent| torrent["info_hash"] == magnet_key));

    // Native route parsing must retain the complete 64-character key for
    // every per-torrent surface; it must never route through a v1-only
    // InfoHash parser or truncate the BEP 52 identity.
    for key in [&file_key, &magnet_key] {
        let (status, detail) = get_json(&app, &format!("/api/v1/torrents/{key}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["data"]["info_hash"], *key);

        let (status, diagnostics) = get_json(&app, &format!("/api/v1/torrents/{key}/stats")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(diagnostics["data"]["info_hash"], *key);
    }
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
    const ADD_COUNT: usize = 1000;

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
async fn bulk_add_accepts_many_magnets_paused() {
    const ADD_COUNT: usize = 1000;

    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let magnets = (0..ADD_COUNT).map(bulk_magnet).collect::<Vec<_>>();
    let body = serde_json::json!({
        "magnets": magnets,
        "paused": true
    })
    .to_string();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/bulk")
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
    assert_eq!(v["data"]["added"].as_array().unwrap().len(), ADD_COUNT);
    assert!(v["data"]["failed"].as_array().unwrap().is_empty());

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
    let torrents = v["data"].as_array().unwrap();
    assert_eq!(torrents.len(), ADD_COUNT);
    assert!(torrents.iter().all(|torrent| torrent["state"] == "paused"));
}

#[tokio::test]
async fn bulk_metainfo_base64_accepts_exact_decoded_limit_and_rejects_one_over() {
    let state = fake_daemon::fake_state();
    let app =
        swarmotter_api::routes::app_router_with_body_limit(state, MAX_TORRENT_METADATA_BYTES * 2);

    let exact = torrent_padded_to_size("bulk-api-limit.bin", MAX_TORRENT_METADATA_BYTES);
    let encoded = test_base64(&exact);
    drop(exact);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/bulk?paused=true")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "torrent_files": [{ "metainfo": encoded }] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["data"]["added"].as_array().unwrap().len(), 1);
    assert!(envelope["data"]["failed"].as_array().unwrap().is_empty());

    let mut one_over = torrent_padded_to_size("bulk-api-limit.bin", MAX_TORRENT_METADATA_BYTES);
    one_over.push(b'X');
    let encoded = test_base64(&one_over);
    drop(one_over);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/bulk?paused=true")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "torrent_files": [{ "metainfo": encoded }] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(envelope["data"]["added"].as_array().unwrap().is_empty());
    assert_eq!(envelope["data"]["failed"][0]["code"], "malformed_torrent");
    assert!(envelope["data"]["failed"][0]["message"]
        .as_str()
        .is_some_and(|message| message.contains("exceeds maximum")));

    let (status, list) = get_json(&app, "/api/v1/torrents").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn bulk_query_scales_to_1000_torrents() {
    const ADD_COUNT: usize = 1000usize;
    const PAGE_SIZE: usize = 50usize;

    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let magnets = (0..ADD_COUNT)
        .map(|index| {
            let bucket = if index % 2 == 0 {
                "bulk-even"
            } else {
                "bulk-odd"
            };
            named_magnet(index, &format!("{bucket}-{index:04}"))
        })
        .collect::<Vec<_>>();
    let body = serde_json::json!({
        "magnets": magnets,
        "paused": true
    })
    .to_string();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/bulk")
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
    assert_eq!(v["data"]["added"].as_array().unwrap().len(), ADD_COUNT);
    assert!(v["data"]["failed"].as_array().unwrap().is_empty());

    let (status, value) = get_json(
        &app,
        "/api/v1/torrents/query?state=paused&sort=name&dir=asc&per_page=100&page=2&group_by=state",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let data = &value["data"];
    assert_eq!(data["total"], ADD_COUNT);
    assert_eq!(data["filtered"], ADD_COUNT);
    assert_eq!(data["page"], 2);
    assert_eq!(data["per_page"], 100);
    assert_eq!(data["rows"].as_array().unwrap().len(), 100);
    assert_eq!(data["rows"][0]["state"], "paused");
    assert!(data["counts"]["states"]["paused"].as_u64().unwrap() == 1000);
    assert!(data["groups"]
        .as_array()
        .unwrap()
        .iter()
        .any(|group| group["key"] == "paused" && group["count"] == 1000));

    let page_expectation = (500.0 / PAGE_SIZE as f64).ceil() as usize;
    let (status, value) = get_json(
        &app,
        "/api/v1/torrents/query?q=bulk-even&per_page=50&dir=asc&sort=name&page=1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let data = &value["data"];
    assert_eq!(data["total"], ADD_COUNT);
    assert_eq!(data["filtered"], 500);
    assert_eq!(data["page_count"], page_expectation);
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows.len(), PAGE_SIZE);
    assert!(rows.iter().all(|row| {
        row["name"]
            .as_str()
            .map(|name| name.starts_with("bulk-even"))
            .unwrap_or(false)
    }));
}

#[tokio::test]
async fn bulk_remove_handles_selected_torrent_count() {
    const REMOVE_COUNT: usize = 98;

    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let magnets = (0..REMOVE_COUNT).map(bulk_magnet).collect::<Vec<_>>();
    let body = serde_json::json!({ "magnets": magnets }).to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/bulk")
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
    let hashes = v["data"]["added"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["info_hash"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(hashes.len(), REMOVE_COUNT);
    assert!(v["data"]["failed"].as_array().unwrap().is_empty());

    let body = serde_json::json!({ "info_hashes": hashes }).to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/remove")
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
    assert_eq!(v["data"]["removed"].as_array().unwrap().len(), REMOVE_COUNT);
    assert!(v["data"]["not_found"].as_array().unwrap().is_empty());

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
        "bandwidth": { "global_download": 1000, "global_upload": 500, "alt_download": 0, "alt_upload": 0, "alt_enabled": false, "max_peers": 0, "max_peers_per_torrent": 0 },
        "autopilot": { "mode": "act" }
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

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/autopilot/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["data"]["mode"], "act");
}

#[tokio::test]
async fn policy_profiles_apply_at_add_and_expose_explainable_native_routes() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some("/global/complete".into());
    cfg.storage.incomplete_dir = Some("/global/incomplete".into());
    cfg.profiles.profiles.insert(
        "linux".into(),
        PolicyProfile {
            storage: PolicyStorage {
                download_dir: Some("/profiles/linux/complete".into()),
                incomplete_dir: Some("/profiles/linux/incomplete".into()),
            },
            queue: PolicyQueue {
                priority: Some(QueuePriority::High),
                start_behavior: Some(StartBehavior::Paused),
            },
            seeding: PolicySeeding {
                ratio_limit: Some(2.0),
                idle_limit: Some(600),
                seed_forever: Some(false),
            },
            bandwidth: PolicyBandwidth {
                download_limit: Some(1_000),
                upload_limit: Some(2_000),
            },
            ..Default::default()
        },
    );
    cfg.profiles.labels.insert("linux".into(), "linux".into());
    let state = fake_daemon::fake_state_with_config(cfg.clone());
    let app = swarmotter_api::app_router(state);

    let (status, profiles) = get_json(&app, "/api/v1/profiles").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profiles["data"]["labels"]["linux"], "linux");
    assert_eq!(
        profiles["data"]["profiles"]["linux"]["storage"]["download_dir"],
        "/profiles/linux/complete"
    );

    let (status, added) = post_json(
        &app,
        "/api/v1/torrents/magnet",
        serde_json::json!({ "magnet": named_magnet(9_001, "profiled-linux"), "labels": ["linux"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let hash = added["data"].as_str().unwrap();

    let (status, policy) = get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(policy["data"]["profile"]["name"], "linux");
    assert_eq!(policy["data"]["profile"]["source"]["kind"], "label");
    assert_eq!(
        policy["data"]["download_dir"]["value"],
        "/profiles/linux/complete"
    );
    assert_eq!(
        policy["data"]["download_dir"]["source"]["kind"],
        "profile_storage_snapshot"
    );
    assert_eq!(policy["data"]["queue_priority"]["value"], "high");
    assert_eq!(policy["data"]["start_behavior"]["value"], "paused");
    assert_eq!(policy["data"]["download_limit"]["value"], 1_000);

    let mut replacement = cfg.profiles.clone();
    replacement.profiles.insert(
        "manual".into(),
        PolicyProfile {
            bandwidth: PolicyBandwidth {
                download_limit: Some(3_000),
                upload_limit: Some(4_000),
            },
            ..Default::default()
        },
    );
    let (status, replaced) = put_json(
        &app,
        "/api/v1/profiles",
        serde_json::to_value(&replacement).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(replaced["data"]["profiles"]["manual"].is_object());

    let (status, assigned) = put_json(
        &app,
        &format!("/api/v1/torrents/{hash}/policy"),
        serde_json::json!({ "profile": "manual" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(assigned["success"], true);

    let (status, assigned_policy) =
        get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(assigned_policy["data"]["profile"]["name"], "manual");
    assert_eq!(
        assigned_policy["data"]["profile"]["source"]["kind"],
        "profile"
    );
    // Reassignment changes live fields but preserves the original add-time
    // storage snapshot rather than moving the payload into the manual profile.
    assert_eq!(
        assigned_policy["data"]["download_dir"]["value"],
        "/profiles/linux/complete"
    );
    assert_eq!(assigned_policy["data"]["download_limit"]["value"], 3_000);
}

#[tokio::test]
async fn metadata_preview_intake_is_exposed_by_native_api() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.profiles.profiles.insert(
        "review".into(),
        PolicyProfile {
            intake: PolicyIntake {
                excluded_file_patterns: vec!["samples/*".into(), "*.nfo".into()],
                excluded_file_rules: vec![PolicyFileExclusionRule {
                    suffix: Some(".sfv".into()),
                    ..Default::default()
                }],
                organization_subdirectory: Some("lawful/releases".into()),
                incomplete_subdirectory: Some("staging/review".into()),
                force_top_level_folder: true,
                partial_file_suffix: Some(".part".into()),
            },
            tracker: PolicyTracker {
                host_rules: vec![TrackerHostRule {
                    host_pattern: "*.lawful.example".into(),
                    enabled: Some(false),
                    priority: Some(QueuePriority::Low),
                }],
            },
            ..Default::default()
        },
    );
    let app = swarmotter_api::app_router(fake_daemon::fake_state_with_config(cfg));

    let (status, added) = post_json(
        &app,
        "/api/v1/torrents/magnet",
        serde_json::json!({
            "magnet": named_magnet(9_050, "review-before-start"),
            "profile": "review",
            "preview": true,
            "unwanted_file_indices": [4, 1, 4],
            "file_exclusion_rules": [{ "path_segment": "proof" }],
            "incomplete_dir": "/review/incomplete",
            "partial_file_suffix": ".request"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let hash = added["data"].as_str().unwrap();

    let (status, policy) = get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        policy["data"]["intake"]["excluded_file_patterns"]["value"],
        serde_json::json!(["samples/*", "*.nfo"])
    );
    assert_eq!(
        policy["data"]["intake"]["excluded_file_patterns"]["source"]["kind"],
        "intake_snapshot"
    );
    assert_eq!(
        policy["data"]["intake"]["excluded_file_rules"]["value"],
        serde_json::json!([
            { "suffix": ".sfv", "path_pattern": null, "path_segment": null, "min_size_bytes": null, "max_size_bytes": null },
            { "suffix": null, "path_pattern": null, "path_segment": "proof", "min_size_bytes": null, "max_size_bytes": null }
        ])
    );
    assert_eq!(
        policy["data"]["intake"]["organization_subdirectory"]["value"],
        "lawful/releases"
    );
    assert_eq!(
        policy["data"]["intake"]["incomplete_subdirectory"]["value"],
        "staging/review"
    );
    assert_eq!(
        policy["data"]["intake"]["force_top_level_folder"]["value"],
        true
    );
    assert_eq!(
        policy["data"]["intake"]["partial_file_suffix"]["value"],
        ".request"
    );
    assert_eq!(
        policy["data"]["incomplete_dir"]["value"],
        "/review/incomplete"
    );
    assert_eq!(
        policy["data"]["tracker"]["host_rules"]["value"],
        serde_json::json!([{ "host_pattern": "*.lawful.example", "enabled": false, "priority": "low" }])
    );
    assert_eq!(
        policy["data"]["tracker"]["host_rules"]["source"]["kind"],
        "profile"
    );
    assert_eq!(
        policy["data"]["intake"]["unwanted_file_indices"],
        serde_json::json!([1, 4])
    );
    assert_eq!(policy["data"]["intake"]["preview_until_started"], true);

    let (status, preview) = post_json(
        &app,
        &format!("/api/v1/torrents/{hash}/storage-preview"),
        serde_json::json!({
            "download_dir": "/preview/complete",
            "incomplete_dir": "/preview/incomplete"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        preview["data"]["complete_dir"],
        "/preview/complete/lawful/releases/review-before-start"
    );
    assert_eq!(
        preview["data"]["incomplete_dir"],
        "/preview/incomplete/staging/review/review-before-start"
    );
    assert_eq!(preview["data"]["partial_file_suffix"], ".request");
    assert!(preview["data"]["incomplete_files"][0]
        .as_str()
        .is_some_and(|path| path.ends_with(".request")));
}

#[tokio::test]
async fn per_torrent_encryption_override_is_explainable_and_clearable() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.torrent.encryption_mode = PeerEncryptionMode::Disabled;
    cfg.profiles.profiles.insert(
        "encrypted".into(),
        PolicyProfile {
            encryption_mode: Some(PeerEncryptionMode::Required),
            ..Default::default()
        },
    );
    cfg.profiles
        .labels
        .insert("encrypted".into(), "encrypted".into());
    let app = swarmotter_api::app_router(fake_daemon::fake_state_with_config(cfg));

    let (status, added) = post_json(
        &app,
        "/api/v1/torrents/magnet",
        serde_json::json!({
            "magnet": named_magnet(9_101, "policy-encryption"),
            "labels": ["encrypted"]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let hash = added["data"].as_str().unwrap();

    let (status, inherited) = get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(inherited["data"]["encryption_mode"]["value"], "required");
    assert_eq!(
        inherited["data"]["encryption_mode"]["source"]["kind"],
        "label"
    );

    let missing_field = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/torrents/{hash}/encryption-mode"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_field.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let (status, changed) = put_json(
        &app,
        &format!("/api/v1/torrents/{hash}/encryption-mode"),
        serde_json::json!({ "encryption_mode": "preferred" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(changed["success"], true);
    let (status, overridden) = get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(overridden["data"]["encryption_mode"]["value"], "preferred");
    assert_eq!(
        overridden["data"]["encryption_mode"]["source"]["kind"],
        "torrent"
    );

    let (status, cleared) = put_json(
        &app,
        &format!("/api/v1/torrents/{hash}/encryption-mode"),
        serde_json::json!({ "encryption_mode": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cleared["success"], true);
    let (status, restored) = get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(restored["data"]["encryption_mode"]["value"], "required");
    assert_eq!(
        restored["data"]["encryption_mode"]["source"]["kind"],
        "label"
    );
}

#[tokio::test]
async fn settings_put_replaces_and_preserves_auth_token() {
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
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
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let scheduler = &v["data"]["scheduler"];
    assert!(scheduler["managed_torrents"].is_u64());
    assert!(scheduler["requested_downloads"].is_u64());
    assert!(scheduler["granted_downloads"].is_u64());
    assert!(scheduler["requested_metadata_fetches"].is_u64());
    assert!(scheduler["granted_metadata_fetches"].is_u64());
    assert!(scheduler["peer_worker_budget_saturated"].is_boolean());
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
async fn native_api_rejects_cross_origin_browser_mutations() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/reset")
                .header("host", "127.0.0.1:9091")
                .header("origin", "https://malicious.example")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/reset")
                .header("host", "malicious.example")
                .header("origin", "http://malicious.example")
                .header("sec-fetch-site", "same-site")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn native_api_allows_unauthenticated_remote_same_origin_browser_clients() {
    let mut cfg = Config::default();
    cfg.api.bind_address = "0.0.0.0:9091".into();
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .header("host", "127.0.0.1:9091")
                .header("origin", "http://127.0.0.1:9091")
                .header("sec-fetch-site", "same-origin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .header("host", "192.0.2.10:9091")
                .header("origin", "http://192.0.2.10:9091")
                .header("sec-fetch-site", "same-origin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .header("host", "[::1]:9091")
                .header("origin", "http://[::1]:9091")
                .header("sec-fetch-site", "same-origin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn native_api_accepts_authenticated_same_origin_reverse_proxy_requests() {
    let mut cfg = Config::default();
    cfg.api.require_auth = true;
    cfg.api.auth_token = Some("test-token".into());
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents")
                .header("host", "swarmotter.example")
                .header("origin", "https://swarmotter.example")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/ws")
                .header("host", "swarmotter.example")
                .header("origin", "https://malicious.example")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
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
async fn qbittorrent_api_is_disabled_by_default() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let (status, body) = qb_get(app, "/api/v2/app/version", None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("disabled"));
}

#[tokio::test]
async fn qbittorrent_api_reuses_api_token_and_sid_cookie_auth() {
    let mut cfg = Config::default();
    cfg.compatibility.qbittorrent.enabled = true;
    cfg.api.require_auth = true;
    cfg.api.auth_token = Some("test-token".into());
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let (status, _) = qb_get(app.clone(), "/api/v2/app/version", None, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, body) = qb_get(
        app.clone(),
        "/api/v2/app/webapiVersion",
        Some("Bearer test-token"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "2.11.4");

    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/auth/login",
        "username=admin&password=wrong",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Fails.");

    let cookie = qb_login(app.clone(), "test-token").await;
    let (status, body) = qb_get(app, "/api/v2/app/version", None, Some(&cookie)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.starts_with('v'));
}

#[tokio::test]
async fn qbittorrent_api_adds_lists_controls_and_deletes_magnets() {
    let mut cfg = Config::default();
    cfg.compatibility.qbittorrent.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);
    let magnet =
        "magnet%3A%3Fxt%3Durn%3Abtih%3Add8255ecdc7ca55fb0bbf81323d87062ba1f7a4e%26dn%3Dtest";
    let add_body = format!("urls={magnet}&paused=true&savepath=%2Ftmp%2Fqb&category=linux");

    let (status, _, body) =
        qb_post_form(app.clone(), "/api/v2/torrents/add", &add_body, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");

    let (status, body) = qb_get(
        app.clone(),
        "/api/v2/torrents/info?category=linux",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "test");
    assert_eq!(rows[0]["category"], "linux");
    assert_eq!(rows[0]["save_path"], "/tmp/qb");
    assert_eq!(rows[0]["state"], "pausedDL");
    let hash = rows[0]["hash"].as_str().unwrap();
    assert_eq!(hash.len(), 40);

    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/resume",
        &format!("hashes={hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    let (status, body) = qb_get(
        app.clone(),
        &format!("/api/v2/torrents/info?hashes={hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(rows[0]["state"], "downloading");

    let (status, _, _) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/setCategory",
        &format!("hashes={hash}&category=distros"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = qb_get(
        app.clone(),
        "/api/v2/torrents/info?category=distros",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);

    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/delete",
        &format!("hashes={hash}&deleteFiles=true"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    let (status, body) = qb_get(app, "/api/v2/torrents/info", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(rows.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn compatibility_adapters_preserve_and_accept_full_pure_v2_locators() {
    let mut cfg = Config::default();
    cfg.compatibility.qbittorrent.enabled = true;
    cfg.compatibility.transmission.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let qb_key = "a".repeat(64);
    let qb_add = format!("urls=magnet%3A%3Fxt%3Durn%3Abtmh%3A1220{qb_key}&paused=true");
    let (status, _, body) =
        qb_post_form(app.clone(), "/api/v2/torrents/add", &qb_add, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");

    let (status, body) = qb_get(
        app.clone(),
        &format!("/api/v2/torrents/info?hashes={qb_key}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(rows.as_array().unwrap()[0]["hash"], qb_key);

    let session = transmission_session(app.clone(), None).await;
    let transmission_key = "b".repeat(64);
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-add",
            "arguments": { "filename": transmission_key },
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["arguments"]["torrent-added"]["hashString"],
        transmission_key
    );

    let (status, body) = transmission_rpc(
        app,
        &session,
        serde_json::json!({
            "method": "torrent-get",
            "arguments": {
                "ids": [transmission_key],
                "fields": ["hashString", "magnetLink"],
            },
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let torrent = &body["arguments"]["torrents"][0];
    assert_eq!(torrent["hashString"], transmission_key);
    assert!(torrent["magnetLink"]
        .as_str()
        .unwrap()
        .contains("xt=urn:btmh:1220"));
}

#[tokio::test]
async fn qbittorrent_api_rejects_remote_torrent_urls() {
    let mut cfg = Config::default();
    cfg.compatibility.qbittorrent.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let (status, _, body) = qb_post_form(
        app,
        "/api/v2/torrents/add",
        "urls=https%3A%2F%2Fexample.invalid%2Flinux.torrent",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("remote torrent URL intake is not supported"));
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
    assert_eq!(
        body["arguments"]["version"],
        format!("4.0.0 (SwarmOtter {})", env!("CARGO_PKG_VERSION"))
    );
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
async fn transmission_rpc_supports_prowlarr_get_session_negotiation() {
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
                .uri("/transmission/rpc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let auth = "Basic dXNlcjp0ZXN0LXRva2Vu";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/transmission/rpc")
                .header("authorization", auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let session = resp
        .headers()
        .get("x-transmission-session-id")
        .and_then(|value| value.to_str().ok())
        .expect("Prowlarr negotiation session header")
        .to_string();

    let payload = serde_json::json!({ "method": "session-get" });
    let (status, body) = transmission_rpc(app.clone(), &session, payload, Some(auth)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "success");
    assert!(body["arguments"]["version"]
        .as_str()
        .is_some_and(|version| version.starts_with("4.0.0 (SwarmOtter ")));

    let add = serde_json::json!({
        "method": "torrent-add",
        "arguments": { "filename": known_magnet(), "paused": true }
    });
    let (status, body) = transmission_rpc(app.clone(), &session, add, Some(auth)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "success");

    let get = serde_json::json!({
        "method": "torrent-get",
        "arguments": {
            "fields": [
                "id", "hashString", "name", "downloadDir", "totalSize",
                "leftUntilDone", "isFinished", "eta", "status",
                "secondsDownloading", "secondsSeeding", "errorString",
                "uploadedEver", "downloadedEver", "seedRatioLimit",
                "seedRatioMode", "seedIdleLimit", "seedIdleMode", "fileCount"
            ]
        }
    });
    let (status, body) = transmission_rpc(app.clone(), &session, get, Some(auth)).await;
    assert_eq!(status, StatusCode::OK);
    let row = &body["arguments"]["torrents"][0];
    for field in [
        "id",
        "totalSize",
        "leftUntilDone",
        "eta",
        "status",
        "secondsDownloading",
        "secondsSeeding",
        "uploadedEver",
        "downloadedEver",
        "seedRatioLimit",
        "seedRatioMode",
        "seedIdleLimit",
        "seedIdleMode",
        "fileCount",
    ] {
        assert!(row[field].is_number(), "{field} must be numeric");
    }
    assert!(row["isFinished"].is_boolean());
    for field in ["hashString", "name", "downloadDir", "errorString"] {
        assert!(row[field].is_string(), "{field} must be a string");
    }

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/transmission/rpc")
                .header("authorization", auth)
                .header("x-transmission-session-id", session)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn compatibility_reports_effective_seeding_limits_and_upload_fields() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    cfg.compatibility.qbittorrent.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);
    let hash = add_named_test_magnet(&app, 92, "compat-seeding", false, "/data").await;
    let (status, _) = put_json(
        &app,
        &format!("/api/v1/torrents/{hash}/seeding"),
        serde_json::json!({
            "ratio_limit": 1.25,
            "idle_limit": 75,
            "seed_forever": false,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let session = transmission_session(app.clone(), None).await;
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-get",
            "arguments": {
                "fields": [
                    "hashString", "uploadRatio", "uploadedEver", "seedRatioLimit",
                    "seedRatioMode", "seedIdleLimit", "seedIdleMode"
                ]
            }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let row = &body["arguments"]["torrents"][0];
    assert_eq!(row["uploadRatio"], 0.0);
    assert_eq!(row["uploadedEver"], 0);
    assert_eq!(row["seedRatioLimit"], 1.25);
    assert_eq!(row["seedRatioMode"], 1);
    assert_eq!(row["seedIdleLimit"], 2);
    assert_eq!(row["seedIdleMode"], 1);

    let (status, text) = qb_get(app, "/api/v2/torrents/info", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(rows[0]["ratio"], 0.0);
    assert_eq!(rows[0]["uploaded"], 0);
    assert!(rows[0].get("ratio_limit").is_none());
    assert!(rows[0].get("seeding_time_limit").is_none());
}

#[tokio::test]
async fn compatibility_omitted_paused_uses_label_profile_admission() {
    let mut cfg = Config::default();
    cfg.compatibility.qbittorrent.enabled = true;
    cfg.compatibility.transmission.enabled = true;
    cfg.profiles.profiles.insert(
        "hold".into(),
        PolicyProfile {
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Paused),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    cfg.profiles.labels.insert("hold".into(), "hold".into());
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let qb_paused_magnet =
        "magnet%3A%3Fxt%3Durn%3Abtih%3Add8255ecdc7ca55fb0bbf81323d87062ba1f7a4e%26dn%3Dqb-profile-paused";
    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/add",
        &format!("urls={qb_paused_magnet}&category=hold"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");

    let (status, body) = qb_get(
        app.clone(),
        "/api/v2/torrents/info?category=hold",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(rows.as_array().unwrap()[0]["state"], "pausedDL");

    // Explicit false must remain an explicit start request, even when the
    // category selects a paused profile.
    let qb_started_magnet =
        "magnet%3A%3Fxt%3Durn%3Abtih%3A1111111111111111111111111111111111111111%26dn%3Dqb-profile-started";
    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/add",
        &format!("urls={qb_started_magnet}&paused=false&category=hold"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    let (status, body) = qb_get(
        app.clone(),
        "/api/v2/torrents/info?category=hold",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    let started = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "qb-profile-started")
        .unwrap();
    assert_eq!(started["state"], "metaDL");

    let session = transmission_session(app.clone(), None).await;
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-add",
            "arguments": {
                "filename": named_magnet(9_199, "transmission-profile-paused"),
                "labels": ["hold"]
            }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let paused_hash = body["arguments"]["torrent-added"]["hashString"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-get",
            "arguments": { "fields": ["hashString", "status"] }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let paused = body["arguments"]["torrents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["hashString"] == paused_hash)
        .unwrap();
    assert_eq!(paused["status"], 0);

    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-add",
            "arguments": {
                "filename": named_magnet(9_200, "transmission-profile-started"),
                "labels": ["hold"],
                "paused": false
            }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let started_hash = body["arguments"]["torrent-added"]["hashString"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = transmission_rpc(
        app,
        &session,
        serde_json::json!({
            "method": "torrent-get",
            "arguments": { "fields": ["hashString", "status"] }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let started = body["arguments"]["torrents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["hashString"] == started_hash)
        .unwrap();
    assert_eq!(started["status"], 4);
}

#[tokio::test]
async fn qbittorrent_categories_profiles_and_lifecycle_inspection_flow() {
    let mut cfg = Config::default();
    cfg.compatibility.qbittorrent.enabled = true;
    cfg.storage.download_dir = Some("/global/downloads".into());
    cfg.profiles.profiles.insert(
        "archive".into(),
        PolicyProfile {
            storage: PolicyStorage {
                download_dir: Some("/profiles/archive".into()),
                ..Default::default()
            },
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Paused),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    cfg.profiles
        .labels
        .insert("release".into(), "archive".into());
    let app = swarmotter_api::app_router(fake_daemon::fake_state_with_config(cfg));

    let (status, body) = qb_get(app.clone(), "/api/v2/torrents/categories", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let categories: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(categories["release"]["savePath"], "/profiles/archive");
    assert_eq!(categories["archive"]["savePath"], "/profiles/archive");

    let release_magnet =
        "magnet%3A%3Fxt%3Durn%3Abtih%3Add8255ecdc7ca55fb0bbf81323d87062ba1f7a4e%26dn%3Dqb-compatible-release";
    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/add",
        &format!("urls={release_magnet}&category=release&savepath=%2Farr%2Fincoming"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");

    let (status, body) = qb_get(
        app.clone(),
        "/api/v2/torrents/info?category=release",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    let release = &rows.as_array().unwrap()[0];
    assert_eq!(release["state"], "pausedDL");
    assert_eq!(
        release["content_path"],
        "/arr/incoming/qb-compatible-release"
    );
    assert_eq!(release["dl_limit"], 0);
    let release_hash = release["hash"].as_str().unwrap().to_string();

    let (status, policy) = get_json(&app, &format!("/api/v1/torrents/{release_hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(policy["data"]["profile"]["name"], "archive");
    assert_eq!(policy["data"]["profile"]["source"]["kind"], "label");

    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/setCategory",
        &format!("hashes={release_hash}&category=archive"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    let (status, policy) = get_json(&app, &format!("/api/v1/torrents/{release_hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(policy["data"]["profile"]["name"], "archive");
    assert_eq!(policy["data"]["profile"]["source"]["kind"], "profile");

    let (status, body) = qb_get(
        app.clone(),
        &format!("/api/v2/torrents/properties?hash={release_hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let properties: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(properties["save_path"], "/arr/incoming");
    assert_eq!(properties["completion_date"], 0);
    assert_eq!(properties["private"], false);

    let (status, body) = qb_get(
        app.clone(),
        &format!("/api/v2/torrents/files?hash={release_hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let files: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(files.as_array().unwrap().len(), 1);
    assert_eq!(files[0]["priority"], 4);

    let (status, body) = qb_get(
        app.clone(),
        &format!("/api/v2/torrents/trackers?hash={release_hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let trackers: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(trackers.as_array().unwrap().is_empty());

    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/setLocation",
        &format!("hashes={release_hash}&location=%2Farr%2Fcomplete"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    let (status, body) = qb_get(
        app.clone(),
        &format!("/api/v2/torrents/properties?hash={release_hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let moved: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(moved["save_path"], "/arr/complete");

    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/recheck",
        &format!("hashes={release_hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    let (status, body) = qb_get(
        app.clone(),
        &format!("/api/v2/torrents/info?hashes={release_hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let checked: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(checked[0]["state"], "checkingDL");
    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/reannounce",
        &format!("hashes={release_hash}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");

    // A category whose name is a configured profile becomes an explicit add
    // profile, while regular categories remain label-driven.
    let direct_magnet =
        "magnet%3A%3Fxt%3Durn%3Abtih%3A1111111111111111111111111111111111111111%26dn%3Dqb-direct-profile";
    let (status, _, body) = qb_post_form(
        app.clone(),
        "/api/v2/torrents/add",
        &format!("urls={direct_magnet}&category=archive"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Ok.");
    let (status, body) = qb_get(
        app.clone(),
        "/api/v2/torrents/info?category=archive",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body).unwrap();
    let direct = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "qb-direct-profile")
        .unwrap();
    let direct_hash = direct["hash"].as_str().unwrap();
    let (status, policy) = get_json(&app, &format!("/api/v1/torrents/{direct_hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(policy["data"]["profile"]["name"], "archive");
    assert_eq!(policy["data"]["profile"]["source"]["kind"], "profile");
}

#[tokio::test]
async fn transmission_profile_add_set_and_status_flow() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    cfg.profiles.profiles.insert(
        "automation".into(),
        PolicyProfile {
            queue: PolicyQueue {
                start_behavior: Some(StartBehavior::Paused),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let app = swarmotter_api::app_router(fake_daemon::fake_state_with_config(cfg));
    let session = transmission_session(app.clone(), None).await;

    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-add",
            "arguments": {
                "filename": named_magnet(9_301, "transmission-profile-flow"),
                "download-dir": "/arr/incoming",
                "labels": ["arr", "import"],
                "profile": "automation"
            }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let added = &body["arguments"]["torrent-added"];
    assert_eq!(added["status"], 0);
    assert_eq!(added["downloadDir"], "/arr/incoming");
    assert_eq!(added["labels"], serde_json::json!(["arr", "import"]));
    let id = added["id"].as_i64().unwrap();
    let hash = added["hashString"].as_str().unwrap().to_string();

    let (status, policy) = get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(policy["data"]["profile"]["name"], "automation");

    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-get",
            "arguments": {
                "ids": [id],
                "fields": ["status", "isFinished", "doneDate", "errorString", "labels", "downloadDir"]
            }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let row = &body["arguments"]["torrents"][0];
    assert_eq!(row["status"], 0);
    assert_eq!(row["isFinished"], false);
    assert_eq!(row["doneDate"], 0);
    assert_eq!(row["errorString"], "");
    assert_eq!(row["labels"], serde_json::json!(["arr", "import"]));
    assert_eq!(row["downloadDir"], "/arr/incoming");

    // `profile: null` deliberately clears the compatibility profile extension
    // while normal labels and location mutations still use native operations.
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-set",
            "arguments": {
                "ids": [id],
                "profile": null,
                "labels": ["arr", "complete"],
                "location": "/arr/complete"
            }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "success");
    let (status, policy) = get_json(&app, &format!("/api/v1/torrents/{hash}/policy")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(policy["data"]["profile"].is_null());

    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "method": "torrent-start-now",
            "arguments": { "ids": [id] }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "success");
    let (status, body) = transmission_rpc(
        app,
        &session,
        serde_json::json!({
            "method": "torrent-get",
            "arguments": { "ids": [id], "fields": ["status", "labels", "downloadDir"] }
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let row = &body["arguments"]["torrents"][0];
    assert_eq!(row["status"], 4);
    assert_eq!(row["labels"], serde_json::json!(["arr", "complete"]));
    assert_eq!(row["downloadDir"], "/arr/complete");
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
async fn transmission_metainfo_base64_accepts_exact_decoded_limit_and_rejects_one_over() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app =
        swarmotter_api::routes::app_router_with_body_limit(state, MAX_TORRENT_METADATA_BYTES * 2);
    let session = transmission_session(app.clone(), None).await;

    let exact = torrent_padded_to_size("transmission-api-limit.bin", MAX_TORRENT_METADATA_BYTES);
    let encoded = test_base64(&exact);
    drop(exact);
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_add",
            "params": { "metainfo": encoded, "paused": true },
            "id": 41
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["result"]["torrent_added"]["name"],
        "transmission-api-limit.bin"
    );

    let mut one_over =
        torrent_padded_to_size("transmission-api-limit.bin", MAX_TORRENT_METADATA_BYTES);
    one_over.push(b'X');
    let encoded = test_base64(&one_over);
    drop(one_over);
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_add",
            "params": { "metainfo": encoded, "paused": true },
            "id": 42
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32000);
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("exceeds maximum")));

    let (status, list) = get_json(&app, "/api/v1/torrents").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
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
async fn settings_redacts_and_preserves_socks5_password() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.network.socks5.enabled = true;
    cfg.network.socks5.host = Some("proxy.example".into());
    cfg.network.socks5.username = Some("operator".into());
    cfg.network.socks5.password = Some("proxy-secret".into());
    cfg.torrent.utp_enabled = false;
    cfg.dht.enabled = false;
    let (state, daemon) = fake_daemon::fake_state_with_config_and_daemon(cfg);
    let app = swarmotter_api::app_router(state);

    let (status, settings) = get_json(&app, "/api/v1/settings").await;
    assert_eq!(status, StatusCode::OK);
    assert!(settings["data"]["network"]["socks5"]["password"].is_null());

    let (status, result) = put_json(&app, "/api/v1/settings", settings["data"].clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(result["data"]["config"]["network"]["socks5"]["password"].is_null());
    assert_eq!(
        daemon.get_config().await.network.socks5.password.as_deref(),
        Some("proxy-secret")
    );
}

#[tokio::test]
async fn api_body_limit_rejects_oversized_upload() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::routes::app_router_with_body_limit(state, 8);

    for uri in ["/api/v1/torrents/file", "/api/v1/torrents"] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(vec![0u8; 16]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE, "{uri}");
    }
}

#[tokio::test]
async fn torrent_metadata_limit_applies_through_real_router_when_api_body_limit_is_higher() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::routes::app_router_with_body_limit(
        state,
        MAX_TORRENT_METADATA_BYTES + 1024,
    );
    for (uri, name) in [
        ("/api/v1/torrents/file", "dedicated-api-limit.bin"),
        ("/api/v1/torrents", "multiplex-api-limit.bin"),
    ] {
        let exact = torrent_padded_to_size(name, MAX_TORRENT_METADATA_BYTES);
        let mut one_over = exact.clone();
        one_over.push(b'X');

        let accepted = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(exact))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK, "{uri}");

        let rejected = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(one_over))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body = axum::body::to_bytes(rejected.into_body(), usize::MAX)
            .await
            .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope["error"]["code"], "malformed_torrent");
        assert!(envelope["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("exceeds maximum")));
    }

    let malformed = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file")
                .header("content-type", "application/octet-stream")
                .body(Body::from("not bencoded metainfo"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(malformed.into_body(), usize::MAX)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["error"]["code"], "bencode_error");
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
    assert!(!v.socks5_enabled);
    assert!(!v.socks5_udp_blocked);
    let checks = v.checks;
    assert!(!checks.is_empty());
    assert!(!v.interfaces.is_empty());
}

#[tokio::test]
async fn storage_roots_endpoint_reports_reserve_configuration() {
    let root = std::env::temp_dir().join(format!(
        "swarmotter-storage-api-test-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.minimum_free_space_bytes = 4096;
    cfg.storage.minimum_free_space_percent = 5;
    cfg.storage.root_controls = vec![StorageRootControl {
        path: root.display().to_string(),
        max_active_downloads: 2,
        max_active_bytes: 8_192,
        max_write_bytes_per_second: 1_024,
        max_concurrent_rechecks: 1,
    }];
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let (status, value) = get_json(&app, "/api/v1/storage/roots").await;
    assert_eq!(status, StatusCode::OK);
    let data = &value["data"];
    assert_eq!(data["minimum_free_space_bytes"], 4096);
    assert_eq!(data["minimum_free_space_percent"], 5);
    let roots = data["roots"].as_array().unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0]["path"], root.display().to_string());
    assert!(roots[0]["available_space_bytes"].is_u64());
    assert_eq!(roots[0]["root_control_path"], root.display().to_string());
    assert_eq!(roots[0]["max_active_downloads"], 2);
    assert_eq!(roots[0]["max_active_bytes"], 8_192);
    assert_eq!(roots[0]["max_write_bytes_per_second"], 1_024);
    assert_eq!(roots[0]["max_concurrent_rechecks"], 1);
    assert_eq!(roots[0]["active_bytes"], 0);
    assert_eq!(roots[0]["active_rechecks"], 0);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn storage_roots_endpoint_reports_state_placement_and_optional_mount_diagnostics() {
    let root = std::env::temp_dir().join(format!(
        "swarmotter-storage-placement-api-test-{}",
        std::process::id()
    ));
    let resume = root.join("resume");
    let state_dir = root.join("state");
    let temp = root.join("scratch");
    for directory in [&resume, &state_dir, &temp] {
        std::fs::create_dir_all(directory).unwrap();
    }
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.resume_dir = Some(resume.display().to_string());
    cfg.storage.state_dir = Some(state_dir.display().to_string());
    cfg.storage.temp_dir = Some(temp.display().to_string());
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let (status, value) = get_json(&app, "/api/v1/storage/roots").await;
    assert_eq!(status, StatusCode::OK);
    let roots = value["data"]["roots"].as_array().unwrap();
    let find = |path: &std::path::Path| {
        roots
            .iter()
            .find(|root| root["path"] == path.display().to_string())
            .unwrap()
    };
    assert!(find(&resume)["roles"]
        .as_array()
        .unwrap()
        .iter()
        .any(|role| role == "resume"));
    assert!(find(&state_dir)["roles"]
        .as_array()
        .unwrap()
        .iter()
        .any(|role| role == "state"));
    assert!(find(&temp)["roles"]
        .as_array()
        .unwrap()
        .iter()
        .any(|role| role == "temporary"));
    let download = find(&root);
    assert!(download.get("mount_point").is_some());
    assert!(download.get("mount_options").is_some());
    assert_eq!(download["sustained_write_bytes_per_second"], 0);
    assert_eq!(download["sustained_verification_bytes_per_second"], 0);
    assert_eq!(download["cow_strategy"], "conservative");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn watch_status_endpoint_reflects_config() {
    let mut cfg = Config::default();
    cfg.watch.push(WatchFolderConfig {
        path: "/tmp/swarmotter-nonexistent-watch".into(),
        recursive: true,
        download_dir: Some("/tmp/downloads".into()),
        label: Some("linux".into()),
        profile: None,
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

#[tokio::test]
async fn autopilot_status_route_returns_current_config_mode() {
    let mut cfg = Config::default();
    cfg.autopilot.mode = swarmotter_core::autopilot::AutopilotMode::Act;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/autopilot/status")
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
    assert_eq!(v["data"]["mode"], "act");
}

#[tokio::test]
async fn torrent_autopilot_routes_support_get_and_mode_override() {
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
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hash = v["data"].as_str().unwrap().to_string();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/torrents/{hash}/autopilot"))
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
    assert!(v["data"]["apply"].is_boolean());

    let set = serde_json::json!({"mode":"disabled"}).to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/torrents/{hash}/autopilot"))
                .header("content-type", "application/json")
                .body(Body::from(set))
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
    assert_eq!(v["data"]["autopilot_mode_override"], "disabled");

    let clear = serde_json::json!({ "mode": serde_json::Value::Null }).to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/torrents/{hash}/autopilot"))
                .header("content-type", "application/json")
                .body(Body::from(clear))
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
    assert!(v["data"]["autopilot_mode_override"].is_null());
}

async fn post_empty(
    app: &Router,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

async fn delete_uri(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

#[tokio::test]
async fn queue_move_endpoints_cover_all_actions() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let hash = add_named_test_magnet(&app, 100, "queue-1", true, "/tmp/dl").await;

    for action in ["move-up", "move-down", "move-top", "move-bottom"] {
        let (status, v) = post_empty(
            &app,
            &format!("/api/v1/torrents/{hash}/queue/{action}"),
            serde_json::json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "queue/{action} status");
        assert_eq!(v["success"], true, "queue/{action} success");
    }

    let bad = "not-a-hex-hash";
    for action in ["move-up", "move-down", "move-top", "move-bottom"] {
        let (status, _v) = post_empty(
            &app,
            &format!("/api/v1/torrents/{bad}/queue/{action}"),
            serde_json::json!({}),
        )
        .await;
        assert!(
            status.is_client_error() || status.is_server_error(),
            "queue/{action} with bad hash should error, got {status}"
        );
    }
}

#[tokio::test]
async fn list_peers_returns_empty_for_added_torrent() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let hash = add_named_test_magnet(&app, 200, "peers-1", true, "/tmp/dl").await;

    let (status, v) = get_json(&app, &format!("/api/v1/torrents/{hash}/peers")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert!(v["data"].is_array());
    assert_eq!(v["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn peer_filter_policy_status_and_manual_bans_are_available_through_api() {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    let app = swarmotter_api::routes::app_router(fake_daemon::fake_state_with_config(config));

    let (status, initial) = get_json(&app, "/api/v1/peer-filter").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(initial["data"]["enabled"], false);
    assert_eq!(initial["data"]["configured_rule_count"], 0);

    let (status, replaced) = put_json(
        &app,
        "/api/v1/peer-filter",
        serde_json::json!({
            "enabled": true,
            "rules": ["198.51.100.0/24"],
            "blocklist_paths": [],
            "manual_bans": [],
            "blocked_client_ids": ["-ABCD"],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replaced}");
    assert_eq!(replaced["data"]["enabled"], true);
    assert_eq!(
        replaced["data"]["rules"],
        serde_json::json!(["198.51.100.0/24"])
    );
    assert_eq!(replaced["data"]["configured_rule_count"], 1);
    assert_eq!(replaced["data"]["blocked_client_ids"][0], "-ABCD");

    let hash = add_named_test_magnet(&app, 210, "peer-filter", true, "/tmp/dl").await;
    let (status, banned) = post_json(
        &app,
        &format!("/api/v1/torrents/{hash}/peers/ban"),
        serde_json::json!({ "ip": "203.0.113.7", "reason": "operator review" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(banned["data"]["enabled"], true);
    assert_eq!(banned["data"]["manual_bans"][0]["ip"], "203.0.113.7");
    assert_eq!(
        banned["data"]["manual_bans"][0]["reason"],
        "operator review"
    );

    let (status, global_unbanned) = post_json(
        &app,
        "/api/v1/peer-filter/unban",
        serde_json::json!({ "ip": "203.0.113.7" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        global_unbanned["data"]["manual_bans"],
        serde_json::json!([])
    );

    let (status, unbanned) = post_json(
        &app,
        &format!("/api/v1/torrents/{hash}/peers/unban"),
        serde_json::json!({ "ip": "203.0.113.7" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(unbanned["data"]["manual_bans"], serde_json::json!([]));
}

#[tokio::test]
async fn list_peers_rejects_bad_hash() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let (status, _v) = get_json(&app, "/api/v1/torrents/not-a-hex/peers").await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "bad hash must error, got {status}"
    );
}

#[tokio::test]
async fn watch_scan_and_history_endpoints() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let (status, v) = post_empty(&app, "/api/v1/watch/scan", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["success"], true);

    let (status, v) = get_json(&app, "/api/v1/watch/history").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert!(v["data"].is_array());
}

#[tokio::test]
async fn trackers_crud_and_bad_hash() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let (status, added) = post_json(
        &app,
        "/api/v1/torrents/magnet",
        serde_json::json!({
            "magnet": format!(
                "{}&tr=http%3A%2F%2Ftracker.example.com%2Fannounce",
                named_magnet(300, "trackers-1")
            ),
            "paused": true,
            "download_dir": "/tmp/dl",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let hash = added["data"].as_str().unwrap().to_string();

    let (status, v) = get_json(&app, &format!("/api/v1/torrents/{hash}/trackers")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert!(v["data"].is_array());
    let tracker = v["data"]
        .as_array()
        .and_then(|rows| rows.first())
        .expect("magnet tracker row");
    assert_eq!(tracker["scrape_status"], "not_contacted");
    assert_eq!(tracker["last_scrape"], serde_json::Value::Null);
    assert_eq!(tracker["scrape_seeders"], serde_json::Value::Null);
    assert_eq!(tracker["scrape_leechers"], serde_json::Value::Null);
    assert_eq!(tracker["scrape_downloads"], serde_json::Value::Null);
    assert_eq!(tracker["last_scrape_error"], serde_json::Value::Null);

    let (status, v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{hash}/trackers"),
        serde_json::json!({ "url": "udp://tracker.example.com:80/announce" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "add_tracker status");
    assert_eq!(v["success"], true);

    // The :url path segment can't contain '/', so use a tracker id without slashes.
    let tracker_id = "udp%3A%2F%2Ftracker.example.com%3A80%2Fannounce";
    let (status, v) = delete_uri(
        &app,
        &format!("/api/v1/torrents/{hash}/trackers/{tracker_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "remove_tracker status");
    assert_eq!(v["success"], true);

    let (status, v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{hash}/trackers/edit"),
        serde_json::json!({
            "old_url": "udp://tracker.example.com:80/announce",
            "new_url": "udp://tracker.example.com:81/announce",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "edit_tracker status");
    assert_eq!(v["success"], true);

    let bad = "not-hex";
    let (status, _v) = get_json(&app, &format!("/api/v1/torrents/{bad}/trackers")).await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "list_trackers bad hash must error, got {status}"
    );

    let (status, _v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{bad}/trackers"),
        serde_json::json!({ "url": "udp://x" }),
    )
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "add_tracker bad hash must error, got {status}"
    );

    let (status, _v) = delete_uri(
        &app,
        &format!("/api/v1/torrents/{bad}/trackers/udp%3A%2F%2Fx"),
    )
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "remove_tracker bad hash must error, got {status}"
    );

    let (status, _v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{bad}/trackers/edit"),
        serde_json::json!({ "old_url": "a", "new_url": "b" }),
    )
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "edit_tracker bad hash must error, got {status}"
    );
}

#[tokio::test]
async fn trackers_crud_against_missing_torrent_returns_404() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    // 40 hex chars, parses cleanly, but no torrent with this hash exists.
    let ghost = "0000000000000000000000000000000000000000";

    let (status, v) = get_json(&app, &format!("/api/v1/torrents/{ghost}/trackers")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "list_trackers ghost");
    assert_eq!(v["success"], false);

    let (status, _v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{ghost}/trackers"),
        serde_json::json!({ "url": "udp://tracker.example.com:80/announce" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "add_tracker ghost");

    let (status, _v) = delete_uri(
        &app,
        &format!("/api/v1/torrents/{ghost}/trackers/udp%3A%2F%2Fx"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "remove_tracker ghost");

    let (status, _v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{ghost}/trackers/edit"),
        serde_json::json!({ "old_url": "a", "new_url": "b" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "edit_tracker ghost");
}

#[tokio::test]
async fn files_list_wanted_priority_rename_and_bad_hash() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let hash = add_named_test_magnet(&app, 400, "files-1", true, "/tmp/dl").await;

    let (status, v) = get_json(&app, &format!("/api/v1/torrents/{hash}/files")).await;
    assert_eq!(status, StatusCode::OK, "list_files status");
    assert_eq!(v["success"], true);
    assert!(v["data"].is_array());

    let (status, v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{hash}/files/wanted"),
        serde_json::json!({ "file_indices": [0], "wanted": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set_wanted status");
    assert_eq!(v["success"], true);

    let (status, v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{hash}/files/priority"),
        serde_json::json!({ "file_indices": [0], "priority": "high" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set_priority status");
    assert_eq!(v["success"], true);

    let (status, v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{hash}/files/0/rename"),
        serde_json::json!({ "new_path": "renamed.bin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "rename_path status");
    assert_eq!(v["success"], true);

    // patch_files delegates to set_wanted
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/torrents/{hash}/files"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "file_indices": [0], "wanted": false }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "patch_files status");

    let bad = "not-hex";
    let (status, _v) = get_json(&app, &format!("/api/v1/torrents/{bad}/files")).await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "list_files bad hash must error, got {status}"
    );

    let (status, _v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{bad}/files/wanted"),
        serde_json::json!({ "file_indices": [0], "wanted": true }),
    )
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "set_wanted bad hash must error, got {status}"
    );

    let (status, _v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{bad}/files/priority"),
        serde_json::json!({ "file_indices": [0], "priority": "low" }),
    )
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "set_priority bad hash must error, got {status}"
    );

    let (status, _v) = post_empty(
        &app,
        &format!("/api/v1/torrents/{bad}/files/0/rename"),
        serde_json::json!({ "new_path": "x" }),
    )
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "rename_path bad hash must error, got {status}"
    );
}

#[tokio::test]
async fn list_peers_against_missing_torrent_returns_404() {
    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);
    let ghost = "0000000000000000000000000000000000000000";
    let (status, v) = get_json(&app, &format!("/api/v1/torrents/{ghost}/peers")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(v["success"], false);
}

async fn transmission_state_with_torrent() -> (Router, String, i64) {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);
    let session = transmission_session(app.clone(), None).await;
    let add = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "torrent_add",
        "params": { "filename": known_magnet(), "paused": true },
        "id": 1
    });
    let (_status, body) = transmission_rpc(app.clone(), &session, add, None).await;
    let torrent_id = body["result"]["torrent_added"]["id"].as_i64().unwrap();
    (app, session, torrent_id)
}

#[tokio::test]
async fn transmission_rpc_covers_remaining_dispatch_methods() {
    let (app, session, torrent_id) = transmission_state_with_torrent().await;

    // torrent_start_now
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_start_now",
            "params": { "ids": [torrent_id] },
            "id": 10
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // torrent_stop
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_stop",
            "params": { "ids": [torrent_id] },
            "id": 11
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // torrent_verify
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_verify",
            "params": { "ids": [torrent_id] },
            "id": 12
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // torrent_reannounce
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_reannounce",
            "params": { "ids": [torrent_id] },
            "id": 13
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // torrent_set with labels + limits
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_set",
            "params": {
                "ids": [torrent_id],
                "labels": ["alpha", "beta"],
                "downloadLimit": 4096,
                "downloadLimited": true,
                "uploadLimit": 2048,
                "uploadLimited": true
            },
            "id": 14
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // torrent_set_location
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_set_location",
            "params": { "ids": [torrent_id], "location": "/tmp/new-loc" },
            "id": 15
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // torrent_set_location missing location -> error
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_set_location",
            "params": { "ids": [torrent_id] },
            "id": 16
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["error"].is_object());
    assert_eq!(body["error"]["code"], -32602);

    // torrent_rename_path
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_rename_path",
            "params": { "ids": [torrent_id], "path": "test", "name": "renamed.txt" },
            "id": 17
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // torrent_rename_path missing path -> error
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "torrent_rename_path",
            "params": { "ids": [torrent_id], "name": "x" },
            "id": 18
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["error"].is_object());
    assert_eq!(body["error"]["code"], -32602);

    // queue_move_*
    for (name, id) in [
        ("queue_move_top", 20),
        ("queue_move_up", 21),
        ("queue_move_down", 22),
        ("queue_move_bottom", 23),
    ] {
        let (status, body) = transmission_rpc(
            app.clone(),
            &session,
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": name,
                "params": { "ids": [torrent_id] },
                "id": id
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{name} status");
        assert!(body["result"].is_object(), "{name} result");
    }

    // free_space
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "free_space",
            "params": { "path": "/tmp" },
            "id": 30
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["path"], "/tmp");
    assert!(body["result"]["size_bytes"].is_number());

    // free_space default path
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "free_space",
            "params": {},
            "id": 31
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"]["path"].is_string());

    // port_test
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "port_test",
            "params": { "ip_protocol": "tcp" },
            "id": 32
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["port_is_open"], false);
    assert_eq!(body["result"]["ip_protocol"], "tcp");

    // port_test default protocol
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "port_test",
            "params": {},
            "id": 33
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"]["ip_protocol"].is_string());

    // session_set: change download_dir via patch
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session_set",
            "params": { "download_dir": "/tmp/swarmotter-test-dl" },
            "id": 40
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // session_stats
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session_stats",
            "id": 41
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // session_close
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session_close",
            "id": 42
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["result"].is_object());

    // blocklist_update
    let (status, body) = transmission_rpc(
        app.clone(),
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "blocklist_update",
            "id": 43
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["blocklist_size"], 0);

    // Unknown method -> method_not_found (-32601)
    let (status, body) = transmission_rpc(
        app,
        &session,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "nonexistent_method",
            "id": 99
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32601);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("nonexistent_method"));

    // Sanity: the torrent_id was used across all calls.
    assert!(torrent_id > 0);
}

#[tokio::test]
async fn transmission_rpc_returns_error_on_invalid_json_body() {
    let mut cfg = Config::default();
    cfg.compatibility.transmission.enabled = true;
    let state = fake_daemon::fake_state_with_config(cfg);
    let app = swarmotter_api::app_router(state);
    let session = transmission_session(app.clone(), None).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/transmission/rpc")
                .header("content-type", "application/json")
                .header("x-transmission-session-id", &session)
                .body(Body::from("not a json body"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
