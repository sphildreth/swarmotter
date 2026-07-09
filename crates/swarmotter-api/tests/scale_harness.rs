// SPDX-License-Identifier: Apache-2.0

//! Scale/soak harness (explicitly ignored) for large synthetic torrent counts.
//!
//! The harness uses generated single-file `.torrent` payloads and local API routes
//! only. It is intended for explicit invocation when stress validating managed
//! torrent flows such as add/query/retry/remove/reset.

mod fake_daemon;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use serde::de::DeserializeOwned;
use serde_json::Value;
use swarmotter_core::meta::build_single_file_torrent;
use tower::ServiceExt;

fn synthetic_torrent_bytes(index: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1024);
    let byte = (index % 256) as u8;
    for i in 0..1024u16 {
        payload.push(byte ^ (i as u8));
    }
    build_single_file_torrent(
        &format!("scale-fixture-{index:05}.bin"),
        &payload,
        256,
        None,
        false,
    )
}

async fn get_json(app: &Router, uri: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

async fn post_empty_body(app: &Router, method: &str, uri: &str) -> StatusCode {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

fn parse_json<T: DeserializeOwned>(body: &[u8]) -> T {
    let v: Value = serde_json::from_slice(body).unwrap();
    serde_json::from_value(v["data"].clone()).unwrap()
}

async fn add_torrent_file(app: &Router, index: usize) -> String {
    let body = synthetic_torrent_bytes(index);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file?start_behavior=paused")
                .header("content-type", "application/octet-stream")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    parse_json::<String>(&body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit scale/soak harness"]
async fn ignored_scale_harness_add_query_retry_remove_reset_2000_torrents() {
    const ADD_COUNT: usize = 2000;
    const RECHECK_COUNT: usize = 200;

    let state = fake_daemon::fake_state();
    let app = swarmotter_api::app_router(state);

    let mut hashes = Vec::with_capacity(ADD_COUNT);
    for index in 0..ADD_COUNT {
        hashes.push(add_torrent_file(&app, index).await);
    }
    assert_eq!(hashes.len(), ADD_COUNT);

    let (status, value) = get_json(&app, "/api/v1/torrents").await;
    assert_eq!(status, StatusCode::OK);
    let listed = value["data"].as_array().unwrap();
    assert_eq!(listed.len(), ADD_COUNT);

    let (status, value) = get_json(
        &app,
        "/api/v1/torrents/query?state=paused&sort=name&dir=asc&per_page=100&page=1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["data"]["total"].as_u64().unwrap(), ADD_COUNT as u64);
    assert_eq!(
        value["data"]["counts"]["states"]["paused"]
            .as_u64()
            .unwrap(),
        ADD_COUNT as u64
    );

    for index in 0..RECHECK_COUNT {
        let hash = &hashes[index];
        assert_eq!(
            post_empty_body(&app, "POST", &format!("/api/v1/torrents/{hash}/recheck")).await,
            StatusCode::OK,
            "recheck should return success for scale harness"
        );
        assert_eq!(
            post_empty_body(&app, "POST", &format!("/api/v1/torrents/{hash}/reannounce")).await,
            StatusCode::OK,
            "reannounce should return success for scale harness"
        );
    }

    let remove_count = ADD_COUNT / 2;
    let remove_slice = &hashes[0..remove_count];
    let remove_body = serde_json::json!({
        "info_hashes": remove_slice
    })
    .to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/remove")
                .header("content-type", "application/json")
                .body(Body::from(remove_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let removed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        removed["data"]["removed"].as_array().unwrap().len(),
        remove_count
    );
    assert!(removed["data"]["not_found"].as_array().unwrap().is_empty());
    hashes.drain(0..remove_count);

    let (status, value) = get_json(&app, "/api/v1/torrents").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["data"].as_array().unwrap().len(), hashes.len());

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
    let reset: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reset["data"]["torrents_removed"].as_u64().unwrap(),
        hashes.len() as u64
    );

    let (status, value) = get_json(&app, "/api/v1/torrents").await;
    assert_eq!(status, StatusCode::OK);
    assert!(value["data"].as_array().unwrap().is_empty());
}
