// SPDX-License-Identifier: Apache-2.0

//! Native API coverage for the opt-in listener reachability diagnostic.

#[allow(dead_code)]
mod fake_daemon;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use swarmotter_core::config::Config;
use tower::ServiceExt;

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn native_health_and_port_test_routes_expose_opt_in_unknown_result() {
    let mut config = Config::default();
    config.network.mode = swarmotter_core::models::NetworkContainmentMode::Disabled;
    config.port_test.enabled = true;
    config.port_test.endpoint = Some("https://port-test.example/check".into());
    let app = swarmotter_api::app_router(fake_daemon::fake_state_with_config(config));

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/network/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let health = response_json(health).await;
    assert_eq!(health["data"]["status"], "disabled");
    assert_eq!(health["data"]["port_test"]["state"], "unknown");
    assert_eq!(health["data"]["port_test"]["endpoint_configured"], true);
    assert!(health["data"]["port_test"].get("endpoint").is_none());

    let test = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/network/port-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(test.status(), StatusCode::OK);
    let test = response_json(test).await;
    assert_eq!(test["data"]["state"], "unknown");
    assert_eq!(test["data"]["listen_port"], 51413);
}

#[tokio::test]
async fn native_router_mapping_routes_expose_opt_in_pending_status() {
    let mut config = Config::default();
    config.network.mode = swarmotter_core::models::NetworkContainmentMode::Strict;
    config.network.fail_closed = true;
    config.network.required_interface = Some("contained0".into());
    config.port_mapping.enabled = true;
    config.port_mapping.protocols = vec![
        swarmotter_core::port_mapping::PortMappingProtocol::NatPmp,
        swarmotter_core::port_mapping::PortMappingProtocol::Upnp,
    ];
    let app = swarmotter_api::app_router(fake_daemon::fake_state_with_config(config));

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/network/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let health = response_json(health).await;
    assert_eq!(health["data"]["port_mapping"]["state"], "pending");
    assert_eq!(
        health["data"]["port_mapping"]["protocols"],
        serde_json::json!(["nat_pmp", "upnp"])
    );

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/network/port-mapping")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    assert_eq!(response_json(status).await["data"]["state"], "pending");

    let refresh = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/network/port-mapping/refresh")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(refresh.status(), StatusCode::OK);
    assert_eq!(response_json(refresh).await["data"]["state"], "pending");
}
