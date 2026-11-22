// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{CONNECTION, CONTENT_TYPE, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use swarmotter_api::handlers::events::EventBroker;
use swarmotter_api::state::{AppState, BuildInfo, DaemonOps};
use swarmotter_core::config::Config;
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotterd::daemon::DaemonRuntime;

fn unique_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "swarmotter-openapi-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn state(runtime: Arc<DaemonRuntime>, config: Config) -> swarmotter_api::state::SharedState {
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

async fn http_request(
    address: std::net::SocketAddr,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> (StatusCode, hyper::HeaderMap, Vec<u8>) {
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(HOST, format!("{address}"))
        .header(CONNECTION, "close");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    if body.is_some() {
        builder = builder.header(CONTENT_TYPE, "application/json");
    }
    let request = builder
        .body(Full::new(Bytes::from(body.unwrap_or_default())))
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, headers, body)
}

fn resolve<'a>(root: &'a Value, reference: &str) -> &'a Value {
    let pointer = reference.strip_prefix('#').unwrap_or(reference);
    root.pointer(pointer)
        .unwrap_or_else(|| panic!("unresolved schema reference {reference}"))
}

fn validate(value: &Value, schema: &Value, root: &Value, path: &str) -> Result<(), String> {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return validate(value, resolve(root, reference), root, path);
    }
    if let Some(options) = schema.get("anyOf").and_then(Value::as_array) {
        let errors = options
            .iter()
            .map(|option| validate(value, option, root, path))
            .collect::<Vec<_>>();
        if errors.iter().any(Result::is_ok) {
            return Ok(());
        }
        return Err(format!("{path}: no anyOf branch matched: {errors:?}"));
    }
    if let Some(constant) = schema.get("const") {
        if value != constant {
            return Err(format!("{path}: expected const {constant}, got {value}"));
        }
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        if !values.contains(value) {
            return Err(format!("{path}: {value} is not in enum {values:?}"));
        }
    }
    if let Some(kind) = schema.get("type") {
        let matches = match kind {
            Value::String(kind) => type_matches(value, kind),
            Value::Array(kinds) => kinds
                .iter()
                .filter_map(Value::as_str)
                .any(|kind| type_matches(value, kind)),
            _ => false,
        };
        if !matches {
            return Err(format!("{path}: value {value} does not match type {kind}"));
        }
    }
    if value.is_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required.iter().filter_map(Value::as_str) {
                if value.get(key).is_none() {
                    return Err(format!("{path}: missing required property {key}"));
                }
            }
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (key, subschema) in properties {
                if let Some(field) = value.get(key) {
                    validate(field, subschema, root, &format!("{path}.{key}"))?;
                }
            }
            if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                for key in value.as_object().unwrap().keys() {
                    if !properties.contains_key(key) {
                        return Err(format!("{path}: unexpected property {key}"));
                    }
                }
            }
        }
    }
    if value.is_array() {
        if let Some(items) = schema.get("items") {
            for (index, item) in value.as_array().unwrap().iter().enumerate() {
                validate(item, items, root, &format!("{path}[{index}]"))?;
            }
        }
    }
    if let Some(number) = value.as_f64() {
        if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64) {
            if number < minimum {
                return Err(format!("{path}: {number} is below minimum {minimum}"));
            }
        }
        if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64) {
            if number > maximum {
                return Err(format!("{path}: {number} is above maximum {maximum}"));
            }
        }
    }
    Ok(())
}

fn type_matches(value: &Value, kind: &str) -> bool {
    match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value.is_i64() || value.is_u64(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => true,
    }
}

#[test]
fn emitted_schemas_validate_representative_examples() {
    let document = swarmotter_api::openapi::document();
    assert_eq!(document["openapi"], "3.1.0");
    let schemas = &document["components"]["schemas"];
    let examples = &document["components"]["examples"];
    validate(
        &examples["SuccessEnvelope"]["value"],
        &schemas["Envelope"],
        &document,
        "SuccessEnvelope",
    )
    .unwrap();
    validate(
        &examples["ErrorEnvelope"]["value"],
        &schemas["ErrorEnvelope"],
        &document,
        "ErrorEnvelope",
    )
    .unwrap();
    validate(
        &schemas["TorrentSummary"]["example"],
        &schemas["TorrentSummary"],
        &document,
        "TorrentSummary",
    )
    .unwrap();
    validate(
        &examples["TorrentListPage"]["value"]["data"],
        &schemas["TorrentListResponse"],
        &document,
        "TorrentListResponse",
    )
    .unwrap();
    for (path, methods) in document["paths"].as_object().unwrap() {
        for (method, operation) in methods.as_object().unwrap() {
            if let Some(content) = operation
                .get("requestBody")
                .and_then(|body| body.get("content"))
                .and_then(|content| content.get("application/json"))
            {
                if let Some(example) = content.get("example") {
                    validate(
                        example,
                        &content["schema"],
                        &document,
                        &format!("request {method} {path}"),
                    )
                    .unwrap();
                }
            }
            assert!(
                operation.get("responses").is_some(),
                "{method} {path} has no responses"
            );
            assert!(
                operation.get("security").is_some(),
                "{method} {path} has no security declaration"
            );
            if method != "get" {
                assert_eq!(
                    operation["x-swarmotter-state-changing"], true,
                    "{method} {path} is not marked state-changing"
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_daemon_exchanges_validate_for_native_qbittorrent_and_transmission_surfaces() {
    let root = unique_dir("live");
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.storage.download_dir = Some(root.join("downloads").display().to_string());
    config.compatibility.qbittorrent.enabled = true;
    config.compatibility.transmission.enabled = true;
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
    let server = tokio::spawn(async move {
        axum::serve(listener, swarmotter_api::app_router(state(runtime, config)))
            .await
            .unwrap();
    });
    let document = swarmotter_api::openapi::document();
    let schemas = &document["components"]["schemas"];

    let (status, _, body) = http_request(address, Method::GET, "/api/v1/version", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).unwrap();
    validate(&value, &schemas["Envelope"], &document, "native.version").unwrap();

    let (status, _, body) = http_request(
        address,
        Method::GET,
        "/api/v1/torrents/query?page=1&per_page=10",
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).unwrap();
    validate(&value, &schemas["Envelope"], &document, "native.query").unwrap();
    validate(
        &value["data"],
        &schemas["TorrentListResponse"],
        &document,
        "native.query.data",
    )
    .unwrap();

    let (status, _, body) = http_request(
        address,
        Method::GET,
        "/api/v1/torrents/0000000000000000000000000000000000000000",
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let value: Value = serde_json::from_slice(&body).unwrap();
    validate(&value, &schemas["ErrorEnvelope"], &document, "native.error").unwrap();

    let (status, _, body) =
        http_request(address, Method::GET, "/api/v2/app/version", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    let qbit_version: Value = Value::String(String::from_utf8(body).unwrap());
    validate(
        &qbit_version,
        &schemas["QbittorrentVersion"],
        &document,
        "qbittorrent.version",
    )
    .unwrap();

    let (status, headers, _) =
        http_request(address, Method::GET, "/transmission/rpc", &[], None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let session = headers
        .get("x-transmission-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let (status, _, body) = http_request(
        address,
        Method::POST,
        "/transmission/rpc",
        &[("x-transmission-session-id", &session)],
        Some(
            serde_json::to_vec(&serde_json::json!({
                "method": "session-get",
                "arguments": {},
                "tag": 1
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).unwrap();
    validate(
        &value,
        &schemas["TransmissionRpcResponse"],
        &document,
        "transmission.success",
    )
    .unwrap();

    let (status, _, body) = http_request(
        address,
        Method::POST,
        "/transmission/rpc",
        &[("x-transmission-session-id", &session)],
        Some(
            serde_json::to_vec(&serde_json::json!({
                "method": "torrent-does-not-exist",
                "arguments": {}
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).unwrap();
    validate(
        &value,
        &schemas["TransmissionRpcResponse"],
        &document,
        "transmission.error",
    )
    .unwrap();

    server.abort();
    std::fs::remove_dir_all(root).ok();
}
