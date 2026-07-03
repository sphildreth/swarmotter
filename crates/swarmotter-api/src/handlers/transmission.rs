// SPDX-License-Identifier: Apache-2.0

//! Transmission RPC compatibility adapter.
//!
//! This module translates Transmission-style RPC requests to the native
//! `DaemonOps` surface. It intentionally does not create torrent network
//! sockets or bypass the daemon's network containment layer.

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use swarmotter_core::bandwidth::TorrentBandwidth;
use swarmotter_core::config::Config;
use swarmotter_core::error::CoreError;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::models::peer::{Peer, PeerDirection};
use swarmotter_core::models::torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
use swarmotter_core::models::tracker::{TrackerInfo, TrackerStatus};

use crate::routes::constant_time_eq;
use crate::state::SharedState;

const SESSION_HEADER: &str = "x-transmission-session-id";

#[derive(Debug, Deserialize)]
struct RpcRequest {
    #[serde(default)]
    jsonrpc: Option<Value>,
    method: String,
    #[serde(default)]
    arguments: Option<Value>,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    tag: Option<Value>,
    #[serde(default)]
    id: Option<Value>,
}

impl RpcRequest {
    fn is_json_rpc(&self) -> bool {
        self.jsonrpc.is_some() || self.params.is_some() || self.id.is_some()
    }

    fn args(&self) -> Value {
        self.params
            .clone()
            .or_else(|| self.arguments.clone())
            .unwrap_or_else(|| Value::Object(Map::new()))
    }

    fn normalized_method(&self) -> String {
        self.method.replace('-', "_")
    }

    fn legacy_key_style(&self) -> bool {
        self.method.contains('-')
    }
}

#[derive(Debug)]
struct RpcFailure {
    code: i64,
    message: String,
}

impl RpcFailure {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("unsupported method: {method}"),
        }
    }

    fn from_core(error: CoreError) -> Self {
        Self {
            code: -32000,
            message: error.to_string(),
        }
    }
}

type RpcResult<T> = std::result::Result<T, RpcFailure>;

/// Handle `POST /transmission/rpc`.
pub async fn rpc(State(state): State<SharedState>, headers: HeaderMap, body: Bytes) -> Response {
    let cfg = state.daemon.get_config().await;
    if !cfg.compatibility.transmission.enabled {
        return (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "application/json")],
            json!({ "error": "transmission rpc compatibility is disabled" }).to_string(),
        )
            .into_response();
    }

    if let Some(response) = require_auth(&headers, &cfg) {
        return response;
    }
    if let Some(response) = require_session(&headers, &state) {
        return response;
    }

    let request = match serde_json::from_slice::<RpcRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "application/json")],
                json!({ "error": format!("invalid JSON RPC request: {error}") }).to_string(),
            )
                .into_response();
        }
    };

    let result = dispatch(&state, &cfg, &request).await;
    match result {
        Ok(arguments) => rpc_success(&request, arguments),
        Err(error) => rpc_error(&request, error),
    }
}

fn require_auth(headers: &HeaderMap, cfg: &Config) -> Option<Response> {
    if !cfg.api.require_auth {
        return None;
    }
    let Some(expected) = cfg.api.auth_token.as_deref() else {
        return Some(auth_response("api authentication is not configured"));
    };
    if headers_have_token(headers, expected) {
        return None;
    }
    Some(auth_response("missing or invalid API token"))
}

fn headers_have_token(headers: &HeaderMap, expected: &str) -> bool {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let direct = headers
        .get("x-swarmotter-auth")
        .and_then(|v| v.to_str().ok());
    let basic = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(basic_password);

    bearer
        .into_iter()
        .chain(direct)
        .chain(basic.as_deref())
        .any(|candidate| constant_time_eq(candidate.as_bytes(), expected.as_bytes()))
}

fn auth_response(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::WWW_AUTHENTICATE, "Basic realm=\"SwarmOtter\""),
        ],
        json!({ "error": message }).to_string(),
    )
        .into_response()
}

fn require_session(headers: &HeaderMap, state: &SharedState) -> Option<Response> {
    let expected = state.transmission.session_id();
    let valid = headers
        .get(SESSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|candidate| constant_time_eq(candidate.as_bytes(), expected.as_bytes()))
        .unwrap_or(false);
    if valid {
        return None;
    }

    let mut response = (
        StatusCode::CONFLICT,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "X-Transmission-Session-Id required",
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(expected) {
        response.headers_mut().insert(SESSION_HEADER, value);
    }
    Some(response)
}

async fn dispatch(state: &SharedState, cfg: &Config, request: &RpcRequest) -> RpcResult<Value> {
    match request.normalized_method().as_str() {
        "session_get" => session_get(state, cfg, request).await,
        "session_set" => session_set(state, cfg, request).await,
        "session_stats" => session_stats(state).await,
        "session_close" => Ok(Value::Object(Map::new())),
        "torrent_get" => torrent_get(state, request).await,
        "torrent_add" => torrent_add(state, request).await,
        "torrent_remove" => torrent_remove(state, request).await,
        "torrent_start" => torrent_action(state, request, TorrentAction::Resume).await,
        "torrent_start_now" => torrent_action(state, request, TorrentAction::StartNow).await,
        "torrent_stop" => torrent_action(state, request, TorrentAction::Stop).await,
        "torrent_verify" => torrent_action(state, request, TorrentAction::Recheck).await,
        "torrent_reannounce" => torrent_action(state, request, TorrentAction::Reannounce).await,
        "torrent_set" => torrent_set(state, request).await,
        "torrent_set_location" => torrent_set_location(state, request).await,
        "torrent_rename_path" => torrent_rename_path(state, request).await,
        "queue_move_top" => torrent_action(state, request, TorrentAction::QueueTop).await,
        "queue_move_up" => torrent_action(state, request, TorrentAction::QueueUp).await,
        "queue_move_down" => torrent_action(state, request, TorrentAction::QueueDown).await,
        "queue_move_bottom" => torrent_action(state, request, TorrentAction::QueueBottom).await,
        "free_space" => free_space(state, cfg, request).await,
        "port_test" => port_test(state, request).await,
        "blocklist_update" => Ok(json!({ "blocklist_size": 0 })),
        method => Err(RpcFailure::method_not_found(method)),
    }
}

async fn session_get(state: &SharedState, cfg: &Config, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let fields = string_array_arg(&args, &["fields"]);
    let stats = state.daemon.global_stats().await;
    let default_download_dir = cfg
        .storage
        .download_dir
        .clone()
        .unwrap_or_else(|| ".".to_string());
    let all_fields = [
        "version",
        "rpc_version",
        "rpc_version_minimum",
        "rpc_version_semver",
        "session_id",
        "download_dir",
        "download_dir_free_space",
        "speed_limit_down",
        "speed_limit_down_enabled",
        "speed_limit_up",
        "speed_limit_up_enabled",
        "alt_speed_down",
        "alt_speed_enabled",
        "alt_speed_up",
        "peer_limit_global",
        "peer_limit_per_torrent",
        "peer_port",
        "dht_enabled",
        "pex_enabled",
        "utp_enabled",
        "preferred_transports",
        "download_queue_enabled",
        "download_queue_size",
        "seed_queue_enabled",
        "seed_queue_size",
        "seed_ratio_limit",
        "seed_ratio_limited",
        "start_added_torrents",
        "blocklist_size",
        "units",
    ];
    let requested: Vec<String> =
        fields.unwrap_or_else(|| default_session_fields(request, &all_fields));

    let mut out = Map::new();
    for field in requested {
        let normalized = normalize_key(&field);
        let value = match normalized.as_str() {
            "version" => json!(format!("SwarmOtter {}", state.build.version)),
            "rpc_version" => json!(17),
            "rpc_version_minimum" => json!(1),
            "rpc_version_semver" => json!("6.0.0"),
            "session_id" => json!(state.transmission.session_id()),
            "download_dir" => json!(default_download_dir),
            "download_dir_free_space" => json!(stats.free_space.unwrap_or(0)),
            "speed_limit_down" => json!(bytes_to_kib(cfg.bandwidth.global_download)),
            "speed_limit_down_enabled" => json!(cfg.bandwidth.global_download > 0),
            "speed_limit_up" => json!(bytes_to_kib(cfg.bandwidth.global_upload)),
            "speed_limit_up_enabled" => json!(cfg.bandwidth.global_upload > 0),
            "alt_speed_down" => json!(bytes_to_kib(cfg.bandwidth.alt_download)),
            "alt_speed_enabled" => json!(cfg.bandwidth.alt_enabled),
            "alt_speed_up" => json!(bytes_to_kib(cfg.bandwidth.alt_upload)),
            "peer_limit_global" => json!(cfg.bandwidth.max_peers),
            "peer_limit_per_torrent" => json!(cfg.bandwidth.max_peers_per_torrent),
            "peer_port" => json!(cfg.torrent.listen_port),
            "dht_enabled" => json!(cfg.dht.enabled),
            "pex_enabled" => json!(cfg.pex.enabled),
            "utp_enabled" => json!(cfg.torrent.utp_enabled),
            "preferred_transports" => {
                if cfg.torrent.utp_enabled {
                    json!(["tcp", "utp"])
                } else {
                    json!(["tcp"])
                }
            }
            "download_queue_enabled" => json!(cfg.queue.max_active_downloads > 0),
            "download_queue_size" => json!(cfg.queue.max_active_downloads),
            "seed_queue_enabled" => json!(cfg.queue.max_active_seeds > 0),
            "seed_queue_size" => json!(cfg.queue.max_active_seeds),
            "seed_ratio_limit" => json!(cfg.seeding.global_ratio_limit.unwrap_or(0.0)),
            "seed_ratio_limited" => json!(cfg.seeding.global_ratio_limit.is_some()),
            "start_added_torrents" => json!(cfg.queue.auto_start),
            "blocklist_size" => json!(0),
            "units" => json!({
                "speed_units": ["B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"],
                "speed_bytes": 1024,
                "size_units": ["B", "KiB", "MiB", "GiB", "TiB"],
                "size_bytes": 1024,
                "memory_units": ["B", "KiB", "MiB", "GiB", "TiB"],
                "memory_bytes": 1024
            }),
            _ => Value::Null,
        };
        out.insert(field, value);
    }
    Ok(Value::Object(out))
}

async fn session_set(state: &SharedState, cfg: &Config, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let mut bandwidth = cfg.bandwidth.clone();
    let mut queue = cfg.queue.clone();
    let mut seeding = cfg.seeding.clone();
    let mut changed = false;

    if let Some(limit) = u64_arg(&args, &["speed_limit_down", "speed-limit-down"]) {
        bandwidth.global_download = kib_to_bytes(limit);
        changed = true;
    }
    if let Some(enabled) = bool_arg(
        &args,
        &["speed_limit_down_enabled", "speed-limit-down-enabled"],
    ) {
        if !enabled {
            bandwidth.global_download = 0;
            changed = true;
        }
    }
    if let Some(limit) = u64_arg(&args, &["speed_limit_up", "speed-limit-up"]) {
        bandwidth.global_upload = kib_to_bytes(limit);
        changed = true;
    }
    if let Some(enabled) = bool_arg(&args, &["speed_limit_up_enabled", "speed-limit-up-enabled"]) {
        if !enabled {
            bandwidth.global_upload = 0;
            changed = true;
        }
    }
    if let Some(limit) = u64_arg(&args, &["alt_speed_down", "alt-speed-down"]) {
        bandwidth.alt_download = kib_to_bytes(limit);
        changed = true;
    }
    if let Some(limit) = u64_arg(&args, &["alt_speed_up", "alt-speed-up"]) {
        bandwidth.alt_upload = kib_to_bytes(limit);
        changed = true;
    }
    if let Some(enabled) = bool_arg(&args, &["alt_speed_enabled", "alt-speed-enabled"]) {
        bandwidth.alt_enabled = enabled;
        changed = true;
    }
    if let Some(limit) = usize_arg(&args, &["peer_limit_global", "peer-limit-global"]) {
        bandwidth.max_peers = limit;
        changed = true;
    }
    if let Some(limit) = usize_arg(&args, &["peer_limit_per_torrent", "peer-limit-per-torrent"]) {
        bandwidth.max_peers_per_torrent = limit;
        changed = true;
    }
    if let Some(size) = usize_arg(&args, &["download_queue_size", "download-queue-size"]) {
        queue.max_active_downloads = size;
        changed = true;
    }
    if let Some(size) = usize_arg(&args, &["seed_queue_size", "seed-queue-size"]) {
        queue.max_active_seeds = size;
        changed = true;
    }
    if let Some(start) = bool_arg(&args, &["start_added_torrents", "start-added-torrents"]) {
        queue.auto_start = start;
        changed = true;
    }
    if let Some(limit) = f64_arg(&args, &["seed_ratio_limit", "seed-ratio-limit"]) {
        seeding.global_ratio_limit = Some(limit);
        changed = true;
    }
    if let Some(limited) = bool_arg(&args, &["seed_ratio_limited", "seed-ratio-limited"]) {
        if !limited {
            seeding.global_ratio_limit = None;
            changed = true;
        }
    }

    if changed {
        state
            .daemon
            .update_settings(crate::state::SettingsPatch {
                bandwidth: Some(bandwidth),
                queue: Some(queue),
                seeding: Some(seeding),
            })
            .await
            .map_err(RpcFailure::from_core)?;
    }
    Ok(Value::Object(Map::new()))
}

async fn session_stats(state: &SharedState) -> RpcResult<Value> {
    let stats = state.daemon.global_stats().await;
    let stat_object = json!({
        "uploaded_bytes": stats.total_uploaded,
        "downloaded_bytes": stats.total_downloaded,
        "files_added": stats.torrent_count,
        "seconds_active": stats.uptime_seconds,
        "session_count": 1,
    });
    Ok(json!({
        "active_torrent_count": stats.active_downloads + stats.active_seeds,
        "download_speed": stats.download_rate,
        "paused_torrent_count": stats.paused,
        "torrent_count": stats.torrent_count,
        "upload_speed": stats.upload_rate,
        "cumulative_stats": stat_object,
        "current_stats": stat_object,
    }))
}

async fn torrent_get(state: &SharedState, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let fields =
        string_array_arg(&args, &["fields"]).unwrap_or_else(|| default_torrent_fields(request));
    let summaries = selected_summaries(state, &args).await?;
    let ids_recently_active = ids_is_recently_active(&args);
    let table = string_arg(&args, &["format"]).as_deref() == Some("table");

    if table {
        let mut rows = Vec::new();
        rows.push(Value::Array(
            fields
                .iter()
                .map(|field| Value::String(field.clone()))
                .collect(),
        ));
        for summary in summaries {
            let id = id_for_summary(state, &summary).await;
            let ctx = TorrentFieldContext::load(state, &summary, &fields).await;
            let row = fields
                .iter()
                .map(|field| torrent_field_value(&summary, id, field, &ctx))
                .collect();
            rows.push(Value::Array(row));
        }
        let mut out = Map::new();
        out.insert("torrents".into(), Value::Array(rows));
        if ids_recently_active {
            out.insert("removed".into(), Value::Array(Vec::new()));
        }
        return Ok(Value::Object(out));
    }

    let mut torrents = Vec::new();
    for summary in summaries {
        let id = id_for_summary(state, &summary).await;
        let ctx = TorrentFieldContext::load(state, &summary, &fields).await;
        let mut object = Map::new();
        for field in &fields {
            object.insert(
                field.clone(),
                torrent_field_value(&summary, id, field, &ctx),
            );
        }
        torrents.push(Value::Object(object));
    }
    let mut out = Map::new();
    out.insert("torrents".into(), Value::Array(torrents));
    if ids_recently_active {
        out.insert("removed".into(), Value::Array(Vec::new()));
    }
    Ok(Value::Object(out))
}

async fn torrent_add(state: &SharedState, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let download_dir = string_arg(&args, &["download_dir", "download-dir"]);
    let labels = string_array_arg(&args, &["labels"]).unwrap_or_default();
    let paused = bool_arg(&args, &["paused"]).unwrap_or(false);

    let add_result = if let Some(metainfo) = string_arg(&args, &["metainfo"]) {
        let bytes = decode_base64(&metainfo)
            .ok_or_else(|| RpcFailure::invalid("metainfo must be valid base64"))?;
        state
            .daemon
            .add_torrent_file(bytes, download_dir.clone())
            .await
    } else if let Some(filename) = string_arg(&args, &["filename"]) {
        if filename.starts_with("magnet:?") {
            state
                .daemon
                .add_magnet(&filename, download_dir.clone())
                .await
        } else if filename.starts_with("http://") || filename.starts_with("https://") {
            return Err(RpcFailure::invalid(
                "remote torrent URL fetching is not supported by the compatibility adapter",
            ));
        } else if filename.len() == 40 && filename.chars().all(|c| c.is_ascii_hexdigit()) {
            let magnet = format!("magnet:?xt=urn:btih:{filename}");
            state.daemon.add_magnet(&magnet, download_dir.clone()).await
        } else {
            return Err(RpcFailure::invalid(
                "filename must be a magnet link; use metainfo for torrent file bytes",
            ));
        }
    } else {
        return Err(RpcFailure::invalid(
            "torrent_add requires filename or metainfo",
        ));
    };

    let (hash, duplicate) = match add_result {
        Ok(hash) => (hash, false),
        Err(CoreError::DuplicateTorrent(hash)) => (
            InfoHash::from_hex(&hash).map_err(RpcFailure::from_core)?,
            true,
        ),
        Err(error) => return Err(RpcFailure::from_core(error)),
    };

    if !labels.is_empty() {
        state
            .daemon
            .set_labels(&hash, labels)
            .await
            .map_err(RpcFailure::from_core)?;
    }
    if paused {
        state
            .daemon
            .pause(&hash)
            .await
            .map_err(RpcFailure::from_core)?;
    }
    apply_file_args(state, &hash, &args).await?;

    let summary = state
        .daemon
        .get_torrent(&hash)
        .await
        .ok_or_else(|| RpcFailure::invalid("added torrent was not found"))?;
    let id = id_for_summary(state, &summary).await;
    let object = json!({
        "id": id,
        "name": summary.name,
        "hash_string": summary.info_hash.to_hex(),
        "hashString": summary.info_hash.to_hex(),
    });
    let mut out = Map::new();
    let key = if duplicate {
        if request.legacy_key_style() {
            "torrent-duplicate"
        } else {
            "torrent_duplicate"
        }
    } else if request.legacy_key_style() {
        "torrent-added"
    } else {
        "torrent_added"
    };
    out.insert(key.into(), object);
    Ok(Value::Object(out))
}

async fn torrent_remove(state: &SharedState, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let delete_data = bool_arg(&args, &["delete_local_data", "delete-local-data"]).unwrap_or(false);
    let summaries = selected_summaries(state, &args).await?;
    for summary in summaries {
        state
            .daemon
            .remove_torrent(&summary.info_hash, delete_data)
            .await
            .map_err(RpcFailure::from_core)?;
    }
    Ok(Value::Object(Map::new()))
}

#[derive(Clone, Copy)]
enum TorrentAction {
    Resume,
    StartNow,
    Stop,
    Recheck,
    Reannounce,
    QueueTop,
    QueueUp,
    QueueDown,
    QueueBottom,
}

async fn torrent_action(
    state: &SharedState,
    request: &RpcRequest,
    action: TorrentAction,
) -> RpcResult<Value> {
    let args = request.args();
    let summaries = selected_summaries(state, &args).await?;
    for summary in summaries {
        let hash = summary.info_hash;
        let result = match action {
            TorrentAction::Resume => state.daemon.resume(&hash).await,
            TorrentAction::StartNow => state.daemon.start_now(&hash).await,
            TorrentAction::Stop => state.daemon.stop(&hash).await,
            TorrentAction::Recheck => state.daemon.recheck(&hash).await,
            TorrentAction::Reannounce => state.daemon.reannounce(&hash).await,
            TorrentAction::QueueTop => state.daemon.queue_move_to_top(&hash).await,
            TorrentAction::QueueUp => state.daemon.queue_move_up(&hash).await,
            TorrentAction::QueueDown => state.daemon.queue_move_down(&hash).await,
            TorrentAction::QueueBottom => state.daemon.queue_move_to_bottom(&hash).await,
        };
        result.map_err(RpcFailure::from_core)?;
    }
    Ok(Value::Object(Map::new()))
}

async fn torrent_set(state: &SharedState, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let summaries = selected_summaries(state, &args).await?;
    for summary in summaries {
        let hash = summary.info_hash;
        if let Some(labels) = string_array_arg(&args, &["labels"]) {
            state
                .daemon
                .set_labels(&hash, labels)
                .await
                .map_err(RpcFailure::from_core)?;
        }
        if let Some(location) = string_arg(&args, &["location"]) {
            state
                .daemon
                .move_data(&hash, location)
                .await
                .map_err(RpcFailure::from_core)?;
        }

        let download_limit = torrent_limit_from_args(
            &args,
            summary.download_limit,
            &["download_limit", "download-limit"],
            &["download_limited", "download-limited"],
        );
        let upload_limit = torrent_limit_from_args(
            &args,
            summary.upload_limit,
            &["upload_limit", "upload-limit"],
            &["upload_limited", "upload-limited"],
        );
        if download_limit != summary.download_limit || upload_limit != summary.upload_limit {
            state
                .daemon
                .set_torrent_limits(
                    &hash,
                    TorrentBandwidth {
                        download: download_limit,
                        upload: upload_limit,
                    },
                )
                .await
                .map_err(RpcFailure::from_core)?;
        }

        apply_file_args(state, &hash, &args).await?;
        apply_tracker_args(state, &hash, &args).await?;
    }
    Ok(Value::Object(Map::new()))
}

async fn torrent_set_location(state: &SharedState, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let location =
        string_arg(&args, &["location"]).ok_or_else(|| RpcFailure::invalid("location required"))?;
    let summaries = selected_summaries(state, &args).await?;
    for summary in summaries {
        state
            .daemon
            .move_data(&summary.info_hash, location.clone())
            .await
            .map_err(RpcFailure::from_core)?;
    }
    Ok(Value::Object(Map::new()))
}

async fn torrent_rename_path(state: &SharedState, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let old_path =
        string_arg(&args, &["path"]).ok_or_else(|| RpcFailure::invalid("path required"))?;
    let name = string_arg(&args, &["name"]).ok_or_else(|| RpcFailure::invalid("name required"))?;
    let summaries = selected_summaries(state, &args).await?;
    for summary in summaries {
        let files = state
            .daemon
            .list_files(&summary.info_hash)
            .await
            .unwrap_or_default();
        let index = files
            .iter()
            .find(|file| file.path == old_path)
            .map(|file| file.index)
            .or_else(|| (files.len() == 1).then_some(0))
            .ok_or_else(|| RpcFailure::invalid("path did not match a torrent file"))?;
        let new_path = renamed_path(&old_path, &name);
        state
            .daemon
            .rename_path(&summary.info_hash, index, new_path)
            .await
            .map_err(RpcFailure::from_core)?;
    }
    Ok(Value::Object(Map::new()))
}

async fn free_space(state: &SharedState, cfg: &Config, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let path = string_arg(&args, &["path"]).unwrap_or_else(|| {
        cfg.storage
            .download_dir
            .clone()
            .unwrap_or_else(|| ".".to_string())
    });
    let stats = state.daemon.global_stats().await;
    let size = stats.free_space.unwrap_or(0);
    Ok(json!({
        "path": path,
        "size_bytes": size,
        "total_size": 0,
    }))
}

async fn port_test(state: &SharedState, request: &RpcRequest) -> RpcResult<Value> {
    let args = request.args();
    let ip_protocol = string_arg(&args, &["ip_protocol", "ip-protocol"]);
    let health = state.daemon.network_health().await;
    Ok(json!({
        "port_is_open": false,
        "ip_protocol": ip_protocol.unwrap_or_else(|| {
            if health.allow_ipv6 { "ipv6".into() } else { "ipv4".into() }
        }),
    }))
}

async fn selected_summaries(state: &SharedState, args: &Value) -> RpcResult<Vec<TorrentSummary>> {
    let summaries = state.daemon.list_torrents().await;
    {
        let mut ids = state.transmission.ids.lock().await;
        for summary in &summaries {
            ids.id_for(summary.info_hash);
        }
    }

    let Some(selector) = value_arg(args, &["ids"]) else {
        return Ok(summaries);
    };
    if selector
        .as_array()
        .map(|items| items.is_empty())
        .unwrap_or(false)
    {
        return Ok(summaries);
    }
    if selector
        .as_str()
        .map(is_recently_active_selector)
        .unwrap_or(false)
    {
        return Ok(summaries
            .into_iter()
            .filter(|summary| summary.state.is_active())
            .collect());
    }

    let selected = selector_values(selector)?;
    let mut hashes = Vec::new();
    let id_cache = state.transmission.ids.lock().await;
    for value in selected {
        if let Some(id) = value.as_i64() {
            if let Some(hash) = id_cache.hash_for_id(id) {
                hashes.push(hash);
            }
            continue;
        }
        if let Some(hash) = value.as_str().and_then(|s| InfoHash::from_hex(s).ok()) {
            hashes.push(hash);
        }
    }
    drop(id_cache);

    Ok(summaries
        .into_iter()
        .filter(|summary| hashes.contains(&summary.info_hash))
        .collect())
}

fn selector_values(selector: &Value) -> RpcResult<Vec<&Value>> {
    if selector.is_number() || selector.is_string() {
        return Ok(vec![selector]);
    }
    selector
        .as_array()
        .map(|items| items.iter().collect())
        .ok_or_else(|| {
            RpcFailure::invalid("ids must be an integer, hash, array, or recently_active")
        })
}

async fn id_for_summary(state: &SharedState, summary: &TorrentSummary) -> i64 {
    state
        .transmission
        .ids
        .lock()
        .await
        .id_for(summary.info_hash)
}

fn ids_is_recently_active(args: &Value) -> bool {
    value_arg(args, &["ids"])
        .and_then(Value::as_str)
        .map(is_recently_active_selector)
        .unwrap_or(false)
}

fn is_recently_active_selector(s: &str) -> bool {
    matches!(s, "recently_active" | "recently-active")
}

struct TorrentFieldContext {
    files: Option<Vec<TorrentFile>>,
    trackers: Option<Vec<TrackerInfo>>,
    peers: Option<Vec<Peer>>,
}

impl TorrentFieldContext {
    async fn load(state: &SharedState, summary: &TorrentSummary, fields: &[String]) -> Self {
        let needs_files = fields.iter().any(|field| {
            matches!(
                normalize_key(field).as_str(),
                "files" | "file_stats" | "priorities" | "wanted"
            )
        });
        let needs_trackers = fields.iter().any(|field| {
            matches!(
                normalize_key(field).as_str(),
                "trackers" | "tracker_stats" | "tracker_list"
            )
        });
        let needs_peers = fields.iter().any(|field| {
            matches!(
                normalize_key(field).as_str(),
                "peers"
                    | "peers_connected"
                    | "peers_from"
                    | "peers_getting_from_us"
                    | "peers_sending_to_us"
            )
        });
        Self {
            files: if needs_files {
                state.daemon.list_files(&summary.info_hash).await
            } else {
                None
            },
            trackers: if needs_trackers {
                state.daemon.list_trackers(&summary.info_hash).await
            } else {
                None
            },
            peers: if needs_peers {
                state.daemon.list_peers(&summary.info_hash).await
            } else {
                None
            },
        }
    }
}

fn torrent_field_value(
    summary: &TorrentSummary,
    id: i64,
    field: &str,
    ctx: &TorrentFieldContext,
) -> Value {
    match normalize_key(field).as_str() {
        "id" => json!(id),
        "name" => json!(summary.name),
        "hash_string" => json!(summary.info_hash.to_hex()),
        "status" => json!(transmission_status(summary.state)),
        "total_size" => json!(summary.total_length),
        "percent_done" | "percent_complete" => json!(clamped_progress(summary)),
        "metadata_percent_complete" => json!(metadata_progress(summary)),
        "rate_download" => json!(summary.rate_down),
        "rate_upload" => json!(summary.rate_up),
        "downloaded_ever" => json!(summary.downloaded),
        "uploaded_ever" => json!(summary.uploaded),
        "upload_ratio" => json!(summary.ratio),
        "added_date" => json!(summary.date_added),
        "done_date" => json!(summary.date_completed.unwrap_or(0)),
        "activity_date" | "edit_date" => json!(summary.date_added),
        "date_created" => json!(0),
        "left_until_done" => json!(summary.total_length.saturating_sub(summary.bytes_completed)),
        "have_valid" => json!(summary.bytes_completed.min(summary.total_length)),
        "have_unchecked" | "corrupt_ever" | "desired_available" => json!(0),
        "piece_count" => json!(summary.piece_count),
        "piece_size" => json!(summary.piece_length),
        "file_count" => json!(ctx.files.as_ref().map(Vec::len).unwrap_or(0)),
        "is_private" => json!(summary.private),
        "is_finished" => json!(matches!(
            summary.state,
            TorrentState::Completed | TorrentState::Seeding
        )),
        "is_stalled" => json!(summary.state.is_error()),
        "error" => json!(if summary.state.is_error() { 1 } else { 0 }),
        "error_string" => json!(if summary.state.is_error() {
            summary.state.as_str()
        } else {
            ""
        }),
        "labels" => json!(summary.labels),
        "download_dir" => json!(summary.download_dir.clone().unwrap_or_default()),
        "download_limit" => json!(bytes_to_kib(summary.download_limit)),
        "download_limited" => json!(summary.download_limit > 0),
        "upload_limit" => json!(bytes_to_kib(summary.upload_limit)),
        "upload_limited" => json!(summary.upload_limit > 0),
        "honors_session_limits" => json!(true),
        "bandwidth_priority" => json!(0),
        "queue_position" => json!(summary.queue_position.unwrap_or(0)),
        "magnet_link" => json!(format!(
            "magnet:?xt=urn:btih:{}&dn={}",
            summary.info_hash.to_hex(),
            summary.name
        )),
        "files" => json!(torrent_files(ctx.files.as_deref().unwrap_or(&[]))),
        "file_stats" => json!(torrent_file_stats(ctx.files.as_deref().unwrap_or(&[]))),
        "priorities" => json!(ctx
            .files
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|file| transmission_priority(file.priority))
            .collect::<Vec<_>>()),
        "wanted" => json!(ctx
            .files
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|file| file.wanted)
            .collect::<Vec<_>>()),
        "peers" => json!(torrent_peers(ctx.peers.as_deref().unwrap_or(&[]))),
        "peers_connected" => json!(ctx
            .peers
            .as_ref()
            .map(Vec::len)
            .unwrap_or(summary.active_peer_workers)),
        "peers_getting_from_us" => json!(0),
        "peers_sending_to_us" => json!(ctx
            .peers
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|peer| peer.rate_down > 0)
            .count()),
        "peers_from" => json!({
            "from_cache": 0,
            "from_dht": 0,
            "from_incoming": 0,
            "from_lpd": 0,
            "from_ltep": 0,
            "from_pex": 0,
            "from_tracker": summary.known_peers,
        }),
        "trackers" => json!(torrent_trackers(ctx.trackers.as_deref().unwrap_or(&[]))),
        "tracker_stats" => json!(torrent_tracker_stats(
            ctx.trackers.as_deref().unwrap_or(&[])
        )),
        "tracker_list" => json!(tracker_list(ctx.trackers.as_deref().unwrap_or(&[]))),
        "comment" | "creator" | "group" | "primary_mime_type" => json!(""),
        "availability" | "bytes_completed" | "webseeds" | "webseeds_ex" => json!([]),
        "pieces" => json!(""),
        "eta" | "eta_idle" | "manual_announce_time" | "max_connected_peers" | "peer_limit" => {
            json!(0)
        }
        _ => Value::Null,
    }
}

fn torrent_files(files: &[TorrentFile]) -> Vec<Value> {
    files
        .iter()
        .map(|file| {
            json!({
                "name": file.path,
                "length": file.length,
                "bytes_completed": file.bytes_completed,
                "begin_piece": 0,
                "end_piece": 0,
            })
        })
        .collect()
}

fn torrent_file_stats(files: &[TorrentFile]) -> Vec<Value> {
    files
        .iter()
        .map(|file| {
            json!({
                "bytes_completed": file.bytes_completed,
                "wanted": file.wanted,
                "priority": transmission_priority(file.priority),
            })
        })
        .collect()
}

fn torrent_peers(peers: &[Peer]) -> Vec<Value> {
    peers
        .iter()
        .map(|peer| {
            json!({
                "address": peer.ip.to_string(),
                "client_name": peer.client.clone().unwrap_or_default(),
                "clientName": peer.client.clone().unwrap_or_default(),
                "flag_str": "",
                "flagStr": "",
                "is_downloading_from": peer.rate_down > 0,
                "isDownloadingFrom": peer.rate_down > 0,
                "is_encrypted": false,
                "isEncrypted": false,
                "is_incoming": matches!(peer.direction, PeerDirection::Inbound),
                "isIncoming": matches!(peer.direction, PeerDirection::Inbound),
                "is_utp": false,
                "isUTP": false,
                "is_uploading_to": peer.rate_up > 0,
                "isUploadingTo": peer.rate_up > 0,
                "peer_is_choked": peer.flags.peer_choking,
                "peerIsChoked": peer.flags.peer_choking,
                "peer_is_interested": peer.flags.interested,
                "peerIsInterested": peer.flags.interested,
                "port": peer.port,
                "progress": peer.progress,
                "rate_to_client": peer.rate_up,
                "rateToClient": peer.rate_up,
                "rate_to_peer": peer.rate_down,
                "rateToPeer": peer.rate_down,
            })
        })
        .collect()
}

fn torrent_trackers(trackers: &[TrackerInfo]) -> Vec<Value> {
    trackers
        .iter()
        .enumerate()
        .map(|(index, tracker)| {
            json!({
                "id": index,
                "announce": tracker.url,
                "scrape": "",
                "tier": tracker.tier,
            })
        })
        .collect()
}

fn torrent_tracker_stats(trackers: &[TrackerInfo]) -> Vec<Value> {
    trackers
        .iter()
        .enumerate()
        .map(|(index, tracker)| {
            json!({
                "id": index,
                "announce": tracker.url,
                "scrape": "",
                "host": tracker.url,
                "site_name": "",
                "sitename": "",
                "tier": tracker.tier,
                "leecher_count": tracker.leechers,
                "seeder_count": tracker.seeders,
                "download_count": tracker.downloads,
                "has_announced": tracker.last_announce.is_some(),
                "last_announce_succeeded": matches!(
                    tracker.status,
                    TrackerStatus::Working | TrackerStatus::Ok
                ),
                "last_announce_time": tracker.last_announce.unwrap_or(0),
                "last_announce_result": tracker.last_error.clone().unwrap_or_default(),
                "next_announce_time": tracker.next_announce.unwrap_or(0),
            })
        })
        .collect()
}

fn tracker_list(trackers: &[TrackerInfo]) -> String {
    let mut out = String::new();
    let mut current_tier = None;
    for tracker in trackers {
        if current_tier.is_some() && current_tier != Some(tracker.tier) {
            out.push('\n');
        }
        current_tier = Some(tracker.tier);
        out.push_str(&tracker.url);
        out.push('\n');
    }
    out
}

async fn apply_file_args(state: &SharedState, hash: &InfoHash, args: &Value) -> RpcResult<()> {
    if let Some(indices) =
        indices_arg_with_all(state, hash, args, &["files_wanted", "files-wanted"]).await?
    {
        state
            .daemon
            .set_wanted(hash, indices, true)
            .await
            .map_err(RpcFailure::from_core)?;
    }
    if let Some(indices) =
        indices_arg_with_all(state, hash, args, &["files_unwanted", "files-unwanted"]).await?
    {
        state
            .daemon
            .set_wanted(hash, indices, false)
            .await
            .map_err(RpcFailure::from_core)?;
    }
    for (keys, priority) in [
        (&["priority_high", "priority-high"][..], FilePriority::High),
        (
            &["priority_normal", "priority-normal"][..],
            FilePriority::Normal,
        ),
        (&["priority_low", "priority-low"][..], FilePriority::Low),
    ] {
        if let Some(indices) = indices_arg_with_all(state, hash, args, keys).await? {
            state
                .daemon
                .set_priority(hash, indices, priority)
                .await
                .map_err(RpcFailure::from_core)?;
        }
    }
    Ok(())
}

async fn apply_tracker_args(state: &SharedState, hash: &InfoHash, args: &Value) -> RpcResult<()> {
    if let Some(urls) = string_array_arg(args, &["tracker_add", "tracker-add"]) {
        for url in urls {
            state
                .daemon
                .add_tracker(hash, url)
                .await
                .map_err(RpcFailure::from_core)?;
        }
    }
    if let Some(urls) = string_array_arg(args, &["tracker_remove", "tracker-remove"]) {
        for url in urls {
            state
                .daemon
                .remove_tracker(hash, url)
                .await
                .map_err(RpcFailure::from_core)?;
        }
    }
    if let Some(list) = string_arg(args, &["tracker_list", "tracker-list"]) {
        let desired = list
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        let current = state.daemon.list_trackers(hash).await.unwrap_or_default();
        for tracker in &current {
            if !desired.iter().any(|url| url == &tracker.url) {
                state
                    .daemon
                    .remove_tracker(hash, tracker.url.clone())
                    .await
                    .map_err(RpcFailure::from_core)?;
            }
        }
        for url in desired {
            if !current.iter().any(|tracker| tracker.url == url) {
                state
                    .daemon
                    .add_tracker(hash, url)
                    .await
                    .map_err(RpcFailure::from_core)?;
            }
        }
    }
    Ok(())
}

async fn indices_arg_with_all(
    state: &SharedState,
    hash: &InfoHash,
    args: &Value,
    keys: &[&str],
) -> RpcResult<Option<Vec<usize>>> {
    let Some(value) = value_arg(args, keys) else {
        return Ok(None);
    };
    let Some(items) = value.as_array() else {
        return Err(RpcFailure::invalid("file index selector must be an array"));
    };
    if items.is_empty() {
        let files = state.daemon.list_files(hash).await.unwrap_or_default();
        return Ok(Some(files.iter().map(|file| file.index).collect()));
    }
    let mut out = Vec::new();
    for item in items {
        let Some(index) = item.as_u64().and_then(|value| usize::try_from(value).ok()) else {
            return Err(RpcFailure::invalid(
                "file index must be an unsigned integer",
            ));
        };
        out.push(index);
    }
    Ok(Some(out))
}

fn torrent_limit_from_args(
    args: &Value,
    current: u64,
    limit_keys: &[&str],
    limited_keys: &[&str],
) -> u64 {
    if let Some(false) = bool_arg(args, limited_keys) {
        return 0;
    }
    u64_arg(args, limit_keys)
        .map(kib_to_bytes)
        .unwrap_or(current)
}

fn rpc_success(request: &RpcRequest, arguments: Value) -> Response {
    let body = if request.is_json_rpc() {
        json!({
            "jsonrpc": "2.0",
            "result": arguments,
            "id": request.id.clone().unwrap_or(Value::Null),
        })
    } else {
        let mut object = Map::new();
        object.insert("result".into(), Value::String("success".into()));
        object.insert("arguments".into(), arguments);
        if let Some(tag) = request.tag.clone() {
            object.insert("tag".into(), tag);
        }
        Value::Object(object)
    };
    json_response(StatusCode::OK, body)
}

fn rpc_error(request: &RpcRequest, error: RpcFailure) -> Response {
    let body = if request.is_json_rpc() {
        json!({
            "jsonrpc": "2.0",
            "error": {
                "code": error.code,
                "message": error.message,
            },
            "id": request.id.clone().unwrap_or(Value::Null),
        })
    } else {
        let mut object = Map::new();
        object.insert("result".into(), Value::String(error.message));
        object.insert("arguments".into(), Value::Object(Map::new()));
        if let Some(tag) = request.tag.clone() {
            object.insert("tag".into(), tag);
        }
        Value::Object(object)
    };
    json_response(StatusCode::OK, body)
}

fn json_response(status: StatusCode, value: Value) -> Response {
    let body =
        serde_json::to_vec(&value).unwrap_or_else(|_| b"{\"error\":\"serialization\"}".to_vec());
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn value_arg<'a>(args: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let object = args.as_object()?;
    keys.iter()
        .find_map(|key| object.get(*key).or_else(|| object.get(&normalize_key(key))))
}

fn string_arg(args: &Value, keys: &[&str]) -> Option<String> {
    value_arg(args, keys)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn bool_arg(args: &Value, keys: &[&str]) -> Option<bool> {
    value_arg(args, keys).and_then(Value::as_bool)
}

fn u64_arg(args: &Value, keys: &[&str]) -> Option<u64> {
    value_arg(args, keys).and_then(Value::as_u64)
}

fn usize_arg(args: &Value, keys: &[&str]) -> Option<usize> {
    u64_arg(args, keys).and_then(|value| usize::try_from(value).ok())
}

fn f64_arg(args: &Value, keys: &[&str]) -> Option<f64> {
    value_arg(args, keys).and_then(Value::as_f64)
}

fn string_array_arg(args: &Value, keys: &[&str]) -> Option<Vec<String>> {
    value_arg(args, keys).and_then(|value| {
        value.as_array().map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
    })
}

fn default_session_fields(request: &RpcRequest, fields: &[&str]) -> Vec<String> {
    if request.legacy_key_style() {
        fields.iter().map(|field| field.replace('_', "-")).collect()
    } else {
        fields.iter().map(|field| field.to_string()).collect()
    }
}

fn default_torrent_fields(request: &RpcRequest) -> Vec<String> {
    if request.legacy_key_style() {
        vec![
            "id".into(),
            "name".into(),
            "hashString".into(),
            "status".into(),
        ]
    } else {
        vec![
            "id".into(),
            "name".into(),
            "hash_string".into(),
            "status".into(),
        ]
    }
}

fn normalize_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len() + 4);
    let mut prev_was_sep = false;
    for (idx, c) in key.chars().enumerate() {
        if c == '-' {
            out.push('_');
            prev_was_sep = true;
            continue;
        }
        if c.is_ascii_uppercase() {
            if idx > 0 && !prev_was_sep {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
        prev_was_sep = c == '_';
    }
    out
}

fn basic_password(header_value: &str) -> Option<String> {
    let encoded = header_value.strip_prefix("Basic ")?;
    let bytes = decode_base64(encoded)?;
    let decoded = String::from_utf8(bytes).ok()?;
    decoded
        .split_once(':')
        .map(|(_, password)| password.to_string())
}

fn decode_base64(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0u32;
    let mut saw_padding = false;
    for c in input.chars() {
        if c.is_ascii_whitespace() {
            continue;
        }
        if c == '=' {
            saw_padding = true;
            continue;
        }
        if saw_padding {
            return None;
        }
        let value = base64_value(c)? as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    Some(out)
}

fn base64_value(c: char) -> Option<u8> {
    match c {
        'A'..='Z' => Some(c as u8 - b'A'),
        'a'..='z' => Some(26 + c as u8 - b'a'),
        '0'..='9' => Some(52 + c as u8 - b'0'),
        '+' | '-' => Some(62),
        '/' | '_' => Some(63),
        _ => None,
    }
}

fn clamped_progress(summary: &TorrentSummary) -> f64 {
    if summary.total_length == 0 {
        return 0.0;
    }
    (summary.bytes_completed as f64 / summary.total_length as f64).clamp(0.0, 1.0)
}

fn metadata_progress(summary: &TorrentSummary) -> f64 {
    if summary.state == TorrentState::DownloadingMetadata {
        0.0
    } else {
        1.0
    }
}

fn transmission_status(state: TorrentState) -> i64 {
    match state {
        TorrentState::Paused
        | TorrentState::Error
        | TorrentState::NetworkBlocked
        | TorrentState::StorageError
        | TorrentState::TrackerError => 0,
        TorrentState::Queued => 3,
        TorrentState::Checking => 2,
        TorrentState::DownloadingMetadata | TorrentState::Downloading => 4,
        TorrentState::Seeding | TorrentState::Completed => 6,
    }
}

fn transmission_priority(priority: FilePriority) -> i64 {
    match priority {
        FilePriority::High => 1,
        FilePriority::Normal => 0,
        FilePriority::Low => -1,
        FilePriority::Unwanted => 0,
    }
}

fn bytes_to_kib(bytes: u64) -> u64 {
    bytes / 1024
}

fn kib_to_bytes(kib: u64) -> u64 {
    kib.saturating_mul(1024)
}

fn renamed_path(old_path: &str, new_name: &str) -> String {
    match old_path.rsplit_once('/') {
        Some((parent, _)) if !parent.is_empty() => format!("{parent}/{new_name}"),
        _ => new_name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_supports_transmission_key_styles() {
        assert_eq!(normalize_key("hashString"), "hash_string");
        assert_eq!(normalize_key("hash-string"), "hash_string");
        assert_eq!(normalize_key("hash_string"), "hash_string");
    }

    #[test]
    fn basic_auth_password_decodes() {
        assert_eq!(
            basic_password("Basic dXNlcjpzZWNyZXQ=").as_deref(),
            Some("secret")
        );
    }

    #[test]
    fn base64_decode_handles_metainfo_payloads() {
        assert_eq!(
            decode_base64("aGVsbG8=").as_deref(),
            Some(b"hello".as_slice())
        );
    }
}
