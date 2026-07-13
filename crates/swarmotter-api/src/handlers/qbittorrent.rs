// SPDX-License-Identifier: Apache-2.0

//! qBittorrent Web API compatibility adapter.
//!
//! This module translates a bounded `/api/v2` qBittorrent-compatible surface to
//! native daemon operations. It never creates torrent network sockets and does
//! not expose indexer, search, or discovery behavior.

use std::collections::BTreeMap;

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use swarmotter_core::config::Config;
use swarmotter_core::error::CoreError;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::models::torrent::{FilePriority, TorrentState, TorrentSummary};
use swarmotter_core::models::tracker::TrackerStatus;
use url::form_urlencoded;

use crate::routes::{constant_time_eq, parse_hash};
use crate::state::{AddTorrentOptions, SharedState};

const QBITTORRENT_WEBAPI_VERSION: &str = "2.11.4";
const SID_COOKIE: &str = "SID";

#[derive(Debug, Default, Deserialize)]
pub struct TorrentInfoQuery {
    #[serde(default)]
    pub hashes: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TorrentHashQuery {
    pub hash: String,
}

pub async fn login(State(state): State<SharedState>, headers: HeaderMap, body: Bytes) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }

    let form = parse_form(&body);
    let password = form.get("password").map(String::as_str);
    let authenticated = if cfg.api.require_auth {
        cfg.api
            .auth_token
            .as_deref()
            .zip(password)
            .map(|(expected, candidate)| {
                constant_time_eq(candidate.as_bytes(), expected.as_bytes())
            })
            .unwrap_or(false)
            || bearer_or_direct_token_valid(&headers, &cfg)
    } else {
        true
    };

    if !authenticated {
        return text_response(StatusCode::OK, "Fails.");
    }

    let cookie = format!(
        "{SID_COOKIE}={}; HttpOnly; SameSite=Lax; Path=/",
        state.qbittorrent.session_id()
    );
    let mut response = text_response(StatusCode::OK, "Ok.");
    if let Ok(value) = HeaderValue::from_str(&cookie) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

pub async fn logout(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }
    text_response(StatusCode::OK, "Ok.")
}

pub async fn version(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }
    text_response(StatusCode::OK, &format!("v{}", state.build.version))
}

pub async fn webapi_version(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }
    text_response(StatusCode::OK, QBITTORRENT_WEBAPI_VERSION)
}

pub async fn torrents_info(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<TorrentInfoQuery>,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let rows = filter_torrents(state.daemon.list_torrents().await, &query);
    json_response(
        StatusCode::OK,
        json!(rows
            .iter()
            .map(qb_torrent_info)
            .collect::<Vec<serde_json::Value>>()),
    )
}

/// Return categories inferred from configured profile mappings and existing
/// torrents. Categories remain labels in native state; this endpoint gives
/// qBittorrent automation a stable catalog without adding a second category
/// store that could drift from profile resolution.
pub async fn torrents_categories(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let default_path = cfg.storage.download_dir.clone().unwrap_or_default();
    let mut categories = BTreeMap::<String, String>::new();
    for (name, profile) in &cfg.profiles.profiles {
        categories.insert(
            name.clone(),
            profile
                .storage
                .download_dir
                .clone()
                .unwrap_or_else(|| default_path.clone()),
        );
    }
    for (label, profile_name) in &cfg.profiles.labels {
        let path = cfg
            .profiles
            .profiles
            .get(profile_name)
            .and_then(|profile| profile.storage.download_dir.clone())
            .unwrap_or_else(|| default_path.clone());
        categories.insert(label.clone(), path);
    }
    for torrent in state.daemon.list_torrents().await {
        let category = qb_category(&torrent);
        if !category.is_empty() {
            categories
                .entry(category.to_string())
                .or_insert_with(|| torrent.download_dir.unwrap_or_else(|| default_path.clone()));
        }
    }

    let body = categories
        .into_iter()
        .map(|(name, save_path)| {
            let value = json!({
                "name": name,
                "savePath": save_path,
            });
            (name, value)
        })
        .collect::<serde_json::Map<String, serde_json::Value>>();
    json_response(StatusCode::OK, serde_json::Value::Object(body))
}

pub async fn torrents_add(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let form = parse_form(&body);
    let Some(urls) = form.get("urls").filter(|value| !value.trim().is_empty()) else {
        return text_response(StatusCode::BAD_REQUEST, "missing urls");
    };
    let save_path = form
        .get("savepath")
        .or_else(|| form.get("save_path"))
        .filter(|value| !value.trim().is_empty())
        .cloned();
    let paused = form_bool(&form, "paused").or_else(|| form_bool(&form, "stopped"));
    let category = form
        .get("category")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    for url in split_urls(urls) {
        if is_remote_torrent_url(url) {
            return text_response(
                StatusCode::BAD_REQUEST,
                "remote torrent URL intake is not supported",
            );
        }
        if Magnet::parse(url).is_err() {
            return text_response(StatusCode::BAD_REQUEST, "only magnet URLs are supported");
        }
        // An omitted qBittorrent `paused`/`stopped` field is not an explicit
        // request to start. Preserve that omission so the daemon can apply a
        // label-mapped profile's initial admission behavior. An explicit
        // false still wins over that policy.
        // A configured profile with the same name as the qBittorrent category
        // is an explicit policy choice. Label mappings still work normally
        // for every other category and retain their dynamic semantics.
        let profile = category
            .as_deref()
            .and_then(|category| configured_profile_name(&cfg, category));
        let options = AddTorrentOptions::request(
            save_path.clone(),
            paused.unwrap_or(false),
            paused.is_some(),
            profile,
            // qBittorrent categories map to SwarmOtter labels. Supply it at
            // add time so a label-derived policy selects its create-time
            // storage and admission behavior.
            category.iter().cloned().collect(),
        );
        match state.daemon.add_magnet(url, options).await {
            Ok(_) => {}
            Err(error) => return core_error_text(error),
        }
    }

    text_response(StatusCode::OK, "Ok.")
}

pub async fn torrents_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let form = parse_form(&body);
    let delete_data = form_bool(&form, "deleteFiles").unwrap_or(false);
    let hashes = match resolve_hashes(&state, form.get("hashes").map(String::as_str)).await {
        Ok(hashes) => hashes,
        Err(response) => return response,
    };
    for hash in hashes {
        if let Err(error) = state.daemon.remove_torrent(&hash, delete_data).await {
            return core_error_text(error);
        }
    }
    text_response(StatusCode::OK, "Ok.")
}

pub async fn torrents_pause(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    torrent_action(state, headers, body, TorrentAction::Pause).await
}

pub async fn torrents_resume(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    torrent_action(state, headers, body, TorrentAction::Resume).await
}

pub async fn torrents_set_category(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let form = parse_form(&body);
    let category = form
        .get("category")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| vec![s.to_string()])
        .unwrap_or_default();
    // Compatibility categories are the policy-selection label. An exact
    // profile-name category is made explicit; every other category clears an
    // older explicit assignment so the configured label mapping can resolve
    // it. Storage remains governed by the native snapshot/move rules.
    let profile = category
        .first()
        .and_then(|category| configured_profile_name(&cfg, category));
    let hashes = match resolve_hashes(&state, form.get("hashes").map(String::as_str)).await {
        Ok(hashes) => hashes,
        Err(response) => return response,
    };
    for hash in hashes {
        if let Err(error) = state.daemon.set_labels(&hash, category.clone()).await {
            return core_error_text(error);
        }
        if let Err(error) = state
            .daemon
            .set_torrent_profile(&hash, profile.clone())
            .await
        {
            return core_error_text(error);
        }
    }
    text_response(StatusCode::OK, "Ok.")
}

pub async fn torrents_recheck(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    torrent_action(state, headers, body, TorrentAction::Recheck).await
}

pub async fn torrents_reannounce(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    torrent_action(state, headers, body, TorrentAction::Reannounce).await
}

/// Move one or more torrent payloads through the same native move operation
/// used by the Web UI. Compatibility callers cannot bypass storage ownership
/// or containment checks with this endpoint.
pub async fn torrents_set_location(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let form = parse_form(&body);
    let Some(location) = form
        .get("location")
        .map(String::as_str)
        .map(str::trim)
        .filter(|location| !location.is_empty())
    else {
        return text_response(StatusCode::BAD_REQUEST, "missing location");
    };
    let hashes = match resolve_hashes(&state, form.get("hashes").map(String::as_str)).await {
        Ok(hashes) => hashes,
        Err(response) => return response,
    };
    for hash in hashes {
        if let Err(error) = state.daemon.move_data(&hash, location.to_string()).await {
            return core_error_text(error);
        }
    }
    text_response(StatusCode::OK, "Ok.")
}

pub async fn torrents_rename_file(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let form = parse_form(&body);
    let hash = match form.get("hash").map(String::as_str).map(parse_hash) {
        Some(Ok(hash)) => hash,
        Some(Err(error)) => return core_error_text(error),
        None => return text_response(StatusCode::BAD_REQUEST, "missing hash"),
    };
    let Some(old_path) = form
        .get("oldPath")
        .map(String::as_str)
        .filter(|path| !path.is_empty())
    else {
        return text_response(StatusCode::BAD_REQUEST, "missing oldPath");
    };
    let Some(new_path) = form
        .get("newPath")
        .map(String::as_str)
        .filter(|path| !path.is_empty())
    else {
        return text_response(StatusCode::BAD_REQUEST, "missing newPath");
    };
    let files = match state.daemon.list_files(&hash).await {
        Some(files) => files,
        None => return text_response(StatusCode::NOT_FOUND, "torrent not found"),
    };
    let Some(file) = files.iter().find(|file| file.path == old_path) else {
        return text_response(StatusCode::NOT_FOUND, "torrent file not found");
    };
    match state
        .daemon
        .rename_path(&hash, file.index, new_path.to_string())
        .await
    {
        Ok(()) => text_response(StatusCode::OK, "Ok."),
        Err(error) => core_error_text(error),
    }
}

pub async fn torrents_properties(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<TorrentHashQuery>,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }
    let hash = match parse_hash(&query.hash) {
        Ok(hash) => hash,
        Err(error) => return core_error_text(error),
    };
    let Some(torrent) = state.daemon.get_torrent(&hash).await else {
        return text_response(StatusCode::NOT_FOUND, "torrent not found");
    };
    json_response(StatusCode::OK, qb_torrent_properties(&torrent))
}

pub async fn torrents_trackers(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<TorrentHashQuery>,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }
    let hash = match parse_hash(&query.hash) {
        Ok(hash) => hash,
        Err(error) => return core_error_text(error),
    };
    let Some(trackers) = state.daemon.list_trackers(&hash).await else {
        return text_response(StatusCode::NOT_FOUND, "torrent not found");
    };
    json_response(
        StatusCode::OK,
        json!(trackers
            .into_iter()
            .map(|tracker| json!({
                "url": tracker.url,
                "tier": tracker.tier,
                "status": qb_tracker_status(tracker.status),
                "msg": tracker.last_error.or(tracker.last_message).unwrap_or_default(),
                "num_peers": tracker.leechers,
                "num_seeds": tracker.seeders,
                "num_leeches": tracker.leechers,
                "num_downloaded": tracker.downloads,
                "next_announce": tracker.next_announce.unwrap_or(0),
                "next_scrape": 0,
            }))
            .collect::<Vec<_>>()),
    )
}

pub async fn torrents_files(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<TorrentHashQuery>,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }
    let hash = match parse_hash(&query.hash) {
        Ok(hash) => hash,
        Err(error) => return core_error_text(error),
    };
    let Some(files) = state.daemon.list_files(&hash).await else {
        return text_response(StatusCode::NOT_FOUND, "torrent not found");
    };
    json_response(
        StatusCode::OK,
        json!(files
            .into_iter()
            .map(|file| json!({
                "index": file.index,
                "name": file.path,
                "size": file.length,
                "progress": if file.length == 0 {
                    0.0
                } else {
                    (file.bytes_completed as f64 / file.length as f64).clamp(0.0, 1.0)
                },
                "priority": qb_file_priority(file.priority),
                "is_seed": false,
                "piece_range": [0, 0],
                "availability": -1.0,
            }))
            .collect::<Vec<_>>()),
    )
}

#[derive(Debug, Clone, Copy)]
enum TorrentAction {
    Pause,
    Resume,
    Recheck,
    Reannounce,
}

async fn torrent_action(
    state: SharedState,
    headers: HeaderMap,
    body: Bytes,
    action: TorrentAction,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if let Some(response) = require_enabled(&cfg) {
        return response;
    }
    if let Some(response) = require_auth(&headers, &cfg, &state) {
        return response;
    }

    let form = parse_form(&body);
    let hashes = match resolve_hashes(&state, form.get("hashes").map(String::as_str)).await {
        Ok(hashes) => hashes,
        Err(response) => return response,
    };
    for hash in hashes {
        let result = match action {
            TorrentAction::Pause => state.daemon.pause(&hash).await,
            TorrentAction::Resume => state.daemon.resume(&hash).await,
            TorrentAction::Recheck => state.daemon.recheck(&hash).await,
            TorrentAction::Reannounce => state.daemon.reannounce(&hash).await,
        };
        if let Err(error) = result {
            return core_error_text(error);
        }
    }
    text_response(StatusCode::OK, "Ok.")
}

fn require_enabled(cfg: &Config) -> Option<Response> {
    if cfg.compatibility.qbittorrent.enabled {
        return None;
    }
    Some(text_response(
        StatusCode::NOT_FOUND,
        "qBittorrent compatibility API is disabled",
    ))
}

fn require_auth(headers: &HeaderMap, cfg: &Config, state: &SharedState) -> Option<Response> {
    if !cfg.api.require_auth {
        return None;
    }
    if bearer_or_direct_token_valid(headers, cfg) || sid_cookie_valid(headers, state) {
        return None;
    }
    Some(text_response(StatusCode::FORBIDDEN, "Forbidden"))
}

fn bearer_or_direct_token_valid(headers: &HeaderMap, cfg: &Config) -> bool {
    let Some(expected) = cfg.api.auth_token.as_deref() else {
        return false;
    };
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let direct = headers
        .get("x-swarmotter-auth")
        .and_then(|v| v.to_str().ok());
    bearer
        .into_iter()
        .chain(direct)
        .any(|candidate| constant_time_eq(candidate.as_bytes(), expected.as_bytes()))
}

fn sid_cookie_valid(headers: &HeaderMap, state: &SharedState) -> bool {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|cookie| cookie.trim().split_once('='))
        .any(|(name, value)| {
            name == SID_COOKIE
                && constant_time_eq(value.as_bytes(), state.qbittorrent.session_id().as_bytes())
        })
}

fn parse_form(body: &[u8]) -> BTreeMap<String, String> {
    form_urlencoded::parse(body)
        .into_owned()
        .collect::<BTreeMap<String, String>>()
}

fn form_bool(form: &BTreeMap<String, String>, key: &str) -> Option<bool> {
    form.get(key).map(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        )
    })
}

fn split_urls(urls: &str) -> Vec<&str> {
    urls.lines()
        .flat_map(|line| line.split('\0'))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

fn is_remote_torrent_url(value: &str) -> bool {
    let lower = value.trim().to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

async fn resolve_hashes(
    state: &SharedState,
    value: Option<&str>,
) -> Result<Vec<InfoHash>, Response> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(text_response(StatusCode::BAD_REQUEST, "missing hashes"));
    };
    if value.eq_ignore_ascii_case("all") {
        return Ok(state
            .daemon
            .list_torrents()
            .await
            .into_iter()
            .map(|torrent| torrent.info_hash)
            .collect());
    }
    value
        .split('|')
        .map(str::trim)
        .filter(|hash| !hash.is_empty())
        .map(parse_hash)
        .collect::<Result<Vec<_>, _>>()
        .map_err(core_error_text)
}

fn filter_torrents(rows: Vec<TorrentSummary>, query: &TorrentInfoQuery) -> Vec<TorrentSummary> {
    let hashes = query.hashes.as_deref().map(|value| {
        value
            .split('|')
            .map(str::trim)
            .filter(|hash| !hash.is_empty())
            .collect::<Vec<_>>()
    });
    let category = query.category.as_deref().map(str::trim);
    rows.into_iter()
        .filter(|row| {
            if let Some(hashes) = hashes.as_ref() {
                if !hashes.is_empty()
                    && !hashes.iter().any(|hash| {
                        hash.eq_ignore_ascii_case("all")
                            || hash.eq_ignore_ascii_case(&row.info_hash.to_hex())
                    })
                {
                    return false;
                }
            }
            if let Some(category) = category {
                if !category.is_empty() && qb_category(row) != category {
                    return false;
                }
            }
            true
        })
        .collect()
}

fn qb_torrent_info(row: &TorrentSummary) -> serde_json::Value {
    let amount_left = row.total_length.saturating_sub(row.bytes_completed);
    json!({
        "hash": row.info_hash.to_hex(),
        "name": row.name,
        "size": row.total_length,
        "progress": row.progress(),
        "state": qb_state(row.state),
        "dlspeed": row.rate_down,
        "upspeed": row.rate_up,
        "downloaded": row.downloaded,
        "uploaded": row.uploaded,
        "amount_left": amount_left,
        "ratio": row.ratio,
        "category": qb_category(row),
        "tags": row.labels.join(","),
        "save_path": row.download_dir.clone().unwrap_or_default(),
        "content_path": qb_content_path(row),
        "added_on": row.date_added,
        "completion_on": row.date_completed.unwrap_or(0),
        "last_activity": row.date_completed.unwrap_or(row.date_added),
        "private": row.private,
        "dl_limit": row.download_limit,
        "up_limit": row.upload_limit,
        "auto_tmm": false,
        "force_start": false,
        "super_seeding": false,
        "num_leechs": row.active_peer_workers,
        "num_seeds": row.known_peers,
        "num_complete": row.known_peers,
        "num_incomplete": 0,
        "priority": row.queue_position.unwrap_or(0),
    })
}

fn qb_torrent_properties(row: &TorrentSummary) -> serde_json::Value {
    json!({
        "save_path": row.download_dir.clone().unwrap_or_default(),
        "creation_date": 0,
        "addition_date": row.date_added,
        "completion_date": row.date_completed.unwrap_or(0),
        "created_by": "",
        "comment": "",
        "total_size": row.total_length,
        "dl_speed": row.rate_down,
        "up_speed": row.rate_up,
        "dl_limit": row.download_limit,
        "up_limit": row.upload_limit,
        "time_elapsed": 0,
        "seeding_time": 0,
        "nb_connections": row.active_peer_workers,
        "nb_connections_limit": -1,
        "share_ratio": row.ratio,
        "total_downloaded": row.downloaded,
        "total_uploaded": row.uploaded,
        "total_wasted": 0,
        "private": row.private,
        "error": row.error.clone().unwrap_or_default(),
    })
}

fn qb_category(row: &TorrentSummary) -> &str {
    row.labels.first().map(String::as_str).unwrap_or("")
}

fn configured_profile_name(cfg: &Config, category: &str) -> Option<String> {
    cfg.profiles
        .profiles
        .keys()
        .find(|name| name.eq_ignore_ascii_case(category))
        .cloned()
}

fn qb_content_path(row: &TorrentSummary) -> String {
    let Some(root) = row.download_dir.as_deref() else {
        return String::new();
    };
    let root = root.trim_end_matches(['/', '\\']);
    if root.is_empty() {
        row.name.clone()
    } else {
        format!("{root}/{}", row.name)
    }
}

fn qb_tracker_status(status: TrackerStatus) -> i64 {
    match status {
        TrackerStatus::Disabled => 0,
        TrackerStatus::NotContacted => 1,
        TrackerStatus::Working | TrackerStatus::Ok => 2,
        TrackerStatus::Updating => 3,
        TrackerStatus::Error => 4,
    }
}

fn qb_file_priority(priority: FilePriority) -> i64 {
    match priority {
        FilePriority::Unwanted => 0,
        FilePriority::Low => 1,
        FilePriority::Normal => 4,
        FilePriority::High => 6,
    }
}

fn qb_state(state: TorrentState) -> &'static str {
    match state {
        TorrentState::Queued => "queuedDL",
        TorrentState::Checking => "checkingDL",
        TorrentState::DownloadingMetadata => "metaDL",
        TorrentState::Downloading => "downloading",
        TorrentState::Seeding => "uploading",
        TorrentState::Paused => "pausedDL",
        TorrentState::Completed => "pausedUP",
        TorrentState::Error | TorrentState::NetworkBlocked | TorrentState::StorageError => "error",
        TorrentState::TrackerError => "stalledDL",
    }
}

fn core_error_text(error: CoreError) -> Response {
    let status = match error {
        CoreError::NotFound(_) => StatusCode::NOT_FOUND,
        CoreError::DuplicateTorrent(_) => StatusCode::CONFLICT,
        CoreError::InvalidArgument(_)
        | CoreError::MalformedMagnet(_)
        | CoreError::MalformedTorrent(_)
        | CoreError::InvalidInfoHash(_) => StatusCode::BAD_REQUEST,
        CoreError::NetworkBlocked(_) => StatusCode::SERVICE_UNAVAILABLE,
        CoreError::HttpProtocol(_) | CoreError::HttpStatus(_) => StatusCode::BAD_GATEWAY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    text_response(status, &error.to_string())
}

fn text_response(status: StatusCode, body: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body.to_string(),
    )
        .into_response()
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}
