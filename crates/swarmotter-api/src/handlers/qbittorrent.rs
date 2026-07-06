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
use swarmotter_core::models::torrent::{TorrentState, TorrentSummary};
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
        match state
            .daemon
            .add_magnet(
                url,
                AddTorrentOptions::new(save_path.clone(), paused.unwrap_or(false)),
            )
            .await
        {
            Ok(hash) => {
                if let Some(category) = category.as_ref() {
                    if let Err(error) = state.daemon.set_labels(&hash, vec![category.clone()]).await
                    {
                        return core_error_text(error);
                    }
                }
            }
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
    let hashes = match resolve_hashes(&state, form.get("hashes").map(String::as_str)).await {
        Ok(hashes) => hashes,
        Err(response) => return response,
    };
    for hash in hashes {
        if let Err(error) = state.daemon.set_labels(&hash, category.clone()).await {
            return core_error_text(error);
        }
    }
    text_response(StatusCode::OK, "Ok.")
}

#[derive(Debug, Clone, Copy)]
enum TorrentAction {
    Pause,
    Resume,
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
        "added_on": row.date_added,
        "completion_on": row.date_completed.unwrap_or(0),
        "num_leechs": row.active_peer_workers,
        "num_seeds": row.known_peers,
        "num_complete": row.known_peers,
        "num_incomplete": 0,
        "priority": row.queue_position.unwrap_or(0),
    })
}

fn qb_category(row: &TorrentSummary) -> &str {
    row.labels.first().map(String::as_str).unwrap_or("")
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
