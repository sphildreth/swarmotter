// SPDX-License-Identifier: Apache-2.0

//! API route definitions and the assembled router.

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{header, Request, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use swarmotter_core::hash::InfoHash;

use crate::state::SharedState;
use crate::{envelope, handlers};

const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Build the full API router, mounted under `/api/v1`.
pub fn app_router(state: SharedState) -> Router {
    app_router_with_body_limit(state, DEFAULT_MAX_REQUEST_BODY_BYTES)
}

/// Build the full API router with an explicit request body limit.
pub fn app_router_with_body_limit(state: SharedState, max_request_body_bytes: usize) -> Router {
    let v1 = api_v1_router(state.clone(), max_request_body_bytes);
    Router::new()
        .route("/health", get(handlers::health::root_health))
        .route("/transmission/rpc", post(handlers::transmission::rpc))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .nest("/api/v1", v1)
        .with_state(state)
}

fn api_v1_router(state: SharedState, max_request_body_bytes: usize) -> Router<SharedState> {
    Router::new()
        // Health & version
        .route("/health", get(handlers::health::health))
        .route("/version", get(handlers::health::version))
        // Stats
        .route("/stats", get(handlers::stats::global_stats))
        // Torrent management
        .route("/torrents", get(handlers::torrents::list_torrents))
        .route(
            "/torrents",
            post(handlers::torrents::add_torrent_file_or_magnet),
        )
        .route("/torrents/magnet", post(handlers::torrents::add_magnet))
        .route("/torrents/file", post(handlers::torrents::add_torrent_file))
        .route(
            "/torrents/:hash",
            get(handlers::torrents::get_torrent).delete(handlers::torrents::remove_torrent),
        )
        .route("/torrents/:hash/stats", get(handlers::stats::torrent_stats))
        .route("/torrents/:hash/pause", post(handlers::torrents::pause))
        .route("/torrents/:hash/resume", post(handlers::torrents::resume))
        .route("/torrents/:hash/start", post(handlers::torrents::start_now))
        .route("/torrents/:hash/stop", post(handlers::torrents::stop))
        .route("/torrents/:hash/recheck", post(handlers::torrents::recheck))
        .route(
            "/torrents/:hash/reannounce",
            post(handlers::torrents::reannounce),
        )
        .route("/torrents/:hash/move", post(handlers::torrents::move_data))
        .route(
            "/torrents/:hash/labels",
            post(handlers::torrents::set_labels),
        )
        .route(
            "/torrents/:hash/limits",
            post(handlers::torrents::set_limits),
        )
        .route(
            "/torrents/:hash/files",
            get(handlers::files::list_files).patch(handlers::files::patch_files),
        )
        .route(
            "/torrents/:hash/files/wanted",
            post(handlers::files::set_wanted),
        )
        .route(
            "/torrents/:hash/files/priority",
            post(handlers::files::set_priority),
        )
        .route(
            "/torrents/:hash/files/:index/rename",
            post(handlers::files::rename_path),
        )
        .route(
            "/torrents/:hash/trackers",
            get(handlers::trackers::list_trackers).post(handlers::trackers::add_tracker),
        )
        .route(
            "/torrents/:hash/trackers/:url",
            axum::routing::delete(handlers::trackers::remove_tracker),
        )
        .route(
            "/torrents/:hash/trackers/edit",
            post(handlers::trackers::edit_tracker),
        )
        .route("/torrents/:hash/peers", get(handlers::peers::list_peers))
        .route(
            "/torrents/:hash/queue/move-up",
            post(handlers::queue::move_up),
        )
        .route(
            "/torrents/:hash/queue/move-down",
            post(handlers::queue::move_down),
        )
        .route(
            "/torrents/:hash/queue/move-top",
            post(handlers::queue::move_top),
        )
        .route(
            "/torrents/:hash/queue/move-bottom",
            post(handlers::queue::move_bottom),
        )
        // Settings
        .route(
            "/settings",
            get(handlers::settings::get_settings)
                .patch(handlers::settings::update_settings)
                .put(handlers::settings::replace_settings),
        )
        // Network
        .route("/network/health", get(handlers::network::network_health))
        .route(
            "/network/diagnostics",
            get(handlers::diagnostics::network_diagnostics),
        )
        // Watch folders
        .route("/watch/scan", post(handlers::watch::watch_scan))
        .route("/watch/history", get(handlers::watch::watch_history))
        .route("/watch/status", get(handlers::diagnostics::watch_status))
        // Logs and health checks.
        .route("/logs/recent", get(handlers::diagnostics::recent_logs))
        .route("/doctor", get(handlers::diagnostics::doctor_report))
        .route("/reset", post(handlers::diagnostics::reset_downloads))
        // Events (SSE)
        .route("/events", get(handlers::events::sse_events))
        // WebSocket
        .route("/ws", get(handlers::events::ws_handler))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .layer(from_fn_with_state(state.clone(), require_api_auth))
        .with_state(state)
}

async fn require_api_auth(
    State(state): State<SharedState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let cfg = state.daemon.get_config().await;
    if !cfg.api.require_auth {
        return next.run(req).await;
    }
    let Some(expected) = cfg.api.auth_token.as_deref() else {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "api authentication is not configured",
        );
    };
    if request_has_token(&req, expected) {
        return next.run(req).await;
    }
    auth_error(StatusCode::UNAUTHORIZED, "missing or invalid API token")
}

pub(crate) fn request_has_token(req: &Request<Body>, expected: &str) -> bool {
    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let direct = req
        .headers()
        .get("x-swarmotter-auth")
        .and_then(|v| v.to_str().ok());
    bearer
        .into_iter()
        .chain(direct)
        .any(|candidate| constant_time_eq(candidate.as_bytes(), expected.as_bytes()))
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn auth_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        envelope::error_to_json("unauthorized", message),
    )
        .into_response()
}

/// Query params for the delete-torrent endpoint.
#[derive(Debug, Deserialize)]
pub struct DeleteQuery {
    pub delete_data: Option<bool>,
}

/// Parse an info hash from a path segment.
pub fn parse_hash(s: &str) -> swarmotter_core::error::Result<InfoHash> {
    InfoHash::from_hex(s)
}

// Suppress unused import warnings for handlers used via macro.
#[allow(unused_imports)]
use handlers as _;
