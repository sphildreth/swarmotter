// SPDX-License-Identifier: Apache-2.0

//! API route definitions and the assembled router.

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{from_fn, from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Extension, Router,
};
use serde::Deserialize;
use swarmotter_core::hash::InfoHash;

use crate::state::SharedState;
use crate::{envelope, handlers};

const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Router-selected request limit for handlers that stream bodies instead of
/// using Axum's eager byte extractor.
#[derive(Debug, Clone, Copy)]
pub struct ConfiguredRequestBodyLimit(pub usize);

/// Build the full API router, mounted under `/api/v1`.
pub fn app_router(state: SharedState) -> Router {
    app_router_with_body_limit(state, DEFAULT_MAX_REQUEST_BODY_BYTES)
}

/// Build the full API router with an explicit request body limit.
pub fn app_router_with_body_limit(state: SharedState, max_request_body_bytes: usize) -> Router {
    let v1 = api_v1_router(state.clone(), max_request_body_bytes);
    let transmission = Router::new()
        .route("/transmission/rpc", post(handlers::transmission::rpc))
        .layer(DefaultBodyLimit::max(max_request_body_bytes));
    let qbittorrent = Router::new()
        .route("/api/v2/auth/login", post(handlers::qbittorrent::login))
        .route("/api/v2/auth/logout", post(handlers::qbittorrent::logout))
        .route("/api/v2/app/version", get(handlers::qbittorrent::version))
        .route(
            "/api/v2/app/webapiVersion",
            get(handlers::qbittorrent::webapi_version),
        )
        .route(
            "/api/v2/torrents/info",
            get(handlers::qbittorrent::torrents_info),
        )
        .route(
            "/api/v2/torrents/add",
            post(handlers::qbittorrent::torrents_add),
        )
        .route(
            "/api/v2/torrents/delete",
            post(handlers::qbittorrent::torrents_delete),
        )
        .route(
            "/api/v2/torrents/pause",
            post(handlers::qbittorrent::torrents_pause),
        )
        .route(
            "/api/v2/torrents/resume",
            post(handlers::qbittorrent::torrents_resume),
        )
        .route(
            "/api/v2/torrents/stop",
            post(handlers::qbittorrent::torrents_pause),
        )
        .route(
            "/api/v2/torrents/start",
            post(handlers::qbittorrent::torrents_resume),
        )
        .route(
            "/api/v2/torrents/setCategory",
            post(handlers::qbittorrent::torrents_set_category),
        )
        .layer(DefaultBodyLimit::max(max_request_body_bytes));

    // Router layers are applied inside-out: adding the guard after all control
    // surfaces makes it the single outermost layer. It therefore rejects an
    // unsafe browser request before native auth, Transmission auth/session
    // negotiation, qBittorrent auth, compatibility-enabled checks, body
    // extraction, or any daemon operation. See ADR-0044/ADR-0049.
    let controls = Router::new()
        .merge(transmission)
        .merge(qbittorrent)
        .nest("/api/v1", v1)
        .layer(from_fn(browser_origin_guard));

    Router::new()
        // Public health route that neither mutates nor reveals torrent data.
        .route("/health", get(handlers::health::root_health))
        .merge(controls)
        .with_state(state)
}

fn api_v1_router(state: SharedState, max_request_body_bytes: usize) -> Router<SharedState> {
    Router::new()
        // Health & version
        .route("/health", get(handlers::health::health))
        .route("/version", get(handlers::health::version))
        // Stats
        .route("/stats", get(handlers::stats::global_stats))
        // Storage
        .route("/storage/roots", get(handlers::storage::storage_roots))
        // Autopilot
        .route("/autopilot/status", get(handlers::autopilot::status))
        // Torrent management
        .route("/torrents", get(handlers::torrents::list_torrents))
        .route(
            "/torrents",
            post(handlers::torrents::add_torrent_file_or_magnet),
        )
        .route("/torrents/query", get(handlers::torrents::query_torrents))
        .route("/torrents/magnet", post(handlers::torrents::add_magnet))
        .route("/torrents/file", post(handlers::torrents::add_torrent_file))
        .route("/torrents/bulk", post(handlers::torrents::add_torrents))
        .route(
            "/torrents/remove",
            post(handlers::torrents::remove_torrents),
        )
        .route(
            "/torrents/:hash",
            get(handlers::torrents::get_torrent).delete(handlers::torrents::remove_torrent),
        )
        .route("/torrents/:hash/stats", get(handlers::stats::torrent_stats))
        .route(
            "/torrents/:hash/autopilot",
            get(handlers::autopilot::get_torrent_autopilot)
                .post(handlers::autopilot::set_torrent_autopilot),
        )
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
            "/torrents/:hash/seeding",
            put(handlers::torrents::set_seeding),
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
        .layer(Extension(ConfiguredRequestBodyLimit(
            max_request_body_bytes,
        )))
        .layer(from_fn_with_state(state.clone(), require_api_auth))
        .with_state(state)
}

/// Shared browser-origin guard applied to every control route (`/api/v1`,
/// `/transmission/rpc`, `/api/v2`) before authentication/session checks and
/// before compatibility-enabled checks. Rejects cross-site/same-site Fetch
/// Metadata and mismatched/malformed Origin headers with 403. When both browser
/// headers are absent the request continues as a non-browser client to normal
/// authentication. See ADR-0044/ADR-0049 (Phase 3).
pub async fn browser_origin_guard(req: Request<Body>, next: Next) -> Response {
    if let Some(response) = reject_unsafe_browser_request(&req) {
        return response;
    }
    next.run(req).await
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

fn reject_unsafe_browser_request(req: &Request<Body>) -> Option<Response> {
    let headers = req.headers();
    let origin = match single_header(headers, header::ORIGIN.as_str()) {
        Ok(origin) => origin,
        Err(()) => {
            return Some(browser_security_error(
                req,
                "cross_origin_forbidden",
                "Origin must be one valid UTF-8 header value",
            ));
        }
    };
    let fetch_site = match single_header(headers, "sec-fetch-site") {
        Ok(fetch_site) => fetch_site,
        Err(()) => {
            return Some(browser_security_error(
                req,
                "cross_origin_forbidden",
                "Sec-Fetch-Site must be one valid UTF-8 header value",
            ));
        }
    };

    // Fetch Metadata is an allowlist. Unknown, malformed, and differently
    // cased values are rejected rather than treated as non-browser traffic.
    if !matches!(fetch_site, None | Some("same-origin") | Some("none")) {
        return Some(browser_security_error(
            req,
            "cross_origin_forbidden",
            "cross-origin browser requests are not allowed",
        ));
    }

    if let Some(origin) = origin {
        // A serialized origin is exactly `scheme://authority`. It cannot carry
        // a list separator, whitespace, user information, a path, a query, or
        // a fragment. The scheme is deliberately not compared with the request
        // because TLS-terminating reverse proxies are supported by ADR-0044.
        if origin.eq_ignore_ascii_case("null") {
            return Some(browser_security_error(
                req,
                "cross_origin_forbidden",
                "opaque browser Origin is not allowed",
            ));
        }
        if origin.contains(',') || origin.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return Some(browser_security_error(
                req,
                "cross_origin_forbidden",
                "malformed browser Origin header",
            ));
        }
        let Some(parsed) = origin.parse::<axum::http::Uri>().ok() else {
            return Some(browser_security_error(
                req,
                "cross_origin_forbidden",
                "malformed browser Origin header",
            ));
        };
        let origin_authority = parsed.authority();
        // `http::Uri` normalizes an authority-only absolute URI to path `/`, so
        // compare the serialized suffix rather than `path_and_query()` to tell
        // `scheme://authority` from an Origin that actually supplied a suffix.
        let authority_only = origin_authority.is_some_and(|authority| {
            origin
                .find("://")
                .and_then(|separator| origin.get(separator + 3..))
                == Some(authority.as_str())
        });
        if parsed.scheme().is_none()
            || origin_authority.is_none()
            || !authority_only
            || origin_authority.is_some_and(|authority| authority.as_str().contains('@'))
        {
            return Some(browser_security_error(
                req,
                "cross_origin_forbidden",
                "malformed browser Origin header",
            ));
        }
        let request_authority = match single_header(headers, header::HOST.as_str()) {
            Ok(Some(host)) => host
                .parse::<axum::http::uri::Authority>()
                .ok()
                .filter(|authority| !authority.as_str().contains('@')),
            Ok(None) | Err(()) => None,
        };
        let same_authority = origin_authority
            .zip(request_authority.as_ref())
            .is_some_and(|(origin, request)| authority_matches(origin, request));
        if !same_authority {
            return Some(browser_security_error(
                req,
                "cross_origin_forbidden",
                "browser Origin must match the request Host",
            ));
        }
    }

    None
}

/// Read an origin-policy header without HeaderMap's first-value collapsing.
/// Duplicate field lines and non-UTF-8 values are ambiguous and fail closed.
fn single_header<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<Option<&'a str>, ()> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    value.to_str().map(Some).map_err(|_| ())
}

fn authority_matches(
    left: &axum::http::uri::Authority,
    right: &axum::http::uri::Authority,
) -> bool {
    left.host()
        .trim_end_matches('.')
        .eq_ignore_ascii_case(right.host().trim_end_matches('.'))
        && left.port_u16() == right.port_u16()
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

fn browser_security_error(req: &Request<Body>, code: &str, message: &str) -> Response {
    let path = req.uri().path();
    if path == "/transmission/rpc" {
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::json!({ "error": message }).to_string(),
        )
            .into_response();
    }
    if path.starts_with("/api/v2/") {
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "Forbidden",
        )
            .into_response();
    }
    (
        StatusCode::FORBIDDEN,
        [(header::CONTENT_TYPE, "application/json")],
        envelope::error_to_json(code, message),
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
