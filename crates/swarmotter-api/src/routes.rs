// SPDX-License-Identifier: Apache-2.0

//! API route definitions and the assembled router.

use axum::{
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use swarmotter_core::hash::InfoHash;

use crate::handlers;
use crate::state::SharedState;

/// Build the full API router, mounted under `/api/v1`.
pub fn app_router(state: SharedState) -> Router {
    let v1 = api_v1_router(state.clone());
    Router::new()
        .route("/health", get(handlers::health::root_health))
        .nest("/api/v1", v1)
        .with_state(state)
}

fn api_v1_router(state: SharedState) -> Router<SharedState> {
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
            get(handlers::settings::get_settings).patch(handlers::settings::update_settings),
        )
        // Network
        .route("/network/health", get(handlers::network::network_health))
        // Watch folders
        .route("/watch/scan", post(handlers::watch::watch_scan))
        .route("/watch/history", get(handlers::watch::watch_history))
        // Events (SSE)
        .route("/events", get(handlers::events::sse_events))
        // WebSocket
        .route("/ws", get(handlers::events::ws_handler))
        .with_state(state)
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
