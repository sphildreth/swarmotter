// SPDX-License-Identifier: Apache-2.0

//! Global peer-admission policy and per-peer manual-ban endpoints.

use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};
use serde::Deserialize;
use swarmotter_core::peer_filter::{ManualPeerBan, PeerFilterConfig};

use crate::error::{err_response, into_response};
use crate::routes::parse_hash;
use crate::state::SharedState;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerUnbanRequest {
    pub ip: String,
}

/// Return active direct rules/import paths, local blocklist outcomes, and
/// counters for rejected candidate/handshake peers.
pub async fn status(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.peer_filter_status().await))
}

/// Replace the complete global peer-admission policy.
pub async fn replace(
    State(state): State<SharedState>,
    Json(peer_filter): Json<PeerFilterConfig>,
) -> Response {
    into_response(state.daemon.replace_peer_filter(peer_filter).await)
}

/// Add or update a global manual IP ban from a torrent peer view.
pub async fn ban(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(ban): Json<ManualPeerBan>,
) -> Response {
    match parse_hash(&hash) {
        Ok(hash) => into_response(state.daemon.ban_peer(&hash, ban).await),
        Err(error) => err_response(error),
    }
}

/// Remove a global manual IP ban from a torrent peer view.
pub async fn unban(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(request): Json<PeerUnbanRequest>,
) -> Response {
    match parse_hash(&hash) {
        Ok(hash) => into_response(state.daemon.unban_peer(&hash, request.ip).await),
        Err(error) => err_response(error),
    }
}

/// Remove a global manual IP ban from the peer-admission settings view.
pub async fn unban_global(
    State(state): State<SharedState>,
    Json(request): Json<PeerUnbanRequest>,
) -> Response {
    into_response(state.daemon.unban_global_peer(request.ip).await)
}
