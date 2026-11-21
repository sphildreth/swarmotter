// SPDX-License-Identifier: Apache-2.0

//! Local torrent creation handlers.

use crate::error::{err_response, into_response};
use crate::state::SharedState;
use axum::{extract::State, response::Response, Json};
use swarmotter_core::creation::TorrentCreationRequest;

pub async fn create_torrent(
    State(state): State<SharedState>,
    Json(request): Json<TorrentCreationRequest>,
) -> Response {
    match state.daemon.create_torrent(request).await {
        Ok(result) => into_response(Ok(result)),
        Err(error) => err_response(error),
    }
}

pub async fn creation_roots(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.torrent_creation_roots().await))
}
