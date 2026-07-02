// SPDX-License-Identifier: Apache-2.0

//! Global stats endpoint.

use crate::error::{err_response, into_response};
use crate::routes::parse_hash;
use crate::state::SharedState;
use axum::{
    extract::{Path, State},
    response::Response,
};
use swarmotter_core::error::CoreError;

pub async fn global_stats(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.global_stats().await))
}

pub async fn torrent_stats(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.torrent_stats(&h).await {
            Some(stats) => into_response(Ok(stats)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(e) => err_response(e),
    }
}
