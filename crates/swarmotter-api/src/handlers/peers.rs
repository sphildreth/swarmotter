// SPDX-License-Identifier: Apache-2.0

//! Peer handlers.

use axum::{
    extract::{Path, State},
    response::Response,
};
use swarmotter_core::error::CoreError;

use crate::error::{err_response, into_response};
use crate::routes::parse_hash;
use crate::state::SharedState;

pub async fn list_peers(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.list_peers(&h).await {
            Some(p) => into_response(Ok(p)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(e) => err_response(e),
    }
}
