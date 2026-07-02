// SPDX-License-Identifier: Apache-2.0

//! Watch-folder handlers.

use crate::error::{err_response, into_response, ok_empty_response};
use crate::state::SharedState;
use axum::{extract::State, response::Response};

pub async fn watch_scan(State(state): State<SharedState>) -> Response {
    match state.daemon.watch_scan().await {
        Ok(()) => ok_empty_response(),
        Err(e) => err_response(e),
    }
}

pub async fn watch_history(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.watch_history().await))
}
