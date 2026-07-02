// SPDX-License-Identifier: Apache-2.0

//! Global stats endpoint.

use crate::error::into_response;
use crate::state::SharedState;
use axum::{extract::State, response::Response};

pub async fn global_stats(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.global_stats().await))
}
