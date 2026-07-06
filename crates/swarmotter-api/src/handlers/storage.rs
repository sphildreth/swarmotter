// SPDX-License-Identifier: Apache-2.0

//! Storage diagnostics endpoints.

use axum::{extract::State, response::Response};

use crate::error::into_response;
use crate::state::SharedState;

pub async fn storage_roots(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.storage_roots().await))
}
