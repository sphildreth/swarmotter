// SPDX-License-Identifier: Apache-2.0

//! Network containment health handler.

use crate::error::into_response;
use crate::state::SharedState;
use axum::{extract::State, response::Response};

pub async fn network_health(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.network_health().await))
}
