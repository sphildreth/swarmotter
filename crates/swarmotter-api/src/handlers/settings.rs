// SPDX-License-Identifier: Apache-2.0

//! Settings handlers.

use axum::{extract::State, response::Response, Json};
use swarmotter_core::config::Config;

use crate::error::{err_response, into_response, ok_empty_response};
use crate::state::{SettingsPatch, SharedState};

pub async fn get_settings(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.get_config().await))
}

pub async fn update_settings(
    State(state): State<SharedState>,
    Json(patch): Json<SettingsPatch>,
) -> Response {
    match state.daemon.update_settings(patch).await {
        Ok(()) => ok_empty_response(),
        Err(e) => err_response(e),
    }
}

// Suppress unused import.
#[allow(unused_imports)]
use Config as _;
