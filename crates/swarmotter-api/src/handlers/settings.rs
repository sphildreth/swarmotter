// SPDX-License-Identifier: Apache-2.0

//! Settings handlers.

use axum::{extract::State, response::Response, Json};
use swarmotter_core::config::Config;

use crate::error::{err_response, into_response, ok_empty_response};
use crate::state::{SettingsPatch, SharedState};

pub async fn get_settings(State(state): State<SharedState>) -> Response {
    let mut cfg = state.daemon.get_config().await;
    cfg.api.auth_token = None;
    into_response(Ok(cfg))
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

pub async fn replace_settings(
    State(state): State<SharedState>,
    Json(mut config): Json<Config>,
) -> Response {
    if config.api.auth_token.is_none() {
        config.api.auth_token = state.daemon.get_config().await.api.auth_token;
    }
    match state.daemon.replace_config(config).await {
        Ok(result) => into_response(Ok(result)),
        Err(e) => err_response(e),
    }
}
