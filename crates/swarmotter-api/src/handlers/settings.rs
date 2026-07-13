// SPDX-License-Identifier: Apache-2.0

//! Settings handlers.

use axum::{extract::State, response::Response, Json};
use swarmotter_core::config::Config;

use crate::error::{err_response, into_response, ok_empty_response};
use crate::state::{SettingsPatch, SharedState};

pub async fn get_settings(State(state): State<SharedState>) -> Response {
    let mut cfg = state.daemon.get_config().await;
    cfg.api.auth_token = None;
    cfg.network.socks5.password = None;
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
    let current = state.daemon.get_config().await;
    if config.api.auth_token.is_none() {
        config.api.auth_token = current.api.auth_token;
    }
    // A read view intentionally redacts the SOCKS5 password. Preserve that
    // stored value only when the username is unchanged; clearing or changing
    // the username is an intentional authentication change and must include
    // a complete new credential pair.
    if config.network.socks5.password.is_none()
        && config.network.socks5.username == current.network.socks5.username
    {
        config.network.socks5.password = current.network.socks5.password;
    }
    match state.daemon.replace_config(config).await {
        Ok(result) => into_response(Ok(result)),
        Err(e) => err_response(e),
    }
}
