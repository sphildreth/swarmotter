// SPDX-License-Identifier: Apache-2.0

//! Autopilot control endpoints.

use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};
use serde::Deserialize;
use swarmotter_core::autopilot::AutopilotMode;
use swarmotter_core::error::CoreError;

use crate::error::{err_response, into_response, ok_empty_response};
use crate::routes::parse_hash;
use crate::state::SharedState;

#[derive(Debug, Deserialize)]
pub struct SetAutopilotBody {
    #[serde(default)]
    pub mode: Option<AutopilotMode>,
}

pub async fn status(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.autopilot_status().await))
}

pub async fn get_torrent_autopilot(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.torrent_autopilot_decision(&h).await {
            Some(decision) => into_response(Ok(decision)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(e) => err_response(e),
    }
}

pub async fn set_torrent_autopilot(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<SetAutopilotBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state
            .daemon
            .set_torrent_autopilot_mode_override(&h, body.mode)
            .await
        {
            Ok(()) => ok_empty_response(),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}
