// SPDX-License-Identifier: Apache-2.0

//! Master session traffic switch.

use axum::{extract::State, response::Response, Json};
use serde::{Deserialize, Serialize};

use crate::error::{err_response, into_response};
use crate::state::SharedState;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionState {
    pub paused: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionUpdate {
    pub paused: bool,
}

pub async fn get_session(State(state): State<SharedState>) -> Response {
    let paused = state.daemon.session_paused().await;
    into_response(Ok(SessionState { paused }))
}

pub async fn set_session(
    State(state): State<SharedState>,
    Json(update): Json<SessionUpdate>,
) -> Response {
    match state.daemon.set_session_paused(update.paused).await {
        Ok(()) => {
            let paused = state.daemon.session_paused().await;
            into_response(Ok(SessionState { paused }))
        }
        Err(error) => err_response(error),
    }
}
