// SPDX-License-Identifier: Apache-2.0

//! File handlers.

use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};
use serde::Deserialize;
use swarmotter_core::error::CoreError;
use swarmotter_core::models::torrent::FilePriority;

use crate::error::{err_response, into_response, ok_empty_response};
use crate::routes::parse_hash;
use crate::state::SharedState;

#[derive(Debug, Deserialize)]
pub struct SetWantedBody {
    pub file_indices: Vec<usize>,
    pub wanted: bool,
}

#[derive(Debug, Deserialize)]
pub struct SetPriorityBody {
    pub file_indices: Vec<usize>,
    pub priority: FilePriority,
}

#[derive(Debug, Deserialize)]
pub struct RenameBody {
    pub new_path: String,
}

pub async fn list_files(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.list_files(&h).await {
            Some(f) => into_response(Ok(f)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(e) => err_response(e),
    }
}

pub async fn patch_files(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<SetWantedBody>,
) -> Response {
    set_wanted(State(state), Path(hash), Json(body)).await
}

pub async fn set_wanted(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<SetWantedBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => {
            let res = state
                .daemon
                .set_wanted(&h, body.file_indices, body.wanted)
                .await;
            match res {
                Ok(()) => ok_empty_response(),
                Err(e) => err_response(e),
            }
        }
        Err(e) => err_response(e),
    }
}

pub async fn set_priority(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<SetPriorityBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => {
            let res = state
                .daemon
                .set_priority(&h, body.file_indices, body.priority)
                .await;
            match res {
                Ok(()) => ok_empty_response(),
                Err(e) => err_response(e),
            }
        }
        Err(e) => err_response(e),
    }
}

pub async fn rename_path(
    State(state): State<SharedState>,
    Path((hash, index)): Path<(String, usize)>,
    Json(body): Json<RenameBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => into_response(state.daemon.rename_path(&h, index, body.new_path).await),
        Err(e) => err_response(e),
    }
}
