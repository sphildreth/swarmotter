// SPDX-License-Identifier: Apache-2.0

//! Trash/quarantine handlers.

use crate::error::{err_response, into_response, ok_empty_response};
use crate::state::SharedState;
use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};
use swarmotter_core::trash::{RestoreRequest, TrashSelectionRequest};

pub async fn status(State(state): State<SharedState>) -> Response {
    match state.daemon.trash_status().await {
        Ok(status) => into_response(Ok(status)),
        Err(error) => err_response(error),
    }
}

pub async fn preview(
    State(state): State<SharedState>,
    Json(request): Json<TrashSelectionRequest>,
) -> Response {
    match state.daemon.preview_trash(request).await {
        Ok(preview) => into_response(Ok(preview)),
        Err(error) => err_response(error),
    }
}

pub async fn apply(
    State(state): State<SharedState>,
    Json(request): Json<TrashSelectionRequest>,
) -> Response {
    match state.daemon.apply_trash(request).await {
        Ok(result) => into_response(Ok(result)),
        Err(error) => err_response(error),
    }
}

pub async fn restore(
    State(state): State<SharedState>,
    Path(entry_id): Path<String>,
    Json(request): Json<RestoreRequest>,
) -> Response {
    match state.daemon.restore_trash(&entry_id, request).await {
        Ok(result) => into_response(Ok(result)),
        Err(error) => err_response(error),
    }
}

pub async fn purge(State(state): State<SharedState>, Path(entry_id): Path<String>) -> Response {
    match state.daemon.purge_trash(&entry_id).await {
        Ok(()) => ok_empty_response(),
        Err(error) => err_response(error),
    }
}
