// SPDX-License-Identifier: Apache-2.0

//! Tracker handlers.

use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};
use serde::Deserialize;
use swarmotter_core::error::CoreError;

use crate::error::{err_response, into_response, ok_empty_response};
use crate::routes::parse_hash;
use crate::state::SharedState;

#[derive(Debug, Deserialize)]
pub struct AddTrackerBody {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct EditTrackerBody {
    pub old_url: String,
    pub new_url: String,
}

pub async fn list_trackers(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.list_trackers(&h).await {
            Some(t) => into_response(Ok(t)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(e) => err_response(e),
    }
}

pub async fn add_tracker(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<AddTrackerBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.add_tracker(&h, body.url).await {
            Ok(()) => ok_empty_response(),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}

pub async fn remove_tracker(
    State(state): State<SharedState>,
    Path((hash, url)): Path<(String, String)>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.remove_tracker(&h, url).await {
            Ok(()) => ok_empty_response(),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}

pub async fn edit_tracker(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<EditTrackerBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state
            .daemon
            .edit_tracker(&h, body.old_url, body.new_url)
            .await
        {
            Ok(()) => ok_empty_response(),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}
