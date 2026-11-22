// SPDX-License-Identifier: Apache-2.0

//! Tracker handlers.

use axum::{
    extract::{Path, State},
    http::header,
    response::{IntoResponse, Response},
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

#[derive(Debug, Deserialize)]
pub struct TrackerUrlBody {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct TrackerPolicyBody {
    pub url: String,
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct TrackerCleanupBody {
    pub failure_threshold: u32,
    #[serde(default)]
    pub confirm: bool,
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

pub async fn retry_tracker(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<TrackerUrlBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.retry_tracker(&h, body.url).await {
            Ok(()) => ok_empty_response(),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}

pub async fn set_tracker_policy(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<TrackerPolicyBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state
            .daemon
            .set_tracker_enabled(&h, body.url, body.enabled)
            .await
        {
            Ok(trackers) => into_response(Ok(trackers)),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}

pub async fn cleanup_trackers_preview(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<TrackerCleanupBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state
            .daemon
            .cleanup_trackers_preview(&h, body.failure_threshold)
            .await
        {
            Ok(trackers) => into_response(Ok(trackers)),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}

pub async fn cleanup_trackers(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<TrackerCleanupBody>,
) -> Response {
    if !body.confirm {
        return err_response(CoreError::InvalidArgument(
            "tracker cleanup requires confirm: true after preview".into(),
        ));
    }
    match parse_hash(&hash) {
        Ok(h) => match state
            .daemon
            .cleanup_trackers(&h, body.failure_threshold)
            .await
        {
            Ok(trackers) => into_response(Ok(trackers)),
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}

pub async fn tracker_diagnostics(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.tracker_diagnostics(&h).await {
            Ok(text) => {
                ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], text).into_response()
            }
            Err(e) => err_response(e),
        },
        Err(e) => err_response(e),
    }
}

pub async fn list_webseeds(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
    match parse_hash(&hash) {
        Ok(h) => match state.daemon.list_webseeds(&h).await {
            Some(webseeds) => into_response(Ok(webseeds)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(e) => err_response(e),
    }
}
