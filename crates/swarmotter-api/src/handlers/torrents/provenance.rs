// SPDX-License-Identifier: Apache-2.0

//! Provenance annotation, deterministic magnets, and metainfo archive export.

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use swarmotter_core::error::CoreError;
use swarmotter_core::hash::TorrentKey;
use swarmotter_core::magnet::MagnetTrackerPolicy;

use crate::error::{err_response, into_response};
use crate::state::SharedState;

use super::require_hash;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MagnetQuery {
    #[serde(default)]
    pub tracker_policy: MagnetTrackerPolicy,
    #[serde(default)]
    pub include_private_trackers: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotationBody {
    pub annotation: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportArchiveBody {
    pub hashes: Vec<String>,
}

pub async fn get_magnet(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Query(query): Query<MagnetQuery>,
) -> Response {
    match require_hash(&hash).await {
        Ok(hash) => match state
            .daemon
            .torrent_magnet(&hash, query.tracker_policy, query.include_private_trackers)
            .await
        {
            Ok(magnet) => into_response(Ok(serde_json::json!({
                "magnet": magnet,
                "tracker_policy": query.tracker_policy,
                "include_private_trackers": query.include_private_trackers,
            }))),
            Err(error) => err_response(error),
        },
        Err(error) => err_response(error),
    }
}

pub async fn set_annotation(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<AnnotationBody>,
) -> Response {
    match require_hash(&hash).await {
        Ok(hash) => match state
            .daemon
            .set_torrent_annotation(&hash, body.annotation)
            .await
        {
            Ok(summary) => into_response(Ok(summary)),
            Err(error) => err_response(error),
        },
        Err(error) => err_response(error),
    }
}

pub async fn export_archive(
    State(state): State<SharedState>,
    Json(body): Json<ExportArchiveBody>,
) -> Response {
    if body.hashes.is_empty() {
        return err_response(CoreError::InvalidArgument(
            "metainfo export requires at least one torrent".into(),
        ));
    }
    let mut hashes = Vec::with_capacity(body.hashes.len());
    for hash in body.hashes {
        match TorrentKey::from_locator(&hash) {
            Ok(key) => hashes.push(key),
            Err(error) => return err_response(error),
        }
    }
    match state.daemon.export_metainfo_archive(hashes).await {
        Ok(bytes) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/x-tar"),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"swarmotter-metainfo-export.tar\"",
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(error) => err_response(error),
    }
}
