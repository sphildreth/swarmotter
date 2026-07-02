// SPDX-License-Identifier: Apache-2.0

//! Torrent management handlers.

use axum::{
    extract::{Path, Query, State},
    response::Response,
    Json,
};
use serde::{Deserialize, Serialize};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;

use crate::error::{err_response, into_response, ok_empty_response};
use crate::routes::{parse_hash, DeleteQuery};
use crate::state::SharedState;

#[derive(Debug, Deserialize)]
pub struct AddMagnetBody {
    pub magnet: String,
    #[serde(default)]
    pub download_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AddLabelsBody {
    pub labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct MoveDataBody {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct SetLimitsBody {
    /// Per-torrent download limit in bytes/sec (0 = unlimited).
    #[serde(default)]
    pub download_limit: u64,
    /// Per-torrent upload limit in bytes/sec (0 = unlimited).
    #[serde(default)]
    pub upload_limit: u64,
}

/// List all torrents.
pub async fn list_torrents(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.list_torrents().await))
}

/// Add via magnet (JSON body with magnet) or file (multipart). Dispatches based
/// on content-type: application/json -> magnet; multipart -> file.
pub async fn add_torrent_file_or_magnet(
    State(state): State<SharedState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let ct = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.contains("application/json") {
        match serde_json::from_slice::<AddMagnetBody>(&body) {
            Ok(b) => {
                return into_response(
                    state
                        .daemon
                        .add_magnet(&b.magnet, b.download_dir)
                        .await
                        .map(|h| h.to_hex()),
                )
            }
            Err(e) => return err_response(CoreError::InvalidArgument(e.to_string())),
        }
    }
    // Treat raw body as torrent file bytes.
    into_response(
        state
            .daemon
            .add_torrent_file(body.to_vec(), None)
            .await
            .map(|h| h.to_hex()),
    )
}

pub async fn add_magnet(
    State(state): State<SharedState>,
    Json(body): Json<AddMagnetBody>,
) -> Response {
    into_response(
        state
            .daemon
            .add_magnet(&body.magnet, body.download_dir)
            .await
            .map(|h| h.to_hex()),
    )
}

pub async fn add_torrent_file(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Response {
    into_response(
        state
            .daemon
            .add_torrent_file(body.to_vec(), None)
            .await
            .map(|h| h.to_hex()),
    )
}

async fn require_hash(hash: &str) -> Result<InfoHash> {
    parse_hash(hash)
}

pub async fn get_torrent(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
    match require_hash(&hash).await {
        Ok(h) => match state.daemon.get_torrent(&h).await {
            Some(s) => into_response(Ok(s)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(e) => err_response(e),
    }
}

pub async fn remove_torrent(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Query(q): Query<DeleteQuery>,
) -> Response {
    match require_hash(&hash).await {
        Ok(h) => into_response(
            state
                .daemon
                .remove_torrent(&h, q.delete_data.unwrap_or(false))
                .await,
        ),
        Err(e) => err_response(e),
    }
}

macro_rules! action {
    ($name:ident, $method:ident) => {
        pub async fn $name(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
            match require_hash(&hash).await {
                Ok(h) => {
                    let res = state.daemon.$method(&h).await;
                    match res {
                        Ok(()) => ok_empty_response(),
                        Err(e) => err_response(e),
                    }
                }
                Err(e) => err_response(e),
            }
        }
    };
}

action!(pause, pause);
action!(resume, resume);
action!(start_now, start_now);
action!(stop, stop);
action!(recheck, recheck);
action!(reannounce, reannounce);

pub async fn move_data(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<MoveDataBody>,
) -> Response {
    match require_hash(&hash).await {
        Ok(h) => into_response(state.daemon.move_data(&h, body.path).await),
        Err(e) => err_response(e),
    }
}

pub async fn set_labels(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<AddLabelsBody>,
) -> Response {
    match require_hash(&hash).await {
        Ok(h) => into_response(state.daemon.set_labels(&h, body.labels).await),
        Err(e) => err_response(e),
    }
}

pub async fn set_limits(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<SetLimitsBody>,
) -> Response {
    match require_hash(&hash).await {
        Ok(h) => into_response(
            state
                .daemon
                .set_torrent_limits(
                    &h,
                    swarmotter_core::bandwidth::TorrentBandwidth {
                        download: body.download_limit,
                        upload: body.upload_limit,
                    },
                )
                .await,
        ),
        Err(e) => err_response(e),
    }
}

// Suppress unused warnings for helper used across handlers.
#[allow(unused_imports)]
use Serialize as _;
