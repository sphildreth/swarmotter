// SPDX-License-Identifier: Apache-2.0

//! Torrent management handlers.

use axum::{
    extract::{Path, Query, State},
    response::Response,
    Json,
};
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use swarmotter_core::config::StartBehavior;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;

use crate::encoding::decode_base64;
use crate::error::{err_response, into_response, ok_empty_response};
use crate::routes::{parse_hash, DeleteQuery};
use crate::state::{AddTorrentOptions, SharedState};

#[derive(Debug, Deserialize)]
pub struct AddMagnetBody {
    pub magnet: String,
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub start_behavior: Option<StartBehavior>,
}

#[derive(Debug, Deserialize)]
pub struct AddTorrentsBody {
    #[serde(default)]
    pub magnets: Vec<String>,
    #[serde(default)]
    pub torrent_files: Vec<AddTorrentFileBody>,
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub start_behavior: Option<StartBehavior>,
}

#[derive(Debug, Deserialize)]
pub struct AddTorrentFileBody {
    pub metainfo: String,
}

#[derive(Debug, Serialize)]
pub struct AddTorrentsResult {
    pub added: Vec<AddTorrentItemResult>,
    pub failed: Vec<AddTorrentItemFailure>,
}

#[derive(Debug, Serialize)]
pub struct AddTorrentItemResult {
    pub kind: &'static str,
    pub index: usize,
    pub info_hash: String,
}

#[derive(Debug, Serialize)]
pub struct AddTorrentItemFailure {
    pub kind: &'static str,
    pub index: usize,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct AddTorrentQuery {
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub start_behavior: Option<StartBehavior>,
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

#[derive(Debug, Deserialize)]
pub struct RemoveTorrentsBody {
    pub info_hashes: Vec<String>,
    #[serde(default)]
    pub delete_data: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct RemoveTorrentsResult {
    pub removed: Vec<String>,
    pub not_found: Vec<String>,
}

/// List all torrents.
pub async fn list_torrents(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.list_torrents().await))
}

/// Add via magnet (JSON body with magnet) or file (multipart). Dispatches based
/// on content-type: application/json -> magnet; multipart -> file.
pub async fn add_torrent_file_or_magnet(
    State(state): State<SharedState>,
    Query(query): Query<AddTorrentQuery>,
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
                let options = match add_options(
                    b.download_dir.clone(),
                    b.paused,
                    b.start_behavior,
                    Some(&query),
                ) {
                    Ok(options) => options,
                    Err(e) => return err_response(e),
                };
                return into_response(
                    state
                        .daemon
                        .add_magnet(&b.magnet, options)
                        .await
                        .map(|h| h.to_hex()),
                );
            }
            Err(e) => return err_response(CoreError::InvalidArgument(e.to_string())),
        }
    }
    // Treat raw body as torrent file bytes.
    let options = match add_options(None, None, None, Some(&query)) {
        Ok(options) => options,
        Err(e) => return err_response(e),
    };
    into_response(
        state
            .daemon
            .add_torrent_file(body.to_vec(), options)
            .await
            .map(|h| h.to_hex()),
    )
}

pub async fn add_magnet(
    State(state): State<SharedState>,
    Query(query): Query<AddTorrentQuery>,
    Json(body): Json<AddMagnetBody>,
) -> Response {
    let options = match add_options(
        body.download_dir.clone(),
        body.paused,
        body.start_behavior,
        Some(&query),
    ) {
        Ok(options) => options,
        Err(e) => return err_response(e),
    };
    into_response(
        state
            .daemon
            .add_magnet(&body.magnet, options)
            .await
            .map(|h| h.to_hex()),
    )
}

pub async fn add_torrent_file(
    State(state): State<SharedState>,
    Query(query): Query<AddTorrentQuery>,
    body: axum::body::Bytes,
) -> Response {
    let options = match add_options(None, None, None, Some(&query)) {
        Ok(options) => options,
        Err(e) => return err_response(e),
    };
    into_response(
        state
            .daemon
            .add_torrent_file(body.to_vec(), options)
            .await
            .map(|h| h.to_hex()),
    )
}

pub async fn add_torrents(
    State(state): State<SharedState>,
    Query(query): Query<AddTorrentQuery>,
    Json(body): Json<AddTorrentsBody>,
) -> Response {
    if body.magnets.is_empty() && body.torrent_files.is_empty() {
        return err_response(CoreError::InvalidArgument(
            "bulk add requires magnets or torrent_files".into(),
        ));
    }
    let options = match add_options(
        body.download_dir.clone(),
        body.paused,
        body.start_behavior,
        Some(&query),
    ) {
        Ok(options) => options,
        Err(e) => return err_response(e),
    };

    let mut added = Vec::new();
    let mut failed = Vec::new();
    for (index, magnet) in body.magnets.into_iter().enumerate() {
        match state.daemon.add_magnet(&magnet, options.clone()).await {
            Ok(hash) => added.push(AddTorrentItemResult {
                kind: "magnet",
                index,
                info_hash: hash.to_hex(),
            }),
            Err(e) => failed.push(add_failure("magnet", index, e)),
        }
    }
    for (index, file) in body.torrent_files.into_iter().enumerate() {
        let Some(bytes) = decode_base64(&file.metainfo) else {
            failed.push(add_failure(
                "torrent_file",
                index,
                CoreError::InvalidArgument("metainfo must be valid base64".into()),
            ));
            continue;
        };
        match state.daemon.add_torrent_file(bytes, options.clone()).await {
            Ok(hash) => added.push(AddTorrentItemResult {
                kind: "torrent_file",
                index,
                info_hash: hash.to_hex(),
            }),
            Err(e) => failed.push(add_failure("torrent_file", index, e)),
        }
    }

    into_response(Ok(AddTorrentsResult { added, failed }))
}

fn add_failure(kind: &'static str, index: usize, error: CoreError) -> AddTorrentItemFailure {
    AddTorrentItemFailure {
        kind,
        index,
        code: error.code().as_str().to_string(),
        message: error.to_string(),
    }
}

fn add_options(
    download_dir: Option<String>,
    body_paused: Option<bool>,
    body_start_behavior: Option<StartBehavior>,
    query: Option<&AddTorrentQuery>,
) -> Result<AddTorrentOptions> {
    let paused = merge_paused(body_paused, query.and_then(|q| q.paused), "paused")?;
    let start_behavior =
        merge_start_behavior(body_start_behavior, query.and_then(|q| q.start_behavior))?;
    Ok(AddTorrentOptions::new(
        download_dir,
        resolve_start_paused(paused, start_behavior)?,
    ))
}

fn merge_paused(body: Option<bool>, query: Option<bool>, field: &str) -> Result<Option<bool>> {
    match (body, query) {
        (Some(a), Some(b)) if a != b => Err(CoreError::InvalidArgument(format!(
            "body and query {field} values conflict"
        ))),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        _ => Ok(None),
    }
}

fn merge_start_behavior(
    body: Option<StartBehavior>,
    query: Option<StartBehavior>,
) -> Result<Option<StartBehavior>> {
    match (body, query) {
        (Some(a), Some(b)) if !start_behavior_eq(a, b) => Err(CoreError::InvalidArgument(
            "body and query start_behavior values conflict".into(),
        )),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        _ => Ok(None),
    }
}

fn start_behavior_eq(a: StartBehavior, b: StartBehavior) -> bool {
    matches!(
        (a, b),
        (StartBehavior::Start, StartBehavior::Start)
            | (StartBehavior::Paused, StartBehavior::Paused)
    )
}

fn resolve_start_paused(
    paused: Option<bool>,
    start_behavior: Option<StartBehavior>,
) -> Result<bool> {
    if let (Some(paused), Some(start_behavior)) = (paused, start_behavior) {
        let behavior_paused = matches!(start_behavior, StartBehavior::Paused);
        if paused != behavior_paused {
            return Err(CoreError::InvalidArgument(
                "paused and start_behavior values conflict".into(),
            ));
        }
    }
    Ok(paused.unwrap_or_else(|| matches!(start_behavior, Some(StartBehavior::Paused))))
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

pub async fn remove_torrents(
    State(state): State<SharedState>,
    Json(body): Json<RemoveTorrentsBody>,
) -> Response {
    let mut hashes = Vec::new();
    for raw in body.info_hashes {
        match require_hash(&raw).await {
            Ok(hash) if !hashes.contains(&hash) => hashes.push(hash),
            Ok(_) => {}
            Err(e) => return err_response(e),
        }
    }
    let requested: BTreeSet<InfoHash> = hashes.iter().copied().collect();
    match state
        .daemon
        .remove_torrents(hashes, body.delete_data.unwrap_or(false))
        .await
    {
        Ok(removed) => {
            let removed_set: BTreeSet<InfoHash> = removed.iter().copied().collect();
            let not_found = requested
                .difference(&removed_set)
                .map(InfoHash::to_hex)
                .collect();
            into_response(Ok(RemoveTorrentsResult {
                removed: removed.into_iter().map(|hash| hash.to_hex()).collect(),
                not_found,
            }))
        }
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
