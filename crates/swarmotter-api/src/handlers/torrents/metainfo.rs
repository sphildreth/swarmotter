// SPDX-License-Identifier: Apache-2.0

//! Read-only export of retained original torrent metainfo.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

use crate::error::err_response;
use crate::state::SharedState;

use super::require_hash;

/// Return the exact original full `.torrent` document retained at ingestion.
///
/// This handler deliberately has no fallback to canonical BEP 9 `info` bytes
/// and never initiates metadata retrieval. If the torrent was added from a
/// magnet or predates original-metainfo retention, the daemon reports a clear
/// not-found/unavailable response instead.
pub async fn export_metainfo(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
) -> Response {
    match require_hash(&hash).await {
        Ok(hash) => match state.daemon.original_metainfo(&hash).await {
            Ok(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/x-bittorrent")],
                bytes,
            )
                .into_response(),
            Err(error) => err_response(error),
        },
        Err(error) => err_response(error),
    }
}
