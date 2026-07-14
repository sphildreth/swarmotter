// SPDX-License-Identifier: Apache-2.0

//! Torrent management handlers.

use axum::{
    body::Body,
    extract::rejection::JsonRejection,
    extract::{Extension, Path, Query, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use swarmotter_core::config::StartBehavior;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::TorrentKey;
use swarmotter_core::meta::MAX_TORRENT_METADATA_BYTES;
use swarmotter_core::models::torrent::{HealthLabel, TorrentState, TorrentSummary};

use crate::encoding::{decode_base64_bounded, BoundedBase64DecodeError};
use crate::error::{err_response, into_response, ok_empty_response};
use crate::routes::{parse_hash, ConfiguredRequestBodyLimit, DeleteQuery};
use crate::state::{AddTorrentOptions, SharedState};

mod add;
mod bulk;
mod lifecycle;
mod metainfo;
mod query;
mod settings;

pub use add::{
    add_magnet, add_torrent_file, add_torrent_file_or_magnet, AddMagnetBody, AddTorrentQuery,
};
pub use bulk::{
    add_torrents, AddTorrentFileBody, AddTorrentItemFailure, AddTorrentItemResult, AddTorrentsBody,
    AddTorrentsResult,
};
pub use lifecycle::{
    get_torrent, pause, reannounce, recheck, remove_torrent, remove_torrents, resume, start_now,
    stop, RemoveTorrentsBody, RemoveTorrentsResult,
};
pub use metainfo::export_metainfo;
pub use query::{
    list_torrents, query_torrents, TorrentListCounts, TorrentListDirection, TorrentListGroup,
    TorrentListGroupBy, TorrentListQuery, TorrentListResponse, TorrentListSort,
};
pub use settings::{
    move_data, set_labels, set_limits, set_seeding, AddLabelsBody, MoveDataBody, SetLimitsBody,
};

pub(super) use add::decode_torrent_metainfo_base64;
use add::{add_options, validate_torrent_metadata_size};
use lifecycle::require_hash;

#[cfg(test)]
mod tests;
