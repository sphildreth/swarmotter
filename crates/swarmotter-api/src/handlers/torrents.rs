// SPDX-License-Identifier: Apache-2.0

//! Torrent management handlers.

use axum::{
    extract::{Path, Query, State},
    response::Response,
    Json,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use swarmotter_core::config::StartBehavior;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::models::torrent::{HealthLabel, TorrentState, TorrentSummary};

use crate::encoding::decode_base64;
use crate::error::{err_response, into_response, ok_empty_response};
use crate::routes::{parse_hash, DeleteQuery};
use crate::state::{AddTorrentOptions, SharedState};

const DEFAULT_TORRENT_LIST_PAGE_SIZE: usize = 200;
const MAX_TORRENT_LIST_PAGE_SIZE: usize = 500;

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

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentListSort {
    Name,
    State,
    Health,
    #[serde(alias = "health_score")]
    HealthScore,
    Progress,
    #[serde(alias = "total_length")]
    Size,
    #[serde(alias = "rate_down", alias = "download_rate")]
    DownRate,
    #[serde(alias = "rate_up", alias = "upload_rate")]
    UpRate,
    Ratio,
    #[serde(alias = "active_peer_workers", alias = "known_peers")]
    Peers,
    #[serde(alias = "date_added")]
    Added,
    #[serde(alias = "date_completed")]
    Completed,
    Queue,
}

impl Default for TorrentListSort {
    fn default() -> Self {
        Self::Name
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TorrentListDirection {
    Asc,
    Desc,
}

impl Default for TorrentListDirection {
    fn default() -> Self {
        Self::Asc
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentListGroupBy {
    State,
    Health,
    Label,
    StorageRoot,
    Performance,
}

#[derive(Debug, Default, Deserialize)]
pub struct TorrentListQuery {
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub health: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub storage_root: Option<String>,
    #[serde(default)]
    pub performance: Option<String>,
    #[serde(default)]
    pub min_peers: Option<usize>,
    #[serde(default)]
    pub max_peers: Option<usize>,
    #[serde(default)]
    pub min_down_rate: Option<u64>,
    #[serde(default)]
    pub min_up_rate: Option<u64>,
    #[serde(default)]
    pub sort: Option<TorrentListSort>,
    #[serde(default)]
    pub dir: Option<TorrentListDirection>,
    #[serde(default)]
    pub page: Option<usize>,
    #[serde(default)]
    pub per_page: Option<usize>,
    #[serde(default)]
    pub group_by: Option<TorrentListGroupBy>,
}

#[derive(Debug, Serialize)]
pub struct TorrentListResponse {
    pub rows: Vec<TorrentSummary>,
    pub total: usize,
    pub filtered: usize,
    pub page: usize,
    pub per_page: usize,
    pub page_count: usize,
    pub sort: TorrentListSort,
    pub dir: TorrentListDirection,
    pub counts: TorrentListCounts,
    pub groups: Vec<TorrentListGroup>,
}

#[derive(Debug, Default, Serialize)]
pub struct TorrentListCounts {
    pub states: BTreeMap<String, usize>,
    pub health: BTreeMap<String, usize>,
    pub labels: BTreeMap<String, usize>,
    pub storage_roots: BTreeMap<String, usize>,
    pub performance: BTreeMap<String, usize>,
}

#[derive(Debug, Serialize)]
pub struct TorrentListGroup {
    pub key: String,
    pub label: String,
    pub count: usize,
}

/// List all torrents.
pub async fn list_torrents(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.list_torrents().await))
}

/// Query torrents with server-side filtering, sorting, pagination, counts, and
/// optional grouping for large-library Web UI views.
pub async fn query_torrents(
    State(state): State<SharedState>,
    Query(query): Query<TorrentListQuery>,
) -> Response {
    let all_rows = state.daemon.list_torrents().await;
    let total = all_rows.len();
    let mut rows = filter_torrent_rows(all_rows, &query);
    let filtered = rows.len();
    let counts = torrent_list_counts(&rows);
    let groups = torrent_list_groups(&rows, query.group_by);
    let sort = query.sort.unwrap_or_default();
    let dir = query.dir.unwrap_or_default();
    sort_torrent_rows(&mut rows, sort, dir);

    let page = query.page.unwrap_or(1).max(1);
    let per_page = query
        .per_page
        .unwrap_or(DEFAULT_TORRENT_LIST_PAGE_SIZE)
        .min(MAX_TORRENT_LIST_PAGE_SIZE);
    let page_count = if per_page == 0 {
        0
    } else {
        filtered.div_ceil(per_page)
    };
    let rows = if per_page == 0 {
        Vec::new()
    } else {
        let start = page.saturating_sub(1).saturating_mul(per_page);
        rows.into_iter().skip(start).take(per_page).collect()
    };

    into_response(Ok(TorrentListResponse {
        rows,
        total,
        filtered,
        page,
        per_page,
        page_count,
        sort,
        dir,
        counts,
        groups,
    }))
}

fn filter_torrent_rows(rows: Vec<TorrentSummary>, query: &TorrentListQuery) -> Vec<TorrentSummary> {
    let q = query.q.as_deref().map(normalize_filter_text);
    let states = token_set(query.state.as_deref());
    let health = token_set(query.health.as_deref());
    let labels = token_set(query.label.as_deref());
    let storage_roots = token_set(query.storage_root.as_deref());
    let performance = token_set(query.performance.as_deref());

    rows.into_iter()
        .filter(|row| {
            if let Some(q) = &q {
                if !torrent_matches_search(row, q) {
                    return false;
                }
            }
            if !states.is_empty() && !states.contains(row.state.as_str()) {
                return false;
            }
            if !health.is_empty() && !health.contains(health_label_key(&row.health.label)) {
                return false;
            }
            if !labels.is_empty()
                && !label_keys(row)
                    .into_iter()
                    .any(|label| labels.contains(label.as_str()))
            {
                return false;
            }
            if !storage_roots.is_empty()
                && !storage_roots.contains(normalize_filter_text(storage_root_key(row)).as_str())
            {
                return false;
            }
            if !performance.is_empty()
                && !performance_keys(row)
                    .into_iter()
                    .any(|key| performance.contains(key))
            {
                return false;
            }
            if query.min_peers.is_some_and(|min| peer_count(row) < min) {
                return false;
            }
            if query.max_peers.is_some_and(|max| peer_count(row) > max) {
                return false;
            }
            if query.min_down_rate.is_some_and(|min| row.rate_down < min) {
                return false;
            }
            if query.min_up_rate.is_some_and(|min| row.rate_up < min) {
                return false;
            }
            true
        })
        .collect()
}

fn sort_torrent_rows(
    rows: &mut [TorrentSummary],
    sort: TorrentListSort,
    dir: TorrentListDirection,
) {
    rows.sort_by(|a, b| {
        compare_torrent_rows(a, b, sort).then_with(|| compare_strings(&a.name, &b.name))
    });
    if dir == TorrentListDirection::Desc {
        rows.reverse();
    }
}

fn compare_torrent_rows(a: &TorrentSummary, b: &TorrentSummary, sort: TorrentListSort) -> Ordering {
    match sort {
        TorrentListSort::Name => compare_strings(&a.name, &b.name),
        TorrentListSort::State => a.state.as_str().cmp(b.state.as_str()),
        TorrentListSort::Health => {
            health_label_key(&a.health.label).cmp(health_label_key(&b.health.label))
        }
        TorrentListSort::HealthScore => a.health.score.cmp(&b.health.score),
        TorrentListSort::Progress => compare_f64(a.progress(), b.progress()),
        TorrentListSort::Size => a.total_length.cmp(&b.total_length),
        TorrentListSort::DownRate => a.rate_down.cmp(&b.rate_down),
        TorrentListSort::UpRate => a.rate_up.cmp(&b.rate_up),
        TorrentListSort::Ratio => compare_f64(a.ratio, b.ratio),
        TorrentListSort::Peers => peer_count(a).cmp(&peer_count(b)),
        TorrentListSort::Added => a.date_added.cmp(&b.date_added),
        TorrentListSort::Completed => a
            .date_completed
            .unwrap_or(0)
            .cmp(&b.date_completed.unwrap_or(0)),
        TorrentListSort::Queue => a
            .queue_position
            .unwrap_or(usize::MAX)
            .cmp(&b.queue_position.unwrap_or(usize::MAX)),
    }
}

fn torrent_list_counts(rows: &[TorrentSummary]) -> TorrentListCounts {
    let mut counts = TorrentListCounts::default();
    for row in rows {
        increment_count(&mut counts.states, row.state.as_str());
        increment_count(&mut counts.health, health_label_key(&row.health.label));
        for label in label_keys(row) {
            increment_count(&mut counts.labels, &label);
        }
        increment_count(&mut counts.storage_roots, storage_root_key(row));
        for key in performance_keys(row) {
            increment_count(&mut counts.performance, key);
        }
    }
    counts
}

fn torrent_list_groups(
    rows: &[TorrentSummary],
    group_by: Option<TorrentListGroupBy>,
) -> Vec<TorrentListGroup> {
    let Some(group_by) = group_by else {
        return Vec::new();
    };
    let mut groups = BTreeMap::new();
    for row in rows {
        match group_by {
            TorrentListGroupBy::State => increment_count(&mut groups, row.state.as_str()),
            TorrentListGroupBy::Health => {
                increment_count(&mut groups, health_label_key(&row.health.label))
            }
            TorrentListGroupBy::Label => {
                for label in label_keys(row) {
                    increment_count(&mut groups, &label);
                }
            }
            TorrentListGroupBy::StorageRoot => increment_count(&mut groups, storage_root_key(row)),
            TorrentListGroupBy::Performance => {
                for key in performance_keys(row) {
                    increment_count(&mut groups, key);
                }
            }
        }
    }
    groups
        .into_iter()
        .map(|(key, count)| TorrentListGroup {
            label: display_group_label(&key),
            key,
            count,
        })
        .collect()
}

fn torrent_matches_search(row: &TorrentSummary, query: &str) -> bool {
    let hash = row.info_hash.to_hex();
    let fields = [
        row.name.as_str(),
        hash.as_str(),
        row.state.as_str(),
        health_label_key(&row.health.label),
        storage_root_key(row),
    ];
    fields
        .iter()
        .any(|value| normalize_filter_text(value).contains(query))
        || row
            .labels
            .iter()
            .any(|label| normalize_filter_text(label).contains(query))
}

fn token_set(value: Option<&str>) -> BTreeSet<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(normalize_filter_text)
        .filter(|token| !token.is_empty())
        .collect()
}

fn normalize_filter_text(value: impl AsRef<str>) -> String {
    value.as_ref().trim().to_ascii_lowercase()
}

fn compare_strings(a: &str, b: &str) -> Ordering {
    normalize_filter_text(a).cmp(&normalize_filter_text(b))
}

fn compare_f64(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn health_label_key(label: &HealthLabel) -> &'static str {
    match label {
        HealthLabel::Unknown => "unknown",
        HealthLabel::NetworkBlocked => "network_blocked",
        HealthLabel::Stalled => "stalled",
        HealthLabel::Critical => "critical",
        HealthLabel::Poor => "poor",
        HealthLabel::Fair => "fair",
        HealthLabel::Good => "good",
        HealthLabel::Excellent => "excellent",
        HealthLabel::Paused => "paused",
        HealthLabel::Complete => "complete",
    }
}

fn storage_root_key(row: &TorrentSummary) -> &str {
    row.download_dir
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or("default")
}

fn label_keys(row: &TorrentSummary) -> Vec<String> {
    if row.labels.is_empty() {
        return vec!["unlabeled".to_string()];
    }
    let labels: Vec<String> = row
        .labels
        .iter()
        .map(normalize_filter_text)
        .filter(|label| !label.is_empty())
        .collect();
    if labels.is_empty() {
        vec!["unlabeled".to_string()]
    } else {
        labels
    }
}

fn peer_count(row: &TorrentSummary) -> usize {
    row.active_peer_workers.max(row.known_peers)
}

fn performance_keys(row: &TorrentSummary) -> Vec<&'static str> {
    let mut keys = Vec::new();
    if row.state.is_active() {
        keys.push("active");
    }
    if row.state.is_error() {
        keys.push("error");
    }
    if matches!(row.state, TorrentState::Completed | TorrentState::Seeding) {
        keys.push("complete");
    }
    if row.rate_down > 0 || row.rate_up > 0 {
        keys.push("transferring");
    }
    if peer_count(row) > 0 {
        keys.push("has_peers");
    } else {
        keys.push("no_peers");
    }
    if matches!(
        row.state,
        TorrentState::Downloading | TorrentState::DownloadingMetadata
    ) && row.rate_down == 0
    {
        keys.push("stalled");
    }
    if row.health.score <= 25
        && !matches!(row.state, TorrentState::Completed | TorrentState::Seeding)
    {
        keys.push("unhealthy");
    }
    keys
}

fn increment_count(counts: &mut BTreeMap<String, usize>, key: &str) {
    *counts.entry(key.to_string()).or_default() += 1;
}

fn display_group_label(key: &str) -> String {
    key.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
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
    Ok(paused.unwrap_or(matches!(start_behavior, Some(StartBehavior::Paused))))
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
