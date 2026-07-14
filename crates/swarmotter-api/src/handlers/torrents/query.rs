// SPDX-License-Identifier: Apache-2.0

use super::*;

const DEFAULT_TORRENT_LIST_PAGE_SIZE: usize = 200;
const MAX_TORRENT_LIST_PAGE_SIZE: usize = 500;
#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentListSort {
    #[default]
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

#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TorrentListDirection {
    #[default]
    Asc,
    Desc,
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

pub(super) fn filter_torrent_rows(
    rows: Vec<TorrentSummary>,
    query: &TorrentListQuery,
) -> Vec<TorrentSummary> {
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

pub(super) fn sort_torrent_rows(
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

pub(super) fn compare_torrent_rows(
    a: &TorrentSummary,
    b: &TorrentSummary,
    sort: TorrentListSort,
) -> Ordering {
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

pub(super) fn torrent_list_counts(rows: &[TorrentSummary]) -> TorrentListCounts {
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

pub(super) fn torrent_list_groups(
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

pub(super) fn torrent_matches_search(row: &TorrentSummary, query: &str) -> bool {
    let hash = row.info_hash.to_locator();
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

pub(super) fn token_set(value: Option<&str>) -> BTreeSet<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(normalize_filter_text)
        .filter(|token| !token.is_empty())
        .collect()
}

pub(super) fn normalize_filter_text(value: impl AsRef<str>) -> String {
    value.as_ref().trim().to_ascii_lowercase()
}

pub(super) fn compare_strings(a: &str, b: &str) -> Ordering {
    normalize_filter_text(a).cmp(&normalize_filter_text(b))
}

pub(super) fn compare_f64(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

pub(super) fn health_label_key(label: &HealthLabel) -> &'static str {
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

pub(super) fn storage_root_key(row: &TorrentSummary) -> &str {
    row.download_dir
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or("default")
}

pub(super) fn label_keys(row: &TorrentSummary) -> Vec<String> {
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

pub(super) fn peer_count(row: &TorrentSummary) -> usize {
    row.active_peer_workers.max(row.known_peers)
}

pub(super) fn performance_keys(row: &TorrentSummary) -> Vec<&'static str> {
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

pub(super) fn increment_count(counts: &mut BTreeMap<String, usize>, key: &str) {
    *counts.entry(key.to_string()).or_default() += 1;
}

pub(super) fn display_group_label(key: &str) -> String {
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
