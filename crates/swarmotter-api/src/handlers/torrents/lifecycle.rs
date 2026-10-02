// SPDX-License-Identifier: Apache-2.0

use super::*;

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
pub(super) async fn require_hash(hash: &str) -> Result<TorrentKey> {
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
    let requested: BTreeSet<TorrentKey> = hashes.iter().copied().collect();
    match state
        .daemon
        .remove_torrents(hashes, body.delete_data.unwrap_or(false))
        .await
    {
        Ok(removed) => {
            let removed_set: BTreeSet<TorrentKey> = removed.iter().copied().collect();
            let not_found = requested
                .difference(&removed_set)
                .map(|key| key.to_locator())
                .collect();
            into_response(Ok(RemoveTorrentsResult {
                removed: removed.into_iter().map(TorrentKey::to_locator).collect(),
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

/// Request body for bulk lifecycle actions: the selected torrent locators.
#[derive(Debug, Deserialize)]
pub struct BulkTorrentActionBody {
    pub info_hashes: Vec<String>,
}

/// One torrent in a bulk action that could not be applied.
#[derive(Debug, Serialize)]
pub struct BulkActionFailure {
    pub info_hash: String,
    pub code: String,
    pub message: String,
}

/// Per-item result of a bulk lifecycle action.
#[derive(Debug, Serialize)]
pub struct BulkTorrentActionResult {
    pub action: String,
    pub succeeded: Vec<String>,
    pub failed: Vec<BulkActionFailure>,
    pub not_found: Vec<String>,
}

/// The lifecycle operations that can be applied to a selection in one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BulkAction {
    Pause,
    Resume,
    Recheck,
    Restart,
}

impl BulkAction {
    pub fn name(self) -> &'static str {
        match self {
            BulkAction::Pause => "pause",
            BulkAction::Resume => "resume",
            BulkAction::Recheck => "recheck",
            BulkAction::Restart => "restart",
        }
    }
}

async fn apply_bulk_action(
    state: &SharedState,
    action: BulkAction,
    key: &TorrentKey,
) -> Result<()> {
    match action {
        BulkAction::Pause => state.daemon.pause(key).await,
        BulkAction::Resume => state.daemon.resume(key).await,
        BulkAction::Recheck => state.daemon.recheck(key).await,
        // Restart stops the live engine and starts the torrent again, so
        // tracking, announce, and payload state are rebuilt from the durable
        // registry.
        BulkAction::Restart => {
            state.daemon.stop(key).await?;
            state.daemon.start_now(key).await
        }
    }
}

async fn bulk_lifecycle_action(
    state: &SharedState,
    body: BulkTorrentActionBody,
    action: BulkAction,
) -> Response {
    // Parse and dedupe first so one malformed locator does not prevent valid
    // selections from being processed; malformed locators are reported as
    // per-item failures.
    let mut requested: Vec<TorrentKey> = Vec::new();
    let mut malformed: Vec<String> = Vec::new();
    for raw in body.info_hashes {
        match require_hash(&raw).await {
            Ok(hash) if !requested.contains(&hash) => requested.push(hash),
            Ok(_) => {}
            Err(_) => malformed.push(raw),
        }
    }
    let mut result = BulkTorrentActionResult {
        action: action.name().to_string(),
        succeeded: Vec::new(),
        failed: Vec::new(),
        not_found: Vec::new(),
    };
    for raw in malformed {
        result.failed.push(BulkActionFailure {
            info_hash: raw,
            code: "invalid_info_hash".to_string(),
            message: "request contained a locator that is not a valid torrent info hash".into(),
        });
    }
    for hash in requested {
        let locator = hash.to_locator();
        match apply_bulk_action(state, action, &hash).await {
            Ok(()) => result.succeeded.push(locator),
            Err(CoreError::NotFound(_)) => result.not_found.push(locator),
            Err(e) => result.failed.push(BulkActionFailure {
                info_hash: locator,
                code: e.code().to_string(),
                message: e.to_string(),
            }),
        }
    }
    into_response(Ok(result))
}

macro_rules! bulk_action {
    ($name:ident, $variant:expr) => {
        pub async fn $name(
            State(state): State<SharedState>,
            Json(body): Json<BulkTorrentActionBody>,
        ) -> Response {
            bulk_lifecycle_action(&state, body, $variant).await
        }
    };
}

bulk_action!(bulk_pause, BulkAction::Pause);
bulk_action!(bulk_resume, BulkAction::Resume);
bulk_action!(bulk_recheck, BulkAction::Recheck);
bulk_action!(bulk_restart, BulkAction::Restart);
