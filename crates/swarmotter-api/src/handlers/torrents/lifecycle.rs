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
pub(super) async fn require_hash(hash: &str) -> Result<InfoHash> {
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
