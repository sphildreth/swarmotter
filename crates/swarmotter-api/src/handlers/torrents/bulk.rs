// SPDX-License-Identifier: Apache-2.0

use super::*;

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
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
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
    let options =
        match super::add::apply_policy_add_options(options, body.profile, body.labels, &query) {
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
        let bytes = match decode_torrent_metainfo_base64(&file.metainfo) {
            Ok(bytes) => bytes,
            Err(error) => {
                failed.push(add_failure("torrent_file", index, error));
                continue;
            }
        };
        if let Err(error) = validate_torrent_metadata_size(bytes.len()) {
            failed.push(add_failure("torrent_file", index, error));
            continue;
        }
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

pub(super) fn add_failure(
    kind: &'static str,
    index: usize,
    error: CoreError,
) -> AddTorrentItemFailure {
    AddTorrentItemFailure {
        kind,
        index,
        code: error.code().as_str().to_string(),
        message: error.to_string(),
    }
}
