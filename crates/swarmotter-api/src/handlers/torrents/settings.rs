// SPDX-License-Identifier: Apache-2.0

use super::*;

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

pub async fn set_seeding(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    body: std::result::Result<Json<serde_json::Value>, JsonRejection>,
) -> Response {
    let Json(value) = match body {
        Ok(body) => body,
        Err(error) => {
            return err_response(CoreError::InvalidArgument(format!(
                "invalid seeding policy: {}",
                error.body_text()
            )))
        }
    };
    let Some(body) = value.as_object() else {
        return err_response(CoreError::InvalidArgument(
            "seeding policy must be a JSON object".into(),
        ));
    };
    let required = ["ratio_limit", "idle_limit", "seed_forever"];
    if body.len() != required.len() || required.iter().any(|key| !body.contains_key(*key)) {
        return err_response(CoreError::InvalidArgument(
            "seeding policy PUT requires exactly ratio_limit, idle_limit, and seed_forever".into(),
        ));
    }
    let ratio_limit = match &body["ratio_limit"] {
        serde_json::Value::Null => None,
        serde_json::Value::Number(number) => number.as_f64(),
        _ => None,
    };
    if !body["ratio_limit"].is_null() && ratio_limit.is_none()
        || ratio_limit.is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return err_response(CoreError::InvalidArgument(
            "ratio_limit must be a finite non-negative number or null".into(),
        ));
    }
    let idle_limit = match &body["idle_limit"] {
        serde_json::Value::Null => None,
        serde_json::Value::Number(number) => number.as_u64(),
        _ => None,
    };
    if !body["idle_limit"].is_null() && idle_limit.is_none() {
        return err_response(CoreError::InvalidArgument(
            "idle_limit must be a non-negative integer number of seconds or null".into(),
        ));
    }
    let Some(seed_forever) = body["seed_forever"].as_bool() else {
        return err_response(CoreError::InvalidArgument(
            "seed_forever must be a boolean".into(),
        ));
    };
    match require_hash(&hash).await {
        Ok(hash) => into_response(
            state
                .daemon
                .set_torrent_seeding(
                    &hash,
                    swarmotter_core::ratio::TorrentSeeding {
                        ratio_limit,
                        idle_limit,
                        seed_forever,
                    },
                )
                .await,
        ),
        Err(error) => err_response(error),
    }
}
