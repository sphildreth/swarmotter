// SPDX-License-Identifier: Apache-2.0

//! Native API endpoints for named policy profiles and explainable torrent
//! inheritance.

use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};
use serde::Deserialize;
use swarmotter_core::error::CoreError;
use swarmotter_core::policy::PolicyProfilesConfig;

use crate::error::{err_response, into_response};
use crate::routes::parse_hash;
use crate::state::SharedState;

#[derive(Debug, Deserialize)]
pub struct SetTorrentProfileBody {
    /// `null` clears the explicit assignment and resumes label/global
    /// selection. It never moves existing payload data.
    #[serde(default)]
    pub profile: Option<String>,
}

/// Return all named profiles and label mappings.
pub async fn list_profiles(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.get_config().await.profiles))
}

/// Replace the complete profile section while retaining all other daemon
/// settings. The daemon validates and persists this through its normal
/// configuration transaction.
pub async fn replace_profiles(
    State(state): State<SharedState>,
    Json(profiles): Json<PolicyProfilesConfig>,
) -> Response {
    let mut config = state.daemon.get_config().await;
    config.profiles = profiles.clone();
    match state.daemon.replace_config(config).await {
        Ok(_) => into_response(Ok(profiles)),
        Err(error) => err_response(error),
    }
}

/// Return every effective policy value plus its source layer.
pub async fn torrent_policy(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
) -> Response {
    match parse_hash(&hash) {
        Ok(hash) => match state.daemon.torrent_policy(&hash).await {
            Some(policy) => into_response(Ok(policy)),
            None => err_response(CoreError::NotFound("torrent".into())),
        },
        Err(error) => err_response(error),
    }
}

/// Set or clear an explicit profile assignment for one torrent. Profile
/// storage paths are intentionally not applied to existing data; callers use
/// the explicit move endpoint when relocation is desired.
pub async fn set_torrent_profile(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<SetTorrentProfileBody>,
) -> Response {
    if body
        .profile
        .as_deref()
        .is_some_and(|profile| profile.trim().is_empty())
    {
        return err_response(CoreError::InvalidArgument(
            "profile must not be empty when set".into(),
        ));
    }
    match parse_hash(&hash) {
        Ok(hash) => into_response(state.daemon.set_torrent_profile(&hash, body.profile).await),
        Err(error) => err_response(error),
    }
}
