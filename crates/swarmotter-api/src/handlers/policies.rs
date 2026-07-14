// SPDX-License-Identifier: Apache-2.0

//! Native API endpoints for named policy profiles and explainable torrent
//! inheritance.

use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use std::fmt;
use swarmotter_core::config::PeerEncryptionMode;
use swarmotter_core::error::CoreError;
use swarmotter_core::policy::PolicyProfilesConfig;

use crate::error::{err_response, into_response};
use crate::routes::parse_hash;
use crate::state::{SharedState, StoragePathPreviewRequest};

#[derive(Debug, Deserialize)]
pub struct SetTorrentProfileBody {
    /// `null` clears the explicit assignment and resumes label/global
    /// selection. It never moves existing payload data.
    #[serde(default)]
    pub profile: Option<String>,
}

/// Replace one torrent's explicit peer-wire encryption override. A JSON
/// `null` value clears it and resumes profile/label/global inheritance.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetTorrentEncryptionModeBody {
    pub encryption_mode: ExplicitEncryptionMode,
}

/// A nullable value that must still be present in the request body. A direct
/// `Option<T>` field would let serde interpret an omitted key as `None`, which
/// would make `{}` accidentally clear a durable override.
#[derive(Debug)]
pub struct ExplicitEncryptionMode(pub Option<PeerEncryptionMode>);

impl<'de> Deserialize<'de> for ExplicitEncryptionMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ExplicitEncryptionModeVisitor;

        impl<'de> Visitor<'de> for ExplicitEncryptionModeVisitor {
            type Value = ExplicitEncryptionMode;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a peer encryption mode or null")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(ExplicitEncryptionMode(None))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let mode = match value {
                    "disabled" => PeerEncryptionMode::Disabled,
                    "preferred" => PeerEncryptionMode::Preferred,
                    "required" => PeerEncryptionMode::Required,
                    _ => {
                        return Err(E::unknown_variant(
                            value,
                            &["disabled", "preferred", "required"],
                        ));
                    }
                };
                Ok(ExplicitEncryptionMode(Some(mode)))
            }
        }

        // `deserialize_any` makes serde's missing-field deserializer return
        // its normal "missing field" error. Calling `Option::deserialize`
        // here would instead turn an omitted key into `None`.
        deserializer.deserialize_any(ExplicitEncryptionModeVisitor)
    }
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

/// Resolve a bounded, read-only preview of the paths a move or profile
/// assignment would use. The daemon performs no filesystem or network I/O
/// here, so operators can inspect placement before applying a change.
pub async fn storage_path_preview(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(request): Json<StoragePathPreviewRequest>,
) -> Response {
    match parse_hash(&hash) {
        Ok(hash) => into_response(
            state
                .daemon
                .preview_torrent_storage_paths(&hash, request)
                .await,
        ),
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

/// Set or clear the durable peer-wire encryption override for one torrent.
/// Active download/metadata work is rebuilt only when the resulting effective
/// mode differs from the previous policy.
pub async fn set_torrent_encryption_mode(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    Json(body): Json<SetTorrentEncryptionModeBody>,
) -> Response {
    match parse_hash(&hash) {
        Ok(hash) => into_response(
            state
                .daemon
                .set_torrent_encryption_mode(&hash, body.encryption_mode.0)
                .await,
        ),
        Err(error) => err_response(error),
    }
}
