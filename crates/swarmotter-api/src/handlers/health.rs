// SPDX-License-Identifier: Apache-2.0

//! Health and version endpoints.

use axum::{extract::State, response::Response};
use swarmotter_core::models::network::NetworkHealth;

use crate::envelope;
use crate::error::{into_response, ok_empty_response};
use crate::state::SharedState;

#[derive(serde::Serialize)]
pub struct HealthBody {
    pub status: &'static str,
    pub network: NetworkHealth,
}

pub async fn root_health(State(state): State<SharedState>) -> Response {
    health(State(state)).await
}

pub async fn health(State(state): State<SharedState>) -> Response {
    let network = state.daemon.network_health().await;
    let status = if network.traffic_allowed {
        "ok"
    } else {
        "degraded"
    };
    into_response(Ok(HealthBody { status, network }))
}

#[derive(serde::Serialize)]
pub struct VersionBody {
    pub version: &'static str,
    pub commit: &'static str,
    pub target: &'static str,
    pub name: &'static str,
}

pub async fn version(State(state): State<SharedState>) -> Response {
    let b = &state.build;
    into_response(Ok(VersionBody {
        version: b.version,
        commit: b.commit,
        target: b.target,
        name: "SwarmOtter",
    }))
}

// Re-export envelope for tests.
#[allow(unused_imports)]
use envelope as _;
#[allow(unused_imports)]
use ok_empty_response as _;
