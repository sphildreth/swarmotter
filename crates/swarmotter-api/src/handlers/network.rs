// SPDX-License-Identifier: Apache-2.0

//! Network containment health handler.

use crate::error::into_response;
use crate::state::SharedState;
use axum::{extract::State, response::Response};
use swarmotter_core::models::network::NetworkHealth;
use swarmotter_core::port_mapping::PortMappingStatus;
use swarmotter_core::port_test::PortTestStatus;

/// Existing containment fields are flattened to retain the established
/// `/network/health` response shape while adding the independent, optional
/// listener reachability diagnostic.
#[derive(serde::Serialize)]
pub struct NetworkHealthResponse {
    #[serde(flatten)]
    pub health: NetworkHealth,
    pub port_mapping: PortMappingStatus,
    pub port_test: PortTestStatus,
}

pub async fn network_health(State(state): State<SharedState>) -> Response {
    let health = state.daemon.network_health().await;
    let port_mapping = state.daemon.port_mapping_status().await;
    let port_test = state.daemon.port_test_status().await;
    into_response(Ok(NetworkHealthResponse {
        health,
        port_mapping,
        port_test,
    }))
}

/// Return the current opt-in router-mapping snapshot without sending traffic.
pub async fn port_mapping_status(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.port_mapping_status().await))
}

/// Force an immediate contained router-mapping reconciliation. A router
/// failure remains visible status data and never changes torrent lifecycle.
pub async fn refresh_port_mapping(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.refresh_port_mapping().await))
}

/// Run the operator-configured reachability test, or return its fresh cache.
/// A failed result is diagnostic data, not an API failure or lifecycle block.
pub async fn port_test(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.run_port_test().await))
}
