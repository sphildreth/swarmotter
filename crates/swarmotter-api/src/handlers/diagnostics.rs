// SPDX-License-Identifier: Apache-2.0

//! Observability and system health diagnostics endpoints.

use axum::{
    extract::{Query, State},
    response::Response,
};
use serde::Deserialize;

use crate::error::into_response;
use crate::state::SharedState;

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    lines: Option<usize>,
}

pub async fn network_diagnostics(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.network_diagnostics().await))
}

pub async fn watch_status(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.watch_status().await))
}

pub async fn recent_logs(
    State(state): State<SharedState>,
    Query(query): Query<LogsQuery>,
) -> Response {
    let requested = query.lines.unwrap_or(100).clamp(1, 500);
    into_response(Ok(state.daemon.recent_logs(requested).await))
}

pub async fn doctor_report(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.doctor_report().await))
}

pub async fn reset_downloads(State(state): State<SharedState>) -> Response {
    into_response(state.daemon.reset_downloads().await)
}
