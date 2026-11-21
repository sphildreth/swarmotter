// SPDX-License-Identifier: Apache-2.0

//! RSS subscription handlers.
//!
//! Every route is authenticated by the same native API middleware as the rest
//! of `/api/v1`. Status responses redact feed URL credentials.

use crate::error::{err_response, into_response, ok_empty_response};
use crate::state::SharedState;
use axum::{
    extract::{Path, Query, State},
    response::Response,
    Json,
};
use serde::Deserialize;
use serde_json::json;
use swarmotter_core::error::CoreError;
use swarmotter_core::rss::RssSubscription;

#[derive(Debug, Default, Deserialize)]
pub struct PollQuery {
    #[serde(default)]
    pub import_current: bool,
    #[serde(default)]
    pub reconsider: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionQuery {
    pub limit: Option<usize>,
    pub before: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryRequest {
    pub entry_key: Option<String>,
}

pub async fn list_subscriptions(State(state): State<SharedState>) -> Response {
    into_response(Ok(state.daemon.rss_status().await))
}

pub async fn create_subscription(
    State(state): State<SharedState>,
    Json(subscription): Json<RssSubscription>,
) -> Response {
    match state
        .daemon
        .rss_upsert_subscription(subscription, None)
        .await
    {
        Ok(status) => into_response(Ok(status)),
        Err(error) => err_response(error),
    }
}

pub async fn update_subscription(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(mut subscription): Json<RssSubscription>,
) -> Response {
    if !subscription.id.is_empty() && subscription.id != id {
        return err_response(CoreError::InvalidArgument(
            "subscription id is immutable; delete and create to rename".into(),
        ));
    }
    subscription.id = id.clone();
    if subscription.url.trim().is_empty() {
        // Status responses redact feed credentials. A GUI edit that leaves the
        // URL blank preserves the configured value rather than writing the
        // redacted display string back as a new URL.
        let existing = state
            .daemon
            .get_config()
            .await
            .rss
            .subscription(&id)
            .cloned();
        let Some(existing) = existing else {
            return err_response(CoreError::NotFound(format!("rss subscription {id}")));
        };
        subscription.url = existing.url;
    }
    match state
        .daemon
        .rss_upsert_subscription(subscription, Some(id))
        .await
    {
        Ok(status) => into_response(Ok(status)),
        Err(error) => err_response(error),
    }
}

pub async fn delete_subscription(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    match state.daemon.rss_delete_subscription(&id).await {
        Ok(()) => ok_empty_response(),
        Err(error) => err_response(error),
    }
}

pub async fn preview_draft_subscription(
    State(state): State<SharedState>,
    Json(subscription): Json<RssSubscription>,
) -> Response {
    match state.daemon.rss_preview_draft(subscription).await {
        Ok(preview) => into_response(Ok(preview)),
        Err(error) => err_response(error),
    }
}

pub async fn preview_subscription(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    match state.daemon.rss_preview(&id).await {
        Ok(preview) => into_response(Ok(preview)),
        Err(error) => err_response(error),
    }
}

pub async fn poll_subscription(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Query(query): Query<PollQuery>,
) -> Response {
    match state
        .daemon
        .rss_poll(&id, query.import_current, query.reconsider)
        .await
    {
        Ok(result) => into_response(Ok(result)),
        Err(error) => err_response(error),
    }
}

pub async fn retry_subscription(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(request): Json<RetryRequest>,
) -> Response {
    match state.daemon.rss_retry(&id, request.entry_key).await {
        Ok(result) => into_response(Ok(result)),
        Err(error) => err_response(error),
    }
}

pub async fn decisions(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Query(query): Query<DecisionQuery>,
) -> Response {
    match state
        .daemon
        .rss_decisions(&id, query.limit.unwrap_or(100), query.before)
        .await
    {
        Ok(page) => into_response(Ok(page)),
        Err(error) => err_response(error),
    }
}

pub async fn clear_completions(State(state): State<SharedState>) -> Response {
    match state.daemon.rss_clear_completions().await {
        Ok(cleared) => into_response(Ok(json!({ "cleared_alias_rows": cleared }))),
        Err(error) => err_response(error),
    }
}

pub async fn diagnostics(State(state): State<SharedState>) -> Response {
    match state.daemon.rss_duplicate_lookup_is_indexed().await {
        Ok(indexed) => into_response(Ok(json!({ "duplicate_lookup_uses_index": indexed }))),
        Err(error) => err_response(error),
    }
}
