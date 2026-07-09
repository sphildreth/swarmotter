// SPDX-License-Identifier: Apache-2.0

//! Event delivery: Server-Sent Events (SSE) and WebSocket.
//!
//! Required event types (per `design/PRD.md`): `torrent_added`,
//! `torrent_changed`, `torrent_removed`, `torrent_error`,
//! `torrent_metadata_received`, `torrent_completed`, `torrent_files_changed`,
//! `torrent_trackers_changed`, `torrent_peers_changed`, `stats_updated`,
//! `network_status_changed`, `watch_folder_imported`, `watch_folder_failed`,
//! `settings_changed`, `daemon_health_changed`.
//!
//! Clients subscribe via `/api/v1/events` (SSE) or `/api/v1/ws` (WebSocket)
//! and may filter to a specific torrent via `?info_hash=<hex>`.

use axum::{
    extract::{
        ws::{Message, WebSocketUpgrade},
        FromRef, Query, State,
    },
    response::{sse::KeepAlive, IntoResponse, Response, Sse},
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::broadcast;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tokio_stream::StreamExt as _;

use crate::state::SharedState;

/// An event delivered to subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub kind: String,
    pub info_hash: Option<String>,
    pub payload: serde_json::Value,
}

impl Event {
    pub fn new(kind: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            kind: kind.into(),
            info_hash: None,
            payload,
        }
    }

    pub fn with_info_hash(mut self, hex: String) -> Self {
        self.info_hash = Some(hex);
        self
    }
}

/// A broker that broadcasts events to all subscribers.
#[derive(Clone)]
pub struct EventBroker {
    tx: broadcast::Sender<PublishedEvent>,
}

/// Serialized event frame shared by all subscribers.
#[derive(Debug, Clone)]
pub struct PublishedEvent {
    pub kind: String,
    pub info_hash: Option<String>,
    pub json: Arc<str>,
}

impl EventBroker {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn publish(&self, event: Event) {
        let json = match serde_json::to_string(&event) {
            Ok(json) => json,
            Err(e) => {
                tracing::warn!(error = %e, kind = %event.kind, "event serialization failed");
                return;
            }
        };
        let event = PublishedEvent {
            kind: event.kind,
            info_hash: event.info_hash,
            json: Arc::<str>::from(json),
        };
        // Ignore send errors (no subscribers).
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> BroadcastStream<PublishedEvent> {
        BroadcastStream::new(self.tx.subscribe())
    }
}

impl Default for EventBroker {
    fn default() -> Self {
        Self::new(256)
    }
}

#[derive(Debug, Deserialize)]
pub struct EventFilter {
    pub info_hash: Option<String>,
}

pub async fn sse_events(
    State(state): State<SharedState>,
    Query(filter): Query<EventFilter>,
) -> Response {
    let broker = state.broker.clone();
    let stream = make_event_stream(broker, filter.info_hash);
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<SharedState>,
    Query(filter): Query<EventFilter>,
) -> Response {
    ws.on_upgrade(move |mut socket| {
        let broker = state.broker.clone();
        async move {
            let mut stream = broker.subscribe();
            let mut ping = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = ping.tick() => {
                        if socket.send(Message::Ping(Vec::new())).await.is_err() {
                            break;
                        }
                    }
                    res = stream.next() => {
                        match res {
                            Some(Ok(event)) => {
                                if let Some(want) = &filter.info_hash {
                                    if event.info_hash.as_deref() != Some(want) {
                                        continue;
                                    }
                                }
                                if socket.send(Message::Text(event.json.to_string())).await.is_err() {
                                    break;
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let payload = lagged_event_json(skipped);
                                if socket.send(Message::Text(payload)).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        }
    })
}

fn make_event_stream(
    broker: EventBroker,
    info_hash: Option<String>,
) -> impl Stream<Item = Result<axum::response::sse::Event, Infallible>> {
    broker.subscribe().filter_map(move |res| match res {
        Ok(event) => {
            if let Some(want) = &info_hash {
                if event.info_hash.as_deref() != Some(want) {
                    return None;
                }
            }
            Some(Ok(axum::response::sse::Event::default()
                .event(event.kind)
                .data(&event.json)))
        }
        Err(BroadcastStreamRecvError::Lagged(skipped)) => {
            tracing::warn!(skipped, "event subscriber lagged behind broadcast buffer");
            Some(Ok(axum::response::sse::Event::default()
                .event("events_dropped")
                .data(lagged_event_json(skipped))))
        }
    })
}

fn lagged_event_json(skipped: u64) -> String {
    serde_json::to_string(&Event::new(
        "events_dropped",
        serde_json::json!({ "skipped": skipped }),
    ))
    .unwrap_or_else(|_| {
        r#"{"kind":"events_dropped","info_hash":null,"payload":{"skipped":0}}"#.to_string()
    })
}

// Allow extracting EventBroker from SharedState.
impl FromRef<SharedState> for EventBroker {
    fn from_ref(state: &SharedState) -> Self {
        state.broker.clone()
    }
}
