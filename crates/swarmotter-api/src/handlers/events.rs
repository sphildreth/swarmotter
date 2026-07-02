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
    response::{IntoResponse, Response, Sse},
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
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
    tx: broadcast::Sender<Event>,
}

impl EventBroker {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn publish(&self, event: Event) {
        // Ignore send errors (no subscribers).
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> BroadcastStream<Event> {
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
    Sse::new(stream).into_response()
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
            while let Some(res) = stream.next().await {
                if let Ok(event) = res {
                    if let Some(want) = &filter.info_hash {
                        if event.info_hash.as_deref() != Some(want) {
                            continue;
                        }
                    }
                    let json = match serde_json::to_string(&event) {
                        Ok(j) => j,
                        Err(_) => continue,
                    };
                    if socket.send(Message::Text(json)).await.is_err() {
                        break;
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
            let json = serde_json::to_string(&event).unwrap_or_default();
            Some(Ok(axum::response::sse::Event::default()
                .event(event.kind.clone())
                .data(json)))
        }
        Err(_) => None,
    })
}

// Allow extracting EventBroker from SharedState.
impl FromRef<SharedState> for EventBroker {
    fn from_ref(state: &SharedState) -> Self {
        state.broker.clone()
    }
}
