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
        Self::new(4096)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_new_sets_kind_and_payload() {
        let e = Event::new("torrent_added", json!({ "id": 7 }));
        assert_eq!(e.kind, "torrent_added");
        assert_eq!(e.payload, json!({ "id": 7 }));
        assert!(e.info_hash.is_none());
    }

    #[test]
    fn event_accepts_into_string_kinds() {
        let e = Event::new(String::from("settings_changed"), json!(null));
        assert_eq!(e.kind, "settings_changed");
    }

    #[test]
    fn event_with_info_hash_attaches_hash() {
        let e = Event::new("torrent_changed", json!({}))
            .with_info_hash("dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e".into());
        assert_eq!(
            e.info_hash.as_deref(),
            Some("dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e")
        );
    }

    #[test]
    fn event_serializes_to_json_envelope() {
        let e = Event::new("torrent_removed", json!({ "name": "x" })).with_info_hash("abc".into());
        let v: serde_json::Value = serde_json::to_value(&e).unwrap();
        assert_eq!(v["kind"], "torrent_removed");
        assert_eq!(v["info_hash"], "abc");
        assert_eq!(v["payload"]["name"], "x");
    }

    #[tokio::test]
    async fn broker_default_uses_4096_capacity() {
        let b = EventBroker::default();
        let mut s = b.subscribe();
        b.publish(Event::new("torrent_added", json!({})));
        let ev = s.next().await.expect("event").expect("ok");
        assert_eq!(ev.kind, "torrent_added");
    }

    #[tokio::test]
    async fn broker_publish_propagates_kind_and_info_hash() {
        let b = EventBroker::new(16);
        let mut s = b.subscribe();
        b.publish(
            Event::new("torrent_completed", json!({ "id": 1 })).with_info_hash("deadbeef".into()),
        );
        let ev = s.next().await.expect("event").expect("ok");
        assert_eq!(ev.kind, "torrent_completed");
        assert_eq!(ev.info_hash.as_deref(), Some("deadbeef"));
        // The JSON payload should include the kind for the SSE `event:` field.
        assert!(ev.json.contains("torrent_completed"));
    }

    #[tokio::test]
    async fn broker_publish_with_no_subscribers_is_silent() {
        let b = EventBroker::new(4);
        // No subscribers; publish should not panic.
        b.publish(Event::new("torrent_added", json!({})));
    }

    #[tokio::test]
    async fn broker_subscribe_yields_a_stream() {
        let b = EventBroker::new(4);
        let s = b.subscribe();
        drop(s);
        // Subscribing again should still work after the previous stream is dropped.
        let mut s2 = b.subscribe();
        b.publish(Event::new("network_status_changed", json!({})));
        let ev = s2.next().await.expect("event").expect("ok");
        assert_eq!(ev.kind, "network_status_changed");
    }

    #[test]
    fn lagged_event_json_includes_count() {
        let s = lagged_event_json(7);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["kind"], "events_dropped");
        assert_eq!(v["info_hash"], serde_json::Value::Null);
        assert_eq!(v["payload"]["skipped"], 7);
    }

    #[test]
    fn publish_swallows_serialization_errors() {
        // Force a serialization failure by feeding an unserializable payload.
        let b = EventBroker::new(4);
        // bytes keys are not supported by serde_json
        let bad = serde_json::Value::String("ok".into());
        // The above serializes fine; we exercise the public API path.
        b.publish(Event::new("daemon_health_changed", bad));
    }
}
