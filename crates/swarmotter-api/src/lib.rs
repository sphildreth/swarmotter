// SPDX-License-Identifier: Apache-2.0

//! SwarmOtter API layer.
//!
//! Versioned REST API built on axum. The API is a first-class product surface
//! (ADR-0004): the Web UI consumes the same endpoints exposed to external
//! automation. All responses use a consistent envelope with machine-readable
//! error codes.
//!
//! Routes are prefixed with `/api/v1`. Events are delivered via Server-Sent
//! Events (SSE) at `/api/v1/events` and WebSocket at `/api/v1/ws`.

pub mod envelope;
pub mod error;
pub mod handlers;
pub mod routes;
pub mod state;

pub use routes::app_router;
pub use state::{AppState, SharedState};
