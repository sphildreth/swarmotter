# ADR-0010: API Versioning, Envelope, and Event Delivery

## Status

Accepted

## Context

The API is a first-class product surface (ADR-0004). It must be stable enough
for external automation and the Web UI, use consistent JSON responses, expose
machine-readable error codes, and deliver real-time updates. Decisions about
versioning, the response shape, and event transport have lasting impact on
clients and scripts.

## Decision

- All API routes are mounted under `/api/v1/`. Breaking changes require a new
  version prefix, not in-place modification of `v1` routes.
- Every response uses the envelope `{ success, data, error }`. `error` is either
  `null` or `{ code, message }` where `code` is a stable snake_case machine
  code derived from the core error model.
- HTTP status codes reflect the error class (400 bad input, 404 not found, 409
  duplicate, 503 network/containment blocked, 500 internal).
- Real-time updates are delivered via Server-Sent Events at `/api/v1/events`
  and WebSocket at `/api/v1/ws`. Both use the same `Event` JSON shape
  (`{ kind, info_hash, payload }`) and support per-torrent filtering via
  `?info_hash=<hex>`. A broadcast broker fans events out to subscribers.
- The Web UI consumes the identical API (no privileged internal channel).

## Consequences

- Clients can rely on a stable shape and code set; the envelope is uniform
  across success and error responses.
- SSE is simple for browsers (EventSource); WebSocket supports bidirectional
  use. Both are provided so integrations can choose.
- Adding a `v2` namespace later is non-breaking for `v1` clients.
- The broker is in-memory per process; horizontal scaling of the daemon is out
  of scope for v1.0.0.

## Related Documents

- `design/api.md`
- `design/architecture.md`
- ADR-0004 (API-first)