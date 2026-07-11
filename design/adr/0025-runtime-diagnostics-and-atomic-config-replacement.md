# ADR-0025: Runtime Diagnostics and Atomic Config Replacement

## Status

Accepted

## Context

SwarmOtter now needs richer operational surfaces for a practical operator dashboard:
network diagnostics, watch-folder status, recent logs, and a consolidated doctor report.
At the same time, configuration editing must support both safe runtime tuning and full-file
reconfiguration with strict validation semantics.

The existing safe settings patch path (`PATCH /api/v1/settings`) updates only in-memory,
runtime-safe sections. The full configuration model, including restart-requiring fields and
containing metadata, still needed a predictable replacement flow that validates before
persistence and does not expose sensitive auth tokens in API responses.

## Decision

- Add runtime diagnostics endpoints under `/api/v1` for health and operations visibility:
  - `GET /api/v1/network/diagnostics`
  - `GET /api/v1/watch/status`
  - `GET /api/v1/logs/recent`
  - `GET /api/v1/doctor`
- Keep `GET /api/v1/settings` as a read endpoint that redacts `api.auth_token`.
- Keep `PATCH /api/v1/settings` for runtime-safe partial updates to currently live-configurable
  fields (bandwidth, queue, seeding limits).
- Add `PUT /api/v1/settings` for full configuration replacement:
  - The incoming config must pass full validation before being considered for persistence.
  - Persistence is atomic; failed validation prevents any write.
  - A missing `api.auth_token` in the request body preserves the current token.
  - The endpoint returns restart metadata so clients can distinguish:
    - fields applied live,
    - fields that require process restart,
    - and whether a write reached disk.
- Include the auth-token preservation and restart metadata in the API contract for settings
  replacement responses.
- Supported package and container deployments give the daemon a private,
  writable configuration directory. Containers mount the directory rather than
  the individual file so atomic rename remains available.

## Consequences

- Operators can use a single flow for both live-safe adjustments and full config replacement
  without adding a separate file-edit deployment step.
- Configuration updates become safer because invalid full configuration edits do not partially
  commit.
- Restart-required behavior is explicit and machine-readable, so the UI can prompt for restart.
- Sensitive token material remains protected even during full configuration exchange.
- Deployment umasks keep token-bearing atomic replacements private to the
  service account.

## Related Documents

- `design/api.md`
- `design/configuration.md`
- `design/PRD.md`
- `docs/web-ui.md`
- `docs/configuration.md`
- `crates/swarmotter-api/src/routes.rs`
- `crates/swarmotter-api/src/handlers/settings.rs`
- `crates/swarmotter-api/src/handlers/diagnostics.rs`
- `crates/swarmotter-core/src/models/diagnostics.rs`
- `crates/swarmotter-core/src/config.rs`
