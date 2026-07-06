# API Design Notes

This document records the design contract for SwarmOtter's API. User-facing
endpoint documentation belongs in the published mdBook page:
`../docs/api.md`.

The API is a first-class product surface (ADR-0004). It is implemented in the
`swarmotter-api` crate on top of `axum`; foundational decisions are recorded in
ADR-0009 and ADR-0010.

## Design principles

- JSON request/response by default.
- Consistent `{ success, data, error }` response envelope.
- Stable snake_case machine-readable error codes.
- Stable object identifiers based on torrent info hashes.
- Native API versioning through the `/api/v1` prefix.
- Complete coverage of user-facing daemon features.
- Suitable for scripts, browser integrations, and the built-in Web UI.
- The Web UI uses the same API as external automation; it does not have a
  privileged internal channel.

## Compatibility contract

- Breaking native API changes require a new version prefix, such as `/api/v2`,
  rather than changing `/api/v1` in place.
- Error codes are part of the automation contract. Rename or removal requires
  the same compatibility treatment as a breaking API field change.
- SSE and WebSocket events share the same event object shape.
- Native torrent add requests support add-time options such as paused start
  behavior without requiring add-then-pause sequencing; see ADR-0029.
- Add requests return after registration and queue insertion; expensive queue
  reconciliation and engine startup are asynchronous and coalesced for rapid
  add bursts; see ADR-0030.
- Batch add and remove endpoints are part of the native `/api/v1` compatibility
  contract for clients that submit or operate on many torrents at once; see
  ADR-0031.
- `GET /api/v1/torrents` remains the legacy full-array list endpoint. Large
  libraries should use `GET /api/v1/torrents/query` for explicit filtering,
  sorting, pagination, counts, and grouping without changing the legacy
  response shape; see ADR-0036.
- Storage add-time preflight is part of `/api/v1` compatibility: when
  configured reserves are not met on the target storage root, add requests reject
  before data write.
- Optional compatibility endpoints, currently `/transmission/rpc` and `/api/v2`,
  are isolated from the native API and delegate to native daemon operations
  rather than a separate engine.
- Authentication policy is shared: when API auth is enabled, compatibility
  adapters must map their auth mechanism back to `api.auth_token`, including
  `/api/v2` Bearer and SID-cookie flows.
- Optional qBittorrent compatibility is intentionally limited to core
  automation endpoints and does not include indexer/search/discovery APIs.

## Storage API contract

- `GET /api/v1/storage/roots` exposes storage-root diagnostics used for
  operator visibility and add-time preflight checks.

- `[torrent].encryption_mode` is part of transport compatibility.
  `/api/v1/settings` GET includes it in configuration snapshots.
  `/api/v1/settings` PUT accepts `disabled` | `preferred` | `required`.
  `preferred` is the default when not set.
  Changing this field is reported in `restart_required_fields` for existing
  torrent tasks.
  Encryption mode is documented for interoperability and must remain under the
  same contained peer transport path.

## Storage configuration contract

- `[storage].minimum_free_space_bytes` and `[storage].minimum_free_space_percent`
  define the reserve rule used by add/start-time checks. These values are
  validated and enforced before payload writes.

## Autopilot API contract

- `GET /api/v1/autopilot/status` returns current global autopilot state, including
  `mode`.
- `GET /api/v1/torrents/:hash/autopilot` returns the current per-torrent diagnostic
  decision, reasons, and snapshot.
- `POST /api/v1/torrents/:hash/autopilot` sets or clears per-torrent override mode
  with `{ "mode": "disabled" | "observe" | "act" | null }`.
- `GET /api/v1/settings` returns `autopilot.mode` in the configuration snapshot with
  a redacted `api.auth_token`.
- `PATCH /api/v1/settings` can update `autopilot.mode` as a safe runtime
  setting.
- `PUT /api/v1/settings` replaces full configuration and accepts `[autopilot].mode`
  after validation.

`PATCH /api/v1/settings` remains constrained to runtime-safe settings and does
not accept restart-required fields.

## Implementation ownership

- Route assembly lives in `crates/swarmotter-api/src/routes.rs`.
- Handler modules live under `crates/swarmotter-api/src/handlers/`.
- Shared daemon-facing traits and response state live in
  `crates/swarmotter-api/src/state.rs`.
- API-visible model structs should come from stable core/domain models where
  practical, not ad hoc handler-local shapes.

## Maintenance

When API behavior changes:

1. Update handlers and tests.
2. Update `../docs/api.md` for user-facing endpoint or payload changes.
3. Update ADRs or this design note only when the compatibility contract or
   architecture changes.
4. Treat `/api/v1` compatibility as release-facing behavior; see
   `VERSIONING_GUIDE.md`.
