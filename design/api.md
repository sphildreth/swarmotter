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
- Optional compatibility endpoints, currently `/transmission/rpc`, are isolated
  from the native API and delegate to native daemon operations rather than a
  second torrent engine.
- Authentication policy is shared: when API auth is enabled, compatibility
  adapters must map their auth mechanism back to `api.auth_token`.

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
