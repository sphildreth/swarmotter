# ADR-0026: Transmission RPC Compatibility Adapter

## Status

Accepted

## Context

A number of existing BitTorrent operational tools and scripts integrate through
Transmission RPC endpoints, especially `POST /transmission/rpc`. Adding a
compatible adapter lowers migration friction for deployments and automation while
keeping SwarmOtter's own API as the control-plane primary interface.

The adapter must not bypass existing architecture or containment guarantees:
it should delegate to existing daemon operations and preserve strict network
containment and authentication behavior.

## Decision

- Add an optional Transmission RPC endpoint at `/transmission/rpc` that is served by
  the API layer as a compatibility adapter over existing `DaemonOps`.
- The adapter is not a second torrent engine; it must map incoming calls to
  `DaemonOps` operations only.
- Enablement is explicit configuration, with a default-off mode.
- Transmission HTTP Basic authentication is mapped to SwarmOtter API authentication
  when `api.require_auth = true`: the Basic password must equal
  `api.auth_token`; the username is not security-significant.
- The adapter must implement `X-Transmission-Session-Id` session enforcement.
  Requests without the current session value are rejected with a session handshake
  response and the required session ID header.
- The initial method surface includes common Transmission session, torrent
  lifecycle, torrent mutation, queue movement, and helper methods needed by
  existing tools. Mutating calls are direct translations to native daemon
  operations; `torrent-remove` with `delete-local-data` / `delete_local_data`
  can delete payload data.
- Initial `torrent-add` support is limited to:
  - magnet links (for `filename`)
  - inline base64-encoded torrent metadata (for `metainfo`)
- The adapter must reject remote HTTP/HTTPS torrent URL fetching in
  `torrent-add` to preserve existing containment boundaries until a separate
  design decision.
- Compatibility support is scoped to Transmission RPC only for now. No qBittorrent API
  compatibility work is included, and compatibility surfaces are treated as isolated
  adapters so another API can be added later if justified.

## Consequences

- Adds a new optional external integration path for existing tooling without changing
  core torrent-engine semantics.
- Centralized engine usage preserves existing eventing, diagnostics, and network
  containment behavior.
- `api.require_auth = true` and existing token policy can continue to secure both
  the native API and Transmission compatibility clients.
- Operators enabling the adapter expose a broader Transmission-compatible control
  surface, including mutating calls. This is intentional interoperability, not a
  read-only dashboard API.
- Operators gain clear boundaries: richer interoperability is accepted incrementally,
  and remote torrent URL intake remains intentionally unsupported at launch.

## Related Documents

- `design/api.md`
- `design/configuration.md`
- `docs/configuration.md`
- `docs/getting-started.md`
- `CHANGELOG.md`
