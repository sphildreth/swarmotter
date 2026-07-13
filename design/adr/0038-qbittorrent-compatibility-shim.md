# ADR-0038: qBittorrent Compatibility Shim

## Status

Accepted

## Context

SwarmOtter serves automation ecosystems such as Sonarr and Radarr, where
compatibility surfaces are a major adoption path. The native `/api/v1` contract is
already complete for first-party use, but optional compatibility shims are
required for ecosystem tooling.

## Decision

1. Add an opt-in configuration section `[compatibility.qbittorrent]` with:
   - `enabled` (default `false`).
2. When enabled, expose `/api/v2` as a compatibility adapter over native daemon
   operations, not as a second data-plane implementation.
3. Restrict compatibility behavior to explicit lifecycle/version integration points
   for this phase:
   - `GET /api/v2/app/version`
   - `GET /api/v2/app/webapiVersion`
   - `GET /api/v2/torrents/info`
   - `POST /api/v2/torrents/add`
   - `POST /api/v2/torrents/delete`
   - `POST /api/v2/torrents/pause`
   - `POST /api/v2/torrents/resume`
   - `POST /api/v2/torrents/start`
   - `POST /api/v2/torrents/stop`
   - `POST /api/v2/torrents/setCategory`
4. Support authentication through native token flow and qBittorrent-style SID
   sessions:
   - Token-based auth using existing `api.auth_token` with `Authorization` or
     `X-SwarmOtter-Auth`.
   - `POST /api/v2/auth/login` returning a `SID` cookie for subsequent calls.
5. Keep qBittorrent endpoint scope limited in this phase: no indexer/search or
   content-discovery endpoints; no torrent data-plane socket or transport behavior
   changes.
6. Keep the native API as the source of truth for all compatibility operations.

## Consequences

- Operators can use qBittorrent-style tools for automation without changing
  SwarmOtter’s native API contract.
- Compatibility behavior stays isolated in control-plane routing and reuses existing
  containment and auth policy.
- The shim’s surface remains intentionally narrow for this phase and leaves
  broader parity gaps for future planning.

## Related Documents

- [ADR-0061: Compatible Automation Profile and Lifecycle Parity](0061-compatible-automation-profile-and-lifecycle-parity.md)
- [API design notes](../api.md)
- [Configuration design notes](../configuration.md)
- [API reference](../../docs/api.md)
- [Configuration reference](../../docs/configuration.md)
