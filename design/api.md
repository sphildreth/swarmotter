# API

This document describes SwarmOtter's API surface. It is a stub; route names and
schemas will be finalized during implementation. The API is a first-class
product surface (see ADR-0004).

## Principles

- JSON request/response by default.
- Consistent error format with machine-readable codes and human-readable
  messages.
- Stable object identifiers.
- API versioning.
- Complete coverage of user-facing features.
- Suitable for scripts and browser integrations.
- The Web UI uses the same API as external automation.

## Response format

```json
{
  "success": true,
  "data": {},
  "error": null
}
```

Errors:

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "network_interface_missing",
    "message": "Required torrent network interface tun0 is not available. Torrent networking is blocked."
  }
}
```

## Endpoint areas

Exact route names may be adjusted during implementation, but the API must
cover:

- **Torrent management:** list, details, add magnet, upload torrent file,
  remove, remove+delete, pause, resume, start-now, recheck, reannounce, move
  data, rename path, update labels/categories.
- **Files:** list, set wanted/unwanted, set priority, rename path.
- **Trackers:** list, add, remove, edit, reannounce, status.
- **Peers:** list, client, progress, address, transfer rates, disconnect,
  suppress/ban.
- **Queue:** state, move up/down/top/bottom.
- **Settings:** get, update safe runtime settings, validate, report
  restart-required settings.
- **Network:** containment mode, configured interface/source/namespace, current
  health, fail-closed state, DHT state, tracker state.
- **Watch folders:** list, status, trigger scan, import history.
- **Stats and health:** global stats, per-torrent stats, daemon health,
  storage health, network health, API health, version/build info.

## Events (WebSocket/SSE)

Required event types include `torrent_added`, `torrent_changed`,
`torrent_removed`, `torrent_error`, `torrent_metadata_received`,
`torrent_completed`, `torrent_files_changed`, `torrent_trackers_changed`,
`torrent_peers_changed`, `stats_updated`, `network_status_changed`,
`watch_folder_imported`, `watch_folder_failed`, `settings_changed`, and
`daemon_health_changed`. Clients must be able to subscribe to all torrents or
specific torrents.

## TODO

- Finalize versioning scheme and route paths.
- Specify request/response schemas per endpoint.
- Specify authentication/authorization model.
- Keep this document aligned with `architecture.md` and `requirements.md`.