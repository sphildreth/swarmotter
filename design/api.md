# API

This document describes SwarmOtter's API surface. The API is a first-class
product surface (see ADR-0004) and is implemented in the `swarmotter-api`
crate on top of `axum` (see ADR-0009, ADR-0010).

## Principles

- JSON request/response by default.
- Consistent envelope with machine-readable error codes and human-readable
  messages.
- Stable object identifiers (info hashes).
- API versioning via the `/api/v1` prefix.
- Complete coverage of user-facing features.
- Suitable for scripts and browser integrations.
- The Web UI uses the same API as external automation.

## Response format

All responses use:

```json
{ "success": true, "data": {}, "error": null }
```

Errors:

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "network_blocked",
    "message": "Required torrent network interface tun0 is not available."
  }
}
```

HTTP status codes reflect the error class: 400 (bad input), 404 (not found),
409 (duplicate), 503 (network/containment blocked), 500 (internal). Stable
error codes are derived from the core error model
(`swarmotter-core::error::CoreError`).

## Endpoints

All routes are prefixed with `/api/v1`. A root `/health` alias also exists.

### Health, version, stats

| Method | Path | Description |
| --- | --- | --- |
| GET | `/health` | Daemon + network health |
| GET | `/version` | Version/build info |
| GET | `/stats` | Global stats |

### Torrent management

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents` | List torrents |
| POST | `/torrents` | Add magnet (JSON) or torrent file (raw body) |
| POST | `/torrents/magnet` | Add magnet (JSON `{ magnet, download_dir? }`) |
| POST | `/torrents/file` | Upload `.torrent` (raw body) |
| GET | `/torrents/:hash` | Torrent details |
| DELETE | `/torrents/:hash?delete_data=bool` | Remove (optionally delete data) |
| POST | `/torrents/:hash/pause` | Pause |
| POST | `/torrents/:hash/resume` | Resume |
| POST | `/torrents/:hash/start` | Start now (bypass queue) |
| POST | `/torrents/:hash/stop` | Stop |
| POST | `/torrents/:hash/recheck` | Force recheck |
| POST | `/torrents/:hash/reannounce` | Reannounce |
| POST | `/torrents/:hash/move` | Move data (`{ path }`) |
| POST | `/torrents/:hash/labels` | Set labels (`{ labels }`) |

### Files

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents/:hash/files` | List files |
| PATCH | `/torrents/:hash/files` | Alias for set wanted |
| POST | `/torrents/:hash/files/wanted` | Set wanted (`{ file_indices, wanted }`) |
| POST | `/torrents/:hash/files/priority` | Set priority (`{ file_indices, priority }`) |
| POST | `/torrents/:hash/files/:index/rename` | Rename path (`{ new_path }`) |

### Trackers

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents/:hash/trackers` | List trackers |
| POST | `/torrents/:hash/trackers` | Add tracker (`{ url }`) |
| DELETE | `/torrents/:hash/trackers/:url` | Remove tracker |
| POST | `/torrents/:hash/trackers/edit` | Edit tracker (`{ old_url, new_url }`) |

### Peers

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents/:hash/peers` | List peers |

### Queue

| Method | Path | Description |
| --- | --- | --- |
| POST | `/torrents/:hash/queue/move-up` | Move up |
| POST | `/torrents/:hash/queue/move-down` | Move down |
| POST | `/torrents/:hash/queue/move-top` | Move to top |
| POST | `/torrents/:hash/queue/move-bottom` | Move to bottom |

### Settings

| Method | Path | Description |
| --- | --- | --- |
| GET | `/settings` | Get configuration |
| PATCH | `/settings` | Update safe runtime settings (bandwidth/queue/seeding) |

### Network

| Method | Path | Description |
| --- | --- | --- |
| GET | `/network/health` | Network containment health |

### Watch folders

| Method | Path | Description |
| --- | --- | --- |
| POST | `/watch/scan` | Trigger a scan |
| GET | `/watch/history` | Import history |

## Events (WebSocket/SSE)

Required event types (per `design/PRD.md`): `torrent_added`,
`torrent_changed`, `torrent_removed`, `torrent_error`,
`torrent_metadata_received`, `torrent_completed`, `torrent_files_changed`,
`torrent_trackers_changed`, `torrent_peers_changed`, `stats_updated`,
`network_status_changed`, `watch_folder_imported`, `watch_folder_failed`,
`settings_changed`, `daemon_health_changed`.

- SSE: `GET /api/v1/events` (text/event-stream). Each event carries the event
  `kind` and a JSON `payload`.
- WebSocket: `GET /api/v1/ws`. Sends JSON `Event` objects
  (`{ kind, info_hash, payload }`).
- Both support per-torrent filtering via `?info_hash=<40-hex>`.

Clients may subscribe to all torrents or filter to a specific torrent.