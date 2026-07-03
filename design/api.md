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

## Authentication and Limits

When `api.require_auth = true`, every `/api/v1` route requires the configured
token via either `Authorization: Bearer <token>` or
`X-SwarmOtter-Auth: <token>`. Startup validation rejects this mode unless
`api.auth_token` is set. `GET /api/v1/settings` never returns the token value.

API request bodies are capped by `api.max_request_body_bytes`; this applies to
JSON requests and raw `.torrent` uploads. The root `/health` alias remains a
control-plane health endpoint outside `/api/v1`.

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
| GET | `/torrents/:hash/stats` | Per-torrent counters and live engine diagnostics |
| DELETE | `/torrents/:hash?delete_data=bool` | Remove (optionally delete data) |
| POST | `/torrents/:hash/pause` | Pause |
| POST | `/torrents/:hash/resume` | Resume |
| POST | `/torrents/:hash/start` | Start now (bypass queue) |
| POST | `/torrents/:hash/stop` | Stop |
| POST | `/torrents/:hash/recheck` | Force recheck |
| POST | `/torrents/:hash/reannounce` | Reannounce |
| POST | `/torrents/:hash/move` | Move data (`{ path }`) |
| POST | `/torrents/:hash/labels` | Set labels (`{ labels }`) |
| POST | `/torrents/:hash/limits` | Set per-torrent bandwidth limits (`{ download_limit, upload_limit }`, bytes/sec, 0 = unlimited; applies live) |

`/torrents/:hash/stats` includes download/upload counters, rates, limits,
`active_peer_workers`, `known_peers`, live peer diagnostics (`useful_peers`,
`unchoked_peers`, `choked_peers`, `recent_peer_failures`), tracker diagnostics
(`tracker_ok`, `tracker_message`, `last_announce`,
`recent_tracker_failures`, `tracker_last_ok_seconds_ago`), and discovery
freshness (`dht_discovery_ok`, `dht_last_seen_seconds_ago`,
`pex_discovery_ok`, `pex_last_seen_seconds_ago`). Nullable diagnostic fields
mean the daemon has not published that live signal.

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
| GET | `/settings` | Get configuration (API auth token redacted) |
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

## Per-torrent health

Every torrent list row and detail response includes a `health` object that
answers the question "can this torrent complete, and is it downloading well
right now?" Health is computed from real engine state — piece availability,
peer usefulness, throughput, recent stability, and discovery — and is not a
proxy for seed count or completion percentage.

Torrent summaries also include `active_peer_workers` and `known_peers` so UI
and API clients can show current peer activity without making a separate
diagnostics request for every row.

```json
{
  "health": {
    "score": 82,
    "bars": 4,
    "label": "good",
    "availability_score": 91,
    "throughput_score": 76,
    "peer_score": 80,
    "stability_score": 88,
    "discovery_score": 70,
    "reasons": [
      "all missing pieces are available",
      "6 useful peers are active"
    ]
  }
}
```

Fields:

- `score` (`0..100`): weighted health score. `0` = stalled / blocked /
  paused (inactive), `100` = complete.
- `bars` (`0..5`): UI mapping for signal-bars rendering.
- `label`: one of `unknown`, `network_blocked`, `stalled`, `critical`,
  `poor`, `fair`, `good`, `excellent`, `paused`, `complete`.
- `availability_score` / `throughput_score` / `peer_score` /
  `stability_score` / `discovery_score` (`0..100` each): the five
  component sub-scores that combine into the overall score.
- `reasons`: short human-readable strings explaining the score. Surfaced in
  the Web UI as a tooltip and a list under the bars.

Score formula:

```text
health_score =
    availability_score * 0.40
  + throughput_score   * 0.25
  + peer_score         * 0.15
  + stability_score    * 0.10
  + discovery_score    * 0.10
```

Bar/label mapping:

| Score  | Bars | Label             |
| ---    | ---  | ---               |
| 0      | 0    | `stalled`         |
| 1..34  | 1    | `critical`        |
| 35..54 | 2    | `poor`            |
| 55..74 | 3    | `fair`            |
| 75..89 | 4    | `good`            |
| 90..100| 5    | `excellent`       |

Hard caps override the weighted score: network containment blocking
(`network_blocked`), paused (`paused`), or complete (`complete`) always
short-circuit to their own label and score. Incomplete torrents with missing
pieces that have zero known sources cap at 35; incomplete torrents with no
useful peer cap at 30; incomplete torrents with no recently received valid
block cap at 25; torrents with no discovery and no connected peers cap at
20.

Why health is not seed count: a torrent can have many seeders but no
piece the client can actually fetch (choked, banned, slow, unidirectional
NAT), or the client can be network-blocked. Health looks at whether the
missing pieces are actually reachable from connected peers that are
sending data right now. Health is also distinct from completion
percentage: a 99% torrent with one missing piece that no connected peer
has is stalled, not excellent, even though almost everything is on disk.
