# API Reference

SwarmOtter exposes a native REST API under `/api/v1`. The Web UI uses the same
API as external automation.

## Response format

All responses use a common envelope:

```json
{ "success": true, "data": {}, "error": null }
```

Errors use the same envelope:

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

HTTP status codes reflect the error class:

- `400`: bad input.
- `404`: not found.
- `409`: duplicate.
- `503`: network or containment blocked.
- `500`: internal error.

## Authentication and limits

When `api.require_auth = true`, every `/api/v1` route requires the configured
token through one of these headers:

```text
Authorization: Bearer <token>
X-SwarmOtter-Auth: <token>
```

Startup validation rejects authenticated mode unless `api.auth_token` is set.
`GET /api/v1/settings` never returns the token value.

API request bodies are capped by `api.max_request_body_bytes`; this applies to
JSON requests and raw `.torrent` uploads. The root `/health` alias remains a
control-plane health endpoint outside `/api/v1`.

## Health, version, and stats

All paths in this section are under `/api/v1`, except the root `/health` alias.

| Method | Path | Description |
| --- | --- | --- |
| GET | `/health` | Daemon and network health. |
| GET | `/version` | Version and build info. |
| GET | `/stats` | Global stats. |

The root `/health` path is also available without the `/api/v1` prefix.

## Torrent management

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents` | List torrents. |
| POST | `/torrents` | Add magnet JSON or raw `.torrent` body. |
| POST | `/torrents/magnet` | Add magnet JSON: `{ magnet, download_dir? }`. |
| POST | `/torrents/file` | Upload raw `.torrent` body. |
| GET | `/torrents/:hash` | Torrent details. |
| GET | `/torrents/:hash/stats` | Per-torrent counters and live engine diagnostics. |
| DELETE | `/torrents/:hash?delete_data=bool` | Remove torrent, optionally deleting data. |
| POST | `/torrents/:hash/pause` | Pause. |
| POST | `/torrents/:hash/resume` | Resume. |
| POST | `/torrents/:hash/start` | Start now, bypassing queue. |
| POST | `/torrents/:hash/stop` | Stop. |
| POST | `/torrents/:hash/recheck` | Force recheck. |
| POST | `/torrents/:hash/reannounce` | Reannounce. |
| POST | `/torrents/:hash/move` | Move data: `{ path }`. |
| POST | `/torrents/:hash/labels` | Set labels: `{ labels }`. |
| POST | `/torrents/:hash/limits` | Set per-torrent bandwidth limits: `{ download_limit, upload_limit }`, bytes/sec, `0` = unlimited. |

`/torrents/:hash/stats` includes counters, rates, limits, active peer workers,
known peers, live peer scheduler diagnostics, tracker diagnostics, and DHT/PEX
freshness. Nullable diagnostic fields mean the daemon has not published that
live signal yet.

## Files

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents/:hash/files` | List files. |
| PATCH | `/torrents/:hash/files` | Alias for set wanted. |
| POST | `/torrents/:hash/files/wanted` | Set wanted: `{ file_indices, wanted }`. |
| POST | `/torrents/:hash/files/priority` | Set priority: `{ file_indices, priority }`. |
| POST | `/torrents/:hash/files/:index/rename` | Rename path: `{ new_path }`. |

## Trackers

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents/:hash/trackers` | List trackers. |
| POST | `/torrents/:hash/trackers` | Add tracker: `{ url }`. |
| DELETE | `/torrents/:hash/trackers/:url` | Remove tracker. |
| POST | `/torrents/:hash/trackers/edit` | Edit tracker: `{ old_url, new_url }`. |

Tracker rows expose per-URL announce status. `last_error` is populated only for
failed announces, while `last_message` carries the latest successful announce
message. `seeders`, `leechers`, `downloads`, and `last_announce` are populated
from the last live announce result for that tracker URL when the engine has
reported one.

## Peers

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents/:hash/peers` | List peers. |

## Queue

| Method | Path | Description |
| --- | --- | --- |
| POST | `/torrents/:hash/queue/move-up` | Move up. |
| POST | `/torrents/:hash/queue/move-down` | Move down. |
| POST | `/torrents/:hash/queue/move-top` | Move to top. |
| POST | `/torrents/:hash/queue/move-bottom` | Move to bottom. |

## Settings

| Method | Path | Description |
| --- | --- | --- |
| GET | `/settings` | Get configuration with API auth token redacted. |
| PATCH | `/settings` | Update live-safe runtime settings. |
| PUT | `/settings` | Replace full configuration atomically after validation. |

`PATCH /settings` updates live-safe bandwidth, queue, and seeding fields.

`PUT /settings` validates the full config before persistence, preserves the
existing `api.auth_token` when omitted, applies live-safe fields immediately,
and reports fields that require restart.

## Network

| Method | Path | Description |
| --- | --- | --- |
| GET | `/network/health` | Network containment health. |
| GET | `/network/diagnostics` | Detailed network/path diagnostics. |

See [Network Containment](network-containment.md) for health state meanings.

## Watch folders

| Method | Path | Description |
| --- | --- | --- |
| POST | `/watch/scan` | Trigger a scan. |
| GET | `/watch/history` | Import history. |
| GET | `/watch/status` | Watch-folder status, folder readiness, and recent imports. |

## Logs and doctor

| Method | Path | Description |
| --- | --- | --- |
| GET | `/logs/recent` | Recent daemon logs, with `lines=1..500`, default `100`. |
| GET | `/doctor` | Consolidated operational health report. |
| POST | `/reset` | Stop torrent work, remove torrent records, delete configured download/incomplete contents, and clear daemon log files. |

`POST /reset` is destructive and clients should present an explicit
confirmation step. The daemon preserves configured `download_dir` and
`incomplete_dir` root directories themselves, removes registered torrent
payloads from per-torrent override locations, clears in-memory torrent/queue
state, and truncates the active daemon log file so the running logger can
continue writing to the same path.

## Events

SSE and WebSocket events use the same JSON event shape:

```json
{ "kind": "torrent_changed", "info_hash": "40hex...", "payload": {} }
```

- SSE: `GET /api/v1/events`
- WebSocket: `GET /api/v1/ws`
- Both support per-torrent filtering with `?info_hash=<40-hex>`.

Current event kinds include `torrent_added`, `torrent_changed`,
`torrent_removed`, `torrent_error`, `torrent_metadata_received`,
`torrent_completed`, `torrent_files_changed`, `torrent_trackers_changed`,
`torrent_peers_changed`, `stats_updated`, `network_status_changed`,
`watch_folder_imported`, `watch_folder_failed`, `settings_changed`, and
`daemon_health_changed`.

## Per-torrent health

Every torrent list row and detail response includes a `health` object that
answers whether the torrent can complete and whether it is downloading well
right now. Health is computed from engine state: piece availability, peer
usefulness, throughput, recent stability, and discovery. It is not a proxy for
seed count or completion percentage.

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

- `score` (`0..100`): weighted health score. `0` means stalled, blocked, or
  paused; `100` means complete.
- `bars` (`0..5`): UI mapping for signal-bars rendering.
- `label`: one of `unknown`, `network_blocked`, `stalled`, `critical`,
  `poor`, `fair`, `good`, `excellent`, `paused`, `complete`.
- `availability_score`, `throughput_score`, `peer_score`, `stability_score`,
  and `discovery_score` (`0..100` each): component sub-scores.
- `reasons`: short human-readable strings explaining the score.

Score formula:

```text
health_score =
    availability_score * 0.40
  + throughput_score   * 0.25
  + peer_score         * 0.15
  + stability_score    * 0.10
  + discovery_score    * 0.10
```

Bar and label mapping:

| Score | Bars | Label |
| --- | --- | --- |
| `0` | `0` | `stalled` |
| `1..34` | `1` | `critical` |
| `35..54` | `2` | `poor` |
| `55..74` | `3` | `fair` |
| `75..89` | `4` | `good` |
| `90..100` | `5` | `excellent` |

Hard caps override the weighted score: network containment blocking
(`network_blocked`), paused (`paused`), or complete (`complete`) always
short-circuit to their own label and score. Incomplete torrents with missing
pieces that have zero known sources cap at `35`; incomplete torrents with no
useful peer cap at `30`; incomplete torrents with no recently received valid
block cap at `25`; torrents with no discovery and no connected peers cap at
`20`.

## Transmission RPC compatibility

When enabled, `POST /transmission/rpc` is a compatibility adapter over native
daemon operations. It is not part of the native `/api/v1` surface.

Enable it with:

```toml
[compatibility.transmission]
enabled = true
```

Authentication follows `api.require_auth`:

- When auth is required, HTTP Basic auth is accepted and the Basic password
  must equal `api.auth_token`; the username is not security-significant.
- When auth is disabled, auth headers are not required for this endpoint.
- The endpoint enforces `X-Transmission-Session-Id` and returns a new session
  ID header on session mismatch.

The adapter currently supports common session, torrent lifecycle, queue, and
helper calls:

- `session-get`, `session-set`, `session-stats`, `session-close`
- `torrent-get`, `torrent-start`, `torrent-start-now`, `torrent-stop`,
  `torrent-verify`, `torrent-reannounce`
- `torrent-add`, `torrent-remove`, `torrent-set`, `torrent-set-location`,
  `torrent-rename-path`
- `queue-move-top`, `queue-move-up`, `queue-move-down`, `queue-move-bottom`
- `free-space`, `port-test`, `blocklist-update`

`torrent-remove` maps `delete-local-data` and `delete_local_data` to the native
delete-data option.

`torrent-add` accepts magnet links via `filename` and base64 torrent metadata
via `metainfo`. Remote HTTP/HTTPS torrent metadata URLs are rejected.
