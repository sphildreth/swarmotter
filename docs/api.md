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
- `401`: missing or invalid API authentication.
- `403`: browser origin, Fetch Metadata, or Host validation failed.
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

When `api.require_auth = false`, API and Web UI requests do not require a token,
including on a configured LAN listener. Every client that can reach such a
listener can control SwarmOtter, so authenticated mode is strongly recommended
unless the reachable network is the intended trust boundary.

Browser requests to `/api/v1` must be same-origin: `Origin` must match `Host`,
and Fetch Metadata marked as cross-site or same-site is rejected. This includes
WebSocket handshakes. CLI and automation clients that do not send browser origin
metadata are unaffected. Authenticated reverse proxies must preserve `Host`.

API request bodies are capped by `api.max_request_body_bytes`; this applies to
JSON requests and raw `.torrent` uploads. The root `/health` alias remains a
control-plane health endpoint outside `/api/v1`.

Torrent metadata (`.torrent` uploads, bulk base64 `metainfo`, magnet `info`
dicts fetched via BEP 9, watch-folder files, and restored durable state) is
additionally bounded by a shared 16 MiB metadata limit
(`MAX_TORRENT_METADATA_BYTES`) enforced by the core parser before any
piece-sized allocation. A `.torrent` body or assembled magnet `info` dict that
exceeds the limit is rejected with `malformed_torrent` (or `bencode_error` for
raw decoder overruns) regardless of `api.max_request_body_bytes`, which may be
higher for other request payloads. See ADR-0050.

## Health, version, and stats

All paths in this section are under `/api/v1`, except the root `/health` alias.

| Method | Path | Description |
| --- | --- | --- |
| GET | `/health` | Daemon and network health. |
| GET | `/version` | Version and build info. |
| GET | `/stats` | Global stats. |

The root `/health` path is also available without the `/api/v1` prefix.

`/stats` returns aggregate transfer counters plus a `scheduler` object for
large-library diagnostics. Scheduler fields include managed and queued torrent
counts, running engine counts, requested and granted download/metadata slots,
retry-backoff counts, active queue limits, peer-worker budget fields, and
boolean saturation flags for download slots, metadata fetch slots, and peer
worker budget.

## Torrent management

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents` | List torrents. |
| GET | `/torrents/query` | Query torrents with server-side filters, sorting, pagination, counts, and optional grouping. |
| POST | `/torrents` | Add magnet JSON or raw `.torrent` body. |
| POST | `/torrents/magnet` | Add magnet JSON: `{ magnet, download_dir?, paused?, start_behavior? }`. |
| POST | `/torrents/file` | Upload raw `.torrent` body. |
| POST | `/torrents/bulk` | Add multiple magnets and/or base64 `.torrent` payloads. |
| GET | `/torrents/:hash` | Torrent details. |
| GET | `/torrents/:hash/stats` | Per-torrent counters and live engine diagnostics. |
| DELETE | `/torrents/:hash?delete_data=bool` | Remove torrent, optionally deleting data. |
| POST | `/torrents/remove` | Remove multiple torrents: `{ info_hashes, delete_data? }`. |
| POST | `/torrents/:hash/pause` | Pause. |
| POST | `/torrents/:hash/resume` | Resume. |
| POST | `/torrents/:hash/start` | Start now, bypassing queue. |
| POST | `/torrents/:hash/stop` | Stop. |
| POST | `/torrents/:hash/recheck` | Force recheck. |
| POST | `/torrents/:hash/reannounce` | Reannounce. |
| POST | `/torrents/:hash/move` | Move data: `{ path }`. |
| POST | `/torrents/:hash/labels` | Set labels: `{ labels }`. |
| POST | `/torrents/:hash/limits` | Set per-torrent bandwidth limits: `{ download_limit, upload_limit }`, bytes/sec, `0` = unlimited. |

Add requests can start paused while still inserting the torrent into queue
order. For JSON magnet adds, set either `paused: true` or
`start_behavior: "paused"`. For raw `.torrent` uploads, use
`?paused=true` or `?start_behavior=paused` on `/torrents` or
`/torrents/file`. `paused` and `start_behavior` must agree when both are
provided. If add-time free-space preflight is configured, add requests can fail
before write with a storage-capacity error when the target root does not meet the
reserve configured under `[storage]`.

Strict fail-closed network blocking can still put the new torrent in
`network_blocked` instead of `paused`.

Successful add responses mean the torrent record was registered and inserted
into queue order. The daemon does not wait for queue reconciliation, metadata
fetching, tracker announces, peer connections, or engine startup before
returning. Rapid add bursts are coalesced by the daemon scheduler.

Bulk add requests use:

```json
{
  "magnets": ["magnet:?xt=urn:btih:..."],
  "torrent_files": [{ "metainfo": "base64 .torrent bytes" }],
  "download_dir": "/data/downloads",
  "paused": true
}
```

`download_dir`, `paused`, and `start_behavior` apply to every item in the
batch. The response includes `added` items with `{ kind, index, info_hash }`
and `failed` items with `{ kind, index, code, message }`, so one invalid or
duplicate item does not prevent other valid items from being registered.

Bulk remove requests use:

```json
{ "info_hashes": ["40hex..."], "delete_data": false }
```

The response includes `removed` and `not_found` info-hash arrays. The daemon
removes all found records and reconciles queue state once for the batch.

Large-library clients should use `GET /torrents/query` instead of repeatedly
fetching the full list. Supported query parameters are:

| Parameter | Description |
| --- | --- |
| `q` | Case-insensitive search across name, info hash, state, health, label, and storage root. |
| `state` | Comma-separated torrent states such as `downloading`, `paused`, or `error`. |
| `health` | Comma-separated health labels such as `good`, `stalled`, or `network_blocked`. |
| `label` | Comma-separated labels; unlabeled torrents use `unlabeled`. |
| `storage_root` | Comma-separated download roots; torrents without an explicit root use `default`. |
| `performance` | Comma-separated buckets: `active`, `error`, `complete`, `transferring`, `has_peers`, `no_peers`, `stalled`, `unhealthy`. |
| `min_peers`, `max_peers` | Filter by the greater of active peer workers and known peers. |
| `min_down_rate`, `min_up_rate` | Filter by current byte/sec rates. |
| `sort` | One of `name`, `state`, `health`, `health_score`, `progress`, `size`, `down_rate`, `up_rate`, `ratio`, `peers`, `added`, `completed`, or `queue`. |
| `dir` | `asc` or `desc`. |
| `page` | 1-based page number. |
| `per_page` | Page size, capped by the daemon; `0` returns counts and groups without rows. |
| `group_by` | Optional grouping: `state`, `health`, `label`, `storage_root`, or `performance`. |

The response data object is:

```json
{
  "rows": [],
  "total": 1000,
  "filtered": 42,
  "page": 1,
  "per_page": 100,
  "page_count": 1,
  "sort": "name",
  "dir": "asc",
  "counts": {
    "states": { "downloading": 10 },
    "health": { "good": 8 },
    "labels": { "linux": 6 },
    "storage_roots": { "/data/linux": 6 },
    "performance": { "active": 10 }
  },
  "groups": [{ "key": "downloading", "label": "Downloading", "count": 10 }]
}
```

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

Peer rows include the discovered peer address, direction, current rates, flags,
and ban state. Negotiated per-peer encryption state is not exposed in this
phase.

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

`PUT /settings` includes `torrent.encryption_mode` and
`[torrent].encryption_mode` values:

- `disabled`
- `preferred` (default)
- `required`

Changing this field is reported in `restart_required_fields` for already-running
torrent tasks.

Per-profile and per-torrent overrides are not yet documented in this phase.

`PUT /settings` validates the full config before persistence, preserves the
existing `api.auth_token` when omitted, applies live-safe fields immediately,
and reports fields that require restart.

## Network

| Method | Path | Description |
| --- | --- | --- |
| GET | `/network/health` | Network containment health. |
| GET | `/network/diagnostics` | Detailed network/path diagnostics. |

`/network/diagnostics` includes transport settings such as `utp_enabled`,
`utp_prefer_tcp`, and `peer_encryption_mode`. See
[Network Containment](network-containment.md) for health state meanings.

## Storage

| Method | Path | Description |
| --- | --- | --- |
| GET | `/storage/roots` | Return diagnostics for configured storage roots, including free space and availability. |

The storage diagnostics response currently includes per-root identity and space
data needed by operators and automation. Typical fields include:

```json
{
  "roots": [
    {
      "path": "/mnt/media/downloads",
      "roles": ["download"],
      "exists": true,
      "is_directory": true,
      "writable": true,
      "filesystem_type": "ext",
      "total_space_bytes": 1024,
      "free_space_bytes": 128,
      "available_space_bytes": 120,
      "required_free_space_bytes": 64,
      "reserve_satisfied": true,
      "torrent_count": 4,
      "active_torrents": 2,
      "active_write_rate": 1048576,
      "active_recheck_rate": 0,
      "warnings": []
    }
  ],
  "minimum_free_space_bytes": 0,
  "minimum_free_space_percent": 0,
  "generated_at": 1783227600
}
```

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

## qBittorrent-compatible API compatibility

When enabled, `/api/v2` is a compatibility adapter over native daemon
operations. It is not a separate data-plane implementation and does not expose
indexing, search, or discovery endpoints.

Enable it with:

```toml
[compatibility.qbittorrent]
enabled = true
```

Authentication follows `api.require_auth`:

- Bearer token flow via `Authorization` / `X-SwarmOtter-Auth`.
- qBittorrent-style `SID` flow via `POST /api/v2/auth/login` and a returned `SID`
  cookie.

Representative automation endpoints:

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
