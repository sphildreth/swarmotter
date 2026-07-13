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

Browser requests to every control route (`/api/v1`, `/transmission/rpc`, and
`/api/v2`) must be same-origin, except for the authenticated Chrome extension
client described below. An Origin-bearing request must provide exactly one
valid UTF-8 `Origin` and `Host`; an ordinary browser Origin must be only
`scheme://authority`, its normalized host and explicit port must match Host, and
it may not contain user information, a path, query, or fragment. `Origin: null`,
opaque, foreign, malformed, duplicate, multi-value, and invalid-byte headers are
rejected. Scheme is intentionally not compared, so a TLS-terminating reverse
proxy is supported when it preserves the public Host authority.

`Sec-Fetch-Site` permits only `same-origin`, `none`, or an absent header.
`same-site`, `cross-site`, unknown, duplicated, and invalid-byte values are
rejected. This includes WebSocket and SSE requests. The shared
`browser_origin_guard` is the outermost control-route layer, before native
authentication, Transmission session negotiation, qBittorrent SID handling,
compatibility-enabled checks, request extraction, and daemon operations. The
same-origin and headerless-client policy is identical whether
`api.require_auth` is true or false. CLI and automation clients with neither
Origin nor `Sec-Fetch-Site` are unaffected.

A Chrome Manifest V3 extension service worker with host permission sends an
Origin such as `chrome-extension://abcdefghijklmnopabcdefghijklmnop`; Chromium
uses `Sec-Fetch-Site: none` for this privileged request. SwarmOtter accepts this
cross-origin shape only when all of these conditions hold:

- the Origin contains exactly one valid Chrome extension ID: 32 lowercase
  characters from `a` through `p`, with no port, path, query, or fragment;
- `Host` is one valid authority and Fetch Metadata is otherwise permitted;
- `api.require_auth = true`; and
- exactly one `Authorization: Bearer <token>` or `X-SwarmOtter-Auth: <token>`
  value matches `api.auth_token`.

Auth-disabled mode always rejects extension Origins, even if an `auth_token`
value is present. A valid token never permits a foreign HTTP(S), `null`, opaque,
or malformed Origin. This is token-authenticated extension access, not a broad
extension-origin allowlist.

An origin rejection always uses HTTP 403 but preserves the selected surface's
error format: the native API returns its JSON error envelope with
`cross_origin_forbidden` or the extension-specific
`extension_origin_forbidden`, Transmission returns a JSON `error` object, and
qBittorrent returns plain-text `Forbidden`. The native extension error explains
that authenticated mode and a valid configured token are required. Rejections
are never redirected to the Web UI.

API request bodies are capped by `api.max_request_body_bytes`; this applies to
JSON requests and raw `.torrent` uploads. The root `/health` alias remains a
control-plane health endpoint outside `/api/v1`.

Bencoded torrent metadata (`.torrent` uploads, bulk base64 `metainfo`, magnet
`info` dicts fetched via BEP 9, and watch-folder files) is additionally bounded
by a shared 16 MiB limit (`MAX_TORRENT_METADATA_BYTES`) enforced by the core
parser before any piece-sized allocation. A `.torrent` body or assembled magnet
`info` dict that exceeds the metadata limit is rejected with
`malformed_torrent` (or `bencode_error` for raw decoder overruns). Raw torrent
uploads are streamed and stop at the lower of the configured request limit and
16 MiB: when `api.max_request_body_bytes` is lower, crossing that configured
limit returns HTTP 413 with `payload_too_large` before the metadata-specific
error. Bulk and Transmission base64 metainfo decoding stops before decoded
output can exceed 16 MiB.

Restored daemon state is JSON, not bencode. Its piece-hash sequence is capped at
`MAX_TORRENT_PIECES`; each hash must encode exactly 20 bytes before hex
decoding/copying, and restored metainfo must pass `TorrentMeta::validate()`
before runtime use. See ADR-0050.

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

The authoritative process-wide peer-session fields are:

- `peer_limit`: configured process-wide limit; `0` means unlimited.
- `peer_permits_in_use`: observed live inbound plus outbound peer sessions.
- `peer_permits_available`: remaining bounded capacity, or `null` when
  unlimited.
- `peer_sessions_denied`: inbound sockets rejected by the global or routed
  per-torrent cap before a session starts.

The older `peer_worker_global_limit`, `peer_worker_per_torrent_limit`,
`effective_peer_worker_limit`, `peer_worker_budget`, and saturation values are
retained compatibility diagnostics for engine worker scheduling. They are not
the process-wide connection-limit authority.

## Torrent management

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents` | List torrents. |
| GET | `/torrents/query` | Query torrents with server-side filters, sorting, pagination, counts, and optional grouping. |
| POST | `/torrents` | Add magnet JSON or raw `.torrent` body. |
| POST | `/torrents/magnet` | Add magnet JSON: `{ magnet, download_dir?, paused?, start_behavior?, profile?, labels? }`. |
| POST | `/torrents/file` | Upload raw `.torrent` body; query supports `paused`, `start_behavior`, `profile`, and comma-separated `labels`. |
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
| PUT | `/torrents/:hash/seeding` | Replace the persisted per-torrent ratio/idle/forever policy. |
| GET | `/torrents/:hash/policy` | Effective profile values plus the source of every value. |
| PUT | `/torrents/:hash/policy` | Set/clear explicit profile: `{ profile: "name" }` or `{ profile: null }`. |

Torrent list/detail rows include nullable `error`, `uploaded`, `ratio`,
`seeding`, `seeding_status`, `effective_ratio_limit`, and
`effective_idle_limit`. `error` retains the last terminal/runtime failure for
operator diagnosis and is `null` after a successful retry or lifecycle action
that clears it. The persisted `seeding` object has nullable `ratio_limit` and
`idle_limit` fields plus `seed_forever`. Nullable targets inherit `[seeding]`
globals; explicit zero is an immediate target. `seed_forever: true` makes both
effective fields `null` without erasing stored overrides.

When every attempted configured tracker fails and no usable DHT, PEX,
direct-peer, or webseed source exists, the daemon stops the bounded engine
attempt in `tracker_error` and exposes the last tracker failure in `error`.
`POST /torrents/:hash/reannounce` or Resume/Start Now clears the terminal error
and starts a new attempt. A successful tracker response or usable alternative
source prevents `tracker_error`.

The replacement request requires exactly these keys:

```json
{
  "ratio_limit": 1.5,
  "idle_limit": 1800,
  "seed_forever": false
}
```

`ratio_limit` must be a finite non-negative number or `null`; `idle_limit`
must be a non-negative integer number of seconds or `null`; and
`seed_forever` must be boolean. Missing, unknown, negative, non-finite,
fractional-idle, or numeric-overflow input returns `invalid_argument`. The
daemon persists before success, then immediately re-evaluates active or
automatically stopped complete content. It never auto-resumes a manual pause.

`seeding_status` is one of `not_eligible`, `queued`, `active`,
`stopped_ratio`, `stopped_idle`, or `stopped_manual`. Fully verified queued
content is `completed` + `queued`; a live registered seeder is `seeding` +
`active`; automatic stops return to `completed`; and a complete operator pause
is `paused` + `stopped_manual`. During containment failure the coarse state is
`network_blocked` and the fine status is preserved for recovery.

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

## Policy profiles

| Method | Path | Description |
| --- | --- | --- |
| GET | `/profiles` | Return the complete `{ profiles, labels }` configuration section. |
| PUT | `/profiles` | Replace the complete `{ profiles, labels }` section after validation. |
| PUT | `/torrents/:hash/encryption-mode` | Set a durable per-torrent peer-wire encryption override: `{ "encryption_mode": "disabled" \| "preferred" \| "required" }`; `{ "encryption_mode": null }` clears it. |

Add requests may include `profile` and `labels`; labels are applied before
resolution so a label mapping can select a profile. Bulk add accepts the same
top-level `profile` and `labels` values for every item. An unknown or empty
profile is rejected. Compatibility adapters likewise attach their category or
labels before registration.

`GET /torrents/:hash/policy` returns the selected profile plus each effective
storage, queue, seeding, bandwidth, and peer-encryption value with a
machine-readable source:
`global`, `profile`, `label`, `torrent`, `legacy_torrent`,
`profile_storage_snapshot`, `registration_storage_snapshot`,
`existing_storage_snapshot`, or `initial_admission_snapshot`. This lets
clients explain why a value applies without duplicating daemon precedence
rules. `registration_storage_snapshot` means the resolved storage choice was
fixed when the torrent was registered; `initial_admission_snapshot` means the
one-time start-or-paused decision was captured for that torrent, so later
profile, label, or global edits do not retroactively change its admission.
The encryption override endpoint requires the `encryption_mode` key: omitting
it is rejected, while explicit JSON `null` restores normal inheritance.

Resolved storage is captured at registration, including a global/no-profile
result. Assigning or clearing a profile, or changing labels later, preserves
the torrent's existing completed and incomplete locations; use
`POST /torrents/:hash/move` to relocate data. The initial start-or-paused
decision is captured too, so profile/global auto-start edits cannot revoke a
new or migrated queued torrent's admission. Queue priority, seeding, and rate
caps update live while a torrent inherits them. Legacy state is migrated
transactionally during a profile configuration replacement.

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
  "paused": true,
  "profile": "linux-release",
  "labels": ["linux"]
}
```

`download_dir`, `paused`, `start_behavior`, `profile`, and `labels` apply to every item in the
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
message. They also expose:

- `scrape_status`: `not_contacted`, `updating`, `ok`, `error`, or
  `unsupported`.
- `last_scrape`: Unix seconds for the latest attempt.
- `scrape_seeders`, `scrape_leechers`, and `scrape_downloads`: nullable counts
  retained from the latest successful exact-key BEP 48 response.
- `last_scrape_error`: the latest failed-attempt or task-failure detail without
  erasing retained counts.

Initial download discovery, magnet discovery, explicit/periodic reannounce,
completion, and active seeder announces schedule supported HTTP/HTTPS scrape.
Only tracker paths whose final component begins with `announce` are derivable;
UDP scrape is `unsupported`. `seeders` and `leechers` prefer a successful live
announce, then fall back to retained scrape counts. `downloads` uses retained
scrape data when available. Existing compatibility adapters keep their prior
field shapes.

## Peers

| Method | Path | Description |
| --- | --- | --- |
| GET | `/torrents/:hash/peers` | List peers. |
| POST | `/torrents/:hash/peers/ban` | Add or update a global manual ban: `{ ip, reason? }`. |
| POST | `/torrents/:hash/peers/unban` | Remove a global manual ban: `{ ip }`. |

Peer rows include the discovered peer address, direction, current rates, flags,
and ban state. Negotiated per-peer encryption state is not exposed in this
phase.

## Peer admission policy

| Method | Path | Description |
| --- | --- | --- |
| GET | `/peer-filter` | Return active direct rules/import paths, local import results, manual bans, client-ID prefixes, and rejection counters. |
| PUT | `/peer-filter` | Replace `{ enabled, rules, blocklist_paths, manual_bans, blocked_client_ids }`. |
| POST | `/peer-filter/unban` | Remove a global manual ban: `{ ip }`. |

Manual bans are global by IP even when created from a torrent peer view. A
replacement validates and compiles all local sources before it affects live
peer work. The status response contains trimmed direct `rules`,
`blocklist_paths`, per-source import outcomes, manual bans, client-ID prefixes,
and counters for the active compiled policy instance; those counters reset on a
successful replacement. It also reports any fail-closed loading detail.

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
| GET | `/settings` | Get configuration with API auth token and SOCKS5 password redacted. |
| PATCH | `/settings` | Update live-safe runtime settings. |
| PUT | `/settings` | Replace full configuration atomically after validation. |

`PATCH /settings` updates live-safe bandwidth, queue, and seeding fields.

`PUT /settings` accepts `[torrent].encryption_mode` with these values:

- `disabled`
- `preferred` (default)
- `required`

The global mode applies MSE/PE to contained TCP and uTP peer streams. In
`preferred` mode, failed negotiation may retry plaintext only on the same
selected contained transport; `required` never retries plaintext. Changing the
global field live-rebuilds existing data-plane tasks and does not require a
process restart. A named profile may set `encryption_mode`, and the per-torrent
endpoint overrides it durably. Profile or label-map changes restart only active
download/metadata engines whose resolved mode changes after persistence. Future
inbound TCP seeding sessions use the refreshed mode; existing negotiated
sessions retain their established wire stream.

Named profiles are part of the full settings configuration under `profiles`;
the dedicated `/profiles` endpoints are preferred when only that section needs
to change.

`PUT /settings` validates the full config before persistence, preserves the
existing `api.auth_token` when omitted, applies live-safe fields immediately,
and reports fields that require restart. It also preserves a redacted
`network.socks5.password` only when the submitted SOCKS5 username is unchanged;
clearing or changing the username requires a complete new credential pair.

## Network

| Method | Path | Description |
| --- | --- | --- |
| GET | `/network/health` | Network containment health plus non-sensitive port-mapping and listen-port-test status. |
| GET | `/network/port-mapping` | Read the current opt-in router mapping status without sending router traffic. |
| POST | `/network/port-mapping/refresh` | Immediately reconcile the configured router mapping through the contained path. |
| POST | `/network/port-test` | Run or return the fresh cached operator-configured listen-port test. |
| GET | `/network/diagnostics` | Detailed network/path diagnostics. |

`/network/diagnostics` includes transport settings such as `utp_enabled`,
`utp_prefer_tcp`, `peer_encryption_mode`, `socks5_enabled`, and
`socks5_udp_blocked`. SOCKS diagnostics reveal neither proxy host nor
credentials. See
[Network Containment](network-containment.md) for health state meanings.

The `port_test` object returned by `/network/health` is informational and
contains `enabled`, `endpoint_configured`, the TCP listener port, state,
timestamps, and bounded detail—but never the configured endpoint URL. States
are `unknown`, `open`, `closed`, `error`, or `timeout`. A POST only sends a
request when testing is enabled and an endpoint is configured; a fresh cached
result is reused. See [Configuration](configuration.md#router-port-mapping-and-listener-reachability).

The `port_mapping` object returned by `/network/health` and
`/network/port-mapping` contains its enabled flag, configured protocol order,
listener/external port, active protocol, local gateway diagnostic, attempt and
lease timestamps, state, and bounded detail. States are `disabled`, `pending`,
`active`, `unavailable`, `blocked`, or `error`. `POST /network/port-mapping/refresh`
does not bypass strict containment: it returns an informational blocked or
unavailable status if the contained path or router cannot complete the request.

## Storage

| Method | Path | Description |
| --- | --- | --- |
| GET | `/storage/roots` | Return diagnostics for configured and state-placement roots, including free space, mount data, actual local I/O, and root controls. |

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
      "mount_point": "/mnt/media",
      "mount_options": ["rw", "relatime"],
      "mount_source": "/dev/sdb1",
      "total_space_bytes": 1024,
      "free_space_bytes": 128,
      "available_space_bytes": 120,
      "required_free_space_bytes": 64,
      "reserve_satisfied": true,
      "torrent_count": 4,
      "active_torrents": 2,
      "active_bytes": 67108864,
      "active_write_rate": 1048576,
      "active_recheck_rate": 0,
      "sustained_write_bytes_per_second": 1048576,
      "sustained_verification_bytes_per_second": 524288,
      "cow_strategy": "conservative",
      "cow_strategy_supported": true,
      "active_rechecks": 0,
      "root_control_path": "/mnt/media",
      "max_active_downloads": 2,
      "max_active_bytes": 107374182400,
      "max_write_bytes_per_second": 52428800,
      "max_concurrent_rechecks": 1,
      "warnings": []
    }
  ],
  "minimum_free_space_bytes": 0,
  "minimum_free_space_percent": 0,
  "generated_at": 1783227600
}
```

`active_bytes` is the aggregate declared payload budget reserved by active
engines, not free space consumed on the filesystem. A limit value of `0` means
unlimited. `root_control_path` is `null` when no `[[storage.root_controls]]`
entry applies; nested controls resolve to the most-specific lexical root.
`sustained_write_bytes_per_second` and
`sustained_verification_bytes_per_second` are observed local storage I/O, not
peer transfer rates; verification excludes ordinary seeding reads. Mount fields
are best-effort and may be `null` in restricted containers or on platforms that
do not expose compatible mount metadata. Roles also identify configured
resume/state/temporary/log placement roots. `cow_strategy_supported` is `null`
when the host cannot determine support safely; an explicit unsupported NOCOW
request fails before payload bytes are written.

## Watch folders

| Method | Path | Description |
| --- | --- | --- |
| POST | `/watch/scan` | Trigger a scan. |
| GET | `/watch/history` | Import history. |
| GET | `/watch/status` | Watch-folder status, folder readiness, and recent imports. |

Watch `recent_imports`, history rows, and each folder's `last_result` retain the
compatibility fields `path`, `success`, `info_hash_hex`, `error`, and
`duplicate`, and add:

- `outcome`: `imported`, `duplicate`, `permanent_failure`, or
  `transient_failure`.
- `post_action_error`: `null` or the archive/delete/failure-move error. A
  post-action error does not replace the primary outcome.

History is insertion ordered, in-memory only, and capped at the newest 10,000
rows. Unstable first/changed observations produce no row. `pending_torrent_files`
counts unseen, changed, stabilizing, and transient-retry files, but excludes an
unchanged fingerprint already processed in this daemon run. Calling status does
not advance stability.

Watch `duplicate` is a successful operational outcome: the existing torrent
and queue entry remain byte-for-byte/position-for-position unchanged, and the
configured success file action runs. This does not change the native torrent-
add compatibility contract; an API duplicate still returns HTTP 409 with
`duplicate_torrent`. New API/watch adds share a durable registry/queue
transaction, so persistence failure returns the existing typed error envelope
without a visible torrent, queue entry, add event, or scheduled start.

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
`port_mapping_changed`, `port_test_changed`, `watch_folder_imported`,
`watch_folder_failed`, `settings_changed`, and `daemon_health_changed`.

`watch_folder_imported` covers both `imported` and successful `duplicate`.
`watch_folder_failed` covers permanent and transient attempts. Their payloads
contain `path`, `outcome`, `success`, `duplicate`, `info_hash`, `error`, and
`post_action_error`; the top-level event `info_hash` is present when parsing
produced one. A changing/unstable observation emits neither event.

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

The browser-origin policy in [Authentication and limits](#authentication-and-limits)
runs before the enabled check, authentication, and session negotiation. An
origin rejection returns HTTP 403 with the Transmission JSON `error` object and
does not issue a session ID or dispatch an RPC method.

A Chrome extension calling this compatibility route must satisfy the shared
extension rule before Transmission authentication: enable API auth and send the
configured token as Bearer or `X-SwarmOtter-Auth`. Transmission Basic auth by
itself does not identify an allowed extension Origin at the outer guard.

The adapter currently supports common session, torrent lifecycle, queue, and
helper calls:

- `session-get`, `session-set`, `session-stats`, `session-close`
- `torrent-get`, `torrent-start`, `torrent-start-now`, `torrent-stop`,
  `torrent-verify`, `torrent-reannounce`
- `torrent-add`, `torrent-remove`, `torrent-set`, `torrent-set-location`,
  `torrent-rename-path`
- `queue-move-top`, `queue-move-up`, `queue-move-down`, `queue-move-bottom`
- `free-space`, `port-test`, `blocklist-update`

Per-torrent seeding policy does not add Transmission adapter options. Existing
`torrent-get` fields `uploadRatio`/`upload_ratio` and
`uploadedEver`/`uploaded_ever` use the same truthful native accounting; an
unsupported per-torrent `seedRatioLimit` request remains `null`.

`torrent-remove` maps `delete-local-data` and `delete_local_data` to the native
delete-data option.

`torrent-add` accepts magnet links via `filename` and base64 torrent metadata
via `metainfo`. Remote HTTP/HTTPS torrent metadata URLs are rejected.

`torrent-add` and `torrent-set` additionally accept an optional native
compatibility extension `profile`. A string selects a configured profile during
the same durable add/assignment path used by the native API; explicit `null` in
`torrent-set` clears an existing assignment. Labels are present before profile
resolution. Add and list responses include truthful state, completion,
directory, labels, and terminal error data where their established field names
allow it. Transmission `port-test` maps to the latest configured listener-test
result and returns `port_is_open: true` only for an `open` result.

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

The browser-origin policy in [Authentication and limits](#authentication-and-limits)
runs before the enabled check, login/SID handling, form extraction, and daemon
operations. An origin rejection returns HTTP 403 with plain-text `Forbidden`;
it does not create a SID or dispatch the requested operation.

A Chrome extension must send the configured Bearer or `X-SwarmOtter-Auth`
token on every `/api/v2` request. A qBittorrent `SID` cookie alone does not
authorize the cross-origin exception because the shared guard runs before SID
handling.

Representative automation endpoints:

- `GET /api/v2/app/version`
- `GET /api/v2/app/webapiVersion`
- `GET /api/v2/torrents/info`
- `GET /api/v2/torrents/categories`
- `POST /api/v2/torrents/add`
- `POST /api/v2/torrents/delete`
- `POST /api/v2/torrents/pause`
- `POST /api/v2/torrents/resume`
- `POST /api/v2/torrents/start`
- `POST /api/v2/torrents/stop`
- `POST /api/v2/torrents/recheck`
- `POST /api/v2/torrents/reannounce`
- `POST /api/v2/torrents/setCategory`
- `POST /api/v2/torrents/setLocation`
- `POST /api/v2/torrents/renameFile`
- `GET /api/v2/torrents/properties?hash=...`
- `GET /api/v2/torrents/trackers?hash=...`
- `GET /api/v2/torrents/files?hash=...`

qBittorrent categories are derived from native labels, profile names, and
label-to-profile mappings; there is no second category store. Supplying a
category at add time always records the label. If it exactly matches a named
profile, it also selects that profile before registration so its add-time
storage and start policy apply. Category mutation continues to use native label
and profile-assignment transactions. The new lifecycle, location, rename,
tracker, and file endpoints delegate to their native operations.

The qBittorrent torrent-info response continues to expose its documented
`ratio` and `uploaded` counters from the native summary. It does not claim
`ratio_limit` or `seeding_time_limit` policy options.
