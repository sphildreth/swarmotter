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
- Browser requests to `/api/v1`, `/transmission/rpc`, and `/api/v2` are guarded
  by one outer origin policy. Same-origin Web UI behavior is identical in both
  authentication modes. Configured unauthenticated LAN listeners still trust
  their network boundary, and non-browser clients remain compatible when both
  Origin and `Sec-Fetch-Site` are absent. A valid Chrome extension Origin is a
  narrow cross-origin exception only in authenticated mode with a valid API
  token. See ADR-0044 and ADR-0049.

## Compatibility contract

- Breaking native API changes require a new version prefix, such as `/api/v2`,
  rather than changing `/api/v1` in place.
- Error codes are part of the automation contract. Rename or removal requires
  the same compatibility treatment as a breaking API field change.
- SSE and WebSocket events share the same event object shape. The daemon
  publishes lifecycle and status changes through the shared broker so clients
  can subscribe instead of polling list endpoints for every update.
- SSE streams use keep-alives, WebSocket streams use pings, and subscribers that
  fall behind the broker buffer receive an `events_dropped` notice.
- Native torrent add requests support add-time options such as paused start
  behavior without requiring add-then-pause sequencing; see ADR-0029.
- Add requests return after registration, queue insertion, and durable state
  persistence; persistence failure restores exact hash-specific snapshots and
  emits/schedules nothing. Expensive queue reconciliation and engine startup
  remain asynchronous and coalesced for rapid add bursts; see ADR-0030 and
  ADR-0054.
- Batch add and remove endpoints are part of the native `/api/v1` compatibility
  contract for clients that submit or operate on many torrents at once; see
  ADR-0031.
- `GET /api/v1/torrents` remains the legacy full-array list endpoint. Large
  libraries should use `GET /api/v1/torrents/query` for explicit filtering,
  sorting, pagination, counts, and grouping without changing the legacy
  response shape; see ADR-0036.
- `GET /api/v1/stats` includes scheduler diagnostics for large libraries:
  configured active caps, requested and granted download/metadata slots,
  running engine counts, retry-backoff counts, peer-worker budget, and
  saturation booleans. These fields are additive to the `/api/v1` stats shape;
  see ADR-0042.
- The scheduler's authoritative peer-session fields are `peer_limit`,
  `peer_permits_in_use`, `peer_permits_available`, and
  `peer_sessions_denied` (ADR-0053). Unlimited global mode reports limit `0`
  and available `null` while in-use still counts observed sessions. Retained
  `peer_worker_global_limit`, `peer_worker_per_torrent_limit`,
  `effective_peer_worker_limit`, `peer_worker_budget`, and worker-saturation
  fields are additive compatibility telemetry for engine worker pressure; they
  do not enforce or measure the process-wide connection cap.
- A peer-limit PATCH or full PUT returns success only after transactional
  data-plane reconstruction succeeds. Full PUT persists only after eligible
  ownership is verified; failures restore prior runtime/config/state files.
- Storage add-time preflight is part of `/api/v1` compatibility: when
  configured reserves are not met on the target storage root, add requests reject
  before data write.
- Optional compatibility endpoints, currently `/transmission/rpc` and `/api/v2`,
  are isolated from the native API and delegate to native daemon operations
  rather than a separate engine. qBittorrent category catalogs derive from
  native labels/profiles and category/profile changes use the native durable
  label and profile-assignment transactions. qBittorrent lifecycle, location, file, tracker, and
  property endpoints plus Transmission's optional `profile` add/set field are
  translations over the same `DaemonOps` behavior; neither surface maintains
  a second torrent store or routing path (ADR-0061).
- `/api/v1/network/health` preserves the flattened containment health shape
  and additively returns non-sensitive `port_mapping` and `port_test` status.
  Dedicated mapping-status/refresh and port-test routes delegate only to the
  daemon's contained runtime operations. Mapping and test results are
  informational and cannot change the containment gate or torrent lifecycle
  (ADR-0059, ADR-0060).
- Origin protection is not compatibility-specific authentication. The shared
  guard runs before native auth, Transmission session negotiation, qBittorrent
  SID handling, compatibility-enabled checks, request extraction, and daemon
  operations. It rejects with HTTP 403 using the native JSON envelope,
  Transmission JSON error object, or qBittorrent plain-text error as appropriate.
- An Origin-bearing request must contain exactly one UTF-8 Origin and Host. The
  Origin must be a serialized `scheme://authority` without user information,
  path, query, or fragment and must match the normalized Host authority. Scheme
  is intentionally ignored for TLS termination. `Sec-Fetch-Site` accepts only
  one `same-origin` or `none` value, or no value; every other or duplicate value
  fails closed.
- A `chrome-extension://` Origin is classified separately instead of compared
  to Host. Its authority must be exactly one Chromium extension ID (32 lowercase
  `a`-`p` characters) without port or suffix, and Host must still be one valid
  authority. The outer guard permits it only when `api.require_auth = true` and
  one unambiguous Bearer or `X-SwarmOtter-Auth` value matches `api.auth_token`.
  Auth-disabled mode and missing/invalid/duplicate credentials return 403 before
  extraction or mutation. A token never exempts an HTTP(S) Origin from the
  same-origin rule.
- Authentication policy is shared: when API auth is enabled, compatibility
  adapters must map their auth mechanism back to `api.auth_token`, including
  `/api/v2` Bearer and SID-cookie flows.
- Optional qBittorrent compatibility is intentionally limited to bounded
  automation endpoints and does not include indexer/search/discovery APIs.

## Metadata trust boundary

- Bencoded torrent metadata ingress — `.torrent` uploads to
  `/api/v1/torrents`, bulk base64 `metainfo`, magnet `info` dicts fetched via
  BEP 9, and watch-folder files — shares one bounded parser in
  `swarmotter-core` (see ADR-0050).
- The shared bencoded metadata limit `MAX_TORRENT_METADATA_BYTES` (16 MiB) is
  enforced by the core parser before any piece-sized allocation, independent
  of `api.max_request_body_bytes` (which may be higher for other request
  payloads).
- Raw torrent uploads are streamed into a bounded accumulator capped at the
  lower of `api.max_request_body_bytes` and 16 MiB. If the configured API limit
  is lower, crossing it returns HTTP 413 `payload_too_large`; otherwise crossing
  16 MiB returns `malformed_torrent`. Bulk and Transmission base64 metainfo use
  a decoder that stops before its decoded output can exceed 16 MiB.
- Restored daemon state is JSON rather than bencode. Its piece hashes are
  decoded through a sequence capped at `MAX_TORRENT_PIECES`, each hash is
  required to encode exactly 20 bytes before hex decoding/copying, and each
  restored `TorrentMeta` is checked with `TorrentMeta::validate()` before
  runtime use.
- Oversized or malformed bencoded metadata is rejected with
  `malformed_torrent` (or `bencode_error` for raw decoder overruns). No
  malformed input may panic the daemon or cause an unbounded or piece-sized
  allocation outside the documented limits.

## Tracker API contract

- `GET /api/v1/torrents/:hash/trackers` retains the existing announce fields
  and adds `scrape_status`, `last_scrape`, nullable `scrape_seeders`,
  `scrape_leechers`, `scrape_downloads`, and `last_scrape_error` (ADR-0055).
  Status values are `not_contacted`, `updating`, `ok`, `error`, or
  `unsupported`.
- Scrape attempt state and last-success counts are separate. A failed or
  task-aborted attempt updates status/time/error without erasing prior counts.
  Unsupported UDP/non-derivable trackers report `unsupported` without a
  network call.
- Existing numeric `seeders` and `leechers` prefer a successful announce and
  fall back to retained scrape counts when announce has not succeeded.
  `downloads` uses the retained scrape value when available. This is additive;
  Transmission/qBittorrent mappings keep their existing fields and consume the
  same compatibility counts without new native scrape objects.

## Storage API contract

- `GET /api/v1/storage/roots` exposes storage-root diagnostics used for
  operator visibility, add-time preflight checks, and root-control admission
  diagnostics. Each row additively reports declared active bytes, active
  rechecks, the matching lexical control path, configured limits, and
  saturation warnings.

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
- Repeatable `[[storage.root_controls]]` entries define the local
  `max_active_downloads`, `max_active_bytes`,
  `max_write_bytes_per_second`, and `max_concurrent_rechecks` budgets. The
  most-specific matching lexical active-write root wins; zero means unlimited.
  Root-budget saturation keeps work queued rather than converting it into a
  permanent payload error. See ADR-0056.

## Policy-profile API contract

- `GET`/`PUT /api/v1/profiles` expose and replace only the persisted
  `{ profiles, labels }` configuration section. Full replacement validates
  profile names, references, paths, and finite/non-negative ratio values before
  it affects runtime state.
- Native add requests and bulk add may supply a profile and labels. Labels are
  assigned before resolution so their mapping can select a profile before
  storage is chosen. Watch and compatibility intake paths follow the same
  ordering.
- `GET /api/v1/torrents/:hash/policy` returns the effective profile plus every
  resolved storage, queue, seeding, and bandwidth value with its source layer.
  Serialized source kinds are `global`, `profile`, `label`, `torrent`,
  `legacy_torrent`, `profile_storage_snapshot`,
  `registration_storage_snapshot`, `existing_storage_snapshot`, and
  `initial_admission_snapshot`. The registration storage source means the
  resolved storage choice was fixed at registration; the initial admission
  source means the torrent's one-time start-or-paused decision was captured
  and cannot be retroactively changed by later inherited-policy edits. `PUT`
  sets or clears an explicit assignment transactionally.
- Resolved storage and initial admission are durable create-time snapshots.
  Profile reassignment and label changes preserve existing storage, never move
  payload data, and cannot revoke a queued torrent's admission. Queue priority,
  seeding, and rate caps remain live for inheriting torrents. Legacy records
  are migrated transactionally during profile replacement.

## Peer-admission API contract

- `GET`/`PUT /api/v1/peer-filter` expose and replace the global
  `{ enabled, rules, blocklist_paths, manual_bans, blocked_client_ids }`
  policy. GET includes the active direct rules, local import paths/outcomes,
  manual bans, client-ID prefixes, and rejection counters for the active
  compiled policy instance.
- `POST /api/v1/torrents/:hash/peers/ban` and `unban` modify the same global
  manual-ban list; a ban initiated from one torrent is therefore effective for
  all torrents. `POST /api/v1/peer-filter/unban` removes a global manual ban
  without requiring a selected torrent.
- Replacement parses and compiles every local source before it affects active
  peer work. Engine/session reconstruction and persistence are transactional;
  a failed update retains the preceding policy and data-plane generation.
- Filtering is an admission decision only. It rejects candidates before socket
  creation or inbound service, with peer-ID-prefix checks after handshake, and
  does not change the required contained network path (ADR-0058).

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

## Per-torrent seeding contract

- Native list/detail summaries expose nullable `error`. A bounded engine run
  that exhausts every attempted configured tracker with no usable DHT, PEX,
  direct-peer, or webseed source transitions to `tracker_error` and retains the
  last tracker failure there. Reannounce, Resume, or Start Now clears the error
  and retries; a successful tracker or alternative source prevents the
  terminal classification.
- Native list and detail summaries expose the persisted `seeding` object,
  `seeding_status`, ratio/uploaded counters, and resolved
  `effective_ratio_limit` / `effective_idle_limit`.
- `PUT /api/v1/torrents/:hash/seeding` replaces all three policy fields. The
  body must contain exactly `ratio_limit`, `idle_limit`, and `seed_forever`.
  Limits are non-negative (`ratio_limit` must also be finite); `null` inherits
  the global setting and zero remains an explicit target.
- The daemon persists the replacement before reporting success. Persistence
  failure restores the previous policy while leaving the truthful live
  lifecycle unchanged. Successful replacement immediately stops a newly-met
  target or requeues a complete automatic stop, but never resumes
  `stopped_manual`.
- Transmission and qBittorrent retain their documented compatibility shapes.
  Existing ratio/uploaded fields use native truthful accounting; no new
  compatibility policy controls are implied by this endpoint.

## Watch-folder API and event contract

- `ImportResult` retains `path`, `success`, `info_hash_hex`, `error`, and
  `duplicate`. Additive fields are `outcome` (`imported`, `duplicate`,
  `permanent_failure`, `transient_failure`) and nullable
  `post_action_error`. History is insertion ordered, capped at 10,000, and not
  persisted.
- Status calls are observational: they do not advance stability. Pending counts
  include unseen/changed/stabilizing/transient-retry files and exclude unchanged
  processed fingerprints.
- Watch duplicate is a successful import result with the existing hash and no
  torrent/queue/settings mutation. Native API duplicate behavior remains the
  established `duplicate_torrent` conflict.
- `watch_folder_imported` covers imported and duplicate outcomes;
  `watch_folder_failed` covers permanent and transient attempts. Payloads carry
  path, outcome, compatibility flags, hash, primary error, and post-action
  error. Unstable or changed-during-read files emit no result/event (ADR-0054).

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
