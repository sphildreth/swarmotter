# Changelog

This file records notable project changes. It follows the
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format and uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

All notable changes are recorded by capability and acceptance criteria, not by
date or duration estimates.

## [2.0.3] - [2026-07-20]

### Fixed

- **Prowlarr Transmission compatibility:** the Transmission adapter now
  accepts safe `GET /transmission/rpc` session negotiation, returns the
  required session challenge before Prowlarr sends RPC methods with `POST`, and
  reports a parseable Transmission compatibility version separately from the
  native SwarmOtter product version. Prowlarr's required torrent-list duration
  and seeding-policy fields now return compatible numeric values instead of
  `null`; the complete connection and listing flow is verified with Prowlarr
  2.3.x.

## [2.0.2] - [2026-07-18]

### Fixed

- **Bounded multi-file storage memory:** read-only verification and seeding
  handles are no longer retained after each operation, and the reusable
  writable-handle working set is capped. This prevents Tokio's per-file I/O
  buffers and descriptors from exhausting daemon memory while rechecking or
  downloading torrents with large file counts. Sparse fast resume now trusts
  structurally valid piece progress when complete file identity/change stamps
  still match, rather than comparing sparse logical file lengths with verified
  bytes and forcing a full recheck after every restart.

## [2.0.1] - [2026-07-17]

### Fixed

- **Durable torrent restarts:** metadata-discovery snapshots no longer replace
  synthetic placeholder progress with an incompatible zero-piece bitfield, and
  transient uninitialized payload-engine snapshots cannot overwrite valid
  progress. Persistence now rejects piece-progress mismatches before commit,
  while startup narrowly normalizes the legacy zero-completion shape already
  written for unresolved magnets so one affected record cannot prevent the
  daemon and control plane from starting.

## [2.0.0] - [2026-07-14]

### Upgrade notes

- Existing `1.x` installations that omitted `[network]` must configure a
  strict interface/source/namespace path before upgrading. Explicit
  `mode = "disabled"` is accepted only for development or when a separately
  enforced boundary supplies containment. Validate the migrated file with
  `swarmotterd --check-config --config PATH` before restarting.

### Added

- **BEP 52 v2/hybrid interoperability:** SwarmOtter now accepts and operates
  v1, pure-v2, and hybrid magnets and `.torrent` files with explicit SHA-1/
  SHA-256 identities from exact bencoded `info` bytes. The separate pure-v2
  engine validates file trees and piece layers, verifies SHA-256 Merkle roots,
  uses contained v2 peer transfer plus tracker/DHT/metadata discovery, and
  persists full-key fast resume. Registry, queue, SQLite, native API, Web UI,
  qBittorrent, and Transmission paths preserve canonical 40/64-character
  locators; hybrid v2 locators resolve as aliases of their v1-primary record.
  No durable or API path truncates a v2 identity to a peer-wire hash. See
  [ADR-0065](design/adr/0065-bep-52-v2-hybrid-torrent-identity.md).

- **Policy-driven metadata-first intake:** add requests and the Web UI can
  create `.torrent` or magnet previews; a magnet may fetch only contained BEP
  9 metadata and remains durably paused before payload transfer. Named profiles
  now support deterministic tracker-host enablement/priority, structured
  suffix/path-glob/path-segment/size exclusions, content/incomplete
  organization, forced top-level folders, and active-only partial suffixes.
  The resolved intake selection is durable and explainable, with a read-only
  storage-path preview; BEP 53 `so=` can only reduce it, and literal `x.pe`
  hints still pass through peer admission and the contained binder. Completion
  continues to use bounded seed-forever, ratio, or idle policy without deletion
  hooks. The native metainfo endpoint exports only retained byte-exact original
  `.torrent` inputs. See
  [ADR-0066](design/adr/0066-policy-driven-metadata-first-intake.md).

- **SQLite durable library state:** the daemon now uses a
  versioned, local SQLite state store with WAL/full-sync durability, indexed
  registry/queue/health/current-metric/history/audit records, raw metainfo BLOB
  retention, deterministic history caps, and crash-safe rollback snapshots.
  Valid legacy JSON state migrates in place on its first successful save;
  full v1/v2 keys and exact original `.torrent` documents remain distinct from
  canonical magnet `info` bytes. The offline projection rebuild validates a
  supported database and rebuilds only derived indexes, refusing missing,
  legacy, corrupt, or unsupported state rather than attempting a broad repair.
  See
  [ADR-0067](design/adr/0067-sqlite-durable-library-state.md).

- **Contained SOCKS5 TCP proxy:** optional SOCKS5 `CONNECT` support now routes
  outbound peer TCP, HTTP(S) tracker/scrape, and webseed requests through the
  existing fail-closed network path. It supports no-authentication and RFC 1929
  credentials, resolves target hostnames remotely through the proxy, redacts
  credentials from settings views, and never falls back to a direct target
  connection. This initial TCP-only scope requires DHT and uTP to be disabled
  and rejects UDP tracker traffic rather than bypassing the proxy. See
  [ADR-0062](design/adr/0062-contained-socks5-tcp-proxy.md).

- **Complete contained MSE/PE policy:** peer-wire encryption now works over
  contained TCP and uTP streams, with global, named-profile, and durable
  per-torrent modes. `required` mode never retries plaintext, while
  `preferred` retries only the already-selected contained transport. The native
  API and Web UI show the effective value and its inheritance source. See
  [ADR-0063](design/adr/0063-contained-mse-utp-and-effective-encryption-policy.md).

- **Filesystem-aware storage strategy and placement:** storage diagnostics now
  report best-effort mount details and observed payload-write/verification
  throughput. Operators can place resume metadata, daemon state, and fallback
  payload storage deliberately, and may explicitly request Btrfs NOCOW only
  for new files; unsupported requests fail instead of silently changing
  strategy. See
  [ADR-0064](design/adr/0064-filesystem-aware-storage-strategy-and-state-placement.md).

- **Contained router port mapping:** opt-in NAT-PMP and UPnP mapping now
  creates, refreshes, reports, and best-effort removes the TCP peer-listener
  lease only through the configured strict, fail-closed interface path. Router
  discovery, UDP exchange, and SOAP control requests never create a default
  route socket; mapping failure is a visible reachability condition rather
  than a containment bypass. See
  [ADR-0059](design/adr/0059-contained-opt-in-router-port-mapping.md).

- **Operator-configured listen-port reachability tests:** an optional bounded
  HTTP(S) test endpoint can report whether the TCP peer listener is externally
  open. Runs are contained, serialized, cached, surfaced in the native API,
  Transmission compatibility response, and Web UI, and remain informational
  when the endpoint or network path fails. No third-party endpoint is bundled.
  See
  [ADR-0060](design/adr/0060-contained-listener-reachability-testing.md).

- **Broader automation compatibility:** qBittorrent-compatible categories,
  properties, tracker/file inspection, recheck/reannounce, location, and
  rename workflows now delegate to native durable operations. Transmission
  add/set accepts explicit named profiles and reports truthful status,
  completion, labels, and errors. Both adapters retain native authorization,
  origin protection, and network containment, without adding search, indexers,
  or discovery APIs. See
  [ADR-0061](design/adr/0061-compatible-automation-profile-and-lifecycle-parity.md).

- **Storage-root resource controls:** repeatable
  `[[storage.root_controls]]` entries now provide independently observable,
  longest-path-matched active-download, declared-byte, verified-write, and
  full-recheck budgets. Queue admission is atomic, bounded rechecks release
  capacity on cancellation, and the API, Doctor view, and Settings expose
  limits, use, and saturation without changing torrent network containment.
  See [ADR-0056](design/adr/0056-storage-root-resource-controls.md).

- **Named policy profiles and explainable inheritance:** profiles can be
  selected explicitly, by watch folder, or by deterministic label mapping for
  storage, queue, seeding, and per-torrent bandwidth behavior. Creation-time
  resolved-storage and initial-admission snapshots prevent profile edits or
  reassignment from moving payloads or changing an existing torrent's start
  intent. Older durable records are migrated transactionally when profile
  policy is replaced, while inheriting operational settings
  resolve live and are visible through native policy endpoints and the Web UI.
  See
  [ADR-0057](design/adr/0057-policy-profiles-and-inherited-settings.md).

- **Global peer-admission filtering:** bounded local IP/CIDR/range blocklists,
  manual IP bans, and peer-ID-prefix rules now apply consistently to peer
  discovery, metadata, engine, and inbound-session admission. Policy updates
  validate and replace transactionally while retaining the required contained
  socket path, and expose audit counters through the API and Web UI. See
  [ADR-0058](design/adr/0058-global-peer-admission-filtering.md).

- **Contained framed tracker/webseed HTTP and real HTTP(S) scrape:** tracker
  announce, supported BEP 48 scrape, and webseed range reads now share one
  bounded HTTP/1 codec over binder-provided TCP/TLS streams. Redirects repeat
  contained resolution/connect, enforce a five-hop/loop limit, allow HTTPS
  upgrade, reject downgrade, preserve exact authorities and Range, and never
  construct a connector, resolver, pool, or general client. Scrape is scheduled
  by download, magnet real-hash, reannounce/completion, and seeder activity.
  Native tracker rows expose separate attempt status/time/error and retained
  last-success counts; failed/task-aborted scrapes preserve those counts, and
  compatibility counts fall back when announce has not succeeded. UDP and
  non-derivable scrape are explicitly unsupported. See
  [ADR-0055](design/adr/0055-contained-http1-client-framing-and-redirect-policy.md).

- **Durable per-torrent seeding lifecycle:** each torrent now persists nullable
  ratio and idle overrides plus seed-forever policy, exposes stored/effective
  targets and exact seeding status through the native API, and provides a strict
  replacement control in Torrent Details. Seeder registration is authoritative
  for active counts and lifecycle state across queue slots, automatic/manual
  stops, restart, and fail-closed containment recovery. Verified piece ranges
  now produce exact single- and multi-file completed-byte accounting at file and
  final-piece boundaries. Downloader and seeder retain one live per-torrent
  limiter, so an upload-limit update shapes an accepted peer transfer without
  replacing the registration. See
  [ADR-0052](design/adr/0052-persisted-per-torrent-seeding-policy-and-runtime-lifecycle.md).

### Fixed

- **Web UI startup validation:** corrected an invalid shared-state
  redeclaration, three stale bare query-state references, and two omitted UI
  helper bindings that prevented the module graph, its first nonempty torrent
  refresh, or Doctor badge from completing. CI, local prechecks, and the Web UI
  Rust suite now parse every embedded JavaScript asset in ES-module mode and
  execute the complete production module graph through its first API-driven
  render.

- **Reachable terminal tracker diagnostics:** a bounded engine attempt now
  enters `tracker_error` when every attempted configured tracker fails and no
  usable DHT, PEX, direct-peer, or webseed source exists. Native summaries and
  Torrent Details retain the last error; reannounce/resume clears and retries.
  Reannounce no longer holds the engine-command lock while delegating to
  resume, preventing a stopped-engine deadlock.

- **Stable, idempotent, transactional watch ingestion:** complete sorted
  directory walks now run off the async workers, reject symlink roots, skip
  child symlinks, and require two unchanged length/modified-time observations.
  Bounded reads recheck path/open-file metadata and reset without a terminal
  result when copying continues. One in-memory processed fingerprint prevents
  repeated `leave` imports; restart duplicates are successful and preserve the
  existing torrent/queue/settings while applying the success action once. API,
  magnet, and watch adds share one locked durable transaction with exact
  registry/queue rollback and no pre-persistence events or scheduling.
  Permanent parser errors move to failure while transient operational errors
  stay for retry. Archive/failure actions use create-new semantics, never
  overwrite, and expose `post_action_error`. Recursive folders exclude their
  own strict-descendant archive/failure paths without affecting separately
  configured overlapping roots; whitespace-only and equal-root action paths
  are rejected. Stable outcomes/events and a 10,000-entry in-memory history are
  rendered in the Watch UI. See
  [ADR-0054](design/adr/0054-watch-folder-stability-idempotence-and-import-atomicity.md).

- **One process-wide peer-session budget:** `bandwidth.max_peers` now enforces
  one runtime-owned limit across metadata, serial, parallel, endgame, inbound
  seeding, TCP, and uTP sessions for every torrent, while
  `max_peers_per_torrent` is an additional per-torrent cap shared across
  inbound and outbound sessions.
  Trackers, webseeds, DHT, DNS, discovery, and retry waits remain outside the
  peer-specific budget. Live diagnostics expose exact limit, observed in-use,
  coherent availability, and inbound-denial counters, including unlimited
  observation. PATCH and full PUT replace pool objects through locked
  data-plane reconstruction; failed provisional work or persistence restores
  exact prior pool identities, task/lifecycle/queue ownership, config bytes,
  and durable state without enabling pre-commit selfish removal. See
  [ADR-0053](design/adr/0053-process-wide-peer-session-permit-pool.md).

- **Browser-origin protection for every control API:** the shared
  `browser_origin_guard` middleware is now applied as the outermost layer to
  every browser-reachable control route — `/api/v1`, `/transmission/rpc`, and
  `/api/v2` — so cross-site/same-site Fetch Metadata, foreign/malformed/`null`
  /multi-value/opaque origins are rejected with 403 before authentication,
  session negotiation, and compatibility-enabled checks in both authentication
  modes. Duplicate or invalid-byte Origin/Host/`Sec-Fetch-Site` fields, unknown
  Fetch Metadata values, and origins containing userinfo, a path, query, or
  fragment now fail closed. Rejections retain the native, Transmission, or
  qBittorrent error format, and the duplicate guard call was removed from native
  auth. See
  [ADR-0044](design/adr/0044-browser-origin-and-loopback-api-security.md),
  [ADR-0049](design/adr/0049-configured-unauthenticated-lan-control-plane.md).
- **Authenticated Chrome extension API access:** Manifest V3 extension service
  workers with a valid `chrome-extension://<extension-id>` Origin and realistic
  `Sec-Fetch-Site: none` can now call native bulk-add and every guarded control
  surface when API authentication is enabled and the request supplies the
  configured Bearer or `X-SwarmOtter-Auth` token. Auth-disabled mode and
  missing, invalid, or duplicated credentials fail with 403 before mutation;
  ordinary foreign HTTP(S), malformed/opaque/`null`, and invalid extension
  Origins remain rejected. Native failures use the actionable
  `extension_origin_forbidden` envelope. See
  [ADR-0044](design/adr/0044-browser-origin-and-loopback-api-security.md) and
  [ADR-0049](design/adr/0049-configured-unauthenticated-lan-control-plane.md).

- **Strict containment is the default (breaking):** `NetworkConfig::default()`
  now selects strict mode, matching the Serde default. An omitted `[network]`
  table produces strict mode without a path and fails `Config::validate()` with
  `invalid_config` before the control listener or any background task starts.
  `--check-config` fails the same way, and full config validation now runs
  before logging initialization and the success message. Existing users who
  relied on the disabled default must configure a strict path or set
  `mode = "disabled"` explicitly. See
  [ADR-0051](design/adr/0051-explicit-network-path-and-live-containment-gate.md).
  This breaking contract is part of the unreleased `v2.0.0` work.

- **Live containment gate:** one process-wide `ContainmentGate` (atomics plus
  `tokio::sync::Notify`) is now shared by every torrent data-plane component.
  Every bind, connect, resolve, accept-loop iteration, UDP send, tracker
  request, webseed request, and DHT send observes the gate. On healthy-to-
  unhealthy transition the gate blocks immediately, the inbound listener and
  DHT runner stop, data-plane tasks are aborted, active torrents enter
  `network_blocked`, state persists, and events publish — all while the control
  plane remains available. Every block advances a wakeup-safe cancellation
  generation, so an immediate block/recovery cycle still terminates streams
  from the old generation. Recovery consumes durable typed intent only for
  downloads, metadata work, and seeders demonstrably live at the block edge;
  paused, queued, stopped, and stale blocked records stay stopped. The health
  loop now uses an injected `InterfaceProbe` (tests inject a mutable fake) and
  exposes one `network_health_tick()` that tests drive without sleeping.
  Bind/listen/source-bind failures block synchronously, expose
  `socket_bind_failed`, and remain latched across healthy probe results until an
  explicit full configuration replacement validates contained UDP and listener
  binds; strict policy denials with no more specific status expose
  `blocked_fail_closed`. A CI harness now proves a real generated local
  tracker/peer transfer stops when its route-less namespace veth is deleted,
  while running fixtures without capabilities and granting the daemon only
  `CAP_NET_RAW` for `SO_BINDTODEVICE`. See
  [ADR-0051](design/adr/0051-explicit-network-path-and-live-containment-gate.md).

- **Bounded untrusted metainfo parsing:** the shared bencode decoder and
  metainfo builder now enforce fixed depth, node-count, file-count,
  piece-count, piece-length, and 16 MiB metadata byte budgets before any
  piece-sized allocation. These bencode budgets cover `.torrent` uploads, bulk
  base64 metainfo, magnet `info` dicts fetched via BEP 9, watch-folder files,
  and direct core parser callers. The decoder rejects empty/leading-zero/
  negative-zero integers, missing terminators, duplicate and non-string
  dictionary keys, overflowing string lengths, and trailing bytes, and
  requires EOF after exactly one top-level value. No malformed input may panic
  the daemon. Raw uploads stream only to the lower configured/metadata limit;
  bulk and Transmission base64 decoders stop before decoded output exceeds the
  metadata limit. BEP 9 uses the hardened prefix parser for a bounded dictionary
  followed by binary piece data and validates advertised/per-message/final
  assembly values. Restored daemon state is JSON: its piece-hash sequence is
  capped at `MAX_TORRENT_PIECES`, each SHA-1 hash is validated to encode exactly
  20 bytes with record/piece-index context before decoding/copying, and restored
  `TorrentMeta` values must pass shape validation before runtime use.
  Engine/storage boundaries narrow piece length with `u32::try_from` rather
  than `as`. See
  [ADR-0050](design/adr/0050-bounded-untrusted-metainfo-parsing.md).

## [1.3.1] - [2026-07-12]

### Fixed

- **Configured LAN Web UI access:** restored `api.require_auth = false` as a
  valid setting on non-loopback listeners, allowing the same-origin Web UI to
  use its API without a token prompt. Such listeners now emit a prominent
  exposure warning while retaining browser Origin/Host and Fetch Metadata
  checks. Authenticated remote access remains the recommended default. See
  [ADR-0049](design/adr/0049-configured-unauthenticated-lan-control-plane.md).
- **Upgrade configuration safety:** environment overrides are now applied
  before final file validation, and release images expose a config-only startup
  check. The Compose updater runs that check against the mounted configuration
  before replacing a healthy stack and prints service status and recent logs
  when post-update validation fails.

## [1.3.0] - [2026-07-11]

### Added

- **Durable daemon library state:** torrent records, queue order, file choices,
  labels, trackers, and per-torrent controls now survive restart in a versioned,
  crash-safe state file. Restore rejects malformed invariants and colliding
  storage ownership, and completed payloads are fully rechecked before restored
  torrents can seed. See [ADR-0045](design/adr/0045-versioned-durable-daemon-state.md).
- **Shared inbound peer listener:** one contained listener now routes plaintext
  and MSE/PE handshakes for every registered torrent, owns bounded accepted
  sessions, and cancels them with the listener. Multiple torrents can seed on
  the configured port without bind collisions. See
  [ADR-0046](design/adr/0046-shared-inbound-peer-listener.md).
- **Operational file selection:** wanted flags and file priorities now drive
  serial, parallel, endgame, and webseed piece scheduling. Move-data and
  rename-path operations update storage transactionally, and path ownership
  prevents two torrents from sharing a payload location. See
  [ADR-0048](design/adr/0048-file-selection-drives-piece-scheduling.md).
- **Complete Web UI operations:** torrent details now expose lifecycle,
  reannounce, queue movement, move-data, labels, per-torrent bandwidth,
  file rename/priority, and tracker editing controls alongside an activity
  view. Add flows can start paused, controls have explicit accessible labels,
  and settings can apply runtime-supported sections when persistent config
  replacement is unavailable.
- **Release runtime prerequisites:** official container and native package
  artifacts now provide the Linux `ip` utility required by strict route and
  DNS validation, and packaged systemd/Compose deployments raise the file
  descriptor limit for concurrent peer, tracker, and storage handles.

### Changed

- **Live data-plane configuration:** containment, listen-port, IP-family, uTP,
  peer-encryption, and DHT replacements now stop and await the complete old
  task set before rebuilding with fresh binders. Engine construction shares
  the transition lock, and uTP streams own and cancel their connection drivers.
  Configuration writes are serialized, atomically replaced, and reject unknown
  fields. See
  [ADR-0047](design/adr/0047-transactional-live-data-plane-reconfiguration.md).
- **Tracker scheduling and validation:** announce behavior now preserves
  BEP 12 fallback tiers, uses one successful tracker at a time, honors bounded
  tracker-provided intervals independently from DHT refresh, and validates UDP
  tracker response source, action, and transaction identifiers.
- **Private writable configuration:** packaged and Compose deployments use a
  service-owned, mode-`0700` configuration directory with a mode-`0600`
  config file so validated settings replacement remains atomic without
  exposing API credentials.
- **Minimum Rust version:** source builds now require Rust 1.88, with a locked
  workspace check at that compiler floor in CI.
- **Browser control-plane security:** unauthenticated API listeners are
  restricted to loopback, browser mutation and WebSocket requests enforce
  same-origin Host checks, and Web assets ship with content security,
  anti-framing, MIME-sniffing, and referrer-policy headers. See
  [ADR-0044](design/adr/0044-browser-origin-and-loopback-api-security.md).

### Fixed

- **Peer protocol interoperability:** uTP now uses BEP 29 header semantics,
  connection-ID transitions, extension chains, and selective acknowledgments;
  inbound peer messages and storage block requests are bounded before
  allocation or disk access.
- **Storage and resume correctness:** metainfo rejects negative and overflowing
  integers and duplicate file paths, fast resume detects same-size payload
  changes and quarantines corrupt records, completed rechecks use the completed
  storage root, move/rename rolls back when durable-state persistence fails,
  and delete failures are returned instead of silently discarding registry
  state.
- **Torrent removal choices:** Web UI removal now distinguishes cancel,
  remove while keeping payload data, and remove while deleting payload data.
- **Filtered-list notifications:** the Web UI infers external removals only
  while observing the complete, unfiltered library, preventing filtered,
  paginated, or state-transitioned torrents from being reported as removed.

## [1.2.2] - [2026-07-10]

### Fixed

- **Live public-swarm Linux torrent throughput:** normal torrent announces now
  collect every concurrent tracker response instead of returning after the
  first peer-bearing tracker, partial `[dht]` TOML config keeps the default
  bootstrap nodes, empty-table DHT lookups actively query bootstrap families,
  and invalid port-zero peer candidates are filtered before scheduling. Strict
  contained networking no longer reports `traffic_allowed = false` merely
  because another peer worker is reading binder config; the fail-closed socket
  guard remains the authoritative gate before any torrent traffic is opened.
  Peer sessions also keep reading when an unchoke arrives before bitfield/have
  availability, allowing standards-compliant peers to advertise pieces before
  requests are selected. On the local strict-`br0` Ubuntu legal torrent test,
  the daemon completed the 6.52 GB ISO and recorded a 222.52 MiB/s peak sample
  with a 134.68 MiB/s smoothed download rate.
- **Autopilot queue release with no replacement:** `ReleaseQueueSlot` now
  skips demoting a stalled active torrent when no other queued download is
  currently eligible to consume the released slot, keeping single-torrent
  legal download tests active for continued discovery and retry.
- **Peer failure diagnostics:** serial and parallel peer sessions now emit
  structured no-progress reasons for closed connections, state waits, missing
  useful work, hash failures, and timeout paths, making live swarm stalls
  actionable from daemon logs.

## [1.2.1] - [2026-07-09]

### Fixed

- **Watch-folder queue startup:** watch-folder imports with
  `start_behavior = "start"` now follow the same lifecycle as API file adds:
  storage preflight, network-state application, queue insertion, scheduled
  reconciliation, and add/stats events. Imported torrents no longer appear as
  `queued` with no queue position.
- **Piece-hash mismatch from duplicate blocks:** the per-piece download loops
  in `crates/swarmotterd/src/engine.rs` previously treated every successful
  return from `PieceAssembler::add_block` (including `Ok(false)` for
  duplicate blocks) as a newly received block, advancing the per-piece
  `received_blocks` counter and calling `data()` on an incomplete buffer when
  the count matched the request count. The SHA-1 of mostly-zero data did not
  match the expected piece hash, producing a flood of `piece hash mismatch;
  rejecting` warnings and no usable downloads for affected pieces. The two
  download loops now treat `Ok(true)` from `add_block` as the only signal
  that a *new* block was accepted, and only then advance the counter. A unit
  test pins the assembler contract.
- **Seeder visibility on trackers:** a daemon that only has completed
  torrents was never announced to trackers because `Seeder::run` only binds
  the inbound peer listener; the announce loop lived on the engine path and
  stops when the engine hands off to the seeder. As a result, a fresh
  daemon acting purely as a seeder was invisible in the swarm, and
  leechers saw an empty peer list. The daemon now spawns a sidecar
  `start_seeder_announce` task on completion that announces
  `event=started` once, `event=empty` every 5 minutes, and `event=stopped`
  on shutdown, through the same network binder the engine uses.
- **Overly conservative per-peer in-flight ceilings that prevented the
  engine from using available hardware bandwidth.** The engine's per-piece
  download path held only `NORMAL_PEER_PIECE_WINDOW = 4` pieces in flight
  per peer (64 KiB at the 16 KiB block size), so per-peer throughput was
  bounded by RTT × 64 KiB — well below the bandwidth of a modern peer on
  a gigabit+ link. The defaults in `crates/swarmotterd/src/engine.rs` are
  raised to better match the operator hardware baseline the project
  targets (`design/scaling-implementation-plan.md`):
  `DEFAULT_PEER_WORKER_LIMIT` 64 → 128, `NORMAL_PEER_PIECE_WINDOW` 4 → 32,
  `NORMAL_REQUEST_FLOOR` 32 → 64, `NORMAL_REQUEST_FALLBACK_CAP` 500 →
  2,000, `NORMAL_REQUEST_LOCAL_CAP` 2,000 → 4,000. These are internal
  engine constants, not operator-configurable settings, and the change is
  backwards compatible — operators who set `bandwidth.max_peers_per_torrent`
  continue to be respected; the new defaults only apply when the operator
  has not pinned a cap. Measured end-to-end on a single 6.52 GB Linux
  distribution ISO: peak 226 MB/s (up from 31 MB/s), sustained 144-189
  MB/s (up from 18 MB/s), exceeding Transmission's 80.98 MB/s reference
  on the same torrent on the same hardware.
- **Per-peer piece reservation monopolisation by a single fast peer.** When
  several peer sessions shared one `ParallelPieceState` and the per-peer
  piece window was wider than the total number of pieces remaining, the
  first session to grab the lock reserved every available piece before the
  other sessions could start, starving the rest of the swarm (they sent no
  useful blocks and were marked unhelpful by the engine). Each session now
  uses a per-peer socket-address FNV-1a shard as the starting point in the
  piece space, and the per-session work cap is
  `min(NORMAL_PEER_PIECE_WINDOW, ceil(remaining / candidates))` so the work
  is shared across concurrent workers in the same parallel round. The
  existing `local_swarm_parallel_download_uses_multiple_seed_peers`
  integration test asserts this property.

### Added

- **Throughput tuning demonstration test:**
  `crates/swarmotterd/tests/local_throughput_tuning.rs::throughput_tuning_baseline_vs_tuned`
  runs 10 generated lawful torrents through the real `TorrentEngine` under
  two configurations (serial / 1 peer worker per torrent vs 10 concurrent
  / 4 peer workers per torrent) and prints the wall-clock speedup. The
  tuned run reaches 50+ MiB/s aggregate over loopback, ~132-181× the
  baseline, on the same engine code that backs the LAN instance.
- **Test torrent generator:** `cargo run --example gen_test_torrents
  --release -p swarmotter-core` writes N small synthetic .torrent files
  and matching payloads, with an optional HTTP tracker URL, for local
  swarm testing without contacting public trackers.

## [1.2.0] - [2026-07-09]

### Added

- **Metadata fetch budget:** `[queue].max_active_metadata_fetches` now limits
  simultaneous magnet metadata fetches independently from resolved download
  slots, preventing large magnet imports from consuming all active download
  capacity or starting unbounded metadata discovery work.
- **Scheduler diagnostics:** `GET /api/v1/stats` now reports scheduler
  requested/granted/running counts, retry-backoff pressure, configured queue
  caps, and peer-worker saturation so large-library operators can see which
  resource pool is limiting progress.
- **Large-library API coverage:** API integration tests now cover 1,000-torrent
  rapid add, bulk add, and query/filter/group behavior using generated lawful
  magnets.
- **Scale validation harnesses:** ignored opt-in tests now cover a 1,200-record
  mixed-state daemon scheduler library across all torrent states and a
  2,000-torrent API add/query/recheck/reannounce/remove/reset flow using
  generated lawful torrent files.
- **Runtime event publishing:** the daemon now publishes torrent add/change,
  metadata, completion, removal, error, settings, network status, and stats
  events through the existing SSE/WebSocket broker. Event streams include
  keep-alives or pings and surface subscriber lag instead of silently dropping
  missed updates.
- **PR precheck helper:** `scripts/do-pr-prechecks.py` now runs the pull-request
  quality gate with Rich progress feedback, including stable Rust component
  setup, formatting, workspace check, Clippy, and tests.

### Fixed

- **Bandwidth limiter contention:** global and per-torrent rate limiters now use
  lock-free atomic token buckets instead of `tokio::sync::Mutex`, eliminating
  serialization across all active torrents during block transfers. Concurrent
  acquire operations use CAS loops for refill-and-consume without mutex
  contention, and refill ownership is coordinated atomically so concurrent
  consumers cannot double-count the same refill window. As a result, 1,000
  concurrent torrents sharing a global limiter no longer serialize on the same
  two locks.
- **Daemon state lock contention:** read-heavy daemon state maps (`config`,
  `network_health`, `engine_states`, `engine_handles`, `torrent_limiters`,
  `rate_samples`, `engine_retry_after`, `autopilot_decisions`,
  `autopilot_last_action`) now use `tokio::sync::RwLock` instead of
  `tokio::sync::Mutex`, allowing concurrent readers during progress
  reconciliation, API reads, and autopilot analysis without blocking on
  write-only operations.
- **Piece assembler completion check:** `PieceAssembler::add_block` now tracks
  a received-block counter for O(1) completion detection instead of scanning
  the entire received-block vector on every block arrival.
- **Queue position lookups:** `QueueState` now maintains a `HashMap<InfoHash,
  usize>` position index for O(1) `position()`, `move_up()`, and `move_down()`
  operations instead of linear scans of the order vector. Move-to-top,
  move-to-bottom, remove, serde restore, and runtime clear operations rebuild
  the index after mutation so persisted queues and reset flows keep accurate
  positions.
- **Torrent lifecycle lock scope:** engine and seeder shutdown paths now remove
  join handles from shared maps before awaiting task completion, preventing
  slow task teardown from blocking unrelated lifecycle readers and writers.
- **Event broadcast buffer:** the SSE/WebSocket event broker default capacity
  increased from 256 to 4,096 messages, preventing subscriber lag notifications
  during reconciliation bursts with large torrent libraries.
- **Storage I/O scaling:** payload block reads and writes now reuse cached
  per-file handles instead of reopening files for every block. Block writes no
  longer flush on every write; storage flushes pending cached writes before
  verification/read and move/remove boundaries. See
  [ADR-0043](design/adr/0043-cached-storage-io-flush-boundaries.md).
- **Read API scaling:** torrent list, single-torrent, stats, diagnostics, and
  storage-root reads no longer trigger hidden full-engine progress
  reconciliation. Reconciliation now snapshots live engine state first, hoists
  config/network reads out of the per-torrent loop, and keeps registry mutation
  in a shorter critical section.
- **Progress summary scaling:** in-memory piece progress now uses a packed
  bitfield with a cached completed-piece count, avoiding full piece-vector
  scans on every torrent summary.
- **Stats aggregation scaling:** global stats now computes counts and transfer
  totals in one registry pass instead of repeatedly scanning the torrent map.
- **Torrent lifecycle caps:** queue reconciliation now force-clears over-limit
  active downloads instead of waiting indefinitely for a graceful engine stop,
  and metadata progress reconciliation no longer reactivates queued retry work
  from stale diagnostics. See
  [ADR-0040](design/adr/0040-force-clear-over-limit-queue-rotation.md).
- **Metadata queue regression coverage:** daemon tests now verify no-peer
  magnet retries remain queued after progress reconciliation and that stale
  metadata diagnostics cannot reactivate a 100-torrent queue beyond configured
  active limits.
- **Large-queue scaling:** queue membership now uses set-backed runtime indexes
  and batch operations for large add/remove/recovery paths while preserving
  stable serialized queue order. Daemon regression tests now cover 10,000
  managed torrent records for stale metadata recovery, metadata retry backoff,
  and bulk removal. See
  [ADR-0041](design/adr/0041-set-backed-queue-and-metadata-fetch-budget.md).

## [1.1.6] - [2026-07-09]

### Fixed

- **Runtime queue setting changes:** runtime settings updates now schedule queue
  reconciliation instead of awaiting engine startup inline, so raising
  `queue.max_active_downloads` can return promptly and continue filling active
  slots after the client request completes.
- **Queue lifecycle recovery:** queue reconciliation now clears stale engine
  bookkeeping for torrents that are no longer active, preventing queued
  torrents from retaining handles that make engine startup skip them.
- **Autopilot queue-slot release:** act-mode recovery now force-stops stalled
  engine tasks when releasing an active queue slot, so a nonresponsive download
  cannot block the rest of the queue from being promoted.
- **Autopilot diagnostics freshness:** per-torrent autopilot decisions are
  recomputed from current torrent and engine state instead of returning stale
  cached snapshots.
- **Large queue regression coverage:** daemon tests now cover queue lifecycle
  recovery for 100- and 1,000-torrent queues with the default high-concurrency
  active slot target.

## [1.1.5] - [2026-07-08]

### Fixed

- **Unattended queue recovery:** unfinished engine exits now requeue the
  torrent with a retry backoff and release the active download slot so queued
  torrents can proceed. Retryable magnet metadata discovery failures also
  return to queued state instead of remaining listed as active with no running
  engine. Queue reconciliation also recovers stale active records that have no
  running engine task and moves them behind waiting work.
- **Autopilot decision responsiveness:** per-torrent autopilot diagnostics now
  compute the requested torrent directly instead of refreshing every torrent,
  preventing the endpoint from hanging behind unrelated engine state.
- **Reset lifecycle verification:** the Web UI now refreshes torrent data after
  every reset attempt and reports an incomplete reset if the API still lists
  torrent records. The daemon also logs reset requests before cleanup and clears
  retry bookkeeping as part of reset state cleanup.

## [1.1.4] - [2026-07-08]

### Changed

- **Autopilot act mode default:** `[autopilot].mode` now defaults to `act` so
  stalled active torrents can receive bounded queue/discovery/peer-worker
  mitigation without requiring an explicit settings change. Operators can still
  select `observe` for diagnostics-only behavior or `disabled` to turn
  autopilot off.

### Fixed

- **Autopilot stalled queue-slot release:** autopilot now preserves the start of
  a zero-download streak and prioritizes queue-slot release when an active
  torrent has no recent block progress. This prevents stalled active torrents
  from pinning download slots ahead of queued torrents during unattended bulk
  downloads.
- **Selfish completion reconciliation:** `torrent.selfish = true` now removes
  already-completed managed torrent records during runtime reconciliation while
  preserving downloaded data. This prevents completed torrents from remaining
  visible in the API/Web UI when they completed before the setting was enabled
  or before the daemon observed the completion callback.

## [1.1.3] - [2026-07-06]

### Fixed

- **Release image glibc compatibility:** Linux release artifacts are now built
  inside a Debian bookworm Rust container and checked against the Debian
  bookworm glibc floor before publishing. The release container image continues
  to stage those same prebuilt binaries, but the staged `swarmotterd` binary no
  longer depends on newer GitHub runner glibc versions that are unavailable in
  the runtime image. The release workflow now invokes the container shell
  without login-shell PATH reset so `cargo`, `rustc`, and `rustup` from the
  Rust image are available during artifact builds.

## [1.1.1] - [2026-07-06]

### Fixed

- **Gluetun Compose control-plane reachability:** the Gluetun environment
  example now sets `FIREWALL_INPUT_PORTS=9091`, so the documented
  host-published API/Web UI health endpoint remains reachable while torrent
  data-plane traffic stays in the Gluetun VPN namespace. The update helper now
  checks container-internal health after a host health failure and reports the
  Gluetun firewall fix when the daemon is healthy inside the shared namespace.

## [1.1.0] - [2026-07-06]

### Added

- **qBittorrent-compatible `/api/v2` shim:** added an optional
  `[compatibility.qbittorrent]` toggle for opt-in qBittorrent-style API
  compatibility. The adapter uses the native API as source of truth, supports
  Bearer token plus `/api/v2/auth/login` SID-cookie auth flow, and documents a
  focused automation subset including version and torrent lifecycle endpoints
  (`/api/v2/app/version`, `/api/v2/app/webapiVersion`,
  `/api/v2/torrents/info`, `/api/v2/torrents/add`, `/api/v2/torrents/delete`,
  `/api/v2/torrents/pause`, `/api/v2/torrents/resume`,
  `/api/v2/torrents/start`, `/api/v2/torrents/stop`,
  `/api/v2/torrents/setCategory`) with no indexer/search or torrent-discovery
  surface. ADR-0038 records this compatibility decision.
- **Web UI sortable and filterable torrent table:** the torrent list now uses
  a vendored Tabulator grid with clickable column sorting, reversible sort
  direction, status/health header filters, numeric comparison filters for
  columns such as progress, rates, ratio, size, and peers, and a Clear Filters
  control while preserving existing row actions and bulk selection behavior.
  ADR-0033 records the Tabulator dependency decision.
- **Web UI light and dark theme toggle:** the header now includes a theme icon
  that toggles between the default dark theme and a light theme, persists the
  browser preference locally, and applies theme-aware Tabulator table colors.
  ADR-0034 records the browser preference decision.
- **Adaptive swarm performance autopilot:** added `[autopilot].mode`
  (`disabled`/`observe`/`act`, default `observe`), per-torrent mode overrides,
  API endpoints for global status and per-torrent "why is this slow?"
  decisions, Settings tab global mode editing, Web UI diagnostics/per-torrent
  override controls, and daemon-side act-mode actions for bounded discovery
  refresh, peer-worker tuning, peer-backoff relaxation, and queue-slot release
  using existing contained telemetry.
  ADR-0035 records the control and containment decision.
- **Disk-aware storage diagnostics and preflight:** added
  `GET /api/v1/storage/roots` for per-root storage diagnostics, configurable
  free-space reserve enforcement under `[storage]`
  (`minimum_free_space_bytes`, `minimum_free_space_percent`), add/start-time
  storage preflight before payload writes, Settings fields for the reserve
  values, and a Doctor storage diagnostics card. ADR-0037 records this phased
  contract.
- **Web UI Settings two-panel layout:** Settings now uses a left section
  navigation menu with one editable detail panel visible at a time, while
  preserving the existing full-configuration save path and keeping the
  Save/Reload/Reset controls in a single header action row.
- **TCP Protocol Encryption / MSE-PE support:** added
  `torrent.encryption_mode` with `disabled`, `preferred` (default), and
  `required` modes. The implementation applies to TCP peer-wire negotiation
  through the same contained peer sockets used by existing transport paths and
  does not create separate encryption sockets or a containment bypass. uTP
  encryption and per-profile/per-torrent overrides remain planned.
  ADR-0039 records this phase decision.
- **Large-library Web UI operations console:** added
  `GET /api/v1/torrents/query` for server-side torrent-list search, filters,
  sorting, pagination, bucket counts, counts-only queries, and optional
  grouping while preserving the legacy full-array `GET /api/v1/torrents`
  response. The Web UI now uses the query endpoint for the torrent list, adds
  state/health/performance filters, page-size and previous/next controls,
  query result metadata, and a browser-local saved view for the large-library
  filter and sort state. ADR-0036 records the query API decision.

### Fixed

- **Retryable magnet metadata discovery:** magnets that discover no peers while
  fetching BEP 9 metadata now remain in `downloading_metadata` with a retry
  backoff instead of being moved to terminal `error`. Completed and failed
  engine tasks also clear their runtime handles so explicit resume/start
  actions and scheduled retries can start a fresh engine task.

## [1.0.0] - [2026-07-04]

This is the active `v1.0.0` initial-release branch. It records completed
capabilities that are part of the first public release scope defined in
`design/requirements.md` and `design/PRD.md`: live TCP and uTP (BEP 29) peer
wire protocol, HTTP/HTTPS/UDP trackers, DHT (BEP 5), PEX (BEP 10/11), BEP 9
magnet metadata fetch, inbound seeding/upload, endgame mode, live bandwidth
shaping, real disk I/O with fast resume, watch folders, browser magnet
submission, queue/ratio/seeding controls, fail-closed VPN/NIC network
containment, the complete REST API with WebSocket/SSE events, and a practical
Web UI. See `design/v1-completion-tracker.md` for the capability-by-capability
status.

### Added

- **Bulk torrent API operations:** native clients can add many torrents with
  `POST /api/v1/torrents/bulk` using magnet links and/or base64 `.torrent`
  payloads, with per-item success and failure results. Clients can also remove
  many torrents with `POST /api/v1/torrents/remove`, which reports removed and
  missing hashes while the daemon reconciles queue state once for the batch.
- **Rapid API torrent add handling:** API torrent adds continue to return after
  registration and queue insertion instead of waiting for engine startup. Queue
  reconciliation now coalesces rapid add bursts before starting work, with
  coverage for 200 fast API add requests and 200 daemon queue inserts.
- **Paused torrent add API:** native add-torrent requests now accept
  `paused: true` or `start_behavior: "paused"` for JSON magnet adds and
  `?paused=true` or `?start_behavior=paused` for raw `.torrent` uploads. The
  daemon inserts paused adds into queue order without scheduling immediate
  startup, and the Transmission compatibility adapter now uses the same
  add-time paused path.
- **Web UI bulk torrent selection:** torrent list rows can now be selected with
  checkboxes, the toolbar can select all visible rows or clear the selection,
  and selected torrents are removed through the bulk remove API in one
  confirmed operation while keeping downloaded data.
- **Web UI application version display:** the Doctor view now shows SwarmOtter
  version, commit, and target details from the native version API.
- **Linux release artifacts:** stable release tags now build Linux `x86_64`
  and `aarch64` tarballs, `.deb`/`.rpm` packages, checksums, and semver-tagged
  GHCR images for `linux/amd64` and `linux/arm64`. ADR-0032 records the release
  artifact strategy.
- **Docker server update helper:** `deploy/update-swarmotter.sh` resolves the
  latest GitHub Release by default, skips when the running container already
  has that version, supports `--force` for repair/reapply updates, and
  otherwise backs up Compose environment files, SwarmOtter configuration and
  state, and Gluetun state before pulling the target image, recreating the
  Compose stack so Docker attaches networks before Gluetun installs VPN routes,
  validating the running container, and keeping a local rollback image tag.
- **FOSS torrent client comparison:** added `design/COMPARISON.md` as a living
  comparison matrix for SwarmOtter versus popular FOSS torrent clients,
  including feature parity, differentiators, source links, and roadmap gaps
  mapped to `design/BACKLOG.md`.
- **Optional Transmission RPC compatibility adapter:** added `POST /transmission/rpc` as an optional compatibility layer over existing `DaemonOps` when enabled. The adapter implements `X-Transmission-Session-Id` enforcement, maps Transmission Basic auth password to `api.auth_token` when API auth is required (username is ignored), supports common session/torrent/queue/helper calls including mutating remove/set/move operations, maps Transmission delete-data removal flags to native delete-data behavior, supports magnet and base64 metainfo `torrent-add`, and rejects remote HTTP/HTTPS torrent URL intake.
- **Contained webseed downloads:** torrent metadata now preserves BEP 19
  `url-list` webseeds, and the engine can fetch missing pieces from HTTP/HTTPS
  webseeds using contained byte-range GETs through `NetworkBinder`. Webseed
  payloads are SHA-1 verified before disk writes, count toward live download
  throughput, respect configured download rate limits, are recognized by
  per-torrent health as a valid active source, and are covered by a loopback
  range-server integration test.
- **Per-torrent health calculation and display**: every torrent summary and
  detail response now includes a `health` object with a 0..100 score, a
  0..5 signal-bar mapping, a human-readable label
  (`unknown`/`network_blocked`/`stalled`/`critical`/`poor`/`fair`/`good`/
  `excellent`/`paused`/`complete`), per-component sub-scores
  (availability, throughput, peers, stability, discovery), and
  human-readable reasons. The score is computed from real engine state
  — piece availability, peer usefulness, throughput, recent stability,
  and discovery — and is **not** a proxy for seed count or completion
  percentage. The Web UI renders a signal-bars indicator on the torrent
  list row and on the details header using CSS-only bars (no image
  asset), with a tooltip and a per-component sub-score table on the
  details view. See `docs/api.md` and `design/v1-completion-tracker.md`.
- **Web UI layout and peer counts:** the main Web UI content now uses the
  available window width instead of a centered 1200px cap, and the torrent
  list Peers column shows active peer workers / known peers from the summary
  API instead of a placeholder. Torrent row actions now use compact icon
  buttons with accessible labels, and dynamic data regions start empty until
  API data is loaded. Transient Web UI operation feedback now uses toast
  notifications with a configurable browser-local display time defaulting to
  5 seconds, including add/upload results and torrent removal notices.
- **Visible incomplete storage layout:** active torrents now create their
  incomplete storage layout as soon as the engine starts even when
  `preallocate = false`; single-file torrents create a zero-length placeholder
  file and multi-file torrents create the top directory before the first piece
  is written.
- **Configurable torrent peer worker cap:** the download engine now uses
  `bandwidth.max_peers_per_torrent` instead of a fixed 16-worker limit. When
  unset (`0`), the daemon uses its default 64-worker pool so popular public
  torrents can use more discovered peers without extra configuration.
- **Torrent throughput and scheduler diagnostics:** the daemon now emits
  structured log records whenever a torrent reaches a new observed download or
  upload throughput peak, including sample rates, smoothed rates, previous
  peaks, active/known/useful peer counts, scheduler eligibility counts, byte
  counters, and tracker/DHT/PEX/webseed discovery freshness for performance
  troubleshooting. Per-torrent stats also expose live peer scheduler state so
  high discovered-peer counts can be distinguished from filtered, failed,
  backed-off, or serial-fallback peer pools.
- **Runtime diagnostics and config replacement:** the API and Web UI now expose
  richer operational diagnostics for network containment, watch folders, recent
  logs, and a consolidated Doctor report that drives the header health badge.
  `PUT /api/v1/settings` validates and atomically replaces the full
  configuration, preserves existing auth tokens when omitted, redacts auth
  tokens in responses, applies live-safe fields immediately, and reports fields
  that require restart. ADR-0025 records the decision.
- **Confirmed download reset:** the API now exposes `POST /api/v1/reset` and
  the Web UI Settings view provides a confirmed Reset action that stops all
  torrent activity, removes torrent records, empties configured download and
  incomplete directory contents while preserving the roots, and clears daemon
  logs. ADR-0027 records the destructive reset workflow.
- **Configuration enforcement pass:** previously modeled runtime settings are
  now wired into daemon behavior: `bandwidth.max_peers` participates in live
  peer worker caps, `queue.max_active_downloads`/`auto_start`/queue move
  operations drive the real scheduler, `queue.max_active_seeds` and global
  seeding ratio/idle policy control completed seeders, `torrent.allow_ipv6`
  filters IPv6 peer candidates, `pex.enabled`/`pex.max_peers` control peer
  exchange, `dht.port` is bound by the shared DHT runner, and `storage.sparse`
  controls whether active files are sized up front when preallocation is off.
  Watch-folder `failure_dir` now receives failed `.torrent` imports.
- **Selfish completion policy** (`torrent.selfish`, default `false`): an
  optional completion policy that removes a torrent from SwarmOtter
  immediately after its download completes. When enabled, on completion the
  engine and seeder are stopped and the torrent record is removed from the
  registry; the downloaded data is preserved on disk (no delete-data behavior
  is invoked) and SwarmOtter does not seed the torrent after completion. When
  disabled, normal completion and seeding behavior is unchanged. Configurable
  via `[torrent] selfish` in the config file or `SWARMOTTER_TORRENT__SELFISH`.
  The torrent disappears from the API/UI torrent list after completion.
- **API request body limit** (`api.max_request_body_bytes`, default
  `16777216`): caps JSON requests and raw `.torrent` uploads.
- **Interface-bound containment for dynamic addresses:** strict mode can bind
  torrent data-plane sockets to all current addresses on a configured interface
  such as `br0`, avoiding fixed source IP configuration for DHCP/SLAAC
  interfaces.
- **mdBook user guide:** `book.toml` now uses `docs/` as the mdBook source
  root, with operator-facing documentation for getting started,
  configuration, network containment, deployment, Web UI usage,
  troubleshooting, lawful use, and legal/content policy.
- **Web UI assets and upload ergonomics:** the embedded Web UI now serves the
  favicon/app-manifest assets, displays the SwarmOtter icon in the header, and
  accepts `.torrent` drag-and-drop uploads anywhere in the app window.
- **Default daemon file logging:** SwarmOtter now records daemon logs to a
  per-user log file by default while continuing to emit logs to stderr/journal.
- Repository scaffolding: governance documentation, ADR process, legal design
  docs, GitHub templates, and a minimal Rust workspace skeleton.
- ADRs 0001 through 0008 recording foundational project decisions.
- Lawful-use, content-policy, and legal posture documentation.
- VPN/NIC network containment design describing fail-closed behavior.
- Documentation stubs for requirements, architecture, API, configuration,
  deployment, and testing.
- **Core engine (`swarmotter-core`):** typed error model with stable
  machine-readable codes; info hash handling (hex/base32); magnet URI parser
  with full test coverage; `.torrent` metadata parser (single/multi-file,
  private flag, tracker tiers) via an in-tree bencode decoder; domain models
  for torrent state, network containment status (all 11 required states),
  peers, trackers, and stats; network containment configuration, validation,
  and fail-closed enforcement with a pluggable `InterfaceProbe`; queue,
  bandwidth (token-bucket), and ratio/seeding policy logic; storage layout,
  piece verification, and fast-resume metadata (JSON format); torrent
  registry with duplicate detection; watch-folder scan/import logic; TOML
  configuration with environment variable overrides and validation.
- **API layer (`swarmotter-api`):** versioned REST API (`/api/v1`) with the
  consistent `{ success, data, error }` envelope and machine-readable error
  codes; complete route coverage for torrents, files, trackers, peers, queue,
  settings, network health, watch folders, stats, health, and version; SSE
  and WebSocket event delivery with per-torrent filtering and an in-process
  broadcast broker; `DaemonOps` trait so the daemon owns all state.
- **Web UI (`swarmotter-web`):** practical function-over-form UI (embedded
  HTML/CSS/vanilla JS) consuming the same API as external automation, covering
  torrent list, add magnet/upload, details (files/peers/trackers), queue
  actions, settings, network health, watch folders, and logs.
- **Daemon (`swarmotterd`):** runtime implementing `DaemonOps` with network
  containment enforcement (torrents enter `network_blocked` when strict mode
  path is unavailable), watch-folder scanner loop, network health monitor
  loop, graceful shutdown, and a single `axum::serve` for API + Web UI.
- ADR-0009 (foundational dependency stack), ADR-0010 (API versioning,
  envelope, events), ADR-0011 (bencode implementation and fast-resume format),
  ADR-0012 (network binder — centralized containment for live sockets),
  ADR-0013 (peer wire protocol architecture), ADR-0014 (tracker implementation
  strategy), ADR-0015 (real storage I/O and fast-resume format), ADR-0016
  (task/runtime model for the live engine), ADR-0017 (local swarm testing
  approach).
- Deployment artifacts: example config, systemd service unit, Dockerfile,
  nginx reverse-proxy example.
- **Live torrent data-plane engine:** `NetworkBinder` abstraction
  (`ContainedBinder` with source-bound TCP + fail-closed; `LoopbackBinder` for
  tests) as the single choke point for peer/tracker/webseed sockets; real TCP
  BitTorrent peer wire protocol (handshake, bitfield, choke/unchoke, request,
  piece, block assembly, SHA-1 verification, bad-peer suppression, bounded
  concurrency) in `swarmotter-core::peer`; HTTP tracker announce with compact
  IPv4/IPv6 peer parsing, tiers, and private-torrent handling in
  `swarmotter-core::tracker`; real async disk I/O with multi-file boundary
  writes, piece verification, fast-resume save/load (with mismatch detection),
  and forced recheck in `swarmotter-core::storage::io`; per-torrent
  `TorrentEngine` task in `swarmotterd::engine` wired into the daemon so
  add/pause/resume/remove/recheck/reannounce drive real peer/tracker activity,
  network health changes stop engines and mark torrents `network_blocked`, and
  API/UI summaries report real progress/peers/trackers; local swarm integration
  tests completing a real download from a generated payload through an
  in-process HTTP tracker and seed peer (tracker and direct-peer paths), plus
  a daemon-driven download test through `DaemonOps`.
- **Network binder extension:** `NetworkBinder` now exposes `udp_socket()`
  (contained, source-bound UDP datagram socket via `ContainedUdpSocket`) and
  `bind_peer_listener()` (contained, source-bound inbound TCP listener via
  `PeerListener`), both fail-closed. `LoopbackBinder` implements both for
  tests; a new `BlockedBinder` proves fail-closed behavior for TCP, UDP, and
  the inbound listener. Used by UDP trackers, DHT, uTP, and seeding.
- **UDP trackers (BEP 15):** live UDP tracker connect + announce in
  `swarmotter-core::udp_tracker`, routed through the binder's contained UDP
  socket, with compact IPv4 peer parsing, transaction-id matching, error
  response handling, and a bounded retry loop. The engine dispatches
  announce by scheme (`udp://` vs `http://`). Includes a local UDP tracker
  fixture test and a fail-closed test, plus an engine-level local swarm test
  downloading via a BEP 15 UDP tracker.
- **Endgame mode:** near completion (remaining pieces at or below
  `ENDGAME_THRESHOLD`), the engine switches to a concurrent endgame path
  (`swarmotter-core::endgame` planner + `engine::run_endgame`) that requests
  the remaining blocks from multiple peers at once, bounds duplicate
  outstanding requests per block, and cancels still-outstanding duplicates as
  pieces complete, keeping request queues bounded. A local swarm test resumes
  a near-complete torrent and completes it through endgame.
- **Live bandwidth shaping:** a `RateLimiter` (token-bucket, async) is wired
  from the daemon's effective global download/upload limits into the engine
  download path (per-piece acquire) and the seeder upload path (per-block
  acquire), including the endgame path. A shared global limiter is cloned into
  every engine and seeder so the global cap is a true aggregate across active
  torrents, and each torrent also has a per-torrent limiter enforced live
  alongside it. Throttling local swarm tests prove a tight download limit
  materially slows a real download, including a per-torrent cap with an
  unlimited global limiter. Per-torrent limits are settable live via
  `POST /api/v1/torrents/:hash/limits` and reflected in the torrent summary.
- **PEX peer exchange (BEP 10/11):** the extension protocol (`swarmotter-core
  ::extensions`) adds extension-handshake encode/decode, a `Handshake.reserved`
  field with the BEP 10 extension bit, an `Extended` peer-wire message variant
  (id 20), and `ut_pex` message encode/decode. The engine sends an extension
  handshake, learns the remote `ut_pex` id, parses incoming PEX messages, and
  adds discovered peers to the candidate pool; private torrents block PEX. All
  PEX-discovered outbound connections go through the binder. A local swarm
  test proves a leecher discovers a seed peer via PEX and completes.
- **BEP 9 magnet metadata fetch:** `swarmotterd::metadata` implements the
  `ut_metadata` extension: extension handshake with `metadata_size`, metadata
  piece request/assembly, SHA-1 info-hash validation of the assembled `info`
  dict, and conversion into a real `TorrentMeta` so the download proceeds as
  for a `.torrent` file. The daemon keys magnet records by the real info hash,
  surfaces `DownloadingMetadata` state, fetches metadata from tracker-
  discovered peers, then replaces the placeholder meta. Magnets with trackers
  are supported; fail-closed blocks metadata fetch. A local swarm test proves
  a magnet fetches metadata then downloads the real content.
- **HTTPS tracker support over contained sockets:** HTTPS is performed as TLS
  (`tokio-rustls` + `rustls` with the ring provider, `webpki-roots`) over the
  binder's contained TCP connection, with system-root certificate validation.
  The engine dispatches `https://` trackers through the same contained
  `http_get` path. Fail-closed blocks HTTPS; a local self-signed TLS fixture
  test proves the contained-socket HTTPS path with certificate validation.
  See ADR-0018.
- **Mainline DHT (BEP 5):** `swarmotter-core::dht` implements pure KRPC
  encode/decode, node IDs with XOR distance, a bounded routing table, compact
  node/peer parsing, and `ping`/`find_node`/`get_peers`/`announce_peer` query
  builders (unit-tested). `swarmotterd::dht::DhtRunner` drives KRPC over the
  binder's contained UDP socket: bootstrap, iterative `get_peers` merged into
  the engine candidate pool for non-private torrents, trackerless magnet
  fallback via DHT, `announce_peer`, node-count status, and fail-closed
  blocking. The engine wraps DHT calls in hard time bounds so unreachable nodes
  cannot stall downloads. A local KRPC fixture test proves `get_peers` peer
  discovery. See ADR-0019.
- **Production uTP (BEP 29):** `swarmotter-core::utp` now implements full
  production uTP over the binder's contained UDP socket, split into focused
  modules: `header` (BEP 29 20-byte header encode/decode, packet types, first
  extension nibble), `sack` (Selective ACK extension encode/decode), and
  `congestion` (LEDBAT-style delay-based congestion control with base/current
  delay tracking, queuing-delay target, slow start, additive growth below
  target, shrink above target, loss/retransmit response with RTO backoff, and
  a bounded window). `UtpConnection` implements the full connection lifecycle:
  SYN/STATE handshake (initiator and responder), connection-id assignment and
  validation, in-order receive reassembly with out-of-order hold and SACK
  recovery, duplicate suppression, cumulative and selective ACK, retransmission
  of timed-out in-flight packets, bounded send/receive buffers, idle timeout,
  graceful close (FIN with transmission tracking), and RESET teardown. `UtpStream`
  exposes the connection as an `AsyncRead`+`AsyncWrite` byte stream via a
  background driver task, so the existing peer wire protocol machinery runs
  unchanged over uTP. A local contained byte-stream round-trip test, a uTP
  fail-closed test, and a full local-swarm uTP download test (generated payload,
  contained uTP seed, SHA-1 piece verification, final file-content check) prove
  the transport; a fail-closed test proves the `BlockedBinder` blocks uTP swarm
  downloads. See ADR-0020.
- **TCP/uTP transport selection in the engine:** the engine opens peer streams
  through `swarmotter_core::utp::connect_peer_stream`, selecting TCP and/or uTP
  per config (`torrent.utp_enabled`, `torrent.utp_prefer_tcp`). The preferred
  transport is tried first with the other as a fallback, and fallback now
  covers peer-wire handshake failures as well as raw connection failures; TCP
  remains available; private-torrent, rate-limit, endgame, and fail-closed
  containment behavior apply unchanged to the uTP path.
- The engine now terminates gracefully after a bounded number of consecutive
  no-peer announce rounds when a torrent has no trackers, no seed peers, and no
  DHT result, instead of looping forever.
- Tests now total: core 168 unit + engine/daemon/seeder/dht/utp/endgame/bandwidth/metadata/tls/containment/api/web + 11 local
  swarm (HTTP tracker, UDP tracker, direct peer, inbound seeding, endgame, global bandwidth, per-torrent bandwidth, PEX, magnet, uTP download, uTP fail-closed) + 1 daemon download.
- **Inbound peer listening and seeding/upload:** the daemon now spawns an
  inbound `Seeder` (`swarmotterd::seeder`) alongside each active torrent. It
  binds a contained TCP listener through the binder's `bind_peer_listener()`,
  validates inbound handshakes, serves verified piece blocks from `StorageIo`,
  handles interested/unchoke, and accounts uploaded bytes into the shared
  `EngineState`. Pause/remove/network-block stop the seeder; fail-closed
  blocks the listener. A local swarm test verifies a completed download is
  served to a fresh leecher over the real protocol with uploaded-byte
  accounting.

### Fixed

- **Bulk torrent add responsiveness:** native magnet and `.torrent` add calls now
  return after registering and queueing the torrent instead of waiting for queue
  reconciliation and engine startup. Rapid add bursts coalesce queue
  reconciliation in the background, and engine startup no longer holds the
  registry lock while resolving runtime paths.
- **Tracker announce diagnostics:** tracker API rows now use per-tracker
  announce results instead of copying a torrent-level status message to every
  tracker. Successful announces populate `last_message`, seeders, leechers, and
  `last_announce`; `last_error` is only populated for failed announces.
- **Delete-data storage cleanup:** removing a torrent with delete-data enabled
  now removes only the torrent payload and fast-resume metadata while preserving
  the configured `download_dir` and `incomplete_dir` root directories.
- **mdBook publishing workflow:** renamed documentation tool version variables
  so mdBook does not treat them as `MDBOOK_*` configuration overrides during
  the GitHub Pages build.
- **GitHub Pages deploy action:** updated the Pages publishing workflow to the
  current Pages action major versions used by GitHub's Node 24 Actions runtime.

### Changed

- The Web UI Settings tab now uses a structured full-configuration editor for
  API, compatibility, storage, network containment, torrent, bandwidth, queue,
  seeding, DHT, PEX, watch folder, and logging settings instead of mixing a
  partial form with a raw JSON editor.
- Active downloads now write partial data and partial fast-resume metadata
  under `[storage].incomplete_dir` when configured. After all pieces verify,
  the engine moves completed data to `[storage].download_dir` and removes
  SwarmOtter fast-resume metadata so completed download directories contain
  only user payload files. If `incomplete_dir` is unset, the active and
  completed roots remain the same.
- API/UI transfer rates are now calculated from live engine byte-counter
  deltas instead of remaining at zero while progress changes. Downloaded byte
  counters now track received network bytes, completed byte counters continue
  to track verified pieces, and displayed rates decay smoothly across short
  quiet samples instead of snapping immediately to zero. Fast-resume and
  recheck progress update verified completion without being counted as newly
  downloaded network bytes, so resumed torrents do not produce false download
  speed spikes. When a non-preallocated payload file is visibly ahead of its
  fast-resume metadata, or when fast-resume claims verified data that is no
  longer present on disk, the engine now rechecks storage instead of trusting a
  stale resume bitfield, so progress reflects verified on-disk pieces rather
  than stale resume state.
- Added `GET /api/v1/torrents/:hash/stats` for per-torrent troubleshooting and
  performance diagnostics, including live rates, byte counters, active peer
  workers, known peers, tracker status/message, and last announce time.
- The normal download loop now uses bounded multi-peer piece downloading when
  more than one peer is known, with per-piece reservations to avoid duplicate
  normal-mode downloads. Tracker announces now request more peers so heavily
  seeded torrents can fill the bounded peer worker set.
- The normal download loop now rotates through eligible peers instead of
  repeatedly selecting only the first tracker results, keeps normal peer
  sessions alive longer to reduce reconnect churn, imports PEX-discovered
  peers from parallel sessions, and uses a bounded in-flight block request
  window while downloading each piece. Peer selection now balances IPv4 and
  IPv6 candidates when both families are available, uses time-bound
  suppression for failed and idle peers instead of permanent per-run exclusion,
  and the contained TCP binder applies bounded connect timeouts plus
  TCP_NODELAY so stalled peer dials cannot pin worker slots. Dual-stack tracker
  hostnames now select a usable address family instead of blindly using the
  first resolver result, while explicit `ipv6.*` tracker hostnames retain IPv6
  preference. uTP now advertises a larger bounded receive window to avoid
  per-flow low-MB/s caps on higher-latency public swarms. The default DHT
  bootstrap set now includes the common `router.utorrent.com:6881` node, and
  normal peer sessions stay open longer so useful peers are not recycled as
  quickly. The live engine now honors manual reannounce commands by
  immediately refreshing tracker peers, and periodic refreshes also retry DHT
  peer discovery instead of relying on a single startup lookup.
- Normal-mode peer scheduling now refills failed/stalled worker slots from the
  full eligible peer candidate pool while useful sessions are still running,
  instead of waiting for an entire fixed batch to finish. Peer failure
  diagnostics now count failed parallel peer attempts so the stats endpoint can
  distinguish a large discovered pool from a small set of productive sockets.
  Normal peer rounds now return on the discovery refresh cadence so PEX,
  tracker, and DHT peers are merged into the candidate pool regularly instead
  of being held behind long-lived peer sessions.
- Normal peer sessions now stay open across multiple discovery refresh windows
  instead of being torn down every refresh cadence, reducing reconnect churn
  against large public swarms. Live peer diagnostics now require recent peer
  activity before counting peers as useful or unchoked, so the UI distinguishes
  a large discovered peer pool from currently productive sockets.
- Long-lived normal peer rounds now wake periodically to import PEX and
  refreshed discovery candidates, then backfill open worker slots without
  waiting for the entire round to end. Parallel peer sessions now keep a small
  window of distinct pieces in flight per peer, bounded by the existing block
  request pipeline, so high-capacity peers do not stall behind a single
  256 KiB piece at a time.
- Normal-mode peer sessions now advertise and parse BEP 10 `reqq`, then adapt
  each peer's outstanding block request window from observed throughput while
  respecting the remote and local caps. Piece reservation now tracks per-peer
  availability, prefers rarer eligible pieces, and avoids idle backoff for
  peers whose useful pieces are only temporarily reserved by other workers.
- Normal-mode and endgame block accounting now accepts only requested offsets
  with the expected block length, ignores duplicate/unsolicited/wrong-sized
  blocks for request and rate accounting, and releases reserved piece/request
  slots when a peer session errors or times out.
- Completed verified pieces are now written through a full-piece storage path
  that validates piece bounds and preserves multi-file boundaries while
  reducing hot-path write overhead. Piece-slice writes are flushed before
  verification or immediate reads so sparse, non-preallocated files cannot
  expose stale lengths during fast local checks.
- DHT KRPC query encoding now uses canonical outer dictionary ordering, batched
  lookups use unique transaction IDs, and DHT lookups follow IPv6 `nodes6`
  results through IPv6-contained UDP sockets instead of trying to use the first
  bootstrap node's address family for every follow-up query.
- `GET /api/v1/torrents/:hash/stats` now includes additional live performance
  diagnostics for useful/choked/unchoked peers, recent peer/tracker failures,
  and tracker/DHT/PEX discovery freshness.
- `[storage].preallocate` is now honored by the live engine. When disabled, the
  engine creates required directories and writes pieces as needed instead of
  pre-sizing all files.
- Partial `[network]` tables that specify a path such as
  `required_interface = "br0"` now default to strict mode, so DHCP/SLAAC-safe
  interface binding no longer requires a redundant `mode` field.
- Partial `[bandwidth]`, `[queue]`, and `[seeding]` tables now apply documented
  defaults for omitted fields instead of requiring every field in the table.
- IPv6 torrent networking is enabled by default in both `[network]` and
  `[torrent]`, while strict containment still blocks traffic unless the
  configured path is enforceable.
- Tracker diagnostics now preserve the concrete last announce error when a
  torrent stops after no peer discovery, instead of replacing it with only a
  generic no-peer message.
- Torrent removal now force-stops active data-plane tasks before deleting
  files, so `delete_data = true` returns promptly and removes active incomplete
  payloads even when a peer session is stalled.
- Linux interface-bound containment can now validate DNS for hostname trackers
  and DHT bootstrap nodes when DNS is tied to the required interface, including
  systemd-resolved link DNS such as `resolvectl dns br0`.
- Strict fail-closed containment now requires an enforceable socket path:
  `required_interface`, `required_source_ipv4`, `required_source_ipv6`, or
  `required_network_namespace`.
- Tracker, UDP tracker, and DHT bootstrap hostname resolution now runs through
  `NetworkBinder::resolve_host()` after containment enforcement.
- UDP tracker and uTP sockets now choose IPv4 or IPv6 binding from the remote
  address family, so interface-bound containment can cover both families.
- Restructured the repository from a single broken crate into a Rust
  workspace under `crates/` (`swarmotterd`, `swarmotter-core`,
  `swarmotter-api`, `swarmotter-web`).
- Updated architecture, API, configuration, and deployment docs to reflect
  the implemented design.
- Expanded the user-facing deployment and network-containment docs with
  Gluetun guidance, Mermaid diagrams, and a GitHub Pages publishing workflow
  for the mdBook output. ADR-0028 records the decision.
- Updated `THIRD_PARTY_LICENSES.md` with the full direct dependency list and
  containment review notes.
- Fixed `piece_file_ranges` to use the correct file index (it previously used
  the output-list length), affecting multi-file piece-to-file mapping.

### Security

- Enforced `api.require_auth` for `/api/v1` routes and require an auth token
  when that mode is enabled.
- Redacted `api.auth_token` from `GET /api/v1/settings`.
- Bounded peer frames, peer bitfields, BEP 9 metadata, tracker HTTP responses,
  and unsafe torrent/storage paths to reject malformed or oversized inputs.

### Notes

- All pure logic layers (parsing, validation, queue/bandwidth/ratio, storage
  layout, fast resume, watch import, network containment) are implemented and
  tested. The live TCP and uTP peer protocols, HTTP/HTTPS/UDP tracker announce,
  DHT, PEX, BEP 9 magnet metadata fetch, inbound seeding/upload, endgame mode,
  live bandwidth shaping, real disk I/O with fast resume, and the local-swarm
  download harness are implemented and tested end to end against local fixtures.
  All required `v1.0.0` data-plane capabilities are complete; see
  `design/v1-completion-tracker.md`.
- The single documented non-blocking limitation is OS-level DNS enforcement,
  which is platform-specific and not implemented in-process; the application
  fails closed (surfacing `dns_not_constrained`) when it cannot confirm DNS is
  constrained in strict mode. See `design/v1-completion-tracker.md`.
