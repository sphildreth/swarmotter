# Changelog

This file records notable project changes. It follows the
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format and uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

All notable changes are recorded by capability and acceptance criteria, not by
date or duration estimates.

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
