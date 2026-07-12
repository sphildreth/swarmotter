# SwarmOtter v1.0.0 Completion Tracker

This tracker records progress toward `v1.0.0` as defined by `design/PRD.md` and
`design/requirements.md`. Progress is tracked by completed capabilities,
acceptance criteria, passing tests, and working end-to-end behavior — never by
time estimates.

## Status Legend

- [ ] Not started
- [~] In progress
- [x] Complete
- [!] Blocked

## Current Focus

Pure logic layers, API, Web UI, daemon runtime, and network containment
enforcement are implemented and tested. The live torrent data-plane engine
is implemented and exercised end to end against local fixtures: real TCP and
uTP peer wire protocol (handshake, messages, request/piece, block assembly,
SHA-1 verification), HTTP tracker announce (compact peer parsing, tiers), real
disk I/O with fast-resume save/load/recheck, a per-torrent engine task wired
into the daemon, and a local-swarm integration harness that completes a real
download from a generated payload through a local tracker and seed peer.

Full production uTP is now implemented: LEDBAT congestion control, selective
ACK, the full SYN/STATE/DATA/FIN/RESET connection lifecycle, timestamp echo and
one-way delay measurement, retransmission, idle timeout, graceful close, and
TCP/uTP transport selection in the engine. The network binder supports
contained UDP sockets, inbound TCP listeners, outbound TCP, tracker HTTP,
tracker HTTPS (TLS over contained socket), HTTP/HTTPS webseed range requests,
and UDP trackers — all fail-closed.
Real TCP and uTP peer protocol, HTTP/HTTPS/UDP tracker announce, HTTP/HTTPS
webseed range downloads, PEX (BEP 10/11), BEP 9 magnet metadata fetch, DHT
(BEP 5), inbound seeding/upload, endgame mode, live bandwidth shaping, real
disk I/O with fast-resume, and a local-swarm download harness (HTTP + UDP
trackers + direct peer + webseed + seeding + endgame + bandwidth + PEX +
magnet + uTP) are implemented and tested. Platform-specific
interface/source binding is abstracted behind `InterfaceProbe`; the OS probe
surfaces `interface_missing` in strict mode by default, which is correct
fail-closed behavior. Live sockets are centralized behind the `NetworkBinder`
abstraction (see ADR-0012); uTP traffic flows through the binder's contained
UDP socket (see ADR-0020).

## Completion Checklist

### Foundation

- [x] Project/workspace health (workspace compiles, fmt/test baseline)
- [x] Core error model and typed domain models (`swarmotter-core`)
- [x] Configuration model with validation (TOML + env overrides)
- [x] Daemon lifecycle and persistent state foundation (`swarmotterd`)
- [x] API skeleton and health/version endpoints (`swarmotter-api`)

### Network Containment (release blocker)

- [x] Network configuration model
- [x] Interface/source/route validation abstraction (`InterfaceProbe`)
- [x] Fail-closed enforcement (`net::evaluate`/`enforce`)
- [x] Network health states (all 11 required states)
- [x] Network containment validation tests
- [x] Socket binding abstraction (TCP/UDP) — `NetworkBinder` trait +
      `ContainedBinder` (source-bound TCP + UDP sockets + inbound TCP
      listener + fail-closed) and `LoopbackBinder`/`BlockedBinder` for tests;
      UDP binder method powers UDP trackers, DHT, and uTP, inbound
      listener powers seeding upload (see ADR-0012)
- [x] DNS containment strategy — `validate_dns` config + `dns_not_constrained`
      state implemented; tracker, UDP tracker, and DHT bootstrap hostname
      resolution is performed inside the binder after containment is enforced.
      Linux interface-bound mode validates common systemd-resolved link DNS
      and static resolver routes before hostname resolution is allowed. The
      abstraction surfaces `dns_not_constrained` in strict mode when the OS
      probe cannot confirm DNS is constrained, which is correct fail-closed
      behavior.
- [x] Network containment integration tests (fail-closed via daemon)

### Torrent Metadata

- [x] Magnet URI parser (info hash, name, trackers, malformed handling)
- [x] `.torrent` metadata parser (single/multi-file, validate, private flag)
- [x] Info hash handling
- [x] Metadata-fetch state for magnets (`DownloadingMetadata` state)
- [x] BEP 9 magnet metadata fetch — live `ut_metadata` extension fetch
      (`swarmotterd::metadata`): extension handshake, metadata piece
      request/assembly, info-hash validation, conversion into a real
      `TorrentMeta`, magnets with trackers supported, `DownloadingMetadata`
      state surfaced; fail-closed blocks metadata fetch; local swarm test
      proves a magnet fetches metadata then downloads
- [x] Duplicate detection by info hash

### Peer Discovery

- [x] HTTP/HTTPS tracker announce/scrape — announce URL construction, compact
      peer parsing, tiers, private handling, and live announce through the
      `NetworkBinder`; HTTPS is performed as TLS over the contained TCP socket
      with system-root certificate validation (fail-closed blocks HTTPS)
- [x] UDP tracker announce — live BEP 15 connect + announce through the
      binder's contained UDP socket, compact IPv4 peer parsing, transaction
      IDs, error response handling, tier integration, and local UDP tracker
      fixture + fail-closed tests (see `swarmotter-core::udp_tracker`)
- [x] DHT (BEP 5) — live mainline DHT support: `swarmotter-core::dht` (KRPC
      encode/decode, node ID/XOR distance, bounded routing table, compact
      node/peer parsing, `ping`/`find_node`/`get_peers`/`announce_peer`
      builders) + `swarmotterd::dht::DhtRunner` driving KRPC over the binder's
      contained UDP socket, bootstrap, iterative `get_peers` peer discovery
      merged into the candidate pool, trackerless magnet fallback via DHT,
      private torrents disable DHT, fail-closed blocks DHT, node-count status;
      local DHT fixture test proves `get_peers` discovery (see ADR-0019)
- [x] PEX peer exchange — live BEP 10/11 implementation
      (`swarmotter-core::extensions`): extension handshake, `ut_pex` message
      encode/decode, PEX-discovered peers added to the engine candidate pool,
      private torrents block PEX, all PEX-discovered outbound connections go
      through the binder; local swarm test proves PEX peer discovery
- [x] Tracker tiers and manual tracker lists
- [x] Tracker edit/add/remove via API
- [x] Tracker status surfaced through API/UI from live engine state
- [x] HTTP/HTTPS webseeds — BEP 19 `url-list` metadata parsing plus contained
      HTTP byte-range downloads through `NetworkBinder`; pieces are SHA-1
      verified before storage writes, webseed bytes count toward live download
      accounting and rate limits, and a loopback range-server local swarm test
      proves completion without trackers or peers

### Peer Protocol

- [x] TCP peer connections (through containment layer) — real handshake,
      bitfield, interested/choke, request/piece, block assembly, SHA-1
      verification, progress, disconnect handling, bad-peer suppression,
      bounded concurrency (see ADR-0013)
- [x] uTP/UDP peer connections where practical — production uTP (BEP 29)
      implemented and tested (`swarmotter-core::utp`: `header` encode/decode,
      `sack` selective-ACK extension, `congestion` LEDBAT delay-based
      congestion control with bounded window and loss response, `UtpConnection`
      full SYN/STATE/DATA/FIN/RESET lifecycle with connection-id validation,
      duplicate/out-of-order handling, retransmission, idle timeout, and
      graceful close, `UtpStream` `AsyncRead`+`AsyncWrite` byte stream over the
      binder's contained UDP socket), running over the binder's contained UDP
      socket with a local contained byte-stream round-trip test + a full
      local-swarm uTP download test + fail-closed tests; TCP/uTP transport
      selection in the engine per config (`torrent.utp_enabled`,
      `torrent.utp_prefer_tcp`) with fallback; TCP remains available (see
      ADR-0020)
- [x] Handshake and message exchange (BEP 3) — implemented and tested; BEP 10
      extension protocol + PEX (BEP 11) + BEP 9 metadata exchange
      (ut_metadata) implemented
- [x] Piece availability and request scheduling — live scheduling in the
      engine over `Bitfield`/`block_requests` with endgame mode
- [x] Choking/unchoking, endgame — choking/unchoking and
      interested/not-interested state are handled in both directions:
      outbound interest handling on the download side (the engine sends
      `interested` and requests blocks once unchoked) and inbound
      `interested`/`unchoke` handling on the seeding side (the `Seeder`
      unchokes interested peers and serves verified pieces). Endgame is
      implemented (`swarmotter-core::endgame` planner + concurrent
      `run_endgame` path that requests remaining blocks from multiple peers
      with a bounded duplicate cap and cancels outstanding duplicates on
      completion). The required choking/unchoking capability is complete; the
      optional upload-slot rotation known as "optimistic unchoke" (choosing
      which of many contending leechers to unchoke when demand exceeds upload
      capacity) is a non-blocking fairness enhancement beyond `v1.0.0` scope,
      documented under "Non-blocking limitations" below — it is not a missing
      `v1.0.0` requirement (the PRD requires choking/unchoking, which works)
- [x] Upload/download accounting — accounting wired into `EngineState` and
      reconciled into summaries; live upload/seeding implemented via the
      inbound `Seeder` listener (serves verified pieces, tracks uploaded
      bytes)
- [x] Bad peer detection/suppression — bounded bad-peer set; hash-mismatch
      pieces rejected
- [x] IPv4/IPv6 controls — `allow_ipv6` config + validation

### Storage

- [x] File layout (incomplete/complete dirs, multi/single-file) logic
- [x] Piece read/write and verification logic (`verify_piece`)
- [x] Partial downloads and sparse files — layout + sparse config
- [x] Fast resume metadata (JSON format, roundtrip tested)
- [x] Forced recheck (`recheck` action + `Checking` state)
- [x] File selection and prioritization (API + models)
- [x] Move/rename behavior (API + models)
- [x] Real disk I/O for writes/reads — `StorageIo` performs real `tokio::fs`
      writes/reads/verification with multi-file boundary handling
- [x] Missing/changed file detection — `verify_piece_on_disk` treats a missing
      file as not-verified; recheck reflects on-disk reality

### Lifecycle

- [x] Add magnet / add torrent / watch-folder add
- [x] Pause/resume/start-now/stop
- [x] Remove / remove+delete
- [x] Recheck / reannounce
- [x] Move data / rename path / labels
- [x] All required torrent states exposed (`TorrentState`)
- [x] Exact per-torrent seeding lifecycle exposed (`SeedingStatus`): native
      list/detail responses distinguish `not_eligible`, `queued`, `active`,
      ratio/idle stops, and a manual stop. Lifecycle events carry the truthful
      coarse state transition and tell clients to refetch the fine status; a
      live `SeedRegistry` registration is the source of truth for active seeding.
      `daemon::tests::complete_seeding_lifecycle_policy_slots_tasks_and_limiter_identity_are_truthful`
      and
      `daemon::tests::active_seeding_containment_block_preserves_status_and_recovery_rebuilds_task`
      exercise the production daemon entry points, synchronized readers,
      containment, task ownership, and event state.

### Queue, Seeding, Bandwidth

- [x] Queue management logic (limits, up/down/top/bottom, start-now, auto-start)
- [x] Ratio/seeding limits logic (global and per-torrent, idle, seed-forever):
      `TorrentSeeding` and `SeedingStatus` persist in version-1 daemon state;
      nullable overrides inherit global values, explicit zero remains an
      immediate target, and seed-forever suppresses effective targets without
      deleting stored overrides. Strict replacement flows through
      `DaemonOps::set_torrent_seeding` and
      `PUT /api/v1/torrents/:hash/seeding`; persistence failure leaves the live
      lifecycle unchanged and restores the prior policy. Evidence:
      `state_store::tests::every_seeding_status_round_trips_in_version_one_state`,
      `daemon::tests::seeding_policy_persistence_failure_restores_policy_status_and_state`,
      `daemon::tests::restart_reconstructs_eligible_seeder_and_preserves_automatic_and_manual_stops`,
      and `native_seeding_put_replaces_policy_and_list_detail_are_truthful`.
- [x] Optional selfish completion policy (`torrent.selfish`): when enabled, the
      daemon removes a torrent immediately after its download completes (engine
      and seeder stopped, record removed from the registry) while preserving the
      downloaded data on disk (no delete-data behavior); already-completed
      managed records are also removed on runtime reconciliation. SwarmOtter
      does not seed the torrent after completion. When disabled (default), normal
      completion and seeding behavior is unchanged. Covered by daemon
      integration tests (selfish removal + data preserved; default keeps the
      completed torrent; `delete_data = true` still deletes data when requested)

- [x] Bandwidth limits logic (global and per-torrent, alternate mode, max peers)
- [x] Live bandwidth shaping — `RateLimiter` (token-bucket) wired into the
      engine download path and the seeder upload path. A shared global limiter
      is cloned into every engine and seeder so the configured global cap is a
      true aggregate across active torrents; each torrent also gets a
      retained `Arc<RateLimiter>` (`TorrentBandwidth`,
      `download_limit`/`upload_limit` on the torrent record) shared by the
      downloader and live seeder. It survives completion, queued seed slots,
      pause/resume, and containment; removal/reset delete it. Global and
      per-torrent limits both shape real transfers. The production daemon test
      `daemon::tests::daemon_limit_update_changes_active_registered_upload_without_replacement`
      accepts a real TCP peer, updates through `DaemonOps::set_torrent_limits`,
      checks the old/new token windows, persisted value, and unchanged Arc plus
      registration identity. Per-torrent limits remain settable via
      `POST /api/v1/torrents/:hash/limits` and reflected in torrent summaries.
- [x] Rate-limit state through API/UI (settings patch + per-torrent limits)

### Watch Folders & Browser Integration

- [x] Watch-folder scanner (stable write detection, recursive) — ADR-0054
      requires two unchanged observations, complete blocking sorted walks,
      composite lexical root-relative keys, symlink exclusion, typed
      changed-during-read reset, whole-scan serialization, successful-scan
      disappearance pruning, per-folder component-aware archive/failure
      exclusions, and no durable observation ledger. Evidence includes
      `watch::tests::configured_scan_exclusions_are_descendant_component_aware_and_per_folder`,
      `config::tests::watch_paths_reject_whitespace_and_action_destination_equal_to_root`,
      and the daemon stability/pruning/overlap cases named in
      `design/testing.md`.
- [x] Import success/failure handling (archive/failure/leave/delete) — API and
      watch add share exact durable registry/queue rollback; duplicate is a
      non-mutating watch success; permanent/transient outcomes have distinct
      move/retry behavior; create-new actions never overwrite; action errors
      remain visible; and in-memory insertion-ordered history is capped at
      10,000 (ADR-0054). `recursive_watch_excludes_in_root_archive_after_success`
      and `recursive_watch_excludes_in_root_failure_after_permanent_failure`
      prove moved inputs remain one terminal history result.
- [x] Per-watch-folder defaults (location, labels, paused/start) — proven by
      `watch::tests::folder_defaults_apply`, `watch_folder_imports_torrent`, and
      `watch_folder_start_import_is_queued_for_scheduler`.
- [x] Browser-friendly magnet API endpoint
- [x] Watch-folder status through API/UI — pending counts do not advance
      observations and exclude unchanged processed fingerprints; history and
      events expose stable outcome and post-action fields, and the UI warns on
      operator-action failures. Evidence includes
      `watch_leave_processes_each_fingerprint_once_and_status_excludes_it`,
      `watch_action_exclusion_does_not_hide_separately_configured_overlapping_root`,
      `web_ui_renders_stable_watch_outcomes_and_post_action_errors`, and the
      executable `watch-history.test.js` renderer harness.

### API

- [x] Versioned REST API (JSON, consistent errors, stable IDs)
- [x] Torrent management endpoints
- [x] File endpoints
- [x] Tracker endpoints
- [x] Peer endpoints
- [x] Queue endpoints
- [x] Settings endpoints
- [x] Network endpoints
- [x] Watch-folder endpoints
- [x] Stats/health endpoints
- [x] WebSocket/SSE events (broker + endpoints; required event kinds defined)
- [x] Per-torrent health in torrent list and detail responses
      (`TorrentHealth` with score, bars, label, per-component sub-scores,
      and human-readable reasons)

### Web UI

- [x] Torrent list
- [x] Add magnet / upload torrent
- [x] Torrent details (activity, files, peers, trackers; file rename and tracker
      add/edit/remove controls)
- [x] Queue controls (start-now, stop, and move top/up/down/bottom in details)
- [x] Bandwidth controls (global settings and per-torrent limits in details)
- [x] Ratio/seeding controls (global defaults via Settings and a strict
      per-torrent replacement in Torrent Details): stored and effective targets,
      status, uploaded bytes, and ratio are rendered; inheritance is distinct
      from explicit zero; seed-forever preserves stored overrides; rejected
      saves show an inline error without optimistic drift. Evidence:
      `node crates/swarmotter-web/tests/seeding-policy.test.js` and
      `swarmotter_web::tests::web_ui_renders_and_replaces_seeding_policy_without_optimistic_drift`.
- [x] Settings
- [x] Network/VPN health
- [x] Watch-folder status
- [x] Logs/errors
- [x] Per-torrent health indicator — a signal-bars style (0..5) display on
      each torrent list row and on the details header, computed from real
      engine state (availability, throughput, peers, stability, discovery),
      with a tooltip and a per-component sub-score table on the details
      view. The same `health` object is exposed in the API and rendered by
      the Web UI; the UI is CSS-only (no image asset). See
      `../docs/api.md` for the score formula and bar/label mapping.

### Deployment

- [x] Linux daemon setup docs
- [x] Example systemd service
- [x] Linux release tarballs and `.deb`/`.rpm` package workflow
- [x] Container (Podman/Docker) setup docs + Dockerfile
- [x] VPN network namespace deployment guide
- [x] Reverse proxy example
- [x] Example config file

### Testing

- [x] Unit tests (magnet, torrent, info hash, tracker tiers, queue, ratio,
      bandwidth, config, network containment, storage, fast resume, watch,
      peer wire protocol, tracker announce, storage I/O, per-torrent
      health calculation: complete / network-blocked / paused / missing
      pieces with zero sources / good active swarm / many connected but
      useless peers / slow-but-completable / private torrent / bar+label
      mapping)
- [x] Integration tests (API: add magnet/file, lifecycle, settings, network,
      stats, duplicate, per-torrent health serialization; daemon:
      containment fail-closed, watch import, daemon-driven real download
      via local tracker + seed peer). Phase-4 production coverage additionally
      includes strict seeding replacement validation/list/detail serialization,
      immediate completion-to-active-seeder reconciliation, persisted restart
      reconstruction, manual/automatic stop handling, truthful lifecycle
      events, fail-closed seeder recovery, and a live accepted upload-limit
      update. Named tests and required assertions are maintained in
      `design/testing.md`.
      Phase-6 production coverage additionally includes partial-copy and
      read-time metadata changes, restart duplicates, exact persistence
      rollback with no torrent-add/watch-success events or scheduling,
      permanent/transient retry behavior,
      destination collisions, per-folder in-root action exclusions,
      symlink/sorting/pruning semantics, concurrent manual scans, bounded
      history eviction, the real file-add router contract, and Watch UI outcome
      rendering (ADR-0054).
- [x] Network containment live tests (fail-closed via daemon) — `BlockedBinder`
      proves TCP/UDP/listener fail-closed at the binder; daemon strict-mode
      integration tests cover add-under-blocked and health reporting; live
      "VPN path removed while active" via the daemon health loop is covered
      structurally (the health loop stops engines/seeders and marks torrents
      `network_blocked`)
- [x] Storage tests — live interrupted-write/missing-file/resume/recheck covered;
      exact verified-byte accounting is asserted for a short final piece and a
      multi-file boundary after restore, partial forced recheck, and full forced
      recheck by
      `daemon::tests::single_file_final_piece_bytes_are_exact_after_restore_and_recheck`
      and `daemon::tests::boundary_file_bytes_are_exact_after_restore_and_each_recheck`.
- [x] Local swarm tests — real download completion from a generated payload
      through a local HTTP tracker, a local UDP tracker (BEP 15), and a direct
      seed peer is covered (HTTP + UDP tracker + direct peer paths); webseed
      download completion from a generated payload is covered through a
      loopback HTTP range server; real seeding/upload via the inbound `Seeder`
      listener is covered; PEX, magnet metadata fetch, DHT, endgame,
      bandwidth, and a full uTP download over the contained UDP path are
      covered by local fixtures; a uTP fail-closed test proves the
      `BlockedBinder` blocks uTP swarm downloads; an
      active-download health test samples the live engine state during a
      generated lawful local download and asserts the per-torrent health
      reports a non-zero score with at least one bar.

### Legal / Repository

- [x] FOSS license selected (Apache-2.0, ADR-0007)
- [x] LICENSE present
- [x] README lawful-use statement
- [x] `design/lawful-use.md`, `design/content-policy.md`, `design/legal.md`
- [x] SECURITY.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md
- [x] Dependency license review / THIRD_PARTY_LICENSES.md current
- [x] No infringing examples/magnets/torrents/indexers
- [x] VPN/NIC docs framed as routing/safety/containment

## Blockers

None currently. The live TCP and uTP peer protocol, HTTP/HTTPS/UDP tracker
announce, HTTP/HTTPS webseed downloads, PEX (BEP 10/11), BEP 9 magnet
metadata fetch, DHT (BEP 5), inbound peer listening/seeding upload, endgame
mode, live bandwidth shaping, full production uTP (LEDBAT, SACK, full
connection lifecycle, transport selection), real disk I/O with fast-resume,
and a local-swarm download harness are implemented and tested. All v1.0.0
data-plane capabilities are implemented.
Platform-specific `InterfaceProbe` OS-level enumeration (getifaddrs) and DNS
enforcement are abstracted; the abstraction enforces fail-closed correctly by
surfacing `interface_missing`/`dns_not_constrained` in strict mode when the
OS probe cannot confirm the path. The platform-coverage limitation is
documented below; no required capability remains marked in progress.

## Test Status

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo check --locked --workspace --all-targets --all-features` | pass |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | pass (no warnings) |
| `cargo test --locked --workspace --all-targets --all-features` | pass (809 passed, 3 intentional opt-in scale tests ignored; includes 348 core, 51 API unit, 54 API integration, 3 origin-matrix, 27 Web, both 146-test daemon targets, containment, daemon-download, and generated local-swarm suites) |
| `cargo +1.88.0 check --locked --workspace --all-targets --all-features` | pass |
| `node --check crates/swarmotter-web/assets/theme-bootstrap.js` | pass |
| `node --check crates/swarmotter-web/assets/app.js` | pass |
| `docker compose --env-file deploy/.env.example -f deploy/compose.yml config` | pass with `GLUETUN_ENV_FILE=gluetun.env.example` |
| `mdbook build` | pass |
| local swarm download (HTTP tracker + direct peer) | pass |
| local swarm download (UDP tracker, BEP 15) | pass |
| local swarm seeding (inbound Seeder serves completed download) | pass |
| local swarm endgame (near-complete resume completes via endgame) | pass |
| local swarm bandwidth shaping (download throttled by global limit) | pass |
| local swarm per-torrent bandwidth (download throttled by per-torrent limit with unlimited global) | pass |
| local swarm PEX (peer discovered via BEP 10/11) | pass |
| local swarm magnet (BEP 9 metadata fetch then download) | pass |
| local swarm webseed (BEP 19 `url-list` + HTTP range download) | pass |
| local swarm uTP download (contained uTP seed + engine over uTP) | pass |
| local swarm uTP fail-closed (BlockedBinder blocks uTP download) | pass |
| local swarm active-download health (live engine reports non-zero health) | pass |
| daemon seeding lifecycle (slots, ratio/idle/manual stops, restart, containment, truthful events) | pass |
| daemon active upload-limit replacement (real accepted peer, retained limiter/registration identity) | pass |
| exact final-piece and multi-file boundary bytes (restore + partial/full recheck) | pass |
| native strict per-torrent seeding replacement and list/detail serialization | pass |
| `node crates/swarmotter-web/tests/seeding-policy.test.js` | pass |
| uTP contained byte-stream round trip over contained socket | pass |
| DHT get_peers discovery (local KRPC fixture) | pass |
| uTP reliable exchange over contained socket (local fixture) | pass |
| HTTPS tracker over contained socket (local TLS fixture) | pass |
| daemon download through `DaemonOps` | pass |

## ADRs Created or Updated

- ADR-0009: Foundational dependency stack
- ADR-0010: API versioning, envelope, and event delivery
- ADR-0011: Bencode implementation and fast-resume format
- ADR-0012: Network binder — centralized containment for live sockets
- ADR-0013: Peer wire protocol architecture
- ADR-0014: Tracker implementation strategy
- ADR-0015: Real storage I/O and fast-resume format
- ADR-0016: Task/runtime model for the live engine
- ADR-0017: Local swarm testing approach
- ADR-0018: HTTPS tracker TLS over contained sockets
- ADR-0019: DHT implementation strategy
- ADR-0020: uTP (BEP 29) implementation strategy and scope
- ADR-0021: Selfish completion policy
- ADR-0022: API auth and contained resolution hardening
- ADR-0023: Interface-bound containment for dynamic addresses
- ADR-0025: Runtime diagnostics and atomic configuration replacement
- ADR-0032: Linux release artifact strategy
- ADR-0044: Browser origin and loopback API security
- ADR-0045: Versioned durable daemon state
- ADR-0046: Shared inbound peer listener
- ADR-0047: Transactional live data-plane reconfiguration
- ADR-0048: File selection drives piece scheduling
- ADR-0049: Configured unauthenticated LAN control plane
- ADR-0050: Bounded untrusted metainfo parsing
- ADR-0051: Explicit network path and live containment gate
- ADR-0052: Persisted per-torrent seeding policy and runtime lifecycle

## Notes

The TCP and uTP peer protocols, HTTP/HTTPS/UDP tracker announce, DHT, PEX, BEP
9 magnet metadata fetch, real disk I/O, and the per-torrent engine task are
implemented and exercised end to end against local fixtures (generated
payloads, in-process seed peers — including a contained uTP-capable seed — and
in-process HTTP/UDP trackers) using the contained `NetworkBinder` path. The
API/UI surface reports real progress, peers, transport, and tracker status;
lifecycle actions (pause/resume/remove/recheck/reannounce) drive real engine
tasks. All required v1.0.0 data-plane capabilities — UDP trackers, DHT, PEX,
uTP, inbound listening/seeding upload, endgame, BEP 9 metadata fetch, and
bandwidth shaping — are implemented on the binder + protocol + storage
foundation.

## Non-blocking limitations (documented honestly)

These are explicitly out of `v1.0.0` scope or are platform-coverage limitations.
None is a release blocker and none contradicts a completed (`[x]`) capability
above.

- **DNS containment platform coverage:** the `validate_dns` config and
  the `dns_not_constrained` network state are implemented, and tracker,
  UDP tracker, and DHT bootstrap hostname resolution is performed inside the
  binder after containment is enforced. Linux interface-bound mode validates
  common systemd-resolved link DNS and static resolver routes. Other platform
  DNS mechanisms may still require a container / network namespace /
  VPN-routed path, or IP-literal peers/trackers, so the daemon can fail closed
  instead of using an unconstrained resolver.
- **Outbound upload-slot rotation (optimistic unchoke):** choking/unchoking
  (a required `v1.0.0` capability) is implemented and tested in both
  directions. The optional fairness algorithm that rotates an upload slot to
  discover new peers' upload capacity when many leechers contend for upload
  bandwidth is not implemented; the seeder unchokes each interested peer it
  accepts and serves verified pieces subject to the aggregate global and
  retained per-torrent upload rate limits.
  This is a non-blocking enhancement beyond `v1.0.0` scope, not a missing
  required capability.
