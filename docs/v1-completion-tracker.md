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
is now partially implemented and exercised end to end against local fixtures:
real TCP peer wire protocol (handshake, messages, request/piece, block
assembly, SHA-1 verification), HTTP tracker announce (compact peer parsing,
tiers), real disk I/O with fast-resume save/load/recheck, a per-torrent engine
task wired into the daemon, and a local-swarm integration harness that
completes a real download from a generated payload through a local tracker and
seed peer.

The remaining major work is the rest of the v1.0.0 data plane: UDP trackers,
DHT, PEX, uTP, inbound peer listening/seeding upload, endgame mode, magnet
metadata fetch (BEP 9), and bandwidth shaping. Platform-specific
interface/source binding is abstracted behind `InterfaceProbe`; the OS probe
surfaces `interface_missing` in strict mode by default, which is correct
fail-closed behavior. Live sockets are centralized behind the `NetworkBinder`
abstraction (see ADR-0012).

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
      `ContainedBinder` (source-bound TCP + fail-closed) and `LoopbackBinder`
      for tests; UDP binder method is part of the remaining UDP tracker/DHT
      work
- [~] DNS containment strategy — `validate_dns` config + `dns_not_constrained`
      state implemented; tracker hostname resolution is performed inside the
      binder subject to config validation; OS-level DNS enforcement is
      platform-specific
- [x] Network containment integration tests (fail-closed via daemon)

### Torrent Metadata

- [x] Magnet URI parser (info hash, name, trackers, malformed handling)
- [x] `.torrent` metadata parser (single/multi-file, validate, private flag)
- [x] Info hash handling
- [x] Metadata-fetch state for magnets (`DownloadingMetadata` state)
- [x] Duplicate detection by info hash

### Peer Discovery

- [x] HTTP/HTTPS tracker announce/scrape — announce URL construction, compact
      peer parsing, tiers, private handling, and live announce through the
      `NetworkBinder` (HTTPS TLS over the contained socket is remaining)
- [~] UDP tracker announce — model (`TrackerKind::Udp`) and compact peer
      parsing present; live UDP announce engine (binder UDP method) remaining
- [~] DHT bootstrap/lookup — config + status model; live DHT engine remaining
- [~] PEX peer exchange — config + status model; the engine accepts
      directly-supplied seed peers (used by the local swarm test); live PEX
      engine remaining
- [x] Tracker tiers and manual tracker lists
- [x] Tracker edit/add/remove via API
- [x] Tracker status surfaced through API/UI from live engine state

### Peer Protocol

- [x] TCP peer connections (through containment layer) — real handshake,
      bitfield, interested/choke, request/piece, block assembly, SHA-1
      verification, progress, disconnect handling, bad-peer suppression,
      bounded concurrency (see ADR-0013)
- [ ] uTP/UDP peer connections where practical — pending binder UDP method
- [x] Handshake and message exchange (BEP 3) — implemented and tested; BEP 9
      metadata exchange pending (magnet metadata fetch)
- [x] Piece availability and request scheduling — live scheduling in the
      engine over `Bitfield`/`block_requests`; endgame mode pending
- [~] Choking/unchoking, endgame — choke/unchoke handled; endgame and our
      outbound unchoke/upload policy pending
- [x] Upload/download accounting — accounting wired into `EngineState` and
      reconciled into summaries; live upload/seeding pending
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

### Queue, Seeding, Bandwidth

- [x] Queue management logic (limits, up/down/top/bottom, start-now, auto-start)
- [x] Ratio/seeding limits logic (global and per-torrent, idle, seed-forever)
- [x] Bandwidth limits logic (global and per-torrent, alternate mode, max peers)
- [x] Rate-limit state through API/UI (settings patch)

### Watch Folders & Browser Integration

- [x] Watch-folder scanner (stable write detection, recursive)
- [x] Import success/failure handling (archive/failure/leave/delete)
- [x] Per-watch-folder defaults (location, labels, paused/start)
- [x] Browser-friendly magnet API endpoint
- [x] Watch-folder status through API/UI

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

### Web UI

- [x] Torrent list
- [x] Add magnet / upload torrent
- [x] Torrent details (files, peers, trackers)
- [x] Queue controls
- [x] Bandwidth controls
- [x] Ratio/seeding controls (via settings)
- [x] Settings
- [x] Network/VPN health
- [x] Watch-folder status
- [x] Logs/errors

### Deployment

- [x] Linux daemon setup docs
- [x] Example systemd service
- [x] Container (Podman/Docker) setup docs + Dockerfile
- [x] VPN network namespace deployment guide
- [x] Reverse proxy example
- [x] Example config file

### Testing

- [x] Unit tests (magnet, torrent, info hash, tracker tiers, queue, ratio,
      bandwidth, config, network containment, storage, fast resume, watch,
      peer wire protocol, tracker announce, storage I/O)
- [x] Integration tests (API: add magnet/file, lifecycle, settings, network,
      stats, duplicate; daemon: containment fail-closed, watch import,
      daemon-driven real download via local tracker + seed peer)
- [ ] Network containment live tests (VPN path removed while active) — pending
      live inbound peer/listening engine
- [x] Storage tests — live interrupted-write/missing-file/multi-file boundary
      /resume roundtrip/recheck covered
- [~] Local swarm tests — real download completion from a generated payload
      through a local tracker and seed peer is covered (HTTP tracker + direct
      peer paths); DHT/PEX/uTP/seeding-upload local swarm tests pending

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

None currently. The live TCP peer protocol, HTTP tracker announce, real disk
I/O with fast-resume, and a local-swarm download harness are implemented and
tested. The remaining v1.0.0 data-plane work is unblocked: UDP trackers
(needs a binder UDP method), DHT, PEX, uTP, inbound peer listening/seeding
upload, endgame mode, magnet metadata fetch (BEP 9), and bandwidth shaping.
Platform-specific `InterfaceProbe` OS-level enumeration (getifaddrs) and DNS
enforcement are abstracted; the abstraction enforces fail-closed correctly by
surfacing `interface_missing`/`dns_not_constrained` in strict mode when the
OS probe cannot confirm the path.

## Test Status

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets` | pass (no warnings) |
| `cargo test --workspace` | pass (core 108 unit + engine/daemon/containment/api/web + 2 local swarm + 1 daemon download) |
| local swarm download (tracker + direct peer) | pass |
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

## Notes

The TCP peer protocol, HTTP tracker announce, real disk I/O, and the
per-torrent engine task are implemented and exercised end to end against local
fixtures (generated payloads, an in-process seed peer, and an in-process HTTP
tracker) using the contained `NetworkBinder` path. The API/UI surface is
unchanged but now reports real progress, peers, and tracker status; lifecycle
actions (pause/resume/remove/recheck/reannounce) drive real engine tasks. The
remaining v1.0.0 capabilities (UDP trackers, DHT, PEX, uTP, inbound
listening/seeding upload, endgame, BEP 9 metadata fetch, bandwidth shaping)
build on the binder + protocol + storage foundation added here.