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
enforcement are implemented and tested. The remaining major work is the live
torrent data-plane engine: peer wire protocol, DHT, PEX, UDP/HTTP tracker
announce, real disk I/O with fast resume, and a local-swarm test harness.
Platform-specific interface/source binding is abstracted behind
`InterfaceProbe`; the OS probe surfaces `interface_missing` in strict mode by
default, which is correct fail-closed behavior.

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
- [~] Socket binding abstraction (TCP/UDP) — abstraction designed; live socket
      creation/binding for peer/DHT/tracker traffic is part of the engine work
- [~] DNS containment strategy — `validate_dns` config + `dns_not_constrained`
      state implemented; OS-level DNS enforcement is platform-specific
- [x] Network containment integration tests (fail-closed via daemon)

### Torrent Metadata

- [x] Magnet URI parser (info hash, name, trackers, malformed handling)
- [x] `.torrent` metadata parser (single/multi-file, validate, private flag)
- [x] Info hash handling
- [x] Metadata-fetch state for magnets (`DownloadingMetadata` state)
- [x] Duplicate detection by info hash

### Peer Discovery

- [~] HTTP/HTTPS tracker announce/scrape — model + tiers; live announce engine
- [~] UDP tracker announce — model (`TrackerKind::Udp`); live announce engine
- [~] DHT bootstrap/lookup — config + status model; live DHT engine
- [~] PEX peer exchange — config + status model; live PEX engine
- [x] Tracker tiers and manual tracker lists
- [x] Tracker edit/add/remove via API

### Peer Protocol

- [ ] TCP peer connections (through containment layer) — pending engine
- [ ] uTP/UDP peer connections where practical — pending engine
- [ ] Handshake and metadata exchange — pending engine
- [ ] Piece availability and request scheduling — logic present in core
      (`PieceProgress`, file/piece range mapping); live scheduling pending
- [ ] Choking/unchoking, endgame — pending engine
- [ ] Upload/download accounting — accounting types + API present; live pending
- [ ] Bad peer detection/suppression — pending engine
- [x] IPv4/IPv6 controls — `allow_ipv6` config + validation

### Storage

- [x] File layout (incomplete/complete dirs, multi/single-file) logic
- [x] Piece read/write and verification logic (`verify_piece`)
- [x] Partial downloads and sparse files — layout + sparse config
- [x] Fast resume metadata (JSON format, roundtrip tested)
- [x] Forced recheck (`recheck` action + `Checking` state)
- [x] File selection and prioritization (API + models)
- [x] Move/rename behavior (API + models)
- [~] Missing/changed file detection — logic scaffolding; live detection pending
- [~] Real disk I/O for writes/reads — `tokio::fs` available; engine pending

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
      bandwidth, config, network containment, storage, fast resume, watch)
- [x] Integration tests (API: add magnet/file, lifecycle, settings, network,
      stats, duplicate; daemon: containment fail-closed, watch import)
- [ ] Network containment live tests (VPN path removed while active) — pending
      live engine
- [~] Storage tests — logic tested; live interrupted-write/missing-file pending
- [ ] Local swarm tests — pending live peer/DHT/PEX/tracker engine

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

None currently. The remaining work is implementation of the live torrent
data-plane engine, which is unblocked but substantial. Platform-specific
`InterfaceProbe` OS-level enumeration (getifaddrs) and DNS enforcement are
abstracted; the abstraction enforces fail-closed correctly by surfacing
`interface_missing`/`dns_not_constrained` in strict mode when the OS probe
cannot confirm the path.

## Test Status

| Command | Result |
| --- | --- |
| `cargo fmt` | pass |
| `cargo check` | pass |
| `cargo test` | pass (92 tests) |
| `cargo clippy --all-targets` | pass (test-only style warnings) |
| end-to-end daemon run (curl health/version/add/list) | pass |

## ADRs Created or Updated

- ADR-0009: Foundational dependency stack
- ADR-0010: API versioning, envelope, and event delivery
- ADR-0011: Bencode implementation and fast-resume format

## Notes

The pure logic layers are complete and tested. The live networked engine
(peer wire protocol, DHT, PEX, UDP/HTTP trackers, real disk I/O, local swarm
tests) is the primary remaining capability for `v1.0.0`. The architecture is
structured so the engine slots into `swarmotterd` and `swarmotter-core` without
changing the API surface or network containment contract.