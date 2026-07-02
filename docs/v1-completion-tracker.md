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

The remaining major work is the rest of the v1.0.0 data plane: DHT, and uTP.
The network binder now supports contained UDP sockets, inbound TCP listeners,
outbound TCP, tracker HTTP, tracker HTTPS (TLS over contained socket), and
UDP trackers — all fail-closed. Real TCP peer protocol, HTTP/HTTPS/UDP
tracker announce, PEX (BEP 10/11), BEP 9 magnet metadata fetch, inbound
seeding/upload, endgame mode, live bandwidth shaping, real disk I/O with
fast-resume, and a local-swarm download harness (HTTP + UDP trackers + direct
peer + seeding + endgame + bandwidth + PEX + magnet) are implemented and
tested. Platform-specific interface/source binding is abstracted behind
`InterfaceProbe`; the OS probe surfaces `interface_missing` in strict mode by
default, which is correct fail-closed behavior. Live sockets are centralized
behind the `NetworkBinder` abstraction (see ADR-0012).

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
      UDP binder method powers UDP trackers and future DHT/uTP, inbound
      listener powers seeding upload (see ADR-0012)
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
- [~] DHT bootstrap/lookup — config + status model; live DHT engine remaining
- [x] PEX peer exchange — live BEP 10/11 implementation
      (`swarmotter-core::extensions`): extension handshake, `ut_pex` message
      encode/decode, PEX-discovered peers added to the engine candidate pool,
      private torrents block PEX, all PEX-discovered outbound connections go
      through the binder; local swarm test proves PEX peer discovery
- [x] Tracker tiers and manual tracker lists
- [x] Tracker edit/add/remove via API
- [x] Tracker status surfaced through API/UI from live engine state

### Peer Protocol

- [x] TCP peer connections (through containment layer) — real handshake,
      bitfield, interested/choke, request/piece, block assembly, SHA-1
      verification, progress, disconnect handling, bad-peer suppression,
      bounded concurrency (see ADR-0013)
- [ ] uTP/UDP peer connections where practical — pending binder UDP method
- [x] Handshake and message exchange (BEP 3) — implemented and tested; BEP 10
      extension protocol + PEX (BEP 11) + BEP 9 metadata exchange
      (ut_metadata) implemented
- [x] Piece availability and request scheduling — live scheduling in the
      engine over `Bitfield`/`block_requests` with endgame mode
- [x] Choking/unchoking, endgame — choke/unchoke handled (inbound seeding
      unchoke + outbound interest handling); endgame implemented
      (`swarmotter-core::endgame` planner + concurrent `run_endgame` path that
      requests remaining blocks from multiple peers with a bounded duplicate
      cap and cancels outstanding duplicates on completion); our outbound
      unchoke/upload policy (optimistic unchoke) pending
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

### Queue, Seeding, Bandwidth

- [x] Queue management logic (limits, up/down/top/bottom, start-now, auto-start)
- [x] Ratio/seeding limits logic (global and per-torrent, idle, seed-forever)
- [x] Bandwidth limits logic (global and per-torrent, alternate mode, max peers)
- [x] Live bandwidth shaping — `RateLimiter` (token-bucket) wired into the
      engine download path and the seeder upload path; global download/upload
      limits affect real transfer behavior (verified by a throttling local
      swarm test); per-torrent limits are modeled (settings) and global limits
      are live
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
- [x] Network containment live tests (fail-closed via daemon) — `BlockedBinder`
      proves TCP/UDP/listener fail-closed at the binder; daemon strict-mode
      integration tests cover add-under-blocked and health reporting; live
      "VPN path removed while active" via the daemon health loop is covered
      structurally (the health loop stops engines/seeders and marks torrents
      `network_blocked`)
- [x] Storage tests — live interrupted-write/missing-file/multi-file boundary
      /resume roundtrip/recheck covered
- [~] Local swarm tests — real download completion from a generated payload
      through a local HTTP tracker, a local UDP tracker (BEP 15), and a direct
      seed peer is covered (HTTP + UDP tracker + direct peer paths); real
      seeding/upload via the inbound `Seeder` listener is covered; DHT/PEX/uTP
      local swarm tests pending

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

None currently. The live TCP peer protocol, HTTP/HTTPS/UDP tracker announce,
PEX (BEP 10/11), BEP 9 magnet metadata fetch, inbound peer listening/seeding
upload, endgame mode, live bandwidth shaping, real disk I/O with fast-resume,
and a local-swarm download harness are implemented and tested. The remaining
v1.0.0 data-plane work is unblocked: DHT, and uTP.
Platform-specific `InterfaceProbe` OS-level enumeration (getifaddrs) and DNS
enforcement are abstracted; the abstraction enforces fail-closed correctly by
surfacing `interface_missing`/`dns_not_constrained` in strict mode when the
OS probe cannot confirm the path.

## Test Status

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets` | pass (no warnings) |
| `cargo test --workspace` | pass (core 138 unit + engine/daemon/seeder/endgame/bandwidth/metadata/tls/containment/api/web + 8 local swarm + 1 daemon download) |
| local swarm download (HTTP tracker + direct peer) | pass |
| local swarm download (UDP tracker, BEP 15) | pass |
| local swarm seeding (inbound Seeder serves completed download) | pass |
| local swarm endgame (near-complete resume completes via endgame) | pass |
| local swarm bandwidth shaping (download throttled by limit) | pass |
| local swarm PEX (peer discovered via BEP 10/11) | pass |
| local swarm magnet (BEP 9 metadata fetch then download) | pass |
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