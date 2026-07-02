# Changelog

This file records notable project changes. It follows the
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format and uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

All notable changes are recorded by capability and acceptance criteria, not by
date or duration estimates. SwarmOtter's first release is `v1.0.0`; there is no
MVP release.

## [Unreleased]

### Added

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
- **Live torrent data-plane engine (partial):** `NetworkBinder` abstraction
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
  the inbound listener. Used by UDP trackers, future DHT/uTP, and seeding.
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
  acquire), including the endgame path. A throttling local swarm test proves a
  tight download limit materially slows a real download. Per-torrent limits
  are modeled in settings; global limits are live.
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
- Tests now total: core 138 unit + engine/daemon/seeder/endgame/bandwidth/metadata/tls/containment/api/web + 8 local
  swarm (HTTP tracker, UDP tracker, direct peer, inbound seeding, endgame, bandwidth, PEX, magnet) + 1 daemon download.
- **Inbound peer listening and seeding/upload:** the daemon now spawns an
  inbound `Seeder` (`swarmotterd::seeder`) alongside each active torrent. It
  binds a contained TCP listener through the binder's `bind_peer_listener()`,
  validates inbound handshakes, serves verified piece blocks from `StorageIo`,
  handles interested/unchoke, and accounts uploaded bytes into the shared
  `EngineState`. Pause/remove/network-block stop the seeder; fail-closed
  blocks the listener. A local swarm test verifies a completed download is
  served to a fresh leecher over the real protocol with uploaded-byte
  accounting.

### Changed

- Restructured the repository from a single broken crate into a Rust
  workspace under `crates/` (`swarmotterd`, `swarmotter-core`,
  `swarmotter-api`, `swarmotter-web`).
- Updated architecture, API, configuration, and deployment docs to reflect
  the implemented design.
- Updated `THIRD_PARTY_LICENSES.md` with the full direct dependency list and
  containment review notes.
- Fixed `piece_file_ranges` to use the correct file index (it previously used
  the output-list length), affecting multi-file piece-to-file mapping.

### Notes

- The pure logic layers (parsing, validation, queue/bandwidth/ratio, storage
  layout, fast resume, watch import, network containment) are implemented and
  tested. The live TCP peer protocol, HTTP tracker announce, real disk I/O
  with fast-resume, and a local-swarm download harness are now implemented and
  tested end to end against local fixtures. The remaining `v1.0.0` data-plane
  work is UDP trackers, DHT, PEX, uTP, inbound peer listening/seeding upload,
  endgame mode, magnet metadata fetch (BEP 9), and bandwidth shaping; see
  `docs/v1-completion-tracker.md`.