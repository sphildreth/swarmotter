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
- Tests now total: core 108 unit + engine/daemon/containment/api/web + 2 local
  swarm + 1 daemon download.

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