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
  envelope, events), ADR-0011 (bencode implementation and fast-resume format).
- Deployment artifacts: example config, systemd service unit, Dockerfile,
  nginx reverse-proxy example.
- 92 passing tests across core, API, web, and daemon (unit + integration,
  including network containment fail-closed and watch-folder import tests).

### Changed

- Restructured the repository from a single broken crate into a Rust
  workspace under `crates/` (`swarmotterd`, `swarmotter-core`,
  `swarmotter-api`, `swarmotter-web`).
- Updated architecture, API, configuration, and deployment docs to reflect
  the implemented design.
- Updated `THIRD_PARTY_LICENSES.md` with the full direct dependency list and
  containment review notes.

### Notes

- The pure logic layers (parsing, validation, queue/bandwidth/ratio, storage
  layout, fast resume, watch import, network containment) are implemented and
  tested. The live torrent peer/DHT/PEX/tracker/storage-I/O engine remains the
  primary remaining work toward `v1.0.0`; see `docs/v1-completion-tracker.md`.