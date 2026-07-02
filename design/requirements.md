# Requirements

This document defines the required capabilities and acceptance criteria for
SwarmOtter. It is the source of truth for `v1.0.0` scope.

## Release model

SwarmOtter does **not** use an MVP release model. The first product release is
`v1.0.0`, reached only when every required capability below is implemented,
tested, documented, and usable.

DHT, PEX, UDP trackers, watch folders, browser magnet handling, file
prioritization, queueing, bandwidth controls, fast resume, VPN/NIC
containment, and legal documentation are all part of `v1.0.0` scope. They are
not optional future enhancements.

Progress is tracked by completed capabilities and acceptance criteria, not by
time or duration estimates.

## Required capabilities (v1.0.0)

- **Torrent input:** magnet links, `.torrent` files, browser-friendly magnet
  submission, watch-folder import.
- **Peer discovery:** HTTP trackers, HTTPS trackers, UDP trackers, DHT, PEX,
  tracker tiers, manual tracker lists.
- **Peer protocol:** TCP peers, uTP/UDP peers where practical, handshake,
  metadata exchange, piece availability, piece scheduling, choking, endgame,
  bad-peer handling, IPv4/IPv6 controls.
- **Storage:** incomplete/complete directories, multi-file and single-file
  torrents, file selection and prioritization, partial downloads, fast resume,
  forced recheck, piece verification, safe interrupted-write recovery, sparse
  files where supported, move/rename behavior, missing/changed file detection.
- **Lifecycle:** add, pause, resume, start-now, stop, remove, remove+delete,
  recheck, reannounce, move data, rename path, labels/categories, queue
  position, file priorities, per-torrent limits.
- **Queueing:** global active download/seed limits, queue order (up/down/top/
  bottom), start-now/bypass, auto-start behavior, per-torrent paused state.
- **Seeding/ratio:** global and per-torrent ratio limits, idle seed limits,
  seed-forever option, stop at target, upload/download accounting, ratio
  calculation.
- **Bandwidth:** global and per-torrent download/upload limits, alternate
  speed mode, maximum peers globally and per torrent, rate-limit state.
- **API:** complete REST API covering all user-facing features, JSON
  request/response, consistent errors, stable identifiers, API versioning,
  WebSocket/SSE event updates.
- **Web UI:** torrent list, add dialog, details, files, peers, trackers,
  activity/stats, settings, network health, watch-folder status, logs/errors.
  Function over form (see ADR-0006).
- **Network containment:** strict torrent traffic containment through a
  configured network path, fail-closed behavior, control plane separate from
  data plane (see `vpn-network-containment.md`).
- **Configuration:** config file plus environment variable overrides,
  validation, safe defaults, startup failure on invalid required settings,
  runtime updates where safe.
- **Deployment:** Linux daemon, systemd, containers (Podman/Docker where
  practical), VPN network namespace, reverse proxy, persistent volumes.
- **Observability:** structured logs, health endpoints, global/per-torrent
  stats, network/DHT/tracker/watch-folder state, optional Prometheus metrics.

## Acceptance criteria

Detailed acceptance criteria are tracked per capability. The project is ready
for `v1.0.0` only when every item in the `v1.0.0` completion checklist (see
`design/PRD.md`) is complete and:

- All required torrent input methods work.
- Magnet metadata fetch, DHT, PEX, HTTP/HTTPS/UDP trackers, and peer protocol
  download/upload work.
- Fast resume, forced recheck, watch folders, browser magnet submission, file
  selection/priorities, queueing, ratio/seeding limits, and bandwidth limits
  work.
- VPN/NIC containment and fail-closed behavior work and are tested.
- The API exposes all required functionality; the Web UI exposes all required
  operational controls; WebSocket/SSE updates work.
- Configuration and deployment are documented; automated, storage, network
  containment, and local swarm tests pass.
- License, legal, content-policy, and dependency-license documentation are
  complete, with no infringing examples or default pirate indexers included.

## Detailed plan

The full requirements and implementation plan, including the complete `v1.0.0`
checklist and data models, lives in `design/PRD.md`. This document summarizes
required capabilities; `PRD.md` remains the detailed reference. When this
document and `PRD.md` diverge, treat it as a documentation issue to resolve
immediately.

## TODO

- Cross-reference each capability above to specific acceptance criteria and
  tracked test areas as implementation begins.
- Keep this document and `PRD.md` aligned.