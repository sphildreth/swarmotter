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
  submission, and watch-folder import. Watch ingestion requires two unchanged
  length/modified-time observations, never follows symlinks, bounds and
  rechecks the file read, processes one terminal result per unchanged
  fingerprint per run, treats registered hashes as successful duplicates, and
  never overwrites or recursively rediscovers its own in-root archive/failure
  destinations. Destination exclusions are lexical, component-aware, and
  scoped to one configured folder so an overlapping root remains independent.
  API and watch adds share one durable registry/queue transaction with exact
  rollback before any event or scheduling side effect (ADR-0054).
- **Peer discovery and alternate data sources:** HTTP trackers, HTTPS
  trackers, UDP trackers, HTTP/HTTPS webseeds (`url-list`), DHT, PEX, tracker
  tiers, manual tracker lists. HTTP(S) announce and supported BEP 48 scrape,
  plus webseed range reads, use one framed and bounded HTTP/1 client over
  binder-provided contained streams. Scrape is derived only from an
  `announce*` final path component; UDP scrape is explicitly unsupported.
  Download, magnet, reannounce, completion, and seeder announce paths schedule
  scrape attempts. The latest attempt status/time is separate from retained
  last-success counts so a failed scrape cannot erase useful data.
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
- **Seeding/ratio:** global and persisted per-torrent ratio limits, idle seed
  limits, seed-forever precedence, stop at target, upload/download accounting,
  ratio calculation, and a truthful queued/active/automatic/manual seeding
  lifecycle. Per-torrent `null` targets inherit globals; explicit zero is a
  real immediate target. Policy replacement must persist before success,
  survive restart, re-evaluate active/automatically-stopped content, and never
  auto-resume an operator pause. An optional **selfish** completion policy
  (`torrent.selfish`) removes a torrent from the daemon immediately after its
  download completes while preserving the downloaded data and not seeding it;
  already-completed managed records are also removed on runtime reconciliation.
- **Bandwidth and peer sessions:** global and per-torrent download/upload
  limits, alternate speed mode, maximum peers globally and per torrent, and
  rate-limit state. One retained per-torrent bandwidth limiter is shared by
  downloader and seeder so live upload changes apply without task replacement.
  `bandwidth.max_peers` is the authoritative process-wide lifetime cap for all
  inbound and outbound peer TCP/uTP sessions across every torrent, including
  metadata, normal serial/parallel, endgame, and seeding sessions.
  `max_peers_per_torrent` composes as an additional lifetime cap for each
  torrent; it does not partition or divide the global cap. Global zero is
  unlimited, but diagnostics must still report observed in-use sessions and
  report available capacity as `null`. Tracker, webseed, DHT, and DNS activity
  is explicitly outside this peer-session budget.
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
- **Deployment:** Linux daemon, systemd, Linux release tarballs and packages,
  containers (Podman/Docker where practical), VPN network namespace, reverse
  proxy, persistent volumes.
- **Observability:** structured logs, health endpoints, global/per-torrent
  stats, network/DHT/tracker/watch-folder state, optional Prometheus metrics.

## Acceptance criteria

Detailed acceptance criteria are tracked per capability. The project is ready
for `v1.0.0` only when every item in the `v1.0.0` completion checklist (see
`design/PRD.md`) is complete and:

- All required torrent input methods work.
- Watch-folder acceptance covers partial copies and read-time changes,
  deterministic recursive/non-recursive scans, symlink exclusion, restart
  duplicate handling, permanent/transient retry classification,
  non-overwriting post actions, 10,000-entry history eviction, concurrent scan
  serialization, and persistence rollback with no ghost state or premature
  events/scheduling.
- Magnet metadata fetch, DHT, PEX, HTTP/HTTPS/UDP trackers, HTTP/HTTPS
  webseeds, and peer protocol download/upload work.
- Contained HTTP acceptance covers framed Content-Length, chunked and legal
  close-delimited bodies; malformed/truncated/over-limit failures; bounded
  redirects with containment repeated on every hop; HTTPS upgrade and
  downgrade rejection; exact webseed 206 ranges; HTTP/HTTPS BEP 48 scrape
  derivation/parsing/scheduling; retained last-success counts; and explicit
  unsupported UDP scrape.
- Fast resume, forced recheck, watch folders, browser magnet submission, file
  selection/priorities, queueing, ratio/seeding limits, bandwidth limits, and
  the authoritative process-wide plus composing per-torrent peer-session caps
  work at their production entry points. Peer-cap acceptance covers inbound
  and outbound TCP/uTP session lifetimes, metadata, serial/parallel, endgame,
  and seeding paths; proves unlimited observed diagnostics; and proves tracker,
  webseed, DHT, and DNS activity is excluded.
- Verified torrent/file byte totals are calculated from actual verified piece
  ranges, including final-piece and multi-file boundaries, before and after
  restore and recheck.
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

## Traceability

The production entry point, named production-path acceptance test, and
operator/developer documentation for every capability and acceptance criterion
above are indexed in [v1-traceability.md](v1-traceability.md). That release
audit also maps every completed row in
[v1-completion-tracker.md](v1-completion-tracker.md) and identifies the
production reachability evidence that cannot be replaced by a type, helper, or
mock-only assertion. Keep this document, the traceability matrix, and `PRD.md`
aligned whenever a capability or acceptance contract changes.
