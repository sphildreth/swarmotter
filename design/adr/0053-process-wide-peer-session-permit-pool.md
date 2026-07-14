# ADR-0053: Process-Wide Peer Session Permit Pool

## Status

Accepted

## Context

`bandwidth.max_peers` was previously divided into per-engine worker estimates.
Rounding guaranteed at least one worker per active torrent, so enough torrents
could exceed the configured process total. Inbound seeding used a separate
allowance, and metadata, serial, parallel, endgame, TCP, and uTP work did not
share one authoritative lifetime count. The documented connection cap was
therefore not an enforced process-wide limit.

Peer-limit changes also replace ownership objects captured by engines and the
shared inbound listener. Resizing one semaphore in place, or committing a new
configuration before task reconstruction succeeds, would let old and new
session policies overlap or leave the runtime and persisted configuration out
of sync.

## Decision

- `DaemonRuntime` owns one process-wide `PeerPermitPool` and one retained pool
  per torrent. A peer session must own both permits. A nonzero limit is backed
  by exactly that many Tokio semaphore permits. Global zero is unlimited while
  retaining an observed in-use counter; per-torrent zero maps to the documented
  daemon default of 64.
- Outbound metadata, serial, normal parallel, and endgame work acquires before
  opening TCP or uTP and holds the RAII guard through connect, encryption,
  handshake, protocol work, and session teardown. Discovery, retry waits,
  trackers, webseeds, DHT nodes, and DNS do not consume peer permits.
- The shared inbound hub nonblockingly acquires the global permit immediately
  after accept and closes a denied socket before handshake. After the routed
  handshake identifies the torrent, it nonblockingly acquires that torrent's
  permit. A denial at either boundary increments one shared rejection counter.
  `SeederHub` is the only production inbound path. The legacy standalone
  `Seeder` is compiled only for focused unit tests and cannot form a
  production bypass.
- Permit guards are RAII-owned. Error, EOF, cancellation, and panic release
  capacity without manual balancing. Bounded diagnostic availability is
  derived from the same captured in-use count so snapshots are internally
  coherent; a defensive invalid pool reports zero availability.
- `bandwidth.max_peers` and `max_peers_per_torrent` values above
  `Semaphore::MAX_PERMITS` are invalid configuration. The global limit of zero
  reports `peer_limit = 0`, `peer_permits_available = null`, and still reports
  observed `peer_permits_in_use`.
- Live changes through both partial settings PATCH and full settings PUT are
  ADR-0047 data-plane transactions. The daemon stages new pools, snapshots the
  exact old global/per-torrent pool identities, every torrent lifecycle,
  persisted config bytes, and the formerly owned engine/seeder set; queue order
  and bypass membership are preserved and prior limits are restored. It then
  holds the data-plane transition lock continuously while stopping old
  peer-bearing tasks, waiting for every old global/per-torrent permit to drain,
  provisionally installing the candidate config/pools, and reconstructing and
  verifying the policy-eligible task set. Candidate pools cannot become active
  while a session still owns an old permit.
- A failed provisional install, reconstruction, or post-reconstruction
  persistence restores the exact old pool Arcs, configuration,
  lifecycle/recovery intent, queue behavior, config file, durable daemon state,
  and formerly owned task set before returning the error. A valid but currently unavailable strict
  containment path commits as blocked with recovery intent and no live peer
  task. A blocked-to-healthy replacement installs candidate health/gate state
  before locked reconstruction.
- Irreversible selfish-completion behavior reads a separately committed policy
  flag. Candidate reconstruction and reversible reconciliation cannot remove a
  torrent before full-config persistence succeeds; the flag and explicit
  selfish sweep change only after commit.
- Scheduler diagnostics expose authoritative `peer_limit`,
  `peer_permits_in_use`, `peer_permits_available`, and
  `peer_sessions_denied`. Older peer-worker fields remain additive
  compatibility telemetry about engine worker pressure, not enforcement of
  the process-wide connection cap.

## Consequences

- Mixed inbound and outbound sessions across all torrents cannot exceed either
  applicable nonzero limit, independent of active-torrent count or worker
  rounding.
- A worker can wait for capacity without opening a socket. Acquiring the
  per-torrent permit first avoids occupying scarce global capacity while the
  same torrent is already at its smaller cap.
- Inbound overload is rejected promptly rather than accumulating handshake
  tasks; operators can observe those rejections.
- Peer-limit changes intentionally interrupt and reconstruct peer-bearing data
  plane work, but verified progress, exact ownership, containment recovery, and
  persistent configuration remain transactional.
- New peer transports or session paths must accept the runtime-owned budget and
  prove permit lifetime in production-path tests. Non-peer discovery and HTTP
  paths must not be added to this cap merely because they use network sockets.

## Related Documents

- [ADR-0046: Shared Inbound Peer Listener](0046-shared-inbound-peer-listener.md)
- [ADR-0047: Transactional Live Data-Plane Reconfiguration](0047-transactional-live-data-plane-reconfiguration.md)
- [Configuration](../configuration.md)
- [Architecture](../architecture.md)
- [API](../api.md)
- [Testing](../testing.md)
- [Phase review](../2026-07-12.REVIEW.md)
