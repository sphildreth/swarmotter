# ADR-0077: Registry and queue lock order

## Status

Accepted

## Context

Persistence snapshots hold the torrent registry while acquiring the queue.
Queue planning previously acquired these locks in the opposite order. When
the operations overlapped, both could wait indefinitely, blocking torrent
lists, statistics, and subsequent persistence while the network health route
continued responding.

## Decision

Any daemon operation that needs both locks at once must acquire the torrent
registry before the queue. Queue planning follows the same order as full and
changed-record persistence. Operations that access them separately must
release the first guard before acquiring the other lock.

Keep both guards during queue planning so pruning stale entries, reading
torrent state, and selecting eligible work use a consistent view. Persistence
continues releasing these guards before serialization and database writes.

## Consequences

- Scheduling and persistence can overlap without the registry/queue lock cycle.
- Queue order, admission limits, and persistence formats remain unchanged.
- Deterministic regression tests arrange contended registry access and verify
  full and incremental saves, scheduling, and control-plane reads complete.
- The existing network health route is not a general application-progress
  watchdog; successful network health alone does not prove torrent operations
  are responsive.

## Related Documents

- [Architecture](../architecture.md)
- [Testing](../testing.md)
- [ADR-0073: Proportional changed-record persistence](0073-proportional-changed-record-persistence.md)
