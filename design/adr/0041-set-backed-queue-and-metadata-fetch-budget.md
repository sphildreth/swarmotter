# ADR-0041: Set-Backed Queue and Metadata Fetch Budget

## Status

Accepted

## Context

SwarmOtter must manage thousands of torrents while keeping queue operations,
bulk lifecycle changes, and active-work planning bounded. The original queue
state used ordered vectors for all membership checks. That preserved ordering
but made repeated add, remove, bypass, and lifecycle recovery operations scan
the queue repeatedly.

Magnet metadata discovery also used the same active planning cap as resolved
piece downloads. Large magnet sets could therefore either occupy the active
download budget with metadata work or, if not bounded separately, create too
many simultaneous metadata discovery tasks.

## Decision

Queue state keeps its serialized `order` and `bypass` vectors for compatibility,
but runtime queue methods maintain set-backed membership indexes for fast
duplicate suppression and batch operations. Daemon lifecycle paths use batch
queue operations for bulk removal and stale-active recovery.

The queue configuration includes a separate
`max_active_metadata_fetches` setting. Active planning applies
`max_active_downloads` to resolved piece downloads and
`max_active_metadata_fetches` to unresolved magnet metadata fetches. If one
pool is full, the planner continues scanning for work eligible for the other
pool.

## Consequences

Adding, removing, and recovering large torrent sets avoids repeated linear
membership scans. Queue ordering remains stable for API/UI compatibility.

Metadata discovery has an explicit resource budget, so large magnet imports can
progress without letting metadata fetches consume all resolved download slots or
starting unbounded discovery tasks.

The queue vectors remain public serialized fields, so code should mutate queue
state through `QueueState` methods. Direct vector edits are still tolerated for
legacy compatibility but should not be used for new hot paths.

## Related Documents

- [Scaling Implementation Plan](../scaling-implementation-plan.md)
- [Configuration](../../docs/configuration.md)
- [Architecture](../architecture.md)
- [Testing](../testing.md)
