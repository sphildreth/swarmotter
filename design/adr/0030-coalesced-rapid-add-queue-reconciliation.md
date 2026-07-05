# ADR-0030: Coalesced Rapid Add Queue Reconciliation

## Status

Accepted

## Context

API clients may add large groups of torrents by issuing many individual
`/api/v1/torrents` requests in quick succession. Add requests must acknowledge
registration promptly and must not wait for torrent engine startup, peer
connections, tracker announces, DHT, PEX, metadata fetching, storage layout
creation, or queue scheduling work.

The daemon already had asynchronous queue reconciliation, but a reconciler task
could run immediately between closely spaced add requests. That preserved
correctness, but it made rapid add bursts more likely to interleave registration
with engine startup work.

## Decision

Torrent add operations register the torrent, insert the info hash into queue
order, and return. Queue reconciliation remains asynchronous. The daemon keeps a
single scheduled reconciliation marker and coalesces additional add requests
that arrive while reconciliation is pending.

Before a scheduled queue reconciliation starts, the daemon applies a short
debounce window. Dirty state accumulated before the pass is absorbed into that
single reconciliation. If more changes arrive while reconciliation is running,
the daemon runs another pass before clearing the scheduled marker.

## Consequences

Clients can send large bursts of individual add requests without each request
waiting for engine startup. Queue order remains deterministic because each add
still inserts into `QueueState` synchronously before the API response.

Engine startup may happen shortly after registration instead of immediately on
the first add in a burst. This is intentional: registration responsiveness and
bounded startup churn are preferred over starting work between each add request.

## Related Documents

- [API docs](../../docs/api.md)
- [API design notes](../api.md)
- [Runtime task model](0016-task-runtime-model-for-live-engine.md)
