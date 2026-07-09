# ADR-0040: Force-Clear Over-Limit Queue Rotation

## Status

Accepted

## Context

SwarmOtter is expected to handle large unattended torrent queues without active
slots becoming pinned by work that no longer belongs in the desired active set.
Queue reconciliation computes the configured active download set and demotes
active torrents outside that set back to queued state.

The previous implementation used the graceful engine stop path for those
over-limit active downloads. If an engine task was stuck in metadata discovery,
peer I/O, or another noncooperative path, queue reconciliation could wait on
that task and stop enforcing the active download cap. Retained engine
diagnostics could also make queued metadata retry work look active again if
progress reconciliation treated every retained engine state as a live engine.

## Decision

Queue reconciliation force-clears active engine tasks that are outside the
desired active download set. The force-clear path sends a stop command when a
command channel is available, aborts the task, clears runtime engine
bookkeeping, and then leaves the torrent record queued unless it is paused or
completed.

Progress reconciliation may continue copying diagnostic counters, resolved
metadata, and completed state from retained engine state. It may only promote a
torrent into `downloading` or `downloading_metadata` when a live engine handle
exists and the torrent is not under retry backoff.

## Consequences

The configured active download cap is enforced even when an engine task does
not cooperate with graceful shutdown. Large queues can continue promoting
queued work instead of waiting behind over-limit active tasks.

Abrupt queue rotation can skip engine-level graceful cleanup for the force-
cleared task. Storage and fast-resume paths therefore remain responsible for
safe restart and recheck behavior after cancellation.

Tests must distinguish live engines from retained diagnostics by creating
engine handles when a fixture expects active-state promotion. Regression tests
cover over-limit queue rotation, no-peer magnet retry state, and stale metadata
diagnostics across a 100-torrent queue.

## Related Documents

- [Architecture](../architecture.md)
- [Requirements](../requirements.md)
- [Testing](../testing.md)
