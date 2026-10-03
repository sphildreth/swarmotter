# ADR-0076: Autopilot decisions over the active set only

## Status

Accepted

## Context

`refresh_autopilot_decisions` cloned and re-analyzed every registry record on
each autopilot tick. A large paused library made each tick clone and analyze
the whole registry, competing with queue and lifecycle maintenance even though
paused, errored, and completed-without-engine records cannot produce autopilot
actions.

## Decision

Autopilot analysis covers only torrents with a running engine or an active
lifecycle state (`Downloading`, `DownloadingMetadata`, `Queued`, `Seeding`).
Records that drop out of the eligible set keep their last decision; removal
and configuration-replacement paths clear stale entries explicitly. The
on-demand per-torrent decision endpoint still recomputes for any torrent so
API responses remain exact.

## Consequences

- Autopilot tick cost is proportional to the active set, not library size.
- Guardrail bookkeeping (applied-action tracking) is unaffected because
  ineligible records cannot apply actions.
- The decision cache is a merge, not a replace, so diagnostics for recently
  stopped torrents stay stable until they are removed or re-eligible.

## Related Documents

- `design/scaling-review-fix-ledger-2026-10-03.md` (W6)
- `design/architecture.md`
