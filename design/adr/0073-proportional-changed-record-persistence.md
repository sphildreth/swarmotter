# ADR-0073: Proportional changed-record persistence

## Status

Accepted

## Context

Progress reconciliation periodically persists live engine counters into the
durable library state. The previous path serialized and rewrote every torrent
record on every save, so a one-torrent progress tick cost O(library) work and
I/O.

## Decision

Progress-sample persistence uses fingerprint-based changed-record saves: each
torrent record carries a durable fingerprint (identity, progress, counters,
policy, and bookkeeping inputs); only records whose fingerprint changed since
the last persisted generation are serialized and rewritten, inside one
`IMMEDIATE` transaction with `synchronous = FULL`. Full saves remain
mandatory for lifecycle transactions (add, remove, import, move, rename,
paired multi-record operations) so durable state can never resurrect a removed
torrent or lose a queue entry. Fingerprints are adopted only after the save
commits, so a failed save is retried on the next tick. The queue projection
has its own fingerprint and is skipped when unchanged.

## Consequences

- Persisted-progress cost is proportional to changes, not library size.
- Removal semantics depend on full saves; the changed-record path never
  deletes durable rows.
- A one-record tick validates only that record's structural consistency.
- SQLite generations gain the incremental path; legacy JSON state migrates
  through a full save first.

## Related Documents

- `design/scaling-review-fix-ledger-2026-10-03.md` (W3)
- `design/adr/0067-sqlite-durable-library-state.md`
- `design/architecture.md`
