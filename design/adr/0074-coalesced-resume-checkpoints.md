# ADR-0074: Coalesced resume checkpoints

## Status

Accepted

## Context

The engine's serial download path persisted a resume checkpoint after every
verified piece. For fast swarms this made resume-checkpoint I/O the dominant
write path and amplified journal contention.

## Decision

Resume checkpoints are written when a dirty-generation threshold is reached:
at least 64 newly verified pieces since the last checkpoint, or 5 seconds
since the first dirty piece after the last checkpoint, whichever comes first.
Lifecycle boundaries (engine stop, completion, pause, removal, and storage
recheck paths) force a checkpoint and reset the dirty state. A piece verified
while a checkpoint is being written remains dirty and is covered by the next
checkpoint, so no verified piece is ever lost from resume state.

## Consequences

- Resume-checkpoint write cost is bounded regardless of piece rate.
- A crash loses at most the coalescing window of verified pieces, which the
  next startup recheck re-derives.
- Checkpoint writes are forced at durability-relevant boundaries.

## Related Documents

- `design/scaling-review-fix-ledger-2026-10-03.md` (W4)
- `design/adr/0067-sqlite-durable-library-state.md`
