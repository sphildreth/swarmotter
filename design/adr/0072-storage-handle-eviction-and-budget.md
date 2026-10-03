# ADR-0072: Storage handle eviction and process-wide handle budget

## Status

Accepted

## Context

`StorageIo::open_file_handle` maintained a per-torrent cache of writable file
handles that was cleared wholesale when it exceeded its capacity, without
flushing evicted entries. On large or many torrents this both lost buffered
writes from the eviction path and allowed the sum of per-torrent caches to
exhaust the process descriptor budget.

## Decision

Handle eviction is per-handle LRU inside each `StorageIo`: the eviction path
flushes a dirty handle before releasing it, and an injected flush failure is
reported to the caller instead of silently dropping data. A process-wide
writable-handle budget spans all registered torrent handle sets; when the
global budget is exceeded, handle sets beyond the most recently used ones
evict their own least-recently-used handles. Retired handles are tracked so a
handle evicted while an operation still expects it is re-opened rather than
reused.

## Consequences

- Written bytes survive eviction; readback after eviction stays verified.
- Descriptor usage is bounded by the global budget regardless of library size.
- Flush failures at eviction surface as storage errors on the requesting
  operation, preserving fail-closed durability semantics.
- A small LRU bookkeeping cost is added to handle open/close paths.

## Related Documents

- `design/scaling-review-fix-ledger-2026-10-03.md` (W2)
- `design/architecture.md`
- `design/adr/0064-filesystem-aware-storage-strategy-and-state-placement.md`
