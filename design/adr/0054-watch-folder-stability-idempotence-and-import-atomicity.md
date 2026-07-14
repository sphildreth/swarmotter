# ADR-0054: Watch-Folder Stability, Idempotence, and Import Atomicity

## Status

Accepted

## Context

The watch scanner previously walked directories synchronously on an async
runtime worker, followed directory symlinks, returned filesystem-dependent
ordering, and attempted every `.torrent` file on every scan. A file still being
copied could be parsed, a successful `leave` import repeated indefinitely, and
duplicates were treated as failures. Archive/failure moves could overwrite an
existing destination and discarded action errors.

Watch and API file additions also used separate registry/queue mutation paths.
The watch path scheduled work before durable persistence and did not roll back
on a state-write error. The API path released its mutation lock before queue
insertion and persistence. Either path could expose ghost registry/queue state
or post events for an add that was not durable.

## Decision

- The complete watch-root walk runs in `tokio::task::spawn_blocking`. Every root
  and child uses `symlink_metadata`; a symlink root is an incomplete-scan error,
  child symlinks are skipped, and symlinked directories are never traversed.
  Recursive and non-recursive results are sorted by normalized relative path.
- Each configured folder computes its own scan exclusions. An `archive_dir` or
  `failure_dir` that is a strict lexical descendant of that folder's normalized
  root excludes that destination path and its entire subtree from that scan.
  The comparison uses component-aware `Path::starts_with`, never a string
  prefix or `canonicalize`. The exclusion is not global: a separately
  configured overlapping root still scans the path according to its own
  settings. A whitespace-only watch/action path is invalid, and an action
  destination that normalizes exactly to its watch root is rejected because it
  cannot be both a destination and a meaningful input boundary.
- Observations are in-memory only and keyed by the composite of a lexically
  normalized absolute configured root and normalized root-relative path.
  `canonicalize` is forbidden because it follows symlinks. The observation
  records length, modified timestamp, consecutive stable-scan count, and last
  processed `(length, modified)` fingerprint. The fingerprint detects changes;
  it is not a content-security hash.
- First sighting records one stable scan. A metadata change resets the count to
  one and clears processed state. Two consecutive identical scans are eligible.
  A processed fingerprint is skipped until metadata changes. A successful
  complete root scan removes observations for missing paths; incomplete scans
  retain them. Removing a configured root removes its observations. No watch
  observation or history ledger is persisted, so restart begins with a fresh
  first sighting.
- One runtime scan mutex spans the complete configured-folder scan. The
  background loop and manual scan endpoint cannot concurrently process the same
  eligible fingerprint. Status scans are read-only blocking walks: they never
  advance stability and count unseen, changed, stabilizing, and transient-retry
  files as pending while excluding unchanged processed files.
- Eligible files use a bounded `MAX_TORRENT_METADATA_BYTES + 1` read. The path
  and opened file must remain regular non-symlinks with the expected length and
  modification time before/open/after the read. Stable oversize input is a
  permanent malformed-torrent result before input-sized allocation. Any typed
  metadata change discards bytes, resets stability to one, and emits no history,
  event, or post action.
- API, magnet, and watch additions share one daemon mutation primitive. The
  existing storage-ownership lock is held continuously through duplicate
  determination, storage/containment/path preflight, exact hash-specific
  registry and queue snapshots, in-memory insertion, and durable state
  persistence. A persistence error restores the exact registry and queue
  snapshots before unlocking and returns the original error. Runtime resource
  creation, queue reconciliation, `torrent_added`, and stats events occur only
  after persistence succeeds.
- Watch duplicates are successful `duplicate` outcomes using the existing hash.
  They do not change the existing torrent, queue position/bypass, labels,
  download path, or settings, and they execute the configured success action.
  Native API compatibility is unchanged: duplicate API adds still return the
  established `duplicate_torrent` error envelope.
- Only `Bencode`, `MalformedTorrent`, `InvalidInfoHash`, and `Parse` are
  permanent input failures. A permanent failure executes the configured
  failure action and marks the fingerprint processed. All other errors are
  transient: the source remains, the fingerprint stays unprocessed, and a
  later stable scan retries it.
- Success and failure destinations are created when absent and are opened with
  create-new semantics before streaming copy and source removal. Existing
  destinations are never overwritten. Delete/copy/flush/remove/destination
  failures preserve the primary outcome, populate `post_action_error`, and mark
  the fingerprint processed so operator intervention is not repeated. `leave`
  also marks successful/duplicate fingerprints processed.
- `ImportResult` retains compatibility fields and adds stable `outcome` values
  (`imported`, `duplicate`, `permanent_failure`, `transient_failure`) plus
  `post_action_error`. History is insertion ordered, in-memory only, and capped
  at 10,000 by evicting the oldest entry. Watch imported/failed events carry
  path, outcome, success, duplicate, hash, primary error, and post-action error.
  Unstable observations emit no terminal event. The Web UI renders outcome
  separately and warns when a post action needs operator attention.

## Consequences

- A `.torrent` file costs at least two unchanged scans before parsing. This
  intentionally favors correctness over immediate pickup and restarts the
  observation process after daemon restart.
- `leave` is safe and idempotent within one run, while replacing the file with a
  new metadata fingerprint intentionally creates one new attempt.
- Recursive roots do not rediscover files moved into their own configured
  archive/failure descendants. Overlapping watch entries remain independent,
  so an archive/failure path can still be an explicit watch root when desired.
- Transient failures remain visible in history/events and retry on later scans;
  permanent failures and post-action errors do not create infinite loops.
- Archive/failure actions are non-overwriting but are not a cross-filesystem
  atomic rename. A process/host crash during create-new copy plus source removal
  can leave both the source and a partial destination. The destination is never
  overwritten on recovery; the next action reports a collision and leaves both
  files for manual resolution.
- The operational observation/history state is bounded and disposable. Durable
  truth remains the torrent registry/queue state, written by the same atomic add
  transaction used by the API.
- Future watch features must preserve no-symlink traversal, complete-scan
  pruning, per-folder destination exclusion, scan serialization, typed
  changed-during-read handling, and non-overwriting actions.

## Related Documents

- [ADR-0045: Versioned Durable Daemon State](0045-versioned-durable-daemon-state.md)
- [ADR-0050: Bounded Untrusted Metainfo Parsing](0050-bounded-untrusted-metainfo-parsing.md)
- [Requirements](../requirements.md)
- [Architecture](../architecture.md)
- [Configuration](../configuration.md)
- [Testing](../testing.md)
- [Operator configuration](../../docs/configuration.md)
- [Web UI](../../docs/web-ui.md)
- [Phase review](../2026-07-12.REVIEW.md)
