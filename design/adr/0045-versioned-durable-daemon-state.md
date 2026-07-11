# ADR-0045: Versioned Durable Daemon State

## Status

Accepted

## Context

The daemon previously kept the torrent registry and queue only in memory. A
normal restart therefore lost torrent lifecycle state, file selection,
per-torrent settings, labels, and queue order even when payload and fast-resume
files remained on disk. Treating those files as the registry would also be
unsafe because they do not contain the complete control-plane state and can be
stale or corrupt.

## Decision

Persist the torrent registry and `QueueState` in a versioned JSON state file
owned by `swarmotterd`.

- The path is selected by `--state-file` or `SWARMOTTER_STATE_FILE`, then by
  the packaged state directory, XDG state directory, home state directory, or
  a working-directory fallback.
- Every state replacement uses a unique mode-`0600` temporary file, flushes
  file contents, renames it over the destination, and flushes the parent
  directory.
- Mutating lifecycle, queue, file-selection, tracker, label, and settings
  operations persist the resulting state. Live progress reconciliation also
  checkpoints state without weakening torrent task ownership.
- Startup rejects corrupt or unsupported state instead of silently starting
  an empty library.
- Startup revalidates metainfo invariants, piece bitfields, derived file
  records, magnet identity, and pairwise active/completed storage ownership
  before installing any restored record.
- Restored runtime-only states are normalized. A torrent recorded as complete
  is fully rechecked against its completed payload before it can seed; missing
  or changed data does not inherit a trusted-complete state.

The configuration file remains the source of daemon configuration and secrets.
The state file contains torrent and queue state, not API credentials.

## Consequences

- Graceful restarts preserve the library, queue order, file choices, and
  per-torrent controls.
- Abrupt termination leaves either the previous complete state file or the new
  complete state file, rather than a partially written document.
- State schema changes require an explicit version and migration or a clear
  startup error.
- A syntactically valid state document cannot bypass torrent parser or storage
  ownership constraints.
- Restoring completed torrents performs disk I/O before exposing them as valid
  seed sources, favoring correctness over optimistic startup.

## Related Documents

- `../architecture.md`
- `../testing.md`
- `../../docs/configuration.md`
- `../../crates/swarmotterd/src/state_store.rs`
- ADR-0011 (bencode and fast-resume format)
- ADR-0016 (task runtime model)
