# ADR-0064: Filesystem-Aware Storage Strategy and State Placement

## Status

Accepted

## Context

ADR-0037 and ADR-0056 provide free-space preflight and root-scoped local work
budgets, but operators could not see mount characteristics or deliberately
place frequent durable writes. Btrfs, local disks, network mounts, containers,
and constrained system volumes have different operational trade-offs for
payload writes, fast-resume metadata, daemon state, logs, and temporary
fallback storage.

Any filesystem optimization must preserve piece verification, atomic durable
state updates, and safe recovery. In particular, a CoW request must not silently
fall back to a different policy or alter an existing payload file.

## Decision

- Extend `GET /api/v1/storage/roots` diagnostics with best-effort mount point,
  mount options, mount source, filesystem type, and actual rolling payload-write
  and verification-read throughput. Platform or container environments that do
  not expose mount metadata return `null` rather than failing diagnostics.
- Add `[storage]` placement controls:
  `resume_dir` for fast-resume metadata, `state_dir` as the default location
  for the durable daemon state file, and `temp_dir` as the fallback payload
  root when no download directory is configured. Existing `logging.file_path`
  remains the deliberate file-log placement control. Explicit `--state-file`
  and `SWARMOTTER_STATE_FILE` take precedence over `storage.state_dir`.
- Resume metadata in a dedicated `resume_dir` is named by info hash, not
  display name. Payload data is never moved merely because resume placement is
  configured. Atomic state and resume replacement temporary files remain
  siblings of their targets so cross-filesystem scratch placement cannot break
  atomic rename or durability.
- Add `storage.cow_strategy`. Its default, `conservative`, does not alter
  filesystem flags. The explicit `disable_for_new_files` strategy is supported
  only for newly created files on Linux Btrfs and fails before payload writes
  on an unsupported filesystem, platform, or permission boundary. Existing
  files are never modified; an existing file without NOCOW is rejected for
  further writes rather than silently changing or bypassing the strategy.
- CoW changes, sparse behavior, and preallocation remain independent explicit
  choices. Piece hashes and normal rechecks remain the authority for payload
  integrity; no optimization bypasses them.
- Changing `storage.state_dir` through full settings replacement is reported
  as restart-required and keeps the running state file in place. Changing
  `storage.resume_dir` is rejected while incomplete or selected-file resume
  state exists. Changing a fallback `temp_dir` is rejected while a torrent
  still depends on that fallback root.

## Consequences

- Operators can correlate local storage pressure with mount behavior and
  actual disk work rather than only peer/network rate estimates.
- High-write metadata can be placed independently of payload disks and OS
  state volumes without changing torrent data paths.
- Btrfs users can explicitly choose NOCOW for new payload files, accepting the
  documented trade-off that filesystem data checksums/compression/snapshot
  behavior and fragmentation characteristics differ. Conservative mode stays
  the safe default for all filesystems.
- State and resume placement changes require deliberate lifecycle handling;
  the daemon never treats a new setting as permission to orphan or relocate
  durable files in a running process.
- No new dependency or network path is introduced. Storage metrics and mount
  inspection are local, best-effort observability only.

## Related Documents

- [ADR-0037: Disk-Aware Storage Diagnostics and Add-Time Preflight](0037-disk-aware-storage-optimizer-preflight.md)
- [ADR-0043: Cached Storage I/O Flush Boundaries](0043-cached-storage-io-flush-boundaries.md)
- [ADR-0045: Versioned Durable Daemon State](0045-versioned-durable-daemon-state.md)
- [ADR-0056: Storage-Root Resource Controls](0056-storage-root-resource-controls.md)
- [Configuration design](../configuration.md)
- [API design](../api.md)
- [Testing strategy](../testing.md)
