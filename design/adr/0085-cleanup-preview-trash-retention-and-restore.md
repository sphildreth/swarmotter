# ADR-0085: Cleanup Preview, Trash, Retention, and Restore

## Status

Accepted

## Context

SwarmOtter could remove a torrent with or without deleting payload data, but an
operator could not inspect exact paths before a destructive cleanup, quarantine
payloads for recovery, or express retention policy by profile, label, and state.
Delete behavior is one of the easiest ways to lose data, especially with shared
payloads, changed file layouts, stale partials, symlinks, or a source set that
changes between preview and execution.

Ordinary torrent removal and its keep-data behavior must remain intelligible.
Trash must not silently overwrite files, and a failed/interrupted move must be
inspectable without pretending a complete trash entry exists.

## Decision

- Add a disabled-by-default `[trash]` configuration with a local `path` and
  explicit retention rules. `path` must not overlap a configured payload or
  storage root. Each retention rule selects by an AND of optional profile,
  case-insensitive label, lifecycle state set, and minimum age from completion
  (or add time when no completion exists). Rules select candidates only; no
  background process deletes data.
- Add authenticated cleanup preview and apply endpoints. Preview performs no
  filesystem, queue, metainfo, or retention mutation. Apply recomputes the
  selection and revalidates ownership, configured roots, path existence,
  symlink status, and shared-payload conflicts before moving anything.
- Report exact paths, path role (complete/active/resume), existence, size,
  intended action, owning torrent, applicable retention rule, qualification
  time/reason, and blockers per item. The API and Web UI expose the same
  explainable decisions.
- Support whole-torrent cleanup and stale-partial cleanup. Stale detection
  covers files marked unwanted in current metainfo and orphan regular files
  under a multi-file torrent's top-level directory that are not in the current
  ownership set. Symlinks and special entries are reported/blocked, never
  followed.
- Store ready entries under `<trash>/entries/<id>` and in-progress moves under
  `<trash>/.staging/<id>`. A ready entry contains `entry.json`, the serialized
  torrent record, optional exact original metainfo, and role-specific payload
  trees. Staging entries are listed as `incomplete` and can be inspected or
  purged but not restored as complete entries.
- Move payloads with the shared exclusive/no-overwrite storage move path. A
  same-filesystem hard-link move is used where possible; cross-device copy uses
  create-new semantics and removes the source only after a complete sync.
  Rollback restores already-moved paths when a later move fails.
- Whole-torrent trash captures the live torrent/queue snapshot before removal.
  It stops engine/seeder work, writes the manifest, moves data, removes the
  durable record, and persists the removal. A persistence failure restores the
  record and moves data back. Stale-partial cleanup leaves the torrent record
  live and moves only the selected stale files.
- Restore supports only a `fail` collision policy. Every destination is
  preflighted; an existing destination aborts that entry without overwriting or
  discarding the trash copy. Ready whole-torrent entries restore payloads first,
  then re-register through the normal durable add transaction and restore the
  saved queue positions. Payload-only entries restore their listed files.
- Purging is explicit and irreversible. Routine cleanup/pruning never removes
  unrelated data, existing destination files, completed trash entries, or
  verified completion evidence. Re-running the same selection does not create a
  duplicate ready entry because the live record/paths no longer satisfy it.
- Trash is local-only and never creates torrent network traffic. All API routes
  use the native authenticated control plane.

## Consequences

- Operators can preview exact cleanup effects, quarantine whole records or
  stale partial files, restore them without overwriting, and deliberately purge
  entries.
- Shared payloads, symlinks, changed layouts, missing paths, restore conflicts,
  and interrupted staging are explicit outcomes rather than silent deletion.
- Trash uses additional local disk space. Operators must monitor the trash root
  and purge entries deliberately; the daemon does not auto-delete trash.
- A crash during a move can leave a `.staging/<id>` directory. It is visible as
  an incomplete entry; recovery is manual inspection/restore or purge.
- Future cleanup changes must preserve dry-run isolation, execution-time
  revalidation, no-overwrite moves, staging visibility, fail-only restore
  collisions, per-item reporting, and the distinction between live removal and
  recoverable trash.

## Related Documents

- [ADR-0027: Confirmed Download Reset](0027-confirmed-download-reset.md)
- [ADR-0056: Storage Root Resource Controls](0056-storage-root-resource-controls.md)
- [ADR-0064: Filesystem-Aware Storage Strategy and State Placement](0064-filesystem-aware-storage-strategy-and-state-placement.md)
- [ADR-0067: SQLite Durable Library State](0067-sqlite-durable-library-state.md)
- [Feature backlog](../BACKLOG.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Configuration design](../configuration.md)
- [Testing strategy](../testing.md)
- [Operator API](../../docs/api.md)
- [Operator configuration](../../docs/configuration.md)
- [Web UI](../../docs/web-ui.md)
- [Troubleshooting](../../docs/troubleshooting.md)
