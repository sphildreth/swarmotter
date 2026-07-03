# ADR-0027: Confirmed Download Reset

## Status

Accepted

## Context

Operators need a single control-plane action that returns a test or local
daemon instance to a clean download state. Manually removing every torrent,
payload file, incomplete file, and log file is error-prone, especially while
download and seeding tasks may still be running.

The operation is destructive. It can remove lawful user payload data and should
not be exposed as an accidental one-click action.

## Decision

Add `POST /api/v1/reset` as an authenticated daemon operation and expose it in
the Web UI Settings view behind an explicit confirmation dialog.

The daemon reset operation:

- Stops active torrent engines and seeders before deleting data.
- Removes all torrent records, queue entries, live engine state, seeder state,
  and rate samples.
- Removes registered torrent payload and fast-resume files from per-torrent
  override directories.
- Deletes the contents of configured `storage.download_dir` and
  `storage.incomplete_dir`, preserving those configured root directories.
- Truncates the active daemon log file instead of unlinking it, so the running
  logger keeps writing to the configured path.
- Returns a structured summary containing removed torrent count, affected
  storage paths, storage entry count, log paths, and log file count.

Clients must present this operation as destructive and require explicit
confirmation before calling the API.

## Consequences

The reset workflow is predictable for local testing and operational cleanup
without requiring users to know every configured storage path.

Preserving storage roots avoids recreating the delete-data bug class where a
configured root directory disappears after cleanup. Truncating logs, rather than
unlinking them, avoids losing subsequent log records from the still-running
process.

The operation intentionally does not change configuration, watch-folder
configuration, network containment state, authentication state, or API/Web UI
availability.

## Related Documents

- [API design](../api.md)
- [Web UI documentation](../../docs/web-ui.md)
- [Changelog](../../CHANGELOG.md)
