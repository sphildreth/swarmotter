# ADR-0037: Disk-Aware Storage Diagnostics and Add-Time Preflight

## Status

Accepted

## Context

The v1.1.0 P0 disk-aware storage optimizer phase requires a minimal, safe
storage foundation before broader queue and CoW optimizations are implemented.
Operators need two things in this phase:

- visibility into storage-root health, and
- prevention of writes when free space is below an explicit reserve.

Without these, adds can fail late in the workflow and expose users to avoidable
storage outages or surprises on constrained roots.

This phase exposes:

- `GET /api/v1/storage/roots` for storage diagnostics, and
- storage reserve settings under `[storage]`:
  `minimum_free_space_bytes` and `minimum_free_space_percent`,
  with add/start-time preflight that rejects new torrents before any payload
  data write.

This ADR covers only that scoped v1.1.0 phase slice.

## Decision

1. Introduce a storage diagnostics endpoint in the native API:
   `GET /api/v1/storage/roots`.

   The endpoint returns control-plane visibility for each storage root used by
   torrent storage, including path, roles, existence/readiness, free/available
   bytes, total bytes, filesystem identity where available, configured reserve
   status, mapped torrent counts, active torrent counts, active write rate, and
   warnings.

   It does not alter daemon behavior or perform torrent traffic.

2. Add storage reserve configuration in the `[storage]` config table:

- `minimum_free_space_bytes` (default: `0`)
- `minimum_free_space_percent` (default: `0`)

3. Add free-space preflight in torrent intake and engine start:

- resolve the target storage root before any payload write or engine registration,
- check configured reserve against currently available space,
- reject the add request when reserve is violated.
- for magnets, repeat preflight after BEP 9 metadata resolution because the
  real payload size is unknown at add time.

4. On preflight failure, return a storage-specific API error that prevents
   partial registration and gives a clear message suitable for UI surfacing.

## Consequences

- Operators can diagnose storage pressure and filesystem class through
   `/api/v1/storage/roots` without changing existing torrent workflow.
- Add operations fail fast under low-space conditions, avoiding partial
  write-path behavior and making the behavior deterministic.
- This phase intentionally does not define CoW policy or state-directory
  routing. Root-level active-work, write-pressure, and recheck controls were
  subsequently decided in ADR-0056.

## Related Documents

- [Product backlog](../BACKLOG.md)
- [API reference](../../docs/api.md)
- [Configuration reference](../../docs/configuration.md)
- [API design notes](../api.md)
- [ADR-0036: Large-Library Torrent Query API](0036-large-library-torrent-query-api.md)
- [ADR-0056: Storage-Root Resource Controls](0056-storage-root-resource-controls.md)
