# ADR-0042: Scheduler Diagnostics Resource Snapshot

## Status

Accepted

## Context

SwarmOtter must make large-library behavior observable enough for operators
and tests to distinguish a slow swarm from an internal resource cap. Previous
global stats exposed aggregate transfer rates and active counts, but they did
not show how many torrents requested scheduler capacity, how many were granted,
which configured limits were in effect, or whether peer-worker capacity was
saturated.

Without this visibility, troubleshooting 100-torrent and larger imports
requires inferring scheduler behavior from per-torrent state transitions and
logs. That is not sufficient for a performance-first daemon that is intended to
handle thousands of managed torrents.

## Decision

`GET /api/v1/stats` includes an additive `scheduler` object derived from the
current daemon queue, registry, retry-backoff map, running engine handles, and
configuration snapshot.

The scheduler snapshot reports managed and queued torrent counts, running
engine counts, requested and granted download and metadata-fetch slots,
retry-backoff pressure, configured active download/metadata/seed limits,
peer-worker global and per-torrent limits, effective peer-worker budget, active
peer workers, and saturation booleans for download slots, metadata fetch slots,
and peer-worker budget.

ADR-0053 adds `peer_limit`, `peer_permits_in_use`,
`peer_permits_available`, and `peer_sessions_denied` from the runtime-owned
process pool. These are authoritative for process-wide peer-session
enforcement. The older peer-worker limit/budget/saturation fields remain
compatibility telemetry for engine scheduling pressure and must not be
interpreted as connection grants.

The snapshot is diagnostic. It does not replace the queue planner or engine
resource acquisition paths. Future scheduler work may move active downloads,
metadata fetches, tracker announces, connection attempts, and peer workers to a
central resource owner, but the compatibility contract for `/api/v1/stats`
starts with this additive resource snapshot.

## Consequences

Large-library operators can tell whether progress is constrained by configured
download slots, metadata-fetch slots, retry backoff, or peer-worker capacity.
Regression tests can assert scheduler intent directly instead of relying only
on derived torrent states.

The stats response grows with additional fields, but the change is additive and
uses the existing `/api/v1` stats envelope. Clients that ignore unknown fields
remain compatible.

Because diagnostics are generated from live daemon state, future changes to the
resource scheduler must keep these fields coherent with the actual grants made
by the scheduler.

## Related Documents

- [API Design Notes](../api.md)
- [API Documentation](../../docs/api.md)
- [Scaling Implementation Plan](../scaling-implementation-plan.md)
- [ADR-0041: Set-Backed Queue and Metadata Fetch Budget](0041-set-backed-queue-and-metadata-fetch-budget.md)
- [ADR-0053: Process-Wide Peer Session Permit Pool](0053-process-wide-peer-session-permit-pool.md)
