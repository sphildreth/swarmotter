# ADR-0043: Cached Storage I/O Flush Boundaries

## Status

Accepted

## Context

SwarmOtter must handle large active torrent sets without making every block I/O
pay the cost of opening a payload file, seeking, writing or reading, flushing,
and closing the handle. The previous storage path did that work for each
touched file slice, including a flush after every block write. At high
concurrency this creates unnecessary file table churn and forces unrelated
torrents to compete with per-block storage waits.

Tokio file writes can also remain pending while a file handle stays open, so
removing every write-side flush requires explicit consistency boundaries before
verification, seeding reads, moves, or deletion.

## Decision

`StorageIo` keeps a per-torrent cache of open file handles keyed by torrent file
index. Block and piece writes reuse the cached writable handle for each touched
file and no longer flush after every block write. Read paths flush pending
writable cached handles for the file slices they are about to read, so piece
verification and seeding reads observe completed writes without making every
write wait. Move and remove operations flush all cached writable handles before
clearing the cache and touching filesystem paths.

Cached read-only handles may be replaced by writable handles when the torrent
later writes the same file. The cache is scoped to the `StorageIo` clone set for
that torrent.

## Consequences

Steady-state downloads and seeding avoid repeated open/seek/close cycles and
avoid per-block write flushing. This reduces cross-torrent storage contention
for large active sets.

Pending writes are guaranteed visible to SwarmOtter verification and read paths
at their read boundary, not after every individual block write. Independent
filesystem readers may observe the last flushed state until SwarmOtter reaches a
read, verification, move, or removal boundary.

The cache increases open file descriptors according to the number of payload
files touched by active torrents. If future workloads show descriptor pressure,
the cache should grow an eviction policy or a configurable open-handle budget
without returning to per-block open/close behavior.

## Related Documents

- [Architecture](../architecture.md)
- [Testing](../testing.md)
- [Changelog](../../CHANGELOG.md)
