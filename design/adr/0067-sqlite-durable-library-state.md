# ADR-0067: SQLite Durable Library State

## Status

Accepted

## Context

The versioned JSON daemon-state document provided an atomic compact snapshot
for the torrent registry and queue. It is not an appropriate durable query
model for a growing library: it cannot provide indexed lifecycle state, queue
state, retained metainfo, health snapshots, current metrics, or a migration
history without growing a collection of unrelated side files.

SwarmOtter needs a local durable library foundation without introducing a
networked service, relaxing its existing rollback boundaries, or losing state
when an existing installation upgrades. The store must preserve exact source
metainfo where it exists and must distinguish it from a canonical `info`
dictionary acquired through BEP 9.

The durable key must also preserve the complete torrent identity: a v1 or
hybrid-primary record uses its 40-character SHA-1 locator, while a pure-v2
record uses its full 64-character SHA-256 locator. A 20-byte peer-wire
truncation is not a valid library or persistence key.

## Decision

- Use an embedded SQLite database, through `rusqlite` with the bundled
  public-domain SQLite amalgamation, as the primary durable daemon-state
  format. It is local-only and creates no torrent network traffic.
- Keep the configured state-file path and migrate a validated version-one JSON
  document in place on its first successful save. A migration writes and
  checkpoints a complete temporary SQLite generation before atomically
  replacing the legacy file; malformed or unsupported input is never
  overwritten.
- Version the SQLite schema with `PRAGMA user_version` and a migration ledger.
  Store lossless per-torrent control-plane records plus indexed projections for
  lifecycle/queue operations, retained raw metainfo, health snapshots, current
  metrics, rolling metric history, and library/audit history. Historical SQL
  column names such as `info_hash` remain for compatibility, but store only
  canonical full `TorrentKey` locators (40 characters for v1/hybrid-primary,
  64 for pure v2). Schema changes require an explicit migration or an explicit
  startup error.
- Use a short-lived connection on the daemon's existing serialized state-write
  lane. Enable foreign keys, WAL, full synchronous durability, and explicit
  checkpoints before creating a rollback snapshot or closing a write
  generation. The main database file is mode `0600` on Unix.
- Retain canonical exact bencoded `info` bytes separately from the exact
  original full `.torrent` input. The former supports identity validation and
  magnet recovery; the latter is the only value that may later be exported as
  the original uploaded metainfo. Neither is reconstructed silently.
- Bound retained operational history by deterministic row caps: 10,000 library
  history rows, 10,000 audit rows, 512 rolling metric samples per torrent, and
  50,000 metric samples globally. Identical current metrics do not create
  duplicate history samples.
- Do not attempt an automatic database rebuild from payload or resume files.
  A corrupt or unsupported database fails startup explicitly because neither
  source is authoritative for the complete library and queue record. The
  explicit offline `--rebuild-state-projections` command is narrower: after a
  read-only integrity check and authoritative-record validation of a supported
  SQLite database, it rebuilds only derived projections and indexes in one
  transaction. It never creates a database, migrates legacy JSON, changes raw
  metainfo/audit/history, or repairs a corrupt database.
- Preserve fast-resume as a separate restart-optimization format. Restored
  library state remains subject to the existing metainfo, storage-ownership,
  and payload revalidation paths before it is trusted to seed.

## Consequences

- Operator-facing library features can build on a versioned, indexed local
  foundation instead of repeatedly parsing one state document.
- Existing deployments retain their configured state-file path while moving
  forward to SQLite; JSON remains import-compatible rather than the primary
  write format.
- A corrupted database cannot silently become an empty library. Operators need
  a normal filesystem backup of the checkpointed state-store file for recovery;
  an automated backup/restore surface remains separate future work.
- The authenticated native metainfo endpoint may export only the retained,
  byte-exact original `.torrent` representation. Magnet-fetched canonical
  `info` bytes, reconstructed data, and SQLite tables are never treated as an
  export substitute or public database API.
- Projection rebuild can recover verified indexes after an interrupted or
  damaged projection layer without broadening corruption recovery semantics;
  it is not payload or fast-resume reconstruction.
- SQLite introduces a reviewed embedded dependency and local filesystem
  recovery semantics. State writes remain blocking work off Tokio workers and
  must remain serialized through the daemon's state-write boundary.
- This ADR supersedes ADR-0045's choice of JSON as the primary durable format
  while retaining its crash-safety, validation, and recovery requirements.

## Related Documents

- [Feature backlog](../BACKLOG.md)
- [Architecture](../architecture.md)
- [Configuration design](../configuration.md)
- [Testing strategy](../testing.md)
- [Third-party licenses](../../THIRD_PARTY_LICENSES.md)
- ADR-0045 (versioned durable daemon state)
- ADR-0050 (bounded untrusted metainfo parsing)
- ADR-0065 (BEP 52 v2 and hybrid torrent identity)
