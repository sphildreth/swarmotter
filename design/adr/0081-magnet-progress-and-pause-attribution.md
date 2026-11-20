# ADR-0081: Magnet progress recovery and pause attribution

## Status

Accepted

## Context

Resolving a magnet establishes its file layout before storage has verified any
payload. A progress sample taken between those operations could replace a
previously verified map with an empty byte string. Validating an entire
library save then made other torrents depend on that sample. Operators also
could not tell which operation had paused a torrent after a restart.

## Decision

Publish resolved metadata and its piece geometry together after the daemon
has accepted the metadata. Treat the initial zero map as provisional until
storage has supplied authoritative progress. Initialize the registry map once
when replacing a placeholder and validate the count, byte length and padding
before applying subsequent engine samples.

On a save, discard an invalid piece update and recover the saved map for that
torrent if its key, metadata phase and piece count match. Recompute file byte
counts from those bits. With no compatible map, start with zero verified
pieces. A torrent whose selection can no longer support its claimed completion
enters a storage error and can be rechecked. Keep other changes, queue state,
and the recovered map in the same SQLite transaction. A database read or
commit failure still fails the operation.

Record the operation responsible for entry into the paused state. Include its
reason in native summaries and durable audit events. A repeated pause preserves
the existing reason. Records created before attribution remain readable.

## Consequences

Metadata previews and restarts preserve verified progress. Recovery reads the
affected SQLite rows only when a snapshot has an invalid map. A successful
recheck can establish newer progress. Pause reasons identify the operation,
and older pauses with no recorded reason appear as unknown.

## Related Documents

- [Changed-record persistence](0073-proportional-changed-record-persistence.md)
- [Library and container recovery](0080-library-and-container-recovery.md)
- [API reference](../../docs/api.md)
