# ADR-0048: File Selection Drives Piece Scheduling

## Status

Accepted

## Context

The API exposed wanted flags and file priorities, but model-only updates do not
change which pieces the engine requests or which files storage preallocates.
Multi-file torrents also have boundary pieces that overlap wanted and unwanted
files, so treating files as independent download units would produce invalid
piece hashes.

## Decision

Translate file selection into a per-piece scheduling map when an engine starts.

- A piece is selected when it overlaps at least one wanted file whose priority
  is not `unwanted`.
- The piece priority is the highest priority among its selected overlapping
  files. Serial, parallel, endgame, and webseed scheduling all use the same
  selection map.
- Boundary pieces are downloaded and verified in full. Storage writes only the
  byte slices described by the torrent layout, including unavoidable boundary
  bytes in adjacent files.
- Initial storage layout preallocates wanted files only. Unwanted files can
  still receive boundary bytes required to verify a selected piece.
- A torrent may finish its current selection without possessing every torrent
  piece. Only a full piece set is promoted to the completed storage root and
  eligible for complete-payload seeding.
- Wanted and priority changes persist to fast resume and durable daemon state,
  then restart active scheduling so the new selection takes effect.

Move-data and rename-path operations are real storage transactions. The daemon
stops active work, performs the filesystem operation with rollback where
possible, updates metadata only after success, and reconciles the torrent.
Incomplete, sparse, and intentionally absent files retain their actual shape.
Destination creation is exclusive, and a durable-state write failure rolls
the filesystem and in-memory path back before the operation returns an error.

## Consequences

- File controls affect network and disk work instead of only API presentation.
- Piece boundaries can create small amounts of data in an otherwise unwanted
  file; this is required for BitTorrent piece verification.
- Selected-file completion and full-payload completion are distinct states for
  storage promotion and seeding decisions.
- All piece acquisition paths must apply the same selection and priority rules.

## Related Documents

- `../architecture.md`
- `../testing.md`
- `../../docs/web-ui.md`
- `../../crates/swarmotterd/src/engine.rs`
- `../../crates/swarmotter-core/src/storage/io.rs`
- ADR-0015 (real storage I/O and fast resume)
- ADR-0045 (versioned durable daemon state)
