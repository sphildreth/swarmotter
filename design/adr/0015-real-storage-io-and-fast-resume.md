# ADR-0015: Real Storage I/O and Fast-Resume Format

## Status

Accepted

## Context

The v1.0.0 release requires real disk I/O: mapping pieces to one or more
files, writing incoming blocks at correct file offsets (including multi-file
boundary crossings), reading blocks for seeding, verifying completed pieces
by SHA-1, persisting fast-resume state, reloading it on restart, detecting
invalid resume metadata, and supporting forced recheck. The fast-resume format
was already chosen as JSON in ADR-0011; this ADR records the real I/O
implementation and resume integration.

## Decision

Implement real async storage I/O in `swarmotter-core::storage::io`:

- `StorageIo` owns the `TorrentMeta` and download directory and performs
  `tokio::fs` reads/writes. `write_block` maps a piece+offset to file slices,
  crossing file boundaries, creating parent directories as needed.
  `read_piece`/`read_block` reassemble bytes for verification and seeding.
- `verify_piece_on_disk` reads a piece and SHA-1-verifies it against metadata;
  a missing file is treated as "not yet present" rather than a hard error.
- `recheck` verifies every piece and returns a `PieceBitfield` of verified
  pieces.
- `save_resume`/`load_resume` persist and reload the JSON fast-resume file
  (`<name>.swarmotter.resume`), validating info hash and piece count; a
  mismatch is reported as a storage error.
- `remove_all`/remove paths delete data files and resume metadata safely.
- `build_resume` constructs a `FastResume` from live state with accurate
  `bytes_completed` computed from verified piece lengths.

The engine writes and verifies each piece, updates `PieceProgress`, and
persists fast-resume after every verified piece. The daemon's `recheck` action
runs `StorageIo::recheck` and reflects the result into the torrent record. A
pre-existing bug in `piece_file_ranges` (file index used the output length
instead of the actual file index) was fixed as part of this work.

## Consequences

- Storage I/O is unit-tested with real temp directories and multi-file
  boundary cases.
- Fast-resume survives daemon restart; invalid/mismatched resume is detected
  and rejected.
- Forced recheck reuses the same verification path as live downloads.
- Sparse preallocation truncates files to full length so random writes land at
  correct offsets.

## Related Documents

- `crates/swarmotter-core/src/storage/io.rs`
- `crates/swarmotter-core/src/storage/resume.rs`
- ADR-0011 (bencode and fast-resume format)