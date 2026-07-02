# Testing

This document describes SwarmOtter's testing strategy. Testing is tracked by
feature completion and acceptance criteria, not by time estimates.

## General expectations

- Add or update tests alongside feature work.
- Prefer generated local torrents and local swarm fixtures so tests do not
  depend on third-party content.
- Run `cargo fmt`, `cargo check`, and `cargo test` before considering work done.
  Fix all reported issues.

## Required test areas

### Unit tests

- Magnet parsing.
- Torrent parsing.
- Info hash handling.
- Tracker tier handling.
- Piece selection.
- Piece verification.
- Queue behavior.
- Ratio/seeding behavior.
- Bandwidth limit logic.
- Config validation.
- Network containment validation logic.
- Per-torrent health calculation: complete / network-blocked / paused /
  missing pieces with zero sources / good active swarm / many connected but
  useless peers / slow-but-completable / private torrent (no DHT/PEX
  penalty) / bar+label mapping.

### Integration tests

- Add magnet through API.
- Add torrent file through API.
- Upload torrent file through Web UI/API path.
- Import torrent from watch folder.
- Pause/resume/remove lifecycle.
- Recheck lifecycle.
- Tracker announce behavior.
- DHT peer discovery behavior.
- PEX peer discovery behavior.
- File priority behavior.
- Queue behavior.
- Settings behavior.
- WebSocket/SSE event delivery.
- Per-torrent health serialization: `TorrentSummary` and the torrent detail
  endpoint both include a `health` object with score, bars, label, and
  per-component sub-scores.

### Network containment tests

- Required interface missing.
- Required interface down.
- Source IP missing.
- Route invalid.
- Socket bind failure.
- VPN path removed while torrents are active.
- Torrent traffic blocked when fail-closed is active.
- API listener remains available when torrent data plane is blocked, if
  configured that way.

### Storage tests

- Fast resume.
- Forced recheck.
- Interrupted write recovery.
- Missing file detection.
- Partial download behavior.
- File selection behavior.
- Move complete behavior.
- Rename path behavior.

### Local swarm tests

- Tracker-based peer discovery (HTTP, compact peers): covered
- Tracker-based peer discovery (UDP/BEP 15, compact peers): covered
- Download completion: covered (generated payload, in-process seed peer)
- Direct-peer (PEX/DHT-style) discovery: covered (directly-supplied seed)
- Seeding/upload behavior: covered (inbound `Seeder` serves a completed
  download to a fresh leecher through the contained listener)
- Daemon-driven download through `DaemonOps`: covered
- Magnet metadata fetch: covered (BEP 9 ut_metadata, info-hash verified)
- DHT-based peer discovery: covered (local KRPC `get_peers` fixture)
- PEX-based peer exchange: covered (BEP 10/11, peer discovered via PEX)
- uTP (BEP 29) peer transport: covered (a contained uTP-capable seed serves a
  generated payload over the contained UDP socket; the engine completes the
  download over uTP, verifying piece hashes and final file contents; a
  fail-closed test proves the `BlockedBinder` blocks uTP swarm downloads)
- Recheck after completion: covered via `StorageIo::recheck`
- Per-torrent health during active download: an actively-downloading
  generated lawful local payload reports a non-zero health score and at
  least one bar, computed from the live engine state.

## Test data

Tests must use clearly lawful sources (generated local torrents, public-domain
files, open datasets, Linux distribution examples, project-owned sample files).
See `content-policy.md`.

## TODO

- Decide on integration test harness layout (e.g., `tests/` directories per
  crate) as implementation begins.
- Add local swarm fixture tooling.
- Keep this document aligned with `requirements.md`.