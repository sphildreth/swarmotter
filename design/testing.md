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
- Download completion: covered (generated payload, in-process seed peer)
- Direct-peer (PEX/DHT-style) discovery: covered (directly-supplied seed)
- Daemon-driven download through `DaemonOps`: covered
- Magnet metadata fetch: pending BEP 9
- DHT-based peer discovery: pending live DHT engine
- PEX-based peer exchange: pending live PEX engine
- Seeding/upload behavior: pending inbound peer listening
- Recheck after completion: covered via `StorageIo::recheck`

## Test data

Tests must use clearly lawful sources (generated local torrents, public-domain
files, open datasets, Linux distribution examples, project-owned sample files).
See `content-policy.md`.

## TODO

- Decide on integration test harness layout (e.g., `tests/` directories per
  crate) as implementation begins.
- Add local swarm fixture tooling.
- Keep this document aligned with `requirements.md`.