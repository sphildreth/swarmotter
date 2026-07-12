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
- UDP tracker source, action, and transaction correlation.
- uTP header, connection-ID, extension-chain, and SACK handling.
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
- Bencode parser budgets and adversarial cases (ADR-0050): depth boundary
  (128 accepted, 129 rejected), node-count boundary (`MAX_BENCODE_NODES`
  accepted, one more rejected), overflowing/beyond-input string lengths,
  missing terminators, empty/leading-zero/negative-zero integer forms,
  duplicate and non-string dictionary keys, unsorted-but-unique keys accepted,
  trailing bytes rejected, and the 16 MiB metadata byte limit accepted at the
  boundary and rejected one byte over. Every malformed corpus case must be
  panic-free under `std::panic::catch_unwind`.
- Metainfo shape budgets (ADR-0050): piece length zero/over-limit, mismatched
  piece count, too many files, too many pieces, and total-length overflow all
  produce typed `MalformedTorrent` errors.
- Durable piece-hash decode: SHA-1 hashes of 0, 19, 20, and 21 decoded bytes;
  only 20 succeeds, with the piece index in the error message and no payload
  data or content paths.
- Production metadata ingress bounds: `.torrent` bodies at and one byte over
  `MAX_TORRENT_METADATA_BYTES` through the core parser, API add, watch import,
  and BEP 9 metadata assembly.

### Integration tests

- Add magnet through API.
- Add torrent file through API.
- Upload torrent file through Web UI/API path.
- Reject cross-origin native API mutations and WebSocket handshakes while
  preserving same-origin browser requests and non-browser API clients.
- Validate Web UI static security headers and required operation wiring.
- Import torrent from watch folder.
- Pause/resume/remove lifecycle.
- Recheck lifecycle.
- Tracker announce behavior.
- DHT peer discovery behavior.
- PEX peer discovery behavior.
- File priority behavior.
- Queue behavior.
- Settings behavior.
- Concurrent atomic configuration replacement.
- Durable torrent and queue restoration after daemon reconstruction.
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
- Same-size changed-file detection and corrupt-resume quarantine.
- Forced recheck.
- Interrupted write recovery.
- Missing file detection.
- Partial download behavior.
- File selection behavior.
- Cross-torrent storage path collision rejection.
- Move complete behavior.
- Rename path behavior.

### Local swarm tests

- Tracker-based peer discovery (HTTP, compact peers): covered
- Tracker-based peer discovery (UDP/BEP 15, compact peers): covered
- Download completion: covered (generated payload, in-process seed peer)
- Direct-peer (PEX/DHT-style) discovery: covered (directly-supplied seed)
- Seeding/upload behavior: covered (the shared inbound `SeederHub` routes
  multiple completed torrents through one contained listener and owns accepted
  sessions until completion or cancellation)
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

### Scale tests

- Queue data-structure tests cover 10,000-entry add/remove/reorder behavior.
- Daemon lifecycle tests cover 1,000- and 10,000-record stale-active recovery,
  metadata retry backoff, desired active cap enforcement, and bulk removal.
- API integration tests cover 1,000-torrent rapid add, bulk add, and
  query/filter/group behavior with generated lawful magnets.
- Ignored opt-in scale tests cover larger synthetic flows:
  `ignored_thousand_mixed_state_torrents_keep_scheduler_bounds` validates a
  1,200-record daemon library across queued, checking, downloading metadata,
  downloading, seeding, paused, completed, and error states while asserting
  scheduler request/grant bounds.
  `ignored_scale_harness_add_query_retry_remove_reset_2000_torrents` validates
  a 2,000-torrent API add/query/recheck/reannounce/remove/reset flow using
  generated lawful torrent files.

Run ignored scale tests explicitly when validating large-library behavior:

```bash
cargo test -p swarmotterd ignored_thousand_mixed_state_torrents_keep_scheduler_bounds -- --ignored
cargo test -p swarmotter-api --test scale_harness -- --ignored
```

## Test data

Tests must use clearly lawful sources (generated local torrents, public-domain
files, open datasets, Linux distribution examples, project-owned sample files).
See `content-policy.md`.

## TODO

- Keep this document aligned with `requirements.md`.
