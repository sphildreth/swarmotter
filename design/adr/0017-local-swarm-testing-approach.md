# ADR-0017: Local Swarm Testing Approach

## Status

Accepted

## Context

SwarmOtter must verify real download behavior without depending on
third-party, copyrighted, or questionable content (`design/testing.md`,
`design/content-policy.md`). Required test areas include local swarm tests:
tracker-based peer discovery, download completion, seeding, and recheck.
These need a repeatable, offline harness driven entirely by generated test
data.

## Decision

Implement local swarm integration tests in `swarmotterd/tests/` using only
generated payloads:

- `local_swarm.rs` spins up an in-process TCP seed peer (minimal BEP 3 seeder
  that handshakes, sends a full bitfield, unchokes, and serves requested
  blocks) and an in-process HTTP tracker returning a compact peer list. The
  SwarmOtter `TorrentEngine` downloads via the `LoopbackBinder` (contained
  loopback path), verifying every piece and persisting fast-resume. One test
  discovers the seed through the tracker; another supplies the seed directly
  (the PEX/DHT/local peer path).
- `daemon_download.rs` exercises the daemon end to end through the
  API-facing `DaemonOps`: add a torrent, observe completion via the summary,
  verify on-disk content, pause, and remove+delete.
- All traffic stays on loopback through the `NetworkBinder` abstraction; no
  default-route traffic and no external content. Peer ids are 20 bytes with an
  az-style prefix.

This gives real end-to-end coverage of announce → peer discovery → handshake →
piece download → verification → disk write → fast-resume → completion, with the
same code path the daemon uses in production.

## Consequences

- Local swarm tests run offline and deterministically, satisfying the lawful
  test-data requirement.
- The harness validates both tracker-based and direct-peer discovery paths.
- Adding DHT/PEX/uTP testing later means adding a fixture to this harness, not
  touching third-party content.
- The seed peer is a minimal test asset, not a general-purpose client; it is
  maintained alongside the tests.

## Related Documents

- `crates/swarmotterd/tests/local_swarm.rs`
- `crates/swarmotterd/tests/daemon_download.rs`
- `design/testing.md`
- ADR-0008 (lawful use and no piracy-oriented features)
- ADR-0013 (peer protocol)