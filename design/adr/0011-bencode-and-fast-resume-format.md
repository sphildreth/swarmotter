# ADR-0011: Bencode Implementation and Fast-Resume Format

## Status

Accepted

## Context

SwarmOtter must parse `.torrent` metadata (BEP 3 bencode) and compute the
info hash over the exact original `info` dictionary bytes. Depending on an
unmaintained bencode crate (`serde_bencode 0.1` pulls yanked `syn 0.10`
transitive dependencies) introduced build fragility and license review burden.
Separately, SwarmOtter needs a fast-resume persistence format that is durable,
debuggable, and versionable.

## Decision

- SwarmOtter implements its own minimal bencode decoder/encoder in
  `swarmotter-core::bencode`. This removes the unmaintained dependency, gives
  full control over raw `info` byte extraction for exact info-hash
  computation, and keeps the dependency footprint minimal.
- The bencode implementation supports the subset needed for `.torrent`
  metadata (byte strings, integers, lists, dicts) and canonical encoding for
  test fixtures.
- Fast-resume metadata is persisted as pretty-printed JSON
  (`.swarmotter.resume`) rather than bencode. JSON is human-readable and
  debuggable, decoupled from the wire protocol, and easy to version with a
  schema. It records the info hash, piece bitfield, byte accounting, file
  priorities, and download directory.

## Consequences

- No unmaintained bencode dependency in the build.
- The bencode module is a maintained project asset and must stay correct for
  metadata parsing; it is unit-tested directly.
- Fast-resume JSON is larger than a binary format but far easier to inspect
  and recover; this is acceptable for a homelab/server daemon.
- A future binary fast-resume format would require an ADR superseding this one.

## Related Documents

- `crates/swarmotter-core/src/bencode.rs`
- `crates/swarmotter-core/src/meta.rs`
- `crates/swarmotter-core/src/storage/resume.rs`
- ADR-0009 (foundational dependency stack)