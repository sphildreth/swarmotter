# ADR-0065: BEP 52 V2 and Hybrid Torrent Identity

## Status

Accepted

## Context

SwarmOtter historically modeled a torrent with one 20-byte SHA-1 `InfoHash`.
That is correct for BEP 3/v1 torrents but cannot represent a BEP 52 v2 torrent,
whose metainfo identity is the full SHA-256 digest of the exact bencoded `info`
dictionary. Treating a v2 digest as a v1 hash would make magnets, durable
state, validation, and wire behavior ambiguous or incorrect.

Hybrid metainfo has both v1 and v2 identities. It interoperates through the
validated v1 swarm while retaining its full v2 identity. A pure-v2 transfer
requires a separate SHA-256 file-tree, piece-layer, file-aligned storage, and
v2 peer-handshake path. Supporting malformed or incomplete v2 data
optimistically would risk data integrity; it also must not create a route or
socket outside the contained network path.

## Decision

- Keep `InfoHash` as the explicit 20-byte v1 SHA-1 identity. Introduce a
  distinct 32-byte v2 SHA-256 identity and a tagged torrent-identity model for
  v1, v2, hybrid, and migrated legacy records.
- Use `TorrentKey` for every library, registry, queue, API, fast-resume, and
  SQLite persistence key. It retains a full 40-character v1/hybrid-primary or
  64-character pure-v2 locator. A hybrid's v2 key is an alias of its canonical
  v1 primary record. Use the deliberately separate `PeerInfoHash` only where
  BitTorrent peer, tracker, and DHT wire formats require 20 bytes; never use a
  truncation as a durable or API identity.
- Compute identities from exact bencoded `info` bytes. Do not decode and
  re-encode before hashing, and validate all v1/v2 identity components of a
  hybrid torrent independently.
- Parse and serialize v2/hybrid magnets without coercing `btmh` into `btih`.
  Contained metadata acquisition validates the claimed complete identity before
  registering or replacing a magnet placeholder.
- Preserve exact metainfo information needed for identity validation and later
  portable export. Original input metainfo and magnet-fetched information are
  distinguishable; a reconstructed document is never represented as an
  original uploaded file.
- Run pure-v2 payload work through a separate contained data plane. It
  validates the BEP 52 file tree and piece layers, verifies SHA-256 Merkle
  roots, uses file-aligned storage I/O and v2 peer handshakes, and persists
  full-key fast-resume state. It does not pass a v2 piece through v1
  contiguous-piece arithmetic or SHA-1 verification.
- Use the v2 `PeerInfoHash` truncation for contained pure-v2 tracker and DHT
  discovery, peer handshakes, and compatible metadata exchange. Hybrid
  payloads retain their v1 compatibility swarm and its SHA-1 verification;
  both hybrid identities remain visible and resolvable.
- Keep the existing fail-closed binder as the only traffic path. Pure-v2 peer,
  tracker, DHT, metadata, and resume-related recovery work must use the same
  containment, peer-admission, and cancellation boundaries as v1 work.
- MSE/PE uses the explicit 20-byte `PeerInfoHash` at its protocol secret and
  routing boundary for both v1 and v2 sessions. Pure-v2 `required` and
  `preferred` modes therefore negotiate only over the selected contained
  transport; the full SHA-256 identity remains the registry, persistence, and
  post-handshake validation key. Ambiguous peer-wire truncations are rejected
  at registration rather than misrouting an encrypted session.

## Consequences

- Existing v1 state and API behavior remain compatible while new records carry
  unambiguous identities. Legacy 40-character records migrate without creating
  a synthetic v2 value.
- Pure-v2 and hybrid torrents are usable across native API, Web UI, watch,
  fast-resume, SQLite state, and compatibility selectors. Full v2 locators are
  never truncated at these boundaries; hybrid aliases cannot create duplicate
  library records.
- Parser, metainfo, storage, peer, tracker, DHT, metadata, resume, registry,
  API, compatibility, and local-swarm tests are required because a failure at
  any one boundary could otherwise reintroduce a lossy v1 fallback.
- No new torrent traffic path is introduced: all metadata, peer, tracker, DHT,
  and webseed traffic continues through the central fail-closed binder.

## Related Documents

- [Feature backlog](../BACKLOG.md)
- [Architecture](../architecture.md)
- [API design](../api.md)
- [Network containment](../vpn-network-containment.md)
- [Testing strategy](../testing.md)
- ADR-0011 (bencode and fast-resume format)
- ADR-0013 (peer wire protocol architecture)
- ADR-0050 (bounded untrusted metainfo parsing)
