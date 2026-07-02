# ADR-0013: Peer Wire Protocol Architecture

## Status

Accepted

## Context

The v1.0.0 release requires a real TCP BitTorrent peer protocol (BEP 3): the
engine must connect to tracker-discovered peers, perform the handshake,
exchange bitfields, handle choke/unchoke, request and assemble blocks, verify
pieces by SHA-1, write them to disk, and report progress. This logic must be
testable independently of live sockets, and must route all connections through
the network containment layer.

## Decision

Implement the peer wire protocol as pure, unit-tested logic in
`swarmotter-core::peer`, plus an async framed reader over a stream obtained
from the `NetworkBinder`:

- `Handshake` encode/decode (68-byte BEP 3 form, exact info-hash validation).
- `Message` enum covering `choke`, `unchoke`, `interested`, `not interested`,
  `have`, `bitfield`, `request`, `piece`, `cancel`, and `keepalive`, with
  canonical length-prefixed framing and forward-compatible `Unknown` variant.
- `Bitfield` for peer piece availability and set/get/missing computation.
- `PieceAssembler` for accumulating out-of-order 16 KiB blocks into a
  verifiable piece.
- `block_requests`/`BLOCK_SIZE` (16 KiB) request scheduling helpers.
- `PeerReader`/`write_message`/`write_handshake` async helpers over any
  `AsyncRead`/`AsyncWrite` (used over the binder's split stream halves).

The engine (`swarmotterd::engine`) drives one peer connection at a time per
candidate, picks missing pieces the peer has, requests all blocks for a piece,
assembles, verifies by SHA-1, writes via `StorageIo`, marks the piece, and
persists fast-resume after each verified piece. Bad peers (handshake mismatch,
hash failure, disconnect) are suppressed via a bounded bad-peer set.

Peer IDs are 20 bytes with an az-style `-SW0001-` prefix, stable per daemon
instance. Concurrency is bounded (a small `max_concurrent` peer cap); queues
and channels are bounded to avoid unbounded growth.

## Consequences

- The protocol framing is fully unit-tested without sockets.
- The engine is identical in production and the local-swarm tests (both use
  real `tokio::net::TcpStream` over the binder).
- uTP/UDP peer connections will extend the binder + add a transport variant,
  not change this protocol module.
- Metadata extension (BEP 9) is intentionally deferred until basic
  `.torrent` download is complete; this ADR covers the BEP 3 wire path only.

## Related Documents

- `crates/swarmotter-core/src/peer.rs`
- `crates/swarmotterd/src/engine.rs`
- ADR-0012 (network binder)
- ADR-0014 (tracker implementation strategy)