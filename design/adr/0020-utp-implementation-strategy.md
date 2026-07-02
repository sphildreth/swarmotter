# ADR-0020: uTP (BEP 29) Implementation Strategy and Scope

## Status

Accepted

## Context

SwarmOtter v1.0.0 requires uTP (the BitTorrent reliable UDP transport, BEP 29)
support where practical. uTP is a reliable, congestion-controlled byte stream
layered over UDP. A full implementation requires the LEDBAT delay-based
congestion-control algorithm, selective ACK (SACK), the full connection
lifecycle (SYN/STATE/DATA/FIN/RESET), one-way delay measurement via timestamp
echo, retransmission, and selection between TCP and uTP peer transports. All
uTP traffic is UDP and must go through the network containment layer; it must
fail closed when the configured path is unavailable and must never silently
fall back to the default route.

## Decision

Implement production-grade uTP entirely on top of the `NetworkBinder`'s
contained UDP socket. The implementation lives in
`swarmotter-core::utp` and is split into focused modules:

- `header`: BEP 29 20-byte packet header encode/decode, packet types
  (DATA/FIN/STATE/RESET/SYN), the first-extension nibble, and the wrapping
  microsecond timestamp helper.
- `sack`: Selective ACK extension (BEP 29 extension 1) encode/decode, built
  from the held out-of-order sequence set relative to the cumulative ack.
- `congestion`: LEDBAT-style delay-based congestion control — base/current
  delay tracking, queuing-delay target, congestion-window growth/shrink,
  slow start, loss/retransmit response with RTO backoff, and a bounded window
  to prevent unbounded growth.
- `stream`: `UtpStream`, an `AsyncRead`+`AsyncWrite` byte-stream adapter that
  runs the connection's drive loop in a background task over the contained UDP
  socket, plus `PeerTransport`/`PeerDuplex`/`connect_peer_stream` so the engine
  opens a transport-agnostic peer stream (TCP or uTP) through the binder.
- `mod` (`UtpConnection`): the live connection — SYN/STATE handshake (initiator
  `connect` and responder `accept_from_syn`), connection-id assignment and
  validation, in-order receive reassembly with out-of-order hold and SACK
  recovery, duplicate suppression, cumulative + selective ACK, retransmission
  of timed-out in-flight packets, bounded send/receive buffers, idle timeout,
  graceful close (FIN with `fin_transmitted` tracking so a connection only
  reports closed once its own FIN has actually been sent), and RESET teardown.

The live transport runs over `NetworkBinder::udp_socket()`, the same contained
UDP socket used by UDP trackers and DHT. The engine selects TCP and/or uTP per
config (`torrent.utp_enabled`, `torrent.utp_prefer_tcp`), tries the preferred
transport first, and falls back to the other when it is available and the
preferred fails. The uTP byte stream is used with `tokio::io::split` and the
existing peer wire protocol machinery (`PeerReader`, `write_message`), so the
BitTorrent handshake and message exchange are identical over TCP and uTP.
Private torrents, rate limiting, endgame, and fail-closed containment apply
unchanged to the uTP path.

## Consequences

- uTP traffic cannot bypass containment: it uses the binder's contained UDP
  socket, which fail-closes (`CoreError::NetworkBlocked`) when the path is
  unavailable. A `BlockedBinder` proves uTP connect and uTP swarm downloads are
  blocked.
- The peer protocol is transport-agnostic: the same code path serves TCP and
  uTP, and uTP can complete a real local-swarm download from a generated
  payload through a contained uTP-capable seed peer, with SHA-1 piece
  verification and final file-content checks.
- LEDBAT keeps uTP low-priority relative to competing TCP and bounds the send
  window, so uTP cannot saturate the contained path or grow unbounded.
- TCP remains the default-preferred transport (`utp_prefer_tcp = true`) for
  broad compatibility; uTP is enabled by default and selectable via config.
- The engine now terminates gracefully after a bounded number of consecutive
  no-peer announce rounds when a torrent has no trackers, no seed peers, and
  no DHT result, instead of looping forever (a correctness and testability
  improvement surfaced while wiring uTP).

## Related Documents

- `crates/swarmotter-core/src/utp/` (`mod.rs`, `header.rs`, `sack.rs`,
  `congestion.rs`, `stream.rs`)
- `crates/swarmotterd/src/engine.rs` (transport selection)
- `crates/swarmotterd/tests/local_swarm.rs` (uTP swarm download + fail-closed)
- `design/vpn-network-containment.md`
- `design/configuration.md`
- ADR-0012 (network binder)
- ADR-0013 (peer wire protocol)