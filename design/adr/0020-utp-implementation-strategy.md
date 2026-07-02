# ADR-0020: uTP (BEP 29) Implementation Strategy and Scope

## Status

Accepted

## Context

SwarmOtter v1.0.0 requires uTP (the BitTorrent reliable UDP transport, BEP 29)
support where practical. A full uTP implementation is large: it requires the
LEDBAT delay-based congestion-control algorithm, selective ACK, the full
connection lifecycle (SYN/DATA/State/FIN/Reset), one-way delay measurement, and
selection between TCP and uTP peer transports. All uTP traffic is UDP and must
go through the network containment layer.

## Decision

Implement a binder-ready uTP architecture and the largest testable subset now,
and document exactly what remains:

- `swarmotter-core::utp` implements the uTP packet header encode/decode per
  BEP 29 (20-byte header: type/extension, version, connection id, timestamps,
  window size, seq/ack numbers), the packet types (DATA, FIN, STATE, RESET,
  SYN), connection-id assignment, and a minimal reliable session
  (`UtpSession`) with in-order send sequence, ACK generation, and in-order
  receive reassembly (with duplicate/out-of-order suppression). This is fully
  unit-tested without sockets.
- The live transport runs over the `NetworkBinder::udp_socket()` contained UDP
  socket. A local uTP echo fixture test proves a SYN/ACK/DATA exchange over the
  contained UDP path on loopback, and a fail-closed test proves the binder
  blocks uTP when the path is unavailable.

What remains for full production uTP (tracked in
`docs/v1-completion-tracker.md`):

- The LEDBAT congestion-control algorithm (delay-based) and dynamic window
  sizing instead of the fixed window used by the testable subset.
- Selective ACK (SACK) extension handling for robust out-of-order recovery.
- Full three-way connection handshake and FIN tear-down with TIME_WAIT.
- Microsecond timestamp echo and one-way delay measurement for LEDBAT.
- Integration of uTP as a peer transport selectable alongside TCP in the
  engine, with connection migration between TCP/uTP as appropriate.

## Consequences

- uTP traffic cannot bypass containment: it uses the binder's contained UDP
  socket, which fail-closes when the path is unavailable.
- The current subset can carry framed, in-order data over the contained UDP
  path, which is the architectural foundation for full uTP.
- Until the remaining work is done, peer connections continue to use TCP; the
  uTP path is present but not yet selected by the engine for live downloads.

## Related Documents

- `crates/swarmotter-core/src/utp.rs`
- `crates/swarmotterd/src/dht.rs` (uTP transport fixture tests)
- `design/vpn-network-containment.md`
- ADR-0012 (network binder)
- ADR-0013 (peer protocol)