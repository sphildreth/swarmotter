# ADR-0012: Network Binder — Centralized Containment for Live Sockets

## Status

Accepted

## Context

SwarmOtter's non-negotiable rule is that all torrent-related traffic must go
through the configured network path and must fail closed if that path is
unavailable. Until the live engine was implemented, the network layer
(`swarmotter-core::net`) only modeled containment health and validated
configuration; there was no live socket creation point. The engine now needs
to open real peer TCP connections and tracker HTTP connections, so there must
be exactly one choke point that performs source binding and fail-closed
enforcement — never letting torrent traffic fall back to the default route.

## Decision

Introduce a `NetworkBinder` trait in `swarmotter-core::net::binder` as the
single abstraction through which the engine obtains torrent data-plane
connections:

- `connect_peer(addr)` opens a TCP stream bound to the configured source
  address/interface.
- `http_get(url)` issues tracker (and webseed/metadata) HTTP GETs through the
  same contained path.
- `udp_socket()` returns a contained, source-bound UDP datagram socket
  (`ContainedUdpSocket` trait) for UDP trackers, DHT, and uTP.
- `bind_peer_listener(port)` returns a contained, source-bound inbound TCP
  listener (`PeerListener` trait) for seeding/upload.
- `traffic_allowed()` reports the current containment gate so the engine can
  decide whether to start/continue peer activity.

The concrete `ContainedBinder` lives in `swarmotterd::netbinder`: it
re-evaluates containment (`net::evaluate`) before each connection and returns
`CoreError::NetworkBlocked` in strict fail-closed mode when the path is
unavailable. Peer connections are returned as concrete `tokio::net::TcpStream`
so the peer protocol code is identical for production and tests.

A `LoopbackBinder` (gated behind the `test-binder` feature) lets cross-crate
integration tests exercise the full engine over loopback without touching the
default route or real hardware. A `BlockedBinder` (test feature) models strict
fail-closed containment: every data-plane operation returns
`NetworkBlocked` and `traffic_allowed` is false, so fail-closed behavior for
TCP, UDP, and the inbound listener is provable in tests.

The trait is defined in core (the contract); the implementation lives in the
daemon (where real sockets and platform binding belong). No engine component
creates `TcpStream::connect`, `UdpSocket::bind`, or tracker HTTP clients
directly.

## Consequences

- Torrent traffic cannot silently use the default route; every socket is gated.
- Adding a new data-plane transport (uTP, DHT UDP) means extending the binder,
  not bypassing it.
- DNS resolution for tracker, UDP tracker, and DHT bootstrap hostnames is
  performed inside the binder after containment has been enforced. Strict
  fail-closed mode blocks hostname resolution when DNS containment cannot be
  validated or provided by the current network namespace.
- The control plane (API/Web UI) remains independently bound and is unaffected.
- Tests use `LoopbackBinder`, keeping the engine deterministic and offline.

## Related Documents

- `crates/swarmotter-core/src/net/binder.rs`
- `crates/swarmotterd/src/netbinder.rs`
- `design/vpn-network-containment.md`
- ADR-0005 (strict VPN/NIC network containment)
- ADR-0013 (peer protocol architecture)
