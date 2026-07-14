# VPN/NIC Network Containment

This document defines SwarmOtter's network containment requirements. It is a
core `v1.0.0` requirement (see ADR-0005 and `requirements.md`).

This feature is documented as routing correctness, privacy-preserving network
design, operational safety, container networking, and fail-closed behavior. It
is **not** a piracy-evasion feature. See `content-policy.md` for prohibited
wording.

## Requirement

All torrent-related traffic must be forced through a configured network path,
such as a VPN interface, source IP address, network namespace, container
network stack, or explicitly configured NIC.

## Traffic covered

Network containment applies to **all** torrent-related traffic, including:

- Peer TCP connections.
- Peer UDP/uTP traffic.
- DHT UDP traffic.
- PEX-discovered peer connections.
- UDP tracker announces.
- HTTP tracker announces.
- HTTPS tracker announces.
- Webseed HTTP/HTTPS traffic.
- Magnet metadata fetching.
- DNS resolution used for torrent, tracker, peer, and webseed operations.

## Control plane vs data plane

The control API and Web UI are separate from torrent data traffic. The API/Web
UI may bind to localhost, a LAN address, or a reverse proxy listener. Torrent
data traffic binds separately to the configured VPN/NIC path. Exposing the Web
UI or API on LAN must not allow torrent peer, tracker, DHT, or webseed traffic
to use the LAN/default network path.

## Fail-closed behavior

The application must fail closed and never silently fall back to the default
route.

If strict network containment is enabled and the configured network path is
unavailable, torrent networking must stop. Fail-closed conditions include:

- Required interface does not exist.
- Required interface exists but is down.
- Required interface has no usable IP address.
- Required source IP is no longer assigned.
- Strict fail-closed configuration lacks an enforceable interface, source
  address, or current network namespace.
- Required route is missing or invalid.
- VPN network namespace is unavailable.
- DNS behavior cannot be constrained as configured.
- Socket binding fails.

When a fail-closed condition occurs:

- The process-wide containment gate must block before teardown begins.
- Existing torrent network sockets must be closed.
- New torrent network connections must be blocked.
- Torrents enter a clear network-blocked state.
- The API must report the network containment failure.
- The Web UI must show the network containment failure.
- Logs must clearly identify the failed requirement.

Each top-level data-plane task captures the gate generation and races normal
work against cancellation. Every block advances that generation, including a
more-specific report while already blocked. Waiter registration is
wakeup-safe, and a block followed immediately by recovery still cancels tasks
created under the old generation; no connected stream may survive a blocked
interval.

The daemon persists recovery intent only for work that was demonstrably live at
the containment edge. Recovery consumes that durable intent once. Paused,
queued, ratio/idle-stopped, completed-without-a-live-seeder, and stale
`network_blocked` records do not start merely because the path becomes healthy.

Socket/source/listener bind errors block the gate synchronously before their
health report is processed. `socket_bind_failed` and generic
`blocked_fail_closed` reports are latched: a later healthy interface probe is
insufficient to reopen traffic. Only an explicit full configuration replacement
that validates the peer-listener bind and, outside SOCKS5 TCP-only mode, an
ephemeral contained UDP bind may clear the latch. Failed validation preserves
the old configuration and blocked state.

## Network health states

Required states include `healthy`, `disabled`, `interface_missing`,
`interface_down`, `no_interface_address`, `source_address_missing`,
`route_invalid`, `socket_bind_failed`, `dns_not_constrained`,
`network_namespace_unavailable`, and `blocked_fail_closed`.

## Acceptance criteria

- The daemon refuses to start torrent networking when strict mode is enabled and
  the required interface, source address, or network namespace is unavailable.
- The daemon blocks torrent traffic when the configured VPN/NIC path disappears
  while running.
- During a generated tracker/peer transfer, deleting the required veth produces
  `interface_missing`, leaves partial verified progress stable, moves the
  torrent to `network_blocked`, empties data-plane scheduler diagnostics, and
  leaves `/health` reachable.
- Peer, tracker, DHT, and webseed traffic cannot fall back to the default
  route.
- Hostname resolution for proxy, tracker, UDP tracker, DHT, and other torrent
  data-plane operations goes through the `NetworkBinder` after containment is
  enforced. With SOCKS5 enabled, the proxy hostname still uses that contained
  resolution path while TCP target hostnames are sent as SOCKS remote-DNS
  domain requests. DNS behavior is otherwise constrained by the current network
  path or blocked in strict fail-closed mode.
- API/Web UI traffic remains independently configurable.

## Binding abstraction

Live torrent sockets and data-plane name resolution are created exclusively
through the `NetworkBinder` trait (`swarmotter-core::net::binder`),
implemented by `ContainedBinder` in the daemon. The binder binds outbound TCP,
UDP sockets, and inbound listeners to the configured source address or
interface, re-evaluates containment before each connection, resolves hostnames
through `resolve_host()` only after containment passes, and returns
`CoreError::NetworkBlocked` in strict fail-closed mode when the path or DNS
policy is unavailable. On Linux, interface binding uses `SO_BINDTODEVICE`, so
`required_interface = "br0"` can constrain torrent sockets to all current
addresses on `br0` without pinning DHCP/SLAAC source addresses.
Hostname resolution remains fail-closed: on Linux, interface-bound mode allows
DNS only when the OS probe can tie DNS to the configured interface, such as
systemd-resolved link DNS from `resolvectl dns br0`, or when static resolver
routes go through the required interface.

`NetworkBinder::connect_host()` preserves a TCP target hostname until the
binder chooses its connection strategy. When `[network.socks5]` is enabled,
`Socks5Binder` wraps the contained binder: the inner binder resolves and opens
the sole TCP connection to the proxy, then the wrapper issues SOCKS5 `CONNECT`.
HTTP(S) tracker, scrape, and webseed hostnames use the SOCKS domain form for
remote target DNS, while known peer IP addresses use SOCKS IP-address forms.
Neither proxy lookup nor proxy failure can create a raw/default-route socket or
a direct target retry.

Tracker announce and supported HTTP/HTTPS scrape, plus webseed range GETs, use
`ContainedHttpClient` through the same binder. Every redirect repeats contained
resolution/connect, TLS is layered only over that stream, and Hyper supplies
HTTP/1 framing without a connector, resolver, pool, or socket path. Decoded
bodies are bounded before accumulation; HTTPS downgrade is rejected. UDP
scrape is explicitly unsupported and makes no HTTP or UDP call. HTTPS
(`https://`) performs TLS with system-root certificate validation over the
binder-provided TCP stream; the TLS layer never creates an independent network
path. UDP data-plane traffic (UDP tracker announce, DHT, and uTP) goes through the
binder's `udp_socket()` / `udp_socket_for()` methods, which return contained
UDP sockets for the requested address family. uTP (BEP 29) is a live peer
transport selected by the engine alongside TCP; all uTP peer traffic - SYN,
DATA, STATE, FIN, RESET, and SACK - flows through the contained UDP socket and
fail-closes when the path is unavailable (see ADR-0020). Inbound peer
connections (seeding) go through `bind_peer_listener()`, which binds contained
TCP listeners to the configured interface/source path. A `LoopbackBinder`
(test feature) lets integration tests exercise the full engine over loopback
without the default route, and a `BlockedBinder` proves fail-closed behavior
for TCP, UDP, uTP, and the listener. See ADR-0012, ADR-0022, and ADR-0023.

SOCKS5 support is deliberately TCP `CONNECT` only (ADR-0062). Configuration
validation requires DHT and uTP to be disabled when it is enabled. The wrapper
rejects every UDP socket and direct target-resolution request, so a UDP tracker
attempt is blocked rather than sent directly; SOCKS5 UDP ASSOCIATE is not an
implemented fallback. SOCKS5 does not supply inbound forwarding, so peer
listeners remain separately bound to the configured contained path. NAT-PMP and
UPnP router control likewise uses an unproxied but still contained binder for
local multicast/gateway traffic; it is not a torrent-data fallback.

The CI acceptance harness
`scripts/test-network-containment-transition.sh` creates PID-qualified daemon
and peer namespaces joined only by a veth pair, gives neither namespace a
default route, generates a lawful payload/torrent, and runs a local compact
HTTP tracker plus throttled TCP BitTorrent seed. Cargo, the fixture, API clients,
and daemon all run with the normal CI identity. Only `sudo ip` performs
namespace/link operations, and the daemon receives only `CAP_NET_RAW` for
`SO_BINDTODEVICE`; the tracker and seed receive no capabilities.

## Maintenance

Keep this document aligned with `architecture.md`, `configuration.md`, and the
accepted containment ADRs whenever the binding or DNS policy changes.
