# ADR-0005: Strict VPN/NIC Network Containment

## Status

Accepted

## Context

Torrent clients normally use whatever network route the operating system
provides. For deployments that route torrent traffic through a VPN interface,
specific source IP, network namespace, or configured NIC, silent fallback to
the default route would leak torrent traffic outside the intended path.

SwarmOtter's network safety depends on torrent traffic never escaping the
configured path. This is a routing-correctness and operational-safety concern,
not a piracy-evasion feature.

## Decision

All torrent-related traffic must go through the configured network path and
must fail closed if that path is unavailable.

Covered traffic includes peer TCP, peer UDP/uTP, DHT UDP, PEX-discovered peers,
UDP trackers, HTTP/HTTPS trackers, webseeds, magnet metadata fetching, and
DNS used by torrent operations. The control plane (API/Web UI) is separate
from the torrent data plane. No engine component may directly create outbound
sockets or HTTP clients without going through the network binding and
containment layer. The application must never silently fall back to the
default route.

## Consequences

- When the configured path is unavailable, torrent networking stops rather than
  leaking; existing torrent sockets are closed and new ones are blocked.
- The daemon exposes network health states and fail-closed status through the
  API and Web UI.
- Engine code is constrained: all socket creation must go through the network
  layer, increasing discipline but preventing leaks.
- Dependencies that create uncontrolled network traffic must not be used for
  torrent operations.

## Related Documents

- `AGENTS.md`
- `design/vpn-network-containment.md`
- `design/requirements.md`