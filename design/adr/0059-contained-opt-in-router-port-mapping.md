# ADR-0059: Contained Opt-In Router Port Mapping

## Status

Accepted

## Context

An inbound peer listener is more useful when a lawful distributor can make its
configured TCP listen port reachable through a local NAT gateway. Router port
mapping protocols require multicast discovery, UDP requests, and HTTP SOAP
requests to devices outside the daemon's control. Creating those sockets or
resolving their endpoints outside SwarmOtter's containment layer would violate
the fail-closed network contract.

Mapping also has a lifecycle that differs from a peer connection: leases must
be renewed and should be removed when the feature is disabled or the daemon
stops. A mapping error must be visible to an operator without incorrectly
claiming that the contained torrent data plane is unhealthy.

## Decision

- Add an opt-in `[port_mapping]` configuration section. It is disabled by
  default and records selected NAT-PMP/UPnP protocols, optional gateway or
  service overrides, a lease duration, and a bounded pre-expiry refresh
  interval.
- Permit enabled mapping only with strict, fail-closed containment and an
  explicit required interface. NAT-PMP gateway discovery, SSDP discovery,
  NAT-PMP UDP exchange, and UPnP SOAP requests use only the existing
  `NetworkBinder` contained UDP/TCP/HTTP paths. No mapping path may create a
  direct socket, resolve through an unconstrained resolver, or fall back to a
  default route.
- Map the configured TCP peer-listen port. The daemon attempts a mapping on
  startup, renews it before lease expiry, rechecks containment before every
  attempt, and exposes a bounded status snapshot plus lifecycle events. A
  successful lease triggers the configured reachability-test workflow when
  enabled.
- On shutdown, disable, or mapping-configuration replacement, send a
  best-effort contained lease deletion and clear the local status. Mapping
  failure is an observable, nonfatal reachability condition; it does not relax
  containment or alter the health of an otherwise valid torrent data path.

## Consequences

- Operators can opt into router mapping while retaining strict routing and
  clear evidence of the selected protocol, gateway, lease, and failure state.
- Router environments without an eligible contained interface, supported
  gateway, or successful mapping remain usable but report unavailable inbound
  reachability rather than implying an open listener.
- The implementation maintains a small background lifecycle and must continue
  to be tested against contained local router fixtures, cancellation, lease
  renewal, settings replacement, and fail-closed transitions.

## Related Documents

- [Product backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [Architecture](../architecture.md)
- [API design](../api.md)
- [Network containment design](../vpn-network-containment.md)
- [ADR-0051: Explicit Network Path and Live Containment Gate](0051-explicit-network-path-and-live-containment-gate.md)
- [ADR-0060: Contained Listener Reachability Testing](0060-contained-listener-reachability-testing.md)
