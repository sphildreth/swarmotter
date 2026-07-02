# ADR-0023: Interface-Bound Containment for Dynamic Addresses

## Status

Accepted

## Context

Strict containment originally required a configured source IPv4/IPv6 address or
network namespace because source-bound sockets were the only enforced socket
path. That is safe for static VPN addresses, but it is brittle for DHCP or SLAAC
interfaces such as `br0`, where addresses can change without the operator
changing intent.

Operators need to express "bind torrent traffic to this interface and all of
its current addresses" without pinning transient addresses in configuration.

## Decision

- Treat `network.required_interface` as an enforceable strict containment path
  on platforms where the daemon can bind sockets to the device.
- On Linux, `ContainedBinder` applies `SO_BINDTODEVICE` to torrent data-plane
  TCP, UDP, and inbound listener sockets when `required_interface` is set.
- `OsInterfaceProbe` enumerates Linux interfaces with `getifaddrs` so strict
  health checks can verify that the named interface exists, is up, and has a
  usable address.
- Source address fields remain supported. When configured, they narrow binding
  to the matching address family. When only `required_interface` is configured,
  the kernel selects the current IPv4/IPv6 source address on that interface.
- The binder enforces address families independently. IPv4 traffic requires an
  IPv4 source, interface bind, or namespace. IPv6 traffic requires an IPv6
  source, interface bind, or namespace.
- On platforms where device binding is unavailable, strict interface-only
  configs fail closed at socket creation rather than falling back to the
  default route.

## Consequences

- DHCP/SLAAC interfaces can be used safely with:
  `required_interface = "br0"` and no fixed source IPs.
- Linux deployments may need the privileges required by `SO_BINDTODEVICE`.
  If the process lacks them, torrent traffic is blocked instead of leaking.
- `required_source_ipv4` and `required_source_ipv6` are now optional refinements
  rather than the only way to make strict mode enforceable.
- DNS containment remains separate. Hostname resolution is still blocked in
  strict fail-closed mode unless DNS containment can be validated or provided by
  the current network namespace. ADR-0024 records Linux interface DNS
  validation for the common systemd-resolved and static resolver-route cases.

## Related Documents

- `crates/swarmotter-core/src/net/config.rs`
- `crates/swarmotter-core/src/net/probe.rs`
- `crates/swarmotterd/src/netbinder.rs`
- `design/configuration.md`
- `design/vpn-network-containment.md`
- ADR-0005 (strict VPN/NIC network containment)
- ADR-0012 (network binder centralized containment)
- ADR-0022 (API auth and contained resolution hardening)
