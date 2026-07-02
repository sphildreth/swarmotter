# ADR-0024: Interface-Bound DNS Validation

## Status

Accepted

## Context

Strict containment requires torrent-related DNS to stay on the configured
network path. ADR-0022 centralized hostname resolution in `NetworkBinder`, and
ADR-0023 made `required_interface` enforceable for dynamic DHCP/SLAAC
addresses. However, interface-only deployments still needed a way to resolve
tracker and DHT bootstrap hostnames without requiring a separate network
namespace.

On Linux systems using systemd-resolved, DNS can be associated with a link and
inspected with `resolvectl dns <interface>`. Static resolver configurations can
also be checked by verifying that configured resolver IP routes use the
required interface.

## Decision

SwarmOtter will treat DNS as constrained for Linux interface-bound strict mode
when the OS probe can prove one of these conditions:

- The process is in the configured network namespace.
- `/etc/resolv.conf` points to a loopback resolver and systemd-resolved reports
  DNS servers for the required interface, for example `resolvectl dns br0`.
- `/etc/resolv.conf` lists static resolver IPs and each resolver route resolves
  through the required interface.

`NetworkBinder::resolve_host()` continues to be the only torrent data-plane
hostname resolution path. In strict fail-closed mode it blocks hostname
resolution unless DNS is constrained by the probe or supplied by the current
network namespace.

The `validate_dns` setting controls proactive network-health reporting. Even
when it is false, hostname resolution remains fail-closed if DNS cannot be
proven constrained.

## Consequences

Dynamic-address interface deployments such as `required_interface = "br0"` can
use ordinary hostname trackers and DHT bootstrap nodes when Linux DNS is tied
to that interface.

Unsupported DNS setups still fail closed for torrent hostname resolution
instead of resolving through an unconstrained path.

The implementation depends on Linux OS inspection and command output for
systemd-resolved and route checks. That keeps the daemon dependency-light, but
other platform DNS mechanisms may require additional probes or a contained
network namespace.

## Related Documents

- `design/vpn-network-containment.md`
- `design/configuration.md`
- `docs/configuration.md`
- `docs/network-containment.md`
- `crates/swarmotter-core/src/net/probe.rs`
- `crates/swarmotterd/src/netbinder.rs`
