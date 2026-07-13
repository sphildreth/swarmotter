# ADR-0062: Contained SOCKS5 TCP Proxy

## Status

Accepted

## Context

Some lawful-distribution and restricted-network deployments need an explicit
SOCKS5 egress proxy for torrent TCP and HTTP traffic. A proxy must not weaken
SwarmOtter's configured VPN/NIC/network-namespace containment, leak target DNS
through an unrelated resolver, or create a direct retry path when the proxy is
unavailable.

SOCKS5 `CONNECT` covers TCP only. Treating it as a generic proxy while allowing
uTP, DHT, or UDP tracker packets to continue directly would make the configured
proxy misleading and could introduce an unreviewed second egress policy.

## Decision

- Add an opt-in `[network.socks5]` configuration with proxy host/port and
  either SOCKS5 no-authentication or RFC 1929 username/password authentication.
  The proxy password is redacted from settings read/update responses; an
  omitted password preserves the stored credential only when the username is
  unchanged.
- Layer `Socks5Binder` on the existing contained `NetworkBinder`. The inner
  binder resolves the proxy hostname and opens the sole outbound socket to the
  proxy using the configured containment path. The proxy layer never creates a
  raw socket or a default-route client.
- Add a hostname-capable TCP connection seam. Contained HTTP(S) tracker,
  scrape, and webseed requests pass target hostnames to the SOCKS5 binder,
  which emits SOCKS domain-form `CONNECT` requests for remote DNS. IP-literal
  peer connections use SOCKS IP address forms. A proxy error never retries the
  target directly.
- Scope the initial implementation deliberately to TCP `CONNECT`. Enabling
  SOCKS5 requires `torrent.utp_enabled = false` and `dht.enabled = false`.
  The proxy binder rejects UDP socket creation and direct target hostname
  resolution, so UDP tracker URLs are rejected rather than routed directly.
  SOCKS5 UDP ASSOCIATE requires a separate containment and lifecycle decision.
- Keep NAT-PMP/UPnP router mapping on an unproxied but still contained binder:
  local multicast/gateway control cannot be represented by SOCKS5 `CONNECT`.
  This is a narrowly scoped router-control path, not a torrent peer/tracker
  fallback.

## Consequences

- Operators can combine a SOCKS5 proxy with strict interface/source/namespace
  containment; containment is still evaluated before proxy DNS and connection.
- TCP peer, HTTP(S) tracker/scrape, and webseed paths gain remote-DNS-capable
  proxy support without adding a second HTTP connector, DNS client, or socket
  implementation.
- DHT, uTP, and UDP tracker functionality is unavailable in a SOCKS5-enabled
  configuration until a future UDP ASSOCIATE design proves equivalent
  containment, authentication, cancellation, and fail-closed behavior.
- Protocol framing, authentication rejection, malformed replies, password
  redaction, proxy-host-only resolution, TCP no-fallback behavior, and blocked
  UDP behavior are durable tests and compatibility contracts.

## Related Documents

- [Product backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [Architecture](../architecture.md)
- [Network containment design](../vpn-network-containment.md)
- [Testing design](../testing.md)
- [ADR-0014: Tracker Implementation Strategy](0014-tracker-implementation-strategy.md)
- [ADR-0051: Explicit Network Path and Live Containment Gate](0051-explicit-network-path-and-live-containment-gate.md)
