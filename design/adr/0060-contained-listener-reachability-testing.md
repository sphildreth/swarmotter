# ADR-0060: Contained Listener Reachability Testing

## Status

Accepted

## Context

Binding a local peer listener and obtaining a router mapping do not prove that
an external peer can reach it. Operators need an explicit diagnostic they can
point at infrastructure they control, without the daemon contacting a hidden
third-party service or bypassing its contained network path.

The result is inherently advisory: a failed check can be caused by an endpoint,
firewall, or router condition and must not cause a torrent to silently change
route, stop a healthy data plane, or claim a containment failure.

## Decision

- Add an opt-in `[port_test]` section with no default endpoint. It accepts an
  operator-provided HTTP(S) endpoint, bounded timeout, and bounded cache TTL.
  The request includes the configured TCP listen port and a fixed request
  format so operators can implement the checker without content-specific
  integration.
- Use only `NetworkBinder` contained HTTP for the outbound request. A run is
  serialized, bounded by the configured timeout, and records `unknown`,
  `open`, `closed`, `error`, or `timeout` in a cacheable status object. The
  endpoint URL itself is not returned by diagnostics.
- Provide native status and explicit-run endpoints, surface the result in the
  Web UI, and map the existing Transmission `port-test` compatibility response
  to the latest truthful result. A successful router-mapping lease may request
  a forced refresh through the same runtime path.
- Treat every result other than `open` as informational. A failed/blocked test
  neither changes the network-containment gate nor starts an uncontained retry.

## Consequences

- Operators can make ingress diagnostics meaningful using their own endpoint
  while avoiding hidden outbound services and credential-bearing result URLs.
- A stale cached result is explicitly timestamped rather than presented as a
  current network fact. Users need to configure an endpoint before a manual
  run can produce a reachability result.
- Endpoint parsing, response-size limits, response validation, cache expiry,
  timeout, and blocked-binder behavior are durable API and security contracts
  that require focused tests.

## Related Documents

- [Product backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [Architecture](../architecture.md)
- [API design](../api.md)
- [Network containment design](../vpn-network-containment.md)
- [ADR-0051: Explicit Network Path and Live Containment Gate](0051-explicit-network-path-and-live-containment-gate.md)
- [ADR-0059: Contained Opt-In Router Port Mapping](0059-contained-opt-in-router-port-mapping.md)
