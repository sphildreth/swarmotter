# ADR-0022: API Auth and Contained Resolution Hardening

## Status

Accepted

## Context

SwarmOtter separates the control plane (API/Web UI) from the torrent data
plane, but both surfaces need fail-closed behavior where a configured security
property would otherwise be silently bypassed.

The API already exposed configuration fields for authentication, but routes did
not enforce them consistently and settings responses could expose the configured
token. The torrent data plane also needed to ensure hostname resolution for
trackers, UDP trackers, and DHT bootstrap nodes happened only after the
`NetworkBinder` had enforced containment. In strict fail-closed mode,
interface-only configuration is not enough for source-bound socket creation,
so it must be rejected unless a source address or current network namespace is
configured.

## Decision

- Enforce API authentication on all `/api/v1` routes when
  `api.require_auth = true`.
- Require `api.auth_token` to be present when authentication is enabled.
- Accept the token through `Authorization: Bearer <token>` or
  `X-SwarmOtter-Auth: <token>`.
- Redact `api.auth_token` from `GET /api/v1/settings`.
- Add `api.max_request_body_bytes` and apply it as the API request body limit.
- Require strict fail-closed network configs to include an enforceable torrent
  socket path: a required source IPv4/IPv6 address or a required network
  namespace. Interface-only strict configuration is rejected because the daemon
  cannot enforce it through socket binding alone.
- Add `NetworkBinder::resolve_host()` and route tracker, UDP tracker, and DHT
  bootstrap hostname resolution through it. Hostnames are blocked in strict
  fail-closed mode when DNS containment cannot be validated or provided by the
  current network namespace.
- Bound tracker HTTP response reads so a tracker cannot force unbounded memory
  growth.

## Consequences

- API deployments can opt into token authentication without relying on a
  reverse proxy to enforce the configured setting.
- Settings clients can inspect runtime configuration without receiving the
  configured bearer token.
- Large or malicious API and tracker responses are rejected before unbounded
  allocation.
- Strict containment starts from an enforceable socket-binding model; invalid
  interface-only strict configs fail during validation instead of appearing
  protected.
- Data-plane hostname resolution remains centralized in the binder, which makes
  new torrent networking features easier to audit for containment compliance.

## Related Documents

- `crates/swarmotter-api/src/routes.rs`
- `crates/swarmotter-api/src/handlers/settings.rs`
- `crates/swarmotter-core/src/config.rs`
- `crates/swarmotter-core/src/net/binder.rs`
- `crates/swarmotter-core/src/net/config.rs`
- `crates/swarmotterd/src/netbinder.rs`
- `crates/swarmotterd/src/dht.rs`
- `design/api.md`
- `design/configuration.md`
- `design/vpn-network-containment.md`
- ADR-0005 (strict VPN/NIC network containment)
- ADR-0012 (network binder centralized containment)
