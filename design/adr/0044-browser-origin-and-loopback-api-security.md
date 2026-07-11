# ADR-0044: Browser Origin and Loopback API Security

## Status

Superseded by [ADR-0049](0049-configured-unauthenticated-lan-control-plane.md)

## Context

The native API includes state-changing routes that browsers can reach with
simple cross-origin requests. Token authentication protects deployments that
enable it, but unauthenticated loopback deployments also need protection from
cross-site request forgery and DNS-rebinding-style Host changes. Browser
WebSocket handshakes likewise carry an `Origin` header and must not be accepted
from unrelated sites. CLI and automation clients generally do not send browser
origin metadata and must remain supported.

## Decision

- Unauthenticated API/Web UI listeners are valid only on loopback addresses.
- Native `/api/v1` requests carrying browser origin metadata must use an
  `Origin` authority that matches the request `Host` authority.
- Browser requests reported as `cross-site` or `same-site` by Fetch Metadata are
  rejected. This policy also covers native WebSocket handshakes.
- Unauthenticated browser requests additionally require a loopback `Host`,
  preventing an attacker-controlled hostname from reaching a loopback daemon.
- API clients without `Origin` or Fetch Metadata headers remain supported.
- Authenticated reverse proxies remain supported when they preserve the public
  `Host`; TLS termination may change the scheme without changing the authority.
- Embedded Web UI responses set a self-only content security policy, disable
  framing, disable MIME sniffing, and suppress referrer data. Theme bootstrap
  code is served as a static script so the policy does not require inline script.

## Consequences

Unauthenticated LAN listeners are rejected during configuration validation;
operators must enable token authentication before binding outside loopback.
Cross-origin browser integrations are intentionally rejected, while same-origin
Web UI requests and non-browser automation continue to work. Reverse proxies
must preserve `Host` and should terminate TLS before forwarding to the daemon.

## Related Documents

- [API authentication](../../docs/api.md#authentication-and-limits)
- [Deployment guide](../../docs/deployment.md#reverse-proxy)
- [Configuration guide](../../docs/configuration.md)
- [ADR-0022](0022-api-auth-and-contained-resolution-hardening.md)
