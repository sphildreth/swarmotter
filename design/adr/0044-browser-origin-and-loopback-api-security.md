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
- Every browser-reachable control route (`/api/v1`, `/transmission/rpc`, and
  `/api/v2`) uses the same origin guard. A request carrying `Origin` must
  provide exactly one valid UTF-8 serialized origin whose authority matches the
  single valid `Host` authority. The origin may not contain user information,
  a path, query, or fragment. Scheme comparison is intentionally omitted for
  TLS-terminating reverse proxies.
- `Sec-Fetch-Site` is an allowlist: only exactly one `same-origin` or `none`
  value, or an absent header, can continue. `cross-site`, `same-site`, unknown,
  duplicated, and invalid-byte values are rejected. This policy also covers
  native WebSocket and SSE handshakes.
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

## Implementation note (Phase 3)

The Origin/Host/Fetch Metadata validation is extracted into one shared
`browser_origin_guard` middleware applied to every browser-reachable control
route — `/api/v1`, `/transmission/rpc`, and `/api/v2` — as the outermost layer
so it runs before authentication/session middleware and before
compatibility-enabled checks. Authentication mode changes credential checks
only, never browser-origin checks. The guard reads every Origin,
`Sec-Fetch-Site`, and (when an Origin is present) Host field with `get_all` so
duplicate field lines and invalid UTF-8 cannot collapse into an accepted value.
It rejects foreign, malformed, opaque/`null`, path/query/fragment/userinfo, and
multi-value origins. Only `Sec-Fetch-Site: same-origin`, `none`, or absence is
accepted. When Origin and `Sec-Fetch-Site` are both absent, the request
continues as a non-browser client to normal authentication.

All rejections use HTTP 403 while retaining the surface contract: native routes
return the native JSON error envelope, Transmission returns its JSON error
object, and qBittorrent returns its plain-text `Forbidden` response. See the
real-router matrix in `crates/swarmotter-api/tests/origin_matrix.rs`.

## Related Documents

- [API authentication](../../docs/api.md#authentication-and-limits)
- [Deployment guide](../../docs/deployment.md#reverse-proxy)
- [Configuration guide](../../docs/configuration.md)
- [ADR-0022](0022-api-auth-and-contained-resolution-hardening.md)
