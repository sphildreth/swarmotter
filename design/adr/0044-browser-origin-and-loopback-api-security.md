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
- A Chrome Manifest V3 extension service worker is a deliberate authenticated
  API client, not a same-origin Web UI. One syntactically valid
  `chrome-extension://<extension-id>` Origin may bypass only the Origin/Host
  authority comparison when `api.require_auth = true` and the same outer guard
  validates the configured API token from `Authorization: Bearer` or
  `X-SwarmOtter-Auth`. Chrome extension IDs are exactly 32 lowercase characters
  from `a` through `p`, and extension Origins cannot include a port or suffix.
  The request still requires one valid Host and permitted Fetch Metadata.
- Auth-disabled mode never accepts an extension Origin, even if `auth_token`
  happens to be populated. Foreign HTTP(S) origins remain rejected even when
  they present a valid token. There is no implicit all-extension allowlist.
- Authenticated reverse proxies remain supported when they preserve the public
  `Host`; TLS termination may change the scheme without changing the authority.
- Embedded Web UI responses set a self-only content security policy, disable
  framing, disable MIME sniffing, and suppress referrer data. Theme bootstrap
  code is served as a static script so the policy does not require inline script.

## Consequences

Unauthenticated LAN listeners are rejected during configuration validation;
operators must enable token authentication before binding outside loopback.
Ordinary cross-origin browser integrations are intentionally rejected, while
same-origin Web UI requests and non-browser automation continue to work.
Authenticated Chrome extension service workers are the sole cross-origin
exception. Reverse proxies must preserve `Host` and should terminate TLS before
forwarding to the daemon.

## Implementation note (Phase 3)

The Origin/Host/Fetch Metadata validation is extracted into one shared
`browser_origin_guard` middleware applied to every browser-reachable control
route — `/api/v1`, `/transmission/rpc`, and `/api/v2` — as the outermost layer
so it runs before surface authentication/session middleware and before
compatibility-enabled checks. The guard reads every Origin,
`Sec-Fetch-Site`, and (when an Origin is present) Host field with `get_all` so
duplicate field lines and invalid UTF-8 cannot collapse into an accepted value.
It rejects foreign, malformed, opaque/`null`, path/query/fragment/userinfo, and
multi-value origins. Only `Sec-Fetch-Site: same-origin`, `none`, or absence is
accepted. When Origin and `Sec-Fetch-Site` are both absent, the request
continues as a non-browser client to normal authentication.

For a valid Chrome extension Origin, the outer guard reads current daemon
configuration and validates authenticated mode plus one unambiguous API-token
header before body extraction or any surface handler. Chromium emits
`Sec-Fetch-Site: none` for a privileged non-webby initiator with host access;
the route matrix uses that production shape. A missing/invalid token or
auth-disabled mode returns 403 before mutation. Native responses use
`extension_origin_forbidden` with configuration guidance; Transmission retains
its JSON error object and qBittorrent retains plain-text `Forbidden`.

All rejections use HTTP 403 while retaining the surface contract: native routes
return the native JSON error envelope, Transmission returns its JSON error
object, and qBittorrent returns its plain-text `Forbidden` response. See the
real-router matrix in `crates/swarmotter-api/tests/origin_matrix.rs`.

## Related Documents

- [API authentication](../../docs/api.md#authentication-and-limits)
- [Deployment guide](../../docs/deployment.md#reverse-proxy)
- [Configuration guide](../../docs/configuration.md)
- [Chrome extension cross-origin requests](https://developer.chrome.com/docs/extensions/develop/concepts/network-requests)
- [Chromium Fetch Metadata generation](https://chromium.googlesource.com/chromium/src/+/HEAD/services/network/sec_header_helpers.cc)
- [Chromium extension ID format](https://chromium.googlesource.com/chromium/src/+/HEAD/extensions/common/extension_id.h)
- [ADR-0022](0022-api-auth-and-contained-resolution-hardening.md)
