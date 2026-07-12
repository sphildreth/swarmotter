# ADR-0049: Configured Unauthenticated LAN Control Plane

## Status

Accepted

## Context

SwarmOtter serves its Web UI and native API from one control-plane listener.
Before v1.3, operators could set `api.require_auth = false` while binding that
listener to a trusted LAN address. ADR-0044 changed this into a startup error
for every non-loopback bind and made the Web UI request the raw API token. That
broke existing configurations and made a valid configuration field misleading.

The Web UI runs in the remote browser, not inside the daemon. Giving it a token
automatically would also give that token to every client able to load the UI,
which is equivalent to unauthenticated access. SwarmOtter must therefore make
the network-trust and token-authentication modes explicit rather than claiming
that an automatically privileged Web UI is authenticated.

## Decision

- `api.require_auth` remains the authority for API and Web UI authentication.
  `false` is valid for loopback and non-loopback listeners.
- A non-loopback listener with authentication disabled emits a prominent
  startup warning that every reachable client can control SwarmOtter.
- One outer same-origin guard protects `/api/v1`, `/transmission/rpc`, and
  `/api/v2` in both authentication modes, before native authentication,
  compatibility authentication/session negotiation, compatibility-enabled
  checks, extraction, or daemon operations. An Origin is accepted only as one
  valid UTF-8 `scheme://authority` value matching the single Host authority;
  user information, paths, queries, fragments, opaque/`null`, duplicate, and
  invalid values are rejected. `Sec-Fetch-Site` permits only `same-origin`,
  `none`, or absence. Scheme is not compared so a TLS-terminating proxy remains
  supported when it preserves the public Host.
- A syntactically valid `chrome-extension://<32-character a-p extension ID>`
  Origin is the only deliberate cross-origin integration. The outer guard
  permits it only when `api.require_auth = true` and the request supplies the
  valid configured API token. It still requires one valid Host and permitted
  Fetch Metadata; Chromium's privileged extension service-worker request uses
  `Sec-Fetch-Site: none`. Auth-disabled mode rejects every extension Origin,
  even when a token value remains configured. Ordinary foreign HTTP(S) Origins
  remain rejected even with a token.
- The built-in Web UI continues to use the public API without a privileged
  internal channel. It requests a token only when the API returns `401`.
- Container examples continue to enable authentication by default. Operators
  may deliberately set `SWARMOTTER_API_REQUIRE_AUTH=false` for a trusted LAN.
- Environment overrides are applied before final validation so deployment
  configuration can supply or change authentication fields atomically.
- Release images expose a config-only validation command so the Compose updater
  can reject incompatible configuration before stopping a healthy stack.

## Consequences

Existing trusted-LAN deployments can upgrade without being forced into a token
prompt or rejected at startup. Authenticated remote access remains the secure
default and the recommended choice for networks that are not fully trusted.

Origin checks reduce browser cross-site request risk but are not authentication
and do not protect an unauthenticated listener from clients that can reach it
directly. Operators choosing unauthenticated LAN access accept that boundary.
Chrome extension access deliberately composes the origin classification with
API authentication and therefore is unavailable on that unauthenticated
boundary.

The shared middleware preserves each API surface's rejection format at HTTP
403: the native envelope, the Transmission JSON error object, and qBittorrent's
plain-text `Forbidden`. The real-router matrix exercises every mutation and
streaming route in both authentication modes and proves a rejected request
causes no daemon call or state change.

## Related Documents

- [ADR-0022](0022-api-auth-and-contained-resolution-hardening.md)
- [ADR-0044](0044-browser-origin-and-loopback-api-security.md)
- [API documentation](../../docs/api.md#authentication-and-limits)
- [Configuration guide](../../docs/configuration.md#api)
- [Deployment guide](../../docs/deployment.md#lan-web-ui-with-contained-torrents)
