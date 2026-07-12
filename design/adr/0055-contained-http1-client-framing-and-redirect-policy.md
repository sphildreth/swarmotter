# ADR-0055: Contained HTTP/1 Client Framing and Redirect Policy

## Status

Proposed

## Context

HTTP/HTTPS tracker and webseed sockets already passed through
`NetworkBinder`, but response parsing split raw headers from a buffer populated
with `read_to_end`. That did not implement HTTP/1 message framing, chunked
transfer coding, redirect policy, or exact webseed range validation. It also
made legal persistent connections indistinguishable from incomplete bodies.
Tracker scrape was reported by product surfaces without a real contained
HTTP/HTTPS scrape request.

Replacing the parser with a general-purpose HTTP client would be unsafe: a
connector, resolver, pool, or redirect implementation that creates its own
sockets could bypass fail-closed torrent data-plane containment.

## Decision

- `swarmotter-core::net::ContainedHttpClient` is the only HTTP/1 transport used
  by tracker announce, supported HTTP/HTTPS scrape, and webseed range reads.
  It resolves each hop with `NetworkBinder::resolve_host` and obtains each TCP
  stream with `NetworkBinder::connect_peer`. HTTPS performs a validating rustls
  handshake over that stream. Hyper is only the HTTP/1 codec over
  `hyper_util::rt::TokioIo`; no Hyper connector, resolver, pool, or general
  client is constructed.
- The client sends an origin-form request target and an exact Host authority,
  including non-default ports and brackets around IPv6 literals. It sends no
  cookies or authorization, rejects URL user information, keeps no pool, and
  owns every one-request connection driver with cancellation-safe abort/await
  cleanup.
- Hyper enforces HTTP/1 framing with bounded response-header buffering/counts.
  Decoded body frames accumulate only to the caller policy's exact cap and fail
  on the first chunk that exceeds the remaining allowance. Tracker announce
  and scrape bodies are capped at 2 MiB. Webseed bodies are capped at the exact
  requested inclusive range length.
- One 30-second timeout covers the complete logical request, including every
  redirect, contained resolution/connect, TLS and Hyper handshakes, headers,
  and decoded body. The binder retains its separate five-second cap on each TCP
  connect. Logical expiration returns the existing typed `timeout` error.
- Only GET redirects 301, 302, 303, 307, and 308 are followed. At most five are
  followed; visited normalized URLs detect loops before another request.
  Relative locations are resolved against the current URL, HTTP-to-HTTPS is
  allowed, HTTPS-to-HTTP is rejected, and every hop repeats contained
  resolution/connect/TLS. Webseed Range is retained. Other 3xx, malformed or
  duplicate Location, 4xx, and 5xx responses fail with HTTP status/protocol
  context without streaming their bodies.
- Tracker announce and scrape accept only final 2xx responses. Webseed accepts
  only final 206 and requires exactly one syntactically valid Content-Range
  whose inclusive start/end and decoded length exactly match the request.
- HTTP/HTTPS scrape is derived only when the final tracker path component
  begins `announce`; that prefix becomes `scrape` and its suffix and unrelated
  query parameters are preserved. Existing `info_hash` pairs are replaced by
  exactly one binary percent-encoded pair per requested torrent. Bounded BEP 48
  decoding updates counts only for exact 20-byte requested keys. Failed or
  missing data never overwrites a separate last-success snapshot. UDP scrape is
  explicitly unsupported.

Direct dependencies are Hyper 1.x with its client codec, `hyper-util` 0.1 with
the Tokio adapter, and `http-body-util` 0.1. Existing `tokio-rustls`, `rustls`,
and `webpki-roots` become direct core dependencies because TLS now resides with
the shared client. All have Apache-2.0-compatible MIT/Apache/ISC/MPL licensing;
none creates a socket.

## Consequences

- Framed Content-Length and chunked responses finish without connection EOF;
  legal close-delimited responses still use EOF as required by HTTP/1.
- Redirects and HTTPS cross-host changes cannot escape containment because the
  client has no independent connection path.
- Tracker and webseed upstream failures have stable `http_protocol_error`,
  `http_status_error`, or existing `timeout` codes. API translation treats the
  first two as bad-gateway failures when they reach a control-plane response.
- Local generated HTTP/rustls fixtures are required for framing, redirect,
  authority, range, containment-spy, cancellation, and scrape behavior.
- ADR-0018's TLS-over-contained-socket rule remains authoritative, but TLS and
  framing move from the daemon's manual helper into the shared core client.

## Related Documents

- [ADR-0014: Tracker Implementation Strategy](0014-tracker-implementation-strategy.md)
- [ADR-0018: HTTPS Tracker TLS Over Contained Sockets](0018-https-tracker-tls-over-contained-sockets.md)
- [Network containment](../vpn-network-containment.md)
- [Architecture](../architecture.md)
- [Testing](../testing.md)
- [Phase review](../2026-07-12.REVIEW.md)
- [Third-party licenses](../../THIRD_PARTY_LICENSES.md)
