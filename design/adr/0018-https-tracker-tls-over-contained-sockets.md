# ADR-0018: HTTPS Tracker TLS Over Contained Sockets

## Status

Accepted

## Context

SwarmOtter must announce to HTTP and HTTPS trackers (BEP 3). All torrent
data-plane traffic must go through the network containment layer and fail
closed if the configured path is unavailable; the HTTP client must never
silently use the default route. Until this decision, the binder's `http_get`
performed plaintext HTTP over a contained TCP socket, and HTTPS trackers
(`https://` scheme) were not supported. HTTPS trackers require a TLS
handshake layered on top of the contained TCP connection, with certificate
validation so that a tampered or invalid tracker certificate is rejected.

## Decision

Perform HTTPS as TLS over the binder-created contained TCP socket, entirely
inside the daemon's `ContainedBinder` (`swarmotterd::netbinder`):

- `http_get(url)` detects the `https://` scheme. For HTTPS it opens the TCP
  connection via the existing containment-gated `connect_peer`, then performs
  a TLS handshake (`tokio-rustls` + `rustls`) with the platform root trust
  store (`webpki-roots`) and SNI set to the tracker hostname.
- The HTTP/1.1 request/response is then exchanged over the encrypted stream
  via a shared `http_over_stream` helper used by both plaintext and TLS paths.
- Certificate validation stays enabled by default; a documented test-only
  path uses a self-signed certificate added to a custom root store to prove
  the machinery locally without disabling validation in production.
- The daemon installs the `rustls` ring crypto provider once at startup so
  HTTPS tracker traffic works in the real binary.

New runtime dependencies: `tokio-rustls`, `rustls` (with the `ring` crypto
provider), and `webpki-roots`. New dev-dependency `rcgen` (self-signed cert
generation for the local TLS fixture only). All are Apache-2.0 / ISC /
MIT-compatible and reviewed for license compatibility and containment: they
operate only on the TLS layer over the binder's already-contained TCP socket
and do not create independent network paths.

## Consequences

- HTTPS tracker traffic is containment-gated exactly like HTTP and UDP: the
  `BlockedBinder` and strict fail-closed mode refuse to open the TCP socket,
  so no HTTPS traffic can bypass containment.
- Certificate validation uses the platform root trust store; invalid tracker
  certificates are rejected (the TLS handshake fails with a typed error).
- ADR-0055 subsequently moved TLS and HTTP/1 framing into
  `swarmotter-core::net::ContainedHttpClient` so tracker announce, supported
  scrape, and webseed ranges share one implementation. Rustls still receives
  only a binder-provided stream; Hyper has no connector or socket path.
- DHT and uTP UDP traffic remain unaffected by this TLS addition.

## Related Documents

- `crates/swarmotter-core/src/net/http.rs`
- `crates/swarmotterd/src/netbinder.rs`
- `design/vpn-network-containment.md`
- ADR-0012 (network binder)
- ADR-0014 (tracker implementation strategy)
- [ADR-0055: Contained HTTP/1 Client Framing and Redirect Policy](0055-contained-http1-client-framing-and-redirect-policy.md)
- `THIRD_PARTY_LICENSES.md`
