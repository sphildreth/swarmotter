# Third-Party Licenses

This file tracks third-party dependencies and licenses used by SwarmOtter.
SwarmOtter source code is licensed under the Apache License, Version 2.0 (see
`LICENSE`). Each direct dependency must be compatible with Apache-2.0.

## Dependency review requirements

Before adding a dependency, review it for:

- License compatibility with Apache-2.0.
- Maintenance quality (actively maintained, no known unpatched issues).
- Security posture (supply-chain risk, auditability).
- Whether it increases project complexity without justified benefit.
- Whether it affects torrent traffic containment. A dependency that creates
  network traffic outside the network containment layer must not be used for
  torrent operations.

Record significant dependency additions or removals here, and create an ADR in
`design/adr/` when the dependency is significant. See ADR-0009 for the
foundational dependency stack rationale.

## Direct dependencies

| Crate | Version | License | Justification |
|-------|---------|---------|---------------|
| tokio | 1 | MIT | Async runtime for daemon and networking |
| bytes | 1 | MIT | Efficient byte buffers for peer/storage I/O |
| serde | 1 | MIT/Apache-2.0 | API and config serialization |
| serde_json | 1 | MIT/Apache-2.0 | API JSON responses |
| toml | 0.8 | MIT/Apache-2.0 | Configuration file parsing |
| thiserror | 1 | MIT/Apache-2.0 | Typed domain error enums |
| tracing | 0.1 | MIT | Structured logging |
| tracing-subscriber | 0.3 | MIT | Log output formatting |
| axum | 0.7 | MIT | Async HTTP API framework (control plane only) |
| tower | 0.5 | MIT | Tower service/middleware for axum |
| tower-http | 0.6 | MIT | HTTP middleware (static fs, trace, cors) |
| hyper | 1 | MIT | HTTP server under axum |
| sha1 | 0.10 | BSD-3-Clause | Info-hash and piece-hash computation (pure Rust) |
| hex | 0.4 | MIT/Apache-2.0 | Hex encoding/decoding |
| url | 2 | MIT/Apache-2.0 | Magnet link and tracker URL parsing |
| once_cell | 1 | MIT/Apache-2.0 | Lazy statics |
| async-trait | 0.1 | MIT/Apache-2.0 | Async trait object dispatch |
| futures-util | 0.3 | MIT/Apache-2.0 | Async stream utilities for SSE/WebSocket |
| tokio-stream | 0.1 | MIT | Broadcast stream adapters for events |
| clap | 4 | MIT/Apache-2.0 | CLI argument parsing |
| libc | 0.2 | MIT/Apache-2.0 | Linux interface discovery and `SO_BINDTODEVICE` socket binding |
| socket2 | 0.6 | MIT/Apache-2.0 | Constructing interface-bound TCP/UDP sockets before handing them to Tokio |
| tokio-rustls | 0.26 | MIT/Apache-2.0 | TLS handshake over contained TCP sockets (HTTPS trackers) |
| rustls | 0.23 | Apache-2.0/MIT/ISC | Rustls TLS implementation with the ring crypto provider (HTTPS trackers) |
| webpki-roots | 0.26 | MPL-2.0 | Platform root CA trust store for HTTPS tracker certificate validation |

### Dev-dependencies (test-only)

| Crate | Version | License | Justification |
|-------|---------|---------|---------------|
| rcgen | 0.13 | MIT/Apache-2.0 | Self-signed certificate generation for the local HTTPS tracker fixture only |
| rustls-pemfile | 2 | Apache-2.0/MIT | PEM parsing for test TLS fixtures |

## Documentation tooling and vendored assets

| Component | Version | License | Justification |
|-----------|---------|---------|---------------|
| mdBook | 0.5.0 | MPL-2.0 | Build-time user-guide site generator for `docs/` |
| mdbook-mermaid | 0.17.0 | MPL-2.0 | Build-time mdBook preprocessor for Mermaid diagrams |
| `assets/mermaid.min.js` | Bundled by mdbook-mermaid 0.17.0 | MIT | Browser runtime for rendered Mermaid diagrams in the published user guide |
| `assets/mermaid-init.js` | Bundled by mdbook-mermaid 0.17.0 | MPL-2.0 | Theme-aware Mermaid initialization script for mdBook |

## Network containment note

None of the direct dependencies above create torrent data-plane network
traffic on their own. All torrent-related sockets (peers, trackers, DHT, PEX,
webseeds, magnet metadata) are created through SwarmOtter's central network
containment layer. `axum`/`hyper`/`tower-http` are scoped to the control plane
(API/Web UI) and do not participate in torrent data traffic.

## Notes

- This file does not constitute legal advice.
- Transitive dependency licenses are resolved by `cargo`; review the full tree
  before distribution. All listed licenses are compatible with Apache-2.0.
