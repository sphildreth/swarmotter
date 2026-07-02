# ADR-0009: Foundational Dependency Stack

## Status

Accepted

## Context

SwarmOtter is moving from a placeholder workspace to implementation. It needs
a concrete set of foundational crates for serialization, async networking,
HTTP serving, bencode parsing, hashing, configuration, structured logging,
and error handling. Dependency choices affect license compatibility, network
containment, and long-term maintenance burden.

Per `AGENTS.md` and `THIRD_PARTY_LICENSES.md`, dependencies must be
Apache-2.0-compatible, well-maintained, and must not create torrent network
traffic outside the containment layer.

## Decision

SwarmOtter adopts the following foundational dependency stack:

- `tokio` (MIT) — async runtime for the daemon and networking.
- `serde` + `serde_json` (MIT/Apache-2.0) — API and config serialization.
- `toml` (MIT/Apache-2.0) — configuration file parsing.
- `thiserror` (MIT/Apache-2.0) — typed domain error enums.
- `tracing` + `tracing-subscriber` (MIT) — structured logging.
- `axum` (MIT) — async HTTP API framework built on hyper/tower; used for the
  control-plane API and Web UI static serving only. It does not create torrent
  data-plane traffic.
- `sha1` (BSD-3-Clause) — BitTorrent info-hash and piece-hash computation.
  This is a pure-Rust crate with no network behavior.
- `serde_bencode` (MIT/Apache-2.0) — bencode deserialization for `.torrent`
  metadata. Pure data format; no network behavior.
- `bytes` (MIT) — efficient byte buffers for peer/storage I/O.

Torrent data-plane traffic (peers, trackers, DHT, webseeds) is implemented
through the central network containment layer using `tokio::net` sockets
bound to the configured source/interface. No dependency is permitted to open
torrent sockets directly.

All listed licenses are compatible with Apache-2.0.

## Consequences

- The project gains a standard, well-maintained async Rust stack.
- The API framework (axum) is scoped to the control plane; torrent data-plane
  sockets remain centralized in the containment layer.
- Adding any future dependency that performs network I/O requires ADR review and
  containment verification.
- `THIRD_PARTY_LICENSES.md` must be kept current as dependencies are added.

## Related Documents

- `AGENTS.md`
- `THIRD_PARTY_LICENSES.md`
- `design/vpn-network-containment.md`
- `design/architecture.md`