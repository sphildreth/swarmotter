# ADR-0002: Use Rust for Daemon and Core Engine

## Status

Accepted

## Context

SwarmOtter is a performance-first BitTorrent daemon expected to run as a
long-lived service managing many torrents, peers, and tracker connections
concurrently. It must handle untrusted network input safely, use memory
predictably, and behave reliably over long uptimes.

Language choice affects performance, memory safety, concurrency ergonomics, and
the long-term maintainability of a networked daemon.

## Decision

SwarmOtter will use Rust for the daemon and core BitTorrent engine because the
project prioritizes performance, memory safety, concurrency, and reliable
long-running service behavior.

The workspace uses Rust edition 2021 and an async runtime. New Rust source
files include the SPDX license identifier (`Apache-2.0`).

## Consequences

- The project gains Rust's memory-safety and concurrency guarantees without a
  garbage collector, which suits a high-throughput daemon.
- Async networking is available, but contributors must centralize socket
  creation through the network containment layer rather than creating ad hoc
  sockets.
- Some contributors face a higher learning curve than with a garbage-collected
  language; this is accepted for the safety and performance trade-off.
- C or C++ interop, if ever needed, must respect the network containment and
  safety rules.

## Related Documents

- `AGENTS.md`
- `Cargo.toml`
- `design/architecture.md`