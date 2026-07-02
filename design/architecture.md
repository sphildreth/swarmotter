# Architecture

This document describes SwarmOtter's architecture. It is a stub; detail will
expand as implementation begins. The full requirements live in
`requirements.md` and `PRD.md`.

## Overview

SwarmOtter is a Rust async daemon with these layers:

- **Core engine** (`swarmotter-core`): torrent/magnet parsing, info hash, peer
  discovery, peer wire protocol, piece management, lifecycle, queue, seeding,
  bandwidth logic.
- **Network layer:** centralized interface/source binding, route validation,
  VPN/NIC health, TCP/UDP/uTP/DHT/tracker/webseed sockets, and torrent-related
  DNS. No engine component creates sockets directly; all traffic goes through
  the containment layer (see `vpn-network-containment.md`).
- **Storage layer:** file layout, partial/sparse files, piece read/write and
  verification, fast resume, forced recheck, move/rename, missing/changed
  file detection.
- **API layer** (`swarmotter-api`): REST endpoints plus WebSocket/SSE events.
  The API is a first-class product surface (see ADR-0004 and `api.md`).
- **Web layer** (`swarmotter-web`): a practical, function-over-form Web UI
  that consumes the same API exposed to external automation (see ADR-0006).
- **Daemon** (`swarmotterd`): owns torrent state, networking, disk I/O,
  queueing, settings, and lifecycle.

## Crate layout

```text
crates/
├── swarmotterd/      # daemon binary
├── swarmotter-core/  # core types and engine logic
├── swarmotter-api/   # API layer
└── swarmotter-web/   # embedded/static web support
```

Module-level layout will be fleshed out inside each crate as implementation
begins, following the small-and-focused module guidance in `AGENTS.md`.

## Control plane vs data plane

The control plane (API/Web UI) is separate from the torrent data plane. The
API/Web UI may bind to localhost, a LAN address, or a reverse proxy listener.
Torrent data traffic binds separately to the configured VPN/NIC path. Exposing
the API on LAN must not let torrent traffic use the LAN/default route.

## Constraints

- Rust edition 2021, async runtime, SPDX license headers on source files.
- No ad hoc socket creation outside the network containment layer.
- Avoid `unwrap`/`expect` in production paths where a meaningful error exists.
- Keep modules small and focused.

## TODO

- Finalize module breakdown inside each crate as implementation begins.
- Add sequence/interaction diagrams for add-magnet, peer download, and
  fail-closed transitions.
- Keep this document aligned with accepted ADRs.