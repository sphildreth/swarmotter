# Architecture

This document describes SwarmOtter's architecture.

## Overview

SwarmOtter is a Rust async daemon with these layers:

- **Core engine** (`swarmotter-core`): bencode, torrent/magnet parsing, info
  hash, domain models, network containment logic, queue/bandwidth/ratio
  logic, storage layout and fast-resume, watch-folder import logic, and the
  torrent registry. Pure, testable logic with no direct socket creation.
- **Network layer** (`swarmotter-core::net`): centralized interface/source
  binding, route validation, VPN/NIC health, and fail-closed enforcement via
  the `InterfaceProbe` trait and the live `NetworkBinder` abstraction. No
  engine component creates sockets directly; all torrent traffic goes through
  the binder (peer TCP, inbound TCP listener, tracker HTTP, UDP trackers,
  and future uTP/DHT/webseed traffic) — see `vpn-network-containment.md` and
  ADR-0012. UDP trackers are implemented in `swarmotter-core::udp_tracker`
  (BEP 15) over the binder's contained UDP socket.
- **Storage layer** (`swarmotter-core::storage`): file layout, partial/sparse
  files, piece read/write and verification, fast resume, forced recheck,
  move/rename, missing/changed file detection logic.
- **API layer** (`swarmotter-api`): REST endpoints plus SSE/WebSocket events
  built on `axum`. The API is a first-class product surface (see ADR-0004 and
  `api.md`). It talks to the daemon through the `DaemonOps` trait, so the
  daemon owns all torrent state and enforces containment.
- **Web layer** (`swarmotter-web`): a practical, function-over-form Web UI
  that consumes the same API exposed to external automation (see ADR-0006).
  Assets are embedded at compile time.
- **Daemon** (`swarmotterd`): owns torrent state, networking, disk I/O,
  queueing, settings, and lifecycle. Implements `DaemonOps`, wires the API +
  Web UI into a single `axum::serve`, runs the network health monitor and
  watch-folder scanner, and spawns the live `TorrentEngine` task per active
  torrent (`swarmotterd::engine`) plus an inbound `Seeder` listener
  (`swarmotterd::seeder`) for serving verified pieces to inbound peers, both
  reconciling real engine state into torrent summaries (see ADR-0016).

## Crate layout

```text
crates/
├── swarmotterd/      # daemon binary + lib (runtime, DaemonOps impl, live engine, seeder, metadata, netbinder)
├── swarmotter-core/  # core types and engine logic
│   └── src/ bencode, endgame, error, extensions, hash, magnet, meta, models/, net/ (binder, config, probe),
│            peer, tracker, udp_tracker, queue, ratio, bandwidth, storage/ (io, layout, resume),
│            torrent, watch, config
├── swarmotter-api/   # API layer (routes, handlers, envelope, events)
└── swarmotter-web/   # embedded static Web UI
```

## Control plane vs data plane

The control plane (API/Web UI) is separate from the torrent data plane. The
API/Web UI may bind to localhost, a LAN address, or a reverse proxy listener.
Torrent data traffic binds separately to the configured VPN/NIC path. Exposing
the API on LAN must not let torrent traffic use the LAN/default route. The
daemon evaluates network containment at startup and periodically; in strict
fail-closed mode, torrents enter `network_blocked` state when the path is
unavailable while the control plane stays available.

## Request flow

1. A client (Web UI or external script) calls `/api/v1/...`.
2. The handler parses the request and calls the `DaemonOps` implementation.
3. The daemon mutates its torrent registry and enforces network containment.
4. The daemon publishes events via the `EventBroker` to SSE/WebSocket
   subscribers.
5. The handler returns the standard `{ success, data, error }` envelope.

## Constraints

- Rust edition 2021, async runtime (tokio), SPDX license headers on source
  files.
- No ad hoc socket creation outside the network containment layer.
- Avoid `unwrap`/`expect` in production paths where a meaningful error exists.
- Keep modules small and focused.
- Minimal, Apache-2.0-compatible dependencies (see ADR-0009).