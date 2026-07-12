# Architecture

This document describes SwarmOtter's architecture.

## Overview

SwarmOtter is a Rust async daemon with these layers:

- **Core engine** (`swarmotter-core`): bencode, torrent/magnet parsing, info
  hash, domain models, network containment logic, queue/bandwidth/ratio
  logic, storage layout and fast-resume, watch-folder import logic, and the
  torrent registry. Pure, testable logic with no direct socket creation. The
  bencode decoder in `swarmotter-core::bencode` and the metainfo builder in
  `swarmotter-core::meta` form the shared parser trust boundary (ADR-0050):
  every untrusted metainfo ingress path — `.torrent` uploads, bulk base64
  metainfo, magnet `info` dicts fetched via BEP 9, watch-folder files, and
  restored durable daemon state — is bounded by `MAX_TORRENT_METADATA_BYTES`,
  `MAX_BENCODE_DEPTH`, `MAX_BENCODE_NODES`, `MAX_TORRENT_FILES`,
  `MAX_TORRENT_PIECES`, and `MAX_PIECE_LENGTH` before any piece-sized
  allocation. No malformed input may panic the daemon.
- **Network layer** (`swarmotter-core::net`): centralized interface/source
  binding, route validation, VPN/NIC health, and fail-closed enforcement via
  the `InterfaceProbe` trait and the live `NetworkBinder` abstraction. No
  engine component creates sockets directly; all torrent traffic goes through
  the binder (peer TCP, inbound TCP listener, tracker HTTP, UDP trackers,
  DHT, and uTP traffic) — see `vpn-network-containment.md` and ADR-0012. UDP
  trackers are implemented in `swarmotter-core::udp_tracker` (BEP 15) and uTP
  (BEP 29, with LEDBAT congestion control, SACK, and the full connection
  lifecycle) is implemented in `swarmotter-core::utp`, both over the binder's
  contained UDP socket. The engine selects TCP/uTP peer transports per config
  (see `configuration.md` and ADR-0020).
- **Storage layer** (`swarmotter-core::storage`): file layout, partial/sparse
  files, piece read/write and verification, fast resume, forced recheck,
  move/rename, missing/changed file detection logic. Runtime storage I/O reuses
  per-torrent file handles and flushes cached writes at read/verification and
  move/remove boundaries rather than after every block write; see ADR-0043.
- **API layer** (`swarmotter-api`): REST endpoints plus SSE/WebSocket events
  built on `axum`. The API is a first-class product surface (see ADR-0004 and
  `api.md`). It talks to the daemon through the `DaemonOps` trait, so the
  daemon owns all torrent state and enforces containment.
- **Web layer** (`swarmotter-web`): a practical, function-over-form Web UI
  that consumes the same API exposed to external automation (see ADR-0006).
  Assets are embedded at compile time.
- **Daemon** (`swarmotterd`): owns torrent state, networking, disk I/O,
  queueing, settings, durable registry state, and lifecycle. Implements
  `DaemonOps`, wires the API + Web UI into a single `axum::serve`, runs the
  network health monitor and watch-folder scanner, and spawns the live
  `TorrentEngine` task per active torrent (`swarmotterd::engine`). A single
  process-wide `SeederHub` (`swarmotterd::seeder`) owns the contained inbound
  peer listener, routes plaintext and encrypted handshakes to registered
  torrents, and owns every accepted peer session. Engine state is reconciled
  into torrent summaries and a versioned state file preserves torrent and
  queue state across restarts (see ADR-0016, ADR-0045, and ADR-0046).
- **Per-torrent health** (`swarmotter-core::models::health`): a deterministic
  calculator that turns live engine state (piece availability, peer
  usefulness, throughput, recent stability, discovery) into a `TorrentHealth`
  with a 0..100 score, 0..5 bar mapping, human-readable label, per-component
  sub-scores, and human-readable reasons. The same calculator is exercised
  by unit tests and by the daemon during state reconciliation so the API
  and the Web UI agree on the score. The Web UI renders a signal-bars
  indicator from the API field (no image asset).

## Crate layout

```text
crates/
├── swarmotterd/      # daemon binary + lib (runtime, DaemonOps impl, live engine, seeder, metadata, dht, netbinder)
├── swarmotter-core/  # core types and engine logic
│   └── src/ bencode, dht, endgame, error, extensions, hash, magnet, meta, models/, net/ (binder, config, probe),
│            peer, tracker, udp_tracker, utp/ (mod, header, sack, congestion, stream), queue, ratio, bandwidth, storage/ (io, layout, resume),
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
4. Durable mutations atomically checkpoint the torrent registry and queue.
5. The daemon publishes events via the `EventBroker` to SSE/WebSocket
   subscribers.
6. The handler returns the standard `{ success, data, error }` envelope.

## Runtime ownership and reconfiguration

The daemon owns and awaits every torrent data-plane task: engines, tracker
announce sidecars, DHT work, the shared inbound listener, and accepted inbound
peer sessions. Network, listen-port, IP-family, uTP, encryption, or DHT changes
stop the complete old task set before the new configuration is installed and
eligible torrents are reconciled with fresh binders. This prevents a task from
retaining an obsolete containment policy (ADR-0047).

File wanted flags and priorities are converted into a shared piece-selection
map used by peer, endgame, and webseed paths. Only a full verified piece set is
promoted to completed storage. Partial and selected-file seeders advertise only
their verified bitfield and read from active storage (ADR-0048).

## Constraints

- Rust edition 2021, async runtime (tokio), SPDX license headers on source
  files.
- No ad hoc socket creation outside the network containment layer.
- Avoid `unwrap`/`expect` in production paths where a meaningful error exists.
- Keep modules small and focused.
- Minimal, Apache-2.0-compatible dependencies (see ADR-0009).
