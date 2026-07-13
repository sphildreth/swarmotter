# Architecture

This document describes SwarmOtter's architecture.

## Overview

SwarmOtter is a Rust async daemon with these layers:

- **Core engine** (`swarmotter-core`): bencode, torrent/magnet parsing, info
  hash, domain models, network containment logic, queue/bandwidth/ratio
  logic, storage layout and fast-resume, watch-folder import logic, and the
  torrent registry. Pure, testable logic with no direct socket creation. The
  bencode decoder in `swarmotter-core::bencode` and the metainfo builder in
  `swarmotter-core::meta` form the shared bencoded-input trust boundary
  (ADR-0050). `.torrent` uploads, bulk base64 metainfo, magnet `info` dicts
  fetched via BEP 9, and watch-folder files are bounded by
  `MAX_TORRENT_METADATA_BYTES`, `MAX_BENCODE_DEPTH`, `MAX_BENCODE_NODES`,
  `MAX_TORRENT_FILES`, `MAX_TORRENT_PIECES`, and `MAX_PIECE_LENGTH` before any
  piece-sized allocation. Restored daemon state is a separate JSON boundary:
  piece hashes must decode to exactly 20 bytes and restored `TorrentMeta`
  values must pass `TorrentMeta::validate()` before runtime use. No malformed
  input may panic the daemon.
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
  queue state across restarts. Each retained torrent owns one
  `Arc<RateLimiter>` shared by its downloader and seeder, and seeder registry
  transitions are serialized with `TorrentState`/`SeedingStatus` updates (see
  ADR-0016, ADR-0045, ADR-0046, and ADR-0052).
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

The daemon and engine use explicit ownership modules while preserving the
`swarmotterd::daemon::*` and `swarmotterd::engine::*` library facades:

```text
swarmotterd/src/
├── daemon/
│   ├── mod.rs                         # runtime types and public facade
│   ├── construction.rs                # shared-resource wiring
│   ├── lifecycle.rs                   # DaemonOps lifecycle implementation
│   ├── scheduler.rs, seeding.rs       # queue and seeder ownership
│   ├── settings.rs, watch.rs          # reconfiguration and watch ingestion
│   ├── containment.rs, diagnostics.rs # gate/recovery and observations
│   ├── persistence.rs                 # restore, checkpoints, rollback
│   └── tests.rs
└── engine/
    ├── mod.rs                         # engine types, builders, public facade
    ├── discovery.rs, peer_session.rs  # candidate sources and wire sessions
    ├── download.rs, parallel.rs       # serial and parallel scheduling
    ├── endgame.rs, webseed.rs         # bounded specialized download paths
    ├── progress.rs                    # accounting updates
    └── tests.rs
```

The binary imports `swarmotterd::{daemon, logging}` and does not redeclare
library modules. Consequently daemon unit tests compile once under the library
target. Native torrent handlers follow the same ownership rule under
`swarmotter-api/src/handlers/torrents/`: `add`, `bulk`, `query`, `lifecycle`,
and `settings` are re-exported by `mod.rs`, preserving the existing router and
handler paths.

## Web UI asset and module boundary

The Web UI remains build-step-free vanilla JavaScript. `/app.js` is an ES-module
entry that alone composes the feature modules served under `/js/`: `api.js`,
`state.js`, `torrents.js`, `details.js`, `settings.js`, `events.js`, and
`ui.js`. Feature modules depend only on the shared API/state/UI layers and use
entry-injected callbacks for cross-feature actions, so the import graph is
acyclic. `swarmotter-web/src/lib.rs` embeds and routes every module with
`application/javascript; charset=utf-8`.

Every HTML and module response passes through the same security middleware.
The Content Security Policy remains `script-src 'self'` with no inline-script
exception, and the module routes retain the entry script's absence of an
explicit cache header. Route tests assert status, content type, CSP, and cache
policy for every module. Contributor checks run
`scripts/check-web-js-modules.sh` so every JavaScript asset is parsed with ES
module semantics; the executable watch-history and seeding-policy DOM harnesses
remain part of the test surface.

## Control plane vs data plane

The control plane (API/Web UI) is separate from the torrent data plane. The
API/Web UI may bind to localhost, a LAN address, or a reverse proxy listener.
Torrent data traffic binds separately to the configured VPN/NIC path. Exposing
the API on LAN must not let torrent traffic use the LAN/default route. The
daemon evaluates network containment at startup and periodically; in strict
fail-closed mode, torrents enter `network_blocked` state when the path is
unavailable while the control plane stays available.

## Contained HTTP and tracker scrape

`swarmotter-core::net::ContainedHttpClient` is the shared tracker announce,
supported HTTP/HTTPS scrape, and webseed range transport (ADR-0055). Each hop
uses `NetworkBinder::resolve_host` and `connect_peer`; TLS is layered over that
stream, and Hyper is only an HTTP/1 codec through its Tokio I/O adapter. There
is no Hyper connector, resolver, pool, or general client capable of creating a
socket. Requests use origin-form targets and exact Host authorities. One
logical timeout spans redirects and the decoded body, while decoded tracker
bodies and exact webseed ranges have independent hard caps.

Only the bounded redirect set is followed, every hop reconnects through the
binder, HTTPS downgrade is rejected, and webseed Range survives redirects.
Tracker announce/scrape require a final 2xx; webseed requires an exact 206 and
matching Content-Range/body length. HTTP(S) scrape is derived only from a final
`announce*` path component and parses BEP 48 through the bounded bencode
decoder. UDP and non-derivable paths are recorded as unsupported without a
network call.

Engine announce activity schedules scrape for initial downloads, magnet
metadata discovery using the real hash, explicit/periodic reannounce, and
completion. The owned seeder announce loop does the same for active seeds.
Each tracker has a separate scrape snapshot: the newest attempt updates
status/time/error, while only a successful exact-key response replaces retained
counts. Join failures are attributed to the tracker and included in recent
tracker-failure accounting. API compatibility counts prefer a successful
announce and fall back to retained scrape counts when announce has not
succeeded; downloaded count uses scrape when available.

## Request flow

1. A client (Web UI or external script) calls `/api/v1/...`.
2. The handler parses the request and calls the `DaemonOps` implementation.
3. The daemon mutates its torrent registry and enforces network containment.
4. Durable mutations atomically checkpoint the torrent registry and queue.
5. The daemon publishes events via the `EventBroker` to SSE/WebSocket
   subscribers.
6. The handler returns the standard `{ success, data, error }` envelope.

## Durable torrent add and watch ingestion

API file, magnet, and watch-folder additions converge on one daemon transaction
(ADR-0054). The existing storage-ownership lock spans duplicate determination,
storage/containment/path preflight, exact registry/queue membership snapshots,
in-memory insertion, and durable state persistence. Persistence failure restores
only the affected hash's exact snapshots before the lock is released. Per-
torrent runtime resources, `torrent_added`/stats events, and queue reconciliation
are created or scheduled only after the durable checkpoint succeeds. Watch
duplicates use this primitive's non-mutating duplicate result; the API maps it
to its existing conflict contract while watch processing treats it as success.

The watch runtime owns one whole-scan mutex and a non-durable observation map
keyed by normalized absolute root plus normalized relative path. Complete
directory walks and bounded file reads run in blocking tasks. Walks use
`symlink_metadata`, reject symlink roots, skip every child symlink, and return
sorted paths. Each folder also excludes its own strict-descendant lexical
archive/failure paths from its scan; exclusions use component boundaries and do
not affect a separately configured overlapping root. Two identical
length/modified-time scans are required before an attempt. Changed-during-read
metadata resets stability without a terminal event. Only successful complete
scans prune missing observations; removed configured roots are pruned
explicitly. Processed fingerprints and the insertion-ordered 10,000-result
history intentionally reset on restart.

Permanent watch input errors and transient operational errors have different
retry/action behavior. Archive/failure actions create missing destinations and
use create-new copy/remove semantics so an existing file is never replaced.
Post-action failure remains attached to the primary result for UI/event
reporting and requires operator intervention rather than repeated import.

## Runtime ownership and reconfiguration

The daemon owns and awaits every torrent data-plane task: engines, tracker
announce sidecars, DHT work, the shared inbound listener, and accepted inbound
peer sessions. Network, listen-port, IP-family, uTP, encryption, or DHT changes
stop the complete old task set before the new configuration is installed and
eligible torrents are reconciled with fresh binders. This prevents a task from
retaining an obsolete containment policy (ADR-0047).

Peer-session ownership is a separate runtime resource boundary (ADR-0053).
Every outbound metadata, serial, parallel, or endgame TCP/uTP connection holds
one process-wide and one per-torrent RAII permit from before socket creation
through session teardown. The shared inbound listener acquires the global
permit immediately after accept and the routed torrent permit after identifying
the info hash. Trackers, webseeds, DHT nodes, DNS, discovery, and retry waits do
not consume this peer-specific budget. `max_peers = 0` is unlimited but still
counts observed live sessions; `max_peers_per_torrent = 0` selects 64.

Peer-cap PATCH/PUT reconstruction holds the data-plane transition lock through
old-task shutdown, provisional config/pool installation, and synchronous
policy-eligible restart. Exact prior pool identities, all torrent lifecycle and
recovery-intent fields, durable state, config bytes, and formerly owned tasks
are restored on reconstruction or persistence failure. Candidate selfish
completion remains inactive until persistent commit so provisional tasks cannot
perform irreversible removal.

File wanted flags and priorities are converted into a shared piece-selection
map used by peer, endgame, and webseed paths. Only a full verified piece set is
eligible for the completed-content seeder registry. Exact torrent and file
progress is recomputed from verified piece byte-range intersections, so a
boundary piece credits only the bytes it covers and the final piece is never
treated as full length (ADR-0048, ADR-0052).

Complete-content seeding uses two coordinated lifecycle layers. `TorrentState`
remains the coarse state while `SeedingStatus` distinguishes queued, active,
ratio-stopped, idle-stopped, and manually stopped records. A live
`SeedRegistry` entry is authoritative for `seeding` + `active` and for the
global active-seed count. Fail-closed containment temporarily sets
`network_blocked` while preserving the prior fine-grained status and durable
recovery intent.

## Constraints

- Rust edition 2021, async runtime (tokio), SPDX license headers on source
  files.
- No ad hoc socket creation outside the network containment layer.
- Avoid `unwrap`/`expect` in production paths where a meaningful error exists.
- Keep modules small and focused.
- Minimal, Apache-2.0-compatible dependencies (see ADR-0009).
