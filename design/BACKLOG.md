# Feature Backlog

This document tracks market-differentiating feature candidates found by
reviewing open feature-request issues in the Transmission and qBittorrent
GitHub repositories on 2026-07-03.

Backlog rule: when a feature in this document is implemented, tested,
documented, and usable in SwarmOtter, remove it from this document. Do not keep
completed items here as checked-off backlog rows.

This is a product backlog, not a `v1.0.0` scope document and not a
release-status document. Items here are not intended for `v1.0.0` by default
and are not limited to `v1.0.0`; they are prioritized product opportunities
that can be selected for whatever release or planning cycle best fits the
project.

## Priority Key

- `P0`: Highest-value differentiator; strong user value and good fit for
  SwarmOtter's architecture.
- `P1`: Strong feature candidate; valuable after the `P0` set or when touching
  the same subsystem.
- `P2`: Useful but narrower, more cosmetic, or less urgent.
- `P3`: Research candidate; requires architecture, legal, dependency, or
  containment review before acceptance.

## Feature Map

| Priority | Feature | User Value | Source Signals |
| --- | --- | --- | --- |
| P0 | Adaptive swarm performance autopilot | Improve real download throughput, reduce bad-peer waste, explain speed bottlenecks | Transmission [#3945](https://github.com/transmission/transmission/issues/3945), qBittorrent [#24254](https://github.com/qbittorrent/qBittorrent/issues/24254), [#24053](https://github.com/qbittorrent/qBittorrent/issues/24053), [#23050](https://github.com/qbittorrent/qBittorrent/issues/23050), [#23476](https://github.com/qbittorrent/qBittorrent/issues/23476), [#24330](https://github.com/qbittorrent/qBittorrent/issues/24330) |
| P0 | Disk-aware storage optimizer | Better performance and fewer storage surprises on Btrfs, NAS, HDD, and constrained disks | qBittorrent [#23683](https://github.com/qbittorrent/qBittorrent/issues/23683), [#22949](https://github.com/qbittorrent/qBittorrent/issues/22949), [#23572](https://github.com/qbittorrent/qBittorrent/issues/23572), Transmission [#5064](https://github.com/transmission/transmission/issues/5064), [#5594](https://github.com/transmission/transmission/issues/5594), [#1060](https://github.com/transmission/transmission/issues/1060) |
| P0 | Policy profiles and inherited torrent settings | Apply consistent path, ratio, queue, bandwidth, tracker, and file rules by label/category/profile | qBittorrent [#9939](https://github.com/qbittorrent/qBittorrent/issues/9939), [#24500](https://github.com/qbittorrent/qBittorrent/issues/24500), [#23722](https://github.com/qbittorrent/qBittorrent/issues/23722), [#24131](https://github.com/qbittorrent/qBittorrent/issues/24131), Transmission [#6710](https://github.com/transmission/transmission/issues/6710), [#1461](https://github.com/transmission/transmission/issues/1461), [#6425](https://github.com/transmission/transmission/issues/6425) |
| P0 | Large-library Web UI operations console | Keep the UI fast and useful with hundreds or thousands of torrents | qBittorrent [#24558](https://github.com/qbittorrent/qBittorrent/issues/24558), [#23127](https://github.com/qbittorrent/qBittorrent/issues/23127), [#23449](https://github.com/qbittorrent/qBittorrent/issues/23449), [#9796](https://github.com/qbittorrent/qBittorrent/issues/9796), [#22111](https://github.com/qbittorrent/qBittorrent/issues/22111), Transmission [#3813](https://github.com/transmission/transmission/issues/3813), [#8237](https://github.com/transmission/transmission/issues/8237) |
| P0 | Ecosystem Compatibility API | Operate alongside Sonarr/Radarr/Flood via qBittorrent-compatible and Transmission-compatible API shims | Deluge API parity requests, Flood UI, Sonarr/Radarr integration, self-hosting ecosystem (2026) |
| P0 | Per-Profile / Per-Torrent Network-Path Binding | Assign a contained network path (namespace/VPN endpoint/interface) per profile, label, or torrent; fail-closed per path | rTorrent/Flood multi-user isolation, Deluge multi-profile routing, self-hosting VPN routing patterns |
| P0 | Multi-User / Multi-Tenant Support | Role-based access control, per-user torrent isolation, per-user quotas, and shared-server deployments | qBittorrent [#3327](https://github.com/qbittorrent/qBittorrent/issues/3327), Flood multi-user, rTorrent+ruTorrent multi-user, Deluge thin-client auth |
| P0 | Protocol Encryption / MSE-PE (BEP 8) | Interoperate with peers that refuse plaintext handshakes and protect the peer wire protocol from ISP throttling/identification | Transmission, qBittorrent, Deluge, BiglyBT all ship MSE/PE; private trackers commonly require it |
| P1 | Metadata-first magnet preview and intake rules | Let users inspect/select files before starting data transfer and enforce file exclusion rules | Transmission [#1611](https://github.com/transmission/transmission/issues/1611), [#2366](https://github.com/transmission/transmission/issues/2366), [#7330](https://github.com/transmission/transmission/issues/7330), [#7399](https://github.com/transmission/transmission/issues/7399), [#2399](https://github.com/transmission/transmission/issues/2399), [#5582](https://github.com/transmission/transmission/issues/5582), [#8793](https://github.com/transmission/transmission/issues/8793), qBittorrent [#23674](https://github.com/qbittorrent/qBittorrent/issues/23674) |
| P1 | File cleanup, trash, and retention safety | Avoid accidental data loss while making unwanted/obsolete partial data easy to remove | qBittorrent [#23575](https://github.com/qbittorrent/qBittorrent/issues/23575), [#23353](https://github.com/qbittorrent/qBittorrent/issues/23353), [#24102](https://github.com/qbittorrent/qBittorrent/issues/24102), [#24601](https://github.com/qbittorrent/qBittorrent/issues/24601), Transmission [#1722](https://github.com/transmission/transmission/issues/1722), [#6513](https://github.com/transmission/transmission/issues/6513) |
| P1 | Tracker and peer operations workbench | Diagnose weak swarms, prioritize trackers, expose known peers, webseeds, and retry state | Transmission [#996](https://github.com/transmission/transmission/issues/996), [#6425](https://github.com/transmission/transmission/issues/6425), [#8326](https://github.com/transmission/transmission/issues/8326), [#8413](https://github.com/transmission/transmission/issues/8413), [#5234](https://github.com/transmission/transmission/issues/5234), qBittorrent [#24013](https://github.com/qbittorrent/qBittorrent/issues/24013), [#24014](https://github.com/qbittorrent/qBittorrent/issues/24014) |
| P1 | Secure remote-operations hardening | Make headless/server use safer and easier behind reverse proxies and automation | qBittorrent [#7172](https://github.com/qbittorrent/qBittorrent/issues/7172), [#24308](https://github.com/qbittorrent/qBittorrent/issues/24308), Transmission [#5899](https://github.com/transmission/transmission/issues/5899), [#5989](https://github.com/transmission/transmission/issues/5989), qBittorrent [#19951](https://github.com/qbittorrent/qBittorrent/issues/19951) |
| P1 | Safe automation hooks | Provide explicit, observable, allowlisted event actions without unsafe hidden scripts | Transmission [#8056](https://github.com/transmission/transmission/issues/8056), [#6984](https://github.com/transmission/transmission/issues/6984), qBittorrent [#23550](https://github.com/qbittorrent/qBittorrent/issues/23550), [#23603](https://github.com/qbittorrent/qBittorrent/issues/23603) |
| P1 | Content organization controls | Keep download directories orderly through folder rules, preset paths, and path normalization | Transmission [#5614](https://github.com/transmission/transmission/issues/5614), [#8225](https://github.com/transmission/transmission/issues/8225), [#6044](https://github.com/transmission/transmission/issues/6044), [#6045](https://github.com/transmission/transmission/issues/6045), qBittorrent [#24239](https://github.com/qbittorrent/qBittorrent/issues/24239) |
| P1 | Torrent Creation (BEP 52 v2/hybrid) | Create `.torrent` files from local lawful content with piece hashing, tracker tiers, and webseed support | BiglyBT torrent creation, aria2 torrent creation, Transmission [#5794](https://github.com/transmission/transmission/issues/5794), Deluge create plugin |
| P1 | Superseeding / Initial Seeding (BEP 16) | Efficient first distribution of new lawful releases via initial-seeding mode | BEP 16, qBittorrent [#20098](https://github.com/qbittorrent/qBittorrent/issues/20098), BiglyBT initial seeding |
| P1 | IP Filtering / Blocklists / Peer Banning | Filter unwanted peers via CIDR/range lists, blocklist import, manual peer bans, and client-ID-based blocking | qBittorrent IP filtering, Deluge IP filtering, eMule/PeerGuardian blocklist formats, qBittorrent [#10258](https://github.com/qbittorrent/qBittorrent/issues/10258) |
| P1 | UPnP / NAT-PMP Port Forwarding | Automatic port mapping for reachability behind NAT without manual router configuration | qBittorrent UPnP/NAT-PMP, Transmission port forwarding, Deluge UPnP/NAT-PMP |
| P1 | SOCKS5 Proxy Support | Route torrent traffic through a SOCKS5 proxy for seedbox and restricted-network deployments | Transmission [#1250](https://github.com/transmission/transmission/issues/1250), qBittorrent SOCKS5, Deluge proxy support |
| P1 | Seed Prioritization (Low-Seed First) | Prefer seeding torrents with few available seeds to improve swarm health and distribution efficiency | qBittorrent [#9063](https://github.com/qbittorrent/qBittorrent/issues/9063), Transmission seed-priority discussions |
| P1 | OpenAPI Specification & Interactive API Docs | Auto-generated OpenAPI/JSON Schema with Swagger UI for native and compatibility API surfaces | Flood Swagger UI, Deluge API docs, self-hosting automation integration |
| P1 | User-Configured Lawful RSS Feeds | Ingest content from user-supplied lawful RSS feeds as part of lawful distribution workflows | Deluge RSS plugin, rTorrent RSS, self-hosting RSS workflows; see lawful-use policy |
| P1 | Native Cross-Seed & Hardlink-Aware Storage | Match on-disk data to new torrents by piece layout; link instead of re-download | cross-seed (external tool), self-hosting hardlink layouts, BiglyBT |
| P1 | Trust and Provenance Signals for Torrents and Trackers | Per-tracker trust state, tracker allowlists/denylists, and signed-`.torrent` provenance verification for lawful-distribution workflows | eMule/PeerGuardian blocklists, signed-release workflows, transmission tracker whitelists |
| P1 | Operator Audit Log for Torrent Lifecycle Events | Structured, exportable, optionally hash-chained audit trail for privileged operations; combines with multi-user for compliance | qBittorrent activity log, rTorrent XMLRPC, Flood multi-user, self-hosting compliance |
| P1 | Explainability API: Structured Reasons for Non-Trivial Decisions | Unified, machine-readable reasons across autopilot, disk optimizer, fail-closed, and bandwidth decisions | Sonarr/Radarr import failure reasons, Flood API exploration, operator tooling |
| P1 | Container / Sandbox-First Deployment Story | First-class OCI image, rootless and read-only-filesystem operation, Helm chart and Compose file as in-repo artifacts | Sonarr/Radarr container deployment, *arr community images, Podman/Kubernetes patterns |
| P1 | Production Health / Availability Surface | Liveness/readiness endpoints, synthetic end-to-end check torrent, SLO-style summaries for orchestrators | Kubernetes liveness/readiness, Consul/Nomad health, cloud-native SLO conventions |
| P1 | Filesystem Snapshot Integration | Opt-in snapshot hooks on Btrfs subvolumes, ZFS, and Snapper; rollback for torrent roots and state | Snapper, ZFS, Btrfs subvolume workflows, self-hosting rollback patterns |
| P1 | Client-Identity Fingerprinting and Rollups | Per-torrent and per-tracker client rollups for swarm composition visibility and prioritization | qBittorrent peer client string, BiglyBT peer view, rTorrent peer text |
| P1 | HTTP / HTTPS Proxy Support | Egress through HTTP/CONNECT proxies common in corporate and filtered environments where SOCKS5 is unavailable | qBittorrent HTTP proxy, aria2 HTTP proxy |
| P1 | Scriptable CLI (`swarmotterctl`) | Add/list/pause/resume/limits with JSON output for automation and SSH workflows without a browser | `transmission-remote`, rTorrent CLI, aria2 CLI-first operation |
| P1 | Seedbox Pre-Seed Warm-Up | Pre-read and pre-hash a new lawful release before announcing so the first peer is served instantly | BiglyBT pre-seed concepts, superseeding efficiency |
| P1 | Idempotent Re-Add / Content-Addressed Import | Recognize re-added torrents whose data already exists and skip re-download and re-verify automatically | qBittorrent re-add friction, large-library operator workflows |
| P1 | Durable State Store (SQLite) | Single durable store enabling cheap queue, health, audit, and history queries beyond per-torrent resume files | Self-hosting operators, Long-Horizon Observability (P2), Operator Audit Log (P1) |
| P2 | Sequential Download / Streaming / File Preview | Sequential/priority-first fetch; in-place preview and verify; metadata-first preview | qBittorrent sequential download, aria2, WebTorrent streaming, Deluge |
| P2 | Protocol modernization roadmap | Stay ahead of compatibility and swarm reachability changes; BEP 52 v2/hybrid handling | qBittorrent [#23421](https://github.com/qbittorrent/qBittorrent/issues/23421), [#24600](https://github.com/qbittorrent/qBittorrent/issues/24600), Transmission [#3387](https://github.com/transmission/transmission/issues/3387), [#3705](https://github.com/transmission/transmission/issues/3705), [#993](https://github.com/transmission/transmission/issues/993) |
| P2 | Long-horizon observability | Preserve useful history beyond current live status and make operational events auditable | Transmission [#5591](https://github.com/transmission/transmission/issues/5591), qBittorrent [#22832](https://github.com/qbittorrent/qBittorrent/issues/22832), [#18525](https://github.com/qbittorrent/qBittorrent/issues/18525), [#24330](https://github.com/qbittorrent/qBittorrent/issues/24330) |
| P2 | Settings search and low-risk UI personalization | Make dense configuration easier to operate without turning the UI into a theme project | qBittorrent [#23654](https://github.com/qbittorrent/qBittorrent/issues/23654), [#22877](https://github.com/qbittorrent/qBittorrent/issues/22877), [#22913](https://github.com/qbittorrent/qBittorrent/issues/22913), Transmission [#4304](https://github.com/transmission/transmission/issues/4304), [#5648](https://github.com/transmission/transmission/issues/5648) |
| P2 | Time-of-Day and Adaptive Bandwidth Policies | Time-of-day schedules merged with the adaptive autopilot into a single per-profile bandwidth policy surface | qBittorrent scheduler, aria2 bandwidth scheduling, Deluge scheduler, adaptive autopilot (P0) |
| P2 | Backup / Restore & Bulk Import/Export | Export/import torrent list and state for migration and disaster recovery of large libraries | qBittorrent backup, Deluge export, Flood backup/restore |
| P2 | Thin Client / Remote Session Architecture | Connect a native or web client to a remote daemon via a streaming RPC protocol without SSH tunneling | Deluge thin-client architecture, qBittorrent remote session requests, Flood multi-backend |
| P2 | OpenTelemetry Observability | Distributed tracing, span export, and OTLP metrics export for cloud-native monitoring | OpenTelemetry standard, Flood OpenAPI+Swagger, cloud-native deployment patterns |
| P2 | Cloud / Object-Storage-Backed Storage Root | S3/WebDAV/rclone-backed torrent storage for institutional lawful distributors of datasets and archives | Institutional dataset distribution, rclone mount patterns, no mainstream client owns this |
| P2 | Local GeoIP / ASN Peer Rollups | On-device geographic and ASN distribution of legal-swarm peers for distribution planning | MaxMind local DB, complements Client-Identity Fingerprinting (P1) |
| P2 | Responsive / Mobile-Friendly Web UI | Touch and small-viewport operation for homelab phone-check workflows | qBittorrent and Transmission Web UIs are minimally responsive |
| P3 | Permissioned extension system | Enable integrations only if permissions, sandboxing, and lawful-use constraints are clear | qBittorrent [#24530](https://github.com/qbittorrent/qBittorrent/issues/24530), [#24531](https://github.com/qbittorrent/qBittorrent/issues/24531) |
| P3 | Alternate privacy-preserving transports | Evaluate only if strict containment, lawful-use messaging, and operational risk are solved | Transmission [#7230](https://github.com/transmission/transmission/issues/7230), qBittorrent [#23665](https://github.com/qbittorrent/qBittorrent/issues/23665), [#24241](https://github.com/qbittorrent/qBittorrent/issues/24241), [#23064](https://github.com/qbittorrent/qBittorrent/issues/23064) |
| P3 | Swarm Merging (BiglyBT-style) | Complete or accelerate a torrent using matching content from other torrents or HTTP sources | BiglyBT swarm merging, self-hosting seedbox workflows |
| P3 | Terminal UI / Console Interface | ncurses-based TUI for low-resource headless environments and terminal-first workflows | rTorrent ncurses TUI, Deluge `deluge-console`, aria2 CLI |
| P3 | Localization Strategy for the Web UI, API Errors, and Docs | Documented translation workflow, source-string extraction, and an explicit English-authoritative policy | qBittorrent translations, Deluge and ruTorrent community translations |
| P3 | Documentation Discoverability | Search index for `docs/` and a built-in help pane in the Web UI tied to daemon version | mdBook search, DocSearch, Sonarr/Radarr in-app help |

## P0 Features

### Adaptive Swarm Performance Autopilot

Problem: users can see many peers but poor throughput, with little guidance on
whether the bottleneck is upload saturation, bad peers, tracker quality, DHT
freshness, disk I/O, transport mix, network containment, or queue policy.

Requested elsewhere:

- Transmission users requested automatic speed limits based on available
  bandwidth in [transmission#3945](https://github.com/transmission/transmission/issues/3945).
- qBittorrent users requested blocking peers with poor progress/upload behavior
  in [qbittorrent#24254](https://github.com/qbittorrent/qBittorrent/issues/24254).
- qBittorrent users requested tracker scalability, DNS caching, and IPv6
  prioritization in [qbittorrent#24053](https://github.com/qbittorrent/qBittorrent/issues/24053).
- Queue starvation from stalled or slow torrents appears in
  [qbittorrent#23050](https://github.com/qbittorrent/qBittorrent/issues/23050)
  and [qbittorrent#23476](https://github.com/qbittorrent/qBittorrent/issues/23476).
- Users want to know when a torrent last received data in
  [qbittorrent#24330](https://github.com/qbittorrent/qBittorrent/issues/24330).

SwarmOtter feature shape:

- Add a per-torrent performance model that tracks useful peer rate, stale peer
  rate, tracker contribution, DHT/PEX freshness, last useful data time,
  disk-write pressure, and containment state.
- Add an optional adaptive bandwidth mode that tunes global upload/download
  limits using measured latency and throughput while respecting configured hard
  caps.
- Add peer and tracker scoring that affects retry order, connection attempts,
  and UI diagnostics.
- Add queue mitigation for stalled torrents that are blocking active slots.
- Add an API and UI "why is this slow?" report with specific causes and recent
  autopilot actions.

Acceptance direction:

- The daemon must log and expose every automatic decision.
- The user must be able to disable or enable feature modes globally through
  `[autopilot].mode` (`disabled` / `observe` / `act`, default `observe`) and
  per torrent through the API.
- All network measurements must use the existing contained data plane; no
  separate uncontained probing is allowed.
- Current documentation and acceptance for this phase are recorded in
  `ADR-0035-adaptive-swarm-performance-autopilot.md`.

### Disk-Aware Storage Optimizer

Problem: torrent clients often treat storage as a passive byte sink, but users
hit real performance and durability problems on Btrfs, HDDs, NAS mounts,
limited SSDs, and large queues.

Requested elsewhere:

- qBittorrent has a heavily discussed CoW filesystem request in
  [qbittorrent#23683](https://github.com/qbittorrent/qBittorrent/issues/23683).
- qBittorrent users requested relocating state files away from OS SSD/NVMe
  drives in [qbittorrent#22949](https://github.com/qbittorrent/qBittorrent/issues/22949).
- qBittorrent users requested a total active-download size cap in
  [qbittorrent#23572](https://github.com/qbittorrent/qBittorrent/issues/23572).
- Transmission users requested parallel verification in
  [transmission#5064](https://github.com/transmission/transmission/issues/5064).
- Transmission users requested prominent free-space display in
  [transmission#5594](https://github.com/transmission/transmission/issues/5594).
- Transmission has a Btrfs/subvolume move-performance issue in
  [transmission#1060](https://github.com/transmission/transmission/issues/1060).

SwarmOtter feature shape:

- Detect filesystem type, free space, mount options, write throughput, and
  verification throughput per configured storage root.
- Add disk-aware queue controls: active byte cap, active write-pressure cap,
  per-storage-root concurrency, and recheck concurrency.
- Add optional CoW-aware write strategy for Btrfs-like filesystems, including
  preallocation policy, sparse policy, and clearly surfaced trade-offs.
- Add UI/API storage diagnostics showing free space, active write rate, active
  recheck rate, and torrents mapped to each root.
- Add disk-space pre-check enforcement: prevent starting new downloads when
  free space on the target storage root falls below a configurable threshold.
  Reject add requests with a clear error before any data is written.
- Add state-directory controls for logs, resume files, database/state, and
  temporary files so high-write paths can be placed intentionally.

Acceptance direction:

- No storage optimization may risk silent data corruption.
- CoW-related behavior must be explicit and documented because checksumming,
  compression, snapshots, and fragmentation trade off differently per
  filesystem.
- Implementing this requires an ADR.

### Policy Profiles and Inherited Torrent Settings

Problem: users manage different classes of lawful torrents with different
rules. Applying those rules one torrent at a time does not scale.

Requested elsewhere:

- qBittorrent's long-running inherited settings request is
  [qbittorrent#9939](https://github.com/qbittorrent/qBittorrent/issues/9939).
- qBittorrent users requested boolean logic for seed limits in
  [qbittorrent#24500](https://github.com/qbittorrent/qBittorrent/issues/24500).
- qBittorrent users requested category-level filename exclusions in
  [qbittorrent#23722](https://github.com/qbittorrent/qBittorrent/issues/23722).
- qBittorrent users requested watch-folder category defaults in
  [qbittorrent#24131](https://github.com/qbittorrent/qBittorrent/issues/24131).
- Transmission users requested Web UI automatic torrent management in
  [transmission#6710](https://github.com/transmission/transmission/issues/6710).
- Transmission users requested per-tracker seed ratio and tracker priority in
  [transmission#1461](https://github.com/transmission/transmission/issues/1461)
  and [transmission#6425](https://github.com/transmission/transmission/issues/6425).

SwarmOtter feature shape:

- Introduce named policy profiles that can be assigned by label, watch folder,
  add request, torrent, tracker host, or explicit user selection.
- Resolve effective settings from global defaults, profile defaults, label
  defaults, watch-folder defaults, and per-torrent overrides.
- Support profile-controlled storage path, incomplete path, queue priority,
  start behavior, ratio/idle rules, bandwidth caps, tracker priority, file
  exclusion patterns, and completion actions.
- Show the effective policy and the source of each setting in the Web UI and
  API.

Acceptance direction:

- Effective values must be deterministic and explainable.
- Profile changes must clearly distinguish between live inheritance and
  create-time snapshots.
- Implementing this requires an ADR because it changes persistent settings and
  runtime behavior.

### Large-Library Web UI Operations Console

Problem: Web UIs that work for a few torrents degrade with hundreds or
thousands. SwarmOtter can differentiate by treating the Web UI as an operations
console for large libraries.

Requested elsewhere:

- qBittorrent users requested virtualized/non-laggy Web UI tables in
  [qbittorrent#24558](https://github.com/qbittorrent/qBittorrent/issues/24558).
- qBittorrent users requested pagination in
  [qbittorrent#23127](https://github.com/qbittorrent/qBittorrent/issues/23127).
- qBittorrent users requested counts-only API endpoints in
  [qbittorrent#23449](https://github.com/qbittorrent/qBittorrent/issues/23449).
- qBittorrent tracks broad Web UI parity work in
  [qbittorrent#9796](https://github.com/qbittorrent/qBittorrent/issues/9796)
  and [qbittorrent#22111](https://github.com/qbittorrent/qBittorrent/issues/22111).
- Transmission users requested grouping in the Web UI in
  [transmission#3813](https://github.com/transmission/transmission/issues/3813)
  and sorting by time left in
  [transmission#8237](https://github.com/transmission/transmission/issues/8237).

SwarmOtter feature shape:

- Add server-side list filtering, sorting, grouping, pagination, and counts.
- Add a virtualized torrent table in the Web UI.
- Add saved filters for state, label, tracker, health, storage root, and
  performance condition.
- Add bulk operations with clear confirmations for destructive actions.
- Add details drawers or pages that do not require reloading the full list.

Acceptance direction:

- The torrent list must remain responsive with large torrent counts.
- API responses must support incremental UI refreshes without sending the full
  world on every poll.
- Bulk operations must use the same API permissions and confirmation semantics
  as single-torrent operations.

### Ecosystem Compatibility API

Problem: the Sonarr/Radarr/Flood ecosystem and self-hosting automation pipelines
expect qBittorrent-compatible or Transmission-compatible API surfaces. SwarmOtter's
native API cannot be adopted by those tools without a compatibility shim layer.

Requested elsewhere:

- Deluge, rTorrent/Flood, and aria2 are used in multi-tool pipelines that
  require API compatibility with mainstream clients.
- Sonarr and Radarr (as of 2026) support qBittorrent-compatible API with Bearer
  API-key auth mode alongside the classic session-cookie flow.
- Flood UI targets rTorrent's RPC interface.
- The `amutorrent`/`got3nks` project demonstrates a pattern of supporting both
  Bearer API-key and session-cookie auth as compatibility shims.

SwarmOtter feature shape:

- Add opt-in qBittorrent `/api/v2` and Transmission RPC compatibility shims
  layered over the native API.
- Support multiple auth modes: Bearer API key (Sonarr/Radarr preferred, as of
  2026), session cookie (`/api/v2/auth/login`), and HTTP Basic Auth as fallback.
- Map category/label semantics, completion/import semantics, and torrent-state
  transitions to match client expectations. qBittorrent's category model maps
  to SwarmOtter policy profiles and labels with explicit per-compatibility-
  endpoint parity; Sonarr/Radarr workflows that key off qBittorrent categories
  work without manual translation.
- Map Sonarr/Radarr "import" semantics: the import path returns the
  expected torrent state transition, label, and download root so *arr
  pipelines complete the import step without custom scripting.
- Native API remains the source of truth; compatibility endpoints delegate to it.
- No indexer, search, or content-discovery surface is exposed through
  compatibility endpoints.

Acceptance direction:

- Compatibility endpoints reuse native permissions and network containment;
  no bypass paths are introduced.
- Auth mode support documented; parity matrix published.
- Integration tests run against representative *arr/Flood flows.
- No bundled infringing trackers, indexers, or discovery integrations.
- Implementing this requires an ADR (new compatibility surface + auth model).

### Per-Profile / Per-Torrent Network-Path Binding

Problem: multi-profile and multi-tenant deployments need different network paths
per class of torrent, but SwarmOtter currently applies a single contained path
globally. Per-profile and per-torrent assignment would enable stricter
containment and operational isolation.

Requested elsewhere:

- rTorrent/Flood multi-user deployments need per-session network isolation.
- Deluge multi-profile setups benefit from per-profile routing rules.
- Self-hosting VPN routing patterns assign different tunnel endpoints to
  different containers or workload classes.

SwarmOtter feature shape:

- Assign a contained network path (network namespace, VPN endpoint, or
  interface) per profile, label, or torrent.
- Policy profiles (existing P0) gain a network-path binding field; the
  `NetworkBinder` containment layer enforces assignment.
- Deterministic resolution: torrent → label → profile → global default.
- Each path fails closed independently; no torrent may egress outside its
  assigned contained path.

Acceptance direction:

- No torrent may egress outside its assigned contained path.
- All measurements and sockets go through the binder; effective path is
  explainable in the API/UI.
- When implemented, `design/vpn-network-containment.md` must be updated to
  document per-path fail-closed conditions.
- Implementing this requires an ADR (changes containment behavior).

### Multi-User / Multi-Tenant Support

Problem: shared-server and seedbox deployments need per-user isolation, quotas,
and role-based access control. Running separate daemon instances per user is
wasteful and operationally complex. This is the #1 most-requested missing feature
across the qBittorrent, rTorrent, and Flood ecosystems.

Requested elsewhere:

- qBittorrent's multi-user WebUI request
  [qbittorrent#3327](https://github.com/qbittorrent/qBittorrent/issues/3327)
  has been open since 2015 and is one of the highest-voted issues.
- Flood ships built-in multi-user support with per-user backend connections.
- rTorrent + ruTorrent multi-user setups are standard in seedbox platforms
  (QuickBox, Swizzin, Saltbox).
- Deluge's thin-client architecture supports per-user daemon connections.

SwarmOtter feature shape:

- Add role-based access control: read-only, operator, and admin roles.
- Add per-user torrent isolation: each user sees only their own torrents.
- Add per-user quotas: storage, active torrents, bandwidth caps.
- Add per-user storage roots: each user has their own download,
  incomplete, and state directories; roots are configurable per user and
  can be combined with per-profile network-path binding (existing P0)
  for full per-user isolation on a shared host.
- Add per-user API keys with scoped permissions.
- Integrate with policy profiles (existing P0) for per-user default settings.
- Integrate with per-profile network-path binding (existing P0) for per-user
  network isolation on shared hosts.
- Add user management via API and Web UI.

Acceptance direction:

- Multi-user support must not weaken network containment; each user's traffic
  remains bound to their assigned network path, and per-user storage roots
  are enforced at the daemon level rather than at the API layer alone.
- User isolation must be enforced at the daemon level, not just the API layer.
- Per-user storage roots and per-user network paths together constitute the
  seedbox-grade isolation model that shared-server deployments require.
- Implementing this requires an ADR (new auth model + user isolation semantics).

### Protocol Encryption / MSE-PE (BEP 8)

Problem: every mainstream torrent client (Transmission, qBittorrent, Deluge,
BiglyBT) implements Message Stream Encryption / Protocol Encryption
(BEP 8). Many peers refuse plaintext handshakes, and private trackers
commonly *require* encrypted connections. ISPs routinely throttle or
shape plain-text BitTorrent handshakes. SwarmOtter's network containment
model constrains routing, not wire-level obfuscation: a contained peer
connection is still a plaintext handshake on the wire. Without MSE/PE
SwarmOtter cannot fully interoperate with a large fraction of swarms, and
the comparison matrix should reflect that honestly rather than imply
parity. This is arguably a core interoperability gap rather than a
differentiator.

Requested elsewhere:

- Transmission, qBittorrent, Deluge, and BiglyBT all ship MSE/PE and have
  for years; it is treated as table stakes.
- Private tracker ecosystems commonly require encryption; peers that do
  not offer it are rejected at the handshake.
- ISPs commonly throttle or deprioritize traffic identified by the
  plaintext BitTorrent handshake, affecting legitimate Linux ISO and
  open-source release distribution.

SwarmOtter feature shape:

- Implement BEP 8 obfuscated handshake plus the encrypted-stream
  negotiation (plaintext fallback, RC4/AES encrypted modes per BEP 8).
- Make the encryption mode configurable: disabled, opportunistic
  (plaintext handshake fallback allowed), and forced (refuse plaintext).
- Per-profile (existing P0) and per-torrent overrides for encryption
  mode.
- Encryption negotiation goes through the existing contained peer
  connection path; no separate socket creation.
- Surface the negotiated encryption state per peer in the API, UI, and
  the client-identity rollup (existing P1).

Acceptance direction:

- Framing is interoperability and wire-level integrity, consistent with
  the lawful-use posture already applied to VPN/NIC containment. This is
  not piracy-evasion framing; SwarmOtter does not advertise or document
  this as a way to evade copyright enforcement.
- Encryption never weakens network containment; the encrypted stream runs
  over the existing contained TCP/uTP transport.
- Forced-encryption mode must not silently fall back to plaintext; it
  must refuse or close the connection.
- Local swarm fixtures exercise encrypted, opportunistic, and
  forced-encryption handshakes before the default mode is set.
- When implemented, `design/COMPARISON.md` must add a "Peer encryption
  (MSE/PE)" row that previously showed an unstated gap; the matrix stays
  truthful.
- Implementing this requires an ADR (new wire-protocol surface and a
  default-mode decision with interop trade-offs).

## P1 Features

### Metadata-First Magnet Preview and Intake Rules

Problem: users want magnet links to behave like `.torrent` files: inspect
metadata, choose files, avoid unwanted suffixes, and decide whether to start
downloading.

Requested elsewhere:

- Transmission requests include magnet file selection
  [transmission#1611](https://github.com/transmission/transmission/issues/1611),
  torrent-only magnet intake
  [transmission#2366](https://github.com/transmission/transmission/issues/2366),
  metadata before start
  [transmission#7330](https://github.com/transmission/transmission/issues/7330),
  suffix-based exclusions
  [transmission#7399](https://github.com/transmission/transmission/issues/7399),
  file-tree filtering
  [transmission#2399](https://github.com/transmission/transmission/issues/2399),
  BEP 53 select-only magnet URI
  [transmission#5582](https://github.com/transmission/transmission/issues/5582),
  and `x.pe` magnet peer support
  [transmission#8793](https://github.com/transmission/transmission/issues/8793).
- qBittorrent users requested stop conditions while still downloading metadata
  in [qbittorrent#23674](https://github.com/qbittorrent/qBittorrent/issues/23674).

SwarmOtter feature shape:

- Add an add-as-preview mode for magnets: fetch metadata, show file tree, do
  not download payload until the user or API explicitly starts it.
- Add reusable file exclusion rules by suffix, glob, size, and path segment.
- Add metadata-only save/export for lawful metadata workflows.
- Support BEP 53 and `x.pe` where compatible with containment.

### File Cleanup, Trash, and Retention Safety

Problem: users need cleanup tools, but delete behavior is one of the easiest
ways to lose data.

Requested elsewhere:

- qBittorrent requests include mirror cleanup
  [qbittorrent#23575](https://github.com/qbittorrent/qBittorrent/issues/23575),
  unwanted-file deletion
  [qbittorrent#23353](https://github.com/qbittorrent/qBittorrent/issues/23353),
  "do not download and delete"
  [qbittorrent#24102](https://github.com/qbittorrent/qBittorrent/issues/24102),
  and moving `.torrent` records to trash
  [qbittorrent#24601](https://github.com/qbittorrent/qBittorrent/issues/24601).
- Transmission users requested daemon trash-directory support in
  [transmission#1722](https://github.com/transmission/transmission/issues/1722)
  and partial-file suffix configuration in
  [transmission#6513](https://github.com/transmission/transmission/issues/6513).

SwarmOtter feature shape:

- Add a local trash/quarantine policy for torrent records and deleted payloads.
- Add cleanup previews that show exactly which paths would be deleted.
- Add stale partial cleanup for files no longer selected or no longer present
  in updated metadata.
- Add retention policies by profile, label, and state.

### Tracker and Peer Operations Workbench

Problem: users and operators need more than "tracker ok" when diagnosing slow
or stale torrents.

Requested elsewhere:

- Transmission requests include tracker whitelist
  [transmission#996](https://github.com/transmission/transmission/issues/996),
  tracker priority
  [transmission#6425](https://github.com/transmission/transmission/issues/6425),
  known peers via RPC
  [transmission#8326](https://github.com/transmission/transmission/issues/8326),
  webseed visibility
  [transmission#8413](https://github.com/transmission/transmission/issues/8413),
  and Web UI tracker editing
  [transmission#5234](https://github.com/transmission/transmission/issues/5234).
- qBittorrent requests include tracker retries columns and tracker cleanup
  actions in [qbittorrent#24013](https://github.com/qbittorrent/qBittorrent/issues/24013)
  and [qbittorrent#24014](https://github.com/qbittorrent/qBittorrent/issues/24014).

SwarmOtter feature shape:

- Add tracker priority, allow/deny policies, retry counts, last error, last ok,
  peer yield, and latency to the API and UI.
- Add peer source attribution: tracker, DHT, PEX, direct, or resume.
- Add webseed visibility with byte contribution and error history.
- Add tracker maintenance actions: retry now, disable, remove failed above
  threshold, and copy diagnostics.

### Secure Remote-Operations Hardening

Problem: headless deployments need safe remote access without pushing users
into fragile reverse-proxy or certificate workflows.

Requested elsewhere:

- qBittorrent users requested Let's Encrypt support in
  [qbittorrent#7172](https://github.com/qbittorrent/qBittorrent/issues/7172)
  and Web UI binding to a Unix domain socket in
  [qbittorrent#24308](https://github.com/qbittorrent/qBittorrent/issues/24308).
- Transmission users requested modern CSRF mitigation and RPC schema
  documentation in [transmission#5899](https://github.com/transmission/transmission/issues/5899)
  and [transmission#5989](https://github.com/transmission/transmission/issues/5989).
- qBittorrent users requested duplicate error notification grouping in
  [qbittorrent#19951](https://github.com/qbittorrent/qBittorrent/issues/19951).

SwarmOtter feature shape:

- Add certificate path reload for API/UI TLS when SwarmOtter terminates TLS
  directly.
- Add optional Unix socket listener for local reverse proxies.
- Add event/log deduplication controls for noisy repeated errors.

### Safe Automation Hooks

Problem: event scripts are useful, but hidden script execution can create
security and operations risk.

Requested elsewhere:

- Transmission users requested user-invoked scripts and visible script-running
  status in [transmission#8056](https://github.com/transmission/transmission/issues/8056)
  and [transmission#6984](https://github.com/transmission/transmission/issues/6984).
- qBittorrent users requested automation around renaming and download-complete
  notifications in [qbittorrent#23550](https://github.com/qbittorrent/qBittorrent/issues/23550)
  and [qbittorrent#23603](https://github.com/qbittorrent/qBittorrent/issues/23603).

SwarmOtter feature shape:

- Add explicit allowlisted hooks for completed, errored, added, removed,
  rechecked, and user-invoked actions.
- Surface running hook state and recent hook results in the API/UI.
- Require per-hook working directory, environment allowlist, timeout, and
  output capture.
- Never bundle content-specific automations or piracy-oriented workflows.
- Add notification transports: webhook, ntfy, Apprise, and email — to extend
  automation with user-preferred delivery channels without requiring per-hook
  scripting.
- Webhook transports support HMAC-signed payloads, configurable retry with
  exponential backoff, replay protection via timestamp and nonce, and
  per-transport rate limits so a noisy upstream cannot exhaust the daemon's
  automation budget.

### Content Organization Controls

Problem: download directories become hard to navigate unless folder creation,
renaming, and path rules are deliberate.

Requested elsewhere:

- Transmission users requested forced top-level folder creation and renaming in
  [transmission#5614](https://github.com/transmission/transmission/issues/5614)
  and [transmission#8225](https://github.com/transmission/transmission/issues/8225).
- Transmission users requested multiple preset download paths and relative
  incomplete paths in [transmission#6044](https://github.com/transmission/transmission/issues/6044)
  and [transmission#6045](https://github.com/transmission/transmission/issues/6045).
- qBittorrent users requested per-torrent incomplete directory and incomplete
  suffix controls in [qbittorrent#24239](https://github.com/qbittorrent/qBittorrent/issues/24239).

SwarmOtter feature shape:

- Add a policy for always creating a top-level folder, including single-file
  torrents.
- Add save-path presets and relative path rules.
- Add per-torrent incomplete-path and partial-suffix overrides.
- Add path preview before move/rename/apply profile.

### Torrent Creation (BEP 52 v2/hybrid)

Problem: lawful distributors who create `.torrent` files from their own content
currently need external tooling. SwarmOtter could create and immediately seed
torrents as part of a distribution workflow.

Requested elsewhere:

- BiglyBT, aria2, and Deluge (via plugin) support torrent creation.
- Transmission has a long-standing torrent-creation request in
  [transmission#5794](https://github.com/transmission/transmission/issues/5794).

SwarmOtter feature shape:

- Create `.torrent` from local lawful content via API and UI.
- Support piece hashing with configurable piece size; tracker tier configuration;
  optional webseed `url-list`; private flag; v1/v2/hybrid (BEP 52) format.
- Immediate seed of created content.
- No bundled trackers, indexers, or discovery integrations.

Acceptance direction:

- Created metadata must be verifiable against the source content.
- Piece hashing must respect storage and performance controls.
- No bundled trackers or indexers.

### Superseeding / Initial Seeding (BEP 16)

Problem: first-time distributors of new lawful releases want efficient initial
seeding where pieces are distributed evenly without requiring complete download
from any single peer.

Requested elsewhere:

- BEP 16 defines initial seeding behavior.
- qBittorrent has an initial-seeding request in
  [qbittorrent#20098](https://github.com/qbittorrent/qBittorrent/issues/20098).
- BiglyBT ships initial seeding as a standard feature.

SwarmOtter feature shape:

- Per-torrent or per-profile initial-seeding mode toggle.
- Correct piece-rarity distribution per BEP 16.
- Contained upload behavior through the configured network path.

Acceptance direction:

- Piece-rarity distribution follows BEP 16 semantics.
- Upload is contained through the configured network path.

### IP Filtering / Blocklists / Peer Banning

Problem: operators need tools to filter abusive, hostile, or misbehaving peers
across all peer sources without ad hoc manual intervention.

Requested elsewhere:

- qBittorrent and Deluge ship built-in IP filtering and blocklist import.
- eMule/PeerGuardian `.dat` blocklist formats are widely used in the community.
- Manual peer banning is a common request across clients.

SwarmOtter feature shape:

- Support CIDR and range-based peer filters.
- Import eMule/PeerGuardian `.dat` blocklist formats.
- Add manual per-peer ban controls.
- Add client-ID-based peer blocking (e.g., Xunlei/Thunder, known bad actors)
  as a complement to IP-based filtering.
- Integrate with the tracker and peer operations workbench (existing P1).
- Filtering is framed as abuse mitigation and operational safety, not evasion.

Acceptance direction:

- Framing is consistent with lawful-use policy: filtering is abuse mitigation.
- Applies to all peer sources through the contained network path.
- Filters are auditable in logs and API.

### UPnP / NAT-PMP Port Forwarding

Problem: users behind NAT routers cannot accept inbound peer connections without
manual port forwarding configuration. This cripples seeding and swarm contribution
for non-VPN users. Every major client ships this as table-stakes functionality.

Requested elsewhere:

- qBittorrent, Transmission, and Deluge all ship UPnP and NAT-PMP support.
- libtorrent-rasterbar (qBittorrent's backend) has mature UPnP/NAT-PMP
  implementation.
- This is expected baseline functionality, not a differentiator.

SwarmOtter feature shape:

- Add UPnP (Universal Plug and Play) and NAT-PMP (NAT Port Mapping Protocol)
  support for automatic port mapping on supported routers.
- Map the configured peer listen port; refresh mappings on lease expiry.
- Surface mapping status in the API and network health UI.
- Respect network containment: port mappings must only be requested on the
  contained network interface, not the default route.

Acceptance direction:

- Port mapping must be opt-in and clearly surfaced.
- Mapping must respect network containment; no mappings on uncontained interfaces.
- UPnP/NAT-PMP traffic must go through the contained network path.

### SOCKS5 Proxy Support

Problem: seedbox and restricted-network users rely on SOCKS5 proxies as a
simpler alternative to full VPN/namespace containment. Many users want both
SOCKS5 and VPN containment for different use cases.

Requested elsewhere:

- Transmission [#1250](https://github.com/transmission/transmission/issues/1250)
  is a top-voted feature request.
- qBittorrent and Deluge ship built-in SOCKS5 proxy support.
- Common in seedbox deployments where VPN is not available or practical.

SwarmOtter feature shape:

- Add optional SOCKS5 proxy configuration for torrent traffic.
- Route peer TCP connections, tracker announces, and webseed requests through
  the proxy.
- Support authentication (username/password) and unauthenticated modes.
- Proxy configuration is per-profile (existing P0) for multi-path deployments.
- SOCKS5 proxy is distinct from network containment; both can coexist with
  clear precedence rules.

Acceptance direction:

- Proxy configuration must be explicit and auditable.
- When both SOCKS5 and network containment are configured, containment takes
  precedence; proxy traffic must still go through the contained path.
- DNS resolution for proxy hostname must respect containment.

### Seed Prioritization (Low-Seed First)

Problem: torrents with few seeds are at risk of dying. Clients should prioritize
seeding bandwidth toward torrents that need it most, improving overall swarm
health and distribution efficiency.

Requested elsewhere:

- qBittorrent [#9063](https://github.com/qbittorrent/qBittorrent/issues/9063)
  requests seed-priority-based seeding.
- Transmission users have discussed seed-count-aware seeding policies.
- Aligns with SwarmOtter's lawful-distribution mission: keeping lawful swarms
  healthy.

SwarmOtter feature shape:

- Add a seed-priority seeding mode that allocates upload bandwidth
  proportionally to torrents with the fewest available seeds.
- Configurable per-torrent and per-profile (existing P0).
- Complements (does not replace) ratio-based and time-based seeding limits.
- Surface seed-count data from trackers, DHT, and PEX in the API and UI.

Acceptance direction:

- Seed-priority mode is opt-in and clearly documented.
- Does not override explicit ratio or time-based stop conditions.
- All seeding traffic remains contained through the configured network path.

### OpenAPI Specification & Interactive API Docs

Problem: the Ecosystem Compatibility API (P0) and native API need clear,
machine-readable documentation for automation and integration. Flood's
auto-generated Swagger UI is the gold standard for torrent client APIs.

Requested elsewhere:

- Flood ships OpenAPI spec with interactive Swagger UI explorer.
- Deluge and qBittorrent have community-maintained API documentation.
- Self-hosting automation pipelines (Sonarr, Radarr, cross-seed) need accurate
  API specs for integration testing.

SwarmOtter feature shape:

- Generate OpenAPI 3.x specification for the native `/api/v1` surface.
- Generate OpenAPI specification for qBittorrent `/api/v2` and Transmission RPC
  compatibility surfaces.
- Serve interactive Swagger UI at `/api/docs` for exploration and testing.
- Publish JSON Schema for configuration, request, and response types.
- Keep specs in sync with implementation via code generation or compile-time
  verification.

Acceptance direction:

- Specs must be accurate and versioned alongside the API.
- Compatibility surface specs must document parity gaps explicitly.
- No indexer, search, or content-discovery endpoints are documented.

### User-Configured Lawful RSS Feeds

Problem: lawful distributors and self-hosting operators use RSS to distribute
updates to lawful content (Linux ISOs, open-source releases, public-domain
media, legal datasets). These workflows require a local RSS ingestion surface
separate from bundled discovery tools.

Requested elsewhere:

- Deluge ships an RSS plugin for lawful feed ingestion.
- rTorrent supports RSS intake via external scripts and plugins.
- Self-hosting communities use RSS feeds from Linux distributions, open-source
  projects, and legal media archives.

SwarmOtter feature shape:

- Support user-configured RSS feed URLs as a lawful-distribution intake channel.
- Feed items are matched against user-defined include/exclude patterns.
- Matched items flow into the standard add workflow (metadata-first preview,
  profile assignment, file exclusion rules).
- Users are responsible for feed selection; no bundled infringing or
  piracy-oriented feeds are included.

Acceptance direction:

- Only user-supplied feeds are supported; no bundled indexers, search plugins,
  or content-discovery integrations.
- Feed ingestion falls within project scope as defined in `design/content-policy.md`
  and `design/lawful-use.md`; no bundled infringing feeds are included.
- Implementing this requires an ADR (new feature surface + project scope decision).

### Native Cross-Seed & Hardlink-Aware Storage

Problem: self-hosting operators maintain large libraries with hardlink-aware
storage layouts. The cross-seed pattern (matching on-disk data to new torrents
by piece layout/size and linking instead of re-downloading) is an established
workflow that currently requires an external tool.

Requested elsewhere:

- cross-seed (external tool) is widely used in the self-hosting community and
  pairs with hardlink-based storage layouts.
- BiglyBT has internal cross-seed-style functionality.
- Self-hosting guides recommend hardlink-aware paths for seedbox efficiency.

SwarmOtter feature shape:

- Match existing on-disk data to new lawful torrents by piece layout and piece
  size.
- Link instead of re-download where the filesystem supports it.
- Integrate with disk-aware storage optimizer (existing P0).
- Explicit user-visible link vs copy behavior; no silent data loss.

Acceptance direction:

- No silent data loss; link vs copy decision is explicit and auditable.
- Implementing this likely requires an ADR (persistent storage conventions).

### Trust and Provenance Signals for Torrents and Trackers

Problem: lawful distributors and operators managing legitimate content need
to know whether a torrent or a tracker host is one they can rely on. Mainstream
clients treat every `.torrent` and every tracker host as equally trusted. For
institutional, educational, public-sector, and enterprise-adjacent deployments
this is a real gap and a real differentiator for a lawful-use daemon.

Requested elsewhere:

- eMule/PeerGuardian blocklists encode CIDR ranges of untrusted peers but do
  not encode tracker host trust.
- Linux distributions, open-source projects, and public archives sign their
  release artifacts; there is no standard `signed-torrent` workflow in
  mainstream clients.
- Operators have asked for tracker whitelists and trusted-tracker workflows
  in transmission and qBittorrent discussions.

SwarmOtter feature shape:

- Add a per-tracker trust state: trusted, neutral, untrusted, or blocked,
  with an explicit operator-controlled source (manual, imported, learned).
- Support tracker allowlists and denylists integrated with the existing IP
  filtering workbench (P1) and policy profiles (P0).
- Surface, for every torrent, the trust state of every active tracker and
  the effective upload/download policy the daemon is applying because of it.
- Support a content provenance mode: import `.torrent` files and magnets
  with an attached cryptographic signature (current-signing-community formats
  plus a documented SwarmOtter-side verification helper) so operators can
  verify a release matches a publisher-signed manifest before intake.
- The provenance mode is opt-in and is not a new bundled indexer or content-
  discovery surface; it verifies what the operator already has.

Acceptance direction:

- Trust state is auditable in the API, the UI, and the operator audit log.
- No tracker host is silently blocked; changes to trust state are surfaced.
- Provenance verification never weakens containment; signature fetch follows
  the same network path as webseeds.
- Implementing this likely requires an ADR (introduces a new metadata
  surface and an operator trust model).

### Operator Audit Log for Torrent Lifecycle Events

Problem: long-horizon observability (existing P2) covers metrics. Operators
of shared servers, seedbox platforms, and institutional deployments also need
**who-did-what-when** records for torrent lifecycle events. Mainstream
clients keep at most a small activity log; there is no structured audit
trail.

Requested elsewhere:

- qBittorrent, rTorrent, Deluge, and Transmission all keep short, plain-text
  activity logs. None ship a structured, exportable, tamper-evident audit
  trail.
- Flood ships multi-user backends with no per-user audit surface beyond log
  files.
- Self-hosting operators running Sonarr/Radarr against a torrent daemon
  frequently want to know which user added, removed, or modified which
  torrent at which time.

SwarmOtter feature shape:

- Emit a structured audit event for every privileged or destructive
  operation: add, remove, delete-data, move, recheck, profile change,
  tracker edit, settings change, user management, automation hook
  execution, and network-binding state change.
- Each event includes actor (user, API key, system), timestamp, target
  (torrent, profile, storage root, user, setting), operation, parameters,
  outcome, and the request identifier that produced it.
- Persist to a dedicated append-only log file with optional hash chaining
  so tampering is detectable.
- Support operator-controlled retention and export (JSON Lines) and
  integration with syslog and OpenTelemetry (P2) as additional sinks.
- Read access is permissioned; only operators with the audit role see the
  full event stream.

Acceptance direction:

- Audit events never include payloads, file contents, or peer IPs by
  default; info hashes, profile names, setting keys, and user identifiers
  are sufficient for compliance and operations.
- Hash chaining is documented; missing or inconsistent chains are reported
  in the Doctor and in the audit API.
- This is the compliance story that combines with Multi-User Support (P0)
  for shared-server and seedbox deployments.

### Explainability API: Structured Reasons for Non-Trivial Decisions

Problem: operators and advanced users want a single, machine-readable way
to ask "why is this torrent dead?", "why was this add request rejected?",
"why is my path fail-closed right now?", and "why is global throughput
limited right now?" Today the data is spread across the API, the logs, the
Doctor report, and individual torrent stats. Mainstream clients have no
unified explainability surface.

Requested elsewhere:

- qBittorrent, rTorrent, Deluge, and Transmission all surface decisions as
  human-readable log lines or tooltips; none expose structured reasons
  suitable for automation.
- Sonarr/Radarr operators want machine-readable reasons for failed imports.

SwarmOtter feature shape:

- Add a `/api/v1/explain/*` surface that returns structured reasons for
  non-trivial decisions: per-torrent, per-add, per-network-path, per-
  bandwidth-cap, per-storage-root, and per-autopilot action.
- Each reason includes a stable code, a human-readable message, the
  measured inputs that produced the decision, the timestamp of the last
  decision, and the relevant subsystem.
- The autopilot "why is this slow?" report, the disk-aware storage
  optimizer decisions, the per-path fail-closed states, and the bandwidth
  scheduler all share the same explainability shape.
- Reasons are versioned; downstream automation can rely on stable codes.

Acceptance direction:

- The same code and shape appear in logs, the API, the UI, and the audit
  log so operators and automation never have to reconcile different
  vocabularies.
- No reason exposes peer IPs or user content; only operational state.
- This is a unique differentiator; no mainstream client offers a unified
  explainability surface.

### Container / Sandbox-First Deployment Story

Problem: the positioning in `design/COMPARISON.md` calls SwarmOtter a
"Linux/server and homelab torrent daemon," but the deployment artifacts in
`docs/` are still a config file, a systemd unit, a Dockerfile, and an nginx
example. Rootless container deployment, OCI image distribution, Helm/
Compose charts, and read-only-filesystem operation are not first-class
deliverables. For a daemon that targets server and homelab users, this is
a real gap.

Requested elsewhere:

- Transmission, qBittorrent, rTorrent, and Deluge all ship native packages
  and have community container images; none of those images is a
  first-class artifact of the upstream project.
- Sonarr/Radarr and the *arr ecosystem treat container deployment as the
  default and require rootless operation and read-only filesystems.

SwarmOtter feature shape:

- Treat the OCI image as a first-class artifact: built and tested in CI,
  published to a documented registry, signed with cosign, and accompanied
  by a SBOM.
- Document and test rootless operation with the existing network
  containment model; add a non-root user and capability set to the
  Dockerfile and verify it.
- Document and test read-only-filesystem operation: state, logs, and
  download roots live on mounted volumes only; the daemon refuses to
  write to `/tmp` or the container filesystem.
- Publish and test a Helm chart and a Compose file as first-class
  artifacts in this repository, with the same containment configuration
  examples that the systemd unit ships.
- Document and test Podman, Docker, and Kubernetes deployments, including
  NetworkPolicy examples for the control plane.

Acceptance direction:

- The CI pipeline builds, signs, and tests the OCI image on every
  release; image digest and SBOM are published alongside the binary.
- Helm chart and Compose file are versioned in lockstep with the daemon
  and are covered by deployment tests.
- Container deployment does not weaken containment; namespace and
  capability configuration is part of the deployment contract.

### Production Health / Availability Surface

Problem: SwarmOtter already has a Doctor/health report and a Prometheus
metrics endpoint, but neither of them is suitable for an orchestrator's
liveness/readiness probe, and neither gives an operator a synthetic
end-to-end check of the data plane. qBittorrent, rTorrent, Deluge, and
Transmission have no equivalent surface; this is a clean differentiator
for a daemon that targets production server deployment.

Requested elsewhere:

- qBittorrent's status endpoints are ad hoc; no liveness/readiness split.
- Flood ships API exploration but no synthetic health checks.
- Kubernetes, Consul, Nomad, and the cloud-native ecosystem treat
  liveness/readiness probes as a baseline expectation.

SwarmOtter feature shape:

- Add explicit `/healthz/live` and `/healthz/ready` HTTP endpoints. Live
  returns 200 as long as the daemon process is up and the API is serving.
  Ready returns 200 only when the data plane is operational and at least
  one torrent can be added and downloaded through the contained network
  path.
- Add a synthetic end-to-end check torrent: a small, locally generated
  torrent the daemon advertises to itself through the contained path on
  a configurable interval; ready=true requires the check torrent to
  complete a full download round-trip.
- Add SLO-style summaries: rolling uptime, ready ratio, and a configurable
  ready-ratio alert threshold.
- Honor Kubernetes liveness/readiness conventions and document probe
  configurations in the deployment docs.

Acceptance direction:

- Liveness and readiness are independent; a torrent stuck in
  `network_blocked` does not trigger a liveness failure.
- The synthetic check torrent is generated locally; it does not depend on
  any third-party tracker, peer, or content.
- Ready and live endpoints are unauthenticated by default but bound to
  the control plane only; the data plane is never probed by the orchestrator.

### Client-Identity Fingerprinting and Rollups

Problem: operators running legal swarms (Linux ISOs, open-source releases,
public archives) want to know what other clients are connecting to their
seeders so they can prioritize compatibility fixes and understand their
contribution to the broader ecosystem. Most clients display a peer-client
string per peer but offer no rollups; the data is effectively invisible
once a swarm has more than a few dozen peers.

Requested elsewhere:

- qBittorrent displays the peer client string per peer; no rollup view.
- BiglyBT ships per-peer client visibility but no aggregate rollup.
- rTorrent's peer view is text-based and does not roll up.

SwarmOtter feature shape:

- Parse and bucket peer client identifiers (Azureus-style `AZ####` and
  other common forms) at session and torrent boundaries.
- Surface per-torrent and per-tracker rollups: top client families,
  percentage share, useful-vs-choked breakdown, and historical trend.
- Add a UI section and API endpoint for the rollup; integrate with the
  per-torrent health score (existing delivered feature) so a swarm
  dominated by legacy or misbehaving clients surfaces as a health
  factor.
- Client identification is purely informational; it never changes
  download/upload behavior, and it is not used to enforce policy.

Acceptance direction:

- Rollups are computed locally from the engine's peer log; no third-
  party lookup.
- Operators can disable the rollup or restrict it to specific torrents
  if they prefer.
- This complements the IP filtering / blocklists workbench (P1) for
  abuse mitigation.

### Filesystem Snapshot Integration

Problem: the disk-aware storage optimizer (P0) and the Btrfs/CoW
discussion in the backlog already recognize that torrent clients run on
sophisticated filesystems. There is no native integration with filesystem
snapshots, so an operator who wants rollback for a torrent root or a
state directory has to script it externally. This is invisible in
mainstream clients and is a natural differentiator on Linux.

Requested elsewhere:

- qBittorrent users have requested CoW-aware behavior but not snapshot
  integration; see qBittorrent CoW discussion in the disk-aware storage
  optimizer entry.
- Self-hosting operators using Snapper, ZFS, or Btrfs subvolumes for
  `/var/lib/swarmotter` and download roots have to script snapshot
  workflows by hand.

SwarmOtter feature shape:

- Detect filesystem type per configured storage root and per state
  directory.
- Add opt-in snapshot hooks: pre-update, post-update, pre-delete-data,
  and operator-invoked.
- Document and test the integration with Btrfs subvolumes, Snapper
  timelines, and ZFS snapshots; expose a snapshot history view in the
  Doctor and the API.
- Snapshot integration is opt-in per root; the daemon never creates
  snapshots unless explicitly configured to do so.

Acceptance direction:

- Snapshot creation is never on the hot path; it is invoked at
  configurable points and never blocks a piece write.
- No bundled Snapper or ZFS tooling is required; the daemon invokes
  the operator-configured command.
- A failing snapshot is reported and does not cause data loss; the
  underlying torrent operation continues.

### HTTP / HTTPS Proxy Support

Problem: the backlog already covers SOCKS5 (P1), but corporate and
egress-filtered environments frequently expose only HTTP/CONNECT proxies.
Users in those environments cannot route SwarmOtter traffic without an
HTTP proxy option. qBittorrent and aria2 both support HTTP proxies
alongside SOCKS5.

Requested elsewhere:

- qBittorrent ships HTTP proxy support alongside SOCKS5.
- aria2 ships HTTP/HTTPS proxy support as a first-class option.
- Corporate and educational networks commonly block SOCKS5 but allow
  authenticated HTTP egress proxies.

SwarmOtter feature shape:

- Add optional HTTP/CONNECT (and HTTPS CONNECT) proxy configuration for
  torrent traffic.
- Route peer TCP connections, tracker announces, webseed requests, and
  DHT where applicable through the configured HTTP proxy.
- Support authenticated and unauthenticated modes.
- Per-profile (existing P0) proxy configuration for multi-path
  deployments.
- HTTP proxy is distinct from SOCKS5 and from network containment; all
  three can coexist with documented precedence rules.

Acceptance direction:

- Proxy configuration is explicit and auditable.
- When both HTTP proxy and network containment are configured,
  containment takes precedence; proxy traffic still goes through the
  contained path.
- DNS resolution for the proxy hostname respects containment.
- Implementing this may share the connection-egress abstraction with the
  existing SOCKS5 work (P1).

### Scriptable CLI (`swarmotterctl`)

Problem: SwarmOtter's API-first posture targets automation, but the only
operator interface beyond the API is the Web UI. Operators working over
SSH, in CI pipelines, or in `*arr`-style automation want a lightweight
scriptable CLI that mirrors the most common daemon operations without a
browser or a full TUI. The TUI entry (P3) mentions a `swarmotterctl`
alternative; pulling the CLI out as its own item lets it ship sooner and
reinforce the Compatibility API (P0) and Sonarr/Radarr automation story.

Requested elsewhere:

- `transmission-remote` is Transmission's long-standing scriptable CLI.
- rTorrent and aria2 are CLI-first.
- Self-hosting operators routinely script torrent operations from shell.

SwarmOtter feature shape:

- Add a `swarmotterctl` binary that talks to the daemon via the same
  REST API as the Web UI; no direct daemon state access.
- Cover the high-frequency operations: add magnet/torrent, list
  torrents (with filters and sorting), pause/resume/stop, remove,
  recheck, reannounce, set per-torrent limits, show details, show health.
- Machine-readable JSON output mode for scripting and piping.
- Human-readable default output for interactive SSH use.
- Reuses the same auth (API keys, scoped permissions) and Multi-User
  (existing P0) model as the API.

Acceptance direction:

- All CLI operations go through the API; no separate daemon code path.
- JSON output is stable and versioned alongside the API.
- The CLI is a first-class build artifact and documented in `docs/`.
- Implementing this may require an ADR (new user-facing binary and an
  output-stability contract).

### Seedbox Pre-Seed Warm-Up

Problem: when a lawful release is newly created and seeded, the first
peers to connect find a seeder that has not yet read or hashed its pieces,
so the first serving round is slow and the swarm's initial health looks
poor. Pre-reading and pre-hashing all pieces *before* the torrent is
announced lets the first peer be served instantly and improves the
measured health of a fresh swarm. No mainstream client markets this as
a deliberate first-distribution optimization.

Requested elsewhere:

- BiglyBT has related pre-seed and swarm-warmup concepts.
- Superseeding / initial seeding (existing P1) benefits from a warm
  seeder because piece distribution is the bottleneck.
- Legal distributors of Linux ISOs, open-source releases, and datasets
  care about first-hour swarm health.

SwarmOtter feature shape:

- Add an optional pre-seed warm-up mode: when a torrent is added in a
  seeding-from-complete state (created content or re-verified complete
  data), pre-read and verify all pieces in the background before the
  tracker announce and DHT announce go out.
- Integrate with superseeding / initial seeding (existing P1) and the
  disk-aware storage optimizer (existing P0) so warm-up respects disk
  pressure and concurrency limits.
- Surface warm-up progress and completion in the API, UI, and the
  explainability API (existing P1).

Acceptance direction:

- Warm-up is opt-in and never blocks a user-initiated start.
- Warm-up respects disk pressure and never degrades other active
  torrents.
- Warm-up traffic stays on the contained network path (it is local I/O
  plus optional local hash, not network egress).

### Idempotent Re-Add / Content-Addressed Import

Problem: operators re-adding a torrent whose data already exists on disk
are forced through a full re-download or full re-verify cycle even when
nothing has changed. This is friction for large libraries and for the
cross-seed (existing P1/P2) workflow. Recognizing that the on-disk
content already satisfies the torrent and skipping re-download/re-verify
automatically reduces operator load and disk wear.

Requested elsewhere:

- qBittorrent re-add workflows require manual re-verify steps.
- cross-seed (external tool) users routinely re-add matching torrents and
  want instant reuse of existing data.
- Self-hosting operators migrating or restoring libraries want re-add to
  be a no-op when data is already present.

SwarmOtter feature shape:

- On add, detect whether the target data already exists at a configured
  or inferred path and matches the expected piece layout.
- Skip re-download where data is present and verified; skip full re-verify
  where the fast-resume hash matches.
- Surface the recognition decision in the explainability API (existing
  P1): "re-add recognized existing complete data, skipped verify."
- Integrate with cross-seed (existing P1/P2) and the disk-aware storage
  optimizer (existing P0).

Acceptance direction:

- Recognition is conservative: when in doubt, verify; never silently
  mark unverified data as complete.
- The decision is auditable and explainable.
- No silent data loss; a misrecognized re-add falls back to normal
  download/verify.

### Durable State Store (SQLite)

Problem: SwarmOtter's current persistent state is built from per-torrent
JSON resume files plus an in-memory registry. This works for v1.0.0 but
does not scale cheaply for the operations that the backlog already
targets: the large-library operations console (P0), the operator audit
log (P1), long-horizon observability (P2), and queue/health history all
want indexed, queryable history. Reconstructing that history from resume
files on every restart is wasteful and slow for thousands of torrents.
A single durable store would make those features far cheaper and more
robust.

Requested elsewhere:

- No mainstream torrent client ships a queryable historical state store;
  this is a SwarmOtter opportunity enabled by the API-first, server
  positioning.
- Self-hosting operators managing large libraries expect fast list,
  filter, and history queries that resume files do not provide.

SwarmOtter feature shape:

- Introduce an optional durable state store (SQLite) as the backing
  store for the registry, queue state, health snapshots, audit events,
  and rolling metrics.
- Keep per-torrent fast-resume files as the authoritative recovery format;
  the store is a queryable index and history layer, not a replacement
  for fast resume.
- Provide migration from the resume-file-only model so existing
  deployments upgrade without losing state.
- Integrate with long-horizon observability (existing P2), the operator
  audit log (existing P1), and the large-library operations console
  (existing P0).

Acceptance direction:

- Fast resume remains the crash-recovery source of truth; the store is
  rebuildable from resume files if corrupted.
- The store never weakens network containment; it is local-only and not
  network-addressable.
- Schema changes are versioned and migrated.
- Implementing this requires an ADR (new persistent format, migration
  path, and a query model decision).

## P2 Features

### Protocol Modernization Roadmap

Problem: protocol support affects long-term compatibility and swarm reach, but
some proposals require careful dependency and architecture review.

Requested elsewhere:

- qBittorrent users requested BitTorrent protocol v3/v3.1 and BitTorrent v2
  swarm preference in [qbittorrent#23421](https://github.com/qbittorrent/qBittorrent/issues/23421)
  and [qbittorrent#24600](https://github.com/qbittorrent/qBittorrent/issues/24600).
- Transmission users requested BEP 47 padding/extended attributes, BEP 55
  holepunch, and IPv6 pinhole support in
  [transmission#3387](https://github.com/transmission/transmission/issues/3387),
  [transmission#3705](https://github.com/transmission/transmission/issues/3705),
  and [transmission#993](https://github.com/transmission/transmission/issues/993).

SwarmOtter feature shape:

- Track BEP support gaps in a protocol compatibility matrix.
- Prioritize changes that improve lawful public swarms and do not compromise
  containment.
- Add local-swarm fixtures before enabling new protocol behavior by default.
- BEP 52 v2/hybrid handling is a related effort: create (P1) and consume (P2)
  v2-format torrents alongside v1 and hybrid formats; see Torrent Creation (P1).

### Long-Horizon Observability

Problem: current state is useful, but operators also need history and audit
data.

Requested elsewhere:

- Transmission users requested session upload display in
  [transmission#5591](https://github.com/transmission/transmission/issues/5591).
- qBittorrent users requested longer traffic graphs, category-change logs, and
  last-data timestamps in [qbittorrent#22832](https://github.com/qbittorrent/qBittorrent/issues/22832),
  [qbittorrent#18525](https://github.com/qbittorrent/qBittorrent/issues/18525),
  and [qbittorrent#24330](https://github.com/qbittorrent/qBittorrent/issues/24330).

SwarmOtter feature shape:

- Persist rolling torrent/session metrics for rates, bytes, health, peer
  counts, tracker outcomes, queue decisions, and storage pressure.
- Add configurable retention and export.
- Add UI charts that support operational diagnosis without becoming a heavy
  analytics product.

### Settings Search and Low-Risk UI Personalization

Problem: dense settings need fast navigation. UI customization should help
operations without distracting from function.

Requested elsewhere:

- qBittorrent users requested settings search in
  [qbittorrent#23654](https://github.com/qbittorrent/qBittorrent/issues/23654).
- qBittorrent and Transmission users requested progress/status color
  improvements in [qbittorrent#22877](https://github.com/qbittorrent/qBittorrent/issues/22877),
  [qbittorrent#22913](https://github.com/qbittorrent/qBittorrent/issues/22913),
  [transmission#4304](https://github.com/transmission/transmission/issues/4304),
  and [transmission#5648](https://github.com/transmission/transmission/issues/5648).

SwarmOtter feature shape:

- Add settings search/filter by section, key, and description.
- Add a small set of accessibility-oriented display options.
- Avoid a broad theme marketplace or styling system unless a later product
  decision justifies it.

### Sequential Download / Streaming / File Preview

Problem: users downloading large files want to preview content before the full
download completes. Sequential and priority-first fetch enables playback-oriented
and preview-oriented workflows.

Requested elsewhere:

- qBittorrent and aria2 ship sequential download controls.
- WebTorrent and Deluge support streaming playback.
- Metadata-first preview (existing P1) complements this for magnet intake.

SwarmOtter feature shape:

- Add sequential download and priority-first fetch controls per torrent and
  per file.
- Add in-place preview and verify: check media integrity before committing to
  download.
- Tie to metadata-first preview (existing P1) for magnet workflows.

Acceptance direction:

- Controls are surfaced in API and UI per torrent and per file.
- Streaming/preview behavior is deterministic and contained.

### Time-of-Day and Adaptive Bandwidth Policies

Problem: the Adaptive Swarm Performance Autopilot (P0) tunes global
bandwidth live based on measured throughput, latency, and queue state.
Operators also want time-of-day schedules (e.g. limit upload during
business hours, full-speed downloads overnight). These are two facets of
the same operational concern and should be implemented as a single
bandwidth-policy surface so the user mental model is one feature.

Requested elsewhere:

- qBittorrent, aria2, and Deluge ship bandwidth scheduling features.
- The adaptive autopilot (P0) is the SwarmOtter counterpart to live
  throughput tuning; combining it with scheduling makes the policy surface
  complete.

SwarmOtter feature shape:

- Add time-of-day alt-speed and bandwidth-limit schedules: multiple named
  schedules with start/end times and assigned bandwidth profiles.
- Schedule assignment per torrent, label, or profile.
- Schedule and adaptive autopilot share the same per-profile bandwidth
  resolution; the operator chooses the active mode per profile (adaptive,
  scheduled, or both with explicit precedence).
- Complements (does not replace) the adaptive autopilot (P0).

Acceptance direction:

- Schedules are deterministic and clearly reflected in the API and UI.
- Adaptive autopilot and scheduler interact predictably; precedence is
  documented and surfaced in the explainability API (P1).

### Backup / Restore & Bulk Import/Export

Problem: large libraries need export and import of torrent lists and state for
migration, disaster recovery, and configuration management.

Requested elsewhere:

- qBittorrent, Deluge, and Flood all ship backup/restore and export/import
  functionality for torrent state.

SwarmOtter feature shape:

- Export torrent list, metadata, state, profile assignments, and settings as a
  portable bundle.
- Import and restore from a bundle with conflict resolution.
- Support bulk torrent re-addition from exported data.

Acceptance direction:

- Export includes enough state for reproducible restoration.
- Import is idempotent and handles conflicts explicitly.

### Thin Client / Remote Session Architecture

Problem: SwarmOtter's Web UI is the only remote interface. Users coming from
Deluge expect a thin-client model where a native or web client connects to a
remote daemon via a streaming RPC protocol without SSH tunneling. This is
Deluge's defining feature and a frequent request across qBittorrent and
Transmission.

Requested elsewhere:

- Deluge's thin-client architecture (daemon/client split with native GTK GUI)
  is its primary differentiator.
- qBittorrent and Transmission users have requested remote session support.
- Flood demonstrates multi-backend Web UI connections to remote daemons.

SwarmOtter feature shape:

- Add a streaming RPC protocol (gRPC or WebSocket-based) for low-latency
  bidirectional daemon communication.
- Support remote daemon connections from the Web UI and potential future
  native clients.
- Connection authentication via API keys with scoped permissions (integrates
  with Multi-User Support, P0).
- Maintain the existing REST API for HTTP-based integration; the streaming
  protocol is an additional surface for real-time operations.

Acceptance direction:

- Remote connections must use the same auth and permission model as the
  local API.
- Network containment is not weakened; remote control-plane connections are
  separate from the torrent data plane.
- Implementing this requires an ADR (new RPC surface + connection model).

### OpenTelemetry Observability

Problem: Prometheus metrics (v1.0.0) provide point-in-time data, but cloud-native
deployments need distributed tracing, span export, and OTLP-based metrics for
integration with modern observability stacks (Jaeger, Grafana Tempo, Honeycomb).
The long-term direction is for OTLP metrics to be the primary export, with the
Prometheus scrape endpoint kept as a compatibility surface for operators who
already have a Prometheus stack.

Requested elsewhere:

- OpenTelemetry is the CNCF standard for cloud-native observability.
- Flood ships OpenAPI + Swagger UI for API exploration.
- Self-hosting operators increasingly use OpenTelemetry for multi-service
  monitoring.

SwarmOtter feature shape:

- Add OpenTelemetry tracing spans for key operations: torrent add, metadata
  fetch, tracker announce, peer connection, piece download, disk write, and
  API request handling.
- Add OTLP metrics export as an alternative to the existing Prometheus scrape
  endpoint, with the OTLP path planned as the primary export and the
  Prometheus endpoint retained for compatibility.
- Add configurable sampling and export targets.
- Integrate with Long-Horizon Observability (existing P2) for unified
  metrics + traces.
- Integrate with the Operator Audit Log (P1) so audit events can be exported
  over the same observability pipeline as metrics and traces.

Acceptance direction:

- OpenTelemetry is opt-in and does not add overhead when disabled.
- Tracing must not leak sensitive data (info hashes in spans are acceptable;
  peer IPs and file paths require configurable redaction).
- All telemetry export respects network containment.

### Cloud / Object-Storage-Backed Storage Root

Problem: institutional lawful distributors (datasets, public archives,
open-source release mirrors) increasingly keep their publishable content
in object storage (S3, S3-compatible, WebDAV) rather than on a local disk.
No mainstream torrent client treats object storage as a first-class
torrent storage root, so these operators must mount the bucket and
accept the limitations of a POSIX-over-object layer. A native
object-storage-backed storage root fits SwarmOtter's lawful-distribution
mission and complements the disk-aware storage optimizer (existing P0)
and torrent creation (existing P1).

Requested elsewhere:

- rclone mount patterns are widely used to back torrent clients with
  object storage, but they introduce a lossy POSIX emulation layer.
- Institutional dataset distribution increasingly lives in S3-compatible
  buckets.
- No mainstream client owns object-storage-backed seeding as a native
  feature.

SwarmOtter feature shape:

- Add an optional object-storage-backed storage root type for S3,
  S3-compatible, and WebDAV targets.
- Support both seeding-from-existing-object-data and
  download-to-object-storage workflows for lawful distribution.
- Reuse the existing piece-hash verification path; an object-storage
  root is treated like any other storage root by the disk-aware storage
  optimizer (existing P0).
- All object-storage access respects network containment; the bucket
  endpoint is resolved and reached through the contained network path.

Acceptance direction:

- Object-storage reads/writes never bypass the piece-verification path;
  no unverified data is served to peers.
- Containment applies to object-storage egress just as it does to peer
  traffic.
- ADR required: new I/O backend, credential handling, and containment
  implications for a non-POSIX storage root.

### Local GeoIP / ASN Peer Rollups

Problem: operators running legal swarms (Linux ISOs, open-source
releases, public archives) want to understand the geographic and ASN
distribution of the peers connecting to their seeders, both for
distribution planning and for abuse detection. Today they get a per-peer
IP string at most. Complementing the existing client-identity
fingerprinting (P1), an on-device GeoIP/ASN rollup gives legal-swarm
operators a view no mainstream client provides, with no third-party
lookup.

Requested elsewhere:

- qBittorrent and BiglyBT show per-peer IP strings but no aggregate
  geographic or ASN view.
- Self-hosting operators ask for swarm composition visibility beyond the
  client string.
- Legal distributors want to know whether their release is reaching the
  regions they expect.

SwarmOtter feature shape:

- Add an optional local GeoIP/ASN lookup (operator-supplied MaxMind or
  equivalent local database; no third-party network lookup).
- Surface per-torrent and per-tracker rollups: top countries, top ASNs,
  and the intersection with the client-identity rollup (existing P1).
- Integrate with the per-torrent health score (existing delivered
  feature) so a swarm concentrated in a single region or ASN can surface
  as a health or diversity factor.
- GeoIP/ASN rollups are purely informational; they do not change
  download/upload behavior and are not used to enforce policy.

Acceptance direction:

- No third-party network lookup; the database is operator-supplied and
  local.
- Operators can disable the rollup or restrict it to specific torrents.
- IP addresses are not exported by the rollup; only aggregated
  country/ASN counts.
- This complements the IP filtering / blocklists workbench (existing P1)
  for abuse mitigation.

### Responsive / Mobile-Friendly Web UI

Problem: SwarmOtter is positioned for homelab and server operators, a
large fraction of whom check torrent status from a phone. Nothing in the
backlog addresses touch or small-viewport operation. qBittorrent and
Transmission Web UIs are at least minimally responsive. For the stated
positioning, mobile-friendly checking is a common expectation, not a
cosmetic nicety.

Requested elsewhere:

- qBittorrent and Transmission Web UIs are minimally responsive and
  usable from a phone.
- Self-hosting operators routinely check daemon status from mobile
  devices.

SwarmOtter feature shape:

- Make the torrent list, details, health, and basic lifecycle actions
  usable on small/touch viewports.
- Keep the function-over-form posture (see ADR-0006): no heavy UI
  framework, no animation work, no theme marketplace.
- Reuse the existing large-library operations console (existing P0)
  server-side filtering and pagination so the mobile view does not load
  the full list.

Acceptance direction:

- Core operations (view list, open details, pause/resume/stop, basic
  add) work without horizontal scrolling on common phone widths.
- No new framework dependency; CSS-only or minimal-adjustment changes
  preferred.
- This may be folded into the settings search and UI personalization
  item (existing P2) if a shared UI polish pass is planned.

## P3 Research Features

### Permissioned Extension System

Problem: plugin systems create integration value but also create security,
support, and lawful-use risk.

Requested elsewhere:

- qBittorrent users discussed plugin permissions and plugin sandboxing in
  [qbittorrent#24530](https://github.com/qbittorrent/qBittorrent/issues/24530)
  and [qbittorrent#24531](https://github.com/qbittorrent/qBittorrent/issues/24531).

SwarmOtter research direction:

- Do not implement arbitrary plugins without a separate ADR and threat model.
- If pursued, prefer narrow, declarative extensions over arbitrary code.
- Any extension surface must prohibit bundled infringing indexes, infringing
  magnets, or content-discovery integrations that violate project policy.

### Alternate Privacy-Preserving Transports

Problem: alternate transports may help some lawful deployments, but they can
materially change containment, dependencies, user expectations, and project
messaging.

Requested elsewhere:

- Transmission has an I2P support request in
  [transmission#7230](https://github.com/transmission/transmission/issues/7230).
- qBittorrent has I2P-related requests in
  [qbittorrent#23665](https://github.com/qbittorrent/qBittorrent/issues/23665),
  [qbittorrent#24241](https://github.com/qbittorrent/qBittorrent/issues/24241),
  and [qbittorrent#23064](https://github.com/qbittorrent/qBittorrent/issues/23064).

SwarmOtter research direction:

- Treat alternate transports as a separate data-plane architecture decision.
- Require strict containment, explicit routing semantics, and local test
  fixtures before implementation.
- Do not frame this work as evasion of copyright enforcement or other unlawful
  activity.

### Swarm Merging (BiglyBT-style)

Problem: operators with partial data from one torrent may have matching content
in another torrent or from an HTTP source. Swarm merging (completing or
accelerating a torrent using matching content from other sources) is a real
feature in BiglyBT and is a useful pattern for seedbox and self-hosting
workflows.

Requested elsewhere:

- BiglyBT ships swarm merging as a standard feature for accelerating downloads
  and completing torrents from multiple sources.
- Self-hosting seedbox workflows benefit from cross-torrent content reuse.

SwarmOtter research direction:

- Evaluate whether matching by piece hash and content length is feasible within
  the storage layer.
- Assess containment implications: merging must not bypass network containment
  for peer traffic.
- Requires design and containment review before acceptance.
- No piracy-oriented content matching is permitted.

### Terminal UI / Console Interface

Problem: headless server and low-resource environments benefit from a
terminal-based interface that does not require a browser or X11/Wayland.
rTorrent's ncurses TUI and Deluge's `deluge-console` are valued by operators
who work primarily over SSH.

Requested elsewhere:

- rTorrent's ncurses TUI is its primary user interface and a key reason for
  its adoption in seedbox environments.
- Deluge ships `deluge-console` for terminal-based management.
- aria2 is CLI-first with no built-in GUI.

SwarmOtter research direction:

- Evaluate a ratatui-based (Rust ncurses) TUI for the daemon.
- Scope: torrent list, add/remove/pause/resume, basic settings, and health
  status. Full settings management may remain Web UI / API only.
- The TUI connects to the daemon via the same API as the Web UI; no separate
  code path.
- Consider a CLI subcommand (`swarmotterctl`) for scriptable operations
  (add magnet, list torrents, pause/resume) as a lighter-weight alternative.

Acceptance direction:

- TUI must not require a display server; pure terminal rendering.
- All TUI operations go through the API; no direct daemon state manipulation.
- CLI tool must be scriptable with machine-readable output (JSON).
- Implementing this requires an ADR (new user interface surface).

### Localization Strategy for the Web UI, API Errors, and Docs

Problem: qBittorrent ships translations into 70+ languages; rTorrent and
ruTorrent rely on community translations. SwarmOtter's Web UI is currently
English-only. International operators and self-hosters expect at least
a documented localization story before adopting a new daemon.

Requested elsewhere:

- qBittorrent, Deluge, and ruTorrent all maintain community translation
  workflows.
- Self-hosting operators in non-English-speaking regions routinely evaluate
  clients on the strength of their localization.

SwarmOtter research direction:

- Pick a translation workflow that fits the project's CI and governance
  posture: tooling, source string extraction, and contribution rules.
- Define the localization scope: Web UI strings, API error messages,
  documentation, and which of those are localized first.
- Decide how localization interacts with lawful-use policy: error
  messages and docs remain authoritative in English; translations are
  best-effort with an explicit "untranslated" fallback.
- Do not localize log messages; structured logs use stable English codes
  for automation compatibility.

Acceptance direction:

- A translation contribution guide is published before any locale is
  accepted.
- Source strings are extracted at build time and missing strings fail
  the build for in-scope locales.
- Implementing this requires an ADR (introduces a new contributor
  surface and a content policy decision).

### Documentation Discoverability

Problem: long-horizon observability (P2) covers operational history;
settings search and UI personalization (P2) covers the Web UI. There is no
parallel row for the **public documentation** (`docs/` mdBook and any
external operator guides) that operators read alongside the daemon.
Search, indexing, and a built-in help affordance in the Web UI are
absent.

Requested elsewhere:

- Sonarr, Radarr, and rTorrent/ruTorrent all publish user guides; few
  surface in-app search.
- Self-hosting operators with large libraries routinely want to search
  their own documentation as much as their settings.

SwarmOtter research direction:

- Add a search index for `docs/` (mdBook search or an external indexer
  such as DocSearch) and a built-in help pane in the Web UI that surfaces
  relevant documentation pages for the current view.
- Keep documentation versioning tied to the daemon version so a search
  result always matches the daemon release it documents.
- The documentation search is read-only and does not phone home; if an
  external indexer is used, the deployment story is documented and
  opt-in.

Acceptance direction:

- The built-in help pane reuses existing in-UI conventions and does not
  introduce a new framework.
- No telemetry, no third-party analytics, no required external service.
- The search index is a build artifact of the same docs source; no
  parallel content.

## Excluded From This Backlog

The investigated issue trackers include requests around built-in search, search
plugins, bundled indexers, and broad content discovery. Those requests are
intentionally excluded from this backlog because SwarmOtter is not a torrent
indexer or piracy-assistant project.

Bundled indexers, search plugins, and content-discovery integrations that serve
piracy use cases are excluded permanently. User-configured lawful RSS feed
ingestion is not excluded (see User-Configured Lawful RSS Feeds, P1). Any future
RSS or discovery-adjacent capability must first pass the lawful-use and content
policy requirements in `design/content-policy.md` and `design/lawful-use.md`.
