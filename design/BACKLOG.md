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
| P1 | Metadata-first magnet preview and intake rules | Let users inspect/select files before starting data transfer and enforce file exclusion rules | Transmission [#1611](https://github.com/transmission/transmission/issues/1611), [#2366](https://github.com/transmission/transmission/issues/2366), [#7330](https://github.com/transmission/transmission/issues/7330), [#7399](https://github.com/transmission/transmission/issues/7399), [#2399](https://github.com/transmission/transmission/issues/2399), [#5582](https://github.com/transmission/transmission/issues/5582), [#8793](https://github.com/transmission/transmission/issues/8793), qBittorrent [#23674](https://github.com/qbittorrent/qBittorrent/issues/23674) |
| P1 | File cleanup, trash, and retention safety | Avoid accidental data loss while making unwanted/obsolete partial data easy to remove | qBittorrent [#23575](https://github.com/qbittorrent/qBittorrent/issues/23575), [#23353](https://github.com/qbittorrent/qBittorrent/issues/23353), [#24102](https://github.com/qbittorrent/qBittorrent/issues/24102), [#24601](https://github.com/qbittorrent/qBittorrent/issues/24601), Transmission [#1722](https://github.com/transmission/transmission/issues/1722), [#6513](https://github.com/transmission/transmission/issues/6513) |
| P1 | Tracker and peer operations workbench | Diagnose weak swarms, prioritize trackers, expose known peers, webseeds, and retry state | Transmission [#996](https://github.com/transmission/transmission/issues/996), [#6425](https://github.com/transmission/transmission/issues/6425), [#8326](https://github.com/transmission/transmission/issues/8326), [#8413](https://github.com/transmission/transmission/issues/8413), [#5234](https://github.com/transmission/transmission/issues/5234), qBittorrent [#24013](https://github.com/qbittorrent/qBittorrent/issues/24013), [#24014](https://github.com/qbittorrent/qBittorrent/issues/24014) |
| P1 | Secure remote-operations hardening | Make headless/server use safer and easier behind reverse proxies and automation | qBittorrent [#7172](https://github.com/qbittorrent/qBittorrent/issues/7172), [#24308](https://github.com/qbittorrent/qBittorrent/issues/24308), Transmission [#5899](https://github.com/transmission/transmission/issues/5899), [#5989](https://github.com/transmission/transmission/issues/5989), qBittorrent [#19951](https://github.com/qbittorrent/qBittorrent/issues/19951) |
| P1 | Safe automation hooks | Provide explicit, observable, allowlisted event actions without unsafe hidden scripts | Transmission [#8056](https://github.com/transmission/transmission/issues/8056), [#6984](https://github.com/transmission/transmission/issues/6984), qBittorrent [#23550](https://github.com/qbittorrent/qBittorrent/issues/23550), [#23603](https://github.com/qbittorrent/qBittorrent/issues/23603) |
| P1 | Content organization controls | Keep download directories orderly through folder rules, preset paths, and path normalization | Transmission [#5614](https://github.com/transmission/transmission/issues/5614), [#8225](https://github.com/transmission/transmission/issues/8225), [#6044](https://github.com/transmission/transmission/issues/6044), [#6045](https://github.com/transmission/transmission/issues/6045), qBittorrent [#24239](https://github.com/qbittorrent/qBittorrent/issues/24239) |
| P1 | Torrent Creation (BEP 52 v2/hybrid) | Create `.torrent` files from local lawful content with piece hashing, tracker tiers, and webseed support | BiglyBT torrent creation, aria2 torrent creation, Transmission [#5794](https://github.com/transmission/transmission/issues/5794), Deluge create plugin |
| P1 | Superseeding / Initial Seeding (BEP 16) | Efficient first distribution of new lawful releases via initial-seeding mode | BEP 16, qBittorrent [#20098](https://github.com/qbittorrent/qBittorrent/issues/20098), BiglyBT initial seeding |
| P1 | IP Filtering / Blocklists / Peer Banning | Filter unwanted peers via CIDR/range lists, blocklist import, and manual peer bans | qBittorrent IP filtering, Deluge IP filtering, eMule/PeerGuardian blocklist formats |
| P1 | User-Configured Lawful RSS Feeds | Ingest content from user-supplied lawful RSS feeds as part of lawful distribution workflows | Deluge RSS plugin, rTorrent RSS, self-hosting RSS workflows; see lawful-use policy |
| P1/P2 | Native Cross-Seed & Hardlink-Aware Storage | Match on-disk data to new torrents by piece layout; link instead of re-download | cross-seed (external tool), self-hosting hardlink layouts, BiglyBT |
| P2 | Sequential Download / Streaming / File Preview | Sequential/priority-first fetch; in-place preview and verify; metadata-first preview | qBittorrent sequential download, aria2, WebTorrent streaming, Deluge |
| P2 | Protocol modernization roadmap | Stay ahead of compatibility and swarm reachability changes; BEP 52 v2/hybrid handling | qBittorrent [#23421](https://github.com/qbittorrent/qBittorrent/issues/23421), [#24600](https://github.com/qbittorrent/qBittorrent/issues/24600), Transmission [#3387](https://github.com/transmission/transmission/issues/3387), [#3705](https://github.com/transmission/transmission/issues/3705), [#993](https://github.com/transmission/transmission/issues/993) |
| P2 | Long-horizon observability | Preserve useful history beyond current live status and make operational events auditable | Transmission [#5591](https://github.com/transmission/transmission/issues/5591), qBittorrent [#22832](https://github.com/qbittorrent/qBittorrent/issues/22832), [#18525](https://github.com/qbittorrent/qBittorrent/issues/18525), [#24330](https://github.com/qbittorrent/qBittorrent/issues/24330) |
| P2 | Settings search and low-risk UI personalization | Make dense configuration easier to operate without turning the UI into a theme project | qBittorrent [#23654](https://github.com/qbittorrent/qBittorrent/issues/23654), [#22877](https://github.com/qbittorrent/qBittorrent/issues/22877), [#22913](https://github.com/qbittorrent/qBittorrent/issues/22913), Transmission [#4304](https://github.com/transmission/transmission/issues/4304), [#5648](https://github.com/transmission/transmission/issues/5648) |
| P2 | Calendar-Based Bandwidth Scheduler | Time-of-day alt-speed/limit schedules to complement the adaptive autopilot | qBittorrent scheduler, aria2 bandwidth scheduling, Deluge scheduler |
| P2 | Backup / Restore & Bulk Import/Export | Export/import torrent list and state for migration and disaster recovery of large libraries | qBittorrent backup, Deluge export, Flood backup/restore |
| P3 | Permissioned extension system | Enable integrations only if permissions, sandboxing, and lawful-use constraints are clear | qBittorrent [#24530](https://github.com/qbittorrent/qBittorrent/issues/24530), [#24531](https://github.com/qbittorrent/qBittorrent/issues/24531) |
| P3 | Alternate privacy-preserving transports | Evaluate only if strict containment, lawful-use messaging, and operational risk are solved | Transmission [#7230](https://github.com/transmission/transmission/issues/7230), qBittorrent [#23665](https://github.com/qbittorrent/qBittorrent/issues/23665), [#24241](https://github.com/qbittorrent/qBittorrent/issues/24241), [#23064](https://github.com/qbittorrent/qBittorrent/issues/23064) |
| P3 | Swarm Merging (BiglyBT-style) | Complete or accelerate a torrent using matching content from other torrents or HTTP sources | BiglyBT swarm merging, self-hosting seedbox workflows |

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
- The user must be able to disable the feature globally and per torrent.
- All network measurements must use the existing contained data plane; no
  separate uncontained probing is allowed.

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
  transitions to match client expectations.
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
- Publish OpenAPI/JSON Schema for native API and compatibility endpoints.
- Add event/log deduplication controls for noisy repeated errors.
- Add multi-user support with role-based access control (read-only, operator,
  admin) to support shared-server deployments without requiring separate
  daemon instances.

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
- Integrate with the tracker and peer operations workbench (existing P1).
- Filtering is framed as abuse mitigation and operational safety, not evasion.

Acceptance direction:

- Framing is consistent with lawful-use policy: filtering is abuse mitigation.
- Applies to all peer sources through the contained network path.
- Filters are auditable in logs and API.

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

### Calendar-Based Bandwidth Scheduler

Problem: users want time-of-day bandwidth schedules to complement the adaptive
autopilot, for example limiting upload during business hours or scheduling
full-speed downloads overnight.

Requested elsewhere:

- qBittorrent, aria2, and Deluge ship bandwidth scheduling features.

SwarmOtter feature shape:

- Add time-of-day alt-speed and bandwidth-limit schedules.
- Multiple named schedules with start/end times and assigned bandwidth profiles.
- Schedule assignment per torrent, label, or profile.
- Complements (does not replace) the adaptive autopilot (existing P0).

Acceptance direction:

- Schedules are deterministic and clearly reflected in the API and UI.
- Adaptive autopilot and scheduler interact predictably.

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
