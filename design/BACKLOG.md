# Feature Backlog

This document tracks market-differentiating feature candidates identified by a
review of the current SwarmOtter codebase, its product documentation, and
comparable BitTorrent clients and operator tools on 2026-07-13. Competitor
feature requests are useful directional evidence, but are not treated as a
comparable vote count or as a substitute for SwarmOtter's product strategy.

Backlog rule: when a feature in this document is implemented, tested,
documented, and usable in SwarmOtter, remove it from this document. Do not keep
completed items here as checked-off backlog rows.

This is a product backlog, not a release-scope or release-status document.
Items are prioritized product opportunities that can be selected only when
their dependencies, acceptance criteria, lawful-use posture, and network
containment requirements are satisfied.

## Priority Key

- `P0`: Immediate, ordered foundation work. The order is intentional because
  later P0 programs depend on the preceding ones.
- `P1`: High-value work that follows the P0 foundations or can proceed when
  its direct dependencies are already being changed.
- `P2`: Useful, bounded, or niche work that should not displace P0/P1 work.
- `P3`: Research work that needs a clear architecture, security, legal,
  dependency, or containment case before acceptance.
- `Conditional`: A strategically valuable candidate that remains at its stated
  priority only after an explicit product-direction decision or customer
  evidence supports it.

All torrent-related network behavior introduced by a backlog item must use the
central containment layer and fail closed. A feature may not silently use the
default route simply to gain interoperability or convenience.

## Prioritization Method

The Feature Map is authoritative for priority and sequence. Detailed sections
below are grouped by technical domain so that related constraints are not
duplicated. Within a priority, a lower sequence number is preferred. A
conditional feature does not advance merely because its technical prerequisites
are available.

The P0 sequence favors compatibility and durable operator correctness before
new shared-server product scope: current metadata and identity handling are
v1/SHA-1-centric; metadata-first selection requires a complete and explicit
policy model; and the current atomically rewritten state document is not the
right long-term query foundation for library operations. The grouped P0
programs address those constraints directly.

## Feature Map

| Priority | Sequence | Feature | User Value | Source Signals |
| --- | --- | --- | --- | --- |
| P0 | 1 | BEP 52 v2/hybrid identity and interoperability | Consume, preserve, and operate interoperably with current v1, v2, and hybrid torrents without weakening containment | [BEP 52](https://www.bittorrent.org/beps/bep_0052.html), qBittorrent v2 support, Transmission v2 requests |
| P0 | 2 | Policy-driven metadata-first intake | Preview metadata, select or exclude files, organize output, and apply deterministic policy before payload transfer | [BEP 53](https://www.bittorrent.org/beps/bep_0053.html), Transmission metadata-first requests, qBittorrent file-selection requests |
| P0 | 3 | Durable library operations foundation (SQLite) | Replace the monolithic state-document limitation with indexed, migratable library state, histories, and raw-metainfo retention | Server-oriented operator workflows; supports audit, history, and portability |
| P1 | 1 | Torrent creation for lawful distribution | Create v1, v2, and hybrid torrents after P0 v2/hybrid support establishes the shared metadata model | BiglyBT, aria2, Transmission [#5794](https://github.com/transmission/transmission/issues/5794) |
| P1 | 2 | Library provenance and portability | Surface stored metadata and offer deterministic magnet and original-metainfo export | qBittorrent, Transmission, Deluge, and BiglyBT torrent detail/export workflows |
| P1 | 3 | File cleanup, trash, and retention safety | Remove unwanted data safely without accidental loss | qBittorrent and Transmission cleanup requests |
| P1 | 4 | Scriptable CLI (`swarmotterctl`) | Provide API-backed SSH and automation workflows with stable JSON output | `transmission-remote`, rTorrent, aria2 |
| P1 | 5 | OpenAPI specification and interactive API docs | Make native and compatibility APIs discoverable and safer to automate | Flood, Deluge, self-hosting automation |
| P1 | 6 | Tracker and peer operations workbench | Diagnose weak swarms and explain tracker, peer, webseed, and retry state | Transmission and qBittorrent operations requests |
| P1 | 7 | Secure remote-operations hardening | Make headless and reverse-proxy deployments safer to operate | qBittorrent and Transmission remote-operation requests |
| P1 | 8 | Native cross-seed and hardlink-aware storage | Reuse matching local data safely rather than downloading it again | cross-seed workflows, BiglyBT |
| P1 | 9 | Idempotent re-add / content-addressed import | Recognize already-present content conservatively and reduce unnecessary verification | Large-library operator workflows |
| P1 | 10 | Explainability API | Give operators structured reasons for non-trivial decisions | Operator tooling and import-failure explanations |
| P1 | 11 | Anonymous mode | Provide privacy-preserving client-identification controls without changing containment behavior | qBittorrent and Deluge anonymous-mode controls |
| P1 | 12 | Baseline production health surface | Define standard liveness and readiness semantics around the existing health check | Orchestrator health conventions |
| P1 (conditional) | 13 | Per-profile / per-torrent network-path binding | Provide separately fail-closed contained paths only if shared-server or managed-distribution strategy requires it | Multi-profile routing and shared-server patterns |
| P1 (conditional) | 14 | Multi-user / multi-tenant support | Add tenant isolation and quotas only after durable state and network-path strategy are established | Flood, rTorrent/ruTorrent, qBittorrent request [#3327](https://github.com/qbittorrent/qBittorrent/issues/3327) |
| P1 (conditional) | 15 | Operator audit log | Audit privileged lifecycle actions after the authorization and durable-state foundations exist | Compliance-oriented server operations |
| P2 | 1 | Safe automation hooks | Offer bounded, observable, allowlisted event actions after the operator surface is mature | Transmission and qBittorrent automation requests |
| P2 | 2 | User-configured lawful RSS feeds | Support user-supplied lawful distribution feeds without bundled discovery | Deluge and rTorrent RSS workflows |
| P2 | 3 | Superseeding / initial seeding (BEP 16) | Improve first distribution efficiency for lawful releases | BEP 16, qBittorrent, BiglyBT |
| P2 | 4 | Seed prioritization (low-seed first) | Prefer work that improves lawful swarm availability | qBittorrent and Transmission discussions |
| P2 | 5 | Container and sandbox hardening | Complete read-only-rootfs validation, image verification guidance, and optional orchestration artifacts beyond the existing non-root multi-architecture image and Compose support | Container operator practice |
| P2 | 6 | Synthetic availability and SLO-style summaries | Add an opt-in contained data-plane synthetic check after baseline health semantics | Cloud-native operations conventions |
| P2 | 7 | Filesystem snapshot integration | Add opt-in rollback hooks for supported local snapshot systems | Btrfs, ZFS, Snapper workflows |
| P2 | 8 | Client-identity fingerprinting and rollups | Summarize peer client composition for operations visibility | qBittorrent and BiglyBT peer views |
| P2 | 9 | HTTP / HTTPS proxy support | Evaluate constrained proxy support where SOCKS5 is unavailable | qBittorrent and aria2 proxy support |
| P2 | 10 | Seedbox pre-seed warm-up | Pre-read and verify a lawful release before announce | BiglyBT-related warm-up concepts |
| P2 | 11 | Disk cache / I/O buffer configuration | Tune storage behavior for specialized deployments | qBittorrent, Deluge, Transmission |
| P2 | 12 | Sequential download / streaming / file preview | Add explicitly opt-in ordered fetching and local preview behavior | qBittorrent, aria2, WebTorrent, Deluge |
| P2 | 13 | Protocol compatibility follow-ons | Track protocol gaps beyond the P0 BEP 52 foundation | qBittorrent and Transmission protocol requests |
| P2 | 14 | Long-horizon observability | Retain useful history beyond current status | Transmission and qBittorrent requests |
| P2 | 15 | Settings search and low-risk UI personalization | Improve configuration usability without making visual polish the product focus | qBittorrent and Transmission requests |
| P2 | 16 | Time-of-day and adaptive bandwidth policies | Extend the implemented adaptive controls with explicit schedules | qBittorrent, aria2, Deluge |
| P2 | 17 | Backup / restore and bulk import/export | Support large-library migration and recovery | qBittorrent, Deluge, Flood |
| P2 | 18 | Thin client / remote session architecture | Consider a streaming client protocol beyond API and CLI operation | Deluge, qBittorrent, Flood |
| P2 | 19 | OpenTelemetry observability | Export traces and metrics for cloud-native deployments | OpenTelemetry |
| P2 | 20 | Cloud / object-storage-backed storage root | Evaluate institutional lawful-distribution storage integrations | rclone and institutional distribution patterns |
| P2 | 21 | Local GeoIP / ASN peer rollups | Provide on-device peer-distribution summaries | Local GeoIP databases |
| P2 | 22 | Responsive / mobile-friendly Web UI | Improve small-viewport operation | qBittorrent and Transmission Web UIs |
| P3 | 1 | Trust and provenance signals for torrents and trackers | Research interoperable, secure provenance semantics before committing to signed-torrent claims | Signed-release workflows; no common signed-torrent standard |
| P3 | 2 | Permissioned extension system | Enable integrations only with a clear permissions and sandbox model | qBittorrent extension requests |
| P3 | 3 | Alternate privacy-preserving transports | Evaluate only with a complete containment and operational-risk case | Transmission and qBittorrent requests |
| P3 | 4 | Swarm merging (BiglyBT-style) | Reuse matching content from other torrents or allowed HTTP sources | BiglyBT |
| P3 | 5 | Terminal UI / console interface | Add a full terminal UI only after the API-backed CLI is established | rTorrent, Deluge, aria2 |
| P3 | 6 | Localization strategy | Establish a sustainable translation and source-string policy | qBittorrent, Deluge, ruTorrent |
| P3 | 7 | Version-aware contextual documentation | Add in-app help tied to the daemon version; mdBook search is already built | mdBook and in-app help patterns |

## Detailed Feature Candidates

The Feature Map above is the authoritative priority and ordering. The detailed
sections remain grouped by technical domain, and their priority notes describe
how each fits the map.

### Protocol Modernization: BEP 52 v2/hybrid Identity and Interoperability

**Priority: P0, sequence 1.**

Problem: SwarmOtter's current torrent identity and metadata model is
v1/SHA-1-centric. Current clients and lawful distributors increasingly use v2
and hybrid torrents, which use SHA-256-based structures and cannot be modeled
by simply treating a v2 identifier as another 20-byte v1 info hash. Supporting
them as a foundation avoids compatibility gaps and gives later creation,
selection, portability, and state work one coherent metadata model.

SwarmOtter feature shape:

- Introduce explicit v1, v2, and hybrid torrent identity rather than overloading
  the existing v1 `InfoHash` representation.
- Parse and validate the BEP 52 file tree, piece layers, SHA-256 hashes, and
  v2/hybrid magnet forms while preserving existing v1 behavior.
- Update peer, tracker, DHT, metadata exchange, fast-resume, persistence, API,
  and UI surfaces as one compatibility program; all network activity continues
  to go through the central binder and fails closed.
- Preserve canonical raw metainfo where needed for exact identity, recovery,
  and later export rather than silently reserializing data into a different
  info dictionary.
- Add generated local v1, v2, and hybrid fixtures and local-swarm coverage
  before enabling behavior by default.

Acceptance direction:

- Existing v1 torrents and magnets remain compatible.
- Invalid, incomplete, or ambiguous v2/hybrid metadata is rejected with a
  meaningful error; it never falls back to an unrelated v1 interpretation.
- No new protocol path may bypass containment or use the default route.
- Implementing this requires an ADR because it changes identity, persistence,
  and compatibility surfaces.

### Advanced Policy-Profile Rules

**Priority: P0, sequence 2.** This is one part of the policy-driven
metadata-first intake program, alongside metadata preview and content
organization controls.

Problem: named profiles now resolve deterministic storage, queue, initial-start,
seeding, and bandwidth policy for adds, labels, watch folders, and explicit
torrent assignment. Advanced tracker, file-selection, and completion rules
still require one-off operations. Metadata-first intake needs those rules to be
decided before any payload transfer begins.

Requested elsewhere:

- qBittorrent users requested boolean logic for seed limits in
  [qbittorrent#24500](https://github.com/qbittorrent/qBittorrent/issues/24500).
- qBittorrent users requested category-level filename exclusions in
  [qbittorrent#23722](https://github.com/qbittorrent/qBittorrent/issues/23722).
- Transmission users requested per-tracker seed ratio and tracker priority in
  [transmission#1461](https://github.com/transmission/transmission/issues/1461)
  and [transmission#6425](https://github.com/transmission/transmission/issues/6425).

SwarmOtter feature shape:

- Add profile-scoped tracker-host matching, tracker priority, and tracker
  policy controls.
- Add profile-scoped file exclusion patterns, content-organization rules, and
  completion actions.
- Apply preview-time file selection and exclusion rules before an explicit
  payload-start decision; expose the resolved decision to the API and UI.
- Retain the existing deterministic effective-policy API/UI so the source of
  each new field remains explainable.

Acceptance direction:

- Effective values must be deterministic and explainable.
- New fields must clearly distinguish between live inheritance and create-time
  snapshots.
- Further persistent policy or runtime-scheduling decisions require an ADR
  update.

### Per-Profile / Per-Torrent Network-Path Binding

**Priority: P1, sequence 13, conditional strategic feature.** Promote this
only after SwarmOtter explicitly commits to shared-server, managed-distribution,
or similarly multi-path deployments. It is not generic next-step work for a
single-path daemon.

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
- The implemented policy-profile model gains a network-path binding field; the
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

**Priority: P1, sequence 14, conditional strategic feature.** This follows the
durable-state foundation and an explicit decision to support shared-server
deployments with separately contained paths.

Problem: shared-server and seedbox deployments need per-user isolation, quotas,
and role-based access control. Running separate daemon instances per user can be
operationally complex. It is a large product and security commitment, rather
than a universally next-most-important client feature.

Requested elsewhere:

- qBittorrent's multi-user WebUI request
  [qbittorrent#3327](https://github.com/qbittorrent/qBittorrent/issues/3327)
  is a long-lived directional signal of demand; its public issue data is not a
  comparable cross-project vote ranking.
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
  can be combined with conditional per-profile network-path binding
  for full per-user isolation on a shared host.
- Add per-user API keys with scoped permissions.
- Integrate with the implemented policy-profile model for per-user default
  settings.
- Integrate with conditional per-profile network-path binding for per-user
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

## Intake, Library, and Operations Details

### Metadata-First Magnet Preview and Intake Rules

**Priority: P0, sequence 2.** This is the runtime entry point for the
policy-driven metadata-first intake program.

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

Acceptance direction:

- The metadata preview must not start payload transfer until an explicit API or
  user action resolves the policy and starts the torrent.
- File-selection, exclusion, and organization results are deterministic,
  inspectable, and persisted as part of the add decision.
- Metadata exchange, tracker use, and any direct peer supplied by `x.pe` remain
  subject to the central containment layer and fail-closed behavior.

### File Cleanup, Trash, and Retention Safety

**Priority: P1, sequence 3.**

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

**Priority: P1, sequence 6.**

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

**Priority: P1, sequence 7.**

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

**Priority: P2, sequence 1.**

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

**Priority: P0, sequence 2.** This is the storage-placement component of the
policy-driven metadata-first intake program.

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

**Priority: P1, sequence 1.** Begin only after the P0 BEP 52 v2/hybrid
foundation supplies one shared identity and metadata model.

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

**Priority: P2, sequence 3.**

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

### Seed Prioritization (Low-Seed First)

**Priority: P2, sequence 4.**

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
- Configurable per-torrent and per-profile.
- Complements (does not replace) ratio-based and time-based seeding limits.
- Surface seed-count data from trackers, DHT, and PEX in the API and UI.

Acceptance direction:

- Seed-priority mode is opt-in and clearly documented.
- Does not override explicit ratio or time-based stop conditions.
- All seeding traffic remains contained through the configured network path.

### OpenAPI Specification & Interactive API Docs

**Priority: P1, sequence 5.**

Problem: the native API and its bounded compatibility adapters need clear,
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

**Priority: P2, sequence 2.**

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

**Priority: P1, sequence 8.**

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
- Integrate with shipped storage-root resource controls and the future
  filesystem-aware storage strategy.
- Explicit user-visible link vs copy behavior; no silent data loss.

Acceptance direction:

- No silent data loss; link vs copy decision is explicit and auditable.
- Implementing this likely requires an ADR (persistent storage conventions).

### Trust and Provenance Signals for Torrents and Trackers

**Priority: P3, sequence 1 (research).** A useful trust workflow needs an
interoperable signature format, threat model, and operator semantics before
SwarmOtter can promise provenance verification. There is no common signed-
`.torrent` standard to adopt as a simple feature.

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

SwarmOtter research direction:

- Define a threat model for tracker trust, content provenance, imported keys,
  revocation, and how a failed check affects an add request.
- Evaluate whether manual tracker allow/deny policy can stand independently of
  a signed-torrent scheme, and whether it belongs in the P0 policy program.
- Evaluate external signed-release manifests without inventing a proprietary
  torrent signature format or creating a bundled discovery surface.
- Retain the rule that any eventual network fetch for verification uses the
  same contained path as other torrent operations.

Acceptance direction:

- No design advances without documented key-management, revocation, and
  failure semantics.
- No tracker host may be silently blocked and no verification path may weaken
  containment.
- A future accepted implementation requires an ADR (new trust model and
  metadata surface).

### Operator Audit Log for Torrent Lifecycle Events

**Priority: P1, sequence 15, conditional strategic feature.** This follows
the durable-state, authorization, and multi-user foundations; it should not
invent an independent persistence or identity model.

Problem: long-horizon observability (P2) covers metrics. Operators
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
- This is the compliance story that combines with conditional Multi-User Support
  for shared-server and seedbox deployments.

### Explainability API: Structured Reasons for Non-Trivial Decisions

**Priority: P1, sequence 10.**

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
- The autopilot "why is this slow?" report, filesystem-aware storage
  decisions, the per-path fail-closed states, and the bandwidth
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

**Priority: P2, sequence 5.**

Problem: SwarmOtter already ships a non-root multi-architecture OCI image,
Compose support, and release provenance/SBOM generation. The remaining work is
to turn those delivered artifacts into a verified hardening and deployment
contract, rather than to describe the project as if it only had a Dockerfile.

Requested elsewhere:

- Transmission, qBittorrent, rTorrent, and Deluge all ship native packages
  and have community container images; none of those images is a
  first-class artifact of the upstream project.
- Sonarr/Radarr and the *arr ecosystem treat container deployment as the
  default and require rootless operation and read-only filesystems.

SwarmOtter feature shape:

- Verify and document rootless and read-only-root-filesystem operation: state,
  logs, and download roots must live on mounted volumes, and failures must be
  actionable.
- Publish an image-signing and verification contract that operators can use
  alongside the existing provenance and SBOM artifacts.
- Keep Compose tested as a first-class deployment path.
- Evaluate Helm, Kubernetes, Podman, and NetworkPolicy examples only when an
  operator use case warrants their ongoing maintenance.

Acceptance direction:

- Read-only-root-filesystem and non-root deployments are tested rather than
  merely documented.
- Image verification guidance corresponds to the actual published artifacts.
- Any Helm chart or orchestration artifact is versioned and covered by
  deployment tests before it is presented as supported.
- Container deployment does not weaken containment; namespace and
  capability configuration is part of the deployment contract.

### Production Health / Availability Surface

**Priority: P1, sequence 12 for baseline liveness/readiness semantics; P2,
sequence 6 for a synthetic data-plane check and SLO-style summaries.**

Problem: SwarmOtter already has a `/health` endpoint and container health
check, but their liveness/readiness semantics are not yet an explicit public
contract. A synthetic end-to-end data-plane check is a distinct, later concern
with more containment and operational cost.

Requested elsewhere:

- qBittorrent's status endpoints are ad hoc; no liveness/readiness split.
- Flood ships API exploration but no synthetic health checks.
- Kubernetes, Consul, Nomad, and the cloud-native ecosystem treat
  liveness/readiness probes as a baseline expectation.

SwarmOtter feature shape:

- Define and document standard liveness and readiness semantics, either by
  extending `/health` or by adding explicit `/healthz/live` and
  `/healthz/ready` endpoints. Liveness reports a running control plane;
  readiness reports whether the daemon can safely accept normal work without
  making a synthetic torrent transfer a prerequisite.
- Later, add an opt-in synthetic end-to-end check torrent: a small, locally generated
  torrent the daemon advertises to itself through the contained path on
  a configurable interval. Its result is reported separately from baseline
  readiness and never silently tests an uncontrolled external route.
- Later, add SLO-style summaries: rolling uptime, ready ratio, and a
  configurable ready-ratio alert threshold.
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

**Priority: P2, sequence 8.**

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
- This complements the implemented peer-admission filtering surface for
  abuse mitigation.

### Filesystem Snapshot Integration

**Priority: P2, sequence 7.**

Problem: shipped storage-root controls and the filesystem-aware storage
discussion in the backlog already recognize that torrent clients run on
sophisticated filesystems. There is no native integration with filesystem
snapshots, so an operator who wants rollback for a torrent root or a
state directory has to script it externally. This is invisible in
mainstream clients and is a natural differentiator on Linux.

Requested elsewhere:

- qBittorrent users have requested CoW-aware behavior but not snapshot
  integration; see the qBittorrent CoW discussion in the filesystem-aware
  storage strategy entry.
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

**Priority: P2, sequence 9.**

Problem: SwarmOtter ships contained TCP-only SOCKS5 support, but corporate and
egress-filtered environments frequently expose only HTTP/CONNECT proxies.
Users in those environments cannot route applicable SwarmOtter traffic without
an HTTP proxy option. qBittorrent and aria2 both support HTTP proxies alongside
SOCKS5.

Requested elsewhere:

- qBittorrent ships HTTP proxy support alongside SOCKS5.
- aria2 ships HTTP/HTTPS proxy support as a first-class option.
- Corporate and educational networks commonly block SOCKS5 but allow
  authenticated HTTP egress proxies.

SwarmOtter feature shape:

- Add optional HTTP/CONNECT (and HTTPS CONNECT) proxy configuration for
  compatible TCP torrent traffic.
- Route only compatible peer TCP connections, HTTP(S) tracker announces, and
  webseed requests through the configured proxy. UDP peer traffic, DHT, and
  uTP must never silently fall back to an HTTP proxy or the default route.
- Support authenticated and unauthenticated modes.
- A future per-profile proxy policy field for multi-path deployments.
- HTTP proxy is distinct from SOCKS5 and from network containment; all
  three can coexist with documented precedence rules.

Acceptance direction:

- Proxy configuration is explicit and auditable.
- When both HTTP proxy and network containment are configured,
  containment takes precedence; proxy traffic still goes through the
  contained path.
- DNS resolution for the proxy hostname respects containment.
- Implementing this may share the connection-egress abstraction with the
  existing contained SOCKS5 TCP support.

### Scriptable CLI (`swarmotterctl`)

**Priority: P1, sequence 4.**

Problem: SwarmOtter's API-first posture targets automation, but the only
operator interface beyond the API is the Web UI. Operators working over
SSH, in CI pipelines, or in `*arr`-style automation want a lightweight
scriptable CLI that mirrors the most common daemon operations without a
browser or a full TUI. The TUI entry (P3) mentions a `swarmotterctl`
alternative; pulling the CLI out as its own item lets it ship sooner and
reinforce the native and compatibility API automation story.

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
- Reuses the same auth (API keys and scoped permissions) as the API and, if
  adopted, the conditional Multi-User model.

Acceptance direction:

- All CLI operations go through the API; no separate daemon code path.
- JSON output is stable and versioned alongside the API.
- The CLI is a first-class build artifact and documented in `docs/`.
- Implementing this may require an ADR (new user-facing binary and an
  output-stability contract).

### Seedbox Pre-Seed Warm-Up

**Priority: P2, sequence 10.**

Problem: when a lawful release is newly created and seeded, the first
peers to connect find a seeder that has not yet read or hashed its pieces,
so the first serving round is slow and the swarm's initial health looks
poor. Pre-reading and pre-hashing all pieces *before* the torrent is
announced lets the first peer be served instantly and improves the
measured health of a fresh swarm. No mainstream client markets this as
a deliberate first-distribution optimization.

Requested elsewhere:

- BiglyBT has related pre-seed and swarm-warmup concepts.
- Superseeding / initial seeding (P2) benefits from a warm
  seeder because piece distribution is the bottleneck.
- Legal distributors of Linux ISOs, open-source releases, and datasets
  care about first-hour swarm health.

SwarmOtter feature shape:

- Add an optional pre-seed warm-up mode: when a torrent is added in a
  seeding-from-complete state (created content or re-verified complete
  data), pre-read and verify all pieces in the background before the
  tracker announce and DHT announce go out.
- Integrate with superseeding / initial seeding (P2), shipped
  storage-root controls, and the future filesystem-aware storage strategy so
  warm-up respects disk pressure and concurrency limits.
- Surface warm-up progress and completion in the API, UI, and the
  explainability API (P1).

Acceptance direction:

- Warm-up is opt-in and never blocks a user-initiated start.
- Warm-up respects disk pressure and never degrades other active
  torrents.
- Warm-up traffic stays on the contained network path (it is local I/O
  plus optional local hash, not network egress).

### Idempotent Re-Add / Content-Addressed Import

**Priority: P1, sequence 9.**

Problem: operators re-adding a torrent whose data already exists on disk
are forced through a full re-download or full re-verify cycle even when
nothing has changed. This is friction for large libraries and for the
cross-seed (P1) workflow. Conservatively recognizing that the on-disk content
already satisfies the torrent can reduce operator load and disk wear.

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
- Integrate with cross-seed (P1), shipped storage-root controls,
  and the future filesystem-aware storage strategy.

Acceptance direction:

- Recognition is conservative: when in doubt, verify; never silently
  mark unverified data as complete.
- The decision is auditable and explainable.
- No silent data loss; a misrecognized re-add falls back to normal
  download/verify.

### Durable State Store (SQLite)

**Priority: P0, sequence 3.**

Problem: SwarmOtter currently persists a single atomically replaced JSON state
document alongside fast-resume state. That is reliable for a compact state
snapshot, but it is not an indexed query foundation for library history,
queue/health queries, raw metainfo retention, or later audit work at large
library scale. A durable local store gives those surfaces one migration and
query model rather than a collection of unrelated side files.

Requested elsewhere:

- No mainstream torrent client ships a queryable historical state store;
  this is a SwarmOtter opportunity enabled by the API-first, server
  positioning.
- Self-hosting operators managing large libraries expect fast list,
  filter, and history queries that resume files do not provide.

SwarmOtter feature shape:

- Introduce a durable SQLite state store as the backing
  store for the registry, queue state, health snapshots, audit events,
  and rolling metrics.
- Define the durable relationship between fast-resume state, the queryable
  library record, and canonical raw metainfo. Original metainfo must be
  retained without changing its identity; fast resume remains optimized for
  restart recovery.
- Provide migration from the current state-document model so existing
  deployments upgrade without losing state.
- Integrate with long-horizon observability (P2), the conditional operator
  audit log (P1), and the implemented large-library operations
  console.

Acceptance direction:

- Crash recovery, database rebuild, and raw-metainfo recovery behavior are
  explicitly defined and tested; no storage layer silently changes a torrent
  identity.
- The store never weakens network containment; it is local-only and not
  network-addressable.
- Schema changes are versioned and migrated.
- Implementing this requires an ADR (new persistent format, migration
  path, and a query model decision).

### Torrent Metadata Display (Comments, Created By, Creation Date)

**Priority: P1, sequence 2.** Together with magnet generation and metainfo
export, this is the Library Provenance and Portability program.

Problem: `.torrent` files contain metadata fields beyond the piece table and
file list: `comment`, `created by`, `creation date`, and `encoding`. Every
mainstream torrent client displays these fields in a properties or info panel.
SwarmOtter's parser already retains these fields, but the native API and Web UI
do not surface them as a coherent library record. For lawful distributors
verifying provenance of downloaded content, and
for operators inspecting torrent origin, this is baseline expected
functionality.

Requested elsewhere:

- qBittorrent displays torrent properties including comment, created by, and
  creation date in the "General" tab of the torrent details panel.
- Transmission shows torrent metadata in the inspector including comment,
  creator, and date created.
- Deluge displays torrent info including comment and creator in the Details
  tab.
- BiglyBT shows comprehensive torrent metadata including all info dictionary
  fields.
- aria2 exposes torrent metadata via RPC including `comment` and `creationDate`.

SwarmOtter feature shape:

- Preserve parser-retained `comment`, `created by`, `creation date`, and
  `encoding` fields in the durable library record without changing original
  metainfo.
- For magnet links, populate these fields once metadata is fetched via BEP 9.
- Expose metadata fields in the torrent detail API response (`GET
  /api/v1/torrents/:hash`).
- Display metadata in a "Properties" or "Info" section of the Web UI torrent
  detail view.
- Support a separate operator annotation via API and UI when needed; it must
  not mutate the original `comment` field or claim to be source metadata.

Acceptance direction:

- Metadata fields are preserved across save/load cycles and remain associated
  with their original metainfo.
- An operator annotation is auditable in the conditional operator audit log
  (P1) when that feature is adopted.
- No metadata field is used to alter download/upload behavior.
- Magnet-fetched metadata is stored once and not re-fetched on restart.

### Magnet Link Generation from Added Torrents

**Priority: P1, sequence 2.** Part of Library Provenance and Portability.

Problem: users frequently need to generate a magnet URI from a torrent already
in their library — for sharing with collaborators, for backup of the magnet
link itself, or for re-adding the same content on another system. Every
mainstream client provides a "copy magnet link" action. SwarmOtter accepts
magnet links as input but does not generate them as output.

Requested elsewhere:

- qBittorrent provides "Copy magnet link" in the right-click context menu
  for any torrent.
- Transmission allows exporting magnet URIs from the torrent inspector.
- Deluge provides magnet link copy in the torrent details.
- BiglyBT generates magnet URIs with configurable tracker inclusion.
- aria2 can output magnet URIs via the `getUris` RPC method.

SwarmOtter feature shape:

- Generate a standards-compliant magnet URI from any torrent in the library,
  including v1 `xt=urn:btih:` and, after P0, v2 `xt=urn:btmh:` identity where
  applicable, display name (`dn=`), tracker URLs (`tr=`), and webseed URLs
  (`ws=`).
- Support configurable tracker inclusion: all trackers, primary tier only,
  or no trackers.
- Add a `GET /api/v1/torrents/:hash/magnet` endpoint that returns the
  generated URI.
- Add a "Copy Magnet Link" action in the Web UI torrent detail and context
  menu.
- Support BEP 53 `so=` (select-only) parameter for partial-torrent magnet
  generation when combined with the P0 metadata-first intake program.

Acceptance direction:

- Generated magnet URIs are valid and can be re-imported into SwarmOtter
  or any BEP 9-compliant client.
- Tracker inclusion options are clearly documented.
- No private tracker URLs are included in generated magnets for private
  torrents unless the operator explicitly opts in.
- The generation is deterministic: the same torrent always produces the
  same magnet URI given the same tracker inclusion setting.

### Torrent File Export

**Priority: P1, sequence 2.** Part of Library Provenance and Portability.

Problem: operators need to export the `.torrent` file for torrents in their
library — for backup, for migration to another client, for sharing the
torrent file itself (not just the magnet), or for archival of the original
metadata. Every mainstream client allows downloading or exporting the
`.torrent` file. SwarmOtter accepts `.torrent` files as input but does not
provide them as output.

Requested elsewhere:

- qBittorrent provides "Export .torrent" in the right-click context menu
  and stores `.torrent` files in its BT_backup directory.
- Transmission serves `.torrent` files via the RPC `torrent-get` method
  with `torrentFile` field or via the web interface download.
- Deluge provides `.torrent` export in the torrent details.
- aria2 saves `.torrent` files to disk when `--bt-save-metadata` is set
  and can serve them via the `getFiles` RPC method.
- BiglyBT stores and exports `.torrent` files with full metadata.

SwarmOtter feature shape:

- Persist the original `.torrent` file bytes at add time and return those
  original bytes for export. Do not reserialize an original torrent in a way
  that could change its info dictionary or identity.
- Add a `GET /api/v1/torrents/:hash/torrent` endpoint that returns the
  `.torrent` file as a binary download with `Content-Disposition:
  attachment`.
- For magnet-fetched torrents where no original `.torrent` exists, offer a
  clearly marked reconstructed export only when the fetched canonical metadata
  is sufficient; it is never represented as the original uploaded file.
- Add a "Download .torrent" action in the Web UI torrent detail view.
- Support batch export of multiple `.torrent` files as a zip archive via
  `POST /api/v1/torrents/export`.

Acceptance direction:

- Exported `.torrent` files are valid and can be imported into SwarmOtter
  or any BEP 3-compliant client.
- Reconstructed `.torrent` files from magnet metadata are clearly marked as
  reconstructed and include only the metadata that was fetched.
- Original metainfo retention survives daemon restart as part of the durable
  library record or its explicitly documented companion storage.
- Batch export respects the same auth and permission model as individual
  export.

### Anonymous Mode

**Priority: P1, sequence 11.**

Problem: some operators want to minimize the identifiable fingerprint their
torrent client presents to trackers and peers. Every mainstream client
offers some form of anonymous mode that hides or randomizes the client
identification (User-Agent header for trackers, peer ID prefix for peer
connections). SwarmOtter uses a fixed peer ID prefix and User-Agent string.
For lawful-use operators who simply want to reduce their operational
footprint, this is expected privacy-preserving functionality.

Requested elsewhere:

- qBittorrent ships an "Anonymous Mode" toggle that randomizes the peer ID
  and removes the User-Agent from tracker requests.
- Deluge provides anonymous mode configuration that hides client identity.
- Transmission discussions include peer-id spoofing and User-Agent
  customization requests.
- BiglyBT allows configurable peer ID and client identification.
- µTorrent provides peer ID and User-Agent customization.

SwarmOtter feature shape:

- Add an `anonymous_mode` configuration toggle (default `false`).
- When enabled: randomize the peer ID prefix (while maintaining the
  Azureus-style `-XX####-` format for protocol compatibility), remove or
  genericize the User-Agent header in tracker HTTP requests, and remove
  the client identification from the BEP 10 extension handshake.
- When disabled (default): use the standard SwarmOtter peer ID prefix and
  User-Agent for normal protocol operation.
- Per-profile anonymous-mode policy for multi-path
  deployments where some traffic classes should be anonymous and others
  should not.
- Surface the anonymous mode state in the API and UI so operators can
  verify the setting is active.

Acceptance direction:

- Anonymous mode is framed as privacy-preserving operation, not as evasion
  of copyright enforcement or lawful-use policy.
- Anonymous mode never weakens network containment; it only changes
  wire-level identification strings.
- The randomized peer ID is generated once per daemon session and reused
  for all connections within that session (consistent with protocol
  expectations).
- Anonymous mode is clearly documented with its scope and limitations: it
  does not hide IP addresses (that is the role of VPN/NIC containment) and it
  does not itself change transport encryption.
- Implementing this requires an ADR (changes wire-protocol identification
  and tracker request headers).

## Deferred Experience and Infrastructure Details

### Disk Cache / I/O Buffer Configuration

**Priority: P2, sequence 11.**

Problem: torrent clients perform intensive random and sequential disk I/O for
piece writes, piece reads (for seeding), and verification. Mainstream clients
provide configurable disk cache and I/O buffer settings to tune performance
for different storage media (SSD, HDD, NAS, RAM disk). SwarmOtter relies on
OS-level page cache and `tokio::fs` defaults with no user-visible cache
configuration. For operators running large queues on HDDs or NAS mounts,
cache tuning is expected baseline functionality.

Requested elsewhere:

- qBittorrent provides extensive disk cache settings: cache size, cache
  expiry, write cache toggle, OS cache toggle, and coalesce reads/writes.
  These are among the most-tuned settings in the qBittorrent community.
- Deluge provides cache size and cache expiry configuration.
- Transmission provides prefetch and cache-related options.
- µTorrent provides disk cache size, write cache, and read cache settings
  as primary performance tuning knobs.
- BiglyBT provides configurable disk cache with separate read and write
  cache sizes and cache strategy selection.

SwarmOtter feature shape:

- Add configurable disk cache settings: write cache size (MB), read cache
  size (MB), cache expiry (seconds), and OS page cache preference.
- Add I/O buffer settings: piece write buffer size, piece read buffer
  size, and verification buffer size.
- Add write coalescing: batch multiple small piece writes into larger
  sequential writes to reduce HDD seek overhead.
- Add read-ahead configuration for sequential download (P2) and
  seeding workloads.
- Integrate with the future filesystem-aware storage strategy: cache
  settings can be per-storage-root so HDD roots get larger caches and
  SSD roots get smaller caches.
- Surface cache hit/miss statistics in the API and UI for performance
  diagnosis.

Acceptance direction:

- Cache settings are opt-in with sensible defaults; the daemon works well
  without any cache configuration.
- Cache settings never compromise data integrity: all writes are flushed
  and verified before a piece is marked complete.
- Cache statistics are informational only and do not affect download/upload
  behavior.
- Cache configuration is documented with clear guidance for common storage
  media (SSD, HDD, NAS, Btrfs).

### Protocol Compatibility Follow-Ons

**Priority: P2, sequence 13.** The P0 BEP 52 v2/hybrid identity and
interoperability program is the prerequisite foundation; this entry covers
distinct follow-on proposals only.

Problem: protocol support affects long-term compatibility and swarm reach, but
follow-on proposals require careful dependency and architecture review after
the v1/v2/hybrid foundation is in place.

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
- Keep new proposals separate from the P0 v1/v2/hybrid identity model so a
  compatibility preference cannot silently alter torrent identity or routing.

### Long-Horizon Observability

**Priority: P2, sequence 14.**

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

**Priority: P2, sequence 15.**

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

**Priority: P2, sequence 12.**

Problem: users downloading large files want to preview content before the full
download completes. Sequential and priority-first fetch enables playback-oriented
and preview-oriented workflows.

Requested elsewhere:

- qBittorrent and aria2 ship sequential download controls.
- WebTorrent and Deluge support streaming playback.
- Metadata-first preview (P0) complements this for magnet intake.

SwarmOtter feature shape:

- Add sequential download and priority-first fetch controls per torrent and
  per file.
- Add in-place preview and verify: check media integrity before committing to
  download.
- Tie to metadata-first preview (P0) for magnet workflows.

Acceptance direction:

- Controls are surfaced in API and UI per torrent and per file.
- Streaming/preview behavior is deterministic and contained.

### Time-of-Day and Adaptive Bandwidth Policies

**Priority: P2, sequence 16.**

Problem: the implemented adaptive swarm performance autopilot tunes global
bandwidth live based on measured throughput, latency, and queue state.
Operators also want time-of-day schedules (e.g. limit upload during
business hours, full-speed downloads overnight). These are two facets of
the same operational concern and should be implemented as a single
bandwidth-policy surface so the user mental model is one feature.

Requested elsewhere:

- qBittorrent, aria2, and Deluge ship bandwidth scheduling features.
- The adaptive autopilot is the SwarmOtter counterpart to live
  throughput tuning; combining it with scheduling makes the policy surface
  complete.

SwarmOtter feature shape:

- Add time-of-day alt-speed and bandwidth-limit schedules: multiple named
  schedules with start/end times and assigned bandwidth profiles.
- Schedule assignment per torrent, label, or profile.
- Schedule and adaptive autopilot share the same per-profile bandwidth
  resolution; the operator chooses the active mode per profile (adaptive,
  scheduled, or both with explicit precedence).
- Complements (does not replace) the implemented adaptive autopilot.

Acceptance direction:

- Schedules are deterministic and clearly reflected in the API and UI.
- Adaptive autopilot and scheduler interact predictably; precedence is
  documented and surfaced in the explainability API (P1).

### Backup / Restore & Bulk Import/Export

**Priority: P2, sequence 17.**

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

**Priority: P2, sequence 18.**

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
- Connection authentication via API keys with scoped permissions (and, if
  adopted, conditional Multi-User Support, P1).
- Maintain the existing REST API for HTTP-based integration; the streaming
  protocol is an additional surface for real-time operations.

Acceptance direction:

- Remote connections must use the same auth and permission model as the
  local API.
- Network containment is not weakened; remote control-plane connections are
  separate from the torrent data plane.
- Implementing this requires an ADR (new RPC surface + connection model).

### OpenTelemetry Observability

**Priority: P2, sequence 19.**

Problem: existing Prometheus metrics provide point-in-time data, but cloud-native
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
- Integrate with Long-Horizon Observability (P2) for unified
  metrics + traces.
- Integrate with the conditional Operator Audit Log (P1) so audit events can be exported
  over the same observability pipeline as metrics and traces.

Acceptance direction:

- OpenTelemetry is opt-in and does not add overhead when disabled.
- Tracing must not leak sensitive data (info hashes in spans are acceptable;
  peer IPs and file paths require configurable redaction).
- All telemetry export respects network containment.

### Cloud / Object-Storage-Backed Storage Root

**Priority: P2, sequence 20.**

Problem: institutional lawful distributors (datasets, public archives,
open-source release mirrors) increasingly keep their publishable content
in object storage (S3, S3-compatible, WebDAV) rather than on a local disk.
No mainstream torrent client treats object storage as a first-class
torrent storage root, so these operators must mount the bucket and
accept the limitations of a POSIX-over-object layer. A native
object-storage-backed storage root fits SwarmOtter's lawful-distribution
mission and complements shipped storage-root controls, the future
filesystem-aware storage strategy, and torrent creation (P1).

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
- Reuse the existing piece-hash verification path; an object-storage root is
  treated like any other storage root by the resource-control and future
  filesystem-aware storage layers.
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

**Priority: P2, sequence 21.**

Problem: operators running legal swarms (Linux ISOs, open-source
releases, public archives) want to understand the geographic and ASN
distribution of the peers connecting to their seeders, both for
distribution planning and for abuse detection. Today they get a per-peer
IP string at most. Complementing the client-identity fingerprinting
rollup (P2), an on-device GeoIP/ASN rollup gives legal-swarm
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
  and the intersection with the client-identity rollup (P2).
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
- This complements the implemented peer-admission filtering surface for abuse
  mitigation.

### Responsive / Mobile-Friendly Web UI

**Priority: P2, sequence 22.**

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
- Reuse the implemented large-library operations console
  server-side filtering and pagination so the mobile view does not load
  the full list.

Acceptance direction:

- Core operations (view list, open details, pause/resume/stop, basic
  add) work without horizontal scrolling on common phone widths.
- No new framework dependency; CSS-only or minimal-adjustment changes
  preferred.
- This may be folded into the settings search and UI personalization
  item (P2) if a shared UI polish pass is planned.

## Research Details

### Permissioned Extension System

**Priority: P3, sequence 2.**

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

**Priority: P3, sequence 3.**

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

**Priority: P3, sequence 4.**

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

**Priority: P3, sequence 5.** The P1 API-backed CLI should establish the
scriptable terminal workflow before a full TUI is considered.

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

**Priority: P3, sequence 6.**

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

### Version-Aware Contextual Documentation

**Priority: P3, sequence 7.** mdBook search is already generated and included
with the documentation. The remaining research candidate is a built-in,
version-aware help affordance for the Web UI and API, not another external
search-index project.

Problem: operators can search the public documentation today, but the Web UI
does not yet point a user from a current screen, status, or error to the
matching version of its operational guidance.

Requested elsewhere:

- Sonarr, Radarr, and rTorrent/ruTorrent all publish user guides; few
  surface in-app search.
- Self-hosting operators with large libraries routinely want to search
  their own documentation as much as their settings.

SwarmOtter research direction:

- Add a built-in help pane or contextual links that surface relevant,
  locally served documentation pages for the current view, API response, or
  operator error.
- Keep contextual documentation tied to the daemon version so a result always
  matches the behavior it describes.
- Reuse the existing mdBook search artifact where search is needed; do not
  introduce a separate external index or telemetry requirement.

Acceptance direction:

- The built-in help pane reuses existing in-UI conventions and does not
  introduce a new framework.
- No telemetry, no third-party analytics, no required external service.
- Contextual help is generated from or linked to the same versioned docs
  source; no parallel content.

## Excluded From This Backlog

The investigated issue trackers include requests around built-in search, search
plugins, bundled indexers, and broad content discovery. Those requests are
intentionally excluded from this backlog because SwarmOtter is not a torrent
indexer or piracy-assistant project.

Bundled indexers, search plugins, and content-discovery integrations that serve
piracy use cases are excluded permanently. User-configured lawful RSS feed
ingestion is not excluded (see User-Configured Lawful RSS Feeds, P2). Any future
RSS or discovery-adjacent capability must first pass the lawful-use and content
policy requirements in `design/content-policy.md` and `design/lawful-use.md`.
