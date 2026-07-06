# SwarmOtter Comparison Matrix

Last reviewed: 2026-07-03

This document compares SwarmOtter with popular free and open-source torrent
clients. It is intended to help users understand product fit, feature coverage,
and roadmap gaps as SwarmOtter matures.

This is a living product comparison, not a release checklist. SwarmOtter
feature status is based on this repository's `README.md`,
`design/requirements.md`, `design/v1-completion-tracker.md`, and
`design/BACKLOG.md`. Other client status is based on official project sites,
project-owned documentation, and project repositories listed in
[Sources](#sources).

SwarmOtter is not a torrent indexer, search engine, piracy assistant, or
content-discovery tool. Features involving feeds or automation are framed for
lawful, user-configured distribution workflows only.

## Why the policy exclusions are a feature, not a gap

Several cells in this matrix show `❌ Policy` for built-in search, bundled
indexers, and content-discovery integrations. This is a deliberate product
posture, not a missing feature. Many institutional, educational,
public-sector, enterprise-adjacent, and family-shared-server deployments
cannot adopt a client that ships search plugins, default indexers, or
content-discovery surfaces, regardless of how those features are framed.
By refusing to ship those surfaces, SwarmOtter fits a deployment niche
that mainstream clients cannot occupy: the daemon is safe to hand to
non-technical operators and to run in policy-restricted environments
without per-deployment content scrubbing. The lawful-use and content-
policy posture is therefore a market-differentiating feature, not a
limitation, and it is one of the project's primary competitive moats.
See `design/lawful-use.md` and `design/content-policy.md` for the
authoritative scope.

## Footnotes

- **Peer encryption (SwarmOtter):** SwarmOtter implemented MSE/PE-style
  Message Stream Encryption / Protocol Encryption for TCP peer connections in v1.1.0 with
  configurable `torrent.encryption_mode` (`disabled` | `preferred` | `required`),
  default `preferred`, and no separate socket paths.
  Encryption runs on the contained peer transport and never bypasses network
  containment. Remaining work is not yet complete for uTP and per-profile/per-torrent
  overrides.

- **Local peer discovery (SwarmOtter ❌):** SwarmOtter deliberately does
  not implement local-network peer discovery. Local discovery is a
  convenience feature for finding peers on the same LAN; for a daemon
  that targets fail-closed network containment, the failure modes
  (unintended broadcast, multicast on hostile LANs, discovery on
  networks the operator did not approve) outweigh the benefit. Operators
  who need LAN-local peer sharing can configure a private tracker or a
  known peer set explicitly. This is a scope decision, not a roadmap
  item.

## Legend

| Status | Meaning |
| --- | --- |
| ✅ | Supported by the project or official distribution. |
| Partial | Supported with meaningful limitations, narrower scope, or less direct UI/API coverage. |
| Plugin | Available through an official or common plugin/extension path. |
| ❌ | Not provided by the checked project sources. |
| ❌ Policy | Intentionally excluded by SwarmOtter policy. |
| Roadmap P0/P1/P2/P3 | Not completed in SwarmOtter and tracked in `design/BACKLOG.md` at that priority. |

## Project Positioning

| Project | Best Fit | Main Surfaces | Notable Strengths | Important Trade-Offs |
| --- | --- | --- | --- | --- |
| SwarmOtter | Linux/server and homelab torrent daemon with strong operational controls | Daemon, REST API, WebSocket/SSE events, Web UI, Transmission RPC compatibility | Fail-closed VPN/NIC containment, API-first design, doctor/health checks, performance diagnostics, lawful-use posture | No desktop-native UI; no built-in search/indexer by policy; some ecosystem and large-library features are roadmap items |
| Transmission | Simple, lightweight desktop/server torrenting | Native desktop UIs, daemon, Web UI, `transmission-remote`, Transmission RPC | Low resource use, mature native UIs, straightforward remote control | Smaller feature surface than qBittorrent/BiglyBT; limited policy/profile model |
| qBittorrent | Full-featured desktop and Web UI client | Qt desktop UI, Web UI, WebUI API | Broad core feature coverage, RSS, search plugins, categories/tags, sequential download, bandwidth scheduler | Desktop-first architecture; no SwarmOtter-style fail-closed data-plane containment |
| Deluge | Daemon/client model with plugin-friendly operation | Daemon, GTK UI, Web UI, Console UI, Deluge RPC/Web API | Thin-client architecture, libtorrent core, plugins, multiple official UIs | Feature depth often depends on plugins; no SwarmOtter-style containment model |
| BiglyBT | Feature-rich desktop client with extensive plugins | Desktop UI, plugins, remote-control plugins | Tags/categories, swarm merging, WebTorrent, I2P helper, rich plugin ecosystem | Large desktop application footprint; broad content-discovery/social features do not match SwarmOtter policy |
| aria2 | Scriptable multi-protocol downloader | CLI, JSON-RPC, XML-RPC | Lightweight multi-source HTTP/FTP/SFTP/BitTorrent/Metalink downloader | Not a full library-management torrent client or integrated Web UI |
| rTorrent + ruTorrent | Terminal/server deployments and seedbox-style operations | ncurses TUI, XMLRPC, ruTorrent Web UI | Lean terminal operation, remote-control ecosystem, ruTorrent plugins | More manual setup; Web UI is a separate front end; no SwarmOtter-style containment model |

## Project Shape

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Desktop GUI | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| Web UI | ✅ | ✅ | ✅ | ✅ | Plugin | ❌ bundled Web UI | ✅ via ruTorrent |
| Headless/server mode | ✅ | ✅ | ✅ | ✅ | Partial | ✅ | ✅ |
| CLI/TUI | Roadmap P1 CLI, Roadmap P3 TUI | `transmission-remote` and tools | Partial server binary | Console UI | ❌ primary CLI/TUI | ✅ | ✅ |
| Native API/RPC | REST, WebSocket, SSE | Transmission RPC | WebUI API | Deluge RPC/Web API | Plugin/remote APIs | JSON-RPC, XML-RPC | XMLRPC |
| Compatibility API | Transmission RPC; qBittorrent API Roadmap P0 | Native Transmission RPC | Native qBittorrent WebUI API | ❌ | Transmission-style remote control support | ❌ | ❌ |
| Plugin/extension model | Roadmap P3 | Limited add-ons | Search plugins | ✅ | ✅ | ❌ | ruTorrent plugins and scripts |
| License | Apache-2.0 | GPLv2/GPLv3 family | GPLv2+ source, GPLv3+ binaries | GPLv3 with OpenSSL exception | GPLv2 | GPLv2 | rTorrent GPLv2, ruTorrent GPLv3+ |

## Core Torrent Features

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `.torrent` file intake | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Magnet links | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| DHT | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| PEX | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Local peer discovery | ❌ | Partial | ✅ | ✅ | ✅ | ✅ | Partial |
| HTTP/HTTPS trackers | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| UDP trackers | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Webseeds | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Partial |
| uTP | ✅ | ✅ | ✅ | ✅ | Plugin/core plugin | ❌ | ❌ |
| IPv6 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Partial |
| Peer encryption (MSE/PE) | Partial | Partial | ✅ | ✅ | ✅ | ✅ | ✅ |
| Private torrent handling | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| File selection/priorities | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Queueing/priorities | ✅ | ✅ | ✅ | ✅ | ✅ | Partial | ✅ |
| Ratio/seeding controls | ✅ | ✅ | ✅ | ✅ | ✅ | Partial | ✅ |
| Bandwidth limits | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Fast resume and recheck | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

## Operations And Automation

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Watch folders | ✅ | ✅ | ✅ | Plugin | ✅ | ❌ | ✅ |
| RSS feed automation | Roadmap P1 for user-configured lawful feeds | ❌ | ✅ | Plugin | ✅ + Plugin | ❌ | Plugin/scripts |
| Built-in search/indexer | ❌ Policy | ❌ | ✅ | ❌ | ✅ | ❌ | Plugin |
| Sequential download/streaming | Roadmap P2 | Partial | ✅ | Partial | ✅ | Partial | Partial |
| Torrent creation | Roadmap P1 | ✅ | ✅ | Plugin | ✅ | ❌ | Plugin/scripts |
| Superseeding/initial seeding | Roadmap P1 | Partial | ✅ | ✅ | ✅ | ❌ | ✅ |
| IP filtering/blocklists | Roadmap P1 | ✅ | ✅ | Plugin | ✅ | ❌ | Partial |
| UPnP/NAT-PMP | Roadmap P1 | ✅ | ✅ | ✅ | ✅ | ❌ | Partial |
| SOCKS/proxy support | Roadmap P1 | ❌ | ✅ | ✅ | ✅ | ✅ | Partial |
| HTTP/HTTPS proxy support | Roadmap P1 | ❌ | ✅ | ❌ | ❌ | ✅ | Partial |
| Categories/tags/policy groups | Roadmap P0 policy profiles | Partial | ✅ | Plugin | ✅ | ❌ | Partial |
| Large-library UI operations | Roadmap P0 | Partial | Partial | Partial | Partial | ❌ | Partial |
| Bulk import/export and backup | Roadmap P2 | Partial | Partial | Partial | Migration tooling | Session files | Session files |
| Automation hooks | Roadmap P1 safe hooks | Completion script | External program/API | Execute plugin/API | Plugins | RPC/scripts | XMLRPC/scripts |
| OpenAPI/interactive API docs | Roadmap P1 | ❌ | ❌ | Partial docs | ❌ | RPC docs | ❌ |
| Long-horizon observability | Roadmap P2; peak logs exist | Partial | Partial | Plugin | Plugin | Logs/RPC | Scripts/plugins |

## SwarmOtter Differentiators

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Fail-closed VPN/NIC data-plane containment | ✅ | ❌ | ❌ | ❌ | Plugin-assisted VPN binding | ❌ | Manual configuration |
| Containment covers peers, trackers, DHT, PEX, webseeds, DNS | ✅ | ❌ | ❌ | ❌ | Partial | ❌ | Manual configuration |
| Runtime doctor/health report | ✅ | ❌ | ❌ | ❌ | Partial plugin/status views | ❌ | ❌ |
| Per-torrent health score | ✅ | ❌ | ❌ | ❌ | Partial status views | ❌ | ❌ |
| Peak throughput performance logging | ✅ | ❌ | ❌ | ❌ | Plugin/stat views | ❌ | Scripts/plugins |
| Native REST API plus event streams | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Transmission API emulation | ✅ | Native API | ❌ | ❌ | Partial remote support | ❌ | ❌ |
| qBittorrent API emulation | Roadmap P0 | ❌ | Native API | ❌ | ❌ | ❌ | ❌ |
| Adaptive swarm performance autopilot | Roadmap P0 | ❌ | ❌ | ❌ | Partial/plugin concepts | ❌ | Scripts/plugins |
| Disk-aware storage optimizer | Roadmap P0 | ❌ | ❌ | ❌ | Partial disk views | ❌ | Manual |
| Per-profile/per-torrent network path binding | Roadmap P0 | ❌ | ❌ | ❌ | Partial VPN helper | ❌ | Manual |
| Multi-user/multi-tenant operation | Roadmap P0 | ❌ | ❌ | Partial auth | ❌ | RPC token only | Partial via deployment |
| Permissioned extension system | Roadmap P3 | ❌ | Search plugins only | ✅ | ✅ | ❌ | ruTorrent plugins |
| Swarm merging | Roadmap P3 | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |

## Roadmap Gap Map

These are SwarmOtter gaps or differentiator candidates from
`design/BACKLOG.md`. When one of these is implemented, tested, documented, and
usable, remove it from `design/BACKLOG.md` and update this comparison.

| Priority | Backlog Feature | Comparison Impact |
| --- | --- | --- |
| P0 | Adaptive swarm performance autopilot | Differentiates SwarmOtter on real-world throughput diagnosis and automatic swarm tuning. |
| P0 | Disk-aware storage optimizer | Adds storage-root health, disk pressure controls, CoW-aware behavior, and queue decisions based on disk conditions. |
| P0 | Policy profiles and inherited torrent settings | Closes category/tag/profile parity gaps and gives SwarmOtter a clearer policy model than most clients. |
| P0 | Large-library Web UI operations console | Closes operational gaps for hundreds or thousands of torrents through server-side filtering, sorting, grouping, pagination, and bulk actions. |
| P0 | Ecosystem Compatibility API | Adds qBittorrent-compatible API support beside the existing Transmission RPC compatibility layer. |
| P0 | Per-Profile / Per-Torrent Network-Path Binding | Extends containment from one daemon-wide path to contained network paths by profile or torrent. |
| P0 | Multi-User / Multi-Tenant Support | Adds role-based access, isolation, quotas, and shared-server workflows. |
| P0 | Protocol Encryption / MSE-PE | TCP MSE/PE is now implemented with configurable `required/preferred/disabled`; remaining work is uTP encryption and per-profile/per-torrent override policy. |
| P1 | HTTP / HTTPS Proxy Support | Adds egress through corporate/filtered HTTP proxies alongside the existing SOCKS5 (P1) entry. |
| P1 | Scriptable CLI (`swarmotterctl`) | Adds a scriptable, JSON-output CLI mirroring the API for SSH and automation workflows without a browser. |
| P1 | Seedbox Pre-Seed Warm-Up | Adds first-peer serving optimization for new lawful releases; complements superseeding (P1). |
| P1 | Idempotent Re-Add / Content-Addressed Import | Reduces re-add/re-verify friction for large libraries and cross-seed workflows. |
| P1 | Durable State Store (SQLite) | Enables cheap queue, health, audit, and history queries beyond per-torrent resume files; underpins several other roadmap items. |
| P1 | Metadata-first magnet preview and intake rules | Closes the gap with clients that let users inspect magnet metadata before data transfer. |
| P1 | File cleanup, trash, and retention safety | Improves destructive-operation safety and partial-data management. |
| P1 | Tracker and peer operations workbench | Improves swarm diagnostics beyond current summary health and log events. |
| P1 | Secure remote-operations hardening | Strengthens reverse-proxy and automation deployments. |
| P1 | Safe automation hooks | Adds explicit allowlisted event actions without hidden unsafe scripts. |
| P1 | Content organization controls | Adds preset paths, folder rules, and path normalization. |
| P1 | Torrent Creation (BEP 52 v2/hybrid) | Closes torrent-creation parity with Transmission, qBittorrent, BiglyBT, and plugin-based clients. |
| P1 | Superseeding / Initial Seeding (BEP 16) | Improves first distribution of lawful releases. |
| P1 | IP Filtering / Blocklists / Peer Banning | Closes blocklist and manual peer-ban parity. |
| P1 | UPnP / NAT-PMP Port Forwarding | Adds automatic port mapping where it is operationally acceptable. |
| P1 | SOCKS5 Proxy Support | Adds a common deployment option while preserving containment requirements. |
| P1 | Seed Prioritization (Low-Seed First) | Improves swarm-health-aware seeding behavior. |
| P1 | OpenAPI Specification & Interactive API Docs | Improves automation discoverability for native and compatibility APIs. |
| P1 | User-Configured Lawful RSS Feeds | Adds feed automation for lawful user-supplied sources without making SwarmOtter a content-discovery product. |
| P1 | Trust and Provenance Signals for Torrents and Trackers | Adds per-tracker trust state, allow/deny integration, and signed-`.torrent` provenance verification for institutional lawful-distribution workflows. No mainstream client offers a comparable tracker-trust surface. |
| P1 | Operator Audit Log for Torrent Lifecycle Events | Adds a structured, exportable, optionally hash-chained audit trail for privileged operations. Combined with Multi-User (P0) this is the compliance story shared-server and seedbox deployments need. No mainstream client offers a comparable surface. |
| P1 | Explainability API: Structured Reasons for Non-Trivial Decisions | Unifies "why is this slow / dead / rejected / blocked" across autopilot, disk optimizer, fail-closed, and bandwidth decisions behind one machine-readable code-and-message surface. No mainstream client offers this. |
| P1 | Container / Sandbox-First Deployment Story | Promotes the OCI image, rootless operation, read-only-filesystem operation, Helm chart, and Compose file to first-class artifacts in this repository. Matches the "Linux/server and homelab" positioning in the Project Positioning table. |
| P1 | Production Health / Availability Surface | Adds `/healthz/live` and `/healthz/ready` plus a synthetic end-to-end check torrent and SLO-style summaries, making SwarmOtter the first torrent daemon suitable as a Kubernetes/CI workload. |
| P1 | Filesystem Snapshot Integration | Adds opt-in snapshot hooks for Btrfs subvolumes, ZFS, and Snapper so operators get rollback for torrent roots and state directories. No mainstream client offers this. |
| P1 | Client-Identity Fingerprinting and Rollups | Adds per-torrent and per-tracker client composition rollups so operators of legal swarms can prioritize compatibility and understand their contribution. |
| P1 | Native Cross-Seed & Hardlink-Aware Storage | Reduces duplicate downloading and improves storage efficiency for legal multi-torrent libraries. |
| P2 | Sequential Download / Streaming / File Preview | Closes a common qBittorrent/BiglyBT/aria2-style user workflow gap. |
| P2 | Protocol modernization roadmap | Tracks BEP 52 v2/hybrid and other protocol compatibility improvements. |
| P2 | Long-horizon observability | Adds historical metrics beyond current live status and peak log events. |
| P2 | Settings search and low-risk UI personalization | Improves dense configuration UX without changing project priorities. |
| P2 | Time-of-Day and Adaptive Bandwidth Policies | Combines calendar-style schedules with the Adaptive Swarm Performance Autopilot (P0) into a single per-profile bandwidth policy surface; the user mental model is one feature, not two. |
| P2 | Backup / Restore & Bulk Import/Export | Improves migration and disaster recovery for large libraries. |
| P2 | Thin Client / Remote Session Architecture | Adds a richer remote-client model beyond the Web UI and API. |
| P2 | OpenTelemetry Observability | Adds cloud-native metrics and tracing export. |
| P2 | Cloud / Object-Storage-Backed Storage Root | Adds S3/WebDAV-backed torrent storage for institutional lawful distributors; no mainstream client owns this. |
| P2 | Local GeoIP / ASN Peer Rollups | Adds on-device geographic and ASN rollups for legal-swarm operators; complements Client-Identity Fingerprinting (P1). |
| P2 | Responsive / Mobile-Friendly Web UI | Adds touch and small-viewport operation for homelab phone-check workflows. |
| P3 | Permissioned extension system | Adds a plugin model only if permissions, sandboxing, and lawful-use constraints are resolved. |
| P3 | Alternate privacy-preserving transports | Requires containment, lawful-use, and operational-risk review before acceptance. |
| P3 | Swarm Merging (BiglyBT-style) | Adds matching-content acceleration across torrents or lawful HTTP sources. |
| P3 | Terminal UI / Console Interface | Adds a terminal-first operator interface similar in spirit to rTorrent or Deluge Console. |
| P3 | Localization Strategy for the Web UI, API Errors, and Docs | Adds a documented translation workflow and source-string extraction without localizing structured logs. |
| P3 | Documentation Discoverability | Adds a search index for `docs/` and a built-in help pane in the Web UI tied to the daemon version. |

## Sources

SwarmOtter sources:

- [`README.md`](../README.md)
- [`design/requirements.md`](requirements.md)
- [`design/v1-completion-tracker.md`](v1-completion-tracker.md)
- [`design/BACKLOG.md`](BACKLOG.md)

Project-owned external sources:

- Transmission: [official site](https://transmissionbt.com/),
  [GitHub repository](https://github.com/transmission/transmission), and
  [license file](https://raw.githubusercontent.com/transmission/transmission/main/COPYING)
- qBittorrent: [official site](https://www.qbittorrent.org/),
  [WebUI API documentation](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-5.0%29),
  and [license file](https://raw.githubusercontent.com/qbittorrent/qBittorrent/master/COPYING)
- Deluge: [GitHub repository](https://github.com/deluge-torrent/deluge),
  [Web UI documentation](https://deluge.readthedocs.io/en/latest/reference/web.html),
  and [license file](https://raw.githubusercontent.com/deluge-torrent/deluge/develop/LICENSE)
- BiglyBT: [official site](https://www.biglybt.com/),
  [feature list](https://www.biglybt.com/features.php),
  [plugins](https://plugins.biglybt.com/),
  [I2P documentation](https://github.com/BiglySoftware/BiglyBT/wiki/I2P),
  and [license file](https://raw.githubusercontent.com/BiglySoftware/BiglyBT/master/LICENSE)
- aria2: [official site](https://aria2.github.io/),
  [manual](https://aria2.github.io/manual/en/html/aria2c.html), and
  [license file](https://raw.githubusercontent.com/aria2/aria2/master/COPYING)
- rTorrent and ruTorrent: [rTorrent repository](https://github.com/rakshasa/rtorrent),
  [rTorrent Handbook](https://rtorrent-docs.readthedocs.io/en/latest/),
  [ruTorrent repository](https://github.com/Novik/ruTorrent), and
  [ruTorrent license file](https://raw.githubusercontent.com/Novik/ruTorrent/master/LICENSE.md)
