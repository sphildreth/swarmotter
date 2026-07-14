# SwarmOtter Comparison Matrix

Last reviewed: 2026-07-13

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

## Table stakes vs. differentiators

This comparison distinguishes between two classes of features:

- **Table stakes**: features that every major mainstream torrent client
  (qBittorrent, Transmission, Deluge, BiglyBT) ships as standard. Users
  expect these by default; their absence disqualifies a client from
  consideration regardless of other strengths. Examples include UPnP/NAT-PMP
  port forwarding, SOCKS5 proxy support, IP filtering/blocklists, peer
  encryption (MSE/PE), and listen port reachability testing. SwarmOtter closes
  those gaps with contained, opt-in controls, including TCP-only SOCKS5 proxy
  support (UDP is deliberately unavailable) and MSE/PE for TCP and uTP. The
  remaining roadmap gaps are prioritized by product value and fit.

- **Differentiators**: features where SwarmOtter offers something no
  mainstream client provides, or offers it in a meaningfully better way.
  Examples include fail-closed VPN/NIC containment, the adaptive autopilot,
  the explainability API, and the lawful-use posture. These are the
  reasons a user would choose SwarmOtter over an established client.

The Roadmap Gap Map records the remaining product gaps and differentiator
candidates.

## Footnotes

- **Peer encryption (SwarmOtter):** SwarmOtter implemented MSE/PE-style
  Message Stream Encryption / Protocol Encryption for contained TCP and uTP
  peer streams. Global, profile, and durable per-torrent modes use
  `disabled` | `preferred` | `required`, with `preferred` as the default and
  no separate socket paths. Encryption remains under network containment;
  required mode never silently retries plaintext.

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
| Roadmap P1/P2/P3 | Not completed in SwarmOtter and tracked in `design/BACKLOG.md` at that priority. |

## Project Positioning

| Project | Best Fit | Main Surfaces | Notable Strengths | Important Trade-Offs |
| --- | --- | --- | --- | --- |
| SwarmOtter | Linux/server and homelab torrent daemon with strong operational controls | Daemon, REST API, WebSocket/SSE events, Web UI, bounded qBittorrent and Transmission automation adapters | Fail-closed VPN/NIC containment, API-first design, doctor/health checks, performance diagnostics, lawful-use posture | No desktop-native UI; no built-in search/indexer by policy; some ecosystem and shared-server features are roadmap items |
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
| Compatibility API | Partial (Transmission RPC and bounded qBittorrent WebUI automation adapters) | Native Transmission RPC | Native qBittorrent WebUI API | ❌ | Transmission-style remote control support | ❌ | ❌ |
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
| Peer encryption (MSE/PE) | ✅ | Partial | ✅ | ✅ | ✅ | ✅ | ✅ |
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
| IP filtering/blocklists | ✅ | ✅ | ✅ | Plugin | ✅ | ❌ | Partial |
| UPnP/NAT-PMP | ✅ (opt-in, contained) | ✅ | ✅ | ✅ | ✅ | ❌ | Partial |
| SOCKS/proxy support | ✅ (opt-in, contained TCP CONNECT; UDP unavailable) | ❌ | ✅ | ✅ | ✅ | ✅ | Partial |
| HTTP/HTTPS proxy support | Roadmap P1 | ❌ | ✅ | ❌ | ❌ | ✅ | Partial |
| Listen port reachability test | ✅ (opt-in, contained operator endpoint) | ✅ | ✅ | ✅ | ✅ | ❌ | Partial |
| Anonymous mode | Roadmap P1 | ❌ | ✅ | ✅ | ✅ | ❌ | Partial |
| Torrent metadata display (comment, creator, date) | Roadmap P1 | ✅ | ✅ | ✅ | ✅ | Partial | Partial |
| Magnet link generation from added torrents | Roadmap P1 | ✅ | ✅ | ✅ | ✅ | Partial | Partial |
| Torrent file export | Partial (native exact-original metainfo API; Web UI/batch export remains roadmap) | ✅ | ✅ | ✅ | ✅ | Partial | Partial |
| Categories/tags/policy groups | Partial (labels and named profiles) | Partial | ✅ | Plugin | ✅ | ❌ | Partial |
| Large-library UI operations | ✅ (server-side query, pagination, and efficient table workflows) | Partial | Partial | Partial | Partial | ❌ | Partial |
| Bulk import/export and backup | Roadmap P2 | Partial | Partial | Partial | Migration tooling | Session files | Session files |
| Automation hooks | Roadmap P1 safe hooks | Completion script | External program/API | Execute plugin/API | Plugins | RPC/scripts | XMLRPC/scripts |
| OpenAPI/interactive API docs | Roadmap P1 | ❌ | ❌ | Partial docs | ❌ | RPC docs | ❌ |
| Long-horizon observability | Roadmap P2; peak logs exist | Partial | Partial | Plugin | Plugin | Logs/RPC | Scripts/plugins |
| Disk cache / I/O buffer configuration | Roadmap P2 | ✅ | ✅ | ✅ | ✅ | Partial | Partial |

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
| qBittorrent API emulation | Partial (opt-in bounded lifecycle, category/profile, and inspection adapter) | ❌ | Native API | ❌ | ❌ | ❌ | ❌ |
| Adaptive swarm performance autopilot | ✅ | ❌ | ❌ | ❌ | Partial/plugin concepts | ❌ | Scripts/plugins |
| Storage-root resource controls | ✅ | ❌ | ❌ | ❌ | Partial disk views | ❌ | Manual |
| Filesystem-aware storage strategy and state placement | ✅ (mount/I/O diagnostics, placement, explicit Btrfs NOCOW) | ❌ | ❌ | ❌ | Partial disk views | ❌ | Manual |
| Per-profile/per-torrent network path binding | Roadmap P1 (conditional) | ❌ | ❌ | ❌ | Partial VPN helper | ❌ | Manual |
| Multi-user/multi-tenant operation | Roadmap P1 (conditional) | ❌ | ❌ | Partial auth | ❌ | RPC token only | Partial via deployment |
| Permissioned extension system | Roadmap P3 | ❌ | Search plugins only | ✅ | ✅ | ❌ | ruTorrent plugins |
| Swarm merging | Roadmap P3 | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |

## Roadmap Gap Map

`design/BACKLOG.md`'s Feature Map is the authoritative, non-duplicated
priority list. It deliberately removes completed work rather than retaining a
checked-off history. This comparison records current support in the tables
above; consult the Feature Map for the ordered remaining opportunities.

The next P1 work is lawful v1/v2/hybrid torrent creation; source-metadata
display, deterministic magnet generation, and Web UI/batch exact-metainfo
portability; cleanup safety; an API-backed CLI; API discovery; and tracker/peer
operations. Shared-server routing and multi-user features remain conditional,
and protocol work beyond the completed BEP 52 foundation remains P2.

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
