# SwarmOtter Comparison Matrix

Reviewed on 2026-11-21 against the upstream releases listed below and the
SwarmOtter working tree. SwarmOtter roadmap status was checked against Hamstik
on the same date. This is a feature comparison based on documentation and
release source. It does not include comparative performance or VPN leak tests.

## Project positioning

| Project and release reviewed | Deployment and distinguishing features |
| --- | --- |
| [SwarmOtter](#swarmotter) | Linux daemon with a Web UI, native REST API, live events, and bounded Transmission and qBittorrent compatibility adapters. Its network gate, storage controls, and diagnostics are part of the daemon. |
| [Transmission 4.1.4](#transmission) | Native macOS, GTK, and Qt clients, a standalone daemon, Web UI, and remote CLI share libtransmission. Labels, bandwidth groups, and scripts are available through the native control surface. |
| [qBittorrent 5.3.0](#qbittorrent) | Qt desktop client and headless `qbittorrent-nox` with a Web UI. Both use libtorrent-rasterbar. RSS rules, search providers, categories, tags, and a bandwidth scheduler are integrated into the client. |
| [Deluge 2.2.0](#deluge) | A libtorrent-rasterbar daemon with GTK, Web, and console clients. GTK can also run in standalone mode. Several familiar operations, including watch folders and blocklists, come from bundled plugins. |
| [BiglyBT 4.2.0.0](#biglybt) | Java client with a desktop UI, headless and console modes, and a Web Remote plugin. Includes tags, swarm merging, network diagnostics, and enforced interface binding. Extensions add WebTorrent and I2P connectivity. |
| [aria2 1.37.0](#aria2) | Command-line downloader and RPC service for HTTP(S), FTP, SFTP, BitTorrent, and Metalink. Remote front ends are separate applications. |
| [rTorrent 0.16.28 and ruTorrent 5.3.23](#rtorrent-and-rutorrent) | rTorrent provides the ncurses client, headless operation, and RPC. Its engine is Rakshasa's libtorrent 0.16.28. ruTorrent is a separately deployed Web UI with its own plugin distribution. |

**How to read the tables.** ✅ means the reviewed release provides the feature.
A named plugin or external tool identifies how the user gets it. A qualified
cell describes the scope of support, including adjacent features such as a
first-piece preference or a connection indicator. **—** means the
review found no documented implementation of the stated feature in that surface.
It makes no claim about every third-party tool or an administrator's OS setup.
For build-dependent features, the table describes upstream support. **Roadmap**
identifies open SwarmOtter work by its original priority tier and Hamstik ID.
P3 includes research decisions.

## Project shape and extensions

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Desktop GUI | — | macOS, GTK, Qt | Qt | GTK | SWT | — | — |
| Web UI | Bundled | Bundled | Bundled, replaceable | deluge-web | Web Remote plugin | External clients | ruTorrent |
| Headless/server mode | ✅ | transmission-daemon | qbittorrent-nox | deluged | Console or UI-free mode | aria2c daemon/RPC | rTorrent daemon mode |
| CLI/TUI | ✅ `swarmotterctl`; P3 TUI (SWO-42) | transmission-remote, create/show/edit tools | Launch/add options in desktop and nox binaries | deluge-console | Interactive console | aria2c CLI | ncurses TUI, commands and Lua scripting |
| Native control API and event delivery | REST, SSE and WebSocket events | HTTP RPC, JSON-RPC 2.0 in 4.1, legacy dialect retained | HTTP WebAPI with incremental sync polling | Daemon RPC with events, Web JSON-RPC | Java plugin API, Simple API and Web Remote plugins | JSON-RPC over HTTP/WebSocket, WebSocket notifications, XML-RPC | XML-RPC or JSON-RPC over SCGI, depending on build/configuration |
| Extension mechanism | Roadmap P3 research (SWO-39) | External scripts and RPC tools | Python search providers, alternative Web UIs | Python plugins with core, GTK and Web entry points | Java plugins | Event commands and RPC clients | rTorrent commands/Lua, ruTorrent PHP/JavaScript plugins |
| Plugin discovery and installation | No shipped catalog. SWO-39 is research. | Project add-ons page lists external tools | Search plugin manager, project-maintained and community provider lists | Bundled plugins, project plugin directory, installable egg packages | Project catalog with install links and an in-client installer | Project lists external RPC front ends | Plugin directories shipped with ruTorrent, project wiki, manual installation |
| Runtime grants to plugin code for files, credentials, or network | Proposed in SWO-39 | — | — | — | — | — | ruTorrent has per-plugin enablement and UI permissions. Runtime grants for these resources are undocumented. |
| Project license | Apache-2.0 | GPLv2 or GPLv3, or a future license endorsed by Mnemosyne LLC. OpenSSL linking permitted. | GPLv2+ source, GPLv3+ binaries, OpenSSL exception | GPLv3 with OpenSSL exception, source headers specify v3 or later | GPLv2+ | GPLv2+ with OpenSSL linking exception | rTorrent GPLv2+ with OpenSSL exception for non-NSS code, ruTorrent GPLv3+ |

A plugin catalog answers how users find code. Enablement, supported extension
points, and runtime access to files or the network are separate properties.
For example, ruTorrent's plugin permissions govern changes to menus, tabs,
columns, and other UI elements. SwarmOtter currently has neither a plugin
runtime nor a catalog. Torrent search/indexer integration is also a separate
product decision. [ruTorrent plugin settings][ru-perms] · [SwarmOtter backlog](BACKLOG.md)

## Core torrent features

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Open .torrent files and magnet links | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| DHT | Mainline | Mainline | Mainline | Mainline | BiglyBT DHT, Mainline DHT plugin | Mainline | Mainline |
| Peer exchange (PEX) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Local-network peer discovery | Deliberately omitted | LPD, off by default | Local Peer Discovery | Local Service Discovery | Bundled LAN Peer Finder | `--bt-enable-lpd` | — |
| HTTP and HTTPS trackers | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| UDP trackers | ✅ | ✅ | ✅ | ✅ | ✅ | Requires IPv4 DHT enabled | ✅ |
| Webseeds | ✅ | ✅ | ✅ | ✅ | External Seed plugin in distribution | ✅ | — |
| uTP peer transport | ✅ | ✅ | ✅ | ✅ | uTP plugin | — | — |
| IPv6 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | IPv6 peers, trackers and DHT in reviewed engine |
| Peer encryption (MSE/PE) | TCP and uTP, disabled/preferred/required | Allowed, preferred, required | ✅ | ✅ | ✅ | ✅ | ✅ |
| Recognize private torrents | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| File selection and priorities | ✅ | ✅ | ✅ | ✅ | ✅ | Select files, prioritize head/tail pieces | ✅ |
| Queue management | ✅ | Download/seed queues and queue positions | ✅ | ✅ | ✅ | Concurrency limit and changeable queue position | Priorities, views and scheduler |
| Ratio/seeding controls | ✅ | ✅ | ✅ | ✅ | ✅ | Seed ratio and seed time | Ratio groups, ruTorrent ratio plugin |
| Bandwidth limits | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Resume and integrity recheck | ✅ | ✅ | ✅ | ✅ | ✅ | .aria2 control files and integrity check | Session state and hash check |

SwarmOtter applies global, profile, and per-torrent peer encryption policy to
both TCP and uTP. Preferred is the default. Required mode rejects plaintext.
SwarmOtter excludes LAN peer discovery because unsolicited multicast could
reach an unintended network. PEX-discovered peers still use its configured
network path. BiglyBT's LAN Peer Finder discovers other clients on the LAN.
This row describes the user-facing capability without assuming the same wire
protocol. [SwarmOtter containment](vpn-network-containment.md) · [BiglyBT LAN Peer Finder][bb-lan]

## Operations and automation

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Watch folders | ✅ | Watch directory | Monitored folders | Bundled AutoAdd plugin | Watch directories | — | rTorrent directory watch, ruTorrent autotools plugin |
| RSS feed automation | Configured feeds, rules and durable history (SWO-1) | External RSS tools | Built-in rules | Third-party YaRSS2, check package compatibility | RSS subscriptions and auto-download | — | ruTorrent rss plugin |
| Torrent search/indexers | — | — | Built-in search UI, Python provider plugins | — in distribution | Integrated search and templates | — | ruTorrent extsearch plugin |
| Sequential torrent downloading | Roadmap P2 (SWO-27) | Per-torrent sequential mode and start piece | Sequential mode | Sequential torrent option | Sequential download | Head/tail piece preference | First/last chunk preference per file |
| Create torrents | v1/v2/hybrid, export and immediate seeding (SWO-2) | transmission-create and GUI | Torrent Creator and WebAPI | Native `create_torrent` RPC and GTK dialog | Creation wizard | — | ruTorrent create plugin |
| Superseeding/initial seeding | Roadmap P2 (SWO-18) | — | Super seeding | Super seeding | Super-Seeding | — | `initial_seed` mode |
| Peer IP filtering/blocklists | ✅ | Blocklists | IP filter | Bundled Blocklist plugin | IP filter | — | rTorrent IPv4 filter tables and file import |
| Automatic port mapping | Contained, opt-in UPnP/NAT-PMP | UPnP/NAT-PMP | UPnP/NAT-PMP | UPnP/NAT-PMP | UPnP/NAT-PMP | — | — |
| SOCKS proxy | Contained TCP CONNECT, UDP unavailable | SOCKS for HTTP(S) requests | SOCKS4/5 | SOCKS4/5 | Tracker and outgoing peer settings | — | SOCKS5 for outgoing peers, separate proxy for HTTP requests |
| HTTP proxy | Roadmap P2 (SWO-24) | HTTP or HTTPS proxy for HTTP(S) requests | HTTP proxy option | HTTP proxy with optional auth | HTTP tracker proxy | HTTP proxy for supported URL transfers and HTTP(S) trackers | HTTP tracker proxy, HTTP CONNECT for outgoing peers |
| Incoming port reachability | Contained test to configured endpoint | Active `port_test` RPC | Incoming-connection status indicator | Active listen-port test | NAT/firewall test | — | ruTorrent check_port plugin |
| Anonymous mode / identity controls | Roadmap P1 (SWO-12) | — | libtorrent anonymous mode | libtorrent anonymous mode | Per-download public/I2P network selection, tracker identity settings | — | — |
| Torrent metadata (comment, creator, creation date) | ✅ (SWO-3) | RPC fields | Torrent properties | Comment and creator in Details | Torrent information | `--show-files` prints all three | Comment and creation date |
| Generate magnet link | From library entries (SWO-3) | `magnet_link` RPC | Copy magnet link | `get_magnet_uri` RPC | Copy magnet URI | `--show-files` prints a magnet from a .torrent | ruTorrent bulk_magnet plugin |
| Export or retain .torrent files | Original bytes or batch archive (SWO-3) | macOS Save Torrent File As, RPC exposes local path | .torrent export via WebAPI and desktop | Copy .torrent files to a configured directory | Save .torrent files | Save magnet metadata and RPC-uploaded .torrent files locally | ruTorrent source plugin downloads the source .torrent |
| Categories, labels and policy groups | Labels and named profiles | Labels and bandwidth groups | Categories and tags | Bundled Label plugin | Tags, categories and tag rules | — | Views, ruTorrent labels and ratio groups |
| Library browsing and bulk operations | Server-side search, sorting, pagination and bulk actions | Filtered UI, RPC selects IDs and fields | UI filters, paginated torrent list API and sync deltas | UI filters, bulk RPC status queries | Filterable library and tag views | RPC active/waiting/stopped lists, offset/limit | TUI views, Web UI filters, RPC multicall |
| Library backup and migration | Portable bundles on roadmap P2 (SWO-32) | Configuration, torrents and resume files | Profile/resume data and torrent file backups | Config and session state files | Built-in configuration backup/restore and migration tools | Session save/load and per-download control files | Session directory and separate ruTorrent settings |
| Automation hooks | Roadmap P2 (SWO-17) | Scripts on add, download completion and seeding completion | External program on add or completion, WebAPI | Bundled Execute plugin and RPC | Command Runner plugin, tag actions | Download event commands and RPC notifications | rTorrent event methods and scripts |
| API documentation / interactive explorer | OpenAPI and interactive docs on roadmap P1 (SWO-6) | RPC specification | WebAPI wiki and changelog | Core and Web API reference | Plugin API and Web Remote RPC specification | RPC reference with examples | Command reference and release source |
| Historical transfer statistics | Peak logs available, extended history on roadmap P2 (SWO-29) | Session and cumulative totals | Speed graphs and transfer totals | Bundled Stats plugin | Built-in long-term statistics, optional Advanced Statistics plugin | Logs, live RPC statistics | ruTorrent trafic and history plugins |
| Disk cache / I/O controls | Roadmap P2 (SWO-26) | Configurable memory cache | Disk I/O options depend on linked libtorrent | Cache settings depend on linked libtorrent | Disk cache controls | `--disk-cache` | Mapped piece memory and sync controls |

The distinction between an HTTP **proxy** and a proxied **protocol** matters.
Transmission's proxy setting covers HTTP(S) fetches such as tracker requests.
aria2's `--https-proxy` selects an HTTP proxy for HTTPS destinations. Neither
setting establishes SOCKS routing for BitTorrent peer traffic. Similarly, a
connection indicator reports observed incoming traffic, while an active port
test asks a remote endpoint to connect. [Transmission settings][tr-settings] · [aria2 manual][ar-manual] · [qBittorrent session code][qb-session]

## Containment, diagnostics and interoperability

| Capability | SwarmOtter | Transmission | qBittorrent | Deluge | BiglyBT | aria2 | rTorrent + ruTorrent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Network interface or address binding | Live fail-closed VPN/NIC gate | IPv4/IPv6 bind-address settings | Network interface and optional address for libtorrent | Listen and outgoing interface settings | Built-in binding can be enforced when the interface disappears | `--interface`, separate DHT and LPD options | rTorrent bind/listen addresses |
| Documented fail-closed coverage of peers, trackers, DHT, PEX-discovered peers, webseeds and DNS together | ✅ | — | — | — | Enforced bind documented. Full listed coverage unverified. | — | — |
| Runtime diagnostics | Doctor API and Web UI | Session stats and port test | Execution log and connection status | Daemon status and port test | Network Status and NAT tests, diagnostic logs | Logs and RPC status | TUI, logging and tracker status |
| Per-torrent health | Health score and reasons | Peer/availability data | Availability and peer counts | Status and peer counts | Built-in color-coded torrent health | Peer and transfer status | Chunk and peer views |
| Peak throughput reporting | Structured peak logs | Transfer rates and totals | Speed graphs | Stats plugin graphs | Peak upload/download speed columns | Transfer rates and logs | Traffic statistics plugin |
| Transmission RPC compatibility | Bounded, opt-in legacy RPC adapter | Native RPC | — | — | Web Remote implements a Transmission-compatible RPC subset | — | — |
| qBittorrent WebAPI compatibility | Bounded, opt-in automation adapter | — | Native WebAPI | — | — | — | — |
| Adaptive swarm performance controls | Per-torrent decisions, explanations and actions | Per-torrent upload slot limit | libtorrent choking/slot settings | libtorrent session settings | Auto-speed and seeding rules, AutoPilot plugin handles ratio quotas | Adaptive concurrent downloads option | Choke groups, throttles and scheduler |
| Per-storage-root resource limits | Write-rate and concurrent-recheck limits | — | — | — | — | — | Process-wide memory/sync settings |
| Filesystem-aware storage and state placement | Mount diagnostics, state placement, explicit Btrfs NOCOW | Preallocation and configurable directories | Allocation and save paths | Allocation and download paths | File allocation and placement settings | Choice of allocation method | Session path and file allocation settings |
| Per-profile/per-torrent network path | Conditional roadmap P1 (SWO-14) | Session bind settings | Session interface setting | Session interface settings | Per-download public/I2P network selection, global NIC bind | Process interface setting | Instance bind settings |
| Multiple users or tenants | Conditional roadmap P1 (SWO-15) | RPC credentials for the instance | Web UI credentials for the instance | Daemon accounts, auth levels, owned/shared torrents | Multi-user console roles | Instance RPC secret | ruTorrent user profiles can target separate rTorrent instances |
| Swarm merging across torrents | Roadmap P3 (SWO-41) | — | — | — | Built-in matching-file swarm merging | — | — |

SwarmOtter documents what happens to **existing and new** torrent connections
when its configured route, interface, address, or DNS policy fails. BiglyBT
documents an enforcement option that prevents connections when the chosen
interface is unavailable. qBittorrent sets libtorrent's listening and outgoing
interfaces. The coverage row records whether upstream documents the entire traffic set and
failure behavior together. [SwarmOtter containment](vpn-network-containment.md) · [BiglyBT VPN configuration][bb-vpn] · [qBittorrent interface implementation][qb-session]

## Roadmap gap map

Hamstik project `sph/SWO` records priorities, acceptance criteria and current
status. SWO-17 covers bounded hooks and notifications. SWO-39 evaluates
permissioned extensions and their trust model. The project considers proposals
for content search, indexers and discovery through its normal design process.
[Backlog guide](BACKLOG.md) · [Project stance](adr/0079-neutral-project-stance-and-user-responsibility.md)

## Sources

### SwarmOtter

[Project license](../LICENSE) ·
[Project overview](../README.md) ·
[Requirements](requirements.md) ·
[Completion tracker](v1-completion-tracker.md) ·
[API and compatibility contracts](../docs/api.md) ·
[Web UI and diagnostics](../docs/web-ui.md) ·
[Network containment](vpn-network-containment.md) ·
[Roadmap](BACKLOG.md)

### Transmission

[4.1.4 release](https://github.com/transmission/transmission/releases/tag/4.1.4) ·
[Settings, proxies, hooks and binding][tr-settings] ·
[RPC fields, port tests and queue operations](https://github.com/transmission/transmission/blob/4.1.4/docs/rpc-spec.md) ·
[Session and torrent files](https://github.com/transmission/transmission/blob/4.1.4/docs/Configuration-Files.md) ·
[macOS torrent file export](https://github.com/transmission/transmission/blob/4.1.4/macosx/Controller.mm) ·
[External add-ons](https://transmissionbt.com/addons) ·
[License](https://github.com/transmission/transmission/blob/4.1.4/COPYING)

### qBittorrent

[5.3.0 release and changes](https://github.com/qbittorrent/qBittorrent/blob/release-5.3.0/Changelog) ·
[Headless CLI](https://github.com/qbittorrent/qBittorrent/blob/release-5.3.0/doc/en/qbittorrent-nox.1.md) ·
[WebAPI](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-5.0%29) ·
[Release API changes](https://github.com/qbittorrent/qBittorrent/blob/release-5.3.0/WebAPI_Changelog.md) ·
[Network and engine settings][qb-session] ·
[libtorrent-rasterbar protocol support](https://libtorrent.org/features.html) ·
[Search plugins and installation](https://github.com/qbittorrent/search-plugins/wiki/Install-search-plugins) ·
[License](https://github.com/qbittorrent/qBittorrent/blob/release-5.3.0/COPYING)

### Deluge

[2.2.0 release](https://pypi.org/project/deluge/2.2.0/) ·
[Core RPC, creation and port test](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/deluge/core/core.py) ·
[Network, proxy and cache preferences](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/deluge/core/preferencesmanager.py) ·
[Sequential download, superseeding and metadata](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/deluge/core/torrent.py) ·
[Torrent ownership and sharing](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/deluge/core/torrentmanager.py) ·
[Bundled plugins](https://github.com/deluge-torrent/deluge/tree/deluge-2.2.0/deluge/plugins) ·
[Daemon, events and Web JSON-RPC](https://github.com/deluge-torrent/deluge/tree/deluge-2.2.0/docs/source/reference) ·
[Plugin loader](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/deluge/pluginmanagerbase.py) ·
[Plugin directory including YaRSS2](https://dev.deluge-torrent.org/wiki/Plugins) ·
[License](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/LICENSE)

### BiglyBT

[4.2.0.0 release](https://github.com/BiglySoftware/BiglyBT/releases/tag/v4.2.0.0) ·
[Features and subscriptions](https://www.biglybt.com/features.php) ·
[Console, headless and desktop UIs](https://github.com/BiglySoftware/BiglyBT/tree/v4.2.0.0/uis/src/com/biglybt/ui) ·
[Plugin catalog](https://plugins.biglybt.com/) ·
[Swarm merging](https://github.com/BiglySoftware/BiglyBT/wiki/Swarm-Merging) ·
[Web Remote RPC specification](https://github.com/BiglySoftware/BiglyBT-plugin-xmwebui/blob/master/rpc-spec.txt) ·
[VPN binding][bb-vpn] ·
[Tracker and peer proxy controls](https://github.com/BiglySoftware/BiglyBT/blob/v4.2.0.0/core/src/com/biglybt/ui/config/ConfigSectionConnectionProxy.java) ·
[LAN Peer Finder][bb-lan] ·
[Health, peaks and statistics in the UI](https://github.com/BiglySoftware/BiglyBT/blob/v4.2.0.0/core/src/com/biglybt/internat/MessagesBundle.properties) ·
[License notice](https://github.com/BiglySoftware/BiglyBT/blob/v4.2.0.0/core/src/com/biglybt/core/Core.java)

### aria2

[1.37.0 feature list and UDP tracker dependency](https://github.com/aria2/aria2/blob/release-1.37.0/README.rst) ·
[Options, RPC and event hooks][ar-manual] ·
[Torrent metadata and magnet output](https://github.com/aria2/aria2/blob/release-1.37.0/src/bittorrent_helper.h) ·
[External front ends](https://aria2.github.io/) ·
[License notice](https://github.com/aria2/aria2/blob/release-1.37.0/src/main.cc)

### rTorrent and ruTorrent

[rTorrent 0.16.28 release](https://github.com/rakshasa/rtorrent/releases/tag/v0.16.28) ·
[Matching libtorrent engine](https://github.com/rakshasa/libtorrent/tree/v0.16.28/src) ·
[RPC, binding and proxy controls](https://github.com/rakshasa/rtorrent/blob/v0.16.28/src/command_network.cc) ·
[Peer proxy implementation](https://github.com/rakshasa/libtorrent/blob/v0.16.28/src/torrent/runtime/proxy_manager.cc) ·
[File priority commands](https://github.com/rakshasa/rtorrent/blob/v0.16.28/src/command_file.cc) ·
[IP filtering](https://github.com/rakshasa/rtorrent/blob/v0.16.28/doc/manual/ip_filtering.md) ·
[Watch directory and session settings](https://github.com/rakshasa/rtorrent/blob/v0.16.28/doc/rtorrent.rc) ·
[ruTorrent 5.3.23 plugins](https://github.com/Novik/ruTorrent/tree/v5.3.23/plugins) ·
[Plugin permissions][ru-perms] ·
[rTorrent license notice](https://github.com/rakshasa/rtorrent/blob/v0.16.28/src/input/bindings.h) ·
[Linking exception](https://github.com/rakshasa/rtorrent/blob/v0.16.28/README.md#license) ·
[ruTorrent license](https://github.com/Novik/ruTorrent/blob/v5.3.23/LICENSE.md)

[tr-settings]: https://github.com/transmission/transmission/blob/4.1.4/docs/Editing-Configuration-Files.md
[qb-session]: https://github.com/qbittorrent/qBittorrent/blob/release-5.3.0/src/base/bittorrent/sessionimpl.cpp
[bb-vpn]: https://github.com/BiglySoftware/BiglyBT/wiki/Proxies-and-VPNs
[bb-lan]: https://github.com/BiglySoftware/BiglyBT/blob/v4.2.0.0/core/src/com/biglybt/plugin/tracker/local/LocalTrackerPlugin.java
[ar-manual]: https://github.com/aria2/aria2/blob/release-1.37.0/doc/manual-src/en/aria2c.rst
[ru-perms]: https://github.com/Novik/ruTorrent/blob/v5.3.23/conf/plugins.ini
