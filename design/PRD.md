# SwarmOtter v1.0.0 Requirements and Implementation Plan

## Project Overview

The goal is to build **SwarmOtter**, a high-performance, production-usable BitTorrent daemon in Rust that can replace a Transmission-style deployment while keeping the project identity distinct from Transmission and other existing BitTorrent clients.

SwarmOtter prioritizes correctness, performance, operational safety, API completeness, lawful-use positioning, and reliable torrent behavior over visual polish. The Web UI should be complete and usable, but it does not need to be fancy, animated, or visually elaborate. Function matters more than form.

SwarmOtter consists of:

1. A Rust-based BitTorrent engine.
2. A daemon process that owns torrent state, networking, disk I/O, queueing, settings, and lifecycle management.
3. A complete REST/WebSocket or REST/SSE API.
4. A practical Web UI that consumes the API.
5. Configuration and deployment support suitable for Linux, containers, and VPN-routed environments.

## Project Identity

The project name is **SwarmOtter**.

SwarmOtter should be described as a performance-first, VPN-aware, FOSS BitTorrent daemon for lawful torrents, open-source distribution, self-hosted automation, and strict network containment.

SwarmOtter is not a fork or rebrand of Transmission. It may be described as Transmission-style because it provides a daemon, API, and practical Web UI for managing torrents, but its branding, code, documentation, API design, and project identity should remain distinct.

Recommended short description:

> SwarmOtter is a performance-first Rust BitTorrent daemon with a practical Web UI, complete torrent-management features, and fail-closed VPN/NIC traffic containment.

Recommended repository topics and keywords:

- `rust`
- `bittorrent`
- `torrent-client`
- `daemon`
- `web-ui`
- `self-hosted`
- `vpn`
- `network-containment`
- `dht`
- `pex`
- `udp-tracker`


## Release Model

### No MVP Scope

This project does not use an MVP model.

There is no intentionally reduced first release. There is no separate "basic" release where DHT, PEX, UDP trackers, watch folders, file prioritization, queueing, bandwidth controls, VPN containment, or browser magnet handling are deferred.

All required features in this document are part of the initial release scope.

### v1.0.0 Definition

The first complete release is `v1.0.0`.

`v1.0.0` is reached only when every required feature and acceptance criterion in this document is implemented, tested, documented, and usable.

The initial public SwarmOtter release is `v1.0.0`. Earlier internal checkpoints may exist for development convenience, but they are not product releases and must not be treated as feature-complete deliverables.

### No Calendar or Duration Estimates

Do not include time estimates, calendar estimates, sprint estimates, week ranges, or duration guesses in this plan.

Time estimates from coding agents are not useful for this project and should not be used as planning criteria.

Progress should be tracked by completed capabilities, passing tests, acceptance criteria, and working end-to-end behavior, not by estimated elapsed time.

Implementation planning should use feature gates and dependency ordering rather than date or duration estimates.

## Core Objectives

### Performance First

The application should be designed for high throughput, low unnecessary memory use, efficient peer management, efficient disk I/O, and predictable behavior under many active torrents and peers.

Performance goals include:

- Efficient async networking.
- Efficient disk reads and writes.
- Low overhead torrent state management.
- Avoiding unnecessary UI/backend chatter.
- Scalable peer and tracker handling.
- Bounded memory usage.
- Practical metrics for identifying bottlenecks.

### Full v1.0.0 Feature Set

The initial release must include complete torrent functionality expected from a serious Transmission-style daemon.

Required v1.0.0 capabilities include:

- Magnet links.
- `.torrent` files.
- Browser-friendly magnet submission.
- Watch-folder torrent import.
- HTTP trackers.
- HTTPS trackers.
- UDP trackers.
- HTTP/HTTPS webseeds.
- DHT.
- PEX.
- TCP peer connections.
- uTP/UDP peer connections where practical.
- Fast resume.
- Forced recheck.
- File selection.
- File prioritization.
- Queue management.
- Seeding controls.
- Ratio controls.
- Bandwidth limits.
- Per-torrent controls.
- Global settings.
- Complete API.
- Practical Web UI.
- VPN/NIC traffic containment.
- Fail-closed networking behavior.
- Health, logs, and metrics.

### API First

The API is a first-class product surface.

The Web UI must be implemented as a consumer of the same API available to external tools and automation.

Any feature available in the Web UI must also be available through the API unless there is a clear security or implementation reason not to expose it.

### Function Over Form

The Web UI must be complete, clear, and usable. It does not need to be visually fancy.

The UI should emphasize:

- Fast page load.
- Clear torrent state.
- Low resource use.
- Reliable controls.
- Useful diagnostics.
- Complete feature coverage.

The UI should avoid unnecessary complexity such as animations, large component frameworks, heavy theming systems, or design work that does not improve operational control.


## Legal, Licensing, and GitHub Repository Documentation

SwarmOtter is intended to be a lawful, general-purpose FOSS BitTorrent client. BitTorrent protocol support is a legitimate technical capability, but the project must be documented and presented carefully because some users misuse torrent software for copyright infringement.

This section defines repository and documentation requirements. It is not legal advice and does not replace review by qualified legal counsel where needed.

### Legal Positioning

SwarmOtter must be positioned as a tool for lawful downloading, sharing, and seeding of content that users have the right to access and distribute.

Appropriate documented use cases include:

- Linux distribution ISO downloads.
- Open-source project release distribution.
- Public-domain media.
- Open datasets.
- Internal company or homelab file distribution where the user has rights to the content.
- User-generated torrents for test fixtures and local swarm testing.

The project documentation must not position SwarmOtter as a piracy tool or as a way to obtain unauthorized copyrighted material.

### Lawful-Use Statement

The README and project documentation must include a clear lawful-use statement similar to the following:

```markdown
## Lawful Use

SwarmOtter is a general-purpose BitTorrent client intended for lawful downloading, sharing, and seeding of content that users have the right to access and distribute.

Examples of appropriate use include downloading and seeding Linux distributions, open-source project releases, public-domain media, open datasets, and other legally distributed files.

SwarmOtter does not include, endorse, host, index, or provide access to copyrighted material that is distributed without authorization. Users are responsible for ensuring that their use of SwarmOtter complies with applicable laws and the rights of content owners.
```

### Repository Documentation Requirements

The public GitHub repository must include the following documentation before `v1.0.0` release:

- `README.md` with project overview, lawful-use statement, feature list, and safe examples.
- `LICENSE` containing the selected FOSS license.
- `NOTICE.md` when required by the selected license or dependencies.
- `THIRD_PARTY_LICENSES.md` or a generated dependency license report.
- `SECURITY.md` explaining how to report security issues.
- `CONTRIBUTING.md` explaining contribution expectations and license terms.
- `CODE_OF_CONDUCT.md` if the project will accept public community contributions.
- `docs/lawful-use.md` explaining intended legal use cases and prohibited project use patterns.
- `docs/legal.md` summarizing project legal posture, disclaimers, and user responsibility.
- `docs/legal.md` stating that the project does not host or link to unauthorized copyrighted content.
- `docs/network-containment.md` documenting VPN/NIC containment as routing control, privacy-preserving network design, and fail-closed safety, not as piracy evasion.

### Prohibited Project Content

The repository must not include:

- Links to unauthorized copyrighted content.
- Magnet links for unauthorized copyrighted content.
- `.torrent` files for unauthorized copyrighted content.
- Default pirate indexers.
- Built-in torrent search providers aimed at infringing content.
- Default tracker lists associated with infringing content.
- Example screenshots showing copyrighted movies, shows, commercial games, music albums, ROM collections, or cracked software.
- Documentation that explains how to find pirated content.
- Documentation that frames VPN/NIC binding as a way to hide piracy or evade copyright enforcement.

### Safe Examples and Test Data

Examples, tests, screenshots, and sample data must use clearly lawful sources.

Acceptable examples include:

- Generated local test torrents.
- Public-domain files.
- Open datasets.
- Linux distribution torrent examples.
- Project-owned sample files created specifically for SwarmOtter testing.

When possible, automated tests should prefer generated local torrents and local swarm fixtures so the test suite does not depend on third-party content availability.

### VPN/NIC Documentation Wording

VPN/NIC containment is a valid SwarmOtter feature and a core differentiator. It must be documented as a network safety and routing-control feature.

Preferred wording:

- Strict network containment.
- Fail-closed torrent data plane.
- Predictable routing.
- Privacy-preserving network design.
- Homelab/container network isolation.
- Compliance with local network policy.

Avoid wording such as:

- Hide piracy from an ISP.
- Evade law enforcement.
- Avoid copyright enforcement.
- Download copyrighted content safely.
- Bypass takedowns or monitoring.

### Licensing Requirements

The project must choose a clear FOSS license before public `v1.0.0` release.

Recommended license options include:

- Apache-2.0.
- MIT.
- Dual MIT/Apache-2.0.

Before release, dependency licenses must be reviewed for compatibility with the selected license and expected distribution model.

### Branding and Trademark Notes

The SwarmOtter name, logo, mascot, and badge artwork should have a clear repository note describing whether the artwork is licensed under the same license as the code or reserved under separate brand/trademark rules.

If logo and brand assets are not intended to be freely reused in modified downstream products, this must be stated clearly in `docs/legal.md` or a dedicated branding policy.

### Content and Abuse Reporting

The repository must provide a clear way for users, contributors, or rights holders to report concerns about project content, documentation, examples, or misuse of official project infrastructure.

The project should make clear that it does not control third-party use of the software, but it does control official SwarmOtter repositories, documentation, examples, issue templates, discussions, release artifacts, and project-hosted assets.

## Non-Negotiable v1.0.0 Release Blockers

SwarmOtter is not ready for `v1.0.0` until all of the following are complete:

- Full magnet link support.
- `.torrent` file support.
- DHT support.
- PEX support.
- UDP tracker support.
- HTTP/HTTPS tracker support.
- HTTP/HTTPS webseed support.
- Browser-friendly magnet submission.
- Watch-folder import.
- Fast resume.
- Forced recheck.
- File selection and prioritization.
- Queue management.
- Bandwidth limits.
- Ratio and seeding controls.
- Strict torrent traffic containment through a configured network path.
- Fail-closed behavior when the VPN/NIC path is unavailable.
- Complete REST API for all user-facing functionality.
- WebSocket or SSE event updates.
- Practical Web UI for all core operations.
- Configuration file support.
- Container-friendly deployment support.
- Health/status endpoints.
- Logs and operational diagnostics.
- Automated tests for core engine, API, storage, and network containment behavior.
- FOSS license selected and included in the repository.
- Lawful-use statement included in the README and documentation.
- Legal/project documentation completed for GitHub publication.
- Dependency license review completed.
- No unauthorized copyrighted sample content, torrent files, magnet links, pirate indexers, or infringing examples are included.

## System Architecture

### Core Engine

The core engine handles:

- Torrent metadata parsing.
- Magnet parsing.
- Info hash handling.
- Peer discovery.
- Tracker communication.
- Webseed downloads.
- DHT.
- PEX.
- Peer wire protocol.
- Piece selection.
- Piece verification.
- Download scheduling.
- Upload/seeding behavior.
- Torrent lifecycle state.

### Network Layer

All torrent-related network activity must pass through a dedicated network layer.

No engine component should directly create outbound sockets or HTTP clients without going through the network binding and containment layer.

The network layer handles:

- Interface binding.
- Source address binding.
- Optional device binding on supported platforms.
- Route validation.
- VPN/NIC health checks.
- TCP sockets.
- UDP sockets.
- DHT UDP sockets.
- UDP tracker sockets.
- uTP sockets.
- Tracker HTTP/HTTPS clients.
- Webseed HTTP/HTTPS clients.
- DNS behavior used by torrent traffic.

### Storage Layer

The storage layer handles:

- File layout.
- Partial files.
- Sparse files where supported.
- Piece writes.
- Piece reads.
- Hash verification.
- Fast resume metadata.
- Forced recheck.
- File moves.
- Incomplete and complete directories.
- Atomic or safe move behavior where possible.

### API Layer

The API layer exposes all daemon functionality through REST endpoints and event streams.

The API layer handles:

- Torrent management.
- Settings management.
- File controls.
- Tracker controls.
- Peer information.
- Watch-folder status.
- Network/VPN health status.
- Global statistics.
- Per-torrent statistics.
- Logs/events.

### Web UI

The Web UI is a lightweight operational dashboard.

The Web UI should not contain torrent logic. It should call the API for all behavior.

### Configuration Layer

The configuration layer handles:

- Config file loading.
- Environment variable overrides.
- Validation.
- Safe defaults.
- Startup failure when required settings are invalid.
- Runtime settings updates where safe.

## Network Containment and VPN/NIC Binding

### Requirement

SwarmOtter must support strict torrent traffic containment.

All torrent-related traffic must be forced through a configured network path, such as a VPN interface, source IP address, network namespace, container network stack, or explicitly configured NIC.

This is a core v1.0.0 requirement.

### Traffic Covered

Network containment applies to all torrent-related traffic, including:

- Peer TCP connections.
- Peer uTP/UDP traffic.
- DHT UDP traffic.
- PEX-discovered peer connections.
- UDP tracker announces.
- HTTP tracker announces.
- HTTPS tracker announces.
- Webseed HTTP/HTTPS traffic.
- Magnet metadata fetching.
- DNS resolution used for torrent, tracker, peer, and webseed operations.

### Control Plane vs Data Plane

The control API and Web UI are separate from torrent data traffic.

The API/Web UI may bind to localhost, a LAN address, or a reverse proxy listener.

Torrent data traffic must bind separately to the configured VPN/NIC path.

Exposing the Web UI or API on LAN must not allow torrent peer, tracker, DHT, or webseed traffic to use the LAN/default network path.

### Fail-Closed Behavior

SwarmOtter must fail closed.

If strict network containment is enabled and the configured network path is unavailable, torrent networking must stop.

The application must never silently fall back to the default route.

Fail-closed conditions include:

- Required interface does not exist.
- Required interface exists but is down.
- Required interface has no usable IP address.
- Required source IP is no longer assigned.
- Required route is missing or invalid.
- VPN network namespace is unavailable.
- DNS behavior cannot be constrained as configured.
- Socket binding fails.

When a fail-closed condition occurs:

- Existing torrent network sockets must be closed.
- New torrent network connections must be blocked.
- Torrents should enter a clear network-blocked state.
- The API must report the network containment failure.
- The Web UI must show the network containment failure.
- Logs must clearly identify the failed requirement.

### Network Health States

The daemon must expose network health states through the API and Web UI.

Required states include:

- `healthy`
- `disabled`
- `interface_missing`
- `interface_down`
- `no_interface_address`
- `source_address_missing`
- `route_invalid`
- `socket_bind_failed`
- `dns_not_constrained`
- `network_namespace_unavailable`
- `blocked_fail_closed`

### Configuration Example

```toml
[project]
name = "SwarmOtter"

[network]
mode = "strict"
required_interface = "tun0"
required_source_ipv4 = "10.8.0.2"
allow_ipv6 = false
fail_closed = true
validate_route = true
validate_dns = true

[api]
bind_address = "0.0.0.0:9091"
require_auth = true
auth_token = "replace-with-a-long-random-token"

[torrent]
listen_port = 51413
```

### Acceptance Criteria

- The daemon refuses to start torrent networking when strict mode is enabled and the required interface is missing.
- The daemon blocks torrent traffic when the configured VPN/NIC path disappears while running.
- Peer traffic cannot fall back to the default route.
- Tracker traffic cannot fall back to the default route.
- DHT traffic cannot fall back to the default route.
- Webseed traffic cannot fall back to the default route.
- DNS behavior is either constrained or explicitly disabled for unsafe operations.
- API/Web UI traffic remains independently configurable.

## Torrent Input Requirements

### Magnet Links

SwarmOtter must support magnet links as a required v1.0.0 feature.

Required behavior:

- Accept magnet links through the API.
- Accept magnet links through the Web UI.
- Accept browser-submitted magnet links.
- Parse info hash.
- Parse display name when present.
- Parse tracker list when present.
- Fetch metadata.
- Display metadata-fetching state.
- Support trackerless magnets through DHT.
- Detect duplicate magnets/torrents by info hash.
- Handle malformed magnets with useful errors.

### Torrent Files

SwarmOtter must support `.torrent` files as a required v1.0.0 feature.

Required behavior:

- Upload `.torrent` files through the API.
- Upload `.torrent` files through the Web UI.
- Import `.torrent` files from watch folders.
- Parse single-file and multi-file torrents.
- Validate torrent metadata.
- Detect duplicate torrents by info hash.
- Support private torrents according to torrent metadata.
- Support tracker tiers.
- Preserve source metadata where useful.

### Browser Magnet Handling

SwarmOtter must support browser-friendly magnet submission.

Required behavior:

- Provide a Web UI paste/add flow for magnet links.
- Provide an API endpoint suitable for browser extensions, scripts, or bookmarklets.
- Document how a browser can send magnet links to a remote daemon.
- Support optional desktop `magnet:` protocol registration where practical.
- Support headless/server use where the browser and daemon are not running on the same machine.

### Watch Folders

SwarmOtter must support one or more watch folders.

Required behavior:

- Watch configured folders for new `.torrent` files.
- Wait for file writes to stabilize before import.
- Support duplicate detection.
- Support optional recursive watching.
- Support per-watch-folder download location defaults.
- Support per-watch-folder labels/categories.
- Support per-watch-folder paused/start behavior.
- Move successfully imported torrent files to an archive folder when configured.
- Move failed imports to a failure folder when configured.
- Leave imported torrent files in place when configured.
- Delete imported torrent files after successful import when configured.
- Expose watch-folder status through the API and Web UI.
- Log import success and failure details.

## Peer Discovery and Alternate Data Source Requirements

SwarmOtter must support all major peer discovery mechanisms and webseed data
sources required for a complete v1.0.0 release.

Required mechanisms:

- HTTP trackers.
- HTTPS trackers.
- UDP trackers.
- HTTP/HTTPS webseeds.
- DHT.
- PEX.
- Tracker-provided peers.
- Magnet-provided trackers.
- Manually edited tracker lists.

### DHT

DHT support is required for v1.0.0.

Required behavior:

- Bootstrap from configured DHT bootstrap nodes.
- Discover peers for trackerless magnets.
- Participate in DHT lookups.
- Respect private torrent restrictions.
- Expose DHT status through API/UI.
- Include DHT network traffic in VPN/NIC containment.

### PEX

PEX support is required for v1.0.0.

Required behavior:

- Exchange peers with connected peers where supported.
- Add PEX-discovered peers to the peer candidate pool.
- Respect private torrent restrictions.
- Include PEX-discovered outbound connections in VPN/NIC containment.
- Expose PEX-derived peer information where useful.

### Trackers

Tracker support is required for v1.0.0.

Required behavior:

- HTTP tracker announce.
- HTTPS tracker announce.
- UDP tracker announce.
- Tracker scrape where supported.
- Tracker tiers.
- Manual reannounce.
- Tracker error reporting.
- Tracker edit/add/remove through API/UI.
- Respect announce intervals.
- Include all tracker traffic in VPN/NIC containment.

### Webseeds

HTTP/HTTPS webseed support is required for v1.0.0.

Required behavior:

- Parse BEP 19 `url-list` webseed metadata.
- Download payload bytes with HTTP byte-range requests.
- Verify pieces before writing them to storage.
- Include all webseed traffic and DNS resolution in VPN/NIC containment.
- Account webseed bytes in download statistics and bandwidth controls.

## Peer Protocol Requirements

SwarmOtter must support:

- TCP peer connections.
- uTP/UDP peer connections where practical.
- Peer handshake.
- Metadata exchange for magnets.
- Piece availability tracking.
- Piece request scheduling.
- Upload and download accounting.
- Choking and unchoking.
- Interested and not-interested state.
- Endgame behavior.
- Bad peer detection.
- Temporary peer suppression or banning.
- IPv4 controls.
- IPv6 controls.
- Ability to disable IPv6 to reduce VPN leak risk.

## Storage Requirements

The storage system must be safe, reliable, and efficient.

Required behavior:

- Incomplete download directory.
- Completed download directory.
- Per-torrent download directory.
- Multi-file torrent support.
- Single-file torrent support.
- File selection.
- File prioritization.
- Partial download support.
- Fast resume.
- Forced recheck.
- Piece verification.
- Safe handling of interrupted writes.
- Configurable preallocation behavior.
- Sparse file support where available.
- Move data after torrent add.
- Move data after completion.
- Rename file/path where supported.
- Detect missing files.
- Detect changed files.
- Clear error state when storage paths are invalid.

## Torrent Lifecycle Requirements

Required torrent actions:

- Add magnet.
- Add torrent file.
- Add from watch folder.
- Pause.
- Resume.
- Start now.
- Stop.
- Remove torrent.
- Remove torrent and delete data.
- Recheck.
- Reannounce.
- Move data.
- Rename path.
- Change labels/categories.
- Change queue position.
- Change file priorities.
- Change wanted/unwanted files.
- Change per-torrent limits.

Required torrent states:

- `queued`
- `checking`
- `downloading_metadata`
- `downloading`
- `seeding`
- `paused`
- `completed`
- `error`
- `network_blocked`
- `storage_error`
- `tracker_error`

## Queue Management Requirements

The application must support queue management as part of v1.0.0.

Required behavior:

- Global active download limit.
- Global active seed limit.
- Queue order.
- Move up.
- Move down.
- Move to top.
- Move to bottom.
- Start now / bypass queue.
- Auto-start behavior by setting.
- Per-torrent paused state.
- Queue state exposed through API/UI.

## Seeding and Ratio Requirements

The application must support seeding controls as part of v1.0.0.

Required behavior:

- Global ratio limit.
- Per-torrent ratio limit.
- Global idle seed limit.
- Per-torrent idle seed limit.
- Seed forever option.
- Stop seeding when ratio target is reached.
- Stop seeding when idle target is reached.
- Uploaded byte count.
- Downloaded byte count.
- Ratio calculation.
- Seeding status through API/UI.

## Bandwidth Requirements

The application must support bandwidth controls as part of v1.0.0.

Required behavior:

- Global download speed limit.
- Global upload speed limit.
- Per-torrent download speed limit.
- Per-torrent upload speed limit.
- Optional alternate speed mode.
- Optional scheduled speed mode without relying on calendar estimates in this implementation plan.
- Maximum peers globally.
- Maximum peers per torrent.
- Rate limit state through API/UI.

## API Requirements

The API must be complete enough for the Web UI and external automation.

### API Principles

- JSON request/response by default.
- Consistent error format.
- Stable object identifiers.
- API versioning.
- Complete coverage of user-facing features.
- Suitable for scripts and browser integrations.
- Web UI must use the same API.

### Required Endpoint Areas

The exact route names may be adjusted during implementation, but the API must cover these areas.

#### Torrent Management

- List torrents.
- Get torrent details.
- Add magnet.
- Upload torrent file.
- Remove torrent.
- Remove torrent and delete data.
- Pause torrent.
- Resume torrent.
- Start torrent now.
- Recheck torrent.
- Reannounce torrent.
- Move torrent data.
- Rename torrent path.
- Update torrent labels/categories.

#### Files

- List torrent files.
- Set wanted/unwanted files.
- Set file priority.
- Rename file/path where supported.

#### Trackers

- List trackers.
- Add tracker.
- Remove tracker.
- Edit tracker.
- Reannounce tracker.
- Show tracker status.

#### Peers

- List peers.
- Show peer client where available.
- Show peer progress.
- Show peer address.
- Show peer transfer rates.
- Disconnect peer where supported.
- Suppress or ban peer where supported.

#### Queue

- Show queue state.
- Move torrent up.
- Move torrent down.
- Move torrent to top.
- Move torrent to bottom.

#### Settings

- Get global settings.
- Update safe runtime settings.
- Validate settings.
- Report settings requiring restart.

#### Network

- Show network containment mode.
- Show configured torrent interface/source/network namespace.
- Show current network health.
- Show fail-closed state.
- Show DHT network state.
- Show tracker network state.

#### Watch Folders

- List watch folders.
- Show watch-folder status.
- Trigger scan.
- Show import success/failure history.

#### Stats and Health

- Global stats.
- Per-torrent stats.
- Daemon health.
- Storage health.
- Network health.
- API health.
- Version/build information.

### Response Format

All API responses should use a consistent structure.

```json
{
  "success": true,
  "data": {},
  "error": null
}
```

Errors should include machine-readable codes and human-readable messages.

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "network_interface_missing",
    "message": "Required torrent network interface tun0 is not available. Torrent networking is blocked."
  }
}
```

## WebSocket/SSE Event Requirements

The daemon must provide real-time updates through WebSocket or SSE.

Required event types:

- `torrent_added`
- `torrent_changed`
- `torrent_removed`
- `torrent_error`
- `torrent_metadata_received`
- `torrent_completed`
- `torrent_files_changed`
- `torrent_trackers_changed`
- `torrent_peers_changed`
- `stats_updated`
- `network_status_changed`
- `watch_folder_imported`
- `watch_folder_failed`
- `settings_changed`
- `daemon_health_changed`

Clients must be able to subscribe to all torrents or specific torrents.

## Web UI Requirements

The Web UI should be simple, fast, and operationally complete.

Required views:

- Torrent list.
- Add torrent/magnet dialog.
- Torrent details.
- Files tab.
- Peers tab.
- Trackers tab.
- Activity/statistics view.
- Settings view.
- Network/VPN health view.
- Watch-folder status view.
- Logs/errors view.

Required actions:

- Add magnet.
- Upload torrent file.
- Pause/resume/start/stop.
- Remove torrent.
- Remove torrent and delete data.
- Recheck.
- Reannounce.
- Move data.
- Rename path.
- Change file priority.
- Change wanted/unwanted files.
- Edit trackers.
- Change queue order.
- Change limits.
- View fail-closed network status.

UI non-goals:

- No elaborate visual design requirement.
- No animation requirement.
- No complex theme system requirement.
- No requirement for a heavy frontend framework.

## Configuration Requirements

The application must support configuration through a file and environment variable overrides.

Required configuration areas:

- API bind address.
- API authentication.
- Download directories.
- Incomplete directory.
- Completed directory.
- Watch folders.
- Torrent listen port.
- DHT enablement and settings.
- PEX enablement and settings.
- Tracker settings.
- Peer limits.
- Bandwidth limits.
- Queue limits.
- Ratio/seeding limits.
- VPN/NIC/network containment settings.
- IPv4/IPv6 behavior.
- Logging.
- Metrics.

Invalid required configuration must produce clear startup errors.

## Rust Crate and Module Architecture

Suggested module layout:

```text
swarmotter/
├── Cargo.toml
├── README.md
├── LICENSE
├── NOTICE.md
├── SECURITY.md
├── CONTRIBUTING.md
├── THIRD_PARTY_LICENSES.md
├── docs/
│   ├── configuration.md
│   ├── api.md
│   ├── vpn-network-containment.md
│   ├── lawful-use.md
│   ├── legal.md
│   ├── content-policy.md
│   └── deployment.md
└── src/
    ├── main.rs
    ├── lib.rs
    ├── config/
    │   ├── mod.rs
    │   └── validation.rs
    ├── daemon/
    │   ├── mod.rs
    │   ├── state.rs
    │   └── lifecycle.rs
    ├── engine/
    │   ├── mod.rs
    │   ├── torrent.rs
    │   ├── session.rs
    │   ├── peer_manager.rs
    │   ├── piece_manager.rs
    │   ├── tracker_manager.rs
    │   ├── dht.rs
    │   ├── pex.rs
    │   └── queue.rs
    ├── net/
    │   ├── mod.rs
    │   ├── binder.rs
    │   ├── interface.rs
    │   ├── route.rs
    │   ├── tcp.rs
    │   ├── udp.rs
    │   ├── dns.rs
    │   └── health.rs
    ├── storage/
    │   ├── mod.rs
    │   ├── files.rs
    │   ├── resume.rs
    │   ├── recheck.rs
    │   └── layout.rs
    ├── api/
    │   ├── mod.rs
    │   ├── routes.rs
    │   ├── handlers.rs
    │   ├── ws.rs
    │   └── errors.rs
    ├── watch/
    │   ├── mod.rs
    │   └── folders.rs
    ├── models/
    │   ├── mod.rs
    │   ├── torrent.rs
    │   ├── status.rs
    │   ├── peer.rs
    │   ├── file.rs
    │   ├── tracker.rs
    │   ├── stats.rs
    │   └── network.rs
    └── web/
        ├── mod.rs
        └── static_assets.rs
```

## Data Model Requirements

The data model must support the full v1.0.0 feature set.

Required entities include:

- Torrent.
- Torrent status.
- Torrent file.
- File priority.
- Peer.
- Tracker.
- Tracker tier.
- DHT status.
- PEX status.
- Global stats.
- Per-torrent stats.
- Network containment status.
- Watch folder.
- Watch-folder import result.
- Queue state.
- Bandwidth settings.
- Seeding settings.
- Storage state.
- Error state.

Example torrent status model:

```rust
pub enum TorrentStatus {
    Queued,
    Checking,
    DownloadingMetadata,
    Downloading,
    Seeding,
    Paused,
    Completed,
    NetworkBlocked,
    StorageError,
    TrackerError,
    Error,
}
```

Example network status model:

```rust
pub enum NetworkContainmentStatus {
    Healthy,
    Disabled,
    InterfaceMissing,
    InterfaceDown,
    NoInterfaceAddress,
    SourceAddressMissing,
    RouteInvalid,
    SocketBindFailed,
    DnsNotConstrained,
    NetworkNamespaceUnavailable,
    BlockedFailClosed,
}
```

## Testing Requirements

Testing must be based on feature completion and acceptance criteria, not time estimates.

Required test areas:

### Unit Tests

- Magnet parsing.
- Torrent parsing.
- Info hash handling.
- Tracker tier handling.
- Piece selection.
- Piece verification.
- Queue behavior.
- Ratio/seeding behavior.
- Bandwidth limit logic.
- Config validation.
- Network containment validation logic.

### Integration Tests

- Add magnet through API.
- Add torrent file through API.
- Upload torrent file through Web UI/API path.
- Import torrent from watch folder.
- Pause/resume/remove lifecycle.
- Recheck lifecycle.
- Tracker announce behavior.
- DHT peer discovery behavior.
- PEX peer discovery behavior.
- File priority behavior.
- Queue behavior.
- Settings behavior.
- WebSocket/SSE event delivery.

### Network Containment Tests

- Required interface missing.
- Required interface down.
- Source IP missing.
- Route invalid.
- Socket bind failure.
- VPN path removed while torrents are active.
- Torrent traffic blocked when fail-closed is active.
- API listener remains available when torrent data plane is blocked, if configured that way.

### Storage Tests

- Fast resume.
- Forced recheck.
- Interrupted write recovery.
- Missing file detection.
- Partial download behavior.
- File selection behavior.
- Move complete behavior.
- Rename path behavior.

### Local Swarm Tests

- Multiple local peers.
- Magnet metadata fetch.
- Tracker-based peer discovery.
- DHT-based peer discovery.
- PEX-based peer discovery.
- Download completion.
- Seeding behavior.
- Recheck after completion.

## Performance Requirements

Performance work must be measurable.

Required performance areas:

### Async Networking

- Use an async runtime such as Tokio.
- Avoid blocking network operations on async worker threads.
- Use bounded queues where appropriate.
- Avoid unbounded peer growth.
- Keep socket creation centralized through the network layer.

### Disk I/O

- Use efficient buffered reads and writes.
- Avoid excessive small writes.
- Keep piece verification efficient.
- Support concurrent verification where safe.
- Avoid unnecessary full rechecks when fast resume is valid.

### Memory Usage

- Bound in-memory queues.
- Avoid keeping unnecessary piece data in memory.
- Avoid loading full torrent data into memory when streaming is sufficient.
- Keep long-running daemon memory behavior observable.

### Observability

Required observability:

- Structured logs.
- Health endpoint.
- Global stats.
- Per-torrent stats.
- Network containment state.
- DHT state.
- Tracker state.
- Watch-folder state.
- Optional Prometheus metrics endpoint.

## Deployment Requirements

The application must support:

- Linux daemon deployment.
- Systemd service deployment.
- Linux `x86_64` and `aarch64` release tarballs.
- Linux `.deb` and `.rpm` packages for supported release architectures.
- Container deployment.
- Podman deployment.
- Docker-compatible deployment where practical.
- VPN container/network namespace deployment.
- Reverse proxy deployment for the Web UI/API.
- Configuration through files and environment variables.
- Persistent storage volumes.
- Clear logs.

Deployment documentation must include:

- Basic Linux daemon setup.
- Container setup.
- VPN/NIC containment setup.
- Fail-closed behavior explanation.
- API/Web UI exposure guidance.
- Example config file.
- Example systemd service.
- Lawful-use documentation.
- License and dependency-license documentation.
- Content policy documentation.
- Brand/logo usage documentation.

## Development Workstreams

The following workstreams describe dependency order and feature grouping. They are not time estimates.

### Workstream: Foundation

- Project structure.
- Config loading and validation.
- Daemon lifecycle.
- Logging.
- Error model.
- API skeleton.
- Persistent state foundation.

### Workstream: Network Containment

- Network configuration model.
- Interface discovery.
- Source address validation.
- Route validation.
- Socket binding abstraction.
- Fail-closed enforcement.
- Network health API.
- Network health Web UI.
- Network containment tests.

### Workstream: Torrent Metadata

- Torrent file parser.
- Magnet parser.
- Info hash model.
- Metadata fetch state.
- Duplicate detection.

### Workstream: Peer Discovery

- HTTP trackers.
- HTTPS trackers.
- UDP trackers.
- DHT.
- PEX.
- Tracker status model.

### Workstream: Peer Protocol

- Peer connections.
- Handshake.
- Piece availability.
- Piece requests.
- Upload/download accounting.
- Endgame behavior.
- Bad peer handling.

### Workstream: Storage

- File layout.
- Piece writes.
- Piece reads.
- Piece verification.
- Partial files.
- Fast resume.
- Forced recheck.
- File selection.
- File priority.
- Move/rename behavior.

### Workstream: Torrent Lifecycle

- Add.
- Pause.
- Resume.
- Stop.
- Remove.
- Remove and delete data.
- Recheck.
- Reannounce.
- Move data.
- Rename path.

### Workstream: Queue, Seeding, and Bandwidth

- Queue order.
- Active limits.
- Ratio limits.
- Idle seed limits.
- Bandwidth limits.
- Per-torrent limits.

### Workstream: Watch Folders and Browser Integration

- Watch-folder scanner.
- Stable file detection.
- Import success/failure handling.
- Browser-friendly magnet API.
- Optional desktop protocol registration documentation.

### Workstream: Web UI

- Torrent list.
- Add dialog.
- Details view.
- Files tab.
- Peers tab.
- Trackers tab.
- Settings.
- Network health.
- Watch-folder status.
- Logs/errors.


### Workstream: Legal and Repository Documentation

- Select FOSS license.
- Add `LICENSE`.
- Add lawful-use statement to `README.md`.
- Add `docs/lawful-use.md`.
- Add `docs/legal.md`.
- Add legal/content-policy guidance to `docs/legal.md`.
- Add dependency license report.
- Add security reporting instructions.
- Add contribution guidelines.
- Confirm no infringing examples, links, magnets, torrent files, screenshots, or default indexers are included.
- Confirm VPN/NIC documentation is framed around routing, privacy, network containment, and fail-closed safety.

### Workstream: Hardening and Release Completion

- End-to-end testing.
- Local swarm testing.
- Network containment validation.
- Performance profiling.
- Documentation completion.
- Configuration examples.
- Deployment examples.
- Version/build metadata.

## v1.0.0 Completion Checklist

The project is ready for `v1.0.0` only when every item below is complete:

- All required torrent input methods work.
- Magnet metadata fetch works.
- DHT works.
- PEX works.
- HTTP/HTTPS trackers work.
- UDP trackers work.
- Peer protocol download and upload work.
- Fast resume works.
- Forced recheck works.
- Watch folders work.
- Browser magnet submission works.
- File selection works.
- File priorities work.
- Queue management works.
- Ratio/seeding limits work.
- Bandwidth limits work.
- VPN/NIC containment works.
- Fail-closed behavior works.
- API exposes all required functionality.
- Web UI exposes all required operational controls.
- WebSocket/SSE updates work.
- Logs are useful.
- Health endpoints are useful.
- Metrics are available where configured.
- Configuration is documented.
- Deployment is documented.
- Automated tests pass.
- Local swarm tests pass.
- Network containment tests pass.
- Project is consistently named SwarmOtter across repository, docs, config examples, service files, and release artifacts.
- FOSS license is selected and included.
- README contains lawful-use statement.
- Legal documentation is complete.
- Content policy documentation is complete.
- Dependency license review is complete.
- No unauthorized copyrighted sample content, magnets, torrent files, screenshots, or default pirate indexers are included.
- VPN/NIC documentation is framed around routing control, privacy-preserving network design, network containment, and fail-closed safety.
