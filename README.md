<p align="center">
  <img src="docs/assets/swarmotter-logo.png" alt="SwarmOtter logo" width="220">
</p>

<h1 align="center">SwarmOtter</h1>

<p align="center">
  <a href="https://github.com/sphildreth/swarmotter/actions/workflows/ci.yml">
    <img src="https://github.com/sphildreth/swarmotter/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
</p>

<p align="center">
  <em>A fast little Rust BitTorrent daemon that keeps your swarm safely in its tunnel.</em>
</p>

<p align="center">
  SwarmOtter is a performance-first Rust BitTorrent daemon with a practical web
  UI, complete API, and fail-closed VPN/NIC traffic containment.
</p>

---

## What SwarmOtter Is

- A **Rust BitTorrent daemon** built for Linux/server and homelab deployments.
- **API-first** — the daemon and its API are the primary product surfaces.
- **Web UI included** — practical, function-over-form, consuming the same API
  exposed to external automation.
- **Performance-first** — efficient async networking, disk I/O, and bounded
  memory under many active torrents and peers.
- **Operationally correct** — predictable behavior, safe recovery, and clear
  diagnostics.
- **Containment-native** — VPN/NIC fail-closed traffic containment is a core
  requirement, not a deployment afterthought.

## What SwarmOtter Is Not

SwarmOtter is **not** a torrent indexer, search engine, piracy assistant, or
content-discovery tool.

It does not include bundled torrent indexes, infringing magnet links,
copyrighted media examples, or documentation encouraging copyright
infringement.

## Features

- Performance-first Rust daemon with a live BitTorrent data plane.
- Native REST API with WebSocket and Server-Sent Events.
- Optional Transmission RPC compatibility endpoint at `/transmission/rpc` for
  Transmission-style tools and scripts.
- Practical Web UI that uses the same API exposed to external automation.
- UI operations-console updates for large libraries, with
  a sortable/filterable Tabulator table, theme toggle, and efficient large-list
  workflows.
- Magnet links and `.torrent` file intake.
- TCP and uTP peer wire protocol support.
- DHT, PEX, HTTP/HTTPS trackers, UDP trackers, and webseeds.
- BEP 9 magnet metadata fetching.
- Fast resume and forced recheck.
- Watch-folder import.
- File selection, file prioritization, and path rename controls.
- Queue, bandwidth, ratio, and seeding controls.
- Adaptive swarm performance autopilot with per-torrent diagnostics and
  override controls.
- Settings two-panel layout for dense configuration in the Web UI.
- Strict VPN/NIC traffic containment with fail-closed behavior.
- Container and homelab-friendly deployment.
- Lawful-use project posture.

## Network Containment

SwarmOtter treats network containment as a product requirement, not a
deployment afterthought.

All torrent-related traffic must be constrained through the configured network
path (VPN interface, source IP, network namespace, or explicitly configured
NIC), including:

- Peer TCP
- Peer UDP / uTP
- DHT UDP
- PEX-discovered peers
- UDP trackers
- HTTP / HTTPS trackers
- Webseeds
- Magnet metadata fetching
- DNS used by torrent operations

The daemon **fails closed** and never silently falls back to the default route
if the configured path is unavailable. The Web UI/API control plane is separate
from the torrent data plane.

See [`docs/network-containment.md`](docs/network-containment.md).

## Lawful Use

SwarmOtter is a general-purpose BitTorrent client intended for lawful
downloading, sharing, and seeding of content that users have the right to
access and distribute.

Examples include Linux distributions, open-source project releases,
public-domain media, open datasets, user-owned files, and
organization-approved distribution workflows.

Users are responsible for ensuring their use complies with applicable laws and
the rights of content owners. This is project policy and documentation, not
legal advice.

See:

- [`docs/lawful-use.md`](docs/lawful-use.md)
- [`docs/legal.md`](docs/legal.md)

## Developer Onboarding

### Prerequisites

- Rust stable (see `rust-version` in `Cargo.toml`)
- Cargo
- Git
- Linux is recommended for network-containment development and testing

### First-Time Setup

```bash
git clone https://github.com/sphildreth/swarmotter.git
cd swarmotter
cargo fmt
cargo check
cargo test
```

### Workspace Layout

SwarmOtter is a Cargo workspace with four crates:

| Crate | Role |
| --- | --- |
| `crates/swarmotterd` | Daemon binary |
| `crates/swarmotter-core` | Core types and live torrent engine logic |
| `crates/swarmotter-api` | API layer |
| `crates/swarmotter-web` | Web UI / static asset support |

## Repository Layout

```text
swarmotter/
├── AGENTS.md                  # Coding-agent governance rules
├── README.md
├── LICENSE                    # Apache-2.0
├── CONTRIBUTING.md
├── SECURITY.md
├── CODE_OF_CONDUCT.md
├── THIRD_PARTY_LICENSES.md
├── CHANGELOG.md
├── Cargo.toml                 # Workspace root
├── crates/
│   ├── swarmotterd/           # Daemon binary
│   ├── swarmotter-core/       # Core types and engine logic
│   ├── swarmotter-api/        # API layer
│   └── swarmotter-web/        # Embedded/static web support
├── docs/                      # User guide and operator documentation
├── design/                    # Requirements, architecture, policy, ADRs
│   ├── requirements.md
│   ├── architecture.md
│   ├── api.md
│   ├── configuration.md
│   ├── vpn-network-containment.md
│   ├── deployment.md
│   ├── testing.md
│   ├── lawful-use.md
│   ├── content-policy.md
│   ├── legal.md
│   └── adr/                   # Architecture decision records
├── assets/                    # Logo and brand graphics
└── .github/                   # Issue and PR templates
```

## ADRs and Decision Records

Important technical, legal, operational, and dependency decisions are recorded
as Architecture Decision Records (ADRs) in
[`design/adr/`](design/adr/).

New architecture, legal, dependency, or network-containment decisions require
ADRs. When in doubt, create one. See
[`design/adr/README.md`](design/adr/README.md) for the format and lifecycle.

## Simple Homelab Deployment

A typical homelab deployment:

1. Run a VPN container or VPN-enabled network namespace.
2. Run `swarmotterd` inside that network path.
3. Mount persistent config and download directories.
4. Expose only the Web UI/API port to the LAN.
5. Keep torrent peer / tracker / DHT traffic constrained to the VPN path.

The full runbook lives in [`docs/deployment.md`](docs/deployment.md). Common
configuration patterns live in [`docs/configuration.md`](docs/configuration.md).

## Documentation

Published user guide:

- <https://sphildreth.github.io/swarmotter/>

User-facing documentation:

- [User guide](docs/index.md)
- [Configuration](docs/configuration.md)
- [API reference](docs/api.md)
- [Network containment](docs/network-containment.md)
- [Deployment](docs/deployment.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Lawful use](docs/lawful-use.md)
- [Legal and content policy](docs/legal.md)

Project design documentation:

- [Requirements](design/requirements.md)
- [Architecture](design/architecture.md)
- [API design](design/api.md)
- [Configuration design](design/configuration.md)
- [Network containment design](design/vpn-network-containment.md)
- [Testing design](design/testing.md)
- [ADRs](design/adr/README.md)

## Contributing

Contributions are welcome. To contribute:

- Read [`AGENTS.md`](AGENTS.md) for coding-agent and contributor governance.
- Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for workflow and conventions.
- Create or update an ADR in [`design/adr/`](design/adr/) for decisions with
  lasting architectural, legal, dependency, or containment impact.
- Do **not** submit piracy-oriented features, indexers, infringing magnets, or
  copyrighted-content examples; see [`docs/legal.md`](docs/legal.md).
- Run `cargo fmt`, `cargo check`, and `cargo test` before considering work
  done.

## License

SwarmOtter is licensed under the Apache License, Version 2.0. See
[`LICENSE`](LICENSE). Dependency licenses are tracked in
[`THIRD_PARTY_LICENSES.md`](THIRD_PARTY_LICENSES.md).
