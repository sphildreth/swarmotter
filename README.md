# SwarmOtter

SwarmOtter is a performance-first Rust BitTorrent daemon with a practical Web
UI, complete API, and fail-closed VPN/NIC traffic containment.

## Status

This repository is in the **early setup phase**. The BitTorrent engine is not
implemented yet. There is no usable release yet. The first product release will
be `v1.0.0`.

## Lawful Use

SwarmOtter is a general-purpose BitTorrent client intended for lawful
downloading, sharing, and seeding of content that users have the right to
access and distribute.

Examples of appropriate use include downloading and seeding Linux
distributions, open-source project releases, public-domain media, open
datasets, and other legally distributed files.

SwarmOtter does not include, endorse, host, index, or provide access to
copyrighted material that is distributed without authorization. Users are
responsible for ensuring that their use of SwarmOtter complies with applicable
laws and the rights of content owners.

## Core Goals

- **Performance first.** Efficient async networking, disk I/O, bounded memory,
  and predictable behavior under many active torrents and peers.
- **Complete v1.0.0 feature set.** Magnets, `.torrent` files, watch folders,
  HTTP/HTTPS/UDP trackers, DHT, PEX, TCP/uTP peers, fast resume, recheck, file
  selection and prioritization, queueing, seeding/ratio controls, bandwidth
  limits, and operational diagnostics.
- **API first.** The daemon and API are the primary product surfaces. The Web
  UI consumes the same API exposed to external automation.
- **Strict network containment.** All torrent-related traffic is constrained
  to a configured network path and fails closed if that path is unavailable.
- **Function over form.** The Web UI is complete and usable, but visual polish,
  animations, and heavy UI frameworks are non-goals unless they materially
  improve operations.

## Release Model: v1.0.0 Only, No MVP

SwarmOtter does **not** use an MVP release model. The initial product release is
`v1.0.0`, reached only when every required feature in
`design/requirements.md` is implemented, tested, documented, and usable.

DHT, PEX, UDP trackers, watch folders, browser magnet handling, file
prioritization, queueing, bandwidth controls, fast resume, VPN/NIC containment,
and legal documentation are all part of the `v1.0.0` scope. They are not
optional future enhancements.

Progress is tracked by completed capabilities, passing tests, acceptance
criteria, working end-to-end behavior, documented decisions, and release
checklist completion — not by time or duration estimates.

## Network Containment

All torrent-related traffic — peer TCP, peer UDP/uTP, DHT UDP, PEX-discovered
peers, UDP trackers, HTTP/HTTPS trackers, webseeds, magnet metadata fetching,
and torrent-related DNS — must flow through the configured network path (VPN
interface, source IP, network namespace, or explicitly configured NIC). The
daemon fails closed and never silently falls back to the default route. The
control plane (API/Web UI) is separate from the torrent data plane. See
`design/vpn-network-containment.md`.

## Repository Layout

```text
swarmotter/
├── AGENTS.md                 # Coding-agent governance rules
├── README.md
├── LICENSE                   # Apache-2.0
├── CONTRIBUTING.md
├── SECURITY.md
├── CODE_OF_CONDUCT.md
├── THIRD_PARTY_LICENSES.md
├── CHANGELOG.md
├── Cargo.toml                # Workspace root
├── crates/
│   ├── swarmotterd/          # Daemon binary
│   ├── swarmotter-core/      # Core types and engine logic
│   ├── swarmotter-api/       # API layer
│   └── swarmotter-web/       # Embedded/static web support
├── design/                   # Requirements, architecture, legal, ADRs
│   └── adr/
└── .github/                  # Issue and PR templates
```

## Development Status

The repository is scaffolded with a minimal Rust workspace. Engine
implementation is deferred until the design and acceptance criteria are
finalized in `design/`. To verify the workspace:

```bash
cargo fmt
cargo check
cargo test
```

## License

SwarmOtter is licensed under the Apache License, Version 2.0. See `LICENSE` for
details. Dependency licenses are tracked in `THIRD_PARTY_LICENSES.md`.