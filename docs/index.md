# SwarmOtter User Guide

<img src="assets/swarmotter-logo.png" alt="SwarmOtter logo" width="256" height="256" style="display:block;margin:1em auto;max-width:50%;height:auto;" />

SwarmOtter is a performance-first Rust BitTorrent daemon with a practical Web
UI, a complete API, and fail-closed VPN/NIC traffic containment.

This guide is for people running SwarmOtter. Architecture, requirements, ADRs,
and contributor-facing design records remain in the repository `design/`
directory.

## What SwarmOtter provides

- A daemon process, `swarmotterd`.
- A Web UI served by the daemon.
- A REST API under [`/api/v1`](api.md).
- `.torrent`, magnet, tracker, DHT, PEX, TCP, UDP tracker, and uTP support.
- IPv4 and IPv6 torrent networking when enabled by configuration.
- Strict data-plane containment through an interface, source address, or network
  namespace.

## Important operating model

SwarmOtter separates the control plane from the torrent data plane.

- The control plane is the API and Web UI listener configured by
  `api.bind_address`.
- The torrent data plane is peer, tracker, DHT, PEX, webseed, magnet metadata,
  and torrent-related DNS traffic.

Network containment applies to the torrent data plane. Binding the Web UI to a
LAN address does not allow torrent traffic to use that LAN path unless the
torrent network configuration explicitly allows and enforces it.

## Start here

Use [Getting Started](getting-started.md) for a local run, then read
[Configuration](configuration.md) for the common `br0`, VPN, and container
configurations. Use [API Reference](api.md) for scripting and integration
work.
