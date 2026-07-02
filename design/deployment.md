# Deployment

This document describes SwarmOtter deployment. It covers Linux daemon,
systemd, container, and VPN network-namespace deployment with strict network
containment and fail-closed behavior.

## Supported deployment models

- Linux daemon deployment.
- Systemd service deployment.
- Container deployment (Podman, Docker-compatible).
- VPN container/network namespace deployment.
- Reverse proxy deployment for the Web UI/API.
- Configuration through files and environment variables.
- Persistent storage volumes.

## Prerequisites

- Rust stable (see `rust-version` in `Cargo.toml`) for building, or a
  prebuilt `swarmotterd` binary.
- Linux recommended for network-containment development and testing.
- A configured VPN path (interface, source IP, or network namespace) when
  strict containment is enabled.

## Building

```bash
git clone https://github.com/sphildreth/swarmotter.git
cd swarmotter
cargo build --release
# Binary: target/release/swarmotterd
```

## Basic Linux daemon setup

```bash
sudo install -d /etc/swarmotter /var/lib/swarmotter
sudo install -m 0644 config/swarmotter.toml.example /etc/swarmotter/swarmotter.toml
# Edit /etc/swarmotter/swarmotter.toml to match your environment.
swarmotterd --config /etc/swarmotter/swarmotter.toml
```

The API and Web UI listen on `api.bind_address` (default `127.0.0.1:9091`).
Open `http://127.0.0.1:9091/` for the Web UI; the API is under `/api/v1/`.

## Systemd service

An example unit is provided at `deploy/swarmotterd.service`:

```bash
sudo install -m 0644 deploy/swarmotterd.service /etc/systemd/system/swarmotterd.service
sudo systemctl daemon-reload
sudo systemctl enable --now swarmotterd
```

Create a dedicated user and directories:

```bash
sudo useradd -r -s /usr/sbin/nologin swarmotter
sudo install -d -o swarmotter -g swarmotter /var/lib/swarmotter /data/downloads /data/incomplete
```

## VPN / NIC containment setup

Strict containment forces all torrent-related traffic (peers, trackers, DHT,
PEX, webseeds, magnet metadata, and torrent-related DNS) through the configured
network path and **fails closed** if that path is unavailable. The control
plane (API/Web UI) is independent of the torrent data plane.

Recommended deployment: run `swarmotterd` inside a VPN network namespace or a
container attached to a VPN network, so the daemon cannot reach peers, DHT, or
trackers except through the tunnel. Configure the `network` section to match:

```toml
[network]
mode = "strict"
required_interface = "tun0"
required_source_ipv4 = "10.8.0.2"
allow_ipv6 = false
fail_closed = true
validate_route = true
validate_dns = true

[torrent]
listen_port = 51413
```

### Fail-closed behavior

When strict containment is enabled and the configured path is unavailable
(missing interface, down interface, missing source address, invalid route,
unavailable namespace, or socket bind failure), SwarmOtter:

- Refuses to start torrent networking.
- Blocks new torrent connections.
- Moves active torrents to the `network_blocked` state.
- Reports the failure through `/api/v1/network/health` and the Web UI.
- Logs the failed requirement.

The API/Web UI listener remains available so operators can diagnose and fix
the path without losing control of the daemon.

### Network health states

See `design/vpn-network-containment.md` for the full list of health states
(`healthy`, `disabled`, `interface_missing`, `interface_down`,
`no_interface_address`, `source_address_missing`, `route_invalid`,
`socket_bind_failed`, `dns_not_constrained`,
`network_namespace_unavailable`, `blocked_fail_closed`).

## Container setup (Podman / Docker)

An example `Dockerfile` is provided at `deploy/Dockerfile`:

```bash
docker build -t swarmotter .
docker run -d --name swarmotter \
  -p 9091:9091 \
  -v /data/downloads:/data/downloads \
  -v /data/incomplete:/data/incomplete \
  -v /etc/swarmotter:/etc/swarmotter:ro \
  swarmotter
```

For containment, attach the container to a VPN network (e.g. via
`--network=container:vpn` or a network namespace) rather than the default
bridge, so torrent traffic cannot escape the tunnel. The API/Web UI port
(`9091`) may be published to the LAN while torrent data traffic stays on the
VPN-attached interface.

## Reverse proxy example (nginx)

```nginx
server {
    listen 80;
    server_name swarmotter.example;

    location / {
        proxy_pass http://127.0.0.1:9091;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # WebSocket and SSE events.
    location /api/v1/ws {
        proxy_pass http://127.0.0.1:9091;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
    location /api/v1/events {
        proxy_pass http://127.0.0.1:9091;
        proxy_buffering off;
        proxy_read_timeout 1h;
    }
}
```

## Environment variable overrides

Settings can be overridden via environment variables using the prefix
`SWARMOTTER_` with nested fields separated by `__`. Examples:

```bash
SWARMOTTER_API__BIND_ADDRESS=0.0.0.0:9091
SWARMOTTER_TORRENT__LISTEN_PORT=51414
SWARMOTTER_NETWORK__MODE=strict
SWARMOTTER_NETWORK__REQUIRED_INTERFACE=tun0
```

## Example configuration

See `config/swarmotter.toml.example` for a complete annotated configuration.

## Lawful use

SwarmOtter is a general-purpose BitTorrent client for lawful downloading,
sharing, and seeding of content that users have the right to access and
distribute. See `design/lawful-use.md`, `design/content-policy.md`, and
`design/legal.md`.

## License and dependency licenses

SwarmOtter is licensed under Apache-2.0 (see `LICENSE`). Dependency licenses
are tracked in `THIRD_PARTY_LICENSES.md`.