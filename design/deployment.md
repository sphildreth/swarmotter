# Deployment

This document describes SwarmOtter deployment. It is a stub; examples will be
fleshed out as implementation matures.

## Supported deployment models

- Linux daemon deployment.
- Systemd service deployment.
- Container deployment (Podman, Docker-compatible where practical).
- VPN container/network namespace deployment.
- Reverse proxy deployment for the Web UI/API.
- Configuration through files and environment variables.
- Persistent storage volumes.

## Documentation must include (before v1.0.0)

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

## Constraints

- Torrent data traffic must bind to the configured VPN/NIC path; the control
  plane (API/Web UI) is separate. See `vpn-network-containment.md`.
- The daemon must fail closed when the configured path is unavailable.

## TODO

- Add example `swarmotterd.service` systemd unit.
- Add example `docker-compose` / Podman deployment.
- Add example reverse-proxy config (API/Web UI).
- Add VPN network namespace deployment guide.
- Keep this document aligned with `configuration.md` and
  `vpn-network-containment.md`.