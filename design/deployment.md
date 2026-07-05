# Deployment Design Notes

This document records SwarmOtter deployment architecture and compatibility
boundaries. Operator-facing setup instructions belong in the published mdBook
page: `../docs/deployment.md`.

## Deployment surfaces

SwarmOtter supports these deployment surfaces:

- `swarmotterd` as a direct Linux daemon.
- `deploy/swarmotterd.service` for systemd.
- GitHub Release tarballs for Linux `x86_64` and `aarch64`.
- `.deb` packages for Linux `amd64` and `arm64`.
- `.rpm` packages for Linux `x86_64` and `aarch64`.
- `deploy/Dockerfile` for multi-architecture Linux container images.
- `deploy/compose.yml` for Docker Compose homelab deployments.
- Reverse proxy deployments in front of the API/Web UI control plane.

## Design constraints

- The API/Web UI control plane is independent from the torrent data plane.
- Deployment docs must preserve the distinction between control-plane exposure
  and torrent data-plane containment.
- Container deployments must not imply that publishing `9091` exposes torrent
  peer/tracker/DHT traffic.
- VPN, NIC, interface, source-address, and namespace deployment patterns must
  be described as routing correctness, privacy-preserving network design,
  operational safety, container networking, and fail-closed behavior.
- User-facing examples must live in `../docs/deployment.md` so they are
  published through mdBook.

## Container contract

The release-facing container contract includes:

- Entrypoint: `/usr/local/bin/swarmotterd`.
- Default command: `--config /etc/swarmotter/swarmotter.toml`.
- Control-plane port: `9091`.
- Persistent paths: `/data/downloads`, `/data/incomplete`, and
  `/var/lib/swarmotter`.
- Config path: `/etc/swarmotter/swarmotter.toml`.
- Healthcheck: `GET http://127.0.0.1:9091/health`.

Changes to this contract are release-facing and should be handled through
`VERSIONING_GUIDE.md`.

## Release artifact contract

Stable `vX.Y.Z` release tags publish these artifacts:

- Linux `x86_64` and `aarch64` tarballs containing the daemon binary,
  configuration examples, deployment examples, license files, and user-guide
  pages.
- Native `.deb` packages for `amd64` and `arm64`.
- Native `.rpm` packages for `x86_64` and `aarch64`.
- A `SHA256SUMS` file for GitHub Release assets.
- A GHCR image manifest for `linux/amd64` and `linux/arm64`.

The native packages install `/usr/bin/swarmotterd`,
`/etc/swarmotter/swarmotter.toml`, and the systemd unit at
`/usr/lib/systemd/system/swarmotterd.service`. Package scripts create the
`swarmotter` service account and reload systemd metadata but do not start the
daemon automatically.

Release automation builds the Linux binaries with Rust target builds and uses
those prebuilt binaries for release container images. This keeps version-tag
image publication from recompiling the daemon under CPU emulation.

Windows and macOS native packages are not release surfaces. Operators on those
hosts should use the Linux container image through their container runtime.

## Maintenance

When deployment behavior changes:

1. Update the deployment artifacts under `deploy/`.
2. Update `../docs/deployment.md` for operator instructions.
3. Update this document only when deployment architecture or compatibility
   boundaries change.
4. Keep network-containment reasoning aligned with
   `vpn-network-containment.md` and the accepted containment ADRs.
