# ADR-0032: Linux Release Artifact Strategy

## Status

Accepted

## Context

SwarmOtter needs a release process that produces useful install artifacts
without expanding the supported native surface beyond the product's Linux
daemon and fail-closed network-containment model. The daemon, systemd unit,
container image, and documented containment paths are Linux-oriented. Native
Windows and macOS packaging would add distribution and support obligations
without matching the current containment design.

Operators still need Raspberry Pi and other ARM server builds, and homelab
operators need a stable container image tag set.

## Decision

Stable `vX.Y.Z` tags publish these release artifacts:

- Linux `x86_64` and `aarch64` tarballs.
- Linux `.deb` packages for `amd64` and `arm64`.
- Linux `.rpm` packages for `x86_64` and `aarch64`.
- `SHA256SUMS` for GitHub Release assets.
- A multi-architecture GHCR image for `linux/amd64` and `linux/arm64`.

The release workflow builds native binaries through the same Docker build
context as the container image by using a `binary` export target in
`deploy/Dockerfile`. The packages install the daemon, default configuration,
systemd unit, service account, and standard state/download directories, but
they do not start the daemon automatically.

Windows and macOS native packages are not supported release artifacts.
Operators on those hosts should use the Linux container image through their
container runtime.

## Consequences

The release process aligns with SwarmOtter's Linux-first containment model and
keeps Raspberry Pi/aarch64 operators first class. The project avoids separate
native package managers, installers, and containment semantics for non-Linux
hosts.

Release tags now have a durable artifact contract: native Linux packages,
tarballs, checksums, and semver-tagged container images. Changes to installed
paths, package behavior, or image tag strategy are release-facing compatibility
changes.

## Related Documents

- [Deployment design notes](../deployment.md)
- [Deployment guide](../../docs/deployment.md)
- [Versioning guide](../VERSIONING_GUIDE.md)
