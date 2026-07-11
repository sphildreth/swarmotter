# Security Policy

## Supported Versions

Security fixes are applied to the latest `1.2.x` release line and the `main`
branch. Older minor release lines should be upgraded before reporting an issue
that is already fixed in the current release.

## Reporting a Vulnerability

To report a security vulnerability responsibly, please **do not** open a
public GitHub issue. Instead, open a private security advisory through
GitHub's "Report a vulnerability" feature on the repository's Security tab, or
contact the maintainers privately if an advisory channel is unavailable.

Please include:

- A description of the vulnerability and its impact.
- Steps to reproduce or a proof of concept.
- Affected components (daemon, API, Web UI, network containment layer).
- Any known mitigations.

The maintainers will acknowledge receipt and coordinate a fix and disclosure
timeline. Please avoid public disclosure until a fix is coordinated.

## Security Scope

The following areas are security-relevant:

- **Network containment.** Fail-closed behavior must never silently fall back
  to the default route. A regression that lets torrent traffic escape the
  configured network path is a security issue. See
  `design/vpn-network-containment.md`.
- **API authentication and authorization.** The API controls torrent
  operations and should not accept untrusted input without validation.
- **Credential and configuration storage.** Secrets must not be logged or
  committed.
- **Unsafe input handling.** Torrent metadata, magnet links, and tracker
  responses are untrusted input and must be parsed defensively.

## Network Containment Failures

SwarmOtter's network containment is a safety feature, not a piracy-evasion
feature. It is documented as routing correctness, privacy-preserving network
design, operational safety, container networking, and fail-closed behavior.
Any change that weakens fail-closed behavior is a security regression.
