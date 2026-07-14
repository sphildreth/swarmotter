# ADR-0061: Compatible Automation Profile and Lifecycle Parity

## Status

Accepted

## Context

SwarmOtter already exposes opt-in qBittorrent and Transmission compatibility
adapters over its native API. Automation clients need more than basic add and
pause operations: they need categories/profiles, lifecycle actions, complete
state/error information, file and tracker inspection, and location/rename
workflows that retain native durable-state and containment guarantees.

Creating a second torrent engine, permissions model, or configuration path for
these adapters would cause drift and could introduce an authorization or
containment bypass. Compatibility must remain an intentionally bounded
automation surface, not a content-discovery or indexer integration.

## Decision

- Expand qBittorrent-compatible endpoints with read-only categories derived
  from SwarmOtter labels and named profiles, richer torrent properties,
  trackers/files inspection, and existing native lifecycle/location/rename
  operations. A category supplied at add time remains a label; an exact named
  profile also selects that profile before add-time policy resolution.
- Expand Transmission-compatible add and set behavior with optional named
  profile assignment (including explicit clearing), labels before profile
  resolution, and truthful completion, state, and error mappings.
- Every adapter operation delegates through `DaemonOps` and the same native
  configuration, persistence, authorization, browser-origin protection, and
  contained data-plane rules. No adapter receives a privileged socket,
  direct-storage path, independent account, indexer, search, or content
  discovery capability.
- Treat compatibility fields as translation contracts. Representative
  qBittorrent and Transmission automation flows are integration-tested so
  changes to category/profile intake, status, imports, and lifecycle actions
  do not silently regress external clients.

## Consequences

- More self-hosted automation can adopt SwarmOtter while still receiving its
  policy snapshots, native durable operations, and fail-closed containment.
- Translation remains intentionally incomplete where a foreign-client concept
  has no safe native equivalent; such gaps must be documented rather than
  imitated with a misleading state.
- Any later compatibility addition that changes authorization, persistent
  format, or data-plane behavior needs a follow-up ADR.

## Related Documents

- [Product backlog](../BACKLOG.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Lawful-use policy](../lawful-use.md)
- [Content policy](../content-policy.md)
- [ADR-0044: Browser Origin and Loopback API Security](0044-browser-origin-and-loopback-api-security.md)
- [ADR-0049: Configured Unauthenticated LAN Control Plane](0049-configured-unauthenticated-lan-control-plane.md)
- [ADR-0057: Policy Profiles and Inherited Settings](0057-policy-profiles-and-inherited-settings.md)
