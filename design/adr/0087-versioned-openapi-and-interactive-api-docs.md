# ADR-0087: Versioned OpenAPI and Interactive API Documentation

## Status

Accepted

## Context

SwarmOtter exposes a large native `/api/v1` control plane plus bounded
qBittorrent `/api/v2` and Transmission RPC compatibility adapters. Integrators
need a version-matched, machine-readable contract so they can discover what this
build actually implements without inferring support from another client or from
undocumented routes.

The API contract includes envelopes, typed errors, authentication, binary
metainfo upload/export, paginated queries, events, compatibility parity limits,
and state-changing operations. A hand-maintained document would drift. The
interactive explorer must respect normal authentication and browser-origin
controls rather than introducing a separate privileged path.

## Decision

- Publish an OpenAPI 3.1 document generated from a route catalog in the API
  crate. The catalog is checked against the actual `.route(...)` registrations
  in `routes.rs`; a deliberate catalog-drift fixture must fail CI.
- Document native `/api/v1` methods, authentication, request/response content
  types, status codes, envelopes, typed errors, representative examples, binary
  metainfo operations, paginated querying, events, and state-changing markers.
- Document qBittorrent `/api/v2` and Transmission RPC as explicit supported
  subsets. Operations carry `x-swarmotter-compatibility` parity limits; the
  Transmission operation carries `x-swarmotter-supported-rpc-methods` so a
  consumer can identify unsupported methods without guessing. Unsupported
  methods are absent from the route catalog or return the adapter's explicit
  not-found response.
- Include JSON Schema components for the shared envelope/error types,
  representative torrent/list/create/RSS/trash/provenance requests and
  responses, and compatibility response shapes. Representative examples and
  live responses from all three surfaces are validated against those schemas.
- Serve the version-matched document at `GET /api/openapi.json` and a
  self-contained interactive explorer at `GET /api/docs`. The explorer fetches
  the document from the same daemon, uses normal same-origin API authentication
  (X-SwarmOtter-Auth from an in-memory tab token field), marks state-changing
  operations visibly, and does not embed tokens in URLs or examples.
- Keep the explorer dependency-free and self-contained. It loads no third-party
  JavaScript, CDN, analytics, or remote font/style resource. The response uses a
  restrictive Content-Security-Policy.
- The documentation surface is public because it contains no secrets; every
  operation it can execute remains subject to the daemon's normal auth,
  browser-origin, compatibility-enablement, and authorization middleware.
- The generated document is the machine-readable artifact. Operators and
  integrators fetch `/api/openapi.json` for the exact build. Schema components
  are embedded in that document rather than duplicated as drifting side files.
- Contract changes require updating route/operation metadata and passing the
  route-catalog, schema-example, and live-daemon exchange tests. Documentation
  must describe the version/update process.

## Consequences

- Integrators get a discoverable, version-matched contract and a browser
  explorer for this build.
- Route additions or changes fail CI until the catalog and documentation are
  updated; this is intentional.
- The compatibility adapters remain explicitly bounded instead of implying full
  support for another client's API.
- The explorer is intentionally simpler than full Swagger UI, avoiding a heavy
  frontend dependency or external CDN while preserving API discovery and
  same-origin try-it behavior.
- Future API changes must preserve generated-route drift checks, schema
  validation, compatibility parity documentation, public/private auth
  boundaries, state-changing markers, and the self-contained explorer.

## Related Documents

- [ADR-0004: API-First Daemon Architecture](0004-api-first-daemon-architecture.md)
- [ADR-0010: API Versioning, Envelope, and Events](0010-api-versioning-envelope-events.md)
- [ADR-0026: Transmission RPC Compatibility Adapter](0026-transmission-rpc-compatibility-adapter.md)
- [ADR-0038: qBittorrent Compatibility Shim](0038-qbittorrent-compatibility-shim.md)
- [ADR-0061: Compatible Automation Profile and Lifecycle Parity](0061-compatible-automation-profile-and-lifecycle-parity.md)
- [ADR-0086: API-Backed `swarmotterctl` CLI](0086-api-backed-swarmotterctl-cli.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Operator API](../../docs/api.md)
- [CLI reference](../../docs/cli.md)
- [Testing strategy](../testing.md)
