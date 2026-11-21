# ADR-0086: API-Backed `swarmotterctl` CLI

## Status

Accepted

## Context

SwarmOtter operators need scriptable control over SSH and from jobs. The native
API already owns authentication, validation, lifecycle transactions, bulk
semantics, pagination, and durable state. A separate CLI must consume that
contract rather than reading daemon-private state or opening data-plane sockets.

Operators also need predictable machine output and exit codes so automation can
distinguish authentication, validation, missing-item, and connection failures
without parsing human prose. Credentials must not appear in routine output or
command arguments.

## Decision

- Add a separate `swarmotterctl` binary crate that uses only the native
  `/api/v1` control-plane API over HTTP/1. It has no torrent data-plane client,
  does not read the daemon state file, trash, fast-resume files, or payload data,
  and performs no tracker/peer work.
- Support add (magnet or `.torrent` file), list with server-side filters and
  sorting, show/details, health, pause/resume/start/stop, remove with optional
  data deletion, recheck, reannounce, and per-torrent bandwidth limits.
- Use the existing API request shapes and semantics. Lifecycle, duplicate,
  queued, paused, preview, containment, and destructive behavior remain owned by
  the daemon.
- Provide human-readable output and a versioned JSON mode. JSON output is one
  document on stdout with `format_version: 1`, command, success, data, and an
  optional error object. Human errors go to stderr. Normal results and
  diagnostics are never interleaved on stdout.
- Define stable exit codes: 0 success, 1 daemon/IO failure, 2 usage or
  configuration, 3 authentication/authorization, 4 missing item, 5 connection
  failure, 6 validation/bad request.
- Resolve the endpoint from `--url`, `SWARMOTTER_API_URL`, or the local default.
  Resolve credentials from `--token-file`, `SWARMOTTER_API_TOKEN`, or
  `SWARMOTTER_API_TOKEN_FILE`. Do not accept a token in a command-line argument,
  print the token, or include user information in the API URL. HTTPS uses the
  system-compatible webpki roots with validated certificates.
- Large lists use the server-side query endpoint and its pagination. `--all`
  requests pages explicitly rather than downloading one unbounded response;
  page size is capped at the server limit.
- Package `swarmotterctl` alongside `swarmotterd` in release tarballs, nFPM
  packages, and container images. Document local/remote endpoint and credential
  setup, package installation, JSON schema, exit codes, pagination, secret
  handling, and scripting examples.

## Consequences

- Operators can automate the same operations as the Web UI without exposing
  private daemon storage or a second control implementation.
- JSON and exit-code contracts become a compatibility surface that must be
  versioned and tested.
- The client remains useful across remote reverse-proxy/TLS deployments while
  retaining the daemon's API authorization boundary.
- New CLI commands should call an existing API contract; if a needed operation
  has no API, add the API first and document both surfaces.
- Future CLI changes must preserve stdout/stderr separation, versioned JSON,
  stable failure classes, secret redaction, server pagination, and the
  no-private-state/no-data-plane boundary.

## Related Documents

- [ADR-0004: API-First Daemon Architecture](0004-api-first-daemon-architecture.md)
- [ADR-0010: API Versioning, Envelope, and Events](0010-api-versioning-envelope-events.md)
- [ADR-0069: Bulk Lifecycle Actions](0069-bulk-lifecycle-actions.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Testing strategy](../testing.md)
- [Operator CLI](../../docs/cli.md)
- [Operator API](../../docs/api.md)
- [Deployment](../../docs/deployment.md)
