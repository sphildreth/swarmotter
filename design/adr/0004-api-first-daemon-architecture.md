# ADR-0004: API-First Daemon Architecture

## Status

Accepted

## Context

SwarmOtter must be operable by external automation, scripts, browser
integrations, and the Web UI. If the Web UI uses privileged internal channels
unavailable to external callers, its features cannot be reproduced by
automation, and the daemon's behavior becomes inconsistent across surfaces.

## Decision

The daemon and API are the primary product surfaces. The Web UI consumes the
same API available to external tools and automation.

Any feature available in the Web UI must also be available through the API
unless there is a clear security or implementation reason not to expose it. The
API uses consistent JSON request/response structures, stable identifiers, API
versioning, and machine-readable error codes.

## Consequences

- External automation gains full operational control parity with the Web UI.
- The Web UI becomes a thin consumer, reducing duplicated torrent logic.
- API design work is required up front for each feature, but this is
  intentional and improves long-term maintainability.
- The Web UI is downstream of the API and must not implement torrent logic
  directly.

## Related Documents

- `AGENTS.md`
- `design/api.md`
- `design/architecture.md`