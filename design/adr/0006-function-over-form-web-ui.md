# ADR-0006: Function Over Form Web UI

## Status

Accepted

## Context

A torrent daemon's Web UI needs to expose many operational controls: torrent
lists, add flows, details, files, peers, trackers, queue, settings, network
health, watch-folder status, and logs. Investing in elaborate visual design,
animations, heavy theming, or large frontend frameworks would add complexity
without improving operational control and would increase maintenance burden.

## Decision

The Web UI must be complete and usable, but visual polish, animations, and
heavy UI frameworks are non-goals unless they materially improve operations.

The Web UI consumes the same API exposed to external automation (see
ADR-0004) and should not contain torrent logic. It prioritizes fast page
load, clear torrent state, low resource use, reliable controls, useful
diagnostics, and complete feature coverage.

## Consequences

- The API and daemon remain the primary product surfaces; the Web UI is a
  practical operational dashboard.
- Maintenance effort stays focused on functionality rather than visual polish.
- Heavy frontend frameworks and animation systems are avoided by default.
- Any future UI investment must be justified by operational improvement, not
  aesthetics alone.

## Related Documents

- `AGENTS.md`
- `design/architecture.md`
- `design/adr/0004-api-first-daemon-architecture.md`