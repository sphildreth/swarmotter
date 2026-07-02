# ADR-0003: Release Model Is v1.0.0 Only, No MVP

## Status

Accepted

## Context

Some projects ship an intentionally reduced first release (an MVP) and defer
features like DHT, PEX, UDP trackers, watch folders, file prioritization,
queueing, bandwidth controls, fast resume, VPN/NIC containment, or browser
magnet handling. For a serious Transmission-style daemon, a reduced first
release would leave core torrent functionality incomplete and undermine the
network-safety guarantees that distinguish SwarmOtter.

SwarmOtter's network containment only provides real safety if it ships
complete; deferring it would ship an unsafe daemon.

## Decision

SwarmOtter does not use an MVP release model. The initial release is `v1.0.0`
only after all required features in `design/requirements.md` are implemented,
tested, documented, and usable.

DHT, PEX, UDP trackers, watch folders, browser magnet handling, file
prioritization, queueing, bandwidth controls, fast resume, VPN/NIC
containment, and legal documentation are all part of `v1.0.0` scope and must
not be described as optional future enhancements.

Progress is tracked by completed capabilities and acceptance criteria, not by
time or duration estimates.

## Consequences

- The first usable release is `v1.0.0`; there is no earlier product release.
- Internal development checkpoints may exist but are not feature-complete
  releases and must not be treated as such.
- Contributors must avoid MVP-style wording that defers required features.
- Network containment and fail-closed behavior must be complete before
  release rather than added later.

## Related Documents

- `AGENTS.md`
- `design/requirements.md`