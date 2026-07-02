# ADR-0001: Record Architecture Decisions

## Status

Accepted

## Context

SwarmOtter is a long-lived project with non-negotiable rules around release
model, network containment, lawful use, and dependency hygiene. Without a
durable record of significant decisions, future contributors and coding agents
risk re-litigating settled choices or silently violating project rules.

Decisions in this project span technical, legal, release, and operational
concerns, so the record must cover more than implementation details.

## Decision

SwarmOtter will record significant technical, legal, release, and operational
decisions as Architecture Decision Records (ADRs) in `design/adr/`.

ADRs use a fixed template (Status, Context, Decision, Consequences, Related
Documents) and a sequential four-digit numbering scheme. The process and
lifecycle are documented in `design/adr/README.md`, and governance for when an
ADR is required is documented in `AGENTS.md`.

## Consequences

- Contributors have a discoverable record of why decisions were made.
- Coding agents can check `design/adr/` before proposing changes that may
  conflict with settled decisions.
- Creating an ADR adds a small step to significant changes; this is
  intentional and worth the clarity.
- ADRs that are superseded must be linked to their successors rather than
  deleted, preserving history.

## Related Documents

- `AGENTS.md`
- `design/adr/README.md`
- `design/adr/0000-template.md`