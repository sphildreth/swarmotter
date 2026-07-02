# Architecture Decision Records (ADRs)

This directory contains Architecture Decision Records (ADRs) for SwarmOtter.

ADRs are the repository's durable record of decisions that have meaningful
architectural, product, legal, release, operational, or dependency impact.
They help contributors understand not only what was chosen, but why.

## Policy source of truth

Repository governance for ADR usage is defined in [`AGENTS.md`](../../AGENTS.md).
This README aligns with that governance and provides the practical workflow for
creating and maintaining ADRs in `design/adr/`.

When in doubt, prefer creating an ADR.

## When an ADR is required

Create an ADR when a decision has lasting impact, meaningful trade-offs, or is
likely to matter to future contributors. This includes decisions that:

- Change product scope or user-visible workflow in a lasting way.
- Introduce or remove a significant dependency.
- Commit the project to a compatibility surface or integration strategy.
- Define persistent formats or storage conventions.
- Change durability, recovery, concurrency, or cancellation behavior.
- Define binding, streaming, import, or export contracts.
- Establish important security, credential-storage, or configuration rules.
- Affect packaging, distribution, or network containment behavior.
- Require future contributors to understand trade-offs to work safely.

## When an ADR is usually not required

An ADR is usually not needed for:

- Local refactors that preserve intended behavior.
- Bug fixes that restore specified behavior without changing design intent.
- Test-only changes.
- Minor implementation details with no lasting architectural impact.
- Small documentation clarifications that do not change a decision.

If a change feels borderline, create the ADR.

## ADR format

Use this template for every ADR:

```markdown
# ADR-0000: Title

## Status

Accepted | Proposed | Superseded

## Context

What problem or decision is being addressed?

## Decision

What decision was made?

## Consequences

What becomes easier, harder, required, or intentionally avoided because of
this decision?

## Related Documents

Links to related design, issues, or requirements.
```

A copy of this template is kept as `0000-template.md`.

## How to create an ADR

1. Copy the template: `design/adr/0000-template.md` →
   `design/adr/NNNN-short-title.md`.
2. Choose the next sequential number `NNNN` using 4 digits.
3. Use a short, kebab-case title. Example: `0009-config-file-layout.md`.
4. Fill out every section concisely and specifically.
5. Link the ADR from the related change description and note its impact.

## ADR numbering rules

- Use 4 digits, zero-padded, sequential.
- Do not reuse numbers.
- If two changes race, the later change should renumber to the next available
  number.

## ADR lifecycle

Use one of these statuses:

- **Proposed** — the decision is under review or documented before acceptance.
- **Accepted** — the decision is approved and is being implemented or has been
  implemented.
- **Superseded** — replaced by a newer ADR; link the newer ADR in Related
  Documents.
- **Rejected** — considered and explicitly not chosen.

## Repository expectations

For meaningful architectural or product-impacting work:

- Create the ADR before or alongside implementation.
- Keep `design/requirements.md`, `design/architecture.md`, `design/api.md`,
  and other design docs aligned with accepted ADRs.
- Update references when an ADR supersedes an earlier decision.

If accepted ADRs and product docs diverge, contributors should treat that as a
documentation issue to resolve immediately.