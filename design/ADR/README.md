# ADRs (Architecture Decision Records)

**Date:** 2026-03-09

This directory contains Architecture Decision Records (ADRs) for SwarmOtter.

ADRs are the repository’s durable record of decisions that have meaningful
architectural, product, compatibility, operational, or dependency impact.
They help contributors understand not only what was chosen, but why.

## Policy source of truth

Repository governance for ADR usage is defined in `AGENTS.md`.

This README aligns with that governance and provides the practical workflow for
creating and maintaining ADRs in `design/adr/`.

When in doubt, prefer creating an ADR.

## When an ADR is required

Create an ADR when a decision has lasting impact, meaningful trade-offs, or is
likely to matter to future contributors. This includes decisions that:

- Change product scope or user-visible workflow in a lasting way
- Introduce or remove a significant dependency
- Commit the project to a compatibility surface or integration strategy
- Define persistent formats or storage conventions
- Change durability, recovery, concurrency, or cancellation behavior
- Define paging, streaming, import, export, or binding contracts
- Establish important security, credential-storage, or configuration rules
- Affect cross-platform packaging or distribution behavior
- Require future contributors to understand trade-offs to work safely

Typical examples in this repository include:

- DecentDB binding strategy
- Results paging and streaming contract
- Import type mapping and transform rules
- Export library and format strategy
- Config file layout and migration approach
- Secret storage approach per operating system
- User-visible workflow changes that alter MVP scope

## When an ADR is usually not required

An ADR is usually not needed for:

- Local refactors that preserve intended behavior
- Bug fixes that restore specified behavior without changing design intent
- Test-only changes
- Minor implementation details with no lasting architectural impact
- Small documentation clarifications that do not change a decision

If a change feels borderline, create the ADR.

## How to create an ADR

1. Copy the template:
   - `design/adr/0000-template.md` → `design/adr/NNNN-short-title.md`
2. Choose the next sequential number `NNNN` using 4 digits.
3. Use a short, kebab-case title.
   - Example: `0003-config-file-layout.md`
4. Fill out every section concisely and specifically.
5. Link the ADR from the related change description and note its impact.

## ADR numbering rules

- Use 4 digits, zero-padded, sequential.
- Do not reuse numbers.
- If two changes race, the later change should renumber to the next available
  number.

## ADR lifecycle

Use one of these statuses:

- **Proposed** — the decision is under review or documented before acceptance
- **Accepted** — the decision is approved and is being implemented or has been
  implemented
- **Superseded** — replaced by a newer ADR; link the newer ADR in References
- **Rejected** — considered and explicitly not chosen

## Writing guidance

Keep ADRs concise and decision-focused. A good ADR should clearly cover:

- the decision
- why it was made
- alternatives considered
- trade-offs and consequences
- references to related docs, specs, or prior ADRs

Prefer concrete language over aspirational wording.

## Repository expectations

For meaningful architectural or product-impacting work:

- create the ADR before or alongside implementation
- keep `design/PRD.md` and `design/SPEC.md` aligned with accepted ADRs
- update references when an ADR supersedes an earlier decision

If accepted ADRs and product docs diverge, contributors should treat that as a
documentation issue to resolve immediately.
