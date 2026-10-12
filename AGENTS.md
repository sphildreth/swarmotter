# AGENTS.md

Guidance for coding agents working in the SwarmOtter repository. Read this
before making changes. Human contributors should follow the same rules.

## Project summary

SwarmOtter is a performance-first Rust BitTorrent daemon with a practical Web
UI, a complete API, and fail-closed VPN/NIC traffic containment. It is a
general-purpose client intended for legal torrenting. Users choose their
content and are responsible for their use; SwarmOtter does not police those
choices.

The first release, `v1.0.0`, required every capability in
`design/requirements.md` to be implemented, tested, documented, and usable —
there was no MVP. The required feature set (live torrent data-plane engine,
network containment, API, and Web UI) is complete: the assembled release was
published as `v2.0.0` because the strict-containment default changed the
configuration compatibility contract (see `design/VERSIONING_GUIDE.md` and
`design/v1-completion-tracker.md`). Follow-on releases continue semantic
versioning; keep every shipped capability complete, tested, and documented —
do not regress, weaken, or downgrade any of them to "future enhancements".

## Non-negotiable rules

1. **No MVP.** SwarmOtter does not use an MVP release model. The first release
   was reached only when every required feature in `design/requirements.md`
   was implemented, tested, documented, and usable (see the project summary).
   All of these capabilities — DHT, PEX, UDP trackers, watch folders, browser
   magnet handling, file prioritization, queueing, bandwidth controls, fast
   resume, and VPN/NIC containment — are shipped and must
   remain so. Do not create wording that implies they are optional future
   enhancements, and do not remove or degrade them.

2. **No time estimates.** Do not provide calendar, sprint, week, month, or
   duration estimates. Track work by completed capabilities and acceptance
   criteria only. Do not add time estimates to documentation, comments,
   commits, issues, or PRs.

3. **Strict network containment.** All torrent-related traffic must go through
   the configured network path and must fail closed if that path is
   unavailable. Never let torrent traffic silently fall back to the default
   route. See `design/vpn-network-containment.md`.

4. **Function over form.** The Web UI must be complete and usable, but visual
   polish, animations, and heavy UI frameworks are non-goals unless they
   materially improve operations. The API and daemon are the primary product
   surfaces.

5. **Engine changes follow the design.** The torrent data-plane engine is
   implemented; it is not to be rebuilt ad hoc or extended outside the design
   and acceptance criteria in `design/requirements.md`,
   `design/architecture.md`, and the relevant ADRs (peer wire protocol,
   tracker strategy, DHT, uTP, storage). Preserve specified behavior and the
   fail-closed containment guarantees when changing engine code.

## ADR requirements

Significant technical, legal, release, and operational decisions are recorded
as Architecture Decision Records (ADRs) in `design/adr/`.

If a change introduces, removes, or materially alters an architectural decision,
create or update an ADR in `design/adr/`.

Create an ADR when a decision has lasting impact, meaningful trade-offs, or is
likely to matter to future contributors, including decisions that:

- Change product scope or user-visible workflow in a lasting way.
- Introduce or remove a significant dependency.
- Commit the project to a compatibility surface or integration strategy.
- Define persistent formats or storage conventions.
- Change durability, recovery, concurrency, or cancellation behavior.
- Establish important security, credential-storage, or configuration rules.
- Affect packaging, distribution, or network containment behavior.

An ADR is usually not needed for local refactors, bug fixes that restore
specified behavior, test-only changes, or minor documentation clarifications.
When in doubt, create the ADR.

ADR format and lifecycle are documented in `design/adr/README.md`. Use the next
sequential four-digit number, kebab-case title, and fill out every section.

## Neutral project stance

- Describe capabilities and operational behavior in plain, factual language.
- Review contributions for functionality, correctness, security, maintenance,
  and license compatibility. Do not use presumed user intent or content
  categories as acceptance gates.
- Keep user responsibility separate from technical safeguards such as network
  containment, authentication, and storage safety.
- See `design/legal.md` and ADR-0079 for the project stance.

## Network containment rules

- All torrent-related traffic must be constrained to the configured network
  path, including: peer TCP, peer UDP/uTP, DHT UDP, PEX-discovered peers, UDP
  trackers, HTTP/HTTPS trackers, webseeds, magnet metadata fetching, and DNS
  used by torrent operations.
- The application must fail closed and never silently fall back to the default
  route.
- The control plane (API/Web UI) is separate from the torrent data plane.
- No engine component should directly create outbound sockets or HTTP clients
  without going through the network binding and containment layer.
- See `design/vpn-network-containment.md`.

## Testing expectations

- Testing is tracked by feature completion and acceptance criteria, not time
  estimates.
- Add or update tests alongside feature work. Prefer generated local torrents
  and local swarm fixtures so tests do not depend on third-party content.
- Required test areas: unit tests (magnet/torrent parsing, info hash, queue,
  ratio/seeding, bandwidth logic, config validation, network containment
  logic), integration tests (API and lifecycle behavior), network containment
  tests (fail-closed conditions), storage tests, and local swarm tests.
- Run `cargo fmt`, `cargo check`, and `cargo test` before considering work
  done. Fix all reported issues. Clippy (`cargo clippy --workspace
  --all-targets -- -D warnings`) must also be clean; fix new lints rather
  than adding allows without justification.
- CI additionally enforces a supply-chain audit (`cargo deny check`, see
  `deny.toml` and ADR-0068): RustSec advisories and yanked crates fail the
  build. Resolve them with lockfile-only updates where a patched version
  exists.
- See `design/testing.md`.

## Rust quality expectations

- Keep code compiling cleanly. `cargo fmt`, `cargo check`, and `cargo test`
  must pass.
- Edition 2021. Add `// SPDX-License-Identifier: Apache-2.0` to new Rust
  source files.
- Use the async runtime and central network layer consistently; avoid ad hoc
  socket creation outside the network containment layer.
- Avoid `unwrap`/`expect` in production paths where a meaningful error can be
  returned; placeholders during setup are acceptable when clearly marked.
- Keep modules small and focused. Follow the crate layout in `Cargo.toml` and
  `design/architecture.md`.

## Documentation expectations

- Keep `design/` documentation aligned with implemented behavior. If accepted
  ADRs and product docs diverge, treat it as a documentation issue to resolve
  immediately.
- Documentation stubs should explain intended content and restate important
  constraints; do not leave empty files.
- Do not add time estimates to documentation.
- Update `CHANGELOG.md` for notable changes, recorded by capability.

## Dependency and license expectations

- Do not add unnecessary dependencies during setup tasks.
- New dependencies must be reviewed for: license compatibility (Apache-2.0),
  maintenance quality, security posture, whether they increase project
  complexity, and whether they affect torrent traffic containment.
- Record dependency additions or removals in `THIRD_PARTY_LICENSES.md` where
  applicable, and create an ADR when the dependency is significant.
- New dependencies must keep `cargo deny check` green; extend the `deny.toml`
  license allow list only with an explicit, justified change.
- Dependency traffic must respect the network containment layer; a dependency
  that cannot be constrained must not be used for torrent operations.

## Git hygiene expectations

- Write concise commit messages matching repo style. Do not commit secrets.
- Do not commit unless explicitly asked.
- Stage only intended files. Do not include `target/`, build artifacts, or
  local secrets.
- Do not include time estimates in commit messages or PR descriptions.

## When to update design

- Update `design/requirements.md` when required capabilities or acceptance
  criteria change.
- Update `design/architecture.md`, `design/api.md`, or
  `design/configuration.md` when the corresponding surface changes
  materially.
- Update `design/vpn-network-containment.md` when containment behavior,
  covered traffic, or fail-closed conditions change.
- Update `design/testing.md` when test strategy or required test areas change.
- Update `design/legal.md` and `docs/legal.md` when the project stance or
  licensing documentation changes.

## When to create an ADR

Create an ADR when a change introduces, removes, or materially alters an
architectural decision. See the ADR requirements section above and
`design/adr/README.md`.
