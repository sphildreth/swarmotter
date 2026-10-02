# ADR-0068: Supply-Chain Auditing in CI with cargo-deny

## Status

Accepted

## Context

SwarmOtter ships a daemon that handles untrusted peer, tracker, and DHT
network input and enforces strict network containment. Its dependency tree is
part of its security posture: a vulnerable `rustls`, `h2`, or HTTP stack in
the control plane or the contained data plane is a daemon-level security
issue, not just a hygiene issue.

Dependency review currently happens at introduction time
(`AGENTS.md` dependency expectations, `THIRD_PARTY_LICENSES.md`), but nothing
in CI continuously re-checks the resolved tree for RustSec advisories or
license drift. Lockfile-only updates (patch bumps within the same semver
requirement) can silently introduce a new dependency or a vulnerable version
without any code change in the repository.

## Decision

- Adopt `cargo-deny` as the continuous supply-chain gate, configured by
  `deny.toml` at the repository root.
- CI runs a dedicated `supply-chain` job (via `EmbarkStudios/cargo-deny-action`)
  on every push and pull request, failing on:
  - Known RustSec advisories for crates in `Cargo.lock` (`advisories`), and
    on yanked crates.
  - Licenses outside the project's permissive allow list (`licenses`), which
    must remain compatible with Apache-2.0 and `THIRD_PARTY_LICENSES.md`.
  - Unknown registries or git sources (`sources`).
  - Duplicate and wildcard dependency findings are reported as warnings
    (`bans`) so workspace-internal path dependencies do not fail builds, while
    remaining visible.
- Advisories are resolved by lockfile-only updates when a compatible patched
  version exists (`cargo update -p <crate>`); requirement bumps in
  `Cargo.toml` are treated as dependency changes and follow the normal
  dependency review process, including an ADR when the dependency is
  significant.

## Consequences

- Easier: continuous detection of vulnerable or mis-licensed dependencies
  without manual audits; the license allow list documents the accepted
  licensing surface in a machine-checkable form.
- Harder: new dependencies with non-permissive or unusual licenses fail CI and
  require an explicit `deny.toml` update with justification; an unfixed public
  advisory can block all merges until patched or explicitly tolerated.
- Required: when CI fails on an advisory, update the lockfile or add a
  documented, dated ignore entry in `deny.toml` with a reason. Unreviewed
  blanket ignores are not acceptable.
- Intentionally avoided: no automated dependency auto-upgrade bot is added by
  this decision; updates remain a deliberate change to the reviewed lockfile.

## Related Documents

- `AGENTS.md` — dependency and license expectations.
- `THIRD_PARTY_LICENSES.md` — reviewed dependency licenses.
- `deny.toml` — the enforcing configuration.
- `.github/workflows/ci.yml` — the `supply-chain` job.
- ADR-0007 (Apache-2.0 license), ADR-0009 (foundational dependency stack),
  ADR-0022 (API auth and contained resolution hardening).
