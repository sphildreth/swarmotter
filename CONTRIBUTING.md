# Contributing to SwarmOtter

Thank you for your interest in contributing to SwarmOtter. This document
explains contribution expectations.

## Read the rules first

Before contributing, read [`AGENTS.md`](./AGENTS.md). It applies to both
coding agents and human contributors and contains the non-negotiable project
rules.

Key points:

- **No MVP.** The first release is `v1.0.0`, complete only when all required
  features in `design/requirements.md` are implemented, tested, documented,
  and usable.
- **No time estimates.** Do not include calendar, sprint, week, month, or
  duration estimates in issues, PRs, commits, or documentation. Track work by
  completed capabilities and acceptance criteria.
- **Lawful use only.** Do not add piracy-oriented features, indexers,
  infringing-content search, bundled copyrighted torrents, infringing magnets,
  or examples/screenshots based on copyrighted movies, shows, music, games,
  ROMs, or cracked software. See `design/content-policy.md`.
- **Strict network containment.** All torrent-related traffic must go through
  the configured network path and must fail closed. See
  `design/vpn-network-containment.md`.

## Architecture Decision Records

If a change introduces, removes, or materially alters an architectural
decision, create or update an ADR in `design/adr/`. See `design/adr/README.md`
for the format and lifecycle.

## Development workflow

1. Ensure the workspace builds and tests pass:

   ```bash
   cargo fmt
   cargo check
   cargo test
   ```

2. Add or update tests alongside feature work. Prefer generated local torrents
   and local swarm fixtures so tests do not depend on third-party content.
3. Keep `design/` documentation aligned with implemented behavior.
4. Update `CHANGELOG.md` for notable changes, recorded by capability.
5. Do not commit secrets, `target/`, or build artifacts.

## License

By contributing, you agree your contributions are licensed under the
Apache License, Version 2.0. Add `// SPDX-License-Identifier: Apache-2.0` to
new Rust source files.

## Dependencies

Do not add unnecessary dependencies. New dependencies must be reviewed for
license compatibility (Apache-2.0), maintenance quality, security posture,
whether they increase project complexity, and whether they affect torrent
traffic containment. Record significant additions in
`THIRD_PARTY_LICENSES.md` and create an ADR when the dependency is
significant.

## Piracy-oriented contributions are not accepted

Contributions that add piracy-oriented features, infringing-content search,
bundled copyrighted torrents, infringing magnet links, or documentation
encouraging copyright infringement will be rejected.