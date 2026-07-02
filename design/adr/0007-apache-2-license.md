# ADR-0007: Apache-2.0 License

## Status

Accepted

## Context

SwarmOtter is a FOSS project and must choose a clear source license before
public release. The license must be permissive enough to encourage adoption
and redistribution, compatible with commonly used Rust ecosystem dependencies,
and clear about patent grants.

## Decision

SwarmOtter uses Apache-2.0 for source code.

The `LICENSE` file contains the full Apache License, Version 2.0 text. Crate
metadata declares `license = "Apache-2.0"`, and new Rust source files include
`// SPDX-License-Identifier: Apache-2.0`.

## Consequences

- The project gets an explicit patent grant and a well-understood permissive
  license.
- Most Rust ecosystem crates (MIT/Apache-2.0) are compatible.
- Contributions are accepted under Apache-2.0, documented in
  `CONTRIBUTING.md`.
- Dependencies must be reviewed for Apache-2.0 compatibility, tracked in
  `THIRD_PARTY_LICENSES.md`.
- Legal restrictions beyond the FOSS license belong in documentation and
  project policy, not in the license itself (see ADR-0008).

## Related Documents

- `LICENSE`
- `CONTRIBUTING.md`
- `THIRD_PARTY_LICENSES.md`
- `design/legal.md`