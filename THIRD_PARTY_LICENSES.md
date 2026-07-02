# Third-Party Licenses

This file tracks third-party dependencies and licenses used by SwarmOtter.

SwarmOtter source code is licensed under the Apache License, Version 2.0 (see
`LICENSE`). Each direct dependency must be compatible with Apache-2.0.

## Dependency review requirements

Before adding a dependency, review it for:

- License compatibility with Apache-2.0.
- Maintenance quality (actively maintained, no known unpatched issues).
- Security posture (supply-chain risk, auditability).
- Whether it increases project complexity without justified benefit.
- Whether it affects torrent traffic containment. A dependency that creates
  network traffic outside the network containment layer must not be used for
  torrent operations.

Record significant dependency additions or removals here, and create an ADR
in `design/adr/` when the dependency is significant.

## Direct dependencies

_None._ The current workspace skeleton has no external dependencies. As
dependencies are added, list them below with crate name, version, license, and
a brief justification.

| Crate | Version | License | Justification |
|-------|---------|---------|---------------|
| _(none yet)_ | | | |

## Notes

- Do not add unnecessary dependencies during setup tasks.
- Dependency traffic must respect the network containment layer.
- This document does not constitute legal advice.