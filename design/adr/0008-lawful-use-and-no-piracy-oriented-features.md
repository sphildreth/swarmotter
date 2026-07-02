# ADR-0008: Lawful Use and No Piracy-Oriented Features

## Status

Accepted

## Context

BitTorrent is a general-purpose protocol with substantial lawful uses, but
torrent software is sometimes misused for copyright infringement. To keep
SwarmOtter a neutral, lawful tool and to keep the project maintainable, the
repository must avoid bundling or encouraging infringing content and must
frame network containment as safety, not piracy evasion.

Lawful-use expectations belong in documentation and project policy, not as
extra restrictions added to the FOSS license itself, since adding restrictions
to a permissive license would defeat its purpose.

## Decision

SwarmOtter is a lawful general-purpose BitTorrent client and must not include
piracy-oriented features, examples, default indexers, bundled copyrighted
torrents, infringing magnet links, or documentation encouraging infringement.

The project will not include pirate indexers, search integrations for
infringing content, bundled copyrighted `.torrent` files, bundled infringing
magnet links, documentation encouraging copyright infringement, or examples
based on copyrighted movies, shows, music, games, ROMs, or cracked software.

VPN/NIC containment is described as routing correctness, privacy-preserving
network design, operational safety, container networking, and fail-closed
behavior — never as piracy evasion. Users are responsible for ensuring their
use complies with applicable laws; the project does not provide legal advice.

## Consequences

- The repository avoids content that could enable or encourage infringement.
- Examples, tests, and screenshots must use clearly lawful sources such as
  generated local torrents, public-domain files, open datasets, or Linux
  distribution examples.
- Network containment documentation uses routing and safety framing only.
- Legal posture is documented in `design/lawful-use.md`,
  `design/content-policy.md`, and `design/legal.md`, separate from the
  Apache-2.0 license.
- Piracy-oriented contributions are rejected.

## Related Documents

- `AGENTS.md`
- `design/lawful-use.md`
- `design/content-policy.md`
- `design/legal.md`
- `design/adr/0007-apache-2-license.md`