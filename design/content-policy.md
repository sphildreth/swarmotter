# Content Policy

This document states what SwarmOtter will and will not include as project
artifacts. It applies to official SwarmOtter repositories, documentation,
examples, issue templates, discussions, release artifacts, and project-hosted
assets.

SwarmOtter is a lawful, general-purpose BitTorrent client. See `lawful-use.md`
for appropriate use cases and `legal.md` for project legal posture.

For user-facing legal and content policy guidance, see `../docs/legal.md`.

## Prohibited project content

The repository must not include:

- Pirate indexers.
- Search integrations aimed at infringing content.
- Bundled copyrighted `.torrent` files.
- Bundled infringing magnet links.
- Default tracker lists associated with infringing content.
- Documentation encouraging copyright infringement.
- Documentation explaining how to find pirated content.
- Example screenshots showing copyrighted movies, shows, commercial games,
  music albums, ROM collections, or cracked software.
- Documentation that frames VPN/NIC binding as a way to hide piracy or evade
  copyright enforcement.

## Prohibited wording

Do not use wording such as:

- "download free movies"
- "avoid copyright enforcement"
- "hide piracy"
- "bypass ISP monitoring"
- "pirate safely"

VPN/NIC containment must be described as routing correctness,
privacy-preserving network design, operational safety, container networking,
and fail-closed behavior.

## Safe examples and test data

Examples, tests, screenshots, and sample data must use clearly lawful sources:

- Generated local test torrents.
- Public-domain files.
- Open datasets.
- Linux distribution torrent examples.
- Project-owned sample files created specifically for SwarmOtter testing.

Automated tests should prefer generated local torrents and local swarm
fixtures so the test suite does not depend on third-party content availability.

## Scope

This policy defines what the SwarmOtter project will and will not include in
its repositories, documentation, examples, issue templates, discussions, release
artifacts, and project-hosted assets. It is a statement of project scope, not a
mechanism for policing or monitoring user behavior.

The project does not operate the software on users' behalf and has no
observability into how users deploy it. Users are responsible for their own
compliance with applicable law.

Piracy-oriented features, indexers, infringing magnets, or infringing-content
examples are not accepted as contributions. Such contributions will be rejected.
See `AGENTS.md` and `legal_content_report.md` for reporting concerns about
project content.
