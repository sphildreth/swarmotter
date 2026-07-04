# Lawful Use

SwarmOtter is a neutral, general-purpose BitTorrent client. BitTorrent is a
protocol with substantial lawful uses. This document describes the project's
intended scope and appropriate use cases.

This document is a statement of project scope and intent, not legal advice. It
does not create any obligation on the part of users and does not replace review
by qualified legal counsel where needed.

For user-facing lawful-use guidance, see `docs/lawful-use.md`.

## Project scope

The SwarmOtter project does not operate the software on users' behalf. The
project does not have observability into how users deploy it. **Users are solely
responsible for ensuring their use of SwarmOtter complies with applicable laws
and the rights of content owners.**

## Appropriate lawful use cases

SwarmOtter is well-suited for lawful downloading, sharing, and seeding of content
that users have the right to access and distribute. Examples include:

- **Linux distributions:** downloading and seeding distribution ISOs and
  updates that are officially distributed via BitTorrent.
- **Open-source project releases:** distributing project binaries, source
  archives, and release artifacts shared via torrent.
- **Public-domain media:** content whose copyright has expired or was
  explicitly placed in the public domain.
- **Open datasets:** research, government, and community datasets published for
  open distribution.
- **User-owned files:** content the user created or owns the rights to
  distribute.
- **Organization-approved distribution:** internal company or homelab file
  distribution where the user has rights to the content.
- **Test fixtures:** generated local torrents and local swarm fixtures used for
  testing SwarmOtter itself.

## What the project will not include

These items are excluded from SwarmOtter repositories, documentation, and project
artifacts as a matter of project scope:

- Pirate indexers.
- Search integrations aimed at infringing content.
- Bundled copyrighted `.torrent` files or infringing magnet links.
- Default tracker lists associated with infringing content.
- Documentation encouraging copyright infringement or explaining how to find
  unauthorized content.
- Example screenshots based on copyrighted movies, shows, commercial games,
  music albums, ROM collections, or cracked software.

See `content-policy.md` for the full list.

## Network containment framing

VPN/NIC containment is a routing-correctness, privacy-preserving, and
operational-safety feature. It is documented as network containment and
fail-closed behavior — not as a way to hide piracy or evade enforcement.
