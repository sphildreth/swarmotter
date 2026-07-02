# Lawful Use

SwarmOtter is a neutral, general-purpose BitTorrent client. BitTorrent is a
protocol with substantial lawful uses. This document explains appropriate use
cases and user responsibilities.

This document is project policy, not legal advice. It does not replace review
by qualified legal counsel where needed.

For user-facing lawful-use guidance, see `docs/lawful-use.md`.

## Appropriate lawful use cases

SwarmOtter is intended for lawful downloading, sharing, and seeding of content
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

## User responsibility

Users are responsible for ensuring that their use of SwarmOtter complies with
applicable laws and the rights of content owners. SwarmOtter does not provide
content, does not index content, and does not provide legal advice.

## What SwarmOtter does not do

- SwarmOtter does not include, endorse, host, index, or provide access to
  copyrighted material distributed without authorization.
- SwarmOtter does not bundle pirate indexers or infringing-content search.
- SwarmOtter does not include bundled copyrighted `.torrent` files or
  infringing magnet links.
- See `content-policy.md` for the full prohibited-content list.

## Network containment framing

VPN/NIC containment is a routing-correctness, privacy-preserving, and
operational-safety feature. It is documented as network containment and
fail-closed behavior — not as a way to hide piracy or evade enforcement.
