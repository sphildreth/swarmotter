# ADR-0075: Inbound verified-piece serving for downloading torrents

## Status

Accepted

## Context

A downloading engine discarded inbound `Request` messages. Two incomplete
daemons in one swarm could therefore never serve each other, and a two-daemon
deployment without a seeder could not complete a torrent even when each side
held complementary pieces.

## Decision

Downloading torrents register a bounded inbound serving context on the shared
contained peer listener already used for seeding. Inbound sessions for a
downloading torrent serve `Request` messages only from byte ranges whose
pieces are verified in the torrent's shared piece bitfield; unverified or
missing ranges are rejected. Outbound peer sessions also serve uploads while
downloading through the same verified-reads rule. Serving sessions hold the
torrent's normal inbound permit and session budget, are torn down with the
engine, and use the identical containment-gated binder as all other data-plane
traffic. Verified-piece `Have` messages fan out to connected peers so remote
peers learn new pieces promptly.

## Consequences

- Two (or more) incomplete daemons complete torrents by mutual exchange.
- Upload-serving reads can never disclose unverified data, and inbound serving
  cannot bypass bandwidth limits or the network containment layer.
- The shared listener's availability gates only inbound serving; outbound
  transfer and containment behavior are unchanged when it is unavailable.
- Uploading while downloading consumes the torrent's upload bandwidth budget,
  which operators size for their deployment.

## Related Documents

- `design/scaling-review-fix-ledger-2026-10-03.md` (W5)
- `design/vpn-network-containment.md`
- `design/architecture.md`
