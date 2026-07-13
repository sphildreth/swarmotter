# ADR-0058: Global Peer-Admission Filtering

## Status

Accepted

## Context

Operators need a bounded, auditable way to decline known abusive, unwanted, or
malformed peer endpoints without changing where torrent traffic is routed.
Peer candidates may enter through trackers, DHT, PEX, direct sources, magnet
metadata discovery, or the shared inbound listener. Filtering only one of
those paths would leave an inconsistent admission boundary.

The existing network-containment layer is still mandatory: a peer filter must
never create a socket, select a route, resolve a hostname, or permit fallback
to an uncontained default route. Blocklist import must likewise be an explicit
local operator action rather than an unbounded remote-fetch subsystem.

## Decision

- Add one global `[peer_filter]` policy with an explicit disabled default. It
  accepts single IPs, CIDRs, inclusive IP ranges, local blocklist paths,
  manual IP bans, and printable peer-ID prefixes. Manual bans are created or
  removed through native Peer UI/API actions but apply to every torrent.
- Compile and validate every configured rule and bounded local blocklist before
  a configuration replacement can commit. Imported files must be regular,
  UTF-8, locally named files within fixed byte, line, and rule-count limits;
  remote URLs are not supported. A failed candidate compile leaves the active
  policy unchanged. If a source cannot be compiled while constructing the
  runtime, install a deny-all policy rather than silently allowing peers.
- Pass one immutable compiled policy generation to every torrent engine,
  metadata fetcher, discovery ingress, PEX import, endgame worker, and inbound
  seeder/listener session. Check IP admission before outbound connection or
  inbound service, and check peer-ID prefixes after the BitTorrent handshake.
  Accepted peers still use only binder-provided contained sockets.
- Treat a peer-filter replacement as a data-plane transaction. Candidate
  policy compilation, engine/session reconstruction, configuration persistence,
  and state reconciliation either complete together or restore the exact prior
  policy generation and live work.
- Expose the effective policy, local import outcomes, and cumulative rejection
  counters through the native API and Web UI so operators can audit decisions.

## Consequences

- The daemon can reduce unwanted peer contact consistently across all ingress
  paths while retaining required fail-closed network containment.
- Replacing a large local blocklist may rebuild active peer work and can fail
  cleanly if the source is invalid, unavailable, or exceeds its fixed bounds.
- Rules are global rather than per-profile, per-tracker, or per-user. Those
  scopes would need separate security, privacy, and containment decisions.
- Filtering is an admission decision, not a routing, anonymity, or content
  classification feature. It does not weaken lawful-use restrictions or the
  configured network path.

## Related Documents

- [Product backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [Architecture](../architecture.md)
- [API design](../api.md)
- [Network containment design](../vpn-network-containment.md)
- [ADR-0051: Explicit Network Path and Live Containment Gate](0051-explicit-network-path-and-live-containment-gate.md)
