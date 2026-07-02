# ADR-0019: Mainline DHT (BEP 5) Implementation Strategy

## Status

Accepted

## Context

SwarmOtter must support trackerless torrents and broader peer discovery via
the mainline DHT (BEP 5). DHT traffic is UDP and is torrent data-plane traffic,
so it must go through the network containment layer and fail closed. Private
torrents must not use DHT. The implementation must be testable without
external DHT nodes.

## Decision

Implement DHT in two layers:

- `swarmotter-core::dht` holds the pure, unit-tested KRPC encode/decode, node
  ID (with XOR distance), a simplified bounded routing table (closest-by-XOR
  lookup), compact node and peer parsing, and query builders for `ping`,
  `find_node`, `get_peers`, and `announce_peer`. This layer has no sockets.
- `swarmotterd::dht::DhtRunner` drives KRPC over a contained UDP socket
  obtained from the `NetworkBinder::udp_socket()`. It bootstraps from
  configured nodes, runs iterative `get_peers` for an info hash (bounded
  rounds, short per-node timeouts, hard total caps in the engine so
  unreachable bootstrap nodes cannot stall downloads), `announce_peer`, and
  exposes a node count for status.

The engine integrates DHT for trackerless and supplemental discovery: for
non-private torrents it asks the DHT for peers holding the info hash and
merges results into the candidate pool; for trackerless magnets the metadata
fetch falls back to DHT-discovered peers. Private torrents skip DHT entirely.
Bootstrap node strings (`host:port`) are resolved via std, subject to DNS
containment validation at the config layer.

## Consequences

- DHT traffic cannot bypass containment: `BlockedBinder` and strict
  fail-closed mode refuse to create the UDP socket, so DHT is blocked when
  the path is unavailable.
- DHT discovery is best-effort and time-bounded; slow/unreachable nodes cannot
  stall the download or magnet metadata fetch.
- Private torrents disable DHT (no `get_peers`/`announce_peer`).
- The routing table is intentionally simplified (no dynamic bucket splitting,
  bounded size) — sufficient for peer discovery; a full K-bucket tree is
  future work if needed for large-scale routing.

## Related Documents

- `crates/swarmotter-core/src/dht.rs`
- `crates/swarmotterd/src/dht.rs`
- `design/vpn-network-containment.md`
- ADR-0012 (network binder)
- ADR-0013 (peer protocol)