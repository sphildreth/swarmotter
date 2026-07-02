# ADR-0014: Tracker Implementation Strategy

## Status

Accepted

## Context

SwarmOtter must announce to HTTP, HTTPS, and UDP trackers, parse compact
peer responses, respect tracker tiers and private-torrent restrictions, and
surface tracker status through the API/UI. Tracker traffic is torrent
data-plane traffic and must route through the network containment layer.

## Decision

Implement tracker announce in `swarmotter-core::tracker`:

- `AnnounceRequest` builds the announce URL with percent-encoded info hash and
  peer id, `port`, `uploaded`, `downloaded`, `left`, compact mode, `event`
  (`started`/`stopped`/`completed`/empty), and optional `numwant`.
- `parse_announce_response` decodes bencoded tracker responses, including the
  `failure reason`, `interval`, `min interval`, complete/incomplete counts,
  `tracker id`, compact IPv4 (BEP 23, 6 bytes) and compact IPv6 (BEP 24,
  18 bytes) peer lists, and non-compact dict peers.
- `announce_tiers` preserves tier order from `announce`/`announce-list`.
- `http_announce` issues the announce through the `NetworkBinder` so the
  request never bypasses containment.

The engine announces `started` on startup, `empty` periodically to refresh
peers, and `completed` when the download finishes. Private torrents are
honored by restricting peer discovery to trackers (DHT/PEX are disabled for
private torrents — modeled here by relying solely on `announce_tiers`).

UDP trackers are implemented in `swarmotter-core::udp_tracker` (BEP 15):
connect request/response handshake to obtain a connection id, announce
request/response with compact IPv4 peer parsing, transaction-id matching,
error response handling, and a bounded retry loop. All UDP traffic goes
through the binder's `udp_socket()` contained UDP socket; no UDP socket is
created directly. The engine's `announce()` dispatches by scheme: `udp://`
URLs use `udp_tracker::udp_announce`, `http://`/`https://` URLs use
`http_announce`. HTTPS TLS over the contained socket remains future work.

## Consequences

- Tracker announce URL construction and compact peer parsing are unit-tested
  without sockets.
- All tracker HTTP and UDP traffic is containment-gated.
- UDP trackers use the binder `udp_socket()` method, not a bypass.
- HTTPS trackers will reuse the contained socket path with TLS (future work).

## Related Documents

- `crates/swarmotter-core/src/tracker.rs`
- `crates/swarmotter-core/src/udp_tracker.rs`
- ADR-0012 (network binder)
- ADR-0013 (peer protocol)