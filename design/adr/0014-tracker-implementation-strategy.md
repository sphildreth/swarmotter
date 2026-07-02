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

UDP trackers are modeled (`TrackerKind::Udp`) but the live UDP announce
engine is not part of this slice; it is tracked as remaining v1.0.0 work in
`docs/v1-completion-tracker.md`. When added, it will use a binder UDP method
and the same compact-peer parsing.

## Consequences

- Tracker announce URL construction and compact peer parsing are unit-tested
  without sockets.
- All tracker HTTP traffic is containment-gated.
- Adding UDP trackers requires a binder UDP method, not a bypass.
- HTTPS trackers reuse the same `http_get` binder path (TLS over the contained
  socket is future work; HTTP is the initial path).

## Related Documents

- `crates/swarmotter-core/src/tracker.rs`
- ADR-0012 (network binder)
- ADR-0013 (peer protocol)