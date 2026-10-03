# ADR-0070: Discovery-aware empty-swarm recovery

## Status

Accepted

## Context

Thin successful tracker announces can report a populated swarm while returning
only an unusable peer, or no peers at all. Counting cadence-skipped discovery
refreshes as empty-swarm evidence stopped download engines before DHT could
provide alternatives. Under concurrent torrent discovery, tracker timeouts and
connection failures were also classified as terminal tracker errors, leaving
torrents stopped until a manual lifecycle action.

Recovery and the native `tracker_error` lifecycle must distinguish discovery
that has not finished, a temporarily unreachable source, and an explicit
tracker rejection. Genuinely empty torrents must retain a bounded engine exit.

## Decision

- Retain whether the last successful announce reported any seeders or leechers,
  independently of returned/admitted peers and subsequent announce failures.
  This population signal prevents empty-swarm give-up until a later successful
  announce reports no population.
- Track DHT lookup completion separately from lookup initiation. For a
  non-private torrent with a DHT runner, the no-peer path forces one bounded
  lookup after known candidates become unusable, even if normal cadence would
  suppress it. Success, empty results, errors, and timeouts all complete an
  attempt. Discovery remains on the central contained network path.
- Replace the fatal no-peer round counter with a bounded empty-swarm grace
  period. It begins only after configured trackers have had a real announce
  attempt and enabled DHT has completed a lookup for the unusable-peer episode,
  with no tracker population signal. Initial completed discovery can provide
  this evidence; a skipped refresh supplies none. Usable candidates or a
  population signal reset the grace period. Trackerless torrents with DHT
  disabled have no discovery work to await and still exit boundedly.
- Record protocol-level tracker rejections separately from transport errors,
  including BEP 15 error replies during either connect or announce.
  `tracker_error` requires explicit rejection by every attempted tracker, no
  successful announce during the engine run, and no usable DHT, PEX, direct
  peer, or webseed signal. Recent scrape/transport failure counters are not
  proof of rejection. Timeouts, connection, I/O, and protocol errors use the
  incomplete-engine retry queue after discovery is exhausted.
- Preserve the last explicit failure in the native summary. Manual Reannounce,
  Resume, and Start Now clear terminal errors and launch another attempt.
  Tracker intervals, queue configuration, and containment remain authoritative.

## Consequences

- Sparse tracker peer lists and discovery contention no longer prematurely
  stop populated downloads or strand torrents in `tracker_error`.
- An unusable-peer episode may trigger an additional contained DHT lookup;
  ordinary discovery cadence resumes once that lookup completes.
- Truly empty engines still release their tasks through the existing retry
  lifecycle. Explicit tracker rejections remain actionable terminal errors.
- Generated local fixtures must cover population retention, forced/completed
  DHT discovery, transient-error retry, bounded empty exits, and manual recovery.
- No dependency, persistent format, configuration, or network path changes are
  required.

## Related Documents

- [Tracker and discovery requirements](../requirements.md)
- [Native API contract](../api.md)
- [Product lifecycle requirements](../PRD.md)
- [Testing acceptance criteria](../testing.md)
- [v1 traceability](../v1-traceability.md)
- [Network containment](../vpn-network-containment.md)
- [Engine discovery](../../crates/swarmotterd/src/engine/discovery.rs)
- [Engine lifecycle](../../crates/swarmotterd/src/engine/download.rs)
- [Daemon scheduler](../../crates/swarmotterd/src/daemon/scheduler.rs)
