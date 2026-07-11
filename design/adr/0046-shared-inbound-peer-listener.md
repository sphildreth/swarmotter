# ADR-0046: Shared Inbound Peer Listener

## Status

Accepted

## Context

Starting one inbound TCP listener for every seeding torrent cannot work when
all torrents use the same configured peer port. It also makes accepted peer
session ownership and shutdown difficult to bound: a listener task can stop
while detached sessions continue using an obsolete network binder or serving
storage.

## Decision

Use one process-wide `SeederHub` for the configured inbound TCP peer port.

- The listener is created only through the current `NetworkBinder` and is
  shared by all registered torrents.
- Plaintext handshakes route by info hash. MSE/PE handshakes route by the
  registered stream key before completing encrypted peer negotiation.
- Each torrent registration contains its metadata, storage root, cancellation
  signal, and upload accounting state.
- Registration is not announced to trackers until the hub acknowledges that
  the contained listener successfully bound the configured port. Bind failure
  removes the pending registration so the daemon never advertises an
  unreachable inbound endpoint.
- Accepted sessions are owned by the hub in a `JoinSet`, subject to a bounded
  concurrent-session limit, and are aborted and awaited when the hub stops.
- Listener health is checked against the binder. A failed containment path or
  data-plane reconfiguration stops the listener and every accepted session;
  no session may retain an obsolete binder policy.

Standalone seeder construction remains available to focused tests, but daemon
operation uses the shared hub.

## Consequences

- Multiple torrents can seed concurrently on one advertised port.
- Inbound session concurrency and cancellation have one explicit owner.
- Adding or removing a completed torrent updates a registry instead of racing
  to bind or release the listen port.
- The routing registry must remain synchronized with torrent lifecycle and
  storage moves.

## Related Documents

- `../architecture.md`
- `../vpn-network-containment.md`
- `../testing.md`
- `../../crates/swarmotterd/src/seeder.rs`
- ADR-0012 (centralized network binder)
- ADR-0013 (peer-wire protocol architecture)
- ADR-0016 (task runtime model)
