# ADR-0029: Paused Torrent Add API

## Status

Accepted

## Context

API clients need to add a torrent record to SwarmOtter's queue without starting
its data-plane activity immediately. The Transmission compatibility adapter
already accepted a `paused` add flag, but implemented it as add-then-pause,
which could race with queue reconciliation when `queue.auto_start` was enabled.

The native API also needs a way to express the same behavior for both JSON
magnet adds and raw `.torrent` uploads, where the request body is torrent bytes
and cannot also carry JSON options.

## Decision

The native `/api/v1` torrent add surface accepts paused add options:

- JSON magnet add requests may include `paused: true` or
  `start_behavior: "paused"`.
- Raw `.torrent` upload requests may include `?paused=true` or
  `?start_behavior=paused`.
- If both `paused` and `start_behavior` are provided, they must agree.

The API passes an explicit `AddTorrentOptions` struct to the daemon. The daemon
always inserts the new torrent into queue order, but when the add request is
paused and network containment is otherwise healthy, it registers the torrent
with state `paused` and does not schedule immediate queue reconciliation. Strict
fail-closed network blocking still takes precedence and may register the torrent
as `network_blocked`.

The Transmission RPC compatibility adapter delegates its `paused` add flag to
the same add-time option instead of adding and then pausing.

## Consequences

Clients can stage torrents in the queue without a transient startup window.
The native API and Transmission compatibility adapter share the same daemon
behavior. Raw upload clients have a query-string control path while JSON clients
can keep options in the request body.

Future add-time options should extend `AddTorrentOptions` rather than adding
more ad hoc daemon trait parameters.

## Related Documents

- [API docs](../../docs/api.md)
- [API design notes](../api.md)
- [Requirements](../requirements.md)
