# ADR-0071: Shared peer worker limit distribution

## Status

Accepted

## Context

`DaemonRuntime::apply_peer_worker_limits` previously sent an
`UpdatePeerWorkerLimit` command to every engine channel sequentially on each
queue reconciliation and configuration change. With a large active library
this serialized maintenance behind per-engine channel sends, delaying queue
starts and lifecycle operations. Only the daemon-wide default limit changes
in the common case; per-torrent overrides are rare and already command-driven.

## Decision

Engines read the daemon-wide peer worker limit from a shared
`Arc<SharedPeerWorkerLimit>` — an atomic default paired with a generation
counter. The daemon bumps the default and the generation in one store;
engines compare generations on their existing maintenance paths and adopt the
new default without a command. Per-torrent overrides continue to flow through
`UpdatePeerWorkerLimit` commands, which pin an engine's effective limit until
cleared.

## Consequences

- Queue reconciliation and configuration replacement no longer scale linearly
  with the number of running engines.
- Engine adoption of a new default is eventual (next maintenance tick), which
  is acceptable because the limit gates worker starts, not live sessions.
- The generation counter must advance on every default change so engines that
  miss a tick still converge.
- Command-driven overrides remain authoritative over the shared default.

## Related Documents

- `design/scaling-review-fix-ledger-2026-10-03.md` (W1)
- `design/architecture.md`
