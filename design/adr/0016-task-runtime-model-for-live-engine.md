# ADR-0016: Task/Runtime Model for the Live Engine

## Status

Accepted

## Context

The live torrent engine must integrate with the daemon so that adding a torrent
starts real lifecycle activity, pause/resume/remove/recheck/reannounce affect
real peer/tracker tasks, network health changes move torrents to
`network_blocked`, and API/UI summaries reflect real progress, speeds, peers,
and tracker status. The engine must not block the control plane and must be
cancellable.

## Decision

Adopt a per-torrent task model in `swarmotterd`:

- `TorrentEngine` (`swarmotterd::engine`) runs as an owned `tokio::spawn` task
  per active torrent. It owns its `StorageIo`, peer id, the `NetworkBinder`,
  and an `Arc<Mutex<EngineState>>` shared with the daemon.
- The daemon keeps three maps keyed by info hash: live `EngineState` (reconciled
  into `Torrent` records on every API read and on the network health loop),
  engine command senders, and task join handles.
- Lifecycle actions drive the engine: add starts the engine (when containment
  allows); pause stops the engine; resume restarts it; remove stops the engine
  and optionally deletes data; recheck stops the engine, runs `StorageIo::recheck`,
  and reflects the result; reannounce sends an `EngineCommand` or restarts the
  engine. Strict fail-closed mode stops all engines and marks torrents
  `network_blocked` while the control plane stays up.
- `EngineState` (pieces have, byte counts, discovered peers, tracker status,
  finished flag) is the single source of live progress; the daemon's
  `reconcile_engine_progress` copies it into `Torrent` records before building
  summaries, so the existing API/UI surface shows real state without schema
  changes.
- Concurrency is bounded (bounded peer cap, bounded channels); tasks are
  awaited on stop to avoid leaked work.

## Consequences

- The API/UI surface is unchanged but now reports real progress/peers/trackers.
- Pause/resume/remove/recheck/reannounce are real, not stubs.
- Network containment changes have immediate effect on active torrents.
- A crash or panic in one engine task is isolated and recorded as a torrent
  error state.
- Endgame mode, per-torrent bandwidth shaping, and inbound peer listening are
  tracked as remaining v1.0.0 work in `docs/v1-completion-tracker.md`.

## Related Documents

- `crates/swarmotterd/src/engine.rs`
- `crates/swarmotterd/src/daemon.rs`
- ADR-0012 (network binder)
- ADR-0013 (peer protocol)
- ADR-0017 (local swarm testing)