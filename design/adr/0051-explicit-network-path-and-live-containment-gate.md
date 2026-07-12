# ADR-0051: Explicit Network Path and Live Containment Gate

## Status

Accepted

## Context

`NetworkConfig::default()` selected disabled containment while Serde's partial
`[network]` default selected strict mode. Starting without a configuration file
therefore enabled the torrent data plane without an explicit operator choice,
and getting-started documentation recommended disabled mode. The live health
loop constructed its own OS probe, was not deterministically testable, and used
normal shutdown paths after a required interface disappeared. Existing UDP
sockets did not re-check a shared containment gate on send. Public statuses
`socket_bind_failed` and `blocked_fail_closed` were not reached by production
status transitions.

This violated the fail-closed claim in ADR-0005, ADR-0012, and ADR-0047: an
omitted path could enable torrent traffic, and active traffic did not reliably
terminate when the required path failed.

## Decision

### Default posture

Make `NetworkConfig::default().mode` `strict`, matching the Serde default. An
omitted `[network]` table produces strict mode without a path and fails
`Config::validate()` with `invalid_config` before the control listener or any
background task starts. `--check-config` fails the same way. Full config
validation runs before logging initialization and before the `--check-config`
success message. A run without `--config` fails unless env overrides provide a
valid strict path or explicit disabled mode.

Disabled containment remains available only through the explicit setting:

```toml
[network]
mode = "disabled"
```

This mode is for development or a separately enforced boundary such as the
supplied Gluetun shared-network-namespace deployment. Never infer disabled mode
from a missing file/table, platform, bind failure, or unavailable interface.
Never auto-change strict to preferred or disabled.

### Live containment gate

Introduce one process-wide `ContainmentGate`, owned by `DaemonRuntime` and
shared by every binder, DHT runner, listener, engine, seeder, tracker, webseed,
and metadata task. It uses atomics plus `tokio::sync::Notify`, not a new
dependency. It provides:

- `allow()`: permit traffic and advance generation only on blocked-to-allowed.
- `block(status, detail)`: deny operations, store status/detail, advance the
  generation, and notify waiters.
- `enforce()`: return `CoreError::NetworkBlocked` when denied.
- `cancelled_since(generation)`: complete when generation changes to blocked.

Every bind, connect, resolve, accept-loop iteration, UDP send, tracker request,
webseed request, and DHT send calls `enforce()`. Each top-level data-plane task
selects normal work against `cancelled_since(start_generation)` so connected
TCP/TLS streams drop on block. The control listener never uses this gate.

### Injected interface probe

Store one injected `Arc<dyn InterfaceProbe + Send + Sync>` on `DaemonRuntime`.
Production injects `OsInterfaceProbe`; tests inject a mutable fake. Extract one
`pub(crate) async fn network_health_tick()` from the loop. The loop only invokes
that operation; tests call it directly without sleeping.

### Transition order

On healthy-to-unhealthy transition, while holding the data-plane transition
lock, perform this exact order:

1. Block the gate immediately.
2. Stop the inbound listener and shared DHT runner.
3. Abort/drop all downloader, metadata, tracker, webseed, and seeder task
   handles; do not await graceful protocol shutdown before dropping sockets.
4. Reconcile byte counters and verified progress already reported by tasks.
5. Set previously active torrents to `network_blocked`, retain prior activity
   for recovery, persist, and publish torrent plus network-status events.

On recovery, update health, allow the gate, reconstruct listener/DHT, and
requeue only work active before the block. Paused and automatically seed-stopped
torrents remain stopped.

### Bind-failure health reporting

Route bind/listen/source-bind failures through a runtime health-report channel.
Such a report blocks the gate and exposes `socket_bind_failed`. Use
`blocked_fail_closed` only when strict policy denies traffic and no more
specific interface/address/namespace/route/DNS/bind status applies. Both statuses
require production-path API tests.

## Consequences

No implicit configuration permits torrent traffic. Every transport observes one
live gate. Local traffic stops on injected and real Linux path loss. The control
plane remains available. Statuses are reachable. Docs agree. This is a breaking
configuration change: existing users who relied on the disabled default must
configure a strict path or explicitly acknowledge disabled mode. Phase 9
performs the version bump.

## Related Documents

- Supersedes earlier text permitting an omitted network table to select
  disabled containment.
- [ADR-0005](0005-strict-network-containment-fail-closed.md)
- [ADR-0012](0012-centralized-network-binder.md)
- [ADR-0047](0047-transactional-live-data-plane-reconfiguration.md)
- `design/vpn-network-containment.md`
- `design/configuration.md`, `docs/configuration.md`
- `design/testing.md`
- `CHANGELOG.md`