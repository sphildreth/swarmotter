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
- `block(status, detail)`: deny operations, store status/detail, always advance
  the generation (including when already blocked), and notify waiters.
- `enforce()`: return `CoreError::NetworkBlocked` when denied.
- `cancelled_since(generation)`: register a wakeup-safe waiter and complete when
  the generation differs, regardless of the gate's state at observation time.

Every bind, connect, resolve, accept-loop iteration, UDP send, tracker request,
webseed request, and DHT send calls `enforce()`. Each top-level data-plane task
selects normal work against `cancelled_since(start_generation)` so connected
TCP/TLS streams drop on block. Registering the notification before checking the
generation closes the lost-wakeup window. A block followed by allow before an
old task polls still changes generation and cancels that task; no stream from an
old generation may bridge a blocked interval. The control listener never uses
this gate.

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
5. Set previously active torrents to `network_blocked`, attach a durable typed
   recovery intent (`downloading`, `downloading_metadata`, or `seeding`),
   persist, and publish torrent plus network-status events.

On recovery, update health, allow the gate, reconstruct listener/DHT, and
consume recovery intent only for work demonstrably live before the block.
Paused, merely queued, ratio/idle-stopped, completed-without-a-live-seeder, and
stale `network_blocked` torrents have no intent and remain stopped. Persisting
the intent makes the same rule survive daemon reconstruction; consuming it once
prevents repeated recovery from restarting unrelated work.

### Bind-failure health reporting

Route bind/listen/source-bind failures through a runtime health-report channel.
The binder blocks the gate synchronously before queueing the report, so no new
operation can race the next health tick. Such a report exposes
`socket_bind_failed`. Use `blocked_fail_closed` only when strict policy denies
traffic and no more specific interface/address/namespace/route/DNS/bind status
applies.

Both statuses are latched. Periodic probe health, a lifecycle command, and a
partial settings patch cannot reopen the gate. Only an explicit full
configuration replacement may attempt recovery. Before clearing the latch it
constructs a binder from the replacement and successfully opens then drops the
configured peer listener and, outside SOCKS5 TCP-only mode, an ephemeral
contained UDP socket. A validation or persistence failure preserves the old
configuration, status, and blocked gate.
Production-path API tests cover immediate block, latching across a healthy
probe, failed repair, successful replacement, and the generic denial status.

### Privileged namespace acceptance

The Linux CI harness builds as the normal runner user and creates two
PID-qualified namespaces connected only by a veth, with no default route. It
generates a lawful payload/torrent and runs a local compact HTTP tracker plus a
throttled TCP BitTorrent seed. After real tracker discovery and partial verified
peer-wire progress, deleting the daemon veth must yield `interface_missing`, a
`network_blocked` torrent, stable bytes, empty data-plane scheduler diagnostics,
and a responsive control API.

The script invokes sudo only as `sudo ip` for namespace/link operations.
Commands entered through `ip netns exec` immediately drop to the caller UID/GID.
Tracker, seed, generator, and API clients have no capabilities. SwarmOtter has a
bounding/effective/ambient set containing only `CAP_NET_RAW`, the capability
required for `SO_BINDTODEVICE`; the harness verifies these sets before traffic.

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
- [ADR-0012](0012-network-binder-centralized-containment.md)
- [ADR-0047](0047-transactional-live-data-plane-reconfiguration.md)
- `design/vpn-network-containment.md`
- `design/configuration.md`, `docs/configuration.md`
- `design/testing.md`
- `CHANGELOG.md`
