# ADR-0078: Supervised shutdown and bounded logging

## Status

Accepted

## Context

An alive HTTP listener and healthy VPN do not establish daemon progress.
Detached background workers can fail silently, full engine command channels
can block lifecycle operations, and open event streams can prevent HTTP drain
from reaching the final checkpoint. Synchronous, unbounded log output can also
stall runtime threads or exhaust local storage.

## Decision

Retain and supervise the watch, network-health, mapping, and autopilot tasks.
Workers finish their current transaction on shutdown and stop starting new
iterations. Each reports progress; mapping lease sleeps report progress without
renewing the lease early. An independent OS watchdog checks completed progress
and the async supervisor heartbeat. A worker stalled for 300 seconds initiates
shutdown. The supervisor also probes registry/queue access with a 500 ms bound;
30 consecutive failed probes initiate shutdown. A transient failed probe only
marks liveness unhealthy. Normal VPN loss does not fail application liveness.

Expose public, data-free `/live` (200 or 503), separate from the existing
network-oriented `/health`. Packaged Docker health checks use `/live`. An
unexpected worker/server exit, panic, or sustained liveness failure initiates
shutdown and exits nonzero, allowing the existing restart policy to recover.

Shutdown permanently closes the process containment gate, closes SSE and
WebSocket streams (including blocked WebSocket sends), waits for in-flight
background work, stops torrent tasks, and checkpoints state. Concurrent health
recovery cannot reopen the terminal gate. HTTP drain and cleanup share a
30-second bound; the independent watchdog enforces a 35-second final process
exit if the executor cannot make progress. Failed/incomplete cleanup exits
nonzero, retaining the last SQLite committed generation. Compose grants 45
seconds before forced termination. Explicit recheck completion still must
precede releasing storage ownership; timeout never authorizes a data move.

Engine command-map guards end before channel waits. Automatic commands use
nonblocking enqueue and retry on later decisions; explicit reannounce reports
queue-full/closed errors. Engine and seeder graceful joins are bounded at five
seconds before task abort, preserving existing storage cancellation signals.

Move stderr and optional file output to a dedicated worker with a 1024-record
queue and 64 KiB record limit. Full queues and oversized records drop whole
records; counters expose drops and output errors through authenticated doctor
diagnostics. Files rotate at 10 MiB with five archives. Shutdown requests a
bounded flush. Docker independently retains three 10 MiB logs per service.
Example configurations reserve at least 1 GiB and 1% free download space;
existing explicit configuration values remain compatible.

## Consequences

- Operational hangs recover without a healthy network endpoint masking them.
- Graceful exit includes a final checkpoint while idle browser event clients
  are disconnected automatically.
- Severe stalls can lose progress since the last successful checkpoint, but
  cannot silently resume traffic through a default route.
- Very long uninterrupted background transactions exceed the watchdog budget;
  operators should inspect persistence timings and storage latency before
  increasing workload.
- Logging cannot block runtime threads, but overload can drop diagnostics.
  Bounded files and explicit counters make that trade-off visible.
- No new dependencies, torrent protocols, or durable storage formats are added.

## Related Documents

- [Architecture](../architecture.md)
- [Configuration](../configuration.md)
- [Testing](../testing.md)
- [Network containment](../vpn-network-containment.md)
- [ADR-0077](0077-registry-queue-lock-order.md)
