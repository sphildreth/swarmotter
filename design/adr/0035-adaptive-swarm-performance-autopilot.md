# ADR-0035: Adaptive swarm performance autopilot

## Status

Accepted

## Context

SwarmOtter’s P0 roadmap includes an adaptive swarm performance autopilot for
throughput diagnosis and automatic tuning. Operators currently have visibility into
health and throughput but limited structured explanation of why a torrent is
underperforming and limited automated mitigation for stalled peer choices,
queue starvation, or transient bandwidth mismatch.

Implementation direction from the feature backlog requires:

- Per-torrent performance observation across useful peer contribution, stale peer
  behavior, tracker/PEX/DHT freshness, disk pressure, and containment state.
- Optional automatic adjustment of global transfer limits while honoring hard caps.
- Peer/tracker scoring to affect retry and connection priority.
- Queue mitigation for stalled torrents blocking active transfer slots.
- A decision and rationale trail that can be explained per torrent.
- Global and per-torrent controls to disable automatic behavior.

The decision is to document this phase as a control-plane behavior contract before
hardening production implementation details.

## Decision

Adopt an adaptive autopilot model with three parts:

1. Observability contract: expose performance signals and reasons through
   existing API surfaces (`/api/v1/torrents/:hash/stats`, `/api/v1/settings`,
   `/api/v1/network/health`) and the Web UI detail/summary views.
2. Control contract: treat autopilot as a runtime automation layer controlled
   by global `disabled` / `observe` / `act` modes plus per-torrent mode
   overrides. Automatic actions remain bounded by configured hard caps from
   `[bandwidth]`, `[queue]`, and network containment health.
   The default global mode is `act` so stalled active torrents can release
   queue slots or refresh discovery without requiring an explicit operator
   change. Operators that want diagnostics-only behavior can select `observe`,
   and operators that want no autopilot analysis can select `disabled`.
   When an active torrent has no recent block progress, queue-slot release is
   prioritized over additional discovery or peer-worker tuning so queued work
   can proceed instead of waiting behind a stalled active set.
3. Containment contract: use only the existing contained torrent data-plane signals
   and network diagnostics in all measurements and never issue uncontained probes.

The Web UI must avoid presenting uncontained probe results as network health
or tuning rationale. Action logs and reasons from both API and UI are mandatory
for operators reviewing automation outcomes.

## Consequences

- Operators gain a consistent explanation surface for poor swarm progress via health,
  peer/transport/queue signals, and action history.
- The daemon can evolve from static limits toward dynamic decisions without changing
  the primary control-plane path model.
- Default operation applies bounded queue/discovery/peer-worker mitigation for
  active torrents that meet deterministic slow/stalled criteria. This improves
  large-library throughput behavior, but operators who prefer manual-only
  tuning must set `[autopilot].mode = "observe"` or `"disabled"`.
- Autopilot must preserve no-download streak state across rate reconciliation
  so torrents that never receive a first useful block can still age into the
  queue-release path.
- Autopilot behavior is constrained by existing fail-closed containment and does
  not bypass `[network]` policy or data-plane enforcement.
- New decision and disablement states must be surfaced in configuration and UI
  docs, and future implementation must keep these contracts stable.

## Related Documents

- [Backlog feature: Adaptive Swarm Performance Autopilot](../BACKLOG.md)
- [Configuration reference](../../docs/configuration.md)
- [Web UI guide](../../docs/web-ui.md)
