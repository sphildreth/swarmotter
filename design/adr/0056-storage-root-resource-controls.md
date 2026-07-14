# ADR-0056: Storage-Root Resource Controls

## Status

Accepted

## Context

ADR-0037 established storage diagnostics and free-space preflight, but did not
control concurrent active work on an individual storage root. A daemon can
have separate SSD, HDD, NAS, or constrained roots with materially different
safe levels of active payload, write pressure, and verification work. Global
queue limits cannot express those local limits.

The controls must be deterministic, observable, and safe under concurrent
starts, magnet metadata resolution, cancellation, and rechecks. They must not
create a network path or weaken the configured fail-closed torrent traffic
containment.

## Decision

- Add repeatable `[[storage.root_controls]]` entries. Each entry has a lexical
  root path plus optional `max_active_downloads`, `max_active_bytes`,
  `max_write_bytes_per_second`, and `max_concurrent_rechecks` fields. Zero is
  unlimited for each field.
- Resolve a control using the longest matching lexical root for the active
  write directory. Duplicate normalized paths are invalid; nested paths are
  intentional and deterministic.
- Reserve active-engine count and declared payload bytes atomically before an
  engine becomes visible. A reservation covers the engine lifetime, is updated
  when magnet metadata resolves, and is released on completion, cancellation,
  forced stop, or failed construction. A saturated root defers eligible work
  in the queue rather than reporting payload corruption or a permanent
  storage error.
- Share one local write limiter among active engines on a controlled root.
  The limiter delays verified local payload writes only; it is independent of
  global and per-torrent network bandwidth limiters.
- Bound full rechecks with root-scoped RAII permits so cancellation always
  releases capacity. Existing work is allowed to finish safely after a
  configuration replacement; waiters re-evaluate the latest configuration.
- Extend storage-root diagnostics with the matched control, active declared
  bytes, recheck count, limits, and saturation warnings. Storage controls are
  local daemon state only and are never a network interface, proxy, or
  containment exception.

## Consequences

- Operators can keep active downloads, writes, and rechecks within the
  capacity of each storage root while preserving global queue behavior.
- New work may remain queued despite free global download slots; diagnostics
  explain the saturated local resource boundary.
- `max_active_bytes` is an admitted declared-payload budget, not a filesystem
  free-space measurement. Free-space reserve preflight from ADR-0037 remains
  separately enforced.
- CoW-specific write strategy, automatic filesystem tuning, and state-root
  relocation remain separate decisions. No control changes data correctness,
  file verification, or torrent network containment.

## Related Documents

- [ADR-0037: Disk-Aware Storage Diagnostics and Add-Time Preflight](0037-disk-aware-storage-optimizer-preflight.md)
- [Product backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [API design](../api.md)
- [Testing strategy](../testing.md)
