# ADR-0090: Durable master traffic switch

## Status

Accepted

## Context

Operators need one control that immediately stops all torrent download and
upload activity without rewriting every torrent to a manual pause. Per-torrent
pause is not a substitute: it changes each record's `pause_reason`, loses the
distinction between an operator pause and a temporary global stop, and requires
a bulk loop that can partially fail.

The switch must also be safe across restart. A daemon that resumes transfer
after a crash or upgrade would violate the expectation that an explicit global
stop remains in force until it is cleared. Finally, the switch must outrank
queue admission, start-now bypass, seeding policy, and containment recovery;
otherwise a recovery path can reintroduce traffic while the switch is active.

## Decision

The daemon owns a single master traffic-switch flag stored in the durable
`QueueState` generation. `GET /api/v1/session` reports it and
`PUT /api/v1/session` sets it. The Web UI header and `swarmotterctl
pause-all`/`resume-all` use the same endpoint.

Activation is serialized by the data-plane transition lock. The daemon first
sets the flag, captures final verified progress, force-stops every downloader,
metadata fetch, seeder, explicit transfer task, DHT runner, and shared inbound
peer listener, normalizes affected records to queued/completed, and persists
the queue generation before releasing the lock. Deactivation persists the
cleared flag before queue and seeding reconciliation run.

Downloader admission (`desired_download_hashes`) and seeder admission
(`reconcile_seeders`, `eligible_seeder_hashes`, and both start paths) read the
same flag. Start-now bypass and containment recovery therefore cannot start
work while it is set. The switch does not change per-torrent `pause_reason` or
`paused` state, so manually paused torrents stay manually paused after resume.

`GET /api/v1/stats` exposes `session_paused` as additive telemetry and the
daemon publishes `session_changed` plus `stats_updated` events. Individual
torrent state changes are published through the existing `torrent_changed`
events.

## Consequences

- A global stop is one durable transaction instead of a partial bulk pause.
- Restart and upgrade stay paused until an operator explicitly clears the
  switch.
- Manually paused torrents and their recorded attribution are preserved.
- The switch intentionally stops DHT and the inbound listener as well as peer
  transfers, so all data-plane traffic is covered rather than only active
  downloads.
- Queue state now carries one additional backward-compatible JSON field. Older
  state files restore as unpaused, and the existing SQLite schema needs no
  migration because the queue is stored as one versioned JSON document.
- Callers that relied on start-now always overriding every stop condition must
  clear the master switch first.

## Related Documents

- `design/requirements.md` (Queueing)
- `design/architecture.md` (request flow and queue locking)
- `design/api.md` and `docs/api.md` (`/session`, `stats.session_paused`)
- ADR-0042 (scheduler diagnostics)
- ADR-0077 (registry/queue lock order)
