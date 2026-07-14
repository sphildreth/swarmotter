# ADR-0052: Persisted Per-Torrent Seeding Policy and Runtime Lifecycle

## Status

Accepted

## Context

Global ratio and idle calculations existed, but a torrent did not durably own
its overrides and the public `completed`/`seeding` states could diverge from
the live inbound seeder registry. Per-file progress was also derived from a
torrent-wide piece fraction, which credited incorrect bytes at file and final-
piece boundaries. Finally, downloader and seeder startup constructed separate
per-torrent rate-limit objects, so an upload-limit change could not reliably
shape an already-active seeder.

These are one lifecycle decision: policy, durable state, task ownership,
accounting, and live shaping must describe the same torrent at every boundary.

## Decision

- Persist `TorrentSeeding` and `SeedingStatus` on every torrent. Both are
  defaulted additions to the version-1 daemon state from ADR-0045; their
  addition alone does not bump the state version. Legacy records inherit
  global targets and have their status recomputed before scheduling.
- Keep `TorrentState` as the coarse lifecycle and use `SeedingStatus` for the
  exact states `not_eligible`, `queued`, `active`, `stopped_ratio`,
  `stopped_idle`, and `stopped_manual`.
- A live `SeedRegistry` registration is authoritative for active seeding.
  Registry snapshots and `seeding`/`active` state normalization use one
  lifecycle lock. `active_seeds` is counted from that registry, not from enum
  values.
- Only fully verified content is eligible. Complete content waiting for a slot
  is `completed` + `queued`; successful registration is `seeding` + `active`;
  automatic targets return it to `completed` with the matching stopped status;
  and an operator pause is `paused` + `stopped_manual`.
- Fail-closed containment preserves the fine-grained status while the coarse
  state is `network_blocked`. Recovery consumes only the durable recovery
  intent, re-evaluates policy, and reconstructs a seeder only when eligible.
- Compute torrent and file completed bytes exclusively from verified piece
  byte ranges. A boundary piece credits only its intersection with each file,
  and the final piece credits only its actual length.
- `PUT /api/v1/torrents/:info_hash/seeding` is a strict replacement operation.
  All three fields are required; nullable limits mean inheritance. The daemon
  persists the replacement before applying any live task transition. If
  persistence fails, it restores the prior policy while the coarse state,
  fine-grained status, registration, and accepting task remain unchanged.
- Store one `Arc<RateLimiter>` per retained torrent. Downloader and seeder
  receive that same Arc; it survives completion, queued slots, pause/resume,
  and containment. Removal/reset are the only normal deletion boundaries.
  Global shaping remains an additional limiter layer.
- Compatibility adapters retain their documented field surfaces. Existing
  ratio and uploaded counters consume the truthful native summary; no new
  Transmission or qBittorrent policy options are implied.

## Consequences

- API, Web UI, persisted state, statistics, and live tasks now agree about
  whether a torrent is eligible, queued, active, or automatically/manually
  stopped.
- Policy edits survive restart and can stop or requeue a completed torrent
  immediately without overriding an operator pause.
- Exact file progress costs a bounded pass over each file's intersecting piece
  indexes at reconciliation points rather than a constant-time fraction.
- A per-torrent upload-limit update changes an active local upload without
  replacing its seeder registration or limiter identity.
- State readers must continue defaulting new fields and validating finite,
  non-negative ratio targets before lifecycle evaluation.

## Related Documents

- [ADR-0045: Versioned Durable Daemon State](0045-versioned-durable-daemon-state.md)
- [ADR-0046: Shared Inbound Peer Listener](0046-shared-inbound-peer-listener.md)
- [Requirements](../requirements.md)
- [Architecture](../architecture.md)
- [API](../api.md)
- [Testing](../testing.md)
- [Phase review](../2026-07-12.REVIEW.md)
