# ADR-0057: Policy Profiles and Inherited Torrent Settings

## Status

Accepted

## Context

Global defaults and independent per-torrent controls make it difficult for an
operator to manage several classes of lawful distribution. The daemon already
has labels, watch-folder defaults, per-torrent bandwidth, queue, and seeding
settings, but had no coherent inherited policy model or a way to explain an
effective value.

Changing storage paths live is unsafe: it can silently relocate active or
completed payloads. Conversely, queue, seeding, and bandwidth defaults must
be able to update live for torrents that deliberately inherit them. The model
also needs stable behavior through configuration replacement and daemon-state
restore.

## Decision

- Add a top-level `[profiles]` configuration section containing named profiles
  and case-insensitive label-to-profile mappings. Profile names and label
  mappings are validated, including unknown references and ambiguous
  case-insensitive label keys.
- A profile can set storage paths, queue priority/start behavior, ratio/idle/
  seed-forever policy, and per-torrent download/upload caps. It can be chosen
  explicitly on an add request, by a watch folder, by a persisted torrent
  assignment, or by a matching label.
- Resolve assignment deterministically in this order: explicit torrent/add/
  watch profile, then the lexically normalized matching label, then global
  defaults. Explicit per-torrent overrides win for individual live fields.
  The native API returns every effective value together with the layer that
  supplied it.
- Snapshot the resolved storage paths at torrent creation, including a
  global/no-profile result, and snapshot the resolved initial start-or-paused
  decision. Reassigning or editing a profile never moves existing data,
  rewrites an explicit torrent path, or retroactively changes a queued
  torrent's admission intent; operators use the existing move-data operation
  for relocation.
- Apply inheriting queue, seeding, and bandwidth values live. Retained
  per-torrent limiters and scheduling/seeding decisions use the resolved
  policy rather than copying profile values into persistent per-torrent
  overrides.
- Use profile `start_behavior` only during initial admission. Changing a
  profile or assignment never stops already-running work or revokes a queued
  torrent's captured admission decision.
- Persist the profile attachment, its origin, resolved creation-time storage
  and admission snapshots, and explicit overrides in the torrent record.
  When profile policy is replaced, records restored from before these fields
  are atomically migrated from their preceding effective values; a failed
  replacement restores both configuration and state. Before that migration,
  old records retain their legacy global queue behavior until a label change
  or explicit assignment captures the preceding admission decision.
- This ADR does not add per-profile network paths, proxies, multi-user
  isolation, tracker-host assignment, file exclusion patterns, or completion
  action scripting. Those require separate containment and security decisions.

## Consequences

- Operators can apply safe, explainable classes of behavior without manually
  changing each torrent.
- A profile edit has intentionally different semantics by field: resolved
  storage and initial admission affect registration only; inheriting queue,
  seeding, and caps update existing torrents.
- Clients and automation can distinguish global, label, profile, snapshot,
  legacy, and explicit-torrent values rather than inferring behavior from
  flattened state.
- Profile assignments and configuration changes require validation and
  persistence before success; invalid or missing profile references do not
  silently fall back to another behavior.

## Related Documents

- [Advanced policy-profile rules backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [ADR-0052: Persisted Per-Torrent Seeding Policy and Runtime Lifecycle](0052-persisted-per-torrent-seeding-policy-and-runtime-lifecycle.md)
