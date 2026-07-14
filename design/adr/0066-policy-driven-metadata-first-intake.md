# ADR-0066: Policy-Driven Metadata-First Intake

## Status

Accepted

## Context

Named policy profiles already resolve storage, queue, start behavior, seeding,
bandwidth, and encryption settings. They did not provide a durable way to
decide file exclusions, tracker selection, and output organization before a
magnet began payload transfer. As a result, an operator could not inspect a
magnet's file tree, apply a repeatable policy, and make an explicit start
decision with an explainable record of what the daemon selected.

The metadata phase is still torrent data-plane activity. It may use BEP 9,
trackers, DHT, PEX, or directly supplied peers, so it remains subject to the
central binder and strict fail-closed containment rules.

## Decision

- Extend named profiles with an intake policy containing validated glob,
  suffix, path-segment, minimum-size, and maximum-size file exclusions;
  organization and incomplete subdirectories; force-top-level-folder behavior;
  and an active-only partial filename suffix. Resolution uses the existing
  deterministic add/profile/label precedence rules.
- Add ordered tracker-host rules with case-insensitive glob matching,
  enablement, and priority. These are deliberately live runtime policy, while
  reviewed file-selection and storage decisions are immutable intake
  snapshots.
- Add metadata-first preview intake. A preview addition may acquire and verify
  metainfo, expose the file tree and effective policy, and persist the
  resulting selection without starting payload transfer.
- Persist an intake snapshot on the torrent record: resolved profile, exclusion
  rules, organization and incomplete-path decisions, force-folder/suffix
  decisions, requested unwanted files, and the preview-until-start state.
  Missing fields in legacy state deserialize to safe defaults.
- Apply file exclusions and path organization before the torrent is scheduled.
  Normalized paths must remain under the resolved storage root; invalid,
  absolute, or traversal-like organization values are rejected rather than
  silently normalized into a different destination.
- Preserve `BEP 53` `so=` selection as a bounded, sorted local allowlist.
  It can only reduce selection and never re-enables an API or profile exclusion.
  Validate its indices after metadata arrives and before payload work begins.
- Accept magnet `x.pe` hints only as bounded IPv4 or bracketed-IPv6 literal
  endpoints with nonzero ports. Do not resolve hostnames; feed accepted hints
  through the ordinary peer filter, permit budget, and contained binder.
- Start of payload transfer is an explicit lifecycle operation. It clears the
  preview gate only after the persistent transition succeeds, then uses the
  normal queue and containment paths.
- Surface the effective intake policy and snapshot through the native API and
  Web UI, including a bounded storage-path preview, so the operator can
  distinguish live profile inheritance from a create-time decision before
  applying a move or organization choice.
- Completion behavior remains bounded and non-destructive: the existing
  persisted seed-forever, ratio-limit, and idle-limit policy determines whether
  seeding continues or stops. This decision does not introduce automatic
  payload deletion, hooks, or external commands.
- Retain exact original `.torrent` bytes when supplied by local file/watch
  intake and expose only that retained representation through the authenticated
  metadata export endpoint. Magnet/BEP 9 data is not synthesized into an
  original upload.

## Consequences

- Operators can add magnets safely for review, use reusable file/tracker and
  organization policy, inspect the exact resulting paths, and start selected
  content deliberately.
- Profile edits do not silently rewrite the decision already captured for a
  registered torrent; an explicit policy or file-selection operation is needed
  to change it.
- Existing file-selection scheduling remains the authority for piece-level
  transfer behavior, including boundary-piece correctness.
- Metadata acquisition and any later payload transfer remain fail closed; a
  preview mode, BEP 53 selector, or `x.pe` hint is not permission to use an
  unconstrained route, DNS lookup, or a separate HTTP client.

## Related Documents

- [Feature backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Network containment](../vpn-network-containment.md)
- [Testing strategy](../testing.md)
- ADR-0048 (file selection drives piece scheduling)
- ADR-0050 (bounded untrusted metainfo parsing)
- ADR-0057 (policy profiles and inherited torrent settings)
