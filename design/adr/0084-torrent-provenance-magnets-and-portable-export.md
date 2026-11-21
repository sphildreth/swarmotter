# ADR-0084: Torrent Provenance, Deterministic Magnets, and Portable Export

## Status

Accepted

## Context

SwarmOtter already retained exact original `.torrent` bytes for local-file and
watch-folder intake and exposed them through a single authenticated export
endpoint. Operators could not inspect the source comment, creator, creation
date, or encoding in the native details/Web UI, annotate a library item
separately from its source comment, copy a deterministic magnet, or export a
mixed batch while clearly identifying records with no retained original.

Magnet generation has protocol-sensitive details: full v1/v2/hybrid
identities, tracker tiers, webseeds, BEP 53 file selection, private-tracker
consent, escaping, and repeatable ordering. Export has a different trust
boundary: a source document may be absent, and reconstructing one from canonical
`info` bytes would create a representation the operator never supplied.

## Decision

- Parse the optional outer `encoding` field (with an information-dictionary
  fallback) alongside the existing source `comment`, `created by`, and
  `creation date` fields. Preserve all four in durable torrent state and expose
  them in native details and Web UI properties. A magnet-only information
  dictionary leaves outer fields unavailable (`null`).
- Store an operator `annotation` separately from source metadata, along with
  `annotation_updated_at` and `annotation_updated_by`. Annotation edits never
  change the source comment, identity, or retained original bytes. The SQLite
  save transaction emits a `torrent_annotation_changed` audit event with the
  update timestamp/actor, and state-change history continues to record library
  lifecycle separately.
- Generate magnets from the authoritative identity model. Hybrid magnets emit
  both full `btih` and `btmh` exact topics; pure-v2 magnets emit the full
  `btmh`; legacy v1 records fall back only to their explicit v1 key. The
  display name, webseeds, and BEP 53 `so` selection are included when
  applicable. Query values are percent-escaped and ordering is deterministic:
  identity topics, name, selection, tracker tiers, webseeds.
- Support tracker policies `all`, `primary_tier`, and `none`. `all` uses the
  primary announce plus announce-list tiers without duplicate URLs;
  `primary_tier` uses the first tier; `none` emits no tracker parameters. A
  private torrent requires explicit consent before any tracker is included.
- Keep the existing single `GET /torrents/:hash/metainfo` export as the only
  source-document download. Add `GET /torrents/:hash/magnet` for generated
  magnets and `PUT /torrents/:hash/annotation` for annotations.
- Add `POST /torrents/export` for a batch tar archive. It applies the same
  authenticated route middleware as single export, includes retained original
  bytes only, and writes a `manifest.json` with one entry per requested
  torrent: `included` with filename, or `unavailable` with a reason. It never
  synthesizes a `.torrent` from canonical metadata or magnet information.
- Expose Copy Magnet and Export actions in torrent details and the torrent
  action/context surface. The UI batches export from selected torrents and
  presents a download for exact single or archive output.

## Consequences

- Operators can inspect source facts, add independent working notes, share
  canonical magnets, and reproduce supplied metainfo exactly.
- Magnet output is reproducible and policy-driven; private tracker URLs are
  never exposed without an explicit operator choice.
- Batch export preserves per-item availability instead of failing the whole
  archive or silently producing a reconstructed document.
- Source data, generated magnets, annotations, and any future reconstructed
  representation remain distinguishable in API fields, UI labels, and ADR
  language.
- Future provenance changes must preserve exact original-byte export, separate
  annotations, full-identity magnet generation, tracker consent, deterministic
  ordering, and explicit unavailable-item reporting.

## Related Documents

- [ADR-0065: BEP 52 V2 and Hybrid Torrent Identity](0065-bep-52-v2-hybrid-torrent-identity.md)
- [ADR-0066: Policy-Driven Metadata-First Intake](0066-policy-driven-metadata-first-intake.md)
- [ADR-0067: SQLite Durable Library State](0067-sqlite-durable-library-state.md)
- [ADR-0083: Local Torrent Creation and Immediate Seeding](0083-local-torrent-creation-and-immediate-seeding.md)
- [Feature backlog](../BACKLOG.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Operator API](../../docs/api.md)
- [Web UI](../../docs/web-ui.md)
