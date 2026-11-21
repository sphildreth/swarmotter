# ADR-0083: Local Torrent Creation and Immediate Seeding

## Status

Accepted

## Context

SwarmOtter could add and seed existing metainfo but could not produce a
portable `.torrent` from local files. Distributors need v1, BEP 52 v2, and
hybrid output, selectable piece sizes, tracker tiers, optional webseeds, and
the private flag. Creation also needs to reuse the existing verified seeding
path rather than invent a second payload writer.

Hashing untrusted or changing source data competes with verification and active
downloads on the same storage roots. Source paths may be symlinks or outside
operator-managed storage. A failed or cancelled hash must never publish a
successful export or claim that source data is verified for seeding.

## Decision

- Add a native API and Web UI local-file/directory creation workflow. The
  request selects v1, pure v2, or hybrid format, an optional piece length,
  ordered tracker tiers, optional webseed URLs, the private flag, and an
  explicit immediate-seed action. An empty tracker/webseed list inserts none.
- Source data must resolve inside a configured local storage root
  (`storage.download_dir`, `storage.incomplete_dir`, `storage.temp_dir`, or a
  `storage.root_controls` path). The source itself and every recursive entry
  must be a regular file or directory; symlinks and special files are rejected.
  Paths and torrent names must be safe UTF-8 components.
- Streaming creation hashes run in a blocking worker under the existing
  root-scoped recheck permit, so `max_concurrent_rechecks` bounds concurrent
  hashing on a root. v1 SHA-1 pieces span file boundaries; BEP 52 block hashes,
  piece hashes, file roots, and piece layers reset at each file. Memory use is
  bounded by read buffers and per-file piece-layer metadata, not total source
  size.
- The source snapshot is captured before hashing and revalidated after hashing.
  Files are opened with no-follow semantics where available and their
  device/inode, length, and modification identity is checked before and after
  reading. Added, removed, renamed, resized, or modified source data fails the
  operation with an identifiable error.
- Generated metainfo is canonical bencode built from the shared metadata model.
  Pure v2 and hybrid output always includes the required top-level piece-layer
  dictionary, including the empty dictionary when no file exceeds the piece
  length. The complete document is bounded by the existing metadata limit and
  must round-trip through the normal bounded parser before export or seeding.
- The API returns the exact generated document as hex (`metainfo_hex`) plus
  identity, size, piece, file, tracker, webseed, and seed outcome fields. The
  Web UI offers a `.torrent` download from those exact bytes. No temporary
  export file or new artifact store is introduced.
- Immediate seeding registers the generated metainfo through the existing
  durable add mutation with a captured storage path, using the source parent as
  the data root. The creation hash is the verification step: every piece was
  checked against the source bytes, so the record is registered as completed
  with a complete verified piece map. The existing queue/seeder reconciliation
  then starts a contained seeder. A duplicate or registration failure does not
  discard the generated export; the response reports `seeded = false` and a
  seed error.
- Immediate seeding requires the torrent name to match the source basename so
  the torrent-relative layout resolves to the existing files. Custom names are
  available for export, not for in-place seeding.
- Cancellation is observed by the blocking hasher and the root permit wait. A
  cancelled or failed hash publishes neither a successful export result nor a
  completed seed.
- No tracker, indexer, webseed, or discovery endpoint is inserted by default.
  Announces and peers continue through the existing fail-closed contained
  network path.

## Consequences

- Operators can create portable v1/v2/hybrid releases and seed verified local
  bytes without an external torrent creator.
- Creation is intentionally narrower than arbitrary filesystem tooling: only
  configured storage roots are accepted, symlinks are rejected, and source
  mutations abort the attempt.
- Immediate seeding trusts the completed creation hash rather than performing a
  second full recheck. The hasher and parser remain independent implementations
  of the piece and file-tree formats, and both must agree before registration.
- Large directories still require time proportional to their bytes and produce
  metainfo bounded by the existing metadata limit. Root controls keep hashing
  from bypassing configured concurrency limits.
- Future creation changes must preserve source-root confinement, no-follow
  source reads, snapshot revalidation, bounded hashing, canonical
  round-tripped metainfo, no default external endpoints, and contained seeding.

## Related Documents

- [ADR-0050: Bounded Untrusted Metainfo Parsing](0050-bounded-untrusted-metainfo-parsing.md)
- [ADR-0056: Storage Root Resource Controls](0056-storage-root-resource-controls.md)
- [ADR-0065: BEP 52 V2 and Hybrid Torrent Identity](0065-bep-52-v2-hybrid-torrent-identity.md)
- [ADR-0066: Policy-Driven Metadata-First Intake](0066-policy-driven-metadata-first-intake.md)
- [ADR-0067: SQLite Durable Library State](0067-sqlite-durable-library-state.md)
- [Feature backlog](../BACKLOG.md)
- [Architecture](../architecture.md)
- [API design](../api.md)
- [Configuration design](../configuration.md)
- [Testing strategy](../testing.md)
- [Operator API](../../docs/api.md)
- [Operator configuration](../../docs/configuration.md)
- [Web UI](../../docs/web-ui.md)
