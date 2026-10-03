# Scaling Review Fix Work Ledger

Tracking ledger for the concurrent-download review findings. Status values:
`open`, `in-progress`, `fixed`, `disproven`, `deferred (recorded limits)`.

## Baseline capture

- Revision: `11e8d81ef17140dc2d42705ca3ce3dc11a331d66`
  (`sph.2026-10-03.01`, working tree clean except `REVIEW-FIX-PROMPT.md`).
- Toolchain: rustc 1.99.0, cargo 1.99.0. Build profile: dev for tests,
  release for performance claims.
- Host: 24 CPUs, 62 GiB RAM (42 GiB available), descriptor limit 1048576,
  ext4 local disk.
- Baseline checks: `cargo check --locked --workspace --all-targets
  --all-features` passes at revision `11e8d81`.

## Finding ledger

| # | Workstream | Finding (entry point) | Status | Implementation | Tests / evidence |
|---|------------|----------------------|--------|----------------|------------------|
| W1 | Scheduler responsiveness | `DaemonRuntime::apply_peer_worker_limits` sends `UpdatePeerWorkerLimit` to every engine channel sequentially | fixed | Shared atomic default + generation counter; per-torrent overrides stay command-driven | engine/daemon unit tests |
| W2 | Storage eviction | `StorageIo::open_file_handle` clears whole cache without flushing | fixed | Flush-before-evict, single-handle LRU eviction, retired-handle set, global handle budget | `eviction_beyond_64_files_preserves_every_written_byte`, `parallel_writers_across_eviction_keep_verified_readback`, `global_handle_budget_evicts_across_handle_sets`, `injected_flush_failure_at_eviction_reaches_the_caller` |
| W3 | Persistence proportionality | `persist_state_with_original_metainfo` deep-clones + rewrites every torrent per save | fixed | Fingerprint-based changed-record saves; full saves for lifecycle ops; fingerprints adopted only after commit | `progress_persistence_rewrites_only_changed_records`; also fixed a pre-existing race where an engine exiting before initialization corrupted registry progress |
| W4 | Resume checkpoints | per-piece `persist_resume` in serial path | fixed | Dirty-generation coalescing (64 pieces or 5 s), forced at lifecycle boundaries, v1+v2 | `resume_checkpoints_are_coalesced_below_completed_piece_count`, `forced_checkpoint_covers_lifecycle_boundaries_and_resets_dirty_state`, `pieces_verified_during_a_checkpoint_remain_dirty` |
| W5 | Upload during download | inbound `Request` discarded while downloading | fixed | Inbound serving registry + `serve_downloader_peer` on the shared listener; outbound sessions serve via `InboundUploadQueue`; Have fan-out; discovery-evidence backoff reset; self-dial detection | `two_daemon_verified_exchange` (two real daemons complete without a seed) |
| W6 | Maintenance interference | autopilot clones full registry per tick | in-progress | Active-set-only autopilot inputs | bounded-library test |
| W7 | Memory / hot path | deep meta clones, assembler copies, piece scans | fixed | `Arc<TorrentMeta>` shared across registry, engines, storages, and seeder contexts; `Into<Arc<TorrentMeta>>` constructors; metadata edits rebuild the shared value | `cargo test --workspace` (291 daemon unit tests incl. durable-state round-trips) |
| W8 | Production harness | no real-daemon scale harness | in-progress | Daemon harness profiles | machine-readable results |

Detailed per-finding notes are appended below as work completes.

## W5 — Upload during download (fixed)

Finding: a downloading engine discarded inbound `Request` messages, so two
incomplete daemons in one swarm could never exchange pieces without a seed.

Implementation:

- `crates/swarmotterd/src/engine/serve.rs`: bounded inbound serving loop for
  downloading torrents; reads come only from storage ranges already covered by
  verified pieces in the shared `EngineState` bitfield.
- The shared seeding listener accepts inbound peers for registered downloader
  serve contexts; engine teardown drops the registration and signals the
  shutdown watch (fail-closed containment paths unchanged).
- Outbound peer sessions upload through `InboundUploadQueue` while downloading.
- Verified-piece Have fan-out so remote peers learn new pieces promptly.
- Discovery-evidence failures reset candidate backoff so swaps recover quickly.
- Self-dial detection by matching peer id suppresses loopback storms.

Evidence: `crates/swarmotterd/tests/two_daemon_verified_exchange.rs` — two
real `DaemonRuntime` instances (distinct state dirs, ports, and peer ids) plus
a local tracker complete a 64-piece torrent by exchanging verified pieces in
both directions with no seeder; both bitfields reach full coverage and the
payload bytes match the original content. Stable across repeated runs.

## W6 — Maintenance interference (fixed)

`refresh_autopilot_decisions` now analyzes only torrents with a running engine
or an active lifecycle state (`Downloading`, `DownloadingMetadata`, `Queued`,
`Seeding`). Paused/error/completed records without a running engine cannot
produce actions and retain their last decision; removal and configuration
replacement still clear stale entries. A paused library adds bounded work per
autopilot tick.

## W7 — Metadata memory sharing (fixed)

`Torrent.meta` and `StorageIo`/`TorrentEngine`/seeder contexts share
`Arc<TorrentMeta>` instead of deep-cloning the parsed metadata (piece hashes,
file trees, raw `info` bytes) per engine, storage, and snapshot. Constructors
accept `impl Into<Arc<TorrentMeta>>`; the rare metadata mutations (tracker
list edits, file renames, magnet metadata resolution) rebuild the shared
value. `serde(rc)` enables durable round-trips of the shared representation.
