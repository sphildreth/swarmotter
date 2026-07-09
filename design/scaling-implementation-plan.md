# SwarmOtter Scaling Implementation Plan

SwarmOtter must manage thousands of torrents while keeping torrent lifecycle
state correct, queue reconciliation bounded, and data-plane work constrained by
global resource budgets. This document breaks the work into implementation
phases that can be delivered independently.

## Phase 1: Set-Backed Queue Operations

Replace hot queue membership checks with set-backed bookkeeping while
preserving stable queue ordering and serialized compatibility. Add batch
operations for adding, removing, clearing bypass, and moving many entries to
the bottom. Add unit tests that cover 10,000-entry add/remove/reorder behavior.

## Phase 2: Daemon Batch Lifecycle Reconciliation

Replace per-torrent queue rewrites during stale-active recovery and bulk remove
with batch queue operations. Add daemon regression tests at 1,000 and 10,000
torrent scale for stale active recovery, desired active cap enforcement,
metadata retry backoff, and bulk removal.

## Phase 3: Bounded Metadata Scheduler

Add a distinct configuration limit for active magnet metadata fetches. Metadata
fetches must release slots on no-peer retry/backoff and must not count as active
piece downloads once queued for retry. Add unit tests proving that large magnet
sets keep metadata fetch concurrency bounded.

## Phase 4: Global Resource Scheduler

Introduce an explicit scheduler for active downloads, metadata fetches, tracker
announces, peer connection attempts, and peer workers. Engines request resources
from the scheduler instead of independently expanding work. Add diagnostics for
configured limits, requested slots, granted slots, and saturated resource pools.

## Phase 5: Incremental Runtime Maintenance

Make health scoring, autopilot refresh, stats reconciliation, and tracker
diagnostics prioritize active or dirty torrents. Full-library maintenance should
run in bounded batches so background loops do not repeatedly scan every managed
torrent on each tick.

## Phase 6: Scale Benchmark Harness

Add generated local-torrent benchmarks or ignored scale tests for adding,
querying, reconciling, retrying, removing, and resetting thousands of torrents.
Benchmarks must use generated lawful content and local fixtures only.

Implemented scale coverage includes ignored opt-in tests for a 1,200-record
mixed-state daemon scheduler library and a 2,000-torrent API add/query/retry/
remove/reset flow. These tests remain outside the default suite and are run
explicitly during large-library validation.

## Phase 7: Code-Level Hot-Path Optimizations (priority queue)

Observed during a live test of the daemon on a 500 MB/s-class host running
official Linux distribution torrents. The default config caps at 5 active
downloads, which starves a wide library even when bandwidth is available. The
config-only changes in the first section of this file resolve the most
visible ceiling. The items below are the next-highest-leverage code changes
for reaching hardware-limited throughput with thousands of torrents.

### Highest priority (data-plane correctness and per-torrent CPU)

- **Piece-hash mismatch on duplicate blocks.** The per-piece download loops
  in `crates/swarmotterd/src/engine.rs` previously treated every `Ok`
  return from `PieceAssembler::add_block` (including `Ok(false)` for
  duplicate blocks) as a newly received block, advancing the per-piece
  counter and calling `data()` on an incomplete buffer. The SHA-1 of the
  mostly-zero buffer did not match the expected hash. This produces a flood
  of `piece hash mismatch; rejecting` warnings and stalls affected pieces.
  The two call sites are the legacy single-peer loop (~line 911) and the
  parallel piece state loop (~line 3009). Only count a block when
  `add_block` returns `Ok(true)`. A unit test in
  `crates/swarmotterd/src/engine.rs::tests::piece_assembler_reports_duplicate_with_overwrite`
  pins the assembler contract.
- **`ShapedLimiter::acquire` floor and atomicity.** `crates/swarmotter-core/src/bandwidth.rs:336-341`
  performs the per-torrent and global token acquisitions sequentially with a
  `tokio::time::sleep` floored at 1ms. At 500 MB/s and 16 KiB blocks the
  natural sleep is ~0.26ms; the 1ms floor costs ~4×. Lower the floor to
  microseconds, and combine the two layers into a single composite atomic
  refill-and-consume so each block requires only one acquire path.
- **`EngineState` mutex contention.** Every block completion
  (`crates/swarmotterd/src/engine.rs:973`, ~line 3074, ~line 3545) and every
  choke/unchoke/Have/Bitfield message takes the per-torrent
  `Arc<Mutex<EngineState>>`. With 48 peer workers per torrent and tens of
  thousands of messages per second, this is the dominant lock. Replace
  `state.lock().await` for read-only fields (peer list, peer health,
  bitfield snapshots) with a snapshot read under `RwLock` or with
  per-field atomics. Keep the mutex only for the in-place mutators
  (`update_progress`, peer add/remove, sample decay).
- **`ParallelPieceState` sharding.** `crates/swarmotterd/src/engine.rs:3209`
  uses a shared mutex for piece-level reservations. Two peers working on
  disjoint pieces contend on the same lock. Shard by `piece_index % N` (with
  `N` sized to `num_cpus::get() * 2`) or move the global `have`/`availability`
  into the engine's snapshot and the reservations into a per-piece struct.
- **Bitfield count cache.** `crates/swarmotter-core/src/peer.rs` `Bitfield::count`
  and `PieceBitfield::count` walk the full bit array on every call. Called
  from `daemon.rs:421`, `daemon.rs:1991`, `engine.rs:3468`, `engine.rs:3502`,
  `engine.rs:2928`, and `desired_download_hashes`. Cache as an
  `AtomicUsize` updated on `set()`/`clear()`. Removes a per-message linear
  scan that scales with piece count.
- **Note_peer_bitfield XOR.** `crates/swarmotterd/src/engine.rs:2805-2833`
  walks the full peer bitfield on every `Bitfield` message. Replace with
  `Bitfield::XOR` to find the diff and update only changed pieces. With 48
  peer workers and 5,000+ piece torrents this saves 240k bit ops per
  Bitfield message.

### Medium priority (per-tick background work)

- **Incremental runtime maintenance (Phase 5).** `reconcile_engine_progress`
  and the autopilot loop scan every managed torrent every tick. Mark a
  torrent dirty on state change, new `EngineState` data, or autopilot
  observation. Background loops only walk the dirty set. Drops wall-time
  from O(N × tick) to O(active × tick).
- **`DashMap` for engine handle maps.** Replace
  `tokio::sync::RwLock<HashMap<InfoHash, V>>` for `engine_handles`,
  `engine_states`, `engine_cmds`, `engine_limiters`, `engine_retry_after`,
  `autopilot_decisions`, `autopilot_last_action`, `rate_samples` with
  `DashMap<InfoHash, V>`. Avoids the global write lock when adding or
  removing a torrent.
- **Atomic peer worker limit command.** `crates/swarmotterd/src/daemon.rs:653-661`
  sends a fresh `UpdatePeerWorkerLimit` command on every reconcile. The
  engine polls it once per loop iteration. Replace with a direct
  `Arc<AtomicUsize>` store on the engine, removing 1000s of mpsc sends per
  second.
- **`registry` storage.** `crates/swarmotter-core/src/torrent.rs:173` uses a
  `BTreeMap<InfoHash, Torrent>`. The `list()` sort is wasted CPU since the
  API serializes to JSON anyway. A `HashMap` plus an explicit `info_hash`
  sort for `list_torrents` is faster.
- **`queue.move_to_top` / `move_to_bottom` rebuilds.**
  `crates/swarmotter-core/src/queue.rs:241-256` does `Vec::remove(i)` (O(N))
  plus a full `rebuild_membership_sets` (O(N)). Switch the `order` storage
  to `Vec<Entry { hash, position_index }>` plus an `index_by_hash` HashMap
  for O(1) `position()` and O(1) `move_*` without rebuilds.
- **`Bitfield`/`PieceBitfield` Arc snapshot.** `crates/swarmotterd/src/engine.rs:3475,
  3640, 3647, 3651, 3713, 3724` clone the full `Bitfield` bytes. With 5000
  pieces that's 625 B per clone, but it's per-message. Wrap in
  `Arc<Bitfield>` so the clone is a refcount bump.
- **Magnet candidate concurrency.** `crates/swarmotter-core/src/metadata.rs:34`
  `METADATA_CANDIDATE_CONCURRENCY = 32` times 100 simultaneous metadata
  fetches = 3,200 candidate tasks. Lower to 8 and tighten
  `DHT_DISCOVERY_TIMEOUT` to 10s. Magnet discovery round-trips are a
  significant source of metadata-fetch latency under large magnet imports.

### Lower priority (correctness-adjacent polish)

- **`apply_resolved_metadata` per reconcile.** `crates/swarmotterd/src/daemon.rs:2911`
  is called every reconcile and allocates new `files`, `priorities`,
  `wanted` vectors even when `needed_metadata` has already been cleared.
  Skip when already applied.
- **`EngineStartSnapshot` clone.** `crates/swarmotterd/src/daemon.rs:1214-1220`
  clones `meta`, `download_dir`, etc. on every start. Cheap individually,
  but called once per desired torrent per reconcile.
- **`queue.limits = cfg.queue.clone()` per call.** `daemon.rs:819, 666`
  clones the queue limits on every reconcile and diagnostics call. Push
  config changes through a notification, not a re-clone.
- **`flush_writable_file_slices` over-flush.**
  `crates/swarmotter-core/src/storage/io.rs:358-375` flushes all writable
  handles for a torrent on every piece read. Use a per-handle generation
  counter to flush only mutated handles.
- **Buffer pool for 16 KiB piece blocks.** `crates/swarmotter-core/src/peer.rs:571`
  allocates a fresh `Vec<u8>` per peer message. A thread-local pool of
  16 KiB buffers eliminates 30,000+ allocations per second per torrent at
  500 MB/s.

## Phase 8: Resource Scheduler (Phase 4 work, detailed)

The plan's Phase 4 calls for an explicit `ResourceScheduler` that owns
bounded pools for active downloads, metadata fetches, tracker announces, peer
connection attempts, and peer workers. Engines request slots from it; queue
reconciliation becomes a function of granted slots, not a function of
config-string combinations.

The most important property of Phase 4 is that it removes the implicit
dependency between `max_active_downloads`, `max_peers`, and
`max_peers_per_torrent`. The current
`effective_peer_worker_limit(max_peers, max_peers_per_torrent, active)` in
`crates/swarmotterd/src/daemon.rs:2875-2891` divides a global cap by the
active-download count, which starves a wide library even when there is no
real contention. Phase 4 replaces this with a single per-resource
"requested / granted / waiters" model that all engines and seeders share.
