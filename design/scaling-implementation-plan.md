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
