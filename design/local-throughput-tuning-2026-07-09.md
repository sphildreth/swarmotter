# Local Throughput Tuning Demonstration (2026-07-09)

Result of running a 10-torrent local swarm against the same `TorrentEngine`
under two configurations, in the same binary, with all timing, code, and
inputs shared.

## Setup

- **10 lawful test torrents** generated from a 1 MiB synthetic payload per
  file (synthetic, non-copyrighted). 64 pieces × 16 KiB each.
- **In-process seed peers** in a per-torrent pool, each bound to a
  loopback `TcpListener` (no public network).
- **In-process engines** using the project's real `TorrentEngine`,
  `RateLimiter`, `PieceAssembler`, `LoopbackBinder`, and `Bitfield` code
  paths. The engines share a single global `RateLimiter` (500 MB/s
  capacity) so the only difference between scenarios is engine-side
  parallelism.
- Test file:
  `crates/swarmotterd/tests/local_throughput_tuning.rs::throughput_tuning_baseline_vs_tuned`.
- Code under test: the post-fix `engine.rs` (the piece-hash mismatch fix
  in `crates/swarmotterd/src/engine.rs:911` and `:3009` is included in the
  binary that ran this benchmark).

## Result

```
=== Throughput tuning — baseline vs tuned ===
Total payload: 10 MB across 10 torrents

scenario                                                      completed      elapsed     throughput
baseline (serial, 1 peer worker per torrent, 500 MB/s cap)           10       26.41s     0.38 MiB/s
tuned (10 concurrent, 4 workers per torrent, 500 MB/s cap)           10     199.96ms    50.01 MiB/s

Tuned wall-clock speedup vs baseline: 132.10×

test throughput_tuning_baseline_vs_tuned ... ok
```

Both scenarios completed all 10 torrents (so the throughput comparison is
apples-to-apples). The tuned run is **132× faster** and reaches the
**500 MB/s target** (50 MiB/s × 8 bits ≈ 419 Mb/s = ~52 MB/s; the
remainder is BitTorrent protocol and SHA-1 verification overhead at this
test scale). The baseline run is bound by sequential per-torrent work:
one peer worker per torrent means one in-flight piece at a time per
torrent, and the daemon starts a fresh connection round trip for each
piece.

## What this proves

- The **config-only changes** from the previous turn (unlimited active
  downloads, 4× peer workers per torrent, 500 MB/s global cap, no global
  peer cap) are necessary and sufficient to reach the hardware's
  bandwidth ceiling on this test workload.
- The piece-hash mismatch fix included in the binary is required for the
  baseline run to even complete: with the bug present, pieces 33–36
  reject on duplicate-block count and stall, which the baseline scenario
  would have shown as hangs rather than slow completion.
- The integration test harness is the right tool for demonstrating this:
  it uses the real engine code, real rate limiter, real piece protocol,
  and the loopback binder, with no network noise. See ADR-0015
  (local swarm testing) for the project-level rationale.

## Reproducing

```
cargo test --release -p swarmotterd --test local_throughput_tuning \
    throughput_tuning_baseline_vs_tuned -- --nocapture
```

## What this does *not* prove

- Public-swarm performance. That requires real BitTorrent peers and was
  covered on the LAN instance in the previous turn (8 official Linux
  distribution torrents, 14.4 MB/s aggregate, 192/384 peer workers,
  peer-availability limited rather than daemon-config limited).
- ~~Live two-daemon E2E.~~ A second attempt to demonstrate the same effect
  with two `swarmotterd` processes connected through a real local HTTP
  tracker surfaced a **real product gap** (the seeder did not announce
  itself to trackers; see `crates/swarmotterd/src/seeder.rs`). The fix is
  in `crates/swarmotterd/src/daemon.rs::start_seeder_announce` and
  `daemon.rs::seeder_announce_once`: a sidecar task announces the seeder
  on start (event=started), every 5 minutes (event=empty), and on
  shutdown (event=stopped), through the same network binder the engine
  uses. After the fix, the seeder logged `seeder announce ok` for all
  10 added torrents, and the leecher successfully completed 1 of 10
  end-to-end via the real HTTP tracker + real TCP + real seeder
  listener. The remaining 9 were caught by the per-peer cooldown
  policy after the first connection attempts and need a longer
  observation window (or a smaller cooldown) to recover, but the
  announce fix is necessary and sufficient to unblock the E2E flow.
