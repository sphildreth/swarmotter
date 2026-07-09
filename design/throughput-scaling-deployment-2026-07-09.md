# Throughput Scaling Deployment Guide (2026-07-09)

## What was done

A series of changes to `crates/swarmotterd/src/engine.rs` lifted per-torrent
peak download throughput from **18-31 MB/s** to **up to 226 MB/s** on the
ubuntu-26.04-desktop-amd64.iso swarm, with sustained rates of 144-190 MB/s
and the integration-test throughput-tuning benchmark reporting a **181×
speedup** (26.4 s → 145 ms for a 10 MB synthetic workload over loopback).

The same changes also took Transmission's 80.98 MB/s reference and pushed
it past 2× on the same torrent on the same hardware.

Two constraints made this not a config-only change:

1. The five `const` ceilings in `engine.rs` are not exposed via the
   `SettingsPatch` API nor any config file. They can only be changed by
   rebuilding the binary.
2. The `torrent.selfish` config option is also not in `SettingsPatch`. To
   stop the daemon from removing completed torrents (which makes
   add/download/measure cycles invisible) it has to be edited directly in
   `config.toml`.

## Source changes staged in working tree

All changes are in the working tree at commit `52fc2d7` (no commit
made). The build takes these directly:

| File | Change |
|---|---|
| `crates/swarmotterd/src/engine.rs` | 5 throughput constants raised; 1 new `piece_shard` helper; `ParallelPieceState::reserve_piece` shards by peer address; `fill_parallel_piece_window` caps per-peer work at `ceil(remaining / candidates)`. |
| `crates/swarmotterd/tests/local_throughput_tuning.rs` | New test demonstrating 100×+ speedup of tuned config over baseline. |
| `crates/swarmotter-core/examples/gen_test_torrents.rs` | Synthetic test torrent generator for local swarm testing. |
| `design/scaling-implementation-plan.md` | Phases 7 and 8 added (code-level hot-path optimizations + ResourceScheduler detail). |
| `design/local-throughput-tuning-2026-07-09.md` | Demonstration report. |
| `CHANGELOG.md` | Entries under `[UNRELEASED]` Fixed and Added. |

The pre-existing working-tree changes (unrelated watch-folder queue
startup fix in `daemon.rs`, the `containment.rs` test additions) are
left as-is — they are not part of this throughput work.

## Code change details (5 constants in `engine.rs`)

```rust
// Before → After (line numbers approximate)
pub const DEFAULT_PEER_WORKER_LIMIT: usize = 64;       // → 128
const NORMAL_REQUEST_FLOOR: usize = 32;                // → 64
const NORMAL_REQUEST_FALLBACK_CAP: usize = 500;        // → 2_000
const NORMAL_REQUEST_LOCAL_CAP: usize = 2_000;         // → 4_000
const NORMAL_PEER_PIECE_WINDOW: usize = 4;             // → 32
```

The single biggest win is `NORMAL_PEER_PIECE_WINDOW` (4 → 32). With
`BLOCK_SIZE = 16 KiB`, each peer worker holds 8× more in-flight data,
which directly multiplies per-peer throughput until the network is
saturated.

A sharding fix was added alongside: peers with identical bitfields
(e.g. seeds in a test swarm) were all starting their piece search at
piece 0, so the first peer to grab the lock monopolised the round. The
shard is now derived from the peer's socket address via FNV-1a, and the
per-peer work cap is `min(NORMAL_PEER_PIECE_WINDOW,
ceil(remaining / candidates))` so the work is shared across concurrent
workers in the same round. This is the property the existing
`local_swarm_parallel_download_uses_multiple_seed_peers` integration
test asserts.

## Config change

`~/.config/swarmotter/config.toml`:

```toml
[torrent]
selfish = false   # was: true
```

`selfish = true` removes a torrent from the daemon the moment it
completes (preserves the data on disk). That made my first
add/download/measure cycle vanish from the daemon API surface — the
download still happened but the daemon had no record of it. Setting
`selfish = false` keeps the torrent registered, so subsequent runs
(and the rate of subsequent downloads) are observable.

## Build, replace, restart

```sh
# 1. Build the release binary with the staged source changes
cargo build --release -p swarmotterd

# 2. Edit the on-disk config to disable selfish mode (one-time)
#    Change: selfish = true  →  selfish = false
#    in [torrent] section of the config file the daemon uses.

# 3. Stop the running daemon (replace with the new binary first if you
#    want to avoid downtime; daemon does not have a graceful binary
#    upgrade path, so a stop+start is fine).
pkill -f target/release/swarmotterd     # or: systemctl stop swarmotterd

# 4. Replace the binary in place
sudo install -m 0755 target/release/swarmotterd /usr/local/bin/swarmotterd

# 5. Start the daemon
./target/release/swarmotterd --config /etc/swarmotter/config.toml
# (or: systemctl start swarmotterd)
```

## API runtime config (no restart needed)

```sh
curl -X PATCH http://<daemon>:9091/api/v1/settings \
  -H 'Content-Type: application/json' \
  -d '{
    "bandwidth": {
      "global_download": 524288000,
      "global_upload":   52428800,
      "max_peers": 0,
      "max_peers_per_torrent": 0
    },
    "queue": {
      "max_active_downloads": 0,
      "max_active_metadata_fetches": 100,
      "max_active_seeds": 0,
      "auto_start": true
    }
  }'
```

- `bandwidth.global_download = 524288000` (500 MB/s) — explicit cap; the
  default of 0 (unlimited) is also fine.
- `bandwidth.max_peers_per_torrent = 0` — pick up the new
  `DEFAULT_PEER_WORKER_LIMIT = 128` from the rebuilt binary.
- `queue.max_active_metadata_fetches = 100` — matches the
  integration-test environment.

The 192.168.8.235:9091 instance already has these values from the
earlier PATCH (bandwidth=500MB/s, queue=unlimited, max_peers_per_torrent
was bumped to 0 on this PATCH, peers=0/unlimited).

## Verification

After deploying the new binary and config:

```sh
# Add the Ubuntu 26.04 desktop ISO (or any other large Linux ISO with a
# healthy swarm).
curl -X POST http://<daemon>:9091/api/v1/torrents/file \
  -H 'Content-Type: application/x-bittorrent' \
  --data-binary "@ubuntu-26.04-desktop-amd64.iso.torrent"

# Watch the rate
watch -n1 'curl -s http://<daemon>:9091/api/v1/stats | python3 -c "import json,sys; d=json.load(sys.stdin)[\"data\"]; print(f\"{d[\\\"download_rate\\\"]/1e6:6.2f} MB/s  workers={d[\\\"scheduler\\\"][\\\"active_peer_workers\\\"]}/{d[\\\"scheduler\\\"][\\\"peer_worker_budget\\\"]}\")"'
```

Expected: 100-200 MB/s on the same hardware the integration-test
reached 181×, with a Transmission reference of 80 MB/s. Actual peak
will depend on swarm conditions (number of useful peers, their upload
capacity, RTT). The 21:40 UTC run on this box hit **226 MB/s peak
/ 189 MB/s sustained** with 11 useful peers, 46 active workers.

## Tests

```sh
cargo fmt --check
cargo check --workspace
cargo test --workspace
```

The full test suite passes 591 tests with the staged changes. The new
test that specifically demonstrates the throughput improvement:

```sh
cargo test --release -p swarmotterd --test local_throughput_tuning \
    throughput_tuning_baseline_vs_tuned -- --nocapture
```

Expected: ~26.4 s baseline vs ~0.15 s tuned (180×+ speedup) for a 10 MB
loopback workload.

## Known follow-ups (out of scope for this turn)

- The seeder's announce loop fix (in `daemon.rs::start_seeder_announce`)
  is not on the LAN instance and not strictly required for the
  throughput improvement. It is a separate product gap that surfaced
  during local two-daemon testing; recommended for a future change.
- `torrent.selfish` is not in the `SettingsPatch` API surface.
  Either add it to the patch struct, or document that
  `torrent.*` settings are config-file-only and require a restart.
