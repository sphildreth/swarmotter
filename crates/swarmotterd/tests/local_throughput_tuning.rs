// SPDX-License-Identifier: Apache-2.0

//! Throughput-tuning demonstration test.
//!
//! Two scenarios over the same 10-torrent local swarm, sharing the same
//! in-process seeds, run in the same binary so wall-clock comparisons are
//! apples-to-apples.
//!
//! - `baseline_serial_throughput`: 10 torrents run *serially* — one at a time
//!   per process — emulating the "5 active downloads" + no peer-parallelism
//!   default ceiling. Each torrent gets exactly one peer worker, so within a
//!   torrent the engine requests one piece at a time and waits for it before
//!   moving on. The bandwidth limiter is set high so it is not the
//!   bottleneck.
//!
//! - `tuned_parallel_throughput`: the same 10 torrents run *concurrently*,
//!   each with 4 peer workers against 4 distinct seed peers, sharing a
//!   single global rate limiter. This is the configuration the project
//!   recommends for a 500 MB/s-class box running a large library
//!   (see `design/scaling-implementation-plan.md`).
//!
//! All torrents are generated from synthetic, public-domain payloads. All
//! seeds are in-process. The network binder is the loopback binder; no
//! external traffic. See ADR-0015 (local swarm testing).

#![allow(clippy::field_reassign_with_default)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use swarmotter_core::bandwidth::RateLimiter;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::meta::{build_single_file_torrent, parse_torrent, TorrentMeta};
use swarmotter_core::net::binder::LoopbackBinder;
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, MessageId, PeerAddr};
use swarmotterd::engine::{EngineCommand, EngineState, TorrentEngine};

const TORRENT_COUNT: usize = 10;
const PIECE_COUNT: usize = 64;
const PIECE_LENGTH: u64 = 16 * 1024;
const SEED_PEERS_PER_TORRENT: usize = 4;
const TUNED_PEER_WORKERS_PER_TORRENT: usize = 4;
const GLOBAL_BANDWIDTH_BPS: u64 = 500 * 1024 * 1024; // 500 MB/s cap (the box's target)

/// One torrent: payload + metadata + info_hash + a fixed peer_id prefix.
struct TestTorrent {
    #[allow(dead_code)]
    label: &'static str,
    content: Vec<u8>,
    meta: TorrentMeta,
    info_hash: InfoHash,
}

fn build_test_torrent(label: String, salt: u8) -> TestTorrent {
    let mut content = Vec::with_capacity(PIECE_COUNT * PIECE_LENGTH as usize);
    for i in 0..PIECE_COUNT * PIECE_LENGTH as usize {
        content.push(((i.wrapping_mul(37).wrapping_add(11 + salt as usize)) % 251) as u8);
    }
    let torrent_bytes = build_single_file_torrent(&label, &content, PIECE_LENGTH, None, true);
    let meta = parse_torrent(&torrent_bytes).unwrap();
    let info_hash = meta.info_hash;
    TestTorrent {
        label: Box::leak(label.into_boxed_str()),
        content,
        meta,
        info_hash,
    }
}

/// A simple in-process seed peer that serves all pieces to a single
/// connecting leecher. The same shape as `local_swarm::SeedPeer` (intentionally
/// duplicated here so this file is self-contained and not coupled to
/// private items in the integration-test module).
async fn run_seed_peer(
    listener: tokio::net::TcpListener,
    content: Vec<u8>,
    meta: TorrentMeta,
    info_hash: InfoHash,
    peer_id: [u8; 20],
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let content = content.clone();
        let meta = meta.clone();
        tokio::spawn(async move {
            let _ = serve_one_leecher(stream, content, meta, info_hash, peer_id).await;
        });
    }
}

async fn serve_one_leecher(
    stream: tokio::net::TcpStream,
    content: Vec<u8>,
    meta: TorrentMeta,
    info_hash: InfoHash,
    seed_peer_id: [u8; 20],
) -> swarmotter_core::Result<()> {
    let (mut rd, mut wr) = tokio::io::split(stream);
    // Read leecher handshake.
    let mut hs_buf = [0u8; 68];
    rd.read_exact(&mut hs_buf).await?;
    let their_hs = Handshake::decode(&hs_buf)?;
    if their_hs.info_hash != info_hash {
        return Err(swarmotter_core::error::CoreError::Internal(
            "info hash mismatch".into(),
        ));
    }
    // Send our handshake.
    let our_hs = Handshake {
        info_hash,
        peer_id: seed_peer_id,
        reserved: swarmotter_core::peer::RESERVED,
    };
    wr.write_all(&our_hs.encode()).await?;

    // Send full bitfield.
    let mut bf = Bitfield::new(meta.piece_count());
    for i in 0..meta.piece_count() {
        bf.set(i);
    }
    peer::write_message(&mut wr, &bf.encode_message()).await?;
    wr.flush().await?;

    let piece_count = meta.piece_count();
    loop {
        let len_buf = match read_len_prefix(&mut rd).await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(()),
            Err(e) => return Err(swarmotter_core::error::CoreError::from(e)),
        };
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 {
            continue;
        }
        let mut body = vec![0u8; len];
        rd.read_exact(&mut body).await?;
        let id = body[0];
        let payload = &body[1..];
        match MessageId::from_u8(id) {
            Some(MessageId::Interested) => {
                peer::write_message(&mut wr, &Message::Unchoke).await?;
                wr.flush().await?;
            }
            Some(MessageId::Request) if payload.len() == 12 => {
                let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                if (piece as usize) >= piece_count {
                    continue;
                }
                let (pstart, _) = meta.piece_byte_range(piece as u64).unwrap();
                let abs = pstart + offset as u64;
                let block = content[abs as usize..(abs + length as u64) as usize].to_vec();
                peer::write_message(
                    &mut wr,
                    &Message::Piece {
                        piece,
                        offset,
                        block,
                    },
                )
                .await?;
                wr.flush().await?;
            }
            _ => {}
        }
    }
}

async fn read_len_prefix<R: tokio::io::AsyncReadExt + Unpin>(
    rd: &mut R,
) -> std::io::Result<Option<[u8; 4]>> {
    let mut buf = [0u8; 4];
    let mut filled = 0;
    while filled < 4 {
        match rd.read(&mut buf[filled..]).await {
            Ok(0) => {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof mid-length",
                ));
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Some(buf))
}

fn peer_id(prefix: &[u8; 8]) -> [u8; 20] {
    let mut out = [0u8; 20];
    out[..8].copy_from_slice(prefix);
    // Fill the rest with a stable per-torrent distinguisher so the leecher's
    // peer_id doesn't collide across the same swarm (it doesn't matter for
    // correctness but keeps logs readable).
    for (i, b) in out[8..].iter_mut().enumerate() {
        *b = i as u8;
    }
    out
}

fn unique_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-tuning-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

struct ScenarioResult {
    label: String,
    total_bytes: u64,
    elapsed: Duration,
    throughput_mib_s: f64,
    torrents_completed: usize,
    #[allow(dead_code)]
    torrents_requested: usize,
}

fn mib_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64().max(1e-9);
    (bytes as f64 / (1024.0 * 1024.0)) / secs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn throughput_tuning_baseline_vs_tuned() {
    // ---------- 1. Build 10 lawful, generated test torrents ----------
    let torrents: Vec<TestTorrent> = (0..TORRENT_COUNT)
        .map(|i| build_test_torrent(format!("tuning-{i:02}.bin"), i as u8))
        .collect();
    let total_bytes: u64 = torrents.iter().map(|t| t.content.len() as u64).sum();

    // ---------- 2. Spawn seeds ----------
    // For each torrent, start SEED_PEERS_PER_TORRENT seed listeners. The
    // "baseline" run only uses 1 of them; the "tuned" run uses all of them.
    let mut seed_addr_by_torrent: Vec<Vec<SocketAddr>> = Vec::with_capacity(TORRENT_COUNT);
    for (i, t) in torrents.iter().enumerate() {
        let mut addrs = Vec::with_capacity(SEED_PEERS_PER_TORRENT);
        for j in 0..SEED_PEERS_PER_TORRENT {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            addrs.push(addr);
            let content = t.content.clone();
            let meta = t.meta.clone();
            let info_hash = t.info_hash;
            let mut seed_id = *b"-TU0000-";
            seed_id[3] = b'0' + (i as u8 / 10);
            seed_id[4] = b'0' + (i as u8 % 10);
            seed_id[6] = b'0' + (j as u8);
            tokio::spawn(async move {
                run_seed_peer(listener, content, meta, info_hash, peer_id(&seed_id)).await;
            });
        }
        seed_addr_by_torrent.push(addrs);
    }

    // Brief settle so all listeners are accepting.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // ---------- 3. Baseline run: serial, one worker per torrent ----------
    let baseline = run_scenario(
        "baseline (serial, 1 peer worker per torrent, 500 MB/s cap)",
        &torrents,
        &seed_addr_by_torrent,
        /* parallel_torrents = */ 1,
        /* workers_per_torrent = */ 1,
    )
    .await;

    // ---------- 4. Tuned run: 10 concurrent torrents, 4 workers each ----------
    let tuned = run_scenario(
        "tuned (10 concurrent, 4 workers per torrent, 500 MB/s cap)",
        &torrents,
        &seed_addr_by_torrent,
        /* parallel_torrents = */ TORRENT_COUNT,
        /* workers_per_torrent = */ TUNED_PEER_WORKERS_PER_TORRENT,
    )
    .await;

    // ---------- 5. Report ----------
    eprintln!();
    eprintln!("=== Throughput tuning — baseline vs tuned ===");
    eprintln!(
        "Total payload: {} MB across {} torrents",
        total_bytes / (1024 * 1024),
        TORRENT_COUNT
    );
    eprintln!();
    eprintln!(
        "{:<60} {:>10} {:>12} {:>14}",
        "scenario", "completed", "elapsed", "throughput"
    );
    eprintln!(
        "{:<60} {:>10} {:>12} {:>14}",
        baseline.label,
        baseline.torrents_completed,
        format!("{:.2?}", baseline.elapsed),
        format!("{:.2} MiB/s", baseline.throughput_mib_s)
    );
    eprintln!(
        "{:<60} {:>10} {:>12} {:>14}",
        tuned.label,
        tuned.torrents_completed,
        format!("{:.2?}", tuned.elapsed),
        format!("{:.2} MiB/s", tuned.throughput_mib_s)
    );
    eprintln!();
    let speedup = if baseline.elapsed.as_secs_f64() > 0.0 {
        baseline.elapsed.as_secs_f64() / tuned.elapsed.as_secs_f64()
    } else {
        0.0
    };
    eprintln!("Tuned wall-clock speedup vs baseline: {:.2}×", speedup);
    eprintln!();

    // Sanity: both scenarios must complete the full set of torrents so the
    // throughput numbers are directly comparable.
    assert_eq!(
        baseline.torrents_completed, TORRENT_COUNT,
        "baseline did not complete all torrents"
    );
    assert_eq!(
        tuned.torrents_completed, TORRENT_COUNT,
        "tuned did not complete all torrents"
    );
    assert_eq!(baseline.total_bytes, total_bytes);
    assert_eq!(tuned.total_bytes, total_bytes);
    assert!(
        tuned.elapsed < baseline.elapsed,
        "tuned should be faster than baseline; baseline={:?} tuned={:?}",
        baseline.elapsed,
        tuned.elapsed
    );
    std::fs::remove_dir_all(unique_dir("")).ok();
}

async fn run_scenario(
    label: &str,
    torrents: &[TestTorrent],
    seed_addr_by_torrent: &[Vec<SocketAddr>],
    parallel_torrents: usize,
    workers_per_torrent: usize,
) -> ScenarioResult {
    let _total_bytes: u64 = torrents.iter().map(|t| t.content.len() as u64).sum();
    let start = Instant::now();

    // Shared global limiter (a fresh clone for every engine so they share
    // bucket state via Arc).
    let global_limiter = RateLimiter::new(GLOBAL_BANDWIDTH_BPS, 0);

    let mut join_set = tokio::task::JoinSet::new();
    let mut running = 0usize;
    let mut next = 0usize;

    while running < parallel_torrents && next < torrents.len() {
        let t = &torrents[next];
        let seed_addrs: Vec<PeerAddr> = seed_addr_by_torrent[next]
            .iter()
            .take(workers_per_torrent)
            .map(|a| PeerAddr::from_socket_addr(*a))
            .collect();
        next += 1;
        running += 1;

        let meta = t.meta.clone();
        let info_hash = t.info_hash;
        let expected_bytes = t.content.len() as u64;
        let global = global_limiter.clone();
        join_set.spawn(async move {
            let dir = unique_dir("run");
            let binder = Arc::new(LoopbackBinder);
            let state = Arc::new(Mutex::new(EngineState::default()));
            let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);

            // Per-torrent limiter: unlimited (we want to test concurrency,
            // not the limiter). The shared global limiter enforces the
            // aggregate cap.
            let per_torrent = RateLimiter::unlimited();
            let engine = TorrentEngine::with_limiter(
                meta.clone(),
                dir.clone(),
                peer_id(b"-TUSW00-"),
                binder,
                state.clone(),
                cmd_rx,
                seed_addrs,
                6881,
                per_torrent,
                None,
            )
            .with_peer_worker_limit(workers_per_torrent)
            .with_transport(false, true)
            .with_global_limiter(Some(global));

            let outcome = tokio::time::timeout(Duration::from_secs(120), engine.run()).await;
            let _ = std::fs::remove_dir_all(&dir);
            (info_hash, expected_bytes, outcome)
        });
    }

    let mut completed = 0usize;
    let mut bytes_downloaded = 0u64;
    while let Some(res) = join_set.join_next().await {
        running -= 1;
        let (_info_hash, expected, outcome) = res.expect("engine task panicked");
        if let Ok(Ok(final_state)) = outcome {
            if final_state.finished
                && final_state.pieces_have.count(final_state.piece_count) == final_state.piece_count
            {
                completed += 1;
                bytes_downloaded += expected;
            }
        }
        // Top up to keep parallel_torrents in flight.
        if next < torrents.len() && running < parallel_torrents {
            let t = &torrents[next];
            let seed_addrs: Vec<PeerAddr> = seed_addr_by_torrent[next]
                .iter()
                .take(workers_per_torrent)
                .map(|a| PeerAddr::from_socket_addr(*a))
                .collect();
            next += 1;
            running += 1;
            let meta = t.meta.clone();
            let info_hash = t.info_hash;
            let expected_bytes = t.content.len() as u64;
            let global = global_limiter.clone();
            join_set.spawn(async move {
                let dir = unique_dir("run");
                let binder = Arc::new(LoopbackBinder);
                let state = Arc::new(Mutex::new(EngineState::default()));
                let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
                let per_torrent = RateLimiter::unlimited();
                let engine = TorrentEngine::with_limiter(
                    meta.clone(),
                    dir.clone(),
                    peer_id(b"-TUSW00-"),
                    binder,
                    state.clone(),
                    cmd_rx,
                    seed_addrs,
                    6881,
                    per_torrent,
                    None,
                )
                .with_peer_worker_limit(workers_per_torrent)
                .with_transport(false, true)
                .with_global_limiter(Some(global));

                let outcome = tokio::time::timeout(Duration::from_secs(120), engine.run()).await;
                let _ = std::fs::remove_dir_all(&dir);
                (info_hash, expected_bytes, outcome)
            });
        }
    }

    let elapsed = start.elapsed();

    ScenarioResult {
        label: label.to_string(),
        total_bytes: bytes_downloaded,
        elapsed,
        throughput_mib_s: mib_per_sec(bytes_downloaded, elapsed),
        torrents_completed: completed,
        torrents_requested: TORRENT_COUNT,
    }
}
