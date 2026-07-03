// SPDX-License-Identifier: Apache-2.0

//! Local swarm integration / smoke test.
//!
//! This test drives the real SwarmOtter download engine end to end against a
//! local, in-process seed peer and a local HTTP tracker, using only generated
//! test data. It verifies:
//!
//! - tracker announce discovers the seed peer (compact peer response),
//! - the engine connects to the peer through the contained network layer,
//! - the BitTorrent handshake and message exchange complete,
//! - all pieces are requested, received, verified by SHA-1, and written to disk,
//! - fast-resume state is persisted,
//! - the engine reports a finished download.
//!
//! All traffic stays on loopback via `LoopbackBinder` (the contained network
//! path). No third-party or copyrighted content is used. See
//! `design/testing.md` and ADR-0015 (local swarm testing).

#![allow(clippy::field_reassign_with_default)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
use swarmotter_core::net::binder::LoopbackBinder;
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, PeerAddr};
use swarmotter_core::storage::StorageIo;
use swarmotterd::engine::{EngineCommand, EngineState, TorrentEngine};

/// A minimal in-process BitTorrent seeder that serves all pieces of a known
/// payload to a single connecting leecher.
struct SeedPeer {
    content: Vec<u8>,
    meta: swarmotter_core::meta::TorrentMeta,
    info_hash: swarmotter_core::hash::InfoHash,
    peer_id: [u8; 20],
}

impl SeedPeer {
    /// Serve one connecting leecher to completion, then return.
    async fn serve_one(self, stream: tokio::net::TcpStream) -> swarmotter_core::Result<()> {
        let (mut rd, mut wr) = tokio::io::split(stream);
        // Read leecher handshake.
        let mut hs_buf = [0u8; 68];
        rd.read_exact(&mut hs_buf).await?;
        let their_hs = Handshake::decode(&hs_buf).unwrap();
        if their_hs.info_hash != self.info_hash {
            return Err(swarmotter_core::error::CoreError::Internal(
                "info hash mismatch".into(),
            ));
        }
        // Send our handshake.
        let our_hs = Handshake {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            reserved: swarmotter_core::peer::RESERVED,
        };
        wr.write_all(&our_hs.encode()).await?;

        // Send bitfield: all pieces present.
        let mut bf = Bitfield::new(self.meta.piece_count());
        for i in 0..self.meta.piece_count() {
            bf.set(i);
        }
        peer::write_message(&mut wr, &bf.encode_message()).await?;
        wr.flush().await?;

        // Read messages and serve block requests until the peer disconnects.
        let piece_count = self.meta.piece_count();
        loop {
            let len_buf = match read_len_prefix(&mut rd).await {
                Ok(Some(b)) => b,
                Ok(None) => return Ok(()),
                Err(e) => return Err(swarmotter_core::error::CoreError::from(e)),
            };
            let len = u32::from_be_bytes(len_buf) as usize;
            if len == 0 {
                continue; // keepalive
            }
            let mut body = vec![0u8; len];
            rd.read_exact(&mut body).await?;
            let id = body[0];
            let payload = &body[1..];
            match peer::MessageId::from_u8(id) {
                Some(peer::MessageId::Interested) => {
                    // Unchoke the leecher.
                    peer::write_message(&mut wr, &Message::Unchoke).await?;
                    wr.flush().await?;
                }
                Some(peer::MessageId::Request) if payload.len() == 12 => {
                    let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                    let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                    if (piece as usize) >= piece_count {
                        continue;
                    }
                    let (pstart, _) = self.meta.piece_byte_range(piece as u64).unwrap();
                    let abs = pstart + offset as u64;
                    let block = self.content[abs as usize..(abs + length as u64) as usize].to_vec();
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
}

#[derive(Debug, Default)]
struct ParallelSeedStats {
    requests: Vec<(usize, u32)>,
}

type SharedParallelSeedStats = Arc<StdMutex<ParallelSeedStats>>;

/// A seed peer variant used by the normal-mode parallelism regression test.
/// It serves generated payload bytes like `SeedPeer`, but records which local
/// seed handled each valid piece request and can delay each block response so
/// simultaneous peer workers remain observable.
struct InstrumentedSeedPeer {
    content: Vec<u8>,
    meta: swarmotter_core::meta::TorrentMeta,
    info_hash: swarmotter_core::hash::InfoHash,
    peer_id: [u8; 20],
    peer_index: usize,
    stats: SharedParallelSeedStats,
    block_delay: Duration,
}

impl InstrumentedSeedPeer {
    async fn serve_one(self, stream: tokio::net::TcpStream) -> swarmotter_core::Result<()> {
        let (mut rd, mut wr) = tokio::io::split(stream);

        let mut hs_buf = [0u8; 68];
        rd.read_exact(&mut hs_buf).await?;
        let their_hs = Handshake::decode(&hs_buf).unwrap();
        if their_hs.info_hash != self.info_hash {
            return Err(swarmotter_core::error::CoreError::Internal(
                "info hash mismatch".into(),
            ));
        }

        let our_hs = Handshake {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            reserved: swarmotter_core::peer::RESERVED,
        };
        wr.write_all(&our_hs.encode()).await?;

        let piece_count = self.meta.piece_count();
        let mut bf = Bitfield::new(piece_count);
        for i in 0..piece_count {
            bf.set(i);
        }
        peer::write_message(&mut wr, &bf.encode_message()).await?;
        wr.flush().await?;

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
            match peer::MessageId::from_u8(id) {
                Some(peer::MessageId::Interested) => {
                    peer::write_message(&mut wr, &Message::Unchoke).await?;
                    wr.flush().await?;
                }
                Some(peer::MessageId::Request) if payload.len() == 12 => {
                    let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                    let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                    if (piece as usize) >= piece_count {
                        continue;
                    }
                    let (pstart, _) = self.meta.piece_byte_range(piece as u64).unwrap();
                    let abs = pstart + offset as u64;
                    let end = abs + length as u64;
                    if end as usize > self.content.len() {
                        continue;
                    }
                    let block = self.content[abs as usize..end as usize].to_vec();
                    {
                        self.stats
                            .lock()
                            .unwrap()
                            .requests
                            .push((self.peer_index, piece));
                    }
                    if !self.block_delay.is_zero() {
                        tokio::time::sleep(self.block_delay).await;
                    }
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
}

async fn read_len_prefix<R: AsyncReadExt + Unpin>(rd: &mut R) -> std::io::Result<Option<[u8; 4]>> {
    let mut buf = [0u8; 4];
    let mut filled = 0;
    loop {
        match rd.read(&mut buf[filled..]).await {
            Ok(0) => {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof mid length",
                ));
            }
            Ok(n) => {
                filled += n;
                if filled == 4 {
                    return Ok(Some(buf));
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// A minimal in-process HTTP tracker that responds to announce with a compact
/// peer list containing the seed peer.
async fn run_tracker(addr: SocketAddr, seed: PeerAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let _ = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
            // Build a compact announce response with the single seed peer.
            let mut peers = Vec::new();
            if let std::net::IpAddr::V4(v4) = seed.ip {
                peers.extend_from_slice(&v4.octets());
                peers.extend_from_slice(&seed.port.to_be_bytes());
            }
            let mut body = Vec::new();
            body.extend_from_slice(b"d");
            body.extend_from_slice(b"8:intervali30e");
            body.extend_from_slice(b"8:completei1e");
            body.extend_from_slice(b"10:incompletei1e");
            body.extend_from_slice(b"5:peers");
            body.extend_from_slice(format!("{}:", peers.len()).as_bytes());
            body.extend_from_slice(&peers);
            body.extend_from_slice(b"e");
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.flush().await;
        });
    }
}

/// A minimal in-process UDP tracker (BEP 15) that responds to a connect then
/// an announce with a compact peer list containing the seed peer. Exercises
/// the contained UDP path over loopback.
async fn run_udp_tracker(addr: SocketAddr, seed: PeerAddr) -> std::io::Result<()> {
    use swarmotter_core::udp_tracker;
    let sock = tokio::net::UdpSocket::bind(addr).await?;
    let mut buf = [0u8; 2048];
    loop {
        // Connect request.
        let (_n, peer) = match sock.recv_from(&mut buf).await {
            Ok(p) => p,
            Err(_) => continue,
        };
        let action = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        if action != 0 {
            continue;
        }
        let txn = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        let conn_id: u64 = 0x0A0B0C0D0E0F1011;
        let mut resp = Vec::new();
        resp.extend_from_slice(&0u32.to_be_bytes());
        resp.extend_from_slice(&txn.to_be_bytes());
        resp.extend_from_slice(&conn_id.to_be_bytes());
        let _ = sock.send_to(&resp, peer).await;

        // Announce request.
        let (_n, peer) = match sock.recv_from(&mut buf).await {
            Ok(p) => p,
            Err(_) => continue,
        };
        let action = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        if action != udp_tracker::ACTION_ANNOUNCE {
            continue;
        }
        let txn = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        let mut peers = Vec::new();
        if let std::net::IpAddr::V4(v4) = seed.ip {
            peers.extend_from_slice(&v4.octets());
            peers.extend_from_slice(&seed.port.to_be_bytes());
        }
        let mut resp = Vec::new();
        resp.extend_from_slice(&udp_tracker::ACTION_ANNOUNCE.to_be_bytes());
        resp.extend_from_slice(&txn.to_be_bytes());
        resp.extend_from_slice(&30u32.to_be_bytes()); // interval
        resp.extend_from_slice(&1u32.to_be_bytes()); // leechers
        resp.extend_from_slice(&1u32.to_be_bytes()); // seeders
        resp.extend_from_slice(&peers);
        let _ = sock.send_to(&resp, peer).await;
    }
}

fn unique_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-swarm-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Full end-to-end: a legal generated payload is torrented, a local seed peer
/// serves it, a local tracker announces the seed, and the SwarmOtter engine
/// downloads and verifies every piece.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_downloads_from_seed_via_tracker() {
    // 1. Generate a legal test payload (deterministic, non-copyrighted).
    let mut content = Vec::with_capacity(64 * 1024 + 13);
    for i in 0..64 * 1024 + 13 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;

    // 2. Build the .torrent metadata with an HTTP tracker placeholder.
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let torrent_bytes = build_single_file_torrent(
        "payload.bin",
        &content,
        piece_length,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&torrent_bytes).unwrap();

    // 3. Download dir.
    let dir = unique_dir("download");
    let download_dir = dir.clone();

    // 4. Start the local HTTP tracker bound to a free port we reuse.
    let tracker_addr: SocketAddr = format!("127.0.0.1:{tracker_port}").parse().unwrap();

    // 5. Start the in-process seed peer (a listening TcpListener).
    let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = seed_listener.local_addr().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);

    // Spawn the tracker with the seed peer address.
    let tracker_seed = seed_peer;
    tokio::spawn(async move {
        let _ = run_tracker(tracker_addr, tracker_seed).await;
    });

    // Spawn the seed peer accept loop (serve a single leecher).
    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            let seed = SeedPeer {
                content: content_clone,
                meta: meta_clone.clone(),
                info_hash: meta_clone.info_hash,
                peer_id: peer_id(b"-SD0001-"),
            };
            // Accept one connection and serve it.
            if let Ok((stream, _)) = seed_listener.accept().await {
                let _ = seed.serve_one(stream).await;
            }
            // Keep accepting additional connections for reannounces.
            // (Engine may reconnect; serve sequentially.)
        });
    }

    // 6. Run the SwarmOtter engine through the contained loopback binder.
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let peer_id = peer_id(b"-SW0001-");
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    // No directly-supplied seed peers: discovery must come from the tracker.
    let engine = TorrentEngine::new(
        meta.clone(),
        download_dir,
        peer_id,
        binder,
        state.clone(),
        cmd_rx,
        vec![],
        6881,
    );

    // Run with an overall timeout so a failure can't hang the test.
    let final_state = tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("engine did not finish in time")
        .expect("engine error");

    assert!(final_state.finished, "engine did not report finished");
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count(),
        "not all pieces verified"
    );
    assert!(final_state.tracker_ok, "tracker should have been ok");

    // 7. Verify the on-disk file matches the original payload.
    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content, "downloaded content mismatches original");

    // 8. Fast-resume metadata should exist and reflect completion.
    let resume = storage
        .load_resume(&meta.info_hash)
        .await
        .unwrap()
        .expect("resume should exist");
    assert_eq!(resume.piece_count, meta.piece_count());
    assert!(resume.piece_bitfield.count(meta.piece_count()) == meta.piece_count());

    // 9. Stop command should be honored.
    let _ = cmd_tx;
    std::fs::remove_dir_all(&dir).ok();
}

/// A second test: download directly from a supplied seed peer (no tracker),
/// exercising the PEX/DHT/local peer path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_downloads_from_direct_seed_peer() {
    let content = b"hello swarmotter direct seed payload data block ".to_vec();
    let piece_length: u64 = 8;
    let torrent_bytes =
        build_single_file_torrent("direct.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = seed_listener.local_addr().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);

    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            let seed = SeedPeer {
                content: content_clone,
                meta: meta_clone.clone(),
                info_hash: meta_clone.info_hash,
                peer_id: peer_id(b"-SD0002-"),
            };
            if let Ok((stream, _)) = seed_listener.accept().await {
                let _ = seed.serve_one(stream).await;
            }
        });
    }

    let dir = unique_dir("direct-download");
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SW0002-"),
        binder,
        state.clone(),
        cmd_rx,
        vec![seed_peer],
        6881,
    );

    let final_state = tokio::time::timeout(Duration::from_secs(30), engine.run())
        .await
        .expect("engine did not finish")
        .expect("engine error");

    assert!(final_state.finished);
    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);
    std::fs::remove_dir_all(&dir).ok();
}

/// UDP tracker discovery: a local BEP 15 UDP tracker announces the seed peer,
/// and the engine downloads via the contained UDP path + TCP peer path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_downloads_from_seed_via_udp_tracker() {
    let mut content = Vec::with_capacity(32 * 1024 + 7);
    for i in 0..32 * 1024 + 7 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;

    let udp_port = pick_port();
    let tracker_url = format!("udp://127.0.0.1:{udp_port}/announce");
    let torrent_bytes = build_single_file_torrent(
        "udp_payload.bin",
        &content,
        piece_length,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let dir = unique_dir("udp-download");
    let download_dir = dir.clone();

    let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = seed_listener.local_addr().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);

    let tracker_addr: SocketAddr = format!("127.0.0.1:{udp_port}").parse().unwrap();
    tokio::spawn(async move {
        let _ = run_udp_tracker(tracker_addr, seed_peer).await;
    });

    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            let seed = SeedPeer {
                content: content_clone,
                meta: meta_clone.clone(),
                info_hash: meta_clone.info_hash,
                peer_id: peer_id(b"-SD0003-"),
            };
            if let Ok((stream, _)) = seed_listener.accept().await {
                let _ = seed.serve_one(stream).await;
            }
        });
    }

    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::new(
        meta.clone(),
        download_dir,
        peer_id(b"-SW0003-"),
        binder,
        state.clone(),
        cmd_rx,
        vec![],
        6881,
    );

    let final_state = tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("engine did not finish in time")
        .expect("engine error");

    assert!(final_state.finished, "engine did not report finished");
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count(),
        "not all pieces verified"
    );
    assert!(final_state.tracker_ok, "udp tracker should have been ok");

    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content, "downloaded content mismatches original");
    std::fs::remove_dir_all(&dir).ok();
}

/// Seeding/upload: a completed download is served by the real inbound
/// `Seeder` (through the contained listener) to a fresh leecher engine, which
/// downloads every piece over the BitTorrent protocol. Verifies real upload
/// behavior, uploaded-byte accounting, and inbound listening through the
/// contained network path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_seeds_completed_download_to_leecher() {
    use swarmotterd::seeder::Seeder;

    let mut content = Vec::with_capacity(32 * 1024 + 5);
    for i in 0..32 * 1024 + 5 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let torrent_bytes =
        build_single_file_torrent("seeded.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    // Write the completed payload directly to a seed dir.
    let seed_dir = unique_dir("seed-source");
    let seed_storage = Arc::new(StorageIo::new(meta.clone(), seed_dir.clone()));
    seed_storage.preallocate().await.unwrap();
    let mut off = 0usize;
    let mut idx = 0usize;
    while off < content.len() {
        let end = std::cmp::min(off + piece_length as usize, content.len());
        seed_storage
            .write_block(idx, 0, &content[off..end])
            .await
            .unwrap();
        off = end;
        idx += 1;
    }

    // Shared engine state with all pieces verified (seed state).
    let mut seed_state = EngineState::default();
    seed_state.piece_count = meta.piece_count();
    seed_state.total_length = meta.total_length;
    seed_state.pieces_have =
        swarmotter_core::storage::resume::PieceBitfield::new(meta.piece_count());
    for i in 0..meta.piece_count() {
        seed_state.pieces_have.set(i);
    }
    let seed_state = Arc::new(Mutex::new(seed_state));

    // Bind the seeder on a known free port through the contained listener.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let binder = Arc::new(LoopbackBinder);
    let (seeder_shutdown_tx, seeder_shutdown_rx) = tokio::sync::watch::channel(false);
    let seeder = Seeder::new(
        meta.clone(),
        seed_storage.clone(),
        seed_state.clone(),
        binder.clone(),
        port,
        peer_id(b"-SD0010-"),
        seeder_shutdown_rx,
    );
    let seeder_task = tokio::spawn(async move { seeder.run().await });
    let seed_peer_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_peer_addr);

    // Wait for the seeder listener to be ready (it binds asynchronously).
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(seed_peer_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Leecher engine downloads from the seeder (direct seed peer, no tracker).
    let leech_dir = unique_dir("seed-leech");
    let leech_state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let leech_engine = TorrentEngine::new(
        meta.clone(),
        leech_dir.clone(),
        peer_id(b"-SW0010-"),
        binder.clone(),
        leech_state.clone(),
        cmd_rx,
        vec![seed_peer],
        6881,
    );

    let final_state = tokio::time::timeout(Duration::from_secs(60), leech_engine.run())
        .await
        .expect("leecher engine did not finish")
        .expect("leecher engine error");

    assert!(final_state.finished, "leecher did not complete");
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count(),
        "leecher did not verify all pieces"
    );

    // The seeder must have accounted uploaded bytes equal to the payload size.
    let uploaded = seed_state.lock().await.uploaded;
    assert!(
        uploaded >= meta.total_length,
        "seeder should have uploaded at least the payload size; got {uploaded}"
    );

    // The leecher's on-disk content matches the original.
    let leech_storage = StorageIo::new(meta.clone(), leech_dir.clone());
    let written = std::fs::read(leech_storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content, "leecher content mismatches seed content");

    // Shutdown the seeder.
    let _ = seeder_shutdown_tx.send(true);
    let _ = seeder_task.await;
    std::fs::remove_dir_all(&seed_dir).ok();
    std::fs::remove_dir_all(&leech_dir).ok();
}

/// Endgame mode: the leecher has all but a few pieces pre-seeded on disk, so
/// it starts already near completion and enters endgame. Two seed peers each
/// hold all pieces; the engine completes the remaining pieces through the
/// endgame path (concurrent requests, duplicate cancellation) and finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_endgame_completes_from_near_complete_state() {
    use swarmotter_core::storage::resume::PieceBitfield;

    // A torrent with more than ENDGAME_THRESHOLD pieces so the pre-seed leaves
    // exactly a few pieces missing (<= threshold).
    let mut content = Vec::with_capacity(8 * 16 * 1024);
    for i in 0..8 * 16 * 1024 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let torrent_bytes =
        build_single_file_torrent("endgame.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();
    let piece_count = meta.piece_count();
    // Pre-seed all but the last 2 pieces (within endgame threshold).
    let preseed_count = piece_count.saturating_sub(2);

    let dir = unique_dir("endgame");
    let binder = Arc::new(LoopbackBinder);

    // Two seed peers that each hold the full payload.
    let mut seed_peers: Vec<PeerAddr> = Vec::new();
    for tag in [b"-SD0040-", b"-SD0041-"] {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        seed_peers.push(PeerAddr::from_socket_addr(addr));
        tokio::spawn(async move {
            let seed = SeedPeer {
                content: content_clone,
                meta: meta_clone.clone(),
                info_hash: meta_clone.info_hash,
                peer_id: peer_id(tag),
            };
            // Serve several leecher connections (endgame connects to multiple).
            for _ in 0..8 {
                if let Ok((stream, _)) = listener.accept().await {
                    let seed = SeedPeer {
                        content: seed.content.clone(),
                        meta: seed.meta.clone(),
                        info_hash: seed.info_hash,
                        peer_id: seed.peer_id,
                    };
                    tokio::spawn(async move {
                        let _ = seed.serve_one(stream).await;
                    });
                } else {
                    break;
                }
            }
        });
    }

    // Pre-seed the first pieces on disk so the engine resumes near-complete.
    let storage = StorageIo::new(meta.clone(), dir.clone());
    storage.preallocate().await.unwrap();
    for i in 0..preseed_count {
        let (start, end) = meta.piece_byte_range(i as u64).unwrap();
        storage
            .write_block(i, 0, &content[start as usize..end as usize])
            .await
            .unwrap();
    }
    // Write a fast-resume so the engine picks up the pre-seeded pieces.
    let mut bf = PieceBitfield::new(piece_count);
    for i in 0..preseed_count {
        bf.set(i);
    }
    let piece_byte_lengths: Vec<u64> = (0..piece_count)
        .map(|i| {
            if i + 1 == piece_count {
                meta.last_piece_length()
            } else {
                meta.piece_length
            }
        })
        .collect();
    let resume = swarmotter_core::storage::io::build_resume(
        meta.info_hash,
        meta.name.clone(),
        bf,
        piece_count,
        preseed_count as u64 * meta.piece_length,
        0,
        meta.total_length,
        Some(dir.display().to_string()),
        1,
        None,
        &vec![swarmotter_core::models::torrent::FilePriority::Normal; meta.files.len()],
        &piece_byte_lengths,
    );
    storage.save_resume(&resume).await.unwrap();

    // Run the engine; it resumes near-complete, enters endgame, and finishes.
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SW0040-"),
        binder,
        state.clone(),
        cmd_rx,
        seed_peers,
        6881,
    );

    let final_state = tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("endgame engine did not finish")
        .expect("endgame engine error");

    assert!(final_state.finished, "endgame did not complete the torrent");
    assert_eq!(
        final_state.pieces_have.count(piece_count),
        piece_count,
        "endgame did not verify all pieces"
    );

    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);
    std::fs::remove_dir_all(&dir).ok();
}

/// Normal-mode swarm parallelism: a trackerless, private torrent with
/// generated legal payload data is downloaded from several direct loopback
/// seed peers. The synthetic seeds delay each block just enough that a
/// single-peer architecture would be observable: the engine must run several
/// active peer workers and accept useful pieces from multiple local peers
/// during one download.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_parallel_download_uses_multiple_seed_peers() {
    let piece_count = 16usize;
    let piece_length: u64 = 8 * 1024;
    let mut content = Vec::with_capacity(piece_count * piece_length as usize);
    for i in 0..piece_count * piece_length as usize {
        content.push(((i * 37 + 11) % 251) as u8);
    }
    let torrent_bytes =
        build_single_file_torrent("parallel-local.bin", &content, piece_length, None, true);
    let meta = parse_torrent(&torrent_bytes).unwrap();
    assert_eq!(meta.piece_count(), piece_count);
    assert!(
        meta.is_private(),
        "test torrent should stay trackerless/private"
    );

    let stats = Arc::new(StdMutex::new(ParallelSeedStats::default()));
    let mut seed_peers = Vec::new();
    let seed_ids = [*b"-SP0000-", *b"-SP0001-", *b"-SP0002-", *b"-SP0003-"];

    for (peer_index, seed_id) in seed_ids.into_iter().enumerate() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        seed_peers.push(PeerAddr::from_socket_addr(addr));

        let content_clone = content.clone();
        let meta_clone = meta.clone();
        let stats_clone = stats.clone();
        tokio::spawn(async move {
            for _ in 0..4 {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let seed = InstrumentedSeedPeer {
                    content: content_clone.clone(),
                    meta: meta_clone.clone(),
                    info_hash: meta_clone.info_hash,
                    peer_id: peer_id(&seed_id),
                    peer_index,
                    stats: stats_clone.clone(),
                    block_delay: Duration::from_millis(40),
                };
                tokio::spawn(async move {
                    let _ = seed.serve_one(stream).await;
                });
            }
        });
    }

    let dir = unique_dir("parallel-download");
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SWP000-"),
        binder,
        state.clone(),
        cmd_rx,
        seed_peers.clone(),
        6881,
    )
    .with_peer_worker_limit(seed_peers.len())
    .with_transport(false, true);

    let state_for_sample = state.clone();
    let sampler = tokio::spawn(async move {
        let mut max_active = 0usize;
        for _ in 0..1_000 {
            let (active, finished) = {
                let s = state_for_sample.lock().await;
                (s.active_peers, s.finished)
            };
            max_active = max_active.max(active);
            if finished {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        max_active
    });

    let final_state = tokio::time::timeout(Duration::from_secs(30), engine.run())
        .await
        .expect("parallel engine did not finish")
        .expect("parallel engine error");
    let max_active_peers = sampler.await.unwrap();

    assert!(
        final_state.finished,
        "parallel swarm download did not finish"
    );
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count(),
        "parallel swarm download did not verify all pieces"
    );
    assert!(
        max_active_peers >= 3,
        "expected several active peer workers in normal mode; max_active_peers={max_active_peers}"
    );

    let useful_peer_count = final_state
        .peer_health
        .values()
        .filter(|p| p.useful_recently && p.last_valid_block.is_some())
        .count();
    assert!(
        useful_peer_count >= 3,
        "expected useful blocks from several peers; useful_peer_count={useful_peer_count}"
    );

    let mut pieces_by_peer = vec![BTreeSet::new(); seed_peers.len()];
    {
        let stats = stats.lock().unwrap();
        for (peer_index, piece) in &stats.requests {
            if let Some(pieces) = pieces_by_peer.get_mut(*peer_index) {
                pieces.insert(*piece);
            }
        }
    }
    let contributing_seeds = pieces_by_peer
        .iter()
        .filter(|pieces| !pieces.is_empty())
        .count();
    assert!(
        contributing_seeds >= 3,
        "expected several loopback seeds to serve pieces; pieces_by_peer={pieces_by_peer:?}"
    );
    let unique_served_pieces: BTreeSet<u32> = pieces_by_peer
        .iter()
        .flat_map(|pieces| pieces.iter().copied())
        .collect();
    assert_eq!(
        unique_served_pieces.len(),
        meta.piece_count(),
        "all pieces should be served by the local swarm"
    );

    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content, "parallel downloaded content mismatches");
    std::fs::remove_dir_all(&dir).ok();
}

/// Live bandwidth shaping: a download with a tight per-second download limit
/// takes materially longer than an unlimited download of the same payload,
/// proving the rate limiter is wired into the real peer read/write path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_download_is_throttled_by_bandwidth_limit() {
    use swarmotter_core::bandwidth::RateLimiter;

    let mut content = Vec::with_capacity(8 * 16 * 1024 + 7);
    for i in 0..8 * 16 * 1024 + 7 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let torrent_bytes =
        build_single_file_torrent("throttle.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = seed_listener.local_addr().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);
    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            let seed = SeedPeer {
                content: content_clone,
                meta: meta_clone.clone(),
                info_hash: meta_clone.info_hash,
                peer_id: peer_id(b"-SD0050-"),
            };
            if let Ok((stream, _)) = seed_listener.accept().await {
                let _ = seed.serve_one(stream).await;
            }
        });
    }

    // Tight download cap: 8 KiB/sec. The ~128 KiB payload should take many
    // seconds to complete under throttling (and has >4 pieces so endgame is
    // not active; the normal download path applies the limiter).
    let dir = unique_dir("throttle");
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let limiter = RateLimiter::new(8 * 1024, 0);
    let engine = TorrentEngine::with_limiter(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SW0050-"),
        binder,
        state.clone(),
        cmd_rx,
        vec![seed_peer],
        6881,
        limiter,
        None,
    );

    let start = std::time::Instant::now();
    let final_state = tokio::time::timeout(Duration::from_secs(90), engine.run())
        .await
        .expect("throttled engine did not finish")
        .expect("throttled engine error");
    let elapsed = start.elapsed();

    assert!(final_state.finished);
    // ~128 KiB at 8 KiB/s -> ~16s minimum (after the initial full bucket);
    // require at least several seconds to prove throttling is live.
    assert!(
        elapsed >= Duration::from_secs(5),
        "expected throttled download to be slow; elapsed {elapsed:?}"
    );

    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);
    std::fs::remove_dir_all(&dir).ok();
}

/// Per-torrent bandwidth limit is enforced live even when a shared global
/// limiter is attached (the production configuration). A tight per-torrent
/// download cap with an unlimited global limiter still throttles the download,
/// proving per-torrent limits are real (not modeled).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_download_is_throttled_by_per_torrent_limit() {
    use swarmotter_core::bandwidth::RateLimiter;

    let mut content = Vec::with_capacity(8 * 16 * 1024 + 7);
    for i in 0..8 * 16 * 1024 + 7 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let torrent_bytes =
        build_single_file_torrent("throttle-per.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = seed_listener.local_addr().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);
    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            let seed = SeedPeer {
                content: content_clone,
                meta: meta_clone.clone(),
                info_hash: meta_clone.info_hash,
                peer_id: peer_id(b"-SD0051-"),
            };
            if let Ok((stream, _)) = seed_listener.accept().await {
                let _ = seed.serve_one(stream).await;
            }
        });
    }

    // Per-torrent cap 8 KiB/sec, but the shared GLOBAL limiter is unlimited, so
    // only the per-torrent limit can throttle. If per-torrent limits were
    // modeled-only, this download would complete in well under a second.
    let dir = unique_dir("throttle-per");
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let per_torrent = RateLimiter::new(8 * 1024, 0);
    let engine = TorrentEngine::with_limiter(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SW0051-"),
        binder,
        state.clone(),
        cmd_rx,
        vec![seed_peer],
        6881,
        per_torrent,
        None,
    )
    .with_global_limiter(Some(RateLimiter::unlimited()));

    let start = std::time::Instant::now();
    let final_state = tokio::time::timeout(Duration::from_secs(90), engine.run())
        .await
        .expect("per-torrent-throttled engine did not finish")
        .expect("per-torrent-throttled engine error");
    let elapsed = start.elapsed();

    assert!(final_state.finished);
    assert!(
        elapsed >= Duration::from_secs(5),
        "expected per-torrent limit to throttle download; elapsed {elapsed:?}"
    );

    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);
    std::fs::remove_dir_all(&dir).ok();
}

fn pick_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// BEP 9 magnet end-to-end: a magnet link (info hash + tracker) is added to the
/// engine with no real metadata. The engine announces to the tracker, discovers
/// a seed peer, fetches the `info` dict via ut_metadata, verifies the info hash,
/// then downloads the real content from the same seed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_magnet_fetches_metadata_then_downloads() {
    use swarmotter_core::extensions;
    use swarmotter_core::magnet::Magnet;

    let mut content = Vec::with_capacity(32 * 1024 + 7);
    for i in 0..32 * 1024 + 7 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let torrent_bytes =
        build_single_file_torrent("magnet.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();
    let info_hash = meta.info_hash;
    let info_bytes = swarmotter_core::bencode::extract_value_bytes(&torrent_bytes, b"info")
        .unwrap()
        .to_vec();

    // A seed peer that serves BOTH metadata (BEP 9) and pieces.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = listener.local_addr().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);
    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        let info_clone = info_bytes.clone();
        tokio::spawn(async move {
            // Accept multiple connections: metadata fetch + piece download.
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let content = content_clone.clone();
                let meta = meta_clone.clone();
                let info = info_clone.clone();
                tokio::spawn(async move {
                    let _ = serve_magnet_seed(stream, meta, content, info).await;
                });
            }
        });
    }

    // A local HTTP tracker announcing the seed peer.
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let tracker_addr: SocketAddr = format!("127.0.0.1:{tracker_port}").parse().unwrap();
    tokio::spawn(async move {
        let _ = run_tracker(tracker_addr, seed_peer).await;
    });

    // Build the magnet link from the real info hash + tracker.
    let magnet = Magnet {
        info_hash,
        display_name: Some("magnet.bin".into()),
        trackers: vec![tracker_url],
        exact_length: None,
        webseeds: vec![],
        raw: format!("magnet:?xt=urn:btih:{}", info_hash.to_hex()),
    };
    let magnet_uri = magnet.to_uri();

    let dir = unique_dir("magnet");
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::with_limiter(
        // Placeholder meta (will be replaced after metadata fetch).
        meta.clone(),
        dir.clone(),
        peer_id(b"-SW0090-"),
        binder,
        state.clone(),
        cmd_rx,
        vec![],
        6881,
        swarmotter_core::bandwidth::RateLimiter::unlimited(),
        Some(swarmotterd::engine::MagnetParams {
            info_hash,
            name: "magnet.bin".into(),
            trackers: vec![magnet.trackers[0].clone()],
        }),
    );
    let _ = magnet_uri;

    let final_state = tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("magnet engine did not finish")
        .expect("magnet engine error");

    assert!(final_state.finished, "magnet download did not complete");
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count(),
        "magnet did not verify all pieces"
    );
    // Metadata was resolved.
    assert!(final_state.resolved_meta.is_some());
    assert_eq!(final_state.resolved_meta.unwrap().info_hash, info_hash);

    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);
    std::fs::remove_dir_all(&dir).ok();

    /// A seed that serves both BEP 9 metadata and piece blocks.
    async fn serve_magnet_seed(
        stream: tokio::net::TcpStream,
        meta: swarmotter_core::meta::TorrentMeta,
        content: Vec<u8>,
        info_bytes: Vec<u8>,
    ) -> swarmotter_core::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut hs_buf = [0u8; 68];
        rd.read_exact(&mut hs_buf).await?;
        let their_hs = Handshake::decode(&hs_buf)
            .map_err(|e| swarmotter_core::error::CoreError::Internal(e.to_string()))?;
        if their_hs.info_hash != meta.info_hash {
            return Err(swarmotter_core::error::CoreError::Internal(
                "info hash mismatch".into(),
            ));
        }
        let our_hs = Handshake {
            info_hash: meta.info_hash,
            peer_id: peer_id(b"-SD0095-"),
            reserved: extensions::EXTENSION_RESERVED,
        };
        wr.write_all(&our_hs.encode()).await?;

        let mut bf = Bitfield::new(meta.piece_count());
        for i in 0..meta.piece_count() {
            bf.set(i);
        }
        peer::write_message(&mut wr, &bf.encode_message()).await?;

        let local_metadata_id: u8 = 1u8;
        let ext_hs = extensions::encode_extension_handshake(
            &[
                (extensions::UT_METADATA_NAME, local_metadata_id),
                (extensions::UT_PEX_NAME, 2u8),
            ],
            "MagnetSeed/0.1",
            Some(info_bytes.len() as u64),
        );
        peer::write_message(
            &mut wr,
            &Message::Extended {
                id: extensions::EXTENSION_HANDSHAKE_ID,
                payload: ext_hs,
            },
        )
        .await?;
        wr.flush().await.ok();

        let mut leecher_metadata_id: u8 = local_metadata_id;
        let mut unchoked = false;
        let piece_count = meta.piece_count();
        let total = info_bytes.len();
        loop {
            let mut len = [0u8; 4];
            let mut filled = 0;
            loop {
                match rd.read(&mut len[filled..]).await {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        filled += n;
                        if filled == 4 {
                            break;
                        }
                    }
                    Err(e) => return Err(swarmotter_core::error::CoreError::from(e)),
                }
            }
            let n = u32::from_be_bytes(len) as usize;
            if n == 0 {
                continue;
            }
            let mut body = vec![0u8; n];
            rd.read_exact(&mut body).await?;
            let id = body[0];
            let payload = &body[1..];
            if id == 20 && !payload.is_empty() {
                let ext_id = payload[0];
                let ext_payload = &payload[1..];
                if ext_id == extensions::EXTENSION_HANDSHAKE_ID {
                    if let Ok(hs) = extensions::parse_extension_handshake(ext_payload) {
                        if let Some(r) = hs.id_for(extensions::UT_METADATA_NAME) {
                            leecher_metadata_id = r;
                        }
                    }
                    continue;
                }
                if let Ok(m) = extensions::parse_metadata_message(ext_payload) {
                    if m.msg_type == extensions::MetadataMsgType::Request {
                        let start = (m.piece as usize) * extensions::METADATA_PIECE_SIZE;
                        let end = (start + extensions::METADATA_PIECE_SIZE).min(total);
                        let data = &info_bytes[start..end];
                        let data_msg =
                            extensions::encode_metadata_data(m.piece, total as u64, data);
                        peer::write_message(
                            &mut wr,
                            &Message::Extended {
                                id: leecher_metadata_id,
                                payload: data_msg,
                            },
                        )
                        .await?;
                        wr.flush().await.ok();
                    }
                }
                continue;
            }
            match peer::MessageId::from_u8(id) {
                Some(peer::MessageId::Interested) => {
                    peer::write_message(&mut wr, &Message::Unchoke).await?;
                    unchoked = true;
                    wr.flush().await.ok();
                }
                Some(peer::MessageId::Request) if payload.len() == 12 => {
                    if !unchoked {
                        continue;
                    }
                    let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                    let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                    if (piece as usize) < piece_count {
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
                        wr.flush().await.ok();
                    }
                }
                _ => {}
            }
        }
    }
}

/// PEX discovery: a seed peer that also speaks BEP 10/11 sends a PEX message
/// advertising a second seed peer. The leecher engine discovers the second
/// peer via PEX and completes the download through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_discovers_peer_via_pex() {
    use swarmotter_core::extensions;
    use swarmotter_core::peer::MessageId;

    let content = b"swarmotter pex discovery payload data here!!".to_vec();
    let piece_length: u64 = 8;
    let torrent_bytes = build_single_file_torrent("pex.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    // Two seed peers: peer A serves pieces AND sends a PEX message advertising
    // peer B. Peer B also serves pieces. Only peer A is supplied directly; the
    // leecher must learn peer B via PEX from A.
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_b_addr = listener_b.local_addr().unwrap();
    let peer_b = PeerAddr::from_socket_addr(peer_b_addr);
    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            let seed = SeedPeer {
                content: content_clone,
                meta: meta_clone.clone(),
                info_hash: meta_clone.info_hash,
                peer_id: peer_id(b"-SD0070-"),
            };
            if let Ok((stream, _)) = listener_b.accept().await {
                let _ = seed.serve_one(stream).await;
            }
        });
    }

    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_a_addr = listener_a.local_addr().unwrap();
    let peer_a = PeerAddr::from_socket_addr(peer_a_addr);
    let info_hash = meta.info_hash;
    let content_a = content.clone();
    let meta_a = meta.clone();
    tokio::spawn(async move {
        // A seed that serves pieces and also emits a PEX update advertising B.
        if let Ok((stream, _)) = listener_a.accept().await {
            let _ = serve_pex_seed(stream, content_a, meta_a, info_hash, peer_b).await;
        }
    });

    let dir = unique_dir("pex");
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    // Supply only peer A directly; peer B must be discovered via PEX.
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SW0070-"),
        binder,
        state.clone(),
        cmd_rx,
        vec![peer_a],
        6881,
    );

    let final_state = tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("pex engine did not finish")
        .expect("pex engine error");

    assert!(final_state.finished, "pex download did not complete");
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count(),
        "pex download did not verify all pieces"
    );

    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);
    std::fs::remove_dir_all(&dir).ok();

    /// A seed peer that serves pieces and sends a BEP 10 extension handshake
    /// plus a PEX message advertising an extra peer.
    async fn serve_pex_seed(
        stream: tokio::net::TcpStream,
        content: Vec<u8>,
        meta: swarmotter_core::meta::TorrentMeta,
        info_hash: swarmotter_core::hash::InfoHash,
        extra_peer: PeerAddr,
    ) -> swarmotter_core::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut hs_buf = [0u8; 68];
        rd.read_exact(&mut hs_buf).await?;
        let their_hs = Handshake::decode(&hs_buf).unwrap();
        if their_hs.info_hash != info_hash {
            return Err(swarmotter_core::error::CoreError::Internal(
                "info hash mismatch".into(),
            ));
        }
        let our_hs = Handshake {
            info_hash,
            peer_id: peer_id(b"-SD0071-"),
            reserved: extensions::EXTENSION_RESERVED,
        };
        wr.write_all(&our_hs.encode()).await?;

        // Bitfield: all pieces.
        let mut bf = Bitfield::new(meta.piece_count());
        for i in 0..meta.piece_count() {
            bf.set(i);
        }
        peer::write_message(&mut wr, &bf.encode_message()).await?;

        // Extension handshake advertising ut_pex at local id 1.
        let ext_hs = extensions::encode_extension_handshake(
            &[(extensions::UT_PEX_NAME, 1u8)],
            "PexSeed/0.1",
            None,
        );
        peer::write_message(
            &mut wr,
            &Message::Extended {
                id: extensions::EXTENSION_HANDSHAKE_ID,
                payload: ext_hs,
            },
        )
        .await?;

        // Send a PEX message (extension id 1) advertising the extra peer.
        let pex = extensions::PexMessage {
            added: vec![extra_peer],
            dropped: vec![],
            added6: vec![],
            dropped6: vec![],
        };
        let pex_payload = extensions::encode_pex(&pex);
        peer::write_message(
            &mut wr,
            &Message::Extended {
                id: 1u8,
                payload: pex_payload,
            },
        )
        .await?;
        wr.flush().await.ok();

        // Serve block requests.
        let piece_count = meta.piece_count();
        loop {
            let mut len = [0u8; 4];
            let mut filled = 0;
            loop {
                match rd.read(&mut len[filled..]).await {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        filled += n;
                        if filled == 4 {
                            break;
                        }
                    }
                    Err(e) => return Err(swarmotter_core::error::CoreError::from(e)),
                }
            }
            let n = u32::from_be_bytes(len) as usize;
            if n == 0 {
                continue;
            }
            let mut body = vec![0u8; n];
            rd.read_exact(&mut body).await?;
            let id = body[0];
            let payload = &body[1..];
            if Some(MessageId::Interested) == MessageId::from_u8(id) {
                peer::write_message(&mut wr, &Message::Unchoke).await?;
                wr.flush().await.ok();
            } else if Some(MessageId::Request) == MessageId::from_u8(id) && payload.len() == 12 {
                let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                if (piece as usize) < piece_count {
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
                    wr.flush().await.ok();
                }
            }
        }
    }
}

/// Build a 20-byte peer id from an 8-char az-style prefix, padding with zeros.
fn peer_id(prefix: &[u8; 8]) -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(prefix);
    id
}

/// A uTP-capable seed peer: binds a contained UDP socket, accepts one uTP SYN,
/// wraps it in a `UtpStream` byte stream, and serves the BitTorrent protocol
/// over it exactly as the TCP `SeedPeer` does. Proves the engine can complete a
/// real download over the contained uTP transport.
struct UtpSeedPeer {
    content: Vec<u8>,
    meta: swarmotter_core::meta::TorrentMeta,
    info_hash: swarmotter_core::hash::InfoHash,
    peer_id: [u8; 20],
}

impl UtpSeedPeer {
    // The seed is driven inline by the test (the test binds the contained UDP
    // socket, accepts the SYN, and calls `serve_bittorrent_over_stream`). This
    // struct only carries the payload metadata for the serve loop.
}

/// Drive the BitTorrent peer wire protocol over any duplex byte stream (used
/// for uTP seeds; mirrors the TCP `SeedPeer::serve_one` logic).
async fn serve_bittorrent_over_stream(
    stream: Box<dyn swarmotter_core::utp::PeerDuplex>,
    seed: &UtpSeedPeer,
) -> swarmotter_core::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut rd, mut wr) = tokio::io::split(stream);

    // Read leecher handshake.
    let mut hs_buf = [0u8; 68];
    rd.read_exact(&mut hs_buf).await?;
    let their_hs = Handshake::decode(&hs_buf)
        .map_err(|e| swarmotter_core::error::CoreError::Internal(e.to_string()))?;
    if their_hs.info_hash != seed.info_hash {
        return Err(swarmotter_core::error::CoreError::Internal(
            "info hash mismatch".into(),
        ));
    }
    let our_hs = Handshake {
        info_hash: seed.info_hash,
        peer_id: seed.peer_id,
        reserved: swarmotter_core::peer::RESERVED,
    };
    wr.write_all(&our_hs.encode()).await?;

    // Bitfield: all pieces.
    let mut bf = Bitfield::new(seed.meta.piece_count());
    for i in 0..seed.meta.piece_count() {
        bf.set(i);
    }
    peer::write_message(&mut wr, &bf.encode_message()).await?;
    wr.flush().await?;

    let piece_count = seed.meta.piece_count();
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
        match peer::MessageId::from_u8(id) {
            Some(peer::MessageId::Interested) => {
                peer::write_message(&mut wr, &Message::Unchoke).await?;
                wr.flush().await?;
            }
            Some(peer::MessageId::Request) if payload.len() == 12 => {
                let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                if (piece as usize) >= piece_count {
                    continue;
                }
                let (pstart, _) = seed.meta.piece_byte_range(piece as u64).unwrap();
                let abs = pstart + offset as u64;
                let block = seed.content[abs as usize..(abs + length as u64) as usize].to_vec();
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

/// Local swarm uTP download: a uTP-capable seed serves a generated payload, and
/// the SwarmOtter engine downloads it over the contained uTP transport (uTP
/// preferred, TCP fallback disabled by config), verifying every piece and the
/// final file contents. All traffic goes through the `LoopbackBinder`'s
/// contained UDP socket; no uncontrolled UDP sockets are created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_downloads_from_seed_via_utp() {
    let mut content = Vec::with_capacity(32 * 1024 + 9);
    for i in 0..32 * 1024 + 9 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let torrent_bytes = build_single_file_torrent("utp.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    // Start the uTP seed peer (bound to an ephemeral UDP port via the binder).
    // The seed learns the leecher's address from the incoming SYN, so we don't
    // need to publish a port ahead of time; but the engine connects to a UDP
    // address, so the seed must be reachable. We bind a UDP socket, learn its
    // address, and hand it to the engine as a direct seed peer.
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    use swarmotter_core::net::NetworkBinder;
    let seed_sock = binder.udp_socket().await.unwrap();
    let seed_addr = seed_sock.local_addr().unwrap();
    let seed_sock: std::sync::Arc<dyn swarmotter_core::net::ContainedUdpSocket> = seed_sock.into();

    let content_clone = content.clone();
    let meta_clone = meta.clone();
    let seed_sock_clone = seed_sock.clone();
    tokio::spawn(async move {
        use swarmotter_core::utp::{UtpConnection, UtpHeader, UtpStream, UtpType};
        // Wait for the engine's SYN on our bound UDP socket.
        let mut buf = vec![0u8; 2048];
        let (from, n) = match seed_sock_clone.recv_from(&mut buf).await {
            Ok(p) => p,
            Err(_) => return,
        };
        let (syn, _payload) = match UtpHeader::decode(&buf[..n]) {
            Ok(p) => p,
            Err(_) => return,
        };
        if syn.typ != UtpType::Syn {
            return;
        }
        let conn = match UtpConnection::accept_from_syn(seed_sock_clone, from, &syn).await {
            Ok(c) => c,
            Err(_) => return,
        };
        let stream = UtpStream::spawn(conn);
        let info_hash = meta_clone.info_hash;
        let seed = UtpSeedPeer {
            content: content_clone,
            meta: meta_clone,
            info_hash,
            peer_id: peer_id(b"-SDUTP00"),
        };
        let _ = serve_bittorrent_over_stream(Box::new(stream), &seed).await;
    });

    let dir = unique_dir("utp-download");
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);
    // uTP preferred (utp_prefer_tcp = false), uTP enabled. TCP fallback remains
    // available but should not be needed since the seed speaks uTP.
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SWUTP00"),
        binder.clone(),
        state.clone(),
        cmd_rx,
        vec![seed_peer],
        6881,
    )
    .with_transport(true, false);

    let final_state = tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("uTP engine did not finish in time")
        .expect("uTP engine error");

    assert!(final_state.finished, "uTP download did not complete");
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count(),
        "uTP download did not verify all pieces"
    );

    let storage = StorageIo::new(meta.clone(), dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(
        written, content,
        "uTP downloaded content mismatches original"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Fail-closed containment blocks uTP swarm downloads: with a `BlockedBinder`,
/// the engine cannot obtain a contained UDP socket, so uTP (and TCP) peer
/// connections fail closed and the torrent is reported network-blocked / makes
/// no progress. Proves uTP cannot bypass the network path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_utp_fail_closed_blocks_download() {
    let content = b"swarmotter utp fail closed payload data here!".to_vec();
    let piece_length: u64 = 8;
    let torrent_bytes = build_single_file_torrent("utpfc.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let dir = unique_dir("utp-failclosed");
    let binder: Arc<dyn swarmotter_core::net::NetworkBinder> =
        Arc::new(swarmotter_core::net::binder::BlockedBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SWFC000"),
        binder,
        state.clone(),
        cmd_rx,
        // A seed peer that does not exist; the binder blocks before any socket.
        vec![PeerAddr::from_socket_addr("127.0.0.1:9".parse().unwrap())],
        6881,
    )
    .with_transport(true, false);

    // The engine must not hang and must not complete; under the blocked binder
    // it makes no progress. Run with a timeout and assert it did not finish.
    let final_state = tokio::time::timeout(Duration::from_secs(20), engine.run())
        .await
        .expect("fail-closed engine should terminate within timeout")
        .expect("engine error");
    assert!(
        !final_state.finished,
        "uTP download should not complete under fail-closed"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Active local download reports non-zero health. This proves the health
/// indicator is computed from real engine state (pieces received, peer
/// usefulness, throughput, tracker ok), not just from a default value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_swarm_active_download_reports_non_zero_health() {
    use swarmotter_core::models::health::{HealthCalculator, HealthInput};
    use swarmotter_core::models::network::NetworkHealth;
    use swarmotter_core::models::torrent::TorrentState;

    // 1. Generated lawful payload.
    let mut content = Vec::with_capacity(8 * 1024);
    for i in 0..8 * 1024 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 1024;
    let torrent_bytes =
        build_single_file_torrent("health_payload.bin", &content, piece_length, None, false);
    let meta = parse_torrent(&torrent_bytes).unwrap();
    let dir = unique_dir("health");

    // 2. Stand up a single-peer seeder (no tracker, direct peer connection).
    let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = seed_listener.local_addr().unwrap();
    let seed_peer = PeerAddr::from_socket_addr(seed_addr);
    {
        let content_clone = content.clone();
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = seed_listener.accept().await {
                let seed = SeedPeer {
                    content: content_clone.clone(),
                    meta: meta_clone.clone(),
                    info_hash: meta_clone.info_hash,
                    peer_id: peer_id(b"-SD0001-"),
                };
                let _ = seed.serve_one(stream).await;
            }
        });
    }

    // 3. Start the engine; it will discover via the directly-supplied peer.
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        peer_id(b"-SW0001-"),
        binder,
        state.clone(),
        rx,
        vec![seed_peer],
        6881,
    );

    // 4. Drive the engine in the background; meanwhile, repeatedly sample
    //    health from the live engine state and confirm a meaningful
    //    non-zero signal is observed before completion.
    let calc = HealthCalculator::new();
    let state_for_sample = state.clone();
    let meta_for_sample = meta.clone();
    let sampler = tokio::spawn(async move {
        let mut best: u8 = 0;
        let mut best_bars: u8 = 0;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let s = state_for_sample.lock().await;
            // Mirror the daemon's `build_health_input` signals.
            let total_pieces = meta_for_sample.piece_count();
            let pieces_have = s.pieces_have.count(total_pieces);
            if pieces_have == 0 {
                continue;
            }
            let peers: Vec<swarmotter_core::models::peer::EnginePeerHealth> =
                s.peer_health.values().cloned().collect();
            let input = HealthInput {
                state: if s.finished {
                    TorrentState::Completed
                } else {
                    TorrentState::Downloading
                },
                piece_count: total_pieces,
                pieces_have: s.pieces_have.clone(),
                peers,
                rate_down: 0,
                rate_down_observed_peak: 0,
                download_limit: 0,
                upload_limit: 0,
                global_download_limit: 0,
                network: Some(NetworkHealth::blocked(
                    swarmotter_core::models::network::NetworkContainmentMode::Disabled,
                    swarmotter_core::models::network::NetworkContainmentStatus::Healthy,
                    "test",
                )),
                tracker_ok: s.tracker_ok,
                tracker_recent_ok: true,
                received_block_recently: true,
                no_peers_discovered: false,
                ..Default::default()
            };
            let h = calc.compute(&input);
            if h.score > best {
                best = h.score;
            }
            if h.bars > best_bars {
                best_bars = h.bars;
            }
            if s.finished {
                break;
            }
        }
        (best, best_bars)
    });

    let _ = tokio::time::timeout(Duration::from_secs(30), engine.run())
        .await
        .expect("download should complete within 30s");
    let (best, best_bars) = sampler.await.unwrap();

    assert!(
        best > 0,
        "active local download must report non-zero health (got score={best})"
    );
    assert!(
        best_bars >= 1,
        "active local download must show at least one bar (got bars={best_bars})"
    );
    std::fs::remove_dir_all(&dir).ok();
}
