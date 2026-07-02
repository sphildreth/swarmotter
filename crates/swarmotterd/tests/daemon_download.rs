// SPDX-License-Identifier: Apache-2.0

//! Daemon-level integration test: adding a torrent through the daemon runtime
//! drives the real engine, which announces to a local HTTP tracker, discovers
//! a local seed peer, downloads and verifies all pieces, writes them to disk,
//! and reports completion through the API-facing `DaemonOps` summary.
//!
//! Network containment is left at the default (disabled) so the contained
//! loopback path permits traffic; the engine still routes all sockets through
//! the `NetworkBinder` abstraction. See `design/testing.md` and ADR-0015.

#![allow(clippy::field_reassign_with_default)]

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use swarmotter_api::state::DaemonOps;
use swarmotter_core::config::Config;
use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, PeerAddr};
use swarmotterd::daemon::DaemonRuntime;

fn unique_dir(label: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-daemon-it-{}-{}-{}",
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

fn pick_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A minimal seed peer serving a known payload.
async fn spawn_seed(content: Vec<u8>, meta: swarmotter_core::meta::TorrentMeta) -> PeerAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let info_hash = meta.info_hash;
    let peer_id = peer_id(b"-SD0010-");
    let piece_count = meta.piece_count();
    tokio::spawn(async move {
        let seed_content = content.clone();
        let seed_meta = meta.clone();
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let content = seed_content.clone();
            let meta = seed_meta.clone();
            tokio::spawn(async move {
                let _ = serve_seed(stream, content, meta, info_hash, peer_id, piece_count).await;
            });
        }
    });
    PeerAddr::from_socket_addr(addr)
}

fn peer_id(prefix: &[u8; 8]) -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(prefix);
    id
}

async fn serve_seed(
    stream: tokio::net::TcpStream,
    content: Vec<u8>,
    meta: swarmotter_core::meta::TorrentMeta,
    info_hash: swarmotter_core::hash::InfoHash,
    peer_id: [u8; 20],
    piece_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut hs = [0u8; 68];
    rd.read_exact(&mut hs).await?;
    let their_hs = Handshake::decode(&hs).map_err(|e| e.to_string())?;
    if their_hs.info_hash != info_hash {
        return Err("info hash mismatch".into());
    }
    let our_hs = Handshake {
        info_hash,
        peer_id,
        reserved: swarmotter_core::peer::RESERVED,
    };
    wr.write_all(&our_hs.encode()).await?;
    let mut bf = Bitfield::new(piece_count);
    for i in 0..piece_count {
        bf.set(i);
    }
    peer::write_message(&mut wr, &bf.encode_message()).await?;
    wr.flush().await?;
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
                Err(e) => return Err(e.into()),
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
        if Some(peer::MessageId::Interested) == peer::MessageId::from_u8(id) {
            peer::write_message(&mut wr, &Message::Unchoke).await?;
            wr.flush().await?;
        } else if Some(peer::MessageId::Request) == peer::MessageId::from_u8(id)
            && payload.len() == 12
        {
            let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
            let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
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
    }
}

async fn spawn_tracker(addr: SocketAddr, seed: PeerAddr) {
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let seed = seed;
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let _ = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
                let mut peers = Vec::new();
                if let std::net::IpAddr::V4(v4) = seed.ip {
                    peers.extend_from_slice(&v4.octets());
                    peers.extend_from_slice(&seed.port.to_be_bytes());
                }
                let mut body = Vec::new();
                body.extend_from_slice(b"d8:intervali30e8:completei1e10:incompletei1e5:peers");
                body.extend_from_slice(format!("{}:", peers.len()).as_bytes());
                body.extend_from_slice(&peers);
                body.push(b'e');
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.write_all(&body).await;
                let _ = stream.flush().await;
            });
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_add_downloads_to_completion_via_engine() {
    // Generate a legal payload.
    let mut content = Vec::with_capacity(32 * 1024 + 7);
    for i in 0..32 * 1024 + 7 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let torrent_bytes = build_single_file_torrent(
        "daemon_payload.bin",
        &content,
        piece_length,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&torrent_bytes).unwrap();

    // Local seed peer + tracker.
    let seed = spawn_seed(content.clone(), meta.clone()).await;
    let tracker_addr: SocketAddr = format!("127.0.0.1:{tracker_port}").parse().unwrap();
    spawn_tracker(tracker_addr, seed).await;

    // Daemon with default (disabled) network containment and a temp download dir.
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    let healthy = swarmotter_core::models::network::NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let download_dir = unique_dir("daemon-dl");
    cfg.storage.download_dir = Some(download_dir.display().to_string());

    let runtime = std::sync::Arc::new(DaemonRuntime::new(cfg, healthy));

    // Add the torrent via the API-facing DaemonOps.
    let hash = runtime
        .add_torrent_file(torrent_bytes, Some(download_dir.display().to_string()))
        .await
        .unwrap();

    // Poll the summary until the torrent reports completion (or timeout).
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(summary) = runtime.get_torrent(&hash).await {
            if summary.state == swarmotter_core::models::torrent::TorrentState::Completed {
                assert_eq!(
                    summary.bytes_completed, summary.total_length,
                    "completed torrent should report full byte progress"
                );
                assert_eq!(summary.pieces_have, summary.piece_count);
                break;
            }
        }
        if std::time::Instant::now() > deadline {
            let s = runtime.get_torrent(&hash).await;
            panic!("daemon download did not complete in time; summary: {:?}", s);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Verify the on-disk file content.
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), download_dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);

    // Pause should stop the engine and move state to paused; resume should
    // restart it (already complete, so it stays completed).
    runtime.pause(&hash).await.unwrap();
    assert_eq!(
        runtime.get_torrent(&hash).await.unwrap().state,
        swarmotter_core::models::torrent::TorrentState::Paused
    );

    // Remove with delete_data should remove files and resume metadata.
    runtime.remove_torrent(&hash, true).await.unwrap();
    assert!(runtime.get_torrent(&hash).await.is_none());
    assert!(!storage.resume_path().exists());

    std::fs::remove_dir_all(&download_dir).ok();
}
