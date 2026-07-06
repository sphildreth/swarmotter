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
use swarmotter_core::meta::{build_multi_file_torrent, build_single_file_torrent, parse_torrent};
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

async fn spawn_stalling_seed(
    meta: swarmotter_core::meta::TorrentMeta,
) -> (PeerAddr, tokio::sync::oneshot::Receiver<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let info_hash = meta.info_hash;
    let peer_id = peer_id(b"-SDSTAL-");
    let piece_count = meta.piece_count();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let _ = serve_stalling_seed(stream, info_hash, peer_id, piece_count, ready_tx).await;
    });
    (PeerAddr::from_socket_addr(addr), ready_rx)
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

async fn serve_stalling_seed(
    stream: tokio::net::TcpStream,
    info_hash: swarmotter_core::hash::InfoHash,
    peer_id: [u8; 20],
    piece_count: usize,
    ready: tokio::sync::oneshot::Sender<()>,
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

    let mut ready = Some(ready);
    loop {
        let mut len = [0u8; 4];
        rd.read_exact(&mut len).await?;
        let n = u32::from_be_bytes(len) as usize;
        if n == 0 {
            continue;
        }
        let mut body = vec![0u8; n];
        rd.read_exact(&mut body).await?;
        let id = body[0];
        if Some(peer::MessageId::Interested) == peer::MessageId::from_u8(id) {
            peer::write_message(&mut wr, &Message::Unchoke).await?;
            wr.flush().await?;
            if let Some(tx) = ready.take() {
                let _ = tx.send(());
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
            return Ok(());
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
async fn daemon_remove_active_torrent_delete_data_returns_promptly() {
    let content = vec![7u8; 128 * 1024];
    let piece_length: u64 = 16 * 1024;
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let torrent_bytes = build_single_file_torrent(
        "remove_active_payload.bin",
        &content,
        piece_length,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let (seed, peer_active) = spawn_stalling_seed(meta.clone()).await;
    let tracker_addr: SocketAddr = format!("127.0.0.1:{tracker_port}").parse().unwrap();
    spawn_tracker(tracker_addr, seed).await;

    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    cfg.dht.enabled = false;
    cfg.bandwidth.max_peers_per_torrent = 1;
    cfg.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    let healthy = swarmotter_core::models::network::NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let download_dir = unique_dir("daemon-remove-active");
    cfg.storage.download_dir = Some(download_dir.display().to_string());
    let runtime = std::sync::Arc::new(DaemonRuntime::new(cfg, healthy));

    let hash = runtime
        .add_torrent_file(torrent_bytes, Some(download_dir.display().to_string()))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), peer_active)
        .await
        .expect("engine should connect to the stalling peer")
        .expect("stalling peer should signal active session");

    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), download_dir.clone());
    assert!(
        storage.file_path(0).unwrap().exists(),
        "active torrent should create the visible incomplete payload path"
    );

    tokio::time::timeout(Duration::from_secs(3), runtime.remove_torrent(&hash, true))
        .await
        .expect("active torrent removal must not wait on the stalled peer")
        .expect("active torrent removal should succeed");
    assert!(runtime.get_torrent(&hash).await.is_none());
    assert!(
        !storage.file_path(0).unwrap().exists(),
        "delete_data = true must remove active payload data"
    );

    std::fs::remove_dir_all(&download_dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_remove_delete_data_preserves_storage_root_directories() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let torrent_bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let active_dir = unique_dir("daemon-remove-preserve-active-root");
    let complete_dir = unique_dir("daemon-remove-preserve-complete-root");
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    cfg.dht.enabled = false;
    cfg.queue.auto_start = false;
    cfg.storage.incomplete_dir = Some(active_dir.display().to_string());
    cfg.storage.download_dir = Some(complete_dir.display().to_string());
    let healthy = swarmotter_core::models::network::NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = std::sync::Arc::new(DaemonRuntime::new(cfg, healthy));

    let hash = runtime.add_torrent_file(torrent_bytes, None).await.unwrap();

    for root in [&active_dir, &complete_dir] {
        let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), root.clone());
        storage.preallocate().await.unwrap();
        storage.write_block(0, 0, b"hell").await.unwrap();
        storage.write_block(1, 0, b"owor").await.unwrap();
        storage.write_block(2, 0, b"ld!!").await.unwrap();
        assert!(root.join("dir").exists());
    }

    runtime.remove_torrent(&hash, true).await.unwrap();

    assert!(
        active_dir.exists(),
        "delete_data removal must preserve incomplete_dir"
    );
    assert!(
        complete_dir.exists(),
        "delete_data removal must preserve download_dir"
    );
    assert!(!active_dir.join("dir").exists());
    assert!(!complete_dir.join("dir").exists());

    std::fs::remove_dir_all(&active_dir).ok();
    std::fs::remove_dir_all(&complete_dir).ok();
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
    // DHT is disabled to keep the test offline and deterministic.
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    cfg.dht.enabled = false;
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
    assert!(
        !storage.file_path(0).unwrap().exists(),
        "delete_data = true must remove the payload file"
    );

    std::fs::remove_dir_all(&download_dir).ok();
}

/// Selfish completion policy (`torrent.selfish = true`): when a download
/// finishes, SwarmOtter must remove the torrent from the daemon and stop
/// seeding it, while preserving the downloaded data on disk. Equivalent to a
/// `remove_torrent(delete_data = false)` driven automatically on completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_selfish_mode_removes_completed_torrent_and_preserves_data() {
    // Generate a legal payload.
    let mut content = Vec::with_capacity(32 * 1024 + 7);
    for i in 0..32 * 1024 + 7 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let torrent_bytes = build_single_file_torrent(
        "selfish_payload.bin",
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

    // Daemon with selfish mode enabled, containment disabled, and a temp dir.
    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    cfg.dht.enabled = false;
    cfg.torrent.selfish = true;
    let healthy = swarmotter_core::models::network::NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let download_dir = unique_dir("daemon-selfish");
    cfg.storage.download_dir = Some(download_dir.display().to_string());

    let runtime = std::sync::Arc::new(DaemonRuntime::new(cfg, healthy));

    let hash = runtime
        .add_torrent_file(torrent_bytes, Some(download_dir.display().to_string()))
        .await
        .unwrap();

    // Poll until the torrent disappears from the daemon (selfish removal on
    // completion). It may briefly report Completed before being removed.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if runtime.get_torrent(&hash).await.is_none() {
            break;
        }
        if std::time::Instant::now() > deadline {
            let s = runtime.get_torrent(&hash).await;
            panic!(
                "selfish torrent was not removed after completion; summary: {:?}",
                s
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // It must no longer appear in the torrent list.
    assert!(
        runtime
            .list_torrents()
            .await
            .iter()
            .all(|t| t.info_hash != hash),
        "selfish mode must remove the torrent from the list"
    );

    // Downloaded data must be preserved with correct content (delete_data is
    // never invoked by selfish mode).
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), download_dir.clone());
    let payload_path = storage.file_path(0).unwrap();
    assert!(payload_path.exists(), "selfish mode must preserve payload");
    let written = std::fs::read(&payload_path).unwrap();
    assert_eq!(written, content, "preserved content must match the payload");

    std::fs::remove_dir_all(&download_dir).ok();
}

/// Regression: with the default `selfish = false`, a completed torrent stays
/// in the registry and continues to be managed (seeded), i.e. existing
/// completion/seeding behavior is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_selfish_disabled_keeps_completed_torrent() {
    let mut content = Vec::with_capacity(32 * 1024 + 7);
    for i in 0..32 * 1024 + 7 {
        content.push((i % 251) as u8);
    }
    let piece_length: u64 = 16 * 1024;
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let torrent_bytes = build_single_file_torrent(
        "kept_payload.bin",
        &content,
        piece_length,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&torrent_bytes).unwrap();

    let seed = spawn_seed(content.clone(), meta.clone()).await;
    let tracker_addr: SocketAddr = format!("127.0.0.1:{tracker_port}").parse().unwrap();
    spawn_tracker(tracker_addr, seed).await;

    let mut cfg = Config::default();
    cfg.network.mode = swarmotter_core::models::network::NetworkContainmentMode::Disabled;
    cfg.dht.enabled = false;
    assert!(!cfg.torrent.selfish, "default must be false");
    let healthy = swarmotter_core::models::network::NetworkHealth::blocked(
        swarmotter_core::models::network::NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let download_dir = unique_dir("daemon-noselfish");
    cfg.storage.download_dir = Some(download_dir.display().to_string());

    let runtime = std::sync::Arc::new(DaemonRuntime::new(cfg, healthy));

    let hash = runtime
        .add_torrent_file(torrent_bytes, Some(download_dir.display().to_string()))
        .await
        .unwrap();

    // Wait for completion and confirm the torrent remains in the registry.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(summary) = runtime.get_torrent(&hash).await {
            if summary.state == swarmotter_core::models::torrent::TorrentState::Completed {
                break;
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("download did not complete in time");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Completed torrent must still be listed and managed (seeding continues).
    assert!(
        runtime.get_torrent(&hash).await.is_some(),
        "non-selfish mode must keep the completed torrent in the registry"
    );
    assert!(
        runtime
            .list_torrents()
            .await
            .iter()
            .any(|t| t.info_hash == hash),
        "non-selfish mode must keep the completed torrent listed"
    );

    std::fs::remove_dir_all(&download_dir).ok();
}
