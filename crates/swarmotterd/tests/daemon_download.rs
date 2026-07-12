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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tower::ServiceExt;

use swarmotter_api::state::{AddTorrentOptions, AppState, BuildInfo, DaemonOps};
use swarmotter_core::config::Config;
use swarmotter_core::meta::{build_multi_file_torrent, build_single_file_torrent, parse_torrent};
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::torrent::{SeedingStatus, TorrentState};
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

fn app_state(runtime: Arc<DaemonRuntime>, config: Config) -> swarmotter_api::state::SharedState {
    let daemon: Arc<dyn DaemonOps> = runtime;
    Arc::new(AppState {
        daemon,
        config: Arc::new(Mutex::new(config)),
        build: BuildInfo::default(),
        broker: swarmotter_api::handlers::events::EventBroker::default(),
        transmission: swarmotter_api::state::TransmissionCompatState::default(),
        qbittorrent: swarmotter_api::state::QbittorrentCompatState::default(),
    })
}

#[tokio::test]
async fn api_torrent_file_add_retains_envelope_and_shared_rollback_contract() {
    let root = unique_dir("api-add-rollback");
    let invalid_state_path = root.join("state-target");
    std::fs::create_dir_all(&invalid_state_path).unwrap();
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let bytes = build_single_file_torrent(
        "api-shared-add.bin",
        b"generated api shared-add rollback payload",
        8,
        None,
        false,
    );
    let expected_hash = parse_torrent(&bytes).unwrap().info_hash;
    let failed_runtime = Arc::new(DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health.clone(),
        None,
        None,
        Some(invalid_state_path),
        swarmotter_api::handlers::events::EventBroker::default(),
    ));
    let response = swarmotter_api::app_router(app_state(failed_runtime.clone(), config.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file?paused=true")
                .header("content-type", "application/x-bittorrent")
                .body(Body::from(bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["success"], false);
    assert_eq!(value["data"], serde_json::Value::Null);
    assert_eq!(value["error"]["code"], "storage_error");
    assert!(failed_runtime.list_torrents().await.is_empty());
    let scheduler = failed_runtime.global_stats().await.scheduler;
    assert_eq!(scheduler.queued_torrents, 0);
    assert_eq!(scheduler.requested_downloads, 0);

    let successful_runtime = Arc::new(DaemonRuntime::new(config.clone(), health));
    let response = swarmotter_api::app_router(app_state(successful_runtime.clone(), config))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/torrents/file?paused=true")
                .header("content-type", "application/x-bittorrent")
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["success"], true);
    assert_eq!(value["data"], expected_hash.to_hex());
    assert_eq!(value["error"], serde_json::Value::Null);
    assert!(successful_runtime
        .get_torrent(&expected_hash)
        .await
        .is_some());
    std::fs::remove_dir_all(root).ok();
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

async fn spawn_tracker_many(addr: SocketAddr, seeds: Vec<PeerAddr>) {
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let seeds = seeds.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let _ = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
                let mut peers = Vec::new();
                for seed in seeds {
                    if let std::net::IpAddr::V4(v4) = seed.ip {
                        peers.extend_from_slice(&v4.octets());
                        peers.extend_from_slice(&seed.port.to_be_bytes());
                    }
                }
                let mut body = Vec::new();
                body.extend_from_slice(b"d8:intervali30e8:completei3e10:incompletei1e5:peers");
                body.extend_from_slice(format!("{}:", peers.len()).as_bytes());
                body.extend_from_slice(&peers);
                body.push(b'e');
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(&body).await;
                let _ = stream.flush().await;
            });
        }
    });
}

struct ActiveSessionGuard(Arc<AtomicUsize>);

impl Drop for ActiveSessionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn spawn_generated_stalling_swarm(
    piece_counts: HashMap<swarmotter_core::hash::InfoHash, usize>,
    peer_count: usize,
) -> (Vec<PeerAddr>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let piece_counts = Arc::new(piece_counts);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut peers = Vec::new();
    for _ in 0..peer_count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        peers.push(PeerAddr::from_socket_addr(listener.local_addr().unwrap()));
        let piece_counts = piece_counts.clone();
        let active = active.clone();
        let peak = peak.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let piece_counts = piece_counts.clone();
                let active = active.clone();
                let peak = peak.clone();
                tokio::spawn(async move {
                    let current = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(current, Ordering::AcqRel);
                    let _guard = ActiveSessionGuard(active);
                    let _ = serve_generated_stalling_peer(stream, piece_counts).await;
                });
            }
        });
    }
    (peers, active, peak)
}

async fn serve_generated_stalling_peer(
    stream: tokio::net::TcpStream,
    piece_counts: Arc<HashMap<swarmotter_core::hash::InfoHash, usize>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut read, mut write) = tokio::io::split(stream);
    let mut encoded = [0u8; 68];
    read.read_exact(&mut encoded).await?;
    let handshake = Handshake::decode(&encoded).map_err(|error| error.to_string())?;
    let piece_count = *piece_counts
        .get(&handshake.info_hash)
        .ok_or("unknown generated torrent")?;
    write
        .write_all(
            &Handshake {
                info_hash: handshake.info_hash,
                peer_id: peer_id(b"-CAPSEED"),
                reserved: swarmotter_core::peer::RESERVED,
            }
            .encode(),
        )
        .await?;
    let mut bitfield = Bitfield::new(piece_count);
    for piece in 0..piece_count {
        bitfield.set(piece);
    }
    peer::write_message(&mut write, &bitfield.encode_message()).await?;
    write.flush().await?;
    loop {
        let mut length = [0u8; 4];
        read.read_exact(&mut length).await?;
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 {
            continue;
        }
        let mut body = vec![0u8; length];
        read.read_exact(&mut body).await?;
        if peer::MessageId::from_u8(body[0]) == Some(peer::MessageId::Interested) {
            peer::write_message(&mut write, &Message::Unchoke).await?;
            write.flush().await?;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn five_torrent_local_swarm_samples_one_process_wide_peer_cap() {
    let root = unique_dir("global-peer-cap");
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let mut torrents = Vec::new();
    let mut piece_counts = HashMap::new();
    for index in 0..5u8 {
        let content = vec![index.wrapping_add(1); 8 * 4096];
        let bytes = build_single_file_torrent(
            &format!("generated-cap-{index}.bin"),
            &content,
            4096,
            Some(&tracker_url),
            false,
        );
        let meta = parse_torrent(&bytes).unwrap();
        piece_counts.insert(meta.info_hash, meta.piece_count());
        torrents.push(bytes);
    }
    let (seeds, active_seed_sessions, peak_seed_sessions) =
        spawn_generated_stalling_swarm(piece_counts, 3).await;
    spawn_tracker_many(format!("127.0.0.1:{tracker_port}").parse().unwrap(), seeds).await;

    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.display().to_string());
    config.torrent.listen_port = 0;
    config.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    config.dht.enabled = false;
    config.pex.enabled = false;
    config.queue.max_active_downloads = 5;
    config.bandwidth.max_peers = 2;
    config.bandwidth.max_peers_per_torrent = 3;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = Arc::new(DaemonRuntime::new(config, health));
    let mut hashes = Vec::new();
    for bytes in torrents {
        hashes.push(runtime.add_torrent_file(bytes, None).await.unwrap());
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut sampled_peak = 0usize;
    let mut saw_parallel_path = false;
    loop {
        let scheduler = runtime.global_stats().await.scheduler;
        assert_eq!(scheduler.peer_limit, 2);
        assert!(scheduler.peer_permits_in_use <= 2);
        assert_eq!(
            scheduler.peer_permits_available,
            Some(2 - scheduler.peer_permits_in_use)
        );
        sampled_peak = sampled_peak.max(scheduler.peer_permits_in_use);
        for hash in &hashes {
            saw_parallel_path |= runtime
                .torrent_stats(hash)
                .await
                .and_then(|stats| stats.peer_scheduler)
                .is_some_and(|peer| peer.parallel_workers_started >= 2);
        }
        if sampled_peak == 2 && active_seed_sessions.load(Ordering::Acquire) == 2 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{scheduler:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Sample while sessions are live, not only after completion, so a brief
    // oversubscription cannot hide behind final zero-valued diagnostics.
    for _ in 0..100 {
        let scheduler = runtime.global_stats().await.scheduler;
        assert!(scheduler.peer_permits_in_use <= 2, "{scheduler:?}");
        assert!(active_seed_sessions.load(Ordering::Acquire) <= 2);
        sampled_peak = sampled_peak.max(scheduler.peer_permits_in_use);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(sampled_peak, 2);
    assert_eq!(peak_seed_sessions.load(Ordering::Acquire), 2);
    assert!(
        saw_parallel_path,
        "generated swarm did not enter the parallel path"
    );

    for hash in hashes {
        runtime.remove_torrent(&hash, false).await.unwrap();
    }
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_torrent_product_path_obeys_per_torrent_cap_below_global_cap() {
    let root = unique_dir("per-torrent-peer-cap");
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let content = vec![0x31; 8 * 4096];
    let bytes = build_single_file_torrent(
        "generated-per-torrent-cap.bin",
        &content,
        4096,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&bytes).unwrap();
    let (seeds, active, peak) =
        spawn_generated_stalling_swarm(HashMap::from([(meta.info_hash, meta.piece_count())]), 3)
            .await;
    spawn_tracker_many(format!("127.0.0.1:{tracker_port}").parse().unwrap(), seeds).await;
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.display().to_string());
    config.torrent.listen_port = 0;
    config.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    config.dht.enabled = false;
    config.pex.enabled = false;
    config.bandwidth.max_peers = 5;
    config.bandwidth.max_peers_per_torrent = 1;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = Arc::new(DaemonRuntime::new(config, health));
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let scheduler = runtime.global_stats().await.scheduler;
        assert!(scheduler.peer_permits_in_use <= 1, "{scheduler:?}");
        assert!(active.load(Ordering::Acquire) <= 1);
        if scheduler.peer_permits_in_use == 1 && active.load(Ordering::Acquire) == 1 {
            assert_eq!(scheduler.peer_limit, 5);
            assert_eq!(scheduler.peer_permits_available, Some(4));
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{scheduler:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for _ in 0..50 {
        assert!(runtime.global_stats().await.scheduler.peer_permits_in_use <= 1);
        assert!(active.load(Ordering::Acquire) <= 1);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(peak.load(Ordering::Acquire), 1);
    runtime.remove_torrent(&hash, false).await.unwrap();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn live_session_cap_replacement_has_no_old_new_pool_overlap() {
    let root = unique_dir("live-peer-cap-replacement");
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let content = vec![0x62; 8 * 4096];
    let bytes = build_single_file_torrent(
        "generated-live-cap-replacement.bin",
        &content,
        4096,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&bytes).unwrap();
    let (seeds, _active, _peak) =
        spawn_generated_stalling_swarm(HashMap::from([(meta.info_hash, meta.piece_count())]), 3)
            .await;
    spawn_tracker_many(format!("127.0.0.1:{tracker_port}").parse().unwrap(), seeds).await;
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.display().to_string());
    config.torrent.listen_port = pick_port();
    config.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    config.dht.enabled = false;
    config.pex.enabled = false;
    config.bandwidth.max_peers = 3;
    config.bandwidth.max_peers_per_torrent = 3;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = Arc::new(DaemonRuntime::new(config, health));
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();
    let (old_global, old_torrent) = runtime.peer_permit_pools_for_test(&hash).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while old_global.snapshot().in_use != 3 || old_torrent.snapshot().in_use != 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    let update_runtime = runtime.clone();
    let mut bandwidth = runtime.get_config().await.bandwidth;
    bandwidth.max_peers = 1;
    bandwidth.max_peers_per_torrent = 1;
    let update = tokio::spawn(async move {
        update_runtime
            .update_settings(swarmotter_api::state::SettingsPatch {
                bandwidth: Some(bandwidth),
                ..Default::default()
            })
            .await
    });
    let mut observed_candidate = false;
    while !update.is_finished() {
        let (current_global, current_torrent) =
            runtime.peer_permit_pools_for_test(&hash).await.unwrap();
        assert!(old_global.snapshot().in_use <= 3);
        assert!(old_torrent.snapshot().in_use <= 3);
        if !Arc::ptr_eq(&current_global, &old_global) {
            observed_candidate = true;
            assert_eq!(old_global.snapshot().in_use, 0);
            assert_eq!(old_torrent.snapshot().in_use, 0);
            assert!(current_global.snapshot().in_use <= 1);
            assert!(current_torrent.snapshot().in_use <= 1);
        }
        tokio::task::yield_now().await;
    }
    update.await.unwrap().unwrap();
    let (new_global, new_torrent) = runtime.peer_permit_pools_for_test(&hash).await.unwrap();
    assert!(!Arc::ptr_eq(&new_global, &old_global));
    assert!(!Arc::ptr_eq(&new_torrent, &old_torrent));
    assert_eq!(old_global.snapshot().in_use, 0);
    assert_eq!(old_torrent.snapshot().in_use, 0);
    tokio::time::timeout(Duration::from_secs(10), async {
        while new_global.snapshot().in_use != 1 || new_torrent.snapshot().in_use != 1 {
            assert!(new_global.snapshot().in_use <= 1);
            assert!(new_torrent.snapshot().in_use <= 1);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(observed_candidate || new_global.snapshot().in_use == 1);
    runtime.remove_torrent(&hash, false).await.unwrap();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_endgame_sessions_hold_all_applicable_permits() {
    let root = unique_dir("endgame-peer-cap");
    let tracker_port = pick_port();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    // Exactly four missing pieces selects the production endgame branch.
    let content = vec![0x47; 4 * 4096];
    let bytes = build_single_file_torrent(
        "generated-endgame-cap.bin",
        &content,
        4096,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&bytes).unwrap();
    assert!(swarmotter_core::endgame::is_endgame(meta.piece_count()));
    let (seeds, active, peak) =
        spawn_generated_stalling_swarm(HashMap::from([(meta.info_hash, meta.piece_count())]), 3)
            .await;
    spawn_tracker_many(format!("127.0.0.1:{tracker_port}").parse().unwrap(), seeds).await;
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.storage.download_dir = Some(root.display().to_string());
    config.torrent.listen_port = 0;
    config.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    config.dht.enabled = false;
    config.pex.enabled = false;
    config.bandwidth.max_peers = 3;
    config.bandwidth.max_peers_per_torrent = 3;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = Arc::new(DaemonRuntime::new(config, health));
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let scheduler = runtime.global_stats().await.scheduler;
        assert!(scheduler.peer_permits_in_use <= 3, "{scheduler:?}");
        if scheduler.peer_permits_in_use == 3 && active.load(Ordering::Acquire) == 3 {
            assert_eq!(scheduler.peer_permits_available, Some(0));
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{scheduler:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for _ in 0..50 {
        assert_eq!(
            runtime.global_stats().await.scheduler.peer_permits_in_use,
            3
        );
        assert_eq!(active.load(Ordering::Acquire), 3);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(peak.load(Ordering::Acquire), 3);
    runtime.remove_torrent(&hash, false).await.unwrap();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_endpoint_with_real_daemon_clears_torrent_query_results() {
    let root = unique_dir("reset-route");
    let download_dir = root.join("downloads");
    let incomplete_dir = root.join("incomplete");
    std::fs::create_dir_all(&download_dir).unwrap();
    std::fs::create_dir_all(&incomplete_dir).unwrap();

    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(download_dir.display().to_string());
    cfg.storage.incomplete_dir = Some(incomplete_dir.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = Arc::new(DaemonRuntime::with_paths(cfg.clone(), health, None, None));
    let torrent_bytes =
        build_single_file_torrent("reset-route.bin", b"reset route payload", 8, None, false);
    let hash = DaemonOps::add_torrent_file(
        runtime.as_ref(),
        torrent_bytes,
        AddTorrentOptions::new(None, true),
    )
    .await
    .unwrap();
    assert!(runtime.get_torrent(&hash).await.is_some());

    let app = swarmotter_api::app_router(app_state(runtime.clone(), cfg));
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/reset")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["data"]["torrents_removed"], 1);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/torrents/query?per_page=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["data"]["total"], 0);
    assert!(value["data"]["rows"].as_array().unwrap().is_empty());
    assert!(runtime.list_torrents().await.is_empty());

    std::fs::remove_dir_all(&root).ok();
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
    cfg.torrent.listen_port = 0;
    cfg.bandwidth.max_peers = 1;
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
    let scheduler = runtime.global_stats().await.scheduler;
    assert_eq!(scheduler.peer_limit, 1);
    assert_eq!(scheduler.peer_permits_in_use, 1);
    assert_eq!(scheduler.peer_permits_available, Some(0));

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
    assert_eq!(
        runtime.global_stats().await.scheduler.peer_permits_in_use,
        0
    );
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
    cfg.torrent.listen_port = 0;
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

    // Poll until full verification reaches either queued seeding or an
    // acquired live seed slot.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(summary) = runtime.get_torrent(&hash).await {
            if (summary.state, summary.seeding_status)
                == (TorrentState::Seeding, SeedingStatus::Active)
            {
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
    let files = runtime.list_files(&hash).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].bytes_completed, files[0].length);
    let stats = runtime.global_stats().await;
    assert_eq!(stats.active_seeds, 1);

    // Verify the on-disk file content.
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), download_dir.clone());
    let written = std::fs::read(storage.file_path(0).unwrap()).unwrap();
    assert_eq!(written, content);

    // Pause should stop the engine and move state to paused; resume should
    // resume it through the complete-content seeding path.
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
    cfg.torrent.listen_port = 0;
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
    cfg.torrent.listen_port = 0;
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

    // Wait for queued/active seeding and confirm the torrent remains managed.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let completed = loop {
        if let Some(summary) = runtime.get_torrent(&hash).await {
            if (summary.state, summary.seeding_status)
                == (TorrentState::Seeding, SeedingStatus::Active)
            {
                break summary;
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("download did not complete in time");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!(completed.bytes_completed, completed.total_length);
    assert_eq!(completed.pieces_have, completed.piece_count);
    assert_eq!(runtime.global_stats().await.active_seeds, 1);

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
