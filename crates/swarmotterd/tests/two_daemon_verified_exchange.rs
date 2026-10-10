// SPDX-License-Identifier: Apache-2.0

//! Two production daemon instances with complementary verified pieces and no
//! complete seed complete a download by exchanging verified payload through
//! the contained peer wire paths (ADR-0075).
//!
//! Lawful generated payload only: every byte is generated locally and no
//! public tracker or third-party content is contacted.

use std::path::{Path, PathBuf};
use std::time::Duration;

use swarmotter_api::handlers::events::EventBroker;
use swarmotter_api::state::{AddTorrentOptions, DaemonOps};
use swarmotter_core::config::Config;
use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::torrent::TorrentState;
use swarmotter_core::peer::PeerAddr;
use swarmotter_core::storage::StorageIo;
use swarmotterd::daemon::DaemonRuntime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn unique_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-two-daemon-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A minimal local HTTP tracker that answers every announce with a compact
/// peer list containing BOTH swarm members.
async fn run_two_peer_tracker(
    listener: tokio::net::TcpListener,
    peers: std::sync::Arc<tokio::sync::RwLock<Vec<PeerAddr>>>,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let peers = peers.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let _ = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
            let mut encoded_peers = Vec::new();
            let peers = loop {
                let peers = peers.read().await.clone();
                if !peers.is_empty() {
                    break peers;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            for peer in &peers {
                if let std::net::IpAddr::V4(v4) = peer.ip {
                    encoded_peers.extend_from_slice(&v4.octets());
                    encoded_peers.extend_from_slice(&peer.port.to_be_bytes());
                }
            }
            let mut body = Vec::new();
            body.extend_from_slice(b"d8:intervali5e8:completei0e10:incompletei2e5:peers");
            body.extend_from_slice(format!("{}:", encoded_peers.len()).as_bytes());
            body.extend_from_slice(&encoded_peers);
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

fn daemon_config(dir: &Path, listen_port: u16) -> Config {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(dir.display().to_string());
    cfg.torrent.listen_port = listen_port;
    cfg.torrent.utp_enabled = false;
    cfg.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    cfg.dht.enabled = false;
    cfg.pex.enabled = false;
    cfg
}

fn daemon_health() -> NetworkHealth {
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    health
}

/// Write one torrent's verified piece subset into its active download
/// directory so the engine's startup recheck establishes exactly those
/// pieces as verified.
async fn prewrite_pieces(
    torrent_bytes: &[u8],
    download_dir: &Path,
    payload: &[u8],
    pieces: std::ops::Range<usize>,
) {
    let meta = parse_torrent(torrent_bytes).unwrap();
    let storage = StorageIo::new(meta.clone(), download_dir.to_path_buf());
    for piece in pieces {
        let start = piece * meta.piece_length as usize;
        let end = (start + meta.piece_length as usize).min(payload.len());
        storage
            .write_piece(piece, &payload[start..end])
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_incomplete_daemons_exchange_verified_pieces_without_a_seed() {
    // Generated lawful payload: 16 pieces of 16 bytes.
    let payload: Vec<u8> = (0..256usize).map(|i| (i * 37 % 251) as u8).collect();
    let piece_len = 16u64;

    let tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tracker_port = tracker.local_addr().unwrap().port();
    let dir_a = unique_dir("peer-a");
    let dir_b = unique_dir("peer-b");

    let tracker_peers = std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new()));
    let tracker_peers_for_task = tracker_peers.clone();
    tokio::spawn(async move {
        let _ = run_two_peer_tracker(tracker, tracker_peers_for_task).await;
    });

    let torrent_bytes = build_single_file_torrent(
        "verified-exchange.bin",
        &payload,
        piece_len,
        Some(&format!("http://127.0.0.1:{tracker_port}/announce")),
        false,
    );
    let piece_count = parse_torrent(&torrent_bytes).unwrap().piece_count();
    assert_eq!(piece_count, 16);

    // Complementary halves: A verifies pieces 0..8, B verifies 8..16.
    prewrite_pieces(&torrent_bytes, &dir_a, &payload, 0..8).await;
    prewrite_pieces(&torrent_bytes, &dir_b, &payload, 8..16).await;

    let runtime_a = DaemonRuntime::with_paths_broker_and_state(
        daemon_config(&dir_a, 0),
        daemon_health(),
        None,
        None,
        Some(dir_a.join("state.sqlite")),
        EventBroker::default(),
    );
    let runtime_b = DaemonRuntime::with_paths_broker_and_state(
        daemon_config(&dir_b, 0),
        daemon_health(),
        None,
        None,
        Some(dir_b.join("state.sqlite")),
        EventBroker::default(),
    );

    let hash_a = runtime_a
        .add_torrent_file_with_options(torrent_bytes.clone(), AddTorrentOptions::new(None, false))
        .await
        .unwrap();
    let hash_b = runtime_b
        .add_torrent_file_with_options(torrent_bytes.clone(), AddTorrentOptions::new(None, false))
        .await
        .unwrap();
    assert_eq!(
        hash_a, hash_b,
        "identical payload must produce one identity"
    );

    runtime_a.start_now(&hash_a).await.unwrap();
    runtime_b.start_now(&hash_b).await.unwrap();
    let addr_a = runtime_a.seeder_listener_addr().await.unwrap();
    let addr_b = runtime_b.seeder_listener_addr().await.unwrap();
    *tracker_peers.write().await = vec![
        PeerAddr {
            ip: addr_a.ip(),
            port: addr_a.port(),
        },
        PeerAddr {
            ip: addr_b.ip(),
            port: addr_b.port(),
        },
    ];

    // Both daemons must reach full completion by exchanging their verified
    // halves; neither has a complete seed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let state_a = runtime_a.get_torrent(&hash_a).await.map(|s| s.state);
        let state_b = runtime_b.get_torrent(&hash_b).await.map(|s| s.state);
        if state_a == Some(TorrentState::Seeding) && state_b == Some(TorrentState::Seeding) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let summary_a = runtime_a.get_torrent(&hash_a).await;
            let summary_b = runtime_b.get_torrent(&hash_b).await;
            panic!(
                "two-daemon verified exchange did not complete in time: a={state_a:?} ({summary_a:?}) b={state_b:?} ({summary_b:?})"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Final payload integrity: each daemon's data matches the generated
    // payload byte for byte.
    let stored_a = std::fs::read(dir_a.join("verified-exchange.bin")).unwrap();
    let stored_b = std::fs::read(dir_b.join("verified-exchange.bin")).unwrap();
    assert_eq!(stored_a, payload);
    assert_eq!(stored_b, payload);

    // Accounting sanity: each daemon uploaded at least its half. Read the
    // live engine states; the completed-transfer registry counters are also
    // updated by the engine task wrapper.
    let uploaded_a = runtime_a.engine_uploaded_bytes(&hash_a).await;
    let uploaded_b = runtime_b.engine_uploaded_bytes(&hash_b).await;
    println!("uploaded: a={uploaded_a} b={uploaded_b}");
    assert!(uploaded_a >= 128, "a must have uploaded its half");
    assert!(uploaded_b >= 128, "b must have uploaded its half");

    std::fs::remove_dir_all(dir_a).ok();
    std::fs::remove_dir_all(dir_b).ok();
}
