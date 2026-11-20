// SPDX-License-Identifier: Apache-2.0

//! Two production daemon instances with complementary verified pieces and no
//! complete seed complete a download by exchanging verified payload through
//! the contained peer wire paths (ADR-0075).
//!
//! Generated local payload: every byte is generated locally and no
//! public tracker or third-party content is contacted.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use swarmotter_api::handlers::events::EventBroker;
use swarmotter_api::state::{AddTorrentOptions, DaemonOps};
use swarmotter_core::config::Config;
use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::torrent::TorrentState;
use swarmotter_core::storage::StorageIo;
use swarmotterd::daemon::DaemonRuntime;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{oneshot, Barrier};

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

/// Each daemon has its own announce URL for the same info hash. Return only
/// its counterpart's loopback endpoint, after both engines have announced
/// their verified pieces and registered their inbound serving contexts.
async fn swarm_tracker(first_announces: Arc<Barrier>) -> (String, oneshot::Sender<u16>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let (port_tx, port_rx) = oneshot::channel::<u16>();
    tokio::spawn(async move {
        let port = port_rx.await.unwrap();
        let mut body = b"d8:intervali5e8:completei0e10:incompletei2e5:peers6:".to_vec();
        body.extend_from_slice(&std::net::Ipv4Addr::LOCALHOST.octets());
        body.extend_from_slice(&port.to_be_bytes());
        body.push(b'e');
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        ).into_bytes();
        response.extend_from_slice(&body);
        let mut ready = Some(first_announces);
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut headers = BufReader::new(&mut stream).lines();
            while let Ok(Some(line)) = headers.next_line().await {
                if line.is_empty() {
                    break;
                }
            }
            if let Some(barrier) = ready.take() {
                barrier.wait().await;
            }
            let _ = stream.write_all(&response).await;
        }
    });
    (url, port_tx)
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
    let layout = meta
        .requires_v2_data_plane()
        .then(|| meta.v2_piece_layout().unwrap());
    for piece in pieces {
        let start = piece * meta.piece_length as usize;
        let end = (start + meta.piece_length as usize).min(payload.len());
        if let Some(layout) = &layout {
            storage
                .write_v2_piece(layout, piece, &payload[start..end])
                .await
                .unwrap();
        } else {
            storage
                .write_piece(piece, &payload[start..end])
                .await
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_incomplete_daemons_exchange_verified_pieces_without_a_seed() {
    exchange(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pure_v2_two_daemons_exchange_and_restore_verified_pieces() {
    exchange(true).await;
}

fn pure_v2_metainfo(payload: &[u8], piece_len: u64, tracker: &str) -> Vec<u8> {
    let name = "verified-exchange.bin";
    let root = swarmotter_core::v2::v2_file_root(payload).unwrap();
    let mut data = format!(
        "d8:announce{}:{}4:infod9:file treed{}:{}d0:d6:lengthi{}e11:pieces root32:",
        tracker.len(),
        tracker,
        name.len(),
        name,
        payload.len()
    )
    .into_bytes();
    data.extend(root.as_bytes());
    data.extend(
        format!(
            "eee12:meta versioni2e4:name{}:{}12:piece lengthi{}ee12:piece layersd32:",
            name.len(),
            name,
            piece_len
        )
        .as_bytes(),
    );
    data.extend(root.as_bytes());
    let chunks = payload.chunks(piece_len as usize);
    data.extend(format!("{}:", chunks.len() * 32).as_bytes());
    for chunk in chunks {
        data.extend(swarmotter_core::v2::v2_leaf_hash(chunk).as_bytes());
    }
    data.extend(b"ee");
    data
}

async fn exchange(pure_v2: bool) {
    let piece_len = if pure_v2 { 16384 } else { 16 };
    let payload: Vec<u8> = (0..16 * piece_len as usize)
        .map(|i| (i * 37 % 251) as u8)
        .collect();

    let dir_a = unique_dir("peer-a");
    let dir_b = unique_dir("peer-b");
    let first_announces = Arc::new(Barrier::new(2));
    let (url_a, peer_for_a) = swarm_tracker(first_announces.clone()).await;
    let (url_b, peer_for_b) = swarm_tracker(first_announces).await;
    let metainfo = |url: &str| {
        if pure_v2 {
            pure_v2_metainfo(&payload, piece_len, url)
        } else {
            build_single_file_torrent(
                "verified-exchange.bin",
                &payload,
                piece_len,
                Some(url),
                false,
            )
        }
    };
    let torrent_a = metainfo(&url_a);
    let torrent_b = metainfo(&url_b);
    let piece_count = parse_torrent(&torrent_a)
        .unwrap()
        .data_piece_count()
        .unwrap();
    assert_eq!(piece_count, 16);

    // Complementary halves: A verifies pieces 0..8, B verifies 8..16.
    prewrite_pieces(&torrent_a, &dir_a, &payload, 0..8).await;
    prewrite_pieces(&torrent_b, &dir_b, &payload, 8..16).await;

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
        .add_torrent_file_with_options(torrent_a, AddTorrentOptions::new(None, false))
        .await
        .unwrap();
    let hash_b = runtime_b
        .add_torrent_file_with_options(torrent_b, AddTorrentOptions::new(None, false))
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
    peer_for_a.send(addr_b.port()).unwrap();
    peer_for_b.send(addr_a.port()).unwrap();

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
    assert!(
        uploaded_a >= payload.len() as u64 / 2,
        "a must have uploaded its half"
    );
    assert!(
        uploaded_b >= payload.len() as u64 / 2,
        "b must have uploaded its half"
    );

    runtime_a.shutdown().await.unwrap();
    runtime_b.shutdown().await.unwrap();
    if pure_v2 {
        let restored = DaemonRuntime::with_paths_broker_and_state(
            daemon_config(&dir_a, 0),
            daemon_health(),
            None,
            None,
            Some(dir_a.join("state.sqlite")),
            EventBroker::default(),
        );
        restored.restore_persisted_state().await.unwrap();
        assert_eq!(restored.get_torrent(&hash_a).await.unwrap().pieces_have, 16);
        let mut damaged = payload.clone();
        damaged[0] ^= 0xff;
        std::fs::write(dir_a.join("verified-exchange.bin"), damaged).unwrap();
        restored.recheck(&hash_a).await.unwrap();
        assert_eq!(restored.get_torrent(&hash_a).await.unwrap().pieces_have, 15);
        std::fs::remove_file(dir_a.join("verified-exchange.bin")).unwrap();
        restored.recheck(&hash_a).await.unwrap();
        assert_eq!(restored.get_torrent(&hash_a).await.unwrap().pieces_have, 0);
        restored.shutdown().await.unwrap();
    }
    std::fs::remove_dir_all(dir_a).ok();
    std::fs::remove_dir_all(dir_b).ok();
}
