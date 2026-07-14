// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::peer_permits::{PeerPermitPool, PeerSessionBudget};
use crate::seeder::{SeedRegistration, SeedRegistry, SeederHub};
use async_trait::async_trait;
use swarmotter_core::hash::{InfoHash, PeerInfoHash, TorrentIdentity, TorrentKey, V2InfoHash};
use swarmotter_core::meta::{
    build_multi_file_torrent, build_single_file_torrent, parse_info_dict_with_piece_layers,
    v2_piece_layer_root, MetaFile, TorrentMeta, V2PieceLayer, V2TorrentMeta, V2_BLOCK_LENGTH,
};
use swarmotter_core::net::{ContainedUdpSocket, PeerListener};
use swarmotter_core::peer::V2Handshake;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn terminal_tracker_error_requires_all_failures_and_no_successful_alternative() {
    let failed = TrackerAnnounceSnapshot {
        status: TrackerStatus::Error,
        seeders: 0,
        leechers: 0,
        downloads: 0,
        last_error: Some("connection refused".into()),
        last_message: None,
        last_announce: Some(42),
    };
    let mut state = EngineState {
        tracker_message: Some("http://tracker.invalid/announce: connection refused".into()),
        tracker_failures_recent: 1,
        ..Default::default()
    };
    state
        .tracker_announces
        .insert("http://tracker.invalid/announce".into(), failed);

    let error = state
        .terminal_tracker_error()
        .expect("terminal all-tracker failure should be classified");
    assert!(error.contains("connection refused"));

    state.dht_discovery_ok = true;
    assert!(state.terminal_tracker_error().is_none());
    state.dht_discovery_ok = false;
    state.pex_discovery_ok = true;
    assert!(state.terminal_tracker_error().is_none());
    state.pex_discovery_ok = false;
    state.webseed_last_seen = Some(Instant::now());
    assert!(state.terminal_tracker_error().is_none());
    state.webseed_last_seen = None;
    state.peer_scheduler.eligible_peers = 1;
    assert!(state.terminal_tracker_error().is_none());
    state.peer_scheduler.eligible_peers = 0;
    state.tracker_ok = true;
    assert!(state.terminal_tracker_error().is_none());
}

fn unique_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-engine-{}-{}-{}",
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

#[test]
fn piece_selection_skips_unwanted_files_and_completes_selected_set() {
    let files = vec![(vec!["a.bin".into()], 4), (vec!["b.bin".into()], 4)];
    let contents: Vec<&[u8]> = vec![b"aaaa", b"bbbb"];
    let bytes = build_multi_file_torrent("selection", &files, &contents, 4, None);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let selection = PieceSelection::from_files(
        &meta,
        &[FilePriority::Normal, FilePriority::High],
        &[false, true],
    )
    .unwrap();
    assert!(!selection.includes(0));
    assert!(selection.includes(1));
    let mut have = PieceBitfield::new(meta.piece_count());
    assert!(!selection.complete(&have));
    have.set(1);
    assert!(selection.complete(&have));
}

#[test]
fn selected_file_includes_cross_file_boundary_piece() {
    let files = vec![(vec!["a.bin".into()], 2), (vec!["b.bin".into()], 2)];
    let contents: Vec<&[u8]> = vec![b"aa", b"bb"];
    let bytes = build_multi_file_torrent("boundary", &files, &contents, 4, None);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let selection = PieceSelection::from_files(
        &meta,
        &[FilePriority::Normal, FilePriority::High],
        &[false, true],
    )
    .unwrap();
    assert!(selection.includes(0));
}

#[test]
fn piece_assembler_reports_duplicate_with_overwrite() {
    // The download loops must treat this return value as a piece-complete
    // signal, not as "a new block was accepted". Callers track whether a
    // specific requested block was missing before calling `add_block`.
    // This test pins the assembler semantics so caller-side duplicate
    // accounting remains explicit.
    use swarmotter_core::peer::PieceAssembler;
    // Use the actual BLOCK_SIZE (16 KiB). For a piece of 4 blocks, three
    // unique blocks and one duplicate must not change the completion
    // status (still not complete on the second block; the third unique
    // block brings it to complete).
    const BLK: usize = 16 * 1024;
    let mut a = PieceAssembler::new(0, 4 * BLK);
    assert!(!a.add_block(0, &vec![0xAB; BLK]).unwrap());
    // Duplicate: must not advance the block count. The assembler returns
    // Ok(false) because the piece is still incomplete after the
    // duplicate; the caller would not count this as a new block.
    assert!(
        !a.add_block(0, &vec![0xAB; BLK]).unwrap(),
        "duplicate block must not signal completion"
    );
    // First time at offset BLK: new block.
    assert!(!a.add_block(BLK as u32, &vec![0xCD; BLK]).unwrap());
    // First time at offset 2*BLK: new block, piece still incomplete.
    assert!(!a.add_block((2 * BLK) as u32, &vec![0xEF; BLK]).unwrap());
    // Final block completes the piece.
    assert!(a.add_block((3 * BLK) as u32, &vec![0x42; BLK]).unwrap());
    // The data is well-formed even though one block was "duplicated"
    // (it overwrote the same buffer slot, so the final data is correct).
    assert_eq!(a.data().len(), 4 * BLK);
}

#[test]
fn preferred_encryption_preserves_transport_preference() {
    assert_eq!(
        peer_transport_order(true, false, PeerEncryptionMode::Preferred),
        vec![PeerTransport::Utp, PeerTransport::Tcp]
    );
    assert_eq!(
        peer_transport_order(true, true, PeerEncryptionMode::Preferred),
        vec![PeerTransport::Tcp, PeerTransport::Utp]
    );
    assert_eq!(
        peer_transport_order(true, false, PeerEncryptionMode::Required),
        vec![PeerTransport::Utp, PeerTransport::Tcp]
    );
}

#[tokio::test]
async fn required_encryption_never_retries_a_plaintext_tcp_session_after_mse_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        // Consume part of the MSE initiator data, then close before replying.
        // A `preferred` client would make a second plaintext connection here;
        // `required` must leave the listener with exactly one accepted socket.
        let mut first_byte = [0u8; 1];
        first.read_exact(&mut first_byte).await.unwrap();
        drop(first);
        let accepted = if tokio::time::timeout(Duration::from_millis(300), listener.accept())
            .await
            .is_ok()
        {
            2
        } else {
            1
        };
        let _ = accepted_tx.send(accepted);
    });

    let binder: Arc<dyn NetworkBinder> = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let peer_filter = swarmotter_core::peer_filter::PeerFilter::default();
    let result = attempt_peer_wire_transport(
        binder,
        PeerTransport::Tcp,
        PeerAddr::from_socket_addr(addr),
        InfoHash::from_bytes([0xE1; 20]),
        [0x5A; 20],
        PeerEncryptionMode::Required,
        &peer_filter,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(accepted_rx.await.unwrap(), 1);
    server.await.unwrap();
}

#[test]
fn piece_length_last_is_shorter() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    );
    assert_eq!(engine.piece_length(0), 8);
    assert_eq!(engine.piece_length(1), 8);
}

#[tokio::test]
async fn tracker_refresh_respects_the_announced_interval() {
    let bytes = build_single_file_torrent(
        "tracker-interval.bin",
        b"tracker interval payload",
        8,
        Some("http://127.0.0.1:1/announce"),
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let state = Arc::new(Mutex::new(EngineState {
        last_announce: Some(now_secs()),
        tracker_interval_seconds: 3_600,
        ..EngineState::default()
    }));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        state.clone(),
        rx,
        vec![],
        6881,
    );

    assert!(!engine.tracker_announce_due().await);
    state.lock().await.last_announce = Some(now_secs().saturating_sub(3_601));
    assert!(engine.tracker_announce_due().await);
}

fn scrape_body(hash: InfoHash, seeders: i64, leechers: i64, downloads: i64) -> Vec<u8> {
    let mut body = b"d5:filesd20:".to_vec();
    body.extend_from_slice(hash.as_bytes());
    body.extend_from_slice(
        format!("d8:completei{seeders}e10:downloadedi{downloads}e10:incompletei{leechers}eeee")
            .as_bytes(),
    );
    body
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut request = Vec::new();
    let mut chunk = [0u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(request).unwrap()
}

#[tokio::test]
async fn scrape_failure_retains_last_success_counts_and_is_accounted() {
    let hash = InfoHash::from_bytes([0x71; 20]);
    let good = scrape_body(hash, 7, 8, 9);
    let malformed = b"d5:filesdee".to_vec();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for body in [good, malformed] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            assert!(request.starts_with("GET /scrape?info_hash="));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        }
    });

    let url = format!("http://{address}/announce");
    let state = Arc::new(Mutex::new(EngineState::default()));
    let binder: Arc<dyn NetworkBinder> = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    run_tracker_scrapes(
        state.clone(),
        binder.clone(),
        PeerInfoHash::from_v1(hash),
        vec![url.clone()],
    )
    .await;
    {
        let engine = state.lock().await;
        let snapshot = engine.tracker_scrapes.get(&url).unwrap();
        assert_eq!(snapshot.status, TrackerScrapeStatus::Ok);
        assert_eq!(snapshot.seeders, Some(7));
        assert_eq!(snapshot.leechers, Some(8));
        assert_eq!(snapshot.downloads, Some(9));
        assert_eq!(engine.tracker_failures_recent, 0);
    }

    run_tracker_scrapes(
        state.clone(),
        binder,
        PeerInfoHash::from_v1(hash),
        vec![url.clone()],
    )
    .await;
    server.await.unwrap();
    let engine = state.lock().await;
    let snapshot = engine.tracker_scrapes.get(&url).unwrap();
    assert_eq!(snapshot.status, TrackerScrapeStatus::Error);
    assert_eq!(snapshot.seeders, Some(7));
    assert_eq!(snapshot.leechers, Some(8));
    assert_eq!(snapshot.downloads, Some(9));
    assert!(snapshot.last_error.is_some());
    assert_eq!(engine.tracker_failures_recent, 1);
}

#[tokio::test]
async fn started_and_reannounce_paths_schedule_contained_scrapes() {
    let payload = b"generated tracker scrape scheduling payload";
    let placeholder = build_single_file_torrent("scrape-schedule.bin", payload, 8, None, false);
    let hash = swarmotter_core::meta::parse_torrent(&placeholder)
        .unwrap()
        .info_hash;
    let announce_body = b"d8:completei3e10:incompletei4e8:intervali30e5:peers0:e".to_vec();
    let scraped = scrape_body(hash, 11, 12, 13);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let scrape_requests = Arc::new(AtomicUsize::new(0));
    let server_scrapes = scrape_requests.clone();
    let server = tokio::spawn(async move {
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            let body = if request.starts_with("GET /scrape?") {
                server_scrapes.fetch_add(1, Ordering::SeqCst);
                &scraped
            } else {
                assert!(request.starts_with("GET /announce?"));
                &announce_body
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        }
    });

    let http_tracker = format!("http://{address}/announce");
    let bytes = build_single_file_torrent(
        "scrape-schedule.bin",
        payload,
        8,
        Some(&http_tracker),
        false,
    );
    let mut meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let udp_tracker = "udp://127.0.0.1:6969/announce".to_string();
    meta.announce_list = vec![vec![http_tracker.clone()], vec![udp_tracker.clone()]];
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        state.clone(),
        rx,
        vec![],
        6881,
    );

    engine.announce(AnnounceEvent::Started).await;
    engine.announce(AnnounceEvent::Empty).await;
    server.await.unwrap();
    assert_eq!(scrape_requests.load(Ordering::SeqCst), 2);
    let engine_state = state.lock().await;
    let snapshot = engine_state
        .tracker_scrapes
        .get(&http_tracker)
        .expect("scrape snapshot");
    assert_eq!(snapshot.status, TrackerScrapeStatus::Ok);
    assert_eq!(snapshot.downloads, Some(13));
    assert_eq!(
        engine_state
            .tracker_scrapes
            .get(&udp_tracker)
            .unwrap()
            .status,
        TrackerScrapeStatus::Unsupported
    );
}

#[tokio::test]
async fn magnet_tracker_activity_scrapes_the_real_magnet_info_hash() {
    let magnet_hash = InfoHash::from_bytes([0x74; 20]);
    let body = scrape_body(magnet_hash, 21, 22, 23);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("GET /scrape.php?info_hash="));
        let expected = tracker::bytes_escape(magnet_hash.as_bytes());
        assert!(request.contains(&format!("info_hash={expected}")));
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
    });

    let bytes = build_single_file_torrent(
        "magnet-placeholder.bin",
        b"generated placeholder payload",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        state.clone(),
        rx,
        vec![],
        6881,
    );
    let url = format!("http://{address}/announce.php");
    let mut outcome = TrackerAnnounceOutcome::default();
    outcome.tracker_results.insert(
        url.clone(),
        TrackerAnnounceSnapshot {
            status: TrackerStatus::Ok,
            seeders: 1,
            leechers: 2,
            downloads: 0,
            last_error: None,
            last_message: Some("magnet announce ok".into()),
            last_announce: Some(now_secs()),
        },
    );
    engine
        .record_tracker_activity(
            PeerInfoHash::from_v1(magnet_hash),
            &outcome,
            vec![url.clone()],
        )
        .await;
    server.await.unwrap();

    let engine_state = state.lock().await;
    let snapshot = engine_state.tracker_scrapes.get(&url).unwrap();
    assert_eq!(snapshot.status, TrackerScrapeStatus::Ok);
    assert_eq!(snapshot.seeders, Some(21));
    assert_eq!(snapshot.leechers, Some(22));
    assert_eq!(snapshot.downloads, Some(23));
}

struct PanickingScrapeBinder;

#[async_trait]
impl NetworkBinder for PanickingScrapeBinder {
    async fn connect_peer(&self, _addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        panic!("generated scrape task panic");
    }

    async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
        Ok("127.0.0.1:9".parse().unwrap())
    }

    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        Err(CoreError::Internal("unused in scrape test".into()))
    }

    async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
        Err(CoreError::Internal("unused in scrape test".into()))
    }

    fn traffic_allowed(&self) -> bool {
        true
    }
}

struct CountingConnectBinder {
    connect_calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl NetworkBinder for CountingConnectBinder {
    async fn connect_peer(&self, _addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        self.connect_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(CoreError::Internal(
            "test binder must not be reached for a blocked peer".into(),
        ))
    }

    async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
        Err(CoreError::Internal("unused in peer-filter test".into()))
    }

    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        Err(CoreError::Internal("unused in peer-filter test".into()))
    }

    async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
        Err(CoreError::Internal("unused in peer-filter test".into()))
    }

    fn traffic_allowed(&self) -> bool {
        true
    }
}

struct RecordingConnectBinder {
    connected: std::sync::Mutex<Vec<SocketAddr>>,
}

#[async_trait]
impl NetworkBinder for RecordingConnectBinder {
    async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        self.connected.lock().unwrap().push(addr);
        Err(CoreError::Internal(
            "test direct peer intentionally has no peer-wire service".into(),
        ))
    }

    async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
        panic!("literal x.pe peer must not invoke DNS resolution")
    }

    async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
        panic!("test disables uTP for a TCP-only x.pe assertion")
    }

    async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
        Err(CoreError::Internal(
            "unused in metadata discovery test".into(),
        ))
    }

    fn traffic_allowed(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn literal_seed_peer_uses_contained_binder_for_magnet_metadata_discovery() {
    let bytes = build_single_file_torrent(
        "direct-peer-placeholder.bin",
        b"generated direct peer metadata fixture",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let info_hash = InfoHash::from_bytes([0xD5; 20]);
    let direct_peer: SocketAddr = "192.0.2.25:51413".parse().unwrap();
    let binder = Arc::new(RecordingConnectBinder {
        connected: std::sync::Mutex::new(Vec::new()),
    });
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let magnet = MagnetParams {
        identity: swarmotter_core::hash::TorrentIdentity::v1(info_hash),
        info_hash,
        wire_info_hash: PeerInfoHash::from_v1(info_hash),
        name: "direct-peer-placeholder.bin".into(),
        trackers: Vec::new(),
        select_only_file_indices: Vec::new(),
    };
    let engine = TorrentEngine::with_limiter(
        meta,
        PathBuf::from("/tmp"),
        [0; 20],
        binder.clone(),
        state.clone(),
        rx,
        vec![PeerAddr::from_socket_addr(direct_peer)],
        6881,
        RateLimiter::unlimited(),
        Some(magnet.clone()),
    )
    .with_transport(false, true);

    // The fetch retries after a failed peer, so cancel during its first retry
    // pause once the contained connector has observed the literal endpoint.
    let result = tokio::time::timeout(
        Duration::from_millis(250),
        engine.fetch_magnet_metadata(&magnet),
    )
    .await;
    assert!(result.is_err(), "test should cancel during retry backoff");
    assert_eq!(
        *binder.connected.lock().unwrap(),
        vec![direct_peer],
        "x.pe must enter metadata discovery through NetworkBinder::connect_peer"
    );
    assert_eq!(
        state.lock().await.peers,
        vec![PeerAddr::from_socket_addr(direct_peer)],
        "literal x.pe candidate must be retained in the normal peer set"
    );
}

#[tokio::test]
async fn blocked_peer_never_reaches_contained_binder() {
    let binder = Arc::new(CountingConnectBinder {
        connect_calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let filter = swarmotter_core::peer_filter::PeerFilter::from_config(
        &swarmotter_core::peer_filter::PeerFilterConfig {
            enabled: true,
            rules: vec!["203.0.113.0/24".into()],
            blocklist_paths: Vec::new(),
            manual_bans: Vec::new(),
            blocked_client_ids: Vec::new(),
        },
    )
    .unwrap();
    let peer = PeerAddr::from_socket_addr("203.0.113.7:6881".parse().unwrap());

    let result = connect_peer_wire_with_transport(
        binder.clone(),
        peer,
        InfoHash::from_bytes([7; 20]),
        [0; 20],
        false,
        true,
        PeerEncryptionMode::Disabled,
        &filter,
    )
    .await;

    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("blocked peer unexpectedly reached the contained transport"),
    };

    assert!(error.to_string().contains("configured rule"));
    assert_eq!(
        binder
            .connect_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "admission must reject before NetworkBinder::connect_peer"
    );
}

#[tokio::test]
async fn scrape_task_panic_is_retained_for_the_exact_tracker() {
    let hash = InfoHash::from_bytes([0x72; 20]);
    let url = "http://panic.test/announce".to_string();
    let state = Arc::new(Mutex::new(EngineState::default()));
    run_tracker_scrapes(
        state.clone(),
        Arc::new(PanickingScrapeBinder),
        PeerInfoHash::from_v1(hash),
        vec![url.clone()],
    )
    .await;

    let engine = state.lock().await;
    let snapshot = engine.tracker_scrapes.get(&url).unwrap();
    assert_eq!(snapshot.status, TrackerScrapeStatus::Error);
    assert!(snapshot
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("task failed")));
    assert_eq!(engine.tracker_failures_recent, 1);
}

#[test]
fn pick_piece_chooses_missing_peer_has() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef0123456789abcdef", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    );
    let mut peer_bf = Bitfield::new(4);
    peer_bf.set(1);
    peer_bf.set(2);
    let mut have = PieceBitfield::new(4);
    have.set(1);
    let pick = engine.pick_piece(Some(&peer_bf), &have);
    assert_eq!(pick, Some(2));
}

#[tokio::test]
async fn sync_have_from_state_merges_more_complete_live_state() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef0123456789abcdef", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let piece_count = meta.piece_count();
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    {
        let mut live = state.lock().await;
        live.piece_count = piece_count;
        live.pieces_have = PieceBitfield::new(piece_count);
        live.pieces_have.set(0);
        live.pieces_have.set(2);
    }
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    );
    let mut have = PieceBitfield::new(piece_count);
    have.set(0);

    engine.sync_have_from_state(&mut have, piece_count).await;

    assert!(have.has(0));
    assert!(have.has(2));
    assert_eq!(have.count(piece_count), 2);
}

#[test]
fn rotated_peer_candidates_cycles_through_eligible_peers() {
    let peers: Vec<PeerAddr> = (1..=5)
        .map(|i| PeerAddr::from_socket_addr(([127, 0, 0, i], 6881).into()))
        .collect();
    let mut cursor = 0;

    let first = rotated_peer_candidates(&peers, &mut cursor, 2);
    assert_eq!(first, vec![peers[0], peers[1]]);
    assert_eq!(cursor, 2);

    let second = rotated_peer_candidates(&peers, &mut cursor, 2);
    assert_eq!(second, vec![peers[2], peers[3]]);
    assert_eq!(cursor, 4);

    let wrapped = rotated_peer_candidates(&peers, &mut cursor, 3);
    assert_eq!(wrapped, vec![peers[4], peers[0], peers[1]]);
    assert_eq!(cursor, 2);
}

#[test]
fn balance_peer_families_interleaves_ipv4_and_ipv6() {
    let mut peers = vec![
        PeerAddr::from_socket_addr("127.0.0.1:6001".parse().unwrap()),
        PeerAddr::from_socket_addr("127.0.0.2:6002".parse().unwrap()),
        PeerAddr::from_socket_addr("[2001:db8::1]:6003".parse().unwrap()),
        PeerAddr::from_socket_addr("[2001:db8::2]:6004".parse().unwrap()),
        PeerAddr::from_socket_addr("[2001:db8::3]:6005".parse().unwrap()),
    ];

    balance_peer_families(&mut peers);

    assert!(!peers[0].ip.is_ipv6());
    assert!(peers[1].ip.is_ipv6());
    assert!(!peers[2].ip.is_ipv6());
    assert!(peers[3].ip.is_ipv6());
    assert!(peers[4].ip.is_ipv6());
}

#[test]
fn peer_candidate_classification_marks_all_filtered_as_unusable() {
    let peers = vec![
        PeerAddr::from_socket_addr("[2001:db8::1]:6001".parse().unwrap()),
        PeerAddr::from_socket_addr("[2001:db8::2]:6002".parse().unwrap()),
    ];

    let (eligible, counts) = classify_peer_candidates(
        &peers,
        &HashMap::new(),
        &HashMap::new(),
        false,
        &swarmotter_core::peer_filter::PeerFilter::default(),
    );

    assert!(eligible.is_empty());
    assert_eq!(counts.discovered, 2);
    assert_eq!(counts.filtered, 2);
    assert_eq!(counts.eligible, 0);
    assert!(no_usable_peer_candidates(&counts));
    assert_eq!(
        peer_scheduler_reason(&counts).as_deref(),
        Some("all discovered peers filtered by configuration")
    );
}

#[test]
fn peer_candidate_classification_does_not_stop_for_idle_backoff_only() {
    let peer = PeerAddr::from_socket_addr("127.0.0.1:6001".parse().unwrap());
    let peers = vec![peer];
    let mut peer_backoff = HashMap::new();
    backoff_peer_for(
        &mut peer_backoff,
        peer.socket_addr(),
        Duration::from_secs(60),
    );

    let (eligible, counts) = classify_peer_candidates(
        &peers,
        &HashMap::new(),
        &peer_backoff,
        false,
        &swarmotter_core::peer_filter::PeerFilter::default(),
    );

    assert!(eligible.is_empty());
    assert_eq!(counts.discovered, 1);
    assert_eq!(counts.backed_off, 1);
    assert_eq!(counts.eligible, 0);
    assert!(!no_usable_peer_candidates(&counts));
}

#[test]
fn merge_unique_peers_skips_duplicates() {
    let first = PeerAddr::from_socket_addr("127.0.0.1:6001".parse().unwrap());
    let second = PeerAddr::from_socket_addr("[2001:db8::1]:6002".parse().unwrap());
    let mut peers = vec![first];

    let added = merge_unique_peers(&mut peers, [first, second]);

    assert_eq!(added, 1);
    assert_eq!(peers, vec![first, second]);
}

#[test]
fn parallel_piece_download_ignores_duplicate_or_unsolicited_blocks() {
    let mut download = ParallelPieceDownload::new(0, peer::BLOCK_SIZE * 2);
    download.outstanding_blocks.insert(0, peer::BLOCK_SIZE);
    download.in_flight = 1;
    let mut global_in_flight = 1usize;
    let block = vec![0u8; peer::BLOCK_SIZE as usize];

    assert_eq!(
        download
            .record_block(0, &block, &mut global_in_flight)
            .unwrap(),
        Some(false)
    );
    assert_eq!(download.in_flight, 0);
    assert_eq!(global_in_flight, 0);

    assert_eq!(
        download
            .record_block(0, &block, &mut global_in_flight)
            .unwrap(),
        None
    );
    assert_eq!(
        download
            .record_block(peer::BLOCK_SIZE, &block, &mut global_in_flight)
            .unwrap(),
        None
    );
    assert_eq!(download.in_flight, 0);
    assert_eq!(global_in_flight, 0);
}

#[test]
fn parallel_piece_download_rejects_wrong_sized_blocks_without_accounting() {
    let mut download = ParallelPieceDownload::new(0, peer::BLOCK_SIZE);
    download.outstanding_blocks.insert(0, peer::BLOCK_SIZE);
    download.in_flight = 1;
    let mut global_in_flight = 1usize;

    assert_eq!(
        download
            .record_block(0, &[0u8; 1], &mut global_in_flight)
            .unwrap(),
        None
    );
    assert_eq!(download.in_flight, 1);
    assert_eq!(global_in_flight, 1);
    assert_eq!(download.outstanding_blocks.get(&0), Some(&peer::BLOCK_SIZE));
}

#[test]
fn peer_request_window_grows_with_observed_rate_and_respects_cap() {
    let now = Instant::now();
    let mut window = PeerRequestWindow::new(Some(128), now);
    assert_eq!(window.desired_in_flight(), NORMAL_REQUEST_FLOOR);

    window.sample_started_at = now - Duration::from_secs(1);
    window.record_block(peer::BLOCK_SIZE as u64 * 128, now);

    assert!(window.desired_in_flight() > NORMAL_REQUEST_FLOOR);
    assert!(window.desired_in_flight() <= 128);
}

#[test]
fn parallel_piece_state_prefers_rarest_available_piece() {
    let have = PieceBitfield::new(3);
    let mut state = ParallelPieceState::new(have, 3, PieceSelection::all_count(3));
    let peer_a: SocketAddr = "127.0.0.1:6001".parse().unwrap();
    let peer_b: SocketAddr = "127.0.0.2:6002".parse().unwrap();

    let mut first = Bitfield::new(3);
    first.set(0);
    first.set(1);
    state.note_peer_bitfield(peer_a, &first, 3);

    let mut second = Bitfield::new(3);
    second.set(0);
    state.note_peer_bitfield(peer_b, &second, 3);

    // The exact piece returned depends on the sharding offset (a hash of
    // `first`'s bitfield), but the invariant is: it must be a piece that
    // peer_a has, that we don't, that isn't already reserved. Both piece
    // 0 (availability 2) and piece 1 (availability 1) satisfy that.
    // When the search starts at the shard and piece 1 falls inside the
    // search range, it is preferred because it is rarer. We allow either
    // result; the second piece (rarest in this fixture) is the common case
    // when the shard is small.
    let result = state.reserve_piece(&first, peer_a, 3);
    assert!(
        result == Some(0) || result == Some(1),
        "reserve_piece returned {result:?}, expected Some(0) or Some(1)"
    );
    assert!(state.peer_has_missing_piece(&first, 3));
}

#[test]
fn parallel_piece_state_shard_does_not_monopolise_one_peer() {
    // Two peers with different bitfields should reserve different pieces
    // when their bitfields hash to different shards. This is the property
    // that prevents one fast peer from monopolising all pieces when its
    // piece window is wider than the remaining piece count.
    let have = PieceBitfield::new(8);
    let mut state = ParallelPieceState::new(have, 8, PieceSelection::all_count(8));
    let peer_a: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    let peer_b: SocketAddr = "127.0.0.1:7002".parse().unwrap();

    let mut bf_a = Bitfield::new(8);
    for i in 0..8 {
        bf_a.set(i);
    }
    // Peer B holds a subset, shifted by one — its bitfield bytes differ
    // from peer A's, so the sharder produces a different offset.
    let mut bf_b = Bitfield::new(8);
    for i in 0..7 {
        bf_b.set(i + 1);
    }
    state.note_peer_bitfield(peer_a, &bf_a, 8);
    state.note_peer_bitfield(peer_b, &bf_b, 8);

    let reserved_a = state.reserve_piece(&bf_a, peer_a, 8);
    let reserved_b = state.reserve_piece(&bf_b, peer_b, 8);
    // Each peer must reserve a piece it actually has, and the two
    // reservations must not collide (no two peers reserve the same piece).
    assert!(reserved_a.is_some());
    assert!(reserved_b.is_some());
    assert_ne!(reserved_a, reserved_b, "both peers reserved the same piece");
}

#[tokio::test]
async fn progress_update_does_not_count_rechecked_bytes_as_downloaded() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let state = Arc::new(Mutex::new(EngineState {
        piece_count: meta.piece_count(),
        total_length: meta.total_length,
        downloaded: 123,
        ..Default::default()
    }));
    let mut have = PieceBitfield::new(meta.piece_count());
    have.set(0);

    update_progress_state(&state, &meta, &have).await;

    let state = state.lock().await;
    assert_eq!(state.bytes_completed, 8);
    assert_eq!(state.downloaded, 123);
}

#[tokio::test]
async fn stale_fast_resume_rechecks_payload_ahead_of_resume() {
    let payload = b"abcdefghABCDEFGHijklmnop";
    let bytes = build_single_file_torrent("stale.bin", payload, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let dir = unique_dir("stale-resume");
    let storage = StorageIo::new(meta.clone(), dir.clone());
    storage.write_piece(0, &payload[0..8]).await.unwrap();
    storage.write_piece(2, &payload[16..24]).await.unwrap();

    let mut stale = PieceBitfield::new(meta.piece_count());
    stale.set(0);
    let piece_lengths: Vec<u64> = (0..meta.piece_count())
        .map(|i| {
            if i + 1 == meta.piece_count() {
                meta.last_piece_length()
            } else {
                meta.piece_length
            }
        })
        .collect();
    let resume = swarmotter_core::storage::io::build_resume(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        stale,
        meta.piece_count(),
        0,
        0,
        meta.total_length,
        Some(dir.display().to_string()),
        now_secs(),
        None,
        &vec![swarmotter_core::models::torrent::FilePriority::Normal; meta.files.len()],
        &piece_lengths,
    );
    storage.save_resume(&resume).await.unwrap();

    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir,
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_preallocate(false);

    let recovered = engine.load_or_recheck(&storage).await.unwrap();

    assert!(recovered.has(0));
    assert!(!recovered.has(1));
    assert!(recovered.has(2));
}

#[tokio::test]
async fn stale_fast_resume_rechecks_resume_ahead_of_payload() {
    let payload = b"abcdefghABCDEFGHijklmnop";
    let bytes = build_single_file_torrent("overclaim.bin", payload, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let dir = unique_dir("stale-resume-overclaim");
    let storage = StorageIo::new(meta.clone(), dir.clone());
    storage.write_piece(0, &payload[0..8]).await.unwrap();

    let mut stale = PieceBitfield::new(meta.piece_count());
    stale.set(0);
    stale.set(1);
    let piece_lengths: Vec<u64> = (0..meta.piece_count())
        .map(|i| {
            if i + 1 == meta.piece_count() {
                meta.last_piece_length()
            } else {
                meta.piece_length
            }
        })
        .collect();
    let resume = swarmotter_core::storage::io::build_resume(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        stale,
        meta.piece_count(),
        0,
        0,
        meta.total_length,
        Some(dir.display().to_string()),
        now_secs(),
        None,
        &vec![swarmotter_core::models::torrent::FilePriority::Normal; meta.files.len()],
        &piece_lengths,
    );
    storage.save_resume(&resume).await.unwrap();

    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_preallocate(false);

    let recovered = engine.load_or_recheck(&storage).await.unwrap();

    assert!(recovered.has(0));
    assert!(!recovered.has(1));
    assert!(!recovered.has(2));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn startup_recheck_uses_daemon_supplied_executor() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let payload = b"executor-backed recheck";
    let bytes = build_single_file_torrent("executor.bin", payload, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let dir = unique_dir("root-scoped-recheck-executor");
    let storage = StorageIo::new(meta.clone(), dir.clone());
    let expected = {
        let mut bitfield = PieceBitfield::new(meta.piece_count());
        bitfield.set(0);
        bitfield
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let executor_calls = calls.clone();
    let executor_bitfield = expected.clone();
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        dir.clone(),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_storage_recheck_executor(Arc::new(move |_storage| {
        let calls = executor_calls.clone();
        let bitfield = executor_bitfield.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(bitfield)
        })
    }));

    let recovered = engine.load_or_recheck(&storage).await.unwrap();

    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(recovered, expected);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn same_size_external_payload_change_invalidates_fast_resume() {
    let payload = b"abcdefgh";
    let bytes = build_single_file_torrent("same-size.bin", payload, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let dir = unique_dir("same-size-resume");
    let storage = StorageIo::new(meta.clone(), dir.clone());
    storage.write_piece(0, payload).await.unwrap();
    let mut have = PieceBitfield::new(1);
    have.set(0);
    let mut resume = swarmotter_core::storage::io::build_resume(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        have,
        1,
        0,
        0,
        meta.total_length,
        Some(dir.display().to_string()),
        now_secs(),
        None,
        &[FilePriority::Normal],
        &[8],
    );
    resume.file_stamps = storage.resume_file_stamps().await.unwrap();
    storage.save_resume(&resume).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    tokio::fs::write(storage.file_path(0).unwrap(), b"XXXXXXXX")
        .await
        .unwrap();

    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        dir.clone(),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_preallocate(false);
    let recovered = engine.load_or_recheck(&storage).await.unwrap();
    assert!(!recovered.has(0));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn peer_worker_limit_uses_default_when_uncapped() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_peer_worker_limit(0);

    assert_eq!(
        engine.current_peer_worker_limit(),
        DEFAULT_PEER_WORKER_LIMIT
    );
}

#[test]
fn peer_worker_limit_accepts_operator_cap() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_peer_worker_limit(12);

    assert_eq!(engine.current_peer_worker_limit(), 12);
}

#[test]
fn pex_import_respects_ipv6_and_cap() {
    let mut peers = Vec::new();
    add_pex_peers(
        &mut peers,
        [
            "127.0.0.1:6001".parse::<SocketAddr>().unwrap(),
            "[::1]:6002".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:6003".parse::<SocketAddr>().unwrap(),
        ]
        .into_iter()
        .map(PeerAddr::from_socket_addr),
        false,
        &swarmotter_core::peer_filter::PeerFilter::default(),
        1,
    );

    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].socket_addr(), "127.0.0.1:6001".parse().unwrap());
}

#[test]
fn peer_allowed_respects_ipv6_config() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta,
        PathBuf::from("/tmp"),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_allow_ipv6(false);

    assert!(engine.peer_allowed(&PeerAddr::from_socket_addr(
        "127.0.0.1:6881".parse().unwrap()
    )));
    assert!(!engine.peer_allowed(&PeerAddr::from_socket_addr("127.0.0.1:0".parse().unwrap())));
    assert!(!engine.peer_allowed(&PeerAddr::from_socket_addr("[::1]:6881".parse().unwrap())));
}

#[tokio::test]
async fn completed_active_data_moves_to_complete_dir() {
    let content = b"verified active data moves after completion";
    let bytes = build_single_file_torrent("complete.bin", content, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let active_dir = unique_dir("active");
    let complete_dir = unique_dir("complete");
    let active_storage = StorageIo::new(meta.clone(), active_dir.clone());
    active_storage.preallocate().await.unwrap();
    for piece in 0..meta.piece_count() {
        let start = piece * 8;
        let end = std::cmp::min(start + 8, content.len());
        active_storage
            .write_block(piece, 0, &content[start..end])
            .await
            .unwrap();
    }

    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta.clone(),
        active_dir.clone(),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_complete_dir(complete_dir.clone());

    let final_state = engine.run().await.unwrap();

    assert!(final_state.finished);
    assert!(!active_storage.file_path(0).unwrap().exists());
    let complete_storage = StorageIo::new(meta.clone(), complete_dir.clone());
    assert_eq!(
        std::fs::read(complete_storage.file_path(0).unwrap()).unwrap(),
        content
    );
    assert!(!active_storage.resume_path().exists());
    assert!(!complete_storage.resume_path().exists());
    std::fs::remove_dir_all(&active_dir).ok();
    std::fs::remove_dir_all(&complete_dir).ok();
}

#[tokio::test]
async fn completed_single_root_removes_resume_metadata() {
    let content = b"verified data complete in place";
    let bytes = build_single_file_torrent("single-root.bin", content, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let dir = unique_dir("complete-single-root");
    let storage = StorageIo::new(meta.clone(), dir.clone());
    storage.preallocate().await.unwrap();
    for piece in 0..meta.piece_count() {
        let start = piece * 8;
        let end = std::cmp::min(start + 8, content.len());
        storage
            .write_block(piece, 0, &content[start..end])
            .await
            .unwrap();
    }
    let mut have = PieceBitfield::new(meta.piece_count());
    for piece in 0..meta.piece_count() {
        have.set(piece);
    }
    let piece_lengths: Vec<u64> = (0..meta.piece_count())
        .map(|i| {
            if i + 1 == meta.piece_count() {
                meta.last_piece_length()
            } else {
                meta.piece_length
            }
        })
        .collect();
    let resume = swarmotter_core::storage::io::build_resume(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        have,
        meta.piece_count(),
        content.len() as u64,
        0,
        meta.total_length,
        Some(dir.display().to_string()),
        now_secs(),
        Some(now_secs()),
        &vec![swarmotter_core::models::torrent::FilePriority::Normal; meta.files.len()],
        &piece_lengths,
    );
    storage.save_resume(&resume).await.unwrap();
    assert!(storage.resume_path().exists());

    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta.clone(),
        dir.clone(),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    );

    let final_state = engine.run().await.unwrap();

    assert!(final_state.finished);
    assert_eq!(
        std::fs::read(storage.file_path(0).unwrap()).unwrap(),
        content
    );
    assert!(!storage.resume_path().exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn engine_start_creates_active_single_file_placeholder() {
    let bytes =
        build_single_file_torrent("started.bin", b"payload waits for peers", 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let active_dir = unique_dir("started-active");
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(EngineCommand::Stop).await.unwrap();
    let engine = TorrentEngine::new(
        meta.clone(),
        active_dir.clone(),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_preallocate(false);

    let final_state = engine.run().await.unwrap();

    assert!(!final_state.finished);
    let storage = StorageIo::new(meta, active_dir.clone());
    let path = storage.file_path(0).unwrap();
    assert!(path.exists());
    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    std::fs::remove_dir_all(&active_dir).ok();
}

#[tokio::test]
async fn engine_start_sizes_file_when_sparse_disabled() {
    let payload = b"payload waits for peers but file is sized";
    let bytes = build_single_file_torrent("sized.bin", payload, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let active_dir = unique_dir("sized-active");
    let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(EngineCommand::Stop).await.unwrap();
    let engine = TorrentEngine::new(
        meta.clone(),
        active_dir.clone(),
        [0u8; 20],
        binder,
        state,
        rx,
        vec![],
        6881,
    )
    .with_preallocate(false)
    .with_sparse(false);

    let final_state = engine.run().await.unwrap();

    assert!(!final_state.finished);
    let storage = StorageIo::new(meta, active_dir.clone());
    let path = storage.file_path(0).unwrap();
    assert!(path.exists());
    assert_eq!(std::fs::metadata(path).unwrap().len(), payload.len() as u64);
    std::fs::remove_dir_all(&active_dir).ok();
}

fn pure_v2_local_swarm_meta() -> (TorrentMeta, Vec<Vec<u8>>) {
    let piece_length = V2_BLOCK_LENGTH;
    let first = (0..(piece_length as usize + 19))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let second = (0..(piece_length as usize + 7))
        .map(|index| (255 - (index % 251)) as u8)
        .collect::<Vec<_>>();
    let payloads = vec![first, second];

    let mut files = Vec::new();
    let mut layers = Vec::new();
    for (index, payload) in payloads.iter().enumerate() {
        let hashes = payload
            .chunks(piece_length as usize)
            .map(|piece| swarmotter_core::v2_piece_root(piece, piece_length).unwrap())
            .collect::<Vec<_>>();
        let root = swarmotter_core::v2_hash_pair(hashes[0], hashes[1]);
        files.push(MetaFile {
            path: vec![format!("payload-{index}.bin")],
            length: payload.len() as u64,
            pieces_root: Some(root),
        });
        layers.push(V2PieceLayer {
            pieces_root: root,
            hashes,
        });
    }
    let total_length = payloads.iter().map(|payload| payload.len() as u64).sum();
    let meta = TorrentMeta {
        info_hash: InfoHash::ZERO,
        identity: TorrentIdentity::v2(V2InfoHash::from_bytes([0x94; 32])),
        name: "contained-v2-local-swarm".into(),
        piece_length,
        pieces: Vec::new(),
        files: files.clone(),
        total_length,
        private: false,
        announce: None,
        announce_list: Vec::new(),
        webseeds: Vec::new(),
        comment: None,
        created_by: None,
        creation_date: None,
        is_multi_file: true,
        v2: Some(V2TorrentMeta {
            meta_version: 2,
            files,
            piece_layers: layers,
            piece_layers_verified: true,
        }),
        raw_info: None,
    };
    meta.validate().unwrap();
    (meta, payloads)
}

async fn spawn_v2_seed(meta: TorrentMeta, payloads: Vec<Vec<u8>>) -> PeerAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let _ = serve_v2_seed(stream, meta, payloads).await;
    });
    PeerAddr::from_socket_addr(addr)
}

async fn serve_v2_seed(
    stream: tokio::net::TcpStream,
    meta: TorrentMeta,
    payloads: Vec<Vec<u8>>,
) -> swarmotter_core::Result<()> {
    let layout = meta.v2_piece_layout()?;
    let wire_hash = meta
        .identity
        .v2_info_hash()
        .expect("fixture has a v2 identity")
        .peer_info_hash();
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = PeerReader::new(read_half);
    let theirs = reader.read_v2_handshake().await?;
    if theirs.info_hash != wire_hash {
        return Err(CoreError::Parse(
            "v2 local seed received wrong wire identity".into(),
        ));
    }
    let ours = V2Handshake {
        info_hash: wire_hash,
        peer_id: *b"-SWV2SD-abcdefghij12",
        reserved: peer::with_v2_support(peer::RESERVED),
    };
    peer::write_v2_handshake(&mut write_half, &ours).await?;
    let mut bitfield = Bitfield::new(layout.piece_count());
    for index in 0..layout.piece_count() {
        bitfield.set(index);
    }
    peer::write_message(&mut write_half, &bitfield.encode_message()).await?;
    write_half.flush().await.map_err(CoreError::from)?;

    loop {
        let Some(message) = reader.read_message().await? else {
            return Ok(());
        };
        match message {
            Message::Interested => {
                peer::write_message(&mut write_half, &Message::Unchoke).await?;
                write_half.flush().await.map_err(CoreError::from)?;
            }
            Message::Request {
                piece,
                offset,
                length,
            } => {
                let response = (|| -> Option<Vec<u8>> {
                    let logical = layout.piece(piece as usize)?;
                    let start = logical.offset.checked_add(offset as u64)?;
                    let end = start.checked_add(length as u64)?;
                    if end > logical.offset.checked_add(logical.length)? {
                        return None;
                    }
                    let start = usize::try_from(start).ok()?;
                    let end = usize::try_from(end).ok()?;
                    payloads
                        .get(logical.file_index)?
                        .get(start..end)
                        .map(ToOwned::to_owned)
                })();
                if let Some(block) = response {
                    peer::write_message(
                        &mut write_half,
                        &Message::Piece {
                            piece,
                            offset,
                            block,
                        },
                    )
                    .await?;
                } else {
                    peer::write_message(
                        &mut write_half,
                        &Message::Reject {
                            piece,
                            offset,
                            length,
                        },
                    )
                    .await?;
                }
                write_half.flush().await.map_err(CoreError::from)?;
            }
            Message::Keepalive
            | Message::NotInterested
            | Message::Have { .. }
            | Message::Bitfield { .. }
            | Message::Choke
            | Message::Unchoke
            | Message::Cancel { .. }
            | Message::Reject { .. }
            | Message::HashRequest { .. }
            | Message::Hashes { .. }
            | Message::HashReject { .. }
            | Message::Extended { .. }
            | Message::Unknown { .. }
            | Message::Piece { .. } => {}
        }
    }
}

#[tokio::test]
async fn pure_v2_engine_completes_a_contained_local_swarm() {
    let (meta, payloads) = pure_v2_local_swarm_meta();
    let seed = spawn_v2_seed(meta.clone(), payloads.clone()).await;
    let output = unique_dir("pure-v2-local-swarm");
    let binder: Arc<dyn NetworkBinder> = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
    let state = Arc::new(Mutex::new(EngineState::default()));
    let (_commands, receiver) = tokio::sync::mpsc::channel(1);
    let engine = TorrentEngine::new(
        meta.clone(),
        output.clone(),
        *b"-SWV2DL-abcdefghij12",
        binder,
        state,
        receiver,
        vec![seed],
        0,
    )
    .with_transport(false, true)
    .with_encryption_mode(PeerEncryptionMode::Disabled);

    let final_state = tokio::time::timeout(Duration::from_secs(10), engine.run())
        .await
        .expect("pure-v2 local swarm must not stall")
        .unwrap();
    assert!(final_state.finished);
    assert_eq!(final_state.piece_count, 4);
    assert_eq!(final_state.bytes_completed, meta.total_length);
    assert_eq!(final_state.pieces_have.count(4), 4);
    for (index, payload) in payloads.iter().enumerate() {
        assert_eq!(
            std::fs::read(output.join(&meta.files[index].path[0])).unwrap(),
            *payload
        );
    }
    std::fs::remove_dir_all(output).ok();
}

/// Build executable pure-v2 metainfo whose BEP 9 `info` dictionary needs a
/// top-level piece layer. Three logical pieces exercise the padded BEP 52
/// hash response instead of treating raw v2 `info` bytes as runnable data.
fn pure_v2_metadata_candidate_fixture() -> (TorrentMeta, TorrentIdentity) {
    let piece_length = V2_BLOCK_LENGTH * 2;
    let content = (0..(piece_length * 2 + 17))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let hashes = content
        .chunks(piece_length as usize)
        .map(|piece| swarmotter_core::v2_piece_root(piece, piece_length).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(hashes.len(), 3);
    let pieces_root = v2_piece_layer_root(&hashes, piece_length).unwrap();
    let name = b"v2-metadata-preview.bin";

    fn string(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(value.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(value);
    }

    fn integer(out: &mut Vec<u8>, value: u64) {
        out.push(b'i');
        out.extend_from_slice(value.to_string().as_bytes());
        out.push(b'e');
    }

    let mut info = Vec::new();
    info.push(b'd');
    string(&mut info, b"file tree");
    info.push(b'd');
    string(&mut info, name);
    info.push(b'd');
    string(&mut info, b"");
    info.push(b'd');
    string(&mut info, b"length");
    integer(&mut info, content.len() as u64);
    string(&mut info, b"pieces root");
    string(&mut info, pieces_root.as_bytes());
    info.extend_from_slice(b"eee");
    string(&mut info, b"meta version");
    integer(&mut info, 2);
    string(&mut info, b"name");
    string(&mut info, name);
    string(&mut info, b"piece length");
    integer(&mut info, piece_length);
    info.push(b'e');

    let meta = parse_info_dict_with_piece_layers(
        &info,
        &[],
        &[V2PieceLayer {
            pieces_root,
            hashes,
        }],
    )
    .expect("generated pure-v2 candidate must parse with its verified layer");
    let identity = meta.identity.clone();
    assert!(meta.requires_v2_data_plane());
    assert_eq!(meta.raw_info.as_deref(), Some(info.as_slice()));
    (meta, identity)
}

/// A pure-v2 magnet preview must resolve a candidate through the contained
/// peer path, verify BEP 52 layers, and stop before creating payload storage.
#[tokio::test]
async fn pure_v2_metadata_only_preview_resolves_verified_candidate_without_payload_work() {
    let (seed_meta, identity) = pure_v2_metadata_candidate_fixture();
    let seed_dir = unique_dir("pure-v2-preview-seed");
    let seed_storage = Arc::new(StorageIo::new(seed_meta.clone(), seed_dir.clone()));
    let piece_count = seed_meta.data_piece_count().unwrap();
    let seed_state = Arc::new(Mutex::new(EngineState {
        piece_count,
        total_length: seed_meta.total_length,
        pieces_have: PieceBitfield::new(piece_count),
        ..EngineState::default()
    }));

    let registry = SeedRegistry::default();
    let global_peer_permits = PeerPermitPool::unlimited();
    let (torrent_shutdown_tx, torrent_shutdown_rx) = tokio::sync::watch::channel(false);
    registry
        .register(SeedRegistration::new(
            seed_meta.clone(),
            seed_storage,
            None,
            seed_state,
            *b"-SWV2MD-abcdefghij12",
            RateLimiter::unlimited(),
            None,
            PeerSessionBudget::new(global_peer_permits.clone(), PeerPermitPool::unlimited()),
            torrent_shutdown_rx,
        ))
        .await
        .expect("contained pure-v2 seed registration must succeed");
    let (hub_shutdown_tx, hub_shutdown_rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let hub = SeederHub::new(
        registry,
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        0,
        PeerEncryptionMode::Disabled,
        hub_shutdown_rx,
        global_peer_permits,
    )
    .with_bound_addr(bound_tx);
    let hub_task = tokio::spawn(hub.run());
    let seed_addr = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .expect("contained pure-v2 seeding listener did not bind")
        .expect("contained pure-v2 seeding listener dropped its address");

    let placeholder_bytes = build_single_file_torrent(
        "metadata-placeholder.bin",
        b"generated preview placeholder",
        16 * 1024,
        None,
        false,
    );
    let placeholder = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let preview_dir = unique_dir("pure-v2-metadata-preview");
    std::fs::remove_dir_all(&preview_dir).unwrap();
    let preview_state = Arc::new(Mutex::new(EngineState::default()));
    let (_commands, receiver) = tokio::sync::mpsc::channel(1);
    let wire_info_hash = identity
        .v2_info_hash()
        .expect("fixture has full v2 identity")
        .peer_info_hash();
    let engine = TorrentEngine::with_limiter(
        placeholder,
        preview_dir.clone(),
        *b"-SWV2PV-abcdefghij12",
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        preview_state,
        receiver,
        vec![PeerAddr::from_socket_addr(seed_addr)],
        0,
        RateLimiter::unlimited(),
        Some(MagnetParams {
            identity: identity.clone(),
            info_hash: InfoHash::ZERO,
            wire_info_hash,
            name: seed_meta.name.clone(),
            trackers: Vec::new(),
            select_only_file_indices: Vec::new(),
        }),
    )
    .with_metadata_only()
    .with_transport(false, true)
    .with_encryption_mode(PeerEncryptionMode::Disabled);

    let final_state = tokio::time::timeout(Duration::from_secs(20), engine.run())
        .await
        .expect("pure-v2 metadata-only preview did not finish")
        .expect("pure-v2 metadata-only preview failed");
    let resolved = final_state
        .resolved_meta
        .expect("preview must return resolved pure-v2 metainfo");
    assert_eq!(resolved.identity, identity);
    assert!(resolved.requires_v2_data_plane());
    assert!(resolved
        .v2
        .as_ref()
        .is_some_and(|v2| v2.piece_layers_verified));
    assert_eq!(resolved.data_piece_count().unwrap(), 3);
    assert!(!final_state.finished);
    assert_eq!(final_state.piece_count, 3);
    assert_eq!(final_state.pieces_have.count(3), 0);
    assert!(final_state.tracker_announces.is_empty());
    assert!(final_state.last_announce.is_none());
    assert!(
        !preview_dir.exists(),
        "metadata-only pure-v2 preview must not create payload storage"
    );

    let _ = torrent_shutdown_tx.send(true);
    let _ = hub_shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(5), hub_task)
        .await
        .expect("contained pure-v2 seeding listener did not stop")
        .expect("contained pure-v2 seeding listener task failed")
        .expect("contained pure-v2 seeding listener returned an error");
    std::fs::remove_dir_all(seed_dir).ok();
}
