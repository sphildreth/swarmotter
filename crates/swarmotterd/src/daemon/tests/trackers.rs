// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn list_trackers_exposes_scrape_state_and_falls_back_without_announce_success() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let primary = "http://tracker.example/announce";
    let secondary = "http://backup.example/announce.php";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "trackers.bin",
        b"0123456789abcdef",
        8,
        Some(primary),
        false,
    );
    let mut meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    meta.announce_list = vec![vec![primary.into(), secondary.into()]];
    let hash = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();

    let mut state = EngineState::default();
    state.tracker_announces.insert(
        primary.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Ok,
            explicit_failure: false,
            seeders: 256,
            leechers: 12,
            downloads: 0,
            last_error: None,
            last_message: Some("announce returned 64 peers".into()),
            last_announce: Some(1234),
        },
    );
    state.tracker_announces.insert(
        secondary.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Error,
            explicit_failure: false,
            seeders: 0,
            leechers: 0,
            downloads: 0,
            last_error: Some("tracker announce timed out".into()),
            last_message: None,
            last_announce: Some(1235),
        },
    );
    state.tracker_scrapes.insert(
        primary.into(),
        crate::engine::TrackerScrapeSnapshot {
            status: TrackerScrapeStatus::Ok,
            seeders: Some(300),
            leechers: Some(20),
            downloads: Some(99),
            last_error: None,
            last_scrape: Some(1240),
        },
    );
    state.tracker_scrapes.insert(
        secondary.into(),
        crate::engine::TrackerScrapeSnapshot {
            status: TrackerScrapeStatus::Error,
            seeders: Some(40),
            leechers: Some(5),
            downloads: Some(6),
            last_error: Some("latest scrape was malformed".into()),
            last_scrape: Some(1241),
        },
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(state)));

    let trackers = runtime.list_trackers(&hash).await.unwrap();
    let primary_row = trackers.iter().find(|t| t.url == primary).unwrap();
    assert_eq!(primary_row.status, TrackerStatus::Ok);
    assert_eq!(primary_row.seeders, 256);
    assert_eq!(primary_row.leechers, 12);
    assert_eq!(primary_row.downloads, 99);
    assert_eq!(primary_row.last_error, None);
    assert_eq!(
        primary_row.last_message.as_deref(),
        Some("announce returned 64 peers")
    );
    assert_eq!(primary_row.last_announce, Some(1234));
    assert_eq!(primary_row.scrape_status, TrackerScrapeStatus::Ok);
    assert_eq!(primary_row.last_scrape, Some(1240));
    assert_eq!(primary_row.scrape_seeders, Some(300));
    assert_eq!(primary_row.scrape_leechers, Some(20));
    assert_eq!(primary_row.scrape_downloads, Some(99));
    assert_eq!(primary_row.tier, 0);

    let secondary_row = trackers.iter().find(|t| t.url == secondary).unwrap();
    assert_eq!(secondary_row.status, TrackerStatus::Error);
    assert_eq!(
        secondary_row.last_error.as_deref(),
        Some("tracker announce timed out")
    );
    assert_eq!(secondary_row.last_message, None);
    assert_eq!(secondary_row.seeders, 40);
    assert_eq!(secondary_row.leechers, 5);
    assert_eq!(secondary_row.downloads, 6);
    assert_eq!(secondary_row.scrape_status, TrackerScrapeStatus::Error);
    assert_eq!(secondary_row.last_scrape, Some(1241));
    assert_eq!(secondary_row.scrape_seeders, Some(40));
    assert_eq!(secondary_row.scrape_leechers, Some(5));
    assert_eq!(secondary_row.scrape_downloads, Some(6));
    assert_eq!(
        secondary_row.last_scrape_error.as_deref(),
        Some("latest scrape was malformed")
    );
    assert_eq!(secondary_row.tier, 0);
}

#[tokio::test]
async fn seeder_announce_schedules_scrape_into_the_shared_engine_state() {
    let hash = InfoHash::from_bytes([0x73; 20]);
    let announce_body = b"d8:completei5e10:incompletei6e8:intervali30e5:peers0:e".to_vec();
    let mut scrape_body = b"d5:filesd20:".to_vec();
    scrape_body.extend_from_slice(hash.as_bytes());
    scrape_body.extend_from_slice(b"d8:completei15e10:downloadedi17e10:incompletei16eeee");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            let body = if request.starts_with("GET /scrape?") {
                &scrape_body
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

    let url = format!("http://{address}/announce");
    let state = Arc::new(Mutex::new(EngineState::default()));
    let interval = DaemonRuntime::seeder_announce_once(
        &[vec![url.clone()]],
        TorrentKey::v1(hash),
        [0u8; 20],
        6881,
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        state.clone(),
        AnnounceEvent::Started,
    )
    .await;
    server.await.unwrap();

    assert_eq!(interval, 30);
    let engine = state.lock().await;
    assert_eq!(
        engine.tracker_announces.get(&url).unwrap().status,
        TrackerStatus::Ok
    );
    let scrape = engine.tracker_scrapes.get(&url).unwrap();
    assert_eq!(scrape.status, TrackerScrapeStatus::Ok);
    assert_eq!(scrape.seeders, Some(15));
    assert_eq!(scrape.leechers, Some(16));
    assert_eq!(scrape.downloads, Some(17));
}

#[tokio::test]
async fn tracker_scrape_snapshot_serializes_through_the_real_native_router() {
    use axum::body::Body;
    use swarmotter_api::state::{
        AppState, BuildInfo, QbittorrentCompatState, TransmissionCompatState,
    };
    use tower::ServiceExt as _;

    let config = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = Arc::new(DaemonRuntime::new(config.clone(), health));
    let tracker_url = "http://tracker.example/announce";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "router-scrape.bin",
        b"generated router scrape payload",
        8,
        Some(tracker_url),
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();
    let mut engine = EngineState::default();
    engine.tracker_scrapes.insert(
        tracker_url.into(),
        crate::engine::TrackerScrapeSnapshot {
            status: TrackerScrapeStatus::Ok,
            seeders: Some(31),
            leechers: Some(32),
            downloads: Some(33),
            last_error: None,
            last_scrape: Some(34),
        },
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(engine)));

    let app_state = Arc::new(AppState {
        daemon: runtime,
        config: Arc::new(Mutex::new(config)),
        build: BuildInfo::default(),
        broker: EventBroker::default(),
        transmission: TransmissionCompatState::default(),
        qbittorrent: QbittorrentCompatState::default(),
    });
    let response = swarmotter_api::app_router(app_state)
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/api/v1/torrents/{hash}/trackers"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let row = &envelope["data"][0];
    assert_eq!(row["scrape_status"], "ok");
    assert_eq!(row["last_scrape"], 34);
    assert_eq!(row["scrape_seeders"], 31);
    assert_eq!(row["scrape_leechers"], 32);
    assert_eq!(row["scrape_downloads"], 33);
    assert_eq!(row["last_scrape_error"], serde_json::Value::Null);
    assert_eq!(row["seeders"], 31);
    assert_eq!(row["leechers"], 32);
    assert_eq!(row["downloads"], 33);
}
