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
            last_success: None,
            retries: 0,
            latency_ms: None,
            peers_returned: None,
            recent_errors: Vec::new(),
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
            last_success: None,
            retries: 0,
            latency_ms: None,
            peers_returned: None,
            recent_errors: Vec::new(),
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

#[tokio::test]
async fn tracker_policy_sample_fields_and_measured_zero_are_explicit() {
    use swarmotter_core::config::Config;
    use swarmotter_core::models::network::{NetworkContainmentMode, NetworkHealth};
    use swarmotter_core::models::tracker::{TrackerPriority, TrackerSampleStatus};
    use swarmotter_core::policy::{PolicyProfile, PolicyTracker, QueuePriority, TrackerHostRule};

    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.profiles.profiles.insert(
        "ops".into(),
        PolicyProfile {
            tracker: PolicyTracker {
                host_rules: vec![
                    TrackerHostRule {
                        host_pattern: "backup.example".into(),
                        enabled: None,
                        priority: Some(QueuePriority::High),
                    },
                    TrackerHostRule {
                        host_pattern: "disabled.example".into(),
                        enabled: Some(false),
                        priority: None,
                    },
                ],
            },
            ..Default::default()
        },
    );
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(config, health);
    let primary = "http://primary.example/announce";
    let backup = "http://backup.example/announce";
    let disabled = "http://disabled.example/announce";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "policy.bin",
        b"policy payload",
        8,
        Some(primary),
        false,
    );
    let mut meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    meta.announce_list = vec![vec![primary.into(), backup.into(), disabled.into()]];
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta, 1);
    torrent.policy.profile = Some("ops".into());
    runtime.registry.lock().await.add(torrent).unwrap();
    let now = now();
    let mut state = EngineState {
        tracker_interval_seconds: 300,
        ..EngineState::default()
    };
    state.tracker_announces.insert(
        primary.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Ok,
            explicit_failure: false,
            seeders: 0,
            leechers: 0,
            downloads: 0,
            last_error: None,
            last_message: Some("announce returned 0 peers".into()),
            last_announce: Some(now),
            last_success: Some(now),
            retries: 0,
            latency_ms: Some(17),
            peers_returned: Some(0),
            recent_errors: Vec::new(),
        },
    );
    state.tracker_announces.insert(
        backup.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Error,
            explicit_failure: false,
            seeders: 0,
            leechers: 0,
            downloads: 0,
            last_error: Some("timeout".into()),
            last_message: None,
            last_announce: Some(now),
            last_success: None,
            retries: 2,
            latency_ms: Some(30000),
            peers_returned: None,
            recent_errors: vec!["timeout".into()],
        },
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(state)));

    let trackers = runtime.list_trackers(&hash).await.unwrap();
    let primary_row = trackers.iter().find(|row| row.url == primary).unwrap();
    assert!(primary_row.allowed);
    assert_eq!(primary_row.effective_priority, TrackerPriority::Normal);
    assert_eq!(primary_row.sample_status, TrackerSampleStatus::Current);
    assert_eq!(primary_row.peers_returned, Some(0));
    assert_eq!(primary_row.latency_ms, Some(17));
    let backup_row = trackers.iter().find(|row| row.url == backup).unwrap();
    assert!(backup_row.allowed);
    assert_eq!(backup_row.effective_priority, TrackerPriority::High);
    assert_eq!(backup_row.sample_status, TrackerSampleStatus::Unavailable);
    assert_eq!(backup_row.peers_returned, None);
    assert_eq!(backup_row.retries, 2);
    assert_eq!(backup_row.recent_errors, vec!["timeout".to_string()]);
    let disabled_row = trackers.iter().find(|row| row.url == disabled).unwrap();
    assert!(!disabled_row.allowed);
    assert_eq!(disabled_row.status, TrackerStatus::Disabled);
    assert_eq!(disabled_row.sample_status, TrackerSampleStatus::Unavailable);
}

#[tokio::test]
async fn webseeds_and_peer_sources_expose_only_observed_evidence() {
    use swarmotter_core::models::peer::PeerSource;
    use swarmotter_core::models::tracker::{TrackerStatus, WebseedInfo};
    use swarmotter_core::peer::PeerAddr;

    let config = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(config, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent_with_webseeds(
        "webseed.bin",
        b"webseed payload",
        8,
        None,
        false,
        &["https://seed.example/files/"],
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();
    let peer = PeerAddr {
        ip: "192.0.2.10".parse().unwrap(),
        port: 51413,
    };
    let mut state = EngineState::default();
    state.webseeds.insert(
        "https://seed.example/files/".into(),
        crate::engine::WebseedSnapshot {
            status: TrackerStatus::Error,
            bytes_verified: 4096,
            attempts: 3,
            retries: 2,
            last_success: Some(100),
            last_attempt: Some(200),
            last_error: Some("range timeout".into()),
            recent_errors: vec!["range timeout".into()],
            latency_ms: Some(500),
        },
    );
    state
        .peer_sources
        .entry(peer.socket_addr())
        .or_default()
        .extend([
            PeerSource::Tracker("http://tracker.example/announce".into()),
            PeerSource::Pex,
        ]);
    state.peers.push(peer);
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(state)));

    let webseeds: Vec<WebseedInfo> = runtime.list_webseeds(&hash).await.unwrap();
    assert_eq!(webseeds.len(), 1);
    assert_eq!(webseeds[0].bytes_verified, 4096);
    assert_eq!(webseeds[0].retries, 2);
    assert_eq!(webseeds[0].last_error.as_deref(), Some("range timeout"));
    let peers = runtime.list_peers(&hash).await.unwrap();
    assert_eq!(peers.len(), 1);
    assert!(peers[0].sources.contains(&PeerSource::Pex));
    assert!(peers[0].sources.iter().any(
        |source| matches!(source, PeerSource::Tracker(url) if url.contains("tracker.example"))
    ));
}

#[tokio::test]
async fn tracker_cleanup_preview_disable_and_diagnostics_redact_credentials() {
    let config = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(config, health);
    let tracker_url = "http://tracker.example/announce?passkey=super-secret&x=1";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "cleanup.bin",
        b"cleanup payload",
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
    let now = now();
    let mut state = EngineState::default();
    state.tracker_announces.insert(
        tracker_url.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Error,
            explicit_failure: false,
            seeders: 0,
            leechers: 0,
            downloads: 0,
            last_error: Some("timeout".into()),
            last_message: None,
            last_announce: Some(now),
            last_success: None,
            retries: 4,
            latency_ms: Some(1000),
            peers_returned: None,
            recent_errors: vec!["timeout".into()],
        },
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(state)));

    assert!(runtime
        .cleanup_trackers_preview(&hash, 5)
        .await
        .unwrap()
        .is_empty());
    let preview = runtime.cleanup_trackers_preview(&hash, 3).await.unwrap();
    assert_eq!(preview.len(), 1);
    let diagnostics = runtime.tracker_diagnostics(&hash).await.unwrap();
    assert!(diagnostics.contains("passkey=***"));
    assert!(!diagnostics.contains("super-secret"));
    assert!(diagnostics.contains("retries=4"));
    let disabled = runtime.cleanup_trackers(&hash, 3).await.unwrap();
    assert_eq!(disabled.len(), 1);
    let trackers = runtime.list_trackers(&hash).await.unwrap();
    assert!(!trackers[0].allowed);
    assert_eq!(trackers[0].status, TrackerStatus::Disabled);
}

#[tokio::test]
async fn tracker_cleanup_and_redacted_diagnostics_serialize_through_native_router() {
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
    let tracker_url = "http://tracker.example/announce?passkey=router-secret";
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "router-cleanup.bin",
        b"generated router cleanup payload",
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
    engine.tracker_announces.insert(
        tracker_url.into(),
        crate::engine::TrackerAnnounceSnapshot {
            status: TrackerStatus::Error,
            explicit_failure: false,
            seeders: 0,
            leechers: 0,
            downloads: 0,
            last_error: Some("timeout".into()),
            last_message: None,
            last_announce: Some(40),
            last_success: None,
            retries: 4,
            latency_ms: Some(250),
            peers_returned: None,
            recent_errors: vec!["timeout".into()],
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

    let response = swarmotter_api::app_router(app_state.clone())
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/api/v1/torrents/{hash}/trackers/diagnostics"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert!(response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/plain")));
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let diagnostics = String::from_utf8(body.to_vec()).unwrap();
    assert!(diagnostics.contains("passkey=***"));
    assert!(!diagnostics.contains("router-secret"));
    assert!(diagnostics.contains("retries=4"));

    let cleanup_body = |confirm: bool| {
        serde_json::to_vec(&serde_json::json!({
            "failure_threshold": 3,
            "confirm": confirm,
        }))
        .unwrap()
    };
    let response = swarmotter_api::app_router(app_state.clone())
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/api/v1/torrents/{hash}/trackers/cleanup"))
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(cleanup_body(false)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);

    let response = swarmotter_api::app_router(app_state)
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/api/v1/torrents/{hash}/trackers/cleanup"))
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(cleanup_body(true)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["data"][0]["url"], tracker_url);
    assert_eq!(envelope["data"][0]["retries"], 4);
}

#[tokio::test]
async fn local_tracker_fixture_shows_contrasting_history_and_policy_change() {
    use swarmotter_core::models::tracker::TrackerSampleStatus;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
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
            let body: &[u8] = if request.contains("/scrape?") {
                b"d5:filesdee"
            } else if request.starts_with("GET /fail/announce?") {
                b"d14:failure reason11:unavailablee"
            } else {
                b"d8:completei1e10:incompletei0e8:intervali30e5:peers0:e"
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        }
    });

    let failure_url = format!("http://{address}/fail/announce");
    let success_url = format!("http://{address}/ok/announce");
    let config = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(config, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "local-tracker-fixture.bin",
        b"local tracker fixture payload",
        8,
        Some(&failure_url),
        false,
    );
    let mut meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    meta.announce_list = vec![vec![failure_url.clone()], vec![success_url.clone()]];
    let hash = TorrentKey::v1(meta.info_hash);
    runtime
        .registry
        .lock()
        .await
        .add(Torrent::new(meta, 1))
        .unwrap();
    let state = Arc::new(Mutex::new(EngineState::default()));
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, state.clone());
    DaemonRuntime::seeder_announce_once(
        &[vec![failure_url.clone()], vec![success_url.clone()]],
        hash,
        [0u8; 20],
        6881,
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        state,
        AnnounceEvent::Started,
    )
    .await;
    server.abort();

    let trackers = runtime.list_trackers(&hash).await.unwrap();
    let failed = trackers
        .iter()
        .find(|tracker| tracker.url == failure_url)
        .unwrap();
    assert_eq!(failed.status, TrackerStatus::Error);
    assert_eq!(failed.sample_status, TrackerSampleStatus::Unavailable);
    assert_eq!(failed.retries, 1);
    assert_eq!(failed.last_error.as_deref(), Some("unavailable"));
    assert!(failed.last_success.is_none());
    assert!(failed.last_attempt.is_some());
    let succeeded = trackers
        .iter()
        .find(|tracker| tracker.url == success_url)
        .unwrap();
    assert_eq!(succeeded.status, TrackerStatus::Ok);
    assert_eq!(succeeded.sample_status, TrackerSampleStatus::Current);
    assert_eq!(succeeded.peers_returned, Some(0));
    assert_eq!(succeeded.retries, 0);
    assert!(succeeded.last_success.is_some());

    let preview = runtime.cleanup_trackers_preview(&hash, 1).await.unwrap();
    assert_eq!(preview.len(), 1);
    assert_eq!(preview[0].url, failure_url);
    let disabled = runtime.cleanup_trackers(&hash, 1).await.unwrap();
    assert_eq!(disabled.len(), 1);
    let trackers = runtime.list_trackers(&hash).await.unwrap();
    assert!(
        !trackers
            .iter()
            .find(|tracker| tracker.url == failure_url)
            .unwrap()
            .allowed
    );
    let trackers = runtime
        .set_tracker_enabled(&hash, &failure_url, true)
        .await
        .unwrap();
    assert!(
        trackers
            .iter()
            .find(|tracker| tracker.url == failure_url)
            .unwrap()
            .allowed
    );
}
