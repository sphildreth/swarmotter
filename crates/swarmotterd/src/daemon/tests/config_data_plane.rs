// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn storage_preflight_rejects_torrent_file_add_before_registration() {
    let root = unique_dir("storage-preflight");
    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.storage.minimum_free_space_bytes = u64::MAX;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "too-large.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );

    let err = runtime.add_torrent_file(bytes, None).await.unwrap_err();

    assert_eq!(err.code().as_str(), "storage_error");
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
}

#[tokio::test]
async fn reset_downloads_clears_storage_roots_registry_and_logs() {
    let root = unique_dir("reset");
    let download_dir = root.join("downloads");
    let incomplete_dir = root.join("incomplete");
    let log_file = root.join("swarmotterd.log");
    tokio::fs::create_dir_all(download_dir.join("nested"))
        .await
        .unwrap();
    tokio::fs::create_dir_all(&incomplete_dir).await.unwrap();
    tokio::fs::write(download_dir.join("nested").join("old.bin"), b"old")
        .await
        .unwrap();
    tokio::fs::write(incomplete_dir.join("partial.bin"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(&log_file, b"old log line\n")
        .await
        .unwrap();

    let mut cfg = Config::default();
    cfg.storage.download_dir = Some(download_dir.display().to_string());
    cfg.storage.incomplete_dir = Some(incomplete_dir.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::with_paths(cfg, health, None, Some(log_file.clone()));
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "reset.bin",
        b"0123456789abcdef",
        8,
        None,
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
    runtime.queue.lock().await.add(hash);
    runtime
        .engine_retry_after
        .write()
        .await
        .insert(hash, Instant::now() + Duration::from_secs(60));

    let result = runtime.reset_downloads().await.unwrap();

    assert_eq!(result.torrents_removed, 1);
    assert_eq!(result.log_files_cleared, 1);
    assert!(result
        .storage_paths
        .contains(&download_dir.display().to_string()));
    assert!(result
        .storage_paths
        .contains(&incomplete_dir.display().to_string()));
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
    assert!(runtime.engine_retry_after.read().await.is_empty());
    assert!(download_dir.is_dir());
    assert!(incomplete_dir.is_dir());
    assert!(tokio::fs::read_dir(&download_dir)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_none());
    assert!(tokio::fs::read_dir(&incomplete_dir)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_none());
    assert_eq!(tokio::fs::metadata(&log_file).await.unwrap().len(), 0);
}

#[test]
fn per_torrent_worker_limit_is_independent_of_global_session_budget() {
    assert_eq!(
        DaemonRuntime::effective_per_torrent_peer_limit(0),
        DEFAULT_PER_TORRENT_PEER_LIMIT
    );
    assert_eq!(DaemonRuntime::effective_per_torrent_peer_limit(24), 24);
}

#[tokio::test]
async fn peer_diagnostics_report_unlimited_observation_and_bounded_denial() {
    let mut unlimited_config = Config::default();
    unlimited_config.network.mode = NetworkContainmentMode::Disabled;
    unlimited_config.bandwidth.max_peers = 0;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let unlimited = DaemonRuntime::new(unlimited_config, health.clone());
    let unlimited_pool = unlimited.peer_permit_pool.read().await.clone();
    let permit = unlimited_pool.acquire().await.unwrap();
    let scheduler = unlimited.global_stats().await.scheduler;
    assert_eq!(scheduler.peer_limit, 0);
    assert_eq!(scheduler.peer_permits_in_use, 1);
    assert_eq!(scheduler.peer_permits_available, None);
    assert_eq!(scheduler.peer_sessions_denied, 0);
    drop(permit);
    assert_eq!(
        unlimited.global_stats().await.scheduler.peer_permits_in_use,
        0
    );

    let mut bounded_config = Config::default();
    bounded_config.network.mode = NetworkContainmentMode::Disabled;
    bounded_config.bandwidth.max_peers = 1;
    let bounded = DaemonRuntime::new(bounded_config, health);
    let bounded_pool = bounded.peer_permit_pool.read().await.clone();
    let permit = bounded_pool.try_acquire().unwrap();
    assert!(bounded_pool.try_acquire().is_none());
    let scheduler = bounded.global_stats().await.scheduler;
    assert_eq!(scheduler.peer_limit, 1);
    assert_eq!(scheduler.peer_permits_in_use, 1);
    assert_eq!(scheduler.peer_permits_available, Some(0));
    assert_eq!(scheduler.peer_sessions_denied, 1);
    drop(permit);
}

#[test]
fn strip_ansi_controls_removes_terminal_sequences_from_logs() {
    let raw = "\u{1b}[2m2026-07-03T19:43:03Z\u{1b}[0m \u{1b}[32mINFO\u{1b}[0m message";
    assert_eq!(
        strip_ansi_controls(raw),
        "2026-07-03T19:43:03Z INFO message"
    );
}

#[test]
fn encryption_mode_change_rebuilds_data_plane_without_process_restart() {
    let previous = Config::default();
    let mut next = previous.clone();
    next.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Required;

    assert!(data_plane_config_changed(&previous, &next));
    assert!(restart_required_fields(&previous, &next).is_empty());
}

#[test]
fn cow_strategy_change_rebuilds_data_plane_without_process_restart() {
    let previous = Config::default();
    let mut next = previous.clone();
    next.storage.cow_strategy = swarmotter_core::config::CowStrategy::DisableForNewFiles;

    assert!(data_plane_config_changed(&previous, &next));
    assert!(restart_required_fields(&previous, &next).is_empty());
}

#[test]
fn storage_root_changes_reject_torrents_that_still_depend_on_old_roots() {
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "storage-transition.bin",
        b"storage transition payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), 1);
    let previous = Config::default();
    let mut next = previous.clone();
    next.storage.download_dir = Some("/tmp/swarmotter-new-root".into());

    assert!(matches!(
        validate_storage_config_transition(&previous, &next, &[torrent]),
        Err(CoreError::InvalidConfig(_))
    ));
}

#[test]
fn storage_resume_and_fallback_root_changes_preserve_existing_torrent_placement() {
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "storage-resume-transition.bin",
        b"storage resume transition payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(swarmotter_core::meta::parse_torrent(&bytes).unwrap(), 1);
    let previous = Config::default();

    let mut changed_resume = previous.clone();
    changed_resume.storage.resume_dir = Some("/tmp/swarmotter-new-resume".into());
    assert!(matches!(
        validate_storage_config_transition(
            &previous,
            &changed_resume,
            std::slice::from_ref(&torrent),
        ),
        Err(CoreError::InvalidConfig(message)) if message.contains("storage.resume_dir")
    ));

    let mut changed_temp = previous.clone();
    changed_temp.storage.temp_dir = Some("/tmp/swarmotter-new-scratch".into());
    assert!(matches!(
        validate_storage_config_transition(&previous, &changed_temp, &[torrent]),
        Err(CoreError::InvalidConfig(message)) if message.contains("storage.temp_dir")
    ));
}

#[tokio::test]
async fn state_directory_change_requires_restart_and_retains_active_state_path() {
    let root = unique_dir("state-directory-transition");
    let active_state_path = root.join("active-state.json");
    let configured_state_dir = root.join("configured-next-state");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    let runtime = DaemonRuntime::with_paths_broker_and_state(
        cfg.clone(),
        disabled_health(),
        None,
        None,
        Some(active_state_path.clone()),
        EventBroker::default(),
    );
    let mut next = cfg;
    next.storage.state_dir = Some(configured_state_dir.display().to_string());

    let result = runtime.replace_config(next).await.unwrap();

    assert!(result.restart_required);
    assert_eq!(result.restart_required_fields, vec!["storage.state_dir"]);
    assert_eq!(
        runtime.state_path.as_deref(),
        Some(active_state_path.as_path())
    );
    assert_eq!(
        runtime.get_config().await.storage.state_dir.as_deref(),
        Some(configured_state_dir.to_string_lossy().as_ref())
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn replace_config_preserves_and_redacts_auth_token() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.api.auth_token = Some("existing-token".into());
    cfg.api.require_auth = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    let mut next = runtime.get_config().await;
    next.api.auth_token = None;
    next.api.require_auth = true;
    let result = runtime.replace_config(next).await.unwrap();

    assert_eq!(
        runtime.get_config().await.api.auth_token.as_deref(),
        Some("existing-token")
    );
    assert_eq!(result.config.api.auth_token, None);
}

#[tokio::test]
async fn socks5_data_plane_binder_proxies_tracker_and_webseed_http() {
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = proxy_listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        for (expected_host, expected_path, response) in [
            (
                "tracker.example",
                "GET /announce HTTP/1.1",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            ),
            (
                "webseed.example",
                "GET /payload HTTP/1.1",
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/2\r\nConnection: close\r\n\r\nok",
            ),
        ] {
            let (mut stream, _) = proxy_listener.accept().await.unwrap();
            let mut greeting = [0u8; 3];
            stream.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, 0]);
            stream.write_all(&[5, 0]).await.unwrap();

            let mut request_head = [0u8; 5];
            stream.read_exact(&mut request_head).await.unwrap();
            assert_eq!(&request_head[..4], &[5, 1, 0, 3]);
            let mut target = vec![0u8; usize::from(request_head[4]) + 2];
            stream.read_exact(&mut target).await.unwrap();
            assert_eq!(&target[..request_head[4] as usize], expected_host.as_bytes());
            assert_eq!(&target[request_head[4] as usize..], &80u16.to_be_bytes());
            stream
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();

            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0, "HTTP request ended before its headers");
                request.extend_from_slice(&chunk[..read]);
                assert!(request.len() <= 16 * 1024, "HTTP request headers exceeded cap");
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            assert!(request.starts_with(expected_path));
            let request_lower = request.to_ascii_lowercase();
            assert!(request_lower.contains(&format!("host: {expected_host}")));
            if expected_host == "webseed.example" {
                assert!(request_lower.contains("range: bytes=0-1"));
            }
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.network.socks5.enabled = true;
    cfg.network.socks5.host = Some("127.0.0.1".into());
    cfg.network.socks5.port = proxy_port;
    cfg.torrent.utp_enabled = false;
    cfg.dht.enabled = false;
    cfg.validate().unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);
    let binder = runtime.data_plane_binder_for_test().await;

    let tracker = binder
        .http_get("http://tracker.example/announce")
        .await
        .unwrap();
    assert_eq!(tracker.body, b"ok");
    let webseed = binder
        .http_get_range("http://webseed.example/payload", 0, 2)
        .await
        .unwrap();
    assert_eq!(webseed.body, b"ok");
    proxy.await.unwrap();
}

#[tokio::test]
async fn replace_config_preserves_and_redacts_socks5_password() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.network.socks5.enabled = true;
    cfg.network.socks5.host = Some("proxy.example".into());
    cfg.network.socks5.username = Some("operator".into());
    cfg.network.socks5.password = Some("proxy-secret".into());
    cfg.torrent.utp_enabled = false;
    cfg.dht.enabled = false;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);

    let mut next = runtime.get_config().await;
    next.network.socks5.password = None;
    let result = runtime.replace_config(next).await.unwrap();

    assert_eq!(
        runtime
            .get_config()
            .await
            .network
            .socks5
            .password
            .as_deref(),
        Some("proxy-secret")
    );
    assert_eq!(result.config.network.socks5.password, None);
}

#[tokio::test]
async fn socks5_network_diagnostics_are_auditable_without_proxy_secrets() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.network.socks5.enabled = true;
    cfg.network.socks5.host = Some("proxy.example".into());
    cfg.network.socks5.username = Some("operator".into());
    cfg.network.socks5.password = Some("proxy-secret".into());
    cfg.torrent.utp_enabled = false;
    cfg.dht.enabled = false;
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::new(cfg, health);

    let diagnostics = runtime.network_diagnostics().await;
    assert!(diagnostics.socks5_enabled);
    assert!(diagnostics.socks5_udp_blocked);
    assert!(diagnostics.checks.iter().any(|check| {
        check.id == "socks5_proxy"
            && check.detail.contains("target DNS is remote")
            && check
                .detail
                .contains("UDP tracker, DHT, and uTP are blocked")
    }));
    let serialized = serde_json::to_string(&diagnostics).unwrap();
    assert!(!serialized.contains("proxy.example"));
    assert!(!serialized.contains("proxy-secret"));
}
