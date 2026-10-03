// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn unfinished_engine_exit_requeues_and_releases_active_slot() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first_bytes = swarmotter_core::meta::build_single_file_torrent(
        "unfinished-first.bin",
        b"unfinished first payload",
        8,
        None,
        false,
    );
    let second_bytes = swarmotter_core::meta::build_single_file_torrent(
        "unfinished-second.bin",
        b"unfinished second payload",
        8,
        None,
        false,
    );
    let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
    let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
    let first_hash = TorrentKey::v1(first.info_hash);
    let second_hash = TorrentKey::v1(second.info_hash);
    let mut first_torrent = Torrent::new(first, 1);
    first_torrent.state = TorrentState::Downloading;
    {
        let mut reg = runtime.registry.lock().await;
        reg.add(first_torrent).unwrap();
        reg.add(Torrent::new(second, 2)).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
    }

    let queued = runtime
        .queue_torrent_for_retry(
            first_hash,
            "engine stopped before completion; queued for retry",
            ENGINE_INCOMPLETE_RETRY_DELAY,
        )
        .await;

    assert!(queued);
    assert_eq!(
        runtime
            .registry
            .lock()
            .await
            .get(&first_hash)
            .unwrap()
            .state,
        TorrentState::Queued
    );
    assert_eq!(runtime.queue.lock().await.position(&second_hash), Some(1));
    assert_eq!(runtime.queue.lock().await.position(&first_hash), Some(2));
    assert!(runtime
        .engine_retry_after
        .read()
        .await
        .get(&first_hash)
        .is_some_and(|retry_at| *retry_at > Instant::now()));
    assert_eq!(runtime.desired_download_hashes().await, vec![second_hash]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_announce_timeout_waits_for_dht_then_queues_for_retry() {
    let tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    // This path has no derived scrape endpoint; only announce is under test.
    let tracker_url = format!("http://{}/tracker", tracker.local_addr().unwrap());
    let tracker_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = tracker.accept().await.unwrap();
            tokio::spawn(async move {
                let mut first_byte = [0; 1];
                stream.read_exact(&mut first_byte).await.unwrap();
                std::future::pending::<()>().await;
            });
        }
    });
    let silent_dht = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dir = unique_dir("tracker-timeout-retry");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.torrent.listen_port = 0;
    cfg.dht.port = 0;
    cfg.dht.bootstrap_nodes = vec![silent_dht.local_addr().unwrap().to_string()];
    cfg.storage.download_dir = Some(dir.display().to_string());
    cfg.storage.incomplete_dir = Some(dir.display().to_string());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "timeout.bin",
        b"generated local tracker timeout payload",
        8,
        Some(&tracker_url),
        false,
    );
    let hash = runtime
        .add_torrent_file(bytes, Some(dir.display().to_string()))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let state = runtime.engine_states.read().await.get(&hash).cloned();
            if let Some(state) = state {
                let state = state.lock().await;
                if state.dht_last_lookup.is_some() && state.dht_last_lookup_completed.is_none() {
                    let snapshot = state.tracker_announces.get(&tracker_url).unwrap();
                    assert!(snapshot
                        .last_error
                        .as_deref()
                        .unwrap()
                        .contains("timed out"));
                    assert!(!snapshot.explicit_failure);
                    assert!(state.terminal_tracker_error().is_none());
                    drop(state);
                    assert_ne!(
                        runtime.get_torrent(&hash).await.unwrap().state,
                        TorrentState::TrackerError
                    );
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let summary = runtime.get_torrent(&hash).await.unwrap();
            assert_ne!(summary.state, TorrentState::TrackerError);
            if summary.state == TorrentState::Queued
                && runtime.engine_retry_after.read().await.contains_key(&hash)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(runtime
        .engine_retry_after
        .read()
        .await
        .get(&hash)
        .is_some_and(|at| *at > Instant::now()));
    assert_eq!(runtime.queue.lock().await.position(&hash), Some(1));
    runtime.shutdown().await.unwrap();
    tracker_task.abort();
    drop(silent_dht);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn stale_active_without_engine_is_requeued_and_releases_active_slot() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let stale_bytes = swarmotter_core::meta::build_single_file_torrent(
        "stale-active.bin",
        b"stale active payload",
        8,
        None,
        false,
    );
    let queued_bytes = swarmotter_core::meta::build_single_file_torrent(
        "queued-behind-stale.bin",
        b"queued behind stale payload",
        8,
        None,
        false,
    );
    let stale_meta = swarmotter_core::meta::parse_torrent(&stale_bytes).unwrap();
    let queued_meta = swarmotter_core::meta::parse_torrent(&queued_bytes).unwrap();
    let stale_hash = TorrentKey::v1(stale_meta.info_hash);
    let queued_hash = TorrentKey::v1(queued_meta.info_hash);
    let mut stale_torrent = Torrent::new(stale_meta, 1);
    stale_torrent.state = TorrentState::Downloading;
    {
        let mut reg = runtime.registry.lock().await;
        reg.add(stale_torrent).unwrap();
        reg.add(Torrent::new(queued_meta, 2)).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(stale_hash);
        queue.add(queued_hash);
    }

    let recovered = runtime.sweep_stale_active_torrents("test").await;

    assert_eq!(recovered, 1);
    {
        let reg = runtime.registry.lock().await;
        let torrent = reg.get(&stale_hash).unwrap();
        assert_eq!(torrent.state, TorrentState::Queued);
        assert_eq!(
            torrent.error.as_deref(),
            Some(STALE_ACTIVE_RECOVERY_MESSAGE)
        );
    }
    assert_eq!(runtime.queue.lock().await.position(&queued_hash), Some(1));
    assert_eq!(runtime.queue.lock().await.position(&stale_hash), Some(2));
    assert_eq!(runtime.desired_download_hashes().await, vec![queued_hash]);
}

#[tokio::test]
async fn stale_metadata_progress_does_not_reactivate_large_queue_above_limit() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.max_active_metadata_fetches = 50;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "magnet placeholder",
        b"placeholder",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();

    {
        let mut reg = runtime.registry.lock().await;
        let mut queue = runtime.queue.lock().await;
        let mut states = runtime.engine_states.write().await;
        for idx in 1..=100u8 {
            let hash = TorrentKey::v1(InfoHash::from_bytes([idx; 20]));
            let mut torrent = Torrent::new(placeholder_meta.clone(), idx as u64);
            torrent.state = TorrentState::DownloadingMetadata;
            torrent.needs_metadata = true;
            set_test_v1_magnet_identity(&mut torrent, hash);
            reg.add(torrent).unwrap();
            queue.add(hash);
            states.insert(
                hash,
                Arc::new(Mutex::new(EngineState {
                    piece_count: placeholder_meta.piece_count(),
                    total_length: placeholder_meta.total_length,
                    ..Default::default()
                })),
            );
        }
    }

    let recovered = runtime.sweep_stale_active_torrents("test").await;
    assert_eq!(recovered, 100);

    runtime.reconcile_engine_progress().await;

    let active_count = runtime
        .registry
        .lock()
        .await
        .torrents
        .values()
        .filter(|torrent| {
            matches!(
                torrent.state,
                TorrentState::Downloading | TorrentState::DownloadingMetadata
            )
        })
        .count();
    assert_eq!(
        active_count, 0,
        "retained metadata diagnostics must not bypass active queue limits"
    );
    assert_eq!(runtime.desired_download_hashes().await.len(), 50);
}

#[tokio::test]
async fn ten_thousand_stale_metadata_records_recover_without_active_leak() {
    const TOTAL_TORRENTS: usize = 10_000;

    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.max_active_metadata_fetches = 50;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "magnet placeholder",
        b"placeholder",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let hashes = (0..TOTAL_TORRENTS)
        .map(|idx| TorrentKey::v1(InfoHash::from_bytes(scale_hash_bytes(idx as u32))))
        .collect::<Vec<_>>();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            torrent.state = TorrentState::DownloadingMetadata;
            torrent.needs_metadata = true;
            set_test_v1_magnet_identity(&mut torrent, hash);
            reg.add(torrent).unwrap();
        }
    }
    runtime.queue.lock().await.add_many(hashes.iter().copied());

    let recovered = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.sweep_stale_active_torrents("test"),
    )
    .await
    .expect("stale active recovery should be bounded for 10,000 records");

    assert_eq!(recovered, TOTAL_TORRENTS);
    let reg = runtime.registry.lock().await;
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| {
                matches!(
                    torrent.state,
                    TorrentState::Downloading | TorrentState::DownloadingMetadata
                )
            })
            .count(),
        0
    );
    drop(reg);
    assert_eq!(runtime.desired_download_hashes().await.len(), 50);
    assert_eq!(runtime.queue.lock().await.order.len(), TOTAL_TORRENTS);
}

#[tokio::test]
async fn ten_thousand_metadata_retry_backoffs_leave_no_active_desired_slots() {
    const TOTAL_TORRENTS: usize = 10_000;

    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.max_active_metadata_fetches = 50;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "magnet placeholder",
        b"placeholder",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let hashes = (0..TOTAL_TORRENTS)
        .map(|idx| TorrentKey::v1(InfoHash::from_bytes(scale_hash_bytes(idx as u32))))
        .collect::<Vec<_>>();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            torrent.state = TorrentState::Queued;
            torrent.needs_metadata = true;
            set_test_v1_magnet_identity(&mut torrent, hash);
            reg.add(torrent).unwrap();
        }
    }
    runtime.queue.lock().await.add_many(hashes.iter().copied());
    {
        let mut retry_after = runtime.engine_retry_after.write().await;
        let retry_until = Instant::now() + MAGNET_METADATA_NO_PEERS_RETRY_DELAY;
        for hash in &hashes {
            retry_after.insert(*hash, retry_until);
        }
    }

    let desired = tokio::time::timeout(Duration::from_secs(5), runtime.desired_download_hashes())
        .await
        .expect("desired active planning should be bounded for 10,000 retrying magnets");

    assert!(desired.is_empty());
    assert_eq!(runtime.queue.lock().await.order.len(), TOTAL_TORRENTS);
}

#[tokio::test]
#[ignore = "scale regression: mixed lifecycle states at 1k+ records"]
async fn ignored_thousand_mixed_state_torrents_keep_scheduler_bounds() {
    const TOTAL_TORRENTS: usize = 1_200;
    const MAX_ACTIVE_DOWNLOADS: usize = 32;
    const MAX_ACTIVE_METADATA_FETCHES: usize = 24;
    const LIVE_DOWNLOAD_COUNT: usize = 20;
    const LIVE_METADATA_COUNT: usize = 16;
    const STALE_DOWNLOAD_COUNT: usize = 40;
    const STALE_METADATA_COUNT: usize = 44;
    const QUEUED_DOWNLOAD_COUNT: usize = 260;
    const QUEUED_METADATA_COUNT: usize = 220;
    const BACKOFF_METADATA_COUNT: usize = 150;
    const COMPLETED_COUNT: usize = 120;
    const PAUSED_COUNT: usize = 100;
    const SEEDING_COUNT: usize = 60;
    const CHECKING_COUNT: usize = 50;
    const ERROR_COUNT: usize = 40;
    const NETWORK_BLOCKED_COUNT: usize = 30;
    const STORAGE_ERROR_COUNT: usize = 25;
    const TRACKER_ERROR_COUNT: usize = 25;
    const LIVE_METADATA_START: usize = LIVE_DOWNLOAD_COUNT;
    const STALE_DOWNLOAD_START: usize = LIVE_METADATA_START + LIVE_METADATA_COUNT;
    const STALE_METADATA_START: usize = STALE_DOWNLOAD_START + STALE_DOWNLOAD_COUNT;
    const QUEUED_DOWNLOAD_START: usize = STALE_METADATA_START + STALE_METADATA_COUNT;
    const QUEUED_METADATA_START: usize = QUEUED_DOWNLOAD_START + QUEUED_DOWNLOAD_COUNT;
    const BACKOFF_METADATA_START: usize = QUEUED_METADATA_START + QUEUED_METADATA_COUNT;
    const COMPLETED_START: usize = BACKOFF_METADATA_START + BACKOFF_METADATA_COUNT;
    const PAUSED_START: usize = COMPLETED_START + COMPLETED_COUNT;
    const SEEDING_START: usize = PAUSED_START + PAUSED_COUNT;
    const CHECKING_START: usize = SEEDING_START + SEEDING_COUNT;
    const ERROR_START: usize = CHECKING_START + CHECKING_COUNT;
    const NETWORK_BLOCKED_START: usize = ERROR_START + ERROR_COUNT;
    const STORAGE_ERROR_START: usize = NETWORK_BLOCKED_START + NETWORK_BLOCKED_COUNT;
    const TRACKER_ERROR_START: usize = STORAGE_ERROR_START + STORAGE_ERROR_COUNT;

    assert_eq!(TRACKER_ERROR_START + TRACKER_ERROR_COUNT, TOTAL_TORRENTS);

    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = MAX_ACTIVE_DOWNLOADS;
    cfg.queue.max_active_metadata_fetches = MAX_ACTIVE_METADATA_FETCHES;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "mixed-state placeholder",
        b"placeholder payload",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let hashes = (0..TOTAL_TORRENTS)
        .map(|idx| TorrentKey::v1(InfoHash::from_bytes(scale_hash_bytes(idx as u32))))
        .collect::<Vec<_>>();
    let backoff_start = Instant::now();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            set_test_v1_magnet_identity(&mut torrent, hash);
            if idx < LIVE_DOWNLOAD_COUNT {
                torrent.state = TorrentState::Downloading;
                torrent.needs_metadata = false;
            } else if idx < LIVE_METADATA_START + LIVE_METADATA_COUNT {
                torrent.state = TorrentState::DownloadingMetadata;
                torrent.needs_metadata = true;
            } else if idx < STALE_DOWNLOAD_START + STALE_DOWNLOAD_COUNT {
                torrent.state = TorrentState::Downloading;
                torrent.needs_metadata = false;
            } else if idx < STALE_METADATA_START + STALE_METADATA_COUNT {
                torrent.state = TorrentState::DownloadingMetadata;
                torrent.needs_metadata = true;
            } else if idx < QUEUED_DOWNLOAD_START + QUEUED_DOWNLOAD_COUNT {
                torrent.state = TorrentState::Queued;
                torrent.needs_metadata = false;
            } else if idx < BACKOFF_METADATA_START + BACKOFF_METADATA_COUNT {
                torrent.state = TorrentState::Queued;
                torrent.needs_metadata = true;
            } else if idx < COMPLETED_START + COMPLETED_COUNT {
                torrent.state = TorrentState::Completed;
                torrent.date_completed = Some((idx + 1) as u64);
                torrent.needs_metadata = false;
            } else if idx < PAUSED_START + PAUSED_COUNT {
                torrent.state = TorrentState::Paused;
                torrent.needs_metadata = false;
            } else if idx < SEEDING_START + SEEDING_COUNT {
                torrent.state = TorrentState::Seeding;
                torrent.needs_metadata = false;
            } else if idx < CHECKING_START + CHECKING_COUNT {
                torrent.state = TorrentState::Checking;
                torrent.needs_metadata = false;
            } else {
                torrent.state = if idx < ERROR_START + ERROR_COUNT {
                    TorrentState::Error
                } else if idx < NETWORK_BLOCKED_START + NETWORK_BLOCKED_COUNT {
                    TorrentState::NetworkBlocked
                } else if idx < STORAGE_ERROR_START + STORAGE_ERROR_COUNT {
                    TorrentState::StorageError
                } else {
                    TorrentState::TrackerError
                };
                torrent.needs_metadata = false;
                torrent.error = Some("mixed-state scale fixture error".to_string());
            }
            reg.add(torrent).unwrap();
        }
    }

    runtime.queue.lock().await.add_many(hashes.iter().copied());
    {
        let mut handles = runtime.engine_handles.write().await;
        for hash in hashes.iter().take(LIVE_DOWNLOAD_COUNT).chain(
            hashes
                .iter()
                .skip(LIVE_METADATA_START)
                .take(LIVE_METADATA_COUNT),
        ) {
            handles.insert(
                *hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
        }
    }
    {
        let mut retry_after = runtime.engine_retry_after.write().await;
        for hash in hashes
            .iter()
            .skip(BACKOFF_METADATA_START)
            .take(BACKOFF_METADATA_COUNT)
        {
            retry_after.insert(*hash, backoff_start + Duration::from_secs(60));
        }
    }

    let reg = runtime.registry.lock().await;
    assert_eq!(reg.torrents.len(), TOTAL_TORRENTS);
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Queued)
            .count(),
        QUEUED_DOWNLOAD_COUNT + QUEUED_METADATA_COUNT + BACKOFF_METADATA_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::DownloadingMetadata)
            .count(),
        LIVE_METADATA_COUNT + STALE_METADATA_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Downloading)
            .count(),
        LIVE_DOWNLOAD_COUNT + STALE_DOWNLOAD_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Completed)
            .count(),
        COMPLETED_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Paused)
            .count(),
        PAUSED_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Seeding)
            .count(),
        SEEDING_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Checking)
            .count(),
        CHECKING_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Error)
            .count(),
        ERROR_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::NetworkBlocked)
            .count(),
        NETWORK_BLOCKED_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::StorageError)
            .count(),
        STORAGE_ERROR_COUNT
    );
    assert_eq!(
        reg.torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::TrackerError)
            .count(),
        TRACKER_ERROR_COUNT
    );
    drop(reg);

    let stale_recovered = runtime.sweep_stale_active_torrents("scale_test").await;
    assert_eq!(stale_recovered, STALE_DOWNLOAD_COUNT + STALE_METADATA_COUNT);

    let desired = tokio::time::timeout(Duration::from_secs(5), runtime.desired_download_hashes())
        .await
        .expect("mixed-state scheduler planning should remain bounded for 1,200 records");
    assert_eq!(
        desired.len(),
        MAX_ACTIVE_DOWNLOADS + MAX_ACTIVE_METADATA_FETCHES
    );
    let desired_backoff_hashes = hashes
        .iter()
        .skip(BACKOFF_METADATA_START)
        .take(BACKOFF_METADATA_COUNT)
        .copied()
        .collect::<Vec<_>>();
    assert!(desired
        .iter()
        .all(|hash| !desired_backoff_hashes.contains(hash)));
    {
        let reg = runtime.registry.lock().await;
        assert_eq!(
            desired
                .iter()
                .filter(|hash| reg.get(hash).is_some_and(|torrent| torrent.needs_metadata))
                .count(),
            MAX_ACTIVE_METADATA_FETCHES
        );
    }

    let stats = runtime.global_stats().await;
    assert_eq!(
        stats.scheduler.requested_downloads,
        LIVE_DOWNLOAD_COUNT + STALE_DOWNLOAD_COUNT + QUEUED_DOWNLOAD_COUNT
    );
    assert_eq!(
        stats.scheduler.requested_metadata_fetches,
        LIVE_METADATA_COUNT + STALE_METADATA_COUNT + QUEUED_METADATA_COUNT
    );
    assert_eq!(stats.scheduler.granted_downloads, MAX_ACTIVE_DOWNLOADS);
    assert_eq!(
        stats.scheduler.granted_metadata_fetches,
        MAX_ACTIVE_METADATA_FETCHES
    );
    assert_eq!(
        stats.scheduler.retry_backoff_torrents,
        BACKOFF_METADATA_COUNT
    );
    assert_eq!(
        stats.scheduler.queued_torrents,
        QUEUED_DOWNLOAD_COUNT
            + QUEUED_METADATA_COUNT
            + BACKOFF_METADATA_COUNT
            + STALE_DOWNLOAD_COUNT
            + STALE_METADATA_COUNT
    );
    assert_eq!(
        stats.scheduler.running_engines,
        LIVE_DOWNLOAD_COUNT + LIVE_METADATA_COUNT
    );
    assert_eq!(stats.scheduler.running_downloads, LIVE_DOWNLOAD_COUNT);
    assert_eq!(
        stats.scheduler.running_metadata_fetches,
        LIVE_METADATA_COUNT
    );
    assert_eq!(stats.scheduler.active_download_limit, MAX_ACTIVE_DOWNLOADS);
    assert_eq!(
        stats.scheduler.active_metadata_fetch_limit,
        MAX_ACTIVE_METADATA_FETCHES
    );
    assert_eq!(
        runtime.active_download_hashes().await.len(),
        LIVE_DOWNLOAD_COUNT + LIVE_METADATA_COUNT
    );
    assert_eq!(
        runtime.engine_retry_after.read().await.len(),
        BACKOFF_METADATA_COUNT
    );
    assert!(stats.scheduler.download_slots_saturated);
    assert!(stats.scheduler.metadata_fetch_slots_saturated);

    for hash in hashes.iter().take(LIVE_DOWNLOAD_COUNT).chain(
        hashes
            .iter()
            .skip(LIVE_METADATA_START)
            .take(LIVE_METADATA_COUNT),
    ) {
        runtime.force_stop_engine(hash).await;
    }
}

#[tokio::test]
async fn metadata_fetch_limit_is_separate_from_download_slot_limit() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 2;
    cfg.queue.max_active_metadata_fetches = 3;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "magnet placeholder",
        b"placeholder",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let mut metadata_hashes = Vec::new();
    let mut download_hashes = Vec::new();

    {
        let mut reg = runtime.registry.lock().await;
        let mut queue = runtime.queue.lock().await;
        for idx in 0..6u32 {
            let hash = TorrentKey::v1(InfoHash::from_bytes(scale_hash_bytes(idx)));
            let mut torrent = Torrent::new(placeholder_meta.clone(), idx as u64 + 1);
            torrent.state = TorrentState::Queued;
            torrent.needs_metadata = true;
            set_test_v1_magnet_identity(&mut torrent, hash);
            reg.add(torrent).unwrap();
            queue.add(hash);
            metadata_hashes.push(hash);
        }
        for idx in 0..5u32 {
            let name = format!("resolved-download-{idx}.bin");
            let payload = format!("resolved download payload {idx}");
            let bytes = swarmotter_core::meta::build_single_file_torrent(
                &name,
                payload.as_bytes(),
                8,
                None,
                false,
            );
            let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
            let hash = TorrentKey::v1(meta.info_hash);
            reg.add(Torrent::new(meta, idx as u64 + 10)).unwrap();
            queue.add(hash);
            download_hashes.push(hash);
        }
    }

    let desired = runtime.desired_download_hashes().await;

    assert_eq!(
        desired
            .iter()
            .filter(|hash| metadata_hashes.contains(hash))
            .count(),
        3
    );
    assert_eq!(
        desired
            .iter()
            .filter(|hash| download_hashes.contains(hash))
            .count(),
        2
    );
    assert_eq!(desired.len(), 5);

    let stats = runtime.global_stats().await;
    assert_eq!(stats.scheduler.requested_metadata_fetches, 6);
    assert_eq!(stats.scheduler.granted_metadata_fetches, 3);
    assert_eq!(stats.scheduler.requested_downloads, 5);
    assert_eq!(stats.scheduler.granted_downloads, 2);
    assert_eq!(stats.scheduler.active_metadata_fetch_limit, 3);
    assert_eq!(stats.scheduler.active_download_limit, 2);
    assert!(stats.scheduler.metadata_fetch_slots_saturated);
    assert!(stats.scheduler.download_slots_saturated);
}

#[tokio::test]
async fn queued_torrent_with_stale_engine_handle_is_cleared_for_restart() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "stale-queued-handle.bin",
        b"stale queued handle payload",
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
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(hash, tx);
    runtime.engine_handles.write().await.insert(
        hash,
        tokio::spawn(async {
            std::future::pending::<()>().await;
        }),
    );
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(EngineState::default())));

    let recovered = tokio::time::timeout(
        Duration::from_millis(100),
        runtime.sweep_inactive_engine_handles("test"),
    )
    .await
    .expect("stale queued handles should be force-cleared promptly");

    assert_eq!(recovered, 1);
    assert!(!runtime.engine_handles.read().await.contains_key(&hash));
    assert!(!runtime.engine_cmds.lock().await.contains_key(&hash));
    assert!(!runtime.engine_states.read().await.contains_key(&hash));
    {
        let reg = runtime.registry.lock().await;
        let torrent = reg.get(&hash).unwrap();
        assert_eq!(torrent.state, TorrentState::Queued);
        assert_eq!(
            torrent.error.as_deref(),
            Some(STALE_INACTIVE_ENGINE_RECOVERY_MESSAGE)
        );
    }
    assert_eq!(runtime.desired_download_hashes().await, vec![hash]);
}

#[tokio::test]
async fn reconcile_queue_force_clears_over_limit_active_engine() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first_bytes = swarmotter_core::meta::build_single_file_torrent(
        "active-slot-one.bin",
        b"active slot one payload",
        8,
        None,
        false,
    );
    let second_bytes = swarmotter_core::meta::build_single_file_torrent(
        "active-slot-two.bin",
        b"active slot two payload",
        8,
        None,
        false,
    );
    let first_meta = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
    let second_meta = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
    let first_hash = TorrentKey::v1(first_meta.info_hash);
    let second_hash = TorrentKey::v1(second_meta.info_hash);
    let mut first_torrent = Torrent::new(first_meta, 1);
    first_torrent.state = TorrentState::Downloading;
    let mut second_torrent = Torrent::new(second_meta, 2);
    second_torrent.state = TorrentState::Downloading;
    {
        let mut reg = runtime.registry.lock().await;
        reg.add(first_torrent).unwrap();
        reg.add(second_torrent).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
    }
    {
        let mut handles = runtime.engine_handles.write().await;
        handles.insert(
            first_hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
        handles.insert(
            second_hash,
            tokio::spawn(async {
                std::future::pending::<()>().await;
            }),
        );
    }

    tokio::time::timeout(Duration::from_millis(100), runtime.reconcile_queue())
        .await
        .expect("queue reconciliation must not hang on over-limit active work");

    assert!(runtime
        .engine_handles
        .read()
        .await
        .contains_key(&first_hash));
    assert!(!runtime
        .engine_handles
        .read()
        .await
        .contains_key(&second_hash));
    {
        let reg = runtime.registry.lock().await;
        assert_eq!(
            reg.get(&first_hash).unwrap().state,
            TorrentState::Downloading
        );
        assert_eq!(reg.get(&second_hash).unwrap().state, TorrentState::Queued);
    }
    assert_eq!(runtime.active_download_hashes().await, vec![first_hash]);

    runtime.force_stop_engine(&first_hash).await;
}

#[tokio::test]
async fn large_queue_recovery_keeps_configured_active_slots_startable() {
    assert_large_queue_recovery_keeps_configured_active_slots_startable(100).await;
}

#[tokio::test]
async fn thousand_torrent_queue_recovery_keeps_configured_active_slots_startable() {
    assert_large_queue_recovery_keeps_configured_active_slots_startable(1_000).await;
}

async fn assert_large_queue_recovery_keeps_configured_active_slots_startable(
    total_torrents: usize,
) {
    assert!(total_torrents >= 50);
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 50;
    cfg.queue.auto_start = true;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let mut hashes = Vec::new();
    {
        let mut reg = runtime.registry.lock().await;
        let mut queue = runtime.queue.lock().await;
        for idx in 0..total_torrents {
            let name = format!("large-queue-{idx}.bin");
            let payload = format!("large queue payload {idx}");
            let bytes = swarmotter_core::meta::build_single_file_torrent(
                &name,
                payload.as_bytes(),
                8,
                None,
                false,
            );
            let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
            let hash = TorrentKey::v1(meta.info_hash);
            let mut torrent = Torrent::new(meta, (idx + 1) as u64);
            if idx < 18 {
                torrent.state = TorrentState::Downloading;
            }
            reg.add(torrent).unwrap();
            queue.add(hash);
            hashes.push(hash);
        }
    }

    {
        let mut handles = runtime.engine_handles.write().await;
        for hash in hashes.iter().take(18) {
            handles.insert(
                *hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
        }
        for hash in hashes.iter().skip(18).take(32) {
            handles.insert(
                *hash,
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                }),
            );
        }
    }

    assert_eq!(runtime.active_download_hashes().await.len(), 18);
    let recovered = runtime.sweep_inactive_engine_handles("test").await;
    assert_eq!(recovered, 32);

    let desired = runtime.desired_download_hashes().await;
    assert_eq!(desired.len(), 50);
    assert_eq!(
        desired
            .iter()
            .filter(|hash| hashes[..18].contains(hash))
            .count(),
        18
    );
    let running = runtime.engine_handles.read().await;
    let blocked_startable = desired
        .iter()
        .filter(|hash| !hashes[..18].contains(hash) && running.contains_key(hash))
        .count();
    assert_eq!(
            blocked_startable, 0,
            "queued torrents selected to fill the configured active slots must not retain stale handles that make start_engine skip them"
        );
    drop(running);

    for hash in hashes.iter().take(18) {
        runtime.force_stop_engine(hash).await;
    }
}

#[tokio::test]
async fn engine_task_finished_clears_restart_blocking_runtime_bookkeeping() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let hash = TorrentKey::v1(
        swarmotter_core::hash::InfoHash::from_hex("95c6c298c84fee2eee10c044d673537da158f0f8")
            .unwrap(),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    runtime.engine_cmds.lock().await.insert(hash, tx);
    runtime
        .engine_handles
        .write()
        .await
        .insert(hash, tokio::spawn(async {}));
    runtime
        .engine_states
        .write()
        .await
        .insert(hash, Arc::new(Mutex::new(EngineState::default())));
    runtime.torrent_limiters.write().await.insert(
        hash,
        Arc::new(swarmotter_core::bandwidth::RateLimiter::new(0, 0)),
    );
    runtime.rate_samples.write().await.insert(
        hash,
        RateSample {
            downloaded: 1,
            uploaded: 1,
            rate_down: 1,
            rate_up: 1,
            last_download_at: Some(Instant::now()),
            last_upload_at: Some(Instant::now()),
            no_download_since: None,
            at: Instant::now(),
            peak_rate_down: 1,
            peak_rate_up: 1,
        },
    );

    runtime.engine_task_finished(hash).await;

    assert!(!runtime.engine_cmds.lock().await.contains_key(&hash));
    assert!(!runtime.engine_handles.read().await.contains_key(&hash));
    assert!(
        runtime.torrent_limiters.read().await.contains_key(&hash),
        "normal engine completion must retain the torrent limiter for queued seeding"
    );
    assert!(
        runtime.engine_states.read().await.contains_key(&hash),
        "diagnostic state should survive normal engine task exit"
    );
    assert!(
        runtime.rate_samples.read().await.contains_key(&hash),
        "rate samples should survive normal engine task exit"
    );
}
