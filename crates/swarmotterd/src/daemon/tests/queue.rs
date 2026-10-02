// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn queue_scheduler_respects_auto_start_and_moves() {
    let mut cfg = Config::default();
    cfg.queue.max_active_downloads = 1;
    cfg.queue.auto_start = false;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let first_bytes =
        swarmotter_core::meta::build_single_file_torrent("q1.bin", b"queue-one", 4, None, false);
    let second_bytes =
        swarmotter_core::meta::build_single_file_torrent("q2.bin", b"queue-two", 4, None, false);
    let first = swarmotter_core::meta::parse_torrent(&first_bytes).unwrap();
    let second = swarmotter_core::meta::parse_torrent(&second_bytes).unwrap();
    let first_hash = TorrentKey::v1(first.info_hash);
    let second_hash = TorrentKey::v1(second.info_hash);

    {
        let mut reg = runtime.registry.lock().await;
        reg.add(Torrent::new(first, 1)).unwrap();
        reg.add(Torrent::new(second, 2)).unwrap();
    }
    {
        let mut queue = runtime.queue.lock().await;
        queue.add(first_hash);
        queue.add(second_hash);
    }

    assert!(runtime.desired_download_hashes().await.is_empty());

    runtime.queue.lock().await.start_now(&second_hash);
    assert_eq!(runtime.desired_download_hashes().await, vec![second_hash]);

    {
        let mut queue = runtime.queue.lock().await;
        queue.clear_bypass(&second_hash);
        queue.move_to_top(&first_hash);
    }
    runtime.config.write().await.queue.auto_start = true;
    assert_eq!(runtime.desired_download_hashes().await, vec![first_hash]);
}

#[tokio::test]
async fn add_operations_mark_existing_queue_reconcile_dirty() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.scheduled = true;
        state.dirty = false;
    }

    let magnet_hash = runtime
        .add_magnet(
            "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e&dn=bulk-one",
            None,
        )
        .await
        .unwrap();

    assert!(runtime.registry.lock().await.contains(&magnet_hash));
    assert_eq!(runtime.queue.lock().await.position(&magnet_hash), Some(1));
    assert!(runtime.engine_handles.read().await.is_empty());
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }

    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.dirty = false;
    }

    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "bulk-two.bin",
        b"bulk torrent file payload",
        4,
        None,
        false,
    );
    let file_hash = runtime.add_torrent_file(bytes, None).await.unwrap();

    assert!(runtime.registry.lock().await.contains(&file_hash));
    assert_eq!(runtime.queue.lock().await.position(&file_hash), Some(2));
    assert!(runtime.engine_handles.read().await.is_empty());
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }
}

#[tokio::test]
async fn runtime_queue_limit_update_marks_scheduled_reconcile_dirty() {
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.queue.max_active_downloads = 25;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.scheduled = true;
        state.dirty = false;
    }

    runtime
        .update_settings(swarmotter_api::state::SettingsPatch {
            queue: Some(swarmotter_core::queue::QueueLimits {
                max_active_downloads: 50,
                max_active_metadata_fetches: 100,
                max_active_seeds: 5,
                auto_start: true,
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(runtime.config.read().await.queue.max_active_downloads, 50);
    assert_eq!(runtime.queue.lock().await.limits.max_active_downloads, 50);
    let state = runtime.queue_reconcile.lock().await;
    assert!(state.scheduled);
    assert!(
            state.dirty,
            "runtime queue limit updates should schedule queue reconciliation instead of awaiting engine startup inline"
        );
}

#[tokio::test]
async fn queue_reconcile_scheduler_clears_after_rapid_adds() {
    let mut cfg = Config::default();
    cfg.queue.auto_start = false;
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    let first_hash = runtime
        .add_magnet(
            "magnet:?xt=urn:btih:000000000000000000000000000000000000000a&dn=schedule-one",
            None,
        )
        .await
        .unwrap();
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(!state.dirty);
    }

    for index in 1..3 {
        let magnet = format!(
            "magnet:?xt=urn:btih:{:040x}&dn=schedule-{index}",
            index + 10
        );
        runtime.add_magnet(&magnet, None).await.unwrap();
    }
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let complete = {
                let state = runtime.queue_reconcile.lock().await;
                !state.scheduled && !state.dirty
            };
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    assert_eq!(runtime.registry.lock().await.torrents.len(), 3);
    assert_eq!(runtime.queue.lock().await.order.len(), 3);
    assert_eq!(runtime.queue.lock().await.position(&first_hash), Some(1));
    assert!(runtime.engine_handles.read().await.is_empty());
}

#[tokio::test]
async fn rapid_adds_queue_without_waiting_for_reconcile() {
    const ADD_COUNT: usize = 200;

    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);

    {
        let mut state = runtime.queue_reconcile.lock().await;
        state.scheduled = true;
        state.dirty = false;
    }

    for index in 0..ADD_COUNT {
        let magnet = format!("magnet:?xt=urn:btih:{:040x}&dn=rapid-{index}", index + 1);
        let hash = runtime.add_magnet(&magnet, None).await.unwrap();
        assert_eq!(runtime.queue.lock().await.position(&hash), Some(index + 1));
    }

    assert_eq!(runtime.registry.lock().await.torrents.len(), ADD_COUNT);
    assert_eq!(runtime.queue.lock().await.order.len(), ADD_COUNT);
    assert!(runtime.engine_handles.read().await.is_empty());
    {
        let state = runtime.queue_reconcile.lock().await;
        assert!(state.scheduled);
        assert!(state.dirty);
    }
}

#[tokio::test]
async fn bulk_remove_clears_many_torrents_and_queue_entries() {
    const REMOVE_COUNT: usize = 98;

    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let mut hashes = Vec::with_capacity(REMOVE_COUNT);

    for index in 0..REMOVE_COUNT {
        let magnet = format!("magnet:?xt=urn:btih:{:040x}&dn=remove-{index}", index + 1);
        let hash = runtime.add_magnet(&magnet, None).await.unwrap();
        hashes.push(hash);
    }
    hashes.push(TorrentKey::v1(
        InfoHash::from_hex("ffffffffffffffffffffffffffffffffffffffff").unwrap(),
    ));

    let removed = runtime.remove_torrents(hashes, false).await.unwrap();

    assert_eq!(removed.len(), REMOVE_COUNT);
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
    assert!(runtime.engine_handles.read().await.is_empty());
}

#[tokio::test]
async fn bulk_remove_clears_ten_thousand_torrents_and_runtime_indexes() {
    const REMOVE_COUNT: usize = 10_000;

    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let placeholder_bytes = swarmotter_core::meta::build_single_file_torrent(
        "managed placeholder",
        b"managed placeholder payload",
        8,
        None,
        false,
    );
    let placeholder_meta = swarmotter_core::meta::parse_torrent(&placeholder_bytes).unwrap();
    let hashes = (0..REMOVE_COUNT)
        .map(|idx| TorrentKey::v1(InfoHash::from_bytes(scale_hash_bytes(idx as u32))))
        .collect::<Vec<_>>();

    {
        let mut reg = runtime.registry.lock().await;
        for (idx, hash) in hashes.iter().copied().enumerate() {
            let mut torrent = Torrent::new(placeholder_meta.clone(), (idx + 1) as u64);
            set_test_v1_magnet_identity(&mut torrent, hash);
            reg.add(torrent).unwrap();
        }
    }
    runtime.queue.lock().await.add_many(hashes.iter().copied());
    runtime.rate_samples.write().await.insert(
        hashes[0],
        RateSample {
            downloaded: 1,
            uploaded: 0,
            rate_down: 1,
            rate_up: 0,
            last_download_at: Some(Instant::now()),
            last_upload_at: None,
            no_download_since: None,
            at: Instant::now(),
            peak_rate_down: 1,
            peak_rate_up: 0,
        },
    );
    runtime
        .engine_retry_after
        .write()
        .await
        .insert(hashes[1], Instant::now() + ENGINE_INCOMPLETE_RETRY_DELAY);

    let removed = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.remove_torrents(hashes.clone(), false),
    )
    .await
    .expect("bulk remove should be bounded for 10,000 records")
    .unwrap();

    assert_eq!(removed.len(), REMOVE_COUNT);
    assert!(runtime.registry.lock().await.torrents.is_empty());
    assert!(runtime.queue.lock().await.order.is_empty());
    assert!(runtime.queue.lock().await.bypass.is_empty());
    assert!(runtime.rate_samples.read().await.is_empty());
    assert!(runtime.engine_retry_after.read().await.is_empty());
    assert!(runtime.engine_handles.read().await.is_empty());
}

#[tokio::test]
async fn paused_add_is_queued_without_reconcile_start() {
    let cfg = Config::default();
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    let runtime = DaemonRuntime::new(cfg, health);
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "paused-add.bin",
        b"paused add payload",
        4,
        None,
        false,
    );

    let hash = runtime
        .add_torrent_file_with_options(bytes, AddTorrentOptions::new(None, true))
        .await
        .unwrap();

    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::Paused);
    assert_eq!(summary.queue_position, Some(1));
    assert!(runtime.desired_download_hashes().await.is_empty());
    assert!(runtime.engine_handles.read().await.is_empty());
    assert!(!runtime.queue_reconcile.lock().await.scheduled);
}
