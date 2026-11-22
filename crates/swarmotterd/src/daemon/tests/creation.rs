// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use super::creation_runtime::{collect_source, hash_source};
use super::storage_controls::{is_storage_work_cancelled, StorageWorkCancellation};
use crate::engine::{EngineCommand, EngineState, TorrentEngine};
use swarmotter_core::creation::{CreationHasher, TorrentCreationFormat, TorrentCreationRequest};
use swarmotter_core::hash::TorrentIdentity;
use swarmotter_core::net::binder::LoopbackBinder;
use swarmotter_core::peer::PeerAddr;

fn creation_config(root: &Path, _state_path: &Path) -> Config {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.storage.download_dir = Some(root.join("downloads").display().to_string());
    config.storage.incomplete_dir = Some(root.join("incomplete").display().to_string());
    config.torrent.listen_port = free_port();
    config
}

fn creation_health() -> NetworkHealth {
    let mut health = disabled_health();
    health.traffic_allowed = true;
    health
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr().map(|addr| addr.port()))
        .unwrap()
}

fn start_creation_runtime(root: &Path, state_path: &Path, config: Config) -> Arc<DaemonRuntime> {
    std::fs::create_dir_all(root.join("downloads")).unwrap();
    std::fs::create_dir_all(root.join("incomplete")).unwrap();
    Arc::new(DaemonRuntime::with_paths_broker_and_state(
        config,
        creation_health(),
        None,
        None,
        Some(state_path.to_path_buf()),
        EventBroker::default(),
    ))
}

fn request(source: &Path, format: TorrentCreationFormat, seed: bool) -> TorrentCreationRequest {
    TorrentCreationRequest {
        source: source.display().to_string(),
        name: None,
        format,
        piece_length: Some(16 * 1024),
        tracker_tiers: Vec::new(),
        webseeds: Vec::new(),
        private: false,
        seed,
    }
}

fn decode(result: &swarmotter_core::creation::TorrentCreationResult) -> Vec<u8> {
    hex::decode(&result.metainfo_hex).unwrap()
}

#[tokio::test]
async fn creates_v1_v2_hybrid_file_and_directory_metainfo() {
    let root = unique_dir("creation-formats");
    let state_path = root.join("state.json");
    let config = creation_config(&root, &state_path);
    let runtime = start_creation_runtime(&root, &state_path, config);
    runtime.restore_persisted_state().await.unwrap();

    let file = root.join("downloads").join("single.bin");
    let content = b"generated single-file creation payload with a partial final piece".repeat(3);
    std::fs::write(&file, &content).unwrap();
    for format in [
        TorrentCreationFormat::V1,
        TorrentCreationFormat::V2,
        TorrentCreationFormat::Hybrid,
    ] {
        let result = runtime
            .create_torrent(request(&file, format, false))
            .await
            .unwrap();
        assert_eq!(result.name, "single.bin");
        assert_eq!(result.total_length, content.len() as u64);
        assert!(!result.seeded);
        assert!(result.tracker_tiers.is_empty());
        assert!(result.webseeds.is_empty());
        let parsed = swarmotter_core::meta::parse_torrent(&decode(&result)).unwrap();
        match format {
            TorrentCreationFormat::V1 => {
                assert!(matches!(parsed.identity, TorrentIdentity::V1 { .. }))
            }
            TorrentCreationFormat::V2 => {
                assert!(matches!(parsed.identity, TorrentIdentity::V2 { .. }))
            }
            TorrentCreationFormat::Hybrid => {
                assert!(matches!(parsed.identity, TorrentIdentity::Hybrid { .. }))
            }
        }
        if format.includes_v1() {
            assert_eq!(parsed.pieces.len(), content.len().div_ceil(16 * 1024));
        } else {
            assert!(parsed.pieces.is_empty());
            assert_eq!(
                parsed.data_piece_count().unwrap(),
                content.len().div_ceil(16 * 1024)
            );
        }
        assert_eq!(parsed.files[0].length, content.len() as u64);
    }

    let mut decorated = request(&file, TorrentCreationFormat::V1, false);
    decorated.tracker_tiers = vec![
        vec![" https://tracker.example/announce ".into()],
        vec![
            "https://backup-a.example/announce".into(),
            "https://backup-b.example/announce".into(),
        ],
    ];
    decorated.webseeds = vec!["https://seed.example/files/".into()];
    decorated.private = true;
    let result = runtime.create_torrent(decorated).await.unwrap();
    let parsed = swarmotter_core::meta::parse_torrent(&decode(&result)).unwrap();
    assert!(parsed.private);
    assert_eq!(
        parsed.announce.as_deref(),
        Some("https://tracker.example/announce")
    );
    assert_eq!(parsed.announce_list.len(), 2);
    assert_eq!(parsed.announce_list[1].len(), 2);
    assert_eq!(parsed.webseeds, vec!["https://seed.example/files/"]);

    let directory = root.join("downloads").join("bundle");
    std::fs::create_dir_all(directory.join("nested")).unwrap();
    std::fs::write(directory.join("a.bin"), vec![b'A'; 20 * 1024]).unwrap();
    std::fs::write(directory.join("nested").join("b.bin"), vec![b'B'; 3 * 1024]).unwrap();
    let result = runtime
        .create_torrent(request(&directory, TorrentCreationFormat::Hybrid, false))
        .await
        .unwrap();
    assert_eq!(result.file_count, 2);
    assert_eq!(result.total_length, 23 * 1024);
    let parsed = swarmotter_core::meta::parse_torrent(&decode(&result)).unwrap();
    assert!(parsed.is_multi_file);
    assert_eq!(parsed.files.len(), 2);
    assert_eq!(parsed.files[0].path, vec!["bundle", "a.bin"]);
    assert_eq!(parsed.files[1].path, vec!["bundle", "nested", "b.bin"]);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn creation_rejects_out_of_root_symlink_and_changed_source() {
    let root = unique_dir("creation-reject");
    let state_path = root.join("state.json");
    let config = creation_config(&root, &state_path);
    let runtime = start_creation_runtime(&root, &state_path, config);
    runtime.restore_persisted_state().await.unwrap();
    let outside = root.join("outside.bin");
    std::fs::write(&outside, b"outside source").unwrap();
    let error = runtime
        .create_torrent(request(&outside, TorrentCreationFormat::V1, false))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("outside the configured storage roots"));

    let directory = root.join("downloads").join("with-symlink");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("real.bin"), b"real").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, directory.join("link.bin")).unwrap();
    #[cfg(unix)]
    {
        let error = runtime
            .create_torrent(request(&directory, TorrentCreationFormat::V1, false))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }

    let source = root.join("downloads").join("changing.bin");
    std::fs::write(&source, b"initial payload").unwrap();
    let snapshot = collect_source(&source, None).unwrap();
    std::fs::write(&source, b"changed payload").unwrap();
    let after = collect_source(&source, None).unwrap();
    assert!(!snapshot.same_source_set(&after));
    let cancellation = StorageWorkCancellation::new();
    let error = hash_source(&snapshot, TorrentCreationFormat::V1, 16, &cancellation).unwrap_err();
    assert!(error.to_string().contains("changed"));

    cancellation.cancel();
    let snapshot = collect_source(&source, None).unwrap();
    let error = hash_source(&snapshot, TorrentCreationFormat::V1, 16, &cancellation).unwrap_err();
    assert!(is_storage_work_cancelled(&error));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn created_torrent_seeds_original_payload_to_local_peer() {
    let root = unique_dir("creation-seed");
    let state_path = root.join("state.json");
    let config = creation_config(&root, &state_path);
    let runtime = start_creation_runtime(&root, &state_path, config);
    runtime.restore_persisted_state().await.unwrap();
    let content = (0..40 * 1024 + 7)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let source = root.join("downloads").join("created-payload.bin");
    std::fs::write(&source, &content).unwrap();
    let mut create = request(&source, TorrentCreationFormat::V1, true);
    create.piece_length = Some(16 * 1024);
    let result = runtime.create_torrent(create).await.unwrap();
    assert!(result.seeded, "{result:?}");
    assert_eq!(result.seed_error, None);
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    let listener = runtime
        .seeder_listener_addr()
        .await
        .expect("created torrent should start the shared seeder listener");
    let seed_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        listener.port(),
    );

    let meta = swarmotter_core::meta::parse_torrent(&decode(&result)).unwrap();
    let leech_dir = root.join("leech");
    std::fs::create_dir_all(&leech_dir).unwrap();
    let binder = Arc::new(LoopbackBinder);
    let state = Arc::new(tokio::sync::Mutex::new(EngineState::default()));
    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    let engine = TorrentEngine::new(
        Arc::new(meta.clone()),
        leech_dir.clone(),
        [0x42; 20],
        binder,
        state,
        cmd_rx,
        vec![PeerAddr::from_socket_addr(seed_addr)],
        6881,
    );
    let final_state = tokio::time::timeout(Duration::from_secs(15), engine.run())
        .await
        .expect("local peer did not finish")
        .expect("local peer engine error");
    assert!(final_state.finished);
    assert_eq!(
        final_state.pieces_have.count(meta.piece_count()),
        meta.piece_count()
    );
    let downloaded = std::fs::read(leech_dir.join("created-payload.bin")).unwrap();
    assert_eq!(downloaded, content);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn streaming_hasher_matches_generated_metainfo() {
    let content = vec![b'x'; 20 * 1024 + 9];
    let piece_length = 16 * 1024;
    let mut hasher = CreationHasher::new(piece_length, TorrentCreationFormat::Hybrid).unwrap();
    hasher.begin_file().unwrap();
    hasher.update(&content).unwrap();
    let file = hasher.finish_file().unwrap();
    let v1 = hasher.finish_torrent().unwrap();
    assert_eq!(v1.len(), content.len().div_ceil(piece_length as usize));
    assert!(file.pieces_root.is_some());
    assert_eq!(
        file.piece_layer.len(),
        content.len().div_ceil(piece_length as usize)
    );
}

#[tokio::test]
async fn creation_hashing_waits_for_root_recheck_concurrency_permit() {
    let root = unique_dir("creation-bounded");
    let state_path = root.join("state.json");
    let mut config = creation_config(&root, &state_path);
    let downloads = root.join("downloads");
    config.storage.root_controls = vec![swarmotter_core::config::StorageRootControl {
        path: downloads.display().to_string(),
        max_active_downloads: 0,
        max_active_bytes: 0,
        max_write_bytes_per_second: 0,
        max_concurrent_rechecks: 1,
    }];
    let runtime = start_creation_runtime(&root, &state_path, config);
    runtime.restore_persisted_state().await.unwrap();
    let source = downloads.join("bounded.bin");
    std::fs::write(&source, vec![b'z'; 32 * 1024]).unwrap();

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let occupier = {
        let runtime = runtime.clone();
        let downloads = downloads.clone();
        tokio::spawn(async move {
            runtime
                .run_root_scoped_recheck(&downloads, None, async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    Ok(())
                })
                .await
        })
    };
    started_rx.await.unwrap();

    let creator = {
        let runtime = runtime.clone();
        let request = request(&source, TorrentCreationFormat::V1, false);
        tokio::spawn(async move { runtime.create_torrent(request).await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !creator.is_finished(),
        "creation hashing bypassed max_concurrent_rechecks"
    );
    let _ = release_tx.send(());
    occupier.await.unwrap().unwrap();
    let result = creator.await.unwrap().unwrap();
    assert_eq!(result.total_length, 32 * 1024);
    std::fs::remove_dir_all(root).ok();
}
