// SPDX-License-Identifier: Apache-2.0

use super::lifecycle::ExplicitRecheckRestoreState;
use super::*;
use futures_util::StreamExt;
use swarmotter_api::state::DaemonOps;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn unique_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-daemon-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A self-contained, structurally valid pure-BEP-52 fixture with a file no
/// larger than its piece length, so no piece-layer entries are required. The
/// daemon test only exercises metainfo admission; no payload is transferred.
fn pure_v2_single_file_fixture() -> Vec<u8> {
    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d4:infod9:file treed10:lawful.bind0:d6:lengthi1e11:pieces root32:");
    torrent.extend_from_slice(&[0x42; 32]);
    torrent.extend_from_slice(
        b"eee12:meta versioni2e4:name10:lawful.bin12:piece lengthi16384ee12:piece layersdee",
    );
    torrent
}

/// A small hybrid fixture whose v1 piece layout is usable by the current
/// transfer engine while retaining an independently hashed BEP 52 identity.
fn hybrid_v1_compatible_fixture() -> Vec<u8> {
    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d4:infod9:file treed10:hybrid.bind0:d6:lengthi1e11:pieces root32:");
    // SHA-256 of the lawful generated single-byte payload `x`.
    torrent.extend_from_slice(&[
        0x2d, 0x71, 0x16, 0x42, 0xb7, 0x26, 0xb0, 0x44, 0x01, 0x62, 0x7c, 0xa9, 0xfb, 0xac, 0x32,
        0xf5, 0xc8, 0x53, 0x0f, 0xb1, 0x90, 0x3c, 0xc4, 0xdb, 0x02, 0x25, 0x87, 0x17, 0x92, 0x1a,
        0x48, 0x81,
    ]);
    torrent.extend_from_slice(
        b"eee6:lengthi1e12:meta versioni2e4:name10:hybrid.bin12:piece lengthi16384e6:pieces20:",
    );
    // SHA-1 of the same lawful generated single-byte payload `x`.
    torrent.extend_from_slice(&[
        0x11, 0xf6, 0xad, 0x8e, 0xc5, 0x2a, 0x29, 0x84, 0xab, 0xaa, 0xfd, 0x7c, 0x3b, 0x51, 0x65,
        0x03, 0x78, 0x5c, 0x20, 0x72,
    ]);
    torrent.extend_from_slice(b"e12:piece layersdee");
    torrent
}

async fn add_complete_seed_fixture(
    runtime: &DaemonRuntime,
    name: &str,
    content: &[u8],
) -> (TorrentKey, Arc<swarmotter_core::bandwidth::RateLimiter>) {
    let bytes = swarmotter_core::meta::build_single_file_torrent(name, content, 8, None, false);
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let root = runtime
        .config
        .read()
        .await
        .storage
        .download_dir
        .clone()
        .unwrap();
    let storage = swarmotter_core::storage::StorageIo::new(meta.clone(), PathBuf::from(root));
    for piece in 0..meta.piece_count() {
        let start = piece * meta.piece_length as usize;
        let end = (start + meta.piece_length as usize).min(content.len());
        storage
            .write_piece(piece, &content[start..end])
            .await
            .unwrap();
    }
    let mut torrent = Torrent::new(meta.clone(), now());
    torrent.state = TorrentState::Completed;
    torrent.downloaded = meta.total_length;
    torrent.date_completed = Some(now());
    torrent.seeding.seed_forever = true;
    for piece in 0..meta.piece_count() {
        torrent.progress.have_piece(piece);
    }
    torrent.recompute_file_bytes_completed();
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    let limiter = runtime.ensure_torrent_limiter(hash, 0, 0).await;
    (hash, limiter)
}

async fn assert_seeder_state_registry_invariant(runtime: &DaemonRuntime) {
    let _lifecycle = runtime.seeder_lifecycle_lock.lock().await;
    let live = runtime.seeder_registry.keys().await;
    let registry = runtime.registry.lock().await;
    for hash in &live {
        let torrent = registry.get(hash).expect("live seeder has a torrent");
        assert_eq!(torrent.state, TorrentState::Seeding);
        assert_eq!(torrent.seeding_status, SeedingStatus::Active);
    }
    for (hash, torrent) in &registry.torrents {
        if torrent.state != TorrentState::NetworkBlocked
            && (torrent.state == TorrentState::Seeding
                || torrent.seeding_status == SeedingStatus::Active)
        {
            assert!(live.contains(hash), "modeled active seeder is not live");
        }
    }
}

async fn peer_reconfiguration_fixture(
    label: &str,
) -> (DaemonRuntime, TorrentKey, PathBuf, PathBuf) {
    let root = unique_dir(label);
    let config_path = root.join("swarmotter.toml");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    cfg.bandwidth.max_peers = 3;
    cfg.bandwidth.max_peers_per_torrent = 2;
    cfg.queue.max_active_seeds = 1;
    cfg.seeding.global_ratio_limit = None;
    cfg.seeding.global_idle_limit = None;
    write_config_atomically(&config_path, &cfg).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_and_broker(
        cfg,
        health,
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );
    let (hash, _) = add_complete_seed_fixture(
        &runtime,
        "peer-reconfiguration-seed.bin",
        b"generated lawful peer reconfiguration fixture",
    )
    .await;
    runtime.reconcile_seeders().await;
    assert!(runtime.seeder_registry.contains(&hash).await);
    (runtime, hash, root, config_path)
}

async fn active_engine_reconfiguration_fixture(
    label: &str,
) -> (DaemonRuntime, TorrentKey, PathBuf, PathBuf) {
    let root = unique_dir(label);
    let config_path = root.join("swarmotter.toml");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    cfg.torrent.listen_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    cfg.torrent.encryption_mode = swarmotter_core::config::PeerEncryptionMode::Disabled;
    cfg.dht.enabled = false;
    cfg.pex.enabled = false;
    cfg.bandwidth.max_peers = 3;
    cfg.bandwidth.max_peers_per_torrent = 2;
    write_config_atomically(&config_path, &cfg).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_and_broker(
        cfg,
        health,
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "active-peer-reconfiguration.bin",
        b"generated active engine peer reconfiguration fixture",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let hash = TorrentKey::v1(meta.info_hash);
    let mut torrent = Torrent::new(meta, now());
    torrent.state = TorrentState::Downloading;
    runtime.registry.lock().await.add(torrent).unwrap();
    runtime.queue.lock().await.add(hash);
    runtime.ensure_torrent_peer_permit_pool(hash).await;
    runtime.start_engine(hash).await;
    assert!(runtime.engine_running_for_key_for_test(hash).await);
    (runtime, hash, root, config_path)
}

fn scale_hash_bytes(n: u32) -> [u8; 20] {
    let mut bytes = [0u8; 20];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    bytes
}

/// Give a synthetic metadata-placeholder torrent the same canonical owner as
/// the v1 magnet it represents. These scale fixtures deliberately reuse one
/// parsed placeholder metainfo record, so its raw metainfo identity must not
/// become the registry key.
fn set_test_v1_magnet_identity(torrent: &mut Torrent, key: TorrentKey) {
    let info_hash = key
        .as_v1()
        .expect("synthetic magnet fixtures use a v1 torrent key");
    torrent.magnet_info_hash = Some(info_hash);
    torrent.magnet_identity = Some(swarmotter_core::hash::TorrentIdentity::v1(info_hash));
}

fn watch_test_config(
    root: &Path,
    start_behavior: swarmotter_core::config::StartBehavior,
) -> Config {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.watch = vec![swarmotter_core::config::WatchFolderConfig {
        path: root.display().to_string(),
        recursive: false,
        download_dir: None,
        label: None,
        profile: None,
        start_behavior,
        archive_dir: None,
        failure_dir: None,
        delete_after_import: false,
    }];
    config
}

fn disabled_health() -> NetworkHealth {
    NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
        "disabled",
    )
}

mod autopilot;
mod config_data_plane;
mod health;
mod intake;
mod metadata_identity;
mod peer_engine;
mod persistence;
mod profiles;
mod queue;
mod recheck_control;
mod reconcile;
mod scheduler;
mod trackers;
mod watch;

// --- Coalesced peer worker limit distribution (ADR-0071) ---

#[tokio::test]
async fn apply_peer_worker_limits_never_blocks_on_a_saturated_engine_channel() {
    let root = unique_dir("peer-worker-limit-shared");
    let config_path = root.join("swarmotter.toml");
    let mut cfg = Config::default();
    cfg.network.mode = NetworkContainmentMode::Disabled;
    cfg.storage.download_dir = Some(root.display().to_string());
    write_config_atomically(&config_path, &cfg).unwrap();
    let mut health = NetworkHealth::blocked(
        NetworkContainmentMode::Disabled,
        NetworkContainmentStatus::Disabled,
        "disabled",
    );
    health.traffic_allowed = true;
    let runtime = DaemonRuntime::with_paths_and_broker(
        cfg,
        health,
        Some(config_path.clone()),
        None,
        EventBroker::default(),
    );

    // Register a fake engine channel that no receiver drains. The old
    // per-tick command fan-out would eventually block on this channel; the
    // shared-state distribution must never touch it.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<EngineCommand>(8);
    for _ in 0..8 {
        tx.send(EngineCommand::Reannounce).await.unwrap();
    }
    let fake_key = TorrentKey::v1(swarmotter_core::hash::InfoHash::from_bytes([0x11u8; 20]));
    runtime.engine_cmds.lock().await.insert(fake_key, tx);
    assert!(runtime.engine_cmds.lock().await.contains_key(&fake_key));

    runtime.config.write().await.bandwidth.max_peers_per_torrent = 5;
    for _ in 0..100 {
        tokio::time::timeout(Duration::from_secs(1), runtime.apply_peer_worker_limits())
            .await
            .expect("coalesced limit application must never wait on an engine channel");
    }
    assert_eq!(runtime.shared_peer_limit.load_default(), 5);
    // The fake engine channel still holds exactly its eight prefilled
    // commands: nothing was enqueued by limit distribution.
    let mut drained = 0;
    while rx.try_recv().is_ok() {
        drained += 1;
    }
    assert_eq!(drained, 8);

    std::fs::remove_dir_all(root).ok();
}
