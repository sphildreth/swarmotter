// SPDX-License-Identifier: Apache-2.0

//! Production-path integration tests for ADR-0051 live containment.
//!
//! All payload, tracker, peer, UDP, and control traffic is generated locally.
//! The runtime uses its real contained binder and engine; only the interface
//! inventory is a mutable injected probe so a path transition is deterministic.

#![allow(clippy::field_reassign_with_default)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tower::ServiceExt;

use swarmotter_api::state::{AddTorrentOptions, AppState, BuildInfo, DaemonOps};
use swarmotter_core::config::{Config, PeerEncryptionMode};
use swarmotter_core::error::CoreError;
use swarmotter_core::hash::TorrentKey;
use swarmotter_core::meta::{build_single_file_torrent, parse_torrent, TorrentMeta};
use swarmotter_core::models::network::{
    NetworkContainmentMode as Mode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::torrent::TorrentState;
use swarmotter_core::net::{InterfaceStatus, NetworkConfig};
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, PeerAddr};
use swarmotterd::containment_gate::FakeInterfaceProbe;
use swarmotterd::daemon::DaemonRuntime;

fn unique_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "swarmotter-containment-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn pick_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn replace_config_with_available_listener(runtime: &Arc<DaemonRuntime>, config: &mut Config) {
    const MAX_PORT_ATTEMPTS: usize = 16;
    let mut last_addr_in_use = None;

    for _ in 0..MAX_PORT_ATTEMPTS {
        // The listener cannot remain reserved while replacement validates the
        // same address. Retry only the narrow TOCTOU case where another
        // parallel test claims the ephemeral port after pick_port() drops it.
        config.torrent.listen_port = pick_port();
        match runtime.replace_config(config.clone()).await {
            Ok(_) => return,
            Err(CoreError::NetworkBlocked(detail)) if detail.contains("Address already in use") => {
                last_addr_in_use = Some(detail);
            }
            Err(error) => panic!("configuration replacement unexpectedly failed: {error}"),
        }
    }

    panic!(
        "configuration replacement could not reserve a listener after {MAX_PORT_ATTEMPTS} attempts: {}",
        last_addr_in_use.unwrap_or_else(|| "address remained unavailable".into())
    );
}

fn strict_config_with_interface(iface: &str, source: &str, root: &std::path::Path) -> Config {
    let mut cfg = Config::default();
    cfg.network = NetworkConfig {
        mode: Mode::Strict,
        required_interface: Some(iface.into()),
        required_source_ipv4: Some(source.into()),
        required_source_ipv6: None,
        required_network_namespace: None,
        allow_ipv6: false,
        fail_closed: true,
        validate_route: false,
        validate_dns: false,
        socks5: Default::default(),
    };
    cfg.api.require_auth = false;
    cfg.dht.enabled = false;
    cfg.pex.enabled = false;
    cfg.torrent.utp_enabled = false;
    cfg.torrent.encryption_mode = PeerEncryptionMode::Disabled;
    cfg.torrent.listen_port = pick_port();
    cfg.bandwidth.max_peers_per_torrent = 1;
    cfg.storage.download_dir = Some(root.join("complete").display().to_string());
    cfg.storage.incomplete_dir = Some(root.join("incomplete").display().to_string());
    cfg
}

fn healthy_probe(iface: &str, source: &str) -> FakeInterfaceProbe {
    let probe = FakeInterfaceProbe::new();
    probe.set_interface(iface, InterfaceStatus::Up, vec![source.parse().unwrap()]);
    probe.set_route_valid(true);
    probe.set_dns_ok(true);
    probe
}

fn healthy_runtime(probe: FakeInterfaceProbe, cfg: Config) -> Arc<DaemonRuntime> {
    healthy_runtime_with_state(probe, cfg, None)
}

fn healthy_runtime_with_state(
    probe: FakeInterfaceProbe,
    cfg: Config,
    state_path: Option<std::path::PathBuf>,
) -> Arc<DaemonRuntime> {
    let health = NetworkHealth {
        mode: cfg.network.mode,
        status: NetworkContainmentStatus::Healthy,
        required_interface: cfg.network.required_interface.clone(),
        required_source_ipv4: cfg.network.required_source_ipv4.clone(),
        required_source_ipv6: cfg.network.required_source_ipv6.clone(),
        allow_ipv6: cfg.network.allow_ipv6,
        fail_closed: cfg.network.fail_closed,
        detail: "healthy".into(),
        traffic_allowed: true,
    };
    Arc::new(DaemonRuntime::with_paths_broker_state_and_probe(
        cfg,
        health,
        None,
        None,
        state_path,
        swarmotter_api::handlers::events::EventBroker::default(),
        Arc::new(probe),
    ))
}

fn app_state(runtime: Arc<DaemonRuntime>, config: Config) -> swarmotter_api::state::SharedState {
    let daemon: Arc<dyn DaemonOps> = runtime;
    Arc::new(AppState {
        daemon,
        config: Arc::new(Mutex::new(config)),
        build: BuildInfo::default(),
        broker: swarmotter_api::handlers::events::EventBroker::default(),
        transmission: swarmotter_api::state::TransmissionCompatState::default(),
        qbittorrent: swarmotter_api::state::QbittorrentCompatState::default(),
    })
}

fn peer_id(prefix: &[u8; 8]) -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(prefix);
    id
}

async fn spawn_slow_seed(
    content: Arc<Vec<u8>>,
    meta: TorrentMeta,
    bytes_served: Arc<AtomicU64>,
) -> PeerAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let content = content.clone();
            let meta = meta.clone();
            let bytes_served = bytes_served.clone();
            tokio::spawn(async move {
                let _ = serve_slow_seed(stream, content, meta, bytes_served).await;
            });
        }
    });
    PeerAddr::from_socket_addr(addr)
}

async fn serve_slow_seed(
    stream: tokio::net::TcpStream,
    content: Arc<Vec<u8>>,
    meta: TorrentMeta,
    bytes_served: Arc<AtomicU64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let info_hash = meta.info_hash;
    let piece_count = meta.piece_count();
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut encoded = [0u8; 68];
    rd.read_exact(&mut encoded).await?;
    let request = Handshake::decode(&encoded).map_err(|error| error.to_string())?;
    if request.info_hash != info_hash {
        return Err("info hash mismatch".into());
    }
    wr.write_all(
        &Handshake {
            info_hash,
            peer_id: peer_id(b"-SWSLOW-"),
            reserved: peer::RESERVED,
        }
        .encode(),
    )
    .await?;
    let mut bitfield = Bitfield::new(piece_count);
    for piece in 0..piece_count {
        bitfield.set(piece);
    }
    peer::write_message(&mut wr, &bitfield.encode_message()).await?;
    wr.flush().await?;

    loop {
        let mut length = [0u8; 4];
        rd.read_exact(&mut length).await?;
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 {
            continue;
        }
        let mut body = vec![0u8; length];
        rd.read_exact(&mut body).await?;
        let message_id = body[0];
        let payload = &body[1..];
        if peer::MessageId::from_u8(message_id) == Some(peer::MessageId::Interested) {
            peer::write_message(&mut wr, &Message::Unchoke).await?;
            wr.flush().await?;
        } else if peer::MessageId::from_u8(message_id) == Some(peer::MessageId::Request)
            && payload.len() == 12
        {
            let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
            let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let block_length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
            let (piece_start, _) = meta.piece_byte_range(piece as u64).unwrap();
            let start = usize::try_from(piece_start + u64::from(offset)).unwrap();
            let end = start + block_length as usize;
            let block = content[start..end].to_vec();
            peer::write_message(
                &mut wr,
                &Message::Piece {
                    piece,
                    offset,
                    block,
                },
            )
            .await?;
            wr.flush().await?;
            bytes_served.fetch_add(u64::from(block_length), Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn spawn_tracker(listener: tokio::net::TcpListener, seed: PeerAddr) {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = [0u8; 4096];
                let _ =
                    tokio::time::timeout(Duration::from_secs(2), stream.read(&mut request)).await;
                let mut peers = Vec::new();
                if let std::net::IpAddr::V4(ip) = seed.ip {
                    peers.extend_from_slice(&ip.octets());
                    peers.extend_from_slice(&seed.port.to_be_bytes());
                }
                let mut body = b"d8:intervali30e8:completei1e10:incompletei1e5:peers".to_vec();
                body.extend_from_slice(format!("{}:", peers.len()).as_bytes());
                body.extend_from_slice(&peers);
                body.push(b'e');
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(headers.as_bytes()).await;
                let _ = stream.write_all(&body).await;
                let _ = stream.flush().await;
            });
        }
    });
}

struct ActiveTransfer {
    runtime: Arc<DaemonRuntime>,
    config: Config,
    probe: FakeInterfaceProbe,
    hash: TorrentKey,
    bytes_served: Arc<AtomicU64>,
    root: std::path::PathBuf,
    state_path: std::path::PathBuf,
}

async fn start_active_transfer(label: &str) -> ActiveTransfer {
    let iface = "lo";
    let source = "127.0.0.1";
    let root = unique_dir(label);
    let config = strict_config_with_interface(iface, source, &root);
    let probe = healthy_probe(iface, source);
    let state_path = root.join("state.json");
    let runtime =
        healthy_runtime_with_state(probe.clone(), config.clone(), Some(state_path.clone()));

    let tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tracker_url = format!("http://{}/announce", tracker.local_addr().unwrap());
    let content = Arc::new(
        (0..4 * 1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let torrent_bytes = build_single_file_torrent(
        "generated-containment.bin",
        &content,
        16 * 1024,
        Some(&tracker_url),
        false,
    );
    let meta = parse_torrent(&torrent_bytes).unwrap();
    let bytes_served = Arc::new(AtomicU64::new(0));
    let seed = spawn_slow_seed(content, meta, bytes_served.clone()).await;
    spawn_tracker(tracker, seed).await;
    let hash = runtime
        .add_torrent_file_with_options(torrent_bytes, AddTorrentOptions::new(None, false))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let state = runtime.get_torrent(&hash).await.unwrap().state;
            if state == TorrentState::Downloading
                && bytes_served.load(Ordering::SeqCst) > 0
                && !runtime.engine_handles_empty().await
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real local payload transfer did not become active");

    ActiveTransfer {
        runtime,
        config,
        probe,
        hash,
        bytes_served,
        root,
        state_path,
    }
}

async fn get_json(
    runtime: Arc<DaemonRuntime>,
    config: Config,
    uri: &str,
) -> (StatusCode, serde_json::Value) {
    let response = swarmotter_api::app_router(app_state(runtime, config))
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_payload_path_loss_blocks_udp_tears_down_tasks_and_keeps_control_api() {
    let fixture = start_active_transfer("active-loss").await;
    let binder = fixture.runtime.data_plane_binder_for_test().await;
    let udp_spy = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_addr = udp_spy.local_addr().unwrap();
    let existing_udp = binder.udp_socket_for(Some(udp_addr)).await.unwrap();
    existing_udp.send_to(udp_addr, b"before").await.unwrap();
    let mut datagram = [0u8; 16];
    tokio::time::timeout(Duration::from_secs(1), udp_spy.recv_from(&mut datagram))
        .await
        .unwrap()
        .unwrap();
    let served_before_loss = fixture.bytes_served.load(Ordering::SeqCst);
    assert!(
        served_before_loss > 0,
        "fixture did not prove payload traffic"
    );

    fixture.probe.remove_interface("lo");
    fixture.runtime.network_health_tick().await;

    assert!(!fixture.runtime.containment_gate().traffic_allowed());
    assert_eq!(
        fixture.runtime.containment_gate().blocked_status(),
        Some(NetworkContainmentStatus::InterfaceMissing)
    );
    assert!(fixture.runtime.engine_handles_empty().await);
    assert!(fixture.runtime.seeder_registries_empty().await);
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&fixture.hash)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );
    let error = existing_udp.send_to(udp_addr, b"after").await.unwrap_err();
    assert!(error.is_network_blocked());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), udp_spy.recv_from(&mut datagram))
            .await
            .is_err(),
        "an already-created UDP socket sent after containment blocked"
    );
    // A block already written by the peer before the gate edge may finish its
    // local accounting after teardown. Establish the post-abort baseline,
    // then prove it cannot advance further without new contained requests.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let served_after_teardown = fixture.bytes_served.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        fixture.bytes_served.load(Ordering::SeqCst),
        served_after_teardown,
        "payload traffic continued after containment teardown"
    );

    let (status, network) = get_json(
        fixture.runtime.clone(),
        fixture.config.clone(),
        "/api/v1/network/health",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(network["data"]["status"], "interface_missing");
    assert_eq!(network["data"]["traffic_allowed"], false);
    let (status, _) = get_json(fixture.runtime.clone(), fixture.config.clone(), "/health").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "control health route stopped responding"
    );
    std::fs::remove_dir_all(&fixture.root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_consumes_only_durable_formerly_live_intent() {
    let fixture = start_active_transfer("recovery-intent").await;
    let paused = fixture
        .runtime
        .add_torrent_file_with_options(
            build_single_file_torrent("paused", b"paused", 4, None, false),
            AddTorrentOptions::new(None, true),
        )
        .await
        .unwrap();
    let queued = fixture
        .runtime
        .add_torrent_file_with_options(
            build_single_file_torrent("queued", b"queued", 4, None, false),
            AddTorrentOptions::new(None, true),
        )
        .await
        .unwrap();
    let ratio_stopped = fixture
        .runtime
        .add_torrent_file_with_options(
            build_single_file_torrent("ratio", b"ratio", 4, None, false),
            AddTorrentOptions::new(None, true),
        )
        .await
        .unwrap();
    let idle_stopped = fixture
        .runtime
        .add_torrent_file_with_options(
            build_single_file_torrent("idle", b"idle", 4, None, false),
            AddTorrentOptions::new(None, true),
        )
        .await
        .unwrap();
    let preexisting_block = fixture
        .runtime
        .add_torrent_file_with_options(
            build_single_file_torrent("preblocked", b"preblocked", 4, None, false),
            AddTorrentOptions::new(None, true),
        )
        .await
        .unwrap();
    let stale_active = fixture
        .runtime
        .add_torrent_file_with_options(
            build_single_file_torrent("stale", b"stale", 4, None, false),
            AddTorrentOptions::new(None, true),
        )
        .await
        .unwrap();
    {
        let mut registry = fixture.runtime.registry.lock().await;
        registry.get_mut(&queued).unwrap().state = TorrentState::Queued;
        let ratio = registry.get_mut(&ratio_stopped).unwrap();
        ratio.state = TorrentState::Completed;
        ratio.downloaded = 1;
        ratio.uploaded = 2;
        let idle = registry.get_mut(&idle_stopped).unwrap();
        idle.state = TorrentState::Completed;
        idle.date_completed = Some(0);
        registry.get_mut(&preexisting_block).unwrap().state = TorrentState::NetworkBlocked;
        registry.get_mut(&stale_active).unwrap().state = TorrentState::Downloading;
    }

    fixture.probe.remove_interface("lo");
    fixture.runtime.network_health_tick().await;
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&fixture.hash)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );
    assert_eq!(
        fixture.runtime.get_torrent(&paused).await.unwrap().state,
        TorrentState::Paused
    );
    assert_eq!(
        fixture.runtime.get_torrent(&queued).await.unwrap().state,
        TorrentState::Queued
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&ratio_stopped)
            .await
            .unwrap()
            .state,
        TorrentState::Completed
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&idle_stopped)
            .await
            .unwrap()
            .state,
        TorrentState::Completed
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&preexisting_block)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&stale_active)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );

    fixture.probe.set_interface(
        "lo",
        InterfaceStatus::Up,
        vec!["127.0.0.1".parse().unwrap()],
    );
    fixture.runtime.network_health_tick().await;
    assert!(fixture.runtime.containment_gate().traffic_allowed());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !fixture.runtime.engine_handles_empty().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("formerly active download did not restart");
    assert_ne!(
        fixture
            .runtime
            .get_torrent(&fixture.hash)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );
    assert_eq!(
        fixture.runtime.get_torrent(&queued).await.unwrap().state,
        TorrentState::Queued,
        "ordinary queued work was incorrectly started by containment recovery"
    );
    assert!(
        !fixture
            .runtime
            .engine_running_for_key_for_test(queued)
            .await
    );
    assert_eq!(
        fixture.runtime.get_torrent(&paused).await.unwrap().state,
        TorrentState::Paused
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&ratio_stopped)
            .await
            .unwrap()
            .state,
        TorrentState::Completed
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&idle_stopped)
            .await
            .unwrap()
            .state,
        TorrentState::Completed
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&preexisting_block)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&stale_active)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked,
        "stale modeled activity was incorrectly granted recovery intent"
    );
    assert!(
        !fixture
            .runtime
            .engine_running_for_key_for_test(stale_active)
            .await
    );
    fixture.runtime.shutdown().await.unwrap();
    std::fs::remove_dir_all(&fixture.root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_intent_survives_restart_while_path_remains_blocked() {
    let fixture = start_active_transfer("restart-intent").await;
    fixture.probe.remove_interface("lo");
    fixture.runtime.network_health_tick().await;
    assert_eq!(
        fixture
            .runtime
            .get_torrent(&fixture.hash)
            .await
            .unwrap()
            .state,
        TorrentState::NetworkBlocked
    );
    assert!(
        fixture.state_path.is_file(),
        "blocked intent was not persisted"
    );

    let config = fixture.config.clone();
    let hash = fixture.hash;
    let state_path = fixture.state_path.clone();
    let root = fixture.root.clone();
    drop(fixture.runtime);

    let blocked_probe = FakeInterfaceProbe::new();
    blocked_probe.set_route_valid(true);
    blocked_probe.set_dns_ok(true);
    let blocked_health = swarmotter_core::net::evaluate(&config.network, &blocked_probe);
    assert!(!blocked_health.traffic_allowed);
    let restored = Arc::new(DaemonRuntime::with_paths_broker_state_and_probe(
        config.clone(),
        blocked_health,
        None,
        None,
        Some(state_path),
        swarmotter_api::handlers::events::EventBroker::default(),
        Arc::new(blocked_probe.clone()),
    ));
    assert_eq!(restored.restore_persisted_state().await.unwrap(), 1);
    assert_eq!(
        restored.get_torrent(&hash).await.unwrap().state,
        TorrentState::NetworkBlocked
    );
    assert!(!restored.engine_running_for_key_for_test(hash).await);

    blocked_probe.set_interface(
        "lo",
        InterfaceStatus::Up,
        vec!["127.0.0.1".parse().unwrap()],
    );
    restored.network_health_tick().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if restored.engine_running_for_key_for_test(hash).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("restored formerly-live intent did not restart");
    assert_ne!(
        restored.get_torrent(&hash).await.unwrap().state,
        TorrentState::NetworkBlocked
    );
    restored.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_source_bind_failure_latches_until_validated_config_replacement() {
    let root = unique_dir("bind-latch");
    let bad = strict_config_with_interface("lo", "192.0.2.254", &root);
    let probe = healthy_probe("lo", "192.0.2.254");
    let runtime = healthy_runtime(probe.clone(), bad.clone());
    let binder = runtime.data_plane_binder_for_test().await;

    assert!(binder.udp_socket().await.is_err());
    assert!(
        !runtime.containment_gate().traffic_allowed(),
        "bind failure was not an immediate fail-close"
    );
    runtime.network_health_tick().await;
    let (status, network) = get_json(runtime.clone(), bad.clone(), "/api/v1/network/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(network["data"]["status"], "socket_bind_failed");

    // A healthy fake probe alone must not clear a concrete bind failure.
    runtime.network_health_tick().await;
    assert_eq!(
        runtime.network_health().await.status,
        NetworkContainmentStatus::SocketBindFailed
    );
    assert!(!runtime.containment_gate().traffic_allowed());

    let failed_repair = runtime.replace_config(bad.clone()).await.unwrap_err();
    assert!(failed_repair.is_network_blocked());
    assert_eq!(
        runtime.network_health().await.status,
        NetworkContainmentStatus::SocketBindFailed,
        "failed bind revalidation cleared the latched status"
    );
    assert!(
        !runtime.containment_gate().traffic_allowed(),
        "failed bind revalidation reopened the gate"
    );

    let mut repaired = bad;
    repaired.network.required_source_ipv4 = Some("127.0.0.1".into());
    probe.set_interface(
        "lo",
        InterfaceStatus::Up,
        vec!["127.0.0.1".parse().unwrap()],
    );
    replace_config_with_available_listener(&runtime, &mut repaired).await;
    assert!(runtime.containment_gate().traffic_allowed());
    assert_eq!(
        runtime.network_health().await.status,
        NetworkContainmentStatus::Healthy
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_policy_denial_exposes_blocked_fail_closed_through_control_api() {
    let root = unique_dir("policy-denial");
    let mut cfg = strict_config_with_interface("lo", "127.0.0.1", &root);
    // Source-only policy lets the IPv6 family exercise the generic strict
    // denial while retaining a healthy, specific IPv4 path.
    cfg.network.required_interface = None;
    cfg.network.allow_ipv6 = true;
    let probe = healthy_probe("lo", "127.0.0.1");
    let runtime = healthy_runtime(probe, cfg.clone());
    let binder = runtime.data_plane_binder_for_test().await;
    let denied = match binder
        .udp_socket_for(Some("[::1]:9".parse().unwrap()))
        .await
    {
        Ok(_) => panic!("uncontained IPv6 socket unexpectedly opened"),
        Err(error) => error,
    };
    assert!(denied.is_network_blocked());
    runtime.network_health_tick().await;

    let (status, network) = get_json(runtime.clone(), cfg, "/api/v1/network/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(network["data"]["status"], "blocked_fail_closed");
    assert_eq!(network["data"]["traffic_allowed"], false);
    std::fs::remove_dir_all(root).ok();
}
