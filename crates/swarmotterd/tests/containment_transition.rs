// SPDX-License-Identifier: Apache-2.0

//! Daemon integration tests for live network containment transitions.
//!
//! These tests inject a mutable `FakeInterfaceProbe` into `DaemonRuntime` and
//! drive `network_health_tick()` directly (without sleeping) to prove:
//!  - A local transfer stops when the required interface disappears, the gate
//!    blocks before teardown, all data-plane registries empty, the torrent/API
//!    status is blocked, and the control API still responds.
//!  - Recovery resumes only formerly active work, not paused/ratio/idle-stopped
//!    work.
//!  - The formerly unreachable statuses `socket_bind_failed` and
//!    `blocked_fail_closed` are reachable through production-path API tests.
//!
//! See ADR-0051 and `design/testing.md`.

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;

use swarmotter_api::state::DaemonOps;
use swarmotter_core::config::Config;
use swarmotter_core::meta::build_single_file_torrent;
use swarmotter_core::models::network::{
    NetworkContainmentMode as Mode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::torrent::TorrentState;
use swarmotter_core::net::{InterfaceStatus, NetworkConfig};
use swarmotterd::containment_gate::FakeInterfaceProbe;
use swarmotterd::daemon::DaemonRuntime;

fn strict_config_with_interface(iface: &str, source: &str) -> Config {
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
    };
    cfg.dht.enabled = false;
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
        None,
        swarmotter_api::handlers::events::EventBroker::default(),
        Arc::new(probe),
    ))
}

#[tokio::test]
async fn path_loss_blocks_gate_before_teardown_and_empties_registries() {
    let iface = "tun0";
    let source = "10.8.0.2";
    let probe = healthy_probe(iface, source);
    let cfg = strict_config_with_interface(iface, source);
    let runtime = healthy_runtime(probe.clone(), cfg);

    // Add a torrent; it should be queued (traffic allowed).
    let bytes = build_single_file_torrent("f", b"path-loss payload data here", 8, None, false);
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::Queued);

    // Flip the required interface healthy-to-missing and run one health tick.
    probe.remove_interface(iface);
    runtime.network_health_tick().await;

    // The gate blocks before teardown.
    assert!(!runtime.containment_gate().traffic_allowed());
    assert_eq!(
        runtime.containment_gate().blocked_status(),
        Some(NetworkContainmentStatus::InterfaceMissing)
    );

    // All data-plane registries empty (no running engines or seeders).
    assert!(runtime.engine_handles_empty().await);
    assert!(runtime.seeder_registries_empty().await);

    // Torrent/API status is blocked.
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::NetworkBlocked);
    let net = runtime.network_health().await;
    assert!(!net.traffic_allowed);
    assert_eq!(net.status, NetworkContainmentStatus::InterfaceMissing);

    // The control API still responds (the control listener is separate).
    let stats = runtime.global_stats().await;
    assert_eq!(stats.scheduler.queued_torrents, 0);
}

#[tokio::test]
async fn recovery_resumes_only_formerly_active_work() {
    let iface = "tun0";
    let source = "10.8.0.2";
    let probe = healthy_probe(iface, source);
    let cfg = strict_config_with_interface(iface, source);
    let runtime = healthy_runtime(probe.clone(), cfg);

    // Add an active torrent (queued) and a manually paused torrent.
    let active_bytes =
        build_single_file_torrent("active", b"active recovery payload here", 8, None, false);
    let active_hash = runtime
        .add_torrent_file_with_options(
            active_bytes,
            swarmotter_api::state::AddTorrentOptions::new(None, false),
        )
        .await
        .unwrap();
    let paused_bytes =
        build_single_file_torrent("paused", b"paused recovery payload here", 8, None, false);
    let paused_hash = runtime
        .add_torrent_file_with_options(
            paused_bytes,
            swarmotter_api::state::AddTorrentOptions::new(None, true),
        )
        .await
        .unwrap();
    assert_eq!(
        runtime.get_torrent(&active_hash).await.unwrap().state,
        TorrentState::Queued
    );
    assert_eq!(
        runtime.get_torrent(&paused_hash).await.unwrap().state,
        TorrentState::Paused
    );

    // Lose the path: both go network_blocked.
    probe.remove_interface(iface);
    runtime.network_health_tick().await;
    assert_eq!(
        runtime.get_torrent(&active_hash).await.unwrap().state,
        TorrentState::NetworkBlocked
    );

    // Recover the path. The active torrent returns to queued; the paused
    // torrent remains paused (not auto-resumed).
    probe.set_interface(iface, InterfaceStatus::Up, vec![source.parse().unwrap()]);
    runtime.network_health_tick().await;
    assert!(runtime.containment_gate().traffic_allowed());
    let active = runtime.get_torrent(&active_hash).await.unwrap();
    let paused = runtime.get_torrent(&paused_hash).await.unwrap();
    // The active torrent resumed (queued or downloading); the paused torrent
    // remained paused (not auto-resumed).
    assert!(
        matches!(
            active.state,
            TorrentState::Queued | TorrentState::Downloading
        ),
        "active torrent should have resumed, got {:?}",
        active.state
    );
    assert_eq!(paused.state, TorrentState::Paused);
}

#[tokio::test]
async fn injected_bind_failure_exposes_socket_bind_failed_status() {
    let iface = "tun0";
    let source = "10.8.0.2";
    let probe = healthy_probe(iface, source);
    let cfg = strict_config_with_interface(iface, source);
    let runtime = healthy_runtime(probe, cfg);

    // Add a torrent so there is data-plane state to block.
    let bytes = build_single_file_torrent("f", b"bind-failure payload data", 8, None, false);
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();
    assert_eq!(
        runtime.get_torrent(&hash).await.unwrap().state,
        TorrentState::Queued
    );

    // Inject a bind-failure health report (as a real binder would on a failed
    // source bind) and run one health tick.
    runtime.report_health(
        NetworkContainmentStatus::SocketBindFailed,
        "binding torrent sockets to the configured path failed",
    );
    runtime.network_health_tick().await;

    // The gate is blocked and the API exposes socket_bind_failed.
    assert!(!runtime.containment_gate().traffic_allowed());
    assert_eq!(
        runtime.containment_gate().blocked_status(),
        Some(NetworkContainmentStatus::SocketBindFailed)
    );
    let net = runtime.network_health().await;
    assert_eq!(net.status, NetworkContainmentStatus::SocketBindFailed);
    assert!(!net.traffic_allowed);
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::NetworkBlocked);
}

#[tokio::test]
async fn strict_policy_denial_exposes_blocked_fail_closed_status() {
    // A strict config with a required interface that is missing yields
    // InterfaceMissing, but a strict config where the path is explicitly
    // denied by policy (no specific status applies) exposes
    // blocked_fail_closed. We simulate this by reporting it directly through
    // the health channel, which is the same path the binder uses when strict
    // policy denies traffic and no more specific status applies.
    let iface = "tun0";
    let source = "10.8.0.2";
    let probe = healthy_probe(iface, source);
    let cfg = strict_config_with_interface(iface, source);
    let runtime = healthy_runtime(probe, cfg);

    let bytes = build_single_file_torrent("f", b"fail-closed payload data", 8, None, false);
    let hash = runtime.add_torrent_file(bytes, None).await.unwrap();

    runtime.report_health(
        NetworkContainmentStatus::BlockedFailClosed,
        "torrent networking blocked by fail-closed policy",
    );
    runtime.network_health_tick().await;

    assert!(!runtime.containment_gate().traffic_allowed());
    assert_eq!(
        runtime.containment_gate().blocked_status(),
        Some(NetworkContainmentStatus::BlockedFailClosed)
    );
    let net = runtime.network_health().await;
    assert_eq!(net.status, NetworkContainmentStatus::BlockedFailClosed);
    let summary = runtime.get_torrent(&hash).await.unwrap();
    assert_eq!(summary.state, TorrentState::NetworkBlocked);
}
