// SPDX-License-Identifier: Apache-2.0

//! SwarmOtter daemon entry point.
//!
//! The daemon owns torrent state, networking, disk I/O, queueing, settings,
//! and lifecycle. It exposes the API and Web UI via axum. All torrent
//! data-plane traffic is enforced through the network containment layer.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use swarmotter_core::config::Config;
use swarmotter_core::error::Result;
use swarmotter_core::net::{self, OsInterfaceProbe};
use swarmotterd::{daemon, logging};

use swarmotter_api::state::{AppState, BuildInfo};

/// Command-line arguments.
#[derive(Parser, Debug)]
#[command(name = "swarmotterd", about = "SwarmOtter BitTorrent daemon")]
struct Args {
    /// Path to the configuration file.
    #[arg(short, long, env = "SWARMOTTER_CONFIG")]
    config: Option<PathBuf>,

    /// Path to the durable torrent and queue state file.
    #[arg(long, env = "SWARMOTTER_STATE_FILE")]
    state_file: Option<PathBuf>,

    /// Validate the effective configuration and exit without starting services.
    #[arg(long)]
    check_config: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Install the rustls crypto provider (ring) so HTTPS trackers over
    // contained sockets work.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let env_vars: Vec<(String, String)> = std::env::vars().collect();
    let config = if let Some(path) = &args.config {
        Config::from_file_with_env_overrides(path, &env_vars)?
    } else {
        Config::default().apply_env_overrides(&env_vars)?
    };

    // Validate the effective configuration before logging initialization and
    // before the --check-config success message. A run without --config fails
    // unless env overrides provide a valid strict path or explicit disabled
    // mode. See ADR-0051.
    config.validate()?;

    if args.check_config {
        println!("SwarmOtter configuration is valid");
        return Ok(());
    }

    let log_file = logging::init(&config.logging)?;
    if let Some(path) = &log_file {
        tracing::info!(path = %path.display(), "daemon file logging enabled");
    }
    if let Some(path) = &args.config {
        tracing::info!(path = %path.display(), "loading configuration");
    } else {
        tracing::info!("no config file provided; using defaults");
    }
    tracing::info!(bind = %config.api.bind_address, "configured API bind address");
    let api_bind = config
        .api
        .bind_address
        .parse::<std::net::SocketAddr>()
        .map_err(|e| {
            swarmotter_core::error::CoreError::InvalidConfig(format!("api.bind_address: {e}"))
        })?;
    if !config.api.require_auth && !api_bind.ip().is_loopback() {
        tracing::warn!(
            bind = %api_bind,
            "API and Web UI authentication is disabled on a non-loopback listener; every client that can reach this address can control SwarmOtter"
        );
    }

    // Validate network containment at startup. In strict mode with fail_closed,
    // this surfaces configuration/path issues immediately rather than at first
    // torrent operation.
    let probe = OsInterfaceProbe;
    let health = net::evaluate(&config.network, &probe);
    tracing::info!(status = %health.status, traffic_allowed = health.traffic_allowed, "network containment status at startup");
    if config.network.mode != swarmotter_core::models::network::NetworkContainmentMode::Disabled
        && !health.traffic_allowed
    {
        tracing::warn!(detail = %health.detail, "torrent data plane is NOT healthy; torrents will enter network_blocked state until the path is available");
    }

    let max_request_body_bytes = config.api.max_request_body_bytes;
    let broker = swarmotter_api::handlers::events::EventBroker::default();
    let state_file = args.state_file.clone().unwrap_or_else(default_state_file);
    let runtime = Arc::new(daemon::DaemonRuntime::with_paths_broker_and_state(
        config.clone(),
        health,
        args.config.clone(),
        log_file,
        Some(state_file.clone()),
        broker.clone(),
    ));
    runtime.restore_persisted_state().await?;

    let state = Arc::new(AppState {
        daemon: runtime.clone(),
        config: Arc::new(tokio::sync::Mutex::new(config)),
        build: BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            // Build-time git commit, if provided via SWARMOTTER_BUILD_COMMIT
            // at compile time (e.g. by CI/release packaging). Honest fallback
            // rather than echoing the version as the commit.
            commit: option_env!("SWARMOTTER_BUILD_COMMIT").unwrap_or("unknown"),
            target: std::env::consts::ARCH,
        },
        broker,
        transmission: swarmotter_api::state::TransmissionCompatState::default(),
        qbittorrent: swarmotter_api::state::QbittorrentCompatState::default(),
    });

    let bind = api_bind;

    tracing::info!(%bind, "swarmotterd starting; API + Web UI on control plane");

    let serve = axum::serve(
        tokio::net::TcpListener::bind(bind)
            .await
            .map_err(swarmotter_core::error::CoreError::from)?,
        swarmotter_api::routes::app_router_with_body_limit(state.clone(), max_request_body_bytes)
            .merge(swarmotter_web::web_router())
            .into_make_service(),
    );

    // Spawn watch-folder scanner. It reads the live daemon config each pass,
    // so watch folders added through settings start working without restart.
    {
        let rt = runtime.clone();
        tokio::spawn(async move {
            rt.watch_loop().await;
        });
    }

    // Spawn the network containment health monitor so fail-closed state
    // transitions are detected while the daemon is running.
    {
        let rt = runtime.clone();
        tokio::spawn(async move {
            rt.network_health_loop().await;
        });
    }

    // Spawn the adaptive swarm autopilot. Observe mode only records decisions;
    // act mode applies bounded engine/queue commands from contained telemetry.
    {
        let rt = runtime.clone();
        tokio::spawn(async move {
            rt.autopilot_loop().await;
        });
    }

    // Graceful shutdown on Ctrl-C.
    serve
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| swarmotter_core::error::CoreError::Internal(format!("server error: {e}")))?;

    runtime.shutdown().await?;
    tracing::info!("swarmotterd stopped");
    Ok(())
}

fn default_state_file() -> PathBuf {
    if let Some(directory) = std::env::var_os("STATE_DIRECTORY") {
        if let Some(first) = std::env::split_paths(&directory).next() {
            return first.join("state.json");
        }
    }
    let packaged = PathBuf::from("/var/lib/swarmotter");
    if packaged.is_dir() {
        return packaged.join("state.json");
    }
    if let Some(directory) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(directory).join("swarmotter/state.json");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".local/state/swarmotter/state.json");
    }
    PathBuf::from("swarmotter-state.json")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install terminate handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
