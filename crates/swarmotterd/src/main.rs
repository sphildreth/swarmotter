// SPDX-License-Identifier: Apache-2.0

//! SwarmOtter daemon entry point.
//!
//! The daemon owns torrent state, networking, disk I/O, queueing, settings,
//! and lifecycle. It exposes the API and Web UI via axum. All torrent
//! data-plane traffic is enforced through the network containment layer.

mod daemon;
mod engine;
mod netbinder;
mod runtime;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use swarmotter_core::config::Config;
use swarmotter_core::error::Result;
use swarmotter_core::net::{self, OsInterfaceProbe};

use swarmotter_api::state::{AppState, BuildInfo};

/// Command-line arguments.
#[derive(Parser, Debug)]
#[command(name = "swarmotterd", about = "SwarmOtter BitTorrent daemon")]
struct Args {
    /// Path to the configuration file.
    #[arg(short, long, env = "SWARMOTTER_CONFIG")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let mut config = if let Some(path) = &args.config {
        tracing::info!(path = %path.display(), "loading configuration");
        Config::from_file(path)?
    } else {
        tracing::info!("no config file provided; using defaults");
        Config::default()
    };

    // Apply environment variable overrides.
    let env_vars: Vec<(String, String)> = std::env::vars().collect();
    config = config.apply_env_overrides(&env_vars)?;
    tracing::info!(bind = %config.api.bind_address, "configured API bind address");

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

    let runtime = Arc::new(daemon::DaemonRuntime::new(config.clone(), health));
    let broker = swarmotter_api::handlers::events::EventBroker::default();

    let state = Arc::new(AppState {
        daemon: runtime.clone(),
        config: Arc::new(tokio::sync::Mutex::new(config)),
        build: BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            commit: env!("CARGO_PKG_VERSION"),
            target: std::env::consts::ARCH,
        },
        broker,
    });

    let bind: std::net::SocketAddr =
        state
            .config
            .lock()
            .await
            .api
            .bind_address
            .parse()
            .map_err(|e| {
                swarmotter_core::error::CoreError::InvalidConfig(format!("api.bind_address: {e}"))
            })?;

    tracing::info!(%bind, "swarmotterd starting; API + Web UI on control plane");

    let serve = axum::serve(
        tokio::net::TcpListener::bind(bind)
            .await
            .map_err(swarmotter_core::error::CoreError::from)?,
        swarmotter_api::app_router(state.clone())
            .merge(swarmotter_web::web_router())
            .into_make_service(),
    );

    // Spawn watch-folder scanner if configured.
    if !state.config.lock().await.watch.is_empty() {
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

    // Graceful shutdown on Ctrl-C.
    serve
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| swarmotter_core::error::CoreError::Internal(format!("server error: {e}")))?;

    tracing::info!("swarmotterd stopped");
    Ok(())
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
