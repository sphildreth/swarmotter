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
use swarmotterd::{daemon, logging, state_store};

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

    /// Rebuild verified SQLite state projections and indexes, then exit.
    ///
    /// This is an offline maintenance operation. It refuses missing, legacy,
    /// or integrity-failing state files and does not attempt to repair
    /// database corruption.
    #[arg(long, conflicts_with = "check_config")]
    rebuild_state_projections: bool,
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // Aborted engine tasks may have already submitted blocking I/O. Never let
    // Tokio's destructor turn a completed process shutdown into an endless wait.
    runtime.shutdown_timeout(std::time::Duration::from_secs(2));
    result
}

async fn run() -> Result<()> {
    let args = Args::parse();

    // Offline projection rebuild is intentionally independent of daemon
    // configuration. It must remain usable when a strict containment path is
    // currently unavailable or a stale configuration needs separate repair.
    // `--state-file` (including its environment source) still selects the
    // target; without it this uses only the platform compatibility default,
    // never `storage.state_dir` from a configuration that we deliberately do
    // not load for this maintenance command.
    if run_offline_state_maintenance(&args)? {
        return Ok(());
    }

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

    let state_file = resolve_state_file(args.state_file.clone(), &config);

    if args.check_config {
        println!("SwarmOtter configuration is valid");
        return Ok(());
    }

    let log_guard = logging::init(&config.logging)?;
    let log_file = log_guard.path.clone();
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

    // Register handlers before serving requests: a client can send SIGTERM
    // as soon as the API responds, before a lazily polled future runs.
    let shutdown_signal = shutdown_signal()?;
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

    let watchdog = runtime.start_watchdog()?;
    let mut workers = tokio::task::JoinSet::new();
    let rt = runtime.clone();
    workers.spawn(async move {
        rt.watch_loop().await;
        "watch"
    });
    let rt = runtime.clone();
    workers.spawn(async move {
        rt.network_health_loop().await;
        "network"
    });
    let rt = runtime.clone();
    workers.spawn(async move {
        rt.port_mapping_loop().await;
        "port mapping"
    });
    let rt = runtime.clone();
    workers.spawn(async move {
        rt.autopilot_loop().await;
        "autopilot"
    });

    let broker = state.broker.clone();
    let mut server = tokio::spawn(async move {
        serve
            .with_graceful_shutdown(async move { broker.closed().await })
            .await
    });
    let mut server_done = false;
    let failure = tokio::select! {
        result = shutdown_signal => result.err().map(|error| format!("shutdown signal failed: {error}")),
        ended = workers.join_next() => Some(format!("essential worker stopped: {ended:?}")),
        result = runtime.supervise_progress() => Some(format!("progress supervisor stopped: {result:?}")),
        result = &mut server => {
            server_done = true;
            Some(format!("HTTP server stopped unexpectedly: {result:?}"))
        }
    };
    runtime.begin_shutdown();
    if let Some(reason) = &failure {
        tracing::error!(%reason, "daemon recovery shutdown");
    }

    // Finish current background transactions rather than cancelling them in
    // the middle of a storage mutation. The process watchdog is the final
    // bound if either a transaction or the final checkpoint cannot finish.
    let cleanup = async {
        while let Some(result) = workers.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "worker failed during shutdown");
            }
        }
        runtime.shutdown().await?;
        if !server_done {
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut server)
                .await
                .map_err(|_| {
                    swarmotter_core::error::CoreError::Internal("HTTP drain timed out".into())
                })?
                .map_err(|e| {
                    swarmotter_core::error::CoreError::Internal(format!("HTTP task: {e}"))
                })?
                .map_err(swarmotter_core::error::CoreError::from)?;
        }
        Ok::<_, swarmotter_core::error::CoreError>(())
    };
    match tokio::time::timeout(std::time::Duration::from_secs(30), cleanup).await {
        Ok(Ok(())) => {}
        result => {
            tracing::error!(
                ?result,
                "shutdown incomplete; last committed state retained"
            );
            // A cancelled spawn_blocking operation can still run; do not
            // return into a runtime destructor that waits without a bound.
            log_guard.flush();
            std::process::exit(1);
        }
    }
    watchdog.disarm();
    if let Some(reason) = failure {
        return Err(swarmotter_core::error::CoreError::Internal(reason));
    }
    tracing::info!("swarmotterd stopped");
    Ok(())
}

/// Run an offline state operation before loading daemon configuration.
///
/// Returns whether an operation was selected. This narrow helper keeps the
/// execution ordering testable: an invalid or unavailable network
/// configuration cannot prevent a local, explicit state-file rebuild.
fn run_offline_state_maintenance(args: &Args) -> Result<bool> {
    if !args.rebuild_state_projections {
        return Ok(false);
    }
    let state_file = args.state_file.clone().unwrap_or_else(default_state_file);
    let report = state_store::rebuild_projections(&state_file)?;
    println!(
        "SwarmOtter SQLite state projections rebuilt at {} (torrents: {}, queue entries: {})",
        state_file.display(),
        report.torrents,
        report.queue_entries
    );
    Ok(true)
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

/// Resolve durable state placement with deliberate precedence. A command-line
/// or environment-supplied state file always wins; `storage.state_dir` is the
/// configured default for the next daemon start; historical platform paths
/// remain the compatibility fallback.
fn resolve_state_file(explicit: Option<PathBuf>, config: &Config) -> PathBuf {
    explicit
        .or_else(|| {
            config
                .storage
                .state_dir_path()
                .ok()
                .flatten()
                .map(|directory| directory.join("state.json"))
        })
        .unwrap_or_else(default_state_file)
}

fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = std::io::Result<()>>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => {},
                _ = terminate.recv() => {},
            }
            Ok(())
        })
    }
    #[cfg(not(unix))]
    {
        Ok(tokio::signal::ctrl_c())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_state_file_wins_over_configured_state_directory() {
        let mut config = Config::default();
        config.storage.state_dir = Some("configured-state".into());
        let explicit = PathBuf::from("explicit-state.json");
        assert_eq!(
            resolve_state_file(Some(explicit.clone()), &config),
            explicit
        );

        let configured = resolve_state_file(None, &config);
        assert!(configured.ends_with("configured-state/state.json"));
    }

    #[test]
    fn rebuild_state_projections_accepts_an_explicit_state_file() {
        let args = Args::try_parse_from([
            "swarmotterd",
            "--state-file",
            "operator-state.sqlite",
            "--rebuild-state-projections",
        ])
        .unwrap();

        assert_eq!(
            args.state_file,
            Some(PathBuf::from("operator-state.sqlite"))
        );
        assert!(args.rebuild_state_projections);
        assert!(!args.check_config);
    }

    #[test]
    fn rebuild_state_projections_and_check_config_are_mutually_exclusive() {
        assert!(Args::try_parse_from([
            "swarmotterd",
            "--check-config",
            "--rebuild-state-projections",
        ])
        .is_err());
    }

    #[test]
    fn offline_projection_rebuild_skips_invalid_config_and_uses_explicit_state_file() {
        let root = std::env::temp_dir().join(format!(
            "swarmotterd-offline-rebuild-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let state_file = root.join("operator-state.sqlite");
        state_store::save(
            &state_file,
            &state_store::DaemonState::new(
                Vec::new(),
                swarmotter_core::queue::QueueState::default(),
            ),
        )
        .unwrap();
        let args = Args {
            // The file need not exist or be valid: this offline command must
            // return before normal configuration loading/validation.
            config: Some(root.join("invalid-strict-network-config.toml")),
            state_file: Some(state_file.clone()),
            check_config: false,
            rebuild_state_projections: true,
        };

        assert!(run_offline_state_maintenance(&args).unwrap());
        assert!(state_file.is_file());
        std::fs::remove_dir_all(root).unwrap();
    }
}
