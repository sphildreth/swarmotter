// SPDX-License-Identifier: Apache-2.0

use super::*;

impl DaemonRuntime {
    #[allow(dead_code)]
    pub fn new(config: Config, startup_health: NetworkHealth) -> Self {
        Self::with_paths(config, startup_health, None, None)
    }

    pub fn with_paths(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
    ) -> Self {
        Self::with_paths_and_broker(
            config,
            startup_health,
            config_path,
            log_file_path,
            EventBroker::default(),
        )
    }

    pub fn with_paths_and_broker(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
        event_broker: EventBroker,
    ) -> Self {
        Self::with_paths_broker_and_state(
            config,
            startup_health,
            config_path,
            log_file_path,
            None,
            event_broker,
        )
    }

    pub fn with_paths_broker_and_state(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
        state_path: Option<PathBuf>,
        event_broker: EventBroker,
    ) -> Self {
        Self::with_paths_broker_state_and_probe(
            config,
            startup_health,
            config_path,
            log_file_path,
            state_path,
            event_broker,
            Arc::new(OsInterfaceProbe),
        )
    }

    /// Construct a runtime with an injected interface probe for deterministic
    /// containment testing. Production injects `OsInterfaceProbe`; tests inject
    /// a mutable fake. See ADR-0051.
    #[allow(clippy::too_many_arguments)]
    pub fn with_paths_broker_state_and_probe(
        config: Config,
        startup_health: NetworkHealth,
        config_path: Option<PathBuf>,
        log_file_path: Option<PathBuf>,
        state_path: Option<PathBuf>,
        event_broker: EventBroker,
        interface_probe: Arc<dyn InterfaceProbe + Send + Sync>,
    ) -> Self {
        let global_limiter = swarmotter_core::bandwidth::RateLimiter::new(
            config.bandwidth.effective_download(),
            config.bandwidth.effective_upload(),
        );
        let selfish_completion_enabled = config.torrent.selfish;
        let peer_filter = match swarmotter_core::peer_filter::PeerFilter::from_config(
            &config.peer_filter,
        ) {
            Ok(filter) => Arc::new(filter),
            Err(error) => {
                tracing::error!(%error, "peer filter could not be compiled during runtime construction; peer admission is fail-closed");
                Arc::new(swarmotter_core::peer_filter::PeerFilter::fail_closed(
                    error.to_string(),
                ))
            }
        };
        let peer_sessions_denied = Arc::new(AtomicU64::new(0));
        let port_mapping = Arc::new(port_mapping::PortMappingRuntime::new(&config));
        let peer_permit_pool =
            PeerPermitPool::new(config.bandwidth.max_peers, peer_sessions_denied.clone())
                .unwrap_or_else(|_| {
                    PeerPermitPool::invalid_fail_closed(
                        config.bandwidth.max_peers,
                        peer_sessions_denied.clone(),
                    )
                });
        let containment_gate = ContainmentGate::new(startup_health.traffic_allowed);
        if !startup_health.traffic_allowed {
            containment_gate.block(startup_health.status, startup_health.detail.clone());
        }
        let (health_report_tx, health_report_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            registry: Arc::new(Mutex::new(TorrentRegistry::default())),
            queue: Arc::new(Mutex::new(QueueState::new(config.queue.clone()))),
            config: Arc::new(RwLock::new(config)),
            peer_filter: Arc::new(RwLock::new(peer_filter)),
            port_mapping,
            port_test: port_test::PortTestRuntime::default(),
            network_health: Arc::new(RwLock::new(startup_health)),
            watch_imports: Arc::new(Mutex::new(VecDeque::new())),
            watch_observations: Arc::new(Mutex::new(HashMap::new())),
            watch_scan_lock: Arc::new(Mutex::new(())),
            config_path,
            config_write_lock: Arc::new(Mutex::new(())),
            data_plane_transition_lock: Arc::new(Mutex::new(())),
            log_file_path,
            state_path,
            state_write_lock: Arc::new(Mutex::new(())),
            storage_ownership_lock: Arc::new(Mutex::new(())),
            storage_admissions: StorageAdmissionController::default(),
            storage_rechecks: StorageRecheckController::default(),
            engine_storage_cancellations: Arc::new(Mutex::new(HashMap::new())),
            explicit_rechecks: Arc::new(Mutex::new(HashMap::new())),
            engine_states: Arc::new(RwLock::new(HashMap::new())),
            engine_cmds: Arc::new(Mutex::new(HashMap::new())),
            engine_handles: Arc::new(RwLock::new(HashMap::new())),
            seeder_shutdowns: Arc::new(Mutex::new(HashMap::new())),
            seeder_registry: SeedRegistry::default(),
            seeder_lifecycle_lock: Arc::new(Mutex::new(())),
            seeder_listener_shutdown: Arc::new(Mutex::new(None)),
            seeder_listener_handle: Arc::new(Mutex::new(None)),
            seeder_handles: Arc::new(Mutex::new(HashMap::new())),
            peer_permit_pool: Arc::new(RwLock::new(peer_permit_pool)),
            torrent_peer_permit_pools: Arc::new(RwLock::new(HashMap::new())),
            peer_sessions_denied,
            selfish_completion_enabled: Arc::new(AtomicBool::new(selfish_completion_enabled)),
            #[cfg(test)]
            peer_reconfiguration_fail_after_teardown: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            peer_reconfiguration_fail_persistence: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            peer_reconfiguration_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            peer_reconfiguration_persistence_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            add_mutation_fail_persistence: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            watch_after_read_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            storage_admission_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            explicit_recheck_before_persist_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            root_control_replacement_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            generic_config_fail_after_rename: Arc::new(AtomicBool::new(false)),
            global_limiter,
            torrent_limiters: Arc::new(RwLock::new(HashMap::new())),
            rate_samples: Arc::new(RwLock::new(HashMap::new())),
            engine_retry_after: Arc::new(RwLock::new(HashMap::new())),
            autopilot_decisions: Arc::new(RwLock::new(HashMap::new())),
            autopilot_last_action: Arc::new(RwLock::new(HashMap::new())),
            dht_runner: Arc::new(Mutex::new(None)),
            queue_reconcile: Arc::new(Mutex::new(QueueReconcileState::default())),
            event_broker,
            containment_gate,
            interface_probe,
            health_report_tx,
            health_report_rx: Arc::new(Mutex::new(health_report_rx)),
            bind_failure_latched: Arc::new(RwLock::new(None)),
        }
    }
}
