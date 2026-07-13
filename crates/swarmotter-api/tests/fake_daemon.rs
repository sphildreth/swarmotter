// SPDX-License-Identifier: Apache-2.0

//! Test support: a fake in-memory `DaemonOps` implementation.
//!
//! Only compiled under `cfg(test)` / dev-dependents so it does not pollute the
//! public API surface.

use async_trait::async_trait;
use std::sync::Arc;
use swarmotter_core::autopilot::{AutopilotAnalyzer, AutopilotConfig, AutopilotMode};
use swarmotter_core::config::{Config, PeerEncryptionMode, StartBehavior};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::meta::{self};
use swarmotter_core::models::network::{
    NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth,
};
use swarmotter_core::models::peer::Peer;
use swarmotter_core::models::stats::{
    AutopilotDecision, AutopilotInput, GlobalStats, TorrentDiagnostics,
};
use swarmotter_core::models::torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
use swarmotter_core::models::tracker::{
    TrackerId, TrackerInfo, TrackerKind, TrackerScrapeStatus, TrackerStatus, TrackerTier,
};
use swarmotter_core::models::{
    ConfigUpdateResult, DiagnosticLevel, DoctorCheck, DoctorReport, LogSnapshot,
    NetworkDiagnostics, NetworkInterfaceDiagnostic, NetworkPathCheck, ResetResult,
    WatchFolderStatus, WatchStatus,
};
use swarmotter_core::peer_filter::{ManualPeerBan, PeerFilter, PeerFilterConfig, PeerFilterStatus};
use swarmotter_core::policy::{EffectiveTorrentPolicy, PolicyProfileOrigin, PolicyStorageSnapshot};
use swarmotter_core::torrent::{Torrent, TorrentRegistry};
use swarmotter_core::watch::ImportResult;
use tokio::sync::Mutex;

use swarmotter_api::state::AddTorrentOptions;

pub struct FakeDaemon {
    registry: Arc<Mutex<TorrentRegistry>>,
    config: Arc<Mutex<Config>>,
    health: Arc<Mutex<NetworkHealth>>,
    /// Production-boundary calls observed by route-ordering tests. The origin
    /// guard must reject an unsafe browser request before this list changes.
    pub calls: Arc<Mutex<Vec<&'static str>>>,
    pub watch_imports: Arc<Mutex<Vec<ImportResult>>>,
    #[allow(dead_code)]
    pub events: Arc<Mutex<Vec<String>>>,
}

impl FakeDaemon {
    pub fn new() -> Self {
        Self::with_config(Config::default())
    }

    pub fn with_config(config: Config) -> Self {
        let mut health = NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            NetworkContainmentStatus::Disabled,
            "disabled for tests",
        );
        health.traffic_allowed = true;
        Self {
            registry: Arc::new(Mutex::new(TorrentRegistry::default())),
            config: Arc::new(Mutex::new(config)),
            health: Arc::new(Mutex::new(health)),
            calls: Arc::new(Mutex::new(Vec::new())),
            events: Arc::new(Mutex::new(Vec::new())),
            watch_imports: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn insert(&self, torrent: Torrent) -> Result<InfoHash> {
        let hash = torrent.info_hash();
        let mut reg = self.registry.lock().await;
        reg.add(torrent)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_hex()))?;
        Ok(hash)
    }

    async fn summary(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        let torrent = self.registry.lock().await.get(hash).cloned()?;
        let config = self.config.lock().await.clone();
        let policy = EffectiveTorrentPolicy::resolve(&config, &torrent);
        let mut summary = torrent.to_summary();
        summary.effective_ratio_limit = (!policy.seed_forever.value)
            .then_some(policy.ratio_limit.value)
            .flatten();
        summary.effective_idle_limit = (!policy.seed_forever.value)
            .then_some(policy.idle_limit.value)
            .flatten();
        Some(summary)
    }

    async fn record_call(&self, call: &'static str) {
        self.calls.lock().await.push(call);
    }

    async fn remove_manual_ban(&self, ip: String) -> Result<PeerFilterStatus> {
        let ip = ip.trim().parse::<std::net::IpAddr>().map_err(|error| {
            CoreError::InvalidArgument(format!("manual peer ban IP '{ip}': {error}"))
        })?;
        let mut next = self.config.lock().await.clone();
        next.peer_filter
            .manual_bans
            .retain(|existing| existing.ip.trim().parse::<std::net::IpAddr>() != Ok(ip));
        next.validate()?;
        *self.config.lock().await = next.clone();
        Ok(PeerFilter::from_config(&next.peer_filter)?.status())
    }

    #[allow(dead_code)]
    pub async fn clear_calls(&self) {
        self.calls.lock().await.clear();
    }

    #[allow(dead_code)]
    pub async fn observed_calls(&self) -> Vec<&'static str> {
        self.calls.lock().await.clone()
    }

    /// Snapshot mutable fake-daemon state without recording an API operation.
    /// This lets security tests prove rejected requests did not cause a hidden
    /// state transition even if a handler later gains additional calls.
    #[allow(dead_code)]
    pub async fn state_snapshot(&self) -> serde_json::Value {
        let torrents = self
            .registry
            .lock()
            .await
            .list()
            .iter()
            .map(|torrent| torrent.to_summary())
            .collect::<Vec<_>>();
        let config = self.config.lock().await.clone();
        serde_json::json!({
            "torrents": torrents,
            "config": config,
        })
    }
}

async fn apply_fake_add_policy(
    config: &Mutex<Config>,
    torrent: &mut Torrent,
    options: &AddTorrentOptions,
) -> Result<bool> {
    torrent.labels = options.labels.clone();
    if let Some(profile) = options.profile.as_ref() {
        let config = config.lock().await.clone();
        if !config.profiles.profiles.contains_key(profile) {
            return Err(CoreError::InvalidArgument(format!(
                "unknown policy profile {profile}"
            )));
        }
        torrent.policy.profile = Some(profile.clone());
        torrent.policy.profile_origin = Some(PolicyProfileOrigin::AddRequest);
    }
    let config = config.lock().await.clone();
    let effective = EffectiveTorrentPolicy::resolve(&config, torrent);
    if torrent.policy.storage_snapshot.is_none() {
        torrent.policy.storage_snapshot = Some(PolicyStorageSnapshot {
            profile: effective
                .profile
                .as_ref()
                .map(|profile| profile.name.clone())
                .unwrap_or_default(),
            preserve_existing_storage: false,
            download_dir: effective.download_dir.value,
            incomplete_dir: effective.incomplete_dir.value,
        });
    }
    let initial_start_behavior = if options.start_behavior_explicit {
        if options.paused {
            StartBehavior::Paused
        } else {
            StartBehavior::Start
        }
    } else {
        effective.start_behavior.value
    };
    torrent.policy.initial_start_behavior = Some(initial_start_behavior);
    let profile_requests_pause = effective
        .profile
        .as_ref()
        .and_then(|assignment| config.profiles.profiles.get(&assignment.name))
        .and_then(|profile| profile.queue.start_behavior)
        .is_some_and(|behavior| matches!(behavior, StartBehavior::Paused));
    Ok(if options.start_behavior_explicit {
        options.paused
    } else {
        profile_requests_pause
    })
}

impl Default for FakeDaemon {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl swarmotter_api::state::DaemonOps for FakeDaemon {
    async fn list_torrents(&self) -> Vec<TorrentSummary> {
        let torrents = self
            .registry
            .lock()
            .await
            .list()
            .iter()
            .map(|t| (*t).clone())
            .collect::<Vec<_>>();
        let config = self.config.lock().await.clone();
        torrents
            .iter()
            .map(|torrent| {
                let policy = EffectiveTorrentPolicy::resolve(&config, torrent);
                let mut summary = torrent.to_summary();
                summary.effective_ratio_limit = (!policy.seed_forever.value)
                    .then_some(policy.ratio_limit.value)
                    .flatten();
                summary.effective_idle_limit = (!policy.seed_forever.value)
                    .then_some(policy.idle_limit.value)
                    .flatten();
                summary
            })
            .collect()
    }
    async fn get_torrent(&self, hash: &InfoHash) -> Option<TorrentSummary> {
        self.summary(hash).await
    }
    async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        options: AddTorrentOptions,
    ) -> Result<InfoHash> {
        self.record_call("add_torrent_file").await;
        let meta = meta::parse_torrent(&bytes)?;
        let mut t = Torrent::new(meta, now());
        t.download_dir = options.download_dir.clone();
        let paused = apply_fake_add_policy(&self.config, &mut t, &options).await?;
        if paused {
            t.state = TorrentState::Paused;
        }
        self.insert(t).await
    }
    async fn add_magnet(&self, magnet: &str, options: AddTorrentOptions) -> Result<InfoHash> {
        self.record_call("add_magnet").await;
        let m = Magnet::parse(magnet)?;
        // Build a minimal single-file meta from the magnet for testing.
        let bytes = meta::build_single_file_torrent(
            m.display_name.as_deref().unwrap_or("magnet"),
            b"magnet placeholder data",
            16,
            m.trackers.first().map(|s| s.as_str()),
            false,
        );
        let meta = meta::parse_torrent(&bytes)?;
        let mut t = Torrent::new(meta, now());
        t.download_dir = options.download_dir.clone();
        let paused = apply_fake_add_policy(&self.config, &mut t, &options).await?;
        t.state = if paused {
            TorrentState::Paused
        } else {
            TorrentState::DownloadingMetadata
        };
        self.insert(t).await
    }
    async fn remove_torrent(&self, hash: &InfoHash, _delete_data: bool) -> Result<()> {
        self.record_call("remove_torrent").await;
        self.registry
            .lock()
            .await
            .remove(hash)
            .map(|_| ())
            .ok_or_else(|| CoreError::NotFound("torrent".into()))
    }
    async fn pause(&self, hash: &InfoHash) -> Result<()> {
        self.record_call("pause").await;
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.state = TorrentState::Paused;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn resume(&self, hash: &InfoHash) -> Result<()> {
        self.record_call("resume").await;
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.state = TorrentState::Downloading;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn start_now(&self, hash: &InfoHash) -> Result<()> {
        self.resume(hash).await
    }
    async fn stop(&self, hash: &InfoHash) -> Result<()> {
        self.pause(hash).await
    }
    async fn recheck(&self, hash: &InfoHash) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.state = TorrentState::Checking;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn reannounce(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn move_data(&self, hash: &InfoHash, path: String) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.download_dir = Some(path);
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn rename_path(
        &self,
        _hash: &InfoHash,
        _file_index: usize,
        _new_path: String,
    ) -> Result<()> {
        Ok(())
    }
    async fn set_labels(&self, hash: &InfoHash, labels: Vec<String>) -> Result<()> {
        let config = self.config.lock().await.clone();
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                if t.policy.storage_snapshot.is_none() {
                    let effective = EffectiveTorrentPolicy::resolve(&config, t);
                    t.policy.storage_snapshot = Some(PolicyStorageSnapshot {
                        profile: String::new(),
                        preserve_existing_storage: true,
                        download_dir: if t.download_dir.is_none() {
                            effective.download_dir.value
                        } else {
                            None
                        },
                        incomplete_dir: if t.policy.overrides.incomplete_dir.is_none() {
                            effective.incomplete_dir.value
                        } else {
                            None
                        },
                    });
                }
                t.labels = labels;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn set_torrent_limits(
        &self,
        hash: &InfoHash,
        limits: swarmotter_core::bandwidth::TorrentBandwidth,
    ) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.download_limit = limits.download;
                t.upload_limit = limits.upload;
                t.policy.overrides.download_limit = Some(limits.download);
                t.policy.overrides.upload_limit = Some(limits.upload);
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn set_torrent_seeding(
        &self,
        hash: &InfoHash,
        seeding: swarmotter_core::ratio::TorrentSeeding,
    ) -> Result<TorrentSummary> {
        self.record_call("set_torrent_seeding").await;
        {
            let mut reg = self.registry.lock().await;
            let torrent = reg
                .get_mut(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            torrent.seeding = seeding;
            torrent.policy.overrides.ratio_limit = torrent.seeding.ratio_limit;
            torrent.policy.overrides.idle_limit = torrent.seeding.idle_limit;
            torrent.policy.overrides.seed_forever = Some(torrent.seeding.seed_forever);
        }
        self.summary(hash)
            .await
            .ok_or_else(|| CoreError::NotFound("torrent".into()))
    }
    async fn list_files(&self, hash: &InfoHash) -> Option<Vec<TorrentFile>> {
        self.registry
            .lock()
            .await
            .get(hash)
            .map(|t| t.files.clone())
    }
    async fn set_wanted(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        wanted: bool,
    ) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                for i in file_indices {
                    if i < t.wanted.len() {
                        t.wanted[i] = wanted;
                        t.files[i].wanted = wanted;
                    }
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn set_priority(
        &self,
        hash: &InfoHash,
        file_indices: Vec<usize>,
        priority: FilePriority,
    ) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                for i in file_indices {
                    if i < t.priorities.len() {
                        t.priorities[i] = priority;
                        t.files[i].priority = priority;
                    }
                }
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn list_trackers(&self, hash: &InfoHash) -> Option<Vec<TrackerInfo>> {
        self.registry.lock().await.get(hash).map(|t| {
            let mut out = Vec::new();
            let mut tier = 0;
            if let Some(a) = &t.meta.announce {
                out.push(TrackerInfo {
                    id: TrackerId(a.clone()),
                    url: a.clone(),
                    kind: TrackerKind::from_url(a).unwrap_or(TrackerKind::Http),
                    tier,
                    status: TrackerStatus::NotContacted,
                    seeders: 0,
                    leechers: 0,
                    downloads: 0,
                    last_error: None,
                    last_message: None,
                    next_announce: None,
                    last_announce: None,
                    scrape_status: TrackerScrapeStatus::NotContacted,
                    last_scrape: None,
                    scrape_seeders: None,
                    scrape_leechers: None,
                    scrape_downloads: None,
                    last_scrape_error: None,
                });
                tier += 1;
            }
            for t_list in &t.meta.announce_list {
                for url in t_list {
                    out.push(TrackerInfo {
                        id: TrackerId(url.clone()),
                        url: url.clone(),
                        kind: TrackerKind::from_url(url).unwrap_or(TrackerKind::Http),
                        tier,
                        status: TrackerStatus::NotContacted,
                        seeders: 0,
                        leechers: 0,
                        downloads: 0,
                        last_error: None,
                        last_message: None,
                        next_announce: None,
                        last_announce: None,
                        scrape_status: TrackerScrapeStatus::NotContacted,
                        last_scrape: None,
                        scrape_seeders: None,
                        scrape_leechers: None,
                        scrape_downloads: None,
                        last_scrape_error: None,
                    });
                }
                tier += 1;
            }
            out
        })
    }
    async fn add_tracker(&self, hash: &InfoHash, _url: String) -> Result<()> {
        if self.registry.lock().await.get(hash).is_none() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        Ok(())
    }
    async fn remove_tracker(&self, hash: &InfoHash, _url: String) -> Result<()> {
        if self.registry.lock().await.get(hash).is_none() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        Ok(())
    }
    async fn edit_tracker(
        &self,
        hash: &InfoHash,
        _old_url: String,
        _new_url: String,
    ) -> Result<()> {
        if self.registry.lock().await.get(hash).is_none() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        Ok(())
    }
    async fn list_peers(&self, hash: &InfoHash) -> Option<Vec<Peer>> {
        self.registry.lock().await.get(hash)?;
        Some(Vec::new())
    }
    async fn peer_filter_status(&self) -> PeerFilterStatus {
        let config = self.config.lock().await.clone();
        PeerFilter::from_config(&config.peer_filter)
            .unwrap_or_else(|error| PeerFilter::fail_closed(error.to_string()))
            .status()
    }
    async fn replace_peer_filter(&self, peer_filter: PeerFilterConfig) -> Result<PeerFilterStatus> {
        self.record_call("replace_peer_filter").await;
        let mut next = self.config.lock().await.clone();
        next.peer_filter = peer_filter;
        next.validate()?;
        *self.config.lock().await = next.clone();
        Ok(PeerFilter::from_config(&next.peer_filter)?.status())
    }
    async fn ban_peer(&self, hash: &InfoHash, ban: ManualPeerBan) -> Result<PeerFilterStatus> {
        self.record_call("ban_peer").await;
        if self.registry.lock().await.get(hash).is_none() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        let ip = ban.ip.trim().parse::<std::net::IpAddr>().map_err(|error| {
            CoreError::InvalidArgument(format!("manual peer ban IP '{}': {error}", ban.ip))
        })?;
        let mut next = self.config.lock().await.clone();
        next.peer_filter.enabled = true;
        let ban = ManualPeerBan {
            ip: ip.to_string(),
            reason: ban.reason.map(|reason| reason.trim().to_string()),
        };
        if let Some(existing) = next.peer_filter.manual_bans.iter_mut().find(|existing| {
            existing
                .ip
                .trim()
                .parse::<std::net::IpAddr>()
                .is_ok_and(|current| current == ip)
        }) {
            *existing = ban;
        } else {
            next.peer_filter.manual_bans.push(ban);
        }
        next.validate()?;
        *self.config.lock().await = next.clone();
        Ok(PeerFilter::from_config(&next.peer_filter)?.status())
    }
    async fn unban_peer(&self, hash: &InfoHash, ip: String) -> Result<PeerFilterStatus> {
        self.record_call("unban_peer").await;
        if self.registry.lock().await.get(hash).is_none() {
            return Err(CoreError::NotFound("torrent".into()));
        }
        self.remove_manual_ban(ip).await
    }
    async fn unban_global_peer(&self, ip: String) -> Result<PeerFilterStatus> {
        self.record_call("unban_global_peer").await;
        self.remove_manual_ban(ip).await
    }
    async fn queue_move_up(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn queue_move_down(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn queue_move_to_top(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn queue_move_to_bottom(&self, _hash: &InfoHash) -> Result<()> {
        Ok(())
    }
    async fn get_config(&self) -> Config {
        self.record_call("get_config").await;
        self.config.lock().await.clone()
    }
    async fn update_settings(&self, patch: swarmotter_api::state::SettingsPatch) -> Result<()> {
        self.record_call("update_settings").await;
        let mut cfg = self.config.lock().await;
        if let Some(b) = patch.bandwidth {
            cfg.bandwidth = b;
        }
        if let Some(q) = patch.queue {
            cfg.queue = q;
        }
        if let Some(s) = patch.seeding {
            cfg.seeding = s;
        }
        if let Some(autopilot) = patch.autopilot {
            cfg.autopilot = autopilot;
        }
        Ok(())
    }
    async fn replace_config(&self, config: Config) -> Result<ConfigUpdateResult> {
        self.record_call("replace_config").await;
        config.validate()?;
        *self.config.lock().await = config.clone();
        let mut redacted = config;
        redacted.api.auth_token = None;
        redacted.network.socks5.password = None;
        Ok(ConfigUpdateResult {
            persisted: false,
            config_path: None,
            restart_required: false,
            restart_required_fields: Vec::new(),
            applied_runtime_fields: vec!["config".into()],
            config: redacted,
        })
    }
    async fn torrent_policy(
        &self,
        hash: &InfoHash,
    ) -> Option<swarmotter_core::policy::EffectiveTorrentPolicy> {
        let torrent = self.registry.lock().await.get(hash).cloned()?;
        let config = self.config.lock().await.clone();
        Some(EffectiveTorrentPolicy::resolve(&config, &torrent))
    }
    async fn set_torrent_profile(&self, hash: &InfoHash, profile: Option<String>) -> Result<()> {
        let config = self.config.lock().await.clone();
        if let Some(profile) = profile.as_ref() {
            if !config.profiles.profiles.contains_key(profile) {
                return Err(CoreError::InvalidArgument(format!(
                    "unknown policy profile {profile}"
                )));
            }
        }
        let mut registry = self.registry.lock().await;
        let torrent = registry
            .get_mut(hash)
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        if torrent.policy.storage_snapshot.is_none() {
            let effective = EffectiveTorrentPolicy::resolve(&config, torrent);
            torrent.policy.storage_snapshot = Some(PolicyStorageSnapshot {
                profile: String::new(),
                preserve_existing_storage: true,
                download_dir: if torrent.download_dir.is_none() {
                    effective.download_dir.value
                } else {
                    None
                },
                incomplete_dir: if torrent.policy.overrides.incomplete_dir.is_none() {
                    effective.incomplete_dir.value
                } else {
                    None
                },
            });
        }
        torrent.policy.profile = profile;
        torrent.policy.profile_origin = torrent
            .policy
            .profile
            .as_ref()
            .map(|_| PolicyProfileOrigin::Torrent);
        Ok(())
    }
    async fn set_torrent_encryption_mode(
        &self,
        hash: &InfoHash,
        encryption_mode: Option<PeerEncryptionMode>,
    ) -> Result<()> {
        let mut registry = self.registry.lock().await;
        let torrent = registry
            .get_mut(hash)
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        torrent.policy.overrides.encryption_mode = encryption_mode;
        Ok(())
    }
    async fn reset_downloads(&self) -> Result<ResetResult> {
        let removed = self.registry.lock().await.torrents.len();
        self.registry.lock().await.torrents.clear();
        Ok(ResetResult {
            torrents_removed: removed,
            storage_paths: Vec::new(),
            storage_entries_removed: 0,
            log_paths: Vec::new(),
            log_files_cleared: 0,
        })
    }
    async fn network_health(&self) -> NetworkHealth {
        self.health.lock().await.clone()
    }
    async fn network_diagnostics(&self) -> NetworkDiagnostics {
        let health = self.network_health().await;
        let cfg = self.config.lock().await.clone();
        NetworkDiagnostics {
            listen_port: cfg.torrent.listen_port,
            dht_port: cfg.dht.port,
            torrent_allow_ipv6: cfg.torrent.allow_ipv6,
            utp_enabled: cfg.torrent.utp_enabled,
            utp_prefer_tcp: cfg.torrent.utp_prefer_tcp,
            peer_encryption_mode: cfg.torrent.encryption_mode,
            socks5_enabled: cfg.network.socks5.enabled,
            socks5_udp_blocked: cfg.network.socks5.enabled,
            interfaces: vec![NetworkInterfaceDiagnostic {
                name: "lo".into(),
                status: "up".into(),
                addresses: vec!["127.0.0.1".into(), "::1".into()],
                selected: false,
                has_ipv4: true,
                has_ipv6: true,
            }],
            checks: vec![NetworkPathCheck {
                id: "network_containment".into(),
                label: "Network containment".into(),
                level: if health.traffic_allowed {
                    DiagnosticLevel::Ok
                } else {
                    DiagnosticLevel::Invalid
                },
                detail: health.detail.clone(),
            }],
            containment_matrix: Vec::new(),
            health,
        }
    }
    async fn doctor_report(&self) -> DoctorReport {
        let network = self.network_health().await;
        let mut level = if network.traffic_allowed {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Invalid
        };
        let watch = self.watch_status().await;
        if watch.folders.is_empty() {
            level = DiagnosticLevel::worst(level, DiagnosticLevel::Warning);
        }
        DoctorReport {
            level,
            summary: match level {
                DiagnosticLevel::Ok => "all configured checks passed".into(),
                DiagnosticLevel::Warning => "one or more checks need attention".into(),
                DiagnosticLevel::Invalid => "one or more checks are invalid".into(),
            },
            checks: vec![
                DoctorCheck {
                    id: "network".into(),
                    label: "Network containment".into(),
                    level: if network.traffic_allowed {
                        DiagnosticLevel::Ok
                    } else {
                        DiagnosticLevel::Invalid
                    },
                    detail: network.detail,
                    remediation: None,
                },
                DoctorCheck {
                    id: "watch".into(),
                    label: "Watch folders".into(),
                    level: if watch.folders.is_empty() {
                        DiagnosticLevel::Warning
                    } else {
                        DiagnosticLevel::Ok
                    },
                    detail: if watch.folders.is_empty() {
                        "no watch folders are configured".into()
                    } else {
                        "watch folders are configured".into()
                    },
                    remediation: None,
                },
            ],
        }
    }
    async fn recent_logs(&self, _max_lines: usize) -> LogSnapshot {
        LogSnapshot {
            enabled: true,
            path: None,
            lines: vec!["fake daemon log line".into()],
            truncated: false,
        }
    }
    async fn global_stats(&self) -> GlobalStats {
        let reg = self.registry.lock().await;
        GlobalStats {
            torrent_count: reg.torrents.len(),
            ..Default::default()
        }
    }
    async fn torrent_stats(&self, hash: &InfoHash) -> Option<TorrentDiagnostics> {
        let reg = self.registry.lock().await;
        let t = reg.get(hash)?;
        let total_length = t.meta.total_length;
        let bytes_completed = t.bytes_completed();
        Some(TorrentDiagnostics {
            info_hash: t.info_hash(),
            name: t.name().to_string(),
            state: t.state,
            total_length,
            bytes_completed,
            downloaded: t.downloaded,
            uploaded: t.uploaded,
            piece_count: t.meta.piece_count(),
            pieces_have: t.pieces_have(),
            piece_length: t.meta.piece_length,
            progress: if total_length == 0 {
                0.0
            } else {
                bytes_completed as f64 / total_length as f64
            },
            rate_down: t.rate_down,
            rate_up: t.rate_up,
            download_limit: t.download_limit,
            upload_limit: t.upload_limit,
            active_peer_workers: 0,
            known_peers: 0,
            peer_scheduler: None,
            useful_peers: None,
            choked_peers: None,
            unchoked_peers: None,
            recent_peer_failures: None,
            recent_tracker_failures: None,
            tracker_ok: false,
            tracker_message: None,
            last_announce: None,
            tracker_last_ok_seconds_ago: None,
            dht_discovery_ok: None,
            dht_last_seen_seconds_ago: None,
            pex_discovery_ok: None,
            pex_last_seen_seconds_ago: None,
            private: t.meta.is_private(),
        })
    }
    async fn autopilot_status(&self) -> AutopilotConfig {
        self.config.lock().await.autopilot.clone()
    }
    async fn torrent_autopilot_decision(&self, hash: &InfoHash) -> Option<AutopilotDecision> {
        let torrent = self.registry.lock().await.get(hash).cloned()?;
        let mode = {
            let cfg = self.config.lock().await;
            torrent
                .autopilot_mode_override
                .unwrap_or(cfg.autopilot.mode)
        };
        let input = AutopilotInput {
            state: torrent.state,
            rate_down: torrent.rate_down,
            rate_up: torrent.rate_up,
            rate_down_observed_peak: torrent.rate_down.max(8192),
            download_limit: torrent.download_limit,
            piece_count: torrent.meta.piece_count(),
            pieces_have: torrent.pieces_have(),
            known_peers: torrent.known_peers,
            useful_peers: Some(torrent.known_peers.min(1)),
            active_peer_workers: torrent.active_peer_workers,
            discovered_peers: Some(torrent.known_peers.saturating_add(1)),
            eligible_peers: Some(torrent.known_peers),
            peer_worker_limit: Some(torrent.known_peers.saturating_add(2)),
            backed_off_peers: Some(0),
            tracker_ok: torrent.state.is_active(),
            tracker_recent_ok_seconds_ago: if torrent.state.is_active() {
                Some(0)
            } else {
                None
            },
            tracker_failures_recent: 0,
            dht_discovery_ok: Some(torrent.state.is_active()),
            dht_last_seen_seconds_ago: if torrent.state.is_active() {
                Some(0)
            } else {
                None
            },
            pex_discovery_ok: Some(torrent.state.is_active()),
            pex_last_seen_seconds_ago: if torrent.state.is_active() {
                Some(0)
            } else {
                None
            },
            no_progress_seconds: if torrent.state.is_active() {
                Some(0)
            } else {
                None
            },
            peer_failures_recent: Some(0),
            serial_peer_active: false,
            ..Default::default()
        };
        Some(AutopilotAnalyzer::new().analyze(&input, mode))
    }
    async fn set_torrent_autopilot_mode_override(
        &self,
        hash: &InfoHash,
        mode: Option<AutopilotMode>,
    ) -> Result<()> {
        let mut reg = self.registry.lock().await;
        match reg.get_mut(hash) {
            Some(t) => {
                t.autopilot_mode_override = mode;
                Ok(())
            }
            None => Err(CoreError::NotFound("torrent".into())),
        }
    }
    async fn watch_scan(&self) -> Result<()> {
        Ok(())
    }
    async fn watch_status(&self) -> WatchStatus {
        let cfg = self.config.lock().await.clone();
        let history = self.watch_imports.lock().await.clone();
        WatchStatus {
            enabled: !cfg.watch.is_empty(),
            folders: cfg
                .watch
                .into_iter()
                .map(|config| {
                    let exists = std::path::Path::new(&config.path).is_dir();
                    WatchFolderStatus {
                        config,
                        exists,
                        pending_torrent_files: 0,
                        last_result: history.last().cloned(),
                    }
                })
                .collect(),
            recent_imports: history,
        }
    }
    async fn watch_history(&self) -> Vec<ImportResult> {
        self.watch_imports.lock().await.clone()
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Build a shared state backed by a fresh fake daemon.
pub fn fake_state() -> swarmotter_api::state::SharedState {
    fake_state_with_config(Config::default())
}

pub fn fake_state_with_config(config: Config) -> swarmotter_api::state::SharedState {
    fake_state_with_config_and_daemon(config).0
}

pub fn fake_state_with_config_and_daemon(
    config: Config,
) -> (swarmotter_api::state::SharedState, Arc<FakeDaemon>) {
    use swarmotter_api::state::{AppState, BuildInfo};
    let daemon = Arc::new(FakeDaemon::with_config(config.clone()));
    let state = Arc::new(AppState {
        daemon: daemon.clone(),
        config: Arc::new(Mutex::new(config)),
        build: BuildInfo::default(),
        broker: swarmotter_api::handlers::events::EventBroker::default(),
        transmission: swarmotter_api::state::TransmissionCompatState::default(),
        qbittorrent: swarmotter_api::state::QbittorrentCompatState::default(),
    });
    (state, daemon)
}

// Suppress unused import warnings for models referenced only in trait impls.
#[allow(unused_imports)]
use TrackerTier as _;
