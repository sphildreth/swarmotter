// SPDX-License-Identifier: Apache-2.0

//! Tracker/webseed/peer-source diagnostics and safe tracker maintenance.

use super::*;
use swarmotter_core::models::tracker::{
    redact_tracker_url, TrackerInfo, TrackerSampleStatus, TrackerStatus, WebseedInfo,
};

const DIAGNOSTICS_MAX_BYTES: usize = 512 * 1024;

impl DaemonRuntime {
    pub(super) async fn list_webseeds(&self, hash: &TorrentKey) -> Option<Vec<WebseedInfo>> {
        let canonical = self.canonical_torrent_key(*hash).await?;
        let torrent = self.registry.lock().await.get(&canonical).cloned()?;
        let runtime = self
            .engine_states
            .read()
            .await
            .get(&canonical)
            .and_then(|state| state.try_lock().ok())
            .map(|state| state.webseeds.clone())
            .unwrap_or_default();
        Some(
            torrent
                .meta
                .webseeds
                .iter()
                .map(|url| {
                    let enabled = url::Url::parse(url)
                        .ok()
                        .is_some_and(|parsed| matches!(parsed.scheme(), "http" | "https"));
                    let snapshot = runtime.get(url);
                    WebseedInfo {
                        url: url.clone(),
                        enabled,
                        status: snapshot.map_or(TrackerStatus::NotContacted, |s| s.status),
                        bytes_verified: snapshot.map_or(0, |s| s.bytes_verified),
                        attempts: snapshot.map_or(0, |s| s.attempts),
                        retries: snapshot.map_or(0, |s| s.retries),
                        last_success: snapshot.and_then(|s| s.last_success),
                        last_attempt: snapshot.and_then(|s| s.last_attempt),
                        last_error: snapshot.and_then(|s| s.last_error.clone()),
                        recent_errors: snapshot
                            .map(|s| s.recent_errors.clone())
                            .unwrap_or_default(),
                        latency_ms: snapshot.and_then(|s| s.latency_ms),
                    }
                })
                .collect(),
        )
    }

    pub(super) async fn retry_tracker(&self, hash: &TorrentKey, url: &str) -> Result<()> {
        let canonical = self.require_torrent_key(*hash).await?;
        let trackers = <Self as DaemonOps>::list_trackers(self, &canonical)
            .await
            .unwrap_or_default();
        if !trackers.iter().any(|tracker| tracker.url == url) {
            return Err(CoreError::NotFound(format!("tracker {url}")));
        }
        self.reannounce(&canonical).await
    }

    pub(super) async fn set_tracker_enabled(
        &self,
        hash: &TorrentKey,
        url: &str,
        enabled: bool,
    ) -> Result<Vec<TrackerInfo>> {
        let canonical = self.require_torrent_key(*hash).await?;
        let previous = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(&canonical)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let known = tracker::announce_tiers(
                torrent.meta.announce.as_deref(),
                &torrent.meta.announce_list,
            )
            .iter()
            .flatten()
            .any(|candidate| candidate == url);
            if !known {
                return Err(CoreError::NotFound(format!("tracker {url}")));
            }
            let previous = torrent.clone();
            torrent
                .disabled_trackers
                .retain(|candidate| candidate != url);
            if !enabled {
                torrent.disabled_trackers.push(url.to_string());
            }
            previous
        };
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(&canonical) {
                *torrent = previous;
            }
            return Err(error);
        }
        self.restart_engine_for_settings(&canonical).await;
        self.reconcile_seeders().await;
        Ok(<Self as DaemonOps>::list_trackers(self, &canonical)
            .await
            .unwrap_or_default())
    }

    pub(super) async fn cleanup_trackers_preview(
        &self,
        hash: &TorrentKey,
        failure_threshold: u32,
    ) -> Result<Vec<TrackerInfo>> {
        if failure_threshold == 0 {
            return Err(CoreError::InvalidArgument(
                "failure threshold must be greater than zero".into(),
            ));
        }
        let canonical = self.require_torrent_key(*hash).await?;
        let trackers = <Self as DaemonOps>::list_trackers(self, &canonical)
            .await
            .unwrap_or_default();
        Ok(trackers
            .into_iter()
            .filter(|tracker| tracker.retries >= failure_threshold)
            .collect())
    }

    pub(super) async fn cleanup_trackers(
        &self,
        hash: &TorrentKey,
        failure_threshold: u32,
    ) -> Result<Vec<TrackerInfo>> {
        let affected = self
            .cleanup_trackers_preview(hash, failure_threshold)
            .await?;
        if affected.is_empty() {
            return Ok(Vec::new());
        }
        let canonical = self.require_torrent_key(*hash).await?;
        let urls = affected
            .iter()
            .map(|tracker| tracker.url.clone())
            .collect::<Vec<_>>();
        let previous = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(&canonical)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.clone();
            for url in &urls {
                if !torrent.disabled_trackers.contains(url) {
                    torrent.disabled_trackers.push(url.clone());
                }
            }
            previous
        };
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(&canonical) {
                *torrent = previous;
            }
            return Err(error);
        }
        self.restart_engine_for_settings(&canonical).await;
        self.reconcile_seeders().await;
        Ok(affected)
    }

    pub(super) async fn tracker_diagnostics(&self, hash: &TorrentKey) -> Result<String> {
        let canonical = self.require_torrent_key(*hash).await?;
        let (name, trackers, webseeds, peers) = {
            let name = self
                .registry
                .lock()
                .await
                .get(&canonical)
                .map(|torrent| torrent.meta.name.clone())
                .unwrap_or_else(|| canonical.to_locator());
            (
                name,
                <Self as DaemonOps>::list_trackers(self, &canonical)
                    .await
                    .unwrap_or_default(),
                self.list_webseeds(&canonical).await.unwrap_or_default(),
                <Self as DaemonOps>::list_peers(self, &canonical)
                    .await
                    .unwrap_or_default(),
            )
        };
        let mut text = String::new();
        text.push_str(&format!("torrent: {name}\nkey: {canonical}\n"));
        text.push_str("\ntrackers:\n");
        for tracker in trackers {
            text.push_str(&format!(
                "  {} priority={} allowed={} status={} last_success={} last_attempt={} retries={} latency_ms={} peers_returned={} sample={} policy={}\n",
                redact_tracker_url(&tracker.url),
                tracker.effective_priority.as_str(),
                tracker.allowed,
                tracker.status_str(),
                option_u64(tracker.last_success),
                option_u64(tracker.last_attempt),
                tracker.retries,
                option_u64(tracker.latency_ms),
                option_u64(tracker.peers_returned),
                sample_str(tracker.sample_status),
                tracker.policy_source,
            ));
            if let Some(error) = tracker.last_error.as_deref() {
                text.push_str(&format!("    last_error: {error}\n"));
            }
            for error in tracker.recent_errors.iter().take(8) {
                text.push_str(&format!(
                    "    recent_error: {}\n",
                    redact_tracker_url(error)
                ));
            }
        }
        text.push_str("\nwebseeds:\n");
        for webseed in webseeds {
            text.push_str(&format!(
                "  {} enabled={} status={} bytes_verified={} attempts={} retries={} last_success={} last_attempt={} latency_ms={}\n",
                redact_tracker_url(&webseed.url),
                webseed.enabled,
                tracker_status_str(webseed.status),
                webseed.bytes_verified,
                webseed.attempts,
                webseed.retries,
                option_u64(webseed.last_success),
                option_u64(webseed.last_attempt),
                option_u64(webseed.latency_ms),
            ));
            if let Some(error) = webseed.last_error.as_deref() {
                text.push_str(&format!("    last_error: {error}\n"));
            }
            for error in webseed.recent_errors.iter().take(8) {
                text.push_str(&format!("    recent_error: {}", redact_tracker_url(error)));
                text.push('\n');
            }
        }
        text.push_str("\npeers:\n");
        for peer in peers.into_iter().take(500) {
            text.push_str(&format!(
                "  {} sources={:?} banned={}\n",
                peer.address, peer.sources, peer.banned
            ));
        }
        if text.len() > DIAGNOSTICS_MAX_BYTES {
            let mut boundary = DIAGNOSTICS_MAX_BYTES;
            while !text.is_char_boundary(boundary) {
                boundary -= 1;
            }
            text.truncate(boundary);
            text.push_str("\n[truncated]\n");
        }
        Ok(text)
    }
}

fn option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "unavailable".into(), |value| value.to_string())
}

fn sample_str(value: TrackerSampleStatus) -> &'static str {
    match value {
        TrackerSampleStatus::Unavailable => "unavailable",
        TrackerSampleStatus::Stale => "stale",
        TrackerSampleStatus::Current => "current",
    }
}

fn tracker_status_str(value: TrackerStatus) -> &'static str {
    match value {
        TrackerStatus::NotContacted => "not_contacted",
        TrackerStatus::Working => "working",
        TrackerStatus::Updating => "updating",
        TrackerStatus::Ok => "ok",
        TrackerStatus::Error => "error",
        TrackerStatus::Disabled => "disabled",
    }
}

trait TrackerStatusText {
    fn status_str(&self) -> &'static str;
}

impl TrackerStatusText for TrackerInfo {
    fn status_str(&self) -> &'static str {
        tracker_status_str(self.status)
    }
}
