// SPDX-License-Identifier: Apache-2.0

//! Local trash/quarantine preview, apply, restore, purge, and retention.
//!
//! Trash entries are filesystem-backed. Every move uses exclusive/no-overwrite
//! semantics, and staging directories make interrupted moves inspectable.

use super::*;
use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use swarmotter_core::trash::{
    evaluate_retention_rule, validate_collision_policy, RestoreRequest, RestoreResult,
    TrashApplyItem, TrashApplyResult, TrashEntryManifest, TrashEntryView, TrashLocation, TrashPath,
    TrashPreview, TrashPreviewItem, TrashRetentionRule, TrashSelectionMode, TrashSelectionRequest,
    TrashStaleFile, TrashStatus,
};

static TRASH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct PreparedLocation {
    role: &'static str,
    original_root: PathBuf,
    partial_file_suffix: Option<String>,
    storage: swarmotter_core::storage::StorageIo,
}

struct PreparedItem {
    torrent: Torrent,
    key: TorrentKey,
    effective_profile: Option<String>,
    rule: Option<TrashRetentionRule>,
    rule_evaluation: Option<swarmotter_core::trash::TrashRuleEvaluation>,
    paths: Vec<TrashPath>,
    stale_files: Vec<PathBuf>,
    eligible: bool,
    explicit: bool,
}

impl DaemonRuntime {
    pub(super) async fn trash_status(&self) -> Result<TrashStatus> {
        let config = self.config.read().await.clone();
        let Some(root) = trash_root(&config)? else {
            return Ok(TrashStatus {
                enabled: false,
                path: None,
                retention: config.trash.retention.clone(),
                entries: Vec::new(),
            });
        };
        let entries = scan_trash_entries(&root).await?;
        Ok(TrashStatus {
            enabled: true,
            path: Some(root.display().to_string()),
            retention: config.trash.retention.clone(),
            entries,
        })
    }

    pub(super) async fn preview_trash(
        &self,
        request: TrashSelectionRequest,
    ) -> Result<TrashPreview> {
        let (preview, _) = self.prepare_trash(request).await?;
        Ok(preview)
    }

    pub(super) async fn apply_trash(
        &self,
        request: TrashSelectionRequest,
    ) -> Result<TrashApplyResult> {
        let (preview, prepared) = self.prepare_trash(request.clone()).await?;
        if request.hashes.is_empty() && request.rule.is_none() {
            return Err(CoreError::InvalidArgument(
                "trash apply requires explicit hashes or a retention rule; use preview first"
                    .into(),
            ));
        }
        let mut items = Vec::new();
        for item in prepared {
            if !item.eligible {
                items.push(TrashApplyItem {
                    torrent_key: item.key.to_locator(),
                    name: item.torrent.meta.name.clone(),
                    action: match request.mode {
                        TrashSelectionMode::Torrent => "trash",
                        TrashSelectionMode::StalePartials => "trash_stale",
                    }
                    .into(),
                    status: "skipped".into(),
                    entry_id: None,
                    moved_bytes: 0,
                    paths_moved: 0,
                    error: Some("not eligible after execution-time revalidation".into()),
                });
                continue;
            }
            let result = match request.mode {
                TrashSelectionMode::Torrent => self.trash_torrent_record(&item).await,
                TrashSelectionMode::StalePartials => self.trash_stale_files(&item).await,
            };
            items.push(result);
        }
        let _ = preview;
        Ok(TrashApplyResult {
            mode: request.mode,
            items,
        })
    }

    pub(super) async fn restore_trash(
        &self,
        entry_id: &str,
        request: RestoreRequest,
    ) -> Result<RestoreResult> {
        validate_collision_policy(request.collision_policy.as_deref())?;
        let config = self.config.read().await.clone();
        let root = trash_root(&config)?.ok_or_else(|| {
            CoreError::InvalidConfig("trash is not configured; set trash.path".into())
        })?;
        let entry_dir = validated_entry_dir(&root, entry_id)?;
        let manifest = read_manifest(&entry_dir).await?;
        if manifest.kind == "payload" {
            return restore_payload_entry(&entry_dir, &manifest).await;
        }
        restore_torrent_entry(self, &entry_dir, &manifest).await
    }

    pub(super) async fn purge_trash(&self, entry_id: &str) -> Result<()> {
        let config = self.config.read().await.clone();
        let root = trash_root(&config)?.ok_or_else(|| {
            CoreError::InvalidConfig("trash is not configured; set trash.path".into())
        })?;
        let entry_dir = validated_entry_dir(&root, entry_id)?;
        if !entry_dir.exists() {
            let staging = root.join(".staging").join(entry_id);
            if !staging.exists() {
                return Err(CoreError::NotFound(format!("trash entry {entry_id}")));
            }
            tokio::fs::remove_dir_all(&staging).await?;
            return Ok(());
        }
        tokio::fs::remove_dir_all(&entry_dir).await?;
        Ok(())
    }

    async fn prepare_trash(
        &self,
        request: TrashSelectionRequest,
    ) -> Result<(TrashPreview, Vec<PreparedItem>)> {
        let config = self.config.read().await.clone();
        trash_root(&config)?.ok_or_else(|| {
            CoreError::InvalidConfig("trash is not configured; set trash.path".into())
        })?;
        let requested_rule = request.rule.as_deref().map(|name| {
            config
                .trash
                .rule(name)
                .cloned()
                .ok_or_else(|| CoreError::NotFound(format!("trash retention rule {name}")))
        });
        let requested_rule = requested_rule.transpose()?;
        let explicit_keys = request
            .hashes
            .iter()
            .map(|value| TorrentKey::from_locator(value))
            .collect::<Result<Vec<_>>>()?;
        let explicit = !explicit_keys.is_empty();
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .iter()
            .map(|(key, torrent)| (*key, torrent.clone()))
            .collect::<Vec<_>>();
        let now = now();
        let mut prepared = Vec::new();
        for (key, torrent) in &torrents {
            let key = *key;
            let torrent = torrent.clone();
            if explicit && !explicit_keys.contains(&key) {
                continue;
            }
            let effective_profile = self
                .effective_policy(&torrent)
                .await
                .profile
                .as_ref()
                .map(|profile| profile.name.clone());
            let rule_eval = requested_rule.as_ref().map(|rule| {
                evaluate_retention_rule(rule, &torrent, effective_profile.as_deref(), now)
            });
            if let Some(rule_eval) = &rule_eval {
                if !rule_eval.matches {
                    continue;
                }
            }
            if !explicit && requested_rule.is_none() {
                // Without an explicit rule, preview every torrent for stale
                // partials, but only rule-qualified matches for whole-record
                // cleanup. This keeps whole-library apply impossible without
                // an explicit rule or hash list.
                if request.mode == TrashSelectionMode::Torrent {
                    continue;
                }
            }
            let other_paths = other_torrent_paths(&config, &torrents, Some(key));
            let locations = prepared_locations(&config, &torrent).await?;
            let (paths, stale_files) = if request.mode == TrashSelectionMode::Torrent {
                (torrent_paths(&locations, &other_paths).await?, Vec::new())
            } else {
                stale_partial_paths(&config, &torrent, &locations, &other_paths).await?
            };
            if request.mode == TrashSelectionMode::StalePartials && stale_files.is_empty() {
                continue;
            }
            let blockers = paths
                .iter()
                .filter_map(|path| path.blocked_reason.clone())
                .collect::<Vec<_>>();
            let rule_qualified = rule_eval
                .as_ref()
                .is_none_or(|evaluation| evaluation.qualified);
            let eligible = blockers.is_empty()
                && (!paths.is_empty() || request.mode == TrashSelectionMode::Torrent)
                && (explicit || rule_qualified);
            prepared.push(PreparedItem {
                key,
                rule: requested_rule.clone().or_else(|| {
                    config
                        .trash
                        .retention
                        .iter()
                        .find(|rule| {
                            evaluate_retention_rule(
                                rule,
                                &torrent,
                                effective_profile.as_deref(),
                                now,
                            )
                            .matches
                        })
                        .cloned()
                }),
                rule_evaluation: rule_eval.clone(),
                paths: paths.clone(),
                stale_files,
                eligible,
                explicit,
                effective_profile,
                torrent,
            });
        }
        let items = prepared
            .iter()
            .map(|item| TrashPreviewItem {
                torrent_key: item.key.to_locator(),
                name: item.torrent.meta.name.clone(),
                state: item.torrent.state.as_str().into(),
                effective_profile: item.effective_profile.clone(),
                labels: item.torrent.labels.clone(),
                age_seconds: item.rule_evaluation.as_ref().map_or_else(
                    || {
                        now.saturating_sub(
                            item.torrent
                                .date_completed
                                .unwrap_or(item.torrent.date_added),
                        )
                    },
                    |evaluation| evaluation.age_seconds,
                ),
                applied_rule: item.rule.as_ref().map(|rule| rule.name.clone()),
                rule_reason: item.rule_evaluation.as_ref().map_or_else(
                    || {
                        if item.explicit {
                            "explicitly selected by operator".into()
                        } else {
                            "no retention rule matched".into()
                        }
                    },
                    |evaluation| evaluation.reason.clone(),
                ),
                qualifies_at: item
                    .rule_evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.qualifies_at),
                eligible: item.eligible,
                action: match request.mode {
                    TrashSelectionMode::Torrent => "trash",
                    TrashSelectionMode::StalePartials => "trash_stale",
                }
                .into(),
                paths: item.paths.clone(),
                blockers: item
                    .paths
                    .iter()
                    .filter_map(|path| path.blocked_reason.clone())
                    .collect(),
            })
            .collect();
        Ok((
            TrashPreview {
                mode: request.mode,
                generated_at: now,
                items,
            },
            prepared,
        ))
    }

    async fn trash_torrent_record(&self, item: &PreparedItem) -> TrashApplyItem {
        match self.trash_torrent_record_inner(item).await {
            Ok((entry_id, moved_bytes, paths_moved)) => TrashApplyItem {
                torrent_key: item.key.to_locator(),
                name: item.torrent.meta.name.clone(),
                action: "trash".into(),
                status: "trashed".into(),
                entry_id: Some(entry_id),
                moved_bytes,
                paths_moved,
                error: None,
            },
            Err(error) => TrashApplyItem {
                torrent_key: item.key.to_locator(),
                name: item.torrent.meta.name.clone(),
                action: "trash".into(),
                status: "failed".into(),
                entry_id: None,
                moved_bytes: 0,
                paths_moved: 0,
                error: Some(error.to_string()),
            },
        }
    }

    async fn trash_torrent_record_inner(
        &self,
        item: &PreparedItem,
    ) -> Result<(String, u64, usize)> {
        let config = self.config.read().await.clone();
        let root = trash_root(&config)?.ok_or_else(|| {
            CoreError::InvalidConfig("trash is not configured; set trash.path".into())
        })?;
        let original_metainfo = self.retained_original_metainfo(item.key).await.ok();
        let locations = prepared_locations(&config, &item.torrent).await?;
        let id = new_entry_id(item.key);
        let entries = root.join("entries");
        let staging_root = root.join(".staging");
        tokio::fs::create_dir_all(&entries).await?;
        tokio::fs::create_dir_all(&staging_root).await?;
        let staging = staging_root.join(&id);
        tokio::fs::create_dir_all(&staging).await?;
        let mut manifest_locations = Vec::new();
        let mut moved_bytes = 0u64;
        let mut paths_moved = 0usize;
        for (index, location) in locations.into_iter().enumerate() {
            let subdir = format!("data-{index}");
            let destination = staging.join(&subdir);
            match location
                .storage
                .move_to_with_partial_file_suffix(destination.clone(), None)
                .await
            {
                Ok(_) => {
                    moved_bytes = moved_bytes
                        .saturating_add(directory_file_bytes(&destination).await.unwrap_or(0));
                    paths_moved = paths_moved
                        .saturating_add(count_regular_files(&destination).await.unwrap_or(0));
                    manifest_locations.push(TrashLocation {
                        role: location.role.into(),
                        original_root: location.original_root.display().to_string(),
                        trash_subdir: subdir,
                        partial_file_suffix: location.partial_file_suffix,
                    });
                }
                Err(error) => {
                    if let Err(rollback) = rollback_locations(&staging, &manifest_locations).await {
                        return Err(CoreError::Storage(format!(
                            "trash move failed: {error}; rollback failed: {rollback}"
                        )));
                    }
                    let _ = tokio::fs::remove_dir_all(&staging).await;
                    return Err(error);
                }
            }
        }
        let torrent_json = serde_json::to_string(&item.torrent)
            .map_err(|error| CoreError::Internal(format!("serialize trash torrent: {error}")))?;
        tokio::fs::write(staging.join("torrent.json"), torrent_json.as_bytes()).await?;
        let original_file = if let Some(bytes) = original_metainfo {
            tokio::fs::write(staging.join("original.torrent"), &bytes).await?;
            Some("original.torrent".to_string())
        } else {
            None
        };
        let queue = self.queue.lock().await.membership_snapshot(&item.key);
        let manifest = TrashEntryManifest {
            format: "swarmotter-trash-entry-v1".into(),
            id: id.clone(),
            kind: "torrent".into(),
            created_at: now(),
            reason: item.rule_evaluation.as_ref().map_or_else(
                || "explicit operator cleanup".into(),
                |evaluation| evaluation.reason.clone(),
            ),
            applied_rule: item.rule.as_ref().map(|rule| rule.name.clone()),
            torrent_key: Some(item.key.to_locator()),
            name: item.torrent.meta.name.clone(),
            state: Some(item.torrent.state.as_str().into()),
            profile: item.effective_profile.clone(),
            labels: item.torrent.labels.clone(),
            queue_order_index: queue.order_index(),
            queue_bypass_index: queue.bypass_index(),
            original_metainfo_file: original_file,
            torrent_json: Some("torrent.json".into()),
            locations: manifest_locations,
            stale_files: Vec::new(),
            moved_bytes,
        };
        let manifest_json = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| CoreError::Internal(format!("serialize trash manifest: {error}")))?;
        tokio::fs::write(staging.join("entry.json"), manifest_json).await?;
        let final_dir = entries.join(&id);
        tokio::fs::rename(&staging, &final_dir).await?;

        self.force_stop_engine(&item.key).await;
        self.stop_seeder(&item.key).await;
        let previous = {
            let registry = self.registry.lock().await;
            registry.get(&item.key).cloned()
        };
        if previous.is_none() {
            let _ = rollback_locations(&final_dir, &manifest.locations).await;
            let _ = tokio::fs::remove_dir_all(&final_dir).await;
            return Err(CoreError::NotFound("torrent".into()));
        }
        self.queue_rss_removal(&item.torrent).await;
        {
            let mut registry = self.registry.lock().await;
            registry.remove(&item.key);
        }
        self.queue.lock().await.remove(&item.key);
        {
            let mut rate_samples = self.rate_samples.write().await;
            let mut decisions = self.autopilot_decisions.write().await;
            let mut last_actions = self.autopilot_last_action.write().await;
            let mut limiters = self.torrent_limiters.write().await;
            let mut peer_permit_pools = self.torrent_peer_permit_pools.write().await;
            rate_samples.remove(&item.key);
            decisions.remove(&item.key);
            last_actions.remove(&item.key);
            limiters.remove(&item.key);
            peer_permit_pools.remove(&item.key);
        }
        if let Err(error) = self.persist_state_full().await {
            if let Some(torrent) = previous {
                let _ = self.registry.lock().await.add(torrent);
            }
            self.queue.lock().await.add(item.key);
            let rollback = rollback_locations(&final_dir, &manifest.locations).await;
            let _ = tokio::fs::remove_dir_all(&final_dir).await;
            return Err(match rollback {
                Ok(()) => error,
                Err(rollback) => CoreError::Storage(format!(
                    "trash removal persistence failed: {error}; data rollback failed: {rollback}"
                )),
            });
        }
        self.reconcile_queue().await;
        self.publish_event(torrent_removed_event(item.key, false));
        self.publish_event(stats_updated_event());
        Ok((id, moved_bytes, paths_moved))
    }

    async fn trash_stale_files(&self, item: &PreparedItem) -> TrashApplyItem {
        match self.trash_stale_files_inner(item).await {
            Ok((entry_id, moved_bytes, paths_moved)) => TrashApplyItem {
                torrent_key: item.key.to_locator(),
                name: item.torrent.meta.name.clone(),
                action: "trash_stale".into(),
                status: "trashed".into(),
                entry_id: Some(entry_id),
                moved_bytes,
                paths_moved,
                error: None,
            },
            Err(error) => TrashApplyItem {
                torrent_key: item.key.to_locator(),
                name: item.torrent.meta.name.clone(),
                action: "trash_stale".into(),
                status: "failed".into(),
                entry_id: None,
                moved_bytes: 0,
                paths_moved: 0,
                error: Some(error.to_string()),
            },
        }
    }

    async fn trash_stale_files_inner(&self, item: &PreparedItem) -> Result<(String, u64, usize)> {
        let config = self.config.read().await.clone();
        let root = trash_root(&config)?.ok_or_else(|| {
            CoreError::InvalidConfig("trash is not configured; set trash.path".into())
        })?;
        let id = new_entry_id(item.key);
        let entries = root.join("entries");
        let staging_root = root.join(".staging");
        tokio::fs::create_dir_all(&entries).await?;
        tokio::fs::create_dir_all(&staging_root).await?;
        let staging = staging_root.join(&id);
        let data_dir = staging.join("data");
        tokio::fs::create_dir_all(&data_dir).await?;
        let mut moved = Vec::new();
        let mut bytes = 0u64;
        for path in &item.stale_files {
            let relative = safe_trash_relative(path)?;
            let destination = data_dir.join(&relative);
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            match move_file_no_overwrite(path, &destination).await {
                Ok(length) => {
                    bytes = bytes.saturating_add(length);
                    moved.push(TrashStaleFile {
                        original_path: path.display().to_string(),
                        trashed_path: format!("data/{}", relative.display()),
                        bytes: length,
                    });
                }
                Err(error) => {
                    let rollback = rollback_stale_files(&staging, &moved).await;
                    let _ = tokio::fs::remove_dir_all(&staging).await;
                    return Err(match rollback {
                        Ok(()) => error,
                        Err(rollback) => CoreError::Storage(format!(
                            "stale move failed: {error}; rollback failed: {rollback}"
                        )),
                    });
                }
            }
        }
        let manifest = TrashEntryManifest {
            format: "swarmotter-trash-entry-v1".into(),
            id: id.clone(),
            kind: "payload".into(),
            created_at: now(),
            reason: item.rule_evaluation.as_ref().map_or_else(
                || "stale partial cleanup".into(),
                |evaluation| evaluation.reason.clone(),
            ),
            applied_rule: item.rule.as_ref().map(|rule| rule.name.clone()),
            torrent_key: Some(item.key.to_locator()),
            name: item.torrent.meta.name.clone(),
            state: Some(item.torrent.state.as_str().into()),
            profile: item.effective_profile.clone(),
            labels: item.torrent.labels.clone(),
            queue_order_index: None,
            queue_bypass_index: None,
            original_metainfo_file: None,
            torrent_json: None,
            locations: Vec::new(),
            stale_files: moved,
            moved_bytes: bytes,
        };
        let manifest_json = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| CoreError::Internal(format!("serialize trash manifest: {error}")))?;
        tokio::fs::write(staging.join("entry.json"), manifest_json).await?;
        tokio::fs::rename(&staging, entries.join(&id)).await?;
        Ok((id, bytes, manifest.stale_files.len()))
    }
}

fn trash_root(config: &Config) -> Result<Option<PathBuf>> {
    config
        .trash
        .path
        .as_deref()
        .map(|path| swarmotter_core::config::lexical_absolute_path(Path::new(path)))
        .transpose()
}

async fn prepared_locations(config: &Config, torrent: &Torrent) -> Result<Vec<PreparedLocation>> {
    let (complete, active) = DaemonRuntime::policy_storage_paths_with_config(config, torrent);
    let complete = PathBuf::from(complete);
    let active = PathBuf::from(active);
    let suffix = DaemonRuntime::partial_file_suffix_for_active_storage(torrent);
    let mut locations = Vec::new();
    locations.push(PreparedLocation {
        role: "complete",
        storage: storage_io_with_config(torrent.meta.clone(), complete.clone(), config),
        original_root: complete.clone(),
        partial_file_suffix: None,
    });
    if active != complete || suffix.is_some() {
        locations.push(PreparedLocation {
            role: "active",
            storage: storage_io_with_config(torrent.meta.clone(), active.clone(), config)
                .with_partial_file_suffix(suffix.clone()),
            original_root: active,
            partial_file_suffix: suffix,
        });
    }
    Ok(locations)
}

async fn torrent_paths(
    locations: &[PreparedLocation],
    other_paths: &[(TorrentKey, PathBuf)],
) -> Result<Vec<TrashPath>> {
    let mut paths = Vec::new();
    for location in locations {
        for path in location.storage.path_ownership()?.payload_paths {
            let exists = tokio::fs::symlink_metadata(&path).await;
            let (exists, kind, bytes) = match exists {
                Ok(metadata) if metadata.is_file() => (true, "file".to_string(), metadata.len()),
                Ok(metadata) if metadata.is_dir() => (true, "directory".to_string(), 0),
                Ok(_) => (true, "special".to_string(), 0),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    (false, "missing".to_string(), 0)
                }
                Err(error) => return Err(CoreError::from(error)),
            };
            let blocked = ownership_blocker(&path, other_paths);
            paths.push(TrashPath {
                path: path.display().to_string(),
                role: location.role.into(),
                exists,
                kind: kind.clone(),
                bytes,
                action: if exists && kind == "file" {
                    "trash".into()
                } else {
                    "skip".into()
                },
                reason: if exists {
                    format!("owned payload in {} root", location.role)
                } else {
                    "payload path is not present".into()
                },
                blocked_reason: blocked,
            });
        }
        let resume = location.storage.resume_path();
        if tokio::fs::symlink_metadata(&resume).await.is_ok() {
            paths.push(TrashPath {
                path: resume.display().to_string(),
                role: "resume".into(),
                exists: true,
                kind: "file".into(),
                bytes: 0,
                action: "remove".into(),
                reason: "fast-resume metadata is not retained in trash".into(),
                blocked_reason: None,
            });
        }
    }
    Ok(paths)
}

async fn stale_partial_paths(
    config: &Config,
    torrent: &Torrent,
    locations: &[PreparedLocation],
    other_paths: &[(TorrentKey, PathBuf)],
) -> Result<(Vec<TrashPath>, Vec<PathBuf>)> {
    let mut paths = Vec::new();
    let mut stale = BTreeSet::new();
    for location in locations {
        let ownership = location.storage.path_ownership()?;
        let expected = ownership
            .payload_paths
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        for (index, _file) in torrent.meta.files.iter().enumerate() {
            if torrent.files.get(index).is_some_and(|file| file.wanted) {
                continue;
            }
            let path = location.storage.file_path(index)?;
            if tokio::fs::symlink_metadata(&path).await.is_ok() {
                stale.insert(path);
            }
        }
        if torrent.meta.is_multi_file {
            let scan_root = location.original_root.join(&torrent.meta.name);
            if let Ok(found) = tokio::task::spawn_blocking({
                let scan_root = scan_root.clone();
                move || scan_orphans(&scan_root, &expected)
            })
            .await
            .map_err(|error| CoreError::Storage(format!("stale scan task: {error}")))?
            {
                for path in found {
                    stale.insert(path);
                }
            }
        }
    }
    for path in stale {
        let blocked = ownership_blocker(&path, other_paths);
        let bytes = tokio::fs::metadata(&path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        paths.push(TrashPath {
            path: path.display().to_string(),
            role: "stale".into(),
            exists: true,
            kind: "file".into(),
            bytes,
            action: "trash_stale".into(),
            reason:
                "file is not selected in current metainfo or is an orphan from a changed layout"
                    .into(),
            blocked_reason: blocked,
        });
    }
    let stale_paths = paths.iter().map(|path| PathBuf::from(&path.path)).collect();
    let _ = config;
    Ok((paths, stale_paths))
}

fn ownership_blocker(path: &Path, others: &[(TorrentKey, PathBuf)]) -> Option<String> {
    others
        .iter()
        .find(|(_, other)| paths_overlap(path, other))
        .map(|(key, other)| {
            format!(
                "shared payload path with torrent {key}: {}",
                other.display()
            )
        })
}

fn other_torrent_paths(
    config: &Config,
    torrents: &[(TorrentKey, Torrent)],
    exclude: Option<TorrentKey>,
) -> Vec<(TorrentKey, PathBuf)> {
    let mut paths = Vec::new();
    for (key, torrent) in torrents {
        if exclude == Some(*key) {
            continue;
        }
        let (complete, active) = DaemonRuntime::policy_storage_paths_with_config(config, torrent);
        let suffix = DaemonRuntime::partial_file_suffix_for_active_storage(torrent);
        for (root, suffix) in [(complete, None), (active, suffix)] {
            let storage = storage_io_with_config(torrent.meta.clone(), PathBuf::from(root), config)
                .with_partial_file_suffix(suffix);
            if let Ok(ownership) = storage.path_ownership() {
                paths.extend(ownership.payload_paths.into_iter().map(|path| (*key, path)));
            }
        }
    }
    paths
}

fn scan_orphans(root: &Path, expected: &HashSet<PathBuf>) -> Result<Vec<PathBuf>> {
    let metadata = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(CoreError::from(error)),
    };
    if metadata.file_type().is_symlink() {
        return Ok(vec![root.to_path_buf()]);
    }
    if !metadata.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        let entries = std::fs::read_dir(&directory).map_err(CoreError::from)?;
        for entry in entries {
            let entry = entry.map_err(CoreError::from)?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(CoreError::from)?;
            if metadata.file_type().is_symlink() {
                out.push(path);
            } else if metadata.is_dir() {
                stack.push(path);
            } else if metadata.is_file() && !expected.contains(&path) {
                out.push(path);
            }
        }
    }
    Ok(out)
}

async fn directory_file_bytes(root: &Path) -> Result<u64> {
    if !root.exists() {
        return Ok(0);
    }
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<u64> {
        let mut total = 0u64;
        let mut stack = vec![root];
        while let Some(directory) = stack.pop() {
            for entry in std::fs::read_dir(&directory).map_err(CoreError::from)? {
                let entry = entry.map_err(CoreError::from)?;
                let metadata = std::fs::symlink_metadata(entry.path()).map_err(CoreError::from)?;
                if metadata.is_dir() {
                    stack.push(entry.path());
                } else if metadata.is_file() {
                    total = total.saturating_add(metadata.len());
                }
            }
        }
        Ok(total)
    })
    .await
    .map_err(|error| CoreError::Storage(format!("trash byte scan task: {error}")))?
}

async fn count_regular_files(root: &Path) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<usize> {
        let mut count = 0usize;
        let mut stack = vec![root];
        while let Some(directory) = stack.pop() {
            for entry in std::fs::read_dir(&directory).map_err(CoreError::from)? {
                let entry = entry.map_err(CoreError::from)?;
                let metadata = std::fs::symlink_metadata(entry.path()).map_err(CoreError::from)?;
                if metadata.is_dir() {
                    stack.push(entry.path());
                } else if metadata.is_file() {
                    count += 1;
                }
            }
        }
        Ok(count)
    })
    .await
    .map_err(|error| CoreError::Storage(format!("trash file count task: {error}")))?
}

async fn rollback_locations(staging: &Path, locations: &[TrashLocation]) -> Result<()> {
    for location in locations.iter().rev() {
        let from = staging.join(&location.trash_subdir);
        let to = PathBuf::from(&location.original_root);
        copy_tree_or_rename(&from, &to).await?;
    }
    Ok(())
}

fn safe_trash_relative(path: &Path) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => out.push(value),
            Component::RootDir | Component::CurDir => {}
            _ => {
                return Err(CoreError::Storage(format!(
                    "trash path is not safely relative: {}",
                    path.display()
                )))
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(CoreError::Storage("trash path is empty".into()));
    }
    Ok(out)
}

async fn move_file_no_overwrite(source: &Path, destination: &Path) -> Result<u64> {
    let metadata = tokio::fs::symlink_metadata(source).await?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(CoreError::Storage(format!(
            "trash source is not a regular file: {}",
            source.display()
        )));
    }
    if tokio::fs::symlink_metadata(destination).await.is_ok() {
        return Err(CoreError::Storage(format!(
            "trash destination already exists: {}",
            destination.display()
        )));
    }
    match tokio::fs::hard_link(source, destination).await {
        Ok(()) => {
            tokio::fs::remove_file(source).await?;
        }
        Err(_) => {
            let mut input = tokio::fs::File::open(source).await?;
            let mut output = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(destination)
                .await?;
            let copied = tokio::io::copy(&mut input, &mut output).await?;
            output.sync_all().await?;
            drop(output);
            if copied != metadata.len() {
                let _ = tokio::fs::remove_file(destination).await;
                return Err(CoreError::Storage(format!(
                    "copied {copied} of {} bytes into trash: {}",
                    metadata.len(),
                    source.display()
                )));
            }
            tokio::fs::remove_file(source).await?;
        }
    }
    Ok(metadata.len())
}

async fn rollback_stale_files(staging: &Path, files: &[TrashStaleFile]) -> Result<()> {
    for file in files.iter().rev() {
        let source = staging.join(&file.trashed_path);
        let destination = PathBuf::from(&file.original_path);
        copy_tree_or_rename(&source, &destination).await?;
    }
    Ok(())
}

async fn copy_tree_or_rename(from: &Path, to: &Path) -> Result<()> {
    let metadata = match tokio::fs::symlink_metadata(from).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(CoreError::from(error)),
    };
    if metadata.is_file() {
        if tokio::fs::symlink_metadata(to).await.is_ok() {
            return Err(CoreError::Storage(format!(
                "restore destination already exists: {}",
                to.display()
            )));
        }
        if let Some(parent) = to.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        move_file_no_overwrite(from, to).await?;
        return Ok(());
    }
    tokio::fs::create_dir_all(to).await?;
    let mut stack = vec![from.to_path_buf()];
    while let Some(directory) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let source = entry.path();
            let relative = source.strip_prefix(from).unwrap_or(&source);
            let destination = to.join(relative);
            if source.is_dir() {
                tokio::fs::create_dir_all(&destination).await?;
                stack.push(source);
            } else if tokio::fs::symlink_metadata(&destination).await.is_ok() {
                return Err(CoreError::Storage(format!(
                    "restore destination already exists: {}",
                    destination.display()
                )));
            } else {
                if let Some(parent) = destination.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                move_file_no_overwrite(&source, &destination).await?;
            }
        }
    }
    Ok(())
}

async fn scan_trash_entries(root: &Path) -> Result<Vec<TrashEntryView>> {
    let mut entries = Vec::new();
    let entries_root = root.join("entries");
    if entries_root.exists() {
        let mut directory = tokio::fs::read_dir(&entries_root).await?;
        while let Some(entry) = directory.next_entry().await? {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            entries.push(entry_view_from_dir(&path).await);
        }
    }
    let staging_root = root.join(".staging");
    if staging_root.exists() {
        let mut directory = tokio::fs::read_dir(&staging_root).await?;
        while let Some(entry) = directory.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                let id = entry.file_name().to_string_lossy().to_string();
                entries.push(TrashEntryView {
                    id,
                    kind: "unknown".into(),
                    created_at: 0,
                    torrent_key: None,
                    name: "(incomplete move)".into(),
                    state: None,
                    profile: None,
                    labels: Vec::new(),
                    reason: "interrupted trash move; staging directory retained".into(),
                    applied_rule: None,
                    status: "incomplete".into(),
                    has_original_metainfo: false,
                    moved_bytes: 0,
                    paths: vec![path.display().to_string()],
                    error: Some(
                        "staging entry has no completed manifest; inspect or purge it".into(),
                    ),
                });
            }
        }
    }
    entries.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    Ok(entries)
}

async fn entry_view_from_dir(path: &Path) -> TrashEntryView {
    let id = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    match read_manifest(path).await {
        Ok(manifest) => TrashEntryView {
            id: manifest.id,
            kind: manifest.kind,
            created_at: manifest.created_at,
            torrent_key: manifest.torrent_key,
            name: manifest.name,
            state: manifest.state,
            profile: manifest.profile,
            labels: manifest.labels,
            reason: manifest.reason,
            applied_rule: manifest.applied_rule,
            status: "ready".into(),
            has_original_metainfo: manifest.original_metainfo_file.is_some(),
            moved_bytes: manifest.moved_bytes,
            paths: manifest
                .locations
                .iter()
                .map(|location| path.join(&location.trash_subdir).display().to_string())
                .chain(
                    manifest
                        .stale_files
                        .iter()
                        .map(|file| path.join(&file.trashed_path).display().to_string()),
                )
                .collect(),
            error: None,
        },
        Err(error) => TrashEntryView {
            id,
            kind: "unknown".into(),
            created_at: 0,
            torrent_key: None,
            name: "(corrupt trash entry)".into(),
            state: None,
            profile: None,
            labels: Vec::new(),
            reason: "trash entry manifest could not be read".into(),
            applied_rule: None,
            status: "incomplete".into(),
            has_original_metainfo: false,
            moved_bytes: 0,
            paths: vec![path.display().to_string()],
            error: Some(error.to_string()),
        },
    }
}

async fn read_manifest(entry_dir: &Path) -> Result<TrashEntryManifest> {
    let bytes = tokio::fs::read(entry_dir.join("entry.json")).await?;
    serde_json::from_slice(&bytes)
        .map_err(|error| CoreError::Storage(format!("read trash manifest: {error}")))
}

fn validated_entry_dir(root: &Path, entry_id: &str) -> Result<PathBuf> {
    if entry_id.is_empty()
        || entry_id.contains('/')
        || entry_id.contains('\\')
        || entry_id == "."
        || entry_id == ".."
    {
        return Err(CoreError::InvalidArgument("invalid trash entry id".into()));
    }
    Ok(root.join("entries").join(entry_id))
}

async fn restore_payload_entry(
    entry_dir: &Path,
    manifest: &TrashEntryManifest,
) -> Result<RestoreResult> {
    for file in &manifest.stale_files {
        let destination = PathBuf::from(&file.original_path);
        if tokio::fs::symlink_metadata(&destination).await.is_ok() {
            return Err(CoreError::Storage(format!(
                "restore destination already exists: {}",
                destination.display()
            )));
        }
    }
    let mut restored = 0usize;
    for file in &manifest.stale_files {
        let source = entry_dir.join(&file.trashed_path);
        let destination = PathBuf::from(&file.original_path);
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        move_file_no_overwrite(&source, &destination).await?;
        restored += 1;
    }
    tokio::fs::remove_dir_all(entry_dir).await?;
    Ok(RestoreResult {
        entry_id: manifest.id.clone(),
        status: "restored".into(),
        torrent_key: manifest.torrent_key.clone(),
        paths_restored: restored,
        error: None,
    })
}

async fn restore_torrent_entry(
    runtime: &DaemonRuntime,
    entry_dir: &Path,
    manifest: &TrashEntryManifest,
) -> Result<RestoreResult> {
    let key = manifest
        .torrent_key
        .as_deref()
        .ok_or_else(|| CoreError::Storage("trash torrent entry has no key".into()))
        .and_then(TorrentKey::from_locator)?;
    if runtime.registry.lock().await.contains(&key) {
        return Err(CoreError::DuplicateTorrent(key.to_locator()));
    }
    let torrent_json = manifest
        .torrent_json
        .as_deref()
        .ok_or_else(|| CoreError::Storage("trash torrent entry has no torrent.json".into()))?;
    let bytes = tokio::fs::read(entry_dir.join(torrent_json)).await?;
    let restored_torrent: Torrent = serde_json::from_slice(&bytes)
        .map_err(|error| CoreError::Storage(format!("read trash torrent record: {error}")))?;
    for location in &manifest.locations {
        let source = entry_dir.join(&location.trash_subdir);
        let destination = PathBuf::from(&location.original_root);
        // Preflight every destination before any move. Existing paths are a
        // hard conflict; there is no overwrite or rename policy.
        if let Ok(ownership) = storage_io_with_config(
            restored_torrent.meta.clone(),
            source.clone(),
            &Config::default(),
        )
        .path_ownership()
        {
            for path in ownership.payload_paths {
                let relative = path.strip_prefix(&source).unwrap_or(&path);
                let target = destination.join(relative);
                if tokio::fs::symlink_metadata(&target).await.is_ok() {
                    return Err(CoreError::Storage(format!(
                        "restore destination already exists: {}",
                        target.display()
                    )));
                }
            }
        }
    }
    let mut restored_locations = Vec::new();
    for location in &manifest.locations {
        let source = storage_io_with_config(
            restored_torrent.meta.clone(),
            entry_dir.join(&location.trash_subdir),
            &Config::default(),
        );
        match source
            .move_to_with_partial_file_suffix(
                PathBuf::from(&location.original_root),
                location.partial_file_suffix.clone(),
            )
            .await
        {
            Ok(_) => restored_locations.push(location.clone()),
            Err(error) => {
                for restored in restored_locations.iter().rev() {
                    let _ = storage_io_with_config(
                        restored_torrent.meta.clone(),
                        PathBuf::from(&restored.original_root),
                        &Config::default(),
                    )
                    .with_partial_file_suffix(restored.partial_file_suffix.clone())
                    .move_to_with_partial_file_suffix(entry_dir.join(&restored.trash_subdir), None)
                    .await;
                }
                return Err(error);
            }
        }
    }
    let original = match manifest.original_metainfo_file.as_deref() {
        Some(file) => Some(tokio::fs::read(entry_dir.join(file)).await?),
        None => None,
    };
    let result = runtime
        .add_torrent_mutation_with_original_metainfo(
            restored_torrent.clone(),
            matches!(restored_torrent.state, TorrentState::Paused),
            "trash_restore",
            original,
        )
        .await;
    if let Err(error) = result {
        for location in restored_locations.iter().rev() {
            let _ = storage_io_with_config(
                restored_torrent.meta.clone(),
                PathBuf::from(&location.original_root),
                &Config::default(),
            )
            .with_partial_file_suffix(location.partial_file_suffix.clone())
            .move_to_with_partial_file_suffix(entry_dir.join(&location.trash_subdir), None)
            .await;
        }
        return Err(error);
    }
    if let Some(index) = manifest.queue_order_index {
        let mut queue = runtime.queue.lock().await;
        let current = queue.membership_snapshot(&key);
        let order_len = queue.order.len();
        queue.restore_membership(
            key,
            swarmotter_core::queue::QueueMembershipSnapshot::new(
                Some(index.min(order_len)),
                manifest.queue_bypass_index.or(current.bypass_index()),
            ),
        );
        drop(queue);
        runtime.persist_state_full().await?;
    }
    tokio::fs::remove_dir_all(entry_dir).await?;
    Ok(RestoreResult {
        entry_id: manifest.id.clone(),
        status: "restored".into(),
        torrent_key: Some(key.to_locator()),
        paths_restored: restored_locations.len(),
        error: None,
    })
}

fn new_entry_id(key: TorrentKey) -> String {
    let sequence = TRASH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("{millis}-{sequence:04}-{}", &key.to_locator()[..12])
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}
