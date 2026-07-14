// SPDX-License-Identifier: Apache-2.0

//! Runtime application of named torrent policy profiles.
//!
//! Storage profile values are captured before a torrent is inserted. All
//! remaining profile fields resolve on demand, so changing a profile updates
//! inheriting queue, seeding, and rate-limit behavior without copying values
//! into per-torrent overrides.

use super::*;
use swarmotter_api::state::{StoragePathPreview, StoragePathPreviewRequest};
use swarmotter_core::config::{PeerEncryptionMode, StartBehavior};
use swarmotter_core::policy::{
    apply_intake_file_rules, force_top_level_storage_path, organize_storage_path,
    validate_intake_selection_indices, EffectiveTorrentPolicy, IntakePolicySnapshot,
    PolicyProfileOrigin, PolicyStorageSnapshot,
};

/// Policy fields migrated for a legacy torrent during a profile-configuration
/// replacement. The migration deliberately contains only the
/// two create-time fields, so it cannot overwrite progress, labels, queue
/// order, or any live per-torrent setting changed by another operation.
#[derive(Debug, Clone)]
pub(super) struct LegacyPolicySnapshotMigration {
    records: Vec<LegacyPolicySnapshotMigrationRecord>,
}

#[derive(Debug, Clone)]
struct LegacyPolicySnapshotMigrationRecord {
    key: TorrentKey,
    previous_storage_snapshot: Option<PolicyStorageSnapshot>,
    applied_storage_snapshot: Option<PolicyStorageSnapshot>,
    previous_initial_start_behavior: Option<StartBehavior>,
    applied_initial_start_behavior: Option<StartBehavior>,
}

impl LegacyPolicySnapshotMigration {
    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl DaemonRuntime {
    pub(super) async fn effective_policy(&self, torrent: &Torrent) -> EffectiveTorrentPolicy {
        let config = self.config.read().await;
        EffectiveTorrentPolicy::resolve(&config, torrent)
    }

    pub(super) fn effective_policy_with_config(
        config: &Config,
        torrent: &Torrent,
    ) -> EffectiveTorrentPolicy {
        EffectiveTorrentPolicy::resolve(config, torrent)
    }

    /// Return only torrents whose resolved encryption policy changes between
    /// two otherwise valid configurations. Profile edits that leave an
    /// effective mode unchanged must not tear down their data-plane work.
    pub(super) fn effective_encryption_mode_changes(
        previous: &Config,
        next: &Config,
        torrents: &[Torrent],
    ) -> Vec<TorrentKey> {
        torrents
            .iter()
            .filter_map(|torrent| {
                (Self::effective_policy_with_config(previous, torrent)
                    .encryption_mode
                    .value
                    != Self::effective_policy_with_config(next, torrent)
                        .encryption_mode
                        .value)
                    .then_some(torrent.key())
            })
            .collect()
    }

    /// Apply an already-persisted effective encryption change. Existing
    /// inbound sessions keep their negotiated wire stream, but newly accepted
    /// seeding sessions use the updated registration immediately. Running
    /// download/metadata engines are rebuilt so every new outbound session
    /// uses the new mode.
    pub(super) async fn restart_changed_encryption_policy_work(&self, hashes: &[TorrentKey]) {
        if hashes.is_empty() {
            return;
        }
        let active = {
            let handles = self.engine_handles.read().await;
            hashes
                .iter()
                .copied()
                .filter(|hash| {
                    handles
                        .get(hash)
                        .is_some_and(|handle| !handle.is_finished())
                })
                .collect::<Vec<_>>()
        };
        for hash in active {
            self.restart_engine_for_settings(&hash).await;
        }
    }

    /// Capture the resolved storage paths once at registration. This also
    /// freezes a global/no-profile result: a later label-to-profile mapping
    /// must not silently redirect an existing payload to a new root.
    pub(super) fn snapshot_registration_storage(config: &Config, torrent: &mut Torrent) {
        if torrent.policy.storage_snapshot.is_some() {
            return;
        }
        let effective = Self::effective_policy_with_config(config, torrent);
        torrent.policy.storage_snapshot = Some(PolicyStorageSnapshot {
            profile: effective
                .profile
                .as_ref()
                .map(|profile| profile.name.clone())
                .unwrap_or_default(),
            preserve_existing_storage: false,
            // Capture the resolved values, including a global/no-profile
            // fallback. A profile with only one storage field must still not
            // inherit a newly edited profile or global path after creation.
            download_dir: effective.download_dir.value,
            incomplete_dir: effective.incomplete_dir.value,
        });
    }

    /// Freeze an existing record's start decision before changing a profile
    /// assignment or labels. New records are captured during registration;
    /// this preserves legacy records without letting a later profile edit
    /// retroactively alter their admission intent.
    pub(super) fn snapshot_initial_admission(config: &Config, torrent: &mut Torrent) {
        if torrent.policy.initial_start_behavior.is_some() {
            return;
        }
        torrent.policy.initial_start_behavior = Some(
            Self::effective_policy_with_config(config, torrent)
                .start_behavior
                .value,
        );
    }

    /// Prepare a durable one-time migration for records that predate policy
    /// snapshots. A profile/label edit is otherwise able to select storage or
    /// start behavior for one of those records after it already owns data.
    ///
    /// The caller validates the supplied cloned records first, then installs
    /// and persists this migration under `config_write_lock` before replacing
    /// the live configuration. That ordering makes a restart observe either
    /// the old configuration with old state or the new configuration with the
    /// corresponding frozen legacy records.
    pub(super) fn prepare_legacy_policy_snapshot_migration(
        previous: &Config,
        next: &Config,
        torrents: &mut [Torrent],
    ) -> LegacyPolicySnapshotMigration {
        let profile_policy_changed = previous.profiles != next.profiles;
        if !profile_policy_changed {
            return LegacyPolicySnapshotMigration {
                records: Vec::new(),
            };
        }

        let mut records = Vec::new();
        for torrent in torrents {
            let previous_storage_snapshot = torrent.policy.storage_snapshot.clone();
            let previous_initial_start_behavior = torrent.policy.initial_start_behavior;
            if profile_policy_changed {
                // Existing payloads retain the exact effective locations they
                // used before a profile/label replacement. `snapshot_existing`
                // also respects an explicit completed-data location.
                Self::snapshot_existing_storage(previous, torrent);
            }
            Self::snapshot_initial_admission(previous, torrent);
            let applied_storage_snapshot = torrent.policy.storage_snapshot.clone();
            let applied_initial_start_behavior = torrent.policy.initial_start_behavior;
            if previous_storage_snapshot != applied_storage_snapshot
                || previous_initial_start_behavior != applied_initial_start_behavior
            {
                records.push(LegacyPolicySnapshotMigrationRecord {
                    key: torrent.key(),
                    previous_storage_snapshot,
                    applied_storage_snapshot,
                    previous_initial_start_behavior,
                    applied_initial_start_behavior,
                });
            }
        }
        LegacyPolicySnapshotMigration { records }
    }

    /// Install a prepared migration atomically with respect to its policy
    /// fields. Other torrent fields are purposefully left untouched.
    pub(super) async fn install_legacy_policy_snapshot_migration(
        &self,
        migration: &LegacyPolicySnapshotMigration,
    ) -> Result<()> {
        if migration.is_empty() {
            return Ok(());
        }
        let mut registry = self.registry.lock().await;
        for record in &migration.records {
            let Some(torrent) = registry.get(&record.key) else {
                continue;
            };
            if torrent.policy.storage_snapshot != record.previous_storage_snapshot
                || torrent.policy.initial_start_behavior != record.previous_initial_start_behavior
            {
                return Err(CoreError::Internal(format!(
                    "torrent {} policy changed during configuration replacement",
                    record.key
                )));
            }
        }
        for record in &migration.records {
            let Some(torrent) = registry.get_mut(&record.key) else {
                continue;
            };
            torrent.policy.storage_snapshot = record.applied_storage_snapshot.clone();
            torrent.policy.initial_start_behavior = record.applied_initial_start_behavior;
        }
        Ok(())
    }

    /// Restore a failed configuration replacement's legacy migration before
    /// releasing `config_write_lock`. The same exact-field precondition keeps
    /// an unrelated torrent mutation from being overwritten on rollback.
    pub(super) async fn rollback_legacy_policy_snapshot_migration(
        &self,
        migration: &LegacyPolicySnapshotMigration,
    ) -> Result<()> {
        if migration.is_empty() {
            return Ok(());
        }
        let mut registry = self.registry.lock().await;
        for record in &migration.records {
            let Some(torrent) = registry.get(&record.key) else {
                continue;
            };
            if torrent.policy.storage_snapshot != record.applied_storage_snapshot
                || torrent.policy.initial_start_behavior != record.applied_initial_start_behavior
            {
                return Err(CoreError::Internal(format!(
                    "torrent {} policy changed during configuration rollback",
                    record.key
                )));
            }
        }
        for record in &migration.records {
            let Some(torrent) = registry.get_mut(&record.key) else {
                continue;
            };
            torrent.policy.storage_snapshot = record.previous_storage_snapshot.clone();
            torrent.policy.initial_start_behavior = record.previous_initial_start_behavior;
        }
        Ok(())
    }

    /// Restore and durably rewrite the previous legacy fields after an
    /// already-persisted migration cannot be paired with its config update.
    pub(super) async fn restore_legacy_policy_snapshot_migration(
        &self,
        migration: &LegacyPolicySnapshotMigration,
    ) -> Result<()> {
        self.rollback_legacy_policy_snapshot_migration(migration)
            .await?;
        match self.persist_state_with_file_rollback().await {
            Ok(()) => Ok(()),
            Err(error) => {
                // The persistence helper restored the file generation that
                // existed before this rollback (the migrated record). Match
                // live policy to that durable generation rather than leaving
                // a restart to observe a different policy representation.
                let reinstall = self
                    .install_legacy_policy_snapshot_migration(migration)
                    .await;
                Err(CoreError::Internal(format!(
                    "legacy policy state rollback failed: {error}; restored runtime migration: {reinstall:?}"
                )))
            }
        }
    }

    /// Freeze the paths already in use before a profile-selection change on an
    /// existing torrent. This is deliberately separate from an add-time
    /// profile snapshot: label/profile changes after registration may update
    /// live policy fields, but never relocate an existing payload.
    pub(super) fn snapshot_existing_storage(config: &Config, torrent: &mut Torrent) {
        if torrent.policy.storage_snapshot.is_some() {
            return;
        }
        let effective = Self::effective_policy_with_config(config, torrent);
        torrent.policy.storage_snapshot = Some(PolicyStorageSnapshot {
            profile: String::new(),
            preserve_existing_storage: true,
            // A legacy completed-data path is already durable and explicit;
            // capture the inherited active directory independently.
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

    /// Resolve complete and active paths through a torrent's effective policy.
    /// Per-root storage controls are still selected from the resulting path;
    /// profiles never alter the controls themselves.
    pub(super) async fn policy_storage_paths(&self, torrent: &Torrent) -> (String, String) {
        let config = self.config.read().await;
        Self::policy_storage_paths_with_config(&config, torrent)
    }

    pub(super) fn policy_storage_paths_with_config(
        config: &Config,
        torrent: &Torrent,
    ) -> (String, String) {
        let effective = Self::effective_policy_with_config(config, torrent);
        let complete_dir = effective.download_dir.value.unwrap_or_else(|| {
            std::env::temp_dir()
                .join("swarmotter-downloads")
                .display()
                .to_string()
        });
        let active_dir = effective
            .incomplete_dir
            .value
            .unwrap_or_else(|| complete_dir.clone());
        let organization_subdirectory = torrent
            .policy
            .intake_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.organization_subdirectory.as_deref());
        let incomplete_subdirectory =
            torrent
                .policy
                .intake_snapshot
                .as_ref()
                .and_then(|snapshot| {
                    snapshot
                        .incomplete_subdirectory
                        .as_deref()
                        .or(snapshot.organization_subdirectory.as_deref())
                });
        let force_top_level_folder = torrent
            .policy
            .intake_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.force_top_level_folder);
        let complete_dir = force_top_level_storage_path(
            organize_storage_path(complete_dir, organization_subdirectory),
            &torrent.meta.name,
            torrent.meta.is_multi_file,
            force_top_level_folder,
        );
        let active_dir = force_top_level_storage_path(
            organize_storage_path(active_dir, incomplete_subdirectory),
            &torrent.meta.name,
            torrent.meta.is_multi_file,
            force_top_level_folder,
        );
        (complete_dir, active_dir)
    }

    /// Return the immutable add-time suffix used for an incomplete payload
    /// root. Completed storage always uses canonical metainfo names; callers
    /// that construct an active-root handle must apply this value so rechecks,
    /// moves, deletes, and ownership checks address the same files as the
    /// transfer engine.
    pub(super) fn partial_file_suffix_for_active_storage(torrent: &Torrent) -> Option<String> {
        torrent
            .policy
            .intake_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.partial_file_suffix.clone())
    }

    /// Resolve a bounded, side-effect-free placement preview. The candidate is
    /// deliberately cloned, and profile reassignment freezes legacy storage
    /// first just as the real lifecycle operation does; a preview cannot make
    /// an existing payload appear movable by a profile edit alone.
    pub(super) fn preview_storage_paths_with_config(
        config: &Config,
        torrent: &Torrent,
        request: StoragePathPreviewRequest,
    ) -> Result<StoragePathPreview> {
        let mut candidate = torrent.clone();
        if let Some(profile) = request.profile {
            if profile.trim().is_empty() || !config.profiles.profiles.contains_key(&profile) {
                return Err(CoreError::InvalidArgument(format!(
                    "unknown or empty policy profile {profile:?}"
                )));
            }
            Self::snapshot_existing_storage(config, &mut candidate);
            candidate.policy.profile = Some(profile);
            candidate.policy.profile_origin = Some(PolicyProfileOrigin::Torrent);
        }
        if let Some(download_dir) = request.download_dir {
            if download_dir.trim().is_empty() {
                return Err(CoreError::InvalidArgument(
                    "download_dir must not be empty when previewing placement".into(),
                ));
            }
            candidate.download_dir = Some(download_dir);
        }
        if let Some(incomplete_dir) = request.incomplete_dir {
            if incomplete_dir.trim().is_empty() {
                return Err(CoreError::InvalidArgument(
                    "incomplete_dir must not be empty when previewing placement".into(),
                ));
            }
            candidate.policy.overrides.incomplete_dir = Some(incomplete_dir);
        }

        let (complete_dir, incomplete_dir) =
            Self::policy_storage_paths_with_config(config, &candidate);
        let partial_file_suffix = Self::partial_file_suffix_for_active_storage(&candidate);
        swarmotter_core::policy::validate_partial_file_suffix(partial_file_suffix.as_deref())
            .map_err(CoreError::InvalidArgument)?;

        const FILE_LIMIT: usize = 256;
        let file_count = candidate.meta.files.len();
        let complete_storage =
            storage_io_with_config(candidate.meta.clone(), PathBuf::from(&complete_dir), config);
        let incomplete_storage = storage_io_with_config(
            candidate.meta.clone(),
            PathBuf::from(&incomplete_dir),
            config,
        )
        .with_partial_file_suffix(partial_file_suffix.clone());
        let mut complete_files = Vec::with_capacity(file_count.min(FILE_LIMIT));
        let mut incomplete_files = Vec::with_capacity(file_count.min(FILE_LIMIT));
        for index in 0..file_count.min(FILE_LIMIT) {
            complete_files.push(complete_storage.file_path(index)?.display().to_string());
            incomplete_files.push(incomplete_storage.file_path(index)?.display().to_string());
        }
        Ok(StoragePathPreview {
            complete_dir,
            incomplete_dir,
            partial_file_suffix,
            complete_files,
            incomplete_files,
            file_count,
            truncated: file_count > FILE_LIMIT,
        })
    }

    /// Capture the exact profile-derived intake behavior before a torrent can
    /// enter the scheduler. This makes profile/label selection explainable
    /// and keeps later profile edits from changing a reviewed file selection
    /// or content location.
    pub(super) fn snapshot_registration_intake(
        config: &Config,
        torrent: &mut Torrent,
        mut unwanted_file_indices: Vec<usize>,
        file_exclusion_rules: Vec<swarmotter_core::policy::PolicyFileExclusionRule>,
        partial_file_suffix: Option<String>,
    ) {
        if torrent.policy.intake_snapshot.is_some() {
            return;
        }
        unwanted_file_indices.sort_unstable();
        unwanted_file_indices.dedup();
        let effective = Self::effective_policy_with_config(config, torrent);
        let profile_name = effective
            .profile
            .as_ref()
            .map(|profile| profile.name.clone())
            .unwrap_or_default();
        let intake = effective
            .profile
            .as_ref()
            .and_then(|profile| config.profiles.profiles.get(&profile.name))
            .map(|profile| profile.intake.clone())
            .unwrap_or_default();
        let mut excluded_file_rules = intake.excluded_file_rules;
        for rule in file_exclusion_rules {
            if !excluded_file_rules.contains(&rule) {
                excluded_file_rules.push(rule);
            }
        }
        torrent.policy.intake_snapshot = Some(IntakePolicySnapshot {
            profile: profile_name,
            excluded_file_patterns: intake.excluded_file_patterns,
            excluded_file_rules,
            organization_subdirectory: intake.organization_subdirectory,
            incomplete_subdirectory: intake.incomplete_subdirectory,
            force_top_level_folder: intake.force_top_level_folder,
            partial_file_suffix: partial_file_suffix.or(intake.partial_file_suffix),
            unwanted_file_indices,
        });
    }

    /// Apply a profile assignment received through the add API. Profile names
    /// must already have passed configuration validation; checking here gives
    /// callers a useful error instead of silently falling back to global.
    pub(super) async fn apply_add_profile(
        &self,
        torrent: &mut Torrent,
        options: &AddTorrentOptions,
    ) -> Result<bool> {
        torrent.labels = options.labels.clone();
        let config = self.config.read().await;
        if let Some(profile) = options.profile.as_ref() {
            if !config.profiles.profiles.contains_key(profile) {
                return Err(CoreError::InvalidArgument(format!(
                    "unknown policy profile {profile}"
                )));
            }
            torrent.policy.profile = Some(profile.clone());
            torrent.policy.profile_origin = Some(PolicyProfileOrigin::AddRequest);
        }
        // A label may select a profile even when no explicit add profile was
        // provided. The storage selection is always a create-time snapshot,
        // including an intentionally global result.
        Self::snapshot_registration_intake(
            &config,
            torrent,
            options.unwanted_file_indices.clone(),
            options.file_exclusion_rules.clone(),
            options.partial_file_suffix.clone(),
        );
        if !torrent.needs_metadata {
            let intake = torrent.policy.intake_snapshot.as_ref().ok_or_else(|| {
                CoreError::Internal("missing intake snapshot after registration policy".into())
            })?;
            validate_intake_selection_indices(intake, torrent.meta.files.len())
                .map_err(CoreError::InvalidArgument)?;
        }
        Self::snapshot_registration_storage(&config, torrent);
        if !torrent.needs_metadata {
            apply_intake_file_rules(torrent);
        }
        if options.preview {
            // A preview has a deliberately stronger admission rule than a
            // profile's default: `.torrent` payload work is paused now, while
            // a magnet is permitted to fetch only metadata through the same
            // contained binder before pausing.
            torrent.policy.preview_until_started = true;
            torrent.policy.initial_start_behavior = Some(StartBehavior::Paused);
            return Ok(!torrent.needs_metadata);
        }
        let effective = Self::effective_policy_with_config(&config, torrent);
        let start_behavior = if options.start_behavior_explicit {
            if options.paused {
                StartBehavior::Paused
            } else {
                StartBehavior::Start
            }
        } else {
            effective.start_behavior.value
        };
        torrent.policy.initial_start_behavior = Some(start_behavior);
        // `queue.auto_start = false` historically leaves an add queued rather
        // than treating it as a manual pause. Preserve that state distinction
        // while recording the initial admission decision for the scheduler.
        // A profile that explicitly says `paused`, or an explicit caller
        // request, does create a paused torrent as compatibility adapters and
        // the native API promise.
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

    /// Change only a torrent's profile assignment. Storage snapshots are not
    /// changed here: operators must use the existing move operation for an
    /// explicit data relocation. This makes profile reassignment safe for
    /// completed and active payloads.
    pub(super) async fn assign_torrent_profile(
        &self,
        hash: &TorrentKey,
        profile: Option<String>,
    ) -> Result<()> {
        // Serialize validation, durable assignment, and profile replacement.
        // A deleted profile must never be committed as a dangling attachment.
        let _config_transaction = self.config_write_lock.lock().await;
        let config = self.config.read().await.clone();
        if let Some(profile) = profile.as_ref() {
            if !config.profiles.profiles.contains_key(profile) {
                return Err(CoreError::InvalidArgument(format!(
                    "unknown policy profile {profile}"
                )));
            }
        }
        let (previous, mode_changed) = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.clone();
            let previous_mode = Self::effective_policy_with_config(&config, &previous)
                .encryption_mode
                .value;
            Self::snapshot_initial_admission(&config, torrent);
            Self::snapshot_existing_storage(&config, torrent);
            torrent.policy.profile = profile;
            torrent.policy.profile_origin = torrent
                .policy
                .profile
                .as_ref()
                .map(|_| PolicyProfileOrigin::Torrent);
            let next_mode = Self::effective_policy_with_config(&config, torrent)
                .encryption_mode
                .value;
            (previous, previous_mode != next_mode)
        };
        // Do not expose a new assignment to live limiters/queue reconciliation
        // until the durable state write succeeds. On failure, restore the
        // exact record before any runtime policy is applied.
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                *torrent = previous;
            }
            return Err(error);
        }
        self.refresh_profile_runtime_fields().await;
        if mode_changed {
            self.restart_changed_encryption_policy_work(std::slice::from_ref(hash))
                .await;
        }
        self.schedule_reconcile_queue("torrent_profile_assignment")
            .await;
        self.reconcile_seeders().await;
        self.publish_event(Event::new(
            "torrent_policy_changed",
            json!({
                "info_hash": hash.to_locator(),
            }),
        ));
        Ok(())
    }

    /// Set or clear one torrent's durable peer-wire encryption override.
    /// Clearing the value restores deterministic profile/label/global
    /// inheritance. The replacement is persisted before active data-plane
    /// work is restarted, so a failed durable write cannot expose a transient
    /// transport policy.
    pub(super) async fn assign_torrent_encryption_mode(
        &self,
        hash: &TorrentKey,
        encryption_mode: Option<PeerEncryptionMode>,
    ) -> Result<()> {
        let _config_transaction = self.config_write_lock.lock().await;
        let config = self.config.read().await.clone();
        let (previous, mode_changed) = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.clone();
            let previous_mode = Self::effective_policy_with_config(&config, &previous)
                .encryption_mode
                .value;
            torrent.policy.overrides.encryption_mode = encryption_mode;
            let next_mode = Self::effective_policy_with_config(&config, torrent)
                .encryption_mode
                .value;
            (previous, previous_mode != next_mode)
        };
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                *torrent = previous;
            }
            return Err(error);
        }
        self.refresh_profile_runtime_fields().await;
        if mode_changed {
            self.restart_changed_encryption_policy_work(std::slice::from_ref(hash))
                .await;
        }
        self.publish_event(Event::new(
            "torrent_policy_changed",
            json!({
                "info_hash": hash.to_locator(),
            }),
        ));
        Ok(())
    }

    /// Update retained per-torrent rate limiters and registered inbound
    /// encryption policies for live inheritance. Queue selection and seeding
    /// target evaluation resolve profiles on demand, so no profile value is
    /// copied into a torrent record here.
    pub(super) async fn refresh_profile_runtime_fields(&self) {
        let config = self.config.read().await.clone();
        let effective = {
            let registry = self.registry.lock().await;
            registry
                .torrents
                .iter()
                .map(|(hash, torrent)| {
                    let policy = Self::effective_policy_with_config(&config, torrent);
                    (
                        *hash,
                        policy.download_limit.value,
                        policy.upload_limit.value,
                        policy.encryption_mode.value,
                    )
                })
                .collect::<Vec<_>>()
        };
        let limiters = self.torrent_limiters.read().await;
        for (hash, download, upload, _) in &effective {
            if let Some(limiter) = limiters.get(hash) {
                limiter.set_capacity(
                    swarmotter_core::bandwidth::RateDirection::Download,
                    *download,
                );
                limiter.set_capacity(swarmotter_core::bandwidth::RateDirection::Upload, *upload);
            }
        }
        drop(limiters);
        for (hash, _, _, encryption_mode) in effective {
            self.seeder_registry
                .update_encryption_mode(&hash, encryption_mode)
                .await;
        }
    }

    pub(super) fn effective_ratio_policy(
        config: &Config,
        torrent: &Torrent,
    ) -> (
        swarmotter_core::ratio::SeedingPolicy,
        swarmotter_core::ratio::TorrentSeeding,
    ) {
        let policy = Self::effective_policy_with_config(config, torrent);
        (
            swarmotter_core::ratio::SeedingPolicy {
                global_ratio_limit: policy.ratio_limit.value,
                global_idle_limit: policy.idle_limit.value,
            },
            swarmotter_core::ratio::TorrentSeeding {
                ratio_limit: None,
                idle_limit: None,
                seed_forever: policy.seed_forever.value,
            },
        )
    }
}

/// Reject profile deletion/renaming while a durable explicit assignment still
/// refers to it. Label-derived selection intentionally remains dynamic and
/// may change with a mapping edit; explicit operator choices must not silently
/// disappear.
pub(super) fn validate_explicit_profile_assignments(
    config: &Config,
    torrents: &[Torrent],
) -> Result<()> {
    for torrent in torrents {
        let Some(profile) = torrent.policy.profile.as_deref() else {
            continue;
        };
        if !config.profiles.profiles.contains_key(profile) {
            return Err(CoreError::InvalidConfig(format!(
                "policy profile {profile} is still assigned to torrent {}; reassign or clear it before removing the profile",
                torrent.key()
            )));
        }
    }
    Ok(())
}
