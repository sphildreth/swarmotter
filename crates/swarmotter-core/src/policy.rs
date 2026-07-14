// SPDX-License-Identifier: Apache-2.0

//! Named policy profiles and deterministic effective-setting resolution.
//!
//! Resolution is pure and never mutates torrent state. This lets the daemon
//! apply live settings safely while API and Web UI callers can show the value
//! and layer that supplied it.

use crate::config::{Config, PeerEncryptionMode, StartBehavior};
use crate::meta::MetaFile;
use crate::models::torrent::FilePriority;
use crate::torrent::Torrent;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

/// Top-level `[profiles]` configuration section.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyProfilesConfig {
    /// Named reusable profiles.
    #[serde(default)]
    pub profiles: BTreeMap<String, PolicyProfile>,
    /// Case-insensitive label-to-profile mappings. Multiple matching labels
    /// resolve by normalized label name, so the outcome is deterministic.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// One named reusable torrent policy.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyProfile {
    #[serde(default)]
    pub storage: PolicyStorage,
    #[serde(default)]
    pub queue: PolicyQueue,
    #[serde(default)]
    pub seeding: PolicySeeding,
    #[serde(default)]
    pub bandwidth: PolicyBandwidth,
    /// Deterministic tracker-host controls. Unlike intake decisions, these
    /// remain live profile policy and are applied whenever an engine starts
    /// discovery work.
    #[serde(default)]
    pub tracker: PolicyTracker,
    /// Intake rules captured when a torrent is registered. They are applied
    /// before payload transfer, never retroactively to existing torrents.
    #[serde(default)]
    pub intake: PolicyIntake,
    /// Optional peer-wire encryption policy. When omitted, torrents assigned
    /// to this profile inherit the global `torrent.encryption_mode` setting.
    /// This is a live transport policy: it does not affect storage placement
    /// or the immutable creation-time policy snapshot.
    #[serde(default)]
    pub encryption_mode: Option<PeerEncryptionMode>,
}

/// Storage values selected at torrent creation.
///
/// They are snapshotted into the torrent rather than inherited live. A later
/// profile edit must never silently relocate an existing payload.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyStorage {
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub incomplete_dir: Option<String>,
}

/// Queue and initial start behavior controlled by a profile.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyQueue {
    #[serde(default)]
    pub priority: Option<QueuePriority>,
    #[serde(default)]
    pub start_behavior: Option<StartBehavior>,
}

/// Stable priority used ahead of normal queue order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueuePriority {
    Low,
    #[default]
    Normal,
    High,
}

impl QueuePriority {
    pub const fn weight(self) -> i8 {
        match self {
            Self::Low => -1,
            Self::Normal => 0,
            Self::High => 1,
        }
    }
}

/// Profile-level seeding defaults; omitted ratio/idle fields inherit global
/// seeding defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySeeding {
    #[serde(default)]
    pub ratio_limit: Option<f64>,
    #[serde(default)]
    pub idle_limit: Option<u64>,
    #[serde(default)]
    pub seed_forever: Option<bool>,
}

/// Per-torrent bandwidth defaults; `0` means unlimited.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyBandwidth {
    #[serde(default)]
    pub download_limit: Option<u64>,
    #[serde(default)]
    pub upload_limit: Option<u64>,
}

/// Profile-scoped tracker selection and priority controls.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyTracker {
    /// Ordered host rules. The first matching rule supplies both enablement
    /// and priority so overlapping patterns remain deterministic.
    #[serde(default)]
    pub host_rules: Vec<TrackerHostRule>,
}

/// A single tracker-host decision. The host is matched case-insensitively
/// using `*` and `?`; it is a host pattern, not a URL or route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrackerHostRule {
    pub host_pattern: String,
    /// `false` removes matching trackers from announce and scrape work. A
    /// missing value keeps the tracker eligible.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Higher-priority matching trackers are tried first. Equal priorities
    /// preserve metainfo order.
    #[serde(default)]
    pub priority: Option<QueuePriority>,
}

/// Profile defaults applied while a torrent is admitted to the library.
///
/// These values intentionally differ from live queue/bandwidth/seeding
/// fields: the resolved result is snapshotted into each torrent so later
/// profile edits cannot silently change an already-reviewed file selection or
/// storage organization.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyIntake {
    /// Case-insensitive glob patterns matched against slash-separated torrent
    /// file paths. Multi-file torrents accept both their canonical path and
    /// the path relative to the torrent's top-level directory. `*` matches
    /// any sequence (including `/`) and `?` matches one character. Matching
    /// files are marked unwanted before payload work begins.
    #[serde(default)]
    pub excluded_file_patterns: Vec<String>,
    /// Composable structured exclusion rules. Every populated field in one
    /// rule must match a file; a file is excluded when any rule matches.
    /// These cover suffix, path, path-segment, and size-based intake without
    /// requiring callers to encode size semantics into a glob.
    #[serde(default)]
    pub excluded_file_rules: Vec<PolicyFileExclusionRule>,
    /// A safe relative directory inserted below the selected complete and
    /// incomplete roots. It provides deterministic profile-scoped content
    /// organization while retaining the torrent's normal top-level name.
    #[serde(default)]
    pub organization_subdirectory: Option<String>,
    /// Optional safe relative directory for incomplete data. When omitted,
    /// incomplete data uses `organization_subdirectory` so a profile has one
    /// stable content placement; when set, it provides a per-torrent staging
    /// location below the selected incomplete root.
    #[serde(default)]
    pub incomplete_subdirectory: Option<String>,
    /// Put a single-file torrent below a directory named after the torrent.
    /// Multi-file torrents already have that top-level directory in their
    /// canonical metainfo layout, so this setting is a no-op for them.
    #[serde(default)]
    pub force_top_level_folder: bool,
    /// Optional suffix used only while payload files are incomplete, for
    /// example `.part`. Completion atomically moves or renames files back to
    /// their canonical metainfo paths. The resolved value is snapshotted at
    /// registration and never changes an existing payload in place.
    #[serde(default)]
    pub partial_file_suffix: Option<String>,
}

/// One deterministic file exclusion condition used during torrent intake.
///
/// All populated criteria are combined with logical AND. Multiple rules are
/// combined with logical OR. Matching textual fields is ASCII
/// case-insensitive so profile behavior does not depend on common filesystem
/// case conventions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyFileExclusionRule {
    /// Slash-separated file-path glob. `*` and `?` have the same semantics as
    /// [`PolicyIntake::excluded_file_patterns`].
    #[serde(default)]
    pub path_pattern: Option<String>,
    /// Filename suffix such as `.nfo` or `.txt`.
    #[serde(default)]
    pub suffix: Option<String>,
    /// An exact path component, such as `samples` or `proof`.
    #[serde(default)]
    pub path_segment: Option<String>,
    /// Inclusive lower size bound in bytes.
    #[serde(default)]
    pub min_size_bytes: Option<u64>,
    /// Inclusive upper size bound in bytes.
    #[serde(default)]
    pub max_size_bytes: Option<u64>,
}

/// Immutable intake decision captured when a torrent is registered.
///
/// The profile can be edited later, but a registered torrent must retain the
/// exact selection and organization policy it was reviewed with. Explicit
/// unwanted indices are included so API callers can make a deterministic
/// selection for a known `.torrent` or a magnet after its metadata resolves.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntakePolicySnapshot {
    /// The resolved profile name at registration, or an empty string when the
    /// result came directly from global defaults.
    #[serde(default)]
    pub profile: String,
    #[serde(default)]
    pub excluded_file_patterns: Vec<String>,
    #[serde(default)]
    pub excluded_file_rules: Vec<PolicyFileExclusionRule>,
    #[serde(default)]
    pub organization_subdirectory: Option<String>,
    #[serde(default)]
    pub incomplete_subdirectory: Option<String>,
    #[serde(default)]
    pub force_top_level_folder: bool,
    #[serde(default)]
    pub partial_file_suffix: Option<String>,
    #[serde(default)]
    pub unwanted_file_indices: Vec<usize>,
}

/// Why a profile was attached to the torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyProfileOrigin {
    Torrent,
    AddRequest,
    WatchFolder,
}

/// Resolved storage selected while creating a torrent. This differs from a
/// per-torrent override: the snapshot preserves creation-time provenance and
/// intentionally does not change when a profile, label mapping, or global
/// storage default is edited.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyStorageSnapshot {
    /// The profile selected during registration. An empty value records that
    /// registration resolved directly from the global storage defaults.
    pub profile: String,
    /// Snapshot created during reassignment to preserve the already-effective
    /// storage location. It is not a per-torrent override and must not be
    /// presented as a profile storage value.
    #[serde(default)]
    pub preserve_existing_storage: bool,
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub incomplete_dir: Option<String>,
}

/// Explicit per-torrent policy overrides. Existing durable torrent fields are
/// retained as legacy overrides; new writes use this type so explicit `0`
/// caps and explicit `false` seed-forever values remain distinguishable from
/// absent inheritance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TorrentPolicyOverrides {
    #[serde(default)]
    pub incomplete_dir: Option<String>,
    #[serde(default)]
    pub queue_priority: Option<QueuePriority>,
    #[serde(default)]
    pub start_behavior: Option<StartBehavior>,
    #[serde(default)]
    pub ratio_limit: Option<f64>,
    #[serde(default)]
    pub idle_limit: Option<u64>,
    #[serde(default)]
    pub seed_forever: Option<bool>,
    #[serde(default)]
    pub download_limit: Option<u64>,
    #[serde(default)]
    pub upload_limit: Option<u64>,
    /// Explicit peer-wire encryption mode for this torrent. `None` retains
    /// deterministic profile/label/global inheritance; a set value is
    /// durably retained across daemon restarts.
    #[serde(default)]
    pub encryption_mode: Option<PeerEncryptionMode>,
}

/// Durable policy attachment for one torrent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TorrentPolicy {
    /// Named profile assigned by an add request, watch folder, or operator.
    #[serde(default)]
    pub profile: Option<String>,
    /// Legacy records with a profile and no origin are treated as explicit
    /// torrent assignments.
    #[serde(default)]
    pub profile_origin: Option<PolicyProfileOrigin>,
    #[serde(default)]
    pub storage_snapshot: Option<PolicyStorageSnapshot>,
    /// One-time start decision captured at registration. It prevents later
    /// profile, label, or global queue edits from retroactively changing a
    /// torrent's initial admission intent.
    #[serde(default)]
    pub initial_start_behavior: Option<StartBehavior>,
    /// Deterministic profile and request-level file/organization decisions
    /// captured at registration. Missing data denotes a legacy torrent and
    /// must never cause a later profile edit to alter that torrent's files.
    #[serde(default)]
    pub intake_snapshot: Option<IntakePolicySnapshot>,
    /// A preview magnet may fetch only metadata through the contained network
    /// path. Once metadata is available, payload transfer remains paused
    /// until an explicit start/resume operation clears this gate.
    #[serde(default)]
    pub preview_until_started: bool,
    #[serde(default)]
    pub overrides: TorrentPolicyOverrides,
}

/// The layer which supplied one effective setting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PolicyValueSource {
    Global,
    Profile {
        profile: String,
        origin: PolicyProfileOrigin,
    },
    Label {
        label: String,
        profile: String,
    },
    Torrent,
    LegacyTorrent,
    ProfileStorageSnapshot {
        profile: String,
    },
    RegistrationStorageSnapshot,
    ExistingStorageSnapshot,
    InitialAdmissionSnapshot,
    IntakeSnapshot {
        profile: String,
    },
}

/// An effective value paired with its source.
#[derive(Debug, Clone, Serialize)]
pub struct EffectivePolicyValue<T> {
    pub value: T,
    pub source: PolicyValueSource,
}

/// Selected profile and the assignment layer, if any.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveProfileAssignment {
    pub name: String,
    pub source: PolicyValueSource,
}

/// Explainable create-time intake policy for one torrent.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveIntakePolicy {
    pub excluded_file_patterns: EffectivePolicyValue<Vec<String>>,
    pub excluded_file_rules: EffectivePolicyValue<Vec<PolicyFileExclusionRule>>,
    pub organization_subdirectory: EffectivePolicyValue<Option<String>>,
    pub incomplete_subdirectory: EffectivePolicyValue<Option<String>>,
    pub force_top_level_folder: EffectivePolicyValue<bool>,
    pub partial_file_suffix: EffectivePolicyValue<Option<String>>,
    pub unwanted_file_indices: Vec<usize>,
    pub preview_until_started: bool,
}

/// Explainable live tracker-host policy for one torrent.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveTrackerPolicy {
    pub host_rules: EffectivePolicyValue<Vec<TrackerHostRule>>,
}

/// Complete explainable policy used for one torrent.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveTorrentPolicy {
    pub profile: Option<EffectiveProfileAssignment>,
    pub download_dir: EffectivePolicyValue<Option<String>>,
    pub incomplete_dir: EffectivePolicyValue<Option<String>>,
    pub queue_priority: EffectivePolicyValue<QueuePriority>,
    pub start_behavior: EffectivePolicyValue<StartBehavior>,
    pub ratio_limit: EffectivePolicyValue<Option<f64>>,
    pub idle_limit: EffectivePolicyValue<Option<u64>>,
    pub seed_forever: EffectivePolicyValue<bool>,
    pub download_limit: EffectivePolicyValue<u64>,
    pub upload_limit: EffectivePolicyValue<u64>,
    /// Effective peer-wire encryption mode. This applies MSE/PE over both
    /// contained TCP and contained uTP byte streams.
    pub encryption_mode: EffectivePolicyValue<PeerEncryptionMode>,
    /// Create-time file selection, content organization, and preview gate.
    pub intake: EffectiveIntakePolicy,
    /// Live tracker-host enablement and priority rules.
    pub tracker: EffectiveTrackerPolicy,
    /// Profile fields that update existing inheriting torrents immediately.
    /// Start behavior is intentionally omitted: it controls admission when a
    /// torrent is created, and changing it never stops running work.
    pub live_inheritance_fields: Vec<&'static str>,
    /// Resolved storage and admission behavior are selected only at creation,
    /// never retroactively changed for an existing torrent.
    pub create_time_snapshot_fields: Vec<&'static str>,
}

impl EffectiveTorrentPolicy {
    /// Resolve a torrent's policy with deterministic precedence:
    /// torrent profile/add/watch assignment, then matching label, then global.
    /// Per-field torrent overrides always win.
    pub fn resolve(config: &Config, torrent: &Torrent) -> Self {
        let profile = resolve_profile(config, torrent);
        let profile_value = profile
            .as_ref()
            .and_then(|assignment| config.profiles.profiles.get(&assignment.name));
        let profile_source = profile.as_ref().map(|assignment| assignment.source.clone());
        let default_start = if config.queue.auto_start {
            StartBehavior::Start
        } else {
            StartBehavior::Paused
        };

        let download_dir = if let Some(path) = torrent.download_dir.clone() {
            EffectivePolicyValue {
                value: Some(path),
                source: PolicyValueSource::LegacyTorrent,
            }
        } else if let Some(snapshot) = torrent.policy.storage_snapshot.as_ref() {
            // A snapshot intentionally wins even when it captured `None`.
            // Otherwise a later profile edit could retroactively select a
            // directory for an existing torrent.
            EffectivePolicyValue {
                value: snapshot.download_dir.clone(),
                source: storage_snapshot_source(snapshot),
            }
        } else if let Some(path) =
            profile_value.and_then(|value| value.storage.download_dir.clone())
        {
            EffectivePolicyValue {
                value: Some(path),
                source: profile_source.clone().unwrap_or(PolicyValueSource::Global),
            }
        } else {
            global_option(config.storage.download_dir.clone())
        };
        let incomplete_dir = if let Some(path) = torrent.policy.overrides.incomplete_dir.clone() {
            EffectivePolicyValue {
                value: Some(path),
                source: PolicyValueSource::Torrent,
            }
        } else if let Some(snapshot) = torrent.policy.storage_snapshot.as_ref() {
            // See the corresponding completed-data path above. `None` is a
            // meaningful create-time result, not an invitation to re-inherit.
            EffectivePolicyValue {
                value: snapshot.incomplete_dir.clone(),
                source: storage_snapshot_source(snapshot),
            }
        } else if let Some(path) =
            profile_value.and_then(|value| value.storage.incomplete_dir.clone())
        {
            EffectivePolicyValue {
                value: Some(path),
                source: profile_source.clone().unwrap_or(PolicyValueSource::Global),
            }
        } else {
            global_option(config.storage.incomplete_dir.clone())
        };

        let queue_priority = resolve_value(
            torrent.policy.overrides.queue_priority,
            profile_value.and_then(|value| value.queue.priority),
            QueuePriority::Normal,
            profile_source.clone(),
        );
        let start_behavior = if let Some(value) = torrent.policy.initial_start_behavior {
            EffectivePolicyValue {
                value,
                source: PolicyValueSource::InitialAdmissionSnapshot,
            }
        } else {
            resolve_value(
                torrent.policy.overrides.start_behavior,
                profile_value.and_then(|value| value.queue.start_behavior),
                default_start,
                profile_source.clone(),
            )
        };
        let ratio_limit = if let Some(value) = torrent.policy.overrides.ratio_limit {
            torrent_option(value)
        } else if let Some(value) = torrent.seeding.ratio_limit {
            legacy_option(value)
        } else {
            resolve_optional_value(
                profile_value.and_then(|value| value.seeding.ratio_limit),
                config.seeding.global_ratio_limit,
                profile_source.clone(),
            )
        };
        let idle_limit = if let Some(value) = torrent.policy.overrides.idle_limit {
            torrent_option(value)
        } else if let Some(value) = torrent.seeding.idle_limit {
            legacy_option(value)
        } else {
            resolve_optional_value(
                profile_value.and_then(|value| value.seeding.idle_limit),
                config.seeding.global_idle_limit,
                profile_source.clone(),
            )
        };
        let seed_forever = if let Some(value) = torrent.policy.overrides.seed_forever {
            EffectivePolicyValue {
                value,
                source: PolicyValueSource::Torrent,
            }
        } else if torrent.seeding.seed_forever {
            EffectivePolicyValue {
                value: true,
                source: PolicyValueSource::LegacyTorrent,
            }
        } else {
            resolve_value(
                None,
                profile_value.and_then(|value| value.seeding.seed_forever),
                false,
                profile_source.clone(),
            )
        };
        let download_limit = if let Some(value) = torrent.policy.overrides.download_limit {
            torrent_value(value)
        } else if torrent.download_limit != 0 {
            legacy_value(torrent.download_limit)
        } else {
            resolve_value(
                None,
                profile_value.and_then(|value| value.bandwidth.download_limit),
                0,
                profile_source.clone(),
            )
        };
        let upload_limit = if let Some(value) = torrent.policy.overrides.upload_limit {
            torrent_value(value)
        } else if torrent.upload_limit != 0 {
            legacy_value(torrent.upload_limit)
        } else {
            resolve_value(
                None,
                profile_value.and_then(|value| value.bandwidth.upload_limit),
                0,
                profile_source.clone(),
            )
        };
        let encryption_mode = resolve_value(
            torrent.policy.overrides.encryption_mode,
            profile_value.and_then(|value| value.encryption_mode),
            config.torrent.encryption_mode,
            profile_source.clone(),
        );
        let intake = if let Some(snapshot) = torrent.policy.intake_snapshot.as_ref() {
            let source = intake_snapshot_source(snapshot);
            EffectiveIntakePolicy {
                excluded_file_patterns: EffectivePolicyValue {
                    value: snapshot.excluded_file_patterns.clone(),
                    source: source.clone(),
                },
                excluded_file_rules: EffectivePolicyValue {
                    value: snapshot.excluded_file_rules.clone(),
                    source: source.clone(),
                },
                organization_subdirectory: EffectivePolicyValue {
                    value: snapshot.organization_subdirectory.clone(),
                    source: source.clone(),
                },
                incomplete_subdirectory: EffectivePolicyValue {
                    value: snapshot.incomplete_subdirectory.clone(),
                    source: source.clone(),
                },
                force_top_level_folder: EffectivePolicyValue {
                    value: snapshot.force_top_level_folder,
                    source: source.clone(),
                },
                partial_file_suffix: EffectivePolicyValue {
                    value: snapshot.partial_file_suffix.clone(),
                    source,
                },
                unwanted_file_indices: snapshot.unwanted_file_indices.clone(),
                preview_until_started: torrent.policy.preview_until_started,
            }
        } else {
            // Legacy torrents deliberately retain their already-persisted
            // selection. Do not let a newly-added profile rule alter them.
            EffectiveIntakePolicy {
                excluded_file_patterns: EffectivePolicyValue {
                    value: Vec::new(),
                    source: PolicyValueSource::Global,
                },
                excluded_file_rules: EffectivePolicyValue {
                    value: Vec::new(),
                    source: PolicyValueSource::Global,
                },
                organization_subdirectory: EffectivePolicyValue {
                    value: None,
                    source: PolicyValueSource::Global,
                },
                incomplete_subdirectory: EffectivePolicyValue {
                    value: None,
                    source: PolicyValueSource::Global,
                },
                force_top_level_folder: EffectivePolicyValue {
                    value: false,
                    source: PolicyValueSource::Global,
                },
                partial_file_suffix: EffectivePolicyValue {
                    value: None,
                    source: PolicyValueSource::Global,
                },
                unwanted_file_indices: Vec::new(),
                preview_until_started: false,
            }
        };
        let tracker = EffectiveTrackerPolicy {
            host_rules: EffectivePolicyValue {
                value: profile_value
                    .map(|profile| profile.tracker.host_rules.clone())
                    .unwrap_or_default(),
                source: profile_source.clone().unwrap_or(PolicyValueSource::Global),
            },
        };

        Self {
            profile,
            download_dir,
            incomplete_dir,
            queue_priority,
            start_behavior,
            ratio_limit,
            idle_limit,
            seed_forever,
            download_limit,
            upload_limit,
            encryption_mode,
            intake,
            tracker,
            live_inheritance_fields: vec![
                "queue_priority",
                "ratio_limit",
                "idle_limit",
                "seed_forever",
                "download_limit",
                "upload_limit",
                "encryption_mode",
                "tracker_host_rules",
            ],
            create_time_snapshot_fields: vec![
                "download_dir",
                "incomplete_dir",
                "start_behavior",
                "file_selection",
                "content_organization",
                "top_level_folder",
                "partial_file_suffix",
            ],
        }
    }
}

fn global_option<T>(value: Option<T>) -> EffectivePolicyValue<Option<T>> {
    EffectivePolicyValue {
        value,
        source: PolicyValueSource::Global,
    }
}

fn storage_snapshot_source(snapshot: &PolicyStorageSnapshot) -> PolicyValueSource {
    if snapshot.preserve_existing_storage {
        PolicyValueSource::ExistingStorageSnapshot
    } else if snapshot.profile.is_empty() {
        PolicyValueSource::RegistrationStorageSnapshot
    } else {
        PolicyValueSource::ProfileStorageSnapshot {
            profile: snapshot.profile.clone(),
        }
    }
}

fn intake_snapshot_source(snapshot: &IntakePolicySnapshot) -> PolicyValueSource {
    PolicyValueSource::IntakeSnapshot {
        profile: snapshot.profile.clone(),
    }
}

/// Apply the captured intake selection to a torrent's durable file settings.
///
/// This is idempotent and intentionally only removes files from selection; it
/// never overrides an operator's later decision to select a file explicitly.
pub fn apply_intake_file_rules(torrent: &mut Torrent) {
    let Some(snapshot) = torrent.policy.intake_snapshot.as_ref() else {
        return;
    };
    apply_intake_selection(
        &torrent.meta.files,
        &mut torrent.priorities,
        &mut torrent.wanted,
        snapshot,
    );
    for (index, file) in torrent.files.iter_mut().enumerate() {
        if let (Some(priority), Some(wanted)) =
            (torrent.priorities.get(index), torrent.wanted.get(index))
        {
            file.priority = *priority;
            file.wanted = *wanted;
        }
    }
}

/// Validate request-level file selections against an authoritative file list.
///
/// Magnets defer this check until contained metadata retrieval supplies the
/// real file count. Callers must reject an out-of-range index rather than
/// silently treating it as no selection, so the recorded intake decision is
/// deterministic and explainable.
pub fn validate_intake_selection_indices(
    snapshot: &IntakePolicySnapshot,
    file_count: usize,
) -> std::result::Result<(), String> {
    if let Some(index) = snapshot
        .unwanted_file_indices
        .iter()
        .copied()
        .find(|index| *index >= file_count)
    {
        return Err(format!(
            "unwanted_file_indices contains index {index} outside this torrent's file list"
        ));
    }
    Ok(())
}

/// Apply a captured selection to engine-local file vectors after a magnet's
/// real metadata becomes available. This shares exact matching semantics with
/// daemon persistence without requiring an engine to access daemon state.
pub fn apply_intake_selection(
    files: &[MetaFile],
    priorities: &mut [FilePriority],
    wanted: &mut [bool],
    snapshot: &IntakePolicySnapshot,
) {
    for (index, file) in files.iter().enumerate() {
        let excluded = snapshot.unwanted_file_indices.contains(&index)
            || snapshot
                .excluded_file_patterns
                .iter()
                .any(|pattern| intake_file_path_matches(file, pattern))
            || snapshot
                .excluded_file_rules
                .iter()
                .any(|rule| intake_file_rule_matches(file, rule));
        if excluded {
            if let Some(priority) = priorities.get_mut(index) {
                *priority = FilePriority::Unwanted;
            }
            if let Some(wanted) = wanted.get_mut(index) {
                *wanted = false;
            }
        }
    }
}

/// Return whether one file matches every populated condition in an intake
/// exclusion rule. An empty rule is not considered a match; configuration
/// validation rejects those rules before they can be saved.
pub fn intake_file_rule_matches(file: &MetaFile, rule: &PolicyFileExclusionRule) -> bool {
    let has_condition = rule.path_pattern.is_some()
        || rule.suffix.is_some()
        || rule.path_segment.is_some()
        || rule.min_size_bytes.is_some()
        || rule.max_size_bytes.is_some();
    has_condition
        && rule
            .path_pattern
            .as_deref()
            .is_none_or(|pattern| intake_file_path_matches(file, pattern))
        && rule.suffix.as_deref().is_none_or(|suffix| {
            file.path
                .last()
                .is_some_and(|name| ascii_ends_with(name.as_bytes(), suffix.as_bytes()))
        })
        && rule.path_segment.as_deref().is_none_or(|segment| {
            file.path
                .iter()
                .any(|component| component.eq_ignore_ascii_case(segment))
        })
        && rule
            .min_size_bytes
            .is_none_or(|minimum| file.length >= minimum)
        && rule
            .max_size_bytes
            .is_none_or(|maximum| file.length <= maximum)
}

fn intake_file_path_matches(file: &MetaFile, pattern: &str) -> bool {
    let path = file.path.join("/");
    // v1 multi-file metadata may include the torrent's display name as the
    // first path component. Profiles should be portable between a source that
    // records `release/samples/clip.bin` and the usual operator rule
    // `samples/*`, while still allowing a fully-qualified rule.
    let relative_path = (file.path.len() > 1).then(|| file.path[1..].join("/"));
    intake_pattern_matches(pattern, &path)
        || relative_path
            .as_deref()
            .is_some_and(|relative| intake_pattern_matches(pattern, relative))
}

/// Match one configured intake pattern against a torrent's slash-separated
/// file path. Matching is ASCII case-insensitive to make profile behavior
/// stable across common local filesystems.
pub fn intake_pattern_matches(pattern: &str, path: &str) -> bool {
    let pattern = pattern.trim().as_bytes();
    let path = path.as_bytes();
    let mut pattern_index = 0usize;
    let mut path_index = 0usize;
    let mut star = None;
    let mut retry_path = 0usize;

    while path_index < path.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?'
                || ascii_eq(pattern[pattern_index], path[path_index]))
        {
            pattern_index += 1;
            path_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star = Some(pattern_index);
            pattern_index += 1;
            retry_path = path_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            retry_path += 1;
            path_index = retry_path;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

/// Apply deterministic profile tracker-host rules to normal metainfo tracker
/// tiers. High-priority hosts are tried before normal and low-priority hosts;
/// within the same priority the original metainfo order is preserved. A
/// disabled matching host is removed entirely. When no rules are configured,
/// the original BEP tier structure is returned unchanged.
pub fn prioritized_tracker_tiers(
    announce: Option<&str>,
    announce_list: &[Vec<String>],
    rules: &[TrackerHostRule],
) -> Vec<Vec<String>> {
    let tiers = crate::models::tracker::build_tiers(announce, Some(announce_list));
    if rules.is_empty() {
        return tiers
            .into_iter()
            .fold(Vec::<Vec<String>>::new(), |mut grouped, tracker| {
                while grouped.len() <= tracker.tier {
                    grouped.push(Vec::new());
                }
                grouped[tracker.tier].push(tracker.url);
                grouped
            });
    }
    let urls = tiers
        .into_iter()
        .map(|tracker| tracker.url)
        .collect::<Vec<_>>();
    let ordered = prioritize_tracker_urls(&urls, rules);
    (!ordered.is_empty())
        .then_some(vec![ordered])
        .unwrap_or_default()
}

/// Filter and reorder an already-flat list of tracker URLs using the same
/// host rules as regular metainfo discovery. This is used for magnets, whose
/// `tr` parameters do not preserve BEP announce tiers.
pub fn prioritize_tracker_urls(urls: &[String], rules: &[TrackerHostRule]) -> Vec<String> {
    if rules.is_empty() {
        return urls.to_vec();
    }
    let mut candidates = urls
        .iter()
        .enumerate()
        .filter_map(|(index, url)| {
            let rule = rules
                .iter()
                .find(|rule| tracker_host_rule_matches(url, rule));
            if rule.is_some_and(|rule| rule.enabled == Some(false)) {
                return None;
            }
            Some((
                index,
                rule.and_then(|rule| rule.priority)
                    .unwrap_or(QueuePriority::Normal),
                url.clone(),
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .1
            .weight()
            .cmp(&left.1.weight())
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates.into_iter().map(|(_, _, url)| url).collect()
}

/// Return whether one tracker URL matches a host rule. Malformed URLs never
/// match a policy rule; they remain visible to normal tracker diagnostics
/// rather than being silently discarded.
pub fn tracker_host_rule_matches(url: &str, rule: &TrackerHostRule) -> bool {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .is_some_and(|host| intake_pattern_matches(&rule.host_pattern, &host))
}

fn ascii_eq(left: u8, right: u8) -> bool {
    left.eq_ignore_ascii_case(&right)
}

fn ascii_ends_with(value: &[u8], suffix: &[u8]) -> bool {
    value
        .get(value.len().saturating_sub(suffix.len())..)
        .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
}

/// Resolve a profile-scoped content directory below a selected storage root.
/// Configuration validation guarantees the value is safe; the defensive
/// fallback preserves the original root for malformed legacy state rather
/// than allowing a relative path to escape it.
pub fn organize_storage_path(base: String, subdirectory: Option<&str>) -> String {
    let Some(subdirectory) = subdirectory else {
        return base;
    };
    if !is_safe_content_subdirectory(subdirectory) {
        return base;
    }
    Path::new(&base).join(subdirectory).display().to_string()
}

/// Resolve the storage root for a torrent that elects to force a visible
/// top-level directory. Multi-file torrents already store every file below
/// their metainfo name, so only a single-file torrent gains a container.
/// Malformed legacy metadata leaves the selected root unchanged rather than
/// constructing an unsafe path.
pub fn force_top_level_storage_path(
    base: String,
    torrent_name: &str,
    is_multi_file: bool,
    force_top_level_folder: bool,
) -> String {
    if !force_top_level_folder || is_multi_file || !is_safe_content_subdirectory(torrent_name) {
        return base;
    }
    Path::new(&base).join(torrent_name).display().to_string()
}

fn is_safe_content_subdirectory(value: &str) -> bool {
    !value.trim().is_empty()
        && value.trim() == value
        && Path::new(value).components().all(
            |component| matches!(component, Component::Normal(component) if !component.is_empty()),
        )
}

/// Validate a requested incomplete payload suffix before it is captured in a
/// durable intake decision. A suffix is appended only to a final file-name
/// component, so separators, drive delimiters, and control characters are
/// rejected rather than interpreted as a path.
pub fn validate_partial_file_suffix(value: Option<&str>) -> std::result::Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > 64
        || value.contains(['/', '\\', ':'])
        || value.chars().any(char::is_control)
    {
        return Err(
            "partial_file_suffix must be a trimmed non-empty suffix of at most 64 bytes without path separators, drive delimiters, or control characters"
                .into(),
        );
    }
    Ok(())
}

fn torrent_option<T>(value: T) -> EffectivePolicyValue<Option<T>> {
    EffectivePolicyValue {
        value: Some(value),
        source: PolicyValueSource::Torrent,
    }
}

fn legacy_option<T>(value: T) -> EffectivePolicyValue<Option<T>> {
    EffectivePolicyValue {
        value: Some(value),
        source: PolicyValueSource::LegacyTorrent,
    }
}

fn torrent_value<T>(value: T) -> EffectivePolicyValue<T> {
    EffectivePolicyValue {
        value,
        source: PolicyValueSource::Torrent,
    }
}

fn legacy_value<T>(value: T) -> EffectivePolicyValue<T> {
    EffectivePolicyValue {
        value,
        source: PolicyValueSource::LegacyTorrent,
    }
}

fn resolve_value<T: Copy>(
    torrent: Option<T>,
    profile: Option<T>,
    global: T,
    profile_source: Option<PolicyValueSource>,
) -> EffectivePolicyValue<T> {
    if let Some(value) = torrent {
        return torrent_value(value);
    }
    if let Some(value) = profile {
        return EffectivePolicyValue {
            value,
            source: profile_source.unwrap_or(PolicyValueSource::Global),
        };
    }
    EffectivePolicyValue {
        value: global,
        source: PolicyValueSource::Global,
    }
}

fn resolve_optional_value<T: Copy>(
    profile: Option<T>,
    global: Option<T>,
    profile_source: Option<PolicyValueSource>,
) -> EffectivePolicyValue<Option<T>> {
    if let Some(value) = profile {
        return EffectivePolicyValue {
            value: Some(value),
            source: profile_source.unwrap_or(PolicyValueSource::Global),
        };
    }
    global_option(global)
}

fn resolve_profile(config: &Config, torrent: &Torrent) -> Option<EffectiveProfileAssignment> {
    if let Some(name) = torrent.policy.profile.as_ref() {
        if config.profiles.profiles.contains_key(name) {
            return Some(EffectiveProfileAssignment {
                name: name.clone(),
                source: PolicyValueSource::Profile {
                    profile: name.clone(),
                    origin: torrent
                        .policy
                        .profile_origin
                        .unwrap_or(PolicyProfileOrigin::Torrent),
                },
            });
        }
    }
    let labels = torrent
        .labels
        .iter()
        .filter_map(|label| {
            let normalized = normalize_label(label);
            (!normalized.is_empty()).then_some((normalized, label.trim().to_string()))
        })
        .collect::<BTreeSet<_>>();
    for (normalized, display) in labels {
        let profile = config
            .profiles
            .labels
            .iter()
            .find_map(|(configured, profile)| {
                (normalize_label(configured) == normalized).then_some(profile)
            });
        let Some(profile) = profile else {
            continue;
        };
        if config.profiles.profiles.contains_key(profile) {
            return Some(EffectiveProfileAssignment {
                name: profile.clone(),
                source: PolicyValueSource::Label {
                    label: display,
                    profile: profile.clone(),
                },
            });
        }
    }
    None
}

/// Canonical label key used for matching and ambiguity validation.
pub fn normalize_label(label: &str) -> String {
    label.trim().to_ascii_lowercase()
}

/// Validate profile-specific configuration independent of global daemon
/// configuration.
pub fn validate_profiles(profiles: &PolicyProfilesConfig) -> std::result::Result<(), String> {
    let mut names = BTreeSet::new();
    for (name, profile) in &profiles.profiles {
        if name.trim().is_empty() || name.trim() != name {
            return Err(format!(
                "profiles.profiles name {name:?} must be non-empty and trimmed"
            ));
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!("duplicate profile name ignoring case: {name}"));
        }
        for (field, path) in [
            (
                "storage.download_dir",
                profile.storage.download_dir.as_deref(),
            ),
            (
                "storage.incomplete_dir",
                profile.storage.incomplete_dir.as_deref(),
            ),
        ] {
            if path.is_some_and(|path| path.trim().is_empty()) {
                return Err(format!(
                    "profiles.profiles.{name}.{field} must not be empty when set"
                ));
            }
        }
        if profile
            .seeding
            .ratio_limit
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(format!(
                "profiles.profiles.{name}.seeding.ratio_limit must be a finite non-negative number"
            ));
        }
        for (index, pattern) in profile.intake.excluded_file_patterns.iter().enumerate() {
            if pattern.trim().is_empty() || pattern.len() > 1024 {
                return Err(format!(
                    "profiles.profiles.{name}.intake.excluded_file_patterns[{index}] must be non-empty and at most 1024 bytes"
                ));
            }
        }
        validate_profile_file_exclusion_rules(name, &profile.intake.excluded_file_rules)?;
        validate_tracker_host_rules(name, &profile.tracker.host_rules)?;
        if let Some(subdirectory) = profile.intake.organization_subdirectory.as_deref() {
            if !is_safe_content_subdirectory(subdirectory) {
                return Err(format!(
                    "profiles.profiles.{name}.intake.organization_subdirectory must be a non-empty relative path without dot or parent components"
                ));
            }
        }
        if let Some(subdirectory) = profile.intake.incomplete_subdirectory.as_deref() {
            if !is_safe_content_subdirectory(subdirectory) {
                return Err(format!(
                    "profiles.profiles.{name}.intake.incomplete_subdirectory must be a non-empty relative path without dot or parent components"
                ));
            }
        }
        validate_partial_file_suffix(profile.intake.partial_file_suffix.as_deref())
            .map_err(|error| format!("profiles.profiles.{name}.intake.{error}"))?;
    }
    let mut labels = BTreeSet::new();
    for (label, profile) in &profiles.labels {
        let normalized = normalize_label(label);
        if normalized.is_empty() || !labels.insert(normalized) {
            return Err(format!(
                "invalid or duplicate profiles.labels key {label:?}"
            ));
        }
        if !profiles.profiles.contains_key(profile) {
            return Err(format!(
                "profiles.labels.{label} references unknown profile {profile}"
            ));
        }
    }
    Ok(())
}

fn validate_tracker_host_rules(
    profile_name: &str,
    rules: &[TrackerHostRule],
) -> std::result::Result<(), String> {
    if rules.len() > 128 {
        return Err(format!(
            "profiles.profiles.{profile_name}.tracker.host_rules must contain at most 128 rules"
        ));
    }
    for (index, rule) in rules.iter().enumerate() {
        let rule_path = format!("profiles.profiles.{profile_name}.tracker.host_rules[{index}]");
        if rule.host_pattern.trim().is_empty()
            || rule.host_pattern.trim() != rule.host_pattern
            || rule.host_pattern.len() > 253
            || rule.host_pattern.contains(['/', '\\', '@', '#', '?'])
            || rule.host_pattern.chars().any(char::is_whitespace)
        {
            return Err(format!(
                "{rule_path}.host_pattern must be a non-empty trimmed host glob without URL delimiters"
            ));
        }
        if rule.enabled.is_none() && rule.priority.is_none() {
            return Err(format!("{rule_path} must set enabled and/or priority"));
        }
    }
    Ok(())
}

/// Validate request-level structured intake rules before they become part of a
/// durable add decision. Profile validation calls the same implementation with
/// its fully-qualified configuration path.
pub fn validate_intake_file_exclusion_rules(
    rules: &[PolicyFileExclusionRule],
) -> std::result::Result<(), String> {
    validate_file_exclusion_rules_at("file_exclusion_rules", rules)
}

fn validate_profile_file_exclusion_rules(
    profile_name: &str,
    rules: &[PolicyFileExclusionRule],
) -> std::result::Result<(), String> {
    validate_file_exclusion_rules_at(
        &format!("profiles.profiles.{profile_name}.intake.excluded_file_rules"),
        rules,
    )
}

fn validate_file_exclusion_rules_at(
    rules_path: &str,
    rules: &[PolicyFileExclusionRule],
) -> std::result::Result<(), String> {
    if rules.len() > 256 {
        return Err(format!("{rules_path} must contain at most 256 rules"));
    }
    for (index, rule) in rules.iter().enumerate() {
        let rule_path = format!("{rules_path}[{index}]");
        let has_condition = rule.path_pattern.is_some()
            || rule.suffix.is_some()
            || rule.path_segment.is_some()
            || rule.min_size_bytes.is_some()
            || rule.max_size_bytes.is_some();
        if !has_condition {
            return Err(format!("{rule_path} must contain at least one condition"));
        }
        if let Some(pattern) = rule.path_pattern.as_deref() {
            validate_nonempty_intake_text(&rule_path, "path_pattern", pattern, 1024)?;
        }
        if let Some(suffix) = rule.suffix.as_deref() {
            validate_nonempty_intake_text(&rule_path, "suffix", suffix, 255)?;
            if suffix.contains(['/', '\\']) {
                return Err(format!(
                    "{rule_path}.suffix must not contain a path separator"
                ));
            }
        }
        if let Some(segment) = rule.path_segment.as_deref() {
            validate_nonempty_intake_text(&rule_path, "path_segment", segment, 255)?;
            if segment.contains(['/', '\\']) {
                return Err(format!(
                    "{rule_path}.path_segment must not contain a path separator"
                ));
            }
        }
        if rule
            .min_size_bytes
            .zip(rule.max_size_bytes)
            .is_some_and(|(minimum, maximum)| minimum > maximum)
        {
            return Err(format!(
                "{rule_path}.min_size_bytes must not exceed max_size_bytes"
            ));
        }
    }
    Ok(())
}

fn validate_nonempty_intake_text(
    rule_path: &str,
    field: &str,
    value: &str,
    maximum_bytes: usize,
) -> std::result::Result<(), String> {
    if value.trim().is_empty() || value.trim() != value || value.len() > maximum_bytes {
        return Err(format!(
            "{rule_path}.{field} must be non-empty, trimmed, and at most {maximum_bytes} bytes"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{build_single_file_torrent, parse_torrent};

    fn torrent() -> Torrent {
        let bytes = build_single_file_torrent("linux.iso", b"test", 4, None, false);
        Torrent::new(parse_torrent(&bytes).unwrap(), 1)
    }

    fn config() -> Config {
        let mut config = Config::default();
        config.network.mode = crate::models::network::NetworkContainmentMode::Disabled;
        config.storage.download_dir = Some("/global/downloads".into());
        config.storage.incomplete_dir = Some("/global/incomplete".into());
        config.seeding.global_ratio_limit = Some(2.0);
        config.profiles.profiles.insert(
            "linux".into(),
            PolicyProfile {
                storage: PolicyStorage {
                    download_dir: Some("/profile/downloads".into()),
                    incomplete_dir: Some("/profile/incomplete".into()),
                },
                queue: PolicyQueue {
                    priority: Some(QueuePriority::High),
                    start_behavior: Some(StartBehavior::Paused),
                },
                seeding: PolicySeeding {
                    ratio_limit: Some(3.0),
                    idle_limit: Some(600),
                    seed_forever: Some(false),
                },
                bandwidth: PolicyBandwidth {
                    download_limit: Some(1_000),
                    upload_limit: Some(2_000),
                },
                tracker: PolicyTracker::default(),
                intake: PolicyIntake::default(),
                encryption_mode: Some(PeerEncryptionMode::Required),
            },
        );
        config
            .profiles
            .labels
            .insert("Linux".into(), "linux".into());
        config
    }

    #[test]
    fn label_resolution_is_case_insensitive_and_explainable() {
        let config = config();
        let mut torrent = torrent();
        torrent.labels = vec!["LINUX".into()];
        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);
        assert_eq!(effective.profile.as_ref().unwrap().name, "linux");
        assert!(matches!(
            effective.profile.unwrap().source,
            PolicyValueSource::Label { .. }
        ));
        assert_eq!(effective.queue_priority.value, QueuePriority::High);
        assert_eq!(effective.ratio_limit.value, Some(3.0));
        assert_eq!(effective.download_limit.value, 1_000);
        assert_eq!(
            effective.encryption_mode.value,
            PeerEncryptionMode::Required
        );
        assert!(matches!(
            effective.encryption_mode.source,
            PolicyValueSource::Label { .. }
        ));
    }

    #[test]
    fn explicit_profile_and_override_beat_label_and_profile() {
        let mut config = config();
        config
            .profiles
            .profiles
            .insert("other".into(), PolicyProfile::default());
        let mut torrent = torrent();
        torrent.labels = vec!["linux".into()];
        torrent.policy.profile = Some("other".into());
        torrent.policy.profile_origin = Some(PolicyProfileOrigin::AddRequest);
        torrent.policy.overrides.download_limit = Some(0);
        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);
        assert_eq!(effective.profile.as_ref().unwrap().name, "other");
        assert_eq!(effective.download_limit.value, 0);
        assert!(matches!(
            effective.download_limit.source,
            PolicyValueSource::Torrent
        ));
    }

    #[test]
    fn torrent_encryption_override_beats_profile_and_global() {
        let mut config = config();
        config.torrent.encryption_mode = PeerEncryptionMode::Disabled;
        let mut torrent = torrent();
        torrent.labels = vec!["linux".into()];
        torrent.policy.overrides.encryption_mode = Some(PeerEncryptionMode::Preferred);

        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);

        assert_eq!(
            effective.encryption_mode.value,
            PeerEncryptionMode::Preferred
        );
        assert!(matches!(
            effective.encryption_mode.source,
            PolicyValueSource::Torrent
        ));
        assert!(effective
            .live_inheritance_fields
            .contains(&"encryption_mode"));
    }

    #[test]
    fn encryption_mode_falls_back_to_global_when_profile_omits_it() {
        let mut config = config();
        config.torrent.encryption_mode = PeerEncryptionMode::Required;
        config
            .profiles
            .profiles
            .get_mut("linux")
            .unwrap()
            .encryption_mode = None;
        let mut torrent = torrent();
        torrent.labels = vec!["linux".into()];

        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);

        assert_eq!(
            effective.encryption_mode.value,
            PeerEncryptionMode::Required
        );
        assert!(matches!(
            effective.encryption_mode.source,
            PolicyValueSource::Global
        ));
    }

    #[test]
    fn storage_snapshot_survives_profile_edit() {
        let mut config = config();
        let mut torrent = torrent();
        torrent.policy.profile = Some("linux".into());
        torrent.policy.storage_snapshot = Some(PolicyStorageSnapshot {
            profile: "linux".into(),
            preserve_existing_storage: false,
            download_dir: Some("/snapshot/downloads".into()),
            incomplete_dir: Some("/snapshot/incomplete".into()),
        });
        config
            .profiles
            .profiles
            .get_mut("linux")
            .unwrap()
            .storage
            .download_dir = Some("/changed/downloads".into());
        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);
        assert_eq!(
            effective.download_dir.value.as_deref(),
            Some("/snapshot/downloads")
        );
        assert!(matches!(
            effective.download_dir.source,
            PolicyValueSource::ProfileStorageSnapshot { .. }
        ));
    }

    #[test]
    fn profile_storage_resolves_before_its_create_time_snapshot() {
        let config = config();
        let mut torrent = torrent();
        torrent.policy.profile = Some("linux".into());
        torrent.policy.profile_origin = Some(PolicyProfileOrigin::AddRequest);

        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);
        assert_eq!(
            effective.download_dir.value.as_deref(),
            Some("/profile/downloads")
        );
        assert_eq!(
            effective.incomplete_dir.value.as_deref(),
            Some("/profile/incomplete")
        );
        assert!(matches!(
            effective.download_dir.source,
            PolicyValueSource::Profile { .. }
        ));
    }

    #[test]
    fn an_existing_storage_snapshot_blocks_later_profile_storage() {
        let config = config();
        let mut torrent = torrent();
        torrent.policy.profile = Some("linux".into());
        torrent.policy.storage_snapshot = Some(PolicyStorageSnapshot {
            profile: String::new(),
            preserve_existing_storage: true,
            download_dir: None,
            incomplete_dir: Some("/existing/incomplete".into()),
        });

        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);
        assert_eq!(effective.download_dir.value, None);
        assert_eq!(
            effective.incomplete_dir.value.as_deref(),
            Some("/existing/incomplete")
        );
        assert!(matches!(
            effective.incomplete_dir.source,
            PolicyValueSource::ExistingStorageSnapshot
        ));
    }

    #[test]
    fn registration_storage_and_initial_admission_sources_are_explainable() {
        let mut config = config();
        config.queue.auto_start = false;
        let mut torrent = torrent();
        torrent.policy.storage_snapshot = Some(PolicyStorageSnapshot {
            profile: String::new(),
            preserve_existing_storage: false,
            download_dir: Some("/registered/downloads".into()),
            incomplete_dir: Some("/registered/incomplete".into()),
        });
        torrent.policy.initial_start_behavior = Some(StartBehavior::Start);
        // A later mapping/profile change can still select a profile for live
        // fields, but it cannot replace either captured creation-time value.
        torrent.labels = vec!["linux".into()];
        config
            .profiles
            .profiles
            .get_mut("linux")
            .unwrap()
            .queue
            .start_behavior = Some(StartBehavior::Paused);

        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);
        assert_eq!(
            effective.download_dir.value.as_deref(),
            Some("/registered/downloads")
        );
        assert!(matches!(
            effective.download_dir.source,
            PolicyValueSource::RegistrationStorageSnapshot
        ));
        assert_eq!(effective.start_behavior.value, StartBehavior::Start);
        assert!(matches!(
            effective.start_behavior.source,
            PolicyValueSource::InitialAdmissionSnapshot
        ));
        assert_eq!(
            effective.create_time_snapshot_fields,
            vec![
                "download_dir",
                "incomplete_dir",
                "start_behavior",
                "file_selection",
                "content_organization",
                "top_level_folder",
                "partial_file_suffix",
            ]
        );
    }

    #[test]
    fn validation_rejects_bad_references_and_ratios() {
        let mut profiles = PolicyProfilesConfig::default();
        profiles.labels.insert("linux".into(), "missing".into());
        assert!(validate_profiles(&profiles)
            .unwrap_err()
            .contains("unknown profile"));
        profiles.profiles.insert(
            "missing".into(),
            PolicyProfile {
                seeding: PolicySeeding {
                    ratio_limit: Some(-1.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        assert!(validate_profiles(&profiles)
            .unwrap_err()
            .contains("ratio_limit"));
    }

    #[test]
    fn intake_rules_match_paths_and_are_captured_explainably() {
        let mut config = config();
        let profile = config.profiles.profiles.get_mut("linux").unwrap();
        profile.intake = PolicyIntake {
            excluded_file_patterns: vec!["*.nfo".into(), "samples/*".into()],
            excluded_file_rules: Vec::new(),
            organization_subdirectory: Some("lawful/linux".into()),
            incomplete_subdirectory: Some("staging/linux".into()),
            force_top_level_folder: true,
            partial_file_suffix: Some(".part".into()),
        };
        let mut torrent = torrent();
        torrent.policy.intake_snapshot = Some(IntakePolicySnapshot {
            profile: "linux".into(),
            excluded_file_patterns: profile.intake.excluded_file_patterns.clone(),
            excluded_file_rules: profile.intake.excluded_file_rules.clone(),
            organization_subdirectory: profile.intake.organization_subdirectory.clone(),
            incomplete_subdirectory: profile.intake.incomplete_subdirectory.clone(),
            force_top_level_folder: profile.intake.force_top_level_folder,
            partial_file_suffix: profile.intake.partial_file_suffix.clone(),
            unwanted_file_indices: vec![4],
        });
        torrent.policy.preview_until_started = true;

        let effective = EffectiveTorrentPolicy::resolve(&config, &torrent);
        assert_eq!(
            effective.intake.excluded_file_patterns.value,
            vec!["*.nfo", "samples/*"]
        );
        assert_eq!(
            effective.intake.organization_subdirectory.value.as_deref(),
            Some("lawful/linux")
        );
        assert_eq!(
            effective.intake.incomplete_subdirectory.value.as_deref(),
            Some("staging/linux")
        );
        assert!(effective.intake.force_top_level_folder.value);
        assert_eq!(
            effective.intake.partial_file_suffix.value.as_deref(),
            Some(".part")
        );
        assert!(effective.intake.preview_until_started);
        assert!(matches!(
            effective.intake.excluded_file_patterns.source,
            PolicyValueSource::IntakeSnapshot { .. }
        ));
        assert!(intake_pattern_matches("*.NFO", "release/readme.nfo"));
        assert!(intake_pattern_matches("samples/*", "samples/clip.txt"));
        assert!(!intake_pattern_matches("samples/*", "docs/clip.txt"));
    }

    #[test]
    fn intake_selection_marks_profile_and_explicitly_excluded_files_unwanted() {
        let files = vec![
            MetaFile {
                path: vec!["keep.txt".into()],
                length: 1,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["samples".into(), "clip.txt".into()],
                length: 1,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["readme.nfo".into()],
                length: 1,
                pieces_root: None,
            },
        ];
        let snapshot = IntakePolicySnapshot {
            excluded_file_patterns: vec!["samples/*".into(), "*.nfo".into()],
            unwanted_file_indices: vec![0],
            ..Default::default()
        };
        let mut priorities = vec![FilePriority::Normal; files.len()];
        let mut wanted = vec![true; files.len()];
        apply_intake_selection(&files, &mut priorities, &mut wanted, &snapshot);
        assert_eq!(priorities, vec![FilePriority::Unwanted; files.len()]);
        assert_eq!(wanted, vec![false; files.len()]);
    }

    #[test]
    fn structured_intake_rules_combine_suffix_path_segment_glob_and_size() {
        let files = vec![
            MetaFile {
                path: vec!["release".into(), "samples".into(), "clip.bin".into()],
                length: 100,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["release".into(), "docs".into(), "guide.pdf".into()],
                length: 10,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["release".into(), "notices".into(), "readme.NFO".into()],
                length: 20,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["release".into(), "media".into(), "large.iso".into()],
                length: 4_096,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["release".into(), "keep.iso".into()],
                length: 1_024,
                pieces_root: None,
            },
        ];
        let snapshot = IntakePolicySnapshot {
            excluded_file_rules: vec![
                PolicyFileExclusionRule {
                    path_segment: Some("samples".into()),
                    ..Default::default()
                },
                PolicyFileExclusionRule {
                    path_pattern: Some("docs/*".into()),
                    max_size_bytes: Some(100),
                    ..Default::default()
                },
                PolicyFileExclusionRule {
                    suffix: Some(".nfo".into()),
                    ..Default::default()
                },
                PolicyFileExclusionRule {
                    min_size_bytes: Some(4_000),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut priorities = vec![FilePriority::Normal; files.len()];
        let mut wanted = vec![true; files.len()];
        apply_intake_selection(&files, &mut priorities, &mut wanted, &snapshot);

        assert_eq!(
            priorities,
            vec![
                FilePriority::Unwanted,
                FilePriority::Unwanted,
                FilePriority::Unwanted,
                FilePriority::Unwanted,
                FilePriority::Normal,
            ]
        );
        assert_eq!(wanted, vec![false, false, false, false, true]);
    }

    #[test]
    fn tracker_host_rules_filter_and_prioritize_without_losing_stable_order() {
        let tiers = prioritized_tracker_tiers(
            Some("https://announce.example/announce"),
            &[
                vec![
                    "https://normal.example/announce".into(),
                    "udp://priority.example:6969/announce".into(),
                ],
                vec![
                    "https://low.example/announce".into(),
                    "https://disabled.example/announce".into(),
                ],
            ],
            &[
                TrackerHostRule {
                    host_pattern: "disabled.example".into(),
                    enabled: Some(false),
                    priority: None,
                },
                TrackerHostRule {
                    host_pattern: "priority.example".into(),
                    enabled: None,
                    priority: Some(QueuePriority::High),
                },
                TrackerHostRule {
                    host_pattern: "low.example".into(),
                    enabled: None,
                    priority: Some(QueuePriority::Low),
                },
            ],
        );
        assert_eq!(
            tiers,
            vec![vec![
                "udp://priority.example:6969/announce".to_string(),
                "https://normal.example/announce".to_string(),
                "https://low.example/announce".to_string(),
            ]]
        );
        assert!(tracker_host_rule_matches(
            "https://PRIORITY.example/announce",
            &TrackerHostRule {
                host_pattern: "priority.example".into(),
                enabled: None,
                priority: None,
            }
        ));
    }

    #[test]
    fn intake_selection_rejects_an_index_outside_resolved_metadata() {
        let snapshot = IntakePolicySnapshot {
            unwanted_file_indices: vec![3],
            ..Default::default()
        };
        assert!(validate_intake_selection_indices(&snapshot, 3)
            .unwrap_err()
            .contains("index 3"));
    }

    #[test]
    fn validation_rejects_unsafe_intake_configuration() {
        let mut profiles = PolicyProfilesConfig::default();
        profiles.profiles.insert(
            "linux".into(),
            PolicyProfile {
                intake: PolicyIntake {
                    excluded_file_patterns: vec![" ".into()],
                    excluded_file_rules: Vec::new(),
                    organization_subdirectory: Some("../escape".into()),
                    incomplete_subdirectory: None,
                    force_top_level_folder: false,
                    partial_file_suffix: None,
                },
                ..Default::default()
            },
        );
        assert!(validate_profiles(&profiles)
            .unwrap_err()
            .contains("excluded_file_patterns"));
        profiles
            .profiles
            .get_mut("linux")
            .unwrap()
            .intake
            .excluded_file_patterns
            .clear();
        profiles
            .profiles
            .get_mut("linux")
            .unwrap()
            .intake
            .excluded_file_rules = vec![PolicyFileExclusionRule {
            min_size_bytes: Some(2),
            max_size_bytes: Some(1),
            ..Default::default()
        }];
        assert!(validate_profiles(&profiles)
            .unwrap_err()
            .contains("min_size_bytes"));
        profiles
            .profiles
            .get_mut("linux")
            .unwrap()
            .intake
            .excluded_file_patterns = vec!["*.tmp".into()];
        profiles
            .profiles
            .get_mut("linux")
            .unwrap()
            .intake
            .excluded_file_rules
            .clear();
        assert!(validate_profiles(&profiles)
            .unwrap_err()
            .contains("organization_subdirectory"));
    }

    #[test]
    fn organization_path_stays_below_the_selected_root() {
        assert_eq!(
            organize_storage_path("/data/downloads".into(), Some("lawful/linux")),
            "/data/downloads/lawful/linux"
        );
        assert_eq!(
            organize_storage_path("/data/downloads".into(), Some("../escape")),
            "/data/downloads"
        );
    }
}
