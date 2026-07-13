// SPDX-License-Identifier: Apache-2.0

//! Named policy profiles and deterministic effective-setting resolution.
//!
//! Resolution is pure and never mutates torrent state. This lets the daemon
//! apply live settings safely while API and Web UI callers can show the value
//! and layer that supplied it.

use crate::config::{Config, PeerEncryptionMode, StartBehavior};
use crate::torrent::Torrent;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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
            live_inheritance_fields: vec![
                "queue_priority",
                "ratio_limit",
                "idle_limit",
                "seed_forever",
                "download_limit",
                "upload_limit",
                "encryption_mode",
            ],
            create_time_snapshot_fields: vec!["download_dir", "incomplete_dir", "start_behavior"],
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
            vec!["download_dir", "incomplete_dir", "start_behavior"]
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
}
