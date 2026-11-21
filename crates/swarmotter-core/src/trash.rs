// SPDX-License-Identifier: Apache-2.0

//! Trash/quarantine policy and API models.
//!
//! Trash entries are filesystem-backed so a failed or interrupted move leaves
//! an inspectable staging directory rather than a half-written database row.
//! Ordinary live-library mutation remains in the SQLite state transaction.

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};
use crate::models::torrent::TorrentState;
use crate::torrent::Torrent;

/// Configured local trash behavior.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrashConfig {
    /// Absolute local trash root. Trash is disabled when unset.
    #[serde(default)]
    pub path: Option<String>,
    /// Explicit retention rules. Rules select candidates for preview/apply;
    /// they do not silently delete anything in the background.
    #[serde(default)]
    pub retention: Vec<TrashRetentionRule>,
}

/// One explicit retention rule. Every populated field must match.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrashRetentionRule {
    pub name: String,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    /// Empty means any lifecycle state.
    #[serde(default)]
    pub states: Vec<TorrentState>,
    /// Minimum age in seconds. Age is measured from completion when available,
    /// otherwise from the torrent's add time.
    #[serde(default)]
    pub min_age_seconds: u64,
}

impl TrashConfig {
    pub fn enabled(&self) -> bool {
        self.path.is_some()
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(path) = self.path.as_deref() {
            if path.trim().is_empty() {
                return Err(CoreError::InvalidConfig(
                    "trash.path must not be empty when set".into(),
                ));
            }
        }
        let mut names = std::collections::HashSet::new();
        for rule in &self.retention {
            if rule.name.trim().is_empty() {
                return Err(CoreError::InvalidConfig(
                    "trash.retention name must not be empty".into(),
                ));
            }
            if !names.insert(rule.name.as_str()) {
                return Err(CoreError::InvalidConfig(format!(
                    "duplicate trash retention rule {:?}",
                    rule.name
                )));
            }
            if rule
                .profile
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
                || rule
                    .label
                    .as_deref()
                    .is_some_and(|value| value.trim().is_empty())
            {
                return Err(CoreError::InvalidConfig(format!(
                    "trash retention rule {:?} has an empty profile or label",
                    rule.name
                )));
            }
        }
        Ok(())
    }

    pub fn rule(&self, name: &str) -> Option<&TrashRetentionRule> {
        self.retention.iter().find(|rule| rule.name == name)
    }
}

/// Explain whether one retention rule applies and when it would qualify.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrashRuleEvaluation {
    pub name: String,
    pub matches: bool,
    pub qualified: bool,
    pub age_seconds: u64,
    pub qualifies_at: Option<u64>,
    pub reason: String,
}

/// Evaluate one rule against the current torrent, effective profile name, and
/// age reference timestamp.
pub fn evaluate_retention_rule(
    rule: &TrashRetentionRule,
    torrent: &Torrent,
    effective_profile: Option<&str>,
    now: u64,
) -> TrashRuleEvaluation {
    let reference = torrent.date_completed.unwrap_or(torrent.date_added);
    let age_seconds = now.saturating_sub(reference);
    let mut reasons = Vec::new();
    let mut matches = true;
    if let Some(profile) = rule.profile.as_deref() {
        if effective_profile != Some(profile) {
            matches = false;
            reasons.push(format!(
                "profile {} does not match {profile}",
                effective_profile.unwrap_or("none")
            ));
        }
    }
    if let Some(label) = rule.label.as_deref() {
        if !torrent
            .labels
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(label))
        {
            matches = false;
            reasons.push(format!("label {label:?} is not attached"));
        }
    }
    if !rule.states.is_empty() && !rule.states.contains(&torrent.state) {
        matches = false;
        reasons.push(format!(
            "state {} is not one of {}",
            torrent.state.as_str(),
            rule.states
                .iter()
                .map(|state| state.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let qualified = matches && age_seconds >= rule.min_age_seconds;
    if matches && !qualified {
        reasons.push(format!(
            "minimum age {}s not reached ({}s)",
            rule.min_age_seconds, age_seconds
        ));
    }
    if matches && qualified {
        reasons.push(format!(
            "matched profile/label/state and minimum age: {age_seconds}s >= {}s",
            rule.min_age_seconds
        ));
    }
    TrashRuleEvaluation {
        name: rule.name.clone(),
        matches,
        qualified,
        age_seconds,
        qualifies_at: (rule.min_age_seconds > 0)
            .then_some(reference.saturating_add(rule.min_age_seconds)),
        reason: reasons.join("; "),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrashSelectionMode {
    #[default]
    Torrent,
    StalePartials,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrashSelectionRequest {
    /// Explicit torrent locators. Empty selects by retention rule(s).
    #[serde(default)]
    pub hashes: Vec<String>,
    #[serde(default)]
    pub rule: Option<String>,
    #[serde(default)]
    pub mode: TrashSelectionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashPath {
    pub path: String,
    pub role: String,
    pub exists: bool,
    pub kind: String,
    pub bytes: u64,
    pub action: String,
    pub reason: String,
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashPreviewItem {
    pub torrent_key: String,
    pub name: String,
    pub state: String,
    pub effective_profile: Option<String>,
    pub labels: Vec<String>,
    pub age_seconds: u64,
    pub applied_rule: Option<String>,
    pub rule_reason: String,
    pub qualifies_at: Option<u64>,
    pub eligible: bool,
    pub action: String,
    pub paths: Vec<TrashPath>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashPreview {
    pub mode: TrashSelectionMode,
    pub generated_at: u64,
    pub items: Vec<TrashPreviewItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashApplyItem {
    pub torrent_key: String,
    pub name: String,
    pub action: String,
    pub status: String,
    pub entry_id: Option<String>,
    pub moved_bytes: u64,
    pub paths_moved: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashApplyResult {
    pub mode: TrashSelectionMode,
    pub items: Vec<TrashApplyItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashEntryView {
    pub id: String,
    pub kind: String,
    pub created_at: u64,
    pub torrent_key: Option<String>,
    pub name: String,
    pub state: Option<String>,
    pub profile: Option<String>,
    pub labels: Vec<String>,
    pub reason: String,
    pub applied_rule: Option<String>,
    pub status: String,
    pub has_original_metainfo: bool,
    pub moved_bytes: u64,
    pub paths: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashStatus {
    pub enabled: bool,
    pub path: Option<String>,
    pub retention: Vec<TrashRetentionRule>,
    pub entries: Vec<TrashEntryView>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreRequest {
    /// Only `fail` is accepted. Existing destinations are never overwritten.
    #[serde(default)]
    pub collision_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreResult {
    pub entry_id: String,
    pub status: String,
    pub torrent_key: Option<String>,
    pub paths_restored: usize,
    pub error: Option<String>,
}

/// On-disk entry manifest. This is intentionally separate from API views so a
/// corrupt/truncated entry can still be listed as incomplete without exposing
/// a full torrent record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashEntryManifest {
    pub format: String,
    pub id: String,
    pub kind: String,
    pub created_at: u64,
    pub reason: String,
    pub applied_rule: Option<String>,
    pub torrent_key: Option<String>,
    pub name: String,
    pub state: Option<String>,
    pub profile: Option<String>,
    pub labels: Vec<String>,
    pub queue_order_index: Option<usize>,
    pub queue_bypass_index: Option<usize>,
    pub original_metainfo_file: Option<String>,
    pub torrent_json: Option<String>,
    pub locations: Vec<TrashLocation>,
    pub stale_files: Vec<TrashStaleFile>,
    pub moved_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashLocation {
    pub role: String,
    pub original_root: String,
    pub trash_subdir: String,
    pub partial_file_suffix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashStaleFile {
    pub original_path: String,
    pub trashed_path: String,
    pub bytes: u64,
}

/// Validate a restore collision policy. Only no-overwrite behavior exists.
pub fn validate_collision_policy(policy: Option<&str>) -> Result<()> {
    match policy.unwrap_or("fail") {
        "fail" => Ok(()),
        other => Err(CoreError::InvalidArgument(format!(
            "unsupported restore collision policy {other:?}; only 'fail' is supported"
        ))),
    }
}
