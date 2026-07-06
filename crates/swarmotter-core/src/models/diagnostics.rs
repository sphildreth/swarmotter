// SPDX-License-Identifier: Apache-2.0

//! Operational diagnostics exposed by the API and rendered by the Web UI.

use serde::{Deserialize, Serialize};

use crate::config::{Config, PeerEncryptionMode, WatchFolderConfig};
use crate::models::network::NetworkHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticLevel {
    Ok,
    Warning,
    Invalid,
}

impl DiagnosticLevel {
    pub fn worst(a: Self, b: Self) -> Self {
        use DiagnosticLevel::*;
        match (a, b) {
            (Invalid, _) | (_, Invalid) => Invalid,
            (Warning, _) | (_, Warning) => Warning,
            _ => Ok,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorCheck {
    pub id: String,
    pub label: String,
    pub level: DiagnosticLevel,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remediation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    pub level: DiagnosticLevel,
    pub summary: String,
    pub checks: Vec<DoctorCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterfaceDiagnostic {
    pub name: String,
    pub status: String,
    pub addresses: Vec<String>,
    pub selected: bool,
    pub has_ipv4: bool,
    pub has_ipv6: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkPathCheck {
    pub id: String,
    pub label: String,
    pub level: DiagnosticLevel,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkDiagnostics {
    pub health: NetworkHealth,
    pub listen_port: u16,
    pub dht_port: u16,
    pub torrent_allow_ipv6: bool,
    pub utp_enabled: bool,
    pub utp_prefer_tcp: bool,
    pub peer_encryption_mode: PeerEncryptionMode,
    pub interfaces: Vec<NetworkInterfaceDiagnostic>,
    pub checks: Vec<NetworkPathCheck>,
    pub containment_matrix: Vec<NetworkPathCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchFolderStatus {
    pub config: WatchFolderConfig,
    pub exists: bool,
    pub pending_torrent_files: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_result: Option<crate::watch::ImportResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchStatus {
    pub enabled: bool,
    pub folders: Vec<WatchFolderStatus>,
    pub recent_imports: Vec<crate::watch::ImportResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSnapshot {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub lines: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetResult {
    pub torrents_removed: usize,
    pub storage_paths: Vec<String>,
    pub storage_entries_removed: usize,
    pub log_paths: Vec<String>,
    pub log_files_cleared: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateResult {
    pub persisted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
    pub restart_required: bool,
    pub restart_required_fields: Vec<String>,
    pub applied_runtime_fields: Vec<String>,
    pub config: Config,
}
