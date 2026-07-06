// SPDX-License-Identifier: Apache-2.0

//! Storage diagnostics exposed through the API and Web UI.

use serde::{Deserialize, Serialize};

/// Point-in-time diagnostics for configured and discovered storage roots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageDiagnostics {
    pub roots: Vec<StorageRootDiagnostics>,
    pub minimum_free_space_bytes: u64,
    pub minimum_free_space_percent: u8,
    pub generated_at: u64,
}

/// Role a root plays in SwarmOtter's storage layout.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum StorageRootRole {
    Download,
    Incomplete,
    TorrentOverride,
    WatchDownload,
    DefaultDownload,
}

/// Per-root storage health and capacity summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageRootDiagnostics {
    pub path: String,
    pub roles: Vec<StorageRootRole>,
    pub exists: bool,
    pub is_directory: bool,
    pub writable: bool,
    pub filesystem_type: Option<String>,
    pub total_space_bytes: Option<u64>,
    pub free_space_bytes: Option<u64>,
    pub available_space_bytes: Option<u64>,
    pub required_free_space_bytes: u64,
    pub reserve_satisfied: Option<bool>,
    pub torrent_count: usize,
    pub active_torrents: usize,
    pub active_write_rate: u64,
    pub active_recheck_rate: Option<u64>,
    pub warnings: Vec<String>,
}
