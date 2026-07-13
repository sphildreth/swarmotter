// SPDX-License-Identifier: Apache-2.0

//! Storage diagnostics exposed through the API and Web UI.

use serde::{Deserialize, Serialize};

use crate::config::CowStrategy;

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
    /// Durable fast-resume metadata, when `storage.resume_dir` is set.
    Resume,
    /// Durable daemon state (and a future durable database), when configured.
    State,
    /// Configured scratch/fallback temporary storage.
    Temporary,
    /// Daemon file logging destination.
    Log,
    TorrentOverride,
    WatchDownload,
    DefaultDownload,
    Policy,
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
    /// Most-specific mounted filesystem boundary discovered for this root.
    /// `None` is a safe platform fallback when mount metadata is unavailable.
    pub mount_point: Option<String>,
    /// Mount options reported by the host, when available.
    pub mount_options: Option<Vec<String>>,
    /// Mount source reported by the host, when available.
    pub mount_source: Option<String>,
    pub total_space_bytes: Option<u64>,
    pub free_space_bytes: Option<u64>,
    pub available_space_bytes: Option<u64>,
    pub required_free_space_bytes: u64,
    pub reserve_satisfied: Option<bool>,
    pub torrent_count: usize,
    pub active_torrents: usize,
    /// Declared payload bytes currently admitted to this root's active
    /// download engines. This is a scheduling reservation, not a free-space
    /// measurement.
    pub active_bytes: u64,
    pub active_write_rate: u64,
    pub active_recheck_rate: Option<u64>,
    /// Rolling sustained local payload-write throughput observed by storage
    /// handles associated with this root.
    pub sustained_write_bytes_per_second: u64,
    /// Rolling sustained local verification-read throughput observed while
    /// validating pieces on this root. Seeder reads are intentionally not
    /// included.
    pub sustained_verification_bytes_per_second: u64,
    /// The explicit configured CoW strategy. A strategy never changes an
    /// existing payload file.
    pub cow_strategy: CowStrategy,
    /// Whether the configured CoW strategy can be applied on this root.
    /// `None` means the platform could not determine this safely.
    pub cow_strategy_supported: Option<bool>,
    /// Number of full rechecks currently using this root.
    pub active_rechecks: usize,
    /// The matching configured lexical control root, when one applies.
    pub root_control_path: Option<String>,
    /// Per-root active download-engine cap; `0` means unlimited.
    pub max_active_downloads: usize,
    /// Per-root declared active-payload cap; `0` means unlimited.
    pub max_active_bytes: u64,
    /// Shared sustained payload-write cap in bytes/sec; `0` means unlimited.
    pub max_write_bytes_per_second: u64,
    /// Per-root simultaneous full-recheck cap; `0` means unlimited.
    pub max_concurrent_rechecks: usize,
    pub warnings: Vec<String>,
}
