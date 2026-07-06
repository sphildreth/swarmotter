// SPDX-License-Identifier: Apache-2.0

//! Public domain models used across the daemon and API.

pub mod diagnostics;
pub mod health;
pub mod network;
pub mod peer;
pub mod stats;
pub mod storage;
pub mod torrent;
pub mod tracker;

pub use diagnostics::{
    ConfigUpdateResult, DiagnosticLevel, DoctorCheck, DoctorReport, LogSnapshot,
    NetworkDiagnostics, NetworkInterfaceDiagnostic, NetworkPathCheck, ResetResult,
    WatchFolderStatus, WatchStatus,
};
pub use health::{HealthCalculator, HealthInput};
pub use network::{NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth};
pub use peer::{EnginePeerHealth, Peer, PeerDirection, PeerFlags};
pub use stats::{
    AutopilotAction, AutopilotActionKind, AutopilotDecision, AutopilotInput, AutopilotReason,
    AutopilotSnapshot, GlobalStats, SlowCause, TorrentDiagnostics, TorrentStats,
};
pub use storage::{StorageDiagnostics, StorageRootDiagnostics, StorageRootRole};
pub use torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
pub use tracker::{TrackerId, TrackerInfo, TrackerKind, TrackerStatus, TrackerTier};
