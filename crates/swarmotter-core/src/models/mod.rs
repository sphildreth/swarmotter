// SPDX-License-Identifier: Apache-2.0

//! Public domain models used across the daemon and API.

pub mod health;
pub mod network;
pub mod peer;
pub mod stats;
pub mod torrent;
pub mod tracker;

pub use health::{HealthCalculator, HealthInput};
pub use network::{NetworkContainmentMode, NetworkContainmentStatus, NetworkHealth};
pub use peer::{EnginePeerHealth, Peer, PeerDirection, PeerFlags};
pub use stats::{GlobalStats, TorrentDiagnostics, TorrentStats};
pub use torrent::{FilePriority, TorrentFile, TorrentState, TorrentSummary};
pub use tracker::{TrackerId, TrackerInfo, TrackerKind, TrackerStatus, TrackerTier};
