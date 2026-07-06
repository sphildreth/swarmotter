// SPDX-License-Identifier: Apache-2.0

//! Torrent session glue: aggregates metadata, storage progress, state, and
//! per-torrent settings into an in-memory `Torrent` record owned by the daemon.

use crate::autopilot::AutopilotMode;
use crate::hash::InfoHash;
use crate::meta::TorrentMeta;
use crate::models::torrent::{
    FilePriority, TorrentFile, TorrentHealth, TorrentState, TorrentSummary,
};
use crate::storage::PieceProgress;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Per-torrent runtime settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TorrentSettings {
    pub labels: Vec<String>,
    pub download_dir: Option<String>,
    pub priorities: Vec<FilePriority>,
    pub wanted: Vec<bool>,
}

/// An in-memory torrent record.
#[derive(Debug, Clone)]
pub struct Torrent {
    pub meta: TorrentMeta,
    pub state: TorrentState,
    pub progress: PieceProgress,
    pub downloaded: u64,
    pub uploaded: u64,
    pub rate_down: u64,
    pub rate_up: u64,
    pub active_peer_workers: usize,
    pub known_peers: usize,
    pub labels: Vec<String>,
    pub download_dir: Option<String>,
    pub date_added: u64,
    pub date_completed: Option<u64>,
    pub files: Vec<TorrentFile>,
    pub priorities: Vec<FilePriority>,
    pub wanted: Vec<bool>,
    pub error: Option<String>,
    pub health: TorrentHealth,
    /// Per-torrent download limit in bytes/sec (0 = unlimited).
    pub download_limit: u64,
    /// Per-torrent upload limit in bytes/sec (0 = unlimited).
    pub upload_limit: u64,
    /// True for magnets that still need their metadata fetched via BEP 9.
    pub needs_metadata: bool,
    /// The real info hash for a magnet (before metadata is fetched); used as
    /// the registry key and for tracker announce. `None` for `.torrent` files.
    pub magnet_info_hash: Option<InfoHash>,
    /// Magnet display name and trackers (for metadata fetch + announce).
    pub magnet_name: Option<String>,
    pub magnet_trackers: Vec<String>,
    /// Optional per-torrent autopilot mode override.
    pub autopilot_mode_override: Option<AutopilotMode>,
}

impl Torrent {
    pub fn new(meta: TorrentMeta, date_added: u64) -> Self {
        let piece_count = meta.piece_count();
        let file_count = meta.files.len();
        let files = meta
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| TorrentFile {
                index: i,
                path: f.path.join("/"),
                length: f.length,
                bytes_completed: 0,
                priority: FilePriority::Normal,
                wanted: true,
            })
            .collect();
        Self {
            meta,
            state: TorrentState::Queued,
            progress: PieceProgress::new(piece_count),
            downloaded: 0,
            uploaded: 0,
            rate_down: 0,
            rate_up: 0,
            active_peer_workers: 0,
            known_peers: 0,
            labels: Vec::new(),
            download_dir: None,
            date_added,
            date_completed: None,
            files,
            priorities: vec![FilePriority::Normal; file_count],
            wanted: vec![true; file_count],
            error: None,
            health: TorrentHealth::unknown(),
            download_limit: 0,
            upload_limit: 0,
            needs_metadata: false,
            magnet_info_hash: None,
            magnet_name: None,
            magnet_trackers: Vec::new(),
            autopilot_mode_override: None,
        }
    }

    pub fn info_hash(&self) -> InfoHash {
        // For magnets that still need metadata, the real info hash is the
        // magnet's; otherwise use the parsed metadata's info hash.
        self.magnet_info_hash.unwrap_or(self.meta.info_hash)
    }

    pub fn name(&self) -> &str {
        &self.meta.name
    }

    pub fn pieces_have(&self) -> usize {
        self.progress.pieces_have()
    }

    pub fn bytes_completed(&self) -> u64 {
        (self.progress.fraction() * self.meta.total_length as f64) as u64
    }

    pub fn progress(&self) -> f64 {
        self.progress.fraction()
    }

    pub fn ratio(&self) -> f64 {
        if self.downloaded == 0 {
            0.0
        } else {
            self.uploaded as f64 / self.downloaded as f64
        }
    }

    pub fn to_summary(&self) -> TorrentSummary {
        TorrentSummary {
            info_hash: self.info_hash(),
            name: self.name().to_string(),
            state: self.state,
            total_length: self.meta.total_length,
            bytes_completed: self.bytes_completed(),
            uploaded: self.uploaded,
            downloaded: self.downloaded,
            piece_count: self.meta.piece_count(),
            pieces_have: self.pieces_have(),
            piece_length: self.meta.piece_length,
            private: self.meta.is_private(),
            labels: self.labels.clone(),
            download_dir: self.download_dir.clone(),
            download_limit: self.download_limit,
            upload_limit: self.upload_limit,
            autopilot_mode_override: self.autopilot_mode_override,
            rate_down: self.rate_down,
            rate_up: self.rate_up,
            active_peer_workers: self.active_peer_workers,
            known_peers: self.known_peers,
            ratio: self.ratio(),
            queue_position: None,
            date_added: self.date_added,
            date_completed: self.date_completed,
            health: self.health.clone(),
        }
    }
}

/// A registry holding all torrents keyed by info hash, with duplicate
/// detection. Pure logic; the daemon wraps this with channels/locking.
#[derive(Debug, Default)]
pub struct TorrentRegistry {
    pub torrents: BTreeMap<InfoHash, Torrent>,
}

impl TorrentRegistry {
    pub fn add(&mut self, torrent: Torrent) -> Result<(), InfoHash> {
        if self.torrents.contains_key(&torrent.info_hash()) {
            return Err(torrent.info_hash());
        }
        self.torrents.insert(torrent.info_hash(), torrent);
        Ok(())
    }

    pub fn remove(&mut self, hash: &InfoHash) -> Option<Torrent> {
        self.torrents.remove(hash)
    }

    pub fn get(&self, hash: &InfoHash) -> Option<&Torrent> {
        self.torrents.get(hash)
    }

    pub fn get_mut(&mut self, hash: &InfoHash) -> Option<&mut Torrent> {
        self.torrents.get_mut(hash)
    }

    pub fn list(&self) -> Vec<&Torrent> {
        self.torrents.values().collect()
    }

    pub fn contains(&self, hash: &InfoHash) -> bool {
        self.torrents.contains_key(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::build_single_file_torrent;

    #[test]
    fn registry_duplicate_detection() {
        let bytes = build_single_file_torrent("f", b"some data here", 8, None, false);
        let meta = crate::meta::parse_torrent(&bytes).unwrap();
        let mut reg = TorrentRegistry::default();
        let t = Torrent::new(meta.clone(), 1);
        assert!(reg.add(t).is_ok());
        let t2 = Torrent::new(meta, 2);
        assert!(reg.add(t2).is_err()); // duplicate
        assert_eq!(reg.torrents.len(), 1);
    }

    #[test]
    fn new_torrent_defaults() {
        let bytes =
            build_single_file_torrent("f.bin", b"abcd".repeat(4).as_slice(), 8, None, false);
        let meta = crate::meta::parse_torrent(&bytes).unwrap();
        let t = Torrent::new(meta, 100);
        assert!(t.autopilot_mode_override.is_none());
        assert_eq!(t.state, TorrentState::Queued);
        assert_eq!(t.meta.piece_count(), 2);
        assert_eq!(t.files.len(), 1);
        assert_eq!(t.priorities.len(), 1);
        assert!(t.wanted[0]);
        assert!(t.progress().abs() < f64::EPSILON);
        let summary = t.to_summary();
        assert!(summary.autopilot_mode_override.is_none());
    }
}
