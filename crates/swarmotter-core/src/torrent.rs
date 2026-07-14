// SPDX-License-Identifier: Apache-2.0

//! Torrent session glue: aggregates metadata, storage progress, state, and
//! per-torrent settings into an in-memory `Torrent` record owned by the daemon.

use crate::autopilot::AutopilotMode;
use crate::hash::{InfoHash, TorrentIdentity, TorrentKey};
use crate::magnet::MagnetDirectPeer;
use crate::meta::TorrentMeta;
use crate::models::torrent::{
    FilePriority, SeedingStatus, TorrentFile, TorrentHealth, TorrentState, TorrentSummary,
};
use crate::policy::TorrentPolicy;
use crate::ratio::TorrentSeeding;
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

/// Durable intent used only to recover work that was live when fail-closed
/// containment blocked the torrent data plane.
///
/// This is deliberately separate from the coarse public lifecycle state: a
/// `network_blocked` record must not make paused, stopped, queued, or
/// pre-existing blocked work start automatically when the path recovers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainmentRecoveryIntent {
    Downloading,
    DownloadingMetadata,
    Seeding,
}

/// An in-memory torrent record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Torrent {
    pub meta: TorrentMeta,
    pub state: TorrentState,
    pub progress: PieceProgress,
    pub downloaded: u64,
    pub uploaded: u64,
    /// Per-torrent ratio/idle overrides. Missing in legacy state means inherit.
    #[serde(default)]
    pub seeding: TorrentSeeding,
    /// Fine-grained persisted seeding lifecycle.
    #[serde(default)]
    pub seeding_status: SeedingStatus,
    pub rate_down: u64,
    pub rate_up: u64,
    pub active_peer_workers: usize,
    pub known_peers: usize,
    pub labels: Vec<String>,
    /// Named profile assignment and explicit policy overrides. Missing from
    /// pre-profile state means normal global inheritance.
    #[serde(default)]
    pub policy: TorrentPolicy,
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
    /// The complete v1/v2/hybrid magnet identity before metadata is fetched.
    /// This remains separate from placeholder `meta`, whose raw `info` bytes
    /// belong to a synthetic v1 record and must never be relabeled as the
    /// magnet's real hybrid identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub magnet_identity: Option<TorrentIdentity>,
    /// Magnet display name and trackers (for metadata fetch + announce).
    pub magnet_name: Option<String>,
    pub magnet_trackers: Vec<String>,
    /// Deferred BEP 53 select-only indices for an unresolved magnet.
    ///
    /// They are applied once the real metadata makes file indices
    /// authoritative, then cleared so subsequent operator selections remain
    /// durable and authoritative.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub magnet_select_only_file_indices: Vec<usize>,
    /// Literal `x.pe` direct-peer hints originally supplied by a magnet.
    ///
    /// These survive metadata resolution so a restart can still use the same
    /// contained candidates for payload discovery. They never carry a
    /// hostname or trigger DNS resolution.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub magnet_direct_peers: Vec<MagnetDirectPeer>,
    /// Optional per-torrent autopilot mode override.
    pub autopilot_mode_override: Option<AutopilotMode>,
    /// Work that was actually live when containment transitioned to blocked.
    /// Persisted so a daemon restart while the path is down cannot lose or
    /// broaden the recovery set. Cleared atomically when recovery is applied
    /// or an operator lifecycle command supersedes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containment_recovery_intent: Option<ContainmentRecoveryIntent>,
}

impl Torrent {
    pub fn new(meta: TorrentMeta, date_added: u64) -> Self {
        // Pure v2 has a file-aligned logical piece space.  Valid complete
        // metainfo provides that layout; unresolved magnets deliberately fall
        // back to their placeholder count until metadata arrives.
        let piece_count = meta
            .data_piece_count()
            .unwrap_or_else(|_| meta.piece_count());
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
            seeding: TorrentSeeding::default(),
            seeding_status: SeedingStatus::NotEligible,
            rate_down: 0,
            rate_up: 0,
            active_peer_workers: 0,
            known_peers: 0,
            labels: Vec::new(),
            policy: TorrentPolicy::default(),
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
            magnet_identity: None,
            magnet_name: None,
            magnet_trackers: Vec::new(),
            magnet_select_only_file_indices: Vec::new(),
            magnet_direct_peers: Vec::new(),
            autopilot_mode_override: None,
            containment_recovery_intent: None,
        }
    }

    pub fn info_hash(&self) -> InfoHash {
        // For magnets that still need metadata, the real info hash is the
        // magnet's; otherwise use the parsed metadata's info hash.
        self.magnet_info_hash.unwrap_or(self.meta.info_hash)
    }

    /// The authoritative identity visible to callers. While magnet metadata
    /// is pending this is the parsed magnet identity; after resolution it is
    /// the identity from the canonical torrent metadata.
    pub fn identity(&self) -> &TorrentIdentity {
        self.magnet_identity.as_ref().unwrap_or(&self.meta.identity)
    }

    /// Canonical daemon, durable-state, queue, and API key.
    ///
    /// This must be used for record ownership. [`Self::info_hash`] is kept
    /// only for v1 compatibility surfaces and is intentionally not a pure-v2
    /// fallback (a pure-v2 record must never be indexed by `InfoHash::ZERO`).
    pub fn key(&self) -> TorrentKey {
        self.identity()
            .primary_key()
            .unwrap_or_else(|| TorrentKey::v1(self.info_hash()))
    }

    /// All full identifiers that resolve to this record, primary first.
    ///
    /// A hybrid record has its v1 key as primary and its full v2 identity as
    /// an alias. Pure v2 has one full SHA-256 key, never a truncated alias.
    pub fn keys(&self) -> Vec<TorrentKey> {
        let keys = self.identity().keys();
        if keys.is_empty() {
            vec![TorrentKey::v1(self.info_hash())]
        } else {
            keys
        }
    }

    pub fn name(&self) -> &str {
        &self.meta.name
    }

    pub fn pieces_have(&self) -> usize {
        self.progress.pieces_have()
    }

    pub fn bytes_completed(&self) -> u64 {
        verified_bytes(&self.meta, self.progress.bitfield())
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
            info_hash: self.key(),
            identity: self.identity().clone(),
            name: self.name().to_string(),
            state: self.state,
            error: self.error.clone(),
            total_length: self.meta.total_length,
            bytes_completed: self.bytes_completed(),
            uploaded: self.uploaded,
            downloaded: self.downloaded,
            seeding: self.seeding.clone(),
            seeding_status: self.seeding_status,
            effective_ratio_limit: None,
            effective_idle_limit: None,
            piece_count: self
                .meta
                .data_piece_count()
                .unwrap_or_else(|_| self.meta.piece_count()),
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

    /// Recompute every file's progress from verified piece byte ranges.
    /// Boundary pieces credit only the bytes intersecting each file.
    pub fn recompute_file_bytes_completed(&mut self) {
        if self.meta.requires_v2_data_plane() {
            for row in &mut self.files {
                row.bytes_completed = 0;
            }
            if let Ok(layout) = self.meta.v2_piece_layout() {
                for piece in 0..layout.piece_count() {
                    if !self.progress.bitfield().has(piece) {
                        continue;
                    }
                    if let Some(mapping) = layout.piece(piece) {
                        if let Some(row) = self.files.get_mut(mapping.file_index) {
                            row.bytes_completed = row
                                .bytes_completed
                                .saturating_add(mapping.length)
                                .min(row.length);
                        }
                    }
                }
            }
            return;
        }

        let bitfield = self.progress.bitfield();
        let piece_length = self.meta.piece_length;
        let mut file_start = 0u64;
        for (index, file) in self.meta.files.iter().enumerate() {
            let file_end = file_start.saturating_add(file.length);
            let mut completed = 0u64;
            if file.length > 0 && piece_length > 0 {
                let first_piece = file_start / piece_length;
                let last_piece = (file_end - 1) / piece_length;
                for piece in first_piece..=last_piece {
                    if bitfield.has(piece as usize) {
                        let piece_start = piece.saturating_mul(piece_length);
                        let piece_end = piece_start
                            .saturating_add(piece_length)
                            .min(self.meta.total_length);
                        completed = completed.saturating_add(
                            file_end
                                .min(piece_end)
                                .saturating_sub(file_start.max(piece_start)),
                        );
                    }
                }
            }
            if let Some(row) = self.files.get_mut(index) {
                row.bytes_completed = completed.min(file.length);
            }
            file_start = file_end;
        }
    }
}

/// Exact verified bytes represented by a piece bitfield. The final piece is
/// credited only for its actual byte length.
pub fn verified_bytes(meta: &TorrentMeta, bitfield: &crate::storage::PieceBitfield) -> u64 {
    if meta.requires_v2_data_plane() {
        return meta.v2_piece_layout().map_or(0, |layout| {
            (0..layout.piece_count())
                .filter(|index| bitfield.has(*index))
                .filter_map(|index| layout.piece(index))
                .map(|piece| piece.length)
                .sum()
        });
    }

    (0..meta.piece_count())
        .filter(|index| bitfield.has(*index))
        .filter_map(|index| meta.piece_byte_range(index as u64))
        .map(|(start, end)| end.saturating_sub(start))
        .sum()
}

/// A registry holding all torrents by full [`TorrentKey`].
///
/// A hybrid torrent is stored only once under its v1 primary key. Its full
/// v2 key is kept in `aliases`, so callers resolving either full locator see
/// the same record without allowing two records to claim one alias.
#[derive(Debug, Default)]
pub struct TorrentRegistry {
    pub torrents: BTreeMap<TorrentKey, Torrent>,
    aliases: BTreeMap<TorrentKey, TorrentKey>,
}

impl TorrentRegistry {
    /// Insert a record, rejecting primary and hybrid-alias collisions.
    pub fn add(&mut self, torrent: Torrent) -> Result<(), TorrentKey> {
        let primary = torrent.key();
        let keys = torrent.keys();
        for key in &keys {
            if self.torrents.contains_key(key) || self.aliases.contains_key(key) {
                return Err(*key);
            }
        }
        self.torrents.insert(primary, torrent);
        for key in keys {
            if key != primary {
                self.aliases.insert(key, primary);
            }
        }
        Ok(())
    }

    /// Resolve a primary key or a hybrid alias to the stored primary key.
    pub fn canonical_key(&self, key: &TorrentKey) -> Option<TorrentKey> {
        if self.torrents.contains_key(key) {
            Some(*key)
        } else {
            self.aliases.get(key).copied()
        }
    }

    pub fn remove(&mut self, key: &TorrentKey) -> Option<Torrent> {
        let primary = self.canonical_key(key)?;
        self.aliases.retain(|_, target| *target != primary);
        self.torrents.remove(&primary)
    }

    pub fn get(&self, key: &TorrentKey) -> Option<&Torrent> {
        self.canonical_key(key)
            .and_then(|primary| self.torrents.get(&primary))
    }

    pub fn get_mut(&mut self, key: &TorrentKey) -> Option<&mut Torrent> {
        self.canonical_key(key)
            .and_then(|primary| self.torrents.get_mut(&primary))
    }

    pub fn list(&self) -> Vec<&Torrent> {
        self.torrents.values().collect()
    }

    pub fn contains(&self, key: &TorrentKey) -> bool {
        self.canonical_key(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::V2InfoHash;
    use crate::meta::{build_multi_file_torrent, build_single_file_torrent};

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
    fn registry_resolves_hybrid_v2_alias_to_one_primary_record() {
        let bytes = build_single_file_torrent("f", b"some data here", 8, None, false);
        let mut meta = crate::meta::parse_torrent(&bytes).unwrap();
        let v1 = meta.info_hash;
        let v2 = V2InfoHash::from_bytes([0x52; 32]);
        meta.identity = TorrentIdentity::hybrid(v1, v2);

        let mut registry = TorrentRegistry::default();
        registry.add(Torrent::new(meta, 1)).unwrap();

        let primary = TorrentKey::v1(v1);
        let alias = TorrentKey::v2(v2);
        assert_eq!(registry.canonical_key(&primary), Some(primary));
        assert_eq!(registry.canonical_key(&alias), Some(primary));
        assert_eq!(registry.get(&alias).unwrap().key(), primary);
        assert!(registry.contains(&alias));
        assert_eq!(registry.torrents.len(), 1);

        assert!(registry.remove(&alias).is_some());
        assert!(registry.get(&primary).is_none());
        assert!(registry.get(&alias).is_none());
    }

    #[test]
    fn registry_rejects_primary_or_alias_collisions() {
        let bytes = build_single_file_torrent("f", b"some data here", 8, None, false);
        let mut hybrid_meta = crate::meta::parse_torrent(&bytes).unwrap();
        let v1 = hybrid_meta.info_hash;
        let v2 = V2InfoHash::from_bytes([0x53; 32]);
        hybrid_meta.identity = TorrentIdentity::hybrid(v1, v2);

        let mut registry = TorrentRegistry::default();
        registry.add(Torrent::new(hybrid_meta, 1)).unwrap();

        let mut v2_meta = crate::meta::parse_torrent(&bytes).unwrap();
        v2_meta.info_hash = InfoHash::ZERO;
        v2_meta.identity = TorrentIdentity::v2(v2);
        assert_eq!(
            registry.add(Torrent::new(v2_meta, 2)),
            Err(TorrentKey::v2(v2))
        );
        assert_eq!(registry.torrents.len(), 1);
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

    #[test]
    fn exact_single_file_bytes_use_actual_final_piece_length() {
        let bytes = build_single_file_torrent("nine.bin", b"123456789", 4, None, false);
        let meta = crate::meta::parse_torrent(&bytes).unwrap();
        let mut torrent = Torrent::new(meta, 1);
        torrent.progress.have_piece(2);
        torrent.recompute_file_bytes_completed();
        assert_eq!(torrent.bytes_completed(), 1);
        assert_eq!(torrent.files[0].bytes_completed, 1);
    }

    #[test]
    fn exact_multi_file_bytes_split_verified_boundary_pieces() {
        let files = vec![
            (vec!["a.bin".into()], 3),
            (vec!["b.bin".into()], 4),
            (vec!["c.bin".into()], 2),
        ];
        let bytes = build_multi_file_torrent("bundle", &files, &[b"abc", b"defg", b"hi"], 4, None);
        let meta = crate::meta::parse_torrent(&bytes).unwrap();
        let mut torrent = Torrent::new(meta, 1);
        torrent.progress.have_piece(0);
        torrent.recompute_file_bytes_completed();
        assert_eq!(torrent.bytes_completed(), 4);
        assert_eq!(
            torrent
                .files
                .iter()
                .map(|file| file.bytes_completed)
                .collect::<Vec<_>>(),
            vec![3, 1, 0]
        );

        torrent.progress.have_piece(2);
        torrent.recompute_file_bytes_completed();
        assert_eq!(torrent.bytes_completed(), 5);
        assert_eq!(
            torrent
                .files
                .iter()
                .map(|file| file.bytes_completed)
                .collect::<Vec<_>>(),
            vec![3, 1, 1]
        );
    }
}
