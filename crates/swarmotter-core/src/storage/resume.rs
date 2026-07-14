// SPDX-License-Identifier: Apache-2.0

//! Fast resume metadata: persisted piece bitfield and per-torrent accounting.
//!
//! The fast-resume format is JSON (`.swarmotter.resume`) so it is human-readable
//! and debuggable. It records the full torrent key, piece bitfield, byte counts, and
//! file priorities so a torrent can resume without a full recheck.

use crate::hash::TorrentKey;
use crate::models::torrent::FilePriority;
use serde::{Deserialize, Serialize};

/// A piece bitfield serialized as a hex string.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PieceBitfield(#[serde(with = "hex_bitfield")] Vec<u8>);

impl PieceBitfield {
    pub fn new(piece_count: usize) -> Self {
        let bytes = piece_count.div_ceil(8);
        Self(vec![0u8; bytes])
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn set(&mut self, index: usize) {
        let byte = index / 8;
        let bit = 7 - (index % 8);
        if byte < self.0.len() {
            self.0[byte] |= 1 << bit;
        }
    }

    pub fn clear(&mut self, index: usize) {
        let byte = index / 8;
        let bit = 7 - (index % 8);
        if byte < self.0.len() {
            self.0[byte] &= !(1 << bit);
        }
    }

    pub fn has(&self, index: usize) -> bool {
        let byte = index / 8;
        let bit = 7 - (index % 8);
        if byte < self.0.len() {
            self.0[byte] & (1 << bit) != 0
        } else {
            false
        }
    }

    pub fn count(&self, total: usize) -> usize {
        let full = total / 8;
        let rem = total % 8;
        let mut count = self
            .0
            .iter()
            .take(full)
            .map(|b| b.count_ones() as usize)
            .sum();
        if rem > 0 {
            if let Some(b) = self.0.get(full) {
                let mask = 0xFFu8 << (8 - rem);
                count += (b & mask).count_ones() as usize;
            }
        }
        count
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Fast resume data persisted to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FastResume {
    /// Full durable torrent identity. New records serialize this as
    /// `torrent_key`; the old v1-only `info_hash` spelling remains accepted
    /// so pre-v2 resume files safely retain their 40-character identity.
    #[serde(rename = "torrent_key", alias = "info_hash")]
    pub key: TorrentKey,
    pub name: String,
    pub piece_bitfield: PieceBitfield,
    pub piece_count: usize,
    pub downloaded: u64,
    pub uploaded: u64,
    pub bytes_completed: u64,
    pub total_length: u64,
    pub priorities: Vec<FilePriority>,
    /// Per-file wanted state. Missing in older resume files, where callers
    /// should treat an empty list as "all wanted" for compatibility.
    #[serde(default)]
    pub wanted: Vec<bool>,
    /// File metadata captured after the last verified write. A missing stamp
    /// forces a safe recheck, preserving compatibility with older resumes.
    #[serde(default)]
    pub file_stamps: Vec<ResumeFileStamp>,
    pub download_dir: Option<String>,
    pub date_added: u64,
    pub date_completed: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeFileStamp {
    pub exists: bool,
    pub length: u64,
    pub modified_unix_nanos: Option<u64>,
    /// Unix filesystem identity and change timestamp. These catch same-size
    /// edits whose modification time was preserved or rounded by the
    /// filesystem. Missing fields in older resume data force a safe recheck.
    #[serde(default)]
    pub device: Option<u64>,
    #[serde(default)]
    pub inode: Option<u64>,
    #[serde(default)]
    pub changed_unix_seconds: Option<i64>,
    #[serde(default)]
    pub changed_subsec_nanos: Option<i64>,
}

impl FastResume {
    pub fn serialize_json(&self) -> std::result::Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn parse_json(s: &str) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

mod hex_bitfield {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> std::result::Result<S::Ok, S::Error> {
        hex::encode(v).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        hex::decode(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitfield_set_get_count() {
        let mut bf = PieceBitfield::new(20);
        bf.set(0);
        bf.set(7);
        bf.set(19);
        assert!(bf.has(0));
        assert!(!bf.has(1));
        assert!(bf.has(19));
        assert_eq!(bf.count(20), 3);
        bf.clear(0);
        assert_eq!(bf.count(20), 2);
    }

    #[test]
    fn resume_roundtrip() {
        let resume = FastResume {
            key: TorrentKey::from(crate::hash::InfoHash::ZERO),
            name: "test".into(),
            piece_bitfield: PieceBitfield::new(10),
            piece_count: 10,
            downloaded: 1000,
            uploaded: 2000,
            bytes_completed: 1000,
            total_length: 1000,
            priorities: vec![FilePriority::Normal; 2],
            wanted: vec![true, false],
            file_stamps: Vec::new(),
            download_dir: Some("/data".into()),
            date_added: 1,
            date_completed: Some(2),
        };
        let json = resume.serialize_json().unwrap();
        let back = FastResume::parse_json(&json).unwrap();
        assert_eq!(back.key, resume.key);
        assert_eq!(back.piece_count, 10);
        assert_eq!(back.priorities.len(), 2);
        assert_eq!(back.wanted, vec![true, false]);
    }

    #[test]
    fn legacy_resume_defaults_wanted_state() {
        let json = r#"{
            "info_hash":"0000000000000000000000000000000000000000",
            "name":"test",
            "piece_bitfield":"00",
            "piece_count":1,
            "downloaded":0,
            "uploaded":0,
            "bytes_completed":0,
            "total_length":1,
            "priorities":["normal"],
            "download_dir":null,
            "date_added":1,
            "date_completed":null
        }"#;

        let resume = FastResume::parse_json(json).unwrap();
        assert!(resume.wanted.is_empty());
    }
}
