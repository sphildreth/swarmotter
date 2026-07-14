// SPDX-License-Identifier: Apache-2.0

//! Storage layer: file layout, piece read/write and verification, fast resume,
//! forced recheck, and file selection/prioritization.
//!
//! This module models storage layout and verification logic in a testable way
//! over in-memory piece bitsets and byte ranges. Actual disk I/O is performed
//! through the daemon using `tokio::fs`; the pure layout/verification logic
//! lives here.

pub mod diagnostics;
pub mod io;
pub mod layout;
pub mod metrics;
pub mod resume;

pub use diagnostics::{
    check_storage_preflight, inspect_storage_root, required_free_space_bytes, StoragePreflight,
    StorageRootUsage,
};
pub use io::{StorageIo, StoragePathOwnership};
pub use layout::{FileLayout, FileSlice, StorageLayout};
pub use metrics::{StorageIoMetrics, StorageThroughput};
pub use resume::{FastResume, PieceBitfield};

use crate::error::{CoreError, Result};
use crate::meta::TorrentMeta;

/// A piece bitset tracking which pieces have been verified on disk.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PieceProgress {
    bitfield: PieceBitfield,
    pub total: usize,
    have_count: usize,
}

impl PieceProgress {
    pub fn new(total: usize) -> Self {
        Self {
            bitfield: PieceBitfield::new(total),
            total,
            have_count: 0,
        }
    }

    pub fn have_piece(&mut self, index: usize) {
        if index < self.total && !self.bitfield.has(index) {
            self.bitfield.set(index);
            self.have_count += 1;
        }
    }

    pub fn replace_from_bitfield(&mut self, bitfield: &PieceBitfield, total: usize) {
        self.bitfield = bitfield.clone();
        self.total = total;
        self.have_count = self.bitfield.count(total);
    }

    pub fn pieces_have(&self) -> usize {
        self.have_count
    }

    pub fn bitfield(&self) -> &PieceBitfield {
        &self.bitfield
    }

    pub fn is_complete(&self) -> bool {
        self.have_count == self.total
    }

    pub fn fraction(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        self.have_count as f64 / self.total as f64
    }
}

/// Compute which file byte ranges a piece covers, used for partial downloads
/// and file selection.
pub fn piece_file_ranges(meta: &TorrentMeta, piece_index: usize) -> Result<Vec<FileSlice>> {
    let Some((start, end)) = meta.piece_byte_range(piece_index as u64) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut offset = 0u64;
    for (file_index, file) in meta.files.iter().enumerate() {
        let file_end = offset.checked_add(file.length).ok_or_else(|| {
            CoreError::MalformedTorrent(format!("file offset overflow at file index {file_index}"))
        })?;
        if file_end > start && offset < end {
            let slice_start = offset.max(start) - offset;
            let slice_end = file_end.min(end) - offset;
            out.push(FileSlice {
                file_index,
                offset_in_file: slice_start,
                length: slice_end - slice_start,
            });
        }
        offset = file_end;
    }
    Ok(out)
}

/// Verify a piece's SHA-1 hash against the metadata.
pub fn verify_piece(meta: &TorrentMeta, index: usize, data: &[u8]) -> bool {
    if index >= meta.pieces.len() {
        return false;
    }
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(data);
    let digest = hasher.finalize();
    digest.as_slice() == meta.pieces[index]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{build_single_file_torrent, parse_torrent, MetaFile};

    #[test]
    fn piece_progress_uses_cached_count_and_packed_replacement() {
        let mut progress = PieceProgress::new(10);
        assert_eq!(progress.pieces_have(), 0);
        assert_eq!(progress.fraction(), 0.0);

        progress.have_piece(2);
        progress.have_piece(2);
        progress.have_piece(9);
        assert_eq!(progress.pieces_have(), 2);
        assert!(!progress.is_complete());

        let mut bitfield = PieceBitfield::new(10);
        for index in 0..10 {
            bitfield.set(index);
        }
        progress.replace_from_bitfield(&bitfield, 10);
        assert_eq!(progress.pieces_have(), 10);
        assert!(progress.is_complete());
        assert_eq!(progress.fraction(), 1.0);
    }

    #[test]
    fn piece_file_ranges_rejects_file_offset_overflow_without_panicking() {
        let bytes = build_single_file_torrent("offset.bin", b"abcdefgh", 8, None, false);
        let mut meta = parse_torrent(&bytes).unwrap();
        meta.files = vec![
            MetaFile {
                path: vec!["first.bin".into()],
                length: u64::MAX,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["second.bin".into()],
                length: 1,
                pieces_root: None,
            },
        ];

        let result = std::panic::catch_unwind(|| piece_file_ranges(&meta, 0));
        assert!(result.is_ok(), "file offset overflow must not panic");
        let error = result.unwrap().unwrap_err();
        assert!(matches!(&error, CoreError::MalformedTorrent(_)));
        assert!(error.to_string().contains("file index 1"));
    }
}
