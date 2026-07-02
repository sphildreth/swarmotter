// SPDX-License-Identifier: Apache-2.0

//! Storage layer: file layout, piece read/write and verification, fast resume,
//! forced recheck, and file selection/prioritization.
//!
//! This module models storage layout and verification logic in a testable way
//! over in-memory piece bitsets and byte ranges. Actual disk I/O is performed
//! through the daemon using `tokio::fs`; the pure layout/verification logic
//! lives here.

pub mod io;
pub mod layout;
pub mod resume;

pub use io::StorageIo;
pub use layout::{FileLayout, FileSlice, StorageLayout};
pub use resume::{FastResume, PieceBitfield};

use crate::meta::TorrentMeta;

/// A piece bitset tracking which pieces have been verified on disk.
#[derive(Debug, Clone)]
pub struct PieceProgress {
    pub have: Vec<bool>,
    pub total: usize,
}

impl PieceProgress {
    pub fn new(total: usize) -> Self {
        Self {
            have: vec![false; total],
            total,
        }
    }

    pub fn have_piece(&mut self, index: usize) {
        if index < self.total {
            self.have[index] = true;
        }
    }

    pub fn pieces_have(&self) -> usize {
        self.have.iter().filter(|b| **b).count()
    }

    pub fn is_complete(&self) -> bool {
        self.pieces_have() == self.total
    }

    pub fn fraction(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        self.pieces_have() as f64 / self.total as f64
    }
}

/// Compute which file byte ranges a piece covers, used for partial downloads
/// and file selection.
pub fn piece_file_ranges(meta: &TorrentMeta, piece_index: usize) -> Vec<FileSlice> {
    let Some((start, end)) = meta.piece_byte_range(piece_index as u64) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut offset = 0u64;
    for (file_index, file) in meta.files.iter().enumerate() {
        let file_end = offset + file.length;
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
    out
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
