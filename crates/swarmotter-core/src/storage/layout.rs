// SPDX-License-Identifier: Apache-2.0

//! Storage file layout: mapping pieces to files, incomplete/complete dirs.

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};
use crate::meta::TorrentMeta;

/// A slice of a file covered by a piece.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSlice {
    pub file_index: usize,
    pub offset_in_file: u64,
    pub length: u64,
}

/// Logical storage layout for a torrent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageLayout {
    pub download_dir: String,
    pub incomplete_dir: Option<String>,
    pub name: String,
    pub files: Vec<LayoutFile>,
    pub piece_length: u64,
    pub total_length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutFile {
    pub path: String,
    pub length: u64,
}

impl StorageLayout {
    /// Resolve the on-disk path for a file given complete vs incomplete state.
    pub fn file_path(&self, index: usize, complete: bool) -> Option<String> {
        let file = self.files.get(index)?;
        let base = if complete {
            &self.download_dir
        } else {
            self.incomplete_dir.as_ref().unwrap_or(&self.download_dir)
        };
        Some(join_path(base, &self.name, &file.path))
    }

    /// True if any file path contains a component that would escape the base
    /// directory (path traversal).
    pub fn has_unsafe_path(&self) -> bool {
        for f in &self.files {
            for seg in f.path.split('/') {
                if seg == ".." {
                    return true;
                }
            }
        }
        false
    }
}

/// File layout helper mapping file indices to byte ranges across the torrent.
#[derive(Debug)]
pub struct FileLayout {
    pub files: Vec<(usize, u64, u64)>, // (index, start, end)
}

impl FileLayout {
    /// Build from a list of (index, length).
    pub fn from_lengths(files: &[(usize, u64)]) -> Result<Self> {
        Self::from_indexed_lengths(files.iter().copied())
    }

    pub fn from_meta(meta: &TorrentMeta) -> Result<Self> {
        Self::from_indexed_lengths(meta.files.iter().map(|file| file.length).enumerate())
    }

    fn from_indexed_lengths(files: impl ExactSizeIterator<Item = (usize, u64)>) -> Result<Self> {
        let mut out = Vec::with_capacity(files.len());
        let mut offset = 0u64;
        for (index, length) in files {
            let end = offset.checked_add(length).ok_or_else(|| {
                CoreError::MalformedTorrent(format!("file offset overflow at file index {index}"))
            })?;
            out.push((index, offset, end));
            offset = end;
        }
        Ok(Self { files: out })
    }

    /// Map an arbitrary byte interval using the sorted end offsets. Empty
    /// files occupy no bytes and never require opening a payload handle.
    pub fn slices(&self, start: u64, end: u64) -> Vec<FileSlice> {
        if start >= end {
            return Vec::new();
        }
        let first = self
            .files
            .partition_point(|&(_, _, file_end)| file_end <= start);
        self.files[first..]
            .iter()
            .take_while(|&&(_, file_start, _)| file_start < end)
            .filter(|&&(_, file_start, file_end)| file_end > file_start)
            .map(|&(file_index, file_start, file_end)| FileSlice {
                file_index,
                offset_in_file: start.saturating_sub(file_start),
                length: end.min(file_end) - start.max(file_start),
            })
            .collect()
    }

    /// Visit each nonempty file and the v1 pieces containing its bytes. A
    /// boundary piece can appear for multiple files with different priorities.
    pub fn file_piece_ranges(
        &self,
        meta: &TorrentMeta,
    ) -> impl Iterator<Item = (usize, std::ops::Range<usize>)> + '_ {
        let length = meta.piece_length;
        let count = meta.piece_count() as u64;
        self.files.iter().filter_map(move |&(index, start, end)| {
            if start == end || length == 0 {
                return None;
            }
            let first = (start / length).min(count) as usize;
            let limit = end.div_ceil(length).min(count) as usize;
            Some((index, first..limit))
        })
    }

    /// Return file indices overlapping `[start, end)`.
    pub fn overlapping(&self, start: u64, end: u64) -> Vec<usize> {
        self.files
            .iter()
            .filter(|(_, fstart, fend)| *fend > start && *fstart < end)
            .map(|(i, _, _)| *i)
            .collect()
    }
}

fn join_path(base: &str, name: &str, file_path: &str) -> String {
    let mut s = String::from(base);
    if !s.ends_with('/') {
        s.push('/');
    }
    s.push_str(name);
    if !file_path.is_empty() {
        s.push('/');
        s.push_str(file_path);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_path_complete_vs_incomplete() {
        let layout = StorageLayout {
            download_dir: "/data/downloads".into(),
            incomplete_dir: Some("/data/incomplete".into()),
            name: "dir".into(),
            files: vec![LayoutFile {
                path: "a.txt".into(),
                length: 10,
            }],
            piece_length: 16,
            total_length: 10,
        };
        assert_eq!(
            layout.file_path(0, true),
            Some("/data/downloads/dir/a.txt".to_string())
        );
        assert_eq!(
            layout.file_path(0, false),
            Some("/data/incomplete/dir/a.txt".to_string())
        );
    }

    #[test]
    fn detects_path_traversal() {
        let layout = StorageLayout {
            download_dir: "/d".into(),
            incomplete_dir: None,
            name: "n".into(),
            files: vec![LayoutFile {
                path: "../escape".into(),
                length: 1,
            }],
            piece_length: 16,
            total_length: 1,
        };
        assert!(layout.has_unsafe_path());
    }

    #[test]
    fn file_layout_overlapping() {
        let files = vec![(0usize, 10u64), (1, 10), (2, 10)];
        let fl = FileLayout::from_lengths(&files).unwrap();
        assert_eq!(fl.overlapping(5, 15), vec![0, 1]);
        assert_eq!(fl.overlapping(0, 30), vec![0, 1, 2]);
    }

    #[test]
    fn indexed_slices_cover_every_byte_once_across_empty_and_adjacent_files() {
        let lengths = [(0, 0), (1, 3), (2, 0), (3, 1), (4, 0), (5, 3), (6, 0)];
        let layout = FileLayout::from_lengths(&lengths).unwrap();
        let bytes: Vec<_> = lengths
            .into_iter()
            .flat_map(|(file, length)| (0..length).map(move |offset| (file, offset)))
            .collect();
        for start in 0..=bytes.len() {
            for end in start..=bytes.len() {
                let slices = layout.slices(start as u64, end as u64);
                assert!(slices.iter().all(|slice| slice.length > 0));
                let actual: Vec<_> = slices
                    .iter()
                    .flat_map(|slice| {
                        (0..slice.length).map(|i| (slice.file_index, slice.offset_in_file + i))
                    })
                    .collect();
                assert_eq!(actual, bytes[start..end]);
            }
        }
    }

    #[test]
    fn file_layout_rejects_offset_overflow_without_panicking() {
        let files = vec![(0usize, u64::MAX), (1, 1)];
        let result = std::panic::catch_unwind(|| FileLayout::from_lengths(&files));
        assert!(result.is_ok(), "file offset overflow must not panic");
        let error = result.unwrap().unwrap_err();
        assert!(matches!(&error, CoreError::MalformedTorrent(_)));
        assert!(error.to_string().contains("file index 1"));
    }
}
