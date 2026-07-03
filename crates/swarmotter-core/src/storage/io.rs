// SPDX-License-Identifier: Apache-2.0

//! Real async storage I/O: writes blocks to disk, verifies pieces by hash,
//! reads blocks for seeding, and persists/loads fast-resume metadata.
//!
//! This module performs actual disk I/O through `tokio::fs`. It maps each
//! piece to one or more files (handling single- and multi-file torrents and
//! boundaries across files), writes incoming blocks at the correct file
//! offsets, verifies completed pieces by SHA-1, marks verified pieces, and
//! saves/reloads fast-resume state. Storage errors are surfaced through the
//! existing [`CoreError`] model.
//!
//! File layout follows `storage::layout`: a torrent's data lives under
//! `<download_dir>/<name>/<file-relative-path>`. For a single-file torrent the
//! relative path is empty, so the file is `<download_dir>/<name>`.

use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{CoreError, Result};
use crate::hash::InfoHash;
use crate::meta::TorrentMeta;
use crate::storage::resume::{FastResume, PieceBitfield};
use crate::storage::{piece_file_ranges, verify_piece};

/// Per-torrent storage handle performing real disk I/O.
#[derive(Clone)]
pub struct StorageIo {
    meta: TorrentMeta,
    download_dir: PathBuf,
}

impl StorageIo {
    pub fn new(meta: TorrentMeta, download_dir: impl Into<PathBuf>) -> Self {
        Self {
            meta,
            download_dir: download_dir.into(),
        }
    }

    /// The torrent name (top-level directory or single-file name).
    pub fn name(&self) -> &str {
        &self.meta.name
    }

    /// Base directory containing this torrent's data.
    pub fn base_dir(&self) -> &Path {
        &self.download_dir
    }

    /// Sum the current on-disk payload file lengths, capped at each torrent
    /// file's expected length. This is a cheap fast-resume sanity check; it
    /// does not prove bytes are valid, only that data exists beyond what a
    /// resume file claims is verified.
    pub async fn payload_bytes_on_disk(&self) -> Result<u64> {
        let mut total = 0u64;
        for (index, file) in self.meta.files.iter().enumerate() {
            let path = self.file_path(index)?;
            match fs::metadata(&path).await {
                Ok(meta) => {
                    total = total.saturating_add(meta.len().min(file.length));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(CoreError::from(e)),
            }
        }
        Ok(total.min(self.meta.total_length))
    }

    /// Resolve the absolute path for a file index.
    pub fn file_path(&self, index: usize) -> Result<PathBuf> {
        let file = self
            .meta
            .files
            .get(index)
            .ok_or_else(|| CoreError::Storage(format!("file index {index} out of range")))?;
        join(&self.download_dir, &file.path)
    }

    /// Ensure all parent directories for a file path exist.
    async fn ensure_file_dirs(&self, index: usize) -> Result<()> {
        let path = self.file_path(index)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(CoreError::from)?;
        }
        Ok(())
    }

    /// Ensure all parent directories for the torrent exist.
    pub async fn ensure_dirs(&self) -> Result<()> {
        if self.meta.is_multi_file {
            fs::create_dir_all(&self.download_dir)
                .await
                .map_err(CoreError::from)?;
        } else if let Some(parent) = self.download_dir.parent() {
            fs::create_dir_all(parent).await.map_err(CoreError::from)?;
        }
        Ok(())
    }

    /// Create the visible on-disk layout for an active torrent without
    /// pre-sizing payload files. This gives operators immediate evidence that
    /// a just-started torrent has claimed its incomplete path while preserving
    /// the `preallocate = false` behavior.
    pub async fn ensure_active_layout(&self) -> Result<()> {
        self.ensure_dirs().await?;
        if self.meta.files.is_empty() {
            return Ok(());
        }
        self.ensure_file_dirs(0).await?;
        if !self.meta.is_multi_file {
            let path = self.file_path(0)?;
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .await
                .map_err(CoreError::from)?;
        }
        Ok(())
    }

    /// Preallocate (truncate to full length) all files so random writes land at
    /// the right offsets. Uses sparse truncation by default.
    pub async fn preallocate(&self) -> Result<()> {
        self.ensure_dirs().await?;
        for (i, _f) in self.meta.files.iter().enumerate() {
            self.ensure_file_dirs(i).await?;
            let path = self.file_path(i)?;
            let file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .await
                .map_err(CoreError::from)?;
            let len = self.meta.files[i].length;
            file.set_len(len).await.map_err(CoreError::from)?;
        }
        Ok(())
    }

    /// Write a block (a sub-range of a piece) to disk at the correct file
    /// offset(s), crossing file boundaries as needed.
    ///
    /// `piece_index` is the piece, `offset` is the byte offset within that
    /// piece, and `block` is the block bytes.
    pub async fn write_block(&self, piece_index: usize, offset: u64, block: &[u8]) -> Result<()> {
        let Some((piece_start, _piece_end)) = self.meta.piece_byte_range(piece_index as u64) else {
            return Err(CoreError::Storage(format!(
                "piece {piece_index} out of range"
            )));
        };
        let abs_start = piece_start + offset;
        let abs_end = abs_start + block.len() as u64;
        self.write_file_slices(abs_start, abs_end, block).await
    }

    /// Write a complete piece to disk with one open/seek/write per touched
    /// file slice.
    ///
    /// This is intended for callers that assemble and verify a piece before
    /// flushing it to storage. The provided data must match the exact piece
    /// length, including the shorter final piece.
    pub async fn write_piece(&self, piece_index: usize, piece: &[u8]) -> Result<()> {
        let (piece_start, piece_end) = self.piece_range(piece_index)?;
        let expected_len = piece_end - piece_start;
        let actual_len = u64::try_from(piece.len())
            .map_err(|_| CoreError::Storage("piece write length exceeds u64".into()))?;
        if actual_len != expected_len {
            return Err(CoreError::Storage(format!(
                "piece {piece_index} write length {actual_len} does not match expected {expected_len}"
            )));
        }
        self.write_piece_range(piece_index, 0, piece).await
    }

    /// Write contiguous data within a piece with one open/seek/write per
    /// touched file slice.
    ///
    /// Unlike [`Self::write_block`], this validates that the requested span
    /// stays inside the piece. It is suitable for large contiguous piece data
    /// that has already passed protocol-level bounds checks.
    pub async fn write_piece_range(
        &self,
        piece_index: usize,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let (abs_start, abs_end) =
            self.checked_piece_write_range(piece_index, offset, data.len())?;
        self.write_file_slices(abs_start, abs_end, data).await
    }

    async fn write_file_slices(&self, abs_start: u64, abs_end: u64, data: &[u8]) -> Result<()> {
        let slices = byte_ranges_to_file_slices(&self.meta, abs_start, abs_end);
        let mut data_off = 0usize;
        for slice in slices {
            self.ensure_file_dirs(slice.file_index).await?;
            let path = self.file_path(slice.file_index)?;
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .await
                .map_err(CoreError::from)?;
            file.seek(std::io::SeekFrom::Start(slice.offset_in_file))
                .await
                .map_err(CoreError::from)?;
            let chunk = &data[data_off..data_off + slice.length as usize];
            file.write_all(chunk).await.map_err(CoreError::from)?;
            file.flush().await.map_err(CoreError::from)?;
            data_off += slice.length as usize;
        }
        Ok(())
    }

    fn piece_range(&self, piece_index: usize) -> Result<(u64, u64)> {
        self.meta
            .piece_byte_range(piece_index as u64)
            .ok_or_else(|| CoreError::Storage(format!("piece {piece_index} out of range")))
    }

    fn checked_piece_write_range(
        &self,
        piece_index: usize,
        offset: u64,
        data_len: usize,
    ) -> Result<(u64, u64)> {
        let (piece_start, piece_end) = self.piece_range(piece_index)?;
        let piece_len = piece_end - piece_start;
        if offset > piece_len {
            return Err(CoreError::Storage(format!(
                "piece {piece_index} write offset {offset} exceeds piece length {piece_len}"
            )));
        }
        let data_len = u64::try_from(data_len)
            .map_err(|_| CoreError::Storage("piece write length exceeds u64".into()))?;
        let end_offset = offset.checked_add(data_len).ok_or_else(|| {
            CoreError::Storage(format!(
                "piece {piece_index} write range overflows piece offset"
            ))
        })?;
        if end_offset > piece_len {
            return Err(CoreError::Storage(format!(
                "piece {piece_index} write end {end_offset} exceeds piece length {piece_len}"
            )));
        }
        let abs_start = piece_start.checked_add(offset).ok_or_else(|| {
            CoreError::Storage(format!(
                "piece {piece_index} absolute write offset overflows"
            ))
        })?;
        let abs_end = piece_start.checked_add(end_offset).ok_or_else(|| {
            CoreError::Storage(format!("piece {piece_index} absolute write end overflows"))
        })?;
        Ok((abs_start, abs_end))
    }

    /// Read a whole piece from disk (used for verification and seeding).
    pub async fn read_piece(&self, piece_index: usize) -> Result<Vec<u8>> {
        let Some((start, end)) = self.meta.piece_byte_range(piece_index as u64) else {
            return Err(CoreError::Storage(format!(
                "piece {piece_index} out of range"
            )));
        };
        let slices = byte_ranges_to_file_slices(&self.meta, start, end);
        let mut out = Vec::with_capacity((end - start) as usize);
        for slice in slices {
            let path = self.file_path(slice.file_index)?;
            let mut file = fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .map_err(CoreError::from)?;
            file.seek(std::io::SeekFrom::Start(slice.offset_in_file))
                .await
                .map_err(CoreError::from)?;
            let mut buf = vec![0u8; slice.length as usize];
            file.read_exact(&mut buf).await.map_err(CoreError::from)?;
            out.extend_from_slice(&buf);
        }
        Ok(out)
    }

    /// Read the bytes for a block request (piece + offset + length) for
    /// serving peers while seeding.
    pub async fn read_block(
        &self,
        piece_index: usize,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let Some((piece_start, _)) = self.meta.piece_byte_range(piece_index as u64) else {
            return Err(CoreError::Storage(format!(
                "piece {piece_index} out of range"
            )));
        };
        let abs_start = piece_start + offset;
        let abs_end = abs_start + length as u64;
        let slices = byte_ranges_to_file_slices(&self.meta, abs_start, abs_end);
        let mut out = Vec::with_capacity(length);
        for slice in slices {
            let path = self.file_path(slice.file_index)?;
            let mut file = fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .map_err(CoreError::from)?;
            file.seek(std::io::SeekFrom::Start(slice.offset_in_file))
                .await
                .map_err(CoreError::from)?;
            let mut buf = vec![0u8; slice.length as usize];
            file.read_exact(&mut buf).await.map_err(CoreError::from)?;
            out.extend_from_slice(&buf);
        }
        Ok(out)
    }

    /// Verify a piece by reading it from disk and comparing its SHA-1 to the
    /// metadata. Returns true if it matches.
    pub async fn verify_piece_on_disk(&self, piece_index: usize) -> Result<bool> {
        if piece_index >= self.meta.pieces.len() {
            return Ok(false);
        }
        // If the file is missing, treat as not-yet-present (not a hard error).
        let data = match self.read_piece(piece_index).await {
            Ok(d) => d,
            Err(CoreError::Io(_)) => return Ok(false),
            Err(e) => return Err(e),
        };
        Ok(verify_piece(&self.meta, piece_index, &data))
    }

    /// Full recheck: verify every piece on disk and return a bitfield of
    /// verified pieces.
    pub async fn recheck(&self) -> Result<PieceBitfield> {
        let mut bf = PieceBitfield::new(self.meta.piece_count());
        for i in 0..self.meta.piece_count() {
            if self.verify_piece_on_disk(i).await? {
                bf.set(i);
            }
        }
        Ok(bf)
    }

    /// Persist fast-resume metadata next to active torrent data.
    pub async fn save_resume(&self, resume: &FastResume) -> Result<PathBuf> {
        let path = self.resume_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(CoreError::from)?;
        }
        let json = resume
            .serialize_json()
            .map_err(|e| CoreError::Storage(format!("resume serialize: {e}")))?;
        fs::write(&path, json.as_bytes())
            .await
            .map_err(CoreError::from)?;
        Ok(path)
    }

    /// Load fast-resume metadata for this torrent, validating the info hash
    /// matches. Returns `None` if no resume file exists.
    pub async fn load_resume(&self, expected_hash: &InfoHash) -> Result<Option<FastResume>> {
        let path = self.resume_path();
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path).await.map_err(CoreError::from)?;
        let s = std::str::from_utf8(&bytes)
            .map_err(|e| CoreError::Storage(format!("resume not utf8: {e}")))?;
        let resume = FastResume::parse_json(s)
            .map_err(|e| CoreError::Storage(format!("resume parse: {e}")))?;
        if &resume.info_hash != expected_hash {
            return Err(CoreError::Storage(format!(
                "resume info hash mismatch: expected {}, found {}",
                expected_hash, resume.info_hash
            )));
        }
        if resume.piece_count != self.meta.piece_count() {
            return Err(CoreError::Storage(format!(
                "resume piece_count {} != meta {}",
                resume.piece_count,
                self.meta.piece_count()
            )));
        }
        Ok(Some(resume))
    }

    /// Path of the fast-resume file for this torrent.
    pub fn resume_path(&self) -> PathBuf {
        self.download_dir
            .join(format!("{}.swarmotter.resume", self.meta.name))
    }

    /// Remove fast-resume metadata for this torrent, if present.
    pub async fn remove_resume(&self) -> Result<()> {
        let _ = fs::remove_file(self.resume_path()).await;
        Ok(())
    }

    /// Move verified torrent data from this storage root to another root,
    /// preserving torrent-relative paths. The destination must not already
    /// contain the torrent's files; refusing to overwrite avoids clobbering
    /// user data when a path is misconfigured.
    pub async fn move_to(&self, destination_dir: impl Into<PathBuf>) -> Result<Self> {
        let destination = Self::new(self.meta.clone(), destination_dir);
        if self.download_dir == destination.download_dir {
            return Ok(destination);
        }

        for (index, file) in self.meta.files.iter().enumerate() {
            let src = self.file_path(index)?;
            let dst = destination.file_path(index)?;
            if src == dst {
                continue;
            }
            if path_exists(&dst).await? {
                return Err(CoreError::Storage(format!(
                    "destination file already exists while moving completed data: {}",
                    dst.display()
                )));
            }
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent).await.map_err(CoreError::from)?;
            }
            if path_exists(&src).await? {
                rename_or_copy(&src, &dst).await?;
                cleanup_empty_parents(src.parent(), &self.download_dir).await;
            } else if file.length == 0 {
                fs::File::create(&dst).await.map_err(CoreError::from)?;
            } else {
                return Err(CoreError::Storage(format!(
                    "source file missing while moving completed data: {}",
                    src.display()
                )));
            }
        }

        self.remove_resume().await?;
        Ok(destination)
    }

    /// Remove all torrent data files and the resume file.
    pub async fn remove_all(&self) -> Result<()> {
        for i in 0..self.meta.files.len() {
            let p = self.file_path(i)?;
            let _ = fs::remove_file(&p).await;
            cleanup_empty_parents(p.parent(), &self.download_dir).await;
        }
        self.remove_resume().await?;
        // For multi-file, remove the now-empty top-level directory.
        if self.meta.is_multi_file {
            let _ = fs::remove_dir(&self.download_dir).await;
        }
        Ok(())
    }
}

/// A file slice within a piece, with absolute file index and offset.
#[derive(Debug, Clone, Copy)]
struct FileSliceRange {
    file_index: usize,
    offset_in_file: u64,
    length: u64,
}

/// Map an absolute byte range `[start, end)` within the torrent to the file
/// slices it covers. This is the storage equivalent of
/// [`crate::storage::piece_file_ranges`] generalized to arbitrary ranges.
fn byte_ranges_to_file_slices(meta: &TorrentMeta, start: u64, end: u64) -> Vec<FileSliceRange> {
    let mut out = Vec::new();
    let mut offset = 0u64;
    for (i, file) in meta.files.iter().enumerate() {
        let file_end = offset + file.length;
        if file_end > start && offset < end {
            let slice_start = offset.max(start) - offset;
            let slice_end = file_end.min(end) - offset;
            out.push(FileSliceRange {
                file_index: i,
                offset_in_file: slice_start,
                length: slice_end - slice_start,
            });
        }
        offset = file_end;
    }
    out
}

/// Join a base directory with a file's path components, guarding against path
/// traversal.
fn join(base: &Path, path_components: &[String]) -> Result<PathBuf> {
    for seg in path_components {
        validate_path_component(seg)?;
    }
    let mut p = PathBuf::from(base);
    for seg in path_components {
        p.push(seg);
    }
    Ok(p)
}

fn validate_path_component(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(CoreError::Storage("empty path component".into()));
    }
    if value == "." || value == ".." {
        return Err(CoreError::Storage(format!(
            "path component {value:?} is relative traversal"
        )));
    }
    if value.starts_with('/') || value.starts_with('\\') {
        return Err(CoreError::Storage(format!(
            "path component {value:?} is absolute"
        )));
    }
    if value.contains('/') || value.contains('\\') || value.contains(':') {
        return Err(CoreError::Storage(format!(
            "path component {value:?} contains forbidden path characters"
        )));
    }
    Ok(())
}

async fn path_exists(path: &Path) -> Result<bool> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(CoreError::from(e)),
    }
}

async fn rename_or_copy(src: &Path, dst: &Path) -> Result<()> {
    match fs::rename(src, dst).await {
        Ok(()) => Ok(()),
        Err(rename_err) => match fs::copy(src, dst).await {
            Ok(_) => {
                fs::remove_file(src).await.map_err(CoreError::from)?;
                Ok(())
            }
            Err(copy_err) => Err(CoreError::Storage(format!(
                "failed to move {} to {}: rename failed ({rename_err}); copy fallback failed ({copy_err})",
                src.display(),
                dst.display()
            ))),
        },
    }
}

async fn cleanup_empty_parents(parent: Option<&Path>, stop_at: &Path) {
    let Some(mut current) = parent.map(PathBuf::from) else {
        return;
    };
    while current != stop_at {
        if fs::remove_dir(&current).await.is_err() {
            break;
        }
        if !current.pop() {
            break;
        }
    }
}

/// Build a [`FastResume`] from current piece/byte state.
///
/// `piece_byte_lengths` is the length in bytes of each piece (the last piece
/// may be shorter than `piece_length`).
#[allow(clippy::too_many_arguments)]
pub fn build_resume(
    info_hash: InfoHash,
    name: String,
    bitfield: PieceBitfield,
    piece_count: usize,
    downloaded: u64,
    uploaded: u64,
    total_length: u64,
    download_dir: Option<String>,
    date_added: u64,
    date_completed: Option<u64>,
    priorities: &[crate::models::torrent::FilePriority],
    piece_byte_lengths: &[u64],
) -> FastResume {
    let bytes_completed = (0..piece_count)
        .filter(|&i| bitfield.has(i))
        .map(|i| *piece_byte_lengths.get(i).unwrap_or(&0))
        .sum();
    FastResume {
        info_hash,
        name,
        piece_bitfield: bitfield,
        piece_count,
        downloaded,
        uploaded,
        bytes_completed,
        total_length,
        priorities: priorities.to_vec(),
        download_dir,
        date_added,
        date_completed,
    }
}

/// Re-export the piece-to-file mapping for tests.
pub fn piece_file_mapping(meta: &TorrentMeta, piece_index: usize) -> Vec<(usize, u64, u64)> {
    piece_file_ranges(meta, piece_index)
        .into_iter()
        .map(|s| (s.file_index, s.offset_in_file, s.length))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{
        build_multi_file_torrent, build_single_file_torrent, parse_torrent, MetaFile,
    };

    fn unique_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "swarmotter-storage-{}-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            label
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn piece_to_file_offset_mapping_single() {
        let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let m = piece_file_mapping(&meta, 0);
        assert_eq!(m, vec![(0, 0, 8)]);
        let m1 = piece_file_mapping(&meta, 1);
        assert_eq!(m1, vec![(0, 8, 8)]);
    }

    #[test]
    fn piece_to_file_mapping_multi_file_boundary() {
        // dir/a.txt (5 bytes) + dir/sub/b.bin (7 bytes) = 12 bytes, piece_length 4.
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        // Piece 0: bytes 0..4 -> a.txt [0..4]
        assert_eq!(piece_file_mapping(&meta, 0), vec![(0, 0, 4)]);
        // Piece 1: bytes 4..8 -> a.txt [4..5] (1 byte) + b.bin [0..3] (3 bytes)
        let p1 = piece_file_mapping(&meta, 1);
        assert_eq!(p1, vec![(0, 4, 1), (1, 0, 3)]);
        // Piece 2: bytes 8..12 -> b.bin [3..7]
        assert_eq!(piece_file_mapping(&meta, 2), vec![(1, 3, 4)]);
    }

    #[tokio::test]
    async fn write_and_verify_single_file_piece() {
        let content = b"hello swarmotter world data payload here";
        let bytes = build_single_file_torrent("file.bin", content, 16, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("single-write");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();
        // Write piece 0 bytes.
        let p0 = &content[..16];
        let res = store.verify_piece_on_disk(0).await.unwrap();
        assert!(!res);
        store.write_block(0, 0, p0).await.unwrap();
        store.write_block(1, 0, &content[16..32]).await.unwrap();
        store.write_block(2, 0, &content[32..]).await.unwrap();
        assert!(store.verify_piece_on_disk(0).await.unwrap());
        assert!(store.verify_piece_on_disk(1).await.unwrap());
        assert!(store.verify_piece_on_disk(2).await.unwrap());
        let all = store.read_piece(0).await.unwrap();
        assert_eq!(all, p0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn write_piece_writes_single_file_piece() {
        let content = b"0123456789abcdeflast";
        let bytes = build_single_file_torrent("piece.bin", content, 16, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("single-piece-write");
        let store = StorageIo::new(meta.clone(), dir.clone());

        store.write_piece(0, &content[..16]).await.unwrap();
        store.write_piece(1, &content[16..]).await.unwrap();

        assert_eq!(std::fs::read(store.file_path(0).unwrap()).unwrap(), content);
        assert!(store.verify_piece_on_disk(0).await.unwrap());
        assert!(store.verify_piece_on_disk(1).await.unwrap());
        let err = store.write_piece(0, b"short").await;
        assert!(err.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn active_layout_creates_single_file_placeholder_without_preallocating() {
        let content = b"placeholder appears before first piece";
        let bytes = build_single_file_torrent("visible.bin", content, 16, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("active-visible-single");
        let store = StorageIo::new(meta.clone(), dir.clone());

        store.ensure_active_layout().await.unwrap();

        let path = store.file_path(0).unwrap();
        assert!(path.exists());
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn active_layout_creates_multi_file_top_directory() {
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("visible-dir", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("active-visible-multi");
        let store = StorageIo::new(meta.clone(), dir.clone());

        store.ensure_active_layout().await.unwrap();

        assert!(dir.join("visible-dir").exists());
        assert!(!store.file_path(0).unwrap().exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn multi_file_boundary_write() {
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("multi-write");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();
        // Write every piece. Piece 1 crosses the file boundary
        // (a.txt[4..5] = 'o' + b.bin[0..3] = 'wor').
        store.write_block(0, 0, b"hell").await.unwrap();
        store.write_block(1, 0, b"owor").await.unwrap();
        store.write_block(2, 0, b"ld!!").await.unwrap();
        let a = std::fs::read(dir.join("dir").join("a.txt")).unwrap();
        assert_eq!(&a, b"hello");
        let b = std::fs::read(dir.join("dir").join("sub").join("b.bin")).unwrap();
        assert_eq!(&b, b"world!!");
        // All pieces verify against metadata.
        assert!(store.verify_piece_on_disk(0).await.unwrap());
        assert!(store.verify_piece_on_disk(1).await.unwrap());
        assert!(store.verify_piece_on_disk(2).await.unwrap());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn write_piece_range_preserves_multi_file_boundaries() {
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("multi-piece-range-write");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();

        store.write_piece(0, b"hell").await.unwrap();
        store.write_piece_range(1, 0, b"owor").await.unwrap();
        store.write_piece(2, b"ld!!").await.unwrap();

        let a = std::fs::read(dir.join("dir").join("a.txt")).unwrap();
        assert_eq!(&a, b"hello");
        let b = std::fs::read(dir.join("dir").join("sub").join("b.bin")).unwrap();
        assert_eq!(&b, b"world!!");
        let err = store.write_piece_range(1, 3, b"or").await;
        assert!(err.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn verify_rejects_bad_piece() {
        let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("verify-bad");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, b"XXXXXXXX").await.unwrap();
        assert!(!store.verify_piece_on_disk(0).await.unwrap());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resume_save_load_roundtrip() {
        let content = b"0123456789abcdef0123456789abcdef";
        let bytes = build_single_file_torrent("r.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("resume");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, &content[..8]).await.unwrap();
        store.write_block(1, 0, &content[8..16]).await.unwrap();
        store.write_block(2, 0, &content[16..24]).await.unwrap();
        store.write_block(3, 0, &content[24..]).await.unwrap();
        let mut bf = PieceBitfield::new(4);
        bf.set(0);
        bf.set(1);
        let resume = build_resume(
            meta.info_hash,
            meta.name.clone(),
            bf,
            meta.piece_count(),
            content.len() as u64,
            0,
            meta.total_length,
            Some(dir.display().to_string()),
            1,
            None,
            &[crate::models::torrent::FilePriority::Normal],
            &[8u64; 4],
        );
        store.save_resume(&resume).await.unwrap();
        let loaded = store.load_resume(&meta.info_hash).await.unwrap().unwrap();
        assert_eq!(loaded.info_hash, meta.info_hash);
        assert_eq!(loaded.piece_count, meta.piece_count());
        assert!(loaded.piece_bitfield.has(0));
        assert!(loaded.piece_bitfield.has(1));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resume_rejects_mismatched_info_hash() {
        let bytes = build_single_file_torrent("m.bin", b"01234567", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("resume-mismatch");
        let store = StorageIo::new(meta.clone(), dir.clone());
        let other = InfoHash::from_bytes([0u8; 20]);
        let resume = build_resume(
            other,
            "m.bin".into(),
            PieceBitfield::new(1),
            1,
            0,
            0,
            8,
            None,
            1,
            None,
            &[],
            &[8u64],
        );
        store.save_resume(&resume).await.unwrap();
        let err = store.load_resume(&meta.info_hash).await;
        assert!(err.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn recheck_marks_verified_pieces() {
        let content = b"0123456789abcdef0123456789abcdef";
        let bytes = build_single_file_torrent("rc.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("recheck");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, &content[..8]).await.unwrap();
        store.write_block(1, 0, &content[8..16]).await.unwrap();
        let bf = store.recheck().await.unwrap();
        assert!(bf.has(0));
        assert!(bf.has(1));
        assert!(!bf.has(2));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn missing_file_treated_as_not_verified() {
        let bytes = build_single_file_torrent("miss.bin", b"0123456789abcdef", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("missing");
        let store = StorageIo::new(meta.clone(), dir.clone());
        // Do NOT preallocate: file is absent.
        assert!(!store.verify_piece_on_disk(0).await.unwrap());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn read_block_for_seeding() {
        let content = b"0123456789abcdef";
        let bytes = build_single_file_torrent("seed.bin", content, 16, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("seed");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, content).await.unwrap();
        let block = store.read_block(0, 4, 8).await.unwrap();
        assert_eq!(block, b"456789ab");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn move_to_moves_data_and_removes_active_resume() {
        let content = b"0123456789abcdef";
        let bytes = build_single_file_torrent("move.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let active = unique_dir("move-active");
        let complete = unique_dir("move-complete");
        let store = StorageIo::new(meta.clone(), active.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, &content[..8]).await.unwrap();
        store.write_block(1, 0, &content[8..]).await.unwrap();
        let resume = build_resume(
            meta.info_hash,
            meta.name.clone(),
            PieceBitfield::new(meta.piece_count()),
            meta.piece_count(),
            content.len() as u64,
            0,
            meta.total_length,
            Some(active.display().to_string()),
            1,
            None,
            &[crate::models::torrent::FilePriority::Normal],
            &[8u64; 2],
        );
        store.save_resume(&resume).await.unwrap();

        let complete_store = store.move_to(complete.clone()).await.unwrap();

        assert!(!store.file_path(0).unwrap().exists());
        assert!(!store.resume_path().exists());
        assert_eq!(
            std::fs::read(complete_store.file_path(0).unwrap()).unwrap(),
            content
        );
        assert!(!complete_store.resume_path().exists());
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[tokio::test]
    async fn move_to_preserves_multi_file_layout() {
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        let active = unique_dir("move-multi-active");
        let complete = unique_dir("move-multi-complete");
        let store = StorageIo::new(meta.clone(), active.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, b"hell").await.unwrap();
        store.write_block(1, 0, b"owor").await.unwrap();
        store.write_block(2, 0, b"ld!!").await.unwrap();

        let complete_store = store.move_to(complete.clone()).await.unwrap();

        assert!(!active.join("dir").join("a.txt").exists());
        assert_eq!(
            std::fs::read(complete_store.file_path(0).unwrap()).unwrap(),
            b"hello"
        );
        assert_eq!(
            std::fs::read(complete_store.file_path(1).unwrap()).unwrap(),
            b"world!!"
        );
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[test]
    fn storage_rejects_unsafe_path_components() {
        let meta = TorrentMeta {
            info_hash: InfoHash::from_bytes([1u8; 20]),
            name: "safe-name".into(),
            piece_length: 16,
            pieces: vec![[0u8; 20]],
            files: vec![MetaFile {
                path: vec!["safe-name".into(), "../traversal".into()],
                length: 1,
            }],
            total_length: 1,
            private: false,
            announce: None,
            announce_list: vec![],
            webseeds: vec![],
            comment: None,
            created_by: None,
            creation_date: None,
            is_multi_file: true,
        };
        let store = StorageIo::new(meta, std::env::temp_dir());
        assert!(store.file_path(0).is_err());
    }

    #[test]
    fn storage_rejects_empty_path_components() {
        let meta = TorrentMeta {
            info_hash: InfoHash::from_bytes([2u8; 20]),
            name: "safe".into(),
            piece_length: 16,
            pieces: vec![[0u8; 20]],
            files: vec![MetaFile {
                path: vec!["safe".into(), "".into()],
                length: 1,
            }],
            total_length: 1,
            private: false,
            announce: None,
            announce_list: vec![],
            webseeds: vec![],
            comment: None,
            created_by: None,
            creation_date: None,
            is_multi_file: true,
        };
        let store = StorageIo::new(meta, std::env::temp_dir());
        assert!(store.file_path(0).is_err());
    }
}
