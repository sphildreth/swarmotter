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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::error::{CoreError, Result};
use crate::hash::InfoHash;
use crate::meta::TorrentMeta;
use crate::storage::resume::{FastResume, PieceBitfield, ResumeFileStamp};
use crate::storage::{piece_file_ranges, verify_piece};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Per-torrent storage handle performing real disk I/O.
#[derive(Clone)]
pub struct StorageIo {
    meta: TorrentMeta,
    download_dir: PathBuf,
    file_handles: Arc<Mutex<HashMap<usize, CachedFileHandle>>>,
    resume_write_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct CachedFileHandle {
    file: Arc<Mutex<fs::File>>,
    writable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveEntryKind {
    Existing,
    CreateEmpty,
    Absent,
}

#[derive(Debug, Clone)]
struct MovePlanEntry {
    source: PathBuf,
    destination: PathBuf,
    kind: MoveEntryKind,
}

/// Filesystem paths owned by one torrent at a specific storage root.
///
/// Daemon registration can compare these snapshots before starting work so
/// distinct info hashes never write or delete the same payload/resume paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePathOwnership {
    pub info_hash: InfoHash,
    pub base_dir: PathBuf,
    pub payload_paths: Vec<PathBuf>,
    pub resume_path: PathBuf,
}

impl StoragePathOwnership {
    /// Whether two distinct torrents claim overlapping payload/resume paths.
    ///
    /// Ancestor/descendant claims also conflict: one torrent cannot own a
    /// regular file at a path that another torrent needs as a directory.
    pub fn conflicts_with(&self, other: &Self) -> bool {
        if self.info_hash == other.info_hash {
            return false;
        }
        let mut owned = self
            .payload_paths
            .iter()
            .map(PathBuf::as_path)
            .chain(std::iter::once(self.resume_path.as_path()));
        let other_owned = other
            .payload_paths
            .iter()
            .map(PathBuf::as_path)
            .chain(std::iter::once(other.resume_path.as_path()))
            .collect::<Vec<_>>();
        owned.any(|path| {
            other_owned
                .iter()
                .any(|other_path| paths_overlap(path, other_path))
        })
    }

    pub fn ensure_compatible_with(&self, other: &Self) -> Result<()> {
        if self.conflicts_with(other) {
            return Err(CoreError::Storage(format!(
                "storage path ownership conflict between {} and {} under {}",
                self.info_hash,
                other.info_hash,
                self.base_dir.display()
            )));
        }
        Ok(())
    }
}

impl StorageIo {
    pub fn new(meta: TorrentMeta, download_dir: impl Into<PathBuf>) -> Self {
        Self {
            meta,
            download_dir: download_dir.into(),
            file_handles: Arc::new(Mutex::new(HashMap::new())),
            resume_write_lock: Arc::new(Mutex::new(())),
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

    /// Describe every payload and resume path this torrent owns.
    pub fn path_ownership(&self) -> Result<StoragePathOwnership> {
        validate_path_component(&self.meta.name)?;
        let mut payload_paths: Vec<PathBuf> = Vec::with_capacity(self.meta.files.len());
        for index in 0..self.meta.files.len() {
            let path = normalize_lexical_path(&self.file_path(index)?);
            if let Some(existing) = payload_paths
                .iter()
                .find(|existing| paths_overlap(existing, &path))
            {
                return Err(CoreError::Storage(format!(
                    "overlapping payload paths owned by torrent {}: {} and {}",
                    self.meta.info_hash,
                    existing.display(),
                    path.display()
                )));
            }
            payload_paths.push(path);
        }
        payload_paths.sort();
        let resume_path = normalize_lexical_path(&self.resume_path());
        if let Some(payload_path) = payload_paths
            .iter()
            .find(|payload_path| paths_overlap(payload_path, &resume_path))
        {
            return Err(CoreError::Storage(format!(
                "payload path collides with fast-resume path for torrent {}: {} and {}",
                self.meta.info_hash,
                payload_path.display(),
                resume_path.display()
            )));
        }
        Ok(StoragePathOwnership {
            info_hash: self.meta.info_hash,
            base_dir: normalize_lexical_path(&self.download_dir),
            payload_paths,
            resume_path,
        })
    }

    /// Whether both handles resolve to the same storage root.
    pub fn shares_storage_root_with(&self, other: &Self) -> bool {
        normalize_lexical_path(&self.download_dir) == normalize_lexical_path(&other.download_dir)
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

    /// Capture file metadata used to invalidate fast-resume state after an
    /// external same-size payload modification.
    pub async fn resume_file_stamps(&self) -> Result<Vec<ResumeFileStamp>> {
        self.flush_all_writable_handles().await?;
        let mut stamps = Vec::with_capacity(self.meta.files.len());
        for index in 0..self.meta.files.len() {
            let path = self.file_path(index)?;
            match fs::metadata(path).await {
                Ok(metadata) => {
                    let modified_unix_nanos = metadata
                        .modified()
                        .ok()
                        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64);
                    #[cfg(unix)]
                    let (device, inode, changed_unix_seconds, changed_subsec_nanos) = {
                        use std::os::unix::fs::MetadataExt as _;
                        (
                            Some(metadata.dev()),
                            Some(metadata.ino()),
                            Some(metadata.ctime()),
                            Some(metadata.ctime_nsec()),
                        )
                    };
                    #[cfg(not(unix))]
                    let (device, inode, changed_unix_seconds, changed_subsec_nanos) =
                        (None, None, None, None);
                    stamps.push(ResumeFileStamp {
                        exists: true,
                        length: metadata.len(),
                        modified_unix_nanos,
                        device,
                        inode,
                        changed_unix_seconds,
                        changed_subsec_nanos,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    stamps.push(ResumeFileStamp {
                        exists: false,
                        length: 0,
                        modified_unix_nanos: None,
                        device: None,
                        inode: None,
                        changed_unix_seconds: None,
                        changed_subsec_nanos: None,
                    });
                }
                Err(error) => return Err(CoreError::from(error)),
            }
        }
        Ok(stamps)
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
        self.ensure_active_layout_for_files(&vec![true; self.meta.files.len()])
            .await
    }

    /// Create only the selected portion of the visible active layout. Piece
    /// writes may still create an adjacent unselected file when a selected
    /// piece crosses a file boundary, which is required to verify that piece.
    pub async fn ensure_active_layout_for_files(&self, selected: &[bool]) -> Result<()> {
        self.validate_file_selection(selected)?;
        self.ensure_dirs().await?;
        if self.meta.files.is_empty() {
            return Ok(());
        }
        for (index, wanted) in selected.iter().copied().enumerate() {
            if wanted {
                self.ensure_file_dirs(index).await?;
            }
        }
        if !self.meta.is_multi_file && selected[0] {
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
        self.preallocate_files(&vec![true; self.meta.files.len()])
            .await
    }

    /// Preallocate only selected files. This prevents an unwanted large file
    /// from consuming disk merely because another file in the torrent was
    /// selected.
    pub async fn preallocate_files(&self, selected: &[bool]) -> Result<()> {
        self.validate_file_selection(selected)?;
        self.ensure_dirs().await?;
        for (i, wanted) in selected.iter().copied().enumerate() {
            if !wanted {
                continue;
            }
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

    fn validate_file_selection(&self, selected: &[bool]) -> Result<()> {
        if selected.len() != self.meta.files.len() {
            return Err(CoreError::Storage(format!(
                "file selection length {} does not match torrent file count {}",
                selected.len(),
                self.meta.files.len()
            )));
        }
        Ok(())
    }

    /// Write a block (a sub-range of a piece) to disk at the correct file
    /// offset(s), crossing file boundaries as needed.
    ///
    /// `piece_index` is the piece, `offset` is the byte offset within that
    /// piece, and `block` is the block bytes.
    pub async fn write_block(&self, piece_index: usize, offset: u64, block: &[u8]) -> Result<()> {
        let (abs_start, abs_end) = self.checked_piece_range(piece_index, offset, block.len())?;
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
        let range_len = abs_end.checked_sub(abs_start).ok_or_else(|| {
            CoreError::Storage(format!(
                "invalid storage write range {abs_start}..{abs_end}"
            ))
        })?;
        let data_len = u64::try_from(data.len())
            .map_err(|_| CoreError::Storage("storage write length exceeds u64".into()))?;
        if abs_end > self.meta.total_length || range_len != data_len {
            return Err(CoreError::Storage(format!(
                "storage write range {abs_start}..{abs_end} does not match {data_len} bytes within torrent length {}",
                self.meta.total_length
            )));
        }
        let slices = byte_ranges_to_file_slices(&self.meta, abs_start, abs_end)?;
        let mapped_len = slices.iter().try_fold(0u64, |total, slice| {
            total
                .checked_add(slice.length)
                .ok_or_else(|| CoreError::Storage("mapped storage write length overflow".into()))
        })?;
        if mapped_len != data_len {
            return Err(CoreError::Storage(format!(
                "storage write mapped {mapped_len} of {data_len} bytes"
            )));
        }
        let mut data_off = 0usize;
        for slice in slices {
            self.ensure_file_dirs(slice.file_index).await?;
            let file = self.open_file_handle(slice.file_index, true).await?;
            let mut file = file.lock().await;
            file.seek(std::io::SeekFrom::Start(slice.offset_in_file))
                .await
                .map_err(CoreError::from)?;
            let chunk = &data[data_off..data_off + slice.length as usize];
            file.write_all(chunk).await.map_err(CoreError::from)?;
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
        self.checked_piece_range(piece_index, offset, data_len)
    }

    fn checked_piece_range(
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
        let slices = byte_ranges_to_file_slices(&self.meta, start, end)?;
        self.flush_writable_file_slices(&slices).await?;
        let mut out = Vec::with_capacity((end - start) as usize);
        for slice in slices {
            let file = self.open_file_handle(slice.file_index, false).await?;
            let mut file = file.lock().await;
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
        let (abs_start, abs_end) = self.checked_piece_range(piece_index, offset, length)?;
        let slices = byte_ranges_to_file_slices(&self.meta, abs_start, abs_end)?;
        self.flush_writable_file_slices(&slices).await?;
        let mut out = Vec::with_capacity(length);
        for slice in slices {
            let file = self.open_file_handle(slice.file_index, false).await?;
            let mut file = file.lock().await;
            file.seek(std::io::SeekFrom::Start(slice.offset_in_file))
                .await
                .map_err(CoreError::from)?;
            let mut buf = vec![0u8; slice.length as usize];
            file.read_exact(&mut buf).await.map_err(CoreError::from)?;
            out.extend_from_slice(&buf);
        }
        if out.len() != length {
            return Err(CoreError::Storage(format!(
                "storage read returned {} of {length} requested bytes",
                out.len()
            )));
        }
        Ok(out)
    }

    async fn open_file_handle(
        &self,
        index: usize,
        create_if_missing: bool,
    ) -> Result<Arc<Mutex<fs::File>>> {
        if let Some(handle) = self.file_handles.lock().await.get(&index).cloned() {
            if handle.writable || !create_if_missing {
                return Ok(handle.file);
            }
        }

        let path = self.file_path(index)?;
        let mut options = fs::OpenOptions::new();
        options.read(true).truncate(false);
        if create_if_missing {
            options.write(true).create(true);
        }
        let file = CachedFileHandle {
            file: Arc::new(Mutex::new(
                options.open(&path).await.map_err(CoreError::from)?,
            )),
            writable: create_if_missing,
        };

        let mut handles = self.file_handles.lock().await;
        if create_if_missing {
            handles.insert(index, file.clone());
            return Ok(file.file);
        }
        Ok(handles
            .entry(index)
            .or_insert_with(|| file.clone())
            .file
            .clone())
    }

    async fn flush_writable_file_slices(&self, slices: &[FileSliceRange]) -> Result<()> {
        let handles = {
            let handles = self.file_handles.lock().await;
            slices
                .iter()
                .filter_map(|slice| {
                    handles
                        .get(&slice.file_index)
                        .filter(|handle| handle.writable)
                        .map(|handle| handle.file.clone())
                })
                .collect::<Vec<_>>()
        };
        for file in handles {
            file.lock().await.flush().await.map_err(CoreError::from)?;
        }
        Ok(())
    }

    async fn flush_all_writable_handles(&self) -> Result<()> {
        let handles = {
            let handles = self.file_handles.lock().await;
            handles
                .values()
                .filter(|handle| handle.writable)
                .map(|handle| handle.file.clone())
                .collect::<Vec<_>>()
        };
        for file in handles {
            file.lock().await.flush().await.map_err(CoreError::from)?;
        }
        Ok(())
    }

    async fn clear_file_handles(&self) {
        self.file_handles.lock().await.clear();
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
        let _write_guard = self.resume_write_lock.lock().await;
        let path = self.checked_resume_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(CoreError::from)?;
        }
        let json = resume
            .serialize_json()
            .map_err(|e| CoreError::Storage(format!("resume serialize: {e}")))?;
        let temporary = temporary_sibling_path(&path, "tmp")?;
        let write_result: Result<()> = async {
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .await
                .map_err(CoreError::from)?;
            file.write_all(json.as_bytes())
                .await
                .map_err(CoreError::from)?;
            file.flush().await.map_err(CoreError::from)?;
            file.sync_all().await.map_err(CoreError::from)?;
            drop(file);
            fs::rename(&temporary, &path)
                .await
                .map_err(CoreError::from)?;
            sync_parent_directory(&path).await
        }
        .await;
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary).await;
            return Err(error);
        }
        Ok(path)
    }

    /// Load fast-resume metadata for this torrent, validating the info hash
    /// matches. Returns `None` if no resume file exists.
    pub async fn load_resume(&self, expected_hash: &InfoHash) -> Result<Option<FastResume>> {
        let _write_guard = self.resume_write_lock.lock().await;
        let path = self.checked_resume_path()?;
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(CoreError::from(error)),
        };
        let s = match std::str::from_utf8(&bytes) {
            Ok(s) => s,
            Err(error) => {
                self.quarantine_corrupt_resume(&path, &format!("resume not utf8: {error}"))
                    .await;
                return Ok(None);
            }
        };
        let resume = match FastResume::parse_json(s) {
            Ok(resume) => resume,
            Err(error) => {
                self.quarantine_corrupt_resume(&path, &format!("resume parse: {error}"))
                    .await;
                return Ok(None);
            }
        };
        if &resume.info_hash != expected_hash {
            return Err(CoreError::Storage(format!(
                "resume info hash mismatch: expected {}, found {}",
                expected_hash, resume.info_hash
            )));
        }
        let expected_bitfield_bytes = resume.piece_count.div_ceil(8);
        let piece_count_matches = resume.piece_count == self.meta.piece_count();
        let verified_bytes = piece_count_matches.then(|| {
            (0..self.meta.piece_count())
                .filter(|&index| resume.piece_bitfield.has(index))
                .filter_map(|index| self.meta.piece_byte_range(index as u64))
                .map(|(start, end)| end - start)
                .sum::<u64>()
        });
        let structurally_inconsistent = resume.name != self.meta.name
            || !piece_count_matches
            || resume.total_length != self.meta.total_length
            || resume.piece_bitfield.as_bytes().len() != expected_bitfield_bytes
            || resume.bytes_completed > resume.total_length
            || verified_bytes != Some(resume.bytes_completed)
            || (!resume.wanted.is_empty() && resume.wanted.len() != self.meta.files.len())
            || (!resume.file_stamps.is_empty()
                && resume.file_stamps.len() != self.meta.files.len());
        if structurally_inconsistent {
            self.quarantine_corrupt_resume(&path, "resume fields are structurally inconsistent")
                .await;
            return Ok(None);
        }
        Ok(Some(resume))
    }

    async fn quarantine_corrupt_resume(&self, path: &Path, reason: &str) {
        let quarantine = match temporary_sibling_path(path, "corrupt") {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!(path = %path.display(), error = %error, %reason, "invalid fast resume could not be quarantined");
                return;
            }
        };
        match fs::rename(path, &quarantine).await {
            Ok(()) => {
                let _ = sync_parent_directory(path).await;
                tracing::warn!(path = %path.display(), quarantine = %quarantine.display(), %reason, "invalid fast resume quarantined; storage recheck required");
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), error = %error, %reason, "invalid fast resume ignored; storage recheck required");
            }
        }
    }

    /// Path of the fast-resume file for this torrent.
    pub fn resume_path(&self) -> PathBuf {
        self.download_dir
            .join(format!("{}.swarmotter.resume", self.meta.name))
    }

    fn checked_resume_path(&self) -> Result<PathBuf> {
        validate_path_component(&self.meta.name)?;
        Ok(self.resume_path())
    }

    /// Remove fast-resume metadata for this torrent, if present.
    pub async fn remove_resume(&self) -> Result<()> {
        let _write_guard = self.resume_write_lock.lock().await;
        let path = self.checked_resume_path()?;
        match fs::remove_file(&path).await {
            Ok(()) => sync_parent_directory(&path).await,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CoreError::Storage(format!(
                "failed to remove resume {}: {error}",
                path.display()
            ))),
        }
    }

    /// Move torrent data from this storage root to another root,
    /// preserving torrent-relative paths. The destination must not already
    /// contain the torrent's files; refusing to overwrite avoids clobbering
    /// user data when a path is misconfigured.
    pub async fn move_to(&self, destination_dir: impl Into<PathBuf>) -> Result<Self> {
        let destination = Self::new(self.meta.clone(), destination_dir);
        if self.shares_storage_root_with(&destination) {
            return Ok(destination);
        }
        self.flush_all_writable_handles().await?;
        self.clear_file_handles().await;
        let plan = self.build_move_plan(&destination).await?;
        self.remove_resume().await?;
        execute_move_plan(&plan, &destination.download_dir).await?;
        for entry in &plan {
            if entry.kind == MoveEntryKind::Existing {
                cleanup_empty_parents(entry.source.parent(), &self.download_dir).await;
            }
        }
        Ok(destination)
    }

    async fn build_move_plan(&self, destination: &Self) -> Result<Vec<MovePlanEntry>> {
        self.path_ownership()?;
        destination.path_ownership()?;
        if path_lexists(&destination.checked_resume_path()?).await? {
            return Err(CoreError::Storage(format!(
                "destination resume file already exists while moving torrent data: {}",
                destination.resume_path().display()
            )));
        }

        let mut plan = Vec::with_capacity(self.meta.files.len());
        for (index, expected) in self.meta.files.iter().enumerate() {
            let source = self.file_path(index)?;
            let target = destination.file_path(index)?;
            if normalize_lexical_path(&source) == normalize_lexical_path(&target) {
                continue;
            }
            if path_lexists(&target).await? {
                return Err(CoreError::Storage(format!(
                    "destination file already exists while moving torrent data: {}",
                    target.display()
                )));
            }
            let kind = match fs::symlink_metadata(&source).await {
                Ok(metadata) if metadata.is_file() && metadata.len() <= expected.length => {
                    MoveEntryKind::Existing
                }
                Ok(metadata) if !metadata.is_file() => {
                    return Err(CoreError::Storage(format!(
                        "source payload path is not a file: {}",
                        source.display()
                    )));
                }
                Ok(metadata) => {
                    return Err(CoreError::Storage(format!(
                        "source file length {} exceeds expected {} while moving torrent data: {}",
                        metadata.len(),
                        expected.length,
                        source.display()
                    )));
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound && expected.length == 0 =>
                {
                    MoveEntryKind::CreateEmpty
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => MoveEntryKind::Absent,
                Err(error) => return Err(CoreError::from(error)),
            };
            plan.push(MovePlanEntry {
                source,
                destination: target,
                kind,
            });
        }
        Ok(plan)
    }

    /// Remove all torrent data files and the resume file.
    pub async fn remove_all(&self) -> Result<()> {
        self.flush_all_writable_handles().await?;
        self.clear_file_handles().await;
        let mut failures = Vec::new();
        for i in 0..self.meta.files.len() {
            let p = self.file_path(i)?;
            match fs::remove_file(&p).await {
                Ok(()) => cleanup_empty_parents(p.parent(), &self.download_dir).await,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    cleanup_empty_parents(p.parent(), &self.download_dir).await;
                }
                Err(error) => failures.push(format!("{}: {error}", p.display())),
            }
        }
        if let Err(error) = self.remove_resume().await {
            failures.push(error.to_string());
        }
        if !failures.is_empty() {
            return Err(CoreError::Storage(format!(
                "failed to remove all torrent data: {}",
                failures.join("; ")
            )));
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
fn byte_ranges_to_file_slices(
    meta: &TorrentMeta,
    start: u64,
    end: u64,
) -> Result<Vec<FileSliceRange>> {
    if start > end || end > meta.total_length {
        return Err(CoreError::Storage(format!(
            "byte range {start}..{end} is outside torrent length {}",
            meta.total_length
        )));
    }
    let mut out = Vec::new();
    let mut offset = 0u64;
    for (i, file) in meta.files.iter().enumerate() {
        let file_end = offset.checked_add(file.length).ok_or_else(|| {
            CoreError::Storage(format!("file layout overflows at file index {i}"))
        })?;
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
    Ok(out)
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

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    use std::ffi::OsStr;
    use std::path::Component;

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let can_pop = normalized
                    .file_name()
                    .is_some_and(|name| name != OsStr::new(".."));
                if can_pop {
                    normalized.pop();
                } else if !normalized.has_root() {
                    normalized.push("..");
                }
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(value) => normalized.push(value),
        }
    }
    normalized
}

fn temporary_sibling_path(path: &Path, kind: &str) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| CoreError::Storage(format!("path has no file name: {}", path.display())))?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut temporary_name = file_name.to_os_string();
    temporary_name.push(format!(".{kind}-{}-{sequence}", std::process::id()));
    Ok(path.with_file_name(temporary_name))
}

#[cfg(unix)]
async fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let directory = fs::File::open(parent).await.map_err(CoreError::from)?;
    directory.sync_all().await.map_err(CoreError::from)
}

#[cfg(not(unix))]
async fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

async fn path_lexists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(CoreError::from(e)),
    }
}

async fn execute_move_plan(plan: &[MovePlanEntry], target_root: &Path) -> Result<()> {
    let mut completed = Vec::with_capacity(plan.len());
    for entry in plan {
        if entry.kind == MoveEntryKind::Absent {
            for (path, role) in [
                (&entry.source, "source"),
                (&entry.destination, "destination"),
            ] {
                if let Err(error) = ensure_move_path_absent(path, role).await {
                    return rollback_move_plan(&completed, target_root, error).await;
                }
            }
            continue;
        }
        if entry.kind == MoveEntryKind::CreateEmpty {
            if let Err(error) = ensure_move_path_absent(&entry.source, "source").await {
                return rollback_move_plan(&completed, target_root, error).await;
            }
        }
        if let Some(parent) = entry.destination.parent() {
            if let Err(error) = fs::create_dir_all(parent).await {
                return rollback_move_plan(&completed, target_root, CoreError::from(error)).await;
            }
        }
        let result = match entry.kind {
            MoveEntryKind::Existing => move_file_exclusive(&entry.source, &entry.destination).await,
            MoveEntryKind::CreateEmpty => create_empty_file_exclusive(&entry.destination).await,
            MoveEntryKind::Absent => unreachable!("absent entries are handled before mutation"),
        };
        if let Err(error) = result {
            return rollback_move_plan(&completed, target_root, error).await;
        }
        completed.push(entry.clone());
        if let Err(error) = sync_move_entry_parents(entry).await {
            return rollback_move_plan(&completed, target_root, error).await;
        }
    }
    Ok(())
}

async fn ensure_move_path_absent(path: &Path, role: &str) -> Result<()> {
    if path_lexists(path).await? {
        return Err(CoreError::Storage(format!(
            "{role} file appeared after move preflight: {}",
            path.display()
        )));
    }
    Ok(())
}

async fn rollback_move_plan(
    completed: &[MovePlanEntry],
    target_root: &Path,
    original: CoreError,
) -> Result<()> {
    let mut rollback_failures = Vec::new();
    for entry in completed.iter().rev() {
        let result = match entry.kind {
            MoveEntryKind::Existing => move_file_exclusive(&entry.destination, &entry.source).await,
            MoveEntryKind::CreateEmpty => match fs::remove_file(&entry.destination).await {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(CoreError::from(error)),
            },
            MoveEntryKind::Absent => Ok(()),
        };
        let result = match result {
            Ok(()) => sync_move_entry_parents(entry).await,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            rollback_failures.push(format!(
                "{} -> {}: {error}",
                entry.destination.display(),
                entry.source.display()
            ));
        }
        cleanup_empty_parents(entry.destination.parent(), target_root).await;
    }
    if rollback_failures.is_empty() {
        Err(original)
    } else {
        Err(CoreError::Storage(format!(
            "{original}; move rollback also failed: {}",
            rollback_failures.join("; ")
        )))
    }
}

async fn move_file_exclusive(source: &Path, destination: &Path) -> Result<()> {
    if path_lexists(destination).await? {
        return Err(CoreError::Storage(format!(
            "destination file already exists: {}",
            destination.display()
        )));
    }

    match fs::hard_link(source, destination).await {
        Ok(()) => {
            if let Err(error) = fs::remove_file(source).await {
                let cleanup = fs::remove_file(destination).await;
                return Err(CoreError::Storage(format!(
                    "failed to remove source {} after linking to {}: {error}{}",
                    source.display(),
                    destination.display(),
                    cleanup
                        .err()
                        .map(|cleanup| format!("; destination cleanup failed: {cleanup}"))
                        .unwrap_or_default()
                )));
            }
            Ok(())
        }
        Err(link_error) => copy_file_exclusive(source, destination, &link_error).await,
    }
}

async fn copy_file_exclusive(
    source_path: &Path,
    destination_path: &Path,
    link_error: &std::io::Error,
) -> Result<()> {
    let mut source = fs::File::open(source_path).await.map_err(CoreError::from)?;
    let source_metadata = source.metadata().await.map_err(CoreError::from)?;
    let mut destination = match fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination_path)
        .await
    {
        Ok(file) => file,
        Err(error) => {
            return Err(CoreError::Storage(format!(
                "failed to move {} to {}: link failed ({link_error}); exclusive copy create failed ({error})",
                source_path.display(),
                destination_path.display()
            )));
        }
    };

    let copy_result: Result<()> = async {
        let copied = tokio::io::copy(&mut source, &mut destination)
            .await
            .map_err(CoreError::from)?;
        if copied != source_metadata.len() {
            return Err(CoreError::Storage(format!(
                "copied {copied} of {} bytes from {}",
                source_metadata.len(),
                source_path.display()
            )));
        }
        destination.flush().await.map_err(CoreError::from)?;
        fs::set_permissions(destination_path, source_metadata.permissions())
            .await
            .map_err(CoreError::from)?;
        destination.sync_all().await.map_err(CoreError::from)?;
        Ok(())
    }
    .await;
    drop(destination);
    if let Err(error) = copy_result {
        let _ = fs::remove_file(destination_path).await;
        return Err(error);
    }
    if let Err(error) = fs::remove_file(source_path).await {
        let cleanup = fs::remove_file(destination_path).await;
        return Err(CoreError::Storage(format!(
            "copied {} to {} but failed to remove source: {error}{}",
            source_path.display(),
            destination_path.display(),
            cleanup
                .err()
                .map(|cleanup| format!("; destination cleanup failed: {cleanup}"))
                .unwrap_or_default()
        )));
    }
    Ok(())
}

async fn create_empty_file_exclusive(path: &Path) -> Result<()> {
    let file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await
        .map_err(CoreError::from)?;
    file.sync_all().await.map_err(CoreError::from)
}

async fn sync_move_entry_parents(entry: &MovePlanEntry) -> Result<()> {
    match entry.kind {
        MoveEntryKind::Existing => {
            sync_parent_directory(&entry.source).await?;
            if entry.source.parent() != entry.destination.parent() {
                sync_parent_directory(&entry.destination).await?;
            }
        }
        MoveEntryKind::CreateEmpty => {
            sync_parent_directory(&entry.destination).await?;
        }
        MoveEntryKind::Absent => {}
    }
    Ok(())
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
    let wanted = priorities
        .iter()
        .map(|priority| *priority != crate::models::torrent::FilePriority::Unwanted)
        .collect::<Vec<_>>();
    build_resume_with_wanted(
        info_hash,
        name,
        bitfield,
        piece_count,
        downloaded,
        uploaded,
        total_length,
        download_dir,
        date_added,
        date_completed,
        priorities,
        &wanted,
        piece_byte_lengths,
    )
}

/// Build a [`FastResume`] while preserving explicit wanted state separately
/// from scheduling priority.
#[allow(clippy::too_many_arguments)]
pub fn build_resume_with_wanted(
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
    wanted: &[bool],
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
        wanted: wanted.to_vec(),
        file_stamps: Vec::new(),
        download_dir,
        date_added,
        date_completed,
    }
}

/// Re-export the piece-to-file mapping for tests.
pub fn piece_file_mapping(
    meta: &TorrentMeta,
    piece_index: usize,
) -> Result<Vec<(usize, u64, u64)>> {
    Ok(piece_file_ranges(meta, piece_index)?
        .into_iter()
        .map(|s| (s.file_index, s.offset_in_file, s.length))
        .collect())
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
        let m = piece_file_mapping(&meta, 0).unwrap();
        assert_eq!(m, vec![(0, 0, 8)]);
        let m1 = piece_file_mapping(&meta, 1).unwrap();
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
        assert_eq!(piece_file_mapping(&meta, 0).unwrap(), vec![(0, 0, 4)]);
        // Piece 1: bytes 4..8 -> a.txt [4..5] (1 byte) + b.bin [0..3] (3 bytes)
        let p1 = piece_file_mapping(&meta, 1).unwrap();
        assert_eq!(p1, vec![(0, 4, 1), (1, 0, 3)]);
        // Piece 2: bytes 8..12 -> b.bin [3..7]
        assert_eq!(piece_file_mapping(&meta, 2).unwrap(), vec![(1, 3, 4)]);
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

    #[cfg(unix)]
    #[tokio::test]
    async fn resume_stamps_detect_same_size_edits_with_restored_mtime() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        use std::os::unix::fs::MetadataExt as _;

        let original = b"abcdefgh";
        let replacement = b"ABCDEFGH";
        let bytes = build_single_file_torrent("stamp.bin", original, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("resume-stamp-ctime");
        let store = StorageIo::new(meta, dir.clone());
        let path = store.file_path(0).unwrap();
        tokio::fs::write(&path, original).await.unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        let before = store.resume_file_stamps().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        tokio::fs::write(&path, replacement).await.unwrap();
        let times = [
            libc::timespec {
                tv_sec: metadata.atime(),
                tv_nsec: metadata.atime_nsec(),
            },
            libc::timespec {
                tv_sec: metadata.mtime(),
                tv_nsec: metadata.mtime_nsec(),
            },
        ];
        let path_c = CString::new(path.as_os_str().as_bytes()).unwrap();
        let result = unsafe { libc::utimensat(libc::AT_FDCWD, path_c.as_ptr(), times.as_ptr(), 0) };
        assert_eq!(result, 0);

        let after = store.resume_file_stamps().await.unwrap();
        assert_eq!(before[0].modified_unix_nanos, after[0].modified_unix_nanos);
        assert_ne!(before, after);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn storage_reuses_file_handles_for_repeated_block_io() {
        let content = b"0123456789abcdef";
        let bytes = build_single_file_torrent("reuse.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("handle-reuse");
        let store = StorageIo::new(meta.clone(), dir.clone());

        store.write_block(0, 0, &content[..8]).await.unwrap();
        let first_handle = store
            .file_handles
            .lock()
            .await
            .get(&0)
            .unwrap()
            .file
            .clone();
        assert_eq!(store.file_handles.lock().await.len(), 1);

        let clone = store.clone();
        clone.write_block(1, 0, &content[8..]).await.unwrap();
        let second_handle = clone
            .file_handles
            .lock()
            .await
            .get(&0)
            .unwrap()
            .file
            .clone();
        assert!(Arc::ptr_eq(&first_handle, &second_handle));
        assert_eq!(clone.read_block(0, 0, 8).await.unwrap(), &content[..8]);
        assert_eq!(clone.file_handles.lock().await.len(), 1);

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

        assert_eq!(store.read_piece(0).await.unwrap(), &content[..16]);
        assert_eq!(store.read_piece(1).await.unwrap(), &content[16..]);
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
        // All pieces verify against metadata, flushing pending cached writes
        // before the raw filesystem assertions below inspect the files.
        assert!(store.verify_piece_on_disk(0).await.unwrap());
        assert!(store.verify_piece_on_disk(1).await.unwrap());
        assert!(store.verify_piece_on_disk(2).await.unwrap());
        let a = std::fs::read(dir.join("dir").join("a.txt")).unwrap();
        assert_eq!(&a, b"hello");
        let b = std::fs::read(dir.join("dir").join("sub").join("b.bin")).unwrap();
        assert_eq!(&b, b"world!!");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn remove_all_preserves_configured_base_directory() {
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("remove-all-preserves-base");
        let store = StorageIo::new(meta.clone(), dir.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, b"hell").await.unwrap();
        store.write_block(1, 0, b"owor").await.unwrap();
        store.write_block(2, 0, b"ld!!").await.unwrap();

        store.remove_all().await.unwrap();

        assert!(
            dir.exists(),
            "remove_all must preserve the configured storage base directory"
        );
        assert!(
            !dir.join("dir").exists(),
            "remove_all should remove the torrent payload root when empty"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn remove_all_reports_payload_deletion_failures() {
        let bytes = build_single_file_torrent("blocked.bin", b"01234567", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("remove-all-error");
        let store = StorageIo::new(meta, dir.clone());
        let payload = store.file_path(0).unwrap();
        fs::create_dir_all(&payload).await.unwrap();

        let error = store.remove_all().await.unwrap_err();

        assert!(error
            .to_string()
            .contains("failed to remove all torrent data"));
        assert!(error.to_string().contains(&payload.display().to_string()));
        assert!(payload.is_dir());
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

        assert_eq!(store.read_piece(0).await.unwrap(), b"hell");
        assert_eq!(store.read_piece(1).await.unwrap(), b"owor");
        assert_eq!(store.read_piece(2).await.unwrap(), b"ld!!");
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
        let resume = build_resume_with_wanted(
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
            &[false],
            &[8u64; 4],
        );
        store.save_resume(&resume).await.unwrap();
        let loaded = store.load_resume(&meta.info_hash).await.unwrap().unwrap();
        assert_eq!(loaded.info_hash, meta.info_hash);
        assert_eq!(loaded.piece_count, meta.piece_count());
        assert!(loaded.piece_bitfield.has(0));
        assert!(loaded.piece_bitfield.has(1));
        assert_eq!(loaded.wanted, vec![false]);
        assert!(!std::fs::read_dir(&dir).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp-")
        }));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn corrupt_resume_is_quarantined_for_safe_recheck() {
        let bytes = build_single_file_torrent("corrupt.bin", b"01234567", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("resume-corrupt");
        let store = StorageIo::new(meta.clone(), dir.clone());
        fs::write(store.resume_path(), b"{not valid json")
            .await
            .unwrap();

        assert!(store.load_resume(&meta.info_hash).await.unwrap().is_none());
        assert!(!store.resume_path().exists());
        let prefix = "corrupt.bin.swarmotter.resume.corrupt-";
        assert!(std::fs::read_dir(&dir).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(prefix)
        }));
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
    async fn block_io_rejects_ranges_outside_the_piece() {
        let content = b"0123456789abcdeflast";
        let bytes = build_single_file_torrent("range.bin", content, 16, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("block-range");
        let store = StorageIo::new(meta, dir.clone());
        store.preallocate().await.unwrap();

        assert!(store.write_block(0, 15, b"xx").await.is_err());
        assert!(store.write_block(1, 4, b"x").await.is_err());
        assert!(store.write_block(2, 0, b"x").await.is_err());
        assert!(store.read_block(0, 15, 2).await.is_err());
        assert!(store.read_block(1, 4, 1).await.is_err());
        assert!(store.checked_piece_range(0, 1, usize::MAX).is_err());
        assert_eq!(store.read_block(1, 4, 0).await.unwrap(), Vec::<u8>::new());
        assert_eq!(store.read_piece(0).await.unwrap(), vec![0u8; 16]);
        assert_eq!(store.read_piece(1).await.unwrap(), vec![0u8; 4]);
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

    #[tokio::test]
    async fn move_to_preserves_short_sparse_files_and_absent_files() {
        let files = vec![
            (vec!["partial.bin".into()], 8u64),
            (vec!["sub".into(), "unwanted.bin".into()], 8u64),
        ];
        let contents: Vec<&[u8]> = vec![b"partial!", b"unwanted"];
        let bytes = build_multi_file_torrent("sparse", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        let active = unique_dir("move-sparse-active");
        let complete = unique_dir("move-sparse-complete");
        let store = StorageIo::new(meta.clone(), active.clone());
        let partial_path = store.file_path(0).unwrap();
        fs::create_dir_all(partial_path.parent().unwrap())
            .await
            .unwrap();
        fs::write(&partial_path, b"par").await.unwrap();
        assert!(!store.file_path(1).unwrap().exists());

        let moved = store.move_to(complete.clone()).await.unwrap();

        assert!(!partial_path.exists());
        assert_eq!(std::fs::read(moved.file_path(0).unwrap()).unwrap(), b"par");
        assert_eq!(
            std::fs::metadata(moved.file_path(0).unwrap())
                .unwrap()
                .len(),
            3
        );
        assert!(!moved.file_path(1).unwrap().exists());
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[tokio::test]
    async fn move_to_allows_an_entirely_absent_payload_without_claiming_destination_files() {
        let bytes = build_single_file_torrent("not-started.bin", b"not started", 4, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let active = unique_dir("move-absent-active");
        let complete = unique_dir("move-absent-complete");
        let store = StorageIo::new(meta, active.clone());

        let moved = store.move_to(complete.clone()).await.unwrap();

        assert!(!store.file_path(0).unwrap().exists());
        assert!(!moved.file_path(0).unwrap().exists());
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[tokio::test]
    async fn move_to_rejects_destination_collision_for_an_absent_source() {
        let bytes = build_single_file_torrent("absent.bin", b"expected bytes", 4, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let active = unique_dir("move-absent-collision-active");
        let complete = unique_dir("move-absent-collision-complete");
        let store = StorageIo::new(meta.clone(), active.clone());
        let destination = StorageIo::new(meta, complete.clone());
        fs::write(destination.file_path(0).unwrap(), b"existing")
            .await
            .unwrap();

        assert!(store.move_to(complete.clone()).await.is_err());
        assert_eq!(
            std::fs::read(destination.file_path(0).unwrap()).unwrap(),
            b"existing"
        );
        assert!(!store.file_path(0).unwrap().exists());
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[tokio::test]
    async fn move_to_preflights_every_destination_before_mutating_sources() {
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
        let meta = parse_torrent(&bytes).unwrap();
        let active = unique_dir("move-collision-active");
        let complete = unique_dir("move-collision-complete");
        let store = StorageIo::new(meta.clone(), active.clone());
        store.preallocate().await.unwrap();
        store.write_block(0, 0, b"hell").await.unwrap();
        store.write_block(1, 0, b"owor").await.unwrap();
        store.write_block(2, 0, b"ld!!").await.unwrap();
        let resume = build_resume(
            meta.info_hash,
            meta.name.clone(),
            PieceBitfield::new(meta.piece_count()),
            meta.piece_count(),
            0,
            0,
            meta.total_length,
            Some(active.display().to_string()),
            1,
            None,
            &[crate::models::torrent::FilePriority::Normal; 2],
            &[4u64; 3],
        );
        store.save_resume(&resume).await.unwrap();

        let destination = StorageIo::new(meta, complete.clone());
        let collision = destination.file_path(1).unwrap();
        fs::create_dir_all(collision.parent().unwrap())
            .await
            .unwrap();
        fs::write(&collision, b"occupied").await.unwrap();

        assert!(store.move_to(complete.clone()).await.is_err());
        assert_eq!(
            std::fs::read(store.file_path(0).unwrap()).unwrap(),
            b"hello"
        );
        assert_eq!(
            std::fs::read(store.file_path(1).unwrap()).unwrap(),
            b"world!!"
        );
        assert!(!destination.file_path(0).unwrap().exists());
        assert_eq!(std::fs::read(collision).unwrap(), b"occupied");
        assert!(store.resume_path().exists());
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[tokio::test]
    async fn move_plan_rolls_back_completed_entries_after_later_failure() {
        let active = unique_dir("move-rollback-active");
        let complete = unique_dir("move-rollback-complete");
        let source_one = active.join("one.bin");
        let source_two = active.join("two.bin");
        let destination_one = complete.join("one.bin");
        let blocker = complete.join("blocker");
        let destination_two = blocker.join("two.bin");
        fs::write(&source_one, b"one").await.unwrap();
        fs::write(&source_two, b"two").await.unwrap();
        fs::write(&blocker, b"not a directory").await.unwrap();
        let plan = vec![
            MovePlanEntry {
                source: source_one.clone(),
                destination: destination_one.clone(),
                kind: MoveEntryKind::Existing,
            },
            MovePlanEntry {
                source: source_two.clone(),
                destination: destination_two.clone(),
                kind: MoveEntryKind::Existing,
            },
        ];

        assert!(execute_move_plan(&plan, &complete).await.is_err());
        assert_eq!(std::fs::read(source_one).unwrap(), b"one");
        assert_eq!(std::fs::read(source_two).unwrap(), b"two");
        assert!(!destination_one.exists());
        assert_eq!(std::fs::read(blocker).unwrap(), b"not a directory");
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[tokio::test]
    async fn move_plan_rolls_back_when_an_absent_source_appears() {
        let active = unique_dir("move-absent-race-active");
        let complete = unique_dir("move-absent-race-complete");
        let source_one = active.join("one.bin");
        let appeared_source = active.join("appeared.bin");
        let destination_one = complete.join("one.bin");
        let absent_destination = complete.join("appeared.bin");
        fs::write(&source_one, b"one").await.unwrap();
        fs::write(&appeared_source, b"appeared after preflight")
            .await
            .unwrap();
        let plan = vec![
            MovePlanEntry {
                source: source_one.clone(),
                destination: destination_one.clone(),
                kind: MoveEntryKind::Existing,
            },
            MovePlanEntry {
                source: appeared_source.clone(),
                destination: absent_destination.clone(),
                kind: MoveEntryKind::Absent,
            },
        ];

        assert!(execute_move_plan(&plan, &complete).await.is_err());
        assert_eq!(std::fs::read(source_one).unwrap(), b"one");
        assert_eq!(
            std::fs::read(appeared_source).unwrap(),
            b"appeared after preflight"
        );
        assert!(!destination_one.exists());
        assert!(!absent_destination.exists());
        std::fs::remove_dir_all(&active).ok();
        std::fs::remove_dir_all(&complete).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn move_parent_sync_errors_are_returned() {
        let root = unique_dir("move-sync-error");
        let entry = MovePlanEntry {
            source: root.join("missing-parent").join("source.bin"),
            destination: root.join("destination.bin"),
            kind: MoveEntryKind::Existing,
        };

        assert!(sync_move_entry_parents(&entry).await.is_err());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn path_ownership_detects_collisions_and_normalizes_roots() {
        let bytes = build_single_file_torrent("same.bin", b"01234567", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let root = unique_dir("path-ownership");
        let first = StorageIo::new(meta.clone(), root.clone());
        let mut other_meta = meta.clone();
        other_meta.info_hash = InfoHash::from_bytes([9u8; 20]);
        let second = StorageIo::new(other_meta, root.join("child").join(".."));

        assert!(first.shares_storage_root_with(&second));
        let first_ownership = first.path_ownership().unwrap();
        let second_ownership = second.path_ownership().unwrap();
        assert!(first_ownership.conflicts_with(&second_ownership));
        assert!(first_ownership
            .ensure_compatible_with(&second_ownership)
            .is_err());

        let elsewhere = StorageIo::new(meta, root.join("elsewhere"));
        assert!(!first_ownership.conflicts_with(&elsewhere.path_ownership().unwrap()));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn path_ownership_rejects_file_directory_prefix_collisions() {
        assert!(paths_overlap(
            Path::new("/data/file"),
            Path::new("/data/file/child")
        ));
        let meta = TorrentMeta {
            info_hash: InfoHash::from_bytes([8u8; 20]),
            name: "root".into(),
            piece_length: 16,
            pieces: vec![[0u8; 20]],
            files: vec![
                MetaFile {
                    path: vec!["root".into(), "file".into()],
                    length: 1,
                },
                MetaFile {
                    path: vec!["root".into(), "file".into(), "child".into()],
                    length: 1,
                },
            ],
            total_length: 2,
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
        assert!(store.path_ownership().is_err());
    }

    #[test]
    fn path_ownership_rejects_payload_collision_with_its_resume_file() {
        let bytes = build_single_file_torrent("payload.bin", b"01234567", 8, None, false);
        let mut meta = parse_torrent(&bytes).unwrap();
        meta.files[0].path = vec!["payload.bin.swarmotter.resume".into()];
        let store = StorageIo::new(meta, std::env::temp_dir());

        let error = store.path_ownership().unwrap_err();

        assert!(error
            .to_string()
            .contains("payload path collides with fast-resume path"));
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
