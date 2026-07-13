// SPDX-License-Identifier: Apache-2.0

//! Watch-folder import logic.
//!
//! Scans configured folders for `.torrent` files, waits for file writes to
//! stabilize, imports them (with duplicate detection by info hash), and moves
//! successfully imported files to an archive folder, failure folder, leaves
//! them in place, or deletes them per configuration.
//!
//! The pure scan/import logic is separated from the async filesystem watcher so
//! it can be unit-tested deterministically.

pub use crate::config::lexical_absolute_path as lexical_absolute;
use crate::config::{StartBehavior, WatchFolderConfig};
use crate::error::{CoreError, Result};
use crate::meta::{self, TorrentMeta, MAX_TORRENT_METADATA_BYTES};
use crate::policy::PolicyProfileOrigin;
use crate::torrent::Torrent;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Maximum number of operational watch results retained by the daemon.
pub const MAX_IMPORT_HISTORY: usize = 10_000;

/// Filesystem metadata used only to detect stable observations. This is not a
/// content-security fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileFingerprint {
    pub length: u64,
    pub modified: SystemTime,
}

impl FileFingerprint {
    pub fn from_metadata(metadata: &fs::Metadata) -> Result<Self> {
        Ok(Self {
            length: metadata.len(),
            modified: metadata.modified().map_err(CoreError::from)?,
        })
    }
}

/// A watch observation is namespaced by both the normalized configured root
/// and the normalized path relative to that root. Overlapping watch roots
/// therefore cannot alias one another.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObservationKey {
    pub root: PathBuf,
    pub relative_path: PathBuf,
}

impl ObservationKey {
    pub fn absolute_path(&self) -> PathBuf {
        self.root.join(&self.relative_path)
    }
}

/// One regular `.torrent` file returned by a successful root scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedTorrentFile {
    pub key: ObservationKey,
    pub fingerprint: FileFingerprint,
}

impl ScannedTorrentFile {
    pub fn path(&self) -> PathBuf {
        self.key.absolute_path()
    }
}

/// Complete result of one successful root walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchScan {
    pub root: PathBuf,
    pub files: Vec<ScannedTorrentFile>,
}

/// Typed result of a bounded watch-source read. Metadata changes are not parse
/// failures: callers reset stability and emit no terminal import result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedWatchRead {
    Stable(Vec<u8>),
    Changed(FileFingerprint),
}

/// Stable terminal classification exposed by the watch API and Web UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportOutcome {
    Imported,
    Duplicate,
    PermanentFailure,
    TransientFailure,
}

impl ImportOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Imported => "imported",
            Self::Duplicate => "duplicate",
            Self::PermanentFailure => "permanent_failure",
            Self::TransientFailure => "transient_failure",
        }
    }
}

/// Result of an import attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResult {
    pub path: String,
    pub success: bool,
    pub info_hash_hex: Option<String>,
    pub error: Option<String>,
    pub duplicate: bool,
    pub post_action_error: Option<String>,
    pub outcome: ImportOutcome,
}

/// Wait for a file's size to stabilize across consecutive checks (mockable).
/// Returns the stable size or an error if it doesn't stabilize within a number
/// of polls.
pub fn wait_for_stable_size(reads: &[u64]) -> Result<u64> {
    if reads.is_empty() {
        return Err(CoreError::Storage("no size readings".into()));
    }
    let last = *reads.last().unwrap();
    // Require at least two equal consecutive readings to consider stable.
    if reads.len() >= 2 && reads[reads.len() - 2] == last {
        Ok(last)
    } else {
        Err(CoreError::Storage("file size not stable".into()))
    }
}

/// Scan a directory for regular `.torrent` files. The entire function is
/// synchronous by design so callers can move the complete walk into
/// `spawn_blocking`. Every entry uses `symlink_metadata`; symlinks are ignored
/// and are never traversed. Any root/entry error fails the scan so callers do
/// not prune observations from an incomplete view.
pub fn scan_torrent_files(dir: &Path, recursive: bool) -> Result<WatchScan> {
    let root = lexical_absolute(dir)?;
    scan_torrent_files_at_root(root, recursive, &[])
}

/// Scan one configured watch folder while excluding only that folder's
/// archive and failure destinations when they are strict lexical descendants
/// of its root. A separately configured overlapping root computes its own
/// exclusions and can therefore observe the same path. No path is
/// canonicalized and comparisons are component-aware.
pub fn scan_watch_folder(folder: &WatchFolderConfig) -> Result<WatchScan> {
    if folder.path.trim().is_empty() {
        return Err(CoreError::InvalidConfig(
            "watch folder path must not be empty".into(),
        ));
    }
    let root = lexical_absolute(Path::new(&folder.path))?;
    let mut exclusions = Vec::new();
    for (field, configured_destination) in [
        ("archive_dir", folder.archive_dir.as_deref()),
        ("failure_dir", folder.failure_dir.as_deref()),
    ] {
        let Some(configured_destination) = configured_destination else {
            continue;
        };
        if configured_destination.trim().is_empty() {
            return Err(CoreError::InvalidConfig(format!(
                "watch folder {field} must not be empty when set"
            )));
        }
        let destination = lexical_absolute(Path::new(configured_destination))?;
        if destination == root {
            return Err(CoreError::InvalidConfig(format!(
                "watch folder {field} must not normalize to its watch root: {}",
                root.display()
            )));
        }
        if destination.starts_with(&root) {
            exclusions.push(destination);
        }
    }
    exclusions.sort();
    exclusions.dedup();
    scan_torrent_files_at_root(root, folder.recursive, &exclusions)
}

fn scan_torrent_files_at_root(
    root: PathBuf,
    recursive: bool,
    exclusions: &[PathBuf],
) -> Result<WatchScan> {
    let root_metadata = fs::symlink_metadata(&root).map_err(CoreError::from)?;
    if root_metadata.file_type().is_symlink() {
        return Err(CoreError::Storage(format!(
            "watch root must not be a symbolic link: {}",
            root.display()
        )));
    }
    if !root_metadata.is_dir() {
        return Err(CoreError::Storage(format!(
            "watch root is not a directory: {}",
            root.display()
        )));
    }

    let mut directories = vec![PathBuf::new()];
    let mut files = Vec::new();
    while let Some(relative_dir) = directories.pop() {
        let directory = root.join(&relative_dir);
        let mut entries = fs::read_dir(&directory)
            .map_err(CoreError::from)?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(CoreError::from)?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let relative_path = relative_dir.join(entry.file_name());
            let path = root.join(&relative_path);
            if exclusions
                .iter()
                .any(|destination| path.starts_with(destination))
            {
                continue;
            }
            let metadata = fs::symlink_metadata(&path).map_err(CoreError::from)?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                if recursive {
                    directories.push(relative_path);
                }
                continue;
            }
            if metadata.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("torrent")
            {
                files.push(ScannedTorrentFile {
                    key: ObservationKey {
                        root: root.clone(),
                        relative_path,
                    },
                    fingerprint: FileFingerprint::from_metadata(&metadata)?,
                });
            }
        }
    }
    files.sort_by(|left, right| left.key.relative_path.cmp(&right.key.relative_path));
    Ok(WatchScan { root, files })
}

/// Read at most the metainfo limit plus one byte while verifying the expected
/// path and opened-file metadata before and after the read. This avoids both
/// attacker-sized allocation and misclassifying a concurrent copy/update as a
/// permanent malformed input.
pub fn read_bounded_watch_file(path: &Path, expected: FileFingerprint) -> Result<BoundedWatchRead> {
    let before_metadata = fs::symlink_metadata(path).map_err(CoreError::from)?;
    if before_metadata.file_type().is_symlink() || !before_metadata.is_file() {
        return Err(CoreError::Storage(format!(
            "watch source is not a regular file: {}",
            path.display()
        )));
    }
    let before = FileFingerprint::from_metadata(&before_metadata)?;
    if before != expected {
        return Ok(BoundedWatchRead::Changed(before));
    }

    let input = fs::File::open(path).map_err(CoreError::from)?;
    let opened = FileFingerprint::from_metadata(&input.metadata().map_err(CoreError::from)?)?;
    if opened != expected {
        let current = current_regular_file_fingerprint(path)?;
        return Ok(BoundedWatchRead::Changed(current));
    }
    if expected.length > MAX_TORRENT_METADATA_BYTES as u64 {
        return Err(CoreError::MalformedTorrent(format!(
            "torrent metadata size {} exceeds maximum {MAX_TORRENT_METADATA_BYTES} bytes",
            expected.length
        )));
    }

    let mut bounded = input.take((MAX_TORRENT_METADATA_BYTES as u64) + 1);
    let capacity = usize::try_from(expected.length)
        .unwrap_or(MAX_TORRENT_METADATA_BYTES + 1)
        .min(MAX_TORRENT_METADATA_BYTES + 1);
    let mut bytes = Vec::with_capacity(capacity);
    bounded.read_to_end(&mut bytes).map_err(CoreError::from)?;
    let opened_after =
        FileFingerprint::from_metadata(&bounded.get_ref().metadata().map_err(CoreError::from)?)?;
    let path_after = current_regular_file_fingerprint(path)?;
    if opened_after != expected || path_after != expected {
        return Ok(BoundedWatchRead::Changed(path_after));
    }
    if bytes.len() > MAX_TORRENT_METADATA_BYTES {
        return Err(CoreError::MalformedTorrent(format!(
            "torrent metadata size exceeds maximum {MAX_TORRENT_METADATA_BYTES} bytes"
        )));
    }
    if bytes.len() as u64 != expected.length {
        return Err(CoreError::Storage(format!(
            "watch source length did not match stable metadata for {}",
            path.display()
        )));
    }
    Ok(BoundedWatchRead::Stable(bytes))
}

fn current_regular_file_fingerprint(path: &Path) -> Result<FileFingerprint> {
    let metadata = fs::symlink_metadata(path).map_err(CoreError::from)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CoreError::Storage(format!(
            "watch source is not a regular file: {}",
            path.display()
        )));
    }
    FileFingerprint::from_metadata(&metadata)
}

/// Import a single `.torrent` file into a torrent record. Duplicate detection
/// is performed by the caller against the registry; here we parse and build.
/// Reads through the bounded [`meta::read_torrent_file`] helper so oversized
/// files are rejected before allocation.
pub fn import_torrent_file(path: &Path, date_added: u64) -> Result<Torrent> {
    let bytes = meta::read_torrent_file(path)?;
    let parsed = meta::parse_torrent(&bytes)?;
    Ok(Torrent::new(parsed, date_added))
}

/// Decide the archive/leave/delete behavior after a successful import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostImportAction {
    Leave,
    Archive(PathBuf),
    Delete,
}

/// Resolve the post-import action for a file given folder config.
pub fn post_import_action(cfg: &WatchFolderConfig, file: &Path) -> PostImportAction {
    if cfg.delete_after_import && cfg.archive_dir.is_none() {
        return PostImportAction::Delete;
    }
    if let Some(archive) = &cfg.archive_dir {
        let mut dest = PathBuf::from(archive);
        dest.push(file.file_name().unwrap_or_default());
        return PostImportAction::Archive(dest);
    }
    PostImportAction::Leave
}

/// Resolve the configured permanent-failure action. Without a failure folder,
/// the source remains in place but its fingerprint is still terminally
/// processed for this daemon run.
pub fn post_failure_action(cfg: &WatchFolderConfig, file: &Path) -> PostImportAction {
    let Some(failure) = &cfg.failure_dir else {
        return PostImportAction::Leave;
    };
    let mut dest = PathBuf::from(failure);
    dest.push(file.file_name().unwrap_or_default());
    PostImportAction::Archive(dest)
}

/// Execute a post-import action without ever replacing a destination. Archive
/// and failure moves use `create_new` and streaming copy followed by source
/// removal, which also works across filesystems. A partial destination is
/// removed when copying or flushing fails.
pub fn execute_post_import_action(source: &Path, action: &PostImportAction) -> Result<()> {
    match action {
        PostImportAction::Leave => Ok(()),
        PostImportAction::Delete => fs::remove_file(source).map_err(CoreError::from),
        PostImportAction::Archive(destination) => {
            let parent = destination.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(parent).map_err(CoreError::from)?;
            copy_then_remove_no_replace(source, destination)
        }
    }
}

fn copy_then_remove_no_replace(source: &Path, destination: &Path) -> Result<()> {
    let source_metadata = fs::symlink_metadata(source).map_err(CoreError::from)?;
    if source_metadata.file_type().is_symlink() || !source_metadata.is_file() {
        return Err(CoreError::Storage(format!(
            "watch source is not a regular file: {}",
            source.display()
        )));
    }
    let mut input = fs::File::open(source).map_err(CoreError::from)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .map_err(CoreError::from)?;
    let copied = (|| -> std::io::Result<()> {
        std::io::copy(&mut input, &mut output)?;
        output.flush()?;
        output.sync_all()?;
        Ok(())
    })();
    if let Err(error) = copied {
        drop(output);
        let _ = fs::remove_file(destination);
        return Err(CoreError::from(error));
    }
    drop(output);
    fs::remove_file(source).map_err(CoreError::from)
}

/// Build a torrent summary for an imported file, applying per-folder defaults.
pub fn apply_folder_defaults(torrent: &mut Torrent, cfg: &WatchFolderConfig) {
    if let Some(dir) = &cfg.download_dir {
        torrent.download_dir = Some(dir.clone());
    }
    if let Some(label) = &cfg.label {
        torrent.labels.push(label.clone());
    }
    if let Some(profile) = &cfg.profile {
        torrent.policy.profile = Some(profile.clone());
        torrent.policy.profile_origin = Some(PolicyProfileOrigin::WatchFolder);
    }
    match cfg.start_behavior {
        StartBehavior::Start => {}
        StartBehavior::Paused => torrent.state = crate::models::torrent::TorrentState::Paused,
    }
}

/// Parse-only helper for tests: returns the parsed metadata.
pub fn parse_only(bytes: &[u8]) -> Result<TorrentMeta> {
    meta::parse_torrent(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{build_single_file_torrent, MAX_TORRENT_METADATA_BYTES};
    use std::io::Write;

    fn torrent_padded_to_size(target: usize) -> Vec<u8> {
        let mut bytes =
            build_single_file_torrent("watch-limit.bin", b"bounded watch payload", 8, None, false);
        assert_eq!(bytes.pop(), Some(b'e'));
        bytes.extend_from_slice(b"7:padding");

        let mut padding_len = target.saturating_sub(bytes.len() + 2);
        for _ in 0..32 {
            let encoded_len = bytes.len() + padding_len.to_string().len() + 1 + padding_len + 1;
            if encoded_len == target {
                bytes.extend_from_slice(padding_len.to_string().as_bytes());
                bytes.push(b':');
                bytes.extend(std::iter::repeat_n(b'x', padding_len));
                bytes.push(b'e');
                assert_eq!(bytes.len(), target);
                return bytes;
            }
            padding_len = target
                .checked_sub(bytes.len() + padding_len.to_string().len() + 2)
                .expect("target must accommodate the generated torrent");
        }
        panic!("could not solve bencode padding for target size {target}");
    }

    #[test]
    fn scans_torrent_files() {
        let dir = tempfile_dir();
        write_file(&dir, "a.torrent", b"d...");
        write_file(&dir, "b.txt", b"text");
        write_file(&dir, "c.torrent", b"d...");
        let sub = dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        write_file(&sub, "d.torrent", b"d...");

        let flat = scan_torrent_files(&dir, false).unwrap();
        assert_eq!(
            flat.files
                .iter()
                .map(|file| file.key.relative_path.as_path())
                .collect::<Vec<_>>(),
            [Path::new("a.torrent"), Path::new("c.torrent")]
        );
        let rec = scan_torrent_files(&dir, true).unwrap();
        assert_eq!(
            rec.files
                .iter()
                .map(|file| file.key.relative_path.as_path())
                .collect::<Vec<_>>(),
            [
                Path::new("a.torrent"),
                Path::new("c.torrent"),
                Path::new("sub/d.torrent")
            ]
        );
        assert!(flat.root.is_absolute());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn configured_scan_exclusions_are_descendant_component_aware_and_per_folder() {
        let root = tempfile_dir();
        let archive = root.join("archive");
        let failure = root.join("failure");
        let similarly_named = root.join("archive-copy");
        std::fs::create_dir_all(&archive).unwrap();
        std::fs::create_dir_all(&failure).unwrap();
        std::fs::create_dir_all(&similarly_named).unwrap();
        write_file(&root, "incoming.torrent", b"incoming");
        write_file(&archive, "archived.torrent", b"archived");
        write_file(&failure, "failed.torrent", b"failed");
        write_file(&similarly_named, "visible.torrent", b"visible");
        let parent_folder = WatchFolderConfig {
            path: root.display().to_string(),
            recursive: true,
            download_dir: None,
            label: None,
            profile: None,
            start_behavior: StartBehavior::Paused,
            archive_dir: Some(archive.display().to_string()),
            failure_dir: Some(failure.display().to_string()),
            delete_after_import: false,
        };

        let parent_scan = scan_watch_folder(&parent_folder).unwrap();
        assert_eq!(
            parent_scan
                .files
                .iter()
                .map(|file| file.key.relative_path.as_path())
                .collect::<Vec<_>>(),
            [
                Path::new("archive-copy/visible.torrent"),
                Path::new("incoming.torrent")
            ]
        );

        let overlapping_archive_folder = WatchFolderConfig {
            path: archive.display().to_string(),
            recursive: true,
            download_dir: None,
            label: None,
            profile: None,
            start_behavior: StartBehavior::Paused,
            archive_dir: None,
            failure_dir: None,
            delete_after_import: false,
        };
        let archive_scan = scan_watch_folder(&overlapping_archive_folder).unwrap();
        assert_eq!(archive_scan.files.len(), 1);
        assert_eq!(
            archive_scan.files[0].key.relative_path,
            PathBuf::from("archived.torrent")
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[cfg(unix)]
    #[test]
    fn scan_ignores_file_and_directory_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = tempfile_dir();
        let outside = tempfile_dir();
        write_file(&dir, "real.torrent", b"d...");
        write_file(&outside, "outside.torrent", b"d...");
        symlink(outside.join("outside.torrent"), dir.join("linked.torrent")).unwrap();
        symlink(&outside, dir.join("linked-dir")).unwrap();

        let scan = scan_torrent_files(&dir, true).unwrap();
        assert_eq!(scan.files.len(), 1);
        assert_eq!(
            scan.files[0].key.relative_path,
            PathBuf::from("real.torrent")
        );

        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&outside).ok();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_watch_root_is_an_incomplete_scan_error() {
        use std::os::unix::fs::symlink;

        let parent = tempfile_dir();
        let real = parent.join("real");
        let linked = parent.join("linked");
        std::fs::create_dir_all(&real).unwrap();
        write_file(&real, "entry.torrent", b"d...");
        symlink(&real, &linked).unwrap();

        let error = scan_torrent_files(&linked, true).unwrap_err();
        assert_eq!(error.code().as_str(), "storage_error");
        assert!(error.to_string().contains("symbolic link"));
        std::fs::remove_dir_all(&parent).ok();
    }

    #[test]
    fn lexical_absolute_normalizes_without_filesystem_resolution() {
        let root = tempfile_dir();
        let path = root.join("nested").join("..").join("watch").join(".");
        let normalized = lexical_absolute(&path).unwrap();
        assert_eq!(normalized, root.join("watch"));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn wait_for_stable_requires_two_equal() {
        assert!(wait_for_stable_size(&[10]).is_err());
        assert!(wait_for_stable_size(&[10, 20]).is_err());
        assert!(wait_for_stable_size(&[10, 20, 20]).is_ok());
    }

    #[test]
    fn import_parses_and_builds_torrent() {
        let bytes = build_single_file_torrent("f", b"data payload here", 8, None, false);
        let dir = tempfile_dir();
        let file = dir.join("x.torrent");
        std::fs::write(&file, &bytes).unwrap();
        let t = import_torrent_file(&file, 1).unwrap();
        assert_eq!(t.name(), "f");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn watch_import_accepts_exact_limit_and_rejects_one_byte_over() {
        let bytes = torrent_padded_to_size(MAX_TORRENT_METADATA_BYTES);
        let dir = tempfile_dir();
        let exact = dir.join("exact.torrent");
        let over = dir.join("over.torrent");
        std::fs::write(&exact, &bytes).unwrap();
        let mut one_over = bytes;
        one_over.push(b'X');
        std::fs::write(&over, &one_over).unwrap();

        let torrent = import_torrent_file(&exact, 1).unwrap();
        assert_eq!(torrent.name(), "watch-limit.bin");
        let err = import_torrent_file(&over, 1).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn bounded_watch_read_accepts_exact_limit_and_rejects_one_over_before_read() {
        let bytes = torrent_padded_to_size(MAX_TORRENT_METADATA_BYTES);
        let dir = tempfile_dir();
        let exact = dir.join("exact-read.torrent");
        let over = dir.join("over-read.torrent");
        std::fs::write(&exact, &bytes).unwrap();
        let exact_fingerprint =
            FileFingerprint::from_metadata(&std::fs::symlink_metadata(&exact).unwrap()).unwrap();
        assert!(matches!(
            read_bounded_watch_file(&exact, exact_fingerprint).unwrap(),
            BoundedWatchRead::Stable(read) if read == bytes
        ));

        let mut one_over = bytes;
        one_over.push(b'X');
        std::fs::write(&over, one_over).unwrap();
        let over_fingerprint =
            FileFingerprint::from_metadata(&std::fs::symlink_metadata(&over).unwrap()).unwrap();
        let error = read_bounded_watch_file(&over, over_fingerprint).unwrap_err();
        assert!(matches!(error, CoreError::MalformedTorrent(_)));
        assert!(error.to_string().contains("exceeds maximum"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn post_import_actions() {
        let mut cfg = WatchFolderConfig {
            path: "/w".into(),
            recursive: false,
            download_dir: None,
            label: None,
            profile: None,
            start_behavior: StartBehavior::Start,
            archive_dir: None,
            failure_dir: None,
            delete_after_import: true,
        };
        let f = std::path::Path::new("/w/x.torrent");
        assert!(matches!(
            post_import_action(&cfg, f),
            PostImportAction::Delete
        ));
        cfg.archive_dir = Some("/archive".into());
        match post_import_action(&cfg, f) {
            PostImportAction::Archive(p) => assert_eq!(p, PathBuf::from("/archive/x.torrent")),
            _ => panic!("expected archive"),
        }
        cfg.delete_after_import = false;
        cfg.archive_dir = None;
        assert!(matches!(
            post_import_action(&cfg, f),
            PostImportAction::Leave
        ));
    }

    #[test]
    fn archive_action_creates_destination_and_never_overwrites() {
        let dir = tempfile_dir();
        let source = dir.join("source.torrent");
        let destination = dir.join("archive").join("source.torrent");
        std::fs::write(&source, b"first").unwrap();
        execute_post_import_action(&source, &PostImportAction::Archive(destination.clone()))
            .unwrap();
        assert!(!source.exists());
        assert_eq!(std::fs::read(&destination).unwrap(), b"first");

        std::fs::write(&source, b"replacement").unwrap();
        let error =
            execute_post_import_action(&source, &PostImportAction::Archive(destination.clone()))
                .unwrap_err();
        assert_eq!(error.code().as_str(), "io_error");
        assert!(source.exists());
        assert_eq!(std::fs::read(&destination).unwrap(), b"first");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn import_outcomes_serialize_to_stable_strings() {
        for (outcome, expected) in [
            (ImportOutcome::Imported, "\"imported\""),
            (ImportOutcome::Duplicate, "\"duplicate\""),
            (ImportOutcome::PermanentFailure, "\"permanent_failure\""),
            (ImportOutcome::TransientFailure, "\"transient_failure\""),
        ] {
            assert_eq!(serde_json::to_string(&outcome).unwrap(), expected);
            assert_eq!(outcome.as_str(), &expected[1..expected.len() - 1]);
        }
    }

    #[test]
    fn folder_defaults_apply() {
        let bytes = build_single_file_torrent("f", b"abcabcabc", 4, None, false);
        let meta = parse_only(&bytes).unwrap();
        let mut t = Torrent::new(meta, 1);
        let cfg = WatchFolderConfig {
            path: "/w".into(),
            recursive: false,
            download_dir: Some("/downloads".into()),
            label: Some("linux".into()),
            profile: Some("linux-release".into()),
            start_behavior: StartBehavior::Paused,
            archive_dir: None,
            failure_dir: None,
            delete_after_import: false,
        };
        apply_folder_defaults(&mut t, &cfg);
        assert_eq!(t.download_dir.as_deref(), Some("/downloads"));
        assert!(t.labels.contains(&"linux".to_string()));
        assert_eq!(t.policy.profile.as_deref(), Some("linux-release"));
        assert_eq!(
            t.policy.profile_origin,
            Some(PolicyProfileOrigin::WatchFolder)
        );
        assert_eq!(t.state, crate::models::torrent::TorrentState::Paused);
    }

    fn tempfile_dir() -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("swarmotter-test-{}", std::process::id()));
        p.push(format!(
            "{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            rand_u64()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn write_file(dir: &std::path::Path, name: &str, content: &[u8]) {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(content).unwrap();
    }

    fn rand_u64() -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        std::thread::current().id().hash(&mut h);
        h.finish()
    }
}
