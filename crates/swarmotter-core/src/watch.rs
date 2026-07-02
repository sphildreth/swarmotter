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

use crate::config::{StartBehavior, WatchFolderConfig};
use crate::error::{CoreError, Result};
use crate::meta::{self, TorrentMeta};
use crate::torrent::Torrent;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Result of an import attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResult {
    pub path: String,
    pub success: bool,
    pub info_hash_hex: Option<String>,
    pub error: Option<String>,
    pub duplicate: bool,
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

/// Scan a directory for `.torrent` files (optionally recursive).
pub fn scan_torrent_files(dir: &std::path::Path, recursive: bool) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if recursive {
                out.extend(scan_torrent_files(&path, true));
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("torrent") {
            out.push(path);
        }
    }
    out
}

/// Import a single `.torrent` file into a torrent record. Duplicate detection
/// is performed by the caller against the registry; here we parse and build.
pub fn import_torrent_file(path: &std::path::Path, date_added: u64) -> Result<Torrent> {
    let bytes = std::fs::read(path).map_err(CoreError::Io)?;
    let meta = meta::parse_torrent(&bytes)?;
    Ok(Torrent::new(meta, date_added))
}

/// Decide the archive/leave/delete behavior after a successful import.
pub enum PostImportAction {
    Leave,
    Archive(PathBuf),
    Delete,
}

/// Resolve the post-import action for a file given folder config.
pub fn post_import_action(cfg: &WatchFolderConfig, file: &std::path::Path) -> PostImportAction {
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

/// Build a torrent summary for an imported file, applying per-folder defaults.
pub fn apply_folder_defaults(torrent: &mut Torrent, cfg: &WatchFolderConfig) {
    if let Some(dir) = &cfg.download_dir {
        torrent.download_dir = Some(dir.clone());
    }
    if let Some(label) = &cfg.label {
        torrent.labels.push(label.clone());
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
    use crate::meta::build_single_file_torrent;
    use std::io::Write;

    #[test]
    fn scans_torrent_files() {
        let dir = tempfile_dir();
        write_file(&dir, "a.torrent", b"d...");
        write_file(&dir, "b.txt", b"text");
        write_file(&dir, "c.torrent", b"d...");
        let sub = dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        write_file(&sub, "d.torrent", b"d...");

        let flat = scan_torrent_files(&dir, false);
        assert_eq!(flat.len(), 2);
        let rec = scan_torrent_files(&dir, true);
        assert_eq!(rec.len(), 3);
        std::fs::remove_dir_all(&dir).ok();
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
    fn post_import_actions() {
        let mut cfg = WatchFolderConfig {
            path: "/w".into(),
            recursive: false,
            download_dir: None,
            label: None,
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
    fn folder_defaults_apply() {
        let bytes = build_single_file_torrent("f", b"abcabcabc", 4, None, false);
        let meta = parse_only(&bytes).unwrap();
        let mut t = Torrent::new(meta, 1);
        let cfg = WatchFolderConfig {
            path: "/w".into(),
            recursive: false,
            download_dir: Some("/downloads".into()),
            label: Some("linux".into()),
            start_behavior: StartBehavior::Paused,
            archive_dir: None,
            failure_dir: None,
            delete_after_import: false,
        };
        apply_folder_defaults(&mut t, &cfg);
        assert_eq!(t.download_dir.as_deref(), Some("/downloads"));
        assert!(t.labels.contains(&"linux".to_string()));
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
