// SPDX-License-Identifier: Apache-2.0

//! Storage root diagnostics and free-space preflight helpers.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::config::{CowStrategy, StorageConfig};
use crate::error::{CoreError, Result};
use crate::models::storage::{StorageRootDiagnostics, StorageRootRole};

#[derive(Debug, Clone, Default)]
pub struct StorageRootUsage {
    pub torrent_count: usize,
    pub active_torrents: usize,
    pub active_bytes: u64,
    pub active_write_rate: u64,
    pub active_recheck_rate: Option<u64>,
    pub sustained_write_bytes_per_second: u64,
    pub sustained_verification_bytes_per_second: u64,
    pub active_rechecks: usize,
}

#[derive(Debug, Clone)]
pub struct StoragePreflight {
    pub path: PathBuf,
    pub content_bytes: u64,
    pub available_space_bytes: u64,
    pub required_free_space_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct FilesystemSpace {
    total: u64,
    free: u64,
    available: u64,
}

#[derive(Debug, Clone)]
struct MountInfo {
    point: PathBuf,
    options: Vec<String>,
    source: String,
}

pub fn inspect_storage_root(
    path: &Path,
    roles: Vec<StorageRootRole>,
    config: &StorageConfig,
    usage: StorageRootUsage,
) -> StorageRootDiagnostics {
    let metadata = std::fs::metadata(path).ok();
    let exists = metadata.is_some();
    let is_directory = metadata.as_ref().is_some_and(|m| m.is_dir());
    let probe_path = existing_probe_path(path);
    let space = probe_path.as_deref().and_then(filesystem_space);
    let filesystem_type = probe_path.as_deref().and_then(filesystem_type);
    let mount = probe_path.as_deref().and_then(mount_info);
    let required_free_space_bytes = required_free_space_bytes(
        space.map(|s| s.total),
        config.minimum_free_space_bytes,
        config.minimum_free_space_percent,
        0,
    );
    let reserve_satisfied = space.map(|s| s.available >= required_free_space_bytes);
    let writable = storage_path_writable(path, metadata.as_ref(), probe_path.as_deref());
    let root_control = config.root_control_for_path(path);
    let root_control_path = root_control
        .and_then(|control| control.normalized_path().ok())
        .map(|path| path.display().to_string());
    let max_active_downloads = root_control
        .map(|control| control.max_active_downloads)
        .unwrap_or(0);
    let max_active_bytes = root_control
        .map(|control| control.max_active_bytes)
        .unwrap_or(0);
    let max_write_bytes_per_second = root_control
        .map(|control| control.max_write_bytes_per_second)
        .unwrap_or(0);
    let max_concurrent_rechecks = root_control
        .map(|control| control.max_concurrent_rechecks)
        .unwrap_or(0);
    let mut warnings = Vec::new();
    if !exists {
        warnings.push("storage path does not exist; nearest existing parent was inspected".into());
    } else if !is_directory {
        warnings.push("storage path exists but is not a directory".into());
    }
    if !writable {
        warnings.push("storage path or nearest existing parent may not be writable".into());
    }
    if space.is_none() {
        warnings.push("free space could not be inspected for this path".into());
    } else if reserve_satisfied == Some(false) {
        warnings.push("configured free-space reserve is not currently satisfied".into());
    }
    if max_active_downloads > 0 && usage.active_torrents >= max_active_downloads {
        warnings.push("configured active-download control is currently saturated".into());
    }
    if max_active_bytes > 0 && usage.active_bytes >= max_active_bytes {
        warnings.push("configured active-byte control is currently saturated".into());
    }
    if max_concurrent_rechecks > 0 && usage.active_rechecks >= max_concurrent_rechecks {
        warnings.push("configured recheck control is currently saturated".into());
    }
    let cow_strategy_supported =
        cow_strategy_supported(config.cow_strategy, filesystem_type.as_deref());
    match (config.cow_strategy, cow_strategy_supported) {
        (CowStrategy::DisableForNewFiles, Some(false)) => warnings.push(
            "cow_strategy=disable_for_new_files requires a supported Linux Btrfs root; new payload files will fail rather than silently changing strategy".into(),
        ),
        (CowStrategy::DisableForNewFiles, None) => warnings.push(
            "cow_strategy=disable_for_new_files could not be verified for this root; new payload files will fail rather than silently changing strategy".into(),
        ),
        (CowStrategy::Conservative, _) if filesystem_type.as_deref() == Some("btrfs") => {
            warnings.push(
                "Btrfs detected: conservative CoW strategy preserves filesystem defaults; sparse/preallocation choices may affect fragmentation, snapshots, compression, and checksumming trade-offs".into(),
            );
        }
        _ => {}
    }

    StorageRootDiagnostics {
        path: path.display().to_string(),
        roles: dedup_roles(roles),
        exists,
        is_directory,
        writable,
        filesystem_type,
        mount_point: mount
            .as_ref()
            .map(|mount| mount.point.display().to_string()),
        mount_options: mount.as_ref().map(|mount| mount.options.clone()),
        mount_source: mount.map(|mount| mount.source),
        total_space_bytes: space.map(|s| s.total),
        free_space_bytes: space.map(|s| s.free),
        available_space_bytes: space.map(|s| s.available),
        required_free_space_bytes,
        reserve_satisfied,
        torrent_count: usage.torrent_count,
        active_torrents: usage.active_torrents,
        active_bytes: usage.active_bytes,
        active_write_rate: usage.active_write_rate,
        active_recheck_rate: usage.active_recheck_rate,
        sustained_write_bytes_per_second: usage.sustained_write_bytes_per_second,
        sustained_verification_bytes_per_second: usage.sustained_verification_bytes_per_second,
        cow_strategy: config.cow_strategy,
        cow_strategy_supported,
        active_rechecks: usage.active_rechecks,
        root_control_path,
        max_active_downloads,
        max_active_bytes,
        max_write_bytes_per_second,
        max_concurrent_rechecks,
        warnings,
    }
}

fn cow_strategy_supported(strategy: CowStrategy, filesystem_type: Option<&str>) -> Option<bool> {
    match strategy {
        CowStrategy::Conservative => Some(true),
        CowStrategy::DisableForNewFiles => {
            #[cfg(target_os = "linux")]
            {
                filesystem_type.map(|filesystem_type| filesystem_type == "btrfs")
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = filesystem_type;
                None
            }
        }
    }
}

pub fn check_storage_preflight(
    path: &Path,
    config: &StorageConfig,
    content_bytes: u64,
) -> Result<StoragePreflight> {
    if config.minimum_free_space_bytes == 0 && config.minimum_free_space_percent == 0 {
        return Ok(StoragePreflight {
            path: path.to_path_buf(),
            content_bytes,
            available_space_bytes: 0,
            required_free_space_bytes: 0,
        });
    }
    let probe_path = existing_probe_path(path).ok_or_else(|| {
        CoreError::Storage(format!(
            "storage preflight could not find an existing parent for {}",
            path.display()
        ))
    })?;
    let space = filesystem_space(&probe_path).ok_or_else(|| {
        CoreError::Storage(format!(
            "storage preflight could not inspect free space for {}",
            path.display()
        ))
    })?;
    let required = required_free_space_bytes(
        Some(space.total),
        config.minimum_free_space_bytes,
        config.minimum_free_space_percent,
        content_bytes,
    );
    if space.available < required {
        return Err(CoreError::Storage(format!(
            "storage preflight failed for {}: available {} bytes, required {} bytes including configured reserve",
            path.display(),
            space.available,
            required
        )));
    }
    Ok(StoragePreflight {
        path: path.to_path_buf(),
        content_bytes,
        available_space_bytes: space.available,
        required_free_space_bytes: required,
    })
}

pub fn required_free_space_bytes(
    total_space_bytes: Option<u64>,
    minimum_free_space_bytes: u64,
    minimum_free_space_percent: u8,
    content_bytes: u64,
) -> u64 {
    let percent_bytes = total_space_bytes
        .map(|total| {
            ((total as u128) * (minimum_free_space_percent as u128) / 100).min(u64::MAX as u128)
                as u64
        })
        .unwrap_or(0);
    minimum_free_space_bytes
        .max(percent_bytes)
        .saturating_add(content_bytes)
}

fn dedup_roles(roles: Vec<StorageRootRole>) -> Vec<StorageRootRole> {
    roles
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn existing_probe_path(path: &Path) -> Option<PathBuf> {
    let mut current = if path.exists() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    loop {
        if current.exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn storage_path_writable(
    path: &Path,
    metadata: Option<&std::fs::Metadata>,
    probe_path: Option<&Path>,
) -> bool {
    if metadata.is_some_and(|m| m.permissions().readonly()) {
        return false;
    }
    let check_path = if path.exists() {
        path
    } else {
        probe_path.unwrap_or(path)
    };
    path_access_writable(check_path)
}

/// Linux exposes mount metadata without an additional dependency through
/// `/proc/self/mountinfo`. Parsing is deliberately best-effort: diagnostics
/// remain usable when procfs is absent, restricted, or uses an unfamiliar
/// mount record.
#[cfg(target_os = "linux")]
fn mount_info(path: &Path) -> Option<MountInfo> {
    let probe = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let records = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    mount_info_from_records(&probe, &records)
}

#[cfg(target_os = "linux")]
fn mount_info_from_records(path: &Path, records: &str) -> Option<MountInfo> {
    records
        .lines()
        .filter_map(parse_linux_mountinfo_line)
        .filter(|mount| path.starts_with(&mount.point))
        .max_by_key(|mount| mount.point.components().count())
}

#[cfg(not(target_os = "linux"))]
fn mount_info(_path: &Path) -> Option<MountInfo> {
    None
}

#[cfg(target_os = "linux")]
fn parse_linux_mountinfo_line(line: &str) -> Option<MountInfo> {
    let (before_separator, after_separator) = line.split_once(" - ")?;
    let fields = before_separator.split_whitespace().collect::<Vec<_>>();
    // mountinfo's fixed fields are: id, parent, major:minor, root,
    // mount-point, mount-options. Optional fields follow them.
    if fields.len() < 6 {
        return None;
    }
    let post = after_separator.split_whitespace().collect::<Vec<_>>();
    // post-separator: filesystem type, mount source, super options.
    if post.len() < 3 {
        return None;
    }
    let mut options = fields[5]
        .split(',')
        .filter(|option| !option.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    for option in post[2].split(',').filter(|option| !option.is_empty()) {
        if !options.iter().any(|existing| existing == option) {
            options.push(option.to_string());
        }
    }
    Some(MountInfo {
        point: PathBuf::from(unescape_mountinfo_field(fields[4])),
        options,
        source: unescape_mountinfo_field(post[1]),
    })
}

#[cfg(target_os = "linux")]
fn unescape_mountinfo_field(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1..=index + 3]
                .iter()
                .all(|byte| (b'0'..=b'7').contains(byte))
        {
            let octal = (bytes[index + 1] - b'0') * 64
                + (bytes[index + 2] - b'0') * 8
                + (bytes[index + 3] - b'0');
            output.push(octal);
            index += 4;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

#[cfg(unix)]
fn filesystem_space(path: &Path) -> Option<FilesystemSpace> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    let fragment = if stat.f_frsize > 0 {
        stat.f_frsize
    } else {
        stat.f_bsize
    } as u128;
    Some(FilesystemSpace {
        total: bytes_from_blocks(stat.f_blocks as u128, fragment),
        free: bytes_from_blocks(stat.f_bfree as u128, fragment),
        available: bytes_from_blocks(stat.f_bavail as u128, fragment),
    })
}

#[cfg(not(unix))]
fn filesystem_space(_path: &Path) -> Option<FilesystemSpace> {
    None
}

#[cfg(unix)]
fn filesystem_type(path: &Path) -> Option<String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    let rc = unsafe { libc::statfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    Some(filesystem_magic_name(stat.f_type as u64).to_string())
}

#[cfg(not(unix))]
fn filesystem_type(_path: &Path) -> Option<String> {
    None
}

#[cfg(unix)]
fn path_access_writable(path: &Path) -> bool {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(c_path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    unsafe { libc::access(c_path.as_ptr(), libc::W_OK) == 0 }
}

#[cfg(not(unix))]
fn path_access_writable(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| !m.permissions().readonly())
        .unwrap_or(false)
}

fn bytes_from_blocks(blocks: u128, block_size: u128) -> u64 {
    blocks.saturating_mul(block_size).min(u64::MAX as u128) as u64
}

fn filesystem_magic_name(magic: u64) -> String {
    match magic {
        0x9123683e => "btrfs".into(),
        0xef53 => "ext".into(),
        0x58465342 => "xfs".into(),
        0x01021994 => "tmpfs".into(),
        0x6969 => "nfs".into(),
        0x794c7630 => "overlayfs".into(),
        0x61756673 => "aufs".into(),
        0xff534d42 => "cifs".into(),
        other => format!("0x{other:x}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_free_space_uses_stricter_reserve_plus_content() {
        assert_eq!(required_free_space_bytes(Some(10_000), 512, 10, 100), 1_100);
        assert_eq!(
            required_free_space_bytes(Some(10_000), 2_000, 10, 100),
            2_100
        );
        assert_eq!(required_free_space_bytes(None, 2_000, 10, 100), 2_100);
    }

    #[test]
    fn preflight_rejects_impossible_space_requirement() {
        let cfg = StorageConfig {
            minimum_free_space_bytes: u64::MAX,
            ..Default::default()
        };
        let err = check_storage_preflight(&std::env::temp_dir(), &cfg, 1).unwrap_err();
        assert_eq!(err.code().as_str(), "storage_error");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mountinfo_parser_reports_most_specific_mount_and_tolerates_unavailable_data() {
        let records = concat!(
            "29 23 0:25 / / rw,relatime - ext4 /dev/root rw\n",
            "42 29 0:39 / /srv/data rw,nosuid - btrfs /dev/data rw,ssd\n"
        );
        let mount = mount_info_from_records(Path::new("/srv/data/incomplete"), records).unwrap();
        assert_eq!(mount.point, PathBuf::from("/srv/data"));
        assert_eq!(mount.source, "/dev/data");
        assert!(mount.options.contains(&"nosuid".into()));
        assert!(mount.options.contains(&"ssd".into()));

        assert!(mount_info_from_records(Path::new("/srv/data"), "not mountinfo").is_none());
    }

    #[test]
    fn diagnostics_keep_safe_optional_mount_fields_when_metadata_is_unavailable() {
        let root = std::env::temp_dir().join(format!(
            "swarmotter-missing-storage-diagnostics-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let diagnostics = inspect_storage_root(
            &root,
            vec![StorageRootRole::Temporary],
            &StorageConfig::default(),
            StorageRootUsage::default(),
        );
        assert_eq!(diagnostics.path, root.display().to_string());
        // Host mount metadata is deliberately optional; diagnostics must be
        // serializable and useful whether it is present or absent.
        assert!(diagnostics
            .mount_options
            .as_ref()
            .is_none_or(|options| !options.is_empty()));
        assert!(serde_json::to_value(&diagnostics).is_ok());
    }
}
