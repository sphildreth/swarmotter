// SPDX-License-Identifier: Apache-2.0

use super::*;

impl DaemonRuntime {
    pub(super) async fn add_config_file_check(&self, checks: &mut Vec<DoctorCheck>) {
        let Some(path) = &self.config_path else {
            push_check(
                checks,
                "config_file",
                "Config file",
                DiagnosticLevel::Warning,
                "daemon was started without a config file, so full settings cannot be persisted",
                Some("start swarmotterd with --config to enable config.toml writes"),
            );
            return;
        };
        let level = if path.is_file() {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Warning
        };
        push_check(
            checks,
            "config_file",
            "Config file",
            level,
            format!("configured path: {}", path.display()),
            Some("create the config file or verify the daemon has write permissions"),
        );
    }

    pub(super) async fn add_log_file_check(&self, checks: &mut Vec<DoctorCheck>) {
        let Some(path) = &self.log_file_path else {
            push_check(
                checks,
                "log_file",
                "Log file",
                DiagnosticLevel::Warning,
                "file logging is disabled; the Logs page can only show live events",
                Some("enable logging.file or configure logging.file_path"),
            );
            return;
        };
        let level = if path.is_file() {
            DiagnosticLevel::Ok
        } else {
            DiagnosticLevel::Warning
        };
        push_check(
            checks,
            "log_file",
            "Log file",
            level,
            format!("log path: {}", path.display()),
            Some("verify the daemon can create and read the log file"),
        );
    }

    pub(super) async fn add_storage_checks(&self, cfg: &Config, checks: &mut Vec<DoctorCheck>) {
        add_storage_check(
            checks,
            "download_dir",
            "Download directory",
            cfg.storage.download_dir.as_deref(),
        );
        add_storage_check(
            checks,
            "incomplete_dir",
            "Incomplete directory",
            cfg.storage.incomplete_dir.as_deref(),
        );
    }

    pub(super) async fn add_watch_checks(&self, cfg: &Config, checks: &mut Vec<DoctorCheck>) {
        if cfg.watch.is_empty() {
            push_check(
                checks,
                "watch_folders",
                "Watch folders",
                DiagnosticLevel::Warning,
                "no watch folders are configured",
                Some("add [[watch]] entries if automatic .torrent import is desired"),
            );
            return;
        }
        let missing = cfg
            .watch
            .iter()
            .filter(|folder| !Path::new(&folder.path).is_dir())
            .count();
        push_check(
            checks,
            "watch_folders",
            "Watch folders",
            if missing == 0 {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Warning
            },
            format!(
                "{} configured, {} missing or unreadable",
                cfg.watch.len(),
                missing
            ),
            Some("verify watch folder paths and permissions"),
        );
    }

    pub(super) async fn add_torrent_runtime_check(&self, checks: &mut Vec<DoctorCheck>) {
        let reg = self.registry.lock().await;
        let errors = reg
            .torrents
            .values()
            .filter(|torrent| torrent.error.is_some())
            .count();
        push_check(
            checks,
            "torrent_runtime",
            "Torrent runtime",
            if errors == 0 {
                DiagnosticLevel::Ok
            } else {
                DiagnosticLevel::Warning
            },
            format!(
                "{} torrents loaded, {} with errors",
                reg.torrents.len(),
                errors
            ),
            Some("open torrent details or logs for the affected torrents"),
        );
    }
}

pub(super) fn is_permanent_watch_error(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::Bencode(_)
            | CoreError::MalformedTorrent(_)
            | CoreError::InvalidInfoHash(_)
            | CoreError::Parse(_)
    )
}

pub(super) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(super) fn redact_config(mut cfg: Config) -> Config {
    cfg.api.auth_token = None;
    cfg
}

pub(super) fn capture_config_file(path: &Path) -> Result<ConfigFileSnapshot> {
    match fs::read(path) {
        Ok(bytes) => Ok(ConfigFileSnapshot::Bytes(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(ConfigFileSnapshot::Missing)
        }
        Err(error) => Err(CoreError::from(error)),
    }
}

pub(super) fn restore_config_file(path: &Path, snapshot: &ConfigFileSnapshot) -> Result<()> {
    match snapshot {
        ConfigFileSnapshot::Bytes(bytes) => write_config_bytes_atomically(path, bytes),
        ConfigFileSnapshot::Missing => match fs::remove_file(path) {
            Ok(()) => {
                let parent = path.parent().unwrap_or_else(|| Path::new("."));
                fs::File::open(parent)
                    .and_then(|directory| directory.sync_all())
                    .map_err(CoreError::from)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CoreError::from(error)),
        },
    }
}

pub(super) fn write_config_atomically(path: &Path, config: &Config) -> Result<()> {
    let toml = config.to_toml_string()?;
    write_config_bytes_atomically(path, toml.as_bytes())
}

pub(super) fn write_config_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(CoreError::from)?;
    let sequence = CONFIG_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("swarmotter.toml");
    let tmp = path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), sequence));
    let result = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp).map_err(CoreError::from)?;
        file.write_all(bytes).map_err(CoreError::from)?;
        file.sync_all().map_err(CoreError::from)?;
        drop(file);
        fs::rename(&tmp, path).map_err(CoreError::from)?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(CoreError::from)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

pub(super) fn restart_required_fields(previous: &Config, next: &Config) -> Vec<String> {
    let mut fields = Vec::new();
    if previous.api.bind_address != next.api.bind_address {
        fields.push("api.bind_address".into());
    }
    if previous.api.max_request_body_bytes != next.api.max_request_body_bytes {
        fields.push("api.max_request_body_bytes".into());
    }
    if previous.logging.level != next.logging.level {
        fields.push("logging.level".into());
    }
    if previous.logging.json != next.logging.json {
        fields.push("logging.json".into());
    }
    if previous.logging.file != next.logging.file {
        fields.push("logging.file".into());
    }
    if previous.logging.file_path != next.logging.file_path {
        fields.push("logging.file_path".into());
    }
    fields
}

pub(super) fn data_plane_config_changed(previous: &Config, next: &Config) -> bool {
    previous.network != next.network
        || previous.torrent.listen_port != next.torrent.listen_port
        || previous.torrent.allow_ipv6 != next.torrent.allow_ipv6
        || previous.torrent.utp_enabled != next.torrent.utp_enabled
        || previous.torrent.utp_prefer_tcp != next.torrent.utp_prefer_tcp
        || previous.torrent.encryption_mode != next.torrent.encryption_mode
        || previous.dht != next.dht
        || previous.pex.enabled != next.pex.enabled
        || previous.pex.max_peers != next.pex.max_peers
        || peer_limits_changed(previous, next)
        || previous.storage.download_dir != next.storage.download_dir
        || previous.storage.incomplete_dir != next.storage.incomplete_dir
        || previous.storage.minimum_free_space_bytes != next.storage.minimum_free_space_bytes
        || previous.storage.minimum_free_space_percent != next.storage.minimum_free_space_percent
        || previous.storage.preallocate != next.storage.preallocate
        || previous.storage.sparse != next.storage.sparse
}

pub(super) fn peer_limits_changed(previous: &Config, next: &Config) -> bool {
    previous.bandwidth.max_peers != next.bandwidth.max_peers
        || previous.bandwidth.max_peers_per_torrent != next.bandwidth.max_peers_per_torrent
}

pub(super) fn configs_differ_only_in_peer_limits(previous: &Config, next: &Config) -> bool {
    if !peer_limits_changed(previous, next) {
        return false;
    }
    let mut normalized = next.clone();
    normalized.bandwidth.max_peers = previous.bandwidth.max_peers;
    normalized.bandwidth.max_peers_per_torrent = previous.bandwidth.max_peers_per_torrent;
    match (previous.to_toml_string(), normalized.to_toml_string()) {
        (Ok(previous), Ok(normalized)) => previous == normalized,
        _ => false,
    }
}

pub(super) fn validate_storage_config_transition(
    previous: &Config,
    next: &Config,
    torrents: &[Torrent],
) -> Result<()> {
    if previous.storage.download_dir != next.storage.download_dir
        && torrents
            .iter()
            .any(|torrent| torrent.download_dir.is_none())
    {
        return Err(CoreError::InvalidConfig(
            "storage.download_dir cannot change while torrents still use the global download directory; move those torrents to explicit locations first"
                .into(),
        ));
    }
    if previous.storage.incomplete_dir != next.storage.incomplete_dir
        && torrents
            .iter()
            .any(|torrent| !torrent.progress.is_complete())
    {
        return Err(CoreError::InvalidConfig(
            "storage.incomplete_dir cannot change while torrents have incomplete payloads".into(),
        ));
    }
    validate_restored_storage_ownership(torrents.iter(), next)
}

pub(super) fn push_check(
    checks: &mut Vec<DoctorCheck>,
    id: impl Into<String>,
    label: impl Into<String>,
    level: DiagnosticLevel,
    detail: impl Into<String>,
    remediation: Option<&str>,
) {
    checks.push(DoctorCheck {
        id: id.into(),
        label: label.into(),
        level,
        detail: detail.into(),
        remediation: remediation.map(str::to_string),
    });
}

pub(super) fn containment_matrix(config: &Config, level: DiagnosticLevel) -> Vec<NetworkPathCheck> {
    let mut rows = vec![
        (
            "peer_tcp",
            "Peer TCP",
            "outbound peer TCP uses the contained NetworkBinder",
        ),
        (
            "peer_utp",
            "Peer uTP",
            "uTP uses contained UDP sockets with TCP fallback policy",
        ),
        (
            "dht_udp",
            "DHT UDP",
            "DHT packets use the same contained UDP socket layer",
        ),
        (
            "udp_tracker",
            "UDP trackers",
            "UDP tracker announces use contained UDP sockets",
        ),
        (
            "http_tracker",
            "HTTP(S) trackers",
            "tracker HTTP/TLS is performed over contained sockets",
        ),
        (
            "webseed",
            "Web seeds",
            "webseed range requests use contained HTTP/TLS sockets",
        ),
        (
            "dns",
            "DNS resolution",
            "hostname resolution is validated or blocked by containment policy",
        ),
    ];
    if !config.torrent.utp_enabled {
        rows.retain(|(id, _, _)| *id != "peer_utp");
    }
    rows.into_iter()
        .map(|(id, label, detail)| NetworkPathCheck {
            id: id.into(),
            label: label.into(),
            level,
            detail: detail.into(),
        })
        .collect()
}

pub(super) fn add_storage_check(
    checks: &mut Vec<DoctorCheck>,
    id: &'static str,
    label: &'static str,
    path: Option<&str>,
) {
    let Some(path) = path else {
        push_check(
            checks,
            id,
            label,
            DiagnosticLevel::Warning,
            "not configured; daemon will use its default temporary directory behavior",
            Some("set an explicit storage path for predictable operations"),
        );
        return;
    };
    let path = Path::new(path);
    let existing = path.exists();
    let disk = free_space_bytes(path).or_else(|| path.parent().and_then(free_space_bytes));
    let level = match disk {
        Some(bytes) if bytes < 1024 * 1024 * 1024 => DiagnosticLevel::Invalid,
        Some(bytes) if bytes < 10 * 1024 * 1024 * 1024 => DiagnosticLevel::Warning,
        Some(_) if existing || path.parent().map(Path::exists).unwrap_or(false) => {
            DiagnosticLevel::Ok
        }
        Some(_) => DiagnosticLevel::Warning,
        None => DiagnosticLevel::Warning,
    };
    let detail = match disk {
        Some(bytes) => format!("{} available at {}", format_bytes(bytes), path.display()),
        None => format!("unable to inspect free space at {}", path.display()),
    };
    push_check(
        checks,
        id,
        label,
        level,
        detail,
        Some("ensure the path exists, is writable, and has enough free space"),
    );
}

#[cfg(unix)]
pub(super) fn free_space_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    Some(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(not(unix))]
pub(super) fn free_space_bytes(_path: &Path) -> Option<u64> {
    None
}

pub(super) fn format_bytes(bytes: u64) -> String {
    let mut value = bytes as f64;
    let mut unit = "B";
    for next in ["KB", "MB", "GB", "TB"] {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next;
    }
    if unit == "B" {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {unit}")
    }
}

pub(super) fn read_last_lines(path: &Path, max_lines: usize) -> std::io::Result<Vec<String>> {
    if max_lines == 0 {
        return Ok(Vec::new());
    }
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = Vec::new();
    for line in reader.lines() {
        lines.push(strip_ansi_controls(&line?));
        if lines.len() > max_lines {
            lines.remove(0);
        }
    }
    Ok(lines)
}

pub(super) fn is_retryable_magnet_metadata_discovery_error(error: &CoreError) -> bool {
    let CoreError::Internal(message) = error else {
        return false;
    };
    message.contains("magnet metadata fetch failed after discovery retries")
        && message.contains("magnet metadata fetch: no peers discovered")
}

pub(super) fn strip_ansi_controls(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
                continue;
            }
            continue;
        }
        out.push(ch);
    }
    out
}

/// Generate a process-unique peer id with the SwarmOtter client prefix.
pub(super) fn make_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-SW0001-");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x5a17_cafe);
    let mut x = nanos ^ ((std::process::id() as u64) << 32);
    for byte in &mut id[8..] {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *byte = (x & 0xff) as u8;
    }
    id
}

pub(super) fn apply_resolved_metadata(
    t: &mut Torrent,
    real: &swarmotter_core::meta::TorrentMeta,
    state: &EngineState,
) {
    let initialize_files = t.needs_metadata || t.meta.files.len() != real.files.len();
    t.meta = real.clone();
    t.needs_metadata = false;
    t.magnet_info_hash = None;
    t.progress
        .replace_from_bitfield(&state.pieces_have, real.piece_count());
    if initialize_files {
        t.files = real
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
        t.priorities = vec![FilePriority::Normal; real.files.len()];
        t.wanted = vec![true; real.files.len()];
    }
    t.recompute_file_bytes_completed();
    if !t.progress.is_complete() {
        t.seeding_status = SeedingStatus::NotEligible;
    }
}

pub(super) fn automatic_seeding_status(
    torrent: &Torrent,
    global: &swarmotter_core::ratio::SeedingPolicy,
    idle_seconds: u64,
) -> SeedingStatus {
    let accounting = TorrentAccounting {
        downloaded: torrent.downloaded,
        uploaded: torrent.uploaded,
        idle_seconds,
    };
    match ratio::evaluate_seeding(&accounting, global, &torrent.seeding) {
        SeedDecision::Continue => SeedingStatus::Queued,
        SeedDecision::StopOnRatio => SeedingStatus::StoppedRatio,
        SeedDecision::StopOnIdle => SeedingStatus::StoppedIdle,
    }
}

/// Normalize legacy/defaulted seeding fields before restored work is
/// scheduled. A blocked record retains what its pre-block lifecycle implied;
/// all other complete records are re-evaluated against effective targets.
pub(super) fn recompute_restored_seeding_lifecycle(
    torrent: &mut Torrent,
    persisted_state: TorrentState,
    global: &swarmotter_core::ratio::SeedingPolicy,
    now_secs: u64,
) {
    if !torrent.progress.is_complete() {
        torrent.seeding_status = SeedingStatus::NotEligible;
        return;
    }

    if torrent.state == TorrentState::NetworkBlocked {
        torrent.seeding_status = match persisted_state {
            TorrentState::Seeding => SeedingStatus::Active,
            TorrentState::Paused => SeedingStatus::StoppedManual,
            _ if torrent.seeding_status != SeedingStatus::NotEligible => torrent.seeding_status,
            _ => automatic_seeding_status(
                torrent,
                global,
                now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added)),
            ),
        };
        return;
    }

    if torrent.state == TorrentState::Paused {
        torrent.seeding_status = SeedingStatus::StoppedManual;
        return;
    }

    if matches!(
        torrent.state,
        TorrentState::Completed | TorrentState::Seeding
    ) {
        torrent.state = TorrentState::Completed;
        torrent.seeding_status = automatic_seeding_status(
            torrent,
            global,
            now_secs.saturating_sub(torrent.date_completed.unwrap_or(torrent.date_added)),
        );
    } else {
        torrent.seeding_status = SeedingStatus::NotEligible;
    }
}

pub(super) fn make_tracker(url: &str, tier: usize) -> TrackerInfo {
    TrackerInfo {
        id: TrackerId(url.to_string()),
        url: url.to_string(),
        kind: TrackerKind::from_url(url).unwrap_or(TrackerKind::Http),
        tier,
        status: TrackerStatus::NotContacted,
        seeders: 0,
        leechers: 0,
        downloads: 0,
        last_error: None,
        last_message: None,
        next_announce: None,
        last_announce: None,
        scrape_status: TrackerScrapeStatus::NotContacted,
        last_scrape: None,
        scrape_seeders: None,
        scrape_leechers: None,
        scrape_downloads: None,
        last_scrape_error: None,
    }
}

pub(super) fn validate_restored_storage_ownership<'a>(
    torrents: impl IntoIterator<Item = &'a Torrent>,
    config: &Config,
) -> Result<()> {
    let mut ownerships = Vec::new();
    for torrent in torrents {
        let complete_dir =
            resolve_download_dir_from_config(torrent.download_dir.as_deref(), config);
        let active_dir = resolve_incomplete_dir_from_config(&complete_dir, config);
        for root in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
            ownerships.push(
                swarmotter_core::storage::StorageIo::new(torrent.meta.clone(), root)
                    .path_ownership()?,
            );
        }
    }
    for index in 0..ownerships.len() {
        for other in ownerships.iter().skip(index + 1) {
            ownerships[index].ensure_compatible_with(other)?;
        }
    }
    Ok(())
}

pub(super) fn unique_pathbufs<I>(paths: I) -> Vec<PathBuf>
where
    I: IntoIterator<Item = PathBuf>,
{
    let mut out = Vec::new();
    for path in paths {
        if !out.contains(&path) {
            out.push(path);
        }
    }
    out
}

pub(super) fn validated_relative_path(path: &str) -> Result<Vec<String>> {
    if path.trim().is_empty() {
        return Err(CoreError::Storage("renamed path must not be empty".into()));
    }
    let mut components = Vec::new();
    for component in Path::new(path).components() {
        match component {
            std::path::Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| CoreError::Storage("renamed path must be valid UTF-8".into()))?;
                components.push(value.to_string());
            }
            _ => {
                return Err(CoreError::Storage(
                    "renamed path must be relative and must not contain '.' or '..'".into(),
                ));
            }
        }
    }
    if components.is_empty() {
        return Err(CoreError::Storage("renamed path must not be empty".into()));
    }
    Ok(components)
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PayloadRenameOutcome {
    Moved,
    PlaceholderCreated,
}

pub(super) async fn rename_payload_exclusive(
    source: &Path,
    destination: &Path,
) -> Result<PayloadRenameOutcome> {
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let source_metadata = match tokio::fs::symlink_metadata(source).await {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(CoreError::from(error)),
    };
    if source_metadata.is_none() {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .await
            .map_err(|error| {
                CoreError::Storage(format!(
                    "cannot reserve rename destination {}: {error}",
                    destination.display()
                ))
            })?;
        file.sync_all().await.map_err(CoreError::from)?;
        sync_parent_directory(destination).await?;
        return Ok(PayloadRenameOutcome::PlaceholderCreated);
    }
    if !source_metadata.is_some_and(|metadata| metadata.is_file()) {
        return Err(CoreError::Storage(format!(
            "rename source is not a regular file: {}",
            source.display()
        )));
    }

    let move_result: Result<()> = match tokio::fs::hard_link(source, destination).await {
        Ok(()) => Ok(()),
        Err(link_error) => {
            let mut input = tokio::fs::File::open(source)
                .await
                .map_err(CoreError::from)?;
            let mut output = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)
                .await
                .map_err(|error| {
                    CoreError::Storage(format!(
                        "cannot rename {} to {} without replacing data: hard link failed ({link_error}); exclusive copy failed ({error})",
                        source.display(),
                        destination.display()
                    ))
                })?;
            if let Err(error) = tokio::io::copy(&mut input, &mut output).await {
                let _ = tokio::fs::remove_file(destination).await;
                return Err(CoreError::from(error));
            }
            if let Err(error) = output.sync_all().await {
                let _ = tokio::fs::remove_file(destination).await;
                return Err(CoreError::from(error));
            }
            Ok(())
        }
    };
    move_result?;
    if let Err(error) = sync_parent_directory(destination).await {
        let cleanup = tokio::fs::remove_file(destination).await;
        return Err(CoreError::Storage(format!(
            "cannot sync rename destination {}: {error}{}",
            destination.display(),
            cleanup
                .err()
                .map(|cleanup| format!("; destination cleanup failed: {cleanup}"))
                .unwrap_or_default()
        )));
    }
    if let Err(error) = tokio::fs::remove_file(source).await {
        let cleanup = tokio::fs::remove_file(destination).await;
        return Err(CoreError::Storage(format!(
            "cannot remove rename source {}: {error}{}",
            source.display(),
            cleanup
                .err()
                .map(|cleanup| format!("; destination cleanup failed: {cleanup}"))
                .unwrap_or_default()
        )));
    }
    if let Err(error) = sync_parent_directory(source).await {
        let rollback = async {
            tokio::fs::hard_link(destination, source)
                .await
                .map_err(CoreError::from)?;
            tokio::fs::remove_file(destination)
                .await
                .map_err(CoreError::from)?;
            Ok::<(), CoreError>(())
        }
        .await;
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(CoreError::Storage(format!(
                "{error}; rename rollback also failed: {rollback_error}"
            ))),
        };
    }
    Ok(PayloadRenameOutcome::Moved)
}

pub(super) async fn rollback_payload_rename(
    source: &Path,
    destination: &Path,
    outcome: PayloadRenameOutcome,
) -> Result<()> {
    match outcome {
        PayloadRenameOutcome::Moved => {
            if !matches!(
                rename_payload_exclusive(destination, source).await?,
                PayloadRenameOutcome::Moved
            ) {
                return Err(CoreError::Storage(
                    "rename rollback found a missing destination payload".into(),
                ));
            }
        }
        PayloadRenameOutcome::PlaceholderCreated => {
            tokio::fs::remove_file(destination)
                .await
                .map_err(CoreError::from)?;
            sync_parent_directory(destination).await?;
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(super) async fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(parent).and_then(|directory| directory.sync_all())
    })
    .await
    .map_err(|error| CoreError::Storage(format!("sync directory task failed: {error}")))?
    .map_err(CoreError::from)
}

#[cfg(not(unix))]
pub(super) async fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

pub(super) fn torrent_selection_complete(
    torrent: &Torrent,
    have: &swarmotter_core::storage::PieceBitfield,
) -> Result<bool> {
    for piece in 0..torrent.meta.piece_count() {
        let selected = swarmotter_core::storage::piece_file_ranges(&torrent.meta, piece)?
            .into_iter()
            .any(|slice| {
                torrent
                    .wanted
                    .get(slice.file_index)
                    .copied()
                    .unwrap_or(true)
                    && torrent
                        .priorities
                        .get(slice.file_index)
                        .copied()
                        .unwrap_or(FilePriority::Normal)
                        != FilePriority::Unwanted
            });
        if selected && !have.has(piece) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn add_storage_root_role(
    roots: &mut HashMap<String, StorageRootAccumulator>,
    path: String,
    role: StorageRootRole,
) {
    let entry = roots.entry(path).or_default();
    if !entry.roles.contains(&role) {
        entry.roles.push(role);
    }
}

pub(super) fn add_storage_root_usage(
    roots: &mut HashMap<String, StorageRootAccumulator>,
    path: String,
    torrent: &Torrent,
) {
    let entry = roots.entry(path).or_default();
    entry.torrent_count += 1;
    if torrent.state.is_active() {
        entry.active_torrents += 1;
        entry.active_write_rate = entry.active_write_rate.saturating_add(torrent.rate_down);
    }
}

pub(super) fn push_display_path(paths: &mut Vec<String>, path: &Path) {
    let value = path.display().to_string();
    if !paths.contains(&value) {
        paths.push(value);
    }
}

pub(super) async fn remove_directory_contents(path: &Path) -> Result<usize> {
    match tokio::fs::metadata(path).await {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => {
            return Err(CoreError::Storage(format!(
                "reset path is not a directory: {}",
                path.display()
            )));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(path)
                .await
                .map_err(CoreError::from)?;
            return Ok(0);
        }
        Err(e) => return Err(CoreError::from(e)),
    }

    let mut entries = tokio::fs::read_dir(path).await.map_err(CoreError::from)?;
    let mut removed = 0usize;
    while let Some(entry) = entries.next_entry().await.map_err(CoreError::from)? {
        let entry_path = entry.path();
        let meta = tokio::fs::symlink_metadata(&entry_path)
            .await
            .map_err(CoreError::from)?;
        if meta.is_dir() && !meta.file_type().is_symlink() {
            tokio::fs::remove_dir_all(&entry_path)
                .await
                .map_err(CoreError::from)?;
        } else {
            tokio::fs::remove_file(&entry_path)
                .await
                .map_err(CoreError::from)?;
        }
        removed = removed.saturating_add(1);
    }
    Ok(removed)
}

pub(super) async fn truncate_log_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(CoreError::from)?;
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(CoreError::from)?;
    Ok(())
}

/// Apply current network containment state to a torrent's lifecycle state.
pub(super) async fn apply_network_state(t: &mut Torrent, health: &Arc<RwLock<NetworkHealth>>) {
    let h = health.read().await;
    if !h.traffic_allowed && h.mode != NetworkContainmentMode::Disabled {
        t.state = TorrentState::NetworkBlocked;
        t.error = Some(h.detail.clone());
    }
}

pub(super) fn effective_autopilot_mode(
    global_mode: AutopilotMode,
    override_mode: Option<AutopilotMode>,
) -> AutopilotMode {
    if global_mode == AutopilotMode::Disabled {
        AutopilotMode::Disabled
    } else {
        override_mode.unwrap_or(global_mode)
    }
}

pub(super) fn build_autopilot_input(
    torrent: &Torrent,
    state: Option<&EngineState>,
    sample: Option<RateSample>,
    now: Instant,
    network: &NetworkHealth,
) -> AutopilotInput {
    let rate_down = sample.map(|s| s.rate_down).unwrap_or(torrent.rate_down);
    let rate_up = sample.map(|s| s.rate_up).unwrap_or(torrent.rate_up);
    let rate_down_observed_peak = sample
        .map(|s| s.peak_rate_down)
        .unwrap_or(torrent.rate_down)
        .max(rate_down);
    let network_traffic_allowed =
        network.traffic_allowed || network.mode == NetworkContainmentMode::Disabled;

    let no_progress_seconds = latest_progress_instant(sample, state)
        .map(|seen| now.saturating_duration_since(seen).as_secs())
        .or_else(|| sample.map(|s| now.saturating_duration_since(s.at).as_secs()));

    let mut input = AutopilotInput {
        state: torrent.state,
        rate_down,
        rate_up,
        rate_down_observed_peak,
        download_limit: torrent.download_limit,
        piece_count: torrent.meta.piece_count(),
        pieces_have: torrent.pieces_have(),
        known_peers: torrent.known_peers,
        useful_peers: None,
        active_peer_workers: torrent.active_peer_workers,
        tracker_ok: torrent.state.is_active(),
        no_progress_seconds,
        network_traffic_allowed: Some(network_traffic_allowed),
        ..Default::default()
    };

    if let Some(state) = state {
        let piece_count = state.piece_count.max(torrent.meta.piece_count());
        input.piece_count = piece_count;
        input.pieces_have = if state.piece_count > 0 {
            state.pieces_have.count(state.piece_count)
        } else {
            torrent.pieces_have()
        };
        input.known_peers = state.peers.len();
        input.useful_peers = Some(useful_peer_count(&state.peer_health, now));
        input.active_peer_workers = state.active_peers;
        input.discovered_peers = Some(state.peer_scheduler.discovered_peers.max(state.peers.len()));
        input.eligible_peers = Some(state.peer_scheduler.eligible_peers);
        input.peer_worker_limit = Some(state.peer_scheduler.peer_worker_limit);
        input.backed_off_peers = Some(state.peer_scheduler.backed_off_peers);
        input.tracker_ok = state.tracker_ok;
        input.tracker_recent_ok_seconds_ago = instant_age_seconds(now, state.tracker_last_ok);
        input.tracker_failures_recent = state.tracker_failures_recent;
        input.dht_discovery_ok = Some(state.dht_discovery_ok);
        input.dht_last_seen_seconds_ago = instant_age_seconds(now, state.dht_last_seen);
        input.pex_discovery_ok = Some(state.pex_discovery_ok);
        input.pex_last_seen_seconds_ago = instant_age_seconds(now, state.pex_last_seen);
        input.peer_failures_recent = Some(
            state
                .peer_disconnects_recent
                .saturating_add(state.hash_failures)
                .saturating_add(state.timeout_failures),
        );
        input.serial_peer_active = state.peer_scheduler.serial_peer_active;
    }

    input
}

pub(super) fn latest_progress_instant(
    sample: Option<RateSample>,
    state: Option<&EngineState>,
) -> Option<Instant> {
    let mut latest = sample.and_then(|sample| sample.last_download_at);
    if let Some(state) = state {
        for candidate in [
            state.last_valid_block,
            state.block_last_seen,
            state.webseed_last_seen,
        ] {
            if candidate > latest {
                latest = candidate;
            }
        }
    }
    latest
        .or_else(|| sample.and_then(|sample| sample.no_download_since))
        .or_else(|| sample.map(|sample| sample.at))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn log_torrent_throughput_peak(
    hash: &InfoHash,
    torrent: &Torrent,
    state: &EngineState,
    sample_rate_down: u64,
    sample_rate_up: u64,
    previous_peak_rate_down: u64,
    previous_peak_rate_up: u64,
    peak_rate_down: u64,
    peak_rate_up: u64,
    now: Instant,
) {
    tracing::info!(
        info_hash = %hash,
        name = %torrent.name(),
        state = %torrent.state,
        sample_rate_down_bps = sample_rate_down,
        sample_rate_down_mib_s = rate_mib_per_second(sample_rate_down),
        sample_rate_up_bps = sample_rate_up,
        sample_rate_up_mib_s = rate_mib_per_second(sample_rate_up),
        rate_down_bps = torrent.rate_down,
        rate_down_mib_s = rate_mib_per_second(torrent.rate_down),
        rate_up_bps = torrent.rate_up,
        rate_up_mib_s = rate_mib_per_second(torrent.rate_up),
        previous_peak_rate_down_bps = previous_peak_rate_down,
        previous_peak_rate_down_mib_s = rate_mib_per_second(previous_peak_rate_down),
        previous_peak_rate_up_bps = previous_peak_rate_up,
        previous_peak_rate_up_mib_s = rate_mib_per_second(previous_peak_rate_up),
        peak_rate_down_bps = peak_rate_down,
        peak_rate_down_mib_s = rate_mib_per_second(peak_rate_down),
        peak_rate_up_bps = peak_rate_up,
        peak_rate_up_mib_s = rate_mib_per_second(peak_rate_up),
        downloaded = state.downloaded,
        uploaded = state.uploaded,
        active_peer_workers = state.active_peers,
        known_peers = state.peers.len(),
        peer_worker_limit = state.peer_scheduler.peer_worker_limit,
        eligible_peers = state.peer_scheduler.eligible_peers,
        filtered_peers = state.peer_scheduler.filtered_peers,
        failed_peers = state.peer_scheduler.failed_peers,
        backed_off_peers = state.peer_scheduler.backed_off_peers,
        parallel_candidates = state.peer_scheduler.parallel_candidates,
        parallel_workers_started = state.peer_scheduler.parallel_workers_started,
        serial_peer_active = state.peer_scheduler.serial_peer_active,
        scheduler_reason = ?state.peer_scheduler.last_reason,
        useful_peers = useful_peer_count(&state.peer_health, now),
        tracker_ok = state.tracker_ok,
        dht_discovery_ok = state.dht_discovery_ok,
        pex_discovery_ok = state.pex_discovery_ok,
        tracker_last_ok_seconds_ago = ?instant_age_seconds(now, state.tracker_last_ok),
        dht_last_seen_seconds_ago = ?instant_age_seconds(now, state.dht_last_seen),
        pex_last_seen_seconds_ago = ?instant_age_seconds(now, state.pex_last_seen),
        webseed_last_seen_seconds_ago = ?instant_age_seconds(now, state.webseed_last_seen),
        "torrent throughput peak increased"
    );
}

pub(super) fn rate_mib_per_second(bytes_per_second: u64) -> f64 {
    let mib = bytes_per_second as f64 / 1_048_576.0;
    (mib * 100.0).round() / 100.0
}

pub(super) fn smooth_rate(
    previous_rate: u64,
    instantaneous_rate: u64,
    last_activity_at: Option<Instant>,
    now: Instant,
) -> u64 {
    if instantaneous_rate > 0 {
        if previous_rate == 0 {
            instantaneous_rate
        } else {
            ((previous_rate as f64 * 0.65) + (instantaneous_rate as f64 * 0.35)) as u64
        }
    } else if last_activity_at
        .map(|at| now.duration_since(at) <= Duration::from_secs(20))
        .unwrap_or(false)
    {
        ((previous_rate as f64) * 0.85) as u64
    } else {
        0
    }
}

/// Assemble a `HealthInput` from the live engine state and the torrent
/// record. Pulls out every signal the health calculator needs (piece
/// availability, per-peer usefulness, throughput, recent stability,
/// tracker/DHT/PEX freshness, and the network containment health) so that
/// the same scoring function is exercised in tests and in the daemon.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_health_input(
    t: &Torrent,
    piece_count: usize,
    pieces_have: &swarmotter_core::storage::resume::PieceBitfield,
    peer_health: &std::collections::HashMap<std::net::SocketAddr, EnginePeerHealth>,
    tracker_ok: &bool,
    dht_discovery_ok: bool,
    pex_discovery_ok: bool,
    tracker_failures_recent: u32,
    peer_disconnects_recent: u32,
    hash_failures: u32,
    timeout_failures: u32,
    last_valid_block: Option<std::time::Instant>,
    block_last_seen: Option<std::time::Instant>,
    webseed_last_seen: Option<std::time::Instant>,
    dht_last_seen: Option<std::time::Instant>,
    pex_last_seen: Option<std::time::Instant>,
    tracker_last_ok: Option<std::time::Instant>,
    known_peers: usize,
    _tracker_message: Option<&str>,
    rate_down_observed_peak: u64,
    global_download_limit: u64,
    network: NetworkHealth,
) -> HealthInput {
    use std::time::Duration;
    let now = std::time::Instant::now();
    // A "recent" signal is anything seen in the last ~90 seconds.
    let recent_window = Duration::from_secs(90);
    let peer_block_recent = peer_health.values().any(|p| {
        p.last_valid_block
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false)
    });
    let received_block_recently = last_valid_block
        .map(|t| now.duration_since(t) < recent_window)
        .unwrap_or(false)
        || block_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false)
        || webseed_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false)
        || peer_block_recent
        || t.rate_down > 0;
    let webseed_recent_ok = webseed_last_seen
        .map(|t| now.duration_since(t) < recent_window)
        .unwrap_or(false);
    let time_since_last_block = last_valid_block
        .or(block_last_seen)
        .or(webseed_last_seen)
        .map(|t| now.duration_since(t));
    let tracker_recent_ok = *tracker_ok
        || tracker_last_ok
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
    let dht_recent_ok = dht_discovery_ok
        || dht_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
    let pex_recent_ok = pex_discovery_ok
        || pex_last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
    // The engine does not (yet) populate `EnginePeerHealth` automatically for
    // every candidate peer, so derive a coarse per-peer health from what the
    // engine has recorded: peers that have sent a valid block recently are
    // considered useful and unchoked, and peers that have only been seen but
    // not heard from are treated as having no missing pieces.
    let mut peers: Vec<EnginePeerHealth> = Vec::new();
    for p in peer_health.values() {
        let last_valid = p.last_valid_block;
        let last_seen = p.last_seen;
        let last_seen_recent = last_seen
            .map(|t| now.duration_since(t) < recent_window)
            .unwrap_or(false);
        let useful_recently = (p.useful_recently && last_seen_recent)
            || last_valid
                .map(|t| now.duration_since(t) < recent_window)
                .unwrap_or(false);
        let unchoked = (p.unchoked && last_seen_recent) || useful_recently;
        let has_missing = (useful_recently || p.has_missing_pieces) && last_seen_recent;
        peers.push(EnginePeerHealth {
            piece_bitfield: p.piece_bitfield.clone(),
            has_missing_pieces: has_missing,
            unchoked,
            blocked: p.blocked,
            last_valid_block: last_valid,
            useful_recently,
            discovered_from_pex: p.discovered_from_pex,
            last_seen,
        });
    }
    let no_peers_discovered = known_peers == 0 && peers.is_empty() && t.rate_down == 0;
    HealthInput {
        state: t.state,
        private: t.meta.is_private(),
        piece_count,
        pieces_have: pieces_have.clone(),
        peers,
        rate_down: t.rate_down,
        rate_down_observed_peak,
        download_limit: t.download_limit,
        upload_limit: t.upload_limit,
        global_download_limit,
        network: Some(network),
        tracker_ok: *tracker_ok,
        tracker_recent_ok,
        tracker_failures_recent,
        dht_recent_ok,
        pex_recent_ok,
        peer_disconnects_recent,
        hash_failures,
        timeout_failures,
        received_block_recently,
        webseed_recent_ok,
        time_since_last_block,
        known_peers,
        no_peers_discovered,
    }
}
