// SPDX-License-Identifier: Apache-2.0

//! Local torrent creation and immediate seeding.
//!
//! Source traversal and streaming hashes run in a blocking worker under the
//! existing storage-root recheck permit. Every source path must resolve inside
//! a configured local storage root, symlinks are rejected, and the complete
//! source snapshot is revalidated after hashing before anything is exported or
//! registered. Announce and peer work still uses the normal contained data
//! plane.

use super::*;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};

use swarmotter_core::creation::{
    build_torrent, validate_creation_component, validate_creation_name, CreationFile,
    CreationFileHashes, CreationHasher, TorrentCreationFormat, TorrentCreationRequest,
    TorrentCreationResult, TorrentCreationSpec, DEFAULT_CREATION_PIECE_LENGTH,
};
use swarmotter_core::hash::{TorrentIdentity, TorrentKey};
use swarmotter_core::meta::{self, MAX_TORRENT_FILES, MAX_TORRENT_METADATA_BYTES};
use swarmotter_core::rss::redact_urls_in_text;

use super::storage_controls::{storage_work_cancelled_error, StorageWorkCancellation};

const CREATION_READ_BUFFER: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceFile {
    absolute: PathBuf,
    relative: Vec<String>,
    length: u64,
    identity: SourceIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceIdentity {
    length: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

impl SourceIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            length: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SourceSnapshot {
    data_root: PathBuf,
    name: String,
    is_multi_file: bool,
    files: Vec<SourceFile>,
    total_length: u64,
}

impl SourceSnapshot {
    pub(super) fn same_source_set(&self, other: &Self) -> bool {
        self.name == other.name
            && self.is_multi_file == other.is_multi_file
            && self.files.len() == other.files.len()
            && self.files.iter().zip(&other.files).all(|(left, right)| {
                left.relative == right.relative
                    && left.length == right.length
                    && left.identity == right.identity
            })
    }
}

struct CreationCancellation {
    token: StorageWorkCancellation,
}

impl CreationCancellation {
    fn new() -> Self {
        Self {
            token: StorageWorkCancellation::new(),
        }
    }
}

impl Drop for CreationCancellation {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

impl DaemonRuntime {
    /// Configured local roots that may contain creation sources.
    pub(super) async fn torrent_creation_roots(&self) -> Vec<String> {
        let config = self.config.read().await.clone();
        creation_source_roots(&config)
            .into_iter()
            .map(|path| path.display().to_string())
            .collect()
    }

    /// Create metainfo from a local file or directory, optionally registering
    /// and immediately seeding the verified source data.
    pub(super) async fn create_torrent(
        &self,
        request: TorrentCreationRequest,
    ) -> Result<TorrentCreationResult> {
        let config = self.config.read().await.clone();
        let roots = creation_source_roots(&config);
        if roots.is_empty() {
            return Err(CoreError::InvalidArgument(
                "torrent creation requires at least one configured storage root (download_dir, incomplete_dir, temp_dir, or storage.root_controls)"
                    .into(),
            ));
        }
        let source = resolve_creation_source(&request.source, &roots)?;
        let format = request.format;
        let snapshot = collect_source(&source, request.name.as_deref())?;
        if snapshot.total_length == 0 {
            return Err(CoreError::InvalidArgument(
                "torrent creation source is empty".into(),
            ));
        }
        if snapshot.files.len() > MAX_TORRENT_FILES {
            return Err(CoreError::InvalidArgument(format!(
                "source contains {} files; maximum is {MAX_TORRENT_FILES}",
                snapshot.files.len()
            )));
        }
        let piece_length = request
            .piece_length
            .unwrap_or(DEFAULT_CREATION_PIECE_LENGTH);
        swarmotter_core::creation::validate_creation_piece_length(piece_length, format)?;
        let tracker_tiers = request
            .tracker_tiers
            .iter()
            .map(|tier| {
                tier.iter()
                    .map(|url| url.trim().to_string())
                    .filter(|url| !url.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|tier| !tier.is_empty())
            .collect::<Vec<_>>();
        let webseeds = request
            .webseeds
            .iter()
            .map(|url| url.trim().to_string())
            .filter(|url| !url.is_empty())
            .collect::<Vec<_>>();
        validate_tracker_tiers(&tracker_tiers)?;
        validate_webseeds(&webseeds)?;
        if request.seed {
            let source_name = source
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| CoreError::InvalidArgument("source has no usable name".into()))?;
            if snapshot.name != source_name {
                return Err(CoreError::InvalidArgument(format!(
                    "immediate seeding requires the torrent name to match the source name {source_name:?}; create an export with a custom name instead"
                )));
            }
        }

        let snapshot_for_hash = snapshot.clone();
        let cancellation = CreationCancellation::new();
        let cancellation_token = cancellation.token.clone();
        let hash_cancel = cancellation_token.clone();
        let hash_root = source.clone();
        let (hashed_files, v1_pieces) = self
            .run_root_scoped_recheck(&hash_root, Some(&cancellation_token), async move {
                tokio::task::spawn_blocking(move || {
                    hash_source(&snapshot_for_hash, format, piece_length, &hash_cancel)
                })
                .await
                .map_err(|error| {
                    CoreError::Storage(format!("torrent creation hashing task: {error}"))
                })?
            })
            .await?;
        drop(cancellation);

        // Re-walk after hashing so a concurrent add/remove/rename cannot be
        // published as a successful creation.
        let after = collect_source(&source, Some(&snapshot.name))?;
        if !snapshot.same_source_set(&after) {
            return Err(CoreError::Storage(
                "source data changed during torrent creation; retry after the file set is stable"
                    .into(),
            ));
        }
        let files = snapshot
            .files
            .iter()
            .zip(hashed_files)
            .map(|(source, hashed)| CreationFile {
                path: source.relative.clone(),
                length: source.length,
                pieces_root: hashed.pieces_root,
                piece_layer: hashed.piece_layer,
            })
            .collect::<Vec<_>>();
        let spec = TorrentCreationSpec {
            name: snapshot.name.clone(),
            piece_length,
            format,
            is_multi_file: snapshot.is_multi_file,
            files,
            v1_pieces,
            tracker_tiers: tracker_tiers.clone(),
            webseeds: webseeds.clone(),
            private: request.private,
        };
        let metainfo = build_torrent(&spec)?;
        if metainfo.len() > MAX_TORRENT_METADATA_BYTES {
            return Err(CoreError::InvalidArgument(format!(
                "generated metainfo is {} bytes; maximum is {MAX_TORRENT_METADATA_BYTES}",
                metainfo.len()
            )));
        }
        let parsed = meta::parse_torrent(&metainfo)?;
        ensure_created_identity(&parsed.identity, format)?;
        let torrent_keys = parsed
            .identity
            .keys()
            .into_iter()
            .map(TorrentKey::to_locator)
            .collect::<Vec<_>>();
        let piece_count = if format.includes_v1() {
            parsed.pieces.len() as u64
        } else {
            parsed.data_piece_count().unwrap_or(0) as u64
        };
        let mut result = TorrentCreationResult {
            name: parsed.name.clone(),
            format,
            total_length: parsed.total_length,
            piece_length: parsed.piece_length,
            piece_count,
            file_count: parsed.files.len(),
            private: parsed.private,
            torrent_keys,
            tracker_tiers: tracker_tiers.clone(),
            webseeds: parsed.webseeds.clone(),
            metainfo_hex: hex::encode(&metainfo),
            metainfo_bytes: metainfo.len(),
            seeded: false,
            seed_state: None,
            seed_error: None,
        };
        if request.seed {
            match self.seed_created_torrent(parsed, &snapshot, metainfo).await {
                Ok((seeded, state)) => {
                    result.seeded = seeded;
                    result.seed_state = state;
                }
                Err(error) => {
                    result.seed_error = Some(redact_urls_in_text(&error.to_string()));
                }
            }
        }
        Ok(result)
    }

    async fn seed_created_torrent(
        &self,
        parsed: meta::TorrentMeta,
        snapshot: &SourceSnapshot,
        metainfo: Vec<u8>,
    ) -> Result<(bool, Option<String>)> {
        let data_root = snapshot.data_root.display().to_string();
        let mut torrent = Torrent::new(parsed, now());
        torrent.download_dir = Some(data_root.clone());
        let options = AddTorrentOptions::request(Some(data_root), false, true, None, Vec::new());
        let paused = self.apply_add_profile(&mut torrent, &options).await?;
        if paused {
            return Err(CoreError::InvalidArgument(
                "immediate seeding cannot use a paused start behavior".into(),
            ));
        }
        let piece_count = torrent
            .meta
            .data_piece_count()
            .unwrap_or_else(|_| torrent.meta.piece_count());
        for piece in 0..piece_count {
            torrent.progress.have_piece(piece);
        }
        torrent.recompute_file_bytes_completed();
        torrent.state = TorrentState::Completed;
        torrent.seeding_status = SeedingStatus::Queued;
        torrent.date_completed = Some(now());
        let key = torrent.key();
        match self
            .add_torrent_mutation_with_original_metainfo(
                torrent,
                false,
                "torrent_creation_seeded",
                Some(metainfo),
            )
            .await
        {
            Ok(TorrentAddMutationOutcome::Inserted { state, .. }) => {
                self.reconcile_queue().await;
                self.reconcile_seeders().await;
                let seeded = self.registry.lock().await.get(&key).is_some_and(|torrent| {
                    matches!(
                        torrent.state,
                        TorrentState::Completed
                            | TorrentState::Seeding
                            | TorrentState::NetworkBlocked
                    )
                });
                Ok((seeded, Some(state.as_str().to_string())))
            }
            Ok(TorrentAddMutationOutcome::Duplicate { .. }) => Ok((
                false,
                Some("duplicate torrent already exists in the library".into()),
            )),
            Err(error) => Err(error),
        }
    }
}

fn resolve_creation_source(source: &str, roots: &[PathBuf]) -> Result<PathBuf> {
    if source.trim().is_empty() {
        return Err(CoreError::InvalidArgument(
            "creation source path must not be empty".into(),
        ));
    }
    let source_path = Path::new(source);
    let symlink_metadata = std::fs::symlink_metadata(source_path).map_err(|error| {
        CoreError::InvalidArgument(format!(
            "cannot inspect creation source {source:?}: {error}"
        ))
    })?;
    if symlink_metadata.file_type().is_symlink() {
        return Err(CoreError::InvalidArgument(
            "creation source must not be a symlink".into(),
        ));
    }
    let canonical = std::fs::canonicalize(source_path).map_err(|error| {
        CoreError::InvalidArgument(format!(
            "cannot resolve creation source {source:?}: {error}"
        ))
    })?;
    if !canonical.is_dir() && !canonical.is_file() {
        return Err(CoreError::InvalidArgument(
            "creation source must be a regular file or directory".into(),
        ));
    }
    if !roots.iter().any(|root| canonical.starts_with(root)) {
        return Err(CoreError::InvalidArgument(format!(
            "creation source {} is outside the configured storage roots",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn creation_source_roots(config: &Config) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for path in [
        config.storage.download_dir.as_deref(),
        config.storage.incomplete_dir.as_deref(),
        config.storage.temp_dir.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        candidates.push(PathBuf::from(path));
    }
    for control in &config.storage.root_controls {
        candidates.push(PathBuf::from(&control.path));
    }
    let mut roots = Vec::new();
    for candidate in candidates {
        if let Ok(canonical) = std::fs::canonicalize(&candidate) {
            if !roots.contains(&canonical) {
                roots.push(canonical);
            }
        }
    }
    roots
}

pub(super) fn collect_source(
    source: &Path,
    requested_name: Option<&str>,
) -> Result<SourceSnapshot> {
    let metadata = std::fs::symlink_metadata(source).map_err(|error| {
        CoreError::InvalidArgument(format!(
            "cannot inspect creation source {}: {error}",
            source.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(CoreError::InvalidArgument(
            "creation source must not be a symlink".into(),
        ));
    }
    let actual_name = source
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CoreError::InvalidArgument("source has no usable name".into()))?
        .to_string();
    let name = requested_name
        .map(str::to_string)
        .unwrap_or_else(|| actual_name.clone());
    validate_creation_name(&name)?;
    if metadata.is_file() {
        let identity = SourceIdentity::from_metadata(&metadata);
        return Ok(SourceSnapshot {
            data_root: source
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            name: name.clone(),
            is_multi_file: false,
            files: vec![SourceFile {
                absolute: source.to_path_buf(),
                relative: vec![name],
                length: metadata.len(),
                identity,
            }],
            total_length: metadata.len(),
        });
    }
    if !metadata.is_dir() {
        return Err(CoreError::InvalidArgument(
            "creation source must be a regular file or directory".into(),
        ));
    }
    let mut files = Vec::new();
    collect_directory(source, &mut vec![name.clone()], &mut files)?;
    files.sort_by(|left, right| left.relative.cmp(&right.relative));
    let total_length = files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.length)
            .ok_or_else(|| CoreError::InvalidArgument("source length exceeds u64".into()))
    })?;
    Ok(SourceSnapshot {
        data_root: source
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
        name,
        is_multi_file: true,
        files,
        total_length,
    })
}

fn collect_directory(
    directory: &Path,
    relative: &mut Vec<String>,
    files: &mut Vec<SourceFile>,
) -> Result<()> {
    let mut entries = std::fs::read_dir(directory)
        .map_err(|error| {
            CoreError::Storage(format!(
                "cannot read creation source directory {}: {error}",
                directory.display()
            ))
        })?
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| {
            CoreError::Storage(format!(
                "cannot read creation source directory {}: {error}",
                directory.display()
            ))
        })?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            CoreError::Storage(format!(
                "cannot inspect creation source {}: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(CoreError::InvalidArgument(format!(
                "creation source contains a symlink at {}; symlinks are not followed",
                path.display()
            )));
        }
        let component = entry
            .file_name()
            .into_string()
            .map_err(|_| CoreError::InvalidArgument("creation source path is not UTF-8".into()))?;
        validate_creation_component(&component, "source file path component")?;
        relative.push(component);
        if metadata.is_dir() {
            collect_directory(&path, relative, files)?;
        } else if metadata.is_file() {
            files.push(SourceFile {
                absolute: path,
                relative: relative.clone(),
                length: metadata.len(),
                identity: SourceIdentity::from_metadata(&metadata),
            });
            if files.len() > MAX_TORRENT_FILES {
                return Err(CoreError::InvalidArgument(format!(
                    "source contains more than {MAX_TORRENT_FILES} files"
                )));
            }
        } else {
            return Err(CoreError::InvalidArgument(format!(
                "creation source contains a non-file entry at {}",
                path.display()
            )));
        }
        relative.pop();
    }
    Ok(())
}

pub(super) fn hash_source(
    snapshot: &SourceSnapshot,
    format: TorrentCreationFormat,
    piece_length: u64,
    cancellation: &StorageWorkCancellation,
) -> Result<(Vec<CreationFileHashes>, Vec<[u8; 20]>)> {
    let mut hasher = CreationHasher::new(piece_length, format)?;
    let mut hashed_files = Vec::with_capacity(snapshot.files.len());
    let mut buffer = vec![0u8; CREATION_READ_BUFFER];
    for source in &snapshot.files {
        if cancellation.is_cancelled() {
            return Err(storage_work_cancelled_error());
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(&source.absolute).map_err(|error| {
            CoreError::Storage(format!(
                "cannot open creation source {}: {error}",
                source.absolute.display()
            ))
        })?;
        let opened = file.metadata().map_err(|error| {
            CoreError::Storage(format!(
                "cannot inspect opened creation source {}: {error}",
                source.absolute.display()
            ))
        })?;
        if SourceIdentity::from_metadata(&opened) != source.identity {
            return Err(CoreError::Storage(format!(
                "source data changed before hashing {}",
                source.absolute.display()
            )));
        }
        hasher.begin_file()?;
        let mut read_total = 0u64;
        loop {
            if cancellation.is_cancelled() {
                return Err(storage_work_cancelled_error());
            }
            let read = file.read(&mut buffer).map_err(|error| {
                CoreError::Storage(format!(
                    "cannot read creation source {}: {error}",
                    source.absolute.display()
                ))
            })?;
            if read == 0 {
                break;
            }
            read_total = read_total.saturating_add(read as u64);
            if read_total > source.length {
                return Err(CoreError::Storage(format!(
                    "source data grew while hashing {}",
                    source.absolute.display()
                )));
            }
            hasher.update(&buffer[..read])?;
        }
        if read_total != source.length {
            return Err(CoreError::Storage(format!(
                "source data changed length while hashing {}",
                source.absolute.display()
            )));
        }
        let after = file.metadata().map_err(|error| {
            CoreError::Storage(format!(
                "cannot re-inspect creation source {}: {error}",
                source.absolute.display()
            ))
        })?;
        if SourceIdentity::from_metadata(&after) != source.identity {
            return Err(CoreError::Storage(format!(
                "source data changed while hashing {}",
                source.absolute.display()
            )));
        }
        let hashes = hasher.finish_file()?;
        hashed_files.push(hashes);
    }
    let v1_pieces = hasher.finish_torrent()?;
    Ok((hashed_files, v1_pieces))
}

fn ensure_created_identity(
    identity: &TorrentIdentity,
    format: TorrentCreationFormat,
) -> Result<()> {
    let matches = match format {
        TorrentCreationFormat::V1 => matches!(identity, TorrentIdentity::V1 { .. }),
        TorrentCreationFormat::V2 => matches!(identity, TorrentIdentity::V2 { .. }),
        TorrentCreationFormat::Hybrid => matches!(identity, TorrentIdentity::Hybrid { .. }),
    };
    if matches {
        Ok(())
    } else {
        Err(CoreError::Internal(format!(
            "generated metainfo has unexpected identity {identity:?} for {} format",
            format.as_str()
        )))
    }
}

fn validate_tracker_tiers(tiers: &[Vec<String>]) -> Result<()> {
    for tier in tiers {
        for url in tier {
            let url = url.trim();
            if url.is_empty() {
                return Err(CoreError::InvalidArgument(
                    "tracker tiers must not contain empty URLs".into(),
                ));
            }
            let parsed = url::Url::parse(url).map_err(|error| {
                CoreError::InvalidArgument(format!("invalid tracker URL {url:?}: {error}"))
            })?;
            if !matches!(parsed.scheme(), "http" | "https" | "udp") || parsed.host().is_none() {
                return Err(CoreError::InvalidArgument(format!(
                    "tracker URL {url:?} must be an http, https, or udp URL with a host"
                )));
            }
        }
    }
    Ok(())
}

fn validate_webseeds(webseeds: &[String]) -> Result<()> {
    for url in webseeds {
        let url = url.trim();
        if url.is_empty() {
            return Err(CoreError::InvalidArgument(
                "webseed URLs must not be empty".into(),
            ));
        }
        let parsed = url::Url::parse(url).map_err(|error| {
            CoreError::InvalidArgument(format!("invalid webseed URL {url:?}: {error}"))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host().is_none() {
            return Err(CoreError::InvalidArgument(format!(
                "webseed URL {url:?} must be an http or https URL with a host"
            )));
        }
    }
    Ok(())
}
