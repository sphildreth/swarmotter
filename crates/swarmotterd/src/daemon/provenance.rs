// SPDX-License-Identifier: Apache-2.0

//! Provenance, deterministic magnet generation, annotation, and metainfo
//! archive export.
//!
//! These operations are local/control-plane only. They never reconstruct a
//! source `.torrent` document and never initiate torrent network traffic.

use super::*;
use swarmotter_core::magnet::{magnet_for_meta, MagnetTrackerPolicy};
use swarmotter_core::models::torrent::TorrentSummary;

const MAX_ANNOTATION_BYTES: usize = 16 * 1024;
const TAR_BLOCK: usize = 512;

impl DaemonRuntime {
    pub(super) async fn set_torrent_annotation(
        &self,
        hash: &TorrentKey,
        annotation: Option<String>,
    ) -> Result<TorrentSummary> {
        let canonical = self.require_torrent_key(*hash).await?;
        let annotation = annotation
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if annotation
            .as_deref()
            .is_some_and(|value| value.len() > MAX_ANNOTATION_BYTES)
        {
            return Err(CoreError::InvalidArgument(format!(
                "annotation exceeds {MAX_ANNOTATION_BYTES} bytes"
            )));
        }
        let previous = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(&canonical)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let previous = torrent.clone();
            torrent.annotation = annotation;
            torrent.annotation_updated_at = Some(now());
            torrent.annotation_updated_by = Some("api".into());
            previous
        };
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(&canonical) {
                *torrent = previous;
            }
            return Err(error);
        }
        self.registry
            .lock()
            .await
            .get(&canonical)
            .map(Torrent::to_summary)
            .ok_or_else(|| CoreError::NotFound("torrent".into()))
    }

    pub(super) async fn torrent_magnet(
        &self,
        hash: &TorrentKey,
        tracker_policy: MagnetTrackerPolicy,
        include_private_trackers: bool,
    ) -> Result<String> {
        let canonical = self.require_torrent_key(*hash).await?;
        let torrent = self
            .registry
            .lock()
            .await
            .get(&canonical)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        let selected = torrent
            .files
            .iter()
            .filter(|file| file.wanted)
            .map(|file| file.index)
            .collect::<Vec<_>>();
        magnet_for_meta(
            &torrent.meta,
            &selected,
            tracker_policy,
            include_private_trackers,
        )
    }

    /// Build a deterministic tar archive of retained original metainfo. Each
    /// unavailable item is represented in `manifest.json`; no item is ever
    /// reconstructed from canonical metadata or magnet data.
    pub(super) async fn export_metainfo_archive(&self, hashes: Vec<TorrentKey>) -> Result<Vec<u8>> {
        let mut seen = std::collections::HashSet::new();
        let mut manifest_entries = Vec::new();
        let mut archive_entries = Vec::new();
        for (index, hash) in hashes.into_iter().enumerate() {
            let canonical = self.canonical_torrent_key(hash).await;
            let Some(canonical) = canonical else {
                manifest_entries.push(ArchiveManifestEntry {
                    requested_key: hash.to_locator(),
                    canonical_key: None,
                    name: None,
                    status: "unavailable".into(),
                    filename: None,
                    error: Some("torrent not found".into()),
                });
                continue;
            };
            if !seen.insert(canonical) {
                continue;
            }
            let torrent = self
                .registry
                .lock()
                .await
                .get(&canonical)
                .cloned()
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let key = canonical.to_locator();
            let filename = format!(
                "metainfo/{:04}-{}.torrent",
                index + 1,
                &key[..key.len().min(16)]
            );
            match self.retained_original_metainfo(canonical).await {
                Ok(bytes) => {
                    manifest_entries.push(ArchiveManifestEntry {
                        requested_key: hash.to_locator(),
                        canonical_key: Some(key),
                        name: Some(torrent.meta.name.clone()),
                        status: "included".into(),
                        filename: Some(filename.clone()),
                        error: None,
                    });
                    archive_entries.push((filename, bytes));
                }
                Err(error) => {
                    manifest_entries.push(ArchiveManifestEntry {
                        requested_key: hash.to_locator(),
                        canonical_key: Some(key),
                        name: Some(torrent.meta.name.clone()),
                        status: "unavailable".into(),
                        filename: None,
                        error: Some(error.to_string()),
                    });
                }
            }
        }
        let manifest = ArchiveManifest {
            format: "swarmotter-metainfo-export-v1".into(),
            entries: manifest_entries,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| CoreError::Internal(format!("serialize export manifest: {error}")))?;
        let mut entries = vec![("manifest.json".to_string(), manifest_bytes)];
        entries.extend(archive_entries);
        build_tar(&entries)
    }
}

#[derive(Debug, serde::Serialize)]
struct ArchiveManifest {
    format: String,
    entries: Vec<ArchiveManifestEntry>,
}

#[derive(Debug, serde::Serialize)]
struct ArchiveManifestEntry {
    requested_key: String,
    canonical_key: Option<String>,
    name: Option<String>,
    status: String,
    filename: Option<String>,
    error: Option<String>,
}

fn build_tar(entries: &[(String, Vec<u8>)]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for (name, bytes) in entries {
        if name.len() > 100 {
            return Err(CoreError::Internal(
                "metainfo archive entry name exceeds tar limits".into(),
            ));
        }
        let header = tar_header(name, bytes.len());
        output.extend_from_slice(&header);
        output.extend_from_slice(bytes);
        let padding = (TAR_BLOCK - (bytes.len() % TAR_BLOCK)) % TAR_BLOCK;
        output.extend(std::iter::repeat_n(0u8, padding));
    }
    output.extend(std::iter::repeat_n(0u8, TAR_BLOCK * 2));
    Ok(output)
}

fn tar_header(name: &str, size: usize) -> [u8; TAR_BLOCK] {
    let mut header = [0u8; TAR_BLOCK];
    header[..name.len()].copy_from_slice(name.as_bytes());
    header[100..108].copy_from_slice(b"0000644\0");
    header[108..116].copy_from_slice(b"0000000\0");
    header[116..124].copy_from_slice(b"0000000\0");
    let size_field = format!("{:011o}\0", size);
    header[124..136].copy_from_slice(size_field.as_bytes());
    header[136..148].copy_from_slice(b"00000000000\0");
    header[148..156].copy_from_slice(b"        ");
    header[156] = b'0';
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum = header.iter().map(|byte| u64::from(*byte)).sum::<u64>();
    let checksum_field = format!("{:06o}\0 ", checksum);
    header[148..156].copy_from_slice(checksum_field.as_bytes());
    header
}
