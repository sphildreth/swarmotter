// SPDX-License-Identifier: Apache-2.0

//! Versioned, crash-safe persistence for daemon torrent and queue state.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::queue::QueueState;
use swarmotter_core::torrent::Torrent;

const STATE_VERSION: u32 = 1;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
pub struct DaemonState {
    version: u32,
    pub torrents: Vec<Torrent>,
    pub queue: QueueState,
}

impl DaemonState {
    pub fn new(torrents: Vec<Torrent>, queue: QueueState) -> Self {
        Self {
            version: STATE_VERSION,
            torrents,
            queue,
        }
    }
}

#[derive(Deserialize)]
struct StoredDaemonState {
    version: u32,
    torrents: TorrentRecords,
    queue: QueueState,
}

struct TorrentRecords(Vec<Torrent>);

impl<'de> Deserialize<'de> for TorrentRecords {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RecordsVisitor;

        impl<'de> Visitor<'de> for RecordsVisitor {
            type Value = TorrentRecords;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an array of durable torrent records")
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut records = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
                loop {
                    let record_index = records.len();
                    let value = match sequence.next_element::<serde_json::Value>() {
                        Ok(Some(value)) => value,
                        Ok(None) => break,
                        Err(error) => {
                            return Err(A::Error::custom(format!(
                                "torrent record {record_index}: {error}"
                            )));
                        }
                    };
                    let hash = value
                        .get("meta")
                        .and_then(|meta| meta.get("info_hash"))
                        .and_then(serde_json::Value::as_str)
                        .and_then(|hash| swarmotter_core::hash::InfoHash::from_hex(hash).ok())
                        .map(|hash| hash.to_hex());
                    match serde_json::from_value::<Torrent>(value) {
                        Ok(torrent) => records.push(torrent),
                        Err(error) => {
                            let record = hash.map_or_else(
                                || format!("torrent record {record_index}"),
                                |hash| format!("torrent record {record_index} (info hash {hash})"),
                            );
                            return Err(A::Error::custom(format!("{record}: {error}")));
                        }
                    }
                }
                Ok(TorrentRecords(records))
            }
        }

        deserializer.deserialize_seq(RecordsVisitor)
    }
}

pub fn load(path: &Path) -> Result<Option<DaemonState>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(CoreError::Storage(format!("read daemon state: {error}"))),
    };
    let stored: StoredDaemonState = serde_json::from_slice(&bytes)
        .map_err(|error| CoreError::Storage(format!("parse daemon state: {error}")))?;
    if stored.version != STATE_VERSION {
        return Err(CoreError::Storage(format!(
            "unsupported daemon state version {}; expected {STATE_VERSION}",
            stored.version
        )));
    }
    Ok(Some(DaemonState {
        version: stored.version,
        torrents: stored.torrents.0,
        queue: stored.queue,
    }))
}

pub fn save(path: &Path, state: &DaemonState) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|error| CoreError::Storage(format!("serialize daemon state: {error}")))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| CoreError::Storage(format!("create state directory: {error}")))?;
    let temp = temp_path(path);
    let result = write_and_replace(&temp, path, parent, &bytes);
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn temp_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.json");
    path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), sequence))
}

fn write_and_replace(temp: &Path, path: &Path, parent: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(temp)
        .map_err(|error| CoreError::Storage(format!("create temporary daemon state: {error}")))?;
    file.write_all(bytes)
        .map_err(|error| CoreError::Storage(format!("write daemon state: {error}")))?;
    file.sync_all()
        .map_err(|error| CoreError::Storage(format!("sync daemon state: {error}")))?;
    drop(file);
    fs::rename(temp, path)
        .map_err(|error| CoreError::Storage(format!("replace daemon state: {error}")))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| CoreError::Storage(format!("sync state directory: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
    use swarmotter_core::models::torrent::SeedingStatus;
    use swarmotter_core::queue::QueueLimits;
    use swarmotter_core::ratio::TorrentSeeding;

    fn unique_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "swarmotter-{label}-{}-{}.json",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn state_write_is_atomic_and_round_trips() {
        let path = unique_path("daemon-state");
        let state = DaemonState::new(Vec::new(), QueueState::new(QueueLimits::default()));
        save(&path, &state).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert!(loaded.torrents.is_empty());
        assert!(loaded.queue.order.is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn contextual_torrent_record_deserializer_round_trips_valid_state() {
        let path = unique_path("daemon-state-torrent-record");
        let bytes = build_single_file_torrent(
            "state.bin",
            b"generated lawful state payload",
            8,
            None,
            false,
        );
        let torrent = Torrent::new(parse_torrent(&bytes).unwrap(), 1);
        let expected_hash = torrent.info_hash();
        let state = DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default()));
        save(&path, &state).unwrap();

        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.torrents.len(), 1);
        assert_eq!(loaded.torrents[0].info_hash(), expected_hash);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn legacy_state_defaults_absent_seeding_fields_without_version_bump() {
        let path = unique_path("daemon-state-legacy-seeding");
        let bytes = build_single_file_torrent("state.bin", b"generated payload", 8, None, false);
        let torrent = Torrent::new(parse_torrent(&bytes).unwrap(), 1);
        let state = DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default()));
        let mut json = serde_json::to_value(&state).unwrap();
        json["torrents"][0]
            .as_object_mut()
            .unwrap()
            .remove("seeding");
        json["torrents"][0]
            .as_object_mut()
            .unwrap()
            .remove("seeding_status");
        fs::write(&path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();

        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.torrents[0].seeding, TorrentSeeding::default());
        assert_eq!(
            loaded.torrents[0].seeding_status,
            SeedingStatus::NotEligible
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn every_seeding_status_round_trips_in_version_one_state() {
        let path = unique_path("daemon-state-seeding-statuses");
        let statuses = [
            SeedingStatus::NotEligible,
            SeedingStatus::Queued,
            SeedingStatus::Active,
            SeedingStatus::StoppedRatio,
            SeedingStatus::StoppedIdle,
            SeedingStatus::StoppedManual,
        ];
        for status in statuses {
            let bytes = build_single_file_torrent(
                &format!("state-{}.bin", status.as_str()),
                b"generated payload",
                8,
                None,
                false,
            );
            let mut torrent = Torrent::new(parse_torrent(&bytes).unwrap(), 1);
            torrent.seeding = TorrentSeeding {
                ratio_limit: Some(1.25),
                idle_limit: Some(42),
                seed_forever: false,
            };
            torrent.seeding_status = status;
            let state = DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default()));
            save(&path, &state).unwrap();
            let loaded = load(&path).unwrap().unwrap();
            assert_eq!(loaded.torrents[0].seeding_status, status);
            assert_eq!(loaded.torrents[0].seeding.ratio_limit, Some(1.25));
            let _ = fs::remove_file(&path);
        }
    }

    #[test]
    fn corrupt_state_is_not_silently_discarded() {
        let path = unique_path("daemon-state-corrupt");
        fs::write(&path, b"not json").unwrap();
        assert!(load(&path).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn durable_piece_hash_lengths_are_checked_with_record_and_piece_context() {
        let bytes =
            build_single_file_torrent("state.bin", b"two generated lawful pieces", 8, None, false);
        let torrent = Torrent::new(parse_torrent(&bytes).unwrap(), 1);
        let expected_hash = torrent.info_hash().to_hex();
        let state = DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default()));

        for decoded_len in [0usize, 19, 20, 21] {
            let path = unique_path(&format!("daemon-state-piece-hash-{decoded_len}"));
            let encoded_payload = "ab".repeat(decoded_len);
            let mut json = serde_json::to_value(&state).unwrap();
            json["torrents"][0]["meta"]["pieces"][1] =
                serde_json::Value::String(encoded_payload.clone());
            fs::write(&path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();

            if decoded_len == 20 {
                let loaded = load(&path).unwrap().unwrap();
                assert_eq!(loaded.torrents.len(), 1);
                assert_eq!(loaded.torrents[0].meta.pieces[1], [0xabu8; 20]);
            } else {
                let error = load(&path).unwrap_err().to_string();
                assert!(
                    error.contains("torrent record 0"),
                    "record context for decoded length {decoded_len}: {error}"
                );
                assert!(
                    error.contains(&expected_hash),
                    "hash context for decoded length {decoded_len}: {error}"
                );
                assert!(
                    error.contains("piece hash 1"),
                    "piece context for decoded length {decoded_len}: {error}"
                );
                assert!(
                    error.contains(&format!("length {decoded_len}")),
                    "decoded length context for {decoded_len}: {error}"
                );
                assert!(
                    !error.contains("state.bin"),
                    "content path leaked for decoded length {decoded_len}: {error}"
                );
                if !encoded_payload.is_empty() {
                    assert!(
                        !error.contains(&encoded_payload),
                        "piece-hash payload leaked for decoded length {decoded_len}: {error}"
                    );
                }
            }
            let _ = fs::remove_file(path);
        }
    }
}
