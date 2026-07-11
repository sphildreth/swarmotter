// SPDX-License-Identifier: Apache-2.0

//! Versioned, crash-safe persistence for daemon torrent and queue state.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
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

pub fn load(path: &Path) -> Result<Option<DaemonState>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(CoreError::Storage(format!("read daemon state: {error}"))),
    };
    let state: DaemonState = serde_json::from_slice(&bytes)
        .map_err(|error| CoreError::Storage(format!("parse daemon state: {error}")))?;
    if state.version != STATE_VERSION {
        return Err(CoreError::Storage(format!(
            "unsupported daemon state version {}; expected {STATE_VERSION}",
            state.version
        )));
    }
    Ok(Some(state))
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
    use swarmotter_core::queue::QueueLimits;

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
    fn corrupt_state_is_not_silently_discarded() {
        let path = unique_path("daemon-state-corrupt");
        fs::write(&path, b"not json").unwrap();
        assert!(load(&path).is_err());
        let _ = fs::remove_file(path);
    }
}
