// SPDX-License-Identifier: Apache-2.0

//! Daemon logging setup.
//!
//! Logs always go to stderr for terminal/systemd use. File logging is enabled
//! by default and writes to a simple per-user state path unless configured.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc,
};
use std::time::Duration;

use swarmotter_core::config::LoggingConfig;
use swarmotter_core::error::{CoreError, Result};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::EnvFilter;

const MAX_RECORD: usize = 64 * 1024;
const MAX_FILE: u64 = 10 * 1024 * 1024;
const ARCHIVES: usize = 5;
static DROPPED: AtomicU64 = AtomicU64::new(0);
static WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);

pub fn counters() -> (u64, u64) {
    (
        DROPPED.load(Ordering::Relaxed),
        WRITE_ERRORS.load(Ordering::Relaxed),
    )
}

enum Record {
    Line(Vec<u8>),
    Flush(mpsc::SyncSender<()>),
}
#[derive(Clone)]
struct LogWriter {
    sender: mpsc::SyncSender<Record>,
}
struct LogWriterGuard {
    sender: mpsc::SyncSender<Record>,
    bytes: Vec<u8>,
    oversized: bool,
}

impl<'a> MakeWriter<'a> for LogWriter {
    type Writer = LogWriterGuard;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriterGuard {
            sender: self.sender.clone(),
            bytes: Vec::new(),
            oversized: false,
        }
    }
}
impl Write for LogWriterGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(buf.len()) <= MAX_RECORD && !self.oversized {
            self.bytes.extend_from_slice(buf);
        } else {
            self.oversized = true;
            self.bytes.clear();
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Drop for LogWriterGuard {
    fn drop(&mut self) {
        if self.oversized
            || self
                .sender
                .try_send(Record::Line(std::mem::take(&mut self.bytes)))
                .is_err()
        {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct RotatingFile {
    path: PathBuf,
    file: Option<File>,
    size: u64,
    limit: u64,
}
impl RotatingFile {
    fn write_record(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.size.saturating_add(bytes.len() as u64) > self.limit {
            self.file.take();
            for index in (1..=ARCHIVES).rev() {
                let from = if index == 1 {
                    self.path.clone()
                } else {
                    archive_path(&self.path, index - 1)
                };
                let to = archive_path(&self.path, index);
                match std::fs::rename(from, to) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            self.size = 0;
        }
        if self.file.is_none() {
            self.file = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?,
            );
        }
        if let Some(file) = &mut self.file {
            file.write_all(bytes)?;
        }
        self.size += bytes.len() as u64;
        Ok(())
    }
}
fn archive_path(path: &Path, index: usize) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

/// Retain through shutdown. Flush is bounded even if stderr or disk has stalled.
pub struct LoggingGuard {
    pub path: Option<PathBuf>,
    sender: mpsc::SyncSender<Record>,
}
impl LoggingGuard {
    pub fn flush(&self) {
        let (tx, rx) = mpsc::sync_channel(1);
        if self.sender.try_send(Record::Flush(tx)).is_ok() {
            let _ = rx.recv_timeout(Duration::from_secs(1));
        }
    }
}
impl Drop for LoggingGuard {
    fn drop(&mut self) {
        self.flush();
    }
}

/// A bounded worker owns both output sinks; callers never wait for I/O.
pub fn init(config: &LoggingConfig) -> Result<LoggingGuard> {
    let path = config.file.then(|| {
        config
            .file_path
            .as_deref()
            .map(expand_tilde)
            .unwrap_or_else(default_log_path)
    });
    let mut file = if let Some(path) = &path {
        let file = prepare_log_file(path)?;
        let size = file.metadata()?.len();
        Some(RotatingFile {
            path: path.clone(),
            file: Some(file),
            size,
            limit: MAX_FILE,
        })
    } else {
        None
    };
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.level.as_str()))
        .map_err(|e| CoreError::InvalidConfig(format!("logging.level: {e}")))?;
    let (sender, receiver) = mpsc::sync_channel(1024);
    std::thread::Builder::new()
        .name("swarmotter-logs".into())
        .spawn(move || {
            let mut stderr = io::stderr();
            while let Ok(record) = receiver.recv() {
                match record {
                    Record::Line(bytes) => {
                        if stderr.write_all(&bytes).is_err() {
                            WRITE_ERRORS.fetch_add(1, Ordering::Relaxed);
                        }
                        if let Some(file) = &mut file {
                            if file.write_record(&bytes).is_err() {
                                WRITE_ERRORS.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Record::Flush(done) => {
                        let _ = stderr.flush();
                        let _ = done.send(());
                    }
                }
            }
        })
        .map_err(CoreError::from)?;
    let writer = LogWriter {
        sender: sender.clone(),
    };
    if config.json {
        tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_env_filter(filter)
            .with_writer(writer)
            .try_init()
            .map_err(|e| CoreError::Internal(format!("failed to initialize logging: {e}")))?;
    } else {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_env_filter(filter)
            .with_writer(writer)
            .try_init()
            .map_err(|e| CoreError::Internal(format!("failed to initialize logging: {e}")))?;
    }
    Ok(LoggingGuard { path, sender })
}

fn prepare_log_file(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(CoreError::from)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(CoreError::from)
}

fn default_log_path() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(xdg)
            .join("swarmotter")
            .join("swarmotterd.log");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("swarmotter")
            .join("swarmotterd.log");
    }
    PathBuf::from("swarmotterd.log")
}

fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
    }
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_home_prefix() {
        if let Some(home) = std::env::var_os("HOME") {
            assert_eq!(expand_tilde("~/x"), PathBuf::from(home).join("x"));
        }
    }
}

#[cfg(test)]
mod reliability_tests {
    use super::*;
    #[test]
    fn slow_sink_cannot_block_producer_and_oversized_records_are_bounded() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        sender.try_send(Record::Line(vec![])).unwrap();
        let writer = LogWriter { sender };
        let mut record = writer.make_writer();
        record.write_all(&vec![0; MAX_RECORD + 1]).unwrap();
        assert!(record.bytes.is_empty());
        assert!(record.oversized);
        drop(record);
        drop(writer.make_writer()); // Full channel must return immediately.
    }
    #[test]
    fn rotation_bounds_archives_and_keeps_newest_records() {
        let path =
            std::env::temp_dir().join(format!("swarmotter-rotation-{}.log", std::process::id()));
        let file = prepare_log_file(&path).unwrap();
        let mut writer = RotatingFile {
            path: path.clone(),
            file: Some(file),
            size: 0,
            limit: 4,
        };
        for i in 0..12 {
            writer.write_record(format!("{i:03}\n").as_bytes()).unwrap();
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "011\n");
        assert_eq!(
            std::fs::read_to_string(archive_path(&path, 5)).unwrap(),
            "006\n"
        );
        assert!(!archive_path(&path, 6).exists());
        drop(writer);
        std::fs::remove_file(&path).unwrap();
        for i in 1..=5 {
            std::fs::remove_file(archive_path(&path, i)).unwrap();
        }
    }
}
