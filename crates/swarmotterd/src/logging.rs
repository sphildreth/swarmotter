// SPDX-License-Identifier: Apache-2.0

//! Daemon logging setup.
//!
//! Logs always go to stderr for terminal/systemd use. File logging is enabled
//! by default and writes to a simple per-user state path unless configured.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use swarmotter_core::config::LoggingConfig;
use swarmotter_core::error::{CoreError, Result};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct LogWriter {
    file: Option<Arc<Mutex<File>>>,
}

struct LogWriterGuard {
    stderr: io::Stderr,
    file: Option<Arc<Mutex<File>>>,
}

impl<'a> MakeWriter<'a> for LogWriter {
    type Writer = LogWriterGuard;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriterGuard {
            stderr: io::stderr(),
            file: self.file.clone(),
        }
    }
}

impl Write for LogWriterGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stderr.write_all(buf)?;
        if let Some(file) = &self.file {
            file.lock()
                .map_err(|_| io::Error::other("log file lock poisoned"))?
                .write_all(buf)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stderr.flush()?;
        if let Some(file) = &self.file {
            file.lock()
                .map_err(|_| io::Error::other("log file lock poisoned"))?
                .flush()?;
        }
        Ok(())
    }
}

/// Initialize daemon logging and return the file path when file logging is on.
pub fn init(config: &LoggingConfig) -> Result<Option<PathBuf>> {
    let log_path = if config.file {
        let path = config
            .file_path
            .as_deref()
            .map(expand_tilde)
            .unwrap_or_else(default_log_path);
        Some(prepare_log_file(&path).map(|file| (path, file))?)
    } else {
        None
    };

    let log_file = match &log_path {
        Some((_, file)) => Some(Arc::new(Mutex::new(
            file.try_clone().map_err(CoreError::from)?,
        ))),
        None => None,
    };
    let writer = LogWriter { file: log_file };
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.level.as_str()))
        .map_err(|e| CoreError::InvalidConfig(format!("logging.level: {e}")))?;

    if config.json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_writer(writer)
            .try_init()
            .map_err(|e| CoreError::Internal(format!("failed to initialize logging: {e}")))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(writer)
            .try_init()
            .map_err(|e| CoreError::Internal(format!("failed to initialize logging: {e}")))?;
    }

    Ok(log_path.map(|(path, _)| path))
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
