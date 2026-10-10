// SPDX-License-Identifier: Apache-2.0
//! Process-local persistence counters. Durations are cumulative microseconds;
//! the largest individual operation is retained to make stalls visible.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static SAVES: AtomicU64 = AtomicU64::new(0);
static FAILURES: AtomicU64 = AtomicU64::new(0);
static WAIT_US: AtomicU64 = AtomicU64::new(0);
static SCAN_US: AtomicU64 = AtomicU64::new(0);
static TOTAL_US: AtomicU64 = AtomicU64::new(0);
static MAX_US: AtomicU64 = AtomicU64::new(0);
pub static SERIALIZED_BYTES: AtomicU64 = AtomicU64::new(0);
pub static WRITE_US: AtomicU64 = AtomicU64::new(0);

pub struct SaveTimer {
    started: Instant,
    success: bool,
}
impl Default for SaveTimer {
    fn default() -> Self {
        Self::new()
    }
}
impl SaveTimer {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            success: false,
        }
    }
    pub fn locked(&self) {
        WAIT_US.fetch_add(micros(self.started), Ordering::Relaxed);
    }
    pub fn finish(&mut self, success: bool) {
        self.success = success;
    }
}
impl Drop for SaveTimer {
    fn drop(&mut self) {
        let elapsed = micros(self.started);
        SAVES.fetch_add(1, Ordering::Relaxed);
        TOTAL_US.fetch_add(elapsed, Ordering::Relaxed);
        MAX_US.fetch_max(elapsed, Ordering::Relaxed);
        if !self.success {
            FAILURES.fetch_add(1, Ordering::Relaxed);
        }
    }
}
pub fn micros(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}
pub fn scanned(start: Instant) {
    SCAN_US.fetch_add(micros(start), Ordering::Relaxed);
}
pub fn summary() -> String {
    format!("saves={} failures={} lock_wait_us={} snapshot_us={} sqlite_us={} total_us={} max_us={} serialized_bytes={}",
        SAVES.load(Ordering::Relaxed), FAILURES.load(Ordering::Relaxed), WAIT_US.load(Ordering::Relaxed),
        SCAN_US.load(Ordering::Relaxed), WRITE_US.load(Ordering::Relaxed), TOTAL_US.load(Ordering::Relaxed),
        MAX_US.load(Ordering::Relaxed), SERIALIZED_BYTES.load(Ordering::Relaxed))
}
