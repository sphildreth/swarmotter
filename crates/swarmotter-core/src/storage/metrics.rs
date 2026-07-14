// SPDX-License-Identifier: Apache-2.0

//! Lightweight, process-local storage I/O throughput accounting.
//!
//! These counters are observational only. They never decide whether a write
//! or verification is allowed, so a metrics failure cannot weaken storage
//! correctness or the daemon's network containment guarantees.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Rolling local-storage throughput snapshot for one configured root.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageThroughput {
    pub write_bytes_per_second: u64,
    pub verification_bytes_per_second: u64,
}

/// Shared counters supplied to every [`super::StorageIo`] associated with one
/// daemon storage root.
#[derive(Clone)]
pub struct StorageIoMetrics {
    inner: Arc<StorageIoMetricsInner>,
}

struct StorageIoMetricsInner {
    written_bytes: AtomicU64,
    verified_bytes: AtomicU64,
    sample: Mutex<Sample>,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    at: Instant,
    written_bytes: u64,
    verified_bytes: u64,
    throughput: StorageThroughput,
}

impl Default for StorageIoMetrics {
    fn default() -> Self {
        Self {
            inner: Arc::new(StorageIoMetricsInner {
                written_bytes: AtomicU64::new(0),
                verified_bytes: AtomicU64::new(0),
                sample: Mutex::new(Sample {
                    at: Instant::now(),
                    written_bytes: 0,
                    verified_bytes: 0,
                    throughput: StorageThroughput::default(),
                }),
            }),
        }
    }
}

impl StorageIoMetrics {
    /// Record bytes successfully committed through a payload write call.
    pub fn record_payload_write(&self, bytes: u64) {
        self.inner.written_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes successfully read specifically for piece verification.
    /// Seeder reads are intentionally excluded so this remains a verification
    /// diagnostic rather than a generic read-I/O counter.
    pub fn record_verification_read(&self, bytes: u64) {
        self.inner
            .verified_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Return a bounded rolling estimate. Snapshots taken more frequently
    /// than one second retain the latest completed sample; the first partial
    /// interval is normalized to one second so recent activity is visible
    /// without a blocking wait.
    pub fn throughput(&self) -> StorageThroughput {
        let now = Instant::now();
        let written_bytes = self.inner.written_bytes.load(Ordering::Relaxed);
        let verified_bytes = self.inner.verified_bytes.load(Ordering::Relaxed);
        let mut sample = lock_unpoisoned(&self.inner.sample);
        let elapsed = now.saturating_duration_since(sample.at);
        let delta_written = written_bytes.saturating_sub(sample.written_bytes);
        let delta_verified = verified_bytes.saturating_sub(sample.verified_bytes);
        if elapsed.as_secs() == 0 {
            return StorageThroughput {
                write_bytes_per_second: sample.throughput.write_bytes_per_second.max(delta_written),
                verification_bytes_per_second: sample
                    .throughput
                    .verification_bytes_per_second
                    .max(delta_verified),
            };
        }
        let seconds = elapsed.as_secs_f64();
        let throughput = StorageThroughput {
            write_bytes_per_second: (delta_written as f64 / seconds).min(u64::MAX as f64) as u64,
            verification_bytes_per_second: (delta_verified as f64 / seconds).min(u64::MAX as f64)
                as u64,
        };
        *sample = Sample {
            at: now,
            written_bytes,
            verified_bytes,
            throughput,
        };
        throughput
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immediate_samples_expose_recorded_storage_activity() {
        let metrics = StorageIoMetrics::default();
        metrics.record_payload_write(42);
        metrics.record_verification_read(24);

        assert_eq!(
            metrics.throughput(),
            StorageThroughput {
                write_bytes_per_second: 42,
                verification_bytes_per_second: 24,
            }
        );
    }
}
