// SPDX-License-Identifier: Apache-2.0

//! Bandwidth limiting logic.
//!
//! Implements global and per-torrent download/upload rate limits and an
//! alternate speed mode. The limiter is a token-bucket-style accounting over a
//! tick window; actual socket-level shaping lives in the network layer. This
//! module provides the pure scheduling logic so it can be unit-tested.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const REFILL_IN_PROGRESS: u64 = 1 << 63;
const REFILL_TIME_MASK: u64 = !REFILL_IN_PROGRESS;

/// Bandwidth limits.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BandwidthLimits {
    /// Bytes/sec global download (0 = unlimited).
    #[serde(default)]
    pub global_download: u64,
    /// Bytes/sec global upload (0 = unlimited).
    #[serde(default)]
    pub global_upload: u64,
    /// Alternate speed limits (applied when enabled).
    #[serde(default)]
    pub alt_download: u64,
    #[serde(default)]
    pub alt_upload: u64,
    /// Whether alternate speed mode is active.
    #[serde(default)]
    pub alt_enabled: bool,
    /// Max peers globally (0 = unlimited).
    #[serde(default)]
    pub max_peers: usize,
    /// Max peers per torrent (0 = daemon default worker pool).
    #[serde(default)]
    pub max_peers_per_torrent: usize,
}

impl BandwidthLimits {
    /// Effective download limit (bytes/sec), respecting alt mode.
    pub fn effective_download(&self) -> u64 {
        if self.alt_enabled {
            self.alt_download
        } else {
            self.global_download
        }
    }

    /// Effective upload limit (bytes/sec), respecting alt mode.
    pub fn effective_upload(&self) -> u64 {
        if self.alt_enabled {
            self.alt_upload
        } else {
            self.global_upload
        }
    }
}

/// Per-torrent bandwidth limits.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TorrentBandwidth {
    pub download: u64,
    pub upload: u64,
}

/// A lock-free token-bucket limiter state using atomic operations.
///
/// This implementation uses CAS (compare-and-swap) loops to perform refill
/// and consume operations without mutex contention. With 1000 concurrent
/// torrents, this eliminates the serialization point that existed with
/// `tokio::sync::Mutex<TokenBucket>`.
#[derive(Debug)]
pub struct AtomicTokenBucket {
    tokens: AtomicU64,
    capacity: AtomicU64,
    last_refill_ms: AtomicU64,
}

impl AtomicTokenBucket {
    pub fn new(capacity_per_sec: u64, now_ms: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity_per_sec),
            capacity: AtomicU64::new(capacity_per_sec),
            last_refill_ms: AtomicU64::new(now_ms),
        }
    }

    /// Attempt to refill and consume `want` tokens.
    /// Returns the amount actually allowed. Only one caller may apply a refill
    /// window at a time, which prevents concurrent consumers from double-counting
    /// the same elapsed time.
    pub fn refill_and_consume(&self, now_ms: u64, want: u64) -> u64 {
        if self.capacity.load(Ordering::Relaxed) == 0 {
            return want; // unlimited
        }

        self.refill(now_ms & REFILL_TIME_MASK);
        self.consume(want)
    }

    fn refill(&self, now_ms: u64) {
        loop {
            let last_raw = self.last_refill_ms.load(Ordering::Acquire);
            if last_raw & REFILL_IN_PROGRESS != 0 {
                std::hint::spin_loop();
                continue;
            }

            let last_refill = last_raw & REFILL_TIME_MASK;
            if now_ms <= last_refill {
                return;
            }

            match self.last_refill_ms.compare_exchange_weak(
                last_raw,
                now_ms | REFILL_IN_PROGRESS,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let capacity = self.capacity.load(Ordering::Acquire);
                    if capacity > 0 {
                        let elapsed = now_ms.saturating_sub(last_refill);
                        let add = (capacity as u128) * (elapsed as u128) / 1000;
                        if add > 0 {
                            self.add_tokens(add, capacity);
                        }
                    }
                    self.last_refill_ms.store(now_ms, Ordering::Release);
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    fn add_tokens(&self, add: u128, capacity: u64) {
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            let refilled = ((current as u128 + add).min(capacity as u128)) as u64;
            if self
                .tokens
                .compare_exchange_weak(current, refilled, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn consume(&self, want: u64) -> u64 {
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            let allowed = want.min(current);
            let new_tokens = current - allowed;
            if self
                .tokens
                .compare_exchange_weak(current, new_tokens, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return allowed;
            }
        }
    }

    /// Available tokens (approximate, as it may change concurrently).
    pub fn available(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }

    /// Current capacity (bytes/sec), or 0 for unlimited.
    pub fn capacity(&self) -> u64 {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Update the capacity (bytes/sec) at runtime.
    pub fn set_capacity(&self, capacity_per_sec: u64) {
        self.capacity.store(capacity_per_sec, Ordering::Release);
        // Cap tokens if they exceed new capacity
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current <= capacity_per_sec {
                break;
            }
            if self
                .tokens
                .compare_exchange_weak(
                    current,
                    capacity_per_sec,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                break;
            }
        }
    }
}

/// A live async rate limiter combining a download and an upload token bucket,
/// driven by `tokio::time`. `acquire(direction, bytes)` consumes tokens and
/// sleeps until enough are available, enforcing global and per-torrent rate
/// limits in the real peer read/write paths. A capacity of 0 means unlimited.
///
/// This implementation uses lock-free atomic operations for the token buckets,
/// eliminating mutex contention when many torrents share the same global limiter.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    pub download: Arc<AtomicTokenBucket>,
    pub upload: Arc<AtomicTokenBucket>,
}

/// Direction of a rate-limited transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDirection {
    Download,
    Upload,
}

impl RateLimiter {
    /// Build a rate limiter from effective byte/sec limits (0 = unlimited).
    pub fn new(download_bps: u64, upload_bps: u64) -> Self {
        let now_ms = now_millis();
        Self {
            download: Arc::new(AtomicTokenBucket::new(download_bps, now_ms)),
            upload: Arc::new(AtomicTokenBucket::new(upload_bps, now_ms)),
        }
    }

    /// An unlimited limiter (no shaping).
    pub fn unlimited() -> Self {
        Self::new(0, 0)
    }

    /// Consume `bytes` of the given direction, sleeping in small increments
    /// until the bucket can afford them. With capacity 0 this returns
    /// immediately (unlimited).
    pub async fn acquire(&self, dir: RateDirection, mut bytes: u64) {
        if bytes == 0 {
            return;
        }
        let bucket = match dir {
            RateDirection::Download => &self.download,
            RateDirection::Upload => &self.upload,
        };
        loop {
            let cap = bucket.capacity();
            if cap == 0 {
                return; // unlimited
            }

            let allowed = bucket.refill_and_consume(now_millis(), bytes);

            if allowed >= bytes {
                return;
            }
            bytes -= allowed;
            // Sleep long enough to accrue the residual tokens, bounded.
            let sleep_ms = if cap == 0 {
                1
            } else {
                ((bytes as u128) * 1000 / cap as u128).max(1) as u64
            };
            tokio::time::sleep(Duration::from_millis(sleep_ms.min(500))).await;
        }
    }

    /// Current configured capacity for a direction (0 = unlimited).
    pub fn capacity(&self, dir: RateDirection) -> u64 {
        let bucket = match dir {
            RateDirection::Download => &self.download,
            RateDirection::Upload => &self.upload,
        };
        bucket.capacity()
    }

    /// Update the configured capacity for a direction at runtime (0 =
    /// unlimited). Because [`RateLimiter`] holds its buckets behind an
    /// `Arc<AtomicTokenBucket>`, a cheap [`Clone`] shares the same underlying bucket,
    /// so updating a cloned limiter updates the live view of all holders (used
    /// by the daemon to adjust per-torrent and global limits on the fly).
    pub fn set_capacity(&self, dir: RateDirection, capacity_per_sec: u64) {
        let bucket = match dir {
            RateDirection::Download => &self.download,
            RateDirection::Upload => &self.upload,
        };
        bucket.set_capacity(capacity_per_sec);
    }
}

/// A composite limiter enforcing an optional per-torrent cap and an optional
/// shared global cap. A transfer acquires from both: the per-torrent bucket
/// caps this torrent's rate, and the shared global bucket caps aggregate
/// traffic across all torrents/seeders that share the same `RateLimiter`
/// instance. A limit of 0 means unlimited for that layer.
///
/// The daemon holds a single shared global `RateLimiter` (cloned into every
/// engine and seeder) and one per-torrent `RateLimiter` per torrent, so both
/// the global and per-torrent bandwidth limits are enforced live.
#[derive(Debug, Clone)]
pub struct ShapedLimiter {
    pub per_torrent: RateLimiter,
    pub global: Option<RateLimiter>,
}

impl ShapedLimiter {
    /// Wrap a per-torrent limiter with no shared global cap.
    pub fn from_rate_limiter(per_torrent: RateLimiter) -> Self {
        Self {
            per_torrent,
            global: None,
        }
    }

    /// No shaping at all (per-torrent unlimited, no global cap).
    pub fn unlimited() -> Self {
        Self::from_rate_limiter(RateLimiter::unlimited())
    }

    /// Attach a shared global limiter (consumes and returns self).
    pub fn with_global(self, global: RateLimiter) -> Self {
        Self {
            per_torrent: self.per_torrent,
            global: Some(global),
        }
    }

    /// Consume `bytes` of `dir`, sleeping until both the per-torrent and the
    /// shared global buckets can afford them. Unlimited layers return
    /// immediately.
    pub async fn acquire(&self, dir: RateDirection, bytes: u64) {
        self.per_torrent.acquire(dir, bytes).await;
        if let Some(global) = &self.global {
            global.acquire(dir, bytes).await;
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Compute allowed bytes given a per-second limit and elapsed time.
pub fn allowed_bytes(limit: u64, elapsed: Duration) -> u64 {
    if limit == 0 {
        return u64::MAX;
    }
    let ns = elapsed.as_nanos();
    ((limit as u128) * ns / 1_000_000_000) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alt_mode_uses_alt_limits() {
        let mut b = BandwidthLimits {
            global_download: 100,
            global_upload: 50,
            alt_download: 10,
            alt_upload: 5,
            alt_enabled: false,
            ..Default::default()
        };
        assert_eq!(b.effective_download(), 100);
        b.alt_enabled = true;
        assert_eq!(b.effective_download(), 10);
        assert_eq!(b.effective_upload(), 5);
    }

    #[test]
    fn zero_means_unlimited() {
        let b = BandwidthLimits::default();
        assert_eq!(b.effective_download(), 0);
        assert_eq!(allowed_bytes(0, Duration::from_secs(1)), u64::MAX);
    }

    #[test]
    fn atomic_token_bucket_refill_and_consume() {
        let tb = AtomicTokenBucket::new(1000, 0);
        assert_eq!(tb.refill_and_consume(0, 500), 500);
        assert_eq!(tb.available(), 500);
        assert_eq!(tb.refill_and_consume(500, 0), 0);
        assert_eq!(tb.available(), 1000);
        assert_eq!(tb.refill_and_consume(500, 0), 0);
        assert_eq!(tb.available(), 1000);
        assert_eq!(tb.refill_and_consume(500, 1500), 1000);
    }

    #[test]
    fn atomic_token_bucket_unlimited_when_zero() {
        let tb = AtomicTokenBucket::new(0, 0);
        assert_eq!(tb.refill_and_consume(0, 999_999), 999_999);
    }

    #[test]
    fn atomic_token_bucket_set_capacity_caps_tokens() {
        let tb = AtomicTokenBucket::new(10_000, 0);
        assert_eq!(tb.available(), 10_000);
        tb.set_capacity(5_000);
        assert_eq!(tb.capacity(), 5_000);
        assert!(tb.available() <= 5_000);
    }

    #[test]
    fn atomic_token_bucket_concurrent_consume() {
        use std::sync::Arc;
        use std::thread;

        let tb = Arc::new(AtomicTokenBucket::new(100_000, 0));
        let mut handles = vec![];

        for _ in 0..100 {
            let tb_clone = Arc::clone(&tb);
            handles.push(thread::spawn(move || tb_clone.refill_and_consume(0, 1000)));
        }

        let total_consumed: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

        assert_eq!(total_consumed, 100_000);
        assert_eq!(tb.available(), 0);
    }

    #[test]
    fn atomic_token_bucket_concurrent_consume_no_overallocation() {
        use std::sync::Arc;
        use std::thread;

        let tb = Arc::new(AtomicTokenBucket::new(10_000, 0));
        let mut handles = vec![];

        for _ in 0..200 {
            let tb_clone = Arc::clone(&tb);
            handles.push(thread::spawn(move || tb_clone.refill_and_consume(0, 100)));
        }

        let total_consumed: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

        assert_eq!(total_consumed, 10_000);
        assert_eq!(tb.available(), 0);
    }

    #[test]
    fn atomic_token_bucket_concurrent_refill_no_overallocation() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        for _ in 0..25 {
            let tb = Arc::new(AtomicTokenBucket::new(1_000, 0));
            assert_eq!(tb.refill_and_consume(0, 1_000), 1_000);
            assert_eq!(tb.available(), 0);

            let workers = 64;
            let barrier = Arc::new(Barrier::new(workers));
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                let tb_clone = Arc::clone(&tb);
                let barrier_clone = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    barrier_clone.wait();
                    tb_clone.refill_and_consume(1_000, 1_000)
                }));
            }

            let total_consumed: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
            assert_eq!(total_consumed, 1_000);
            assert_eq!(tb.available(), 0);
        }
    }

    #[tokio::test]
    async fn rate_limiter_unlimited_returns_immediately() {
        let rl = RateLimiter::unlimited();
        let start = std::time::Instant::now();
        rl.acquire(RateDirection::Download, 1_000_000).await;
        rl.acquire(RateDirection::Upload, 1_000_000).await;
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
    }

    #[tokio::test]
    async fn rate_limiter_throttles_download() {
        let rl = RateLimiter::new(10_000, 0);
        let start = std::time::Instant::now();
        rl.acquire(RateDirection::Download, 20_000).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(800),
            "expected throttling, elapsed {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(2500),
            "throttling too aggressive: {elapsed:?}"
        );
    }

    #[test]
    fn set_capacity_updates_live_bucket_of_clones() {
        let rl = RateLimiter::new(0, 0);
        let twin = rl.clone();
        rl.set_capacity(RateDirection::Download, 5_000);
        assert_eq!(twin.capacity(RateDirection::Download), 5_000);
    }

    #[tokio::test]
    async fn shaped_limiter_enforces_per_torrent_and_global() {
        let per = RateLimiter::new(4_000, 0);
        let global = RateLimiter::new(1_000, 0);
        let shaped = ShapedLimiter::from_rate_limiter(per).with_global(global);
        let start = std::time::Instant::now();
        shaped.acquire(RateDirection::Download, 5_000).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(800),
            "expected global cap to dominate, elapsed {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn shaped_limiter_unlimited_is_fast() {
        let shaped = ShapedLimiter::unlimited();
        let start = std::time::Instant::now();
        shaped.acquire(RateDirection::Download, 1_000_000).await;
        shaped.acquire(RateDirection::Upload, 1_000_000).await;
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
    }

    #[tokio::test]
    async fn atomic_rate_limiter_concurrent_acquire_no_overallocation() {
        use std::sync::Arc;

        let rl = Arc::new(RateLimiter::new(100_000, 0));
        let mut handles = vec![];

        for _ in 0..200 {
            let rl_clone = Arc::clone(&rl);
            handles.push(tokio::spawn(async move {
                rl_clone.acquire(RateDirection::Download, 500).await;
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let available = rl.download.available();
        assert!(
            available < 10_000,
            "Expected bucket to be nearly empty, but has {available} tokens"
        );
    }

    #[tokio::test]
    async fn atomic_rate_limiter_1000_concurrent_acquires() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let capacity: u64 = 1_000_000;
        let rl = Arc::new(RateLimiter::new(capacity, 0));
        let total_acquired = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];

        let request_size: u64 = 1_000;
        let num_tasks: usize = 1000;

        for _ in 0..num_tasks {
            let rl_clone = Arc::clone(&rl);
            let total = Arc::clone(&total_acquired);
            handles.push(tokio::spawn(async move {
                rl_clone
                    .acquire(RateDirection::Download, request_size)
                    .await;
                total.fetch_add(request_size, Ordering::Relaxed);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let acquired = total_acquired.load(Ordering::Relaxed);
        assert_eq!(acquired, (num_tasks as u64) * request_size);
    }
}
