// SPDX-License-Identifier: Apache-2.0

//! Bandwidth limiting logic.
//!
//! Implements global and per-torrent download/upload rate limits and an
//! alternate speed mode. The limiter is a token-bucket-style accounting over a
//! tick window; actual socket-level shaping lives in the network layer. This
//! module provides the pure scheduling logic so it can be unit-tested.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Bandwidth limits.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BandwidthLimits {
    /// Bytes/sec global download (0 = unlimited).
    pub global_download: u64,
    /// Bytes/sec global upload (0 = unlimited).
    pub global_upload: u64,
    /// Alternate speed limits (applied when enabled).
    pub alt_download: u64,
    pub alt_upload: u64,
    /// Whether alternate speed mode is active.
    pub alt_enabled: bool,
    /// Max peers globally (0 = unlimited).
    pub max_peers: usize,
    /// Max peers per torrent (0 = unlimited).
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

/// A simple token-bucket limiter state.
#[derive(Debug, Clone, Default)]
pub struct TokenBucket {
    tokens: u64,
    capacity: u64,
    last_refill_ms: u128,
}

impl TokenBucket {
    pub fn new(capacity_per_sec: u64, now_ms: u128) -> Self {
        Self {
            tokens: capacity_per_sec,
            capacity: capacity_per_sec,
            last_refill_ms: now_ms,
        }
    }

    /// Refill the bucket given elapsed time. Returns nothing; mutates state.
    pub fn refill(&mut self, now_ms: u128) {
        if self.capacity == 0 {
            return;
        }
        let elapsed = now_ms.saturating_sub(self.last_refill_ms);
        let add = (self.capacity as u128) * elapsed / 1000;
        self.tokens = (self.tokens + add as u64).min(self.capacity);
        self.last_refill_ms = now_ms;
    }

    /// Attempt to consume `want` tokens; returns amount actually allowed.
    pub fn consume(&mut self, want: u64) -> u64 {
        if self.capacity == 0 {
            return want; // unlimited
        }
        let allowed = want.min(self.tokens);
        self.tokens -= allowed;
        allowed
    }

    /// Available tokens.
    pub fn available(&self) -> u64 {
        self.tokens
    }

    /// Current capacity (bytes/sec), or 0 for unlimited.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Update the capacity (bytes/sec) at runtime, preserving the refill clock.
    pub fn set_capacity(&mut self, capacity_per_sec: u64) {
        self.capacity = capacity_per_sec;
        if self.tokens > capacity_per_sec {
            self.tokens = capacity_per_sec;
        }
    }
}

/// A live async rate limiter combining a download and an upload token bucket,
/// driven by `tokio::time`. `acquire(direction, bytes)` consumes tokens and
/// sleeps until enough are available, enforcing global and per-torrent rate
/// limits in the real peer read/write paths. A capacity of 0 means unlimited.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    pub download: Arc<tokio::sync::Mutex<TokenBucket>>,
    pub upload: Arc<tokio::sync::Mutex<TokenBucket>>,
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
            download: Arc::new(tokio::sync::Mutex::new(TokenBucket::new(
                download_bps,
                now_ms,
            ))),
            upload: Arc::new(tokio::sync::Mutex::new(TokenBucket::new(
                upload_bps, now_ms,
            ))),
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
            let allowed;
            let cap;
            {
                let mut tb = bucket.lock().await;
                if tb.capacity() == 0 {
                    return; // unlimited
                }
                tb.refill(now_millis());
                allowed = tb.consume(bytes);
                cap = tb.capacity();
            }
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
    pub async fn capacity(&self, dir: RateDirection) -> u64 {
        let bucket = match dir {
            RateDirection::Download => &self.download,
            RateDirection::Upload => &self.upload,
        };
        bucket.lock().await.capacity()
    }

    /// Update the configured capacity for a direction at runtime (0 =
    /// unlimited). Because [`RateLimiter`] holds its buckets behind an
    /// `Arc<Mutex<...>>`, a cheap [`Clone`] shares the same underlying bucket,
    /// so updating a cloned limiter updates the live view of all holders (used
    /// by the daemon to adjust per-torrent and global limits on the fly).
    pub async fn set_capacity(&self, dir: RateDirection, capacity_per_sec: u64) {
        let bucket = match dir {
            RateDirection::Download => &self.download,
            RateDirection::Upload => &self.upload,
        };
        bucket.lock().await.set_capacity(capacity_per_sec);
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

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
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
    fn token_bucket_refill_and_consume() {
        let mut tb = TokenBucket::new(1000, 0);
        assert_eq!(tb.consume(500), 500);
        assert_eq!(tb.available(), 500);
        // Refill 1000 tokens/sec over 500ms => 500 tokens.
        tb.refill(500);
        assert_eq!(tb.available(), 1000);
        tb.refill(500); // another 500ms, but capped at capacity
        assert_eq!(tb.available(), 1000);
        assert_eq!(tb.consume(1500), 1000);
    }

    #[test]
    fn token_bucket_unlimited_when_zero() {
        let mut tb = TokenBucket::new(0, 0);
        assert_eq!(tb.consume(999_999), 999_999);
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
        // 10_000 bytes/sec cap. The bucket starts full (10_000 tokens), so
        // requesting 20_000 requires ~1s for the second half to accrue.
        let rl = RateLimiter::new(10_000, 0);
        let start = std::time::Instant::now();
        rl.acquire(RateDirection::Download, 20_000).await;
        let elapsed = start.elapsed();
        // Allow generous tolerance for scheduler jitter; must be throttled.
        assert!(
            elapsed >= std::time::Duration::from_millis(800),
            "expected throttling, elapsed {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(2500),
            "throttling too aggressive: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn set_capacity_updates_live_bucket_of_clones() {
        // RateLimiter clones share buckets, so set_capacity on one is visible
        // to the other (the daemon relies on this for live limit changes).
        let rl = RateLimiter::new(0, 0);
        let twin = rl.clone();
        rl.set_capacity(RateDirection::Download, 5_000).await;
        assert_eq!(twin.capacity(RateDirection::Download).await, 5_000);
    }

    #[tokio::test]
    async fn shaped_limiter_enforces_per_torrent_and_global() {
        // Per-torrent cap 4_000 B/s and a tighter global cap 1_000 B/s. Both
        // start full, so requesting 5_000 bytes must wait for the global
        // bucket (1s for the 4_000 residual beyond its initial 1_000).
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
}
