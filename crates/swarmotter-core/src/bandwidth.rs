// SPDX-License-Identifier: Apache-2.0

//! Bandwidth limiting logic.
//!
//! Implements global and per-torrent download/upload rate limits and an
//! alternate speed mode. The limiter is a token-bucket-style accounting over a
//! tick window; actual socket-level shaping lives in the network layer. This
//! module provides the pure scheduling logic so it can be unit-tested.

use serde::{Deserialize, Serialize};
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
}
