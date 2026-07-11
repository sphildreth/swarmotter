// SPDX-License-Identifier: Apache-2.0

//! Ratio and seeding control logic.
//!
//! Implements global and per-torrent ratio limits, idle seed limits, seed-
//! forever option, stop-at-target behavior, and ratio calculation. Pure logic
//! over accounting so it can be unit-tested.

use serde::{Deserialize, Serialize};

/// Seeding policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedingPolicy {
    /// Global ratio limit (uploaded/downloaded). `None` = seed forever.
    #[serde(default = "default_global_ratio_limit")]
    pub global_ratio_limit: Option<f64>,
    /// Global idle seed limit in seconds. `None` = no idle stop.
    #[serde(default = "default_global_idle_limit")]
    pub global_idle_limit: Option<u64>,
}

fn default_global_ratio_limit() -> Option<f64> {
    Some(2.0)
}

fn default_global_idle_limit() -> Option<u64> {
    Some(1800)
}

impl Default for SeedingPolicy {
    fn default() -> Self {
        Self {
            global_ratio_limit: default_global_ratio_limit(),
            global_idle_limit: default_global_idle_limit(),
        }
    }
}

/// Per-torrent seeding settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TorrentSeeding {
    /// Per-torrent ratio limit. `None` = inherit global.
    pub ratio_limit: Option<f64>,
    /// Per-torrent idle limit (seconds). `None` = inherit global.
    pub idle_limit: Option<u64>,
    /// Seed forever overrides limits entirely.
    pub seed_forever: bool,
}

/// Accounting for a torrent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TorrentAccounting {
    pub downloaded: u64,
    pub uploaded: u64,
    /// Seconds since last upload activity.
    pub idle_seconds: u64,
}

impl TorrentAccounting {
    pub fn ratio(&self) -> f64 {
        if self.downloaded == 0 {
            return 0.0;
        }
        self.uploaded as f64 / self.downloaded as f64
    }
}

/// Decision whether seeding should stop for a torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedDecision {
    Continue,
    StopOnRatio,
    StopOnIdle,
}

/// Evaluate whether seeding should continue for a torrent.
pub fn evaluate_seeding(
    acc: &TorrentAccounting,
    global: &SeedingPolicy,
    per: &TorrentSeeding,
) -> SeedDecision {
    if per.seed_forever {
        return SeedDecision::Continue;
    }

    let ratio_limit = per.ratio_limit.or(global.global_ratio_limit);
    if let Some(limit) = ratio_limit {
        if acc.downloaded > 0 && acc.ratio() >= limit {
            return SeedDecision::StopOnRatio;
        }
    }

    let idle_limit = per.idle_limit.or(global.global_idle_limit);
    if let Some(idle) = idle_limit {
        if acc.idle_seconds >= idle {
            return SeedDecision::StopOnIdle;
        }
    }

    SeedDecision::Continue
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_on_global_ratio() {
        let acc = TorrentAccounting {
            downloaded: 1000,
            uploaded: 2000,
            idle_seconds: 0,
        };
        let global = SeedingPolicy {
            global_ratio_limit: Some(2.0),
            global_idle_limit: None,
        };
        let per = TorrentSeeding::default();
        assert_eq!(
            evaluate_seeding(&acc, &global, &per),
            SeedDecision::StopOnRatio
        );
    }

    #[test]
    fn stop_on_idle() {
        let acc = TorrentAccounting {
            downloaded: 1000,
            uploaded: 100,
            idle_seconds: 2000,
        };
        let global = SeedingPolicy {
            global_ratio_limit: Some(5.0),
            global_idle_limit: Some(1000),
        };
        let per = TorrentSeeding::default();
        assert_eq!(
            evaluate_seeding(&acc, &global, &per),
            SeedDecision::StopOnIdle
        );
    }

    #[test]
    fn seed_forever_overrides() {
        let acc = TorrentAccounting {
            downloaded: 1000,
            uploaded: 100000,
            idle_seconds: 999999,
        };
        let global = SeedingPolicy {
            global_ratio_limit: Some(1.0),
            global_idle_limit: Some(10),
        };
        let per = TorrentSeeding {
            seed_forever: true,
            ..Default::default()
        };
        assert_eq!(
            evaluate_seeding(&acc, &global, &per),
            SeedDecision::Continue
        );
    }

    #[test]
    fn per_torrent_ratio_overrides_global() {
        let acc = TorrentAccounting {
            downloaded: 1000,
            uploaded: 500,
            idle_seconds: 0,
        };
        let global = SeedingPolicy {
            global_ratio_limit: Some(2.0),
            global_idle_limit: None,
        };
        let per = TorrentSeeding {
            ratio_limit: Some(0.5),
            ..Default::default()
        };
        assert_eq!(
            evaluate_seeding(&acc, &global, &per),
            SeedDecision::StopOnRatio
        );
    }

    #[test]
    fn no_limit_continues() {
        let acc = TorrentAccounting {
            downloaded: 1000,
            uploaded: 100000,
            idle_seconds: 999999,
        };
        let global = SeedingPolicy {
            global_ratio_limit: None,
            global_idle_limit: None,
        };
        let per = TorrentSeeding::default();
        assert_eq!(
            evaluate_seeding(&acc, &global, &per),
            SeedDecision::Continue
        );
    }

    #[test]
    fn ratio_zero_when_no_download() {
        let acc = TorrentAccounting {
            downloaded: 0,
            uploaded: 100,
            idle_seconds: 0,
        };
        assert_eq!(acc.ratio(), 0.0);
    }
}
