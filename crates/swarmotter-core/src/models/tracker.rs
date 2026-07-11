// SPDX-License-Identifier: Apache-2.0

//! Tracker models and tiers.

use serde::{Deserialize, Serialize};

/// Kind of tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackerKind {
    Http,
    Https,
    Udp,
    Dht,
}

impl TrackerKind {
    pub fn from_url(url: &str) -> Option<Self> {
        if url.starts_with("https://") {
            Some(Self::Https)
        } else if url.starts_with("http://") {
            Some(Self::Http)
        } else if url.starts_with("udp://") {
            Some(Self::Udp)
        } else if url.starts_with("dht://") || url == "dht" {
            Some(Self::Dht)
        } else {
            None
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Udp => "udp",
            Self::Dht => "dht",
        }
    }
}

/// Tracker status reported by announce/scrape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum TrackerStatus {
    #[default]
    NotContacted,
    Working,
    Updating,
    Ok,
    Error,
    Disabled,
}

/// A tracker URL and its tier grouping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerTier {
    pub tier: usize,
    pub url: String,
}

/// Identifier for a tracker (URL-based).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrackerId(pub String);

/// Tracker info as exposed by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerInfo {
    pub id: TrackerId,
    pub url: String,
    pub kind: TrackerKind,
    pub tier: usize,
    pub status: TrackerStatus,
    pub seeders: u64,
    pub leechers: u64,
    pub downloads: u64,
    pub last_error: Option<String>,
    pub last_message: Option<String>,
    pub next_announce: Option<u64>,
    pub last_announce: Option<u64>,
}

/// Build effective BEP 12 tracker tiers. `announce-list` takes precedence over
/// the legacy single `announce` URL when it is present.
pub fn build_tiers(
    announce: Option<&str>,
    announce_list: Option<&[Vec<String>]>,
) -> Vec<TrackerTier> {
    if let Some(list) = announce_list.filter(|list| !list.is_empty()) {
        let mut out = Vec::new();
        for (i, tier) in list.iter().enumerate() {
            for url in tier {
                out.push(TrackerTier {
                    tier: i,
                    url: url.clone(),
                });
            }
        }
        return out;
    }
    announce
        .map(|url| {
            vec![TrackerTier {
                tier: 0,
                url: url.to_string(),
            }]
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_from_url() {
        assert_eq!(
            TrackerKind::from_url("http://a/announce"),
            Some(TrackerKind::Http)
        );
        assert_eq!(
            TrackerKind::from_url("https://a/announce"),
            Some(TrackerKind::Https)
        );
        assert_eq!(
            TrackerKind::from_url("udp://a:1337"),
            Some(TrackerKind::Udp)
        );
        assert_eq!(TrackerKind::from_url("dht"), Some(TrackerKind::Dht));
        assert_eq!(TrackerKind::from_url("ftp://x"), None);
    }

    #[test]
    fn build_tiers_order() {
        let tiers = build_tiers(
            Some("http://primary/a"),
            Some(&[
                vec!["http://b/a".into()],
                vec!["http://c/a".into(), "http://d/a".into()],
            ]),
        );
        assert_eq!(tiers[0].url, "http://b/a");
        assert_eq!(tiers[0].tier, 0);
        assert_eq!(tiers[2].url, "http://d/a");
        assert_eq!(tiers[2].tier, 1);
    }
}
