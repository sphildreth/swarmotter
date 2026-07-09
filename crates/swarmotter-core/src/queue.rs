// SPDX-License-Identifier: Apache-2.0

//! Queue management logic.
//!
//! Implements global active download/seed limits, queue ordering (up/down/
//! top/bottom), start-now/bypass, and per-torrent paused state. The logic is
//! pure over a queue state so it can be unit-tested without the daemon.

use crate::hash::InfoHash;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Queue limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueLimits {
    /// Max simultaneously downloading torrents (0 = unlimited).
    #[serde(default = "default_max_active_downloads")]
    pub max_active_downloads: usize,
    /// Max simultaneous magnet metadata fetches (0 = unlimited).
    #[serde(default = "default_max_active_metadata_fetches")]
    pub max_active_metadata_fetches: usize,
    /// Max simultaneously seeding torrents (0 = unlimited).
    #[serde(default = "default_max_active_seeds")]
    pub max_active_seeds: usize,
    /// Whether newly added torrents auto-start or queue.
    #[serde(default = "default_auto_start")]
    pub auto_start: bool,
}

fn default_max_active_downloads() -> usize {
    5
}

fn default_max_active_metadata_fetches() -> usize {
    100
}

fn default_max_active_seeds() -> usize {
    5
}

fn default_auto_start() -> bool {
    true
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_active_downloads: default_max_active_downloads(),
            max_active_metadata_fetches: default_max_active_metadata_fetches(),
            max_active_seeds: default_max_active_seeds(),
            auto_start: default_auto_start(),
        }
    }
}

/// A queued torrent entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub info_hash: InfoHash,
    pub position: usize,
    pub bypass_queue: bool,
    pub paused: bool,
}

/// Queue state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueState {
    pub limits: QueueLimits,
    /// Ordered list of info hashes (position = index).
    pub order: Vec<InfoHash>,
    pub bypass: Vec<InfoHash>,

    #[serde(skip, default)]
    order_set: HashSet<InfoHash>,
    #[serde(skip, default)]
    bypass_set: HashSet<InfoHash>,
}

impl QueueState {
    pub fn new(limits: QueueLimits) -> Self {
        let mut state = Self {
            limits,
            order: Vec::new(),
            bypass: Vec::new(),
            order_set: HashSet::new(),
            bypass_set: HashSet::new(),
        };
        state.rebuild_membership_sets();
        state
    }

    fn rebuild_membership_sets(&mut self) {
        self.order_set = self.order.iter().copied().collect();
        self.bypass_set = self.bypass.iter().copied().collect();
    }

    fn sync_membership_sets(&mut self) {
        if self.order_set.len() != self.order.len() || self.bypass_set.len() != self.bypass.len() {
            self.rebuild_membership_sets();
        }
    }

    /// Add many torrents to the end of the queue.
    pub fn add_many<I: IntoIterator<Item = InfoHash>>(&mut self, hashes: I) {
        self.sync_membership_sets();
        for hash in hashes {
            if self.order_set.insert(hash) {
                self.order.push(hash);
            }
        }
    }

    /// Add a torrent to the end of the queue.
    pub fn add(&mut self, hash: InfoHash) {
        self.sync_membership_sets();
        if self.order_set.insert(hash) {
            self.order.push(hash);
        }
    }

    /// Remove a torrent from the queue.
    pub fn remove(&mut self, hash: &InfoHash) {
        self.sync_membership_sets();
        if self.order_set.remove(hash) {
            self.order.retain(|h| h != hash);
        }
        self.bypass_set.remove(hash);
        self.bypass.retain(|h| h != hash);
    }

    /// Remove many torrents from the queue.
    pub fn remove_many<I: IntoIterator<Item = InfoHash>>(&mut self, hashes: I) {
        self.sync_membership_sets();
        let to_remove: HashSet<InfoHash> = hashes.into_iter().collect();
        if to_remove.is_empty() {
            return;
        }
        self.order.retain(|h| !to_remove.contains(h));
        self.bypass.retain(|h| !to_remove.contains(h));
        self.rebuild_membership_sets();
    }

    /// Move a torrent up one position.
    pub fn move_up(&mut self, hash: &InfoHash) {
        self.sync_membership_sets();
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            if i > 0 {
                self.order.swap(i, i - 1);
            }
        }
    }

    /// Move a torrent down one position.
    pub fn move_down(&mut self, hash: &InfoHash) {
        self.sync_membership_sets();
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            if i + 1 < self.order.len() {
                self.order.swap(i, i + 1);
            }
        }
    }

    /// Move a torrent to the top of the queue.
    pub fn move_to_top(&mut self, hash: &InfoHash) {
        self.sync_membership_sets();
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            let h = self.order.remove(i);
            self.order.insert(0, h);
        }
    }

    /// Move a torrent to the bottom of the queue.
    pub fn move_to_bottom(&mut self, hash: &InfoHash) {
        self.sync_membership_sets();
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            let h = self.order.remove(i);
            self.order.push(h);
        }
    }

    /// Move many torrents to the bottom of the queue.
    pub fn move_many_to_bottom<I: IntoIterator<Item = InfoHash>>(&mut self, hashes: I) {
        self.sync_membership_sets();
        let to_move: HashSet<InfoHash> = hashes.into_iter().collect();
        if to_move.is_empty() {
            return;
        }
        let mut kept = Vec::with_capacity(self.order.len());
        let mut moved = Vec::new();
        for hash in self.order.drain(..) {
            if to_move.contains(&hash) {
                moved.push(hash);
            } else {
                kept.push(hash);
            }
        }
        kept.extend(moved);
        self.order = kept;
    }

    /// Mark a torrent to start now (bypass queue).
    pub fn start_now(&mut self, hash: &InfoHash) {
        self.sync_membership_sets();
        if self.bypass_set.insert(*hash) {
            self.bypass.push(*hash);
        }
    }

    /// Clear bypass flag.
    pub fn clear_bypass(&mut self, hash: &InfoHash) {
        self.sync_membership_sets();
        self.bypass.retain(|h| h != hash);
        self.bypass_set.remove(hash);
    }

    /// Clear bypass flags.
    pub fn clear_bypass_many<I: IntoIterator<Item = InfoHash>>(&mut self, hashes: I) {
        self.sync_membership_sets();
        let clear_set: HashSet<InfoHash> = hashes.into_iter().collect();
        if clear_set.is_empty() {
            return;
        }
        self.bypass.retain(|h| !clear_set.contains(h));
        self.bypass_set.retain(|h| !clear_set.contains(h));
    }

    /// Position of a torrent (1-based) or None if not queued.
    pub fn position(&self, hash: &InfoHash) -> Option<usize> {
        self.order.iter().position(|h| h == hash).map(|i| i + 1)
    }

    /// Determine which torrents are allowed to download given the limits.
    /// Returns info hashes allowed to be active.
    pub fn active_download_slots(&self) -> Vec<InfoHash> {
        let limit = self.limits.max_active_downloads;
        if limit == 0 {
            return self.order.clone();
        }
        let mut active: Vec<InfoHash> = self.bypass.to_vec();
        let mut active_set: HashSet<InfoHash> = self.bypass.iter().copied().collect();
        for h in &self.order {
            if active.len() >= limit {
                break;
            }
            if active_set.insert(*h) {
                active.push(*h);
            }
        }
        active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u32) -> InfoHash {
        let mut bytes = [0u8; 20];
        bytes[0] = ((n >> 24) & 0xff) as u8;
        bytes[1] = ((n >> 16) & 0xff) as u8;
        bytes[2] = ((n >> 8) & 0xff) as u8;
        bytes[3] = (n & 0xff) as u8;
        InfoHash::from_bytes(bytes)
    }

    #[test]
    fn queue_order_operations() {
        let mut q = QueueState::new(QueueLimits::default());
        q.add(h(1));
        q.add(h(2));
        q.add(h(3));
        assert_eq!(q.order, vec![h(1), h(2), h(3)]);
        q.move_up(&h(3));
        assert_eq!(q.order, vec![h(1), h(3), h(2)]);
        q.move_to_top(&h(2));
        assert_eq!(q.order, vec![h(2), h(1), h(3)]);
        q.move_to_bottom(&h(2));
        assert_eq!(q.order, vec![h(1), h(3), h(2)]);
        q.move_down(&h(1));
        assert_eq!(q.order, vec![h(3), h(1), h(2)]);
        q.remove(&h(1));
        assert_eq!(q.order, vec![h(3), h(2)]);
    }

    #[test]
    fn active_download_slots_respect_limit() {
        let mut q = QueueState::new(QueueLimits {
            max_active_downloads: 2,
            max_active_metadata_fetches: 100,
            max_active_seeds: 0,
            auto_start: true,
        });
        for n in 1..=4 {
            q.add(h(n));
        }
        q.start_now(&h(4));
        let active = q.active_download_slots();
        // bypass + first 2 from order, capped at 2.
        assert_eq!(active.len(), 2);
        assert!(active.contains(&h(4)));
    }

    #[test]
    fn unlimited_when_zero() {
        let mut q = QueueState::new(QueueLimits {
            max_active_downloads: 0,
            max_active_metadata_fetches: 100,
            max_active_seeds: 0,
            auto_start: true,
        });
        for n in 1..=10 {
            q.add(h(n));
        }
        assert_eq!(q.active_download_slots().len(), 10);
    }

    #[test]
    fn position_is_one_based() {
        let mut q = QueueState::new(QueueLimits::default());
        q.add(h(1));
        q.add(h(2));
        assert_eq!(q.position(&h(2)), Some(2));
        assert_eq!(q.position(&h(9)), None);
    }

    #[test]
    fn queue_large_scale_add_remove_reorder() {
        let mut q = QueueState::new(QueueLimits::default());
        let batch: Vec<InfoHash> = (0..10_000).map(h).collect();
        q.add_many(batch.clone());

        assert_eq!(q.order.len(), 10_000);
        for (i, hash) in batch.iter().enumerate() {
            assert_eq!(q.position(hash), Some(i + 1));
        }

        let to_remove: Vec<InfoHash> = (0..5_000).map(h).collect();
        q.remove_many(to_remove.clone());
        assert_eq!(q.order.len(), 5_000);
        for hash in to_remove {
            assert!(!q.order.contains(&hash));
        }

        let remaining: Vec<InfoHash> = (5_000..10_000).map(h).collect();
        let to_bottom: Vec<InfoHash> = remaining.iter().copied().step_by(2).take(10).collect();
        q.move_many_to_bottom(to_bottom.clone());

        let expected: Vec<InfoHash> = remaining
            .iter()
            .copied()
            .filter(|hash| !to_bottom.contains(hash))
            .chain(to_bottom.iter().copied())
            .collect();
        assert_eq!(q.order, expected);
    }

    #[test]
    fn queue_duplicate_suppression() {
        let mut q = QueueState::new(QueueLimits::default());
        let duplicate = h(123);
        q.add(duplicate);
        q.add(duplicate);
        q.add_many(vec![duplicate, duplicate, h(124), duplicate]);
        assert_eq!(q.order, vec![duplicate, h(124)]);

        q.start_now(&duplicate);
        q.start_now(&duplicate);
        q.clear_bypass_many(vec![duplicate, duplicate]);
        assert!(q.bypass.is_empty());
    }
}
