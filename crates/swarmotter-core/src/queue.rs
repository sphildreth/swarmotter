// SPDX-License-Identifier: Apache-2.0

//! Queue management logic.
//!
//! Implements global active download/seed limits, queue ordering (up/down/
//! top/bottom), start-now/bypass, and per-torrent paused state. The logic is
//! pure over a queue state so it can be unit-tested without the daemon.

use crate::hash::InfoHash;
use serde::{Deserialize, Serialize};

/// Queue limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueLimits {
    /// Max simultaneously downloading torrents (0 = unlimited).
    #[serde(default = "default_max_active_downloads")]
    pub max_active_downloads: usize,
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
}

impl QueueState {
    pub fn new(limits: QueueLimits) -> Self {
        Self {
            limits,
            order: Vec::new(),
            bypass: Vec::new(),
        }
    }

    /// Add a torrent to the end of the queue.
    pub fn add(&mut self, hash: InfoHash) {
        if !self.order.contains(&hash) {
            self.order.push(hash);
        }
    }

    /// Remove a torrent from the queue.
    pub fn remove(&mut self, hash: &InfoHash) {
        self.order.retain(|h| h != hash);
        self.bypass.retain(|h| h != hash);
    }

    /// Move a torrent up one position.
    pub fn move_up(&mut self, hash: &InfoHash) {
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            if i > 0 {
                self.order.swap(i, i - 1);
            }
        }
    }

    /// Move a torrent down one position.
    pub fn move_down(&mut self, hash: &InfoHash) {
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            if i + 1 < self.order.len() {
                self.order.swap(i, i + 1);
            }
        }
    }

    /// Move a torrent to the top of the queue.
    pub fn move_to_top(&mut self, hash: &InfoHash) {
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            let h = self.order.remove(i);
            self.order.insert(0, h);
        }
    }

    /// Move a torrent to the bottom of the queue.
    pub fn move_to_bottom(&mut self, hash: &InfoHash) {
        if let Some(i) = self.order.iter().position(|h| h == hash) {
            let h = self.order.remove(i);
            self.order.push(h);
        }
    }

    /// Mark a torrent to start now (bypass queue).
    pub fn start_now(&mut self, hash: &InfoHash) {
        if !self.bypass.contains(hash) {
            self.bypass.push(*hash);
        }
    }

    /// Clear bypass flag.
    pub fn clear_bypass(&mut self, hash: &InfoHash) {
        self.bypass.retain(|h| h != hash);
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
        for h in &self.order {
            if active.len() >= limit {
                break;
            }
            if !active.contains(h) {
                active.push(*h);
            }
        }
        active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> InfoHash {
        InfoHash::from_bytes([n; 20])
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
}
