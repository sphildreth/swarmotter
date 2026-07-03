// SPDX-License-Identifier: Apache-2.0

//! Endgame request scheduling.
//!
//! Near the end of a download, the last few pieces may be held by slow or
//! unresponsive peers. Endgame mode requests the remaining blocks from
//! multiple peers at once (allowing duplicate outstanding requests) and then
//! cancels the duplicates once a block is delivered. This keeps the request
//! queue bounded: a bounded number of duplicate requests per remaining block,
//! and outstanding requests are cancelled as soon as the block completes.
//!
//! This module holds the pure, unit-tested decision logic; the live engine
//! (`swarmotterd::engine`) wires it into the concurrent peer download path.
//! See `design/requirements.md` (endgame) and ADR-0013.

use std::collections::HashSet;

/// Number of remaining pieces at or below which endgame mode activates.
pub const ENDGAME_THRESHOLD: usize = 4;

/// Whether endgame mode should be active given the number of pieces left.
pub fn is_endgame(remaining_pieces: usize) -> bool {
    remaining_pieces > 0 && remaining_pieces <= ENDGAME_THRESHOLD
}

/// A tracker for outstanding block requests during endgame, so duplicate
/// requests can be cancelled once the block is delivered. A "block" is
/// identified by `(piece_index, byte_offset_within_piece)`.
#[derive(Debug, Clone, Default)]
pub struct OutstandingRequests {
    pending: HashSet<(u32, u32)>,
    /// Maximum duplicate requests allowed per block across peers. Keeps the
    /// queue bounded and prevents request explosion.
    max_duplicates: usize,
    /// How many peers currently have an outstanding request for each block.
    counts: std::collections::HashMap<(u32, u32), usize>,
}

impl OutstandingRequests {
    pub fn new(max_duplicates: usize) -> Self {
        Self {
            pending: HashSet::new(),
            max_duplicates: max_duplicates.max(1),
            counts: std::collections::HashMap::new(),
        }
    }

    /// Record an outstanding request for a block, honoring the per-block
    /// duplicate cap. Returns `true` if the request was accepted (i.e. the
    /// caller should send it), `false` if the cap would be exceeded.
    pub fn request(&mut self, piece: u32, offset: u32) -> bool {
        let key = (piece, offset);
        let count = self.counts.entry(key).or_insert(0);
        if *count >= self.max_duplicates {
            return false;
        }
        *count += 1;
        self.pending.insert(key);
        true
    }

    /// Mark a block as delivered; returns the set of `(piece, offset)` blocks
    /// that should now be cancelled because the piece they belong to is
    /// complete. In practice the caller cancels the remaining outstanding
    /// blocks of the completed piece.
    pub fn delivered(&mut self, piece: u32, offset: u32) {
        let key = (piece, offset);
        self.pending.remove(&key);
        self.counts.remove(&key);
    }

    /// Release one peer's outstanding request for a block that was not
    /// delivered. This prevents timed-out or malformed peer sessions from
    /// permanently occupying duplicate request capacity.
    pub fn cancel_request(&mut self, piece: u32, offset: u32) {
        let key = (piece, offset);
        let Some(count) = self.counts.get_mut(&key) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.counts.remove(&key);
            self.pending.remove(&key);
        }
    }

    /// All still-outstanding blocks of a piece (for generating Cancel
    /// messages once the piece completes on any peer).
    pub fn outstanding_for_piece(&self, piece: u32) -> Vec<(u32, u32)> {
        self.pending
            .iter()
            .filter(|(p, _)| *p == piece)
            .copied()
            .collect()
    }

    /// Remove all outstanding requests for a piece (after it completes).
    pub fn clear_piece(&mut self, piece: u32) {
        self.pending.retain(|(p, _)| *p != piece);
        self.counts.retain(|(p, _), _| *p != piece);
    }

    /// Total number of outstanding block requests across all pieces/peers.
    pub fn total(&self) -> usize {
        self.pending.len()
    }

    /// Number of distinct peers that have requested a given block.
    pub fn duplicate_count(&self, piece: u32, offset: u32) -> usize {
        self.counts.get(&(piece, offset)).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endgame_thresholds() {
        assert!(!is_endgame(0));
        assert!(is_endgame(1));
        assert!(is_endgame(ENDGAME_THRESHOLD));
        assert!(!is_endgame(ENDGAME_THRESHOLD + 1));
        assert!(!is_endgame(100));
    }

    #[test]
    fn request_caps_duplicates() {
        let mut t = OutstandingRequests::new(2);
        assert!(t.request(0, 0));
        assert!(t.request(0, 0)); // second duplicate allowed
        assert!(!t.request(0, 0)); // third rejected
        assert_eq!(t.duplicate_count(0, 0), 2);
        assert_eq!(t.total(), 1); // same block key counted once in pending
    }

    #[test]
    fn delivered_clears_and_supports_cancel() {
        let mut t = OutstandingRequests::new(3);
        // Two peers request the same blocks of piece 1.
        for _ in 0..2 {
            t.request(1, 0);
            t.request(1, 16384);
        }
        // Peer A delivers block (1, 0).
        t.delivered(1, 0);
        // Outstanding for the piece should still include (1, 16384) which the
        // caller cancels once the piece completes.
        let outstanding = t.outstanding_for_piece(1);
        assert!(outstanding.contains(&(1, 16384)));
        assert!(!outstanding.contains(&(1, 0)));
        t.clear_piece(1);
        assert!(t.outstanding_for_piece(1).is_empty());
    }

    #[test]
    fn cancel_request_releases_one_duplicate_slot() {
        let mut t = OutstandingRequests::new(2);
        assert!(t.request(1, 0));
        assert!(t.request(1, 0));
        assert!(!t.request(1, 0));

        t.cancel_request(1, 0);

        assert_eq!(t.duplicate_count(1, 0), 1);
        assert!(t.request(1, 0));
    }

    #[test]
    fn total_tracks_distinct_blocks() {
        let mut t = OutstandingRequests::new(4);
        t.request(0, 0);
        t.request(0, 0);
        t.request(0, 16384);
        t.request(2, 0);
        assert_eq!(t.total(), 3); // 3 distinct block keys
    }
}
