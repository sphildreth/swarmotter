// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(super) async fn update_progress_state(
    state: &Arc<Mutex<EngineState>>,
    meta: &TorrentMeta,
    have: &PieceBitfield,
) {
    let mut s = state.lock().await;
    s.pieces_have = have.clone();
    let complete_pieces = have.count(s.piece_count) as u64;
    let mut completed = complete_pieces.saturating_mul(meta.piece_length);
    if s.piece_count > 0 && have.has(s.piece_count - 1) {
        completed = completed.saturating_sub(meta.piece_length - meta.last_piece_length());
    }
    completed = completed.min(meta.total_length);
    s.bytes_completed = completed;
}
