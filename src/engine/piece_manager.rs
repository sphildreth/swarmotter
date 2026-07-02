use std::collections::HashMap;
use tokio::sync::Mutex;

pub struct PieceManager {
    pieces: Mutex<HashMap<u32, bool>>,
}

impl PieceManager {
    pub fn new() -> Self {
        Self {
            pieces: Mutex::new(HashMap::new()),
        }
    }

    pub async fn mark_piece_complete(&self, index: u32) {
        let mut pieces = self.pieces.lock().await;
        pieces.insert(index, true);
    }

    pub async fn get_completed_pieces(&self) -> Vec<u32> {
        let pieces = self.pieces.lock().await;
        pieces.iter()
            .filter(|(_, &completed)| completed)
            .map(|(&index, _)| index)
            .collect()
    }
}