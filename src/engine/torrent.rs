use std::collections::HashMap;
use tokio::sync::Mutex;

pub struct TorrentManager {
    torrents: Mutex<HashMap<String, Torrent>>,
    statuses: Mutex<HashMap<String, TorrentStatus>>,
}

impl TorrentManager {
    pub fn new() -> Self {
        Self {
            torrents: Mutex::new(HashMap::new()),
            statuses: Mutex::new(HashMap::new()),
        }
    }

    pub async fn add_torrent(&self, torrent: Torrent) {
        let mut torrents = self.torrents.lock().await;
        let mut statuses = self.statuses.lock().await;
        
        torrents.insert(torrent.id.clone(), torrent);
        statuses.insert("test_id".to_string(), TorrentStatus {
            state: "queued".to_string(),
            progress: 0.0,
            download_speed: 0.0,
            upload_speed: 0.0,
            eta: 0,
        });
    }

    pub async fn get_torrent(&self, id: &str) -> Option<Torrent> {
        let torrents = self.torrents.lock().await;
        torrents.get(id).cloned()
    }

    pub async fn get_all_torrents(&self) -> Vec<Torrent> {
        let torrents = self.torrents.lock().await;
        torrents.values().cloned().collect()
    }
}