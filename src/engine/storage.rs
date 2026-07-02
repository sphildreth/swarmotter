use std::collections::HashMap;
use tokio::sync::Mutex;

pub struct Storage {
    files: Mutex<HashMap<String, Vec<u8>>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
        }
    }

    pub async fn write_file(&self, path: String, data: Vec<u8>) {
        let mut files = self.files.lock().await;
        files.insert(path, data);
    }

    pub async fn read_file(&self, path: &str) -> Option<Vec<u8>> {
        let files = self.files.lock().await;
        files.get(path).cloned()
    }
}