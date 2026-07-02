use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum WebSocketEvent {
    TorrentAdded { id: String },
    TorrentChanged { id: String, progress: f32 },
    TorrentRemoved { id: String },
}

pub struct WebSocketManager {
    sender: broadcast::Sender<WebSocketEvent>,
}

impl WebSocketManager {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(100);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WebSocketEvent> {
        self.sender.subscribe()
    }

    pub fn broadcast(&self, event: WebSocketEvent) {
        let _ = self.sender.send(event);
    }
}