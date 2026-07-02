use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{info, warn};
use crate::engine::TorrentManager;
use crate::api::create_routes;

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create engine components
    let torrent_manager = TorrentManager::new();
    
    // Create API routes
    let app = create_routes();
    
    // Start server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Starting server on {}", addr);
    
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}