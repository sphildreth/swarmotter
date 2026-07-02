use axum::{routing::{get, post}, Router};
use crate::api::handlers::{AddTorrentRequest, ApiResponse};

pub fn create_routes() -> Router {
    Router::new()
        .route("/torrents", get(get_torrents).post(add_torrent))
        .route("/torrents/:id", get(get_torrent))
        .route("/stats", get(get_stats))
}

async fn get_torrents() -> impl axum::response::IntoResponse {
    ApiResponse::ok(vec![])
}

async fn add_torrent(payload: AddTorrentRequest) -> impl axum::response::IntoResponse {
    ApiResponse::ok(())
}

async fn get_torrent() -> impl axum::response::IntoResponse {
    ApiResponse::ok(())
}

async fn get_stats() -> impl axum::response::IntoResponse {
    ApiResponse::ok(())
}