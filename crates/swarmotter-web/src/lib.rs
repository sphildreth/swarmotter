// SPDX-License-Identifier: Apache-2.0

//! Web support for SwarmOtter.
//!
//! Serves a practical, function-over-form Web UI that consumes the same API
//! exposed to external automation (ADR-0004, ADR-0006). The UI is plain HTML +
//! vanilla JS with no heavy framework, prioritizing fast load and complete
//! operational coverage.
//!
//! The UI assets are embedded at compile time via `include_str!` so the daemon
//! serves a single binary with no external static files.

use axum::{
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_JS: &str = include_str!("../assets/app.js");
const STYLE_CSS: &str = include_str!("../assets/style.css");

/// Build the web UI router, mounted at `/` (excluding `/api`).
pub fn web_router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/index.html", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
}

async fn index() -> Response {
    Html(INDEX_HTML).into_response()
}

async fn app_js() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        APP_JS,
    )
        .into_response()
}

async fn style_css() -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLE_CSS,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assets_are_nonempty() {
        assert!(!INDEX_HTML.is_empty());
        assert!(!APP_JS.is_empty());
        assert!(!STYLE_CSS.is_empty());
    }
}
