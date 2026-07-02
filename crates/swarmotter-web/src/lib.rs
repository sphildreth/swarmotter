// SPDX-License-Identifier: Apache-2.0

//! Web support for SwarmOtter.
//!
//! Serves a practical, function-over-form Web UI that consumes the same API
//! exposed to external automation (ADR-0004, ADR-0006). The UI is plain HTML +
//! vanilla JS with no heavy framework, prioritizing fast load and complete
//! operational coverage.
//!
//! The UI assets are embedded at compile time so the daemon serves a single
//! binary with no external static files.

use axum::{
    body::Body,
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_JS: &str = include_str!("../assets/app.js");
const STYLE_CSS: &str = include_str!("../assets/style.css");
const FAVICON_ICO: &[u8] = include_bytes!("../../../assets/graphics/web/favicon.ico");
const FAVICON_16: &[u8] = include_bytes!("../../../assets/graphics/web/favicon-16x16.png");
const FAVICON_32: &[u8] = include_bytes!("../../../assets/graphics/web/favicon-32x32.png");
const FAVICON_48: &[u8] = include_bytes!("../../../assets/graphics/web/favicon-48x48.png");
const APPLE_TOUCH_ICON: &[u8] = include_bytes!("../../../assets/graphics/web/apple-touch-icon.png");
const ANDROID_CHROME_192: &[u8] =
    include_bytes!("../../../assets/graphics/web/android-chrome-192x192.png");
const ANDROID_CHROME_512: &[u8] =
    include_bytes!("../../../assets/graphics/web/android-chrome-512x512.png");
const MASKABLE_ICON_192: &[u8] =
    include_bytes!("../../../assets/graphics/web/maskable-icon-192x192.png");
const MASKABLE_ICON_512: &[u8] =
    include_bytes!("../../../assets/graphics/web/maskable-icon-512x512.png");
const MSTILE_150: &[u8] = include_bytes!("../../../assets/graphics/web/mstile-150x150.png");
const SITE_WEBMANIFEST: &[u8] = include_bytes!("../../../assets/graphics/web/site.webmanifest");
const HEADER_LOGO: &[u8] =
    include_bytes!("../../../assets/graphics/icons/swarmotter-icon-48x48.png");

/// Build the web UI router, mounted at `/` (excluding `/api`).
pub fn web_router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/index.html", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/favicon.ico", get(favicon_ico))
        .route("/favicon-16x16.png", get(favicon_16))
        .route("/favicon-32x32.png", get(favicon_32))
        .route("/favicon-48x48.png", get(favicon_48))
        .route("/apple-touch-icon.png", get(apple_touch_icon))
        .route("/android-chrome-192x192.png", get(android_chrome_192))
        .route("/android-chrome-512x512.png", get(android_chrome_512))
        .route("/maskable-icon-192x192.png", get(maskable_icon_192))
        .route("/maskable-icon-512x512.png", get(maskable_icon_512))
        .route("/mstile-150x150.png", get(mstile_150))
        .route("/site.webmanifest", get(site_webmanifest))
        .route("/swarmotter-icon-48x48.png", get(header_logo))
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
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLE_CSS,
    )
        .into_response()
}

async fn favicon_ico() -> Response {
    static_asset("image/x-icon", FAVICON_ICO)
}

async fn favicon_16() -> Response {
    static_asset("image/png", FAVICON_16)
}

async fn favicon_32() -> Response {
    static_asset("image/png", FAVICON_32)
}

async fn favicon_48() -> Response {
    static_asset("image/png", FAVICON_48)
}

async fn apple_touch_icon() -> Response {
    static_asset("image/png", APPLE_TOUCH_ICON)
}

async fn android_chrome_192() -> Response {
    static_asset("image/png", ANDROID_CHROME_192)
}

async fn android_chrome_512() -> Response {
    static_asset("image/png", ANDROID_CHROME_512)
}

async fn maskable_icon_192() -> Response {
    static_asset("image/png", MASKABLE_ICON_192)
}

async fn maskable_icon_512() -> Response {
    static_asset("image/png", MASKABLE_ICON_512)
}

async fn mstile_150() -> Response {
    static_asset("image/png", MSTILE_150)
}

async fn site_webmanifest() -> Response {
    static_asset("application/manifest+json", SITE_WEBMANIFEST)
}

async fn header_logo() -> Response {
    static_asset("image/png", HEADER_LOGO)
}

fn static_asset(content_type: &'static str, body: &'static [u8]) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "public, max-age=86400")
        .body(Body::from(body))
        .expect("static asset response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[test]
    fn assets_are_nonempty() {
        assert!(!INDEX_HTML.is_empty());
        assert!(!APP_JS.is_empty());
        assert!(!STYLE_CSS.is_empty());
        assert!(!FAVICON_ICO.is_empty());
        assert!(!FAVICON_48.is_empty());
        assert!(!SITE_WEBMANIFEST.is_empty());
        assert!(!HEADER_LOGO.is_empty());
    }

    #[tokio::test]
    async fn web_asset_routes_serve_expected_content_types() {
        let app = web_router();
        for (path, content_type) in [
            ("/favicon.ico", "image/x-icon"),
            ("/site.webmanifest", "application/manifest+json"),
            ("/swarmotter-icon-48x48.png", "image/png"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("request is valid"),
                )
                .await
                .expect("route responds");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                content_type
            );
        }
    }
}
