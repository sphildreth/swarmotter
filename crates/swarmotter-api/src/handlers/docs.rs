// SPDX-License-Identifier: Apache-2.0

//! Version-matched OpenAPI document and self-contained explorer.

use axum::{
    http::header,
    response::{IntoResponse, Response},
};

pub async fn openapi_json() -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        crate::openapi::document_json(),
    )
        .into_response()
}

pub async fn docs_page() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'self'",
            ),
        ],
        crate::openapi::docs_html(),
    )
        .into_response()
}
