// SPDX-License-Identifier: Apache-2.0

//! API error handling: maps core errors to HTTP status codes and the envelope.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use swarmotter_core::error::CoreError;

/// An API error carrying a core error.
pub struct ApiError(pub CoreError);

impl From<CoreError> for ApiError {
    fn from(e: CoreError) -> Self {
        Self(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = status_for(&self.0);
        let body = crate::envelope::error_to_json(self.0.code().as_str(), &self.0.to_string());
        (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
    }
}

fn status_for(e: &CoreError) -> StatusCode {
    match e {
        CoreError::NotFound(_) => StatusCode::NOT_FOUND,
        CoreError::InvalidConfig(_)
        | CoreError::InvalidArgument(_)
        | CoreError::MalformedMagnet(_)
        | CoreError::MalformedTorrent(_)
        | CoreError::InvalidInfoHash(_)
        | CoreError::UnsupportedTorrentFeature(_)
        | CoreError::Bencode(_) => StatusCode::BAD_REQUEST,
        CoreError::DuplicateTorrent(_) => StatusCode::CONFLICT,
        CoreError::NetworkBlocked(_) => StatusCode::SERVICE_UNAVAILABLE,
        CoreError::HttpProtocol(_) | CoreError::HttpStatus(_) => StatusCode::BAD_GATEWAY,
        CoreError::Storage(_) | CoreError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Convert a core Result into an API response.
pub fn into_response<T: serde::Serialize>(res: swarmotter_core::error::Result<T>) -> Response {
    match res {
        Ok(v) => {
            let env = crate::envelope::Envelope::ok(v);
            let bytes = serde_json::to_vec(&env).unwrap_or_else(|_| {
                crate::envelope::error_to_json("internal_error", "serialization failed")
            });
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                bytes,
            )
                .into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// Convert a core error into an API response (no success payload).
pub fn err_response(e: swarmotter_core::error::CoreError) -> Response {
    ApiError::from(e).into_response()
}

/// Convenience for an empty success response.
pub fn ok_empty_response() -> Response {
    let env = crate::envelope::ok_empty();
    let bytes = serde_json::to_vec(&env).unwrap_or_default();
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        bytes,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_http_errors_map_to_bad_gateway() {
        assert_eq!(
            status_for(&CoreError::HttpProtocol("bad framing".into())),
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            status_for(&CoreError::HttpStatus("upstream 500".into())),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn unsupported_torrent_features_are_client_errors() {
        assert_eq!(
            status_for(&CoreError::UnsupportedTorrentFeature(
                "BEP 52 payload transfer is unavailable".into(),
            )),
            StatusCode::BAD_REQUEST
        );
    }
}
