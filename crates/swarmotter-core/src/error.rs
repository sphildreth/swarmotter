// SPDX-License-Identifier: Apache-2.0

//! Typed error model for SwarmOtter core logic.
//!
//! Errors carry machine-readable codes suitable for the API's consistent error
//! response format. Production paths avoid `unwrap`/`expect`; use `Result`.

/// A machine-readable error code, usable directly in API responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(&'static str);

impl ErrorCode {
    pub const fn new(code: &'static str) -> Self {
        Self(code)
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl serde::Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.0)
    }
}

/// Core domain error.
///
/// Variants map to stable, machine-readable `ErrorCode`s. The `Display` impl
/// yields a human-readable message; the API layer wraps this into the
/// `{ success, data, error: { code, message } }` envelope.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("malformed magnet URI: {0}")]
    MalformedMagnet(String),
    #[error("malformed torrent metadata: {0}")]
    MalformedTorrent(String),
    #[error("invalid info hash: {0}")]
    InvalidInfoHash(String),
    #[error("unsupported torrent feature: {0}")]
    UnsupportedTorrentFeature(String),
    #[error("duplicate torrent: {0}")]
    DuplicateTorrent(String),
    #[error("torrent not found: {0}")]
    NotFound(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("network containment failure: {0}")]
    NetworkBlocked(String),
    #[error("SOCKS5 proxy error: {0}")]
    Proxy(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("operation timed out")]
    Elapsed(#[from] tokio::time::error::Elapsed),
    #[error("bencode error: {0}")]
    Bencode(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("HTTP protocol error: {0}")]
    HttpProtocol(String),
    #[error("HTTP status error: {0}")]
    HttpStatus(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl CoreError {
    /// Stable machine-readable code for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            CoreError::MalformedMagnet(_) => ErrorCode::new("malformed_magnet"),
            CoreError::MalformedTorrent(_) => ErrorCode::new("malformed_torrent"),
            CoreError::InvalidInfoHash(_) => ErrorCode::new("invalid_info_hash"),
            CoreError::UnsupportedTorrentFeature(_) => {
                ErrorCode::new("unsupported_torrent_feature")
            }
            CoreError::DuplicateTorrent(_) => ErrorCode::new("duplicate_torrent"),
            CoreError::NotFound(_) => ErrorCode::new("not_found"),
            CoreError::InvalidConfig(_) => ErrorCode::new("invalid_config"),
            CoreError::NetworkBlocked(_) => ErrorCode::new("network_blocked"),
            CoreError::Proxy(_) => ErrorCode::new("proxy_error"),
            CoreError::Storage(_) => ErrorCode::new("storage_error"),
            CoreError::Io(_) => ErrorCode::new("io_error"),
            CoreError::Elapsed(_) => ErrorCode::new("timeout"),
            CoreError::Bencode(_) => ErrorCode::new("bencode_error"),
            CoreError::Parse(_) => ErrorCode::new("parse_error"),
            CoreError::HttpProtocol(_) => ErrorCode::new("http_protocol_error"),
            CoreError::HttpStatus(_) => ErrorCode::new("http_status_error"),
            CoreError::InvalidArgument(_) => ErrorCode::new("invalid_argument"),
            CoreError::Internal(_) => ErrorCode::new("internal_error"),
        }
    }

    /// Whether this error indicates a network containment fail-closed state.
    pub fn is_network_blocked(&self) -> bool {
        matches!(self, CoreError::NetworkBlocked(_))
    }
}

pub type Result<T> = std::result::Result<T, CoreError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_are_stable_and_serializable() {
        let e = CoreError::MalformedMagnet("bad".into());
        assert_eq!(e.code().as_str(), "malformed_magnet");
        let json = serde_json::to_string(&e.code()).unwrap();
        assert_eq!(json, "\"malformed_magnet\"");
        assert!(e.code().as_str().starts_with("malformed_magnet"));
    }

    #[test]
    fn network_blocked_detection() {
        assert!(CoreError::NetworkBlocked("x".into()).is_network_blocked());
        assert!(!CoreError::NotFound("x".into()).is_network_blocked());
    }

    #[test]
    fn http_error_codes_are_stable() {
        assert_eq!(
            CoreError::HttpProtocol("x".into()).code().as_str(),
            "http_protocol_error"
        );
        assert_eq!(
            CoreError::HttpStatus("x".into()).code().as_str(),
            "http_status_error"
        );
    }
}
