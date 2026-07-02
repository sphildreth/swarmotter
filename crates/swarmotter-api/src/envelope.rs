// SPDX-License-Identifier: Apache-2.0

//! Consistent JSON response envelope.
//!
//! ```json
//! { "success": true, "data": {}, "error": null }
//! ```
//! Errors include machine-readable `code` and human-readable `message`.

use serde::Serialize;

/// The standard API response envelope.
#[derive(Debug, Serialize)]
pub struct Envelope<T: Serialize> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<ErrorBody>,
}

/// Error body.
#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

impl ErrorBody {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl<T: Serialize> Envelope<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }
}

/// A success envelope with no data payload.
pub fn ok_empty() -> Envelope<serde_json::Value> {
    Envelope {
        success: true,
        data: Some(serde_json::Value::Null),
        error: None,
    }
}

/// Helper to serialize an envelope as JSON bytes.
pub fn to_json<T: Serialize>(e: &Envelope<T>) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(e)
}

/// Serialize an error envelope as JSON bytes without a generic data type.
pub fn error_to_json(code: &str, message: &str) -> Vec<u8> {
    // Manually serialize to avoid a generic parameter on the error path.
    let mut s = String::from(r#"{"success":false,"data":null,"error":{"code":""#);
    s.push_str(&json_escape(code));
    s.push_str(r#"","message":""#);
    s.push_str(&json_escape(message));
    s.push_str(r#""}}"#);
    s.into_bytes()
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str(r#"\""#),
            '\\' => out.push_str(r"\\"),
            '\n' => out.push_str(r"\n"),
            '\r' => out.push_str(r"\r"),
            '\t' => out.push_str(r"\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_envelope_serializes() {
        let e = Envelope::ok(vec!["a", "b"]);
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"data\":[\"a\",\"b\"]"));
        assert!(json.contains("\"error\":null"));
    }

    #[test]
    fn error_bytes_are_valid_json() {
        let bytes = error_to_json("network_blocked", "tun0 missing");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["success"], false);
        assert_eq!(v["error"]["code"], "network_blocked");
        assert_eq!(v["error"]["message"], "tun0 missing");
    }
}
