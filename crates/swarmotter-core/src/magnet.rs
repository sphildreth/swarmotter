// SPDX-License-Identifier: Apache-2.0

//! Magnet URI parsing.
//!
//! Supports the `magnet:` scheme with `xt` (exact topic / info hash),
//! `dn` (display name), `tr` (trackers), and `xl`/`xs`/`as`/`kt` fields where
//! present. Malformed magnets produce typed errors.

use crate::error::{CoreError, Result};
use crate::hash::InfoHash;
use serde::{Deserialize, Serialize};
use url::Url;

/// A parsed magnet link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Magnet {
    /// Info hash extracted from `xt=urn:btih:<hash>`.
    pub info_hash: InfoHash,
    /// Display name from `dn` if present.
    pub display_name: Option<String>,
    /// Tracker URLs from `tr` (order preserved).
    pub trackers: Vec<String>,
    /// Exact length (bytes) from `xl` if present and parseable.
    pub exact_length: Option<u64>,
    /// Webseed URLs from `ws` if present.
    pub webseeds: Vec<String>,
    /// Raw source magnet string.
    pub raw: String,
}

impl Magnet {
    /// Parse a magnet URI string.
    pub fn parse(s: &str) -> Result<Self> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(CoreError::MalformedMagnet("empty magnet".into()));
        }
        if !trimmed.starts_with("magnet:?") && !trimmed.starts_with("magnet://") {
            return Err(CoreError::MalformedMagnet(
                "must start with 'magnet:?' or 'magnet://'".into(),
            ));
        }

        // Parse as URL to split query robustly.
        let url = Url::parse(trimmed)
            .map_err(|e| CoreError::MalformedMagnet(format!("url parse: {e}")))?;

        let mut info_hash: Option<InfoHash> = None;
        let mut display_name: Option<String> = None;
        let mut trackers: Vec<String> = Vec::new();
        let mut exact_length: Option<u64> = None;
        let mut webseeds: Vec<String> = Vec::new();

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "xt" => {
                    if let Some(h) = parse_xt(&value)? {
                        info_hash = Some(h);
                    }
                }
                "dn" => {
                    display_name = Some(value.into_owned());
                }
                "tr" => {
                    trackers.push(value.into_owned());
                }
                "xl" => {
                    exact_length = value.parse::<u64>().ok();
                }
                "ws" => {
                    webseeds.push(value.into_owned());
                }
                _ => {}
            }
        }

        let info_hash = info_hash.ok_or_else(|| {
            CoreError::MalformedMagnet("missing or invalid 'xt' info hash".into())
        })?;

        Ok(Magnet {
            info_hash,
            display_name,
            trackers,
            exact_length,
            webseeds,
            raw: trimmed.to_string(),
        })
    }

    /// Construct a magnet from an info hash and optional name/trackers.
    pub fn to_uri(&self) -> String {
        let mut s = format!("magnet:?xt=urn:btih:{}", self.info_hash.to_hex());
        if let Some(name) = &self.display_name {
            s.push_str(&format!("&dn={}", url_encode(name)));
        }
        for tr in &self.trackers {
            s.push_str(&format!("&tr={}", url_encode(tr)));
        }
        for ws in &self.webseeds {
            s.push_str(&format!("&ws={}", url_encode(ws)));
        }
        s
    }
}

fn parse_xt(value: &str) -> Result<Option<InfoHash>> {
    // Expected: urn:btih:<40-hex> or urn:btih:<32-base32>
    let rest = value
        .strip_prefix("urn:btih:")
        .or_else(|| value.strip_prefix("urn:btmh:"));
    let Some(rest) = rest else {
        // Non-btih xt; ignore but do not error (other urn schemes).
        return Ok(None);
    };
    if rest.len() == 40 {
        Ok(Some(InfoHash::from_hex(rest)?))
    } else if rest.len() == 32 {
        Ok(Some(InfoHash::from_base32(rest)?))
    } else {
        Err(CoreError::MalformedMagnet(format!(
            "invalid btih hash length {} in xt",
            rest.len()
        )))
    }
}

fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        // Encode reserved/unsafe chars per RFC 3986.
        if b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b':'
                    | b'/'
                    | b'?'
                    | b'#'
                    | b'['
                    | b']'
                    | b'@'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
            )
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known_hex() -> &'static str {
        "dd8255ecdc7ca55fb0bbf81323d87062ba1f7a4e"
    }

    #[test]
    fn parses_minimal_magnet() {
        let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", known_hex())).unwrap();
        assert_eq!(m.info_hash.to_hex(), known_hex());
        assert!(m.display_name.is_none());
        assert!(m.trackers.is_empty());
    }

    #[test]
    fn parses_full_magnet() {
        let uri = format!(
            "magnet:?xt=urn:btih:{}&dn=test%20file&tr=http%3A%2F%2Ftracker.example%2Fannounce&tr=udp%3A%2F%2Ftracker.example%3A1337&xl=1024&ws=http%3A%2F%2Fwebseed.example%2Ffile",
            known_hex()
        );
        let m = Magnet::parse(&uri).unwrap();
        assert_eq!(m.info_hash.to_hex(), known_hex());
        assert_eq!(m.display_name.as_deref(), Some("test file"));
        assert_eq!(m.trackers.len(), 2);
        assert_eq!(m.trackers[0], "http://tracker.example/announce");
        assert_eq!(m.trackers[1], "udp://tracker.example:1337");
        assert_eq!(m.exact_length, Some(1024));
        assert_eq!(m.webseeds.len(), 1);
    }

    #[test]
    fn parses_base32_hash() {
        // base32 of known_hex
        let bytes = hex::decode(known_hex()).unwrap();
        let mut b32 = String::new();
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let mut buf: u64 = 0;
        let mut bits: u32 = 0;
        for &b in &bytes {
            buf = (buf << 8) | (b as u64);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                b32.push(A[((buf >> bits) & 0x1f) as usize] as char);
                buf &= (1u64 << bits) - 1;
            }
        }
        if bits > 0 {
            b32.push(A[((buf << (5 - bits)) & 0x1f) as usize] as char);
        }
        let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{}", b32)).unwrap();
        assert_eq!(m.info_hash.to_hex(), known_hex());
    }

    #[test]
    fn rejects_malformed() {
        assert!(Magnet::parse("").is_err());
        assert!(Magnet::parse("http://example.com").is_err());
        assert!(Magnet::parse("magnet:?dn=foo").is_err()); // no xt
        assert!(Magnet::parse("magnet:?xt=urn:btih:tooShort").is_err());
    }

    #[test]
    fn roundtrip_uri() {
        let m = Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{}&dn=name&tr=http%3A%2F%2Ft%2Fa",
            known_hex()
        ))
        .unwrap();
        let uri = m.to_uri();
        let back = Magnet::parse(&uri).unwrap();
        assert_eq!(back.info_hash, m.info_hash);
        assert_eq!(back.display_name, m.display_name);
        assert_eq!(back.trackers, m.trackers);
    }
}
