// SPDX-License-Identifier: Apache-2.0

//! Info hash handling for BitTorrent torrents.
//!
//! An info hash is the SHA-1 of the bencoded `info` dictionary of a
//! `.torrent` file (or magnet). It is the stable identifier for a torrent.

use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A 20-byte SHA-1 info hash.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InfoHash(#[serde(with = "hex_serde")] [u8; 20]);

impl InfoHash {
    pub const ZERO: InfoHash = InfoHash([0u8; 20]);

    /// Construct from a raw 20-byte array.
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    /// Compute the info hash from the raw bencoded `info` dictionary bytes.
    pub fn from_info_bencoded(info_bytes: &[u8]) -> Self {
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(info_bytes);
        let digest = hasher.finalize();
        let mut out = [0u8; 20];
        out.copy_from_slice(&digest);
        Self(out)
    }

    /// Raw 20-byte representation.
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Lowercase hex representation (40 chars).
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse from a 40-char lowercase (or uppercase) hex string.
    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 40 {
            return Err(CoreError::InvalidInfoHash(format!(
                "expected 40 hex chars, got {}",
                s.len()
            )));
        }
        let bytes = hex::decode(s).map_err(|e| CoreError::InvalidInfoHash(e.to_string()))?;
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Parse from a 32-char base32-encoded string (used in magnet `xt`).
    pub fn from_base32(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.len() != 32 {
            return Err(CoreError::InvalidInfoHash(format!(
                "expected 32 base32 chars, got {}",
                s.len()
            )));
        }
        let bytes = base32_decode(s)?;
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }
}

impl fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoHash({})", self.to_hex())
    }
}

impl fmt::Display for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// RFC 4648 base32 decode (uppercase or lowercase A-Z2-7).
fn base32_decode(s: &str) -> Result<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 5 / 8);
    let mut buffer: u64 = 0;
    let mut bits: u32 = 0;
    for &c in bytes {
        let val = if c.is_ascii_uppercase() {
            c - b'A'
        } else if c.is_ascii_lowercase() {
            c - b'a'
        } else if (b'2'..=b'7').contains(&c) {
            26 + (c - b'2')
        } else {
            return Err(CoreError::InvalidInfoHash(format!(
                "invalid base32 char: {:?}",
                c as char
            )));
        };
        buffer = (buffer << 5) | (val as u64);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
            buffer &= (1u64 << bits) - 1;
        }
    }
    Ok(out)
}

/// Serde helper: serialize/deserialize the inner `[u8; 20]` as lowercase hex.
mod hex_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(h: &[u8; 20], s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(h))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<[u8; 20], D::Error> {
        let s = String::deserialize(d)?;
        if s.len() != 40 {
            return Err(serde::de::Error::custom("expected 40 hex chars"));
        }
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        Ok(arr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_hex() {
        let bytes = [1u8; 20];
        let h = InfoHash::from_bytes(bytes);
        let hex = h.to_hex();
        assert_eq!(hex.len(), 40);
        let back = InfoHash::from_hex(&hex).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn from_info_bencoded_matches_known() {
        // SHA-1 of the literal bytes "info" placeholder dictionary.
        let info_bytes = b"d4:name3:foo12:piece lengthi1e6:lengthi0ee";
        let h = InfoHash::from_info_bencoded(info_bytes);
        // Recompute expected with sha1 directly.
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(info_bytes);
        let expected = hasher.finalize();
        assert_eq!(h.as_bytes(), expected.as_slice());
    }

    #[test]
    fn base32_roundtrip() {
        let bytes = [0xabu8; 20];
        let h = InfoHash::from_bytes(bytes);
        let hex = h.to_hex();
        // base32 of 20 bytes is 32 chars.
        let b32 = base32_encode(&bytes);
        assert_eq!(b32.len(), 32);
        let back = InfoHash::from_base32(&b32).unwrap();
        assert_eq!(h, back);
        assert_eq!(back.to_hex(), hex);
    }

    #[test]
    fn rejects_bad_hex() {
        assert!(InfoHash::from_hex("abc").is_err());
        assert!(InfoHash::from_hex(&"x".repeat(40)).is_err());
    }

    fn base32_encode(bytes: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let mut out = String::new();
        let mut buffer: u64 = 0;
        let mut bits: u32 = 0;
        for &b in bytes {
            buffer = (buffer << 8) | (b as u64);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                let idx = ((buffer >> bits) & 0x1f) as usize;
                out.push(ALPHABET[idx] as char);
                buffer &= (1u64 << bits) - 1;
            }
        }
        if bits > 0 {
            let idx = ((buffer << (5 - bits)) & 0x1f) as usize;
            out.push(ALPHABET[idx] as char);
        }
        out
    }
}
