// SPDX-License-Identifier: Apache-2.0

//! Torrent identity handling for BitTorrent metainfo.
//!
//! [`InfoHash`] is deliberately the v1 SHA-1 identity only. BEP 52 uses a
//! full SHA-256 digest and only truncates that digest at particular peer and
//! tracker wire boundaries. Keeping [`V2InfoHash`] distinct prevents a v2
//! identifier from being accidentally accepted wherever a v1 identity is
//! required.

use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

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
    pub const fn as_bytes(&self) -> &[u8; 20] {
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

/// A full 32-byte SHA-256 info hash used by BEP 52 v2 torrents.
///
/// This is not interchangeable with [`InfoHash`]. The BEP 52 peer and
/// tracker protocols use a 20-byte truncation in selected places, represented
/// by [`PeerInfoHash`], while metainfo and magnets retain this full value.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct V2InfoHash(#[serde(with = "hex_serde_32")] [u8; 32]);

impl V2InfoHash {
    pub const ZERO: V2InfoHash = V2InfoHash([0u8; 32]);

    /// Construct from a raw 32-byte SHA-256 digest.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Compute the BEP 52 identity from exact bencoded `info` dictionary
    /// bytes. Callers must not decode and re-encode untrusted metainfo before
    /// using this function.
    pub fn from_info_bencoded(info_bytes: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(info_bytes);
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        Self(out)
    }

    /// Raw 32-byte representation.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hexadecimal representation (64 characters).
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse a 64-character hexadecimal SHA-256 digest.
    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 64 {
            return Err(CoreError::InvalidInfoHash(format!(
                "expected 64 hex chars for a v2 info hash, got {}",
                s.len()
            )));
        }
        let bytes = hex::decode(s).map_err(|e| CoreError::InvalidInfoHash(e.to_string()))?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Parse a BEP 9/BEP 52 tagged multihash (`1220` + 64 hex characters).
    pub fn from_magnet_multihash(s: &str) -> Result<Self> {
        let Some(digest) = s.strip_prefix("1220") else {
            return Err(CoreError::InvalidInfoHash(
                "v2 magnet multihash must begin with SHA2-256 tag '1220'".into(),
            ));
        };
        Self::from_hex(digest)
    }

    /// Encode the v2 identity in the tagged multihash form used by `btmh`
    /// magnet exact topics.
    pub fn to_magnet_multihash(&self) -> String {
        format!("1220{}", self.to_hex())
    }

    /// The 20-byte BEP 52 peer/tracker wire value derived from this full
    /// identity. It is intentionally a different type from [`InfoHash`].
    pub fn peer_info_hash(&self) -> PeerInfoHash {
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&self.0[..20]);
        PeerInfoHash(bytes)
    }
}

impl fmt::Debug for V2InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "V2InfoHash({})", self.to_hex())
    }
}

impl fmt::Display for V2InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// A 20-byte info-hash value used on BEP 52-compatible peer/tracker wire
/// boundaries. It is distinct from a v1 [`InfoHash`] because it may be the
/// truncation of a full [`V2InfoHash`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerInfoHash([u8; 20]);

impl PeerInfoHash {
    /// Construct from a raw 20-byte peer/tracker wire value. This does not
    /// imply whether the value originated from a v1 SHA-1 or a truncated v2
    /// SHA-256 identity.
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    /// Construct the peer/tracker value from a v1 SHA-1 identity.
    pub const fn from_v1(info_hash: InfoHash) -> Self {
        Self(*info_hash.as_bytes())
    }

    /// Construct the peer/tracker value from a v2 SHA-256 identity.
    pub fn from_v2(info_hash: V2InfoHash) -> Self {
        info_hash.peer_info_hash()
    }

    /// Raw 20-byte wire representation.
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Lowercase hexadecimal representation.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl From<InfoHash> for PeerInfoHash {
    fn from(value: InfoHash) -> Self {
        Self::from_v1(value)
    }
}

impl fmt::Debug for PeerInfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerInfoHash({})", self.to_hex())
    }
}

impl fmt::Display for PeerInfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// A collision-safe daemon, persistence, queue, and API locator for a
/// torrent record.
///
/// The canonical textual form is deliberately untagged but unambiguous by
/// length: a v1 key is 40 hexadecimal characters and a pure-v2 key is 64.
/// This is not a peer-wire identity; use [`PeerInfoHash`] at tracker, DHT,
/// and peer-protocol boundaries instead.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TorrentKey {
    /// Full v1 SHA-1 key (also the primary key for a hybrid record).
    V1(InfoHash),
    /// Full v2 SHA-256 key for a pure-v2 record.
    V2(V2InfoHash),
}

impl TorrentKey {
    /// Construct a key from a v1 identity.
    pub const fn v1(hash: InfoHash) -> Self {
        Self::V1(hash)
    }

    /// Construct a key from a full v2 identity.
    pub const fn v2(hash: V2InfoHash) -> Self {
        Self::V2(hash)
    }

    /// Parse the canonical 40- or 64-hex-character locator form.
    pub fn from_locator(value: &str) -> Result<Self> {
        match value.len() {
            40 => InfoHash::from_hex(value).map(Self::V1),
            64 => V2InfoHash::from_hex(value).map(Self::V2),
            length => Err(CoreError::InvalidInfoHash(format!(
                "torrent key must contain 40 (v1) or 64 (v2) hexadecimal characters, got {length}"
            ))),
        }
    }

    /// Canonical locator text used at durable and API boundaries.
    pub fn to_locator(self) -> String {
        match self {
            Self::V1(hash) => hash.to_hex(),
            Self::V2(hash) => hash.to_hex(),
        }
    }

    /// Return the v1 identity when this is a v1/hybrid primary key.
    pub const fn as_v1(self) -> Option<InfoHash> {
        match self {
            Self::V1(hash) => Some(hash),
            Self::V2(_) => None,
        }
    }

    /// Return the full v2 identity when this is a pure-v2 key.
    pub const fn as_v2(self) -> Option<V2InfoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(hash) => Some(hash),
        }
    }

    /// Return the explicit 20-byte peer/tracker wire identity for this key.
    pub fn peer_info_hash(self) -> PeerInfoHash {
        match self {
            Self::V1(hash) => PeerInfoHash::from_v1(hash),
            Self::V2(hash) => PeerInfoHash::from_v2(hash),
        }
    }
}

impl From<InfoHash> for TorrentKey {
    fn from(value: InfoHash) -> Self {
        Self::V1(value)
    }
}

impl From<V2InfoHash> for TorrentKey {
    fn from(value: V2InfoHash) -> Self {
        Self::V2(value)
    }
}

impl FromStr for TorrentKey {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self> {
        Self::from_locator(value)
    }
}

impl fmt::Debug for TorrentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TorrentKey({self})")
    }
}

impl fmt::Display for TorrentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&(*self).to_locator())
    }
}

impl Serialize for TorrentKey {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_locator())
    }
}

impl<'de> Deserialize<'de> for TorrentKey {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_locator(&value).map_err(serde::de::Error::custom)
    }
}

/// The authoritative metainfo identity of a torrent.
///
/// `Unknown` exists only for durable records written before this model was
/// introduced. New parsed magnets and metainfo always use one of the explicit
/// variants; callers should not infer a v2 identity from a 20-byte v1 hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TorrentIdentity {
    /// Legacy durable record with no explicit identity metadata.
    #[default]
    Unknown,
    /// A BEP 3/v1 SHA-1 identity.
    V1 { v1: InfoHash },
    /// A BEP 52/v2 full SHA-256 identity.
    V2 { v2: V2InfoHash },
    /// A hybrid torrent that contains independently-computed v1 and v2
    /// identities for the same validated content layout.
    Hybrid { v1: InfoHash, v2: V2InfoHash },
}

impl TorrentIdentity {
    pub const fn v1(v1: InfoHash) -> Self {
        Self::V1 { v1 }
    }

    pub const fn v2(v2: V2InfoHash) -> Self {
        Self::V2 { v2 }
    }

    pub const fn hybrid(v1: InfoHash, v2: V2InfoHash) -> Self {
        Self::Hybrid { v1, v2 }
    }

    /// Canonical durable/API key for this identity.
    ///
    /// Hybrids intentionally retain their v1 key as primary so existing
    /// 40-character API locators and durable records keep resolving. Their
    /// full v2 key is exposed by [`Self::keys`] as an alias, never as a
    /// truncated surrogate.
    pub const fn primary_key(&self) -> Option<TorrentKey> {
        match self {
            Self::V1 { v1 } | Self::Hybrid { v1, .. } => Some(TorrentKey::V1(*v1)),
            Self::V2 { v2 } => Some(TorrentKey::V2(*v2)),
            Self::Unknown => None,
        }
    }

    /// Every full locator that must resolve to this record, primary first.
    pub fn keys(&self) -> Vec<TorrentKey> {
        match self {
            Self::Unknown => Vec::new(),
            Self::V1 { v1 } => vec![TorrentKey::V1(*v1)],
            Self::V2 { v2 } => vec![TorrentKey::V2(*v2)],
            Self::Hybrid { v1, v2 } => vec![TorrentKey::V1(*v1), TorrentKey::V2(*v2)],
        }
    }

    /// The v1 SHA-1 identity, when this is a v1 or hybrid torrent.
    pub const fn v1_info_hash(&self) -> Option<InfoHash> {
        match self {
            Self::V1 { v1 } | Self::Hybrid { v1, .. } => Some(*v1),
            Self::Unknown | Self::V2 { .. } => None,
        }
    }

    /// The full v2 SHA-256 identity, when this is a v2 or hybrid torrent.
    pub const fn v2_info_hash(&self) -> Option<V2InfoHash> {
        match self {
            Self::V2 { v2 } | Self::Hybrid { v2, .. } => Some(*v2),
            Self::Unknown | Self::V1 { .. } => None,
        }
    }

    /// True when this identity has a v1 compatibility swarm.
    pub const fn supports_v1_data_plane(&self) -> bool {
        self.v1_info_hash().is_some()
    }

    /// Return the v1 peer/tracker wire value when one exists.
    ///
    /// The current data plane uses this only for v1/hybrid transfers. Pure v2
    /// callers must explicitly implement the BEP 52 piece-layer data plane;
    /// they must not coerce this into [`InfoHash`].
    pub const fn v1_peer_info_hash(&self) -> Option<PeerInfoHash> {
        match self.v1_info_hash() {
            Some(v1) => Some(PeerInfoHash::from_v1(v1)),
            None => None,
        }
    }

    /// Return the v2 peer/tracker wire value when one exists.
    pub fn v2_peer_info_hash(&self) -> Option<PeerInfoHash> {
        self.v2_info_hash().map(PeerInfoHash::from_v2)
    }

    /// Verify all identity components represented by this value against exact
    /// bencoded `info` bytes.
    pub fn matches_info_bencoded(&self, info_bytes: &[u8]) -> bool {
        match self {
            Self::Unknown => false,
            Self::V1 { v1 } => *v1 == InfoHash::from_info_bencoded(info_bytes),
            Self::V2 { v2 } => *v2 == V2InfoHash::from_info_bencoded(info_bytes),
            Self::Hybrid { v1, v2 } => {
                *v1 == InfoHash::from_info_bencoded(info_bytes)
                    && *v2 == V2InfoHash::from_info_bencoded(info_bytes)
            }
        }
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

/// Serde helper: serialize/deserialize a 32-byte digest as lowercase hex.
mod hex_serde_32 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(h: &[u8; 32], s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(h))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<[u8; 32], D::Error> {
        let s = String::deserialize(d)?;
        if s.len() != 64 {
            return Err(serde::de::Error::custom("expected 64 hex chars"));
        }
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        let mut arr = [0u8; 32];
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

    #[test]
    fn v2_info_hash_is_full_sha256_and_has_explicit_wire_truncation() {
        let bytes = b"d12:meta versioni2e4:name4:teste";
        let hash = V2InfoHash::from_info_bencoded(bytes);
        assert_eq!(hash.to_hex().len(), 64);
        assert_eq!(
            hash.to_hex(),
            "cc7144525a2daec094c604daab9be98b4bfb8a604e919e8606412dea40de2d49"
        );
        assert_eq!(hash.peer_info_hash().to_hex(), &hash.to_hex()[..40]);
        assert_eq!(
            V2InfoHash::from_magnet_multihash(&hash.to_magnet_multihash()).unwrap(),
            hash
        );
    }

    #[test]
    fn torrent_identity_never_coerces_v2_into_v1() {
        let v1 = InfoHash::from_bytes([0x11; 20]);
        let v2 = V2InfoHash::from_bytes([0x22; 32]);
        let v2_only = TorrentIdentity::v2(v2);
        assert!(v2_only.v1_info_hash().is_none());
        assert!(!v2_only.supports_v1_data_plane());
        assert_eq!(v2_only.v2_peer_info_hash().unwrap(), v2.peer_info_hash());

        let hybrid = TorrentIdentity::hybrid(v1, v2);
        assert_eq!(hybrid.v1_info_hash(), Some(v1));
        assert_eq!(hybrid.v2_info_hash(), Some(v2));
        assert!(hybrid.supports_v1_data_plane());
    }

    #[test]
    fn identity_serde_is_explicit_and_legacy_unknown_is_default() {
        let identity = TorrentIdentity::hybrid(
            InfoHash::from_bytes([0x12; 20]),
            V2InfoHash::from_bytes([0x34; 32]),
        );
        let value = serde_json::to_value(&identity).unwrap();
        assert_eq!(value["kind"], "hybrid");
        assert_eq!(
            serde_json::from_value::<TorrentIdentity>(value).unwrap(),
            identity
        );
        assert_eq!(TorrentIdentity::default(), TorrentIdentity::Unknown);
    }

    #[test]
    fn torrent_key_uses_full_unambiguous_locator_and_explicit_wire_hash() {
        let v1 = InfoHash::from_bytes([0x11; 20]);
        let v2 = V2InfoHash::from_bytes([0x22; 32]);

        let v1_key = TorrentKey::v1(v1);
        let v2_key = TorrentKey::v2(v2);
        assert_eq!(TorrentKey::from_locator(&v1.to_hex()).unwrap(), v1_key);
        assert_eq!(TorrentKey::from_locator(&v2.to_hex()).unwrap(), v2_key);
        assert_eq!(v1_key.peer_info_hash(), PeerInfoHash::from_v1(v1));
        assert_eq!(v2_key.peer_info_hash(), v2.peer_info_hash());
        assert_ne!(v2_key.to_locator(), v2_key.peer_info_hash().to_hex());

        let v1_json = serde_json::to_string(&v1_key).unwrap();
        let v2_json = serde_json::to_string(&v2_key).unwrap();
        assert_eq!(
            serde_json::from_str::<TorrentKey>(&v1_json).unwrap(),
            v1_key
        );
        assert_eq!(
            serde_json::from_str::<TorrentKey>(&v2_json).unwrap(),
            v2_key
        );
        assert!(TorrentKey::from_locator(&"a".repeat(41)).is_err());
        assert!(TorrentKey::from_locator(&"a".repeat(63)).is_err());
    }

    #[test]
    fn hybrid_identity_keeps_v1_primary_and_full_v2_alias() {
        let v1 = InfoHash::from_bytes([0x41; 20]);
        let v2 = V2InfoHash::from_bytes([0x42; 32]);
        let hybrid = TorrentIdentity::hybrid(v1, v2);

        assert_eq!(hybrid.primary_key(), Some(TorrentKey::v1(v1)));
        assert_eq!(hybrid.keys(), vec![TorrentKey::v1(v1), TorrentKey::v2(v2)]);
        assert_eq!(
            TorrentIdentity::v2(v2).primary_key(),
            Some(TorrentKey::v2(v2))
        );
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
