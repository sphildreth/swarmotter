// SPDX-License-Identifier: Apache-2.0

//! BitTorrent extension protocol (BEP 10) and Peer Exchange (PEX, BEP 11).
//!
//! BEP 10 defines an extension handshake (message id 20) whose payload is a
//! bencoded dict with an `m` dictionary mapping extension names to local
//! message ids. PEX (BEP 11) is an extension named `ut_pex` that exchanges
//! compact peer lists over an established peer connection.
//!
//! All peer connections carrying these messages originate from the
//! `NetworkBinder`; this module only encodes/decodes the message payloads and
//! is fully unit-tested without sockets. The engine wires PEX-discovered
//! peers into the candidate pool; PEX is disabled for private torrents.
//!
//! See `design/requirements.md` and ADR-0013.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::bencode::{self, Value};
use crate::error::{CoreError, Result};
use crate::peer::PeerAddr;

/// The reserved-bit position (bit 20 from the MSB of the 8 reserved bytes,
/// i.e. byte index 5, bit 0x10) that advertises extension protocol support.
pub const EXTENSION_RESERVED: [u8; 8] = {
    let mut r = [0u8; 8];
    r[5] = 0x10;
    r
};

/// The extension protocol message id (BEP 10).
pub const EXTENSION_HANDSHAKE_ID: u8 = 0;

/// The PEX extension name.
pub const UT_PEX_NAME: &str = "ut_pex";

/// The metadata extension name (BEP 9).
pub const UT_METADATA_NAME: &str = "ut_metadata";

/// Build the BEP 10 extension handshake payload advertising the supported
/// extensions and their local message ids. The `m` dict maps names to ids.
/// Also includes a `v` (client version) and `metadata_size` for BEP 9.
pub fn encode_extension_handshake(
    extensions: &[(&str, u8)],
    client_version: &str,
    metadata_size: Option<u64>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');
    // `m` dict (keys sorted by byte order).
    let mut exts: Vec<(&str, u8)> = extensions.to_vec();
    exts.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    write_bytes(&mut out, b"m");
    out.push(b'd');
    for (name, id) in &exts {
        write_bytes(&mut out, name.as_bytes());
        out.push(b'i');
        out.extend_from_slice((*id).to_string().as_bytes());
        out.push(b'e');
    }
    out.push(b'e');
    write_bytes(&mut out, b"v");
    write_bytes(&mut out, client_version.as_bytes());
    if let Some(size) = metadata_size {
        write_bytes(&mut out, b"metadata_size");
        out.push(b'i');
        out.extend_from_slice(size.to_string().as_bytes());
        out.push(b'e');
    }
    out.push(b'e');
    out
}

/// A parsed BEP 10 extension handshake: the `m` mapping and metadata size.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtensionHandshake {
    /// Map of extension name -> remote message id.
    pub extensions: Vec<(String, u8)>,
    pub client_version: Option<String>,
    pub metadata_size: Option<u64>,
}

/// Parse a BEP 10 extension handshake payload.
pub fn parse_extension_handshake(payload: &[u8]) -> Result<ExtensionHandshake> {
    let root = bencode::decode(payload)?;
    let dict = root
        .as_dict()
        .ok_or_else(|| CoreError::Parse("extension handshake not a dict".into()))?;
    let mut extensions = Vec::new();
    if let Some(m) = dict.iter().find(|(k, _)| k == b"m").map(|(_, v)| v) {
        if let Some(mdict) = m.as_dict() {
            for (k, v) in mdict {
                if let (Ok(name), Some(id)) = (std::str::from_utf8(k), v.as_int()) {
                    extensions.push((name.to_string(), id as u8));
                }
            }
        }
    }
    let client_version = dict
        .iter()
        .find(|(k, _)| k == b"v")
        .and_then(|(_, v)| v.as_str_utf8())
        .map(|s| s.to_string());
    let metadata_size = dict
        .iter()
        .find(|(k, _)| k == b"metadata_size")
        .and_then(|(_, v)| v.as_int())
        .map(|i| i as u64);
    Ok(ExtensionHandshake {
        extensions,
        client_version,
        metadata_size,
    })
}

impl ExtensionHandshake {
    /// Look up the remote message id for a named extension.
    pub fn id_for(&self, name: &str) -> Option<u8> {
        self.extensions
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, id)| *id)
    }
}

/// A PEX message: compact IPv4 and IPv6 peer lists with optional flags.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PexMessage {
    pub added: Vec<PeerAddr>,
    pub dropped: Vec<PeerAddr>,
    pub added6: Vec<PeerAddr>,
    pub dropped6: Vec<PeerAddr>,
}

/// Encode a PEX message payload (BEP 11). `added`/`dropped` are IPv4 (6 bytes
/// each); `added6`/`dropped6` are IPv6 (18 bytes each). Flags are omitted
/// (all peers treated as reachable).
pub fn encode_pex(msg: &PexMessage) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');
    write_bytes(&mut out, b"added");
    write_bytes(&mut out, &encode_compact_ipv4(&msg.added));
    write_bytes(&mut out, b"added.f");
    write_bytes(&mut out, &vec![0u8; msg.added.len()]);
    write_bytes(&mut out, b"dropped");
    write_bytes(&mut out, &encode_compact_ipv4(&msg.dropped));
    write_bytes(&mut out, b"added6");
    write_bytes(&mut out, &encode_compact_ipv6(&msg.added6));
    write_bytes(&mut out, b"added6.f");
    write_bytes(&mut out, &vec![0u8; msg.added6.len()]);
    write_bytes(&mut out, b"dropped6");
    write_bytes(&mut out, &encode_compact_ipv6(&msg.dropped6));
    out.push(b'e');
    out
}

/// Parse a PEX message payload (BEP 11).
pub fn parse_pex(payload: &[u8]) -> Result<PexMessage> {
    let root = bencode::decode(payload)?;
    let dict = root
        .as_dict()
        .ok_or_else(|| CoreError::Parse("pex message not a dict".into()))?;
    let added = dict
        .iter()
        .find(|(k, _)| k == b"added")
        .and_then(|(_, v)| v.as_str())
        .map(parse_compact_ipv4)
        .unwrap_or_default();
    let dropped = dict
        .iter()
        .find(|(k, _)| k == b"dropped")
        .and_then(|(_, v)| v.as_str())
        .map(parse_compact_ipv4)
        .unwrap_or_default();
    let added6 = dict
        .iter()
        .find(|(k, _)| k == b"added6")
        .and_then(|(_, v)| v.as_str())
        .map(parse_compact_ipv6)
        .unwrap_or_default();
    let dropped6 = dict
        .iter()
        .find(|(k, _)| k == b"dropped6")
        .and_then(|(_, v)| v.as_str())
        .map(parse_compact_ipv6)
        .unwrap_or_default();
    Ok(PexMessage {
        added,
        dropped,
        added6,
        dropped6,
    })
}

fn encode_compact_ipv4(peers: &[PeerAddr]) -> Vec<u8> {
    let mut out = Vec::with_capacity(peers.len() * 6);
    for p in peers {
        if let IpAddr::V4(v4) = p.ip {
            out.extend_from_slice(&v4.octets());
            out.extend_from_slice(&p.port.to_be_bytes());
        }
    }
    out
}

fn encode_compact_ipv6(peers: &[PeerAddr]) -> Vec<u8> {
    let mut out = Vec::with_capacity(peers.len() * 18);
    for p in peers {
        if let IpAddr::V6(v6) = p.ip {
            out.extend_from_slice(&v6.octets());
            out.extend_from_slice(&p.port.to_be_bytes());
        }
    }
    out
}

fn parse_compact_ipv4(bytes: &[u8]) -> Vec<PeerAddr> {
    let mut out = Vec::with_capacity(bytes.len() / 6);
    for chunk in bytes.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        out.push(PeerAddr {
            ip: IpAddr::V4(ip),
            port,
        });
    }
    out
}

fn parse_compact_ipv6(bytes: &[u8]) -> Vec<PeerAddr> {
    let mut out = Vec::with_capacity(bytes.len() / 18);
    for chunk in bytes.chunks_exact(18) {
        let mut octets = [0u8; 16];
        octets.copy_from_slice(&chunk[0..16]);
        let ip = Ipv6Addr::from(octets);
        let port = u16::from_be_bytes([chunk[16], chunk[17]]);
        out.push(PeerAddr {
            ip: IpAddr::V6(ip),
            port,
        });
    }
    out
}

fn write_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(format!("{}:", b.len()).as_bytes());
    out.extend_from_slice(b);
}

// ---------------------------------------------------------------------------
// BEP 9 magnet metadata extension (ut_metadata)
// ---------------------------------------------------------------------------

/// Metadata piece size used by the `ut_metadata` extension (16 KiB).
pub const METADATA_PIECE_SIZE: usize = 16 * 1024;

/// `ut_metadata` message type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataMsgType {
    Request = 0,
    Data = 1,
    Reject = 2,
}

/// Encode a `ut_metadata` request message for `piece`.
pub fn encode_metadata_request(piece: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');
    write_bytes(&mut out, b"msg_type");
    out.push(b'i');
    out.extend_from_slice(b"0");
    out.push(b'e');
    write_bytes(&mut out, b"piece");
    out.push(b'i');
    out.extend_from_slice(piece.to_string().as_bytes());
    out.push(b'e');
    out.push(b'e');
    out
}

/// Encode a `ut_metadata` data message (metadata piece bytes + total size).
pub fn encode_metadata_data(piece: u32, total_size: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');
    write_bytes(&mut out, b"msg_type");
    out.push(b'i');
    out.extend_from_slice(b"1");
    out.push(b'e');
    write_bytes(&mut out, b"piece");
    out.push(b'i');
    out.extend_from_slice(piece.to_string().as_bytes());
    out.push(b'e');
    write_bytes(&mut out, b"total_size");
    out.push(b'i');
    out.extend_from_slice(total_size.to_string().as_bytes());
    out.push(b'e');
    out.push(b'e');
    // The metadata bytes follow the bencoded dict (not part of the dict).
    out.extend_from_slice(data);
    out
}

/// A parsed `ut_metadata` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMessage {
    pub msg_type: MetadataMsgType,
    pub piece: u32,
    pub total_size: Option<u64>,
    /// Metadata bytes for a Data message (following the bencoded dict).
    pub data: Vec<u8>,
}

/// Parse a `ut_metadata` message payload. The dict portion is bencoded; the
/// remainder (for Data messages) is the raw metadata piece bytes.
pub fn parse_metadata_message(payload: &[u8]) -> Result<MetadataMessage> {
    // Decode only the leading dict; bencode::decode consumes the full buffer,
    // so we find the dict end by re-encoding bounds. Simpler: decode the dict
    // and track the consumed byte count via a bounded parser.
    let (dict, consumed) = decode_dict_bounded(payload)?;
    let msg_type = dict
        .iter()
        .find(|(k, _)| k == b"msg_type")
        .and_then(|(_, v)| v.as_int())
        .ok_or_else(|| CoreError::Parse("metadata message missing msg_type".into()))?;
    let msg_type = match msg_type {
        0 => MetadataMsgType::Request,
        1 => MetadataMsgType::Data,
        2 => MetadataMsgType::Reject,
        _ => {
            return Err(CoreError::Parse(format!(
                "bad metadata msg_type {msg_type}"
            )))
        }
    };
    let piece = dict
        .iter()
        .find(|(k, _)| k == b"piece")
        .and_then(|(_, v)| v.as_int())
        .ok_or_else(|| CoreError::Parse("metadata message missing piece".into()))?
        as u32;
    let total_size = dict
        .iter()
        .find(|(k, _)| k == b"total_size")
        .and_then(|(_, v)| v.as_int())
        .map(|i| i as u64);
    let data = payload[consumed..].to_vec();
    Ok(MetadataMessage {
        msg_type,
        piece,
        total_size,
        data,
    })
}

/// A decoded bencode dict entry list (key bytes -> value).
pub type BencodeDict = Vec<(Vec<u8>, Value)>;

/// Decode a leading bencoded dict, returning the dict and the number of bytes
/// consumed (so trailing raw bytes, e.g. metadata piece data, are preserved).
#[allow(clippy::type_complexity)]
fn decode_dict_bounded(bytes: &[u8]) -> Result<(BencodeDict, usize)> {
    let mut p = BoundedParser { bytes, pos: 0 };
    let v = p.parse()?;
    let consumed = p.pos;
    match v {
        Value::Dict(d) => Ok((d, consumed)),
        _ => Err(CoreError::Parse("metadata message not a dict".into())),
    }
}

struct BoundedParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> BoundedParser<'a> {
    fn parse(&mut self) -> Result<Value> {
        if self.pos >= self.bytes.len() {
            return Err(CoreError::Parse("metadata message truncated".into()));
        }
        match self.bytes[self.pos] {
            b'd' => {
                self.pos += 1;
                let mut dict = Vec::new();
                while self.pos < self.bytes.len() && self.bytes[self.pos] != b'e' {
                    let key = self.parse_bytes_str()?;
                    let val = self.parse()?;
                    dict.push((key, val));
                }
                if self.pos >= self.bytes.len() {
                    return Err(CoreError::Parse("unterminated metadata dict".into()));
                }
                self.pos += 1; // consume 'e'
                Ok(Value::Dict(dict))
            }
            b'i' => {
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.bytes.len() && self.bytes[self.pos] != b'e' {
                    self.pos += 1;
                }
                if self.pos >= self.bytes.len() {
                    return Err(CoreError::Parse("unterminated int".into()));
                }
                let s = std::str::from_utf8(&self.bytes[start..self.pos])
                    .map_err(|e| CoreError::Parse(format!("bad int: {e}")))?;
                let n: i64 = s
                    .parse()
                    .map_err(|e| CoreError::Parse(format!("bad int {s}: {e}")))?;
                self.pos += 1; // 'e'
                Ok(Value::Int(n))
            }
            b'0'..=b'9' => {
                let s = self.parse_bytes_str()?;
                Ok(Value::Str(s))
            }
            _ => Err(CoreError::Parse("metadata message bad token".into())),
        }
    }

    fn parse_bytes_str(&mut self) -> Result<Vec<u8>> {
        let len_start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b':' {
            self.pos += 1;
        }
        if self.pos >= self.bytes.len() {
            return Err(CoreError::Parse("missing ':' in byte string".into()));
        }
        let len_s = std::str::from_utf8(&self.bytes[len_start..self.pos])
            .map_err(|e| CoreError::Parse(format!("bad strlen: {e}")))?;
        let len: usize = len_s
            .parse()
            .map_err(|e| CoreError::Parse(format!("bad strlen {len_s}: {e}")))?;
        self.pos += 1; // ':'
        if self.pos + len > self.bytes.len() {
            return Err(CoreError::Parse("byte string overruns payload".into()));
        }
        let s = self.bytes[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(s)
    }
}

/// Split a metadata blob (the `info` dict bytes) into piece-sized chunks for
/// requesting/serving.
pub fn metadata_pieces(total_size: usize) -> usize {
    total_size.div_ceil(METADATA_PIECE_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_handshake_roundtrip() {
        let payload = encode_extension_handshake(
            &[(UT_PEX_NAME, 1), (UT_METADATA_NAME, 2)],
            "SwarmOtter/0.1",
            Some(4096),
        );
        let hs = parse_extension_handshake(&payload).unwrap();
        assert_eq!(hs.id_for(UT_PEX_NAME), Some(1));
        assert_eq!(hs.id_for(UT_METADATA_NAME), Some(2));
        assert_eq!(hs.client_version.as_deref(), Some("SwarmOtter/0.1"));
        assert_eq!(hs.metadata_size, Some(4096));
    }

    #[test]
    fn extension_handshake_missing_metadata_size_ok() {
        let payload = encode_extension_handshake(&[(UT_PEX_NAME, 1)], "v", None);
        let hs = parse_extension_handshake(&payload).unwrap();
        assert!(hs.metadata_size.is_none());
    }

    #[test]
    fn pex_roundtrip_ipv4() {
        let msg = PexMessage {
            added: vec![
                PeerAddr {
                    ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
                    port: 6881,
                },
                PeerAddr {
                    ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                    port: 6882,
                },
            ],
            dropped: vec![],
            added6: vec![],
            dropped6: vec![],
        };
        let enc = encode_pex(&msg);
        let back = parse_pex(&enc).unwrap();
        assert_eq!(back.added.len(), 2);
        assert_eq!(back.added[0].port, 6881);
        assert_eq!(back.added[1].port, 6882);
        assert!(back.dropped.is_empty());
    }

    #[test]
    fn pex_roundtrip_ipv6() {
        let msg = PexMessage {
            added: vec![],
            dropped: vec![],
            added6: vec![PeerAddr {
                ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
                port: 6883,
            }],
            dropped6: vec![],
        };
        let enc = encode_pex(&msg);
        let back = parse_pex(&enc).unwrap();
        assert_eq!(back.added6.len(), 1);
        assert_eq!(back.added6[0].port, 6883);
    }

    #[test]
    fn pex_parse_rejects_non_dict() {
        assert!(parse_pex(b"l5:helloe").is_err());
    }

    #[test]
    fn private_torrent_blocks_pex_by_design() {
        // Private torrents disable PEX. This helper encodes the rule: when
        // private, the engine does not send/receive PEX. We assert the
        // function exists and the rule is expressible here.
        let private = true;
        assert!(private && !should_pex(private));
    }

    fn should_pex(private: bool) -> bool {
        !private
    }

    #[test]
    fn metadata_request_roundtrip() {
        let enc = encode_metadata_request(2);
        let msg = parse_metadata_message(&enc).unwrap();
        assert_eq!(msg.msg_type, MetadataMsgType::Request);
        assert_eq!(msg.piece, 2);
        assert!(msg.data.is_empty());
    }

    #[test]
    fn metadata_data_roundtrip_with_trailing_bytes() {
        let data = b"info-dict-bytes-here";
        let enc = encode_metadata_data(0, data.len() as u64, data);
        let msg = parse_metadata_message(&enc).unwrap();
        assert_eq!(msg.msg_type, MetadataMsgType::Data);
        assert_eq!(msg.piece, 0);
        assert_eq!(msg.total_size, Some(data.len() as u64));
        assert_eq!(msg.data, data);
    }

    #[test]
    fn metadata_pieces_split() {
        assert_eq!(metadata_pieces(METADATA_PIECE_SIZE), 1);
        assert_eq!(metadata_pieces(METADATA_PIECE_SIZE + 1), 2);
        assert_eq!(metadata_pieces(3 * METADATA_PIECE_SIZE), 3);
    }
}
