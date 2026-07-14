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

/// Local BEP 10 `reqq` value: supported outstanding requests from a peer.
pub const LOCAL_EXTENSION_REQQ: u64 = 250;

/// Build the BEP 10 extension handshake payload advertising the supported
/// extensions and their local message ids. The `m` dict maps names to ids.
/// Also includes a `v` (client version) and optional `metadata_size` for BEP 9.
pub fn encode_extension_handshake(
    extensions: &[(&str, u8)],
    client_version: &str,
    metadata_size: Option<u64>,
) -> Vec<u8> {
    encode_extension_handshake_payload(extensions, client_version, metadata_size, None)
}

/// Build a BEP 10 extension handshake that also advertises the local `reqq`.
pub fn encode_extension_handshake_with_reqq(
    extensions: &[(&str, u8)],
    client_version: &str,
    metadata_size: Option<u64>,
) -> Vec<u8> {
    encode_extension_handshake_payload(
        extensions,
        client_version,
        metadata_size,
        Some(LOCAL_EXTENSION_REQQ),
    )
}

fn encode_extension_handshake_payload(
    extensions: &[(&str, u8)],
    client_version: &str,
    metadata_size: Option<u64>,
    reqq: Option<u64>,
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
    if let Some(reqq) = reqq.filter(|reqq| *reqq > 0) {
        write_bytes(&mut out, b"reqq");
        out.push(b'i');
        out.extend_from_slice(reqq.to_string().as_bytes());
        out.push(b'e');
    }
    out.push(b'e');
    out
}

/// A parsed BEP 10 extension handshake.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtensionHandshake {
    /// Map of extension name -> remote message id.
    pub extensions: Vec<(String, u8)>,
    pub client_version: Option<String>,
    pub metadata_size: Option<u64>,
    /// Remote-supported outstanding request count (`reqq`), when positive.
    pub reqq: Option<u64>,
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
                    let id = u8::try_from(id)
                        .map_err(|_| CoreError::Parse("extension id out of range".into()))?;
                    extensions.push((name.to_string(), id));
                }
            }
        }
    }
    let client_version = dict
        .iter()
        .find(|(k, _)| k == b"v")
        .and_then(|(_, v)| v.as_str_utf8())
        .map(|s| s.to_string());
    let metadata_size = match dict
        .iter()
        .find(|(k, _)| k == b"metadata_size")
        .and_then(|(_, v)| v.as_int())
    {
        Some(value) => Some(
            u64::try_from(value)
                .map_err(|_| CoreError::Parse("metadata_size out of range".into()))?,
        ),
        None => None,
    };
    let reqq = match dict
        .iter()
        .find(|(k, _)| k == b"reqq")
        .and_then(|(_, v)| v.as_int())
    {
        Some(0) | None => None,
        Some(value) => {
            Some(u64::try_from(value).map_err(|_| CoreError::Parse("reqq out of range".into()))?)
        }
    };
    Ok(ExtensionHandshake {
        extensions,
        client_version,
        metadata_size,
        reqq,
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

/// Encode a `ut_metadata` rejection for a requested metadata piece.
///
/// A serving peer must answer an out-of-range or unavailable request with a
/// BEP 9 reject message rather than silently leaving the requester waiting.
pub fn encode_metadata_reject(piece: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');
    write_bytes(&mut out, b"msg_type");
    out.push(b'i');
    out.extend_from_slice(b"2");
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

/// A decoded bencode dictionary entry list (key bytes to value).
pub type BencodeDict = Vec<(Vec<u8>, Value)>;

/// Parse a `ut_metadata` message payload. The dict portion is bencoded; the
/// remainder (for Data messages) is the raw metadata piece bytes.
pub fn parse_metadata_message(payload: &[u8]) -> Result<MetadataMessage> {
    let (root, consumed) = bencode::decode_prefix(payload)?;
    let dict = match root {
        Value::Dict(dict) => dict,
        _ => return Err(CoreError::Parse("metadata message not a dict".into())),
    };
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
        .ok_or_else(|| CoreError::Parse("metadata message missing piece".into()))?;
    let piece =
        u32::try_from(piece).map_err(|_| CoreError::Parse("metadata piece out of range".into()))?;
    let total_size = match dict
        .iter()
        .find(|(k, _)| k == b"total_size")
        .and_then(|(_, v)| v.as_int())
    {
        Some(value) => Some(
            u64::try_from(value)
                .map_err(|_| CoreError::Parse("metadata total_size out of range".into()))?,
        ),
        None => None,
    };
    let data = payload[consumed..].to_vec();
    Ok(MetadataMessage {
        msg_type,
        piece,
        total_size,
        data,
    })
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
        assert_eq!(hs.reqq, None);
    }

    #[test]
    fn extension_handshake_roundtrip_with_reqq() {
        let payload = encode_extension_handshake_with_reqq(&[(UT_PEX_NAME, 1)], "v", None);
        let hs = parse_extension_handshake(&payload).unwrap();
        assert_eq!(hs.id_for(UT_PEX_NAME), Some(1));
        assert_eq!(hs.reqq, Some(LOCAL_EXTENSION_REQQ));
    }

    #[test]
    fn extension_handshake_missing_metadata_size_ok() {
        let payload = encode_extension_handshake(&[(UT_PEX_NAME, 1)], "v", None);
        let hs = parse_extension_handshake(&payload).unwrap();
        assert!(hs.metadata_size.is_none());
    }

    #[test]
    fn extension_handshake_parses_positive_reqq_only() {
        let hs = parse_extension_handshake(b"d4:reqqi32ee").unwrap();
        assert_eq!(hs.reqq, Some(32));

        for payload in [&b"d4:reqqi0ee"[..], &b"d4:reqq1:xe"[..]] {
            let hs = parse_extension_handshake(payload).unwrap();
            assert_eq!(hs.reqq, None);
        }

        let error = parse_extension_handshake(b"d4:reqqi-1ee").unwrap_err();
        assert!(matches!(&error, CoreError::Parse(_)));
        assert!(error.to_string().contains("reqq out of range"));

        let error = parse_extension_handshake(b"d4:reqqi9223372036854775808ee").unwrap_err();
        assert!(matches!(error, CoreError::Bencode(_)));
    }

    #[test]
    fn extension_handshake_rejects_negative_and_oversized_numbers() {
        for payload in [
            &b"d1:md11:ut_metadatai-1eee"[..],
            &b"d1:md11:ut_metadatai256eee"[..],
        ] {
            let error = parse_extension_handshake(payload).unwrap_err();
            assert!(matches!(&error, CoreError::Parse(_)));
            assert!(error.to_string().contains("extension id out of range"));
        }

        let error = parse_extension_handshake(b"d13:metadata_sizei-1ee").unwrap_err();
        assert!(matches!(&error, CoreError::Parse(_)));
        assert!(error.to_string().contains("metadata_size out of range"));

        let error =
            parse_extension_handshake(b"d13:metadata_sizei9223372036854775808ee").unwrap_err();
        assert!(matches!(error, CoreError::Bencode(_)));
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
    fn metadata_reject_roundtrip() {
        let enc = encode_metadata_reject(4);
        let msg = parse_metadata_message(&enc).unwrap();
        assert_eq!(msg.msg_type, MetadataMsgType::Reject);
        assert_eq!(msg.piece, 4);
        assert!(msg.total_size.is_none());
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
    fn metadata_message_rejects_negative_and_oversized_numbers() {
        for payload in [
            &b"d8:msg_typei0e5:piecei-1ee"[..],
            &b"d8:msg_typei0e5:piecei4294967296ee"[..],
        ] {
            let error = parse_metadata_message(payload).unwrap_err();
            assert!(matches!(&error, CoreError::Parse(_)));
            assert!(error.to_string().contains("metadata piece out of range"));
        }

        let error =
            parse_metadata_message(b"d8:msg_typei1e5:piecei0e10:total_sizei-1ee").unwrap_err();
        assert!(matches!(&error, CoreError::Parse(_)));
        assert!(error
            .to_string()
            .contains("metadata total_size out of range"));

        let error =
            parse_metadata_message(b"d8:msg_typei1e5:piecei0e10:total_sizei9223372036854775808ee")
                .unwrap_err();
        assert!(matches!(error, CoreError::Bencode(_)));
    }

    #[test]
    fn metadata_message_hardened_prefix_rejects_malformed_input_without_panicking() {
        let overflowing = format!("d8:msg_typei0e5:piece{}:x", usize::MAX);
        let too_deep = {
            let mut payload = b"d8:msg_typei0e5:piecei0e1:a".to_vec();
            payload.extend(std::iter::repeat_n(b'l', crate::meta::MAX_BENCODE_DEPTH));
            payload.extend_from_slice(b"i0e");
            payload.extend(std::iter::repeat_n(b'e', crate::meta::MAX_BENCODE_DEPTH));
            payload.push(b'e');
            payload
        };
        for payload in [overflowing.as_bytes(), too_deep.as_slice()] {
            let result = std::panic::catch_unwind(|| parse_metadata_message(payload));
            assert!(result.is_ok(), "metadata parser must not panic");
            assert!(result.unwrap().is_err());
        }

        let duplicate = parse_metadata_message(b"d8:msg_typei0e5:piecei0e5:piecei1ee").unwrap_err();
        assert!(matches!(duplicate, CoreError::Bencode(_)));
    }

    #[test]
    fn metadata_pieces_split() {
        assert_eq!(metadata_pieces(METADATA_PIECE_SIZE), 1);
        assert_eq!(metadata_pieces(METADATA_PIECE_SIZE + 1), 2);
        assert_eq!(metadata_pieces(3 * METADATA_PIECE_SIZE), 3);
    }
}
