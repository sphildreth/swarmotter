// SPDX-License-Identifier: Apache-2.0

//! BitTorrent TCP peer wire protocol (BEP 3).
//!
//! This module implements the protocol framing needed to download pieces from
//! real peers: handshake encode/decode, message frame encode/decode, bitfield
//! handling, and block request/piece assembly. The pure encode/decode logic is
//! unit-tested directly; an async framed reader is provided for use by the
//! engine over a `TcpStream` obtained from the network containment layer.
//!
//! All peer connections originate from [`crate::net::NetworkBinder`]; this
//! module never creates sockets directly.

use std::net::SocketAddr;

use crate::error::{CoreError, Result};
use crate::hash::InfoHash;

/// The standard BitTorrent protocol handshake pstr: "BitTorrent protocol".
pub const PSTR: &[u8] = b"BitTorrent protocol";

/// Reserved bytes (8). All zero by default; the extension-protocol bit
/// (`EXTENSION_RESERVED`) is set by callers that want BEP 10/PEX/metadata.
pub const RESERVED: [u8; 8] = [0u8; 8];

/// Block size used for piece requests (16 KiB), per BEP 3 convention.
pub const BLOCK_SIZE: u32 = 16 * 1024;

/// Maximum peer wire message size (length prefix + payload), in bytes.
/// This blocks untrusted peers from forcing very large allocations during frame
/// parsing while still leaving normal 16 KiB piece blocks fully supported.
pub const MAX_MESSAGE_LEN: usize = 4 * 1024 * 1024;

/// A peer handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
    /// Reserved bytes. Use [`crate::extensions::EXTENSION_RESERVED`] to
    /// advertise BEP 10 extension support.
    pub reserved: [u8; 8],
}

impl Handshake {
    pub fn new(info_hash: InfoHash, peer_id: [u8; 20]) -> Self {
        Self {
            info_hash,
            peer_id,
            reserved: RESERVED,
        }
    }

    /// Encode to the 68-byte wire form: pstrlen(1) pstr(19) reserved(8) info(20) peerid(20).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + PSTR.len() + 8 + 20 + 20);
        out.push(PSTR.len() as u8);
        out.extend_from_slice(PSTR);
        out.extend_from_slice(&self.reserved);
        out.extend_from_slice(self.info_hash.as_bytes());
        out.extend_from_slice(&self.peer_id);
        out
    }

    /// Decode a 68-byte handshake.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 68 {
            return Err(CoreError::Parse(format!(
                "handshake too short: {} bytes",
                buf.len()
            )));
        }
        let pstrlen = buf[0] as usize;
        if pstrlen != 19 || &buf[1..20] != PSTR {
            return Err(CoreError::Parse("handshake has wrong pstr".into()));
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buf[20..28]);
        let mut info = [0u8; 20];
        info.copy_from_slice(&buf[28..48]);
        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&buf[48..68]);
        Ok(Handshake {
            info_hash: InfoHash::from_bytes(info),
            peer_id,
            reserved,
        })
    }

    /// Whether the peer advertises BEP 10 extension protocol support.
    pub fn supports_extensions(&self) -> bool {
        self.reserved[5] & 0x10 != 0
    }
}

/// Peer wire message ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageId {
    Choke = 0,
    Unchoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    Bitfield = 5,
    Request = 6,
    Piece = 7,
    Cancel = 8,
}

impl MessageId {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Choke),
            1 => Some(Self::Unchoke),
            2 => Some(Self::Interested),
            3 => Some(Self::NotInterested),
            4 => Some(Self::Have),
            5 => Some(Self::Bitfield),
            6 => Some(Self::Request),
            7 => Some(Self::Piece),
            8 => Some(Self::Cancel),
            _ => None,
        }
    }
}

/// A decoded peer wire message. Keepalive is represented as `Keepalive`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Keepalive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece: u32,
    },
    Bitfield {
        bits: Vec<u8>,
    },
    Request {
        piece: u32,
        offset: u32,
        length: u32,
    },
    Piece {
        piece: u32,
        offset: u32,
        block: Vec<u8>,
    },
    Cancel {
        piece: u32,
        offset: u32,
        length: u32,
    },
    /// A BEP 10 extension message (wire id 20). `id` is the extension id from
    /// the extension handshake (`0` = handshake itself); `payload` is the
    /// bencoded extension payload following the extension id byte.
    Extended {
        id: u8,
        payload: Vec<u8>,
    },
    /// An unrecognized message id with its raw payload, for forward compat.
    Unknown {
        id: u8,
        payload: Vec<u8>,
    },
}

impl Message {
    /// Encode a message to its wire form: `<4-byte length><1-byte id><payload>`.
    /// Keepalive is encoded as a 4-byte zero length with no id.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Keepalive => len_prefix(0, &[]),
            Self::Choke => len_prefix(1, &[0]),
            Self::Unchoke => len_prefix(1, &[1]),
            Self::Interested => len_prefix(1, &[2]),
            Self::NotInterested => len_prefix(1, &[3]),
            Self::Have { piece } => len_prefix(5, &concat_id_payload(4, &piece.to_be_bytes())),
            Self::Bitfield { bits } => {
                len_prefix(1 + bits.len() as u32, &concat_id_payload(5, bits))
            }
            Self::Request {
                piece,
                offset,
                length,
            } => {
                let mut payload = Vec::with_capacity(13);
                payload.push(6);
                payload.extend_from_slice(&piece.to_be_bytes());
                payload.extend_from_slice(&offset.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
                len_prefix(13, &payload)
            }
            Self::Piece {
                piece,
                offset,
                block,
            } => {
                let mut payload = Vec::with_capacity(9 + block.len());
                payload.push(7);
                payload.extend_from_slice(&piece.to_be_bytes());
                payload.extend_from_slice(&offset.to_be_bytes());
                payload.extend_from_slice(block);
                len_prefix(9 + block.len() as u32, &payload)
            }
            Self::Cancel {
                piece,
                offset,
                length,
            } => {
                let mut payload = Vec::with_capacity(13);
                payload.push(8);
                payload.extend_from_slice(&piece.to_be_bytes());
                payload.extend_from_slice(&offset.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
                len_prefix(13, &payload)
            }
            Self::Extended { id, payload } => {
                let mut p = Vec::with_capacity(1 + payload.len());
                p.push(20); // BEP 10 extension message id
                p.push(*id);
                p.extend_from_slice(payload);
                len_prefix(p.len() as u32, &p)
            }
            Self::Unknown { id, payload } => {
                let mut p = Vec::with_capacity(1 + payload.len());
                p.push(*id);
                p.extend_from_slice(payload);
                len_prefix(p.len() as u32, &p)
            }
        }
    }

    /// Decode a single message from a buffer of exactly `length + 4` bytes
    /// (the 4-byte length prefix plus the body). Used by the framed reader.
    pub fn decode_frame(frame: &[u8]) -> Result<Self> {
        // frame includes the 4-byte length prefix.
        if frame.len() < 4 {
            return Err(CoreError::Parse("message frame too short".into()));
        }
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        if len > MAX_MESSAGE_LEN {
            return Err(CoreError::Parse(format!(
                "peer message too large: {len} bytes"
            )));
        }
        if frame.len() != 4 + len {
            return Err(CoreError::Parse(format!(
                "message frame length mismatch: declared {len}, have {}",
                frame.len() - 4
            )));
        }
        if len == 0 {
            return Ok(Self::Keepalive);
        }
        let id = frame[4];
        let payload = &frame[5..4 + len];
        match MessageId::from_u8(id) {
            Some(MessageId::Choke) if payload.is_empty() => Ok(Self::Choke),
            Some(MessageId::Unchoke) if payload.is_empty() => Ok(Self::Unchoke),
            Some(MessageId::Interested) if payload.is_empty() => Ok(Self::Interested),
            Some(MessageId::NotInterested) if payload.is_empty() => Ok(Self::NotInterested),
            Some(MessageId::Have) if payload.len() == 4 => Ok(Self::Have {
                piece: u32::from_be_bytes(payload.try_into().unwrap()),
            }),
            Some(MessageId::Bitfield) => Ok(Self::Bitfield {
                bits: payload.to_vec(),
            }),
            Some(MessageId::Request) if payload.len() == 12 => {
                let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                Ok(Self::Request {
                    piece,
                    offset,
                    length,
                })
            }
            Some(MessageId::Piece) if payload.len() >= 8 => {
                let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let block = payload[8..].to_vec();
                Ok(Self::Piece {
                    piece,
                    offset,
                    block,
                })
            }
            Some(MessageId::Cancel) if payload.len() == 12 => {
                let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let offset = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                Ok(Self::Cancel {
                    piece,
                    offset,
                    length,
                })
            }
            // BEP 10 extension message: first payload byte is the extension id.
            _ if id == 20 && !payload.is_empty() => {
                let ext_id = payload[0];
                Ok(Self::Extended {
                    id: ext_id,
                    payload: payload[1..].to_vec(),
                })
            }
            _ => Ok(Self::Unknown {
                id,
                payload: payload.to_vec(),
            }),
        }
    }
}

fn len_prefix(len: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn concat_id_payload(id: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(id);
    out.extend_from_slice(payload);
    out
}

/// Bitfield helper: which pieces a peer reports it has.
#[derive(Debug, Clone, Default)]
pub struct Bitfield {
    pub bits: Vec<u8>,
    pub piece_count: usize,
}

impl Bitfield {
    pub fn new(piece_count: usize) -> Self {
        let bytes = piece_count.div_ceil(8);
        Self {
            bits: vec![0u8; bytes],
            piece_count,
        }
    }

    pub fn from_bytes(bits: Vec<u8>, piece_count: usize) -> Self {
        let max_len = piece_count.div_ceil(8);
        let bits = if bits.len() > max_len {
            bits[..max_len].to_vec()
        } else {
            bits
        };
        Self { bits, piece_count }
    }

    pub fn set(&mut self, index: usize) {
        if index < self.piece_count {
            let byte = index / 8;
            if byte < self.bits.len() {
                self.bits[byte] |= 0x80 >> (index % 8);
            }
        }
    }

    pub fn has(&self, index: usize) -> bool {
        if index >= self.piece_count {
            return false;
        }
        let byte = index / 8;
        if byte >= self.bits.len() {
            return false;
        }
        self.bits[byte] & (0x80 >> (index % 8)) != 0
    }

    pub fn count(&self) -> usize {
        let full = self.piece_count / 8;
        let rem = self.piece_count % 8;
        let mut count = self
            .bits
            .iter()
            .take(full)
            .map(|b| b.count_ones() as usize)
            .sum();
        if rem > 0 {
            if let Some(b) = self.bits.get(full) {
                let mask = 0xFFu8 << (8 - rem);
                count += (b & mask).count_ones() as usize;
            }
        }
        count
    }

    /// Encode as a bitfield message.
    pub fn encode_message(&self) -> Message {
        Message::Bitfield {
            bits: self.bits.clone(),
        }
    }

    /// Indices this peer has that we do not.
    pub fn missing_from(&self, have: &Bitfield) -> Vec<usize> {
        (0..self.piece_count)
            .filter(|&i| self.has(i) && !have.has(i))
            .collect()
    }
}

/// A piece assembler: accumulates downloaded blocks for one piece and reports
/// completion. Blocks may arrive out of order.
#[derive(Debug)]
pub struct PieceAssembler {
    pub piece_index: u32,
    pub piece_length: usize,
    pub buf: Vec<u8>,
    pub received: Vec<bool>,
    received_count: usize,
}

impl PieceAssembler {
    pub fn new(piece_index: u32, piece_length: usize) -> Self {
        Self {
            piece_index,
            piece_length,
            buf: vec![0u8; piece_length],
            received: vec![false; blocks_for(piece_length)],
            received_count: 0,
        }
    }

    /// Write a block at the given offset. Returns true when the piece is now
    /// complete (all blocks received).
    pub fn add_block(&mut self, offset: u32, block: &[u8]) -> Result<bool> {
        let off = offset as usize;
        if off >= self.piece_length {
            return Err(CoreError::Parse(format!(
                "block offset {} out of range for piece length {}",
                offset, self.piece_length
            )));
        }
        let end = off + block.len();
        if end > self.piece_length {
            return Err(CoreError::Parse(format!(
                "block end {} exceeds piece length {}",
                end, self.piece_length
            )));
        }
        self.buf[off..end].copy_from_slice(block);
        let block_index = off / BLOCK_SIZE as usize;
        if block_index < self.received.len() && !self.received[block_index] {
            self.received[block_index] = true;
            self.received_count += 1;
        }
        Ok(self.received_count == self.received.len())
    }

    /// Return the assembled piece data once complete.
    pub fn data(&self) -> &[u8] {
        &self.buf[..self.piece_length]
    }
}

/// How many 16KiB blocks fit in a piece of the given length.
pub fn blocks_for(piece_length: usize) -> usize {
    piece_length.div_ceil(BLOCK_SIZE as usize)
}

/// Enumerate the (offset, length) block requests needed for a piece.
pub fn block_requests(piece_length: u32) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    let mut offset = 0u32;
    while offset < piece_length {
        let len = std::cmp::min(BLOCK_SIZE, piece_length - offset);
        out.push((offset, len));
        offset += len;
    }
    out
}

/// A discovered peer address from a tracker or DHT/PEX response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerAddr {
    pub ip: std::net::IpAddr,
    pub port: u16,
}

impl PeerAddr {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }

    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self {
            ip: addr.ip(),
            port: addr.port(),
        }
    }
}

/// Async framed reader for the peer wire protocol over a duplex stream.
///
/// Reads full handshake and then length-prefixed messages. The stream is
/// obtained from the network containment layer; this type never opens
/// sockets itself.
pub struct PeerReader<S> {
    stream: S,
}

impl<S: tokio::io::AsyncRead + Unpin> PeerReader<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    /// Read exactly `n` bytes into `out`.
    async fn read_exact(&mut self, n: usize) -> Result<Vec<u8>> {
        use tokio::io::AsyncReadExt;
        let mut out = vec![0u8; n];
        self.stream
            .read_exact(&mut out)
            .await
            .map_err(CoreError::from)?;
        Ok(out)
    }

    /// Read and decode the 68-byte handshake.
    pub async fn read_handshake(&mut self) -> Result<Handshake> {
        let buf = self.read_exact(68).await?;
        Handshake::decode(&buf)
    }

    /// Read one length-prefixed message (or keepalive). Returns `None` on a
    /// clean EOF between messages.
    pub async fn read_message(&mut self) -> Result<Option<Message>> {
        use tokio::io::AsyncReadExt;
        // Read 4-byte length. Allow clean EOF here.
        let mut len_buf = [0u8; 4];
        let mut filled = 0;
        loop {
            match self.stream.read(&mut len_buf[filled..]).await {
                Ok(0) => {
                    if filled == 0 {
                        return Ok(None);
                    }
                    return Err(CoreError::Parse("peer stream EOF mid-length".into()));
                }
                Ok(n) => {
                    filled += n;
                    if filled == 4 {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    if filled == 0 {
                        return Ok(None);
                    }
                    return Err(CoreError::from(e));
                }
                Err(e) => return Err(CoreError::from(e)),
            }
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_MESSAGE_LEN {
            return Err(CoreError::Parse(format!(
                "peer message too large: {len} bytes"
            )));
        }
        if len == 0 {
            return Ok(Some(Message::Keepalive));
        }
        let mut body = vec![0u8; len];
        self.stream
            .read_exact(&mut body)
            .await
            .map_err(CoreError::from)?;
        let mut frame = Vec::with_capacity(4 + len);
        frame.extend_from_slice(&len_buf);
        frame.extend_from_slice(&body);
        Message::decode_frame(&frame).map(Some)
    }
}

/// Write a message to a duplex stream.
pub async fn write_message<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    msg: &Message,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let enc = msg.encode();
    w.write_all(&enc).await.map_err(CoreError::from)
}

/// Write a handshake to a duplex stream.
pub async fn write_handshake<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    hs: &Handshake,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    w.write_all(&hs.encode()).await.map_err(CoreError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn handshake_roundtrip() {
        let info = InfoHash::from_bytes([7u8; 20]);
        let hs = Handshake {
            info_hash: info,
            peer_id: *b"-SW001-abcdefghij123",
            reserved: RESERVED,
        };
        let enc = hs.encode();
        assert_eq!(enc.len(), 68);
        let back = Handshake::decode(&enc).unwrap();
        assert_eq!(back, hs);
    }

    #[test]
    fn handshake_rejects_wrong_pstr() {
        let mut bad = vec![0u8; 68];
        bad[0] = 19;
        bad[1..20].copy_from_slice(b"NotTorrent protocol");
        assert!(Handshake::decode(&bad).is_err());
    }

    #[test]
    fn message_roundtrip_all_kinds() {
        let cases = vec![
            Message::Keepalive,
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
            Message::Have { piece: 5 },
            Message::Bitfield {
                bits: vec![0b10100000, 0b00000001],
            },
            Message::Request {
                piece: 1,
                offset: 0,
                length: BLOCK_SIZE,
            },
            Message::Piece {
                piece: 1,
                offset: 0,
                block: vec![0xAB; 8],
            },
            Message::Cancel {
                piece: 2,
                offset: 16,
                length: 32,
            },
        ];
        for m in cases {
            let enc = m.encode();
            let back = Message::decode_frame(&enc).unwrap();
            assert_eq!(back, m, "mismatch for {:?}", m);
        }
    }

    #[test]
    fn keepalive_encodes_zero_length() {
        let enc = Message::Keepalive.encode();
        assert_eq!(enc, vec![0, 0, 0, 0]);
    }

    #[test]
    fn unknown_message_preserved() {
        let m = Message::Unknown {
            id: 42,
            payload: vec![1, 2, 3],
        };
        let enc = m.encode();
        let back = Message::decode_frame(&enc).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn bitfield_set_get() {
        let mut bf = Bitfield::new(20);
        assert!(!bf.has(0));
        bf.set(0);
        bf.set(7);
        bf.set(19);
        assert!(bf.has(0));
        assert!(bf.has(7));
        assert!(bf.has(19));
        assert!(!bf.has(8));
        assert_eq!(bf.count(), 3);
    }

    #[test]
    fn bitfield_missing_from() {
        let mut peer = Bitfield::new(16);
        for i in 0..16 {
            peer.set(i);
        }
        let mut ours = Bitfield::new(16);
        ours.set(0);
        ours.set(1);
        let missing = peer.missing_from(&ours);
        assert_eq!(missing, (2..16).collect::<Vec<_>>());
    }

    #[test]
    fn piece_assembler_completes_out_of_order() {
        let mut pa = PieceAssembler::new(0, 3 * BLOCK_SIZE as usize);
        let off1 = BLOCK_SIZE;
        let off2 = 2 * BLOCK_SIZE;
        assert!(!pa.add_block(off1, &[1u8; BLOCK_SIZE as usize]).unwrap());
        assert!(!pa.add_block(off2, &[2u8; BLOCK_SIZE as usize]).unwrap());
        assert!(pa.add_block(0, &[0u8; BLOCK_SIZE as usize]).unwrap());
        assert_eq!(pa.data().len(), 3 * BLOCK_SIZE as usize);
    }

    #[test]
    fn piece_assembler_rejects_overflow() {
        let mut pa = PieceAssembler::new(0, 100);
        assert!(pa.add_block(50, &[1u8; 60]).is_err());
    }

    #[test]
    fn block_requests_align_to_block_size() {
        let reqs = block_requests(3 * BLOCK_SIZE + 5);
        assert_eq!(reqs.len(), 4);
        assert_eq!(reqs[0], (0, BLOCK_SIZE));
        assert_eq!(reqs[3], (3 * BLOCK_SIZE, 5));
    }

    #[test]
    fn block_requests_exact_piece() {
        let reqs = block_requests(BLOCK_SIZE);
        assert_eq!(reqs, vec![(0, BLOCK_SIZE)]);
    }

    #[test]
    fn bitfield_set_and_has_are_safe_for_short_payloads() {
        let mut bf = Bitfield::from_bytes(vec![0b1000_0000], 16);
        assert!(bf.has(0));
        assert!(!bf.has(15));
        bf.set(15);
        assert!(!bf.has(15));
    }

    #[test]
    fn bitfield_from_bytes_truncates_oversized_payload() {
        let bits = vec![0xFF, 0xFF, 0xFF];
        let bf = Bitfield::from_bytes(bits.clone(), 8);
        assert_eq!(bf.bits, vec![0xFF]);
    }

    #[tokio::test]
    async fn read_message_rejects_excessive_length() {
        let mut peer_reader = {
            let (client, mut server) = tokio::io::duplex(8);
            let frame = (MAX_MESSAGE_LEN as u32 + 1).to_be_bytes().to_vec();
            server
                .write_all(&frame)
                .await
                .expect("write oversized length prefix");
            server.shutdown().await.expect("close peer");
            PeerReader::new(client)
        };
        let err = peer_reader.read_message().await.unwrap_err();
        assert!(matches!(err, CoreError::Parse(_)));
        let msg = err.to_string();
        assert!(msg.contains("peer message too large"));
    }
}
