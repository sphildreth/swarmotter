// SPDX-License-Identifier: Apache-2.0

//! uTP packet header (BEP 29) encode/decode.

use crate::error::{CoreError, Result};

/// uTP protocol version (1).
pub const UTP_VERSION: u8 = 1;

/// uTP packet types (high nibble of byte 0; low nibble is the first extension
/// id, 0 = none).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UtpType {
    Data = 1,
    Fin = 2,
    State = 3,
    Reset = 4,
    Syn = 5,
}

impl UtpType {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Data),
            2 => Some(Self::Fin),
            3 => Some(Self::State),
            4 => Some(Self::Reset),
            5 => Some(Self::Syn),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// A parsed uTP packet header (20 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtpHeader {
    pub typ: UtpType,
    pub version: u8,
    /// First extension id (0 = none, 1 = SACK). Additional extensions, if any,
    /// follow the header as length-prefixed blocks; this implementation
    /// processes the SACK extension via [`crate::utp::sack`].
    pub extension: u8,
    pub connection_id: u16,
    /// Sender's microsecond timestamp (32-bit wrapping).
    pub timestamp_micros: u32,
    /// Echo of the peer's most recent timestamp (32-bit wrapping).
    pub timestamp_delta_micros: u32,
    /// Advertised receive window in bytes.
    pub window_size: u32,
    pub seq_number: u16,
    pub ack_number: u16,
}

impl UtpHeader {
    pub const SIZE: usize = 20;

    /// Encode the header + payload to wire bytes.
    pub fn encode(&self, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(20 + payload.len());
        let type_and_ext = (self.typ.as_u8() << 4) | (self.extension & 0x0f);
        out.push(type_and_ext);
        out.push(self.version);
        out.extend_from_slice(&self.connection_id.to_be_bytes());
        out.extend_from_slice(&self.timestamp_micros.to_be_bytes());
        out.extend_from_slice(&self.timestamp_delta_micros.to_be_bytes());
        out.extend_from_slice(&self.window_size.to_be_bytes());
        out.extend_from_slice(&self.seq_number.to_be_bytes());
        out.extend_from_slice(&self.ack_number.to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Decode a header from the front of a buffer, returning the header and
    /// the remaining payload slice.
    pub fn decode(buf: &[u8]) -> Result<(UtpHeader, &[u8])> {
        if buf.len() < 20 {
            return Err(CoreError::Parse(format!(
                "utp packet too short: {} bytes",
                buf.len()
            )));
        }
        let type_and_ext = buf[0];
        let typ = UtpType::from_u8(type_and_ext >> 4)
            .ok_or_else(|| CoreError::Parse(format!("bad utp type {}", type_and_ext >> 4)))?;
        let extension = type_and_ext & 0x0f;
        let version = buf[1];
        let connection_id = u16::from_be_bytes([buf[2], buf[3]]);
        let timestamp_micros = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let timestamp_delta_micros = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let window_size = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let seq_number = u16::from_be_bytes([buf[16], buf[17]]);
        let ack_number = u16::from_be_bytes([buf[18], buf[19]]);
        Ok((
            UtpHeader {
                typ,
                version,
                extension,
                connection_id,
                timestamp_micros,
                timestamp_delta_micros,
                window_size,
                seq_number,
                ack_number,
            },
            &buf[20..],
        ))
    }
}

/// Current microsecond timestamp (monotonic-ish via std, 32-bit wrapping).
pub fn now_micros() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_micros() as u64 & 0xffffffff) as u32)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let h = UtpHeader {
            typ: UtpType::Data,
            version: UTP_VERSION,
            extension: 0,
            connection_id: 0x1234,
            timestamp_micros: 0x11223344,
            timestamp_delta_micros: 0x55667788,
            window_size: 0x00010000,
            seq_number: 5,
            ack_number: 4,
        };
        let enc = h.encode(b"payload bytes");
        let (back, payload) = UtpHeader::decode(&enc).unwrap();
        assert_eq!(back, h);
        assert_eq!(payload, b"payload bytes");
    }

    #[test]
    fn header_decode_rejects_short() {
        assert!(UtpHeader::decode(&[0u8; 10]).is_err());
    }

    #[test]
    fn header_decode_rejects_bad_type() {
        let mut bad = [0u8; 20];
        bad[0] = 0x90; // type 9
        assert!(UtpHeader::decode(&bad).is_err());
    }

    #[test]
    fn type_encoding() {
        let h = UtpHeader {
            typ: UtpType::Syn,
            version: UTP_VERSION,
            extension: 0,
            connection_id: 1,
            timestamp_micros: 0,
            timestamp_delta_micros: 0,
            window_size: 0,
            seq_number: 1,
            ack_number: 0,
        };
        let enc = h.encode(&[]);
        assert_eq!(enc[0] >> 4, UtpType::Syn.as_u8());
        let (back, _) = UtpHeader::decode(&enc).unwrap();
        assert_eq!(back.typ, UtpType::Syn);
    }

    #[test]
    fn sack_extension_nibble_encoded() {
        let h = UtpHeader {
            typ: UtpType::State,
            version: UTP_VERSION,
            extension: 1,
            connection_id: 2,
            timestamp_micros: 0,
            timestamp_delta_micros: 0,
            window_size: 0,
            seq_number: 1,
            ack_number: 0,
        };
        let enc = h.encode(&[]);
        assert_eq!(enc[0] & 0x0f, 1);
        let (back, _) = UtpHeader::decode(&enc).unwrap();
        assert_eq!(back.extension, 1);
    }
}
