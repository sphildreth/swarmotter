// SPDX-License-Identifier: Apache-2.0

//! uTP packet header (BEP 29) encode/decode.

use crate::error::{CoreError, Result};

/// uTP protocol version (1).
pub const UTP_VERSION: u8 = 1;

/// uTP packet types (high nibble of byte 0). The low nibble is the protocol
/// version; byte 1 identifies the first extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UtpType {
    Data = 0,
    Fin = 1,
    State = 2,
    Reset = 3,
    Syn = 4,
}

impl UtpType {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Data),
            1 => Some(Self::Fin),
            2 => Some(Self::State),
            3 => Some(Self::Reset),
            4 => Some(Self::Syn),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// One decoded uTP extension. The header points to the first extension kind;
/// each extension block points to the next kind and carries a byte length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtpExtension {
    pub kind: u8,
    pub data: Vec<u8>,
}

impl UtpExtension {
    pub fn new(kind: u8, data: Vec<u8>) -> Self {
        Self { kind, data }
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
    /// Difference between local time and the peer's last timestamp (wrapping).
    pub timestamp_delta_micros: u32,
    /// Advertised receive window in bytes.
    pub window_size: u32,
    pub seq_number: u16,
    pub ack_number: u16,
}

impl UtpHeader {
    pub const SIZE: usize = 20;

    /// Encode a packet without extensions.
    pub fn encode(&self, payload: &[u8]) -> Vec<u8> {
        let mut out = self.encode_prefix(0, Self::SIZE + payload.len());
        out.extend_from_slice(payload);
        out
    }

    /// Encode the header, linked extension chain, and payload. The first
    /// extension kind is written to byte 1; every extension block stores the
    /// next kind followed by its data length in bytes.
    pub fn encode_with_extensions(
        &self,
        extensions: &[UtpExtension],
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let extension_bytes = extensions.iter().try_fold(0usize, |total, extension| {
            if extension.kind == 0 {
                return Err(CoreError::InvalidArgument(
                    "uTP extension kind 0 is reserved for end-of-chain".into(),
                ));
            }
            if extension.data.len() > u8::MAX as usize {
                return Err(CoreError::InvalidArgument(format!(
                    "uTP extension {} exceeds 255 bytes",
                    extension.kind
                )));
            }
            total
                .checked_add(2 + extension.data.len())
                .ok_or_else(|| CoreError::InvalidArgument("uTP extension length overflow".into()))
        })?;
        let first_extension = extensions
            .first()
            .map(|extension| extension.kind)
            .unwrap_or(0);
        let mut out = self.encode_prefix(
            first_extension,
            Self::SIZE + extension_bytes + payload.len(),
        );
        for (index, extension) in extensions.iter().enumerate() {
            let next = extensions.get(index + 1).map(|next| next.kind).unwrap_or(0);
            out.push(next);
            out.push(extension.data.len() as u8);
            out.extend_from_slice(&extension.data);
        }
        out.extend_from_slice(payload);
        Ok(out)
    }

    fn encode_prefix(&self, first_extension: u8, capacity: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(capacity);
        let type_and_version = (self.typ.as_u8() << 4) | (self.version & 0x0f);
        out.push(type_and_version);
        out.push(first_extension);
        out.extend_from_slice(&self.connection_id.to_be_bytes());
        out.extend_from_slice(&self.timestamp_micros.to_be_bytes());
        out.extend_from_slice(&self.timestamp_delta_micros.to_be_bytes());
        out.extend_from_slice(&self.window_size.to_be_bytes());
        out.extend_from_slice(&self.seq_number.to_be_bytes());
        out.extend_from_slice(&self.ack_number.to_be_bytes());
        out
    }

    /// Decode a header from the front of a buffer, returning the header and
    /// payload slice. Extension blocks are parsed and removed from the returned
    /// payload; callers that need their values should use
    /// [`Self::decode_with_extensions`].
    pub fn decode(buf: &[u8]) -> Result<(UtpHeader, &[u8])> {
        let (header, _, payload) = Self::decode_with_extensions(buf)?;
        Ok((header, payload))
    }

    /// Decode a complete packet, including its linked extension chain.
    pub fn decode_with_extensions(buf: &[u8]) -> Result<(UtpHeader, Vec<UtpExtension>, &[u8])> {
        if buf.len() < 20 {
            return Err(CoreError::Parse(format!(
                "utp packet too short: {} bytes",
                buf.len()
            )));
        }
        let type_and_version = buf[0];
        let typ = UtpType::from_u8(type_and_version >> 4)
            .ok_or_else(|| CoreError::Parse(format!("bad utp type {}", type_and_version >> 4)))?;
        let version = type_and_version & 0x0f;
        if version != UTP_VERSION {
            return Err(CoreError::Parse(format!(
                "unsupported utp version {version}"
            )));
        }
        let extension = buf[1];
        let connection_id = u16::from_be_bytes([buf[2], buf[3]]);
        let timestamp_micros = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let timestamp_delta_micros = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let window_size = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let seq_number = u16::from_be_bytes([buf[16], buf[17]]);
        let ack_number = u16::from_be_bytes([buf[18], buf[19]]);
        let header = UtpHeader {
            typ,
            version,
            extension,
            connection_id,
            timestamp_micros,
            timestamp_delta_micros,
            window_size,
            seq_number,
            ack_number,
        };
        let mut extensions = Vec::new();
        let mut next_kind = extension;
        let mut cursor = Self::SIZE;
        while next_kind != 0 {
            if buf.len().saturating_sub(cursor) < 2 {
                return Err(CoreError::Parse("uTP extension header truncated".into()));
            }
            let following_kind = buf[cursor];
            let length = buf[cursor + 1] as usize;
            cursor += 2;
            let end = cursor
                .checked_add(length)
                .ok_or_else(|| CoreError::Parse("uTP extension length overflow".into()))?;
            if end > buf.len() {
                return Err(CoreError::Parse("uTP extension data truncated".into()));
            }
            extensions.push(UtpExtension::new(next_kind, buf[cursor..end].to_vec()));
            cursor = end;
            next_kind = following_kind;
        }
        Ok((header, extensions, &buf[cursor..]))
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
    fn bep29_syn_header_matches_golden_vector() {
        let h = UtpHeader {
            typ: UtpType::Syn,
            version: UTP_VERSION,
            extension: 0,
            connection_id: 0x1234,
            timestamp_micros: 0x1122_3344,
            timestamp_delta_micros: 0x5566_7788,
            window_size: 0x0102_0304,
            seq_number: 0x0506,
            ack_number: 0x0708,
        };
        assert_eq!(
            h.encode(&[]),
            vec![
                0x41, 0x00, 0x12, 0x34, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x01, 0x02,
                0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            ]
        );
    }

    #[test]
    fn header_decode_rejects_short() {
        assert!(UtpHeader::decode(&[0u8; 10]).is_err());
    }

    #[test]
    fn header_decode_rejects_bad_type() {
        let mut bad = [0u8; 20];
        bad[0] = 0x91; // type 9, version 1
        assert!(UtpHeader::decode(&bad).is_err());
    }

    #[test]
    fn header_decode_rejects_unsupported_version() {
        let mut bad = [0u8; 20];
        bad[0] = 0x02; // DATA, version 2
        assert!(UtpHeader::decode(&bad).is_err());
    }

    #[test]
    fn type_and_version_share_the_first_byte() {
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
        assert_eq!(enc[0] & 0x0f, UTP_VERSION);
        assert_eq!(enc[1], 0);
        let (back, _) = UtpHeader::decode(&enc).unwrap();
        assert_eq!(back.typ, UtpType::Syn);
    }

    #[test]
    fn linked_extensions_are_separate_from_payload() {
        let h = UtpHeader {
            typ: UtpType::State,
            version: UTP_VERSION,
            extension: 0,
            connection_id: 2,
            timestamp_micros: 0,
            timestamp_delta_micros: 0,
            window_size: 0,
            seq_number: 1,
            ack_number: 0,
        };
        let encoded = h
            .encode_with_extensions(
                &[
                    UtpExtension::new(1, vec![0x05, 0, 0, 0]),
                    UtpExtension::new(2, vec![0xaa, 0xbb]),
                ],
                b"payload",
            )
            .unwrap();
        assert_eq!(encoded[0], 0x21);
        assert_eq!(encoded[1], 1);
        assert_eq!(&encoded[20..26], &[2, 4, 0x05, 0, 0, 0]);
        assert_eq!(&encoded[26..30], &[0, 2, 0xaa, 0xbb]);

        let (back, extensions, payload) = UtpHeader::decode_with_extensions(&encoded).unwrap();
        assert_eq!(back.extension, 1);
        assert_eq!(extensions[0], UtpExtension::new(1, vec![0x05, 0, 0, 0]));
        assert_eq!(extensions[1], UtpExtension::new(2, vec![0xaa, 0xbb]));
        assert_eq!(payload, b"payload");
    }

    #[test]
    fn independently_constructed_extension_layout_decodes() {
        let mut packet = vec![
            0x01, 0x01, 0x12, 0x34, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0x10, 0, 0, 3, 0, 2,
        ];
        packet.extend_from_slice(&[0, 4, 0x05, 0, 0, 0]);
        packet.extend_from_slice(b"data");

        let (header, extensions, payload) = UtpHeader::decode_with_extensions(&packet).unwrap();
        assert_eq!(header.typ, UtpType::Data);
        assert_eq!(header.version, UTP_VERSION);
        assert_eq!(header.extension, 1);
        assert_eq!(extensions, vec![UtpExtension::new(1, vec![0x05, 0, 0, 0])]);
        assert_eq!(payload, b"data");
    }

    #[test]
    fn malformed_extension_chain_is_rejected() {
        let mut packet = UtpHeader {
            typ: UtpType::State,
            version: UTP_VERSION,
            extension: 0,
            connection_id: 1,
            timestamp_micros: 0,
            timestamp_delta_micros: 0,
            window_size: 0,
            seq_number: 0,
            ack_number: 0,
        }
        .encode(&[]);
        packet[1] = 1;
        packet.extend_from_slice(&[0, 4, 1]);
        assert!(UtpHeader::decode_with_extensions(&packet).is_err());
    }

    #[test]
    fn extension_encoder_rejects_reserved_kind_and_oversized_data() {
        let header = UtpHeader {
            typ: UtpType::State,
            version: UTP_VERSION,
            extension: 0,
            connection_id: 1,
            timestamp_micros: 0,
            timestamp_delta_micros: 0,
            window_size: 0,
            seq_number: 0,
            ack_number: 0,
        };
        assert!(header
            .encode_with_extensions(&[UtpExtension::new(0, vec![1])], &[])
            .is_err());
        assert!(header
            .encode_with_extensions(&[UtpExtension::new(1, vec![0; 256])], &[])
            .is_err());
    }
}
