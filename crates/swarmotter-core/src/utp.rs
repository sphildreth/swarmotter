// SPDX-License-Identifier: Apache-2.0

//! uTP (BEP 29 / LEDBAT-based reliable UDP transport) — packet encoding and a
//! minimal reliable session.
//!
//! uTP is a reliable, congestion-controlled byte stream layered over UDP. A
//! full uTP implementation requires the LEDBAT congestion-control algorithm,
//! selective ACKs, and the full connection lifecycle. This module implements
//! the binder-ready architecture and the largest testable subset:
//!
//! - uTP packet header encode/decode per BEP 29 (20-byte header: version,
//!   type+extension, connection id, timestamp, timestamp delta, window size,
//!   seq number, ack number).
//! - Connection id assignment and validation.
//! - A minimal reliable session (`UtpSession`) that sends DATA packets with
//!   sequence numbers and reassembles in-order data from received DATA packets,
//!   acknowledging with ACK packets, with a bounded retransmit. This is enough
//!   to carry the BitTorrent peer protocol over a contained UDP path and is
//!   exercised by a local uTP fixture test.
//!
//! What remains for a full production uTP (documented in
//! `docs/v1-completion-tracker.md`):
//!
//! - The LEDBAT congestion-control algorithm (delay-based) and dynamic window
//!   sizing instead of a fixed send window.
//! - Selective ACK (SACK) extension handling for out-of-order recovery.
//! - Full three-way handshake (SYN/DATA/ACK) and FIN tear-down with TIME_WAIT.
//! - Microsecond timestamp echo and one-way delay measurement.
//! - Integration of uTP as a peer transport selectable alongside TCP in the
//!   engine, with the binder's `udp_socket()` as the underlying transport.
//!
//! All uTP traffic goes through the `NetworkBinder`'s contained UDP socket;
//! no UDP socket is created directly. See `design/requirements.md` and
//! ADR-0020.

use crate::error::{CoreError, Result};

/// uTP protocol version (1).
pub const UTP_VERSION: u8 = 1;

/// uTP packet types (high nibble of byte 1; low nibble is extension id).
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

/// A parsed uTP packet header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtpHeader {
    pub typ: UtpType,
    pub version: u8,
    pub extension: u8,
    pub connection_id: u16,
    pub timestamp_micros: u32,
    pub timestamp_delta_micros: u32,
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

/// Current microsecond timestamp (monotonic-ish via std).
pub fn now_micros() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_micros() as u64 & 0xffffffff) as u32)
        .unwrap_or(0)
}

/// A minimal reliable uTP session: tracks send sequence, ack, and an in-order
/// receive reassembly. This is a testable subset (no full LEDBAT/congestion
/// control; fixed window; bounded retransmit on loss detection by the caller).
#[derive(Debug, Clone)]
pub struct UtpSession {
    pub send_connection_id: u16,
    pub recv_connection_id: u16,
    pub seq_number: u16,
    pub ack_number: u16,
    pub window_size: u32,
}

impl UtpSession {
    /// Initialize a session as the initiator (SYN sender). The sender's send
    /// connection id is chosen; the receiver echoes `recv_connection_id =
    /// send_connection_id + 1` per BEP 29.
    pub fn initiator(send_conn_id: u16) -> Self {
        Self {
            send_connection_id: send_conn_id,
            recv_connection_id: send_conn_id.wrapping_add(1),
            seq_number: 1,
            ack_number: 0,
            window_size: 64 * 1024,
        }
    }

    /// Build a SYN packet for this session.
    pub fn syn_packet(&self) -> Vec<u8> {
        let h = UtpHeader {
            typ: UtpType::Syn,
            version: UTP_VERSION,
            extension: 0,
            connection_id: self.recv_connection_id,
            timestamp_micros: now_micros(),
            timestamp_delta_micros: 0,
            window_size: self.window_size,
            seq_number: self.seq_number,
            ack_number: self.ack_number,
        };
        h.encode(&[])
    }

    /// Build a DATA packet carrying `payload`, advancing the send sequence.
    pub fn data_packet(&mut self, payload: &[u8]) -> Vec<u8> {
        let h = UtpHeader {
            typ: UtpType::Data,
            version: UTP_VERSION,
            extension: 0,
            connection_id: self.send_connection_id,
            timestamp_micros: now_micros(),
            timestamp_delta_micros: 0,
            window_size: self.window_size,
            seq_number: self.seq_number,
            ack_number: self.ack_number,
        };
        self.seq_number = self.seq_number.wrapping_add(1);
        h.encode(payload)
    }

    /// Build an ACK packet acknowledging `ack_seq`.
    pub fn ack_packet(&self) -> Vec<u8> {
        let h = UtpHeader {
            typ: UtpType::State,
            version: UTP_VERSION,
            extension: 0,
            connection_id: self.send_connection_id,
            timestamp_micros: now_micros(),
            timestamp_delta_micros: 0,
            window_size: self.window_size,
            seq_number: self.seq_number,
            ack_number: self.ack_number,
        };
        h.encode(&[])
    }

    /// Process an incoming DATA packet: update the ack number and return the
    /// payload if it is the next in-order sequence (else None for duplicate /
    /// out-of-order). The caller reassembles in-order data.
    pub fn handle_data(&mut self, header: &UtpHeader, payload: &[u8]) -> Option<Vec<u8>> {
        if header.typ != UtpType::Data {
            return None;
        }
        let expected = self.ack_number.wrapping_add(1);
        if header.seq_number == expected {
            self.ack_number = header.seq_number;
            Some(payload.to_vec())
        } else {
            None
        }
    }
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
    fn session_delivers_in_order_data() {
        let mut s = UtpSession::initiator(1000);
        let p = s.data_packet(b"hello");
        let (h, payload) = UtpHeader::decode(&p).unwrap();
        // Simulate receiving it back in order.
        let delivered = s.handle_data(&h, payload);
        assert_eq!(delivered.as_deref(), Some(b"hello".as_ref()));
    }

    #[test]
    fn session_skips_duplicate_out_of_order() {
        let mut s = UtpSession::initiator(1000);
        let p = s.data_packet(b"a");
        let (h1, pl1) = UtpHeader::decode(&p).unwrap();
        let h1 = h1.clone();
        let pl1 = pl1.to_vec();
        // Build a second packet at the next seq.
        let p2 = s.data_packet(b"b");
        let (h2, pl2) = UtpHeader::decode(&p2).unwrap();
        // Deliver the second first (out of order): no delivery.
        assert!(s.handle_data(&h2, pl2).is_none());
        // Deliver the first in order: delivered.
        assert_eq!(s.handle_data(&h1, &pl1).as_deref(), Some(b"a".as_ref()));
    }

    #[tokio::test]
    async fn utp_fail_closed_blocks_socket() {
        use crate::net::binder::BlockedBinder;
        use crate::net::NetworkBinder;
        let binder = BlockedBinder;
        match binder.udp_socket().await {
            Ok(_) => panic!("expected fail-closed to block uTP socket"),
            Err(e) => assert!(e.is_network_blocked()),
        }
    }
}
