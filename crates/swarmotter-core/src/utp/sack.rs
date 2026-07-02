// SPDX-License-Identifier: Apache-2.0

//! Selective ACK (SACK) extension for uTP (BEP 29 extension 1).
//!
//! When the receiver holds out-of-order data beyond the cumulative ack number,
//! it appends a SACK extension to its STATE/DATA packets so the sender learns
//! which ranges have arrived and can avoid retransmitting already-received
//! data. The SACK extension is a bitmask of 32-bit words, one bit per packet
//! past the cumulative ack (bit 0 of word 0 = ack+1, bit 1 = ack+2, ...).

use crate::error::{CoreError, Result};

/// A decoded/encodable SACK bitmask. Bit `i` (counting from the least
/// significant bit of word 0) represents sequence number `ack + 1 + i`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sack {
    pub words: Vec<u32>,
}

impl Sack {
    /// Build a SACK from a set of held out-of-order sequence numbers, relative
    /// to the current cumulative ack number. Returns an empty SACK if no
    /// held sequences are within the representable range (the first 32*words
    /// packets past the ack).
    pub fn from_held(ack: u16, held: &[(u16, Vec<u8>)]) -> Self {
        let max_words = 8usize; // up to 256 packets past the ack
        let mut words = vec![0u32; max_words];
        let mut any = false;
        for (seq, _) in held {
            // offset from ack (wrapping), 1-based.
            let off = seq.wrapping_sub(ack);
            if off == 0 || off > (max_words * 32) as u16 {
                continue;
            }
            let bit = (off as usize) - 1;
            let word = bit / 32;
            let mask = 1u32 << (bit % 32);
            if word < words.len() {
                words[word] |= mask;
                any = true;
            }
        }
        if !any {
            return Self { words: Vec::new() };
        }
        // Trim trailing zero words.
        while words.last() == Some(&0) {
            words.pop();
        }
        Self { words }
    }

    /// Whether any bits are set.
    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    /// Iterate over the sequence-number offsets (1-based from ack) that are
    /// selectively acked.
    pub fn offsets(&self) -> Vec<u16> {
        let mut out = Vec::new();
        for (wi, w) in self.words.iter().enumerate() {
            for b in 0..32u32 {
                if w & (1u32 << b) != 0 {
                    out.push((wi * 32 + b as usize + 1) as u16);
                }
            }
        }
        out
    }

    /// Encode the SACK extension as a trailing extension block: a single
    /// `extension id` byte (`1`), a `length` byte (number of 32-bit words),
    /// and the big-endian word bytes. Returns an empty vec if no SACK.
    pub fn encode_extension(&self) -> Vec<u8> {
        if self.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(2 + self.words.len() * 4);
        out.push(1u8); // SACK extension id
        out.push(self.words.len() as u8);
        for w in &self.words {
            out.extend_from_slice(&w.to_be_bytes());
        }
        out
    }

    /// Parse a SACK extension block from the trailing bytes after the uTP
    /// header payload. The first byte is the extension id; if it is 1, the
    /// next byte is the word count followed by the words. Returns `None` if
    /// the block is not a SACK extension or is malformed.
    pub fn parse_extension(buf: &[u8]) -> Result<Option<Self>> {
        if buf.is_empty() {
            return Ok(None);
        }
        if buf[0] != 1 {
            return Ok(None);
        }
        if buf.len() < 2 {
            return Err(CoreError::Parse("uTP SACK extension truncated".into()));
        }
        let count = buf[1] as usize;
        if buf.len() < 2 + count * 4 {
            return Err(CoreError::Parse("uTP SACK extension body truncated".into()));
        }
        let mut words = Vec::with_capacity(count);
        for i in 0..count {
            let off = 2 + i * 4;
            words.push(u32::from_be_bytes([
                buf[off],
                buf[off + 1],
                buf[off + 2],
                buf[off + 3],
            ]));
        }
        Ok(Some(Self { words }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sack_for_nothing_held() {
        let s = Sack::from_held(10, &[]);
        assert!(s.is_empty());
    }

    #[test]
    fn sack_bits_for_held_sequences() {
        let held = vec![(12u16, vec![]), (15u16, vec![]), (40u16, vec![])];
        let s = Sack::from_held(10, &held);
        assert!(!s.is_empty());
        let offs = s.offsets();
        assert!(offs.contains(&2)); // seq 12 -> ack+2
        assert!(offs.contains(&5)); // seq 15 -> ack+5
        assert!(offs.contains(&30)); // seq 40 -> ack+30
    }

    #[test]
    fn encode_decode_roundtrip() {
        let held = vec![(11u16, vec![]), (13u16, vec![])];
        let s = Sack::from_held(10, &held);
        let enc = s.encode_extension();
        assert!(!enc.is_empty());
        let parsed = Sack::parse_extension(&enc).unwrap().unwrap();
        assert_eq!(parsed, s);
        assert_eq!(parsed.offsets(), vec![1, 3]);
    }

    #[test]
    fn parse_returns_none_for_non_sack() {
        let buf = [0u8, 1];
        assert!(Sack::parse_extension(&buf).unwrap().is_none());
    }

    #[test]
    fn parse_returns_none_for_empty() {
        assert!(Sack::parse_extension(&[]).unwrap().is_none());
    }

    #[test]
    fn parse_rejects_truncated() {
        assert!(Sack::parse_extension(&[1u8]).is_err());
        assert!(Sack::parse_extension(&[1u8, 2, 0, 0]).is_err());
    }
}
