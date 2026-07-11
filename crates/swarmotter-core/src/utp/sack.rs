// SPDX-License-Identifier: Apache-2.0

//! Selective ACK (SACK) extension for uTP (BEP 29 extension 1).
//!
//! When the receiver holds out-of-order data beyond the cumulative ack number,
//! it appends a SACK extension to its STATE/DATA packets so the sender learns
//! which ranges have arrived and can avoid retransmitting already-received
//! data. The SACK extension is a bitmask of 32-bit words. Per BEP 29, bit zero
//! represents `ack + 2` because `ack + 1` is necessarily the missing packet
//! that prevented the cumulative acknowledgement from advancing.

use crate::error::{CoreError, Result};

/// A decoded/encodable SACK bitmask. Bit `i` (counting from the least
/// significant bit of the first wire byte) represents sequence number
/// `ack + 2 + i`.
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
            // Offset from ack in wrapping sequence space. ack+1 cannot be
            // selectively acknowledged because it would advance the cumulative
            // acknowledgement.
            let off = seq.wrapping_sub(ack);
            if off < 2 || off > (max_words * 32 + 1) as u16 {
                continue;
            }
            let bit = (off as usize) - 2;
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

    /// Iterate over the sequence-number offsets (starting at 2 from ack) that are
    /// selectively acked.
    pub fn offsets(&self) -> Vec<u16> {
        let mut out = Vec::new();
        for (wi, w) in self.words.iter().enumerate() {
            for b in 0..32u32 {
                if w & (1u32 << b) != 0 {
                    out.push((wi * 32 + b as usize + 2) as u16);
                }
            }
        }
        out
    }

    /// Encode the SACK data bytes carried by a uTP extension block. Bits are in
    /// wire order (least-significant bit of the first byte first), and the data
    /// length is always a multiple of four bytes.
    pub fn encode_data(&self) -> Vec<u8> {
        if self.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.words.len() * 4);
        for w in &self.words {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    /// Parse the data bytes of a SACK extension. BEP 29 requires at least one
    /// 32-bit mask word and a length divisible by four.
    pub fn parse_data(buf: &[u8]) -> Result<Self> {
        if buf.is_empty() || !buf.len().is_multiple_of(4) {
            return Err(CoreError::Parse(
                "uTP SACK extension length must be a non-zero multiple of 4".into(),
            ));
        }
        let mut words = Vec::with_capacity(buf.len() / 4);
        for chunk in buf.chunks_exact(4) {
            words.push(u32::from_le_bytes(chunk.try_into().map_err(|_| {
                CoreError::Parse("uTP SACK extension word truncated".into())
            })?));
        }
        Ok(Self { words })
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
    fn data_encode_decode_roundtrip_matches_wire_bit_order() {
        let held = vec![(12u16, vec![]), (14u16, vec![])];
        let s = Sack::from_held(10, &held);
        let enc = s.encode_data();
        assert_eq!(enc, vec![0x05, 0, 0, 0]);
        let parsed = Sack::parse_data(&enc).unwrap();
        assert_eq!(parsed, s);
        assert_eq!(parsed.offsets(), vec![2, 4]);
    }

    #[test]
    fn parse_rejects_empty_or_non_word_aligned_data() {
        assert!(Sack::parse_data(&[]).is_err());
        assert!(Sack::parse_data(&[1u8]).is_err());
        assert!(Sack::parse_data(&[1u8, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn ack_plus_one_is_never_sacked() {
        let sack = Sack::from_held(10, &[(11, vec![]), (12, vec![])]);
        assert_eq!(sack.offsets(), vec![2]);
    }

    #[test]
    fn sack_offsets_wrap_with_sequence_space() {
        let sack = Sack::from_held(u16::MAX, &[(0, vec![]), (1, vec![])]);
        assert_eq!(sack.offsets(), vec![2]);
    }
}
