// SPDX-License-Identifier: Apache-2.0

//! Small encoding helpers used by API compatibility and batch endpoints.

pub(crate) fn decode_base64(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0u32;
    let mut saw_padding = false;
    for c in input.chars() {
        if c.is_ascii_whitespace() {
            continue;
        }
        if c == '=' {
            saw_padding = true;
            continue;
        }
        if saw_padding {
            return None;
        }
        let value = base64_value(c)? as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    Some(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundedBase64DecodeError {
    InvalidEncoding,
    LimitExceeded,
    AllocationFailed,
}

/// Decode base64 without allowing the decoded accumulator to exceed `limit`.
///
/// This decoder accepts the standard and URL-safe alphabets, optional trailing
/// padding, and ASCII whitespace. It validates padding and unused trailing bits
/// and checks the output length before reserving or appending each decoded byte.
pub(crate) fn decode_base64_bounded(
    input: &str,
    limit: usize,
) -> Result<Vec<u8>, BoundedBase64DecodeError> {
    let mut out = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0u32;
    let mut symbols = 0usize;
    let mut padding = 0usize;
    let mut saw_padding = false;

    for c in input.chars() {
        if c.is_ascii_whitespace() {
            continue;
        }
        if c == '=' {
            saw_padding = true;
            padding = padding
                .checked_add(1)
                .ok_or(BoundedBase64DecodeError::InvalidEncoding)?;
            if padding > 2 {
                return Err(BoundedBase64DecodeError::InvalidEncoding);
            }
            continue;
        }
        if saw_padding {
            return Err(BoundedBase64DecodeError::InvalidEncoding);
        }
        let value = base64_value(c).ok_or(BoundedBase64DecodeError::InvalidEncoding)? as u32;
        symbols = symbols
            .checked_add(1)
            .ok_or(BoundedBase64DecodeError::LimitExceeded)?;
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            let next_len = out
                .len()
                .checked_add(1)
                .ok_or(BoundedBase64DecodeError::LimitExceeded)?;
            if next_len > limit {
                return Err(BoundedBase64DecodeError::LimitExceeded);
            }
            reserve_bounded_base64_output(&mut out, next_len, limit)?;
            out.push(((buffer >> bits) & 0xff) as u8);
            buffer &= (1u32 << bits) - 1;
        }
    }

    let symbol_remainder = symbols % 4;
    let padding_is_valid = match padding {
        0 => matches!(symbol_remainder, 0 | 2 | 3),
        1 => symbol_remainder == 3,
        2 => symbol_remainder == 2,
        _ => false,
    };
    if !padding_is_valid || buffer != 0 {
        return Err(BoundedBase64DecodeError::InvalidEncoding);
    }
    Ok(out)
}

fn reserve_bounded_base64_output(
    output: &mut Vec<u8>,
    next_len: usize,
    limit: usize,
) -> Result<(), BoundedBase64DecodeError> {
    if output.capacity() >= next_len {
        return Ok(());
    }
    let doubled = output.capacity().checked_mul(2).unwrap_or(limit);
    let target = doubled.max(8 * 1024).max(next_len).min(limit);
    let additional = target
        .checked_sub(output.len())
        .ok_or(BoundedBase64DecodeError::LimitExceeded)?;
    output
        .try_reserve_exact(additional)
        .map_err(|_| BoundedBase64DecodeError::AllocationFailed)
}

fn base64_value(c: char) -> Option<u8> {
    match c {
        'A'..='Z' => Some(c as u8 - b'A'),
        'a'..='z' => Some(26 + c as u8 - b'a'),
        '0'..='9' => Some(52 + c as u8 - b'0'),
        '+' | '-' => Some(62),
        '/' | '_' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_decode_handles_payloads() {
        assert_eq!(
            decode_base64("aGVsbG8=").as_deref(),
            Some(b"hello".as_slice())
        );
    }

    #[test]
    fn bounded_base64_accepts_exact_output_and_rejects_before_one_over() {
        assert_eq!(decode_base64_bounded("YWJj", 3).unwrap(), b"abc".as_slice());
        assert_eq!(
            decode_base64_bounded("YWJj", 2),
            Err(BoundedBase64DecodeError::LimitExceeded)
        );
    }

    #[test]
    fn bounded_base64_distinguishes_invalid_encoding_from_output_limit() {
        for invalid in ["A", "TR==", "TQ=", "TQ===", "TQ==A", "YWJ$"] {
            assert_eq!(
                decode_base64_bounded(invalid, usize::MAX),
                Err(BoundedBase64DecodeError::InvalidEncoding),
                "invalid input {invalid:?}"
            );
        }
        assert_eq!(decode_base64_bounded("TQ==", 1).unwrap(), b"M");
    }
}
