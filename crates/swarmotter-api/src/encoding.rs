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
}
