// SPDX-License-Identifier: Apache-2.0

//! Minimal bencode decoder and encoder used for `.torrent` metadata.
//!
//! We implement bencode ourselves rather than depend on an unmaintained crate,
//! so the raw `info` dictionary bytes are available for exact info-hash
//! computation and we control canonical encoding for test fixtures.
//!
//! Bencode grammar:
//!   - byte strings: `<len>:<bytes>`
//!   - integers: `i<int>e`
//!   - lists: `l<items>e`
//!   - dicts: `d<key><value>...e` (keys are byte strings in sorted order)
//!
//! See `design/PRD.md` and BitTorrent BEP 3 for the metadata structure.

use crate::error::{CoreError, Result};
use crate::meta::{MAX_BENCODE_DEPTH, MAX_BENCODE_NODES, MAX_TORRENT_METADATA_BYTES};
use serde::Serialize;

/// A bencode value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Str(Vec<u8>),
    List(Vec<Value>),
    Dict(Vec<(Vec<u8>, Value)>),
}

impl Value {
    pub fn as_str(&self) -> Option<&[u8]> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_str_utf8(&self) -> Option<&str> {
        match self {
            Value::Str(s) => std::str::from_utf8(s).ok(),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&[(Vec<u8>, Value)]> {
        match self {
            Value::Dict(d) => Some(d),
            _ => None,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        match self {
            Value::Dict(d) => d.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key.as_bytes()).and_then(Value::as_str_utf8)
    }
}

/// Decode bencoded bytes into a `Value`.
///
/// The input is bounded by [`crate::meta::MAX_TORRENT_METADATA_BYTES`]. The
/// parser counts nesting depth and total nodes and rejects any input that would
/// exceed [`crate::meta::MAX_BENCODE_DEPTH`] or [`crate::meta::MAX_BENCODE_NODES`].
/// Exactly one top-level value must be followed by EOF; trailing bytes are an
/// error. No malformed input may panic.
pub fn decode(bytes: &[u8]) -> Result<Value> {
    if bytes.len() > MAX_TORRENT_METADATA_BYTES {
        return Err(CoreError::Bencode(format!(
            "input length {} exceeds maximum {MAX_TORRENT_METADATA_BYTES}",
            bytes.len()
        )));
    }
    let mut p = Parser {
        bytes,
        pos: 0,
        depth: 0,
        nodes: 0,
    };
    let v = p.parse()?;
    if p.pos != bytes.len() {
        return Err(CoreError::Bencode(format!(
            "trailing bytes after top-level value at position {}",
            p.pos
        )));
    }
    Ok(v)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    depth: usize,
    nodes: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Result<u8> {
        self.bytes
            .get(self.pos)
            .copied()
            .ok_or_else(|| CoreError::Bencode("unexpected end of input".into()))
    }

    fn bump(&mut self) -> Result<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    /// Count one node and reject the node that would exceed the budget.
    fn count_node(&mut self) -> Result<()> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| CoreError::Bencode("node count overflow".into()))?;
        if self.nodes > MAX_BENCODE_NODES {
            return Err(CoreError::Bencode(format!(
                "bencode node count exceeds maximum {MAX_BENCODE_NODES}"
            )));
        }
        Ok(())
    }

    /// Increment depth on entering a list/dict and reject an entry that would
    /// exceed the maximum. The root is depth zero.
    fn enter(&mut self) -> Result<()> {
        self.depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| CoreError::Bencode("nesting depth overflow".into()))?;
        if self.depth > MAX_BENCODE_DEPTH {
            return Err(CoreError::Bencode(format!(
                "bencode nesting depth exceeds maximum {MAX_BENCODE_DEPTH}"
            )));
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    fn parse(&mut self) -> Result<Value> {
        self.count_node()?;
        match self.peek()? {
            b'i' => self.parse_int(),
            b'l' => self.parse_list(),
            b'd' => self.parse_dict(),
            b'0'..=b'9' => self.parse_str().map(Value::Str),
            _ => Err(CoreError::Bencode(format!("invalid token at {}", self.pos))),
        }
    }

    fn parse_int(&mut self) -> Result<Value> {
        self.bump()?; // 'i'
        let start = self.pos;
        // Scan until terminator. Reject end-of-input before 'e'.
        while let Ok(b) = self.peek() {
            if b == b'e' {
                break;
            }
            self.pos += 1;
        }
        if start == self.pos {
            return Err(CoreError::Bencode("empty integer".into()));
        }
        let s = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| CoreError::Bencode("integer body is not utf8".into()))?;
        // Strict integer validation. Accept optional leading '-' followed by
        // digits. Reject leading zeroes other than the single digit `0`, negative
        // zero, and any non-digit character.
        let chars = s.as_bytes();
        let (sign, digits) = if chars[0] == b'-' {
            if chars.len() == 1 {
                return Err(CoreError::Bencode("integer has sign without digits".into()));
            }
            (true, &chars[1..])
        } else {
            (false, chars)
        };
        if digits.is_empty() {
            return Err(CoreError::Bencode("integer has no digits".into()));
        }
        if !digits.iter().all(|c| c.is_ascii_digit()) {
            return Err(CoreError::Bencode("integer contains non-digit".into()));
        }
        if digits[0] == b'0' {
            // Only the single digit `0` (or `-0`, which is rejected) is allowed
            // to start with zero.
            if digits.len() > 1 {
                return Err(CoreError::Bencode("integer has leading zero".into()));
            }
            if sign {
                return Err(CoreError::Bencode("negative zero is not allowed".into()));
            }
        }
        let n: i64 = s
            .parse()
            .map_err(|_| CoreError::Bencode("integer out of i64 range".into()))?;
        // The parse() above already accepts leading '+' which bencode forbids;
        // the manual check above rejects it because '+' is not a digit.
        self.bump()?; // 'e'
        Ok(Value::Int(n))
    }

    fn parse_str(&mut self) -> Result<Vec<u8>> {
        let colon = self.bytes[self.pos..]
            .iter()
            .position(|&b| b == b':')
            .ok_or_else(|| CoreError::Bencode("missing ':' in string".into()))?;
        let len_bytes = &self.bytes[self.pos..self.pos + colon];
        // Validate length prefix: optional... bencode requires ASCII digits with
        // no leading zero unless the length is exactly zero.
        if colon == 0 {
            return Err(CoreError::Bencode("empty string length prefix".into()));
        }
        if !len_bytes.iter().all(|c| c.is_ascii_digit()) {
            return Err(CoreError::Bencode("string length is not digits".into()));
        }
        if len_bytes.len() > 1 && len_bytes[0] == b'0' {
            return Err(CoreError::Bencode("string length has leading zero".into()));
        }
        let len: usize = std::str::from_utf8(len_bytes)
            .map_err(|_| CoreError::Bencode("string length is not utf8".into()))?
            .parse()
            .map_err(|_| CoreError::Bencode("string length is not a valid usize".into()))?;
        // checked_add for the colon and length to avoid overflow past input end.
        let after_len = self
            .pos
            .checked_add(colon)
            .ok_or_else(|| CoreError::Bencode("string length prefix overflows cursor".into()))?;
        let data_start = after_len
            .checked_add(1)
            .ok_or_else(|| CoreError::Bencode("string colon offset overflows cursor".into()))?;
        let data_end = data_start
            .checked_add(len)
            .ok_or_else(|| CoreError::Bencode("string end overflows cursor".into()))?;
        if data_end > self.bytes.len() {
            return Err(CoreError::Bencode("string overruns input".into()));
        }
        let s = self.bytes[data_start..data_end].to_vec();
        self.pos = data_end;
        Ok(s)
    }

    fn parse_list(&mut self) -> Result<Value> {
        self.bump()?; // 'l'
        self.enter()?;
        let mut items = Vec::new();
        loop {
            if self.peek()? == b'e' {
                break;
            }
            items.push(self.parse()?);
        }
        self.bump()?; // 'e'
        self.leave();
        Ok(Value::List(items))
    }

    fn parse_dict(&mut self) -> Result<Value> {
        self.bump()?; // 'd'
        self.enter()?;
        let mut entries = Vec::new();
        let mut seen = std::collections::HashSet::new();
        loop {
            if self.peek()? == b'e' {
                break;
            }
            // Dictionary keys must be byte strings.
            let first = self.peek()?;
            if !first.is_ascii_digit() {
                return Err(CoreError::Bencode("dictionary key is not a string".into()));
            }
            let key = self.parse_str()?;
            if !seen.insert(key.clone()) {
                return Err(CoreError::Bencode(format!(
                    "duplicate dictionary key (length {})",
                    key.len()
                )));
            }
            let val = self.parse()?;
            entries.push((key, val));
        }
        self.bump()?; // 'e'
        self.leave();
        Ok(Value::Dict(entries))
    }
}

/// Extract the raw value bytes for a top-level key from bencoded bytes.
///
/// This is a bounded, panic-free scanner used to obtain the original `info`
/// slice for info-hash computation. It does not allocate a full `Value` tree.
pub fn extract_value_bytes<'a>(bytes: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    if bytes.len() > MAX_TORRENT_METADATA_BYTES {
        return None;
    }
    let mut p = 0usize;
    if bytes.first()? != &b'd' {
        return None;
    }
    p += 1;
    while p < bytes.len() {
        if bytes[p] == b'e' {
            break;
        }
        let k = read_str(bytes, &mut p)?;
        let start = p;
        skip_value(bytes, &mut p)?;
        if k == key {
            return Some(&bytes[start..p]);
        }
    }
    None
}

fn read_str(bytes: &[u8], p: &mut usize) -> Option<Vec<u8>> {
    let colon = bytes[*p..].iter().position(|&b| b == b':')?;
    let len_bytes = &bytes[*p..*p + colon];
    if colon == 0 || !len_bytes.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if len_bytes.len() > 1 && len_bytes[0] == b'0' {
        return None;
    }
    let len: usize = std::str::from_utf8(len_bytes).ok()?.parse().ok()?;
    let after_len = (*p).checked_add(colon)?;
    let data_start = after_len.checked_add(1)?;
    let data_end = data_start.checked_add(len)?;
    if data_end > bytes.len() {
        return None;
    }
    let s = bytes[data_start..data_end].to_vec();
    *p = data_end;
    Some(s)
}

fn skip_value(bytes: &[u8], p: &mut usize) -> Option<()> {
    match bytes.get(*p)? {
        b'i' => {
            // Scan to the matching terminator; bencode integers contain only
            // digits and an optional leading sign.
            *p += 1;
            let body_start = *p;
            while *p < bytes.len() && bytes[*p] != b'e' {
                *p += 1;
            }
            if *p >= bytes.len() {
                return None;
            }
            if *p == body_start {
                return None; // empty integer
            }
            *p += 1; // consume 'e'
            Some(())
        }
        b'l' | b'd' => {
            *p += 1;
            let mut depth = 1usize;
            while *p < bytes.len() && depth > 0 {
                match bytes[*p] {
                    b'l' | b'd' => {
                        depth += 1;
                        *p += 1;
                    }
                    b'e' => {
                        depth -= 1;
                        *p += 1;
                    }
                    b'0'..=b'9' => {
                        read_str(bytes, p)?;
                    }
                    b'i' => {
                        *p += 1;
                        while *p < bytes.len() && bytes[*p] != b'e' {
                            *p += 1;
                        }
                        if *p >= bytes.len() {
                            return None;
                        }
                        *p += 1;
                    }
                    _ => return None,
                }
            }
            Some(())
        }
        b'0'..=b'9' => {
            read_str(bytes, p)?;
            Some(())
        }
        _ => None,
    }
}

/// Encode a serializable value to bencode using canonical ordering.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let ser = serde_json::to_value(value)
        .map_err(|e| CoreError::Bencode(format!("json pre-encode: {e}")))?;
    let mut out = Vec::new();
    encode_json(&ser, &mut out)?;
    Ok(out)
}

fn encode_json(v: &serde_json::Value, out: &mut Vec<u8>) -> Result<()> {
    match v {
        serde_json::Value::Null => Err(CoreError::Bencode("null not encodable".into())),
        serde_json::Value::Bool(b) => {
            out.push(b'i');
            out.extend_from_slice(if *b { b"1" } else { b"0" });
            out.push(b'e');
            Ok(())
        }
        serde_json::Value::Number(n) => {
            out.push(b'i');
            out.extend_from_slice(n.to_string().as_bytes());
            out.push(b'e');
            Ok(())
        }
        serde_json::Value::String(s) => {
            write_bytes(out, s.as_bytes());
            Ok(())
        }
        serde_json::Value::Array(a) => {
            out.push(b'l');
            for item in a {
                encode_json(item, out)?;
            }
            out.push(b'e');
            Ok(())
        }
        serde_json::Value::Object(o) => {
            // Keys must be sorted by byte order.
            let mut entries: Vec<(&String, &serde_json::Value)> = o.iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push(b'd');
            for (k, val) in entries {
                write_bytes(out, k.as_bytes());
                encode_json(val, out)?;
            }
            out.push(b'e');
            Ok(())
        }
    }
}

fn write_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(format!("{}:", b.len()).as_bytes());
    out.extend_from_slice(b);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_int_str_list_dict() {
        let v = decode(b"i42e").unwrap();
        assert_eq!(v.as_int(), Some(42));
        let v = decode(b"4:spam").unwrap();
        assert_eq!(v.as_str(), Some(b"spam".as_slice()));
        let v = decode(b"l4:spami42ee").unwrap();
        assert_eq!(v.as_list().unwrap().len(), 2);
        let v = decode(b"d3:cow3:moo4:spam4:eggse").unwrap();
        assert_eq!(v.get(b"cow").unwrap().as_str(), Some(b"moo".as_slice()));
        assert_eq!(v.get_str("spam"), Some("eggs"));
    }

    #[test]
    fn extract_info_bytes() {
        // d 4:info d ... e e  with outer key first being something else.
        // announce value "http://x/a" is 10 chars.
        let bytes = b"d8:announce10:http://x/a4:infod4:name3:fooee";
        let info = extract_value_bytes(bytes, b"info").unwrap();
        assert_eq!(info, b"d4:name3:fooe");
    }

    #[test]
    fn rejects_truncated() {
        assert!(decode(b"i").is_err());
        assert!(decode(b"4:ab").is_err());
        assert!(decode(b"l").is_err());
    }

    #[test]
    fn encode_roundtrip() {
        use serde_json::json;
        let v = json!({
            "name": "foo",
            "length": 100,
            "list": [1, 2, 3],
        });
        let encoded = encode(&v).unwrap();
        let back = decode(&encoded).unwrap();
        assert_eq!(back.get_str("name"), Some("foo"));
        assert_eq!(back.get(b"length").unwrap().as_int(), Some(100));
        assert_eq!(back.get(b"list").unwrap().as_list().unwrap().len(), 3);
    }

    // Build a nested structure of the given depth using lists: l<inner>e.
    fn nested_lists(depth: usize, inner: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(std::iter::repeat_n(b'l', depth));
        out.extend_from_slice(inner.as_bytes());
        out.extend(std::iter::repeat_n(b'e', depth));
        out
    }

    #[test]
    fn depth_limit_accepts_boundary_and_rejects_one_more() {
        // Root is depth zero. A list wrapping a leaf is depth 1. So a structure
        // with MAX_BENCODE_DEPTH lists is the deepest accepted case.
        let ok = nested_lists(MAX_BENCODE_DEPTH, "i1e");
        assert!(decode(&ok).is_ok(), "depth boundary must parse");

        let too_deep = nested_lists(MAX_BENCODE_DEPTH + 1, "i1e");
        let err = decode(&too_deep).unwrap_err();
        assert!(err.to_string().contains("depth"), "depth error: {err}");
    }

    #[test]
    fn node_limit_accepts_boundary_and_rejects_one_more() {
        // A list of MAX_BENCODE_NODES leaves is exactly the budget (the list
        // itself is one node, plus each leaf). Build MAX_BENCODE_NODES integers
        // directly as the top-level list contents.
        let mut ok = Vec::new();
        ok.push(b'l');
        for _ in 0..(MAX_BENCODE_NODES - 1) {
            ok.extend_from_slice(b"i1e");
        }
        ok.push(b'e');
        assert!(decode(&ok).is_ok(), "node boundary must parse");

        let mut too_many = Vec::new();
        too_many.push(b'l');
        for _ in 0..MAX_BENCODE_NODES {
            too_many.extend_from_slice(b"i1e");
        }
        too_many.push(b'e');
        let err = decode(&too_many).unwrap_err();
        assert!(err.to_string().contains("node"), "node error: {err}");
    }

    #[test]
    fn rejects_overflowing_and_truncated_strings() {
        // String length prefix far exceeds input.
        assert!(decode(b"9999999999999:x").is_err());
        // Length prefix within usize but past input end.
        assert!(decode(b"100:ab").is_err());
        // Missing colon.
        assert!(decode(b"12").is_err());
        // Empty length prefix.
        assert!(decode(b":ab").is_err());
        // Leading zero in length.
        assert!(decode(b"01:ab").is_err());
    }

    #[test]
    fn rejects_missing_terminators_and_truncated_ints() {
        assert!(decode(b"i").is_err());
        assert!(decode(b"i1").is_err());
        assert!(decode(b"i-").is_err());
    }

    #[test]
    fn rejects_invalid_integer_forms() {
        // Empty integer.
        assert!(decode(b"ie").is_err());
        // Leading zero.
        assert!(decode(b"i01e").is_err());
        // Negative zero.
        assert!(decode(b"i-0e").is_err());
        // Sign without digits.
        assert!(decode(b"i-e").is_err());
        // Non-digit character.
        assert!(decode(b"i1ae").is_err());
        // Plus sign is not a digit and bencode forbids it.
        assert!(decode(b"i+1e").is_err());
    }

    #[test]
    fn accepts_zero_and_negative_integers() {
        assert_eq!(decode(b"i0e").unwrap().as_int(), Some(0));
        assert_eq!(decode(b"i-5e").unwrap().as_int(), Some(-5));
        assert_eq!(decode(b"i100e").unwrap().as_int(), Some(100));
    }

    #[test]
    fn rejects_duplicate_dictionary_keys() {
        assert!(decode(b"d1:ai1e1:ai2ee").is_err());
    }

    #[test]
    fn accepts_unsorted_unique_keys() {
        // Interoperability: keys need not be sorted, only unique.
        let v = decode(b"d1:bi2e1:ai1ee").unwrap();
        assert_eq!(v.get(b"a").unwrap().as_int(), Some(1));
        assert_eq!(v.get(b"b").unwrap().as_int(), Some(2));
    }

    #[test]
    fn rejects_non_string_dictionary_key() {
        assert!(decode(b"di1ei2ee").is_err());
    }

    #[test]
    fn rejects_trailing_bytes() {
        assert!(decode(b"i1eX").is_err());
        assert!(decode(b"l1:aei1ee").is_err());
    }

    #[test]
    fn rejects_oversized_input() {
        let big = vec![b'i'; MAX_TORRENT_METADATA_BYTES + 1];
        let err = decode(&big).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn accepts_input_at_byte_limit() {
        // A single string whose total document size is exactly the byte limit.
        // The document is "<len>:<content>" so total = prefix_len + content_len.
        // Iterate until the prefix length is self-consistent.
        let target = MAX_TORRENT_METADATA_BYTES;
        let content_len = solve_string_content_len(target);
        let prefix = format!("{}:", content_len);
        assert_eq!(prefix.len() + content_len, target);
        let mut input = Vec::with_capacity(target);
        input.extend_from_slice(prefix.as_bytes());
        input.extend(std::iter::repeat_n(b'x', content_len));
        assert_eq!(input.len(), target);
        assert!(decode(&input).is_ok());
    }

    #[test]
    fn rejects_input_one_byte_over_limit() {
        // A document one byte over the limit.
        let target = MAX_TORRENT_METADATA_BYTES + 1;
        let content_len = solve_string_content_len(target);
        let prefix = format!("{}:", content_len);
        let mut input = Vec::with_capacity(target);
        input.extend_from_slice(prefix.as_bytes());
        input.extend(std::iter::repeat_n(b'x', content_len));
        assert_eq!(input.len(), target);
        let err = decode(&input).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
    }

    // Solve for content_len so that format!("{}:", content_len).len() +
    // content_len == target. Iterate because the prefix length depends on
    // content_len's digit count.
    fn solve_string_content_len(target: usize) -> usize {
        let mut content_len = target.saturating_sub(2);
        for _ in 0..16 {
            let prefix = format!("{}:", content_len);
            let total = prefix.len() + content_len;
            if total == target {
                return content_len;
            }
            content_len = target.saturating_sub(prefix.len());
        }
        content_len
    }

    #[test]
    fn malformed_corpus_cases_do_not_panic() {
        let corpus: &[&[u8]] = &[
            b"",
            b"d",
            b"l",
            b"i",
            b"ie",
            b"i-0e",
            b"i01e",
            b"d1:a",
            b"d1:ai1e",
            b"d1:ai1ei2ee",
            b"d1:ai1e1:ai2ee",
            b"l1:ae",
            b"9999:ab",
            b"100:ab",
            b":ab",
            b"01:ab",
            b"di1ei2ee",
            b"i1eX",
            b"llllllllllllllllllllllllllllllllllllllllllllllllllllle",
            b"d1:xi1e1:xX",
        ];
        for case in corpus {
            let result = std::panic::catch_unwind(|| decode(case));
            assert!(
                result.is_ok(),
                "decode panicked on malformed input {:?}",
                std::str::from_utf8(case).unwrap_or("<binary>")
            );
            // It is fine for the decoder to return Ok or Err, but it must not
            // panic and must not leave the cursor in a slicing state that panics.
            let _ = result.unwrap();
        }
    }

    #[test]
    fn extract_value_bytes_rejects_oversized_input() {
        let big = vec![b'i'; MAX_TORRENT_METADATA_BYTES + 1];
        assert!(extract_value_bytes(&big, b"info").is_none());
    }

    #[test]
    fn extract_value_bytes_handles_truncated_value() {
        // Truncated value after a key should return None rather than panic.
        assert!(extract_value_bytes(b"d4:infoi", b"info").is_none());
        assert!(extract_value_bytes(b"d4:info100:ab", b"info").is_none());
    }
}
