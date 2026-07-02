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
pub fn decode(bytes: &[u8]) -> Result<Value> {
    let mut p = Parser { bytes, pos: 0 };
    let v = p.parse()?;
    Ok(v)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
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

    fn parse(&mut self) -> Result<Value> {
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
        while self.peek()? != b'e' {
            self.pos += 1;
        }
        let s = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|e| CoreError::Bencode(e.to_string()))?;
        let n: i64 = s
            .parse()
            .map_err(|e| CoreError::Bencode(format!("bad integer: {e}")))?;
        self.bump()?; // 'e'
        Ok(Value::Int(n))
    }

    fn parse_str(&mut self) -> Result<Vec<u8>> {
        let colon = self.bytes[self.pos..]
            .iter()
            .position(|&b| b == b':')
            .ok_or_else(|| CoreError::Bencode("missing ':' in string".into()))?;
        let len: usize = std::str::from_utf8(&self.bytes[self.pos..self.pos + colon])
            .map_err(|e| CoreError::Bencode(e.to_string()))?
            .parse()
            .map_err(|e| CoreError::Bencode(format!("bad string length: {e}")))?;
        self.pos += colon + 1;
        if self.pos + len > self.bytes.len() {
            return Err(CoreError::Bencode("string overruns input".into()));
        }
        let s = self.bytes[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(s)
    }

    fn parse_list(&mut self) -> Result<Value> {
        self.bump()?; // 'l'
        let mut items = Vec::new();
        while self.peek()? != b'e' {
            items.push(self.parse()?);
        }
        self.bump()?; // 'e'
        Ok(Value::List(items))
    }

    fn parse_dict(&mut self) -> Result<Value> {
        self.bump()?; // 'd'
        let mut entries = Vec::new();
        while self.peek()? != b'e' {
            let key = self.parse_str()?;
            let val = self.parse()?;
            entries.push((key, val));
        }
        self.bump()?; // 'e'
        Ok(Value::Dict(entries))
    }
}

/// Extract the raw value bytes for a top-level key from bencoded bytes.
pub fn extract_value_bytes<'a>(bytes: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
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
    let len: usize = std::str::from_utf8(&bytes[*p..*p + colon])
        .ok()?
        .parse()
        .ok()?;
    *p += colon + 1;
    if *p + len > bytes.len() {
        return None;
    }
    let s = bytes[*p..*p + len].to_vec();
    *p += len;
    Some(s)
}

fn skip_value(bytes: &[u8], p: &mut usize) -> Option<()> {
    match bytes.get(*p)? {
        b'i' => {
            let end = bytes[*p..].iter().position(|&b| b == b'e')?;
            *p += end + 1;
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
                        let end = bytes[*p..].iter().position(|&b| b == b'e')?;
                        *p += end + 1;
                    }
                    _ => return None,
                }
            }
        }
        b'0'..=b'9' => {
            read_str(bytes, p)?;
        }
        _ => return None,
    }
    Some(())
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
}
