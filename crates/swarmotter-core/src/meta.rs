// SPDX-License-Identifier: Apache-2.0

//! `.torrent` metadata parsing.
//!
//! Parses single-file and multi-file torrent metadata dictionaries, computes
//! the info hash from the raw `info` dictionary, validates the structure, and
//! preserves source metadata where useful (announce, announce-list, private
//! flag, comment, created by, creation date).
//!
//! Parsing uses the local `bencode` module. The raw `info` bytes are extracted
//! directly so the info hash is computed over the exact original bytes, not a
//! re-serialized form.

use crate::bencode::{self, Value};
use crate::error::{CoreError, Result};
use crate::hash::InfoHash;
use serde::{Deserialize, Serialize};

/// Parsed torrent metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentMeta {
    pub info_hash: InfoHash,
    pub name: String,
    pub piece_length: u64,
    /// Concatenated SHA-1 piece hashes (20 bytes each).
    #[serde(with = "hex_piece_hashes")]
    pub pieces: Vec<[u8; 20]>,
    /// File list (single-file becomes one entry).
    pub files: Vec<MetaFile>,
    pub total_length: u64,
    pub private: bool,
    pub announce: Option<String>,
    /// Tracker tiers (announce-list), in order.
    pub announce_list: Vec<Vec<String>>,
    /// BEP 19 HTTP/FTP webseed URLs (`url-list`), preserving torrent order.
    #[serde(default)]
    pub webseeds: Vec<String>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<u64>,
    pub is_multi_file: bool,
}

/// A file entry within a torrent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaFile {
    /// Path components; for single-file this is `[name]`.
    pub path: Vec<String>,
    pub length: u64,
}

impl TorrentMeta {
    /// Validate invariants that parsing normally establishes. Durable daemon
    /// state calls this after deserialization so crafted or corrupted state
    /// cannot bypass metainfo safety checks.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(CoreError::MalformedTorrent("empty torrent name".into()));
        }
        validate_path_component(&self.name, "torrent name")?;
        if self.piece_length == 0 {
            return Err(CoreError::MalformedTorrent(
                "piece length must be greater than zero".into(),
            ));
        }
        if self.files.is_empty() {
            return Err(CoreError::MalformedTorrent(
                "torrent must contain at least one file".into(),
            ));
        }
        if !self.is_multi_file && self.files.len() != 1 {
            return Err(CoreError::MalformedTorrent(
                "single-file torrent must contain exactly one file".into(),
            ));
        }

        let mut total = 0u64;
        let mut paths = std::collections::HashSet::with_capacity(self.files.len());
        for file in &self.files {
            if file.path.is_empty() {
                return Err(CoreError::MalformedTorrent("file with empty path".into()));
            }
            for component in &file.path {
                validate_path_component(component, "file path component")?;
            }
            if !paths.insert(file.path.clone()) {
                return Err(CoreError::MalformedTorrent(format!(
                    "duplicate file path: {}",
                    file.path.join("/")
                )));
            }
            total = total.checked_add(file.length).ok_or_else(|| {
                CoreError::MalformedTorrent("total file length exceeds u64".into())
            })?;
        }
        if total != self.total_length {
            return Err(CoreError::MalformedTorrent(format!(
                "file lengths total {total} does not match recorded length {}",
                self.total_length
            )));
        }
        let expected_pieces_u64 = if total == 0 {
            1
        } else {
            total.div_ceil(self.piece_length)
        };
        let expected_pieces = usize::try_from(expected_pieces_u64).map_err(|_| {
            CoreError::MalformedTorrent("piece count exceeds platform limits".into())
        })?;
        if self.pieces.len() != expected_pieces {
            return Err(CoreError::MalformedTorrent(format!(
                "piece count {} does not match expected {expected_pieces}",
                self.pieces.len()
            )));
        }
        Ok(())
    }

    /// Number of pieces.
    pub fn piece_count(&self) -> usize {
        self.pieces.len()
    }

    /// Last piece length (may be smaller than piece_length).
    pub fn last_piece_length(&self) -> u64 {
        if self.total_length == 0 {
            return 0;
        }
        let rem = self.total_length % self.piece_length;
        if rem == 0 {
            self.piece_length
        } else {
            rem
        }
    }

    /// Byte range of a piece index `(start, end)` within the torrent's data.
    pub fn piece_byte_range(&self, index: u64) -> Option<(u64, u64)> {
        if index as usize >= self.pieces.len() {
            return None;
        }
        let start = index * self.piece_length;
        let end = std::cmp::min(start + self.piece_length, self.total_length);
        Some((start, end))
    }

    /// True if the torrent metadata declares it private (DHT/PEX disabled).
    pub fn is_private(&self) -> bool {
        self.private
    }

    /// All trackers flattened across tiers, preserving order (deduplicated).
    pub fn all_trackers(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        if let Some(a) = &self.announce {
            if seen.insert(a.clone()) {
                out.push(a.clone());
            }
        }
        for tier in &self.announce_list {
            for t in tier {
                if seen.insert(t.clone()) {
                    out.push(t.clone());
                }
            }
        }
        out
    }
}

/// Parse a `.torrent` file's raw bytes.
pub fn parse_torrent(bytes: &[u8]) -> Result<TorrentMeta> {
    let root = bencode::decode(bytes)?;
    let root = root
        .as_dict()
        .ok_or_else(|| CoreError::MalformedTorrent("top-level must be a dict".into()))?;

    let info_bytes = bencode::extract_value_bytes(bytes, b"info")
        .ok_or_else(|| CoreError::MalformedTorrent("missing 'info' dictionary".into()))?;

    let info_hash = InfoHash::from_info_bencoded(info_bytes);

    let info = root
        .iter()
        .find(|(k, _)| k == b"info")
        .map(|(_, v)| v)
        .and_then(Value::as_dict)
        .ok_or_else(|| CoreError::MalformedTorrent("missing 'info' dictionary".into()))?;

    let name = get_str(info, b"name")
        .ok_or_else(|| CoreError::MalformedTorrent("missing 'name'".into()))?
        .to_string();
    validate_path_component(&name, "torrent name")?;
    if name.is_empty() {
        return Err(CoreError::MalformedTorrent("empty 'name'".into()));
    }

    let piece_length = get_int(info, b"piece length")
        .ok_or_else(|| CoreError::MalformedTorrent("missing 'piece length'".into()))?;
    if piece_length <= 0 {
        return Err(CoreError::MalformedTorrent(
            "piece_length must be > 0".into(),
        ));
    }
    let piece_length = piece_length as u64;

    let pieces_bytes = info
        .iter()
        .find(|(k, _)| k == b"pieces")
        .map(|(_, v)| v)
        .and_then(Value::as_str)
        .ok_or_else(|| CoreError::MalformedTorrent("missing 'pieces'".into()))?;
    if pieces_bytes.len() % 20 != 0 {
        return Err(CoreError::MalformedTorrent(
            "pieces length not multiple of 20".into(),
        ));
    }
    let pieces: Vec<[u8; 20]> = pieces_bytes
        .chunks_exact(20)
        .map(|c| {
            let mut a = [0u8; 20];
            a.copy_from_slice(c);
            a
        })
        .collect();

    let private = get_int(info, b"private").unwrap_or(0) == 1;

    let (files, total_length, is_multi_file) =
        if let Some(length_v) = info.iter().find(|(k, _)| k == b"length").map(|(_, v)| v) {
            // single-file: length is directly in the info dict.
            let length = length_v
                .as_int()
                .ok_or_else(|| CoreError::MalformedTorrent("'length' must be an integer".into()))?;
            let length = non_negative_length(length, "'length'")?;
            (
                vec![MetaFile {
                    path: vec![name.clone()],
                    length,
                }],
                length,
                false,
            )
        } else if let Some(files_v) = info.iter().find(|(k, _)| k == b"files").map(|(_, v)| v) {
            // multi-file
            let list = files_v
                .as_list()
                .ok_or_else(|| CoreError::MalformedTorrent("'files' must be a list".into()))?;
            let mut total = 0u64;
            let mut out = Vec::with_capacity(list.len());
            let mut paths = std::collections::HashSet::with_capacity(list.len());
            for f in list {
                let length = f
                    .get(b"length")
                    .and_then(Value::as_int)
                    .ok_or_else(|| CoreError::MalformedTorrent("file missing length".into()))?;
                let length = non_negative_length(length, "file length")?;
                let path_vals = f
                    .get(b"path")
                    .and_then(Value::as_list)
                    .ok_or_else(|| CoreError::MalformedTorrent("file missing path".into()))?;
                let mut full_path = vec![name.clone()];
                for p in path_vals {
                    let s = p.as_str_utf8().ok_or_else(|| {
                        CoreError::MalformedTorrent("path component not utf8".into())
                    })?;
                    validate_path_component(s, "file path component")?;
                    full_path.push(s.to_string());
                }
                if path_vals.is_empty() {
                    return Err(CoreError::MalformedTorrent("file with empty path".into()));
                }
                if !paths.insert(full_path.clone()) {
                    return Err(CoreError::MalformedTorrent(format!(
                        "duplicate file path: {}",
                        full_path.join("/")
                    )));
                }
                total = total.checked_add(length).ok_or_else(|| {
                    CoreError::MalformedTorrent("total file length exceeds u64".into())
                })?;
                out.push(MetaFile {
                    path: full_path,
                    length,
                });
            }
            (out, total, true)
        } else {
            return Err(CoreError::MalformedTorrent(
                "info missing file/files".into(),
            ));
        };

    // Validate piece count matches total length within one piece.
    let expected_pieces_u64 = if total_length == 0 {
        1u64
    } else {
        total_length.div_ceil(piece_length)
    };
    let expected_pieces = usize::try_from(expected_pieces_u64)
        .map_err(|_| CoreError::MalformedTorrent("piece count exceeds platform limits".into()))?;
    if pieces.len() != expected_pieces {
        return Err(CoreError::MalformedTorrent(format!(
            "piece count {} does not match expected {} for length {}",
            pieces.len(),
            expected_pieces,
            total_length
        )));
    }

    let announce = get_str(root, b"announce").map(|s| s.to_string());
    let announce_list = root
        .iter()
        .find(|(k, _)| k == b"announce-list")
        .map(|(_, v)| v)
        .and_then(Value::as_list)
        .map(|tiers| {
            tiers
                .iter()
                .map(|tier| {
                    tier.as_list()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|i| i.as_str_utf8().map(|s| s.to_string()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let webseeds = parse_url_list(root);
    let comment = get_str(root, b"comment").map(|s| s.to_string());
    let created_by = get_str(root, b"created by").map(|s| s.to_string());
    let creation_date = get_int(root, b"creation date").map(|i| i as u64);

    Ok(TorrentMeta {
        info_hash,
        name,
        piece_length,
        pieces,
        files,
        total_length,
        private,
        announce,
        announce_list,
        webseeds,
        comment,
        created_by,
        creation_date,
        is_multi_file,
    })
}

fn get_str<'a>(dict: &'a [(Vec<u8>, Value)], key: &[u8]) -> Option<&'a str> {
    dict.iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| v.as_str_utf8())
}

fn get_int(dict: &[(Vec<u8>, Value)], key: &[u8]) -> Option<i64> {
    dict.iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| v.as_int())
}

fn non_negative_length(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| CoreError::MalformedTorrent(format!("{field} must not be negative")))
}

fn parse_url_list(dict: &[(Vec<u8>, Value)]) -> Vec<String> {
    let mut out = match dict.iter().find(|(k, _)| k == b"url-list").map(|(_, v)| v) {
        Some(Value::Str(_)) => get_str(dict, b"url-list")
            .map(|s| vec![s.to_string()])
            .unwrap_or_default(),
        Some(Value::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str_utf8().map(|s| s.to_string()))
            .collect(),
        _ => Vec::new(),
    };
    let mut seen = std::collections::HashSet::new();
    out.retain(|url| !url.is_empty() && seen.insert(url.clone()));
    out
}

fn validate_path_component(value: &str, kind: &str) -> Result<()> {
    if value.is_empty() {
        return Err(CoreError::MalformedTorrent(format!("{kind} is empty")));
    }
    if value == "." || value == ".." {
        return Err(CoreError::MalformedTorrent(format!(
            "{kind} cannot be relative traversal component"
        )));
    }
    if value.starts_with('/') || value.starts_with('\\') {
        return Err(CoreError::MalformedTorrent(format!(
            "{kind} cannot be absolute"
        )));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(CoreError::MalformedTorrent(format!(
            "{kind} cannot contain path separators"
        )));
    }
    if value.contains(':') {
        return Err(CoreError::MalformedTorrent(format!(
            "{kind} cannot contain windows-style prefix characters"
        )));
    }
    Ok(())
}

mod hex_piece_hashes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &[[u8; 20]], s: S) -> std::result::Result<S::Ok, S::Error> {
        let hexes: Vec<String> = v.iter().map(hex::encode).collect();
        hexes.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Vec<[u8; 20]>, D::Error> {
        let hexes: Vec<String> = Vec::deserialize(d)?;
        hexes
            .iter()
            .map(|h| {
                let b = hex::decode(h).map_err(serde::de::Error::custom)?;
                let mut a = [0u8; 20];
                a.copy_from_slice(&b);
                Ok(a)
            })
            .collect()
    }
}

/// Build a minimal valid single-file `.torrent` body (for tests/fixtures) from
/// content. Pieces are computed via SHA-1 of the data.
pub fn build_single_file_torrent(
    name: &str,
    content: &[u8],
    piece_length: u64,
    announce: Option<&str>,
    private: bool,
) -> Vec<u8> {
    build_single_file_torrent_with_webseeds(name, content, piece_length, announce, private, &[])
}

/// Build a minimal valid single-file `.torrent` body with BEP 19 webseeds.
pub fn build_single_file_torrent_with_webseeds(
    name: &str,
    content: &[u8],
    piece_length: u64,
    announce: Option<&str>,
    private: bool,
    webseeds: &[&str],
) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    let mut pieces = Vec::new();
    let mut offset = 0usize;
    while offset < content.len() {
        let end = std::cmp::min(offset + piece_length as usize, content.len());
        let mut hasher = Sha1::new();
        hasher.update(&content[offset..end]);
        pieces.extend_from_slice(&hasher.finalize());
        offset = end;
    }
    if content.is_empty() {
        let mut hasher = Sha1::new();
        hasher.update(b"");
        pieces.extend_from_slice(&hasher.finalize());
    }

    let mut out = Vec::new();
    out.push(b'd');
    if let Some(a) = announce {
        write_str(&mut out, b"announce");
        write_str(&mut out, a.as_bytes());
    }
    write_str(&mut out, b"info");
    let mut info = Vec::new();
    info.push(b'd');
    write_str(&mut info, b"length");
    write_int(&mut info, content.len() as u64);
    write_str(&mut info, b"name");
    write_str(&mut info, name.as_bytes());
    write_str(&mut info, b"piece length");
    write_int(&mut info, piece_length);
    write_str(&mut info, b"pieces");
    write_str(&mut info, &pieces);
    if private {
        write_str(&mut info, b"private");
        write_int(&mut info, 1);
    }
    info.push(b'e');
    out.extend_from_slice(&info);
    write_webseeds(&mut out, webseeds);
    out.push(b'e');
    out
}

/// Build a multi-file `.torrent` body (for tests/fixtures).
pub fn build_multi_file_torrent(
    name: &str,
    files: &[(Vec<String>, u64)],
    contents: &[&[u8]],
    piece_length: u64,
    announce: Option<&str>,
) -> Vec<u8> {
    build_multi_file_torrent_with_webseeds(name, files, contents, piece_length, announce, &[])
}

/// Build a multi-file `.torrent` body with BEP 19 webseeds.
pub fn build_multi_file_torrent_with_webseeds(
    name: &str,
    files: &[(Vec<String>, u64)],
    contents: &[&[u8]],
    piece_length: u64,
    announce: Option<&str>,
    webseeds: &[&str],
) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    assert_eq!(files.len(), contents.len());
    let total: usize = contents.iter().map(|c| c.len()).sum();
    let mut all = Vec::with_capacity(total);
    for c in contents {
        all.extend_from_slice(c);
    }
    let mut pieces = Vec::new();
    let mut offset = 0usize;
    while offset < all.len() {
        let end = std::cmp::min(offset + piece_length as usize, all.len());
        let mut hasher = Sha1::new();
        hasher.update(&all[offset..end]);
        pieces.extend_from_slice(&hasher.finalize());
        offset = end;
    }

    let mut out = Vec::new();
    out.push(b'd');
    if let Some(a) = announce {
        write_str(&mut out, b"announce");
        write_str(&mut out, a.as_bytes());
    }
    write_str(&mut out, b"info");
    let mut info = Vec::new();
    info.push(b'd');
    write_str(&mut info, b"name");
    write_str(&mut info, name.as_bytes());
    write_str(&mut info, b"piece length");
    write_int(&mut info, piece_length);
    write_str(&mut info, b"pieces");
    write_str(&mut info, &pieces);
    write_str(&mut info, b"files");
    info.push(b'l');
    for (path, length) in files {
        info.push(b'd');
        write_str(&mut info, b"length");
        write_int(&mut info, *length);
        write_str(&mut info, b"path");
        info.push(b'l');
        for seg in path {
            write_str(&mut info, seg.as_bytes());
        }
        info.push(b'e');
        info.push(b'e');
    }
    info.push(b'e');
    info.push(b'e');
    out.extend_from_slice(&info);
    write_webseeds(&mut out, webseeds);
    out.push(b'e');
    out
}

fn write_webseeds(out: &mut Vec<u8>, webseeds: &[&str]) {
    if webseeds.is_empty() {
        return;
    }
    write_str(out, b"url-list");
    if webseeds.len() == 1 {
        write_str(out, webseeds[0].as_bytes());
        return;
    }
    out.push(b'l');
    for webseed in webseeds {
        write_str(out, webseed.as_bytes());
    }
    out.push(b'e');
}

fn write_str(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(format!("{}:", s.len()).as_bytes());
    out.extend_from_slice(s);
}
fn write_int(out: &mut Vec<u8>, n: u64) {
    out.push(b'i');
    out.extend_from_slice(n.to_string().as_bytes());
    out.push(b'e');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_single_file_torrent_with_length(length: i64) -> Vec<u8> {
        let mut out = b"d4:infod6:lengthi".to_vec();
        out.extend_from_slice(length.to_string().as_bytes());
        out.extend_from_slice(b"e4:name1:f12:piece lengthi8e6:pieces20:");
        out.extend_from_slice(&[0u8; 20]);
        out.extend_from_slice(b"ee");
        out
    }

    fn raw_multi_file_torrent_with_lengths(lengths: &[i64]) -> Vec<u8> {
        let mut out = b"d4:infod5:filesl".to_vec();
        for (index, length) in lengths.iter().enumerate() {
            out.extend_from_slice(b"d6:lengthi");
            out.extend_from_slice(length.to_string().as_bytes());
            out.extend_from_slice(b"e4:pathl");
            write_str(&mut out, format!("file-{index}").as_bytes());
            out.extend_from_slice(b"ee");
        }
        out.extend_from_slice(b"e4:name3:dir12:piece lengthi8e6:pieces20:");
        out.extend_from_slice(&[0u8; 20]);
        out.extend_from_slice(b"ee");
        out
    }

    #[test]
    fn parses_single_file_torrent() {
        let content = b"hello swarmotter world data payload here";
        let bytes = build_single_file_torrent(
            "file.bin",
            content,
            16,
            Some("http://tracker.example/announce"),
            false,
        );
        let meta = parse_torrent(&bytes).unwrap();
        assert!(!meta.is_multi_file);
        assert_eq!(meta.name, "file.bin");
        assert_eq!(meta.piece_length, 16);
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.files[0].length, content.len() as u64);
        assert_eq!(meta.total_length, content.len() as u64);
        assert!(!meta.private);
        assert_eq!(
            meta.announce.as_deref(),
            Some("http://tracker.example/announce")
        );
        assert!(meta.webseeds.is_empty());
        let expected_pieces = (content.len() as u64).div_ceil(16);
        assert_eq!(meta.piece_count() as u64, expected_pieces);
        let last_len = meta.last_piece_length();
        assert_eq!(last_len, (content.len() as u64) % 16);
    }

    #[test]
    fn parses_multi_file_torrent() {
        let files = vec![
            (vec!["a.txt".into()], 5u64),
            (vec!["sub".into(), "b.bin".into()], 7u64),
        ];
        let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
        let bytes = build_multi_file_torrent("dir", &files, &contents, 4, Some("http://t/a"));
        let meta = parse_torrent(&bytes).unwrap();
        assert!(meta.is_multi_file);
        assert_eq!(meta.name, "dir");
        assert_eq!(meta.files.len(), 2);
        assert_eq!(meta.files[0].path, vec!["dir", "a.txt"]);
        assert_eq!(meta.files[1].path, vec!["dir", "sub", "b.bin"]);
        assert_eq!(meta.total_length, 12);
        assert_eq!(meta.announce.as_deref(), Some("http://t/a"));
    }

    #[test]
    fn info_hash_is_stable() {
        let content = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let bytes = build_single_file_torrent("f", content, 8, None, false);
        let meta1 = parse_torrent(&bytes).unwrap();
        let meta2 = parse_torrent(&bytes).unwrap();
        assert_eq!(meta1.info_hash, meta2.info_hash);
        let bytes2 = build_single_file_torrent("f", b"different content here!!", 8, None, false);
        let meta3 = parse_torrent(&bytes2).unwrap();
        assert_ne!(meta1.info_hash, meta3.info_hash);
    }

    #[test]
    fn private_flag_parsed() {
        let bytes = build_single_file_torrent("f", b"private content data", 8, None, true);
        let meta = parse_torrent(&bytes).unwrap();
        assert!(meta.is_private());
    }

    #[test]
    fn piece_byte_range_correct() {
        let bytes =
            build_single_file_torrent("f", b"0123456789abcdef0123456789abcdef", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        assert_eq!(meta.piece_byte_range(0), Some((0, 8)));
        assert_eq!(meta.piece_byte_range(3), Some((24, 32)));
        assert_eq!(meta.piece_byte_range(4), None);
    }

    #[test]
    fn rejects_bad_torrent() {
        assert!(parse_torrent(b"not bencode").is_err());
        assert!(parse_torrent(b"d4:name3:fooe").is_err());
    }

    #[test]
    fn rejects_negative_single_and_multi_file_lengths() {
        let single = parse_torrent(&raw_single_file_torrent_with_length(-1)).unwrap_err();
        assert!(single.to_string().contains("must not be negative"));

        let multi = parse_torrent(&raw_multi_file_torrent_with_lengths(&[-1])).unwrap_err();
        assert!(multi.to_string().contains("must not be negative"));
    }

    #[test]
    fn rejects_total_file_length_overflow() {
        let torrent = raw_multi_file_torrent_with_lengths(&[i64::MAX, i64::MAX, i64::MAX]);
        let error = parse_torrent(&torrent).unwrap_err();
        assert!(error.to_string().contains("total file length exceeds u64"));
    }

    #[test]
    fn rejects_duplicate_multi_file_paths() {
        let files = vec![(vec!["same.bin".into()], 1), (vec!["same.bin".into()], 1)];
        let contents: Vec<&[u8]> = vec![b"a", b"b"];
        let torrent = build_multi_file_torrent("dir", &files, &contents, 2, None);
        let error = parse_torrent(&torrent).unwrap_err();
        assert!(error.to_string().contains("duplicate file path"));
    }

    #[test]
    fn all_trackers_dedups() {
        let bytes =
            build_single_file_torrent("f", b"data payload", 8, Some("http://a/announce"), false);
        let mut meta = parse_torrent(&bytes).unwrap();
        meta.announce_list = vec![
            vec!["http://a/announce".into(), "http://b/announce".into()],
            vec!["http://c/announce".into()],
        ];
        let t = meta.all_trackers();
        assert_eq!(
            t,
            vec![
                "http://a/announce",
                "http://b/announce",
                "http://c/announce"
            ]
        );
    }

    #[test]
    fn rejects_unsafe_torrent_name() {
        assert!(parse_torrent(&build_single_file_torrent(
            "../escape",
            b"abc",
            16,
            None,
            false
        ))
        .is_err());
        assert!(parse_torrent(&build_single_file_torrent(
            "/absolute",
            b"abc",
            16,
            None,
            false
        ))
        .is_err());
        assert!(parse_torrent(&build_single_file_torrent("a/b", b"abc", 16, None, false)).is_err());
        assert!(parse_torrent(&build_single_file_torrent(
            "C:windows",
            b"abc",
            16,
            None,
            false
        ))
        .is_err());
    }

    #[test]
    fn rejects_unsafe_file_path_components() {
        let files = vec![
            (vec!["a.txt".to_string(), "..".to_string()], 3u64),
            (vec!["".to_string(), "ok".to_string()], 3u64),
            (vec!["b.txt\\c".to_string()], 3u64),
        ];
        let contents: Vec<&[u8]> = vec![b"one", b"two", b"three"];
        let bytes = build_multi_file_torrent("safe", &files, &contents, 8, None);
        assert!(parse_torrent(&bytes).is_err());
    }

    #[test]
    fn parses_single_webseed_url_list() {
        let bytes = with_url_list(
            build_single_file_torrent("f", b"webseed data", 8, None, false),
            string_value(b"http://127.0.0.1/files/f"),
        );

        let meta = parse_torrent(&bytes).unwrap();

        assert_eq!(meta.webseeds, vec!["http://127.0.0.1/files/f"]);
    }

    #[test]
    fn parses_list_webseed_url_list() {
        let mut url_list = Vec::new();
        url_list.push(b'l');
        write_str(&mut url_list, b"http://127.0.0.1/files/f");
        write_str(&mut url_list, b"https://webseed.example/data/f");
        url_list.push(b'e');
        let bytes = with_url_list(
            build_single_file_torrent("f", b"webseed data", 8, None, false),
            url_list,
        );

        let meta = parse_torrent(&bytes).unwrap();

        assert_eq!(
            meta.webseeds,
            vec!["http://127.0.0.1/files/f", "https://webseed.example/data/f"]
        );
    }

    #[test]
    fn deserialized_metadata_validation_rejects_broken_invariants() {
        let bytes = build_single_file_torrent("state.bin", b"state payload", 8, None, false);
        let mut meta = parse_torrent(&bytes).unwrap();
        assert!(meta.validate().is_ok());

        meta.total_length += 1;
        assert!(meta.validate().is_err());

        let mut meta = parse_torrent(&bytes).unwrap();
        meta.files[0].path = vec!["..".into()];
        assert!(meta.validate().is_err());
    }

    fn string_value(value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_str(&mut out, value);
        out
    }

    fn with_url_list(mut torrent: Vec<u8>, value: Vec<u8>) -> Vec<u8> {
        assert_eq!(torrent.pop(), Some(b'e'));
        write_str(&mut torrent, b"url-list");
        torrent.extend_from_slice(&value);
        torrent.push(b'e');
        torrent
    }
}
