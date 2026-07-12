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

/// Maximum total size of a bencoded `.torrent` metadata document (or magnet
/// `info` dict) accepted through API upload, watch-folder import, BEP 9
/// metadata exchange, or a direct core parser call. Restored daemon state is
/// JSON and instead uses exact piece-hash decoding plus [`TorrentMeta::validate`].
/// See ADR-0050.
pub const MAX_TORRENT_METADATA_BYTES: usize = 16 * 1024 * 1024;

/// Maximum bencode nesting depth. The root value is depth zero; entering a
/// list or dictionary increments depth. See ADR-0050.
pub const MAX_BENCODE_DEPTH: usize = 128;

/// Maximum number of bencode nodes (integers, byte strings, lists, and
/// dictionaries) accepted in one document. See ADR-0050.
pub const MAX_BENCODE_NODES: usize = 250_000;

/// Maximum number of files in one torrent. See ADR-0050.
pub const MAX_TORRENT_FILES: usize = 100_000;

/// Maximum number of pieces in one torrent. See ADR-0050.
pub const MAX_TORRENT_PIECES: usize = 750_000;

/// Maximum declared piece length in bytes. See ADR-0050.
pub const MAX_PIECE_LENGTH: u64 = 64 * 1024 * 1024;

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
        if self.piece_length > MAX_PIECE_LENGTH {
            return Err(CoreError::MalformedTorrent(format!(
                "piece length {} exceeds maximum {MAX_PIECE_LENGTH}",
                self.piece_length
            )));
        }
        if self.files.is_empty() {
            return Err(CoreError::MalformedTorrent(
                "torrent must contain at least one file".into(),
            ));
        }
        if self.files.len() > MAX_TORRENT_FILES {
            return Err(CoreError::MalformedTorrent(format!(
                "file count {} exceeds maximum {MAX_TORRENT_FILES}",
                self.files.len()
            )));
        }
        if self.pieces.len() > MAX_TORRENT_PIECES {
            return Err(CoreError::MalformedTorrent(format!(
                "piece count {} exceeds maximum {MAX_TORRENT_PIECES}",
                self.pieces.len()
            )));
        }
        if self.total_length == 0 {
            return Err(CoreError::MalformedTorrent(
                "torrent total length must be greater than zero".into(),
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
        let expected_pieces_u64 = total.div_ceil(self.piece_length);
        if expected_pieces_u64 > MAX_TORRENT_PIECES as u64 {
            return Err(CoreError::MalformedTorrent(format!(
                "expected piece count {expected_pieces_u64} exceeds maximum {MAX_TORRENT_PIECES}"
            )));
        }
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

    /// Convert this torrent's piece length to `u32`, returning
    /// `MalformedTorrent` if it does not fit. [`MAX_PIECE_LENGTH`] guarantees a
    /// valid torrent fits, but this avoids relying on `as` narrowing at
    /// engine/storage boundaries. See ADR-0050.
    pub fn piece_length_u32(&self) -> Result<u32> {
        if self.piece_length > MAX_PIECE_LENGTH {
            return Err(CoreError::MalformedTorrent(format!(
                "piece length {} exceeds maximum {MAX_PIECE_LENGTH}",
                self.piece_length
            )));
        }
        u32::try_from(self.piece_length).map_err(|_| {
            CoreError::MalformedTorrent(format!(
                "piece length {} exceeds u32 range",
                self.piece_length
            ))
        })
    }

    /// Convert the piece length for a given piece index to `u32`, returning
    /// `MalformedTorrent` if it does not fit. The last piece may be shorter.
    pub fn piece_length_for_index_u32(&self, index: usize) -> Result<u32> {
        let regular_piece_length = self.piece_length_u32()?;
        if index >= self.piece_count() {
            return Err(CoreError::MalformedTorrent(format!(
                "piece index {index} is out of range"
            )));
        }
        let len = if index.checked_add(1) == Some(self.piece_count()) {
            self.last_piece_length()
        } else {
            return Ok(regular_piece_length);
        };
        u32::try_from(len).map_err(|_| {
            CoreError::MalformedTorrent(format!("piece length {len} exceeds u32 range"))
        })
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

    parse_torrent_root(root, info_bytes)
}

/// Parse a raw BEP 9 `info` dictionary and attach magnet tracker context.
///
/// The unwrapped `info` bytes are subject to the same metadata byte, bencode
/// depth, and node budgets as a `.torrent` document. Parsing the raw dictionary
/// directly avoids adding trusted wrapper bytes that would incorrectly reject
/// an otherwise valid `info` dictionary at the exact metadata-size boundary.
pub fn parse_info_dict(info_bytes: &[u8], trackers: &[String]) -> Result<TorrentMeta> {
    let info = bencode::decode(info_bytes)?;
    if info.as_dict().is_none() {
        return Err(CoreError::MalformedTorrent(
            "BEP 9 info metadata must be a dictionary".into(),
        ));
    }

    let mut root = Vec::new();
    if let Some(primary) = trackers.first() {
        root.push((
            b"announce".to_vec(),
            Value::Str(primary.as_bytes().to_vec()),
        ));
    }
    if trackers.len() > 1 {
        root.push((
            b"announce-list".to_vec(),
            Value::List(vec![Value::List(
                trackers[1..]
                    .iter()
                    .map(|tracker| Value::Str(tracker.as_bytes().to_vec()))
                    .collect(),
            )]),
        ));
    }
    root.push((b"info".to_vec(), info));

    parse_torrent_root(&root, info_bytes)
}

fn parse_torrent_root(root: &[(Vec<u8>, Value)], info_bytes: &[u8]) -> Result<TorrentMeta> {
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
    if piece_length > MAX_PIECE_LENGTH {
        return Err(CoreError::MalformedTorrent(format!(
            "piece_length {piece_length} exceeds maximum {MAX_PIECE_LENGTH}"
        )));
    }

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
    let piece_count = pieces_bytes.len() / 20;
    if piece_count > MAX_TORRENT_PIECES {
        return Err(CoreError::MalformedTorrent(format!(
            "piece count {piece_count} exceeds maximum {MAX_TORRENT_PIECES}"
        )));
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
            if list.len() > MAX_TORRENT_FILES {
                return Err(CoreError::MalformedTorrent(format!(
                    "file count {} exceeds maximum {MAX_TORRENT_FILES}",
                    list.len()
                )));
            }
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

    if total_length == 0 {
        return Err(CoreError::MalformedTorrent(
            "torrent total length must be greater than zero".into(),
        ));
    }

    // Validate piece count matches total length within one piece.
    let expected_pieces_u64 = total_length.div_ceil(piece_length);
    if expected_pieces_u64 > MAX_TORRENT_PIECES as u64 {
        return Err(CoreError::MalformedTorrent(format!(
            "expected piece count {expected_pieces_u64} exceeds maximum {MAX_TORRENT_PIECES}"
        )));
    }
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

    let meta = TorrentMeta {
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
    };
    meta.validate()?;
    Ok(meta)
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
    use std::fmt;

    use serde::de::{Error as _, IgnoredAny, SeqAccess, Visitor};
    use serde::{Deserializer, Serialize, Serializer};

    use super::MAX_TORRENT_PIECES;

    pub fn serialize<S: Serializer>(v: &[[u8; 20]], s: S) -> std::result::Result<S::Ok, S::Error> {
        let hexes: Vec<String> = v.iter().map(hex::encode).collect();
        hexes.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Vec<[u8; 20]>, D::Error> {
        struct PieceHashesVisitor;

        impl<'de> Visitor<'de> for PieceHashesVisitor {
            type Value = Vec<[u8; 20]>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded array of 40-character SHA-1 hashes")
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                if sequence
                    .size_hint()
                    .is_some_and(|size| size > MAX_TORRENT_PIECES)
                {
                    return Err(A::Error::custom(format!(
                        "piece hash count exceeds maximum {MAX_TORRENT_PIECES}"
                    )));
                }

                let initial_capacity = sequence.size_hint().unwrap_or(0).min(4096);
                let mut hashes = Vec::with_capacity(initial_capacity);
                loop {
                    if hashes.len() == MAX_TORRENT_PIECES {
                        if sequence.next_element::<IgnoredAny>()?.is_some() {
                            return Err(A::Error::custom(format!(
                                "piece hash count exceeds maximum {MAX_TORRENT_PIECES}"
                            )));
                        }
                        break;
                    }

                    let index = hashes.len();
                    let Some(encoded) = sequence.next_element::<String>()? else {
                        break;
                    };
                    if encoded.len() != 40 {
                        if encoded.len() % 2 == 0 {
                            return Err(A::Error::custom(format!(
                                "piece hash {index} has length {} but SHA-1 hashes must be exactly 20 bytes",
                                encoded.len() / 2
                            )));
                        }
                        return Err(A::Error::custom(format!(
                            "piece hash {index} has odd encoded length {}; expected 40 hex characters",
                            encoded.len()
                        )));
                    }

                    let mut hash = [0u8; 20];
                    hex::decode_to_slice(encoded.as_bytes(), &mut hash).map_err(|_| {
                        A::Error::custom(format!("piece hash {index} is not valid hex"))
                    })?;
                    hashes.push(hash);
                }
                Ok(hashes)
            }
        }

        d.deserialize_seq(PieceHashesVisitor)
    }
}

/// Read a `.torrent` metadata file from disk, enforcing the
/// [`MAX_TORRENT_METADATA_BYTES`] limit before allocation. The file size is
/// checked before opening and again on the opened file; then the bytes are read
/// through a `MAX_TORRENT_METADATA_BYTES + 1` limiter into a buffer initially
/// allocated to the checked length. The opened file's final length must equal
/// both its initial length and the number of bytes read. Returns
/// `MalformedTorrent` when the file exceeds the limit or changes during the
/// read and `Io` for filesystem errors.
pub fn read_torrent_file(path: &std::path::Path) -> Result<Vec<u8>> {
    let path_len = std::fs::symlink_metadata(path)
        .map_err(CoreError::Io)?
        .len();
    validate_torrent_file_length(path_len, "path metadata")?;

    let mut file = std::fs::File::open(path).map_err(CoreError::Io)?;
    let initial_len = file.metadata().map_err(CoreError::Io)?.len();
    validate_torrent_file_length(initial_len, "opened file")?;
    if initial_len != path_len {
        return Err(CoreError::MalformedTorrent(
            "torrent file changed between metadata check and open".into(),
        ));
    }

    let len_usize = usize::try_from(initial_len).map_err(|_| {
        CoreError::MalformedTorrent("torrent file size exceeds platform usize".into())
    })?;
    let buf = read_bounded_torrent_bytes(&mut file, len_usize)?;

    let final_len = file.metadata().map_err(CoreError::Io)?.len();
    validate_torrent_file_length(final_len, "final file metadata")?;
    validate_completed_torrent_read(initial_len, final_len, buf.len())?;
    Ok(buf)
}

fn validate_torrent_file_length(len: u64, observation: &str) -> Result<()> {
    if len > MAX_TORRENT_METADATA_BYTES as u64 {
        return Err(CoreError::MalformedTorrent(format!(
            "torrent file {observation} size {len} exceeds maximum {MAX_TORRENT_METADATA_BYTES}"
        )));
    }
    Ok(())
}

fn read_bounded_torrent_bytes<R: std::io::Read>(
    reader: &mut R,
    initial_capacity: usize,
) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(initial_capacity);
    let read_limit = MAX_TORRENT_METADATA_BYTES
        .checked_add(1)
        .ok_or_else(|| CoreError::MalformedTorrent("metadata read limit overflow".into()))?;
    let mut chunk = [0u8; 8192];
    while buf.len() < read_limit {
        let remaining = read_limit - buf.len();
        let to_read = remaining.min(chunk.len());
        let count = std::io::Read::read(reader, &mut chunk[..to_read]).map_err(CoreError::Io)?;
        if count == 0 {
            break;
        }
        let required_capacity = buf
            .len()
            .checked_add(count)
            .ok_or_else(|| CoreError::MalformedTorrent("metadata read length overflow".into()))?;
        if required_capacity > buf.capacity() {
            // The argument is additional capacity relative to `len`, not
            // relative to the allocator-provided current capacity.
            buf.reserve_exact(required_capacity - buf.len());
        }
        buf.extend_from_slice(&chunk[..count]);
    }
    if buf.len() > MAX_TORRENT_METADATA_BYTES {
        return Err(CoreError::MalformedTorrent(format!(
            "torrent file grew to {} during read, exceeding maximum {MAX_TORRENT_METADATA_BYTES}",
            buf.len()
        )));
    }
    Ok(buf)
}

fn validate_completed_torrent_read(
    initial_len: u64,
    final_len: u64,
    read_len: usize,
) -> Result<()> {
    let read_len = u64::try_from(read_len).map_err(|_| {
        CoreError::MalformedTorrent("torrent bytes read exceed platform u64".into())
    })?;
    if initial_len != final_len || final_len != read_len {
        return Err(CoreError::MalformedTorrent(format!(
            "torrent file changed during read (initial {initial_len}, final {final_len}, read {read_len})"
        )));
    }
    Ok(())
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

    fn torrent_padded_to_size(target: usize) -> Vec<u8> {
        let mut bytes =
            build_single_file_torrent("limit.bin", b"bounded metadata payload", 8, None, false);
        assert_eq!(bytes.pop(), Some(b'e'));
        bytes.extend_from_slice(b"7:padding");

        let mut padding_len = target.saturating_sub(bytes.len() + 2);
        for _ in 0..32 {
            let encoded_len = bytes.len() + padding_len.to_string().len() + 1 + padding_len + 1;
            if encoded_len == target {
                bytes.extend_from_slice(padding_len.to_string().as_bytes());
                bytes.push(b':');
                bytes.extend(std::iter::repeat_n(b'x', padding_len));
                bytes.push(b'e');
                assert_eq!(bytes.len(), target);
                return bytes;
            }
            padding_len = target
                .checked_sub(bytes.len() + padding_len.to_string().len() + 2)
                .expect("target must accommodate the generated torrent");
        }
        panic!("could not solve bencode padding for target size {target}");
    }

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
        assert!(matches!(&error, CoreError::MalformedTorrent(_)));
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

    #[test]
    fn deserialized_metadata_validation_enforces_all_metainfo_budgets() {
        let bytes = build_single_file_torrent("state.bin", b"state payload", 8, None, false);

        let mut meta = parse_torrent(&bytes).unwrap();
        meta.piece_length = MAX_PIECE_LENGTH + 1;
        let err = meta.validate().unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("piece length"));
        assert!(err.to_string().contains("exceeds maximum"));

        let mut meta = parse_torrent(&bytes).unwrap();
        meta.files = vec![meta.files[0].clone(); MAX_TORRENT_FILES + 1];
        let err = meta.validate().unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("file count"));
        assert!(err.to_string().contains("exceeds maximum"));

        let mut meta = parse_torrent(&bytes).unwrap();
        meta.pieces = vec![[0u8; 20]; MAX_TORRENT_PIECES + 1];
        let err = meta.validate().unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("piece count"));
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn deserialized_metadata_validation_rejects_zero_total_with_one_hash() {
        let bytes = build_single_file_torrent("state.bin", b"state payload", 8, None, false);
        let mut meta = parse_torrent(&bytes).unwrap();
        meta.total_length = 0;
        meta.files[0].length = 0;
        meta.pieces = vec![[0u8; 20]];

        let err = meta.validate().unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("total length"));
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn rejects_piece_length_zero_and_over_limit() {
        // Zero piece length is invalid; build a torrent with piece_length 0 by
        // manually encoding the info dict.
        let zero = manual_single_file_torrent(0, 1, &[0u8; 20]);
        let err = parse_torrent(&zero).unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("piece_length"));

        // Piece length over the maximum.
        let over = manual_single_file_torrent(MAX_PIECE_LENGTH + 1, 1, &[0u8; 20]);
        let err = parse_torrent(&over).unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn rejects_mismatched_piece_count() {
        // Provide 2 hashes but the total length implies 1 piece.
        let too_many = manual_single_file_torrent(8, 8, &[0u8; 40]);
        let err = parse_torrent(&too_many).unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("piece count"));

        // Provide 0 hashes: pieces string length 0 is a multiple of 20, so the
        // piece-count mismatch (0 vs expected 1) is reported.
        let none = manual_single_file_torrent(8, 8, &[]);
        let err = parse_torrent(&none).unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("piece count"));
    }

    #[test]
    fn rejects_pieces_string_not_multiple_of_sha1_length() {
        let torrent = manual_single_file_torrent(8, 8, &[0u8; 21]);
        let error = parse_torrent(&torrent).unwrap_err();
        assert!(matches!(&error, CoreError::MalformedTorrent(_)));
        assert!(error.to_string().contains("not multiple of 20"));
    }

    #[test]
    fn rejects_too_many_pieces() {
        // Declare a piece count over the maximum by using a small piece length
        // and a huge total length, but provide a matching pieces blob.
        let pieces_blob = vec![0u8; (MAX_TORRENT_PIECES + 1) * 20];
        let total: u64 = (MAX_TORRENT_PIECES as u64 + 1) * 16;
        let over = manual_single_file_torrent(16, total, &pieces_blob);
        let err = parse_torrent(&over).unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn parse_torrent_root_rejects_file_count_over_limit_directly() {
        let info = Value::Dict(vec![
            (b"name".to_vec(), Value::Str(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16)),
            (b"pieces".to_vec(), Value::Str(vec![0u8; 20])),
            (
                b"files".to_vec(),
                Value::List(vec![Value::Int(0); MAX_TORRENT_FILES + 1]),
            ),
        ]);
        let root = vec![(b"info".to_vec(), info)];
        let error = parse_torrent_root(&root, b"direct-file-count-test").unwrap_err();
        assert!(matches!(&error, CoreError::MalformedTorrent(_)));
        assert!(error.to_string().contains("file count"));
        assert!(error.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn public_decode_node_budget_precedes_file_count_for_encoded_corpus() {
        // A fully encoded MAX_TORRENT_FILES + 1 corpus necessarily exceeds the
        // smaller public bencode node budget before metainfo construction.
        let mut out = b"d4:infod5:filesl".to_vec();
        for i in 0..(MAX_TORRENT_FILES + 1) {
            out.extend_from_slice(b"d6:lengthi1e4:pathl");
            let name = format!("f{i}");
            write_str(&mut out, name.as_bytes());
            out.extend_from_slice(b"ee"); // close path list, close file dict
        }
        out.extend_from_slice(b"e4:name1:d12:piece lengthi16e6:pieces20:");
        out.extend_from_slice(&[0u8; 20]);
        out.extend_from_slice(b"ee");
        let error = bencode::decode(&out).unwrap_err();
        assert!(matches!(&error, CoreError::Bencode(_)));
        assert!(error.to_string().contains("node count"));
    }

    #[test]
    fn rejects_total_length_overflow() {
        // Three files whose lengths sum past u64 (2 * i64::MAX fits in u64,
        // 3 * i64::MAX overflows).
        let mut out = b"d4:infod5:filesl".to_vec();
        for name in [b"aa".as_slice(), b"bb".as_slice(), b"cc".as_slice()] {
            out.extend_from_slice(b"d6:lengthi9223372036854775807e4:pathl");
            write_str(&mut out, name);
            out.extend_from_slice(b"ee");
        }
        out.extend_from_slice(b"e4:name1:d12:piece lengthi16e6:pieces60:");
        out.extend_from_slice(&[0u8; 60]);
        out.extend_from_slice(b"ee");
        let err = parse_torrent(&out).unwrap_err();
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("exceeds u64"));
    }

    #[test]
    fn rejects_empty_torrent_with_zero_or_one_piece_hash() {
        let one_hash = [0u8; 20];
        for pieces in [&[][..], one_hash.as_slice()] {
            let empty = manual_single_file_torrent(16, 0, pieces);
            let err = parse_torrent(&empty).unwrap_err();
            assert!(matches!(&err, CoreError::MalformedTorrent(_)));
            assert!(err.to_string().contains("total length"));
            assert!(err.to_string().contains("greater than zero"));
        }
    }

    #[test]
    fn metadata_at_byte_limit_parses() {
        let bytes = torrent_padded_to_size(MAX_TORRENT_METADATA_BYTES);
        assert_eq!(bytes.len(), MAX_TORRENT_METADATA_BYTES);
        assert!(parse_torrent(&bytes).is_ok());
    }

    #[test]
    fn metadata_one_byte_over_limit_rejected() {
        let mut bytes = torrent_padded_to_size(MAX_TORRENT_METADATA_BYTES);
        bytes.push(b'X');
        assert_eq!(bytes.len(), MAX_TORRENT_METADATA_BYTES + 1);
        let err = parse_torrent(&bytes).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn durable_piece_hash_decode_rejects_wrong_lengths() {
        // 20-byte hash decodes.
        let good = serde_json::json!([hex::encode([0u8; 20])]);
        let s = serde_json::to_string(&good).unwrap();
        let mut de = serde_json::Deserializer::from_str(&s);
        let v: Vec<[u8; 20]> = hex_piece_hashes::deserialize(&mut de).unwrap();
        assert_eq!(v.len(), 1);

        // 0, 19, and 21-byte hashes are rejected.
        for len in [0usize, 19usize, 21usize] {
            let bad = serde_json::json!([hex::encode(vec![0u8; len])]);
            let s = serde_json::to_string(&bad).unwrap();
            let mut de = serde_json::Deserializer::from_str(&s);
            let result: std::result::Result<Vec<[u8; 20]>, _> =
                hex_piece_hashes::deserialize(&mut de);
            assert!(result.is_err(), "{len}-byte hash must be rejected");
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("20 bytes") && msg.contains("length"),
                "expected length message, got: {msg}"
            );
        }
    }

    #[test]
    fn durable_piece_hash_decode_includes_index_context() {
        // Two hashes, the second malformed: error mentions index 1.
        let bad = serde_json::json!([hex::encode([0u8; 20]), hex::encode([0u8; 19])]);
        let s = serde_json::to_string(&bad).unwrap();
        let mut de = serde_json::Deserializer::from_str(&s);
        let result: std::result::Result<Vec<[u8; 20]>, _> = hex_piece_hashes::deserialize(&mut de);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("piece hash 1"), "expected index, got: {err}");
    }

    #[test]
    fn durable_piece_hash_rejects_oversized_encoding_before_hex_decode() {
        let oversized = "00".repeat(4096);
        let input = serde_json::to_string(&vec![oversized]).unwrap();
        let mut deserializer = serde_json::Deserializer::from_str(&input);
        let error = hex_piece_hashes::deserialize(&mut deserializer)
            .unwrap_err()
            .to_string();
        assert!(error.contains("piece hash 0"));
        assert!(error.contains("length 4096"));
        assert!(!error.contains("00000000"));
    }

    #[test]
    fn durable_piece_hash_rejects_odd_length_and_non_hex_separately() {
        for (encoded, expected) in [
            ("0".repeat(39), "odd encoded length"),
            ("g".repeat(40), "not valid hex"),
        ] {
            let input = serde_json::to_string(&vec![encoded]).unwrap();
            let mut deserializer = serde_json::Deserializer::from_str(&input);
            let error = hex_piece_hashes::deserialize(&mut deserializer)
                .unwrap_err()
                .to_string();
            assert!(error.contains("piece hash 0"));
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn durable_piece_hash_count_accepts_limit_and_rejects_one_more() {
        use serde::de::value::{Error as ValueError, SeqDeserializer};

        const HASH: &str = "0000000000000000000000000000000000000000";
        let exact =
            SeqDeserializer::<_, ValueError>::new(std::iter::repeat_n(HASH, MAX_TORRENT_PIECES));
        let decoded = hex_piece_hashes::deserialize(exact).unwrap();
        assert_eq!(decoded.len(), MAX_TORRENT_PIECES);

        let one_over = SeqDeserializer::<_, ValueError>::new(std::iter::repeat_n(
            HASH,
            MAX_TORRENT_PIECES + 1,
        ));
        let error = hex_piece_hashes::deserialize(one_over)
            .unwrap_err()
            .to_string();
        assert!(error.contains("piece hash count"));
        assert!(error.contains("exceeds maximum"));
    }

    #[test]
    fn read_torrent_file_rejects_oversized_file() {
        let dir = std::env::temp_dir().join(format!(
            "swarmotter-meta-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("big.torrent");
        // A sparse file is sufficient because the initial metadata check must
        // reject it before allocating or reading the declared length.
        let file = std::fs::File::create(&path).unwrap();
        file.set_len((MAX_TORRENT_METADATA_BYTES + 1) as u64)
            .unwrap();
        let err = read_torrent_file(&path).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
        std::fs::remove_dir_all(&dir).ok();

        // A valid small file reads and parses.
        let content = b"hello swarmotter world data payload here";
        let bytes = build_single_file_torrent("file.bin", content, 16, None, false);
        let dir2 = std::env::temp_dir().join(format!(
            "swarmotter-meta2-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir2).unwrap();
        let path2 = dir2.join("ok.torrent");
        std::fs::write(&path2, &bytes).unwrap();
        let read = read_torrent_file(&path2).unwrap();
        assert!(parse_torrent(&read).is_ok());
        std::fs::remove_dir_all(&dir2).ok();
    }

    #[test]
    fn read_torrent_file_accepts_exact_metadata_limit() {
        let dir = std::env::temp_dir().join(format!(
            "swarmotter-meta-exact-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("exact.torrent");
        let bytes = torrent_padded_to_size(MAX_TORRENT_METADATA_BYTES);
        std::fs::write(&path, &bytes).unwrap();

        let read = read_torrent_file(&path).unwrap();
        assert_eq!(read.len(), MAX_TORRENT_METADATA_BYTES);
        assert!(parse_torrent(&read).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn bounded_reader_stops_after_limit_plus_one_byte() {
        struct CountingReader {
            remaining: usize,
            bytes_read: usize,
        }

        impl std::io::Read for CountingReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let count = buf.len().min(self.remaining);
                buf[..count].fill(b'x');
                self.remaining -= count;
                self.bytes_read += count;
                Ok(count)
            }
        }

        let mut reader = CountingReader {
            remaining: MAX_TORRENT_METADATA_BYTES + 4096,
            bytes_read: 0,
        };
        let err = read_bounded_torrent_bytes(&mut reader, 0).unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"));
        assert_eq!(reader.bytes_read, MAX_TORRENT_METADATA_BYTES + 1);
    }

    #[test]
    fn completed_read_requires_matching_initial_final_and_read_lengths() {
        assert!(validate_completed_torrent_read(10, 10, 10).is_ok());
        for (initial, final_len, read) in [(10, 11, 11), (10, 10, 9), (10, 9, 10)] {
            let err = validate_completed_torrent_read(initial, final_len, read).unwrap_err();
            assert!(err.to_string().contains("changed during read"));
        }
    }

    #[test]
    fn piece_length_u32_helpers_succeed_for_valid_torrent() {
        let bytes = build_single_file_torrent("f", b"abcdef0123456789", 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        assert_eq!(meta.piece_length_u32().unwrap(), 8);
        assert_eq!(meta.piece_length_for_index_u32(0).unwrap(), 8);
    }

    #[test]
    fn piece_length_u32_helpers_reject_limit_violation_before_narrowing() {
        let bytes = build_single_file_torrent("f", b"abcdef0123456789", 8, None, false);
        let mut meta = parse_torrent(&bytes).unwrap();
        meta.piece_length = MAX_PIECE_LENGTH + 1;

        for error in [
            meta.piece_length_u32().unwrap_err(),
            meta.piece_length_for_index_u32(0).unwrap_err(),
        ] {
            assert!(matches!(&error, CoreError::MalformedTorrent(_)));
            assert!(error.to_string().contains("exceeds maximum"));
        }
    }

    fn manual_single_file_torrent(piece_length: u64, total_length: u64, pieces: &[u8]) -> Vec<u8> {
        let mut out = b"d4:infod".to_vec();
        write_str(&mut out, b"length");
        write_int(&mut out, total_length);
        write_str(&mut out, b"name");
        write_str(&mut out, b"f");
        write_str(&mut out, b"piece length");
        write_int(&mut out, piece_length);
        write_str(&mut out, b"pieces");
        write_str(&mut out, pieces);
        out.extend_from_slice(b"ee");
        out
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
