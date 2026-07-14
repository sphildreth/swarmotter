// SPDX-License-Identifier: Apache-2.0

//! BEP 52 v2 payload layout and SHA-256 Merkle verification.
//!
//! The v2 peer protocol does **not** use the v1 contiguous torrent byte
//! address space.  Each non-empty file begins on a logical piece boundary,
//! and a short final piece leaves an address-space gap before the next file.
//! This module keeps that mapping explicit so callers cannot accidentally use
//! v1 cross-file piece arithmetic for v2 data.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use crate::error::{CoreError, Result};
use crate::hash::V2InfoHash;
use crate::meta::{MetaFile, TorrentMeta, MAX_TORRENT_PIECES, V2_BLOCK_LENGTH};

/// One logical v2 peer-protocol piece.
///
/// A v2 piece is always wholly contained in one file.  `file_index` indexes
/// [`TorrentMeta::files`], allowing storage to write it without crossing an
/// alignment gap or an adjacent file boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2Piece {
    /// Piece index used by BEP 52 `bitfield`, `have`, `request`, and `piece`
    /// messages.
    pub index: usize,
    /// Index of the payload file that owns this piece.
    pub file_index: usize,
    /// Byte offset within that file.
    pub offset: u64,
    /// Actual byte length. The final piece in a file may be shorter than the
    /// torrent's logical piece length.
    pub length: u64,
    /// Full owning-file length. A file that fits in one logical piece uses its
    /// file-tree root, whose padding width is based on the file itself rather
    /// than the torrent's declared piece length.
    pub file_length: u64,
    /// File Merkle root named by a BEP 52 hash request.
    pub pieces_root: V2InfoHash,
    /// The expected SHA-256 subtree root for this logical piece.
    pub expected_hash: V2InfoHash,
}

/// File-aligned v2 piece mapping and expected piece-layer hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2PieceLayout {
    piece_length: u64,
    pieces: Vec<V2Piece>,
}

impl V2PieceLayout {
    /// Build an executable v2 piece layout from verified complete metainfo.
    ///
    /// A BEP 9 `info` dictionary alone deliberately cannot construct this
    /// layout when a file needs a piece layer: the top-level layer hashes are
    /// absent and must first be fetched and verified through the BEP 52 hash
    /// exchange.  Complete `.torrent` metainfo has already verified those
    /// hashes during [`TorrentMeta::validate`].
    pub fn from_meta(meta: &TorrentMeta) -> Result<Self> {
        let v2 = meta.v2.as_ref().ok_or_else(|| {
            CoreError::UnsupportedTorrentFeature("torrent has no BEP 52 file-tree metadata".into())
        })?;
        if meta.identity.v2_info_hash().is_none() {
            return Err(CoreError::UnsupportedTorrentFeature(
                "torrent has no BEP 52 identity".into(),
            ));
        }
        if !v2.piece_layers_verified {
            return Err(CoreError::UnsupportedTorrentFeature(
                "BEP 52 payload transfer requires verified top-level piece layers".into(),
            ));
        }
        validate_storage_file_mapping(meta, &v2.files)?;

        let mut layers = HashMap::with_capacity(v2.piece_layers.len());
        for layer in &v2.piece_layers {
            if layers
                .insert(layer.pieces_root, layer.hashes.as_slice())
                .is_some()
            {
                return Err(CoreError::MalformedTorrent(
                    "duplicate BEP 52 piece-layer root".into(),
                ));
            }
        }

        let mut pieces = Vec::new();
        for (file_index, file) in v2.files.iter().enumerate() {
            if file.length == 0 {
                // Zero-length files occupy no v2 peer-protocol address space.
                continue;
            }
            let pieces_root = file.pieces_root.ok_or_else(|| {
                CoreError::MalformedTorrent("non-empty BEP 52 file is missing pieces root".into())
            })?;
            let count_u64 = file.length.div_ceil(meta.piece_length);
            let count = usize::try_from(count_u64).map_err(|_| {
                CoreError::MalformedTorrent(
                    "BEP 52 file piece count exceeds platform limits".into(),
                )
            })?;
            let new_count = pieces
                .len()
                .checked_add(count)
                .ok_or_else(|| CoreError::MalformedTorrent("BEP 52 piece count overflow".into()))?;
            if new_count > MAX_TORRENT_PIECES {
                return Err(CoreError::MalformedTorrent(format!(
                    "BEP 52 piece count {new_count} exceeds maximum {MAX_TORRENT_PIECES}"
                )));
            }

            let layer = if file.length > meta.piece_length {
                Some(layers.get(&pieces_root).ok_or_else(|| {
                    CoreError::MalformedTorrent(
                        "BEP 52 file larger than piece length is missing a piece layer".into(),
                    )
                })?)
            } else {
                None
            };
            if let Some(layer) = layer {
                if layer.len() != count {
                    return Err(CoreError::MalformedTorrent(format!(
                        "BEP 52 piece layer has {} hashes; expected {count}",
                        layer.len()
                    )));
                }
            }

            for within_file in 0..count {
                let offset = u64::try_from(within_file)
                    .map_err(|_| CoreError::MalformedTorrent("piece index overflow".into()))?
                    .checked_mul(meta.piece_length)
                    .ok_or_else(|| CoreError::MalformedTorrent("piece offset overflow".into()))?;
                let length = file.length.saturating_sub(offset).min(meta.piece_length);
                let expected_hash = layer.map_or(pieces_root, |hashes| hashes[within_file]);
                pieces.push(V2Piece {
                    index: pieces.len(),
                    file_index,
                    offset,
                    length,
                    file_length: file.length,
                    pieces_root,
                    expected_hash,
                });
            }
        }

        if pieces.is_empty() {
            return Err(CoreError::MalformedTorrent(
                "BEP 52 torrent has no non-empty payload pieces".into(),
            ));
        }

        Ok(Self {
            piece_length: meta.piece_length,
            pieces,
        })
    }

    /// Logical peer-protocol piece length declared by the torrent.
    pub const fn piece_length(&self) -> u64 {
        self.piece_length
    }

    /// Number of pieces in the v2 file-aligned address space.
    pub fn piece_count(&self) -> usize {
        self.pieces.len()
    }

    /// Return the file mapping and expected hash for a logical piece.
    pub fn piece(&self, index: usize) -> Option<&V2Piece> {
        self.pieces.get(index)
    }

    /// Verify an assembled peer piece against its BEP 52 expected subtree
    /// root. The input must have exactly the file-local piece length.
    pub fn verify_piece(&self, index: usize, data: &[u8]) -> bool {
        let Some(piece) = self.piece(index) else {
            return false;
        };
        let Ok(actual_length) = u64::try_from(data.len()) else {
            return false;
        };
        if actual_length != piece.length {
            return false;
        }
        let actual = if piece.file_length <= self.piece_length {
            v2_file_root(data)
        } else {
            v2_piece_root(data, self.piece_length)
        };
        actual.is_ok_and(|root| root == piece.expected_hash)
    }
}

/// Calculate the BEP 52 SHA-256 subtree root for one logical piece.
///
/// Leaf hashes cover 16 KiB blocks. A short final block is hashed at its real
/// length; all remaining leaves needed to reach `piece_length` are the all-zero
/// hash value, not the hash of zero-filled data. This is intentionally public
/// so hash-request/proof handling can use the same primitive.
pub fn v2_piece_root(data: &[u8], piece_length: u64) -> Result<V2InfoHash> {
    if piece_length < V2_BLOCK_LENGTH || !piece_length.is_power_of_two() {
        return Err(CoreError::MalformedTorrent(format!(
            "BEP 52 piece length must be a power of two at least {V2_BLOCK_LENGTH}"
        )));
    }
    let data_length = u64::try_from(data.len())
        .map_err(|_| CoreError::MalformedTorrent("BEP 52 piece data is too large".into()))?;
    if data_length == 0 || data_length > piece_length {
        return Err(CoreError::MalformedTorrent(format!(
            "BEP 52 piece data length {data_length} is outside 1..={piece_length}"
        )));
    }

    let leaves_per_piece = usize::try_from(piece_length / V2_BLOCK_LENGTH).map_err(|_| {
        CoreError::MalformedTorrent("BEP 52 piece leaf count exceeds platform limits".into())
    })?;
    let mut level = data
        .chunks(V2_BLOCK_LENGTH as usize)
        .map(v2_leaf_hash)
        .collect::<Vec<_>>();
    level.resize(leaves_per_piece, V2InfoHash::ZERO);

    while level.len() > 1 {
        level = level
            .chunks_exact(2)
            .map(|pair| v2_hash_pair(pair[0], pair[1]))
            .collect();
    }
    Ok(level[0])
}

/// Calculate the BEP 52 file-tree root for one non-empty file.
///
/// Unlike [`v2_piece_root`], this pads only to the next power-of-two number
/// of 16 KiB leaves actually needed by the file. In particular, a small file
/// does not acquire artificial zero subtrees merely because a torrent chose a
/// larger logical peer-protocol piece length.
pub fn v2_file_root(data: &[u8]) -> Result<V2InfoHash> {
    if data.is_empty() {
        return Err(CoreError::MalformedTorrent(
            "BEP 52 file root requires non-empty data".into(),
        ));
    }
    let mut level = data
        .chunks(V2_BLOCK_LENGTH as usize)
        .map(v2_leaf_hash)
        .collect::<Vec<_>>();
    let width = level.len().checked_next_power_of_two().ok_or_else(|| {
        CoreError::MalformedTorrent("BEP 52 file leaf count exceeds platform limits".into())
    })?;
    level.resize(width, V2InfoHash::ZERO);
    while level.len() > 1 {
        level = level
            .chunks_exact(2)
            .map(|pair| v2_hash_pair(pair[0], pair[1]))
            .collect();
    }
    Ok(level[0])
}

/// SHA-256 a single BEP 52 16 KiB-or-shorter leaf block.
pub fn v2_leaf_hash(data: &[u8]) -> V2InfoHash {
    let digest = Sha256::digest(data);
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    V2InfoHash::from_bytes(output)
}

/// SHA-256 the concatenation of two BEP 52 child hashes.
pub fn v2_hash_pair(left: V2InfoHash, right: V2InfoHash) -> V2InfoHash {
    let mut hasher = Sha256::new();
    hasher.update(left.as_bytes());
    hasher.update(right.as_bytes());
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    V2InfoHash::from_bytes(output)
}

fn validate_storage_file_mapping(meta: &TorrentMeta, v2_files: &[MetaFile]) -> Result<()> {
    if meta.files.len() != v2_files.len() {
        return Err(CoreError::MalformedTorrent(
            "BEP 52 file tree cannot be mapped to payload storage files".into(),
        ));
    }
    for (index, (storage_file, v2_file)) in meta.files.iter().zip(v2_files).enumerate() {
        if storage_file.length != v2_file.length {
            return Err(CoreError::MalformedTorrent(format!(
                "BEP 52 file {index} length differs from payload storage layout"
            )));
        }
        let exact = storage_file.path == v2_file.path;
        // Hybrid v1 multi-file metainfo commonly has a v1 top-level name
        // component that the rootless v2 file tree omits. `TorrentMeta`
        // validation has already established order/content compatibility; this
        // form only maps that compatible representation to the same storage
        // file index.
        let relative_hybrid = meta.is_multi_file
            && storage_file.path.len() == v2_file.path.len().saturating_add(1)
            && storage_file.path.get(1..) == Some(v2_file.path.as_slice());
        if !exact && !relative_hybrid {
            return Err(CoreError::MalformedTorrent(format!(
                "BEP 52 file {index} path differs from payload storage layout"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::{InfoHash, TorrentIdentity};
    use crate::meta::{MetaFile, V2PieceLayer, V2TorrentMeta};

    fn piece_root_reference(data: &[u8], piece_length: usize) -> V2InfoHash {
        let mut level = data
            .chunks(V2_BLOCK_LENGTH as usize)
            .map(v2_leaf_hash)
            .collect::<Vec<_>>();
        level.resize(piece_length / V2_BLOCK_LENGTH as usize, V2InfoHash::ZERO);
        while level.len() > 1 {
            level = level
                .chunks_exact(2)
                .map(|pair| v2_hash_pair(pair[0], pair[1]))
                .collect();
        }
        level[0]
    }

    fn meta_for_files(files: Vec<(Vec<String>, Vec<u8>)>, piece_length: u64) -> TorrentMeta {
        let v2_files = files
            .iter()
            .map(|(path, data)| MetaFile {
                path: path.clone(),
                length: data.len() as u64,
                pieces_root: (!data.is_empty()).then(|| v2_file_root(data).expect("fixture")),
            })
            .collect::<Vec<_>>();
        let layers = files
            .iter()
            .zip(&v2_files)
            .filter(|((_, data), _)| data.len() as u64 > piece_length)
            .map(|((_, data), file)| V2PieceLayer {
                pieces_root: file.pieces_root.expect("non-empty file root"),
                hashes: data
                    .chunks(piece_length as usize)
                    .map(|piece| piece_root_reference(piece, piece_length as usize))
                    .collect(),
            })
            .collect::<Vec<_>>();
        let total_length = files.iter().map(|(_, data)| data.len() as u64).sum();
        let identity = TorrentIdentity::v2(V2InfoHash::from_bytes([0xA5; 32]));
        TorrentMeta {
            info_hash: InfoHash::ZERO,
            identity,
            name: "v2-layout".into(),
            piece_length,
            pieces: Vec::new(),
            files: v2_files.clone(),
            total_length,
            private: false,
            announce: None,
            announce_list: Vec::new(),
            webseeds: Vec::new(),
            comment: None,
            created_by: None,
            creation_date: None,
            is_multi_file: v2_files.len() != 1,
            v2: Some(V2TorrentMeta {
                meta_version: 2,
                files: v2_files,
                piece_layers: layers,
                piece_layers_verified: true,
            }),
            raw_info: None,
        }
    }

    #[test]
    fn file_aligned_layout_skips_cross_file_v1_boundaries() {
        let first = vec![0x11; V2_BLOCK_LENGTH as usize + 7];
        let second = vec![0x22; 9];
        let meta = meta_for_files(
            vec![
                (vec!["first.bin".into()], first.clone()),
                (vec!["second.bin".into()], second.clone()),
            ],
            V2_BLOCK_LENGTH,
        );
        let layout = V2PieceLayout::from_meta(&meta).unwrap();

        assert_eq!(layout.piece_count(), 3);
        assert_eq!(layout.piece(0).unwrap().file_index, 0);
        assert_eq!(layout.piece(0).unwrap().offset, 0);
        assert_eq!(layout.piece(0).unwrap().length, V2_BLOCK_LENGTH);
        assert_eq!(layout.piece(1).unwrap().file_index, 0);
        assert_eq!(layout.piece(1).unwrap().offset, V2_BLOCK_LENGTH);
        assert_eq!(layout.piece(1).unwrap().length, 7);
        assert_eq!(layout.piece(2).unwrap().file_index, 1);
        assert_eq!(layout.piece(2).unwrap().offset, 0);
        assert_eq!(layout.piece(2).unwrap().length, 9);
        assert!(layout.verify_piece(0, &first[..V2_BLOCK_LENGTH as usize]));
        assert!(layout.verify_piece(1, &first[V2_BLOCK_LENGTH as usize..]));
        assert!(layout.verify_piece(2, &second));
    }

    #[test]
    fn piece_root_uses_zero_hash_padding_not_zero_filled_data() {
        let piece_length = 2 * V2_BLOCK_LENGTH;
        let data = vec![0x5C; V2_BLOCK_LENGTH as usize + 3];
        let actual = v2_piece_root(&data, piece_length).unwrap();
        let expected = piece_root_reference(&data, piece_length as usize);
        assert_eq!(actual, expected);

        let zero_filled = {
            let mut bytes = data.clone();
            bytes.resize(piece_length as usize, 0);
            piece_root_reference(&bytes, piece_length as usize)
        };
        assert_ne!(actual, zero_filled);
    }

    #[test]
    fn single_piece_file_uses_its_own_merkle_width_not_piece_length_padding() {
        let piece_length = 4 * V2_BLOCK_LENGTH;
        let data = vec![0xA1; V2_BLOCK_LENGTH as usize + 1];
        let meta = meta_for_files(vec![(vec!["small.bin".into()], data.clone())], piece_length);
        let layout = V2PieceLayout::from_meta(&meta).unwrap();
        assert_eq!(layout.piece_count(), 1);
        assert!(layout.verify_piece(0, &data));
        assert_ne!(
            v2_file_root(&data).unwrap(),
            v2_piece_root(&data, piece_length).unwrap(),
            "a small file root must not be padded to the logical piece width"
        );
    }

    #[test]
    fn unverified_piece_layers_cannot_start_v2_payload_transfer() {
        let data = vec![0x31; V2_BLOCK_LENGTH as usize + 1];
        let mut meta = meta_for_files(vec![(vec!["payload.bin".into()], data)], V2_BLOCK_LENGTH);
        meta.v2.as_mut().unwrap().piece_layers_verified = false;
        meta.v2.as_mut().unwrap().piece_layers.clear();
        let error = V2PieceLayout::from_meta(&meta).unwrap_err();
        assert!(matches!(error, CoreError::UnsupportedTorrentFeature(_)));
    }

    #[tokio::test]
    async fn storage_rechecks_file_aligned_v2_pieces_without_cross_file_writes() {
        use crate::storage::StorageIo;

        let first = vec![0x71; V2_BLOCK_LENGTH as usize + 3];
        let second = vec![0x72; V2_BLOCK_LENGTH as usize + 5];
        let meta = meta_for_files(
            vec![
                (vec!["first.bin".into()], first.clone()),
                (vec!["second.bin".into()], second.clone()),
            ],
            V2_BLOCK_LENGTH,
        );
        let layout = V2PieceLayout::from_meta(&meta).unwrap();
        assert_eq!(layout.piece_count(), 4);
        let root = std::env::temp_dir().join(format!(
            "swarmotter-v2-storage-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let storage = StorageIo::new(meta.clone(), root.clone())
            .with_partial_file_suffix(Some(".part".into()));

        storage
            .write_v2_piece(&layout, 0, &first[..V2_BLOCK_LENGTH as usize])
            .await
            .unwrap();
        storage
            .write_v2_piece(&layout, 1, &first[V2_BLOCK_LENGTH as usize..])
            .await
            .unwrap();
        storage
            .write_v2_piece(&layout, 2, &second[..V2_BLOCK_LENGTH as usize])
            .await
            .unwrap();
        storage
            .write_v2_piece(&layout, 3, &second[V2_BLOCK_LENGTH as usize..])
            .await
            .unwrap();

        assert_eq!(
            storage.read_v2_piece(&layout, 1).await.unwrap(),
            vec![0x71; 3]
        );
        assert_eq!(
            storage.read_v2_piece(&layout, 2).await.unwrap(),
            second[..V2_BLOCK_LENGTH as usize]
        );
        let rechecked = storage.recheck_v2(&layout).await.unwrap();
        assert_eq!(rechecked.count(layout.piece_count()), layout.piece_count());
        assert!(storage.file_path(0).unwrap().ends_with("first.bin.part"));

        let finalized = storage.finalize_partial_file_suffix().await.unwrap();
        assert_eq!(finalized.file_path(0).unwrap(), root.join("first.bin"));
        assert!(finalized.recheck_v2(&layout).await.unwrap().has(0));
        assert!(!root.join("first.bin.part").exists());
        std::fs::remove_dir_all(root).ok();
    }
}
