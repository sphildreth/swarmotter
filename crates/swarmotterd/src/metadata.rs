// SPDX-License-Identifier: Apache-2.0

//! BEP 9 magnet metadata fetch.
//!
//! For magnet links (which carry only an info hash, not the full metadata),
//! the engine fetches the torrent's `info` dictionary from a peer using the
//! `ut_metadata` extension (BEP 9) over the BEP 10 extension protocol. The
//! raw `info` bytes are assembled from metadata pieces, verified by SHA-1
//! against the info hash, then converted into a normal [`TorrentMeta`] so the
//! download proceeds exactly as for a `.torrent` file.
//!
//! All peer connections go through the `NetworkBinder`; no socket is created
//! directly. Private torrents still fetch metadata over peer connections
//! (PEX/DHT are disabled for private torrents, not metadata exchange over an
//! existing peer). See `design/requirements.md` and ADR-0013.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::extensions::{self, MetadataMsgType};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, PeerAddr};

const MAX_METADATA_SIZE: usize = 16 * 1024 * 1024;

/// Fetch the torrent metadata (`info` dict) from a peer via `ut_metadata`.
/// Returns the assembled, info-hash-verified `info` bytes.
///
/// The caller supplies the peer to fetch from (typically discovered via a
/// tracker, PEX, or DHT). The connection goes through the binder. On success
/// the raw `info` bytes can be turned into a `TorrentMeta` via
/// [`build_meta_from_info`].
pub async fn fetch_metadata(
    binder: &dyn NetworkBinder,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    peer: PeerAddr,
) -> Result<Vec<u8>> {
    if !binder.traffic_allowed() {
        return Err(CoreError::NetworkBlocked(
            "torrent data plane blocked; cannot fetch metadata".into(),
        ));
    }
    let stream = binder.connect_peer(peer.socket_addr()).await?;
    let (read_half, mut write_half) = tokio::io::split(stream);

    // Handshake advertising extension support.
    let hs = Handshake {
        info_hash,
        peer_id,
        reserved: extensions::EXTENSION_RESERVED,
    };
    peer::write_handshake(&mut write_half, &hs).await?;
    let mut reader = swarmotter_core::peer::PeerReader::new(read_half);
    let their_hs = timeout(Duration::from_secs(15), reader.read_handshake()).await??;
    if their_hs.info_hash != info_hash {
        return Err(CoreError::Internal(
            "metadata peer info hash mismatch".into(),
        ));
    }
    if !their_hs.supports_extensions() {
        return Err(CoreError::Internal(
            "metadata peer does not support BEP 10 extensions".into(),
        ));
    }

    // Send an empty bitfield and our extension handshake advertising
    // ut_metadata.
    let bf = Bitfield::new(0);
    peer::write_message(&mut write_half, &bf.encode_message()).await?;
    let local_metadata_id: u8 = 3u8;
    let ext_payload = extensions::encode_extension_handshake(
        &[(extensions::UT_METADATA_NAME, local_metadata_id)],
        "SwarmOtter/0.1",
        None,
    );
    peer::write_message(
        &mut write_half,
        &Message::Extended {
            id: extensions::EXTENSION_HANDSHAKE_ID,
            payload: ext_payload,
        },
    )
    .await?;
    write_half.flush().await.ok();

    // Wait for the peer's extension handshake to learn the remote ut_metadata
    // id and the total metadata size.
    let mut remote_metadata_id: Option<u8> = None;
    let mut metadata_size: Option<u64> = None;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
            Ok(Ok(Some(m))) => m,
            _ => return Err(CoreError::Internal("metadata peer timed out".into())),
        };
        if let Message::Extended { id, payload } = msg {
            if id == extensions::EXTENSION_HANDSHAKE_ID {
                if let Ok(hs) = extensions::parse_extension_handshake(&payload) {
                    remote_metadata_id = hs.id_for(extensions::UT_METADATA_NAME);
                    metadata_size = hs.metadata_size;
                }
                break;
            }
        }
    }
    let remote_id = remote_metadata_id
        .ok_or_else(|| CoreError::Internal("metadata peer does not support ut_metadata".into()))?;
    let total_u64 = metadata_size
        .ok_or_else(|| CoreError::Internal("metadata peer did not report metadata_size".into()))?;
    let total = usize::try_from(total_u64).map_err(|_| {
        CoreError::Internal("metadata peer reported size too large for this platform".into())
    })?;
    if total == 0 || total > MAX_METADATA_SIZE {
        return Err(CoreError::Internal(format!(
            "metadata size {total} exceeds maximum {MAX_METADATA_SIZE}"
        )));
    }

    // Request each metadata piece and assemble.
    let pieces = extensions::metadata_pieces(total).max(1);
    let mut assembled: Vec<u8> = Vec::with_capacity(total);
    for piece in 0..pieces {
        let req = extensions::encode_metadata_request(piece as u32);
        peer::write_message(
            &mut write_half,
            &Message::Extended {
                id: remote_id,
                payload: req,
            },
        )
        .await?;
        write_half.flush().await.ok();

        // Wait for the data response for this piece.
        let piece_deadline = Instant::now() + Duration::from_secs(30);
        let mut got_piece = false;
        while Instant::now() < piece_deadline {
            let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
                Ok(Ok(Some(m))) => m,
                _ => break,
            };
            if let Message::Extended { id, payload } = msg {
                if id != local_metadata_id {
                    continue;
                }
                match extensions::parse_metadata_message(&payload) {
                    Ok(m) if m.msg_type == MetadataMsgType::Data && m.piece as usize == piece => {
                        let room = total - assembled.len();
                        if m.data.len() > room {
                            return Err(CoreError::Internal(
                                "metadata piece data exceeds announced total size".into(),
                            ));
                        }
                        assembled.extend_from_slice(&m.data);
                        if let Some(peer_total) = m.total_size {
                            if peer_total != total_u64 {
                                return Err(CoreError::Internal(
                                    "metadata total_size mismatch".into(),
                                ));
                            }
                        }
                        got_piece = true;
                        break;
                    }
                    Ok(m) if m.msg_type == MetadataMsgType::Reject => {
                        return Err(CoreError::Internal(format!(
                            "metadata piece {piece} rejected by peer"
                        )));
                    }
                    _ => continue,
                }
            }
        }
        if !got_piece {
            return Err(CoreError::Internal(format!(
                "metadata piece {piece} not received"
            )));
        }
    }

    // Truncate to the reported total size (the last piece may be padded).
    assembled.truncate(total);

    // Verify the assembled info dict hashes to the info hash.
    let computed = swarmotter_core::hash::InfoHash::from_info_bencoded(&assembled);
    if computed != info_hash {
        return Err(CoreError::Internal(
            "fetched metadata info hash mismatch; rejecting".into(),
        ));
    }
    Ok(assembled)
}

/// Build a `TorrentMeta` from raw `info` dict bytes plus the magnet's name and
/// trackers. Constructs a full `.torrent`-style bencoded document (announce +
/// info) and parses it, so all the normal metadata validation applies.
pub fn build_meta_from_info(
    info_bytes: &[u8],
    name: &str,
    trackers: &[String],
) -> Result<TorrentMeta> {
    let mut doc = Vec::new();
    doc.push(b'd');
    if let Some(primary) = trackers.first() {
        write_str(&mut doc, b"announce");
        write_str(&mut doc, primary.as_bytes());
    }
    if trackers.len() > 1 {
        write_str(&mut doc, b"announce-list");
        doc.push(b'l');
        // Group trackers into a single tier.
        doc.push(b'l');
        for t in &trackers[1..] {
            write_str(&mut doc, t.as_bytes());
        }
        doc.push(b'e');
        doc.push(b'e');
    }
    write_str(&mut doc, b"info");
    doc.extend_from_slice(info_bytes);
    doc.push(b'e');
    let meta = swarmotter_core::meta::parse_torrent(&doc)?;
    let _ = name; // name is derived from the info dict
    Ok(meta)
}

fn write_str(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(format!("{}:", s.len()).as_bytes());
    out.extend_from_slice(s);
}

// Re-export Instant for module-local use without an extra import line at top.
use std::time::Instant;

/// Convenience: fetch metadata from the first peer that succeeds, trying a
/// list of candidates in order.
pub async fn fetch_metadata_from_candidates(
    binder: Arc<dyn NetworkBinder>,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    candidates: &[PeerAddr],
) -> Result<Vec<u8>> {
    let mut last_err: Option<String> = None;
    for peer in candidates {
        match fetch_metadata(binder.as_ref(), info_hash, peer_id, *peer).await {
            Ok(info) => return Ok(info),
            Err(e) => last_err = Some(e.to_string()),
        }
    }
    Err(CoreError::Internal(format!(
        "metadata fetch failed from all candidates: {}",
        last_err.unwrap_or_else(|| "no candidates".into())
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
    use swarmotter_core::net::binder::LoopbackBinder;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn unique_dir(label: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "swarmotter-meta-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn peer_id(prefix: &[u8; 8]) -> [u8; 20] {
        let mut id = [0u8; 20];
        id[..8].copy_from_slice(prefix);
        id
    }

    /// A peer that serves the `info` dict over ut_metadata. It speaks the
    /// extension protocol and replies to metadata requests with the raw info
    /// bytes split into pieces.
    async fn serve_metadata_peer(
        stream: tokio::net::TcpStream,
        info_hash: InfoHash,
        info_bytes: Vec<u8>,
    ) -> swarmotter_core::Result<()> {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut hs = [0u8; 68];
        rd.read_exact(&mut hs).await?;
        let their_hs = Handshake::decode(&hs)
            .map_err(|e| swarmotter_core::error::CoreError::Internal(e.to_string()))?;
        if their_hs.info_hash != info_hash {
            return Err(swarmotter_core::error::CoreError::Internal(
                "info hash mismatch".into(),
            ));
        }
        let our_hs = Handshake {
            info_hash,
            peer_id: peer_id(b"-SD0090-"),
            reserved: extensions::EXTENSION_RESERVED,
        };
        wr.write_all(&our_hs.encode()).await?;

        // Read the leecher's bitfield + extension handshake.
        let _ = read_one_message(&mut rd).await; // bitfield
                                                 // The leecher sends an Extended handshake (id 0).
        let local_metadata_id: u8 = 1u8;
        let ext_hs = extensions::encode_extension_handshake(
            &[(extensions::UT_METADATA_NAME, local_metadata_id)],
            "MetaSeed/0.1",
            Some(info_bytes.len() as u64),
        );
        peer::write_message(
            &mut wr,
            &Message::Extended {
                id: extensions::EXTENSION_HANDSHAKE_ID,
                payload: ext_hs,
            },
        )
        .await?;
        wr.flush().await.ok();

        // Serve metadata requests. Learn the leecher's ut_metadata id from its
        // extension handshake so we send data with the id it expects.
        let mut leecher_metadata_id: u8 = local_metadata_id;
        let total = info_bytes.len();
        loop {
            let msg = match read_one_message(&mut rd).await {
                Ok(Some(m)) => m,
                _ => return Ok(()),
            };
            if let Message::Extended { id, payload } = msg {
                if id == extensions::EXTENSION_HANDSHAKE_ID {
                    // Learn the leecher's ut_metadata id.
                    if let Ok(hs) = extensions::parse_extension_handshake(&payload) {
                        if let Some(remote) = hs.id_for(extensions::UT_METADATA_NAME) {
                            leecher_metadata_id = remote;
                        }
                    }
                    continue;
                }
                // A metadata request: parse it.
                if let Ok(m) = extensions::parse_metadata_message(&payload) {
                    if m.msg_type == MetadataMsgType::Request {
                        let start = (m.piece as usize) * extensions::METADATA_PIECE_SIZE;
                        let end = (start + extensions::METADATA_PIECE_SIZE).min(total);
                        let data = &info_bytes[start..end];
                        let data_msg =
                            extensions::encode_metadata_data(m.piece, total as u64, data);
                        peer::write_message(
                            &mut wr,
                            &Message::Extended {
                                id: leecher_metadata_id,
                                payload: data_msg,
                            },
                        )
                        .await?;
                        wr.flush().await.ok();
                    }
                }
            }
        }
    }

    async fn serve_metadata_peer_with_reported_size(
        stream: tokio::net::TcpStream,
        info_hash: InfoHash,
        reported_size: u64,
    ) -> swarmotter_core::Result<()> {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut hs = [0u8; 68];
        rd.read_exact(&mut hs).await?;
        let their_hs = Handshake::decode(&hs)
            .map_err(|e| swarmotter_core::error::CoreError::Internal(e.to_string()))?;
        if their_hs.info_hash != info_hash {
            return Err(swarmotter_core::error::CoreError::Internal(
                "info hash mismatch".into(),
            ));
        }
        let our_hs = Handshake {
            info_hash,
            peer_id: peer_id(b"-SD0090-"),
            reserved: extensions::EXTENSION_RESERVED,
        };
        wr.write_all(&our_hs.encode()).await?;
        let _ = read_one_message(&mut rd).await; // bitfield
        let _ = read_one_message(&mut rd).await; // extension handshake
        let local_metadata_id: u8 = 1u8;
        let ext_hs = extensions::encode_extension_handshake(
            &[(extensions::UT_METADATA_NAME, local_metadata_id)],
            "MetaSeed/0.1",
            Some(reported_size),
        );
        peer::write_message(
            &mut wr,
            &Message::Extended {
                id: extensions::EXTENSION_HANDSHAKE_ID,
                payload: ext_hs,
            },
        )
        .await?;
        wr.flush().await.ok();
        Ok(())
    }

    async fn serve_metadata_peer_with_oversize_piece(
        stream: tokio::net::TcpStream,
        info_hash: InfoHash,
        total: usize,
        piece_payload: usize,
    ) -> swarmotter_core::Result<()> {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut hs = [0u8; 68];
        rd.read_exact(&mut hs).await?;
        let their_hs = Handshake::decode(&hs)
            .map_err(|e| swarmotter_core::error::CoreError::Internal(e.to_string()))?;
        if their_hs.info_hash != info_hash {
            return Err(swarmotter_core::error::CoreError::Internal(
                "info hash mismatch".into(),
            ));
        }
        let our_hs = Handshake {
            info_hash,
            peer_id: peer_id(b"-SD0090-"),
            reserved: extensions::EXTENSION_RESERVED,
        };
        wr.write_all(&our_hs.encode()).await?;
        let _ = read_one_message(&mut rd).await; // bitfield
        let local_metadata_id: u8 = 1u8;
        let ext_hs = extensions::encode_extension_handshake(
            &[(extensions::UT_METADATA_NAME, local_metadata_id)],
            "MetaSeed/0.1",
            Some(total as u64),
        );
        peer::write_message(
            &mut wr,
            &Message::Extended {
                id: extensions::EXTENSION_HANDSHAKE_ID,
                payload: ext_hs,
            },
        )
        .await?;
        wr.flush().await.ok();

        let mut leecher_metadata_id: u8 = local_metadata_id;
        loop {
            let msg = match read_one_message(&mut rd).await {
                Ok(Some(m)) => m,
                _ => return Ok(()),
            };
            if let Message::Extended {
                id: remote_id,
                payload,
            } = msg
            {
                if remote_id == extensions::EXTENSION_HANDSHAKE_ID {
                    if let Ok(hs) = extensions::parse_extension_handshake(&payload) {
                        if let Some(remote) = hs.id_for(extensions::UT_METADATA_NAME) {
                            leecher_metadata_id = remote;
                        }
                    }
                    continue;
                }
                if let Ok(m) = extensions::parse_metadata_message(&payload) {
                    if m.msg_type == MetadataMsgType::Request {
                        let data = vec![0xAA; piece_payload];
                        let data_msg = extensions::encode_metadata_data(0, total as u64, &data);
                        peer::write_message(
                            &mut wr,
                            &Message::Extended {
                                id: leecher_metadata_id,
                                payload: data_msg,
                            },
                        )
                        .await?;
                        wr.flush().await.ok();
                    }
                }
            }
        }
    }

    async fn read_one_message<R: AsyncReadExt + Unpin>(
        rd: &mut R,
    ) -> std::io::Result<Option<Message>> {
        let mut len = [0u8; 4];
        let mut filled = 0;
        loop {
            match rd.read(&mut len[filled..]).await {
                Ok(0) => {
                    return Ok(None);
                }
                Ok(n) => {
                    filled += n;
                    if filled == 4 {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        let n = u32::from_be_bytes(len) as usize;
        if n == 0 {
            return Ok(Some(Message::Keepalive));
        }
        let mut body = vec![0u8; n];
        rd.read_exact(&mut body).await?;
        let mut frame = Vec::with_capacity(4 + n);
        frame.extend_from_slice(&len);
        frame.extend_from_slice(&body);
        Message::decode_frame(&frame)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fetch_metadata_assembles_and_verifies_info_hash() {
        // Build a real torrent to get a real info dict + hash.
        let content = b"swarmotter bep9 metadata fetch test payload!!";
        let bytes = build_single_file_torrent("meta.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        // Extract the raw info bytes from the built torrent.
        let info_bytes = swarmotter_core::bencode::extract_value_bytes(&bytes, b"info")
            .expect("info present")
            .to_vec();

        // Spawn a metadata-serving peer.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let info_hash = meta.info_hash;
        let info_for_peer = info_bytes.clone();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_metadata_peer(stream, info_hash, info_for_peer).await;
            }
        });

        let binder = Arc::new(LoopbackBinder);
        let fetched = fetch_metadata(
            binder.as_ref(),
            info_hash,
            peer_id(b"-SW0090-"),
            PeerAddr::from_socket_addr(addr),
        )
        .await
        .expect("metadata fetch should succeed");

        assert_eq!(fetched, info_bytes);

        // The fetched info bytes should round-trip into a TorrentMeta matching
        // the original info hash.
        let rebuilt = build_meta_from_info(&fetched, "meta.bin", &[]).unwrap();
        assert_eq!(rebuilt.info_hash, info_hash);
        assert_eq!(rebuilt.total_length, meta.total_length);
        assert_eq!(rebuilt.piece_count(), meta.piece_count());

        let _ = unique_dir("meta"); // keep helper referenced
    }

    #[tokio::test]
    async fn fetch_metadata_blocked_by_fail_closed_binder() {
        let binder = Arc::new(swarmotter_core::net::binder::BlockedBinder);
        let err = fetch_metadata(
            binder.as_ref(),
            InfoHash::from_bytes([0u8; 20]),
            peer_id(b"-SW0091-"),
            PeerAddr::from_socket_addr("127.0.0.1:9".parse().unwrap()),
        )
        .await
        .unwrap_err();
        assert!(err.is_network_blocked());
    }

    #[tokio::test]
    async fn fetch_metadata_rejects_oversized_reported_size() {
        let content = b"meta metadata size cap test";
        let bytes = build_single_file_torrent("meta.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let info_hash = meta.info_hash;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_metadata_peer_with_reported_size(
                    stream,
                    info_hash,
                    (MAX_METADATA_SIZE as u64) + 1,
                )
                .await;
            }
        });

        let binder = Arc::new(LoopbackBinder);
        let err = fetch_metadata(
            binder.as_ref(),
            info_hash,
            peer_id(b"-SW0092-"),
            PeerAddr::from_socket_addr(addr),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[tokio::test]
    async fn fetch_metadata_rejects_piece_data_exceeding_announced_total() {
        let content = b"metadata piece size cap test payload";
        let bytes = build_single_file_torrent("meta.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let info_hash = meta.info_hash;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_metadata_peer_with_oversize_piece(stream, info_hash, 8, 1024).await;
            }
        });

        let binder = Arc::new(LoopbackBinder);
        let err = fetch_metadata(
            binder.as_ref(),
            info_hash,
            peer_id(b"-SW0093-"),
            PeerAddr::from_socket_addr(addr),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("exceeds announced total"));
    }
}
