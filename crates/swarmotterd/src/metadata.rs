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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use swarmotter_core::config::PeerEncryptionMode;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::extensions::{self, MetadataMsgType};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::meta::{TorrentMeta, MAX_TORRENT_METADATA_BYTES};
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{self, Handshake, Message, PeerAddr};
use swarmotter_core::peer_filter::PeerFilter;
use swarmotter_core::utp::{self, PeerDuplex, PeerTransport};

use crate::peer_permits::PeerSessionBudget;

const METADATA_CANDIDATE_CONCURRENCY: usize = 32;

#[derive(Clone)]
pub(crate) struct MetadataFetchContext {
    peer_session_budget: PeerSessionBudget,
    binder: Arc<dyn NetworkBinder>,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    peer_filter: Arc<PeerFilter>,
}

impl MetadataFetchContext {
    pub(crate) fn new(
        peer_session_budget: PeerSessionBudget,
        binder: Arc<dyn NetworkBinder>,
        info_hash: InfoHash,
        peer_id: [u8; 20],
        utp_enabled: bool,
        utp_prefer_tcp: bool,
        encryption_mode: PeerEncryptionMode,
    ) -> Self {
        Self {
            peer_session_budget,
            binder,
            info_hash,
            peer_id,
            utp_enabled,
            utp_prefer_tcp,
            encryption_mode,
            peer_filter: Arc::new(PeerFilter::default()),
        }
    }

    pub(crate) fn with_peer_filter(mut self, peer_filter: Arc<PeerFilter>) -> Self {
        self.peer_filter = peer_filter;
        self
    }
}

/// Fetch the torrent metadata (`info` dict) from a peer via `ut_metadata`.
/// Returns the assembled, info-hash-verified `info` bytes.
///
/// The caller supplies the peer to fetch from (typically discovered via a
/// tracker, PEX, or DHT). The connection goes through the binder. On success
/// the raw `info` bytes can be turned into a `TorrentMeta` via
/// [`build_meta_from_info`].
pub(crate) async fn fetch_metadata_with_transport(
    context: &MetadataFetchContext,
    peer: PeerAddr,
) -> Result<Vec<u8>> {
    if !context.binder.traffic_allowed() {
        return Err(CoreError::NetworkBlocked(
            "torrent data plane blocked; cannot fetch metadata".into(),
        ));
    }
    let decision = context.peer_filter.admit_ip(peer.ip);
    if !decision.is_allowed() {
        tracing::info!(
            peer = %peer.socket_addr(),
            reason = decision.audit_reason(),
            detail = ?decision.rejection_message(),
            "metadata peer rejected before contained outbound admission"
        );
        return Err(CoreError::Internal(
            decision
                .rejection_message()
                .unwrap_or_else(|| "peer rejected by admission policy".into()),
        ));
    }
    let _peer_permit = context.peer_session_budget.acquire_outbound().await?;
    let transports = peer_transport_order(
        context.utp_enabled,
        context.utp_prefer_tcp,
        context.encryption_mode,
    );

    let mut last_error = None;
    for transport in transports {
        match fetch_metadata_via_transport(
            context.binder.clone(),
            transport,
            context.info_hash,
            context.peer_id,
            peer,
            context.encryption_mode,
            context.peer_filter.as_ref(),
        )
        .await
        {
            Ok(info) => return Ok(info),
            Err(e) => last_error = Some(e),
        }
    }
    Err(last_error
        .unwrap_or_else(|| CoreError::Internal("no metadata transport configured".into())))
}

fn peer_transport_order(
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    _encryption_mode: PeerEncryptionMode,
) -> Vec<PeerTransport> {
    if !utp_enabled {
        return vec![PeerTransport::Tcp];
    }
    if utp_prefer_tcp {
        vec![PeerTransport::Tcp, PeerTransport::Utp]
    } else {
        vec![PeerTransport::Utp, PeerTransport::Tcp]
    }
}

async fn fetch_metadata_via_transport(
    binder: Arc<dyn NetworkBinder>,
    transport: PeerTransport,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    peer: PeerAddr,
    encryption_mode: PeerEncryptionMode,
    peer_filter: &PeerFilter,
) -> Result<Vec<u8>> {
    let (stream, selected) =
        utp::connect_peer_stream(binder.clone(), transport, peer.socket_addr()).await?;
    let stream = match encryption_mode {
        PeerEncryptionMode::Disabled => stream,
        PeerEncryptionMode::Required => {
            let encrypted = timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, info_hash),
            )
            .await??;
            Box::new(encrypted) as Box<dyn PeerDuplex>
        }
        PeerEncryptionMode::Preferred => {
            match timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, info_hash),
            )
            .await
            {
                Ok(Ok(encrypted)) => Box::new(encrypted) as Box<dyn PeerDuplex>,
                Ok(Err(e)) => {
                    tracing::debug!(
                        peer = %peer.socket_addr(),
                        transport = selected.as_str(),
                        error = %e,
                        "MSE/PE metadata negotiation failed; retrying contained metadata transport as plaintext"
                    );
                    let (plain, _) =
                        utp::connect_peer_stream(binder, selected, peer.socket_addr()).await?;
                    plain
                }
                Err(e) => {
                    tracing::debug!(
                        peer = %peer.socket_addr(),
                        transport = selected.as_str(),
                        error = %e,
                        "MSE/PE metadata negotiation timed out; retrying contained metadata transport as plaintext"
                    );
                    let (plain, _) =
                        utp::connect_peer_stream(binder, selected, peer.socket_addr()).await?;
                    plain
                }
            }
        }
    };
    fetch_metadata_over_stream(stream, info_hash, peer_id, peer_filter).await
}

async fn fetch_metadata_over_stream(
    stream: Box<dyn PeerDuplex>,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    peer_filter: &PeerFilter,
) -> Result<Vec<u8>> {
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
    let decision = peer_filter.admit_client_id(&their_hs.peer_id);
    if !decision.is_allowed() {
        tracing::info!(
            reason = decision.audit_reason(),
            detail = ?decision.rejection_message(),
            "metadata peer rejected after contained handshake"
        );
        return Err(CoreError::Internal(
            decision
                .rejection_message()
                .unwrap_or_else(|| "peer rejected by admission policy".into()),
        ));
    }
    if !their_hs.supports_extensions() {
        return Err(CoreError::Internal(
            "metadata peer does not support BEP 10 extensions".into(),
        ));
    }

    // Send our extension handshake advertising ut_metadata. Metadata-only
    // magnet sessions do not know the torrent piece count yet, so sending a
    // zero-length bitfield would be invalid for real multi-piece torrents.
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
    let total_u64 = metadata_size.ok_or_else(|| {
        CoreError::MalformedTorrent("metadata peer did not report metadata_size".into())
    })?;
    let total = usize::try_from(total_u64)
        .map_err(|_| CoreError::MalformedTorrent("metadata size exceeds platform limit".into()))?;
    if total == 0 {
        return Err(CoreError::MalformedTorrent(
            "metadata size must be greater than zero".into(),
        ));
    }
    if total > MAX_TORRENT_METADATA_BYTES {
        return Err(CoreError::MalformedTorrent(format!(
            "metadata size {total} exceeds maximum {MAX_TORRENT_METADATA_BYTES}"
        )));
    }

    // Request each metadata piece and assemble.
    let pieces = extensions::metadata_pieces(total).max(1);
    let mut assembled: Vec<u8> = Vec::with_capacity(total);
    for piece in 0..pieces {
        let piece_index = u32::try_from(piece).map_err(|_| {
            CoreError::MalformedTorrent("metadata piece index exceeds u32 range".into())
        })?;
        let req = extensions::encode_metadata_request(piece_index);
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
                    Ok(m)
                        if m.msg_type == MetadataMsgType::Data
                            && usize::try_from(m.piece).ok() == Some(piece) =>
                    {
                        let room = total.checked_sub(assembled.len()).ok_or_else(|| {
                            CoreError::MalformedTorrent(
                                "assembled metadata exceeds advertised size".into(),
                            )
                        })?;
                        if m.data.len() > room {
                            return Err(CoreError::MalformedTorrent(format!(
                                "metadata piece {piece} data exceeds advertised size {total}"
                            )));
                        }
                        assembled.extend_from_slice(&m.data);
                        if let Some(peer_total) = m.total_size {
                            if peer_total != total_u64 {
                                return Err(CoreError::MalformedTorrent(format!(
                                    "metadata total_size {peer_total} does not match advertised {total_u64}"
                                )));
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

    if assembled.len() != total {
        return Err(CoreError::MalformedTorrent(format!(
            "assembled metadata length {} does not match advertised {total}",
            assembled.len()
        )));
    }

    // Verify the assembled info dict hashes to the info hash.
    let computed = swarmotter_core::hash::InfoHash::from_info_bencoded(&assembled);
    if computed != info_hash {
        return Err(CoreError::MalformedTorrent(
            "fetched metadata info hash mismatch; rejecting".into(),
        ));
    }
    Ok(assembled)
}

/// Build a `TorrentMeta` from raw `info` dict bytes plus the magnet's name and
/// trackers. The raw BEP 9 dictionary is parsed directly so an exact-limit
/// payload is not rejected because of internally generated wrapper bytes.
pub fn build_meta_from_info(
    info_bytes: &[u8],
    name: &str,
    trackers: &[String],
) -> Result<TorrentMeta> {
    let meta = swarmotter_core::meta::parse_info_dict(info_bytes, trackers)?;
    let _ = name; // name is derived from the info dict
    Ok(meta)
}

// Re-export Instant for module-local use without an extra import line at top.
use std::time::Instant;

/// Convenience: fetch metadata from the first peer that succeeds, racing a
/// bounded set of candidates so one slow peer cannot block metadata discovery
/// for an entire public swarm.
pub(crate) async fn fetch_metadata_from_candidates_with_budget(
    fetch_context: MetadataFetchContext,
    candidates: &[PeerAddr],
) -> Result<Vec<u8>> {
    let mut seen = HashSet::new();
    let candidates: Vec<PeerAddr> = candidates
        .iter()
        .copied()
        .filter(|peer| seen.insert(*peer))
        .collect();
    if candidates.is_empty() {
        return Err(CoreError::Internal(
            "metadata fetch failed from all candidates: no candidates".into(),
        ));
    }

    let mut tasks = tokio::task::JoinSet::new();
    let mut last_err: Option<String> = None;
    let mut next = 0usize;
    while next < candidates.len() && tasks.len() < METADATA_CANDIDATE_CONCURRENCY {
        spawn_metadata_fetch(&mut tasks, fetch_context.clone(), candidates[next]);
        next += 1;
    }

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((_, Ok(info))) => return Ok(info),
            Ok((peer, Err(e))) => {
                last_err = Some(format!("{peer:?}: {e}"));
            }
            Err(e) => {
                last_err = Some(format!("metadata candidate task failed: {e}"));
            }
        }
        while next < candidates.len() && tasks.len() < METADATA_CANDIDATE_CONCURRENCY {
            spawn_metadata_fetch(&mut tasks, fetch_context.clone(), candidates[next]);
            next += 1;
        }
    }
    Err(CoreError::Internal(format!(
        "metadata fetch failed from all candidates: {}",
        last_err.unwrap_or_else(|| "all candidate tasks ended without result".into())
    )))
}

fn spawn_metadata_fetch(
    tasks: &mut tokio::task::JoinSet<(PeerAddr, Result<Vec<u8>>)>,
    context: MetadataFetchContext,
    peer: PeerAddr,
) {
    tasks.spawn(async move {
        let result = fetch_metadata_with_transport(&context, peer).await;
        (peer, result)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_permits::PeerPermitPool;
    use std::sync::atomic::AtomicU64;
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

    fn info_dict_padded_to_size(target: usize) -> Vec<u8> {
        let torrent =
            build_single_file_torrent("bep9-limit.bin", b"bounded BEP 9 payload", 8, None, false);
        let mut info = swarmotter_core::bencode::extract_value_bytes(&torrent, b"info")
            .expect("generated torrent contains info")
            .to_vec();
        assert_eq!(info.pop(), Some(b'e'));
        info.extend_from_slice(b"7:padding");

        let mut padding_len = target.saturating_sub(info.len() + 2);
        for _ in 0..32 {
            let encoded_len = info.len() + padding_len.to_string().len() + 1 + padding_len + 1;
            if encoded_len == target {
                info.extend_from_slice(padding_len.to_string().as_bytes());
                info.push(b':');
                info.extend(std::iter::repeat_n(b'x', padding_len));
                info.push(b'e');
                assert_eq!(info.len(), target);
                return info;
            }
            padding_len = target
                .checked_sub(info.len() + padding_len.to_string().len() + 2)
                .expect("target must accommodate the generated info dictionary");
        }
        panic!("could not solve bencode padding for target size {target}");
    }

    #[test]
    fn preferred_encryption_preserves_metadata_transport_preference() {
        assert_eq!(
            peer_transport_order(true, false, PeerEncryptionMode::Preferred),
            vec![PeerTransport::Utp, PeerTransport::Tcp]
        );
        assert_eq!(
            peer_transport_order(true, true, PeerEncryptionMode::Preferred),
            vec![PeerTransport::Tcp, PeerTransport::Utp]
        );
        assert_eq!(
            peer_transport_order(true, false, PeerEncryptionMode::Required),
            vec![PeerTransport::Utp, PeerTransport::Tcp]
        );
    }

    /// A peer that serves the `info` dict over ut_metadata. It speaks the
    /// extension protocol and replies to metadata requests with the raw info
    /// bytes split into pieces.
    async fn serve_metadata_peer<S>(
        stream: S,
        info_hash: InfoHash,
        info_bytes: Vec<u8>,
    ) -> swarmotter_core::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
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

    async fn serve_metadata_peer_with_piece_response(
        stream: tokio::net::TcpStream,
        info_hash: InfoHash,
        advertised_total: usize,
        response_total: u64,
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
        let local_metadata_id: u8 = 1u8;
        let ext_hs = extensions::encode_extension_handshake(
            &[(extensions::UT_METADATA_NAME, local_metadata_id)],
            "MetaSeed/0.1",
            Some(advertised_total as u64),
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
                        let data_msg =
                            extensions::encode_metadata_data(m.piece, response_total, &data);
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

    async fn fetch_custom_piece_error(
        advertised_total: usize,
        response_total: u64,
        piece_payload: usize,
    ) -> CoreError {
        let info_hash = InfoHash::from_bytes([0x5a; 20]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_metadata_peer_with_piece_response(
                    stream,
                    info_hash,
                    advertised_total,
                    response_total,
                    piece_payload,
                )
                .await;
            }
        });

        fetch_metadata_with_transport(
            &MetadataFetchContext::new(
                PeerSessionBudget::unlimited(),
                Arc::new(LoopbackBinder),
                info_hash,
                peer_id(b"-SW0093-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            ),
            PeerAddr::from_socket_addr(addr),
        )
        .await
        .unwrap_err()
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
        let fetched = fetch_metadata_with_transport(
            &MetadataFetchContext::new(
                PeerSessionBudget::unlimited(),
                binder,
                info_hash,
                peer_id(b"-SW0090-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            ),
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
        let err = fetch_metadata_with_transport(
            &MetadataFetchContext::new(
                PeerSessionBudget::unlimited(),
                binder,
                InfoHash::from_bytes([0u8; 20]),
                peer_id(b"-SW0091-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            ),
            PeerAddr::from_socket_addr("127.0.0.1:9".parse().unwrap()),
        )
        .await
        .unwrap_err();
        assert!(err.is_network_blocked());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fetch_metadata_accepts_exact_metadata_size_limit() {
        let info_bytes = info_dict_padded_to_size(MAX_TORRENT_METADATA_BYTES);
        let info_hash = InfoHash::from_info_bencoded(&info_bytes);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = info_bytes.clone();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_metadata_peer(stream, info_hash, served).await;
            }
        });

        let fetched = fetch_metadata_with_transport(
            &MetadataFetchContext::new(
                PeerSessionBudget::unlimited(),
                Arc::new(LoopbackBinder),
                info_hash,
                peer_id(b"-SW0094-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            ),
            PeerAddr::from_socket_addr(addr),
        )
        .await
        .expect("exact-limit BEP 9 metadata must assemble");

        assert_eq!(fetched.len(), MAX_TORRENT_METADATA_BYTES);
        assert_eq!(fetched, info_bytes);
        let rebuilt = build_meta_from_info(&fetched, "bep9-limit.bin", &[])
            .expect("exact-limit assembled info must parse without wrapper overhead");
        assert_eq!(rebuilt.info_hash, info_hash);
    }

    #[tokio::test]
    async fn fetch_metadata_rejects_zero_and_oversized_reported_size_as_malformed() {
        let content = b"meta metadata size cap test";
        let bytes = build_single_file_torrent("meta.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let info_hash = meta.info_hash;
        for (reported_size, expected_context) in [
            (0, "greater than zero"),
            ((MAX_TORRENT_METADATA_BYTES as u64) + 1, "exceeds maximum"),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                if let Ok((stream, _)) = listener.accept().await {
                    let _ =
                        serve_metadata_peer_with_reported_size(stream, info_hash, reported_size)
                            .await;
                }
            });

            let err = fetch_metadata_with_transport(
                &MetadataFetchContext::new(
                    PeerSessionBudget::unlimited(),
                    Arc::new(LoopbackBinder),
                    info_hash,
                    peer_id(b"-SW0092-"),
                    false,
                    true,
                    PeerEncryptionMode::Disabled,
                ),
                PeerAddr::from_socket_addr(addr),
            )
            .await
            .unwrap_err();
            assert!(matches!(&err, CoreError::MalformedTorrent(_)));
            assert!(err.to_string().contains(expected_context), "error: {err}");
        }
    }

    #[tokio::test]
    async fn fetch_metadata_rejects_piece_data_exceeding_announced_total() {
        let err = fetch_custom_piece_error(8, 8, 9).await;
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("exceeds advertised size"));
    }

    #[tokio::test]
    async fn fetch_metadata_rejects_piece_total_size_mismatch_as_malformed() {
        let err = fetch_custom_piece_error(8, 9, 8).await;
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("does not match advertised"));
    }

    #[tokio::test]
    async fn fetch_metadata_rejects_incomplete_assembly_as_malformed() {
        let err = fetch_custom_piece_error(8, 8, 7).await;
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("assembled metadata length 7"));
        assert!(err.to_string().contains("advertised 8"));
    }

    #[tokio::test]
    async fn fetch_metadata_rejects_info_hash_mismatch_as_malformed() {
        let err = fetch_custom_piece_error(8, 8, 8).await;
        assert!(matches!(&err, CoreError::MalformedTorrent(_)));
        assert!(err.to_string().contains("info hash mismatch"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn utp_metadata_path_holds_permit_for_transport_and_protocol_lifetime() {
        let content = b"generated uTP metadata permit fixture";
        let bytes = build_single_file_torrent("utp-meta.bin", content, 8, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let info_bytes = swarmotter_core::bencode::extract_value_bytes(&bytes, b"info")
            .unwrap()
            .to_vec();
        let binder: Arc<dyn NetworkBinder> = Arc::new(LoopbackBinder);
        let socket: Arc<dyn swarmotter_core::net::ContainedUdpSocket> =
            binder.udp_socket().await.unwrap().into();
        let address = socket.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        let server_info = info_bytes.clone();
        let info_hash = meta.info_hash;
        let server = tokio::spawn(async move {
            let mut packet = vec![0u8; 2048];
            let (peer, length) = socket.recv_from(&mut packet).await.unwrap();
            let (syn, _) = swarmotter_core::utp::UtpHeader::decode(&packet[..length]).unwrap();
            assert_eq!(syn.typ, swarmotter_core::utp::UtpType::Syn);
            let connection =
                swarmotter_core::utp::UtpConnection::accept_from_syn(socket, peer, &syn)
                    .await
                    .unwrap();
            let stream = swarmotter_core::utp::UtpStream::spawn(connection);
            let _ = accepted_tx.send(());
            let _ = continue_rx.await;
            serve_metadata_peer(stream, info_hash, server_info).await
        });
        let denied = Arc::new(AtomicU64::new(0));
        let global = PeerPermitPool::new(1, denied.clone()).unwrap();
        let torrent = PeerPermitPool::new(1, denied).unwrap();
        let budget = PeerSessionBudget::new(global.clone(), torrent.clone());
        let client_budget = budget.clone();
        let client = tokio::spawn(async move {
            let context = MetadataFetchContext::new(
                client_budget,
                Arc::new(LoopbackBinder),
                info_hash,
                peer_id(b"-UTPMETA"),
                true,
                false,
                PeerEncryptionMode::Disabled,
            );
            fetch_metadata_with_transport(&context, PeerAddr::from_socket_addr(address)).await
        });
        accepted_rx.await.unwrap();
        assert_eq!(global.snapshot().in_use, 1);
        assert_eq!(torrent.snapshot().in_use, 1);
        continue_tx.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), client)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            info_bytes
        );
        server.abort();
        let _ = server.await;
        assert_eq!(global.snapshot().in_use, 0);
        assert_eq!(torrent.snapshot().in_use, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn metadata_session_raii_releases_on_connect_handshake_eof_and_cancellation() {
        let denied = Arc::new(AtomicU64::new(0));
        let global = PeerPermitPool::new(1, denied.clone()).unwrap();
        let torrent = PeerPermitPool::new(1, denied).unwrap();
        let budget = PeerSessionBudget::new(global.clone(), torrent.clone());
        let info_hash = InfoHash::from_bytes([0x5a; 20]);

        let unused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unused_addr = unused.local_addr().unwrap();
        drop(unused);
        assert!(fetch_metadata_with_transport(
            &MetadataFetchContext::new(
                budget.clone(),
                Arc::new(LoopbackBinder),
                info_hash,
                peer_id(b"-RAIICN-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            ),
            PeerAddr::from_socket_addr(unused_addr),
        )
        .await
        .is_err());
        assert_eq!(global.snapshot().in_use, 0);
        assert_eq!(torrent.snapshot().in_use, 0);

        let malformed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let malformed_addr = malformed.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = malformed.accept().await.unwrap();
            let _ = stream.write_all(b"not-a-peer-handshake").await;
        });
        assert!(fetch_metadata_with_transport(
            &MetadataFetchContext::new(
                budget.clone(),
                Arc::new(LoopbackBinder),
                info_hash,
                peer_id(b"-RAIIHS-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            ),
            PeerAddr::from_socket_addr(malformed_addr),
        )
        .await
        .is_err());
        assert_eq!(global.snapshot().in_use, 0);
        assert_eq!(torrent.snapshot().in_use, 0);

        let eof_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let eof_addr = eof_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = eof_listener.accept().await.unwrap();
            let mut request = [0u8; 68];
            stream.read_exact(&mut request).await.unwrap();
            peer::write_handshake(
                &mut stream,
                &Handshake {
                    info_hash,
                    peer_id: peer_id(b"-RAIIEOF"),
                    reserved: extensions::EXTENSION_RESERVED,
                },
            )
            .await
            .unwrap();
        });
        assert!(fetch_metadata_with_transport(
            &MetadataFetchContext::new(
                budget.clone(),
                Arc::new(LoopbackBinder),
                info_hash,
                peer_id(b"-RAIIOU-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            ),
            PeerAddr::from_socket_addr(eof_addr),
        )
        .await
        .is_err());
        assert_eq!(global.snapshot().in_use, 0);
        assert_eq!(torrent.snapshot().in_use, 0);

        let stalled = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_addr = stalled.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let stalled_server = tokio::spawn(async move {
            let (stream, _) = stalled.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let _stream = stream;
            std::future::pending::<()>().await;
        });
        let cancelled_budget = budget.clone();
        let cancelled = tokio::spawn(async move {
            let context = MetadataFetchContext::new(
                cancelled_budget,
                Arc::new(LoopbackBinder),
                info_hash,
                peer_id(b"-RAIICA-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            );
            fetch_metadata_with_transport(&context, PeerAddr::from_socket_addr(stalled_addr)).await
        });
        accepted_rx.await.unwrap();
        assert_eq!(global.snapshot().in_use, 1);
        assert_eq!(torrent.snapshot().in_use, 1);
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert_eq!(global.snapshot().in_use, 0);
        assert_eq!(torrent.snapshot().in_use, 0);
        stalled_server.abort();
    }
}
