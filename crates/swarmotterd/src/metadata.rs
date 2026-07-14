// SPDX-License-Identifier: Apache-2.0

//! BEP 9 magnet metadata fetch.
//!
//! For magnet links (which carry only an info hash, not the full metadata),
//! the engine fetches the torrent's `info` dictionary from a peer using the
//! `ut_metadata` extension (BEP 9) over the BEP 10 extension protocol. The
//! raw `info` bytes are assembled from metadata pieces and verified against
//! the magnet's explicit v1, v2, or hybrid identity. Pure-v2 sessions then
//! retrieve and validate the BEP 52 top-level piece layers before producing an
//! executable [`TorrentMeta`]; hybrid sessions retain their validated v1
//! compatibility layout.
//!
//! All peer connections go through the `NetworkBinder`; no socket is created
//! directly. Private torrents still fetch metadata over peer connections
//! (PEX/DHT are disabled for private torrents, not metadata exchange over an
//! existing peer). See `design/requirements.md` and ADR-0013.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

use swarmotter_core::config::PeerEncryptionMode;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::extensions::{self, MetadataMsgType};
use swarmotter_core::hash::{InfoHash, PeerInfoHash, TorrentIdentity};
use swarmotter_core::meta::{
    TorrentMeta, V2PieceLayer, MAX_TORRENT_METADATA_BYTES, MAX_TORRENT_PIECES,
};
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{self, Handshake, Message, PeerAddr, V2Handshake};
use swarmotter_core::peer_filter::PeerFilter;
use swarmotter_core::utp::{self, PeerDuplex, PeerTransport};

use crate::peer_permits::PeerSessionBudget;

const METADATA_CANDIDATE_CONCURRENCY: usize = 32;
const METADATA_HASH_REQUEST_MAX: usize = 512;

/// Exact raw BEP 9 metadata plus the executable metainfo derived from it.
///
/// For pure-v2 magnets, `meta` is returned only after the separately
/// exchanged BEP 52 piece layers have been checked against every file-tree
/// root. `raw_info` remains the original bytes received from the peer, rather
/// than a reconstructed dictionary.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedMagnetMetadata {
    pub(crate) raw_info: Vec<u8>,
    pub(crate) meta: TorrentMeta,
}

#[derive(Debug)]
struct FetchedMetadata {
    raw_info: Vec<u8>,
    piece_layers: Vec<V2PieceLayer>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataFetchGoal {
    #[cfg(test)]
    Raw,
    Resolved,
}

/// The peer-wire handshake appropriate for an explicit magnet identity.
///
/// A hybrid magnet intentionally uses its v1 compatibility swarm for BEP 9;
/// its SHA-256 identity is still verified against the exact assembled `info`
/// dictionary before the result can be used. A pure-v2 magnet has no v1
/// fallback and therefore uses the BEP 52 20-byte wire truncation only at the
/// peer-protocol and MSE boundaries.
#[derive(Clone, Copy, Debug)]
enum MetadataHandshake {
    V1(InfoHash),
    V2(PeerInfoHash),
}

impl MetadataHandshake {
    fn for_identity(identity: &TorrentIdentity) -> Result<Self> {
        match identity {
            TorrentIdentity::V1 { v1 } | TorrentIdentity::Hybrid { v1, .. } => Ok(Self::V1(*v1)),
            TorrentIdentity::V2 { v2 } => Ok(Self::V2(v2.peer_info_hash())),
            TorrentIdentity::Unknown => Err(CoreError::InvalidArgument(
                "magnet metadata exchange requires an explicit v1, v2, or hybrid identity".into(),
            )),
        }
    }

    fn wire_info_hash(self) -> PeerInfoHash {
        match self {
            Self::V1(info_hash) => PeerInfoHash::from_v1(info_hash),
            Self::V2(info_hash) => info_hash,
        }
    }
}

#[derive(Clone)]
pub(crate) struct MetadataFetchContext {
    peer_session_budget: PeerSessionBudget,
    binder: Arc<dyn NetworkBinder>,
    identity: TorrentIdentity,
    peer_id: [u8; 20],
    utp_enabled: bool,
    utp_prefer_tcp: bool,
    encryption_mode: PeerEncryptionMode,
    peer_filter: Arc<PeerFilter>,
}

impl MetadataFetchContext {
    #[cfg(test)]
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
            identity: TorrentIdentity::v1(info_hash),
            peer_id,
            utp_enabled,
            utp_prefer_tcp,
            encryption_mode,
            peer_filter: Arc::new(PeerFilter::default()),
        }
    }

    /// Construct a metadata fetch context from a full explicit identity.
    ///
    /// New magnet paths should use this constructor so a pure-v2 magnet never
    /// needs a synthetic v1 placeholder. The legacy [`Self::new`] constructor
    /// remains for v1 call sites and local fixtures.
    pub(crate) fn for_identity(
        peer_session_budget: PeerSessionBudget,
        binder: Arc<dyn NetworkBinder>,
        identity: TorrentIdentity,
        peer_id: [u8; 20],
        utp_enabled: bool,
        utp_prefer_tcp: bool,
        encryption_mode: PeerEncryptionMode,
    ) -> Self {
        Self {
            peer_session_budget,
            binder,
            identity,
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
/// Returns the assembled, full-identity-verified `info` bytes.
///
/// The caller supplies the peer to fetch from (typically discovered via a
/// tracker, PEX, or DHT). The connection goes through the binder. On success
/// v1 and hybrid callers can turn the raw `info` bytes into a `TorrentMeta`
/// via [`build_meta_from_info`]. Pure-v2 callers must use
/// [`fetch_resolved_metadata_with_transport`] so required piece layers are
/// verified before any payload work begins.
#[cfg(test)]
pub(crate) async fn fetch_metadata_with_transport(
    context: &MetadataFetchContext,
    peer: PeerAddr,
) -> Result<Vec<u8>> {
    Ok(
        fetch_metadata_with_goal(context, peer, MetadataFetchGoal::Raw)
            .await?
            .raw_info,
    )
}

/// Fetch metadata and turn it into executable metainfo.
///
/// A pure-v2 response stays on the contained peer stream after BEP 9 so the
/// client can retrieve every required top-level piece layer via BEP 52 hash
/// requests. The final parser independently validates every returned layer
/// against the file tree; a partial, rejected, or mismatched layer never
/// reaches the data plane.
pub(crate) async fn fetch_resolved_metadata_with_transport(
    context: &MetadataFetchContext,
    peer: PeerAddr,
    trackers: &[String],
) -> Result<ResolvedMagnetMetadata> {
    let fetched = fetch_metadata_with_goal(context, peer, MetadataFetchGoal::Resolved).await?;
    let meta = match context.identity {
        TorrentIdentity::V2 { .. } => swarmotter_core::meta::parse_info_dict_with_piece_layers(
            &fetched.raw_info,
            trackers,
            &fetched.piece_layers,
        )?,
        TorrentIdentity::V1 { .. } | TorrentIdentity::Hybrid { .. } => {
            build_meta_from_info(&fetched.raw_info, "", trackers)?
        }
        TorrentIdentity::Unknown => {
            return Err(CoreError::InvalidArgument(
                "magnet metadata exchange requires an explicit torrent identity".into(),
            ));
        }
    };
    if meta.identity != context.identity {
        return Err(CoreError::MalformedTorrent(
            "resolved metadata identity does not match the magnet identity".into(),
        ));
    }
    Ok(ResolvedMagnetMetadata {
        raw_info: fetched.raw_info,
        meta,
    })
}

async fn fetch_metadata_with_goal(
    context: &MetadataFetchContext,
    peer: PeerAddr,
    goal: MetadataFetchGoal,
) -> Result<FetchedMetadata> {
    let handshake = MetadataHandshake::for_identity(&context.identity)?;
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
        match fetch_metadata_via_transport(context, transport, handshake, goal, peer).await {
            Ok(metadata) => return Ok(metadata),
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
    context: &MetadataFetchContext,
    transport: PeerTransport,
    handshake: MetadataHandshake,
    goal: MetadataFetchGoal,
    peer: PeerAddr,
) -> Result<FetchedMetadata> {
    let (stream, selected) =
        utp::connect_peer_stream(context.binder.clone(), transport, peer.socket_addr()).await?;
    let stream = match context.encryption_mode {
        PeerEncryptionMode::Disabled => stream,
        PeerEncryptionMode::Required => {
            let encrypted = timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, handshake.wire_info_hash()),
            )
            .await??;
            Box::new(encrypted) as Box<dyn PeerDuplex>
        }
        PeerEncryptionMode::Preferred => {
            match timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::connect(stream, handshake.wire_info_hash()),
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
                    let (plain, _) = utp::connect_peer_stream(
                        context.binder.clone(),
                        selected,
                        peer.socket_addr(),
                    )
                    .await?;
                    plain
                }
                Err(e) => {
                    tracing::debug!(
                        peer = %peer.socket_addr(),
                        transport = selected.as_str(),
                        error = %e,
                        "MSE/PE metadata negotiation timed out; retrying contained metadata transport as plaintext"
                    );
                    let (plain, _) = utp::connect_peer_stream(
                        context.binder.clone(),
                        selected,
                        peer.socket_addr(),
                    )
                    .await?;
                    plain
                }
            }
        }
    };
    fetch_metadata_over_stream(
        stream,
        handshake,
        &context.identity,
        goal,
        context.peer_id,
        context.peer_filter.as_ref(),
    )
    .await
}

async fn fetch_metadata_over_stream(
    stream: Box<dyn PeerDuplex>,
    handshake: MetadataHandshake,
    identity: &TorrentIdentity,
    goal: MetadataFetchGoal,
    local_peer_id: [u8; 20],
    peer_filter: &PeerFilter,
) -> Result<FetchedMetadata> {
    let (read_half, mut write_half) = tokio::io::split(stream);

    let mut reader = swarmotter_core::peer::PeerReader::new(read_half);
    // The 68-byte BitTorrent handshake is v1-shaped on the wire, but a
    // pure-v2 swarm uses its explicit BEP 52 wire truncation and capability
    // bit. Keep the distinction here rather than coercing the v2 value into
    // `InfoHash` before the full SHA-256 identity is checked.
    let (remote_peer_id, remote_supports_extensions) = match handshake {
        MetadataHandshake::V1(info_hash) => {
            let hs = Handshake {
                info_hash,
                peer_id: local_peer_id,
                reserved: extensions::EXTENSION_RESERVED,
            };
            peer::write_handshake(&mut write_half, &hs).await?;
            let remote = timeout(Duration::from_secs(15), reader.read_handshake()).await??;
            if remote.info_hash != info_hash {
                return Err(CoreError::Internal(
                    "metadata peer v1 info hash mismatch".into(),
                ));
            }
            (remote.peer_id, remote.supports_extensions())
        }
        MetadataHandshake::V2(wire_info_hash) => {
            let hs = V2Handshake {
                info_hash: wire_info_hash,
                peer_id: local_peer_id,
                reserved: peer::with_v2_support(extensions::EXTENSION_RESERVED),
            };
            peer::write_v2_handshake(&mut write_half, &hs).await?;
            let remote = timeout(Duration::from_secs(15), reader.read_v2_handshake()).await??;
            if remote.info_hash != wire_info_hash {
                return Err(CoreError::Internal(
                    "metadata peer v2 wire hash mismatch".into(),
                ));
            }
            (remote.peer_id, remote.supports_extensions())
        }
    };
    let decision = peer_filter.admit_client_id(&remote_peer_id);
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
    if !remote_supports_extensions {
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

    // Do not issue any BEP 52 hash request until the exact raw `info`
    // dictionary has matched the full magnet identity. In particular, the
    // 20-byte v2 wire truncation is not enough to authorize a piece-layer
    // exchange for an arbitrary SHA-256 torrent.
    if !identity.matches_info_bencoded(&assembled) {
        return Err(CoreError::MalformedTorrent(
            "BEP 9 metadata does not match the magnet identity".into(),
        ));
    }
    let piece_layers =
        if goal == MetadataFetchGoal::Resolved && matches!(identity, TorrentIdentity::V2 { .. }) {
            fetch_v2_piece_layers(&mut reader, &mut write_half, &assembled, identity).await?
        } else {
            Vec::new()
        };

    Ok(FetchedMetadata {
        raw_info: assembled,
        piece_layers,
    })
}

/// Retrieve every top-level BEP 52 piece layer needed to make a pure-v2
/// BEP-9 `info` dictionary executable.
///
/// The client fetches the entire logical layer in bounded (at most 512-hash)
/// requests and independently recomputes the file-tree root. This avoids
/// trusting a peer-provided proof while still supporting layers larger than a
/// single peer-wire message. The final parser repeats the validation when it
/// reconstructs the complete metainfo document.
async fn fetch_v2_piece_layers<R, W>(
    reader: &mut swarmotter_core::peer::PeerReader<R>,
    write_half: &mut W,
    info_bytes: &[u8],
    expected_identity: &TorrentIdentity,
) -> Result<Vec<V2PieceLayer>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let inspection = swarmotter_core::meta::inspect_bep9_v2_info(info_bytes)?.ok_or_else(|| {
        CoreError::MalformedTorrent(
            "pure-v2 metadata did not contain a BEP 52 file-tree identity".into(),
        )
    })?;
    let expected_v2 = expected_identity.v2_info_hash().ok_or_else(|| {
        CoreError::InvalidArgument(
            "BEP 52 piece-layer exchange requires a full v2 magnet identity".into(),
        )
    })?;
    if inspection.identity != expected_v2 {
        return Err(CoreError::MalformedTorrent(
            "BEP 9 v2 identity changed before piece-layer exchange".into(),
        ));
    }

    let mut piece_layers = Vec::new();
    for (pieces_root, expected_count) in inspection.required_piece_layers()? {
        if !(2..=MAX_TORRENT_PIECES).contains(&expected_count) {
            return Err(CoreError::MalformedTorrent(format!(
                "BEP 52 piece layer has invalid expected count {expected_count}"
            )));
        }
        let logical_width = expected_count.checked_next_power_of_two().ok_or_else(|| {
            CoreError::MalformedTorrent("BEP 52 piece-layer width exceeds platform limits".into())
        })?;
        let request_length = logical_width.min(METADATA_HASH_REQUEST_MAX);
        debug_assert!(request_length >= 2 && request_length.is_power_of_two());
        let full_layer_depth = logical_width.trailing_zeros();
        let mut hashes = Vec::with_capacity(expected_count);

        for start in (0..logical_width).step_by(request_length) {
            let index = u32::try_from(start).map_err(|_| {
                CoreError::MalformedTorrent("BEP 52 piece-layer index exceeds u32 range".into())
            })?;
            let length = u32::try_from(request_length).map_err(|_| {
                CoreError::MalformedTorrent(
                    "BEP 52 piece-layer request length exceeds u32 range".into(),
                )
            })?;
            let request = Message::HashRequest {
                pieces_root,
                base_layer: (inspection.piece_length / swarmotter_core::meta::V2_BLOCK_LENGTH)
                    .trailing_zeros(),
                index,
                length,
                // Ask for the complete proof depth. A full-width request has
                // no uncle hashes; bounded subrequests carry the deterministic
                // siblings required by BEP 52 while we assemble the whole
                // layer for independent root validation.
                proof_layers: full_layer_depth,
            };
            peer::write_message(write_half, &request).await?;
            write_half.flush().await.map_err(CoreError::from)?;
            let response_hashes = read_piece_layer_response(reader, &request).await?;
            let proof_hashes =
                usize::try_from(full_layer_depth.saturating_sub(request_length.trailing_zeros()))
                    .map_err(|_| {
                    CoreError::MalformedTorrent(
                        "BEP 52 piece-layer proof count exceeds platform limits".into(),
                    )
                })?;
            let expected_response_hashes =
                request_length.checked_add(proof_hashes).ok_or_else(|| {
                    CoreError::MalformedTorrent(
                        "BEP 52 piece-layer response length overflow".into(),
                    )
                })?;
            if response_hashes.len() != expected_response_hashes {
                return Err(CoreError::MalformedTorrent(format!(
                    "BEP 52 piece-layer response contained {} hashes; expected {expected_response_hashes}",
                    response_hashes.len(),
                )));
            }
            for (offset, hash) in response_hashes.into_iter().take(request_length).enumerate() {
                if start + offset < expected_count {
                    hashes.push(hash);
                }
            }
        }

        if hashes.len() != expected_count {
            return Err(CoreError::MalformedTorrent(format!(
                "BEP 52 piece-layer assembly contained {} hashes; expected {expected_count}",
                hashes.len()
            )));
        }
        let computed_root =
            swarmotter_core::meta::v2_piece_layer_root(&hashes, inspection.piece_length)?;
        if computed_root != pieces_root {
            return Err(CoreError::MalformedTorrent(
                "BEP 52 piece-layer hashes do not match the file-tree root".into(),
            ));
        }
        piece_layers.push(V2PieceLayer {
            pieces_root,
            hashes,
        });
    }
    Ok(piece_layers)
}

async fn read_piece_layer_response<R>(
    reader: &mut swarmotter_core::peer::PeerReader<R>,
    request: &Message,
) -> Result<Vec<swarmotter_core::hash::V2InfoHash>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let Message::HashRequest {
        pieces_root,
        base_layer,
        index,
        length,
        proof_layers,
    } = request
    else {
        return Err(CoreError::Internal(
            "piece-layer response requested for a non-hash message".into(),
        ));
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let message = match timeout(Duration::from_secs(15), reader.read_message()).await {
            Ok(Ok(Some(message))) => message,
            Ok(Ok(None)) => {
                return Err(CoreError::Internal(
                    "metadata peer closed during BEP 52 piece-layer exchange".into(),
                ));
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => continue,
        };
        match message {
            Message::Hashes {
                pieces_root: response_root,
                base_layer: response_base_layer,
                index: response_index,
                length: response_length,
                proof_layers: response_proof_layers,
                hashes,
            } => {
                if response_root != *pieces_root
                    || response_base_layer != *base_layer
                    || response_index != *index
                    || response_length != *length
                    || response_proof_layers != *proof_layers
                {
                    return Err(CoreError::MalformedTorrent(
                        "metadata peer returned a mismatched BEP 52 hash response".into(),
                    ));
                }
                return Ok(hashes);
            }
            Message::HashReject {
                pieces_root: response_root,
                base_layer: response_base_layer,
                index: response_index,
                length: response_length,
                proof_layers: response_proof_layers,
            } => {
                if response_root == *pieces_root
                    && response_base_layer == *base_layer
                    && response_index == *index
                    && response_length == *length
                    && response_proof_layers == *proof_layers
                {
                    return Err(CoreError::Internal(
                        "metadata peer rejected a required BEP 52 piece-layer request".into(),
                    ));
                }
                return Err(CoreError::MalformedTorrent(
                    "metadata peer returned a mismatched BEP 52 hash rejection".into(),
                ));
            }
            Message::Keepalive | Message::Extended { .. } => {}
            _ => {}
        }
    }
    Err(CoreError::Internal(
        "metadata peer timed out during BEP 52 piece-layer exchange".into(),
    ))
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

/// Resolve metadata from the first candidate that can supply a complete,
/// identity-verified result. Pure-v2 candidates must also supply every
/// required BEP 52 piece layer on the same contained peer session.
pub(crate) async fn fetch_resolved_metadata_from_candidates_with_budget(
    fetch_context: MetadataFetchContext,
    candidates: &[PeerAddr],
    trackers: &[String],
) -> Result<ResolvedMagnetMetadata> {
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
        spawn_resolved_metadata_fetch(
            &mut tasks,
            fetch_context.clone(),
            candidates[next],
            trackers.to_vec(),
        );
        next += 1;
    }

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((_, Ok(metadata))) => return Ok(metadata),
            Ok((peer, Err(error))) => last_err = Some(format!("{peer:?}: {error}")),
            Err(error) => last_err = Some(format!("metadata candidate task failed: {error}")),
        }
        while next < candidates.len() && tasks.len() < METADATA_CANDIDATE_CONCURRENCY {
            spawn_resolved_metadata_fetch(
                &mut tasks,
                fetch_context.clone(),
                candidates[next],
                trackers.to_vec(),
            );
            next += 1;
        }
    }
    Err(CoreError::Internal(format!(
        "metadata fetch failed from all candidates: {}",
        last_err.unwrap_or_else(|| "all candidate tasks ended without result".into())
    )))
}

fn spawn_resolved_metadata_fetch(
    tasks: &mut tokio::task::JoinSet<(PeerAddr, Result<ResolvedMagnetMetadata>)>,
    context: MetadataFetchContext,
    peer: PeerAddr,
    trackers: Vec<String>,
) {
    tasks.spawn(async move {
        let result = fetch_resolved_metadata_with_transport(&context, peer, &trackers).await;
        (peer, result)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_permits::PeerPermitPool;
    use std::sync::atomic::AtomicU64;
    use swarmotter_core::hash::V2InfoHash;
    use swarmotter_core::meta::{build_single_file_torrent, parse_torrent, V2_BLOCK_LENGTH};
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

    fn bencode_string(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(value.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(value);
    }

    fn bencode_integer(out: &mut Vec<u8>, value: u64) {
        out.push(b'i');
        out.extend_from_slice(value.to_string().as_bytes());
        out.push(b'e');
    }

    /// Generate a small lawful BEP 52 `info` dictionary with three logical
    /// pieces. The non-power-of-two layer count exercises padded hash-layer
    /// retrieval rather than merely the easy two-piece case.
    fn v2_bep9_fixture(
        hybrid: bool,
    ) -> (Vec<u8>, TorrentIdentity, V2InfoHash, Vec<V2InfoHash>, u64) {
        // Use a two-leaf logical piece so padded layer entries are the
        // base-layer zero subtree, not a raw all-zero SHA-256 hash.
        let piece_length = V2_BLOCK_LENGTH * 2;
        let content = (0..((piece_length * 2 + 17) as usize))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let piece_hashes = content
            .chunks(piece_length as usize)
            .map(|piece| swarmotter_core::v2::v2_piece_root(piece, piece_length).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(piece_hashes.len(), 3);
        let pieces_root =
            swarmotter_core::meta::v2_piece_layer_root(&piece_hashes, piece_length).unwrap();
        let name = b"v2-bep9-fixture.bin";

        let mut info = Vec::new();
        info.push(b'd');
        bencode_string(&mut info, b"file tree");
        info.push(b'd');
        bencode_string(&mut info, name);
        info.push(b'd');
        bencode_string(&mut info, b"");
        info.push(b'd');
        bencode_string(&mut info, b"length");
        bencode_integer(&mut info, content.len() as u64);
        bencode_string(&mut info, b"pieces root");
        bencode_string(&mut info, pieces_root.as_bytes());
        info.extend_from_slice(b"eee");
        if hybrid {
            bencode_string(&mut info, b"length");
            bencode_integer(&mut info, content.len() as u64);
        }
        bencode_string(&mut info, b"meta version");
        bencode_integer(&mut info, 2);
        bencode_string(&mut info, b"name");
        bencode_string(&mut info, name);
        bencode_string(&mut info, b"piece length");
        bencode_integer(&mut info, piece_length);
        if hybrid {
            let v1_torrent = build_single_file_torrent(
                "v2-bep9-fixture.bin",
                &content,
                piece_length,
                None,
                false,
            );
            let v1_meta = parse_torrent(&v1_torrent).unwrap();
            let mut v1_pieces = Vec::with_capacity(v1_meta.pieces.len() * 20);
            for hash in &v1_meta.pieces {
                v1_pieces.extend_from_slice(hash);
            }
            bencode_string(&mut info, b"pieces");
            bencode_string(&mut info, &v1_pieces);
        }
        info.push(b'e');

        let v2 = V2InfoHash::from_info_bencoded(&info);
        let identity = if hybrid {
            TorrentIdentity::hybrid(InfoHash::from_info_bencoded(&info), v2)
        } else {
            TorrentIdentity::v2(v2)
        };
        (info, identity, pieces_root, piece_hashes, piece_length)
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

    /// A contained BEP 52 peer fixture that serves both BEP 9 raw `info`
    /// bytes and the BEP 52 top-level piece layer required by pure-v2
    /// metadata. It deliberately uses three logical pieces so the client must
    /// request and discard a padded fourth hash before validating the root.
    async fn serve_v2_metadata_peer(
        stream: tokio::net::TcpStream,
        wire_hash: PeerInfoHash,
        info_bytes: Vec<u8>,
        pieces_root: V2InfoHash,
        piece_hashes: Vec<V2InfoHash>,
        piece_length: u64,
        reject_piece_layers: bool,
    ) -> swarmotter_core::Result<()> {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut raw_handshake = [0u8; 68];
        rd.read_exact(&mut raw_handshake).await?;
        let their_handshake = V2Handshake::decode(&raw_handshake)
            .map_err(|error| swarmotter_core::error::CoreError::Internal(error.to_string()))?;
        if their_handshake.info_hash != wire_hash || !their_handshake.supports_v2() {
            return Err(swarmotter_core::error::CoreError::Internal(
                "pure-v2 metadata handshake mismatch".into(),
            ));
        }
        let our_handshake = V2Handshake {
            info_hash: wire_hash,
            peer_id: peer_id(b"-SDV200-"),
            reserved: peer::with_v2_support(extensions::EXTENSION_RESERVED),
        };
        wr.write_all(&our_handshake.encode()).await?;

        let local_metadata_id = 1u8;
        let extension_handshake = extensions::encode_extension_handshake(
            &[(extensions::UT_METADATA_NAME, local_metadata_id)],
            "MetaV2Seed/0.1",
            Some(info_bytes.len() as u64),
        );
        peer::write_message(
            &mut wr,
            &Message::Extended {
                id: extensions::EXTENSION_HANDSHAKE_ID,
                payload: extension_handshake,
            },
        )
        .await?;
        wr.flush().await?;

        let mut leecher_metadata_id = local_metadata_id;
        let total = info_bytes.len();
        let padded_width = piece_hashes
            .len()
            .checked_next_power_of_two()
            .ok_or_else(|| {
                swarmotter_core::error::CoreError::Internal("fixture width overflow".into())
            })?;
        let base_layer = (piece_length / V2_BLOCK_LENGTH).trailing_zeros();
        let mut padding = V2InfoHash::ZERO;
        for _ in 0..base_layer {
            padding = swarmotter_core::v2::v2_hash_pair(padding, padding);
        }
        let mut padded_hashes = piece_hashes;
        padded_hashes.resize(padded_width, padding);

        loop {
            let message = match read_one_message(&mut rd).await {
                Ok(Some(message)) => message,
                _ => return Ok(()),
            };
            match message {
                Message::Extended { id, payload } if id == extensions::EXTENSION_HANDSHAKE_ID => {
                    if let Ok(handshake) = extensions::parse_extension_handshake(&payload) {
                        if let Some(id) = handshake.id_for(extensions::UT_METADATA_NAME) {
                            leecher_metadata_id = id;
                        }
                    }
                }
                Message::Extended { id, payload } if id != extensions::EXTENSION_HANDSHAKE_ID => {
                    if let Ok(metadata) = extensions::parse_metadata_message(&payload) {
                        if metadata.msg_type == MetadataMsgType::Request {
                            let start = (metadata.piece as usize)
                                .checked_mul(extensions::METADATA_PIECE_SIZE)
                                .ok_or_else(|| {
                                    swarmotter_core::error::CoreError::Internal(
                                        "fixture metadata offset overflow".into(),
                                    )
                                })?;
                            let end = start
                                .checked_add(extensions::METADATA_PIECE_SIZE)
                                .map_or(total, |end| end.min(total));
                            let data = info_bytes.get(start..end).ok_or_else(|| {
                                swarmotter_core::error::CoreError::Internal(
                                    "fixture received invalid metadata piece request".into(),
                                )
                            })?;
                            peer::write_message(
                                &mut wr,
                                &Message::Extended {
                                    id: leecher_metadata_id,
                                    payload: extensions::encode_metadata_data(
                                        metadata.piece,
                                        total as u64,
                                        data,
                                    ),
                                },
                            )
                            .await?;
                            wr.flush().await?;
                        }
                    }
                }
                Message::HashRequest {
                    pieces_root: requested_root,
                    base_layer,
                    index,
                    length,
                    proof_layers,
                } => {
                    if requested_root != pieces_root
                        || base_layer != (piece_length / V2_BLOCK_LENGTH).trailing_zeros()
                        || proof_layers != padded_width.trailing_zeros()
                    {
                        return Err(swarmotter_core::error::CoreError::Internal(
                            "fixture received invalid BEP 52 hash request".into(),
                        ));
                    }
                    if reject_piece_layers {
                        peer::write_message(
                            &mut wr,
                            &Message::HashReject {
                                pieces_root,
                                base_layer,
                                index,
                                length,
                                proof_layers,
                            },
                        )
                        .await?;
                        wr.flush().await?;
                        continue;
                    }
                    let start = usize::try_from(index).map_err(|_| {
                        swarmotter_core::error::CoreError::Internal(
                            "fixture hash request index overflow".into(),
                        )
                    })?;
                    let length_usize = usize::try_from(length).map_err(|_| {
                        swarmotter_core::error::CoreError::Internal(
                            "fixture hash request length overflow".into(),
                        )
                    })?;
                    let end = start.checked_add(length_usize).ok_or_else(|| {
                        swarmotter_core::error::CoreError::Internal(
                            "fixture hash request range overflow".into(),
                        )
                    })?;
                    let hashes = padded_hashes.get(start..end).ok_or_else(|| {
                        swarmotter_core::error::CoreError::Internal(
                            "fixture received out-of-range BEP 52 hash request".into(),
                        )
                    })?;
                    peer::write_message(
                        &mut wr,
                        &Message::Hashes {
                            pieces_root,
                            base_layer,
                            index,
                            length,
                            proof_layers,
                            hashes: hashes.to_vec(),
                        },
                    )
                    .await?;
                    wr.flush().await?;
                }
                _ => {}
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_v2_metadata_exchange_uses_bep52_handshake_and_verified_piece_layers() {
        let (info_bytes, identity, pieces_root, piece_hashes, piece_length) =
            v2_bep9_fixture(false);
        let wire_hash = identity
            .v2_info_hash()
            .expect("pure-v2 fixture has a full SHA-256 identity")
            .peer_info_hash();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let served_info = info_bytes.clone();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_v2_metadata_peer(
                    stream,
                    wire_hash,
                    served_info,
                    pieces_root,
                    piece_hashes,
                    piece_length,
                    false,
                )
                .await;
            }
        });

        let context = MetadataFetchContext::for_identity(
            PeerSessionBudget::unlimited(),
            Arc::new(LoopbackBinder),
            identity.clone(),
            peer_id(b"-SWV2MD-"),
            false,
            true,
            PeerEncryptionMode::Disabled,
        );
        let resolved = fetch_resolved_metadata_with_transport(
            &context,
            PeerAddr::from_socket_addr(address),
            &["https://tracker.example/announce".into()],
        )
        .await
        .expect("contained pure-v2 metadata exchange must resolve");

        assert_eq!(resolved.raw_info, info_bytes);
        assert_eq!(resolved.meta.identity, identity);
        assert!(resolved.meta.requires_v2_data_plane());
        let v2 = resolved
            .meta
            .v2
            .as_ref()
            .expect("pure-v2 metadata retained");
        assert!(v2.piece_layers_verified);
        assert_eq!(v2.piece_layers.len(), 1);
        assert_eq!(v2.piece_layers[0].pieces_root, pieces_root);
        assert_eq!(v2.piece_layers[0].hashes.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_v2_metadata_rejects_piece_layer_that_does_not_match_its_file_tree_root() {
        let (info_bytes, identity, pieces_root, mut piece_hashes, piece_length) =
            v2_bep9_fixture(false);
        let mut tampered = *piece_hashes[1].as_bytes();
        tampered[0] ^= 0x80;
        piece_hashes[1] = V2InfoHash::from_bytes(tampered);
        let wire_hash = identity.v2_info_hash().unwrap().peer_info_hash();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_v2_metadata_peer(
                    stream,
                    wire_hash,
                    info_bytes,
                    pieces_root,
                    piece_hashes,
                    piece_length,
                    false,
                )
                .await;
            }
        });

        let context = MetadataFetchContext::for_identity(
            PeerSessionBudget::unlimited(),
            Arc::new(LoopbackBinder),
            identity,
            peer_id(b"-SWV2ER-"),
            false,
            true,
            PeerEncryptionMode::Disabled,
        );
        let error = fetch_resolved_metadata_with_transport(
            &context,
            PeerAddr::from_socket_addr(address),
            &[],
        )
        .await
        .expect_err("unverified BEP 52 layers must not become executable metadata");
        assert!(matches!(error, CoreError::MalformedTorrent(_)));
        assert!(error
            .to_string()
            .contains("piece-layer hashes do not match the file-tree root"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_v2_metadata_fails_closed_when_peer_rejects_required_piece_layer() {
        let (info_bytes, identity, pieces_root, piece_hashes, piece_length) =
            v2_bep9_fixture(false);
        let wire_hash = identity.v2_info_hash().unwrap().peer_info_hash();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_v2_metadata_peer(
                    stream,
                    wire_hash,
                    info_bytes,
                    pieces_root,
                    piece_hashes,
                    piece_length,
                    true,
                )
                .await;
            }
        });

        let context = MetadataFetchContext::for_identity(
            PeerSessionBudget::unlimited(),
            Arc::new(LoopbackBinder),
            identity,
            peer_id(b"-SWV2RJ-"),
            false,
            true,
            PeerEncryptionMode::Disabled,
        );
        let error = fetch_resolved_metadata_with_transport(
            &context,
            PeerAddr::from_socket_addr(address),
            &[],
        )
        .await
        .expect_err("required pure-v2 piece layers must fail closed when rejected");
        assert!(matches!(error, CoreError::Internal(_)));
        assert!(error
            .to_string()
            .contains("rejected a required BEP 52 piece-layer request"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hybrid_metadata_exchange_keeps_v1_wire_and_validates_both_full_identities() {
        let (info_bytes, identity, _pieces_root, _piece_hashes, _piece_length) =
            v2_bep9_fixture(true);
        let v1 = identity
            .v1_info_hash()
            .expect("hybrid fixture has a v1 compatibility identity");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let served_info = info_bytes.clone();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_metadata_peer(stream, v1, served_info).await;
            }
        });

        let context = MetadataFetchContext::for_identity(
            PeerSessionBudget::unlimited(),
            Arc::new(LoopbackBinder),
            identity.clone(),
            peer_id(b"-SWHYMD-"),
            false,
            true,
            PeerEncryptionMode::Disabled,
        );
        let resolved = fetch_resolved_metadata_with_transport(
            &context,
            PeerAddr::from_socket_addr(address),
            &[],
        )
        .await
        .expect("hybrid metadata must resolve over its v1 compatibility swarm");
        assert_eq!(resolved.raw_info, info_bytes);
        assert_eq!(resolved.meta.identity, identity);
        assert!(matches!(
            resolved.meta.identity,
            TorrentIdentity::Hybrid { .. }
        ));
        assert!(
            !resolved
                .meta
                .v2
                .as_ref()
                .expect("hybrid v2 metadata retained")
                .piece_layers_verified,
            "hybrid payload can use its verified v1 layout before optional v2 layers are needed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hybrid_metadata_rejects_wrong_full_v2_identity_even_when_v1_handshake_matches() {
        let (info_bytes, identity, _pieces_root, _piece_hashes, _piece_length) =
            v2_bep9_fixture(true);
        let v1 = identity.v1_info_hash().unwrap();
        let mut wrong_v2 = *identity.v2_info_hash().unwrap().as_bytes();
        // Preserve the first 20 bytes so any accidental use of a truncated
        // v2 identity would still pass the peer-wire boundary.
        wrong_v2[31] ^= 0x80;
        let wrong_identity = TorrentIdentity::hybrid(v1, V2InfoHash::from_bytes(wrong_v2));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = serve_metadata_peer(stream, v1, info_bytes).await;
            }
        });

        let context = MetadataFetchContext::for_identity(
            PeerSessionBudget::unlimited(),
            Arc::new(LoopbackBinder),
            wrong_identity,
            peer_id(b"-SWHYER-"),
            false,
            true,
            PeerEncryptionMode::Disabled,
        );
        let error = fetch_metadata_with_transport(&context, PeerAddr::from_socket_addr(address))
            .await
            .expect_err("a full-v2 mismatch must not be accepted through the hybrid v1 wire");
        assert!(matches!(error, CoreError::MalformedTorrent(_)));
        assert!(error
            .to_string()
            .contains("does not match the magnet identity"));
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
        assert!(err
            .to_string()
            .contains("does not match the magnet identity"));
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
