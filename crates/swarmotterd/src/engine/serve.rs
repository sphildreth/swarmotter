// SPDX-License-Identifier: Apache-2.0

//! Upload serving for active (incomplete) downloads (ADR-0075).
//!
//! An established outbound peer session of a downloading torrent serves
//! verified pieces to the remote peer over the same contained connection,
//! using the same protocol, storage, and bandwidth-limiting abstractions as
//! completed-seed serving. This lets two complementary incomplete nodes
//! finish without a complete seed and lets a third peer request a verified
//! piece while the torrent is still downloading.
//!
//! Guarantees:
//! - Only pieces that passed verification and completed their payload write
//!   are advertised or served. Requests for unverified or out-of-range data
//!   are ignored, never answered with unverified bytes.
//! - A bidirectional session holds exactly one peer-session permit for its
//!   whole lifetime and shapes both directions through the torrent's existing
//!   shaped limiter (per-torrent plus global). Uploaded bytes are accounted
//!   exactly once on the engine state.
//! - Inbound request work is bounded: at most
//!   [`MAX_INBOUND_SERVE_QUEUE`] queued block responses per session, each at
//!   most [`MAX_SERVE_BLOCK_LENGTH`] bytes, and a bounded serve drain per
//!   read-loop iteration so uploads never starve the download side of the
//!   session.
//! - Download-time uploading never changes torrent state: it cannot convert
//!   a downloading torrent to `Seeding`, does not register a completed-seed
//!   slot, and does not create a second listener or registry entry.

use super::*;

/// Maximum queued inbound block responses per download session.
pub(super) const MAX_INBOUND_SERVE_QUEUE: usize = 32;

/// Maximum size of a single served block. Standard requests are 16 KiB;
/// this cap admits common fast-extension-sized requests while rejecting
/// absurd or malicious lengths.
pub(super) const MAX_SERVE_BLOCK_LENGTH: usize = 128 * 1024;

/// Serve at most this many queued blocks per read-loop iteration so request
/// floods cannot monopolize a session.
pub(super) const SERVE_DRAIN_PER_ITERATION: usize = 2;

/// Bounded queue of validated inbound block requests for one peer session.
#[derive(Default)]
pub(super) struct InboundUploadQueue {
    queue: std::collections::VecDeque<(u32, u32, u32)>,
}

impl InboundUploadQueue {
    /// Queue a validated inbound `Request`. Returns `true` when the request
    /// will be served; `false` when it is ignored (unverified piece,
    /// out-of-range geometry, oversized block, or saturated queue).
    pub(super) fn offer(
        &mut self,
        piece: u32,
        offset: u32,
        length: u32,
        have: &PieceBitfield,
        piece_count: usize,
        piece_length_of: impl Fn(usize) -> u64,
    ) -> bool {
        let index = piece as usize;
        if index >= piece_count || !have.has(index) {
            return false;
        }
        if length == 0 || length as usize > MAX_SERVE_BLOCK_LENGTH {
            return false;
        }
        if u64::from(offset) + u64::from(length) > piece_length_of(index) {
            return false;
        }
        if self.queue.len() >= MAX_INBOUND_SERVE_QUEUE {
            return false;
        }
        self.queue.push_back((piece, offset, length));
        true
    }

    /// Honor an inbound `Cancel` for a queued response.
    pub(super) fn cancel(&mut self, piece: u32, offset: u32, length: u32) {
        self.queue
            .retain(|&(p, o, l)| (p, o, l) != (piece, offset, length));
    }

    pub(super) fn clear(&mut self) {
        self.queue.clear();
    }

    /// Serve one queued request: read the verified piece bytes through the
    /// storage write/read boundary, shape the upload through the torrent's
    /// limiter, send the `Piece` message, and account the bytes exactly once.
    pub(super) async fn serve_one<W>(
        &mut self,
        write_half: &mut W,
        storage: &StorageIo,
        state: &Arc<Mutex<EngineState>>,
        peer_addr: PeerAddr,
        limiter: &ShapedLimiter,
    ) -> Result<bool>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let Some((piece, offset, length)) = self.queue.pop_front() else {
            return Ok(false);
        };
        let block = match storage
            .read_block(piece as usize, u64::from(offset), length as usize)
            .await
        {
            Ok(block) => block,
            // A transient storage error (missing file, I/O pressure) drops
            // the request without ending the session; the peer re-requests.
            Err(_) => return Ok(true),
        };
        limiter
            .acquire(RateDirection::Upload, block.len() as u64)
            .await;
        peer::write_message(
            write_half,
            &Message::Piece {
                piece,
                offset,
                block,
            },
        )
        .await?;
        record_peer_uploaded(state, peer_addr, u64::from(length)).await;
        Ok(true)
    }

    /// Serve up to [`SERVE_DRAIN_PER_ITERATION`] queued requests.
    pub(super) async fn serve_bounded<W>(
        &mut self,
        write_half: &mut W,
        storage: &StorageIo,
        state: &Arc<Mutex<EngineState>>,
        peer_addr: PeerAddr,
        limiter: &ShapedLimiter,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        for _ in 0..SERVE_DRAIN_PER_ITERATION {
            if !self
                .serve_one(write_half, storage, state, peer_addr, limiter)
                .await?
            {
                break;
            }
        }
        Ok(())
    }
}

/// Send `Have` messages for verified pieces this session has not yet
/// advertised. Availability updates fan out through the shared piece state
/// with a bounded per-iteration cap.
pub(super) async fn announce_new_verified_pieces<W>(
    write_half: &mut W,
    shared: &Arc<Mutex<ParallelPieceState>>,
    announced: &mut PieceBitfield,
    piece_count: usize,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let snapshot = {
        let work = shared.lock().await;
        work.have.clone()
    };
    // Bound the fan-out per iteration; remaining updates are picked up on
    // subsequent iterations of the session loop.
    let mut sent = 0usize;
    for piece in 0..piece_count {
        if snapshot.has(piece) && !announced.has(piece) {
            peer::write_message(
                write_half,
                &Message::Have {
                    piece: u32::try_from(piece).map_err(|_| {
                        CoreError::MalformedTorrent("piece index exceeds peer-wire range".into())
                    })?,
                },
            )
            .await?;
            announced.set(piece);
            sent += 1;
            if sent >= 64 {
                break;
            }
        }
    }
    Ok(())
}
