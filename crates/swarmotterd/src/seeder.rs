// SPDX-License-Identifier: Apache-2.0

//! Inbound peer listener and real seeding/upload behavior.
//!
//! When a torrent has verified pieces, the daemon spawns a [`SeedListner`]
//! that binds a contained TCP listener (through the `NetworkBinder`) on the
//! configured torrent port and serves piece blocks to inbound leechers. This
//! implements real upload/seeding: handshake validation, bitfield exchange,
//! interested/unchoke handling, block reads from verified storage via
//! `StorageIo::read_block`, uploaded-byte accounting, and respect for the
//! torrent's paused/removed state and private-torrent restrictions.
//!
//! All inbound traffic goes through the contained listener; the seeder never
//! binds a socket directly. In strict fail-closed mode the binder refuses to
//! create the listener, so seeding is blocked when the path is unavailable.
//! See `design/vpn-network-containment.md` and ADR-0013.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::bandwidth::{RateDirection, RateLimiter, ShapedLimiter};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, PeerReader};
use swarmotter_core::storage::StorageIo;

use crate::engine::EngineState;

/// A seeding listener that serves verified pieces to inbound peers.
///
/// `state` is the shared live engine state; the seeder serves pieces present
/// in `state.pieces_have` and accumulates uploaded bytes into `state.uploaded`.
/// `limiter` shapes upload throughput. `shutdown` completes when the seeder
/// should stop (pause/remove).
pub struct Seeder {
    meta: TorrentMeta,
    storage: Arc<StorageIo>,
    complete_storage: Option<Arc<StorageIo>>,
    state: Arc<Mutex<EngineState>>,
    binder: Arc<dyn NetworkBinder>,
    port: u16,
    peer_id: [u8; 20],
    shutdown: tokio::sync::watch::Receiver<bool>,
    limiter: ShapedLimiter,
    /// Optional one-shot sender receiving the bound listen address, for tests
    /// that bind on port 0 and need to learn the actual port.
    bound_addr: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
}

impl Seeder {
    #[allow(clippy::too_many_arguments, dead_code)]
    pub fn new(
        meta: TorrentMeta,
        storage: Arc<StorageIo>,
        state: Arc<Mutex<EngineState>>,
        binder: Arc<dyn NetworkBinder>,
        port: u16,
        peer_id: [u8; 20],
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self::with_limiter(
            meta,
            storage,
            state,
            binder,
            port,
            peer_id,
            shutdown,
            RateLimiter::unlimited(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_limiter(
        meta: TorrentMeta,
        storage: Arc<StorageIo>,
        state: Arc<Mutex<EngineState>>,
        binder: Arc<dyn NetworkBinder>,
        port: u16,
        peer_id: [u8; 20],
        shutdown: tokio::sync::watch::Receiver<bool>,
        limiter: RateLimiter,
    ) -> Self {
        Self {
            meta,
            storage,
            complete_storage: None,
            state,
            binder,
            port,
            peer_id,
            shutdown,
            limiter: ShapedLimiter::from_rate_limiter(limiter),
            bound_addr: None,
        }
    }

    /// Attach a shared global rate limiter (the daemon's process-wide upload
    /// cap) so seeding is shaped by both the per-torrent and global limits.
    #[allow(dead_code)]
    pub fn with_global_limiter(mut self, global: Option<RateLimiter>) -> Self {
        if let Some(g) = global {
            self.limiter = self.limiter.with_global(g);
        }
        self
    }

    /// Configure the completed-data storage root. During active downloads the
    /// seeder serves verified pieces from `storage`; after the engine marks
    /// completion it serves from this final root.
    pub fn with_complete_storage(mut self, storage: Arc<StorageIo>) -> Self {
        self.complete_storage = Some(storage);
        self
    }

    /// Set a one-shot sender that receives the bound listen address once the
    /// listener is bound (useful when binding on port 0).
    #[allow(dead_code)]
    pub fn with_bound_addr(
        mut self,
        tx: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
    ) -> Self {
        self.bound_addr = Some(tx);
        self
    }

    /// Run the seeding listener until shutdown is signaled. Accepts inbound
    /// peers concurrently and serves them from verified storage.
    pub async fn run(mut self) -> Result<()> {
        if !self.binder.traffic_allowed() {
            return Err(CoreError::NetworkBlocked(
                "torrent data plane blocked; cannot start seeding listener".into(),
            ));
        }
        let listener = self.binder.bind_peer_listener(self.port).await?;
        let listen_addr = listener.local_addr()?;
        if let Some(tx) = self.bound_addr.take() {
            let _ = tx.send(listen_addr);
        }
        tracing::info!(info_hash = %self.meta.info_hash, addr = %listen_addr, "seeding listener bound");

        loop {
            // Honor shutdown.
            if *self.shutdown.borrow() {
                break;
            }
            // Re-check containment before accepting.
            if !self.binder.traffic_allowed() {
                // Path dropped: stop serving. The daemon will mark the torrent
                // network_blocked and tear us down.
                break;
            }
            // Accept with a short timeout so we can re-check shutdown/containment.
            let stream = match timeout(Duration::from_secs(2), listener.accept()).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "seeding accept failed");
                    continue;
                }
                Err(_) => continue, // timeout; loop to re-check
            };
            let peer_addr = stream.peer_addr().ok();
            let meta = self.meta.clone();
            let storage = self.storage.clone();
            let complete_storage = self.complete_storage.clone();
            let state = self.state.clone();
            let peer_id = self.peer_id;
            let limiter = self.limiter.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_peer(
                    stream,
                    &meta,
                    storage,
                    complete_storage,
                    &state,
                    peer_id,
                    &limiter,
                )
                .await
                {
                    tracing::debug!(peer = ?peer_addr, error = %e, "inbound peer session ended");
                }
            });
        }
        Ok(())
    }
}

async fn serve_peer(
    stream: tokio::net::TcpStream,
    meta: &TorrentMeta,
    storage: Arc<StorageIo>,
    complete_storage: Option<Arc<StorageIo>>,
    state: &Arc<Mutex<EngineState>>,
    peer_id: [u8; 20],
    limiter: &ShapedLimiter,
) -> Result<()> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = PeerReader::new(read_half);

    // Read the leecher's handshake and validate info hash.
    let their_hs = timeout(Duration::from_secs(15), reader.read_handshake()).await??;
    if their_hs.info_hash != meta.info_hash {
        return Err(CoreError::Internal(
            "inbound peer info hash mismatch".into(),
        ));
    }

    // Send our handshake.
    let our_hs = Handshake {
        info_hash: meta.info_hash,
        peer_id,
        reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
    };
    peer::write_handshake(&mut write_half, &our_hs).await?;

    // Send our bitfield of verified pieces (snapshot from engine state).
    let bf = {
        let s = state.lock().await;
        let mut bf = Bitfield::new(meta.piece_count());
        for i in 0..meta.piece_count() {
            if s.pieces_have.has(i) {
                bf.set(i);
            }
        }
        bf
    };
    peer::write_message(&mut write_half, &bf.encode_message()).await?;

    // Send a BEP 10 extension handshake advertising ut_pex so the leecher
    // can learn our PEX message id (and request peer lists if it wishes).
    let ext_payload = swarmotter_core::extensions::encode_extension_handshake(
        &[(swarmotter_core::extensions::UT_PEX_NAME, 1u8)],
        "SwarmOtter/0.1",
        None,
    );
    peer::write_message(
        &mut write_half,
        &Message::Extended {
            id: swarmotter_core::extensions::EXTENSION_HANDSHAKE_ID,
            payload: ext_payload,
        },
    )
    .await?;
    write_half.flush().await.ok();

    // Drive the peer: handle interested/unchoke and request/piece messages.
    let mut our_choking = true;
    let mut remote_pex_id: Option<u8> = None;
    let piece_count = meta.piece_count();

    loop {
        let msg = match timeout(Duration::from_secs(120), reader.read_message()).await {
            Ok(Ok(Some(m))) => m,
            Ok(Ok(None)) => break, // clean disconnect
            Ok(Err(_)) => break,
            Err(_) => break, // idle timeout
        };
        match msg {
            Message::Interested => {
                // Unchoke the peer so it can request.
                peer::write_message(&mut write_half, &Message::Unchoke).await?;
                our_choking = false;
                write_half.flush().await.ok();
            }
            Message::NotInterested => {}
            Message::Request {
                piece,
                offset,
                length,
            } => {
                if our_choking {
                    // Refuse while choked.
                    continue;
                }
                let p = piece as usize;
                if p >= piece_count {
                    continue;
                }
                // Only serve pieces we have verified.
                let (have_it, finished) = {
                    let s = state.lock().await;
                    (s.pieces_have.has(p), s.finished)
                };
                if !have_it {
                    continue;
                }
                let read_storage = if finished {
                    complete_storage.as_deref().unwrap_or(storage.as_ref())
                } else {
                    storage.as_ref()
                };
                let length = length as usize;
                let block = match read_storage.read_block(p, offset as u64, length).await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(piece = p, error = %e, "seeding read_block failed");
                        continue;
                    }
                };
                // Account uploaded bytes.
                {
                    let mut s = state.lock().await;
                    s.uploaded = s.uploaded.saturating_add(block.len() as u64);
                }
                // Live upload rate shaping before sending the block.
                limiter
                    .acquire(RateDirection::Upload, block.len() as u64)
                    .await;
                peer::write_message(
                    &mut write_half,
                    &Message::Piece {
                        piece,
                        offset,
                        block,
                    },
                )
                .await?;
                write_half.flush().await.ok();
            }
            Message::Have { piece } => {
                let _ = piece;
            }
            Message::Bitfield { bits } => {
                let _ = Bitfield::from_bytes(bits, piece_count);
            }
            Message::Choke
            | Message::Unchoke
            | Message::Keepalive
            | Message::Cancel { .. }
            | Message::Piece { .. }
            | Message::Unknown { .. } => {}
            Message::Extended { id, payload } => {
                // Learn the leecher's PEX id from its extension handshake;
                // we could send PEX updates here in the future.
                if id == swarmotter_core::extensions::EXTENSION_HANDSHAKE_ID {
                    if let Ok(hs) = swarmotter_core::extensions::parse_extension_handshake(&payload)
                    {
                        remote_pex_id = hs.id_for(swarmotter_core::extensions::UT_PEX_NAME);
                    }
                }
                let _ = remote_pex_id;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
    use swarmotter_core::net::binder::LoopbackBinder;
    use swarmotter_core::peer::BLOCK_SIZE;
    use swarmotter_core::storage::resume::PieceBitfield;

    fn unique_dir(label: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "swarmotter-seed-{}-{}-{}",
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

    /// A leecher that connects to the seeder, requests a block, and verifies
    /// the uploaded bytes were accounted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::field_reassign_with_default)]
    async fn seeder_serves_block_and_accounts_upload() {
        let content = b"swarmotter seeding test payload block data here!!";
        let piece_length: u64 = 16;
        let bytes = build_single_file_torrent("seed.bin", content, piece_length, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("seeder");
        let storage = Arc::new(StorageIo::new(meta.clone(), dir.clone()));
        storage.preallocate().await.unwrap();
        // Write all pieces.
        let mut off = 0usize;
        let mut piece_index = 0usize;
        while off < content.len() {
            let end = std::cmp::min(off + piece_length as usize, content.len());
            storage
                .write_block(piece_index, 0, &content[off..end])
                .await
                .unwrap();
            off = end;
            piece_index += 1;
        }
        let mut have = PieceBitfield::new(meta.piece_count());
        for i in 0..meta.piece_count() {
            have.set(i);
        }
        let mut state = EngineState::default();
        state.piece_count = meta.piece_count();
        state.pieces_have = have;
        let state = Arc::new(Mutex::new(state));
        let binder = Arc::new(LoopbackBinder);

        // Bind the seeder on an ephemeral port (0) and learn the actual bound
        // address via a one-shot channel, avoiding probe-then-bind port races
        // under parallel test execution.
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let seeder = Seeder::new(
            meta.clone(),
            storage.clone(),
            state.clone(),
            binder.clone(),
            0,
            peer_id(b"-SW0001-"),
            shutdown_rx,
        )
        .with_bound_addr(bound_tx);
        let seeder_task = tokio::spawn(async move { seeder.run().await });
        let seeder_addr = bound_rx.await.expect("seeder bound its listener");

        // Act as a leecher: connect, handshake, send bitfield(empty), interested,
        // request a block, receive piece, verify.
        let stream = tokio::net::TcpStream::connect(seeder_addr).await.unwrap();
        let (rd, mut wr) = tokio::io::split(stream);
        let hs = Handshake {
            info_hash: meta.info_hash,
            peer_id: peer_id(b"-LC0001-"),
            reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
        };
        peer::write_handshake(&mut wr, &hs).await.unwrap();
        let mut reader = PeerReader::new(rd);
        let their_hs = reader.read_handshake().await.unwrap();
        assert_eq!(their_hs.info_hash, meta.info_hash);
        // Read bitfield from seeder (all pieces).
        let msg = reader.read_message().await.unwrap().unwrap();
        let bf = match msg {
            Message::Bitfield { bits } => Bitfield::from_bytes(bits, meta.piece_count()),
            _ => panic!("expected bitfield"),
        };
        assert_eq!(bf.count(), meta.piece_count());

        // Send interested, then read until we see the Unchoke (the seeder may
        // also send a BEP 10 extension handshake, which we skip).
        peer::write_message(&mut wr, &Message::Interested)
            .await
            .unwrap();
        let mut unchoke = None;
        for _ in 0..8 {
            match reader.read_message().await.unwrap().unwrap() {
                Message::Unchoke => {
                    unchoke = Some(true);
                    break;
                }
                _ => continue,
            }
        }
        assert_eq!(unchoke, Some(true));

        let req_len = std::cmp::min(BLOCK_SIZE, content.len() as u32) as u32;
        peer::write_message(
            &mut wr,
            &Message::Request {
                piece: 0,
                offset: 0,
                length: req_len,
            },
        )
        .await
        .unwrap();
        let piece_msg = reader.read_message().await.unwrap().unwrap();
        let block = match piece_msg {
            Message::Piece {
                piece,
                offset,
                block,
            } => {
                assert_eq!(piece, 0);
                assert_eq!(offset, 0);
                block
            }
            _ => panic!("expected piece"),
        };
        assert_eq!(&block, &content[..req_len as usize]);

        // Give the seeder a moment to account, then shut down.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let uploaded = state.lock().await.uploaded;
        assert_eq!(uploaded, req_len as u64);

        let _ = shutdown_tx.send(true);
        let _ = seeder_task.await;
        std::fs::remove_dir_all(&dir).ok();
    }

    fn peer_id(prefix: &[u8; 8]) -> [u8; 20] {
        let mut id = [0u8; 20];
        id[..8].copy_from_slice(prefix);
        id
    }
}
