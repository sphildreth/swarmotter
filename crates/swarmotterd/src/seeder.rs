// SPDX-License-Identifier: Apache-2.0

//! Inbound peer listener and real seeding/upload behavior.
//!
//! The daemon runs one [`SeederHub`] that binds a contained TCP listener
//! (through the `NetworkBinder`) on the configured torrent port and routes
//! inbound peers to registered torrents. This implements real upload/seeding:
//! handshake validation, bitfield exchange, interested/unchoke handling,
//! block reads from verified storage via `StorageIo::read_block`, uploaded-byte
//! accounting, and respect for each torrent's paused/removed state.
//!
//! All inbound traffic goes through the contained listener; the seeder never
//! binds a socket directly. In strict fail-closed mode the binder refuses to
//! create the listener, so seeding is blocked when the path is unavailable.
//! See `design/vpn-network-containment.md` and ADR-0013.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tokio::time::timeout;

use swarmotter_core::bandwidth::{RateDirection, RateLimiter, ShapedLimiter};
use swarmotter_core::config::PeerEncryptionMode;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, PeerReader};
use swarmotter_core::storage::StorageIo;
use swarmotter_core::utp::PeerDuplex;

use crate::engine::EngineState;

const DEFAULT_MAX_INBOUND_SESSIONS: usize = 256;

/// A torrent registered with the process-wide inbound peer listener.
#[derive(Clone)]
pub struct SeedRegistration {
    context: PeerServeContext,
}

impl SeedRegistration {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: TorrentMeta,
        storage: Arc<StorageIo>,
        complete_storage: Option<Arc<StorageIo>>,
        state: Arc<Mutex<EngineState>>,
        peer_id: [u8; 20],
        limiter: RateLimiter,
        global_limiter: Option<RateLimiter>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        let mut limiter = ShapedLimiter::from_rate_limiter(limiter);
        if let Some(global) = global_limiter {
            limiter = limiter.with_global(global);
        }
        Self {
            context: PeerServeContext {
                meta,
                storage,
                complete_storage,
                state,
                peer_id,
                limiter,
                shutdown,
            },
        }
    }

    pub fn info_hash(&self) -> swarmotter_core::hash::InfoHash {
        self.context.meta.info_hash
    }
}

/// Shared routing table for the daemon's single contained peer listener.
#[derive(Clone, Default)]
pub struct SeedRegistry {
    inner: Arc<RwLock<HashMap<swarmotter_core::hash::InfoHash, PeerServeContext>>>,
}

impl SeedRegistry {
    pub async fn register(&self, registration: SeedRegistration) {
        self.inner
            .write()
            .await
            .insert(registration.info_hash(), registration.context);
    }

    pub async fn unregister(&self, info_hash: &swarmotter_core::hash::InfoHash) {
        self.inner.write().await.remove(info_hash);
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }

    pub async fn clear(&self) {
        self.inner.write().await.clear();
    }

    async fn context(
        &self,
        info_hash: &swarmotter_core::hash::InfoHash,
    ) -> Option<PeerServeContext> {
        self.inner.read().await.get(info_hash).cloned()
    }

    async fn info_hashes(&self) -> Vec<swarmotter_core::hash::InfoHash> {
        self.inner.read().await.keys().copied().collect()
    }
}

/// Process-wide inbound listener. Plaintext peer handshakes and MSE stream
/// keys are routed to registered torrents by info hash. All accepted sessions
/// remain owned by this task and are aborted when the listener shuts down.
pub struct SeederHub {
    registry: SeedRegistry,
    binder: Arc<dyn NetworkBinder>,
    port: u16,
    encryption_mode: PeerEncryptionMode,
    shutdown: tokio::sync::watch::Receiver<bool>,
    max_sessions: Arc<AtomicUsize>,
    bound_addr: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
}

impl SeederHub {
    pub fn new(
        registry: SeedRegistry,
        binder: Arc<dyn NetworkBinder>,
        port: u16,
        encryption_mode: PeerEncryptionMode,
        shutdown: tokio::sync::watch::Receiver<bool>,
        max_sessions: usize,
    ) -> Self {
        Self {
            registry,
            binder,
            port,
            encryption_mode,
            shutdown,
            max_sessions: Arc::new(AtomicUsize::new(max_sessions.max(1))),
            bound_addr: None,
        }
    }

    pub fn with_dynamic_session_limit(mut self, max_sessions: Arc<AtomicUsize>) -> Self {
        self.max_sessions = max_sessions;
        self
    }

    pub(crate) fn with_bound_addr(
        mut self,
        sender: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
    ) -> Self {
        self.bound_addr = Some(sender);
        self
    }

    pub async fn run(mut self) -> Result<()> {
        if !self.binder.traffic_allowed() {
            return Err(CoreError::NetworkBlocked(
                "torrent data plane blocked; cannot start seeding listener".into(),
            ));
        }
        let listener = self.binder.bind_peer_listener(self.port).await?;
        let listen_addr = listener.local_addr()?;
        if let Some(sender) = self.bound_addr.take() {
            let _ = sender.send(listen_addr);
        }
        tracing::info!(addr = %listen_addr, "shared seeding listener bound");
        let mut sessions = JoinSet::new();

        loop {
            if *self.shutdown.borrow() || !self.binder.traffic_allowed() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                changed = self.shutdown.changed() => {
                    if changed.is_err() || *self.shutdown.borrow() {
                        break;
                    }
                }
                joined = sessions.join_next(), if !sessions.is_empty() => {
                    if let Some(Err(error)) = joined {
                        tracing::debug!(%error, "inbound peer session task failed");
                    }
                }
                accepted = listener.accept() => {
                    let stream = match accepted {
                        Ok(stream) => stream,
                        Err(error) => {
                            tracing::debug!(%error, "shared seeding accept failed");
                            continue;
                        }
                    };
                    let max_sessions = self.max_sessions.load(Ordering::Relaxed).max(1);
                    if sessions.len() >= max_sessions {
                        tracing::warn!(max_sessions, "inbound peer session limit reached");
                        drop(stream);
                        continue;
                    }
                    let peer_addr = stream.peer_addr().ok();
                    let registry = self.registry.clone();
                    let encryption_mode = self.encryption_mode;
                    sessions.spawn(async move {
                        if let Err(error) = serve_routed_peer(stream, registry, encryption_mode).await {
                            tracing::debug!(peer = ?peer_addr, %error, "inbound peer session ended");
                        }
                    });
                }
            }
        }

        sessions.shutdown().await;
        Ok(())
    }
}

/// A seeding listener that serves verified pieces to inbound peers.
///
/// `state` is the shared live engine state; the seeder serves pieces present
/// in `state.pieces_have` and accumulates uploaded bytes into `state.uploaded`.
/// `limiter` shapes upload throughput. `shutdown` completes when the seeder
/// should stop (pause/remove).
#[allow(dead_code)]
pub struct Seeder {
    meta: TorrentMeta,
    storage: Arc<StorageIo>,
    complete_storage: Option<Arc<StorageIo>>,
    state: Arc<Mutex<EngineState>>,
    binder: Arc<dyn NetworkBinder>,
    port: u16,
    peer_id: [u8; 20],
    encryption_mode: PeerEncryptionMode,
    shutdown: tokio::sync::watch::Receiver<bool>,
    limiter: ShapedLimiter,
    max_sessions: usize,
    /// Optional one-shot sender receiving the bound listen address, for tests
    /// that bind on port 0 and need to learn the actual port.
    bound_addr: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
}

#[allow(dead_code)]
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
            encryption_mode: PeerEncryptionMode::default(),
            shutdown,
            limiter: ShapedLimiter::from_rate_limiter(limiter),
            max_sessions: DEFAULT_MAX_INBOUND_SESSIONS,
            bound_addr: None,
        }
    }

    /// Configure inbound TCP peer-wire encryption policy.
    pub fn with_encryption_mode(mut self, encryption_mode: PeerEncryptionMode) -> Self {
        self.encryption_mode = encryption_mode;
        self
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

    /// Bound the number of concurrently accepted inbound sessions.
    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions.max(1);
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

        let mut sessions = JoinSet::new();
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
            tokio::select! {
                changed = self.shutdown.changed() => {
                    if changed.is_err() || *self.shutdown.borrow() {
                        break;
                    }
                }
                joined = sessions.join_next(), if !sessions.is_empty() => {
                    if let Some(Err(error)) = joined {
                        tracing::debug!(%error, "inbound peer session task failed");
                    }
                }
                accepted = listener.accept() => {
                    let stream = match accepted {
                        Ok(stream) => stream,
                        Err(error) => {
                            tracing::debug!(%error, "seeding accept failed");
                            continue;
                        }
                    };
                    if sessions.len() >= self.max_sessions {
                        tracing::warn!(max_sessions = self.max_sessions, "inbound peer session limit reached");
                        drop(stream);
                        continue;
                    }
                    let peer_addr = stream.peer_addr().ok();
                    let context = PeerServeContext {
                        meta: self.meta.clone(),
                        storage: self.storage.clone(),
                        complete_storage: self.complete_storage.clone(),
                        state: self.state.clone(),
                        peer_id: self.peer_id,
                        limiter: self.limiter.clone(),
                        shutdown: self.shutdown.clone(),
                    };
                    let encryption_mode = self.encryption_mode;
                    sessions.spawn(async move {
                        if let Err(error) = serve_known_peer(stream, context, encryption_mode).await {
                            tracing::debug!(peer = ?peer_addr, %error, "inbound peer session ended");
                        }
                    });
                }
            }
        }
        sessions.shutdown().await;
        Ok(())
    }
}

#[derive(Clone)]
struct PeerServeContext {
    meta: TorrentMeta,
    storage: Arc<StorageIo>,
    complete_storage: Option<Arc<StorageIo>>,
    state: Arc<Mutex<EngineState>>,
    peer_id: [u8; 20],
    limiter: ShapedLimiter,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

#[allow(dead_code)]
async fn serve_known_peer(
    stream: tokio::net::TcpStream,
    context: PeerServeContext,
    encryption_mode: PeerEncryptionMode,
) -> Result<()> {
    let info_hash = context.meta.info_hash;
    let mut stream = negotiate_inbound_peer_stream(stream, info_hash, encryption_mode).await?;
    let their_hs = read_peer_handshake(&mut stream).await?;
    serve_peer(stream, context, their_hs).await
}

async fn serve_routed_peer(
    stream: tokio::net::TcpStream,
    registry: SeedRegistry,
    encryption_mode: PeerEncryptionMode,
) -> Result<()> {
    let plaintext = looks_like_plaintext_peer_handshake(&stream).await?;
    let (mut stream, encrypted_hash): (Box<dyn PeerDuplex>, Option<_>) = match encryption_mode {
        PeerEncryptionMode::Disabled => (Box::new(stream), None),
        PeerEncryptionMode::Preferred if plaintext => (Box::new(stream), None),
        PeerEncryptionMode::Required if plaintext => {
            return Err(CoreError::Internal(
                "plaintext inbound peer rejected by required encryption mode".into(),
            ));
        }
        PeerEncryptionMode::Preferred | PeerEncryptionMode::Required => {
            let hashes = registry.info_hashes().await;
            let (hash, encrypted) = timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::accept_any(stream, &hashes),
            )
            .await??;
            (Box::new(encrypted), Some(hash))
        }
    };
    let their_hs = read_peer_handshake(&mut stream).await?;
    if encrypted_hash.is_some_and(|hash| hash != their_hs.info_hash) {
        return Err(CoreError::Internal(
            "encrypted inbound peer handshake did not match its stream key".into(),
        ));
    }
    let context = registry
        .context(&their_hs.info_hash)
        .await
        .ok_or_else(|| CoreError::NotFound("registered inbound torrent".into()))?;
    serve_peer(stream, context, their_hs).await
}

async fn read_peer_handshake(stream: &mut Box<dyn PeerDuplex>) -> Result<Handshake> {
    let mut reader = PeerReader::new(stream);
    timeout(Duration::from_secs(15), reader.read_handshake()).await?
}

async fn serve_peer(
    stream: Box<dyn PeerDuplex>,
    context: PeerServeContext,
    their_hs: Handshake,
) -> Result<()> {
    let PeerServeContext {
        meta,
        storage,
        complete_storage,
        state,
        peer_id,
        limiter,
        mut shutdown,
    } = context;
    if their_hs.info_hash != meta.info_hash {
        return Err(CoreError::Internal(
            "inbound peer info hash mismatch".into(),
        ));
    }
    tokio::select! {
        changed = shutdown.changed() => {
            let _ = changed;
            Ok(())
        }
        result = serve_peer_session(stream, meta, storage, complete_storage, state, peer_id, limiter) => result,
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_peer_session(
    stream: Box<dyn PeerDuplex>,
    meta: TorrentMeta,
    storage: Arc<StorageIo>,
    complete_storage: Option<Arc<StorageIo>>,
    state: Arc<Mutex<EngineState>>,
    peer_id: [u8; 20],
    limiter: ShapedLimiter,
) -> Result<()> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = PeerReader::new(read_half);

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
                let (have_it, fully_complete) = {
                    let s = state.lock().await;
                    (
                        s.pieces_have.has(p),
                        s.pieces_have.count(meta.piece_count()) == meta.piece_count(),
                    )
                };
                if !have_it {
                    continue;
                }
                let read_storage = if fully_complete {
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

#[allow(dead_code)]
async fn negotiate_inbound_peer_stream(
    stream: tokio::net::TcpStream,
    info_hash: swarmotter_core::hash::InfoHash,
    encryption_mode: PeerEncryptionMode,
) -> Result<Box<dyn PeerDuplex>> {
    let plaintext = looks_like_plaintext_peer_handshake(&stream).await?;
    match encryption_mode {
        PeerEncryptionMode::Disabled => Ok(Box::new(stream)),
        PeerEncryptionMode::Preferred if plaintext => Ok(Box::new(stream)),
        PeerEncryptionMode::Preferred => {
            let encrypted = timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::accept(stream, info_hash),
            )
            .await??;
            Ok(Box::new(encrypted))
        }
        PeerEncryptionMode::Required if plaintext => Err(CoreError::Internal(
            "plaintext inbound peer rejected by required encryption mode".into(),
        )),
        PeerEncryptionMode::Required => {
            let encrypted = timeout(
                Duration::from_secs(10),
                swarmotter_core::mse::accept(stream, info_hash),
            )
            .await??;
            Ok(Box::new(encrypted))
        }
    }
}

async fn looks_like_plaintext_peer_handshake(stream: &tokio::net::TcpStream) -> Result<bool> {
    Ok(timeout(Duration::from_secs(5), async {
        let mut prefix = [0u8; 1 + peer::PSTR.len()];
        loop {
            let n = stream.peek(&mut prefix).await?;
            if n == 0 || prefix[0] != peer::PSTR.len() as u8 {
                return Ok::<bool, std::io::Error>(false);
            }
            if n > 1 && prefix[1..n] != peer::PSTR[..n - 1] {
                return Ok::<bool, std::io::Error>(false);
            }
            if n == prefix.len() {
                return Ok::<bool, std::io::Error>(true);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??)
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

    async fn request_first_block(
        address: std::net::SocketAddr,
        meta: &TorrentMeta,
        expected: &[u8],
    ) {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (read, mut write) = tokio::io::split(stream);
        peer::write_handshake(
            &mut write,
            &Handshake {
                info_hash: meta.info_hash,
                peer_id: peer_id(b"-HUBCLI-"),
                reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
            },
        )
        .await
        .unwrap();
        let mut reader = PeerReader::new(read);
        assert_eq!(
            reader.read_handshake().await.unwrap().info_hash,
            meta.info_hash
        );
        assert!(matches!(
            reader.read_message().await.unwrap(),
            Some(Message::Bitfield { .. })
        ));
        peer::write_message(&mut write, &Message::Interested)
            .await
            .unwrap();
        loop {
            if matches!(reader.read_message().await.unwrap(), Some(Message::Unchoke)) {
                break;
            }
        }
        peer::write_message(
            &mut write,
            &Message::Request {
                piece: 0,
                offset: 0,
                length: expected.len() as u32,
            },
        )
        .await
        .unwrap();
        loop {
            if let Some(Message::Piece { block, .. }) = reader.read_message().await.unwrap() {
                assert_eq!(block, expected);
                break;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_listener_routes_multiple_torrents_and_owns_sessions() {
        let registry = SeedRegistry::default();
        let mut torrent_shutdowns = Vec::new();
        let mut fixtures = Vec::new();
        for (name, content) in [
            ("hub-a.bin", b"hub torrent a".as_slice()),
            ("hub-b.bin", b"hub torrent b".as_slice()),
        ] {
            let bytes = build_single_file_torrent(name, content, content.len() as u64, None, false);
            let meta = parse_torrent(&bytes).unwrap();
            let dir = unique_dir(name);
            let storage = Arc::new(StorageIo::new(meta.clone(), dir.clone()));
            storage.write_piece(0, content).await.unwrap();
            let mut have = PieceBitfield::new(1);
            have.set(0);
            let state = Arc::new(Mutex::new(EngineState {
                piece_count: 1,
                total_length: content.len() as u64,
                pieces_have: have,
                finished: true,
                ..EngineState::default()
            }));
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            registry
                .register(SeedRegistration::new(
                    meta.clone(),
                    storage,
                    None,
                    state,
                    peer_id(b"-HUBSRV-"),
                    RateLimiter::unlimited(),
                    None,
                    shutdown_rx,
                ))
                .await;
            torrent_shutdowns.push(shutdown_tx);
            fixtures.push((meta, content.to_vec(), dir));
        }

        let (hub_shutdown_tx, hub_shutdown_rx) = tokio::sync::watch::channel(false);
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let hub = SeederHub::new(
            registry,
            Arc::new(LoopbackBinder),
            0,
            PeerEncryptionMode::Preferred,
            hub_shutdown_rx,
            8,
        )
        .with_bound_addr(bound_tx);
        let task = tokio::spawn(hub.run());
        let address = bound_rx.await.unwrap();
        for (meta, content, _) in &fixtures {
            request_first_block(address, meta, content).await;
        }
        let _ = hub_shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("hub shutdown must own and stop accepted sessions")
            .unwrap()
            .unwrap();
        drop(torrent_shutdowns);
        for (_, _, dir) in fixtures {
            std::fs::remove_dir_all(dir).ok();
        }
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

        let req_len = std::cmp::min(
            BLOCK_SIZE,
            std::cmp::min(piece_length as u32, content.len() as u32),
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seeder_required_mode_accepts_encrypted_peer() {
        let content = b"swarmotter encrypted seeding test payload";
        let piece_length: u64 = content.len() as u64;
        let bytes =
            build_single_file_torrent("seed-encrypted.bin", content, piece_length, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("seeder-encrypted");
        let storage = Arc::new(StorageIo::new(meta.clone(), dir.clone()));
        storage.preallocate().await.unwrap();
        storage.write_block(0, 0, content).await.unwrap();

        let mut have = PieceBitfield::new(meta.piece_count());
        for i in 0..meta.piece_count() {
            have.set(i);
        }
        let mut state = EngineState {
            piece_count: meta.piece_count(),
            pieces_have: have,
            ..EngineState::default()
        };
        state.total_length = meta.total_length;
        let state = Arc::new(Mutex::new(state));
        let binder = Arc::new(LoopbackBinder);

        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let seeder = Seeder::new(
            meta.clone(),
            storage,
            state,
            binder,
            0,
            peer_id(b"-SW0002-"),
            shutdown_rx,
        )
        .with_encryption_mode(PeerEncryptionMode::Required)
        .with_bound_addr(bound_tx);
        let seeder_task = tokio::spawn(async move { seeder.run().await });
        let seeder_addr = bound_rx.await.expect("seeder bound its listener");

        let tcp = tokio::net::TcpStream::connect(seeder_addr).await.unwrap();
        let encrypted = swarmotter_core::mse::connect(tcp, meta.info_hash)
            .await
            .unwrap();
        let (rd, mut wr) = tokio::io::split(encrypted);
        let hs = Handshake {
            info_hash: meta.info_hash,
            peer_id: peer_id(b"-LC0002-"),
            reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
        };
        peer::write_handshake(&mut wr, &hs).await.unwrap();
        let mut reader = PeerReader::new(rd);
        let their_hs = reader.read_handshake().await.unwrap();
        assert_eq!(their_hs.info_hash, meta.info_hash);
        let msg = reader.read_message().await.unwrap().unwrap();
        let bf = match msg {
            Message::Bitfield { bits } => Bitfield::from_bytes(bits, meta.piece_count()),
            _ => panic!("expected bitfield"),
        };
        assert_eq!(bf.count(), meta.piece_count());

        drop(reader);
        drop(wr);
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
