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
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tokio::time::timeout;

use swarmotter_core::bandwidth::{RateDirection, RateLimiter, ShapedLimiter};
use swarmotter_core::config::PeerEncryptionMode;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::extensions::{self, MetadataMsgType};
use swarmotter_core::hash::{PeerInfoHash, TorrentKey, V2InfoHash};
use swarmotter_core::meta::{v2_piece_layer_root, TorrentMeta, MAX_TORRENT_METADATA_BYTES};
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{self, Bitfield, Handshake, Message, PeerReader, V2Handshake};
use swarmotter_core::peer_filter::PeerFilter;
use swarmotter_core::storage::StorageIo;
use swarmotter_core::utp::PeerDuplex;
use swarmotter_core::v2::{v2_hash_pair, V2PieceLayout};

use crate::engine::EngineState;
use crate::peer_permits::{PeerPermit, PeerPermitPool, PeerSessionBudget};

#[cfg(test)]
const DEFAULT_MAX_INBOUND_SESSIONS: usize = 256;

/// A torrent registered with the process-wide inbound peer listener.
#[derive(Clone)]
pub struct SeedRegistration {
    key: TorrentKey,
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
        limiter: impl Into<Arc<RateLimiter>>,
        global_limiter: Option<RateLimiter>,
        peer_session_budget: PeerSessionBudget,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        let mut limiter = ShapedLimiter::from_shared_rate_limiter(limiter.into());
        if let Some(global) = global_limiter {
            limiter = limiter.with_global(global);
        }
        let key = meta
            .identity
            .primary_key()
            .unwrap_or_else(|| TorrentKey::v1(meta.info_hash));
        Self {
            key,
            context: PeerServeContext {
                meta,
                storage,
                complete_storage,
                state,
                peer_id,
                limiter,
                peer_session_budget,
                shutdown,
                encryption_mode: None,
            },
        }
    }

    /// Attach the effective encryption mode resolved for this torrent. The
    /// process-wide listener uses it after routing an inbound handshake, so
    /// per-profile and per-torrent overrides cannot silently admit plaintext
    /// sessions for a `required` torrent.
    pub fn with_encryption_mode(mut self, encryption_mode: PeerEncryptionMode) -> Self {
        self.context.encryption_mode = Some(encryption_mode);
        self
    }

    /// Override the derived key when the daemon has already canonicalized a
    /// hybrid alias. Registration validates that it still represents this
    /// metainfo's primary identity.
    pub fn with_key(mut self, key: TorrentKey) -> Self {
        self.key = key;
        self
    }

    pub fn key(&self) -> TorrentKey {
        self.key
    }

    fn peer_info_hash(&self) -> PeerInfoHash {
        self.key.peer_info_hash()
    }

    fn expected_key(&self) -> TorrentKey {
        self.context
            .meta
            .identity
            .primary_key()
            .unwrap_or_else(|| TorrentKey::v1(self.context.meta.info_hash))
    }
}

/// Shared routing table for the daemon's single contained peer listener.
#[derive(Clone, Default)]
pub struct SeedRegistry {
    inner: Arc<RwLock<SeedRegistryInner>>,
}

#[derive(Default)]
struct SeedRegistryInner {
    /// Runtime owner key to serving context. Every entry is canonical, so a
    /// hybrid v2 alias cannot create a second listener registration.
    contexts: HashMap<TorrentKey, PeerServeContext>,
    /// Plaintext v1/v2 handshake routing. This intentionally contains only a
    /// 20-byte wire identity and is never exposed as an ownership key.
    wire_index: HashMap<PeerInfoHash, TorrentKey>,
    /// MSE stream keys use the same explicit 20-byte peer-wire identity as
    /// the handshake. Full v2 identities remain in `contexts`; ambiguous
    /// truncations are rejected at registration.
    mse_index: HashMap<PeerInfoHash, TorrentKey>,
}

impl SeedRegistry {
    /// Register a canonical torrent owner and reject ambiguous 20-byte
    /// plaintext wire identities before a listener can misroute a peer.
    pub async fn register(&self, registration: SeedRegistration) -> Result<()> {
        let key = registration.key();
        let expected = registration.expected_key();
        if key != expected {
            return Err(CoreError::InvalidArgument(format!(
                "seeder registration key {key} does not match metainfo primary key {expected}"
            )));
        }
        let wire_hash = registration.peer_info_hash();
        let mut inner = self.inner.write().await;
        if let Some(existing) = inner.wire_index.get(&wire_hash) {
            if *existing != key {
                return Err(CoreError::DuplicateTorrent(format!(
                    "peer-wire identity {wire_hash} is already registered by torrent {existing}"
                )));
            }
        }

        if let Some(previous) = inner.contexts.remove(&key) {
            let previous_key = previous
                .meta
                .identity
                .primary_key()
                .unwrap_or_else(|| TorrentKey::v1(previous.meta.info_hash));
            inner.wire_index.retain(|_, mapped| *mapped != previous_key);
            inner.mse_index.retain(|_, mapped| *mapped != previous_key);
        }
        inner.wire_index.insert(wire_hash, key);
        inner.mse_index.insert(wire_hash, key);
        inner.contexts.insert(key, registration.context);
        Ok(())
    }

    pub async fn unregister(&self, key: &TorrentKey) {
        let mut inner = self.inner.write().await;
        if inner.contexts.remove(key).is_some() {
            inner.wire_index.retain(|_, mapped| mapped != key);
            inner.mse_index.retain(|_, mapped| mapped != key);
        }
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.contexts.is_empty()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.contexts.len()
    }

    pub async fn contains(&self, key: &TorrentKey) -> bool {
        self.inner.read().await.contexts.contains_key(key)
    }

    #[cfg(test)]
    pub async fn limiter_for_test(&self, key: &TorrentKey) -> Option<Arc<RateLimiter>> {
        self.inner
            .read()
            .await
            .contexts
            .get(key)
            .map(|context| context.limiter.per_torrent.clone())
    }

    pub async fn clear(&self) {
        *self.inner.write().await = SeedRegistryInner::default();
    }

    /// Update the policy used for subsequently accepted sessions without
    /// disrupting an already-negotiated upload. The caller has already
    /// persisted the corresponding torrent/configuration transition.
    pub async fn update_encryption_mode(
        &self,
        key: &TorrentKey,
        encryption_mode: PeerEncryptionMode,
    ) {
        if let Some(context) = self.inner.write().await.contexts.get_mut(key) {
            context.encryption_mode = Some(encryption_mode);
        }
    }

    async fn context_for_wire(
        &self,
        wire_hash: &PeerInfoHash,
    ) -> Option<(TorrentKey, PeerServeContext)> {
        let inner = self.inner.read().await;
        let key = *inner.wire_index.get(wire_hash)?;
        inner
            .contexts
            .get(&key)
            .cloned()
            .map(|context| (key, context))
    }

    async fn context_for_mse(
        &self,
        wire_hash: &PeerInfoHash,
    ) -> Option<(TorrentKey, PeerServeContext)> {
        let inner = self.inner.read().await;
        let key = *inner.mse_index.get(wire_hash)?;
        inner
            .contexts
            .get(&key)
            .cloned()
            .map(|context| (key, context))
    }

    async fn mse_wire_hashes(&self) -> Vec<PeerInfoHash> {
        self.inner.read().await.mse_index.keys().copied().collect()
    }

    /// Canonical runtime owners currently served by the shared listener.
    pub async fn keys(&self) -> Vec<TorrentKey> {
        self.inner.read().await.contexts.keys().copied().collect()
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
    global_peer_permits: Arc<PeerPermitPool>,
    peer_filter: Arc<PeerFilter>,
    bound_addr: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
}

impl SeederHub {
    pub fn new(
        registry: SeedRegistry,
        binder: Arc<dyn NetworkBinder>,
        port: u16,
        encryption_mode: PeerEncryptionMode,
        shutdown: tokio::sync::watch::Receiver<bool>,
        global_peer_permits: Arc<PeerPermitPool>,
    ) -> Self {
        Self {
            registry,
            binder,
            port,
            encryption_mode,
            shutdown,
            global_peer_permits,
            peer_filter: Arc::new(PeerFilter::default()),
            bound_addr: None,
        }
    }

    /// Attach the immutable admission policy for this listener generation.
    /// Accepted sockets still originate from the contained binder.
    pub fn with_peer_filter(mut self, peer_filter: Arc<PeerFilter>) -> Self {
        self.peer_filter = peer_filter;
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
                    let peer_addr = match stream.peer_addr() {
                        Ok(peer_addr) => peer_addr,
                        Err(error) => {
                            tracing::warn!(%error, "inbound socket rejected because its peer address is unavailable");
                            drop(stream);
                            continue;
                        }
                    };
                    let decision = self.peer_filter.admit_ip(peer_addr.ip());
                    if !decision.is_allowed() {
                        tracing::info!(
                            peer = %peer_addr,
                            reason = decision.audit_reason(),
                            detail = ?decision.rejection_message(),
                            "inbound peer rejected before session admission"
                        );
                        drop(stream);
                        continue;
                    }
                    let Some(global_peer_permit) = self.global_peer_permits.try_acquire() else {
                        tracing::warn!("process-wide peer session limit reached; inbound socket rejected before handshake");
                        drop(stream);
                        continue;
                    };
                    let registry = self.registry.clone();
                    let encryption_mode = self.encryption_mode;
                    let peer_filter = self.peer_filter.clone();
                    sessions.spawn(async move {
                        if let Err(error) = serve_routed_peer(
                            stream,
                            registry,
                            encryption_mode,
                            peer_addr,
                            peer_filter,
                            global_peer_permit,
                        ).await {
                            tracing::debug!(peer = %peer_addr, %error, "inbound peer session ended");
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
#[cfg(test)]
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
    peer_session_budget: PeerSessionBudget,
    /// Optional one-shot sender receiving the bound listen address, for tests
    /// that bind on port 0 and need to learn the actual port.
    bound_addr: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
}

#[allow(dead_code)]
#[cfg(test)]
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
        peer_session_budget: PeerSessionBudget,
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
            peer_session_budget,
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
        limiter: impl Into<Arc<RateLimiter>>,
        peer_session_budget: PeerSessionBudget,
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
            limiter: ShapedLimiter::from_shared_rate_limiter(limiter.into()),
            max_sessions: DEFAULT_MAX_INBOUND_SESSIONS,
            peer_session_budget,
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
                    let Some(global_peer_permit) =
                        self.peer_session_budget.try_acquire_global_inbound()
                    else {
                        tracing::warn!("process-wide peer session limit reached; inbound socket rejected before handshake");
                        drop(stream);
                        continue;
                    };
                    let Some(torrent_peer_permit) =
                        self.peer_session_budget.try_acquire_torrent_inbound()
                    else {
                        tracing::warn!("per-torrent peer session limit reached; inbound socket rejected before handshake");
                        drop(stream);
                        continue;
                    };
                    let peer_addr = stream.peer_addr().ok();
                    let context = PeerServeContext {
                        meta: self.meta.clone(),
                        storage: self.storage.clone(),
                        complete_storage: self.complete_storage.clone(),
                        state: self.state.clone(),
                        peer_id: self.peer_id,
                        limiter: self.limiter.clone(),
                        peer_session_budget: self.peer_session_budget.clone(),
                        shutdown: self.shutdown.clone(),
                        encryption_mode: None,
                    };
                    let encryption_mode = self.encryption_mode;
                    sessions.spawn(async move {
                        let _global_peer_permit = global_peer_permit;
                        let _torrent_peer_permit = torrent_peer_permit;
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
    peer_session_budget: PeerSessionBudget,
    shutdown: tokio::sync::watch::Receiver<bool>,
    /// `None` retains the listener-wide policy for registrations created by
    /// older callers/tests. The daemon always records the resolved effective
    /// mode here.
    encryption_mode: Option<PeerEncryptionMode>,
}

#[cfg(test)]
async fn serve_known_peer(
    stream: tokio::net::TcpStream,
    context: PeerServeContext,
    encryption_mode: PeerEncryptionMode,
) -> Result<()> {
    let info_hash = PeerInfoHash::from_v1(context.meta.info_hash);
    let mut stream = negotiate_inbound_peer_stream(stream, info_hash, encryption_mode).await?;
    let their_hs = read_peer_handshake(&mut stream).await?;
    serve_peer(stream, context, their_hs).await
}

async fn serve_routed_peer(
    stream: tokio::net::TcpStream,
    registry: SeedRegistry,
    listener_encryption_mode: PeerEncryptionMode,
    peer_addr: std::net::SocketAddr,
    peer_filter: Arc<PeerFilter>,
    _global_peer_permit: PeerPermit,
) -> Result<()> {
    let plaintext = looks_like_plaintext_peer_handshake(&stream).await?;
    // An inbound listener must identify the torrent before it can apply a
    // profile/torrent override. Plaintext handshakes identify themselves in
    // their normal peer-wire header; encrypted MSE streams identify
    // themselves through the stream key. Both paths are accepted only long
    // enough to identify the torrent, then the effective policy decides
    // whether the peer-wire session may continue.
    let (mut stream, encrypted_hash): (Box<dyn PeerDuplex>, Option<PeerInfoHash>) = if plaintext {
        (Box::new(stream), None)
    } else {
        let hashes = registry.mse_wire_hashes().await;
        let (hash, encrypted) = timeout(
            Duration::from_secs(10),
            swarmotter_core::mse::accept_any(stream, &hashes),
        )
        .await??;
        (Box::new(encrypted), Some(hash))
    };
    // Decode the neutral 20-byte peer-wire field before choosing v1 or v2
    // semantics. The registry's separate wire index makes that lookup
    // unambiguous even though the on-wire representation is truncated.
    let their_hs = read_v2_peer_handshake(&mut stream).await?;
    if encrypted_hash.is_some_and(|hash| hash != their_hs.info_hash) {
        return Err(CoreError::Internal(
            "encrypted inbound peer handshake did not match its stream key".into(),
        ));
    }
    let (key, context) = match encrypted_hash {
        Some(hash) => registry.context_for_mse(&hash).await,
        None => registry.context_for_wire(&their_hs.info_hash).await,
    }
    .ok_or_else(|| CoreError::NotFound("registered inbound torrent".into()))?;
    let encryption_mode = context.encryption_mode.unwrap_or(listener_encryption_mode);
    match (plaintext, encryption_mode) {
        (true, PeerEncryptionMode::Required) => {
            return Err(CoreError::Internal(
                "plaintext inbound peer rejected by required encryption mode".into(),
            ));
        }
        (false, PeerEncryptionMode::Disabled) => {
            return Err(CoreError::Internal(
                "encrypted inbound peer rejected by disabled encryption mode".into(),
            ));
        }
        _ => {}
    }
    let decision = peer_filter.admit_client_id(&their_hs.peer_id);
    if !decision.is_allowed() {
        tracing::info!(
            peer = %peer_addr,
            reason = decision.audit_reason(),
            detail = ?decision.rejection_message(),
            "inbound peer rejected after contained handshake"
        );
        return Err(CoreError::Internal(
            decision
                .rejection_message()
                .unwrap_or_else(|| "peer rejected by admission policy".into()),
        ));
    }
    let _torrent_peer_permit = context
        .peer_session_budget
        .try_acquire_torrent_inbound()
        .ok_or_else(|| CoreError::Internal("per-torrent peer session limit reached".into()))?;
    if context.meta.requires_v2_data_plane() {
        if !their_hs.supports_v2() {
            return Err(CoreError::Parse(
                "pure-v2 inbound peer did not advertise BEP 52 capability".into(),
            ));
        }
        serve_v2_peer(stream, context, their_hs).await
    } else {
        let v1_hash = key.as_v1().ok_or_else(|| {
            CoreError::Internal("v1 seeder registration has no v1 owner key".into())
        })?;
        serve_peer(
            stream,
            context,
            Handshake {
                info_hash: v1_hash,
                peer_id: their_hs.peer_id,
                reserved: their_hs.reserved,
            },
        )
        .await
    }
}

#[cfg(test)]
async fn read_peer_handshake(stream: &mut Box<dyn PeerDuplex>) -> Result<Handshake> {
    let mut reader = PeerReader::new(stream);
    timeout(Duration::from_secs(15), reader.read_handshake()).await?
}

async fn read_v2_peer_handshake(stream: &mut Box<dyn PeerDuplex>) -> Result<V2Handshake> {
    let mut reader = PeerReader::new(stream);
    timeout(Duration::from_secs(15), reader.read_v2_handshake()).await?
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
        peer_session_budget: _,
        encryption_mode: _,
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

/// Serve an inbound pure-v2 peer after the shared listener has resolved its
/// 20-byte wire identity to a collision-safe canonical [`TorrentKey`].
async fn serve_v2_peer(
    stream: Box<dyn PeerDuplex>,
    context: PeerServeContext,
    their_hs: V2Handshake,
) -> Result<()> {
    let PeerServeContext {
        meta,
        storage,
        complete_storage,
        state,
        peer_id,
        limiter,
        peer_session_budget: _,
        encryption_mode: _,
        mut shutdown,
    } = context;
    let expected = meta
        .identity
        .v2_peer_info_hash()
        .ok_or_else(|| CoreError::Internal("pure-v2 seeder lacks a v2 identity".into()))?;
    if their_hs.info_hash != expected {
        return Err(CoreError::Internal(
            "inbound pure-v2 peer handshake hash mismatch".into(),
        ));
    }
    let layout = meta.v2_piece_layout()?;
    tokio::select! {
        changed = shutdown.changed() => {
            let _ = changed;
            Ok(())
        }
        result = serve_v2_peer_session(
            stream,
            meta,
            layout,
            storage,
            complete_storage,
            state,
            peer_id,
            limiter,
        ) => result,
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_v2_peer_session(
    stream: Box<dyn PeerDuplex>,
    meta: TorrentMeta,
    layout: V2PieceLayout,
    storage: Arc<StorageIo>,
    complete_storage: Option<Arc<StorageIo>>,
    state: Arc<Mutex<EngineState>>,
    peer_id: [u8; 20],
    limiter: ShapedLimiter,
) -> Result<()> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = PeerReader::new(read_half);
    let wire_hash = meta
        .identity
        .v2_peer_info_hash()
        .ok_or_else(|| CoreError::Internal("pure-v2 seeder lacks a v2 identity".into()))?;
    let our_hs = V2Handshake {
        info_hash: wire_hash,
        peer_id,
        reserved: peer::with_v2_support(swarmotter_core::extensions::EXTENSION_RESERVED),
    };
    peer::write_v2_handshake(&mut write_half, &our_hs).await?;

    let piece_count = layout.piece_count();
    let bitfield = {
        let state = state.lock().await;
        let mut bits = Bitfield::new(piece_count);
        for index in 0..piece_count {
            if state.pieces_have.has(index) {
                bits.set(index);
            }
        }
        bits
    };
    peer::write_message(&mut write_half, &bitfield.encode_message()).await?;
    write_half.flush().await.map_err(CoreError::from)?;

    // A pure-v2 magnet receives only the exact `info` dictionary through
    // BEP 9. The top-level piece layers are acquired separately with BEP 52
    // hash requests, so advertise and serve the original bytes here whenever
    // this complete seeder registration retained them.
    const LOCAL_UT_METADATA_ID: u8 = 3;
    let metadata_info = meta
        .raw_info
        .as_deref()
        .filter(|info| !info.is_empty() && info.len() <= MAX_TORRENT_METADATA_BYTES);
    let metadata_size = metadata_info.and_then(|info| u64::try_from(info.len()).ok());
    if let Some(metadata_size) = metadata_size {
        let payload = extensions::encode_extension_handshake(
            &[(extensions::UT_METADATA_NAME, LOCAL_UT_METADATA_ID)],
            "SwarmOtter/2.0",
            Some(metadata_size),
        );
        peer::write_message(
            &mut write_half,
            &Message::Extended {
                id: extensions::EXTENSION_HANDSHAKE_ID,
                payload,
            },
        )
        .await?;
        write_half.flush().await.map_err(CoreError::from)?;
    }

    let mut choking = true;
    // Extension ids are chosen by the receiving peer. We learn the remote
    // `ut_metadata` id from its handshake before sending BEP 9 data back.
    let mut remote_metadata_id = None;
    loop {
        let message = match timeout(Duration::from_secs(120), reader.read_message()).await {
            Ok(Ok(Some(message))) => message,
            Ok(Ok(None)) | Ok(Err(_)) | Err(_) => break,
        };
        match message {
            Message::Interested => {
                peer::write_message(&mut write_half, &Message::Unchoke).await?;
                write_half.flush().await.map_err(CoreError::from)?;
                choking = false;
            }
            Message::NotInterested => {}
            Message::Request {
                piece,
                offset,
                length,
            } => {
                let piece_index = piece as usize;
                let reject = || Message::Reject {
                    piece,
                    offset,
                    length,
                };
                let Some(mapping) = layout.piece(piece_index) else {
                    peer::write_message(&mut write_half, &reject()).await?;
                    continue;
                };
                let end = u64::from(offset).checked_add(u64::from(length));
                let valid_block = !choking
                    && length > 0
                    && length <= peer::BLOCK_SIZE
                    && end.is_some_and(|end| end <= mapping.length);
                let (have_piece, fully_complete) = {
                    let state = state.lock().await;
                    (
                        state.pieces_have.has(piece_index),
                        state.pieces_have.count(piece_count) == piece_count,
                    )
                };
                if !valid_block || !have_piece {
                    peer::write_message(&mut write_half, &reject()).await?;
                    continue;
                }
                let read_storage = if fully_complete {
                    complete_storage.as_deref().unwrap_or(storage.as_ref())
                } else {
                    storage.as_ref()
                };
                let block = match read_storage
                    .read_v2_block(&layout, piece_index, u64::from(offset), length as usize)
                    .await
                {
                    Ok(block) => block,
                    Err(error) => {
                        tracing::debug!(piece = piece_index, %error, "v2 seeding read failed");
                        peer::write_message(&mut write_half, &reject()).await?;
                        continue;
                    }
                };
                limiter
                    .acquire(RateDirection::Upload, block.len() as u64)
                    .await;
                {
                    let mut state = state.lock().await;
                    state.uploaded = state.uploaded.saturating_add(block.len() as u64);
                }
                peer::write_message(
                    &mut write_half,
                    &Message::Piece {
                        piece,
                        offset,
                        block,
                    },
                )
                .await?;
                write_half.flush().await.map_err(CoreError::from)?;
            }
            Message::HashRequest {
                pieces_root,
                base_layer,
                index,
                length,
                proof_layers,
            } => {
                let response =
                    v2_hash_response(&meta, pieces_root, base_layer, index, length, proof_layers)
                        .unwrap_or(Message::HashReject {
                            pieces_root,
                            base_layer,
                            index,
                            length,
                            proof_layers,
                        });
                peer::write_message(&mut write_half, &response).await?;
                write_half.flush().await.map_err(CoreError::from)?;
            }
            Message::Have { .. }
            | Message::Bitfield { .. }
            | Message::Choke
            | Message::Unchoke
            | Message::Keepalive
            | Message::Cancel { .. }
            | Message::Piece { .. }
            | Message::Reject { .. }
            | Message::Hashes { .. }
            | Message::HashReject { .. }
            | Message::Unknown { .. } => {}
            Message::Extended { id, payload } => {
                if id == extensions::EXTENSION_HANDSHAKE_ID {
                    if let Ok(handshake) = extensions::parse_extension_handshake(&payload) {
                        remote_metadata_id = handshake.id_for(extensions::UT_METADATA_NAME);
                    }
                    continue;
                }
                if id != LOCAL_UT_METADATA_ID {
                    continue;
                }

                let (Some(info), Some(total_size), Some(remote_id)) =
                    (metadata_info, metadata_size, remote_metadata_id)
                else {
                    continue;
                };
                let response = match extensions::parse_metadata_message(&payload) {
                    Ok(message) if message.msg_type == MetadataMsgType::Request => {
                        let start = usize::try_from(message.piece)
                            .ok()
                            .and_then(|piece| piece.checked_mul(extensions::METADATA_PIECE_SIZE));
                        match start.filter(|start| *start < info.len()) {
                            Some(start) => {
                                let end = start
                                    .saturating_add(extensions::METADATA_PIECE_SIZE)
                                    .min(info.len());
                                extensions::encode_metadata_data(
                                    message.piece,
                                    total_size,
                                    &info[start..end],
                                )
                            }
                            None => extensions::encode_metadata_reject(message.piece),
                        }
                    }
                    _ => continue,
                };
                peer::write_message(
                    &mut write_half,
                    &Message::Extended {
                        id: remote_id,
                        payload: response,
                    },
                )
                .await?;
                write_half.flush().await.map_err(CoreError::from)?;
            }
        }
    }
    Ok(())
}

/// Build a BEP 52 hash response at the logical-piece layer.
///
/// Complete metainfo retains the validated top-level piece layer, so serving
/// it does not depend on payload reads. This is important for pure-v2 magnet
/// metadata: a requester first obtains the BEP 9 `info` dictionary and then
/// verifies these hashes before it can start a payload transfer. Requests are
/// padded to the next power-of-two width using a *piece-layer* zero subtree,
/// exactly as the BEP 52 metainfo validator does.
#[allow(clippy::too_many_arguments)]
fn v2_hash_response(
    meta: &TorrentMeta,
    pieces_root: V2InfoHash,
    base_layer: u32,
    index: u32,
    length: u32,
    proof_layers: u32,
) -> Option<Message> {
    if length < 2 || !length.is_power_of_two() || !index.is_multiple_of(length) || length > 512 {
        return None;
    }
    let piece_layer = (meta.piece_length / swarmotter_core::meta::V2_BLOCK_LENGTH).trailing_zeros();
    if base_layer != piece_layer {
        return None;
    }

    let v2 = meta.v2.as_ref()?;
    if !v2.piece_layers_verified {
        return None;
    }
    let piece_layer_hashes = v2
        .piece_layers
        .iter()
        .find(|layer| layer.pieces_root == pieces_root)?;
    if v2_piece_layer_root(&piece_layer_hashes.hashes, meta.piece_length).ok()? != pieces_root {
        return None;
    }

    let width = piece_layer_hashes
        .hashes
        .len()
        .checked_next_power_of_two()?;
    let start = usize::try_from(index).ok()?;
    let requested = usize::try_from(length).ok()?;
    let end = start.checked_add(requested)?;
    if end > width {
        return None;
    }

    let mut zero_subtree = V2InfoHash::ZERO;
    for _ in 0..piece_layer {
        zero_subtree = v2_hash_pair(zero_subtree, zero_subtree);
    }
    let mut layers = vec![piece_layer_hashes.hashes.clone()];
    layers.first_mut()?.resize(width, zero_subtree);
    while layers.last()?.len() > 1 {
        let next = layers
            .last()?
            .chunks_exact(2)
            .map(|pair| v2_hash_pair(pair[0], pair[1]))
            .collect::<Vec<_>>();
        layers.push(next);
    }
    if layers.last()?.first().copied()? != pieces_root {
        return None;
    }

    let mut hashes = layers.first()?[start..end].to_vec();
    let requested_depth = length.trailing_zeros();
    for relative_layer in requested_depth..proof_layers {
        let layer_index = usize::try_from(relative_layer).ok()?;
        let Some(layer) = layers.get(layer_index) else {
            break;
        };
        if layer.len() <= 1 {
            break;
        }
        let node_index = start >> layer_index;
        hashes.push(*layer.get(node_index ^ 1)?);
    }
    Some(Message::Hashes {
        pieces_root,
        base_layer,
        index,
        length,
        proof_layers,
        hashes,
    })
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
            | Message::Reject { .. }
            | Message::HashRequest { .. }
            | Message::Hashes { .. }
            | Message::HashReject { .. }
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
    info_hash: PeerInfoHash,
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
    use swarmotter_core::hash::TorrentIdentity;
    use swarmotter_core::meta::{
        build_single_file_torrent, parse_torrent, v2_piece_layer_root, MetaFile, V2PieceLayer,
        V2TorrentMeta, V2_BLOCK_LENGTH,
    };
    use swarmotter_core::net::binder::LoopbackBinder;
    use swarmotter_core::peer::BLOCK_SIZE;
    use swarmotter_core::storage::resume::PieceBitfield;
    use swarmotter_core::v2::v2_piece_root;
    use tokio::io::AsyncReadExt as _;

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

    fn pure_v2_meta(name: &str, content: &[u8]) -> TorrentMeta {
        assert!(content.len() as u64 > V2_BLOCK_LENGTH);
        let piece_length = V2_BLOCK_LENGTH;
        let hashes = content
            .chunks(piece_length as usize)
            .map(|piece| v2_piece_root(piece, piece_length).unwrap())
            .collect::<Vec<_>>();
        let pieces_root = v2_piece_layer_root(&hashes, piece_length).unwrap();
        let file = MetaFile {
            path: vec![name.to_string()],
            length: content.len() as u64,
            pieces_root: Some(pieces_root),
        };
        let raw_info = format!("d4:name{}:{}e", name.len(), name).into_bytes();
        let meta = TorrentMeta {
            info_hash: swarmotter_core::hash::InfoHash::ZERO,
            identity: TorrentIdentity::v2(V2InfoHash::from_info_bencoded(&raw_info)),
            name: name.to_string(),
            piece_length,
            pieces: Vec::new(),
            files: vec![file.clone()],
            total_length: content.len() as u64,
            private: false,
            announce: None,
            announce_list: Vec::new(),
            webseeds: Vec::new(),
            comment: None,
            created_by: None,
            creation_date: None,
            is_multi_file: false,
            v2: Some(V2TorrentMeta {
                meta_version: 2,
                files: vec![file],
                piece_layers: vec![V2PieceLayer {
                    pieces_root,
                    hashes,
                }],
                piece_layers_verified: true,
            }),
            raw_info: Some(raw_info),
        };
        meta.validate().unwrap();
        meta
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
        let global_peer_permits = PeerPermitPool::unlimited();
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
                    PeerSessionBudget::new(
                        global_peer_permits.clone(),
                        PeerPermitPool::unlimited(),
                    ),
                    shutdown_rx,
                ))
                .await
                .unwrap();
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
            global_peer_permits,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registered_required_encryption_rejects_plaintext_when_listener_prefers() {
        let content = b"registered encryption policy fixture";
        let bytes = build_single_file_torrent(
            "registered-required-encryption.bin",
            content,
            content.len() as u64,
            None,
            false,
        );
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("registered-required-encryption");
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
        let registry = SeedRegistry::default();
        let global_peer_permits = PeerPermitPool::unlimited();
        let (torrent_shutdown_tx, torrent_shutdown_rx) = tokio::sync::watch::channel(false);
        registry
            .register(
                SeedRegistration::new(
                    meta.clone(),
                    storage,
                    None,
                    state,
                    peer_id(b"-REQSRV-"),
                    RateLimiter::unlimited(),
                    None,
                    PeerSessionBudget::new(
                        global_peer_permits.clone(),
                        PeerPermitPool::unlimited(),
                    ),
                    torrent_shutdown_rx,
                )
                .with_encryption_mode(PeerEncryptionMode::Required),
            )
            .await
            .unwrap();
        let (hub_shutdown_tx, hub_shutdown_rx) = tokio::sync::watch::channel(false);
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let hub = SeederHub::new(
            registry,
            Arc::new(LoopbackBinder),
            0,
            PeerEncryptionMode::Preferred,
            hub_shutdown_rx,
            global_peer_permits,
        )
        .with_bound_addr(bound_tx);
        let task = tokio::spawn(hub.run());
        let address = bound_rx.await.unwrap();

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut read, mut write) = tokio::io::split(stream);
        peer::write_handshake(
            &mut write,
            &Handshake {
                info_hash: meta.info_hash,
                peer_id: peer_id(b"-REQCLI-"),
                reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
            },
        )
        .await
        .unwrap();
        write.flush().await.unwrap();
        let mut byte = [0u8; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), read.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0,
            "a per-torrent required registration must not serve a plaintext peer"
        );

        let _ = torrent_shutdown_tx.send(true);
        let _ = hub_shutdown_tx.send(true);
        task.await.unwrap().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn required_encryption_routes_a_pure_v2_peer_by_its_truncated_wire_key() {
        let content = vec![0xB7; (2 * V2_BLOCK_LENGTH + 17) as usize];
        let meta = pure_v2_meta("v2-required-mse.bin", &content);
        let layout = meta.v2_piece_layout().unwrap();
        let wire_hash = meta.identity.v2_peer_info_hash().unwrap();
        assert_eq!(
            wire_hash,
            meta.identity.v2_info_hash().unwrap().peer_info_hash(),
            "the MSE stream key must be the BEP 52 wire truncation"
        );

        let dir = unique_dir("v2-required-mse");
        let storage = Arc::new(
            StorageIo::new(meta.clone(), dir.clone())
                .with_torrent_key(TorrentKey::v2(meta.identity.v2_info_hash().unwrap())),
        );
        for index in 0..layout.piece_count() {
            let piece = layout.piece(index).unwrap();
            let start = usize::try_from(piece.offset).unwrap();
            let end = start + usize::try_from(piece.length).unwrap();
            storage
                .write_v2_piece(&layout, index, &content[start..end])
                .await
                .unwrap();
        }
        let mut have = PieceBitfield::new(layout.piece_count());
        for index in 0..layout.piece_count() {
            have.set(index);
        }
        let state = Arc::new(Mutex::new(EngineState {
            piece_count: layout.piece_count(),
            total_length: meta.total_length,
            pieces_have: have,
            finished: true,
            ..EngineState::default()
        }));

        let registry = SeedRegistry::default();
        let global_peer_permits = PeerPermitPool::unlimited();
        let (torrent_shutdown_tx, torrent_shutdown_rx) = tokio::sync::watch::channel(false);
        registry
            .register(
                SeedRegistration::new(
                    meta.clone(),
                    storage,
                    None,
                    state,
                    peer_id(b"-V2MSRV-"),
                    RateLimiter::unlimited(),
                    None,
                    PeerSessionBudget::new(
                        global_peer_permits.clone(),
                        PeerPermitPool::unlimited(),
                    ),
                    torrent_shutdown_rx,
                )
                .with_encryption_mode(PeerEncryptionMode::Required),
            )
            .await
            .unwrap();
        let (hub_shutdown_tx, hub_shutdown_rx) = tokio::sync::watch::channel(false);
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let hub = SeederHub::new(
            registry,
            Arc::new(LoopbackBinder),
            0,
            PeerEncryptionMode::Preferred,
            hub_shutdown_rx,
            global_peer_permits,
        )
        .with_bound_addr(bound_tx);
        let task = tokio::spawn(hub.run());
        let address = bound_rx.await.unwrap();

        let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
        let encrypted = swarmotter_core::mse::connect(tcp, wire_hash).await.unwrap();
        let (read, mut write) = tokio::io::split(encrypted);
        peer::write_v2_handshake(
            &mut write,
            &V2Handshake {
                info_hash: wire_hash,
                peer_id: peer_id(b"-V2MCLT-"),
                reserved: peer::with_v2_support(swarmotter_core::extensions::EXTENSION_RESERVED),
            },
        )
        .await
        .unwrap();
        write.flush().await.unwrap();
        let mut reader = PeerReader::new(read);
        let remote = reader.read_v2_handshake().await.unwrap();
        assert_eq!(remote.info_hash, wire_hash);
        assert!(remote.supports_v2());
        assert!(matches!(
            reader.read_message().await.unwrap(),
            Some(Message::Bitfield { .. })
        ));

        drop(reader);
        drop(write);
        let _ = torrent_shutdown_tx.send(true);
        let _ = hub_shutdown_tx.send(true);
        task.await.unwrap().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mixed_inbound_and_metadata_outbound_share_both_lifetime_caps() {
        let content = b"generated mixed-direction peer budget".to_vec();
        let bytes = build_single_file_torrent(
            "mixed-direction.bin",
            &content,
            content.len() as u64,
            None,
            false,
        );
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("mixed-direction");
        let storage = Arc::new(StorageIo::new(meta.clone(), dir.clone()));
        storage.write_piece(0, &content).await.unwrap();
        let mut have = PieceBitfield::new(1);
        have.set(0);
        let state = Arc::new(Mutex::new(EngineState {
            piece_count: 1,
            total_length: content.len() as u64,
            pieces_have: have,
            finished: true,
            ..EngineState::default()
        }));
        let denied = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let global = PeerPermitPool::new(1, denied.clone()).unwrap();
        let torrent = PeerPermitPool::new(1, denied).unwrap();
        let budget = PeerSessionBudget::new(global.clone(), torrent.clone());
        let registry = SeedRegistry::default();
        let (torrent_shutdown_tx, torrent_shutdown_rx) = tokio::sync::watch::channel(false);
        registry
            .register(SeedRegistration::new(
                meta.clone(),
                storage,
                None,
                state,
                peer_id(b"-MIXSRV-"),
                RateLimiter::unlimited(),
                None,
                budget.clone(),
                torrent_shutdown_rx,
            ))
            .await
            .unwrap();
        let (hub_shutdown_tx, hub_shutdown_rx) = tokio::sync::watch::channel(false);
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let hub = SeederHub::new(
            registry,
            Arc::new(LoopbackBinder),
            0,
            PeerEncryptionMode::Disabled,
            hub_shutdown_rx,
            global.clone(),
        )
        .with_bound_addr(bound_tx);
        let hub_task = tokio::spawn(hub.run());
        let hub_addr = bound_rx.await.unwrap();

        let inbound = tokio::net::TcpStream::connect(hub_addr).await.unwrap();
        let (mut inbound_read, mut inbound_write) = tokio::io::split(inbound);
        peer::write_handshake(
            &mut inbound_write,
            &Handshake {
                info_hash: meta.info_hash,
                peer_id: peer_id(b"-MIXCLI-"),
                reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
            },
        )
        .await
        .unwrap();
        {
            let mut inbound_reader = PeerReader::new(&mut inbound_read);
            inbound_reader.read_handshake().await.unwrap();
            assert!(matches!(
                inbound_reader.read_message().await.unwrap(),
                Some(Message::Bitfield { .. })
            ));
        }
        assert_eq!(global.snapshot().in_use, 1);
        assert_eq!(torrent.snapshot().in_use, 1);

        // A second accepted socket is rejected before its handshake because
        // the process-wide permit is already held by the first inbound peer.
        let mut rejected = tokio::net::TcpStream::connect(hub_addr).await.unwrap();
        let mut byte = [0u8; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rejected.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(global.snapshot().denied, 1);

        let outbound_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let outbound_addr = outbound_listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move { outbound_listener.accept().await.unwrap().0 });
        let outbound_budget = budget.clone();
        let outbound_meta = meta.clone();
        let outbound_task = tokio::spawn(async move {
            let context = crate::metadata::MetadataFetchContext::new(
                outbound_budget,
                Arc::new(LoopbackBinder),
                outbound_meta.info_hash,
                peer_id(b"-MIXOUT-"),
                false,
                true,
                PeerEncryptionMode::Disabled,
            );
            crate::metadata::fetch_metadata_with_transport(
                &context,
                swarmotter_core::peer::PeerAddr::from_socket_addr(outbound_addr),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!accept_task.is_finished());
        assert!(!outbound_task.is_finished());

        drop(inbound_read);
        drop(inbound_write);
        let outbound_stream = tokio::time::timeout(Duration::from_secs(2), accept_task)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while global.snapshot().in_use != 1 || torrent.snapshot().in_use != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(outbound_stream);
        assert!(outbound_task.await.unwrap().is_err());
        assert_eq!(global.snapshot().in_use, 0);
        assert_eq!(torrent.snapshot().in_use, 0);

        let _ = torrent_shutdown_tx.send(true);
        let _ = hub_shutdown_tx.send(true);
        hub_task.await.unwrap().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    /// Production-path upload shaping with Tokio's deterministic clock. The
    /// first 1 KiB consumes the token bucket's documented initial burst. A
    /// second 1 KiB must still be blocked at 400 ms under 1 KiB/s, then finish
    /// at the limiter's 500 ms wake boundary after a live increase to 4 KiB/s.
    /// The 100 ms virtual-time tolerance accounts only for that bounded wake
    /// interval. Wall time only bounds request-dispatch synchronization; all
    /// shaping assertions use the paused virtual clock.
    #[tokio::test(start_paused = true)]
    async fn active_registered_upload_observes_live_limit_without_replacement() {
        let content = vec![0x5au8; 4096];
        let bytes = build_single_file_torrent("live-limit.bin", &content, 4096, None, false);
        let meta = parse_torrent(&bytes).unwrap();
        let dir = unique_dir("live-limit");
        let storage = Arc::new(StorageIo::new(meta.clone(), dir.clone()));
        storage.write_piece(0, &content).await.unwrap();
        let mut have = PieceBitfield::new(1);
        have.set(0);
        let state = Arc::new(Mutex::new(EngineState {
            piece_count: 1,
            total_length: content.len() as u64,
            pieces_have: have,
            finished: true,
            ..EngineState::default()
        }));
        let limiter = Arc::new(RateLimiter::new(0, 1024));
        let (torrent_shutdown_tx, torrent_shutdown_rx) = tokio::sync::watch::channel(false);
        let registry = SeedRegistry::default();
        let global_peer_permits = PeerPermitPool::unlimited();
        let peer_session_budget =
            PeerSessionBudget::new(global_peer_permits.clone(), PeerPermitPool::unlimited());
        registry
            .register(SeedRegistration::new(
                meta.clone(),
                storage,
                None,
                state.clone(),
                peer_id(b"-LVLIM1-"),
                limiter.clone(),
                None,
                peer_session_budget,
                torrent_shutdown_rx,
            ))
            .await
            .unwrap();
        let registered_limiter = registry
            .inner
            .read()
            .await
            .contexts
            .get(&TorrentKey::v1(meta.info_hash))
            .unwrap()
            .limiter
            .per_torrent
            .clone();
        assert!(Arc::ptr_eq(&limiter, &registered_limiter));

        let (hub_shutdown_tx, hub_shutdown_rx) = tokio::sync::watch::channel(false);
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let hub = SeederHub::new(
            registry.clone(),
            Arc::new(LoopbackBinder),
            0,
            PeerEncryptionMode::Preferred,
            hub_shutdown_rx,
            global_peer_permits,
        )
        .with_bound_addr(bound_tx);
        let hub_task = tokio::spawn(hub.run());
        let address = bound_rx.await.unwrap();

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (read, mut write) = tokio::io::split(stream);
        peer::write_handshake(
            &mut write,
            &Handshake {
                info_hash: meta.info_hash,
                peer_id: peer_id(b"-LVCLI1-"),
                reserved: swarmotter_core::extensions::EXTENSION_RESERVED,
            },
        )
        .await
        .unwrap();
        let mut reader = PeerReader::new(read);
        reader.read_handshake().await.unwrap();
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

        for offset in [0, 1024] {
            peer::write_message(
                &mut write,
                &Message::Request {
                    piece: 0,
                    offset,
                    length: 1024,
                },
            )
            .await
            .unwrap();
            if offset == 0 {
                assert!(matches!(
                    reader.read_message().await.unwrap(),
                    Some(Message::Piece { block, .. }) if block.len() == 1024
                ));
            }
        }

        let second_block = tokio::spawn(async move { reader.read_message().await });
        // Accounted bytes are updated immediately before the production
        // limiter is awaited. Wait for the second request to reach that point,
        // then yield once more so its 500 ms limiter sleep is armed before
        // advancing the paused clock. A single unconditional yield can race
        // request dispatch when the full test suite is scheduling many tasks.
        let dispatch_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while state.lock().await.uploaded != 2048 {
            assert!(
                std::time::Instant::now() < dispatch_deadline,
                "second upload request did not reach the live limiter"
            );
            std::thread::yield_now();
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(400)).await;
        tokio::task::yield_now().await;
        assert!(
            !second_block.is_finished(),
            "old 1 KiB/s window was not enforced"
        );
        limiter.set_capacity(RateDirection::Upload, 4096);
        assert!(registry.contains(&TorrentKey::v1(meta.info_hash)).await);
        assert!(Arc::ptr_eq(&limiter, &registered_limiter));
        tokio::time::advance(Duration::from_millis(100)).await;
        for _ in 0..100 {
            if second_block.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            second_block.is_finished(),
            "new 4 KiB/s window was not observed live"
        );
        assert!(matches!(
            second_block.await.unwrap().unwrap(),
            Some(Message::Piece { block, .. }) if block.len() == 1024
        ));

        let _ = torrent_shutdown_tx.send(true);
        let _ = hub_shutdown_tx.send(true);
        hub_task.await.unwrap().unwrap();
        std::fs::remove_dir_all(dir).ok();
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
            PeerSessionBudget::unlimited(),
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
            PeerSessionBudget::unlimited(),
        )
        .with_encryption_mode(PeerEncryptionMode::Required)
        .with_bound_addr(bound_tx);
        let seeder_task = tokio::spawn(async move { seeder.run().await });
        let seeder_addr = bound_rx.await.expect("seeder bound its listener");

        let tcp = tokio::net::TcpStream::connect(seeder_addr).await.unwrap();
        let encrypted = swarmotter_core::mse::connect(tcp, PeerInfoHash::from_v1(meta.info_hash))
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
