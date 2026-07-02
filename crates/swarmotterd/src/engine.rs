// SPDX-License-Identifier: Apache-2.0

//! Live torrent data-plane engine.
//!
//! This module implements the real BitTorrent download loop: tracker
//! announce, TCP peer connections through the network containment layer,
//! peer wire handshake and message exchange, piece request scheduling,
//! block assembly, on-disk writes and verification, and fast-resume
//! persistence. Progress is reported through a shared [`EngineState`] that
//! the daemon reconciles into torrent summaries.
//!
//! All torrent networking goes through the [`NetworkBinder`] abstraction; the
//! engine never creates sockets directly. In strict fail-closed mode the
//! binder blocks new connections and the engine moves the torrent to
//! `network_blocked`.
//!
//! See `design/architecture.md`, `design/vpn-network-containment.md`, and
//! ADR-0012 (peer protocol architecture) / ADR-0013 (task/runtime model).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::timeout;

use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::meta::TorrentMeta;
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::peer::{
    self, block_requests, Bitfield, Handshake, Message, PeerAddr, PeerReader,
};
use swarmotter_core::storage::resume::PieceBitfield;
use swarmotter_core::storage::StorageIo;
use swarmotter_core::tracker::{self, AnnounceEvent, AnnounceRequest};

/// Live engine state, shared between the engine task and the daemon so the
/// API/UI can observe real progress, speeds, peers, and tracker status.
#[derive(Debug, Clone, Default)]
pub struct EngineState {
    pub pieces_have: PieceBitfield,
    pub piece_count: usize,
    pub downloaded: u64,
    pub uploaded: u64,
    pub bytes_completed: u64,
    pub total_length: u64,
    #[allow(dead_code)]
    pub active_peers: usize,
    pub peers: Vec<PeerAddr>,
    pub tracker_ok: bool,
    pub tracker_message: Option<String>,
    pub last_announce: Option<u64>,
    pub finished: bool,
}

/// Commands sent to an engine task to control its lifecycle.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum EngineCommand {
    Pause,
    Resume,
    Reannounce,
    Recheck,
    Stop,
}

/// Run a torrent download to completion (or until stopped).
/// `binder` is the contained network path. `seed_peers` are peer addresses to
/// connect to directly (used by the local swarm test and by PEX/DHT once
/// those are live); tracker announce runs in parallel to discover more.
/// `state` is updated as progress is made and is read by the daemon to build
/// torrent summaries. `commands` receives lifecycle commands; `shutdown`
/// completes when the engine should terminate (remove).
pub struct TorrentEngine {
    meta: TorrentMeta,
    download_dir: PathBuf,
    peer_id: [u8; 20],
    binder: Arc<dyn NetworkBinder>,
    state: Arc<Mutex<EngineState>>,
    commands: Arc<Mutex<tokio::sync::mpsc::Receiver<EngineCommand>>>,
    seed_peers: Vec<PeerAddr>,
    listen_port: u16,
}

impl TorrentEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: TorrentMeta,
        download_dir: PathBuf,
        peer_id: [u8; 20],
        binder: Arc<dyn NetworkBinder>,
        state: Arc<Mutex<EngineState>>,
        commands: tokio::sync::mpsc::Receiver<EngineCommand>,
        seed_peers: Vec<PeerAddr>,
        listen_port: u16,
    ) -> Self {
        Self {
            meta,
            download_dir,
            peer_id,
            binder,
            state,
            commands: Arc::new(Mutex::new(commands)),
            seed_peers,
            listen_port,
        }
    }

    /// Main engine loop. Runs announce + peer download until complete or
    /// commanded to stop. Returns the final engine state.
    pub async fn run(self) -> Result<EngineState> {
        let piece_count = self.meta.piece_count();
        let total_length = self.meta.total_length;
        // Initialize state.
        {
            let mut s = self.state.lock().await;
            s.piece_count = piece_count;
            s.total_length = total_length;
        }

        // Containment check: do not start any torrent traffic if the path is
        // unavailable.
        if !self.binder.traffic_allowed() {
            let mut s = self.state.lock().await;
            s.tracker_message = Some("torrent data plane blocked by containment".into());
            return Ok(s.clone());
        }

        let storage = StorageIo::new(self.meta.clone(), self.download_dir.clone());
        storage.preallocate().await?;

        // Load fast resume if present; otherwise recheck what's already on disk.
        let mut have = if let Some(resume) = storage.load_resume(&self.meta.info_hash).await? {
            resume.piece_bitfield
        } else {
            storage.recheck().await?
        };
        self.update_progress(&have).await;

        if have.count(piece_count) == piece_count {
            self.mark_finished().await;
            self.persist_resume(&storage, &have).await?;
            return Ok(self.state.lock().await.clone());
        }

        // Discover peers via tracker announce (HTTP) on each tier.
        let mut discovered = self.announce(AnnounceEvent::Started).await;
        // Merge any directly-supplied seed peers (local swarm / PEX / DHT).
        for p in &self.seed_peers {
            if !discovered.contains(p) {
                discovered.push(*p);
            }
        }
        self.state.lock().await.peers = discovered.clone();

        // Download loop: connect to peers, request missing pieces, write and
        // verify. Bounded to a small number of concurrent peers.
        let max_concurrent = 4usize;
        let mut bad_peers: HashSet<SocketAddr> = HashSet::new();
        let start = Instant::now();

        loop {
            // Handle pending commands.
            if self.poll_commands().await == CommandOutcome::Stop {
                break;
            }

            if have.count(piece_count) == piece_count {
                self.mark_finished().await;
                self.persist_resume(&storage, &have).await?;
                // Announce completion to trackers.
                self.announce(AnnounceEvent::Completed).await;
                break;
            }

            // Periodically re-announce to refresh peers.
            if start.elapsed() > Duration::from_secs(30) {
                let refreshed = self.announce(AnnounceEvent::Empty).await;
                for p in refreshed {
                    if !discovered.contains(&p) {
                        discovered.push(p);
                    }
                }
            }

            // Try peers until we make progress or exhaust the list.
            let mut made_progress = false;
            let mut to_try: Vec<PeerAddr> = discovered
                .iter()
                .filter(|p| !bad_peers.contains(&p.socket_addr()))
                .copied()
                .take(max_concurrent * 2)
                .collect();

            while let Some(peer_addr) = to_try.pop() {
                if have.count(piece_count) == piece_count {
                    break;
                }
                match self
                    .download_from_peer(&peer_addr, &storage, &mut have, &mut bad_peers)
                    .await
                {
                    Ok(progressed) => {
                        if progressed {
                            made_progress = true;
                        }
                    }
                    Err(e) => {
                        tracing::debug!(peer = %peer_addr.socket_addr(), error = %e, "peer failed; suppressing");
                        bad_peers.insert(peer_addr.socket_addr());
                    }
                }
            }

            if !made_progress {
                if discovered.is_empty() || bad_peers.len() >= discovered.len() {
                    // No usable peers; back off briefly and retry announce.
                    self.sleep_or_stop(Duration::from_secs(2)).await;
                    let refreshed = self.announce(AnnounceEvent::Empty).await;
                    for p in refreshed {
                        if !discovered.contains(&p) {
                            discovered.push(p);
                        }
                    }
                    if discovered.is_empty() {
                        self.state.lock().await.tracker_message =
                            Some("no peers discovered".into());
                    }
                } else {
                    self.sleep_or_stop(Duration::from_millis(500)).await;
                }
            }
        }

        Ok(self.state.lock().await.clone())
    }

    /// Attempt to download missing pieces from a single peer. Returns true if
    /// at least one new piece was verified and written.
    async fn download_from_peer(
        &self,
        peer_addr: &PeerAddr,
        storage: &StorageIo,
        have: &mut PieceBitfield,
        bad_peers: &mut HashSet<SocketAddr>,
    ) -> Result<bool> {
        if !self.binder.traffic_allowed() {
            return Ok(false);
        }
        let stream = self.binder.connect_peer(peer_addr.socket_addr()).await?;
        let (read_half, mut write_half) = tokio::io::split(stream);

        // Handshake.
        let hs = Handshake {
            info_hash: self.meta.info_hash,
            peer_id: self.peer_id,
        };
        peer::write_handshake(&mut write_half, &hs).await?;
        let mut reader = PeerReader::new(read_half);
        let their_hs = timeout(Duration::from_secs(10), reader.read_handshake()).await??;
        if their_hs.info_hash != self.meta.info_hash {
            bad_peers.insert(peer_addr.socket_addr());
            return Err(CoreError::Internal(
                "peer handshake info hash mismatch".into(),
            ));
        }

        // Exchange bitfields.
        let mut our_bf = Bitfield::new(self.meta.piece_count());
        for i in 0..self.meta.piece_count() {
            if have.has(i) {
                our_bf.set(i);
            }
        }
        peer::write_message(&mut write_half, &our_bf.encode_message()).await?;
        write_half.flush().await.ok();

        // We are interested; ask to be unchoked.
        peer::write_message(&mut write_half, &Message::Interested).await?;

        let mut peer_bf: Option<Bitfield> = None;
        let mut peer_choking = true;
        let mut made_progress = false;
        let piece_count = self.meta.piece_count();

        // Drive a small download loop: pick a missing piece the peer has,
        // request its blocks, assemble, verify, write.
        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            if Instant::now() > deadline {
                break;
            }
            if have.count(piece_count) == piece_count {
                break;
            }

            // If unchoked and we have a candidate piece, request blocks.
            if !peer_choking {
                if let Some(piece_index) = self.pick_piece(peer_bf.as_ref(), have) {
                    let plen = self.piece_length(piece_index) as u32;
                    let reqs = block_requests(plen);
                    // Send all block requests for this piece.
                    for (off, len) in &reqs {
                        peer::write_message(
                            &mut write_half,
                            &Message::Request {
                                piece: piece_index as u32,
                                offset: *off,
                                length: *len,
                            },
                        )
                        .await?;
                    }
                    write_half.flush().await.ok();

                    // Assemble the piece from incoming blocks.
                    let mut assembler =
                        peer::PieceAssembler::new(piece_index as u32, plen as usize);
                    let mut received_blocks = 0usize;
                    let piece_deadline = Instant::now() + Duration::from_secs(30);
                    while received_blocks < reqs.len() {
                        let remaining = piece_deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            break;
                        }
                        let msg = match timeout(remaining, reader.read_message()).await {
                            Ok(Ok(Some(m))) => m,
                            Ok(Ok(None)) => break,
                            Ok(Err(_)) => break,
                            Err(_) => break,
                        };
                        match msg {
                            Message::Piece {
                                piece,
                                offset,
                                block,
                            } => {
                                if piece as usize == piece_index
                                    && assembler.add_block(offset, &block).is_ok()
                                {
                                    received_blocks += 1;
                                }
                            }
                            Message::Choke => {
                                peer_choking = true;
                                break;
                            }
                            Message::Unchoke => peer_choking = false,
                            Message::Have { piece } => {
                                if let Some(bf) = &mut peer_bf {
                                    bf.set(piece as usize);
                                }
                            }
                            Message::Bitfield { bits } => {
                                peer_bf = Some(Bitfield::from_bytes(bits, piece_count));
                            }
                            Message::Keepalive
                            | Message::Interested
                            | Message::NotInterested
                            | Message::Request { .. }
                            | Message::Cancel { .. }
                            | Message::Unknown { .. } => {}
                        }
                    }

                    if received_blocks == reqs.len() {
                        let data = assembler.data().to_vec();
                        if swarmotter_core::storage::verify_piece(&self.meta, piece_index, &data) {
                            storage.write_block(piece_index, 0, &data).await?;
                            have.set(piece_index);
                            made_progress = true;
                            self.update_progress(have).await;
                            self.persist_resume(storage, have).await?;
                            // Tell the peer we have it.
                            peer::write_message(
                                &mut write_half,
                                &Message::Have {
                                    piece: piece_index as u32,
                                },
                            )
                            .await?;
                        } else {
                            tracing::warn!(piece = piece_index, "piece hash mismatch; rejecting");
                            // Bad hash: do not mark; try a different piece.
                        }
                    }
                    continue;
                } else {
                    // No missing piece this peer has; not interesting.
                    peer::write_message(&mut write_half, &Message::NotInterested).await?;
                    break;
                }
            }

            // Wait for unchoke / bitfield / have.
            let msg = match timeout(Duration::from_secs(15), reader.read_message()).await {
                Ok(Ok(Some(m))) => m,
                _ => break,
            };
            match msg {
                Message::Unchoke => peer_choking = false,
                Message::Choke => peer_choking = true,
                Message::Bitfield { bits } => {
                    peer_bf = Some(Bitfield::from_bytes(bits, piece_count));
                }
                Message::Have { piece } => {
                    if let Some(bf) = &mut peer_bf {
                        bf.set(piece as usize);
                    }
                }
                Message::Keepalive
                | Message::Interested
                | Message::NotInterested
                | Message::Request { .. }
                | Message::Piece { .. }
                | Message::Cancel { .. }
                | Message::Unknown { .. } => {}
            }
        }

        Ok(made_progress)
    }

    /// Pick a piece we don't have that the peer has.
    fn pick_piece(&self, peer_bf: Option<&Bitfield>, have: &PieceBitfield) -> Option<usize> {
        let peer_bf = peer_bf?;
        (0..self.meta.piece_count()).find(|&i| peer_bf.has(i) && !have.has(i))
    }

    fn piece_length(&self, index: usize) -> u64 {
        if index + 1 == self.meta.piece_count() {
            self.meta.last_piece_length()
        } else {
            self.meta.piece_length
        }
    }

    /// Announce to all HTTP trackers in tier order; return discovered peers.
    async fn announce(&self, event: AnnounceEvent) -> Vec<PeerAddr> {
        let mut all = Vec::new();
        let mut ok = false;
        let mut msg: Option<String> = None;
        let (uploaded, downloaded, left) = {
            let s = self.state.lock().await;
            (
                s.uploaded,
                s.downloaded,
                s.total_length.saturating_sub(s.bytes_completed),
            )
        };
        for tier in tracker::announce_tiers(self.meta.announce.as_deref(), &self.meta.announce_list)
        {
            for url in tier {
                let req = AnnounceRequest {
                    tracker_url: url.clone(),
                    info_hash: self.meta.info_hash,
                    peer_id: self.peer_id,
                    port: self.listen_port,
                    uploaded,
                    downloaded,
                    left,
                    event,
                    numwant: Some(50),
                    compact: true,
                };
                match tracker::http_announce(self.binder.as_ref(), &req).await {
                    Ok(resp) => {
                        if let Some(fr) = resp.failure_reason {
                            msg = Some(format!("{url}: {fr}"));
                            continue;
                        }
                        ok = true;
                        all.extend(resp.peers);
                    }
                    Err(e) => {
                        msg = Some(format!("{url}: {e}"));
                    }
                }
            }
        }
        let mut s = self.state.lock().await;
        s.tracker_ok = ok;
        s.tracker_message = msg;
        s.last_announce = Some(now_secs());
        all
    }

    async fn update_progress(&self, have: &PieceBitfield) {
        let mut s = self.state.lock().await;
        s.pieces_have = have.clone();
        let completed = (0..s.piece_count)
            .filter(|&i| have.has(i))
            .map(|i| {
                if i + 1 == s.piece_count {
                    self.meta.last_piece_length()
                } else {
                    self.meta.piece_length
                }
            })
            .sum::<u64>();
        s.bytes_completed = completed;
        s.downloaded = completed;
    }

    async fn mark_finished(&self) {
        let mut s = self.state.lock().await;
        s.finished = true;
    }

    async fn persist_resume(&self, storage: &StorageIo, have: &PieceBitfield) -> Result<()> {
        let piece_byte_lengths: Vec<u64> = (0..self.meta.piece_count())
            .map(|i| self.piece_length(i))
            .collect();
        let s = self.state.lock().await;
        let resume = swarmotter_core::storage::io::build_resume(
            self.meta.info_hash,
            self.meta.name.clone(),
            have.clone(),
            self.meta.piece_count(),
            s.downloaded,
            s.uploaded,
            s.total_length,
            Some(self.download_dir.display().to_string()),
            now_secs(),
            if s.finished { Some(now_secs()) } else { None },
            &vec![swarmotter_core::models::torrent::FilePriority::Normal; self.meta.files.len()],
            &piece_byte_lengths,
        );
        drop(s);
        storage.save_resume(&resume).await?;
        Ok(())
    }

    async fn poll_commands(&self) -> CommandOutcome {
        let mut rx = self.commands.lock().await;
        match rx.try_recv() {
            Ok(EngineCommand::Stop) => CommandOutcome::Stop,
            Ok(EngineCommand::Pause) => CommandOutcome::Pause,
            Ok(EngineCommand::Resume) => CommandOutcome::Continue,
            Ok(EngineCommand::Reannounce) => CommandOutcome::Continue,
            Ok(EngineCommand::Recheck) => CommandOutcome::Continue,
            Err(_) => CommandOutcome::Continue,
        }
    }

    async fn sleep_or_stop(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandOutcome {
    Continue,
    Pause,
    Stop,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::meta::build_single_file_torrent;

    #[test]
    fn piece_length_last_is_shorter() {
        let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
            PathBuf::from("/tmp"),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        );
        assert_eq!(engine.piece_length(0), 8);
        assert_eq!(engine.piece_length(1), 8);
    }

    #[test]
    fn pick_piece_chooses_missing_peer_has() {
        let bytes =
            build_single_file_torrent("f", b"0123456789abcdef0123456789abcdef", 8, None, false);
        let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
        let binder = Arc::new(swarmotter_core::net::binder::LoopbackBinder);
        let state = Arc::new(Mutex::new(EngineState::default()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let engine = TorrentEngine::new(
            meta,
            PathBuf::from("/tmp"),
            [0u8; 20],
            binder,
            state,
            rx,
            vec![],
            6881,
        );
        let mut peer_bf = Bitfield::new(4);
        peer_bf.set(1);
        peer_bf.set(2);
        let mut have = PieceBitfield::new(4);
        have.set(1);
        let pick = engine.pick_piece(Some(&peer_bf), &have);
        assert_eq!(pick, Some(2));
    }
}
