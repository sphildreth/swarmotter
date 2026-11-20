// SPDX-License-Identifier: Apache-2.0

use super::*;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant as TokioInstant};

// Leave room for a full window of replies while requesting our own blocks.
const REQUEST_WINDOW: usize = MAX_INBOUND_SERVE_QUEUE / 2;

enum V2Write {
    Frame(Message),
    Upload {
        piece: u32,
        offset: u32,
        length: u32,
        verified: bool,
    },
}

fn enqueue(out: &mpsc::Sender<V2Write>, frame: V2Write) -> Result<()> {
    // Waiting for queue space here would stop socket reads while the peer
    // is waiting for us to drain its own send buffer.
    out.try_send(frame).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => {
            CoreError::Parse("v2 peer exceeded the pending write limit".into())
        }
        mpsc::error::TrySendError::Closed(_) => {
            CoreError::Io(std::io::ErrorKind::BrokenPipe.into())
        }
    })
}

pub(super) struct V2Peer<'a> {
    pub(super) engine: &'a TorrentEngine,
    pub(super) storage: &'a StorageIo,
    pub(super) layout: &'a V2PieceLayout,
    pub(super) peer: PeerAddr,
}

impl V2Peer<'_> {
    async fn serve<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        verified: bool,
        piece: u32,
        offset: u32,
        length: u32,
    ) -> Result<()> {
        let index = piece as usize;
        let valid = length > 0
            && length <= peer::BLOCK_SIZE
            && verified
            && self
                .layout
                .piece(index)
                .is_some_and(|mapping| u64::from(offset) + u64::from(length) <= mapping.length);
        if valid {
            if let Ok(block) = self
                .storage
                .read_v2_block(self.layout, index, u64::from(offset), length as usize)
                .await
            {
                self.engine
                    .limiter
                    .acquire(RateDirection::Upload, u64::from(length))
                    .await;
                peer::write_message(
                    writer,
                    &Message::Piece {
                        piece,
                        offset,
                        block,
                    },
                )
                .await?;
                writer.flush().await?;
                record_peer_uploaded(&self.engine.state, self.peer, u64::from(length)).await;
                return Ok(());
            }
        }
        peer::write_message(
            writer,
            &Message::Reject {
                piece,
                offset,
                length,
            },
        )
        .await?;
        writer.flush().await?;
        Ok(())
    }

    pub(super) async fn exchange<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        &self,
        reader: PeerReader<R>,
        mut writer: W,
        have: &mut PieceBitfield,
        duration: Duration,
    ) -> Result<bool> {
        let deadline = TokioInstant::now() + duration;
        let (out, mut queue) = mpsc::channel(MAX_INBOUND_SERVE_QUEUE);
        let write = async {
            while let Some(command) = queue.recv().await {
                if !self.engine.binder.traffic_allowed() {
                    return Err(CoreError::NetworkBlocked("v2 peer traffic blocked".into()));
                }
                match command {
                    V2Write::Frame(message) => {
                        peer::write_message(&mut writer, &message).await?;
                        writer.flush().await?;
                    }
                    V2Write::Upload {
                        piece,
                        offset,
                        length,
                        verified,
                    } => {
                        self.serve(&mut writer, verified, piece, offset, length)
                            .await?;
                    }
                }
            }
            Ok(())
        };
        // Both futures belong to the session. Drain accepted replies on a
        // normal close, and drop any blocked write when the deadline expires.
        let bounded_write = async { timeout_at(deadline, write).await.unwrap_or(Ok(())) };
        let (progress, ()) =
            tokio::try_join!(self.receive(reader, out, have, deadline), bounded_write)?;
        Ok(progress)
    }

    async fn next_message<R: AsyncRead + Unpin>(
        &self,
        reader: &mut PeerReader<R>,
        out: &mpsc::Sender<V2Write>,
        have: &PieceBitfield,
        deadline: TokioInstant,
    ) -> Result<Option<Message>> {
        while self.engine.binder.traffic_allowed() && TokioInstant::now() < deadline {
            match timeout_at(deadline, reader.read_message()).await {
                Ok(Ok(Some(Message::Request {
                    piece,
                    offset,
                    length,
                }))) => {
                    enqueue(
                        out,
                        V2Write::Upload {
                            piece,
                            offset,
                            length,
                            verified: have.has(piece as usize),
                        },
                    )?;
                }
                Ok(result) => return result,
                Err(_) => break,
            }
        }
        Ok(None)
    }

    async fn receive<R: AsyncRead + Unpin>(
        &self,
        mut reader: PeerReader<R>,
        out: mpsc::Sender<V2Write>,
        have: &mut PieceBitfield,
        deadline: TokioInstant,
    ) -> Result<bool> {
        let layout = self.layout;
        let storage = self.storage;
        let mut ours = Bitfield::new(layout.piece_count());
        for index in 0..layout.piece_count() {
            if have.has(index) {
                ours.set(index);
            }
        }
        for message in [ours.encode_message(), Message::Interested, Message::Unchoke] {
            enqueue(&out, V2Write::Frame(message))?;
        }
        let mut peer_bitfield: Option<Bitfield> = None;
        let mut peer_choking = true;
        let mut peer_interested = false;
        let mut interested = true;
        let mut made_progress = false;

        loop {
            if !self.engine.binder.traffic_allowed() || TokioInstant::now() >= deadline {
                return Ok(made_progress);
            }

            if !peer_choking {
                if let Some(piece_index) = peer_bitfield
                    .as_ref()
                    .and_then(|bits| self.engine.pick_v2_piece(layout, bits, have))
                {
                    if !interested {
                        enqueue(&out, V2Write::Frame(Message::Interested))?;
                        interested = true;
                    }
                    let Some(piece) = layout.piece(piece_index) else {
                        return Err(CoreError::Internal(
                            "v2 layout lost a selected piece".into(),
                        ));
                    };
                    let piece_length = u32::try_from(piece.length).map_err(|_| {
                        CoreError::MalformedTorrent(format!(
                            "BEP 52 piece {piece_index} length {} exceeds peer-wire range",
                            piece.length
                        ))
                    })?;
                    let requests = peer::block_requests(piece_length);
                    let wire_piece = u32::try_from(piece_index).map_err(|_| {
                        CoreError::MalformedTorrent(
                            "BEP 52 piece index exceeds peer-wire range".into(),
                        )
                    })?;
                    let mut pending = requests.iter();
                    let request = |&(offset, length): &(u32, u32)| {
                        enqueue(
                            &out,
                            V2Write::Frame(Message::Request {
                                piece: wire_piece,
                                offset,
                                length,
                            }),
                        )
                    };
                    for block in pending.by_ref().take(REQUEST_WINDOW) {
                        request(block)?;
                    }

                    let mut assembler =
                        peer::PieceAssembler::new(piece_index as u32, piece_length as usize);
                    let mut received = 0usize;
                    let mut rejected = false;
                    while received < requests.len() {
                        let Some(message) =
                            self.next_message(&mut reader, &out, have, deadline).await?
                        else {
                            return Ok(made_progress);
                        };
                        match message {
                            Message::Piece {
                                piece: received_piece,
                                offset,
                                block,
                            } if received_piece as usize == piece_index => {
                                let block_index = (offset / peer::BLOCK_SIZE) as usize;
                                let sent = requests.len() - pending.len();
                                if block_index >= sent
                                    || requests[block_index] != (offset, block.len() as u32)
                                {
                                    continue;
                                }
                                let was_missing = assembler
                                    .received
                                    .get(block_index)
                                    .is_some_and(|value| !*value);
                                if was_missing && assembler.add_block(offset, &block).is_ok() {
                                    received += 1;
                                    if let Some(block) = pending.next() {
                                        request(block)?;
                                    }
                                    record_peer_block(
                                        &self.engine.state,
                                        self.peer,
                                        block.len() as u64,
                                    )
                                    .await;
                                }
                            }
                            Message::Reject {
                                piece: rejected_piece,
                                ..
                            } if rejected_piece as usize == piece_index => {
                                rejected = true;
                                break;
                            }
                            Message::Choke => {
                                peer_choking = true;
                                record_peer_choked(&self.engine.state, self.peer).await;
                                break;
                            }
                            Message::Unchoke => {
                                peer_choking = false;
                                record_peer_unchoked(&self.engine.state, self.peer).await;
                            }
                            Message::Bitfield { bits } => {
                                let bits = Bitfield::from_bytes(bits, layout.piece_count());
                                record_peer_availability(
                                    &self.engine.state,
                                    self.peer,
                                    &bits,
                                    have,
                                    layout.piece_count(),
                                )
                                .await;
                                peer_bitfield = Some(bits);
                            }
                            Message::Have { piece } => {
                                apply_peer_have(&mut peer_bitfield, layout.piece_count(), piece);
                            }
                            Message::Interested => peer_interested = true,
                            Message::NotInterested => peer_interested = false,
                            Message::Keepalive
                            | Message::Request { .. }
                            | Message::Cancel { .. }
                            | Message::Reject { .. }
                            | Message::HashRequest { .. }
                            | Message::Hashes { .. }
                            | Message::HashReject { .. }
                            | Message::Extended { .. }
                            | Message::Unknown { .. }
                            | Message::Piece { .. } => {}
                        }
                    }

                    if rejected || received != requests.len() {
                        continue;
                    }
                    let data = assembler.data().to_vec();
                    if !layout.verify_piece(piece_index, &data) {
                        record_peer_hash_failure(&self.engine.state, self.peer).await;
                        continue;
                    }
                    self.engine
                        .limiter
                        .acquire(RateDirection::Download, data.len() as u64)
                        .await;
                    storage.write_v2_piece(layout, piece_index, &data).await?;
                    have.set(piece_index);
                    made_progress = true;
                    self.engine.update_v2_progress(layout, have).await;
                    // Coalesced checkpoint policy (ADR-0074).
                    self.engine.mark_resume_piece_verified();
                    self.engine
                        .maybe_persist_v2_resume(storage, layout, have, false)
                        .await?;
                    enqueue(&out, V2Write::Frame(Message::Have { piece: wire_piece }))?;
                    continue;
                }
                if peer_bitfield.is_some() {
                    if interested {
                        enqueue(&out, V2Write::Frame(Message::NotInterested))?;
                        interested = false;
                    }
                    if !peer_interested {
                        return Ok(made_progress);
                    }
                }
            }

            let Some(message) = self.next_message(&mut reader, &out, have, deadline).await? else {
                return Ok(made_progress);
            };
            match message {
                Message::Unchoke => {
                    peer_choking = false;
                    record_peer_unchoked(&self.engine.state, self.peer).await;
                }
                Message::Choke => {
                    peer_choking = true;
                    record_peer_choked(&self.engine.state, self.peer).await;
                }
                Message::Bitfield { bits } => {
                    let bits = Bitfield::from_bytes(bits, layout.piece_count());
                    record_peer_availability(
                        &self.engine.state,
                        self.peer,
                        &bits,
                        have,
                        layout.piece_count(),
                    )
                    .await;
                    peer_bitfield = Some(bits);
                }
                Message::Have { piece } => {
                    apply_peer_have(&mut peer_bitfield, layout.piece_count(), piece);
                }
                Message::Interested => peer_interested = true,
                Message::NotInterested => peer_interested = false,
                Message::Keepalive
                | Message::Request { .. }
                | Message::Piece { .. }
                | Message::Cancel { .. }
                | Message::Reject { .. }
                | Message::HashRequest { .. }
                | Message::Hashes { .. }
                | Message::HashReject { .. }
                | Message::Extended { .. }
                | Message::Unknown { .. } => {}
            }
        }
    }
}

#[cfg(test)]
mod tests;
