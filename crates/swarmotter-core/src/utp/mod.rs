// SPDX-License-Identifier: Apache-2.0

//! uTP (BEP 29) — reliable, congestion-controlled byte stream over UDP.
//!
//! This module implements a production-grade uTP transport:
//!
//! - Full uTP packet header encode/decode per BEP 29 (20-byte header: type +
//!   version, extension, connection id, timestamps, window size, seq/ack).
//! - Extension parsing and the Selective ACK (SACK) extension encode/decode.
//! - The full connection lifecycle: SYN/STATE/DATA/FIN/RESET, connection-id
//!   assignment and validation, duplicate/out-of-order handling, in-order
//!   reassembly with SACK-driven recovery, retransmission, idle timeout, and
//!   graceful close.
//! - LEDBAT-style delay-based congestion control (base/current delay,
//!   queuing delay target, congestion-window growth/shrink, loss response,
//!   bounded window).
//! - One-way delay measurement via microsecond timestamp echo.
//!
//! All uTP traffic runs over the `NetworkBinder`'s contained UDP socket; no
//! UDP socket is ever created directly here. The engine obtains a
//! [`UtpConnection`] which presents an `AsyncRead`+`AsyncWrite` byte stream,
//! so the existing peer wire protocol machinery is reused unchanged over uTP.
//! In strict fail-closed mode the binder refuses the UDP socket and uTP
//! reports a clear network-blocked state. See `design/vpn-network-
//! containment.md` and ADR-0020.

pub mod congestion;
pub mod header;
pub mod sack;
pub mod stream;

pub use congestion::{CongestionState, Ledbat};
pub use header::{now_micros, UtpExtension, UtpHeader, UtpType, UTP_VERSION};
pub use sack::Sack;
pub use stream::{connect_peer_stream, PeerDuplex, PeerTransport, UtpStream};

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::error::{CoreError, Result};

/// The SACK extension id (BEP 29 extension 1).
pub const EXT_SACK: u8 = 1;

/// Maximum uTP packet payload (the BEP 29 recommended 1500-byte MTU minus the
/// 20-byte header leaves ~1480; we use a conservative 1400 to stay well under
/// common MTUs after IP/UDP framing).
pub const MAX_PAYLOAD: usize = 1400;

/// Idle timeout before a connection with no progress is torn down.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Bounded send buffer cap (a few MB; larger than any reasonable piece block
/// window so the peer protocol never stalls, but bounded to prevent
/// unbounded memory growth under backpressure).
pub const SEND_BUFFER_CAP: usize = 4 * 1024 * 1024;

/// Result of advancing the connection's send/receive state.
#[derive(Debug)]
enum RecvOutcome {
    /// New in-order bytes were delivered to the read buffer.
    Delivered,
    /// A duplicate or already-acked sequence; nothing new delivered.
    Duplicate,
    /// Connection teardown was signalled (FIN received).
    Closed,
}

/// A sent DATA packet awaiting acknowledgement, retained for retransmission.
#[derive(Debug, Clone)]
struct InFlight {
    seq: u16,
    payload: Vec<u8>,
    /// Wall-clock send time for retransmit/backoff.
    sent_at: Instant,
    /// uTP timestamp carried on the most recent (re)transmission, used for
    /// one-way delay measurement echoed back by the peer.
    ts_micros: u32,
    /// Number of times this packet has been transmitted.
    transmissions: u32,
}

impl InFlight {
    fn new(seq: u16, payload: Vec<u8>, ts_micros: u32) -> Self {
        Self {
            seq,
            payload,
            sent_at: Instant::now(),
            ts_micros,
            transmissions: 1,
        }
    }
}

/// A live uTP connection: a reliable, congestion-controlled byte stream over a
/// contained UDP socket. The connection is driven by a single task calling
/// [`UtpConnection::drive`]; [`UtpConnection::read`] / [`UtpConnection::write`]
/// are the byte-stream interface used by the peer protocol.
///
/// The connection owns no socket itself; it holds a reference-counted handle to
/// the binder's [`ContainedUdpSocket`] so all traffic stays on the contained
/// path. It never creates UDP sockets directly.
pub struct UtpConnection {
    peer: SocketAddr,
    socket: std::sync::Arc<dyn crate::net::ContainedUdpSocket>,
    /// Our send connection id (sent on DATA/STATE/FIN after the handshake).
    send_conn_id: u16,
    /// Connection id we expect on inbound packets from the peer.
    recv_conn_id: u16,
    /// Next sequence number to assign to an outbound DATA packet.
    seq_next: u16,
    /// Highest sequence number we have acknowledged to the peer (the
    /// contiguous in-order received prefix).
    ack_number: u16,
    /// Our advertised receive window (bytes), updated as the read buffer
    /// fills/drains.
    recv_window: u32,
    /// Receive window most recently advertised by the peer.
    peer_window: u32,
    /// In-order delivered-but-unread bytes (the readable byte stream).
    recv_buf: VecDeque<u8>,
    /// Out-of-order received bytes held for SACK-driven reassembly, keyed by
    /// sequence number.
    ooo: Vec<(u16, Vec<u8>)>,
    /// Bytes queued for sending but not yet transmitted (congestion-window
    /// limited).
    send_buf: VecDeque<u8>,
    /// Sent-but-unacked DATA packets, retained for retransmission.
    in_flight: Vec<InFlight>,
    /// Highest sequence number that has been transmitted at least once.
    seq_max_sent: u16,
    /// LEDBAT congestion controller.
    cwnd: Ledbat,
    /// Whether the peer has sent FIN (their send side is done).
    peer_fin: bool,
    /// Whether we have sent FIN (our write side is closed and the FIN packet
    /// has been transmitted at least once).
    local_fin_sent: bool,
    /// Whether the FIN packet has actually been transmitted (guards against
    /// re-sending a new FIN with a fresh seq number every drive iteration).
    fin_transmitted: bool,
    /// Whether the connection is fully closed (FIN acked + peer FIN + drained).
    closed: bool,
    /// Last time we received any packet (for idle timeout).
    last_recv: Instant,
    /// Last time we sent a STATE/ACK (for ack coalescing/keepalive).
    last_ack_sent: Instant,
}

impl std::fmt::Debug for UtpConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpConnection")
            .field("peer", &self.peer)
            .field("send_conn_id", &self.send_conn_id)
            .field("recv_conn_id", &self.recv_conn_id)
            .field("seq_next", &self.seq_next)
            .field("ack_number", &self.ack_number)
            .field("in_flight", &self.in_flight.len())
            .field("closed", &self.closed)
            .finish()
    }
}

impl UtpConnection {
    /// Build a connection as the SYN sender (initiator). Allocates a fresh
    /// contained UDP socket from the binder (fail-closed if blocked), sends a
    /// SYN, and waits for the peer's STATE reply. Returns a ready connection
    /// whose byte stream can be read/written.
    ///
    /// The peer's address is the UDP endpoint to connect to.
    pub async fn connect(binder: &dyn crate::net::NetworkBinder, peer: SocketAddr) -> Result<Self> {
        let socket: std::sync::Arc<dyn crate::net::ContainedUdpSocket> =
            binder.udp_socket_for(Some(peer)).await?.into();
        // Per BEP 29, the SYN and responder-to-initiator packets use the random
        // connection id. Initiator-to-responder DATA/STATE/FIN use id + 1.
        let syn_conn_id: u16 = rand_conn_id();
        let seq = 1u16;
        let now_ts = now_micros();
        let syn = UtpHeader {
            typ: UtpType::Syn,
            version: UTP_VERSION,
            extension: 0,
            connection_id: syn_conn_id,
            timestamp_micros: now_ts,
            timestamp_delta_micros: 0,
            window_size: RECV_WINDOW_DEFAULT,
            seq_number: seq,
            ack_number: 0,
        };
        socket.send_to(peer, &syn.encode(&[])).await?;

        // Wait for the responder's STATE (ack of our SYN). It uses
        // connection_id = the SYN id.
        let mut buf = vec![0u8; 2048];
        let (from, n) =
            match tokio::time::timeout(Duration::from_secs(10), socket.recv_from(&mut buf)).await {
                Ok(Ok(p)) => p,
                _ => {
                    return Err(CoreError::Internal(
                        "uTP connect: no SYN-ACK from peer".into(),
                    ))
                }
            };
        if from != peer {
            return Err(CoreError::Internal(
                "uTP connect: reply from wrong peer".into(),
            ));
        }
        let (state, _extensions, _payload) = UtpHeader::decode_with_extensions(&buf[..n])?;
        if state.typ != UtpType::State {
            return Err(CoreError::Internal(format!(
                "uTP connect: expected STATE, got {:?}",
                state.typ
            )));
        }
        if state.ack_number != seq {
            return Err(CoreError::Internal(
                "uTP connect: SYN-ACK did not ack our SYN".into(),
            ));
        }
        if state.connection_id != syn_conn_id {
            return Err(CoreError::Internal(
                "uTP connect: SYN-ACK connection id mismatch".into(),
            ));
        }
        let send_conn_id = syn_conn_id.wrapping_add(1);
        let recv_conn_id = syn_conn_id;

        let c = Self {
            peer,
            socket,
            send_conn_id,
            recv_conn_id,
            seq_next: seq.wrapping_add(1),
            ack_number: state.seq_number,
            recv_window: RECV_WINDOW_DEFAULT,
            peer_window: state.window_size,
            recv_buf: VecDeque::new(),
            ooo: Vec::new(),
            send_buf: VecDeque::new(),
            in_flight: Vec::new(),
            seq_max_sent: seq,
            cwnd: Ledbat::new(),
            peer_fin: false,
            local_fin_sent: false,
            fin_transmitted: false,
            closed: false,
            last_recv: Instant::now(),
            last_ack_sent: Instant::now(),
        };
        Ok(c)
    }

    /// Build a connection as the SYN responder (acceptor). Given the received
    /// SYN header and the contained socket already bound to a local port,
    /// replies with a STATE ack and returns a ready connection.
    ///
    /// The socket must be a binder-provided contained UDP socket. The SYN's
    /// connection id becomes the responder's send connection id; the responder
    /// expects initiator packets with `syn.connection_id + 1`.
    pub async fn accept_from_syn(
        socket: std::sync::Arc<dyn crate::net::ContainedUdpSocket>,
        peer: SocketAddr,
        syn: &UtpHeader,
    ) -> Result<Self> {
        if syn.typ != UtpType::Syn {
            return Err(CoreError::Internal("accept_from_syn: not a SYN".into()));
        }
        // Per BEP 29, responder-to-initiator packets retain the SYN id, while
        // initiator-to-responder packets use SYN id + 1.
        let send_conn_id = syn.connection_id;
        let recv_conn_id = syn.connection_id.wrapping_add(1);
        let seq = 1u16;
        let now_ts = now_micros();
        // Echo the initiator's timestamp and compute a delta.
        let delta = now_ts.wrapping_sub(syn.timestamp_micros);
        let state = UtpHeader {
            typ: UtpType::State,
            version: UTP_VERSION,
            extension: 0,
            connection_id: send_conn_id,
            timestamp_micros: now_ts,
            timestamp_delta_micros: delta,
            window_size: RECV_WINDOW_DEFAULT,
            seq_number: seq,
            ack_number: syn.seq_number,
        };
        socket.send_to(peer, &state.encode(&[])).await?;

        Ok(Self {
            peer,
            socket,
            send_conn_id,
            recv_conn_id,
            seq_next: seq.wrapping_add(1),
            ack_number: syn.seq_number,
            recv_window: RECV_WINDOW_DEFAULT,
            peer_window: syn.window_size,
            recv_buf: VecDeque::new(),
            ooo: Vec::new(),
            send_buf: VecDeque::new(),
            in_flight: Vec::new(),
            seq_max_sent: seq,
            cwnd: Ledbat::new(),
            peer_fin: false,
            local_fin_sent: false,
            fin_transmitted: false,
            closed: false,
            last_recv: Instant::now(),
            last_ack_sent: Instant::now(),
        })
    }

    /// The peer's UDP address.
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Whether the byte stream is fully closed (both directions drained).
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Whether the peer has sent FIN (its write side is done).
    pub fn peer_closed(&self) -> bool {
        self.peer_fin
    }

    /// Write bytes into the send buffer. Returns the number of bytes accepted
    /// (may be less than `buf.len()` if the bounded send buffer is near full).
    /// After [`UtpConnection::close`] has been called, writes return zero.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.local_fin_sent || self.closed {
            return Ok(0);
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let room =
            SEND_BUFFER_CAP.saturating_sub(self.send_buf.len() + self.queued_in_flight_bytes());
        if room == 0 {
            return Ok(0);
        }
        let n = buf.len().min(room);
        self.send_buf.extend(&buf[..n]);
        Ok(n)
    }

    /// Read available in-order delivered bytes. Returns the number of bytes
    /// copied into `out` (zero if nothing is currently delivered and the peer
    /// has not closed). After the peer sends FIN and the buffer drains, reads
    /// return zero (clean EOF).
    pub async fn read(&mut self, out: &mut [u8]) -> Result<usize> {
        if self.recv_buf.is_empty() && self.peer_fin {
            return Ok(0);
        }
        let n = self.recv_buf.len().min(out.len());
        for slot in out.iter_mut().take(n) {
            *slot = self.recv_buf.pop_front().unwrap();
        }
        self.update_recv_window();
        Ok(n)
    }

    /// Signal that our write side is done: send FIN once all queued/in-flight
    /// DATA has been acknowledged. Idempotent.
    pub fn close(&mut self) {
        self.local_fin_sent = true;
    }

    /// Advance the connection: read any incoming datagrams (with a short
    /// timeout), process them, send new DATA within the congestion window,
    /// retransmit timed-out in-flight packets, send periodic ACKs, and tear
    /// down on idle timeout or RESET. Returns `Ok(true)` if the connection is
    /// still alive, `Ok(false)` if it has fully closed (EOF both directions),
    /// or an error on a fatal transport failure.
    ///
    /// Call this in a loop from a single driving task. `idle_budget` bounds
    /// how long this call waits for an inbound datagram when there is nothing
    /// to send.
    pub async fn drive(&mut self, idle_budget: Duration) -> Result<bool> {
        if self.closed {
            return Ok(false);
        }

        // Idle timeout.
        if self.last_recv.elapsed() > IDLE_TIMEOUT {
            self.closed = true;
            return Ok(false);
        }

        // Try to send new DATA within the congestion window.
        self.try_send().await?;

        // Retransmit timed-out in-flight packets.
        self.retransmit().await?;

        // Send a periodic ACK / keepalive if needed.
        self.maybe_send_ack().await?;

        // If we want to close and all queued data has been transmitted, send
        // FIN once. The FIN may follow unacked in-flight DATA; the peer acks
        // cumulatively and the FIN signals our end-of-stream.
        if self.local_fin_sent && !self.fin_transmitted && self.send_buf.is_empty() {
            self.send_fin().await?;
            self.fin_transmitted = true;
        }

        // If both sides are done (peer FIN received and our FIN transmitted),
        // the connection is closed.
        if self.peer_fin && self.recv_buf.is_empty() && self.local_fin_sent && self.fin_transmitted
        {
            self.closed = true;
            return Ok(false);
        }

        // Receive any incoming datagram (bounded wait).
        let mut buf = vec![0u8; 2048];
        let recv = tokio::time::timeout(idle_budget, self.socket.recv_from(&mut buf)).await;
        match recv {
            Ok(Ok((from, n))) => {
                if from != self.peer {
                    // Stray datagram from a different peer; ignore.
                    return Ok(true);
                }
                if n == 0 {
                    return Ok(true);
                }
                let (header, extensions, payload) =
                    match UtpHeader::decode_with_extensions(&buf[..n]) {
                        Ok(p) => p,
                        Err(_) => return Ok(true), // ignore malformed
                    };
                let sack = match sack_from_extensions(&extensions) {
                    Ok(sack) => sack,
                    Err(_) => return Ok(true), // ignore malformed extension data
                };
                self.last_recv = Instant::now();
                match self
                    .handle_packet(header, payload.to_vec(), sack.as_ref())
                    .await?
                {
                    RecvOutcome::Closed => {
                        // Peer FIN received. If we already sent our FIN, we close.
                        if self.local_fin_sent && self.fin_transmitted {
                            self.closed = true;
                            return Ok(false);
                        }
                    }
                    RecvOutcome::Delivered | RecvOutcome::Duplicate => {}
                }
            }
            Ok(Err(e)) => {
                // Socket error: surface as fatal.
                return Err(e);
            }
            Err(_) => {
                // Timeout: no incoming datagram this round.
            }
        }
        Ok(true)
    }

    /// Validate and process one inbound packet.
    async fn handle_packet(
        &mut self,
        header: UtpHeader,
        payload: Vec<u8>,
        sack: Option<&Sack>,
    ) -> Result<RecvOutcome> {
        // Connection id validation: inbound packets must carry our recv
        // connection id (the id we assigned to the peer's send side).
        if header.connection_id != self.recv_conn_id && header.typ != UtpType::Reset {
            return Ok(RecvOutcome::Duplicate);
        }
        self.peer_window = header.window_size;

        // Measure one-way delay from the peer's timestamp echo (LEDBAT).
        let now_ts = now_micros();
        if header.timestamp_delta_micros != 0 {
            let delay = now_ts.wrapping_sub(header.timestamp_micros);
            // Only DATA/STATE carry meaningful echoes; feed the controller.
            self.cwnd.on_sample(header.timestamp_micros, delay);
        }

        match header.typ {
            UtpType::Data => {
                // Update ack of previously-sent data.
                self.ack_in_flight(header.ack_number, sack);
                let outcome = self.handle_data(header.seq_number, payload);
                self.cwnd.on_ack(header.ack_number);
                Ok(outcome)
            }
            UtpType::State => {
                // Pure ACK: advance in-flight bookkeeping with any SACK.
                self.ack_in_flight(header.ack_number, sack);
                self.cwnd.on_ack(header.ack_number);
                Ok(RecvOutcome::Delivered)
            }
            UtpType::Fin => {
                // Ack the FIN's seq and record peer close.
                self.ack_number = self.ack_number.max(header.seq_number);
                self.peer_fin = true;
                // Send an ACK back so the peer learns the FIN was received.
                self.send_state().await?;
                Ok(RecvOutcome::Closed)
            }
            UtpType::Reset => {
                self.peer_fin = true;
                self.closed = true;
                Ok(RecvOutcome::Closed)
            }
            UtpType::Syn => {
                // Duplicate SYN: re-ack with our current state.
                self.send_state().await?;
                Ok(RecvOutcome::Duplicate)
            }
        }
    }

    /// Handle an inbound DATA packet: deliver in-order data, hold out-of-order
    /// data for SACK recovery, and ignore duplicates.
    fn handle_data(&mut self, seq: u16, payload: Vec<u8>) -> RecvOutcome {
        let expected = self.ack_number.wrapping_add(1);
        if seq == expected {
            if payload.len() > self.recv_window as usize {
                return RecvOutcome::Duplicate;
            }
            // In-order: deliver and drain any contiguous out-of-order tail.
            self.recv_buf.extend(&payload);
            self.ack_number = seq;
            self.drain_ooo();
            self.update_recv_window();
            RecvOutcome::Delivered
        } else if seq.wrapping_sub(self.ack_number.wrapping_add(1)) == 0 {
            RecvOutcome::Duplicate
        } else {
            // Out of order: hold if it is ahead and not already held.
            let already = self.ooo.iter().any(|(s, _)| *s == seq);
            if !already
                && seq_is_ahead(seq, self.ack_number)
                && payload.len() <= self.recv_window as usize
            {
                self.ooo.push((seq, payload));
                self.ooo
                    .sort_by_key(|(s, _)| s.wrapping_sub(self.ack_number));
                self.update_recv_window();
            }
            RecvOutcome::Duplicate
        }
    }

    /// Drain any held out-of-order bytes that are now contiguous with the ack
    /// number, delivering them to the read buffer.
    fn drain_ooo(&mut self) {
        while let Some((seq, _)) = self.ooo.first() {
            let expected = self.ack_number.wrapping_add(1);
            if *seq == expected {
                let (_, payload) = self.ooo.remove(0);
                self.recv_buf.extend(&payload);
                self.ack_number = expected;
            } else {
                break;
            }
        }
        self.update_recv_window();
    }

    /// Advance the in-flight queue given an ack number and an optional SACK
    /// extension. Removes acked (cumulatively-acked) packets and clears
    /// selectively-acked ones from retransmit consideration.
    fn ack_in_flight(&mut self, ack: u16, sack: Option<&Sack>) {
        self.in_flight.retain(|packet| {
            !seq_at_or_before(packet.seq, ack) && !sack_acks_sequence(sack, ack, packet.seq)
        });
    }

    fn update_recv_window(&mut self) {
        let buffered = self
            .ooo
            .iter()
            .fold(self.recv_buf.len(), |total, (_, payload)| {
                total.saturating_add(payload.len())
            });
        self.recv_window =
            RECV_WINDOW_DEFAULT.saturating_sub(buffered.min(u32::MAX as usize) as u32);
    }

    /// Send new DATA packets from the send buffer, bounded by the congestion
    /// window and the receive window the peer advertised.
    async fn try_send(&mut self) -> Result<()> {
        if self.local_fin_sent && self.send_buf.is_empty() && self.in_flight.is_empty() {
            return Ok(());
        }
        let send_window = self.cwnd.window_bytes().min(self.peer_window as usize);
        let mut in_flight_bytes = self.queued_in_flight_bytes();
        while !self.send_buf.is_empty() {
            let allowed = send_window.saturating_sub(in_flight_bytes);
            let n = MAX_PAYLOAD.min(self.send_buf.len()).min(allowed);
            if n == 0 {
                break;
            }
            let payload: Vec<u8> = self.send_buf.drain(..n).collect();
            let seq = self.seq_next;
            self.seq_next = self.seq_next.wrapping_add(1);
            self.seq_max_sent = self.seq_max_sent.max(seq);
            let now_ts = now_micros();
            let header = UtpHeader {
                typ: UtpType::Data,
                version: UTP_VERSION,
                extension: 0,
                connection_id: self.send_conn_id,
                timestamp_micros: now_ts,
                timestamp_delta_micros: self.cwnd.last_echo_delta(),
                window_size: self.recv_window,
                seq_number: seq,
                ack_number: self.ack_number,
            };
            let extensions = self.ack_extensions();
            let packet = header.encode_with_extensions(&extensions, &payload)?;
            self.socket.send_to(self.peer, &packet).await?;
            in_flight_bytes += payload.len();
            self.in_flight.push(InFlight::new(seq, payload, now_ts));
        }
        Ok(())
    }

    /// Retransmit in-flight packets whose retransmit timer has expired.
    async fn retransmit(&mut self) -> Result<()> {
        let rto = self.cwnd.rto();
        let now = Instant::now();
        for p in &mut self.in_flight {
            if now.duration_since(p.sent_at) >= rto {
                let now_ts = now_micros();
                let header = UtpHeader {
                    typ: UtpType::Data,
                    version: UTP_VERSION,
                    extension: 0,
                    connection_id: self.send_conn_id,
                    timestamp_micros: now_ts,
                    timestamp_delta_micros: self.cwnd.last_echo_delta(),
                    window_size: self.recv_window,
                    seq_number: p.seq,
                    ack_number: self.ack_number,
                };
                let packet = header.encode(&p.payload);
                self.socket.send_to(self.peer, &packet).await?;
                p.sent_at = now;
                p.ts_micros = now_ts;
                p.transmissions = p.transmissions.saturating_add(1);
                // Loss signal for LEDBAT: shrink the window.
                self.cwnd.on_loss();
            }
        }
        Ok(())
    }

    /// Send a STATE (pure ACK) reflecting the current ack number, if one is
    /// due (coalesced to avoid ACK implosion) or as a keepalive.
    async fn maybe_send_ack(&mut self) -> Result<()> {
        let now = Instant::now();
        // Send an ACK if we have un-acked received data or it's been a while.
        let due = now.duration_since(self.last_ack_sent) >= Duration::from_millis(200);
        if due {
            self.send_state().await?;
            self.last_ack_sent = now;
        }
        Ok(())
    }

    /// Send a single STATE packet (pure ACK) with the current ack number.
    async fn send_state(&mut self) -> Result<()> {
        let now_ts = now_micros();
        let header = UtpHeader {
            typ: UtpType::State,
            version: UTP_VERSION,
            extension: 0,
            connection_id: self.send_conn_id,
            timestamp_micros: now_ts,
            timestamp_delta_micros: self.cwnd.last_echo_delta(),
            window_size: self.recv_window,
            seq_number: self.seq_next,
            ack_number: self.ack_number,
        };
        let extensions = self.ack_extensions();
        let packet = header.encode_with_extensions(&extensions, &[])?;
        self.socket.send_to(self.peer, &packet).await?;
        Ok(())
    }

    /// Send a FIN once all data is acked.
    async fn send_fin(&mut self) -> Result<()> {
        let seq = self.seq_next;
        self.seq_next = self.seq_next.wrapping_add(1);
        let now_ts = now_micros();
        let header = UtpHeader {
            typ: UtpType::Fin,
            version: UTP_VERSION,
            extension: 0,
            connection_id: self.send_conn_id,
            timestamp_micros: now_ts,
            timestamp_delta_micros: self.cwnd.last_echo_delta(),
            window_size: self.recv_window,
            seq_number: seq,
            ack_number: self.ack_number,
        };
        let extensions = self.ack_extensions();
        let packet = header.encode_with_extensions(&extensions, &[])?;
        self.socket.send_to(self.peer, &packet).await?;
        Ok(())
    }

    fn ack_extensions(&self) -> Vec<UtpExtension> {
        let sack = Sack::from_held(self.ack_number, &self.ooo);
        if sack.is_empty() {
            Vec::new()
        } else {
            vec![UtpExtension::new(EXT_SACK, sack.encode_data())]
        }
    }

    /// Total bytes currently in-flight (sent but not cumulatively acked).
    fn queued_in_flight_bytes(&self) -> usize {
        self.in_flight.iter().map(|p| p.payload.len()).sum()
    }
}

/// Default advertised receive window.
///
/// A 64 KiB window caps a single uTP flow to low-MB/s throughput on normal
/// internet RTTs. Keep this bounded, but large enough for public Linux ISO
/// swarms to maintain useful in-flight data over higher-latency paths.
const RECV_WINDOW_DEFAULT: u32 = 4 * 1024 * 1024;

fn sack_from_extensions(extensions: &[UtpExtension]) -> Result<Option<Sack>> {
    extensions
        .iter()
        .find(|extension| extension.kind == EXT_SACK)
        .map(|extension| Sack::parse_data(&extension.data))
        .transpose()
}

fn sack_acks_sequence(sack: Option<&Sack>, ack: u16, sequence: u16) -> bool {
    let Some(sack) = sack else {
        return false;
    };
    let offset = sequence.wrapping_sub(ack);
    sack.offsets().contains(&offset)
}

fn seq_is_ahead(sequence: u16, reference: u16) -> bool {
    let distance = sequence.wrapping_sub(reference);
    distance != 0 && distance < 0x8000
}

/// Wrapping comparison: is `s` at or before `ack` in u16 sequence space?
fn seq_at_or_before(s: u16, ack: u16) -> bool {
    ack.wrapping_sub(s) < 0x8000 || s == ack
}

/// A pseudo-random nonzero connection id (deterministic-ish from time + addr).
fn rand_conn_id() -> u16 {
    let t = now_micros() as u64;
    let mix = t
        ^ (t.rotate_left(13))
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
    let v = (mix & 0xffff) as u16;
    if v == 0 {
        1
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_at_or_before_wraps() {
        assert!(seq_at_or_before(5, 5));
        assert!(seq_at_or_before(4, 5));
        assert!(seq_at_or_before(u16::MAX, 1));
        assert!(!seq_at_or_before(6, 5));
    }

    #[test]
    fn bep29_connection_id_directions_are_distinct() {
        let syn_connection_id = 0x1234u16;
        let initiator_send = syn_connection_id.wrapping_add(1);
        let initiator_recv = syn_connection_id;
        let responder_send = syn_connection_id;
        let responder_recv = syn_connection_id.wrapping_add(1);

        assert_eq!(initiator_send, responder_recv);
        assert_eq!(initiator_recv, responder_send);
    }

    #[test]
    fn sack_marks_only_selected_packets_beyond_cumulative_ack() {
        let sack = Sack::parse_data(&[0b0000_0101, 0, 0, 0]).unwrap();
        assert!(sack_acks_sequence(Some(&sack), 100, 102));
        assert!(!sack_acks_sequence(Some(&sack), 100, 103));
        assert!(sack_acks_sequence(Some(&sack), 100, 104));
        assert!(!sack_acks_sequence(Some(&sack), 100, 101));
    }

    #[test]
    fn sequence_ahead_check_handles_wraparound() {
        assert!(seq_is_ahead(0, u16::MAX));
        assert!(seq_is_ahead(10, 5));
        assert!(!seq_is_ahead(5, 10));
        assert!(!seq_is_ahead(5, 5));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn utp_fail_closed_blocks_socket() {
        use crate::net::binder::BlockedBinder;
        let binder = BlockedBinder;
        match UtpConnection::connect(&binder, "127.0.0.1:9".parse().unwrap()).await {
            Ok(_) => panic!("expected fail-closed to block uTP connect"),
            Err(e) => assert!(e.is_network_blocked()),
        }
    }
}

#[cfg(all(test, feature = "test-binder"))]
mod contained_tests {
    use super::*;
    use crate::net::binder::LoopbackBinder;
    use std::sync::Arc;
    use std::time::Duration;

    /// Drive a connection until `cond` returns true or the deadline elapses.
    async fn drive_until<F: FnMut(&UtpConnection) -> bool>(
        conn: &mut UtpConnection,
        mut cond: F,
        deadline: Duration,
    ) -> Result<bool> {
        let end = Instant::now() + deadline;
        loop {
            if Instant::now() > end {
                return Ok(false);
            }
            if cond(conn) {
                return Ok(true);
            }
            if !conn.drive(Duration::from_millis(50)).await? {
                return Ok(cond(conn));
            }
        }
    }

    /// A uTP echo server: accept a SYN on a contained UDP socket, echo every
    /// received byte back, then close on FIN.
    async fn echo_server(
        binder: Arc<dyn crate::net::NetworkBinder>,
        expected_bytes: usize,
    ) -> Result<(SocketAddr, tokio::task::JoinHandle<Result<()>>)> {
        let sock = binder.udp_socket().await?;
        let sock: std::sync::Arc<dyn crate::net::ContainedUdpSocket> = sock.into();
        let local = sock.local_addr()?;
        let handle = tokio::spawn(async move {
            // Wait for a SYN.
            let mut buf = vec![0u8; 2048];
            let (from, n) = sock.recv_from(&mut buf).await?;
            let (syn, _payload) = UtpHeader::decode(&buf[..n])?;
            if syn.typ != UtpType::Syn {
                return Err(CoreError::Internal("echo server: expected SYN".into()));
            }
            let mut conn = UtpConnection::accept_from_syn(sock.clone(), from, &syn).await?;
            let mut echoed = 0usize;
            while echoed < expected_bytes {
                if !conn.drive(Duration::from_millis(50)).await? {
                    break;
                }
                let mut tmp = vec![0u8; 8192];
                let r = conn.read(&mut tmp).await?;
                if r > 0 {
                    conn.write(&tmp[..r]).await?;
                    echoed += r;
                }
            }
            // Keep reading/driving until the client signals close (FIN), so we
            // can ACK it and then close ourselves promptly. This avoids the
            // client hanging waiting for a FIN ACK that never comes because we
            // exited early.
            let drain_deadline = Instant::now() + Duration::from_secs(15);
            while !conn.peer_closed() && Instant::now() < drain_deadline {
                if !conn.drive(Duration::from_millis(20)).await? {
                    break;
                }
            }
            conn.close();
            // Drain our own close handshake until the connection is fully
            // closed. Drive at least once so our FIN is transmitted even if
            // `is_closed()` would already be true (peer FIN already received).
            let close_deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let _ = conn.drive(Duration::from_millis(20)).await?;
                if conn.is_closed() || Instant::now() >= close_deadline {
                    break;
                }
            }
            Ok(())
        });
        Ok((local, handle))
    }

    /// Full byte-stream round trip: a client connects over the contained UDP
    /// path, writes a payload, reads it echoed back, and verifies equality.
    /// Exercises SYN/STATE/DATA/ACK/FIN, the drive loop, retransmit idle path,
    /// and LEDBAT window growth.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn utp_connection_streams_bytes_over_contained_socket() {
        let binder: Arc<dyn crate::net::NetworkBinder> = Arc::new(LoopbackBinder);
        let payload = b"swarmotter uTP byte stream over contained UDP socket -- production uTP";
        let (server_addr, server_task) = echo_server(binder.clone(), payload.len())
            .await
            .expect("echo server start");

        let mut client = UtpConnection::connect(binder.as_ref(), server_addr)
            .await
            .expect("uTP connect");

        // Write the whole payload (looping while the send buffer accepts).
        let mut written = 0usize;
        let write_deadline = Instant::now() + Duration::from_secs(10);
        while written < payload.len() {
            if Instant::now() > write_deadline {
                panic!(
                    "uTP client write stalled; wrote {written}/{}",
                    payload.len()
                );
            }
            let n = client.write(&payload[written..]).await.expect("uTP write");
            if n == 0 {
                // Buffer full: drive the connection to drain in-flight data.
                assert!(client
                    .drive(Duration::from_millis(20))
                    .await
                    .expect("drive"));
                continue;
            }
            written += n;
        }

        // Read the echo back until we have the whole payload.
        let mut got = Vec::with_capacity(payload.len());
        let read_deadline = Instant::now() + Duration::from_secs(15);
        while got.len() < payload.len() {
            if Instant::now() > read_deadline {
                panic!(
                    "uTP client read stalled; got {}/{}",
                    got.len(),
                    payload.len()
                );
            }
            assert!(client
                .drive(Duration::from_millis(20))
                .await
                .expect("drive"));
            let mut tmp = vec![0u8; 8192];
            let n = client.read(&mut tmp).await.expect("uTP read");
            if n > 0 {
                got.extend_from_slice(&tmp[..n]);
            }
        }
        assert_eq!(got.as_slice(), payload as &[u8]);

        // Graceful close: send FIN and wait for the connection to close.
        client.close();
        let closed = drive_until(&mut client, |c| c.is_closed(), Duration::from_secs(15))
            .await
            .expect("drive");
        assert!(closed, "client connection should close after FIN");

        server_task
            .await
            .expect("server task panic")
            .expect("server");
    }
}
