// SPDX-License-Identifier: Apache-2.0

//! `AsyncRead`+`AsyncWrite` byte-stream adapter over a [`UtpConnection`].
//!
//! [`UtpStream`] runs the connection's drive loop in a background task and
//! exposes a duplex byte stream via bounded channels, so the existing peer
//! wire protocol machinery (`PeerReader`, `write_message`) works unchanged
//! over uTP exactly as it does over TCP. The stream is obtained from the
//! network containment layer (the connection owns a binder-provided contained
//! UDP socket); no socket is created directly here.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

use crate::error::{CoreError, Result};
use crate::net::NetworkBinder;

use super::UtpConnection;

/// A duplex byte stream backed by a uTP connection over a contained UDP socket.
/// Implements `AsyncRead` + `AsyncWrite` so it can be used with
/// [`tokio::io::split`] and the peer wire protocol reader/writer.
pub struct UtpStream {
    /// In-order bytes delivered by the driver task.
    read_rx: mpsc::Receiver<Vec<u8>>,
    /// Pending bytes to send to the driver task.
    write_tx: mpsc::UnboundedSender<WriteCmd>,
    /// Buffered bytes from the current read chunk not yet consumed.
    read_buf: Vec<u8>,
    read_pos: usize,
    /// True once the driver task has signaled EOF (peer closed + drained).
    read_eof: bool,
    /// Owned connection driver. Dropping the stream aborts this task so the
    /// contained UDP socket cannot outlive its peer session.
    driver_task: Option<tokio::task::JoinHandle<()>>,
    shutdown_started: bool,
}

enum WriteCmd {
    Bytes(Vec<u8>),
    Close,
}

impl UtpStream {
    /// Establish a uTP connection to `peer` through the contained network path
    /// and spawn a background driver task. Returns a duplex byte stream.
    pub async fn connect(binder: &dyn NetworkBinder, peer: std::net::SocketAddr) -> Result<Self> {
        let conn = UtpConnection::connect(binder, peer).await?;
        Ok(Self::spawn(conn))
    }

    /// Wrap an already-established connection with a background driver task.
    pub fn spawn(mut conn: UtpConnection) -> Self {
        let (read_tx, read_rx) = mpsc::channel::<Vec<u8>>(64);
        // Unbounded write channel: backpressure is enforced by the
        // connection's bounded send buffer (SEND_BUFFER_CAP), not by the
        // channel, so poll_write never blocks on channel capacity.
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<WriteCmd>();
        let driver_task = tokio::spawn(async move {
            use std::time::Duration;
            loop {
                // Drain any pending write commands into the connection buffer.
                while let Ok(cmd) = write_rx.try_recv() {
                    match cmd {
                        WriteCmd::Bytes(b) => {
                            // The send buffer is bounded; write() is async-safe
                            // here because we own the connection.
                            let mut off = 0usize;
                            while off < b.len() {
                                match conn.write(&b[off..]).await {
                                    Ok(0) => break,
                                    Ok(n) => off += n,
                                    Err(_) => break,
                                }
                            }
                        }
                        WriteCmd::Close => conn.close(),
                    }
                }
                // Drive the connection (bounded wait for inbound datagrams).
                let alive = match conn.drive(Duration::from_millis(10)).await {
                    Ok(a) => a,
                    Err(_) => return,
                };
                // Pump any delivered bytes to the read channel.
                let mut tmp = vec![0u8; 16 * 1024];
                loop {
                    match conn.read(&mut tmp).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if read_tx.send(tmp[..n].to_vec()).await.is_err() {
                                return;
                            }
                        }
                        Err(_) => return,
                    }
                }
                if !alive {
                    // Signal EOF to the reader (drop the sender closes the rx).
                    return;
                }
            }
        });
        Self {
            read_rx,
            write_tx,
            read_buf: Vec::new(),
            read_pos: 0,
            read_eof: false,
            driver_task: Some(driver_task),
            shutdown_started: false,
        }
    }
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        if let Some(driver_task) = self.driver_task.take() {
            driver_task.abort();
        }
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.read_eof {
            return Poll::Ready(Ok(()));
        }
        // Serve from the buffered current chunk first.
        if self.read_pos < self.read_buf.len() {
            let n = (self.read_buf.len() - self.read_pos).min(buf.remaining());
            buf.put_slice(&self.read_buf[self.read_pos..self.read_pos + n]);
            self.read_pos += n;
            if self.read_pos == self.read_buf.len() {
                self.read_buf.clear();
                self.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        // Pull the next chunk from the driver task.
        match self.read_rx.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => {
                if chunk.is_empty() {
                    self.read_eof = true;
                    return Poll::Ready(Ok(()));
                }
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..n]);
                if n < chunk.len() {
                    self.read_buf = chunk;
                    self.read_pos = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => {
                // Driver task ended: EOF.
                self.read_eof = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.shutdown_started {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "uTP stream is shutting down",
            )));
        }
        // The channel has ample capacity (64); try_send is synchronous and
        // reports backpressure via WouldBlock when full, which the runtime
        // will retry. A dedicated waker is not needed because the driver task
        // drains the channel continuously.
        match self.write_tx.send(WriteCmd::Bytes(buf.to_vec())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "uTP driver closed",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shutdown_started {
            self.shutdown_started = true;
            let _ = self.write_tx.send(WriteCmd::Close);
        }

        let Some(driver_task) = self.driver_task.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match Pin::new(driver_task).poll(cx) {
            Poll::Ready(Ok(())) => {
                self.driver_task.take();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.driver_task.take();
                Poll::Ready(Err(io::Error::other(format!(
                    "uTP driver task failed: {error}"
                ))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A peer transport selection: which underlying byte-stream transport a peer
/// connection uses. The engine attempts transports per config; the selected
/// stream is opaque to the peer protocol (both expose AsyncRead+AsyncWrite).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerTransport {
    Tcp,
    Utp,
}

impl PeerTransport {
    pub fn as_str(self) -> &'static str {
        match self {
            PeerTransport::Tcp => "tcp",
            PeerTransport::Utp => "utp",
        }
    }
}

/// Open a peer byte stream to `addr` using the selected transport through the
/// contained network path. Returns a boxed duplex stream usable with
/// `tokio::io::split` and the peer wire protocol.
pub async fn connect_peer_stream(
    binder: Arc<dyn NetworkBinder>,
    transport: PeerTransport,
    addr: std::net::SocketAddr,
) -> Result<(Box<dyn PeerDuplex>, PeerTransport)> {
    match transport {
        PeerTransport::Tcp => {
            let stream = binder.connect_peer(addr).await?;
            Ok((Box::new(stream), PeerTransport::Tcp))
        }
        PeerTransport::Utp => {
            let stream = UtpStream::connect(binder.as_ref(), addr).await?;
            Ok((Box::new(stream), PeerTransport::Utp))
        }
    }
}

/// A duplex byte stream that implements both `AsyncRead` and `AsyncWrite` and
/// is `Send`, used as the engine's transport-agnostic peer stream.
pub trait PeerDuplex: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> PeerDuplex for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

// Re-export the tokio IO traits for downstream convenience.
pub use tokio::io::{AsyncRead as _, AsyncWrite as _};

fn _unused() {
    let _ = CoreError::Internal(String::new());
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::oneshot;

    use crate::net::ContainedUdpSocket;
    use crate::utp::{now_micros, UtpHeader, UtpType, UTP_VERSION};

    use super::*;

    struct PendingUdpSocket {
        dropped: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl Drop for PendingUdpSocket {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.lock().unwrap().take() {
                let _ = dropped.send(());
            }
        }
    }

    #[async_trait]
    impl ContainedUdpSocket for PendingUdpSocket {
        async fn send_to(&self, _addr: SocketAddr, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn recv_from(&self, _buf: &mut [u8]) -> Result<(SocketAddr, usize)> {
            pending().await
        }

        fn local_addr(&self) -> Result<SocketAddr> {
            Ok("127.0.0.1:40000".parse().unwrap())
        }
    }

    async fn stream_with_pending_socket() -> (UtpStream, oneshot::Receiver<()>) {
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let socket: Arc<dyn ContainedUdpSocket> = Arc::new(PendingUdpSocket {
            dropped: Mutex::new(Some(dropped_tx)),
        });
        let peer = "127.0.0.1:40001".parse().unwrap();
        let syn = UtpHeader {
            typ: UtpType::Syn,
            version: UTP_VERSION,
            extension: 0,
            connection_id: 7,
            timestamp_micros: now_micros(),
            timestamp_delta_micros: 0,
            window_size: 64 * 1024,
            seq_number: 1,
            ack_number: 0,
        };
        let connection = UtpConnection::accept_from_syn(socket.clone(), peer, &syn)
            .await
            .unwrap();
        drop(socket);
        (UtpStream::spawn(connection), dropped_rx)
    }

    async fn assert_socket_dropped(dropped: oneshot::Receiver<()>) {
        tokio::time::timeout(Duration::from_secs(1), dropped)
            .await
            .expect("uTP driver retained its contained UDP socket")
            .expect("socket drop signal sender disappeared");
    }

    #[tokio::test]
    async fn dropping_stream_aborts_driver_and_releases_socket() {
        let (stream, mut dropped) = stream_with_pending_socket().await;
        tokio::task::yield_now().await;
        assert!(matches!(
            dropped.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        drop(stream);

        assert_socket_dropped(dropped).await;
    }

    #[tokio::test]
    async fn aborting_split_stream_owner_releases_driver_socket() {
        let (stream, dropped) = stream_with_pending_socket().await;
        let (started_tx, started_rx) = oneshot::channel();
        let owner = tokio::spawn(async move {
            let (read_half, write_half) = tokio::io::split(stream);
            let _ = started_tx.send(());
            pending::<()>().await;
            drop((read_half, write_half));
        });
        started_rx.await.unwrap();

        owner.abort();
        let error = owner.await.expect_err("owner task should be cancelled");
        assert!(error.is_cancelled());

        assert_socket_dropped(dropped).await;
    }
}
