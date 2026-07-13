// SPDX-License-Identifier: Apache-2.0

//! Message Stream Encryption / Protocol Encryption for contained peer streams.
//!
//! MSE/PE is the de facto BitTorrent peer-wire obfuscation layer used by many
//! clients. It is intentionally scoped to wrapping an already-open peer stream;
//! callers are still responsible for creating sockets through `NetworkBinder`.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::error::{CoreError, Result};
use crate::hash::InfoHash;

const DH_BYTES: usize = 96;
const MAX_PAD_BYTES: usize = 512;
const VC_LEN: usize = 8;
const HASH_LEN: usize = 20;
const CRYPTO_PLAINTEXT: u32 = 0x0000_0001;
const CRYPTO_RC4: u32 = 0x0000_0002;
const RC4_DROP_BYTES: usize = 1024;

const DH_GENERATOR: Big768 = Big768::from_u64(2);
const DH_PRIME: Big768 = Big768([
    0xffff_ffff_ffff_ffff,
    0xf44c_42e9_a63a_3620,
    0xe485_b576_625e_7ec6,
    0x4fe1_356d_6d51_c245,
    0x302b_0a6d_f25f_1437,
    0xef95_19b3_cd3a_431b,
    0x514a_0879_8e34_04dd,
    0x020b_bea6_3b13_9b22,
    0x2902_4e08_8a67_cc74,
    0xc4c6_628b_80dc_1cd1,
    0xc90f_daa2_2168_c234,
    0xffff_ffff_ffff_ffff,
]);

/// A peer byte stream after successful MSE/PE negotiation.
pub struct MseStream<S> {
    stream: S,
    incoming: Rc4Cipher,
    outgoing: Rc4Cipher,
    read_prefix: VecDeque<u8>,
    pending_write: Vec<u8>,
    pending_offset: usize,
}

impl<S> MseStream<S> {
    fn new(stream: S, incoming: Rc4Cipher, outgoing: Rc4Cipher, read_prefix: Vec<u8>) -> Self {
        Self {
            stream,
            incoming,
            outgoing,
            read_prefix: VecDeque::from(read_prefix),
            pending_write: Vec::new(),
            pending_offset: 0,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for MseStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while buf.remaining() > 0 {
            match this.read_prefix.pop_front() {
                Some(byte) => buf.put_slice(&[byte]),
                None => break,
            }
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let before = buf.filled().len();
        let poll = Pin::new(&mut this.stream).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let after = buf.filled().len();
            if after > before {
                this.incoming.apply(&mut buf.filled_mut()[before..after]);
            }
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for MseStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.poll_flush_pending(cx))?;
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut encrypted = buf.to_vec();
        this.outgoing.apply(&mut encrypted);
        this.pending_write = encrypted;
        this.pending_offset = 0;
        match this.poll_flush_pending(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) | Poll::Pending => Poll::Ready(Ok(buf.len())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_flush_pending(cx))?;
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_flush_pending(cx))?;
        Pin::new(&mut this.stream).poll_shutdown(cx)
    }
}

impl<S: AsyncWrite + Unpin> MseStream<S> {
    fn poll_flush_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.pending_offset < self.pending_write.len() {
            let n = ready!(Pin::new(&mut self.stream)
                .poll_write(cx, &self.pending_write[self.pending_offset..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "encrypted stream write returned zero bytes",
                )));
            }
            self.pending_offset += n;
        }
        self.pending_write.clear();
        self.pending_offset = 0;
        Poll::Ready(Ok(()))
    }
}

/// Initiate MSE/PE on an outbound peer byte stream.
pub async fn connect<S>(mut stream: S, info_hash: InfoHash) -> Result<MseStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let exponent = random_exponent();
    let public = DH_GENERATOR.mod_pow(&exponent);
    stream.write_all(&public.to_be_bytes()).await?;

    let mut peer_public = [0u8; DH_BYTES];
    stream.read_exact(&mut peer_public).await?;
    let secret = dh_secret(peer_public, &exponent)?;
    let secret_bytes = secret.to_be_bytes();

    let mut incoming = Rc4Cipher::new(&derive_key(b"keyB", &secret_bytes, info_hash.as_bytes()));
    let mut outgoing = Rc4Cipher::new(&derive_key(b"keyA", &secret_bytes, info_hash.as_bytes()));

    let req1 = sha1_concat(&[b"req1", &secret_bytes]);
    let req2 = xor20(
        sha1_concat(&[b"req2", info_hash.as_bytes()]),
        sha1_concat(&[b"req3", &secret_bytes]),
    );
    stream.write_all(&req1).await?;
    stream.write_all(&req2).await?;

    let mut offer = Vec::with_capacity(VC_LEN + 4 + 2 + 2);
    offer.extend_from_slice(&[0u8; VC_LEN]);
    offer.extend_from_slice(&CRYPTO_RC4.to_be_bytes());
    offer.extend_from_slice(&0u16.to_be_bytes()); // PadC length.
    offer.extend_from_slice(&0u16.to_be_bytes()); // IA length.
    outgoing.apply(&mut offer);
    stream.write_all(&offer).await?;
    stream.flush().await.ok();

    let selected = read_responder_selection(&mut stream, &mut incoming).await?;
    if selected & CRYPTO_RC4 == 0 {
        return Err(CoreError::Internal(
            "MSE peer did not select RC4 encryption".into(),
        ));
    }

    Ok(MseStream::new(stream, incoming, outgoing, Vec::new()))
}

/// Accept MSE/PE on an inbound peer byte stream for a known torrent info hash.
pub async fn accept<S>(stream: S, info_hash: InfoHash) -> Result<MseStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (_, stream) = accept_matching(stream, &[info_hash]).await?;
    Ok(stream)
}

/// Accept MSE/PE on an inbound stream and identify which registered torrent
/// the initiator selected. This is used by a process-wide peer listener: the
/// MSE stream key is the first point at which an encrypted inbound connection
/// identifies its torrent.
pub async fn accept_any<S>(stream: S, info_hashes: &[InfoHash]) -> Result<(InfoHash, MseStream<S>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if info_hashes.is_empty() {
        return Err(CoreError::Internal(
            "cannot accept MSE without a registered torrent".into(),
        ));
    }
    accept_matching(stream, info_hashes).await
}

async fn accept_matching<S>(
    mut stream: S,
    info_hashes: &[InfoHash],
) -> Result<(InfoHash, MseStream<S>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut peer_public = [0u8; DH_BYTES];
    stream.read_exact(&mut peer_public).await?;

    let exponent = random_exponent();
    let public = DH_GENERATOR.mod_pow(&exponent);
    stream.write_all(&public.to_be_bytes()).await?;

    let secret = dh_secret(peer_public, &exponent)?;
    let secret_bytes = secret.to_be_bytes();
    let req1 = sha1_concat(&[b"req1", &secret_bytes]);
    read_until_marker(&mut stream, &req1, MAX_PAD_BYTES).await?;

    let mut req2 = [0u8; HASH_LEN];
    stream.read_exact(&mut req2).await?;
    let req3 = sha1_concat(&[b"req3", &secret_bytes]);
    let info_hash = info_hashes
        .iter()
        .copied()
        .find(|candidate| req2 == xor20(sha1_concat(&[b"req2", candidate.as_bytes()]), req3))
        .ok_or_else(|| {
            CoreError::Internal("MSE stream key did not match a registered torrent".into())
        })?;

    let mut incoming = Rc4Cipher::new(&derive_key(b"keyA", &secret_bytes, info_hash.as_bytes()));
    let mut outgoing = Rc4Cipher::new(&derive_key(b"keyB", &secret_bytes, info_hash.as_bytes()));
    let initiator = read_initiator_offer(&mut stream, &mut incoming).await?;
    if initiator.crypto_provide & CRYPTO_RC4 == 0 {
        return Err(CoreError::Internal(
            "MSE initiator did not offer RC4 encryption".into(),
        ));
    }

    let mut response = Vec::with_capacity(VC_LEN + 4 + 2);
    response.extend_from_slice(&[0u8; VC_LEN]);
    response.extend_from_slice(&CRYPTO_RC4.to_be_bytes());
    response.extend_from_slice(&0u16.to_be_bytes());
    outgoing.apply(&mut response);
    stream.write_all(&response).await?;
    stream.flush().await.ok();

    Ok((
        info_hash,
        MseStream::new(stream, incoming, outgoing, initiator.initial_data),
    ))
}

#[derive(Debug)]
struct InitiatorOffer {
    crypto_provide: u32,
    initial_data: Vec<u8>,
}

async fn read_initiator_offer<S: AsyncRead + Unpin>(
    stream: &mut S,
    cipher: &mut Rc4Cipher,
) -> Result<InitiatorOffer> {
    let mut header = [0u8; VC_LEN + 4 + 2];
    read_decrypted_exact(stream, cipher, &mut header).await?;
    if header[..VC_LEN] != [0u8; VC_LEN] {
        return Err(CoreError::Internal(
            "MSE verification constant mismatch".into(),
        ));
    }
    let crypto_provide = u32::from_be_bytes(header[VC_LEN..VC_LEN + 4].try_into().unwrap());
    let pad_len = u16::from_be_bytes(header[VC_LEN + 4..VC_LEN + 6].try_into().unwrap()) as usize;
    if pad_len > MAX_PAD_BYTES {
        return Err(CoreError::Internal("MSE PadC length exceeded limit".into()));
    }
    if pad_len > 0 {
        let mut pad = vec![0u8; pad_len];
        read_decrypted_exact(stream, cipher, &mut pad).await?;
    }

    let mut ia_len = [0u8; 2];
    read_decrypted_exact(stream, cipher, &mut ia_len).await?;
    let ia_len = u16::from_be_bytes(ia_len) as usize;
    let mut initial_data = vec![0u8; ia_len];
    if ia_len > 0 {
        read_decrypted_exact(stream, cipher, &mut initial_data).await?;
    }

    Ok(InitiatorOffer {
        crypto_provide,
        initial_data,
    })
}

async fn read_responder_selection<S: AsyncRead + Unpin>(
    stream: &mut S,
    cipher: &mut Rc4Cipher,
) -> Result<u32> {
    let mut raw = Vec::with_capacity(MAX_PAD_BYTES + VC_LEN + 4 + 2);
    let mut byte = [0u8; 1];
    for _ in 0..(MAX_PAD_BYTES + VC_LEN + 4 + 2) {
        stream.read_exact(&mut byte).await?;
        raw.push(byte[0]);
        if raw.len() < VC_LEN + 4 + 2 {
            continue;
        }
        let max_pad = raw.len().saturating_sub(VC_LEN + 4 + 2).min(MAX_PAD_BYTES);
        for pad_len in 0..=max_pad {
            let end = pad_len + VC_LEN + 4 + 2;
            let mut probe_cipher = cipher.clone();
            let mut decrypted = raw[pad_len..end].to_vec();
            probe_cipher.apply(&mut decrypted);
            if decrypted[..VC_LEN] == [0u8; VC_LEN] {
                let crypto_select =
                    u32::from_be_bytes(decrypted[VC_LEN..VC_LEN + 4].try_into().unwrap());
                let pad_d_len =
                    u16::from_be_bytes(decrypted[VC_LEN + 4..VC_LEN + 6].try_into().unwrap())
                        as usize;
                if pad_d_len > MAX_PAD_BYTES || crypto_select & (CRYPTO_RC4 | CRYPTO_PLAINTEXT) == 0
                {
                    continue;
                }
                let mut consumed = raw[pad_len..end].to_vec();
                cipher.apply(&mut consumed);
                if pad_d_len > 0 {
                    let mut pad = vec![0u8; pad_d_len];
                    read_decrypted_exact(stream, cipher, &mut pad).await?;
                }
                return Ok(crypto_select);
            }
        }
    }
    Err(CoreError::Internal(
        "MSE responder verification constant not found".into(),
    ))
}

async fn read_until_marker<S: AsyncRead + Unpin>(
    stream: &mut S,
    marker: &[u8; HASH_LEN],
    max_pad: usize,
) -> Result<()> {
    let mut raw = Vec::with_capacity(max_pad + marker.len());
    let mut byte = [0u8; 1];
    for _ in 0..(max_pad + marker.len()) {
        stream.read_exact(&mut byte).await?;
        raw.push(byte[0]);
        if raw.ends_with(marker) {
            return Ok(());
        }
    }
    Err(CoreError::Internal("MSE req1 marker not found".into()))
}

async fn read_decrypted_exact<S: AsyncRead + Unpin>(
    stream: &mut S,
    cipher: &mut Rc4Cipher,
    buf: &mut [u8],
) -> Result<()> {
    stream.read_exact(buf).await?;
    cipher.apply(buf);
    Ok(())
}

fn dh_secret(peer_public: [u8; DH_BYTES], exponent: &[u8; 20]) -> Result<Big768> {
    let peer = Big768::from_be_bytes(peer_public);
    if peer <= Big768::one() || peer >= DH_PRIME {
        return Err(CoreError::Internal(
            "MSE peer public key out of range".into(),
        ));
    }
    Ok(peer.mod_pow(exponent))
}

fn random_exponent() -> [u8; 20] {
    let mut out = [0u8; 20];
    fill_random(&mut out);
    out
}

fn fill_random(buf: &mut [u8]) {
    #[cfg(unix)]
    {
        use std::io::Read;
        if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
            if f.read_exact(buf).is_ok() {
                return;
            }
        }
    }

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut offset = 0;
    let mut counter = 0u64;
    while offset < buf.len() {
        let digest = sha1_concat(&[
            b"swarmotter-mse-fallback-random",
            &nanos.to_be_bytes(),
            &counter.to_be_bytes(),
        ]);
        let take = (buf.len() - offset).min(digest.len());
        buf[offset..offset + take].copy_from_slice(&digest[..take]);
        offset += take;
        counter = counter.wrapping_add(1);
    }
}

fn derive_key(label: &[u8], secret: &[u8; DH_BYTES], skey: &[u8; 20]) -> [u8; HASH_LEN] {
    sha1_concat(&[label, secret, skey])
}

fn sha1_concat(parts: &[&[u8]]) -> [u8; HASH_LEN] {
    let mut hasher = Sha1::new();
    for part in parts {
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut out = [0u8; HASH_LEN];
    out.copy_from_slice(&digest);
    out
}

fn xor20(a: [u8; HASH_LEN], b: [u8; HASH_LEN]) -> [u8; HASH_LEN] {
    let mut out = [0u8; HASH_LEN];
    for i in 0..HASH_LEN {
        out[i] = a[i] ^ b[i];
    }
    out
}

#[derive(Clone)]
struct Rc4Cipher {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4Cipher {
    fn new(key: &[u8]) -> Self {
        let mut s = [0u8; 256];
        for (i, slot) in s.iter_mut().enumerate() {
            *slot = i as u8;
        }
        let mut j = 0u8;
        for i in 0..256 {
            j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
            s.swap(i, j as usize);
        }
        let mut cipher = Self { s, i: 0, j: 0 };
        for _ in 0..RC4_DROP_BYTES {
            let _ = cipher.next_byte();
        }
        cipher
    }

    fn apply(&mut self, buf: &mut [u8]) {
        for b in buf {
            *b ^= self.next_byte();
        }
    }

    fn next_byte(&mut self) -> u8 {
        self.i = self.i.wrapping_add(1);
        self.j = self.j.wrapping_add(self.s[self.i as usize]);
        self.s.swap(self.i as usize, self.j as usize);
        let idx = self.s[self.i as usize].wrapping_add(self.s[self.j as usize]);
        self.s[idx as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Big768([u64; 12]);

impl Big768 {
    const fn from_u64(n: u64) -> Self {
        Self([n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    }

    const fn one() -> Self {
        Self::from_u64(1)
    }

    fn from_be_bytes(bytes: [u8; DH_BYTES]) -> Self {
        let mut limbs = [0u64; 12];
        for (idx, chunk) in bytes.chunks_exact(8).rev().enumerate() {
            limbs[idx] = u64::from_be_bytes(chunk.try_into().unwrap());
        }
        Self(limbs)
    }

    fn to_be_bytes(self) -> [u8; DH_BYTES] {
        let mut out = [0u8; DH_BYTES];
        for (idx, limb) in self.0.iter().rev().enumerate() {
            out[idx * 8..idx * 8 + 8].copy_from_slice(&limb.to_be_bytes());
        }
        out
    }

    fn mod_pow(self, exponent: &[u8; 20]) -> Self {
        let mut result = Self::one();
        let base = self;
        for byte in exponent {
            for bit in (0..8).rev() {
                result = result.mul_mod(result);
                if (byte >> bit) & 1 == 1 {
                    result = result.mul_mod(base);
                }
            }
        }
        result
    }

    fn mul_mod(self, rhs: Self) -> Self {
        let mut result = Self::from_u64(0);
        let mut addend = self;
        for limb in rhs.0 {
            for bit in 0..64 {
                if (limb >> bit) & 1 == 1 {
                    result = result.add_mod(addend);
                }
                addend = addend.double_mod();
            }
        }
        result
    }

    fn double_mod(self) -> Self {
        self.add_mod(self)
    }

    fn add_mod(self, rhs: Self) -> Self {
        let mut out = [0u64; 12];
        let mut carry = false;
        for (i, item) in out.iter_mut().enumerate() {
            let (sum1, c1) = self.0[i].overflowing_add(rhs.0[i]);
            let (sum2, c2) = sum1.overflowing_add(carry as u64);
            *item = sum2;
            carry = c1 || c2;
        }
        let mut result = Self(out);
        if carry || result >= DH_PRIME {
            result = result.wrapping_sub(DH_PRIME);
        }
        result
    }

    fn wrapping_sub(self, rhs: Self) -> Self {
        let mut out = [0u64; 12];
        let mut borrow = false;
        for (i, item) in out.iter_mut().enumerate() {
            let (sub1, b1) = self.0[i].overflowing_sub(rhs.0[i]);
            let (sub2, b2) = sub1.overflowing_sub(borrow as u64);
            *item = sub2;
            borrow = b1 || b2;
        }
        Self(out)
    }
}

impl PartialOrd for Big768 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Big768 {
    fn cmp(&self, other: &Self) -> Ordering {
        for i in (0..12).rev() {
            match self.0[i].cmp(&other.0[i]) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn dh_prime_matches_768_bit_modp_group() {
        assert_eq!(
            hex::encode(DH_PRIME.to_be_bytes()),
            concat!(
                "ffffffffffffffffc90fdaa22168c234c4c6628b80dc1cd1",
                "29024e088a67cc74020bbea63b139b22514a08798e3404dd",
                "ef9519b3cd3a431b302b0a6df25f14374fe1356d6d51c245",
                "e485b576625e7ec6f44c42e9a63a3620ffffffffffffffff"
            )
        );
    }

    #[test]
    fn dh_shared_secret_matches() {
        let a = [0x11u8; 20];
        let b = [0x37u8; 20];
        let ya = DH_GENERATOR.mod_pow(&a);
        let yb = DH_GENERATOR.mod_pow(&b);
        let sa = yb.mod_pow(&a);
        let sb = ya.mod_pow(&b);
        assert_eq!(sa, sb);
        assert!(sa > Big768::one());
    }

    #[test]
    fn rc4_round_trip_after_drop() {
        let key = b"test-key";
        let mut a = Rc4Cipher::new(key);
        let mut b = Rc4Cipher::new(key);
        let mut payload = b"hello swarmotter".to_vec();
        a.apply(&mut payload);
        assert_ne!(payload, b"hello swarmotter");
        b.apply(&mut payload);
        assert_eq!(payload, b"hello swarmotter");
    }

    #[tokio::test]
    async fn mse_stream_round_trips_over_duplex() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let info_hash = InfoHash::from_bytes([0x42; 20]);
        let server = tokio::spawn(async move { accept(server_io, info_hash).await });
        let mut client = connect(client_io, info_hash).await.unwrap();
        let mut server = server.await.unwrap().unwrap();

        client.write_all(b"ping").await.unwrap();
        client.flush().await.unwrap();
        let mut got = [0u8; 4];
        server.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"ping");

        server.write_all(b"pong").await.unwrap();
        server.flush().await.unwrap();
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"pong");
    }

    #[tokio::test]
    async fn mse_accept_any_routes_by_stream_key() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let first = InfoHash::from_bytes([0x11; 20]);
        let selected = InfoHash::from_bytes([0x22; 20]);
        let server = tokio::spawn(async move { accept_any(server_io, &[first, selected]).await });
        let mut client = connect(client_io, selected).await.unwrap();
        let (identified, mut server) = server.await.unwrap().unwrap();
        assert_eq!(identified, selected);
        client.write_all(b"routed").await.unwrap();
        client.flush().await.unwrap();
        let mut got = [0u8; 6];
        server.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"routed");
    }
}
