// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::engine::tests::{pure_v2_swarm_meta_with_piece_length, unique_dir};
use tokio::io::{duplex, split, BufWriter};

fn engine(meta: &TorrentMeta, path: &std::path::Path) -> TorrentEngine {
    let (_, commands) = mpsc::channel(1);
    let mut engine = TorrentEngine::new(
        meta.clone(),
        path.to_owned(),
        [0; 20],
        Arc::new(swarmotter_core::net::binder::LoopbackBinder),
        Arc::new(Mutex::new(EngineState::default())),
        commands,
        vec![],
        0,
    );
    engine.prepare_v2_file_selection();
    engine
}

#[tokio::test]
async fn incomplete_v2_peers_read_while_writes_are_backpressured() {
    // Each piece spans two request windows. Even one reply cannot fit in
    // either peer's socket buffer until its reader makes progress.
    let piece_len = u64::from(peer::BLOCK_SIZE) * REQUEST_WINDOW as u64 * 2;
    let (meta, payloads) = pure_v2_swarm_meta_with_piece_length(piece_len);
    let layout = meta.v2_piece_layout().unwrap();
    let paths = [unique_dir("v2-duplex-a"), unique_dir("v2-duplex-b")];
    let address = PeerAddr::from_socket_addr("127.0.0.1:5050".parse().unwrap());
    let (a, b) = duplex(256);
    let run = |side: usize, stream| {
        let layout = &layout;
        let payloads = &payloads;
        let meta = &meta;
        let path = &paths[side];
        async move {
            let engine = engine(meta, path);
            let storage = StorageIo::new(meta.clone(), path);
            let mut have = PieceBitfield::new(layout.piece_count());
            for index in side * 2..side * 2 + 2 {
                let mapping = layout.piece(index).unwrap();
                let data = &payloads[side][mapping.offset as usize..][..mapping.length as usize];
                storage.write_v2_piece(layout, index, data).await.unwrap();
                have.set(index);
            }
            let session = V2Peer {
                engine: &engine,
                storage: &storage,
                layout,
                peer: address,
            };
            let (reader, writer) = split(stream);
            assert!(session
                .exchange(
                    PeerReader::new(reader),
                    writer,
                    &mut have,
                    Duration::from_secs(8)
                )
                .await
                .unwrap());
            assert_eq!(have.count(layout.piece_count()), 4);
            assert_eq!(
                engine.state.lock().await.uploaded,
                payloads[side].len() as u64
            );
            assert_eq!(storage.recheck().await.unwrap(), have);
        }
    };
    tokio::join!(run(0, a), run(1, b));
    for path in paths {
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[tokio::test]
async fn v2_flushes_piece_and_reject_and_expires_a_blocked_upload() {
    let (meta, payloads) = pure_v2_swarm_meta_with_piece_length(u64::from(peer::BLOCK_SIZE));
    let layout = meta.v2_piece_layout().unwrap();
    let path = unique_dir("v2-flush-deadline");
    let engine = engine(&meta, &path);
    let storage = StorageIo::new(meta, &path);
    let block = &payloads[0][..peer::BLOCK_SIZE as usize];
    storage.write_v2_piece(&layout, 0, block).await.unwrap();
    let mut have = PieceBitfield::new(layout.piece_count());
    have.set(0);
    let session = V2Peer {
        engine: &engine,
        storage: &storage,
        layout: &layout,
        peer: PeerAddr::from_socket_addr("127.0.0.1:5050".parse().unwrap()),
    };

    let (local, remote) = duplex(128);
    let (reader, writer) = split(local);
    // Like the encrypted transport, this writer can accept an entire block
    // while retaining bytes until flush.
    let writer = BufWriter::with_capacity(64 * 1024, writer);
    let (remote_read, mut remote_write) = split(remote);
    let mut remote_read = PeerReader::new(remote_read);
    let exercise = async {
        for _ in 0..3 {
            remote_read.read_message().await.unwrap().unwrap();
        }
        for piece in [0, 1] {
            peer::write_message(
                &mut remote_write,
                &Message::Request {
                    piece,
                    offset: 0,
                    length: peer::BLOCK_SIZE,
                },
            )
            .await
            .unwrap();
            let reply = remote_read.read_message().await.unwrap().unwrap();
            match reply {
                Message::Piece {
                    piece: 0,
                    offset: 0,
                    block: bytes,
                } if piece == 0 => assert_eq!(bytes, block),
                Message::Reject {
                    piece: 1,
                    offset: 0,
                    length,
                } if piece == 1 => assert_eq!(length, peer::BLOCK_SIZE),
                other => panic!("unexpected v2 reply {other:?}"),
            }
        }
        peer::write_message(
            &mut remote_write,
            &Message::Request {
                piece: 0,
                offset: 0,
                length: peer::BLOCK_SIZE,
            },
        )
        .await
        .unwrap();
        remote_write.shutdown().await.unwrap();
        // Keep the read half open without draining the last reply.
    };
    let start = TokioInstant::now();
    let duration = Duration::from_secs(1);
    let (result, ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(
            session.exchange(PeerReader::new(reader), writer, &mut have, duration),
            exercise
        )
    })
    .await
    .expect("the peer-session deadline must interrupt a blocked flush");
    assert!(!result.unwrap());
    assert!(start.elapsed() >= duration);
    assert_eq!(
        engine.state.lock().await.uploaded,
        u64::from(peer::BLOCK_SIZE)
    );
    std::fs::remove_dir_all(path).unwrap();
}
