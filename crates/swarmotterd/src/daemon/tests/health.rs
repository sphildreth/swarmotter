// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn health_input_uses_recent_peer_block_activity() {
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        "health.bin",
        b"0123456789abcdef",
        8,
        None,
        false,
    );
    let meta = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    let mut torrent = Torrent::new(meta.clone(), 1);
    torrent.state = TorrentState::Downloading;

    let mut peer_health = HashMap::new();
    peer_health.insert(
        "127.0.0.1:6881".parse().unwrap(),
        EnginePeerHealth {
            has_missing_pieces: true,
            unchoked: true,
            useful_recently: true,
            last_valid_block: Some(Instant::now()),
            last_seen: Some(Instant::now()),
            ..Default::default()
        },
    );

    let input = build_health_input(
        &torrent,
        meta.piece_count(),
        &swarmotter_core::storage::resume::PieceBitfield::new(meta.piece_count()),
        &peer_health,
        &true,
        false,
        false,
        0,
        0,
        0,
        0,
        None,
        None,
        None,
        None,
        None,
        Some(Instant::now()),
        1,
        None,
        0,
        0,
        NetworkHealth::blocked(
            NetworkContainmentMode::Disabled,
            swarmotter_core::models::network::NetworkContainmentStatus::Disabled,
            "disabled",
        ),
    );

    assert!(input.received_block_recently);
    let health = HealthCalculator::new().compute(&input);
    assert!(
        health.score > 25,
        "recent peer blocks should avoid the stalled health cap"
    );
}
