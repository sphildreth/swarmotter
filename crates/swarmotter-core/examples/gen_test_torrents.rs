// SPDX-License-Identifier: Apache-2.0
//
// gen_test_torrents: build N small lawful .torrent files and write each
// payload to disk, so two swarmotterd instances (seeder + leecher) can
// exercise the BitTorrent protocol locally without contacting any public
// tracker or webseed. All content is generated and non-copyrighted.
//
// Usage: cargo run --release --example gen_test_torrents -- [count] [out_dir]

use std::env;
use std::fs;
use std::path::PathBuf;

use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};

fn main() {
    let args: Vec<String> = env::args().collect();
    let count: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
    let piece_length: u64 = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(16 * 1024);
    let pieces_per_file: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);

    let out_dir = PathBuf::from(
        args.get(4)
            .cloned()
            .unwrap_or_else(|| "/home/steven/.cache/swarmotter/test_torrents".to_string()),
    );
    fs::create_dir_all(&out_dir).unwrap();

    let tracker_url = args
        .get(5)
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:6969/announce".to_string());
    let private = args.get(6).map(|s| s == "private").unwrap_or(false);

    for i in 0..count {
        let label = format!("local-{i:03}.bin");
        let mut content = Vec::with_capacity(pieces_per_file * piece_length as usize);
        for j in 0..pieces_per_file * piece_length as usize {
            content.push(((j.wrapping_mul(37).wrapping_add(11 + i * 13)) % 251) as u8);
        }
        let torrent_bytes =
            build_single_file_torrent(&label, &content, piece_length, Some(&tracker_url), private);
        let meta = parse_torrent(&torrent_bytes).unwrap();
        let torrent_path = out_dir.join(format!("{label}.torrent"));
        let payload_path = out_dir.join(&label);
        fs::write(&torrent_path, &torrent_bytes).unwrap();
        fs::write(&payload_path, &content).unwrap();
        println!(
            "wrote {label}: {} bytes, info_hash={}",
            content.len(),
            meta.info_hash
        );
    }
}
