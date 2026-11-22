// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde_json::Value;
use swarmotter_api::state::AddTorrentOptions;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::magnet::{Magnet, MagnetTrackerPolicy};

fn provenance_config(root: &Path, _state_path: &Path) -> Config {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.storage.download_dir = Some(root.join("downloads").display().to_string());
    config.storage.incomplete_dir = Some(root.join("incomplete").display().to_string());
    config
}

fn runtime_with_state(root: &Path, state_path: &Path) -> Arc<DaemonRuntime> {
    std::fs::create_dir_all(root.join("downloads")).unwrap();
    std::fs::create_dir_all(root.join("incomplete")).unwrap();
    let mut health = disabled_health();
    health.traffic_allowed = true;
    Arc::new(DaemonRuntime::with_paths_broker_and_state(
        provenance_config(root, state_path),
        health,
        None,
        None,
        Some(state_path.to_path_buf()),
        EventBroker::default(),
    ))
}

fn write_str(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(value);
}

fn write_int(out: &mut Vec<u8>, value: u64) {
    out.push(b'i');
    out.extend_from_slice(value.to_string().as_bytes());
    out.push(b'e');
}

fn annotated_torrent(
    name: &str,
    content: &[u8],
    comment: &str,
    created_by: &str,
    encoding: &str,
) -> Vec<u8> {
    let base = swarmotter_core::meta::build_single_file_torrent(
        name,
        content,
        16,
        Some("https://tracker.example/announce"),
        false,
    );
    let info = swarmotter_core::bencode::extract_value_bytes(&base, b"info").unwrap();
    let mut out = Vec::new();
    out.push(b'd');
    write_str(&mut out, b"announce");
    write_str(&mut out, b"https://tracker.example/announce");
    write_str(&mut out, b"comment");
    write_str(&mut out, comment.as_bytes());
    write_str(&mut out, b"created by");
    write_str(&mut out, created_by.as_bytes());
    write_str(&mut out, b"creation date");
    write_int(&mut out, 1_700_000_000);
    write_str(&mut out, b"encoding");
    write_str(&mut out, encoding.as_bytes());
    write_str(&mut out, b"info");
    out.extend_from_slice(info);
    out.push(b'e');
    out
}

fn tar_entries(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset + 512 <= bytes.len() {
        let header = &bytes[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let name_end = header[..100]
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(100);
        let name = String::from_utf8(header[..name_end].to_vec()).unwrap();
        let size_text = std::str::from_utf8(&header[124..135]).unwrap();
        let size = usize::from_str_radix(size_text.trim_matches('\0'), 8).unwrap();
        let start = offset + 512;
        entries.push((name, bytes[start..start + size].to_vec()));
        offset = start + size.div_ceil(512) * 512;
    }
    entries
}

#[tokio::test]
async fn source_metadata_and_annotation_remain_distinct_and_persistent() {
    let root = unique_dir("provenance-annotation");
    let state_path = root.join("state.json");
    let runtime = runtime_with_state(&root, &state_path);
    runtime.restore_persisted_state().await.unwrap();
    let content = b"source metadata provenance payload";
    let bytes = annotated_torrent(
        "annotated.bin",
        content,
        "source comment",
        "fixture creator 1.0",
        "UTF-8",
    );
    let key = DaemonOps::add_torrent_file(
        &*runtime,
        bytes.clone(),
        AddTorrentOptions::new(Some(root.join("downloads").display().to_string()), true),
    )
    .await
    .unwrap();
    let summary = runtime
        .registry
        .lock()
        .await
        .get(&key)
        .unwrap()
        .to_summary();
    assert_eq!(summary.comment.as_deref(), Some("source comment"));
    assert_eq!(summary.created_by.as_deref(), Some("fixture creator 1.0"));
    assert_eq!(summary.creation_date, Some(1_700_000_000));
    assert_eq!(summary.encoding.as_deref(), Some("UTF-8"));
    assert_eq!(summary.annotation, None);

    let updated =
        DaemonOps::set_torrent_annotation(&*runtime, &key, Some("  operator note  ".into()))
            .await
            .unwrap();
    assert_eq!(updated.annotation.as_deref(), Some("operator note"));
    assert_eq!(updated.comment.as_deref(), Some("source comment"));
    assert!(updated.annotation_updated_at.is_some());
    assert_eq!(updated.annotation_updated_by.as_deref(), Some("api"));
    assert_eq!(
        DaemonOps::original_metainfo(&*runtime, &key).await.unwrap(),
        bytes
    );

    let persisted = runtime.registry.lock().await.get(&key).unwrap().clone();
    assert_eq!(persisted.annotation.as_deref(), Some("operator note"));
    let audits = {
        let connection = rusqlite::Connection::open(&state_path).unwrap();
        let mut statement = connection
            .prepare("SELECT action FROM audit_events ORDER BY id")
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    };
    assert!(audits
        .iter()
        .any(|action| action == "torrent_annotation_changed"));
    drop(runtime);
    let restarted = runtime_with_state(&root, &state_path);
    restarted.restore_persisted_state().await.unwrap();
    let summary = restarted
        .registry
        .lock()
        .await
        .get(&key)
        .unwrap()
        .to_summary();
    assert_eq!(summary.comment.as_deref(), Some("source comment"));
    assert_eq!(summary.created_by.as_deref(), Some("fixture creator 1.0"));
    assert_eq!(summary.creation_date, Some(1_700_000_000));
    assert_eq!(summary.encoding.as_deref(), Some("UTF-8"));
    assert_eq!(summary.annotation.as_deref(), Some("operator note"));
    assert_eq!(summary.annotation_updated_by.as_deref(), Some("api"));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn magnet_generation_obeys_tracker_policy_selection_and_private_consent() {
    let root = unique_dir("provenance-magnet");
    let state_path = root.join("state.json");
    let runtime = runtime_with_state(&root, &state_path);
    runtime.restore_persisted_state().await.unwrap();
    let files = vec![
        (vec!["one & more.txt".into()], 3u64),
        (vec!["two.txt".into()], 4u64),
    ];
    let bytes = swarmotter_core::meta::build_multi_file_torrent_with_webseeds(
        "special & name",
        &files,
        &[b"one", b"two"],
        8,
        Some("https://primary.example/announce"),
        &["https://seed.example/base/"],
    );
    let key = DaemonOps::add_torrent_file(
        &*runtime,
        bytes,
        AddTorrentOptions::new(Some(root.join("downloads").display().to_string()), true),
    )
    .await
    .unwrap();
    DaemonOps::set_wanted(&*runtime, &key, vec![1], false)
        .await
        .unwrap();
    let magnet = DaemonOps::torrent_magnet(&*runtime, &key, MagnetTrackerPolicy::All, false)
        .await
        .unwrap();
    let parsed = Magnet::parse(&magnet).unwrap();
    assert_eq!(parsed.display_name.as_deref(), Some("special & name"));
    assert_eq!(parsed.select_only_file_indices, vec![0]);
    assert_eq!(parsed.webseeds, vec!["https://seed.example/base/"]);
    assert!(parsed
        .trackers
        .iter()
        .any(|tracker| tracker.contains("primary.example")));
    let none = DaemonOps::torrent_magnet(&*runtime, &key, MagnetTrackerPolicy::None, false)
        .await
        .unwrap();
    assert!(Magnet::parse(&none).unwrap().trackers.is_empty());

    let private_bytes = swarmotter_core::meta::build_single_file_torrent(
        "private.bin",
        b"private payload",
        8,
        Some("https://private.example/announce"),
        true,
    );
    let private_key = DaemonOps::add_torrent_file(
        &*runtime,
        private_bytes,
        AddTorrentOptions::new(Some(root.join("downloads").display().to_string()), true),
    )
    .await
    .unwrap();
    let error = DaemonOps::torrent_magnet(&*runtime, &private_key, MagnetTrackerPolicy::All, false)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("explicit consent"));
    let allowed =
        DaemonOps::torrent_magnet(&*runtime, &private_key, MagnetTrackerPolicy::All, true)
            .await
            .unwrap();
    assert!(allowed.contains("private.example"));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn mixed_metainfo_archive_reports_unavailable_originals_without_reconstruction() {
    let root = unique_dir("provenance-archive");
    let state_path = root.join("state.json");
    let runtime = runtime_with_state(&root, &state_path);
    runtime.restore_persisted_state().await.unwrap();
    let content = b"archive original payload";
    let original = annotated_torrent("archive.bin", content, "comment", "creator", "UTF-8");
    let file_key = DaemonOps::add_torrent_file(
        &*runtime,
        original.clone(),
        AddTorrentOptions::new(Some(root.join("downloads").display().to_string()), true),
    )
    .await
    .unwrap();
    let magnet_key = DaemonOps::add_magnet(
        &*runtime,
        "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=magnet-only",
        AddTorrentOptions::new(Some(root.join("downloads").display().to_string()), true),
    )
    .await
    .unwrap();
    let archive = DaemonOps::export_metainfo_archive(
        &*runtime,
        vec![
            file_key,
            magnet_key,
            TorrentKey::v1(InfoHash::from_bytes([9u8; 20])),
        ],
    )
    .await
    .unwrap();
    let entries = tar_entries(&archive);
    let manifest = entries
        .iter()
        .find(|(name, _)| name == "manifest.json")
        .map(|(_, bytes)| serde_json::from_slice::<Value>(bytes).unwrap())
        .unwrap();
    let rows = manifest["entries"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["status"], "included");
    assert_eq!(rows[1]["status"], "unavailable");
    assert_eq!(rows[2]["status"], "unavailable");
    assert_eq!(manifest["format"], "swarmotter-metainfo-export-v1");
    let filename = rows[0]["filename"].as_str().unwrap();
    let archived = entries
        .iter()
        .find(|(name, _)| name == filename)
        .map(|(_, bytes)| bytes.clone())
        .unwrap();
    assert_eq!(archived, original);
    assert!(!entries
        .iter()
        .any(|(name, bytes)| name != "manifest.json" && bytes == b"reconstructed"));
    std::fs::remove_dir_all(root).ok();
}
