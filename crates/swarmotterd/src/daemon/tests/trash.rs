// SPDX-License-Identifier: Apache-2.0

use super::*;
use swarmotter_api::state::AddTorrentOptions;
use swarmotter_core::trash::{
    RestoreRequest, TrashRetentionRule, TrashSelectionMode, TrashSelectionRequest,
};

fn trash_config(root: &Path, state_path: &Path) -> Config {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.storage.download_dir = Some(root.join("downloads").display().to_string());
    config.storage.incomplete_dir = Some(root.join("incomplete").display().to_string());
    config.trash.path = Some(root.join("trash").display().to_string());
    config.trash.retention = vec![TrashRetentionRule {
        name: "paused-cleanup".into(),
        profile: None,
        label: Some("cleanup".into()),
        states: vec![TorrentState::Paused],
        min_age_seconds: 0,
    }];
    let _ = state_path;
    config
}

async fn runtime_with_trash(root: &Path, state_path: &Path) -> Arc<DaemonRuntime> {
    std::fs::create_dir_all(root.join("downloads")).unwrap();
    std::fs::create_dir_all(root.join("incomplete")).unwrap();
    let mut health = disabled_health();
    health.traffic_allowed = true;
    let runtime = Arc::new(DaemonRuntime::with_paths_broker_and_state(
        trash_config(root, state_path),
        health,
        None,
        None,
        Some(state_path.to_path_buf()),
        EventBroker::default(),
    ));
    runtime.restore_persisted_state().await.unwrap();
    runtime
}

async fn add_multi_file_torrent(
    runtime: &DaemonRuntime,
    root: &Path,
) -> (TorrentKey, Vec<PathBuf>, Vec<u8>) {
    let files = vec![
        (vec!["one.bin".into()], 3u64),
        (vec!["two.bin".into()], 4u64),
    ];
    let bytes = swarmotter_core::meta::build_multi_file_torrent(
        "bundle",
        &files,
        &[b"one", b"two"],
        8,
        None,
    );
    let directory = root.join("downloads").join("bundle");
    std::fs::create_dir_all(&directory).unwrap();
    let paths = vec![directory.join("one.bin"), directory.join("two.bin")];
    std::fs::write(&paths[0], b"one").unwrap();
    std::fs::write(&paths[1], b"two").unwrap();
    let key = DaemonOps::add_torrent_file(
        runtime,
        bytes.clone(),
        AddTorrentOptions::new(Some(root.join("downloads").display().to_string()), true),
    )
    .await
    .unwrap();
    (key, paths, bytes)
}

#[tokio::test]
async fn preview_dry_run_then_trash_and_restore_preserves_exact_paths() {
    let root = unique_dir("trash-roundtrip");
    let state_path = root.join("state.json");
    let runtime = runtime_with_trash(&root, &state_path).await;
    let (key, paths, original_metainfo) = add_multi_file_torrent(&runtime, &root).await;
    let before_queue = {
        let queue = runtime.queue.lock().await;
        (queue.order.clone(), queue.bypass.clone())
    };
    let request = TrashSelectionRequest {
        hashes: vec![key.to_locator()],
        rule: None,
        mode: TrashSelectionMode::Torrent,
    };
    let preview = DaemonOps::preview_trash(&*runtime, request.clone())
        .await
        .unwrap();
    assert_eq!(preview.items.len(), 1);
    assert_eq!(
        preview.items[0]
            .paths
            .iter()
            .filter(|path| path.exists)
            .count(),
        2,
        "{preview:?} paths={paths:?} torrent_download={:?}",
        runtime
            .registry
            .lock()
            .await
            .get(&key)
            .map(|t| t.download_dir.clone())
    );
    assert!(paths.iter().all(|path| path.exists()));
    assert!(runtime.registry.lock().await.contains(&key));
    let after_preview = {
        let queue = runtime.queue.lock().await;
        (queue.order.clone(), queue.bypass.clone())
    };
    assert_eq!(after_preview, before_queue);

    let applied = DaemonOps::apply_trash(&*runtime, request).await.unwrap();
    assert_eq!(applied.items.len(), 1);
    assert_eq!(applied.items[0].status, "trashed");
    let entry_id = applied.items[0].entry_id.clone().unwrap();
    assert!(paths.iter().all(|path| !path.exists()));
    assert!(!runtime.registry.lock().await.contains(&key));
    let status = DaemonOps::trash_status(&*runtime).await.unwrap();
    assert_eq!(status.entries.len(), 1);
    assert_eq!(status.entries[0].id, entry_id);
    assert_eq!(status.entries[0].status, "ready");
    assert!(status.entries[0].has_original_metainfo);
    assert!(root
        .join("trash")
        .join("entries")
        .join(&entry_id)
        .join("original.torrent")
        .exists());

    let restored = DaemonOps::restore_trash(
        &*runtime,
        &entry_id,
        RestoreRequest {
            collision_policy: Some("fail".into()),
        },
    )
    .await
    .unwrap();
    assert_eq!(restored.status, "restored");
    assert!(runtime.registry.lock().await.contains(&key));
    assert!(paths.iter().all(|path| path.exists()));
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"one");
    assert_eq!(std::fs::read(&paths[1]).unwrap(), b"two");
    assert_eq!(
        DaemonOps::original_metainfo(&*runtime, &key).await.unwrap(),
        original_metainfo
    );
    assert!(DaemonOps::trash_status(&*runtime)
        .await
        .unwrap()
        .entries
        .is_empty());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn stale_partial_and_orphan_cleanup_keeps_live_record_and_restores() {
    let root = unique_dir("trash-stale");
    let state_path = root.join("state.json");
    let runtime = runtime_with_trash(&root, &state_path).await;
    let (key, paths, _) = add_multi_file_torrent(&runtime, &root).await;
    DaemonOps::set_wanted(&*runtime, &key, vec![1], false)
        .await
        .unwrap();
    let orphan = root.join("downloads").join("bundle").join("orphan.bin");
    std::fs::write(&orphan, b"orphan data").unwrap();
    let request = TrashSelectionRequest {
        hashes: vec![key.to_locator()],
        rule: None,
        mode: TrashSelectionMode::StalePartials,
    };
    let preview = DaemonOps::preview_trash(&*runtime, request.clone())
        .await
        .unwrap();
    assert_eq!(preview.items.len(), 1);
    let listed = preview.items[0]
        .paths
        .iter()
        .map(|path| path.path.clone())
        .collect::<Vec<_>>();
    assert!(listed.iter().any(|path| path.ends_with("two.bin")));
    assert!(listed.iter().any(|path| path.ends_with("orphan.bin")));
    assert!(!listed.iter().any(|path| path.ends_with("one.bin")));

    let applied = DaemonOps::apply_trash(&*runtime, request).await.unwrap();
    assert_eq!(applied.items[0].status, "trashed", "{applied:?}");
    let entry_id = applied.items[0].entry_id.clone().unwrap();
    assert!(paths[0].exists());
    assert!(!paths[1].exists());
    assert!(!orphan.exists());
    assert!(runtime.registry.lock().await.contains(&key));
    let restored = DaemonOps::restore_trash(
        &*runtime,
        &entry_id,
        RestoreRequest {
            collision_policy: Some("fail".into()),
        },
    )
    .await
    .unwrap();
    assert_eq!(restored.paths_restored, 2);
    assert!(paths[1].exists());
    assert!(orphan.exists());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn retention_rules_explain_qualification_and_apply_only_qualified_items() {
    let root = unique_dir("trash-rules");
    let state_path = root.join("state.json");
    let runtime = runtime_with_trash(&root, &state_path).await;
    let (key, paths, _) = add_multi_file_torrent(&runtime, &root).await;
    let request = TrashSelectionRequest {
        hashes: Vec::new(),
        rule: Some("paused-cleanup".into()),
        mode: TrashSelectionMode::Torrent,
    };
    // The added torrent has no cleanup label, so the rule must not select it.
    let preview = DaemonOps::preview_trash(&*runtime, request.clone())
        .await
        .unwrap();
    assert!(preview.items.is_empty());

    DaemonOps::set_labels(&*runtime, &key, vec!["CLEANUP".into()])
        .await
        .unwrap();
    let preview = DaemonOps::preview_trash(&*runtime, request.clone())
        .await
        .unwrap();
    assert_eq!(preview.items.len(), 1);
    assert_eq!(
        preview.items[0].applied_rule.as_deref(),
        Some("paused-cleanup")
    );
    assert!(preview.items[0].eligible);
    assert!(preview.items[0].rule_reason.contains("minimum age"));
    let applied = DaemonOps::apply_trash(&*runtime, request).await.unwrap();
    assert_eq!(applied.items[0].status, "trashed");
    assert!(paths.iter().all(|path| !path.exists()));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn restore_conflict_is_reported_without_overwriting_or_losing_trash() {
    let root = unique_dir("trash-conflict");
    let state_path = root.join("state.json");
    let runtime = runtime_with_trash(&root, &state_path).await;
    let (key, paths, _) = add_multi_file_torrent(&runtime, &root).await;
    let request = TrashSelectionRequest {
        hashes: vec![key.to_locator()],
        rule: None,
        mode: TrashSelectionMode::Torrent,
    };
    let applied = DaemonOps::apply_trash(&*runtime, request).await.unwrap();
    let entry_id = applied.items[0].entry_id.clone().unwrap();
    std::fs::create_dir_all(paths[0].parent().unwrap()).unwrap();
    std::fs::write(&paths[0], b"collision").unwrap();
    let error = DaemonOps::restore_trash(
        &*runtime,
        &entry_id,
        RestoreRequest {
            collision_policy: Some("fail".into()),
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("already exists"));
    assert!(runtime.registry.lock().await.get(&key).is_none());
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"collision");
    assert!(root
        .join("trash")
        .join("entries")
        .join(&entry_id)
        .join("entry.json")
        .exists());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn interrupted_staging_entry_is_visible_and_purgeable() {
    let root = unique_dir("trash-staging");
    let state_path = root.join("state.json");
    let runtime = runtime_with_trash(&root, &state_path).await;
    let staging = root
        .join("trash")
        .join(".staging")
        .join("interrupted-entry");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(staging.join("partial.bin"), b"partial").unwrap();
    let status = DaemonOps::trash_status(&*runtime).await.unwrap();
    assert!(status
        .entries
        .iter()
        .any(|entry| entry.id == "interrupted-entry" && entry.status == "incomplete"));
    DaemonOps::purge_trash(&*runtime, "interrupted-entry")
        .await
        .unwrap();
    assert!(!staging.exists());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn move_failure_reports_per_item_and_keeps_live_record_and_files() {
    let root = unique_dir("trash-move-failure");
    let state_path = root.join("state.json");
    std::fs::create_dir_all(root.join("downloads")).unwrap();
    std::fs::create_dir_all(root.join("incomplete")).unwrap();
    let mut config = trash_config(&root, &state_path);
    let blocked_trash = root.join("trash-blocked");
    std::fs::write(&blocked_trash, b"not a directory").unwrap();
    config.trash.path = Some(blocked_trash.display().to_string());
    let mut health = disabled_health();
    health.traffic_allowed = true;
    let runtime = Arc::new(DaemonRuntime::with_paths_broker_and_state(
        config,
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    ));
    runtime.restore_persisted_state().await.unwrap();
    let (key, paths, _) = add_multi_file_torrent(&runtime, &root).await;
    let result = DaemonOps::apply_trash(
        &*runtime,
        TrashSelectionRequest {
            hashes: vec![key.to_locator()],
            rule: None,
            mode: TrashSelectionMode::Torrent,
        },
    )
    .await
    .unwrap();
    assert_eq!(result.items[0].status, "failed");
    assert!(result.items[0].error.is_some());
    assert!(runtime.registry.lock().await.contains(&key));
    assert!(paths.iter().all(|path| path.exists()));
    std::fs::remove_dir_all(root).ok();
}
