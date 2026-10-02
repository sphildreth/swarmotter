// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::meta::{build_multi_file_torrent, build_single_file_torrent, parse_torrent, MetaFile};

fn unique_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "swarmotter-storage-{}-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        label
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[tokio::test]
async fn dedicated_resume_directory_uses_torrent_key_without_relocating_payload() {
    let bytes = build_single_file_torrent("same-name.bin", b"payload", 7, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let active = unique_dir("resume-active");
    let resume = unique_dir("resume-state");
    let complete = unique_dir("resume-complete");
    let store = StorageIo::new(meta.clone(), active.clone()).with_resume_dir(Some(resume.clone()));

    store.ensure_active_layout().await.unwrap();
    store.write_piece(0, b"payload").await.unwrap();
    let resume_path = store.resume_path();
    assert_eq!(
        resume_path,
        resume.join(format!("{}.swarmotter.resume", meta.info_hash))
    );
    assert!(!resume_path.starts_with(&active));

    let persisted = build_resume(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        PieceBitfield::new(meta.piece_count()),
        meta.piece_count(),
        0,
        0,
        meta.total_length,
        Some(active.display().to_string()),
        1,
        None,
        &[crate::models::torrent::FilePriority::Normal],
        &[meta.total_length],
    );
    store.save_resume(&persisted).await.unwrap();
    assert!(resume_path.is_file());

    let moved = store.move_to(complete.clone()).await.unwrap();
    assert_eq!(
        std::fs::read(moved.file_path(0).unwrap()).unwrap(),
        b"payload"
    );
    assert!(!active.join("same-name.bin").exists());
    assert!(
        !resume_path.exists(),
        "completion removes only resume metadata"
    );
    assert!(!complete.join("same-name.bin.swarmotter.resume").exists());
    std::fs::remove_dir_all(active).ok();
    std::fs::remove_dir_all(resume).ok();
    std::fs::remove_dir_all(complete).ok();
}

#[tokio::test]
async fn partial_file_suffix_finalizes_single_file_in_place() {
    let content = b"0123456789abcdef";
    let bytes = build_single_file_torrent("active.bin", content, 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let root = unique_dir("partial-suffix-finalize-single");
    let active =
        StorageIo::new(meta.clone(), root.clone()).with_partial_file_suffix(Some(".part".into()));
    let canonical = StorageIo::new(meta.clone(), root.clone());

    active.preallocate().await.unwrap();
    active.write_block(0, 0, &content[..8]).await.unwrap();
    active.write_block(1, 0, &content[8..]).await.unwrap();
    let active_path = active.file_path(0).unwrap();
    assert!(active_path.ends_with("active.bin.part"));
    assert!(active_path.exists());

    let finalized = active.finalize_partial_file_suffix().await.unwrap();

    assert_eq!(finalized.partial_file_suffix(), None);
    assert!(!active_path.exists());
    assert_eq!(
        finalized.file_path(0).unwrap(),
        canonical.file_path(0).unwrap()
    );
    assert_eq!(
        std::fs::read(finalized.file_path(0).unwrap()).unwrap(),
        content
    );
    assert!(finalized.recheck().await.unwrap().has(0));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn partial_file_suffix_moves_multi_file_payload_to_canonical_paths() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("bundle", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let active_root = unique_dir("partial-suffix-move-multi-active");
    let complete_root = unique_dir("partial-suffix-move-multi-complete");
    let active = StorageIo::new(meta.clone(), active_root.clone())
        .with_partial_file_suffix(Some(".part".into()));

    active.preallocate().await.unwrap();
    active.write_block(0, 0, b"hell").await.unwrap();
    active.write_block(1, 0, b"owor").await.unwrap();
    active.write_block(2, 0, b"ld!!").await.unwrap();
    let active_first = active.file_path(0).unwrap();
    let active_second = active.file_path(1).unwrap();

    let complete = active.move_to(complete_root.clone()).await.unwrap();

    assert_eq!(complete.partial_file_suffix(), None);
    assert!(!active_first.exists());
    assert!(!active_second.exists());
    assert_eq!(
        std::fs::read(complete.file_path(0).unwrap()).unwrap(),
        b"hello"
    );
    assert_eq!(
        std::fs::read(complete.file_path(1).unwrap()).unwrap(),
        b"world!!"
    );
    std::fs::remove_dir_all(active_root).ok();
    std::fs::remove_dir_all(complete_root).ok();
}

#[tokio::test]
async fn partial_file_suffix_active_move_preserves_incomplete_names() {
    let bytes = build_single_file_torrent("moving.bin", b"01234567", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let old_root = unique_dir("partial-suffix-active-move-old");
    let new_root = unique_dir("partial-suffix-active-move-new");
    let active = StorageIo::new(meta.clone(), old_root.clone())
        .with_partial_file_suffix(Some(".part".into()));

    active.preallocate().await.unwrap();
    active.write_block(0, 0, b"01234567").await.unwrap();
    let old_path = active.file_path(0).unwrap();
    let moved = active
        .move_to_with_partial_file_suffix(new_root.clone(), Some(".part".into()))
        .await
        .unwrap();

    let new_suffix_path = moved.file_path(0).unwrap();
    let canonical = StorageIo::new(meta, new_root.clone()).file_path(0).unwrap();
    assert_eq!(moved.partial_file_suffix(), Some(".part"));
    assert!(!old_path.exists());
    assert!(new_suffix_path.ends_with("moving.bin.part"));
    assert_eq!(std::fs::read(new_suffix_path).unwrap(), b"01234567");
    assert!(!canonical.exists());
    std::fs::remove_dir_all(old_root).ok();
    std::fs::remove_dir_all(new_root).ok();
}

#[tokio::test]
async fn partial_file_suffix_finalization_rejects_canonical_collision_without_data_loss() {
    let bytes = build_single_file_torrent("collision.bin", b"01234567", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let root = unique_dir("partial-suffix-finalize-collision");
    let active =
        StorageIo::new(meta.clone(), root.clone()).with_partial_file_suffix(Some(".part".into()));
    let canonical = StorageIo::new(meta, root.clone());

    active.preallocate().await.unwrap();
    active.write_block(0, 0, b"01234567").await.unwrap();
    let active_path = active.file_path(0).unwrap();
    let canonical_path = canonical.file_path(0).unwrap();
    fs::write(&canonical_path, b"existing canonical payload")
        .await
        .unwrap();

    let error = match active.finalize_partial_file_suffix().await {
        Ok(_) => panic!("canonical collision must reject suffix finalization"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("destination file already exists"));
    assert_eq!(std::fs::read(active_path).unwrap(), b"01234567");
    assert_eq!(
        std::fs::read(canonical_path).unwrap(),
        b"existing canonical payload"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn storage_metrics_count_successful_writes_and_verification_reads() {
    let bytes = build_single_file_torrent("metrics.bin", b"metrics", 7, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let root = unique_dir("storage-metrics");
    let metrics = StorageIoMetrics::default();
    let store = StorageIo::new(meta, root.clone()).with_metrics(Some(metrics.clone()));

    store.ensure_active_layout().await.unwrap();
    store.write_piece(0, b"metrics").await.unwrap();
    assert!(store.verify_piece_on_disk(0).await.unwrap());
    let throughput = metrics.throughput();
    assert_eq!(throughput.write_bytes_per_second, 7);
    assert_eq!(throughput.verification_bytes_per_second, 7);
    std::fs::remove_dir_all(root).ok();
}

#[cfg(target_os = "linux")]
#[test]
fn nocow_strategy_rejects_an_unsupported_filesystem_before_payload_write() {
    let path = Path::new("unsupported-filesystem-payload");
    let error = ensure_btrfs_filesystem(0xef53, path).unwrap_err();
    assert_eq!(error.code().as_str(), "storage_error");
    assert!(error.to_string().contains("requires Linux Btrfs"));
}

#[test]
fn piece_to_file_offset_mapping_single() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let m = piece_file_mapping(&meta, 0).unwrap();
    assert_eq!(m, vec![(0, 0, 8)]);
    let m1 = piece_file_mapping(&meta, 1).unwrap();
    assert_eq!(m1, vec![(0, 8, 8)]);
}

#[test]
fn piece_to_file_mapping_multi_file_boundary() {
    // dir/a.txt (5 bytes) + dir/sub/b.bin (7 bytes) = 12 bytes, piece_length 4.
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    // Piece 0: bytes 0..4 -> a.txt [0..4]
    assert_eq!(piece_file_mapping(&meta, 0).unwrap(), vec![(0, 0, 4)]);
    // Piece 1: bytes 4..8 -> a.txt [4..5] (1 byte) + b.bin [0..3] (3 bytes)
    let p1 = piece_file_mapping(&meta, 1).unwrap();
    assert_eq!(p1, vec![(0, 4, 1), (1, 0, 3)]);
    // Piece 2: bytes 8..12 -> b.bin [3..7]
    assert_eq!(piece_file_mapping(&meta, 2).unwrap(), vec![(1, 3, 4)]);
}

#[tokio::test]
async fn write_and_verify_single_file_piece() {
    let content = b"hello swarmotter world data payload here";
    let bytes = build_single_file_torrent("file.bin", content, 16, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("single-write");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();
    // Write piece 0 bytes.
    let p0 = &content[..16];
    let res = store.verify_piece_on_disk(0).await.unwrap();
    assert!(!res);
    store.write_block(0, 0, p0).await.unwrap();
    store.write_block(1, 0, &content[16..32]).await.unwrap();
    store.write_block(2, 0, &content[32..]).await.unwrap();
    assert!(store.verify_piece_on_disk(0).await.unwrap());
    assert!(store.verify_piece_on_disk(1).await.unwrap());
    assert!(store.verify_piece_on_disk(2).await.unwrap());
    let all = store.read_piece(0).await.unwrap();
    assert_eq!(all, p0);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn resume_stamps_detect_same_size_edits_with_restored_mtime() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;

    let original = b"abcdefgh";
    let replacement = b"ABCDEFGH";
    let bytes = build_single_file_torrent("stamp.bin", original, 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("resume-stamp-ctime");
    let store = StorageIo::new(meta, dir.clone());
    let path = store.file_path(0).unwrap();
    tokio::fs::write(&path, original).await.unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let before = store.resume_file_stamps().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    tokio::fs::write(&path, replacement).await.unwrap();
    let times = [
        libc::timespec {
            tv_sec: metadata.atime(),
            tv_nsec: metadata.atime_nsec(),
        },
        libc::timespec {
            tv_sec: metadata.mtime(),
            tv_nsec: metadata.mtime_nsec(),
        },
    ];
    let path_c = CString::new(path.as_os_str().as_bytes()).unwrap();
    let result = unsafe { libc::utimensat(libc::AT_FDCWD, path_c.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(result, 0);

    let after = store.resume_file_stamps().await.unwrap();
    assert_eq!(before[0].modified_unix_nanos, after[0].modified_unix_nanos);
    assert_ne!(before, after);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn storage_reuses_file_handles_for_repeated_block_io() {
    let content = b"0123456789abcdef";
    let bytes = build_single_file_torrent("reuse.bin", content, 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("handle-reuse");
    let store = StorageIo::new(meta.clone(), dir.clone());

    store.write_block(0, 0, &content[..8]).await.unwrap();
    let first_handle = store
        .file_handles
        .lock()
        .await
        .get(&0)
        .unwrap()
        .file
        .clone();
    assert_eq!(store.file_handles.lock().await.len(), 1);

    let clone = store.clone();
    clone.write_block(1, 0, &content[8..]).await.unwrap();
    let second_handle = clone
        .file_handles
        .lock()
        .await
        .get(&0)
        .unwrap()
        .file
        .clone();
    assert!(Arc::ptr_eq(&first_handle, &second_handle));
    assert_eq!(clone.read_block(0, 0, 8).await.unwrap(), &content[..8]);
    assert_eq!(clone.file_handles.lock().await.len(), 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn read_only_recheck_does_not_retain_payload_handles() {
    let files = vec![
        (vec!["a.bin".into()], 4u64),
        (vec!["b.bin".into()], 4u64),
        (vec!["c.bin".into()], 4u64),
    ];
    let contents: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc"];
    let bytes = build_multi_file_torrent("recheck", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("read-handles-not-retained");
    let store = StorageIo::new(meta.clone(), dir.clone());
    for (index, content) in contents.iter().enumerate() {
        let path = store.file_path(index).unwrap();
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(path, content).await.unwrap();
    }

    let verified = store.recheck().await.unwrap();

    assert_eq!(verified.count(meta.piece_count()), meta.piece_count());
    assert!(
        store.file_handles.lock().await.is_empty(),
        "read-only rechecks must not retain Tokio file buffers"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn writable_file_handle_cache_is_bounded() {
    let file_count = MAX_CACHED_WRITABLE_FILE_HANDLES + 1;
    let files = (0..file_count)
        .map(|index| (vec![format!("file-{index}.bin")], 1u64))
        .collect::<Vec<_>>();
    let owned_contents = (0..file_count)
        .map(|index| vec![(index % 251) as u8])
        .collect::<Vec<_>>();
    let contents = owned_contents.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let bytes = build_multi_file_torrent("bounded", &files, &contents, 1, None);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("bounded-write-handles");
    let store = StorageIo::new(meta, dir.clone());

    for (index, content) in contents.iter().enumerate() {
        store.write_piece(index, content).await.unwrap();
    }

    assert!(
        store.file_handles.lock().await.len() <= MAX_CACHED_WRITABLE_FILE_HANDLES,
        "writable handle cache exceeded its fixed working-set bound"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn write_piece_writes_single_file_piece() {
    let content = b"0123456789abcdeflast";
    let bytes = build_single_file_torrent("piece.bin", content, 16, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("single-piece-write");
    let store = StorageIo::new(meta.clone(), dir.clone());

    store.write_piece(0, &content[..16]).await.unwrap();
    store.write_piece(1, &content[16..]).await.unwrap();

    assert_eq!(store.read_piece(0).await.unwrap(), &content[..16]);
    assert_eq!(store.read_piece(1).await.unwrap(), &content[16..]);
    assert_eq!(std::fs::read(store.file_path(0).unwrap()).unwrap(), content);
    assert!(store.verify_piece_on_disk(0).await.unwrap());
    assert!(store.verify_piece_on_disk(1).await.unwrap());
    let err = store.write_piece(0, b"short").await;
    assert!(err.is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn active_layout_creates_single_file_placeholder_without_preallocating() {
    let content = b"placeholder appears before first piece";
    let bytes = build_single_file_torrent("visible.bin", content, 16, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("active-visible-single");
    let store = StorageIo::new(meta.clone(), dir.clone());

    store.ensure_active_layout().await.unwrap();

    let path = store.file_path(0).unwrap();
    assert!(path.exists());
    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn active_layout_creates_multi_file_top_directory() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("visible-dir", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("active-visible-multi");
    let store = StorageIo::new(meta.clone(), dir.clone());

    store.ensure_active_layout().await.unwrap();

    assert!(dir.join("visible-dir").exists());
    assert!(!store.file_path(0).unwrap().exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn multi_file_boundary_write() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("multi-write");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();
    // Write every piece. Piece 1 crosses the file boundary
    // (a.txt[4..5] = 'o' + b.bin[0..3] = 'wor').
    store.write_block(0, 0, b"hell").await.unwrap();
    store.write_block(1, 0, b"owor").await.unwrap();
    store.write_block(2, 0, b"ld!!").await.unwrap();
    // All pieces verify against metadata, flushing pending cached writes
    // before the raw filesystem assertions below inspect the files.
    assert!(store.verify_piece_on_disk(0).await.unwrap());
    assert!(store.verify_piece_on_disk(1).await.unwrap());
    assert!(store.verify_piece_on_disk(2).await.unwrap());
    let a = std::fs::read(dir.join("dir").join("a.txt")).unwrap();
    assert_eq!(&a, b"hello");
    let b = std::fs::read(dir.join("dir").join("sub").join("b.bin")).unwrap();
    assert_eq!(&b, b"world!!");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn remove_all_preserves_configured_base_directory() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("remove-all-preserves-base");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, b"hell").await.unwrap();
    store.write_block(1, 0, b"owor").await.unwrap();
    store.write_block(2, 0, b"ld!!").await.unwrap();

    store.remove_all().await.unwrap();

    assert!(
        dir.exists(),
        "remove_all must preserve the configured storage base directory"
    );
    assert!(
        !dir.join("dir").exists(),
        "remove_all should remove the torrent payload root when empty"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn remove_all_reports_payload_deletion_failures() {
    let bytes = build_single_file_torrent("blocked.bin", b"01234567", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("remove-all-error");
    let store = StorageIo::new(meta, dir.clone());
    let payload = store.file_path(0).unwrap();
    fs::create_dir_all(&payload).await.unwrap();

    let error = store.remove_all().await.unwrap_err();

    assert!(error
        .to_string()
        .contains("failed to remove all torrent data"));
    assert!(error.to_string().contains(&payload.display().to_string()));
    assert!(payload.is_dir());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn write_piece_range_preserves_multi_file_boundaries() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("multi-piece-range-write");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();

    store.write_piece(0, b"hell").await.unwrap();
    store.write_piece_range(1, 0, b"owor").await.unwrap();
    store.write_piece(2, b"ld!!").await.unwrap();

    assert_eq!(store.read_piece(0).await.unwrap(), b"hell");
    assert_eq!(store.read_piece(1).await.unwrap(), b"owor");
    assert_eq!(store.read_piece(2).await.unwrap(), b"ld!!");
    let a = std::fs::read(dir.join("dir").join("a.txt")).unwrap();
    assert_eq!(&a, b"hello");
    let b = std::fs::read(dir.join("dir").join("sub").join("b.bin")).unwrap();
    assert_eq!(&b, b"world!!");
    let err = store.write_piece_range(1, 3, b"or").await;
    assert!(err.is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn verify_rejects_bad_piece() {
    let bytes = build_single_file_torrent("f", b"0123456789abcdef", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("verify-bad");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, b"XXXXXXXX").await.unwrap();
    assert!(!store.verify_piece_on_disk(0).await.unwrap());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn resume_save_load_roundtrip() {
    let content = b"0123456789abcdef0123456789abcdef";
    let bytes = build_single_file_torrent("r.bin", content, 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("resume");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, &content[..8]).await.unwrap();
    store.write_block(1, 0, &content[8..16]).await.unwrap();
    store.write_block(2, 0, &content[16..24]).await.unwrap();
    store.write_block(3, 0, &content[24..]).await.unwrap();
    let mut bf = PieceBitfield::new(4);
    bf.set(0);
    bf.set(1);
    let resume = build_resume_with_wanted(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        bf,
        meta.piece_count(),
        content.len() as u64,
        0,
        meta.total_length,
        Some(dir.display().to_string()),
        1,
        None,
        &[crate::models::torrent::FilePriority::Normal],
        &[false],
        &[8u64; 4],
    );
    store.save_resume(&resume).await.unwrap();
    let loaded = store
        .load_resume(&store.torrent_key())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.key, store.torrent_key());
    assert_eq!(loaded.piece_count, meta.piece_count());
    assert!(loaded.piece_bitfield.has(0));
    assert!(loaded.piece_bitfield.has(1));
    assert_eq!(loaded.wanted, vec![false]);
    assert!(!std::fs::read_dir(&dir).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp-")
    }));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn corrupt_resume_is_quarantined_for_safe_recheck() {
    let bytes = build_single_file_torrent("corrupt.bin", b"01234567", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("resume-corrupt");
    let store = StorageIo::new(meta.clone(), dir.clone());
    fs::write(store.resume_path(), b"{not valid json")
        .await
        .unwrap();

    assert!(store
        .load_resume(&store.torrent_key())
        .await
        .unwrap()
        .is_none());
    assert!(!store.resume_path().exists());
    let prefix = format!("{}.swarmotter.resume.corrupt-", store.torrent_key());
    assert!(std::fs::read_dir(&dir).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(&prefix)
    }));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn resume_rejects_mismatched_torrent_key_on_save() {
    let bytes = build_single_file_torrent("m.bin", b"01234567", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("resume-mismatch");
    let store = StorageIo::new(meta.clone(), dir.clone());
    let other = InfoHash::from_bytes([0u8; 20]);
    let resume = build_resume(
        TorrentKey::v1(other),
        "m.bin".into(),
        PieceBitfield::new(1),
        1,
        0,
        0,
        8,
        None,
        1,
        None,
        &[],
        &[8u64],
    );
    let err = store.save_resume(&resume).await;
    assert!(err.is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn recheck_marks_verified_pieces() {
    let content = b"0123456789abcdef0123456789abcdef";
    let bytes = build_single_file_torrent("rc.bin", content, 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("recheck");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, &content[..8]).await.unwrap();
    store.write_block(1, 0, &content[8..16]).await.unwrap();
    let bf = store.recheck().await.unwrap();
    assert!(bf.has(0));
    assert!(bf.has(1));
    assert!(!bf.has(2));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn missing_file_treated_as_not_verified() {
    let bytes = build_single_file_torrent("miss.bin", b"0123456789abcdef", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("missing");
    let store = StorageIo::new(meta.clone(), dir.clone());
    // Do NOT preallocate: file is absent.
    assert!(!store.verify_piece_on_disk(0).await.unwrap());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn read_block_for_seeding() {
    let content = b"0123456789abcdef";
    let bytes = build_single_file_torrent("seed.bin", content, 16, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("seed");
    let store = StorageIo::new(meta.clone(), dir.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, content).await.unwrap();
    let block = store.read_block(0, 4, 8).await.unwrap();
    assert_eq!(block, b"456789ab");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn block_io_rejects_ranges_outside_the_piece() {
    let content = b"0123456789abcdeflast";
    let bytes = build_single_file_torrent("range.bin", content, 16, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let dir = unique_dir("block-range");
    let store = StorageIo::new(meta, dir.clone());
    store.preallocate().await.unwrap();

    assert!(store.write_block(0, 15, b"xx").await.is_err());
    assert!(store.write_block(1, 4, b"x").await.is_err());
    assert!(store.write_block(2, 0, b"x").await.is_err());
    assert!(store.read_block(0, 15, 2).await.is_err());
    assert!(store.read_block(1, 4, 1).await.is_err());
    assert!(store.checked_piece_range(0, 1, usize::MAX).is_err());
    assert_eq!(store.read_block(1, 4, 0).await.unwrap(), Vec::<u8>::new());
    assert_eq!(store.read_piece(0).await.unwrap(), vec![0u8; 16]);
    assert_eq!(store.read_piece(1).await.unwrap(), vec![0u8; 4]);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn move_to_moves_data_and_removes_active_resume() {
    let content = b"0123456789abcdef";
    let bytes = build_single_file_torrent("move.bin", content, 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let active = unique_dir("move-active");
    let complete = unique_dir("move-complete");
    let store = StorageIo::new(meta.clone(), active.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, &content[..8]).await.unwrap();
    store.write_block(1, 0, &content[8..]).await.unwrap();
    let resume = build_resume(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        PieceBitfield::new(meta.piece_count()),
        meta.piece_count(),
        content.len() as u64,
        0,
        meta.total_length,
        Some(active.display().to_string()),
        1,
        None,
        &[crate::models::torrent::FilePriority::Normal],
        &[8u64; 2],
    );
    store.save_resume(&resume).await.unwrap();

    let complete_store = store.move_to(complete.clone()).await.unwrap();

    assert!(!store.file_path(0).unwrap().exists());
    assert!(!store.resume_path().exists());
    assert_eq!(
        std::fs::read(complete_store.file_path(0).unwrap()).unwrap(),
        content
    );
    assert!(!complete_store.resume_path().exists());
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[tokio::test]
async fn move_to_preserves_multi_file_layout() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let active = unique_dir("move-multi-active");
    let complete = unique_dir("move-multi-complete");
    let store = StorageIo::new(meta.clone(), active.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, b"hell").await.unwrap();
    store.write_block(1, 0, b"owor").await.unwrap();
    store.write_block(2, 0, b"ld!!").await.unwrap();

    let complete_store = store.move_to(complete.clone()).await.unwrap();

    assert!(!active.join("dir").join("a.txt").exists());
    assert_eq!(
        std::fs::read(complete_store.file_path(0).unwrap()).unwrap(),
        b"hello"
    );
    assert_eq!(
        std::fs::read(complete_store.file_path(1).unwrap()).unwrap(),
        b"world!!"
    );
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[tokio::test]
async fn move_to_preserves_short_sparse_files_and_absent_files() {
    let files = vec![
        (vec!["partial.bin".into()], 8u64),
        (vec!["sub".into(), "unwanted.bin".into()], 8u64),
    ];
    let contents: Vec<&[u8]> = vec![b"partial!", b"unwanted"];
    let bytes = build_multi_file_torrent("sparse", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let active = unique_dir("move-sparse-active");
    let complete = unique_dir("move-sparse-complete");
    let store = StorageIo::new(meta.clone(), active.clone());
    let partial_path = store.file_path(0).unwrap();
    fs::create_dir_all(partial_path.parent().unwrap())
        .await
        .unwrap();
    fs::write(&partial_path, b"par").await.unwrap();
    assert!(!store.file_path(1).unwrap().exists());

    let moved = store.move_to(complete.clone()).await.unwrap();

    assert!(!partial_path.exists());
    assert_eq!(std::fs::read(moved.file_path(0).unwrap()).unwrap(), b"par");
    assert_eq!(
        std::fs::metadata(moved.file_path(0).unwrap())
            .unwrap()
            .len(),
        3
    );
    assert!(!moved.file_path(1).unwrap().exists());
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[tokio::test]
async fn move_to_allows_an_entirely_absent_payload_without_claiming_destination_files() {
    let bytes = build_single_file_torrent("not-started.bin", b"not started", 4, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let active = unique_dir("move-absent-active");
    let complete = unique_dir("move-absent-complete");
    let store = StorageIo::new(meta, active.clone());

    let moved = store.move_to(complete.clone()).await.unwrap();

    assert!(!store.file_path(0).unwrap().exists());
    assert!(!moved.file_path(0).unwrap().exists());
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[tokio::test]
async fn move_to_rejects_destination_collision_for_an_absent_source() {
    let bytes = build_single_file_torrent("absent.bin", b"expected bytes", 4, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let active = unique_dir("move-absent-collision-active");
    let complete = unique_dir("move-absent-collision-complete");
    let store = StorageIo::new(meta.clone(), active.clone());
    let destination = StorageIo::new(meta, complete.clone());
    fs::write(destination.file_path(0).unwrap(), b"existing")
        .await
        .unwrap();

    assert!(store.move_to(complete.clone()).await.is_err());
    assert_eq!(
        std::fs::read(destination.file_path(0).unwrap()).unwrap(),
        b"existing"
    );
    assert!(!store.file_path(0).unwrap().exists());
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[tokio::test]
async fn move_to_preflights_every_destination_before_mutating_sources() {
    let files = vec![
        (vec!["a.txt".into()], 5u64),
        (vec!["sub".into(), "b.bin".into()], 7u64),
    ];
    let contents: Vec<&[u8]> = vec![b"hello", b"world!!"];
    let bytes = build_multi_file_torrent("dir", &files, &contents, 4, None);
    let meta = parse_torrent(&bytes).unwrap();
    let active = unique_dir("move-collision-active");
    let complete = unique_dir("move-collision-complete");
    let store = StorageIo::new(meta.clone(), active.clone());
    store.preallocate().await.unwrap();
    store.write_block(0, 0, b"hell").await.unwrap();
    store.write_block(1, 0, b"owor").await.unwrap();
    store.write_block(2, 0, b"ld!!").await.unwrap();
    let resume = build_resume(
        TorrentKey::v1(meta.info_hash),
        meta.name.clone(),
        PieceBitfield::new(meta.piece_count()),
        meta.piece_count(),
        0,
        0,
        meta.total_length,
        Some(active.display().to_string()),
        1,
        None,
        &[crate::models::torrent::FilePriority::Normal; 2],
        &[4u64; 3],
    );
    store.save_resume(&resume).await.unwrap();

    let destination = StorageIo::new(meta, complete.clone());
    let collision = destination.file_path(1).unwrap();
    fs::create_dir_all(collision.parent().unwrap())
        .await
        .unwrap();
    fs::write(&collision, b"occupied").await.unwrap();

    assert!(store.move_to(complete.clone()).await.is_err());
    assert_eq!(
        std::fs::read(store.file_path(0).unwrap()).unwrap(),
        b"hello"
    );
    assert_eq!(
        std::fs::read(store.file_path(1).unwrap()).unwrap(),
        b"world!!"
    );
    assert!(!destination.file_path(0).unwrap().exists());
    assert_eq!(std::fs::read(collision).unwrap(), b"occupied");
    assert!(store.resume_path().exists());
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[tokio::test]
async fn move_plan_rolls_back_completed_entries_after_later_failure() {
    let active = unique_dir("move-rollback-active");
    let complete = unique_dir("move-rollback-complete");
    let source_one = active.join("one.bin");
    let source_two = active.join("two.bin");
    let destination_one = complete.join("one.bin");
    let blocker = complete.join("blocker");
    let destination_two = blocker.join("two.bin");
    fs::write(&source_one, b"one").await.unwrap();
    fs::write(&source_two, b"two").await.unwrap();
    fs::write(&blocker, b"not a directory").await.unwrap();
    let plan = vec![
        MovePlanEntry {
            source: source_one.clone(),
            destination: destination_one.clone(),
            kind: MoveEntryKind::Existing,
        },
        MovePlanEntry {
            source: source_two.clone(),
            destination: destination_two.clone(),
            kind: MoveEntryKind::Existing,
        },
    ];

    assert!(execute_move_plan(&plan, &complete).await.is_err());
    assert_eq!(std::fs::read(source_one).unwrap(), b"one");
    assert_eq!(std::fs::read(source_two).unwrap(), b"two");
    assert!(!destination_one.exists());
    assert_eq!(std::fs::read(blocker).unwrap(), b"not a directory");
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[tokio::test]
async fn move_plan_rolls_back_when_an_absent_source_appears() {
    let active = unique_dir("move-absent-race-active");
    let complete = unique_dir("move-absent-race-complete");
    let source_one = active.join("one.bin");
    let appeared_source = active.join("appeared.bin");
    let destination_one = complete.join("one.bin");
    let absent_destination = complete.join("appeared.bin");
    fs::write(&source_one, b"one").await.unwrap();
    fs::write(&appeared_source, b"appeared after preflight")
        .await
        .unwrap();
    let plan = vec![
        MovePlanEntry {
            source: source_one.clone(),
            destination: destination_one.clone(),
            kind: MoveEntryKind::Existing,
        },
        MovePlanEntry {
            source: appeared_source.clone(),
            destination: absent_destination.clone(),
            kind: MoveEntryKind::Absent,
        },
    ];

    assert!(execute_move_plan(&plan, &complete).await.is_err());
    assert_eq!(std::fs::read(source_one).unwrap(), b"one");
    assert_eq!(
        std::fs::read(appeared_source).unwrap(),
        b"appeared after preflight"
    );
    assert!(!destination_one.exists());
    assert!(!absent_destination.exists());
    std::fs::remove_dir_all(&active).ok();
    std::fs::remove_dir_all(&complete).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn move_parent_sync_errors_are_returned() {
    let root = unique_dir("move-sync-error");
    let entry = MovePlanEntry {
        source: root.join("missing-parent").join("source.bin"),
        destination: root.join("destination.bin"),
        kind: MoveEntryKind::Existing,
    };

    assert!(sync_move_entry_parents(&entry).await.is_err());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn path_ownership_detects_collisions_and_normalizes_roots() {
    let bytes = build_single_file_torrent("same.bin", b"01234567", 8, None, false);
    let meta = parse_torrent(&bytes).unwrap();
    let root = unique_dir("path-ownership");
    let first = StorageIo::new(meta.clone(), root.clone());
    let mut other_meta = meta.clone();
    let other_hash = InfoHash::from_bytes([9u8; 20]);
    other_meta.info_hash = other_hash;
    other_meta.identity = crate::hash::TorrentIdentity::v1(other_hash);
    let second = StorageIo::new(other_meta, root.join("child").join(".."));

    assert!(first.shares_storage_root_with(&second));
    let first_ownership = first.path_ownership().unwrap();
    let second_ownership = second.path_ownership().unwrap();
    assert!(first_ownership.conflicts_with(&second_ownership));
    assert!(first_ownership
        .ensure_compatible_with(&second_ownership)
        .is_err());

    let elsewhere = StorageIo::new(meta, root.join("elsewhere"));
    assert!(!first_ownership.conflicts_with(&elsewhere.path_ownership().unwrap()));
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn path_ownership_rejects_file_directory_prefix_collisions() {
    assert!(paths_overlap(
        Path::new("/data/file"),
        Path::new("/data/file/child")
    ));
    let meta = TorrentMeta {
        info_hash: InfoHash::from_bytes([8u8; 20]),
        identity: crate::hash::TorrentIdentity::v1(InfoHash::from_bytes([8u8; 20])),
        name: "root".into(),
        piece_length: 16,
        pieces: vec![[0u8; 20]],
        files: vec![
            MetaFile {
                path: vec!["root".into(), "file".into()],
                length: 1,
                pieces_root: None,
            },
            MetaFile {
                path: vec!["root".into(), "file".into(), "child".into()],
                length: 1,
                pieces_root: None,
            },
        ],
        total_length: 2,
        private: false,
        announce: None,
        announce_list: vec![],
        webseeds: vec![],
        comment: None,
        created_by: None,
        creation_date: None,
        is_multi_file: true,
        v2: None,
        raw_info: None,
    };
    let store = StorageIo::new(meta, std::env::temp_dir());
    assert!(store.path_ownership().is_err());
}

#[test]
fn path_ownership_rejects_payload_collision_with_its_resume_file() {
    let bytes = build_single_file_torrent("payload.bin", b"01234567", 8, None, false);
    let mut meta = parse_torrent(&bytes).unwrap();
    meta.files[0].path = vec![format!(
        "{}.swarmotter.resume",
        meta.identity.primary_key().unwrap()
    )];
    let store = StorageIo::new(meta, std::env::temp_dir());

    let error = store.path_ownership().unwrap_err();

    assert!(error
        .to_string()
        .contains("payload path collides with fast-resume path"));
}

#[test]
fn storage_rejects_unsafe_path_components() {
    let meta = TorrentMeta {
        info_hash: InfoHash::from_bytes([1u8; 20]),
        identity: crate::hash::TorrentIdentity::v1(InfoHash::from_bytes([1u8; 20])),
        name: "safe-name".into(),
        piece_length: 16,
        pieces: vec![[0u8; 20]],
        files: vec![MetaFile {
            path: vec!["safe-name".into(), "../traversal".into()],
            length: 1,
            pieces_root: None,
        }],
        total_length: 1,
        private: false,
        announce: None,
        announce_list: vec![],
        webseeds: vec![],
        comment: None,
        created_by: None,
        creation_date: None,
        is_multi_file: true,
        v2: None,
        raw_info: None,
    };
    let store = StorageIo::new(meta, std::env::temp_dir());
    assert!(store.file_path(0).is_err());
}

#[test]
fn storage_rejects_empty_path_components() {
    let meta = TorrentMeta {
        info_hash: InfoHash::from_bytes([2u8; 20]),
        identity: crate::hash::TorrentIdentity::v1(InfoHash::from_bytes([2u8; 20])),
        name: "safe".into(),
        piece_length: 16,
        pieces: vec![[0u8; 20]],
        files: vec![MetaFile {
            path: vec!["safe".into(), "".into()],
            length: 1,
            pieces_root: None,
        }],
        total_length: 1,
        private: false,
        announce: None,
        announce_list: vec![],
        webseeds: vec![],
        comment: None,
        created_by: None,
        creation_date: None,
        is_multi_file: true,
        v2: None,
        raw_info: None,
    };
    let store = StorageIo::new(meta, std::env::temp_dir());
    assert!(store.file_path(0).is_err());
}
