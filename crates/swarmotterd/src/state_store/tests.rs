// SPDX-License-Identifier: Apache-2.0

use super::*;
use swarmotter_core::meta::{build_single_file_torrent, parse_torrent};
use swarmotter_core::models::torrent::SeedingStatus;
use swarmotter_core::queue::QueueLimits;
use swarmotter_core::ratio::TorrentSeeding;

fn unique_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "swarmotter-{label}-{}-{}.sqlite",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn remove_state(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(sqlite_sidecar_path(path, "-wal"));
    let _ = fs::remove_file(sqlite_sidecar_path(path, "-shm"));
}

fn single_torrent_state() -> DaemonState {
    let bytes = build_single_file_torrent(
        "state.bin",
        b"generated lawful state payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(parse_torrent(&bytes).unwrap(), 1);
    DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default()))
}

/// A self-contained valid pure-BEP-52 document. The single file is no
/// larger than its piece length, so the top-level piece-layers dictionary
/// is intentionally empty.
fn pure_v2_single_file_torrent() -> Vec<u8> {
    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d4:infod9:file treed10:lawful.bind0:d6:lengthi1e11:pieces root32:");
    torrent.extend_from_slice(&[0x42; 32]);
    torrent.extend_from_slice(
        b"eee12:meta versioni2e4:name10:lawful.bin12:piece lengthi16384ee12:piece layersdee",
    );
    torrent
}

#[test]
fn state_write_uses_sqlite_and_round_trips() {
    let path = unique_path("daemon-state");
    let state = single_torrent_state();
    save(&path, &state).unwrap();
    assert!(fs::read(&path).unwrap().starts_with(SQLITE_HEADER));
    let loaded = load(&path).unwrap().unwrap();
    assert_eq!(loaded.torrents.len(), 1);
    assert_eq!(
        loaded.torrents[0].info_hash(),
        state.torrents[0].info_hash()
    );
    assert!(loaded.queue.order.is_empty());
    remove_state(&path);
}

#[test]
fn legacy_json_state_migrates_atomically_on_save() {
    let path = unique_path("legacy-migration");
    let mut state = single_torrent_state();
    let key = state.torrents[0].key();
    // Legacy daemon JSON contains 40-character v1 locators. The
    // TorrentKey deserializer must keep accepting those rows unchanged.
    state.queue.add(key);
    fs::write(&path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
    let restored = load(&path).unwrap().unwrap();
    assert_eq!(
        restored.torrents[0].info_hash(),
        state.torrents[0].info_hash()
    );
    assert_eq!(restored.queue.order, vec![key]);
    save(&path, &restored).unwrap();
    assert!(fs::read(&path).unwrap().starts_with(SQLITE_HEADER));
    let migrated = load(&path).unwrap().unwrap();
    assert_eq!(
        migrated.torrents[0].info_hash(),
        state.torrents[0].info_hash()
    );
    assert_eq!(migrated.queue.order, vec![key]);
    remove_state(&path);
}

#[test]
fn failed_legacy_json_migration_preserves_the_original_generation() {
    let path = unique_path("legacy-migration-failure");
    let legacy = single_torrent_state();
    let legacy_key = legacy.torrents[0].key();
    let legacy_bytes = serde_json::to_vec_pretty(&legacy).unwrap();
    fs::write(&path, &legacy_bytes).unwrap();

    let mut invalid = legacy;
    invalid.torrents[0].meta.info_hash = InfoHash::ZERO;
    invalid.torrents[0].meta.identity = swarmotter_core::hash::TorrentIdentity::Unknown;
    let error = save(&path, &invalid).unwrap_err().to_string();
    assert!(error.contains("all-zero v1 identity"), "{error}");
    assert_eq!(fs::read(&path).unwrap(), legacy_bytes);
    assert_eq!(load(&path).unwrap().unwrap().torrents[0].key(), legacy_key);
    remove_state(&path);
}

#[test]
fn legacy_v1_sqlite_rows_and_queue_locators_load_as_torrent_keys() {
    let path = unique_path("legacy-v1-key-locators");
    let mut state = single_torrent_state();
    let key = state.torrents[0].key();
    let key_locator = key.to_locator();
    assert_eq!(key_locator.len(), 40);
    state.queue.add(key);
    save(&path, &state).unwrap();

    // Emulate a pre-identity SQLite record. The physical `info_hash`
    // column and queue JSON are unchanged legacy 40-character strings;
    // only the modern in-record identity annotation is absent.
    let connection = Connection::open(&path).unwrap();
    let stored_record_key: String = connection
        .query_row("SELECT info_hash FROM torrent_records", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(stored_record_key, key_locator);
    let queue_json: Vec<u8> = connection
        .query_row("SELECT queue_json FROM queue_state", [], |row| row.get(0))
        .unwrap();
    assert!(std::str::from_utf8(&queue_json)
        .unwrap()
        .contains(&key_locator));
    let torrent_json: Vec<u8> = connection
        .query_row("SELECT torrent_json FROM torrent_records", [], |row| {
            row.get(0)
        })
        .unwrap();
    let mut legacy_torrent: serde_json::Value = serde_json::from_slice(&torrent_json).unwrap();
    legacy_torrent["meta"]
        .as_object_mut()
        .unwrap()
        .remove("identity");
    connection
        .execute(
            "UPDATE torrent_records SET torrent_json = ?2 WHERE info_hash = ?1",
            params![&key_locator, serde_json::to_vec(&legacy_torrent).unwrap()],
        )
        .unwrap();
    drop(connection);

    let restored = load(&path).unwrap().unwrap();
    assert_eq!(restored.torrents[0].key(), key);
    assert_eq!(restored.queue.order, vec![key]);
    remove_state(&path);
}

#[test]
fn fresh_sqlite_file_receives_versioned_schema_migration() {
    let path = unique_path("schema-migration");
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("PRAGMA user_version = 0").unwrap();
    drop(connection);

    let state = single_torrent_state();
    save(&path, &state).unwrap();

    let connection = Connection::open(&path).unwrap();
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let recorded: u32 = connection
        .query_row(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let migrations: Vec<u32> = connection
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    let metric_history_exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'torrent_metric_samples'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    assert_eq!(version, SQLITE_SCHEMA_VERSION);
    assert_eq!(recorded, SQLITE_SCHEMA_VERSION);
    assert_eq!(migrations, vec![SQLITE_SCHEMA_V1, SQLITE_SCHEMA_VERSION]);
    assert_eq!(metric_history_exists, Some(1));
    drop(connection);
    remove_state(&path);
}

#[test]
fn sqlite_v1_state_migrates_to_metric_history_schema_without_losing_state() {
    let path = unique_path("sqlite-v1-metric-history-migration");
    let state = single_torrent_state();
    let expected_hash = state.torrents[0].info_hash();
    save(&path, &state).unwrap();

    // Recreate the durable shape emitted by schema version one: preserve
    // all v1 rows, remove only the version-two metric-history table, and
    // roll the ledger/version back together.
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP TABLE torrent_metric_samples;
             DELETE FROM schema_migrations WHERE version = 2;
             PRAGMA user_version = 1;",
        )
        .unwrap();
    drop(connection);

    let restored = load(&path).unwrap().unwrap();
    assert_eq!(restored.torrents.len(), 1);
    assert_eq!(restored.torrents[0].info_hash(), expected_hash);

    let connection = Connection::open(&path).unwrap();
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let migrations: Vec<u32> = connection
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    let metric_history_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM torrent_metric_samples", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, SQLITE_SCHEMA_VERSION);
    assert_eq!(migrations, vec![SQLITE_SCHEMA_V1, SQLITE_SCHEMA_VERSION]);
    // A schema migration preserves v1 facts; it does not invent a sample
    // for a point in time that was never captured.
    assert_eq!(metric_history_rows, 0);
    drop(connection);

    save(&path, &restored).unwrap();
    let connection = Connection::open(&path).unwrap();
    let metric_history_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM torrent_metric_samples", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(metric_history_rows, 1);
    drop(connection);
    remove_state(&path);
}

#[test]
fn state_writes_record_changed_metrics_and_foundational_audit_events() {
    let path = unique_path("metric-and-audit-recording");
    let mut state = single_torrent_state();
    let hash = state.torrents[0].key().to_locator();
    save(&path, &state).unwrap();

    state.torrents[0].downloaded = 17;
    state.torrents[0].uploaded = 9;
    state.torrents[0].rate_down = 3;
    state.torrents[0].rate_up = 2;
    state.torrents[0].state = swarmotter_core::models::torrent::TorrentState::Paused;
    save(&path, &state).unwrap();
    // Identical metrics are not duplicated just because another durable
    // state generation is committed.
    save(&path, &state).unwrap();

    let connection = Connection::open(&path).unwrap();
    let metric_samples: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM torrent_metric_samples WHERE info_hash = ?1",
            params![&hash],
            |row| row.get(0),
        )
        .unwrap();
    let history: Vec<(String, Option<String>, String)> = connection
        .prepare(
            "SELECT event_kind, previous_state, current_state
             FROM library_history WHERE info_hash = ?1 ORDER BY id",
        )
        .unwrap()
        .query_map(params![&hash], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    let audit_actions: Vec<String> = connection
        .prepare("SELECT action FROM audit_events ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(metric_samples, 2);
    assert_eq!(
        history,
        vec![
            ("registered".into(), None, "queued".into()),
            (
                "state_changed".into(),
                Some("queued".into()),
                "paused".into()
            ),
        ]
    );
    assert_eq!(
        audit_actions,
        vec![
            "torrent_registered".to_string(),
            "torrent_state_changed".to_string(),
        ]
    );
    drop(connection);

    save(
        &path,
        &DaemonState::new(Vec::new(), QueueState::new(QueueLimits::default())),
    )
    .unwrap();
    let connection = Connection::open(&path).unwrap();
    let removals: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM audit_events WHERE action = 'torrents_removed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(removals, 1);
    drop(connection);
    remove_state(&path);
}

#[test]
fn durable_history_metric_and_audit_retention_keeps_newest_rows() {
    let path = unique_path("durable-retention");
    let state = single_torrent_state();
    let hash = state.torrents[0].key().to_locator();
    save(&path, &state).unwrap();

    let mut connection = open_sqlite(&path).unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    transaction
        .execute("DELETE FROM library_history", [])
        .unwrap();
    transaction.execute("DELETE FROM audit_events", []).unwrap();
    transaction
        .execute("DELETE FROM torrent_metric_samples", [])
        .unwrap();
    for value in 0_i64..5 {
        transaction
            .execute(
                "INSERT INTO library_history(
                    info_hash, occurred_at, event_kind, previous_state, current_state
                 ) VALUES (?1, ?2, 'state_changed', 'queued', 'paused')",
                params![&hash, value],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO audit_events(occurred_at, actor, action, detail_json)
                 VALUES (?1, 'test', 'retention_test', ?2)",
                params![value, b"{}"],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO torrent_metric_samples(
                    info_hash, observed_at, downloaded, uploaded, rate_down, rate_up
                 ) VALUES (?1, ?2, ?3, '0', '0', '0')",
                params![&hash, value, value.to_string()],
            )
            .unwrap();
    }
    prune_retained_rows(
        &transaction,
        RetentionLimits {
            library_history_rows: 2,
            audit_event_rows: 3,
            metric_samples_per_torrent: 3,
            metric_sample_rows: 2,
        },
    )
    .unwrap();
    transaction.commit().unwrap();
    checkpoint_and_sync(connection, &path).unwrap();

    let connection = Connection::open(&path).unwrap();
    let history_timestamps: Vec<i64> = connection
        .prepare("SELECT occurred_at FROM library_history ORDER BY occurred_at")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    let audit_timestamps: Vec<i64> = connection
        .prepare("SELECT occurred_at FROM audit_events ORDER BY occurred_at")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    let metric_timestamps: Vec<i64> = connection
        .prepare("SELECT observed_at FROM torrent_metric_samples ORDER BY observed_at")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(history_timestamps, vec![3, 4]);
    assert_eq!(audit_timestamps, vec![2, 3, 4]);
    // The per-torrent cap leaves three newest rows, then the global cap
    // deterministically narrows them to the two newest rows.
    assert_eq!(metric_timestamps, vec![3, 4]);
    drop(connection);
    remove_state(&path);
}

#[test]
fn state_file_snapshot_restores_exact_prior_sqlite_generation() {
    let path = unique_path("snapshot");
    let prior = single_torrent_state();
    save(&path, &prior).unwrap();
    let snapshot = capture_file(&path).unwrap();
    let prior_bytes = match &snapshot {
        StateFileSnapshot::Bytes(bytes) => bytes.clone(),
        StateFileSnapshot::Missing => panic!("saved SQLite state must be present"),
    };

    let mut changed_queue = QueueState::new(QueueLimits::default());
    changed_queue.add(TorrentKey::v1(swarmotter_core::hash::InfoHash::from_bytes(
        [7; 20],
    )));
    save(
        &path,
        &DaemonState::new(prior.torrents.clone(), changed_queue),
    )
    .unwrap();
    assert_ne!(fs::read(&path).unwrap(), prior_bytes);

    restore_file(&path, &snapshot).unwrap();
    assert_eq!(fs::read(&path).unwrap(), prior_bytes);
    let restored = load(&path).unwrap().unwrap();
    assert_eq!(
        restored.torrents[0].info_hash(),
        prior.torrents[0].info_hash()
    );
    remove_state(&path);
}

#[test]
fn raw_metainfo_blobs_hydrate_losslessly() {
    let path = unique_path("raw-metainfo");
    let bytes = build_single_file_torrent(
        "raw-state.bin",
        b"generated lawful raw metadata payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(parse_torrent(&bytes).unwrap(), 1);
    let expected_info = torrent.meta.raw_info.clone();
    let key = torrent.key();
    let state = DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default()));
    save_with_original_metainfo(
        &path,
        &state,
        Some(OriginalMetainfo::new(key, bytes.clone())),
    )
    .unwrap();
    eprintln!(
        "after save wal={} shm={}",
        sqlite_sidecar_path(&path, "-wal").exists(),
        sqlite_sidecar_path(&path, "-shm").exists()
    );

    // Keep this inspection read-only. Opening a writable connection to a
    // WAL-mode database can itself recreate `-wal`/`-shm` sidecars, which
    // would make the assertion below test the fixture rather than the
    // original-metainfo lookup.
    let connection = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let original: Vec<u8> = connection
        .query_row(
            "SELECT metainfo FROM torrent_metainfo WHERE representation = 'original_torrent'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(original, bytes);
    drop(connection);
    eprintln!(
        "after inspect wal={} shm={}",
        sqlite_sidecar_path(&path, "-wal").exists(),
        sqlite_sidecar_path(&path, "-shm").exists()
    );

    let state_before_lookup = fs::read(&path).unwrap();
    assert_eq!(
        load_original_metainfo(&path, key).unwrap(),
        Some(bytes.clone())
    );
    eprintln!(
        "after lookup wal={} shm={}",
        sqlite_sidecar_path(&path, "-wal").exists(),
        sqlite_sidecar_path(&path, "-shm").exists()
    );
    assert_eq!(fs::read(&path).unwrap(), state_before_lookup);
    assert!(!sqlite_sidecar_path(&path, "-wal").exists());
    assert!(!sqlite_sidecar_path(&path, "-shm").exists());

    let loaded = load(&path).unwrap().unwrap();
    assert_eq!(loaded.torrents[0].meta.raw_info, expected_info);
    remove_state(&path);
}

#[test]
fn original_metainfo_lookup_never_substitutes_canonical_info() {
    let path = unique_path("canonical-not-original");
    let bytes = build_single_file_torrent(
        "canonical-only.bin",
        b"generated canonical metadata fixture",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(parse_torrent(&bytes).unwrap(), 1);
    let key = torrent.key();
    save(
        &path,
        &DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default())),
    )
    .unwrap();

    assert_eq!(load_original_metainfo(&path, key).unwrap(), None);
    remove_state(&path);
}

#[test]
fn retained_original_metainfo_must_match_the_registered_primary_key() {
    let path = unique_path("original-metainfo-key-mismatch");
    let accepted = build_single_file_torrent(
        "accepted.bin",
        b"generated accepted metadata payload",
        8,
        None,
        false,
    );
    let unrelated = build_single_file_torrent(
        "unrelated.bin",
        b"generated unrelated metadata payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(parse_torrent(&accepted).unwrap(), 1);
    let key = torrent.key();
    let state = DaemonState::new(vec![torrent], QueueState::new(QueueLimits::default()));

    let error =
        save_with_original_metainfo(&path, &state, Some(OriginalMetainfo::new(key, unrelated)))
            .unwrap_err()
            .to_string();
    assert!(error.contains("identity does not match"), "{error}");
    assert!(!path.exists());
    remove_state(&path);
}

#[test]
fn projection_rebuild_restores_derived_rows_and_preserves_authoritative_data() {
    let path = unique_path("projection-rebuild");
    let original_torrent = build_single_file_torrent(
        "projection-rebuild.bin",
        b"generated lawful projection rebuild payload",
        8,
        None,
        false,
    );
    let torrent = Torrent::new(parse_torrent(&original_torrent).unwrap(), 42);
    let key = torrent.key();
    let expected_name = torrent.meta.name.clone();
    let expected_state = torrent.state.as_str().to_string();
    let expected_date_added = torrent.date_added.to_string();
    let expected_total_length = torrent.meta.total_length.to_string();
    let mut queue = QueueState::new(QueueLimits::default());
    queue.add(key);
    queue.start_now(&key);
    let state = DaemonState::new(vec![torrent], queue);
    save_with_original_metainfo(
        &path,
        &state,
        Some(OriginalMetainfo::new(key, original_torrent)),
    )
    .unwrap();

    let connection = Connection::open(&path).unwrap();
    let canonical_before: Vec<u8> = connection
        .query_row(
            "SELECT metainfo FROM torrent_metainfo
             WHERE info_hash = ?1 AND representation = 'canonical_info'",
            params![key.to_locator()],
            |row| row.get(0),
        )
        .unwrap();
    let original_before: Vec<u8> = connection
        .query_row(
            "SELECT metainfo FROM torrent_metainfo
             WHERE info_hash = ?1 AND representation = 'original_torrent'",
            params![key.to_locator()],
            |row| row.get(0),
        )
        .unwrap();
    let queue_json_before: Vec<u8> = connection
        .query_row(
            "SELECT queue_json FROM queue_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let history_before: i64 = connection
        .query_row("SELECT COUNT(*) FROM library_history", [], |row| row.get(0))
        .unwrap();
    let metric_history_before: i64 = connection
        .query_row("SELECT COUNT(*) FROM torrent_metric_samples", [], |row| {
            row.get(0)
        })
        .unwrap();
    connection
        .execute(
            "INSERT INTO audit_events(occurred_at, actor, action, detail_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![123_i64, "operator", "projection_rebuild_test", b"{}"],
        )
        .unwrap();
    let audit_before: Vec<u8> = connection
        .query_row(
            "SELECT detail_json FROM audit_events WHERE action = 'projection_rebuild_test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute_batch(
            "DROP INDEX torrent_records_lifecycle_state;
             DROP INDEX queue_entries_hash;
             UPDATE torrent_records
             SET name = 'stale name',
                 lifecycle_state = 'stale state',
                 date_added = '0',
                 total_length = '0';
             DELETE FROM queue_entries;
             DELETE FROM torrent_health_snapshots;
             DELETE FROM torrent_metrics_current;",
        )
        .unwrap();
    drop(connection);

    let report = rebuild_projections(&path).unwrap();
    assert_eq!(
        report,
        ProjectionRebuildReport {
            torrents: 1,
            queue_entries: 2,
        }
    );

    let connection = Connection::open(&path).unwrap();
    let rebuilt_projection: (String, String, String, String) = connection
        .query_row(
            "SELECT name, lifecycle_state, date_added, total_length
             FROM torrent_records WHERE info_hash = ?1",
            params![key.to_locator()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        rebuilt_projection,
        (
            expected_name,
            expected_state,
            expected_date_added,
            expected_total_length,
        )
    );
    let rebuilt_queue_entries: i64 = connection
        .query_row("SELECT COUNT(*) FROM queue_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rebuilt_queue_entries, 2);
    let rebuilt_health_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM torrent_health_snapshots", [], |row| {
            row.get(0)
        })
        .unwrap();
    let rebuilt_metric_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM torrent_metrics_current", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(rebuilt_health_rows, 1);
    assert_eq!(rebuilt_metric_rows, 1);
    for index in [
        "torrent_records_lifecycle_state",
        "torrent_records_date_added",
        "queue_entries_hash",
        "library_history_torrent_time",
        "torrent_metric_samples_torrent_time",
    ] {
        let exists: Option<i64> = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                params![index],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(exists, Some(1), "index {index} must be restored");
    }
    let canonical_after: Vec<u8> = connection
        .query_row(
            "SELECT metainfo FROM torrent_metainfo
             WHERE info_hash = ?1 AND representation = 'canonical_info'",
            params![key.to_locator()],
            |row| row.get(0),
        )
        .unwrap();
    let original_after: Vec<u8> = connection
        .query_row(
            "SELECT metainfo FROM torrent_metainfo
             WHERE info_hash = ?1 AND representation = 'original_torrent'",
            params![key.to_locator()],
            |row| row.get(0),
        )
        .unwrap();
    let queue_json_after: Vec<u8> = connection
        .query_row(
            "SELECT queue_json FROM queue_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let history_after: i64 = connection
        .query_row("SELECT COUNT(*) FROM library_history", [], |row| row.get(0))
        .unwrap();
    let metric_history_after: i64 = connection
        .query_row("SELECT COUNT(*) FROM torrent_metric_samples", [], |row| {
            row.get(0)
        })
        .unwrap();
    let audit_after: Vec<u8> = connection
        .query_row(
            "SELECT detail_json FROM audit_events WHERE action = 'projection_rebuild_test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(canonical_after, canonical_before);
    assert_eq!(original_after, original_before);
    assert_eq!(queue_json_after, queue_json_before);
    assert_eq!(history_after, history_before);
    assert_eq!(metric_history_after, metric_history_before);
    assert_eq!(audit_after, audit_before);
    drop(connection);

    // The normal loader verifies the reconstructed queue index, proving
    // the rebuild made the resulting state usable by daemon startup.
    let loaded = load(&path).unwrap().unwrap();
    assert_eq!(loaded.queue.order, vec![key]);
    assert_eq!(loaded.queue.bypass, vec![key]);
    remove_state(&path);
}

#[test]
fn projection_rebuild_refuses_missing_legacy_and_corrupt_state_without_writing() {
    let missing = unique_path("projection-rebuild-missing");
    let missing_error = rebuild_projections(&missing).unwrap_err().to_string();
    assert!(missing_error.contains("does not exist"), "{missing_error}");
    assert!(!missing.exists());

    let legacy = unique_path("projection-rebuild-legacy");
    let legacy_state = single_torrent_state();
    let legacy_bytes = serde_json::to_vec_pretty(&legacy_state).unwrap();
    fs::write(&legacy, &legacy_bytes).unwrap();
    let legacy_error = rebuild_projections(&legacy).unwrap_err().to_string();
    assert!(legacy_error.contains("legacy JSON"), "{legacy_error}");
    assert_eq!(fs::read(&legacy).unwrap(), legacy_bytes);
    remove_state(&legacy);

    let corrupt = unique_path("projection-rebuild-corrupt");
    let mut corrupt_bytes = SQLITE_HEADER.to_vec();
    corrupt_bytes.extend_from_slice(b"not a complete sqlite database");
    fs::write(&corrupt, &corrupt_bytes).unwrap();
    assert!(rebuild_projections(&corrupt).is_err());
    assert_eq!(fs::read(&corrupt).unwrap(), corrupt_bytes);
    remove_state(&corrupt);

    let v1 = unique_path("projection-rebuild-v1-schema");
    let state = single_torrent_state();
    save(&v1, &state).unwrap();
    let connection = Connection::open(&v1).unwrap();
    connection
        .execute_batch(
            "DROP TABLE torrent_metric_samples;
             DELETE FROM schema_migrations WHERE version = 2;
             PRAGMA user_version = 1;",
        )
        .unwrap();
    drop(connection);
    let v1_bytes = fs::read(&v1).unwrap();
    let v1_error = rebuild_projections(&v1).unwrap_err().to_string();
    assert!(v1_error.contains("schema version 1"), "{v1_error}");
    assert_eq!(fs::read(&v1).unwrap(), v1_bytes);
    remove_state(&v1);
}

#[test]
fn every_seeding_status_round_trips_in_sqlite_state() {
    let path = unique_path("seeding-statuses");
    let statuses = [
        SeedingStatus::NotEligible,
        SeedingStatus::Queued,
        SeedingStatus::Active,
        SeedingStatus::StoppedRatio,
        SeedingStatus::StoppedIdle,
        SeedingStatus::StoppedManual,
    ];
    for status in statuses {
        let mut state = single_torrent_state();
        state.torrents[0].seeding = TorrentSeeding {
            ratio_limit: Some(1.25),
            idle_limit: Some(42),
            seed_forever: false,
        };
        state.torrents[0].seeding_status = status;
        save(&path, &state).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.torrents[0].seeding_status, status);
        assert_eq!(loaded.torrents[0].seeding.ratio_limit, Some(1.25));
    }
    remove_state(&path);
}

#[test]
fn corrupt_state_is_not_silently_discarded() {
    let path = unique_path("corrupt");
    fs::write(&path, b"not json or sqlite").unwrap();
    assert!(load(&path).is_err());
    assert!(save(
        &path,
        &DaemonState::new(Vec::new(), QueueState::new(QueueLimits::default()))
    )
    .is_err());
    remove_state(&path);
}

#[test]
fn corrupt_sqlite_state_is_not_silently_discarded() {
    let path = unique_path("corrupt-sqlite");
    let mut corrupt = SQLITE_HEADER.to_vec();
    corrupt.extend_from_slice(b"not a complete sqlite database");
    fs::write(&path, &corrupt).unwrap();
    assert!(load(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), corrupt);
    assert!(save(
        &path,
        &DaemonState::new(Vec::new(), QueueState::new(QueueLimits::default()))
    )
    .is_err());
    assert_eq!(fs::read(&path).unwrap(), corrupt);
    remove_state(&path);
}

#[test]
fn nonempty_non_state_sqlite_file_is_not_adopted_by_save() {
    let path = unique_path("non-state-sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE unrelated_data (value TEXT NOT NULL) STRICT;
             INSERT INTO unrelated_data(value) VALUES ('preserve me');",
        )
        .unwrap();
    drop(connection);

    let error = save(&path, &single_torrent_state())
        .unwrap_err()
        .to_string();
    assert!(error.contains("non-empty SQLite database"), "{error}");
    let connection = Connection::open(&path).unwrap();
    let preserved: String = connection
        .query_row("SELECT value FROM unrelated_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(preserved, "preserve me");
    drop(connection);
    remove_state(&path);
}

#[test]
fn pure_v2_record_uses_a_full_durable_key_and_retains_original_metainfo() {
    let path = unique_path("pure-v2-key");
    let original = pure_v2_single_file_torrent();
    let torrent = Torrent::new(parse_torrent(&original).unwrap(), 7);
    let key = torrent.key();
    assert!(matches!(key, TorrentKey::V2(_)));
    assert_eq!(key.to_locator().len(), 64);
    // The legacy field is deliberately zero for pure v2; persistence must
    // use `Torrent::key()` instead of allowing that sentinel to collide.
    assert_eq!(torrent.info_hash(), swarmotter_core::hash::InfoHash::ZERO);

    let mut queue = QueueState::new(QueueLimits::default());
    queue.add(key);
    queue.start_now(&key);
    let state = DaemonState::new(vec![torrent], queue);
    save_with_original_metainfo(
        &path,
        &state,
        Some(OriginalMetainfo::new(key, original.clone())),
    )
    .unwrap();

    let connection = Connection::open(&path).unwrap();
    let stored_key: String = connection
        .query_row("SELECT info_hash FROM torrent_records", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(stored_key, key.to_locator());
    assert_eq!(stored_key.len(), 64);
    let queue_key: String = connection
        .query_row("SELECT info_hash FROM queue_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(queue_key, key.to_locator());
    drop(connection);

    assert_eq!(
        rebuild_projections(&path).unwrap(),
        ProjectionRebuildReport {
            torrents: 1,
            queue_entries: 2,
        }
    );
    assert_eq!(load_original_metainfo(&path, key).unwrap(), Some(original));
    let restored = load(&path).unwrap().unwrap();
    assert_eq!(restored.torrents[0].key(), key);
    assert_eq!(restored.queue.order, vec![key]);
    assert_eq!(restored.queue.bypass, vec![key]);
    remove_state(&path);
}

#[test]
fn zero_identity_sentinels_cannot_enter_durable_state() {
    let path = unique_path("zero-durable-identity");
    let mut v1_state = single_torrent_state();
    v1_state.torrents[0].meta.info_hash = InfoHash::ZERO;
    v1_state.torrents[0].meta.identity = swarmotter_core::hash::TorrentIdentity::Unknown;
    let error = save(&path, &v1_state).unwrap_err().to_string();
    assert!(error.contains("all-zero v1 identity"), "{error}");
    assert!(!path.exists());

    let mut v2_state = single_torrent_state();
    v2_state.torrents[0].meta.info_hash = InfoHash::ZERO;
    v2_state.torrents[0].meta.identity =
        swarmotter_core::hash::TorrentIdentity::v2(V2InfoHash::ZERO);
    let error = save(&path, &v2_state).unwrap_err().to_string();
    assert!(error.contains("all-zero v2 identity"), "{error}");
    assert!(!path.exists());
    remove_state(&path);
}

#[test]
fn durable_piece_hash_lengths_are_checked_with_record_and_piece_context() {
    let state = single_torrent_state();
    let expected_hash = state.torrents[0].info_hash().to_hex();
    for decoded_len in [0usize, 19, 20, 21] {
        let path = unique_path(&format!("piece-hash-{decoded_len}"));
        let encoded_payload = "ab".repeat(decoded_len);
        let mut json = serde_json::to_value(&state).unwrap();
        json["torrents"][0]["meta"]["pieces"][1] =
            serde_json::Value::String(encoded_payload.clone());
        fs::write(&path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();

        if decoded_len == 20 {
            let loaded = load(&path).unwrap().unwrap();
            assert_eq!(loaded.torrents.len(), 1);
            assert_eq!(loaded.torrents[0].meta.pieces[1], [0xabu8; 20]);
        } else {
            let error = load(&path).unwrap_err().to_string();
            assert!(error.contains("torrent record 0"), "{error}");
            assert!(error.contains(&expected_hash), "{error}");
            assert!(error.contains("piece hash 1"), "{error}");
            assert!(error.contains(&format!("length {decoded_len}")), "{error}");
            assert!(!error.contains("state.bin"), "{error}");
            if !encoded_payload.is_empty() {
                assert!(!error.contains(&encoded_payload), "{error}");
            }
        }
        remove_state(&path);
    }
}
