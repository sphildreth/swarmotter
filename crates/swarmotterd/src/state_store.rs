// SPDX-License-Identifier: Apache-2.0

//! Versioned, crash-safe SQLite persistence for daemon torrent and queue
//! state.
//!
//! The daemon deliberately performs all calls into this module from its
//! single async state-write lane. Each call opens a short-lived SQLite
//! connection on a blocking worker, uses a single `IMMEDIATE` transaction,
//! and closes it after a full WAL checkpoint. This keeps the database local,
//! recoverable, and compatible with the daemon's existing rollback boundary
//! without putting synchronous I/O on Tokio workers.
//!
//! Existing version-one JSON state documents remain readable. The first save
//! after a successful restore writes a complete SQLite database beside the old
//! file and atomically replaces it, so an interrupted migration leaves either
//! the valid JSON generation or a valid SQLite generation at the configured
//! path.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{
    params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::{InfoHash, TorrentKey, V2InfoHash};
use swarmotter_core::queue::QueueState;
use swarmotter_core::torrent::Torrent;

const LEGACY_STATE_VERSION: u32 = 1;
const SQLITE_SCHEMA_VERSION: u32 = 2;
const SQLITE_SCHEMA_V1: u32 = 1;
const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
/// Rebuild operations intentionally process a bounded number of authoritative
/// torrent records at a time. The queue is one authoritative JSON document,
/// while torrent records can grow independently of it.
const REBUILD_TORRENT_BATCH_SIZE: i64 = 64;
/// Retention caps keep durable operational history useful without allowing
/// routine state writes to grow the state store without bound. They are row
/// counts rather than elapsed-time promises, so they remain deterministic
/// across machines and write rates.
const MAX_LIBRARY_HISTORY_ROWS: i64 = 10_000;
const MAX_AUDIT_EVENT_ROWS: i64 = 10_000;
const MAX_METRIC_SAMPLES_PER_TORRENT: i64 = 512;
const MAX_METRIC_SAMPLE_ROWS: i64 = 50_000;

// The SQLite tables retain their historical `info_hash` column names so an
// existing state file needs no destructive table rewrite. Every such column
// now stores a canonical `TorrentKey` locator: 40 hex characters for a v1
// (including hybrid-primary) record or 64 for a pure-v2 record.

#[derive(Debug, Clone, Copy)]
struct RetentionLimits {
    library_history_rows: i64,
    audit_event_rows: i64,
    metric_samples_per_torrent: i64,
    metric_sample_rows: i64,
}

const DURABLE_RETENTION: RetentionLimits = RetentionLimits {
    library_history_rows: MAX_LIBRARY_HISTORY_ROWS,
    audit_event_rows: MAX_AUDIT_EVENT_ROWS,
    metric_samples_per_torrent: MAX_METRIC_SAMPLES_PER_TORRENT,
    metric_sample_rows: MAX_METRIC_SAMPLE_ROWS,
};
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
pub struct DaemonState {
    version: u32,
    pub torrents: Vec<Torrent>,
    pub queue: QueueState<TorrentKey>,
}

/// Exact pre-write state-file contents used by a higher-level transaction.
///
/// SQLite writes are checkpointed before this snapshot is taken, so the main
/// database file is a complete generation with no live WAL to replay. This
/// preserves the established paired-filesystem rollback contract without
/// relying on an in-memory database connection across async boundaries.
#[derive(Debug, Clone)]
pub(crate) enum StateFileSnapshot {
    Bytes(Vec<u8>),
    Missing,
}

/// Exact complete `.torrent` source bytes accepted by a local add or watch
/// import. The core metadata model retains a canonical `info` dictionary;
/// this separate value is the only representation that can later prove an
/// export is the original uploaded metainfo document rather than a
/// reconstruction.
#[derive(Debug, Clone)]
pub(crate) struct OriginalMetainfo {
    /// Canonical durable record key. This is a full 40-character v1 or
    /// 64-character pure-v2 locator, never a peer-wire hash or truncated v2
    /// value.
    pub key: TorrentKey,
    pub bytes: Vec<u8>,
}

/// Result of an explicit durable-state projection rebuild.
///
/// The counts describe rows regenerated from the authoritative serialized
/// torrent and queue records. They deliberately do not include retained audit
/// history or raw metainfo BLOBs, which a rebuild never changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionRebuildReport {
    pub torrents: usize,
    pub queue_entries: usize,
}

impl OriginalMetainfo {
    pub(crate) fn new(key: TorrentKey, bytes: Vec<u8>) -> Self {
        Self { key, bytes }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateFileKind {
    Missing,
    LegacyJson,
    Sqlite,
}

impl DaemonState {
    pub fn new(torrents: Vec<Torrent>, queue: QueueState<TorrentKey>) -> Self {
        Self {
            version: LEGACY_STATE_VERSION,
            torrents,
            queue,
        }
    }
}

#[derive(Deserialize)]
struct StoredDaemonState {
    version: u32,
    torrents: TorrentRecords,
    queue: QueueState<TorrentKey>,
}

struct TorrentRecords(Vec<Torrent>);

impl<'de> Deserialize<'de> for TorrentRecords {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RecordsVisitor;

        impl<'de> Visitor<'de> for RecordsVisitor {
            type Value = TorrentRecords;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an array of durable torrent records")
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut records = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
                loop {
                    let record_index = records.len();
                    let value = match sequence.next_element::<serde_json::Value>() {
                        Ok(Some(value)) => value,
                        Ok(None) => break,
                        Err(error) => {
                            return Err(A::Error::custom(format!(
                                "torrent record {record_index}: {error}"
                            )));
                        }
                    };
                    let hash = value
                        .get("meta")
                        .and_then(|meta| meta.get("info_hash"))
                        .and_then(serde_json::Value::as_str)
                        .and_then(|hash| swarmotter_core::hash::InfoHash::from_hex(hash).ok())
                        .map(|hash| hash.to_hex());
                    match serde_json::from_value::<Torrent>(value) {
                        Ok(torrent) => records.push(torrent),
                        Err(error) => {
                            let record = hash.map_or_else(
                                || format!("torrent record {record_index}"),
                                |hash| format!("torrent record {record_index} (info hash {hash})"),
                            );
                            return Err(A::Error::custom(format!("{record}: {error}")));
                        }
                    }
                }
                Ok(TorrentRecords(records))
            }
        }

        deserializer.deserialize_seq(RecordsVisitor)
    }
}

/// Load either a legacy JSON generation or the current SQLite store.
///
/// This function never silently initializes or replaces a corrupt file. A
/// legacy document is migrated only by [`save`] after the daemon has restored
/// and validated it successfully.
pub fn load(path: &Path) -> Result<Option<DaemonState>> {
    match classify_state_file(path)? {
        StateFileKind::Missing => Ok(None),
        StateFileKind::LegacyJson => load_legacy_json(path).map(Some),
        StateFileKind::Sqlite => load_sqlite(path).map(Some),
    }
}

/// Rebuild SQLite rows and indexes that are projections of durable torrent and
/// queue records.
///
/// This is an offline, operator-triggered maintenance action. It first opens
/// the existing database read-only and runs SQLite's integrity check, then
/// validates the supported schema and every authoritative torrent/queue
/// record under one write transaction. Only after those checks pass does it
/// regenerate projected columns, queue entries, current health/metrics, and
/// indexes. It never creates a state database, migrates legacy JSON, or
/// attempts to repair a database that fails integrity verification.
pub fn rebuild_projections(path: &Path) -> Result<ProjectionRebuildReport> {
    match classify_state_file(path)? {
        StateFileKind::Missing => {
            return Err(CoreError::Storage(format!(
                "cannot rebuild SQLite state projections: {} does not exist",
                path.display()
            )));
        }
        StateFileKind::LegacyJson => {
            return Err(CoreError::Storage(format!(
                "cannot rebuild SQLite state projections: {} is legacy JSON; start the daemon normally to perform its atomic migration",
                path.display()
            )));
        }
        StateFileKind::Sqlite => {}
    }

    // Do not configure WAL, migrate, or otherwise mutate the database until a
    // read-only connection has established that the existing file is sound.
    verify_existing_sqlite_for_rebuild(path)?;

    let mut connection = open_sqlite(path)?;
    let report = {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                CoreError::Storage(format!(
                    "begin SQLite state projection rebuild transaction: {error}"
                ))
            })?;

        // Take the write lock before repeating validation, so the checked
        // authoritative rows cannot change between validation and rebuild.
        verify_database(&transaction)?;
        verify_rebuild_schema(&transaction)?;
        let torrents = validate_authoritative_torrent_records(&transaction)?;
        let queue = read_authoritative_queue_state(&transaction)?;
        let queue_entries = queue
            .order
            .len()
            .checked_add(queue.bypass.len())
            .ok_or_else(|| {
                CoreError::Storage("too many durable queue entries to rebuild projections".into())
            })?;

        rebuild_derived_projections(&transaction, &queue)?;
        ensure_projection_indexes(&transaction)?;

        transaction.commit().map_err(|error| {
            CoreError::Storage(format!(
                "commit SQLite state projection rebuild transaction: {error}"
            ))
        })?;
        ProjectionRebuildReport {
            torrents,
            queue_entries,
        }
    };
    checkpoint_and_sync(connection, path)?;
    Ok(report)
}

/// Save a full registry and queue generation.
///
/// The SQLite schema stores a lossless JSON payload for each `Torrent`, plus
/// indexed projections for library/queue operations. Parsed raw metainfo is
/// normalized into BLOB rows when the core metadata model provides it; the
/// BLOB records remain byte-exact and are never rebuilt by this module.
pub fn save(path: &Path, state: &DaemonState) -> Result<()> {
    save_with_original_metainfo(path, state, None)
}

/// Read the byte-exact original `.torrent` document retained for a torrent.
///
/// This is intentionally a read-only lookup: it never opens a writable
/// connection, migrates a schema, checkpoints a WAL, or falls back to the
/// canonical BEP 9 `info` dictionary. Legacy JSON state and records created
/// from a magnet have no retained original document and therefore return
/// `Ok(None)`. Callers resolve a hybrid alias to the registry's canonical
/// primary [`TorrentKey`] before invoking this storage-level lookup.
pub fn load_original_metainfo(path: &Path, key: TorrentKey) -> Result<Option<Vec<u8>>> {
    match classify_state_file(path)? {
        StateFileKind::Missing | StateFileKind::LegacyJson => Ok(None),
        StateFileKind::Sqlite => {
            verify_existing_sqlite_state_for_migration(path, false)?;
            let key_locator = key.to_locator();
            let original = {
                let connection =
                    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(
                        |error| {
                            CoreError::Storage(format!(
                                "open SQLite state store read-only for original metainfo: {error}"
                            ))
                        },
                    )?;
                connection
                    .busy_timeout(SQLITE_BUSY_TIMEOUT)
                    .map_err(|error| {
                        CoreError::Storage(format!(
                            "configure SQLite original-metainfo lookup timeout: {error}"
                        ))
                    })?;
                verify_database(&connection)?;
                connection
                    .query_row(
                        "SELECT metainfo FROM torrent_metainfo
                         WHERE info_hash = ?1 AND representation = 'original_torrent'",
                        params![&key_locator],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    .map_err(|error| {
                        CoreError::Storage(format!(
                            "read durable original metainfo for {key_locator}: {error}"
                        ))
                    })?
            };
            // A read-only WAL-mode connection can recreate the `-wal`/`-shm`
            // sidecar files even though no writes occurred. Remove them so the
            // lookup leaves the durable state directory in the same clean state
            // the save path established. Callers must hold the daemon state
            // write lock to prevent a concurrent save from relying on a live
            // WAL file.
            remove_sqlite_sidecars(path)?;
            let Some(bytes) = original else {
                return Ok(None);
            };

            // Do not serve an arbitrary BLOB merely because it has the right
            // row key. Parsing verifies this remains a complete torrent
            // document for the requested identity without changing its bytes.
            let parsed = swarmotter_core::meta::parse_torrent(&bytes).map_err(|error| {
                CoreError::Storage(format!(
                    "retained original metainfo for {key_locator} is invalid: {error}"
                ))
            })?;
            if parsed.identity.primary_key() != Some(key) {
                return Err(CoreError::Storage(format!(
                    "retained original metainfo identity does not match {key_locator}"
                )));
            }
            Ok(Some(bytes))
        }
    }
}

/// Save state and, when supplied by a `.torrent` import path, record the
/// byte-exact original metainfo in the same SQLite transaction. The native
/// export surface reads only this representation; it never synthesizes one
/// from canonical metadata.
pub(crate) fn save_with_original_metainfo(
    path: &Path,
    state: &DaemonState,
    original_metainfo: Option<OriginalMetainfo>,
) -> Result<()> {
    match classify_state_file(path)? {
        StateFileKind::Missing => write_new_sqlite_generation(path, state, original_metainfo),
        StateFileKind::LegacyJson => {
            // Parse first so a corrupt or unsupported legacy document can
            // never be overwritten by a later in-memory snapshot.
            let _ = load_legacy_json(path)?;
            write_new_sqlite_generation(path, state, original_metainfo)
        }
        StateFileKind::Sqlite => save_sqlite_generation(path, state, original_metainfo),
    }
}

pub(crate) fn capture_file(path: &Path) -> Result<StateFileSnapshot> {
    if classify_state_file(path)? == StateFileKind::Sqlite {
        checkpoint_database_at_path(path)?;
    }
    match fs::read(path) {
        Ok(bytes) => Ok(StateFileSnapshot::Bytes(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(StateFileSnapshot::Missing)
        }
        Err(error) => Err(CoreError::Storage(format!("read daemon state: {error}"))),
    }
}

pub(crate) fn restore_file(path: &Path, snapshot: &StateFileSnapshot) -> Result<()> {
    remove_sqlite_sidecars(path)?;
    match snapshot {
        StateFileSnapshot::Bytes(bytes) => write_bytes_atomically(path, bytes),
        StateFileSnapshot::Missing => match fs::remove_file(path) {
            Ok(()) => sync_directory(
                path.parent().unwrap_or_else(|| Path::new(".")),
                "sync state directory",
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CoreError::Storage(format!("remove daemon state: {error}"))),
        },
    }
}

fn classify_state_file(path: &Path) -> Result<StateFileKind> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StateFileKind::Missing);
        }
        Err(error) => return Err(CoreError::Storage(format!("read daemon state: {error}"))),
    };
    let mut prefix = [0u8; 4096];
    let prefix_len = file
        .read(&mut prefix)
        .map_err(|error| CoreError::Storage(format!("read daemon state prefix: {error}")))?;
    let prefix = &prefix[..prefix_len];
    if prefix.starts_with(SQLITE_HEADER) {
        return Ok(StateFileKind::Sqlite);
    }
    if prefix
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'{')
    {
        return Ok(StateFileKind::LegacyJson);
    }
    Err(CoreError::Storage(
        "unrecognized daemon state format; refusing to overwrite it".into(),
    ))
}

fn load_legacy_json(path: &Path) -> Result<DaemonState> {
    let bytes = fs::read(path)
        .map_err(|error| CoreError::Storage(format!("read daemon state: {error}")))?;
    let stored: StoredDaemonState = serde_json::from_slice(&bytes)
        .map_err(|error| CoreError::Storage(format!("parse daemon state: {error}")))?;
    if stored.version != LEGACY_STATE_VERSION {
        return Err(CoreError::Storage(format!(
            "unsupported daemon state version {}; expected {LEGACY_STATE_VERSION}",
            stored.version
        )));
    }
    Ok(DaemonState {
        version: stored.version,
        torrents: stored.torrents.0,
        queue: stored.queue,
    })
}

fn load_sqlite(path: &Path) -> Result<DaemonState> {
    verify_existing_sqlite_state_for_migration(path, false)?;
    let mut connection = open_sqlite(path)?;
    migrate_schema(&mut connection)?;
    verify_database(&connection)?;

    let queue_bytes: Vec<u8> = connection
        .query_row(
            "SELECT queue_json FROM queue_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| CoreError::Storage(format!("read durable queue state: {error}")))?;
    let queue = serde_json::from_slice(&queue_bytes)
        .map_err(|error| CoreError::Storage(format!("parse durable queue state: {error}")))?;

    let records = {
        let mut statement = connection
            .prepare(
                "SELECT info_hash, torrent_json
                 FROM torrent_records
                 ORDER BY CAST(date_added AS INTEGER), info_hash",
            )
            .map_err(|error| CoreError::Storage(format!("query durable torrents: {error}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(|error| CoreError::Storage(format!("read durable torrents: {error}")))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| CoreError::Storage(format!("read durable torrent row: {error}")))?
    };

    let mut torrents = Vec::with_capacity(records.len());
    for (record_index, (stored_key_locator, bytes)) in records.into_iter().enumerate() {
        let mut torrent = deserialize_torrent_record(record_index, &stored_key_locator, &bytes)?;
        hydrate_raw_metainfo(&connection, &stored_key_locator, &mut torrent)?;
        let actual_key_locator = durable_torrent_key(&torrent)?;
        if actual_key_locator != stored_key_locator {
            return Err(CoreError::Storage(format!(
                "durable torrent record {record_index} has key {stored_key_locator} but payload identity {actual_key_locator}"
            )));
        }
        torrents.push(torrent);
    }

    verify_queue_index(&connection, &queue)?;
    checkpoint_and_sync(connection, path)?;
    Ok(DaemonState::new(torrents, queue))
}

fn deserialize_torrent_record(
    record_index: usize,
    stored_key_locator: &str,
    bytes: &[u8],
) -> Result<Torrent> {
    serde_json::from_slice(bytes).map_err(|error| {
        CoreError::Storage(format!(
            "parse durable torrent record {record_index} (key {stored_key_locator}): {error}"
        ))
    })
}

fn save_sqlite_generation(
    path: &Path,
    state: &DaemonState,
    original_metainfo: Option<OriginalMetainfo>,
) -> Result<()> {
    verify_existing_sqlite_state_for_migration(path, true)?;
    let mut connection = open_sqlite(path)?;
    migrate_schema(&mut connection)?;
    write_state_to_connection(&mut connection, state, original_metainfo.as_ref())?;
    checkpoint_and_sync(connection, path)
}

fn write_new_sqlite_generation(
    path: &Path,
    state: &DaemonState,
    original_metainfo: Option<OriginalMetainfo>,
) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| CoreError::Storage(format!("create state directory: {error}")))?;
    let temporary = temp_path(path);
    let result = (|| {
        let mut connection = Connection::open(&temporary)
            .map_err(|error| CoreError::Storage(format!("create SQLite state store: {error}")))?;
        restrict_permissions(&temporary)?;
        configure_connection(&connection)?;
        migrate_schema(&mut connection)?;
        write_state_to_connection(&mut connection, state, original_metainfo.as_ref())?;
        checkpoint_and_sync(connection, &temporary)?;
        fs::rename(&temporary, path)
            .map_err(|error| CoreError::Storage(format!("replace daemon state: {error}")))?;
        sync_directory(parent, "sync state directory")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = remove_sqlite_sidecars(&temporary);
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn open_sqlite(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)
        .map_err(|error| CoreError::Storage(format!("open SQLite state store: {error}")))?;
    restrict_permissions(path)?;
    configure_connection(&connection)?;
    Ok(connection)
}

/// Apply connection-local safety settings. WAL provides reader/writer
/// isolation; `synchronous=FULL` makes each committed transaction durable
/// before this function returns. We explicitly checkpoint when the short-lived
/// connection closes so raw snapshots and offline backups never depend on a
/// sidecar WAL file.
fn configure_connection(connection: &Connection) -> Result<()> {
    connection
        .busy_timeout(SQLITE_BUSY_TIMEOUT)
        .map_err(|error| CoreError::Storage(format!("configure SQLite busy timeout: {error}")))?;
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|error| CoreError::Storage(format!("enable SQLite WAL mode: {error}")))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(CoreError::Storage(format!(
            "SQLite refused WAL mode and reported {journal_mode}"
        )));
    }
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA synchronous = FULL;
             PRAGMA wal_autocheckpoint = 1000;
             PRAGMA trusted_schema = OFF;",
        )
        .map_err(|error| CoreError::Storage(format!("configure SQLite durability: {error}")))
}

fn migrate_schema(connection: &mut Connection) -> Result<()> {
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| CoreError::Storage(format!("read SQLite schema version: {error}")))?;
    if version > SQLITE_SCHEMA_VERSION {
        return Err(CoreError::Storage(format!(
            "unsupported SQLite state schema version {version}; expected at most {SQLITE_SCHEMA_VERSION}"
        )));
    }
    if version == SQLITE_SCHEMA_VERSION {
        return Ok(());
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| CoreError::Storage(format!("begin SQLite schema migration: {error}")))?;
    let mut applied = version;
    if applied == 0 {
        create_schema_v1(&transaction)?;
        record_schema_migration(&transaction, SQLITE_SCHEMA_V1)?;
        set_schema_version(&transaction, SQLITE_SCHEMA_V1)?;
        applied = SQLITE_SCHEMA_V1;
    }
    if applied == SQLITE_SCHEMA_V1 {
        apply_schema_v2(&transaction)?;
        record_schema_migration(&transaction, SQLITE_SCHEMA_VERSION)?;
        set_schema_version(&transaction, SQLITE_SCHEMA_VERSION)?;
        applied = SQLITE_SCHEMA_VERSION;
    }
    if applied != SQLITE_SCHEMA_VERSION {
        return Err(CoreError::Storage(format!(
            "SQLite state schema migration stopped at version {applied}; expected {SQLITE_SCHEMA_VERSION}"
        )));
    }
    transaction
        .commit()
        .map_err(|error| CoreError::Storage(format!("commit SQLite schema migration: {error}")))
}

fn create_schema_v1(transaction: &Transaction<'_>) -> Result<()> {
    transaction
        .execute_batch(
            "CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY NOT NULL,
                applied_at INTEGER NOT NULL
             ) STRICT;
             CREATE TABLE torrent_records (
                info_hash TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                date_added TEXT NOT NULL,
                total_length TEXT NOT NULL,
                torrent_json BLOB NOT NULL
             ) STRICT;
             CREATE INDEX torrent_records_lifecycle_state
                ON torrent_records(lifecycle_state);
             CREATE INDEX torrent_records_date_added
                ON torrent_records(date_added, info_hash);
             CREATE TABLE queue_state (
                singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton = 1),
                queue_json BLOB NOT NULL
             ) STRICT;
             CREATE TABLE queue_entries (
                queue_kind TEXT NOT NULL CHECK(queue_kind IN ('order', 'bypass')),
                position INTEGER NOT NULL,
                info_hash TEXT NOT NULL,
                PRIMARY KEY(queue_kind, position)
             ) STRICT;
             CREATE UNIQUE INDEX queue_entries_hash
                ON queue_entries(queue_kind, info_hash);
             CREATE TABLE torrent_metainfo (
                info_hash TEXT NOT NULL REFERENCES torrent_records(info_hash) ON DELETE CASCADE,
                representation TEXT NOT NULL CHECK(representation IN ('original_torrent', 'canonical_info')),
                metainfo BLOB NOT NULL,
                stored_at INTEGER NOT NULL,
                PRIMARY KEY(info_hash, representation)
             ) STRICT;
             CREATE TABLE torrent_health_snapshots (
                info_hash TEXT PRIMARY KEY NOT NULL REFERENCES torrent_records(info_hash) ON DELETE CASCADE,
                observed_at INTEGER NOT NULL,
                health_json BLOB NOT NULL
             ) STRICT;
             CREATE TABLE torrent_metrics_current (
                info_hash TEXT PRIMARY KEY NOT NULL REFERENCES torrent_records(info_hash) ON DELETE CASCADE,
                observed_at INTEGER NOT NULL,
                downloaded TEXT NOT NULL,
                uploaded TEXT NOT NULL,
                rate_down TEXT NOT NULL,
                rate_up TEXT NOT NULL
             ) STRICT;
             CREATE TABLE library_history (
                id INTEGER PRIMARY KEY,
                info_hash TEXT NOT NULL,
                occurred_at INTEGER NOT NULL,
                event_kind TEXT NOT NULL CHECK(event_kind IN ('registered', 'state_changed')),
                previous_state TEXT,
                current_state TEXT NOT NULL
             ) STRICT;
             CREATE INDEX library_history_torrent_time
                ON library_history(info_hash, occurred_at, id);
             CREATE TABLE audit_events (
                id INTEGER PRIMARY KEY,
                occurred_at INTEGER NOT NULL,
                actor TEXT,
                action TEXT NOT NULL,
                detail_json BLOB NOT NULL
             ) STRICT;",
        )
        .map_err(|error| CoreError::Storage(format!("create SQLite state schema: {error}")))
}

fn apply_schema_v2(transaction: &Transaction<'_>) -> Result<()> {
    transaction
        .execute_batch(
            "CREATE TABLE torrent_metric_samples (
                id INTEGER PRIMARY KEY,
                info_hash TEXT NOT NULL REFERENCES torrent_records(info_hash) ON DELETE CASCADE,
                observed_at INTEGER NOT NULL,
                downloaded TEXT NOT NULL,
                uploaded TEXT NOT NULL,
                rate_down TEXT NOT NULL,
                rate_up TEXT NOT NULL
             ) STRICT;
             CREATE INDEX torrent_metric_samples_torrent_time
                ON torrent_metric_samples(info_hash, observed_at DESC, id DESC);",
        )
        .map_err(|error| {
            CoreError::Storage(format!("create SQLite metric history schema: {error}"))
        })
}

fn record_schema_migration(transaction: &Transaction<'_>, version: u32) -> Result<()> {
    transaction
        .execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, ?2)",
            params![i64::from(version), unix_timestamp()],
        )
        .map_err(|error| CoreError::Storage(format!("record SQLite schema migration: {error}")))?;
    Ok(())
}

fn set_schema_version(transaction: &Transaction<'_>, version: u32) -> Result<()> {
    transaction
        .execute_batch(&format!("PRAGMA user_version = {version}"))
        .map_err(|error| CoreError::Storage(format!("set SQLite schema version: {error}")))
}

fn verify_database(connection: &Connection) -> Result<()> {
    let result: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|error| CoreError::Storage(format!("run SQLite quick_check: {error}")))?;
    if result != "ok" {
        return Err(CoreError::Storage(format!(
            "SQLite state integrity check failed: {result}"
        )));
    }
    Ok(())
}

fn verify_existing_sqlite_for_rebuild(path: &Path) -> Result<()> {
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|error| {
            CoreError::Storage(format!(
                "open SQLite state store read-only for projection rebuild: {error}"
            ))
        })?;
    connection
        .busy_timeout(SQLITE_BUSY_TIMEOUT)
        .map_err(|error| {
            CoreError::Storage(format!(
                "configure SQLite projection rebuild verification timeout: {error}"
            ))
        })?;
    verify_database(&connection)?;
    verify_rebuild_schema(&connection)
}

/// Check an existing SQLite file without changing journal settings or schema
/// before normal load/save code opens it read-write. Version zero is accepted
/// only for a completely empty database on an explicit save path; this keeps
/// the historical "fresh state file" setup behavior without treating an
/// arbitrary SQLite database as SwarmOtter state.
fn verify_existing_sqlite_state_for_migration(
    path: &Path,
    allow_empty_database: bool,
) -> Result<()> {
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|error| {
            CoreError::Storage(format!(
                "open SQLite state store read-only before schema migration: {error}"
            ))
        })?;
    connection
        .busy_timeout(SQLITE_BUSY_TIMEOUT)
        .map_err(|error| {
            CoreError::Storage(format!(
                "configure SQLite state migration verification timeout: {error}"
            ))
        })?;
    verify_database(&connection)?;
    let version = sqlite_schema_version(&connection, "before schema migration")?;
    match version {
        0 => {
            let object_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type IN ('table', 'index', 'trigger', 'view')
                       AND name NOT LIKE 'sqlite_%'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    CoreError::Storage(format!(
                        "inspect empty SQLite state store before schema migration: {error}"
                    ))
                })?;
            if object_count != 0 {
                return Err(CoreError::Storage(
                    "refusing to initialize a non-empty SQLite database as daemon state".into(),
                ));
            }
            if !allow_empty_database {
                return Err(CoreError::Storage(
                    "SQLite state store is uninitialized; refusing to create state while loading it"
                        .into(),
                ));
            }
            Ok(())
        }
        SQLITE_SCHEMA_V1 => verify_state_schema_tables(
            &connection,
            SQLITE_SCHEMA_V1,
            &[
                "schema_migrations",
                "torrent_records",
                "queue_state",
                "queue_entries",
                "torrent_metainfo",
                "torrent_health_snapshots",
                "torrent_metrics_current",
                "library_history",
                "audit_events",
            ],
            "before schema migration",
        ),
        SQLITE_SCHEMA_VERSION => verify_state_schema_tables(
            &connection,
            SQLITE_SCHEMA_VERSION,
            &[
                "schema_migrations",
                "torrent_records",
                "queue_state",
                "queue_entries",
                "torrent_metainfo",
                "torrent_health_snapshots",
                "torrent_metrics_current",
                "torrent_metric_samples",
                "library_history",
                "audit_events",
            ],
            "before schema migration",
        ),
        newer if newer > SQLITE_SCHEMA_VERSION => Err(CoreError::Storage(format!(
            "unsupported SQLite state schema version {newer}; expected at most {SQLITE_SCHEMA_VERSION}"
        ))),
        unsupported => Err(CoreError::Storage(format!(
            "unsupported SQLite state schema version {unsupported}"
        ))),
    }
}

fn sqlite_schema_version(connection: &Connection, context: &str) -> Result<u32> {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| {
            CoreError::Storage(format!("read SQLite schema version {context}: {error}"))
        })
}

fn verify_state_schema_tables(
    connection: &Connection,
    version: u32,
    required_tables: &[&str],
    context: &str,
) -> Result<()> {
    for table in required_tables {
        let exists: Option<i64> = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                CoreError::Storage(format!("inspect SQLite schema {context}: {error}"))
            })?;
        if exists.is_none() {
            return Err(CoreError::Storage(format!(
                "SQLite state schema version {version} is missing required table {table}"
            )));
        }
    }
    let migration_recorded: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM schema_migrations WHERE version = ?1",
            params![i64::from(version)],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            CoreError::Storage(format!(
                "inspect SQLite schema migration {context}: {error}"
            ))
        })?;
    if migration_recorded.is_none() {
        return Err(CoreError::Storage(format!(
            "SQLite state schema version {version} has no matching migration record"
        )));
    }
    Ok(())
}

/// Reject a database whose state schema is absent, incomplete, or requires a
/// migration. The maintenance command has a deliberately narrower contract
/// than normal daemon startup: it must never turn an arbitrary SQLite file
/// into a daemon state store as a side effect of trying to recover it.
fn verify_rebuild_schema(connection: &Connection) -> Result<()> {
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| {
            CoreError::Storage(format!(
                "read SQLite state schema version for projection rebuild: {error}"
            ))
        })?;
    if version != SQLITE_SCHEMA_VERSION {
        return Err(CoreError::Storage(format!(
            "cannot rebuild SQLite state projections for schema version {version}; expected {SQLITE_SCHEMA_VERSION}"
        )));
    }

    for table in [
        "schema_migrations",
        "torrent_records",
        "queue_state",
        "queue_entries",
        "torrent_metainfo",
        "torrent_health_snapshots",
        "torrent_metrics_current",
        "torrent_metric_samples",
        "library_history",
        "audit_events",
    ] {
        let exists: Option<i64> = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                CoreError::Storage(format!(
                    "inspect SQLite state schema for projection rebuild: {error}"
                ))
            })?;
        if exists.is_none() {
            return Err(CoreError::Storage(format!(
                "cannot rebuild SQLite state projections: required table {table} is missing"
            )));
        }
    }

    let migration_recorded: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM schema_migrations WHERE version = ?1",
            params![i64::from(SQLITE_SCHEMA_VERSION)],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            CoreError::Storage(format!(
                "inspect SQLite schema migration record for projection rebuild: {error}"
            ))
        })?;
    if migration_recorded.is_none() {
        return Err(CoreError::Storage(format!(
            "cannot rebuild SQLite state projections: schema migration {SQLITE_SCHEMA_VERSION} is not recorded"
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct RebuildTorrentRecord {
    key_locator: String,
    torrent: Torrent,
}

/// Read no more than [`REBUILD_TORRENT_BATCH_SIZE`] authoritative records at
/// once. Keeping this deliberately paged avoids making an operator recovery
/// command scale its transient memory with the entire library.
fn read_authoritative_torrent_batch(
    connection: &Connection,
    after_key_locator: Option<&str>,
) -> Result<Vec<RebuildTorrentRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT records.info_hash,
                    records.torrent_json,
                    (
                        SELECT metainfo
                        FROM torrent_metainfo
                        WHERE info_hash = records.info_hash
                          AND representation = 'canonical_info'
                    ) AS canonical_info
             FROM torrent_records AS records
             WHERE (?1 IS NULL OR records.info_hash > ?1)
             ORDER BY records.info_hash
             LIMIT ?2",
        )
        .map_err(|error| {
            CoreError::Storage(format!(
                "query authoritative durable torrents for projection rebuild: {error}"
            ))
        })?;
    let rows = statement
        .query_map(
            params![after_key_locator, REBUILD_TORRENT_BATCH_SIZE],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            },
        )
        .map_err(|error| {
            CoreError::Storage(format!(
                "read authoritative durable torrents for projection rebuild: {error}"
            ))
        })?;

    let mut records = Vec::with_capacity(REBUILD_TORRENT_BATCH_SIZE as usize);
    for (record_index, row) in rows.enumerate() {
        let (key_locator, torrent_json, canonical_info) = row.map_err(|error| {
            CoreError::Storage(format!(
                "read authoritative durable torrent row {record_index} for projection rebuild: {error}"
            ))
        })?;
        let mut torrent = deserialize_torrent_record(record_index, &key_locator, &torrent_json)?;
        torrent.meta.raw_info = canonical_info;
        records.push(RebuildTorrentRecord {
            key_locator,
            torrent,
        });
    }
    Ok(records)
}

fn validate_authoritative_torrent_record(record: &RebuildTorrentRecord) -> Result<()> {
    record.torrent.meta.validate().map_err(|error| {
        CoreError::Storage(format!(
            "invalid metadata in authoritative durable torrent {}: {error}",
            record.key_locator
        ))
    })?;
    let actual_key_locator = durable_torrent_key(&record.torrent)?;
    if actual_key_locator != record.key_locator {
        return Err(CoreError::Storage(format!(
            "authoritative durable torrent key {} does not match payload identity {actual_key_locator}",
            record.key_locator
        )));
    }
    Ok(())
}

fn validate_authoritative_torrent_records(connection: &Connection) -> Result<usize> {
    let mut after_key_locator = None;
    let mut count = 0usize;
    loop {
        let records = read_authoritative_torrent_batch(connection, after_key_locator.as_deref())?;
        if records.is_empty() {
            return Ok(count);
        }
        for record in &records {
            validate_authoritative_torrent_record(record)?;
        }
        count = count.checked_add(records.len()).ok_or_else(|| {
            CoreError::Storage("too many durable torrents to rebuild projections".into())
        })?;
        after_key_locator = records.last().map(|record| record.key_locator.clone());
    }
}

fn read_authoritative_queue_state(connection: &Connection) -> Result<QueueState<TorrentKey>> {
    let queue_bytes: Vec<u8> = connection
        .query_row(
            "SELECT queue_json FROM queue_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            CoreError::Storage(format!(
                "read authoritative durable queue state for projection rebuild: {error}"
            ))
        })?;
    serde_json::from_slice(&queue_bytes).map_err(|error| {
        CoreError::Storage(format!(
            "parse authoritative durable queue state for projection rebuild: {error}"
        ))
    })
}

fn rebuild_derived_projections(
    transaction: &Transaction<'_>,
    queue: &QueueState<TorrentKey>,
) -> Result<()> {
    // These tables are caches/projections only. Do not touch raw metainfo,
    // library history, audit records, or the serialized torrent/queue source
    // data that makes this deterministic rebuild possible.
    transaction
        .execute_batch(
            "DELETE FROM queue_entries;
             DELETE FROM torrent_health_snapshots;
             DELETE FROM torrent_metrics_current;",
        )
        .map_err(|error| {
            CoreError::Storage(format!(
                "clear SQLite durable projections for rebuild: {error}"
            ))
        })?;

    let mut after_key_locator = None;
    loop {
        let records = read_authoritative_torrent_batch(transaction, after_key_locator.as_deref())?;
        if records.is_empty() {
            break;
        }
        for record in &records {
            // Validation completed for every authoritative record before any
            // projection table was cleared. Rechecking here keeps the helper
            // independently safe if its call order changes in the future;
            // the enclosing transaction rolls back all projection writes on
            // failure.
            validate_authoritative_torrent_record(record)?;
            write_torrent_projection(transaction, record)?;
        }
        after_key_locator = records.last().map(|record| record.key_locator.clone());
    }
    write_queue_entries_projection(transaction, queue)
}

fn write_torrent_projection(
    transaction: &Transaction<'_>,
    record: &RebuildTorrentRecord,
) -> Result<()> {
    let torrent = &record.torrent;
    transaction
        .execute(
            "UPDATE torrent_records
             SET name = ?2,
                 lifecycle_state = ?3,
                 date_added = ?4,
                 total_length = ?5
             WHERE info_hash = ?1",
            params![
                &record.key_locator,
                &torrent.meta.name,
                torrent.state.as_str(),
                torrent.date_added.to_string(),
                torrent.meta.total_length.to_string(),
            ],
        )
        .map_err(|error| {
            CoreError::Storage(format!(
                "rebuild SQLite durable torrent projection {}: {error}",
                record.key_locator
            ))
        })?;
    write_health_snapshot(transaction, &record.key_locator, torrent)?;
    write_current_metrics(transaction, &record.key_locator, torrent, unix_timestamp())
}

fn write_queue_entries_projection(
    transaction: &Transaction<'_>,
    queue: &QueueState<TorrentKey>,
) -> Result<()> {
    for (queue_kind, hashes) in [("order", &queue.order), ("bypass", &queue.bypass)] {
        for (position, hash) in hashes.iter().enumerate() {
            let position = i64::try_from(position).map_err(|_| {
                CoreError::Storage("queue position exceeds SQLite integer range".into())
            })?;
            transaction
                .execute(
                    "INSERT INTO queue_entries(queue_kind, position, info_hash)
                     VALUES (?1, ?2, ?3)",
                    params![queue_kind, position, hash.to_locator()],
                )
                .map_err(|error| {
                    CoreError::Storage(format!("rebuild SQLite durable queue projection: {error}"))
                })?;
        }
    }
    Ok(())
}

fn ensure_projection_indexes(transaction: &Transaction<'_>) -> Result<()> {
    transaction
        .execute_batch(
            "CREATE INDEX IF NOT EXISTS torrent_records_lifecycle_state
                ON torrent_records(lifecycle_state);
             CREATE INDEX IF NOT EXISTS torrent_records_date_added
                ON torrent_records(date_added, info_hash);
             CREATE UNIQUE INDEX IF NOT EXISTS queue_entries_hash
                ON queue_entries(queue_kind, info_hash);
             CREATE INDEX IF NOT EXISTS library_history_torrent_time
                ON library_history(info_hash, occurred_at, id);
             CREATE INDEX IF NOT EXISTS torrent_metric_samples_torrent_time
                ON torrent_metric_samples(info_hash, observed_at DESC, id DESC);
             REINDEX;",
        )
        .map_err(|error| {
            CoreError::Storage(format!(
                "rebuild SQLite durable projection indexes: {error}"
            ))
        })
}

fn write_state_to_connection(
    connection: &mut Connection,
    state: &DaemonState,
    original_metainfo: Option<&OriginalMetainfo>,
) -> Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| CoreError::Storage(format!("begin SQLite state transaction: {error}")))?;
    write_state_transaction(&transaction, state, original_metainfo)?;
    transaction
        .commit()
        .map_err(|error| CoreError::Storage(format!("commit SQLite state transaction: {error}")))
}

fn write_state_transaction(
    transaction: &Transaction<'_>,
    state: &DaemonState,
    original_metainfo: Option<&OriginalMetainfo>,
) -> Result<()> {
    transaction
        .execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS incoming_torrent_records (
                info_hash TEXT PRIMARY KEY NOT NULL
             ) STRICT;
             DELETE FROM incoming_torrent_records;",
        )
        .map_err(|error| {
            CoreError::Storage(format!("prepare SQLite torrent replacement: {error}"))
        })?;

    for torrent in &state.torrents {
        let hash = durable_torrent_key(torrent)?;
        let current_state = torrent.state.as_str();
        let observed_at = unix_timestamp();
        let previous_state: Option<String> = transaction
            .query_row(
                "SELECT lifecycle_state FROM torrent_records WHERE info_hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                CoreError::Storage(format!("read durable torrent history: {error}"))
            })?;
        let (torrent_json, raw_info) = encode_torrent(torrent)?;
        transaction
            .execute(
                "INSERT INTO incoming_torrent_records(info_hash) VALUES (?1)",
                params![&hash],
            )
            .map_err(|error| CoreError::Storage(format!("stage durable torrent: {error}")))?;
        transaction
            .execute(
                "INSERT INTO torrent_records(
                    info_hash, name, lifecycle_state, date_added, total_length, torrent_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(info_hash) DO UPDATE SET
                    name = excluded.name,
                    lifecycle_state = excluded.lifecycle_state,
                    date_added = excluded.date_added,
                    total_length = excluded.total_length,
                    torrent_json = excluded.torrent_json",
                params![
                    &hash,
                    &torrent.meta.name,
                    current_state,
                    torrent.date_added.to_string(),
                    torrent.meta.total_length.to_string(),
                    torrent_json,
                ],
            )
            .map_err(|error| CoreError::Storage(format!("write durable torrent: {error}")))?;
        write_history(transaction, &hash, previous_state.as_deref(), current_state)?;
        write_canonical_info(transaction, &hash, raw_info.as_deref())?;
        write_health_snapshot(transaction, &hash, torrent)?;
        write_current_metrics(transaction, &hash, torrent, observed_at)?;
        write_metric_sample(transaction, &hash, torrent, observed_at)?;
    }

    let removed_count: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM torrent_records
             WHERE info_hash NOT IN (SELECT info_hash FROM incoming_torrent_records)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| CoreError::Storage(format!("count stale durable torrents: {error}")))?;
    if removed_count > 0 {
        write_audit_event(
            transaction,
            Some("daemon"),
            "torrents_removed",
            &serde_json::json!({ "count": removed_count }),
        )?;
    }
    transaction
        .execute(
            "DELETE FROM torrent_records
             WHERE info_hash NOT IN (SELECT info_hash FROM incoming_torrent_records)",
            [],
        )
        .map_err(|error| CoreError::Storage(format!("remove stale durable torrents: {error}")))?;
    if let Some(original) = original_metainfo {
        let key = original.key.to_locator();
        let registered = state
            .torrents
            .iter()
            .any(|torrent| torrent.key() == original.key);
        if !registered {
            return Err(CoreError::Storage(format!(
                "refusing to retain original metainfo for unregistered torrent {key}"
            )));
        }
        write_original_metainfo(transaction, original.key, &original.bytes)?;
    }
    write_queue_state(transaction, &state.queue)?;
    prune_retained_rows(transaction, DURABLE_RETENTION)?;
    Ok(())
}

fn encode_torrent(torrent: &Torrent) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
    let mut stored = torrent.clone();
    let raw_info = stored.meta.raw_info.take();
    let json = serde_json::to_vec(&stored)
        .map_err(|error| CoreError::Storage(format!("serialize durable torrent: {error}")))?;
    Ok((json, raw_info))
}

/// Return the canonical collision-safe durable locator for a torrent.
///
/// The SQLite schema retains the historical `info_hash` column name for
/// compatibility, but its values are [`TorrentKey`] locators: 40 hexadecimal
/// characters for v1/hybrid primary records and 64 for pure v2 records. A
/// pure-v2 record is therefore never reduced to its all-zero legacy v1 field
/// or a truncated peer-wire identity.
fn durable_torrent_key(torrent: &Torrent) -> Result<String> {
    match torrent.key() {
        // A parsed pure-v2 record legitimately has a zero *legacy v1 field*,
        // but its durable key is the nonzero full SHA-256 identity. Neither
        // all-zero identity is a valid durable record key: accepting one
        // would reintroduce the sentinel/collision ambiguity this key model
        // removes.
        TorrentKey::V1(hash) if hash == InfoHash::ZERO => Err(CoreError::Storage(
            "refusing to persist a torrent with an all-zero v1 identity".into(),
        )),
        TorrentKey::V2(hash) if hash == V2InfoHash::ZERO => Err(CoreError::Storage(
            "refusing to persist a torrent with an all-zero v2 identity".into(),
        )),
        key => Ok(key.to_locator()),
    }
}

fn write_original_metainfo(
    transaction: &Transaction<'_>,
    key: TorrentKey,
    bytes: &[u8],
) -> Result<()> {
    if bytes.is_empty() {
        return Err(CoreError::Storage(
            "refusing to retain an empty original metainfo document".into(),
        ));
    }
    let key_locator = key.to_locator();
    let parsed = swarmotter_core::meta::parse_torrent(bytes).map_err(|error| {
        CoreError::Storage(format!(
            "refusing to retain invalid original metainfo for {key_locator}: {error}"
        ))
    })?;
    if parsed.identity.primary_key() != Some(key) {
        return Err(CoreError::Storage(format!(
            "refusing to retain original metainfo whose identity does not match {key_locator}"
        )));
    }
    transaction
        .execute(
            "INSERT INTO torrent_metainfo(info_hash, representation, metainfo, stored_at)
             VALUES (?1, 'original_torrent', ?2, ?3)
             ON CONFLICT(info_hash, representation) DO NOTHING",
            params![key_locator, bytes, unix_timestamp()],
        )
        .map_err(|error| CoreError::Storage(format!("write durable original metainfo: {error}")))?;
    Ok(())
}

fn write_history(
    transaction: &Transaction<'_>,
    hash: &str,
    previous_state: Option<&str>,
    current_state: &str,
) -> Result<()> {
    let event_kind = match previous_state {
        None => Some("registered"),
        Some(previous) if previous != current_state => Some("state_changed"),
        Some(_) => None,
    };
    if let Some(event_kind) = event_kind {
        transaction
            .execute(
                "INSERT INTO library_history(
                    info_hash, occurred_at, event_kind, previous_state, current_state
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    hash,
                    unix_timestamp(),
                    event_kind,
                    previous_state,
                    current_state
                ],
            )
            .map_err(|error| {
                CoreError::Storage(format!("write durable library history: {error}"))
            })?;
        let action = if event_kind == "registered" {
            "torrent_registered"
        } else {
            "torrent_state_changed"
        };
        write_audit_event(
            transaction,
            Some("daemon"),
            action,
            &serde_json::json!({
                "info_hash": hash,
                "previous_state": previous_state,
                "current_state": current_state,
            }),
        )?;
    }
    Ok(())
}

fn write_audit_event(
    transaction: &Transaction<'_>,
    actor: Option<&str>,
    action: &str,
    detail: &serde_json::Value,
) -> Result<()> {
    if action.is_empty() {
        return Err(CoreError::Storage(
            "refusing to write durable audit event with an empty action".into(),
        ));
    }
    let detail_json = serde_json::to_vec(detail)
        .map_err(|error| CoreError::Storage(format!("serialize durable audit event: {error}")))?;
    transaction
        .execute(
            "INSERT INTO audit_events(occurred_at, actor, action, detail_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![unix_timestamp(), actor, action, detail_json],
        )
        .map_err(|error| CoreError::Storage(format!("write durable audit event: {error}")))?;
    Ok(())
}

fn write_canonical_info(
    transaction: &Transaction<'_>,
    hash: &str,
    raw_info: Option<&[u8]>,
) -> Result<()> {
    let Some(raw_info) = raw_info else {
        return Ok(());
    };
    transaction
        .execute(
            "INSERT INTO torrent_metainfo(info_hash, representation, metainfo, stored_at)
             VALUES (?1, 'canonical_info', ?2, ?3)
             ON CONFLICT(info_hash, representation) DO UPDATE SET
                metainfo = excluded.metainfo,
                stored_at = excluded.stored_at",
            params![hash, raw_info, unix_timestamp()],
        )
        .map_err(|error| CoreError::Storage(format!("write durable canonical info: {error}")))?;
    Ok(())
}

fn write_health_snapshot(
    transaction: &Transaction<'_>,
    hash: &str,
    torrent: &Torrent,
) -> Result<()> {
    let health = serde_json::to_vec(&torrent.health).map_err(|error| {
        CoreError::Storage(format!("serialize durable health snapshot: {error}"))
    })?;
    transaction
        .execute(
            "INSERT INTO torrent_health_snapshots(info_hash, observed_at, health_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(info_hash) DO UPDATE SET
                observed_at = excluded.observed_at,
                health_json = excluded.health_json",
            params![hash, unix_timestamp(), health],
        )
        .map_err(|error| CoreError::Storage(format!("write durable health snapshot: {error}")))?;
    Ok(())
}

fn write_current_metrics(
    transaction: &Transaction<'_>,
    hash: &str,
    torrent: &Torrent,
    observed_at: i64,
) -> Result<()> {
    transaction
        .execute(
            "INSERT INTO torrent_metrics_current(
                info_hash, observed_at, downloaded, uploaded, rate_down, rate_up
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(info_hash) DO UPDATE SET
                observed_at = excluded.observed_at,
                downloaded = excluded.downloaded,
                uploaded = excluded.uploaded,
                rate_down = excluded.rate_down,
                rate_up = excluded.rate_up",
            params![
                hash,
                observed_at,
                torrent.downloaded.to_string(),
                torrent.uploaded.to_string(),
                torrent.rate_down.to_string(),
                torrent.rate_up.to_string(),
            ],
        )
        .map_err(|error| CoreError::Storage(format!("write durable current metrics: {error}")))?;
    Ok(())
}

fn write_metric_sample(
    transaction: &Transaction<'_>,
    hash: &str,
    torrent: &Torrent,
    observed_at: i64,
) -> Result<()> {
    let downloaded = torrent.downloaded.to_string();
    let uploaded = torrent.uploaded.to_string();
    let rate_down = torrent.rate_down.to_string();
    let rate_up = torrent.rate_up.to_string();
    let previous: Option<(String, String, String, String)> = transaction
        .query_row(
            "SELECT downloaded, uploaded, rate_down, rate_up
             FROM torrent_metric_samples
             WHERE info_hash = ?1
             ORDER BY observed_at DESC, id DESC
             LIMIT 1",
            params![hash],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|error| CoreError::Storage(format!("read durable metric sample: {error}")))?;
    if previous.as_ref().is_some_and(|previous| {
        previous.0 == downloaded
            && previous.1 == uploaded
            && previous.2 == rate_down
            && previous.3 == rate_up
    }) {
        return Ok(());
    }
    transaction
        .execute(
            "INSERT INTO torrent_metric_samples(
                info_hash, observed_at, downloaded, uploaded, rate_down, rate_up
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![hash, observed_at, downloaded, uploaded, rate_down, rate_up],
        )
        .map_err(|error| CoreError::Storage(format!("write durable metric sample: {error}")))?;
    Ok(())
}

fn prune_retained_rows(transaction: &Transaction<'_>, limits: RetentionLimits) -> Result<()> {
    prune_metric_samples(transaction, limits)?;
    prune_table_by_latest_id(
        transaction,
        "library_history",
        "occurred_at",
        limits.library_history_rows,
    )?;
    prune_table_by_latest_id(
        transaction,
        "audit_events",
        "occurred_at",
        limits.audit_event_rows,
    )
}

fn prune_metric_samples(transaction: &Transaction<'_>, limits: RetentionLimits) -> Result<()> {
    if limits.metric_samples_per_torrent < 0 || limits.metric_sample_rows < 0 {
        return Err(CoreError::Storage(
            "durable metric retention limits must be non-negative".into(),
        ));
    }
    transaction
        .execute(
            "DELETE FROM torrent_metric_samples
             WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY info_hash
                               ORDER BY observed_at DESC, id DESC
                           ) AS retained_position
                    FROM torrent_metric_samples
                )
                WHERE retained_position > ?1
             )",
            params![limits.metric_samples_per_torrent],
        )
        .map_err(|error| {
            CoreError::Storage(format!("prune per-torrent durable metric samples: {error}"))
        })?;
    prune_table_by_latest_id(
        transaction,
        "torrent_metric_samples",
        "observed_at",
        limits.metric_sample_rows,
    )
}

fn prune_table_by_latest_id(
    transaction: &Transaction<'_>,
    table: &str,
    timestamp_column: &str,
    limit: i64,
) -> Result<()> {
    if limit < 0 {
        return Err(CoreError::Storage(
            "durable retention limits must be non-negative".into(),
        ));
    }
    let statement = match (table, timestamp_column) {
        ("library_history", "occurred_at") => {
            "DELETE FROM library_history
             WHERE id NOT IN (
                 SELECT id FROM library_history
                 ORDER BY occurred_at DESC, id DESC
                 LIMIT ?1
             )"
        }
        ("audit_events", "occurred_at") => {
            "DELETE FROM audit_events
             WHERE id NOT IN (
                 SELECT id FROM audit_events
                 ORDER BY occurred_at DESC, id DESC
                 LIMIT ?1
             )"
        }
        ("torrent_metric_samples", "observed_at") => {
            "DELETE FROM torrent_metric_samples
             WHERE id NOT IN (
                 SELECT id FROM torrent_metric_samples
                 ORDER BY observed_at DESC, id DESC
                 LIMIT ?1
             )"
        }
        _ => {
            return Err(CoreError::Storage(
                "unsupported durable retention table".into(),
            ));
        }
    };
    transaction
        .execute(statement, params![limit])
        .map_err(|error| CoreError::Storage(format!("prune durable retention rows: {error}")))?;
    Ok(())
}

fn write_queue_state(transaction: &Transaction<'_>, queue: &QueueState<TorrentKey>) -> Result<()> {
    let bytes = serde_json::to_vec(queue)
        .map_err(|error| CoreError::Storage(format!("serialize durable queue state: {error}")))?;
    transaction
        .execute(
            "INSERT INTO queue_state(singleton, queue_json) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET queue_json = excluded.queue_json",
            params![bytes],
        )
        .map_err(|error| CoreError::Storage(format!("write durable queue state: {error}")))?;
    transaction
        .execute("DELETE FROM queue_entries", [])
        .map_err(|error| CoreError::Storage(format!("clear durable queue index: {error}")))?;
    for (queue_kind, hashes) in [("order", &queue.order), ("bypass", &queue.bypass)] {
        for (position, hash) in hashes.iter().enumerate() {
            let position = i64::try_from(position).map_err(|_| {
                CoreError::Storage("queue position exceeds SQLite integer range".into())
            })?;
            transaction
                .execute(
                    "INSERT INTO queue_entries(queue_kind, position, info_hash)
                     VALUES (?1, ?2, ?3)",
                    params![queue_kind, position, hash.to_locator()],
                )
                .map_err(|error| {
                    CoreError::Storage(format!("write durable queue index: {error}"))
                })?;
        }
    }
    Ok(())
}

fn hydrate_raw_metainfo(connection: &Connection, hash: &str, torrent: &mut Torrent) -> Result<()> {
    let mut statement = connection
        .prepare(
            "SELECT representation, metainfo
             FROM torrent_metainfo
             WHERE info_hash = ?1",
        )
        .map_err(|error| CoreError::Storage(format!("query durable raw metainfo: {error}")))?;
    let rows = statement
        .query_map(params![hash], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(|error| CoreError::Storage(format!("read durable raw metainfo: {error}")))?;
    for row in rows {
        let (representation, bytes) = row.map_err(|error| {
            CoreError::Storage(format!("read durable raw metainfo row: {error}"))
        })?;
        match representation.as_str() {
            "canonical_info" => torrent.meta.raw_info = Some(bytes),
            "original_torrent" => {}
            unexpected => {
                return Err(CoreError::Storage(format!(
                    "unknown durable raw metainfo representation {unexpected}"
                )));
            }
        }
    }
    Ok(())
}

fn verify_queue_index(connection: &Connection, queue: &QueueState<TorrentKey>) -> Result<()> {
    for (queue_kind, expected) in [("order", &queue.order), ("bypass", &queue.bypass)] {
        let actual = {
            let mut statement = connection
                .prepare(
                    "SELECT info_hash FROM queue_entries
                     WHERE queue_kind = ?1 ORDER BY position",
                )
                .map_err(|error| {
                    CoreError::Storage(format!("query durable queue index: {error}"))
                })?;
            let rows = statement
                .query_map(params![queue_kind], |row| row.get::<_, String>(0))
                .map_err(|error| {
                    CoreError::Storage(format!("read durable queue index: {error}"))
                })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| {
                    CoreError::Storage(format!("read durable queue index row: {error}"))
                })?
        };
        let expected = expected
            .iter()
            .map(|hash| hash.to_locator())
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(CoreError::Storage(format!(
                "durable queue {queue_kind} index does not match queue state"
            )));
        }
    }
    Ok(())
}

fn checkpoint_database_at_path(path: &Path) -> Result<()> {
    verify_existing_sqlite_state_for_migration(path, false)?;
    let connection = open_sqlite(path)?;
    checkpoint_and_sync(connection, path)
}

fn checkpoint_and_sync(connection: Connection, path: &Path) -> Result<()> {
    let checkpoint = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(|error| CoreError::Storage(format!("checkpoint SQLite state store: {error}")))?;
    if checkpoint.0 != 0 {
        return Err(CoreError::Storage(
            "SQLite state checkpoint remained busy; refusing an incomplete snapshot".into(),
        ));
    }
    connection
        .close()
        .map_err(|(_, error)| CoreError::Storage(format!("close SQLite state store: {error}")))?;
    remove_sqlite_sidecars(path)?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| CoreError::Storage(format!("sync SQLite state store: {error}")))?;
    sync_directory(
        path.parent().unwrap_or_else(|| Path::new(".")),
        "sync state directory",
    )
}

fn remove_sqlite_sidecars(path: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = sqlite_sidecar_path(path, suffix);
        match fs::remove_file(&sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CoreError::Storage(format!(
                    "remove SQLite state sidecar {}: {error}",
                    sidecar.display()
                )));
            }
        }
    }
    Ok(())
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| CoreError::Storage(format!("create state directory: {error}")))?;
    let temp = temp_path(path);
    let result = write_and_replace(&temp, path, parent, bytes);
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn temp_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.sqlite");
    path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), sequence))
}

fn write_and_replace(temp: &Path, path: &Path, parent: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(temp)
        .map_err(|error| CoreError::Storage(format!("create temporary daemon state: {error}")))?;
    file.write_all(bytes)
        .map_err(|error| CoreError::Storage(format!("write daemon state: {error}")))?;
    file.sync_all()
        .map_err(|error| CoreError::Storage(format!("sync daemon state: {error}")))?;
    drop(file);
    fs::rename(temp, path)
        .map_err(|error| CoreError::Storage(format!("replace daemon state: {error}")))?;
    sync_directory(parent, "sync state directory")
}

fn restrict_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)
            .map_err(|error| CoreError::Storage(format!("read state permissions: {error}")))?
            .permissions();
        if permissions.mode() & 0o777 != 0o600 {
            permissions.set_mode(0o600);
            fs::set_permissions(path, permissions).map_err(|error| {
                CoreError::Storage(format!("restrict state permissions: {error}"))
            })?;
        }
    }
    Ok(())
}

fn sync_directory(path: &Path, context: &str) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| CoreError::Storage(format!("{context}: {error}")))
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
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
        torrent.extend_from_slice(
            b"d4:infod9:file treed10:lawful.bind0:d6:lengthi1e11:pieces root32:",
        );
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
        let connection =
            Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
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
}
