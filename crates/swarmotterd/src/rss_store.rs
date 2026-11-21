// SPDX-License-Identifier: Apache-2.0

//! Durable RSS subscription history.
//!
//! One dedicated SQLite database is shared by every subscription. It stores
//! feed poll state, per-listing decisions, torrent-identity attribution, and a
//! compact record of verified completions. Library changes reach this database
//! through the main state store's transactional outbox, so a crash can only
//! replay an idempotent event; it cannot silently lose completion evidence.
//!
//! The database is local-only and creates no network traffic. All calls are
//! blocking and are invoked from the daemon's blocking workers.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::rss::{
    RssAction, RssCounts, RssDecision, RssDecisionPage, RssEntry, RssFeedState,
};

const RSS_SCHEMA_VERSION: u32 = 1;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingEventKind {
    Completed,
    Removed,
}

impl PendingEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Removed => "removed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "completed" => Some(Self::Completed),
            "removed" => Some(Self::Removed),
            _ => None,
        }
    }
}

/// A library change durably enqueued in the main database transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingLibraryEvent {
    pub id: Option<i64>,
    pub kind: PendingEventKind,
    pub canonical_key: String,
    pub aliases: Vec<String>,
    pub name: String,
    pub was_completed: bool,
    pub occurred_at: u64,
}

impl PendingLibraryEvent {
    pub fn completion(
        canonical_key: String,
        aliases: Vec<String>,
        name: String,
        occurred_at: u64,
    ) -> Self {
        Self {
            id: None,
            kind: PendingEventKind::Completed,
            canonical_key,
            aliases,
            name,
            was_completed: true,
            occurred_at,
        }
    }

    pub fn removal(
        canonical_key: String,
        aliases: Vec<String>,
        name: String,
        was_completed: bool,
        occurred_at: u64,
    ) -> Self {
        Self {
            id: None,
            kind: PendingEventKind::Removed,
            canonical_key,
            aliases,
            name,
            was_completed,
            occurred_at,
        }
    }

    pub fn identities(&self) -> Vec<&str> {
        let mut values = vec![self.canonical_key.as_str()];
        for alias in &self.aliases {
            if !values.contains(&alias.as_str()) {
                values.push(alias);
            }
        }
        values
    }
}

/// Internally stored listing snapshot used to retry or reconsider a listing
/// even after it leaves its feed.
#[derive(Debug, Clone)]
pub struct StoredListing {
    pub id: i64,
    pub subscription_id: String,
    pub entry_key: String,
    pub entry: RssEntry,
    pub torrent_keys: Vec<String>,
    pub status: String,
    pub reason: String,
    pub first_seen_at: u64,
    pub last_seen_at: u64,
    pub last_decision_at: u64,
    pub retry_count: u32,
    pub next_attempt_at: Option<u64>,
    pub dismissed: bool,
    pub imported_at: Option<u64>,
    pub completed_at: Option<u64>,
}

impl StoredListing {
    pub fn action(&self) -> RssAction {
        match self.status.as_str() {
            "import" | "imported" => RssAction::Import,
            "already_present" => RssAction::AlreadyPresent,
            "previously_completed" | "completed" => RssAction::PreviouslyCompleted,
            "filtered" => RssAction::Filtered,
            "dismissed" => RssAction::Dismissed,
            "unsupported" => RssAction::Unsupported,
            "failed" => RssAction::Failed,
            "deferred" => RssAction::Deferred,
            "baseline" => RssAction::Baseline,
            _ => RssAction::Failed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateLookup {
    Completed,
    ImportedElsewhere,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionRecord {
    pub canonical_key: String,
    pub first_completed_at: u64,
    pub last_observed_at: u64,
    pub source: String,
}

#[derive(Clone)]
pub struct RssStore {
    path: PathBuf,
}

impl RssStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn initialize(&self) -> Result<()> {
        static INIT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _init_guard = INIT_LOCK
            .lock()
            .map_err(|_| CoreError::Storage("RSS history init lock poisoned".into()))?;
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| {
            CoreError::Storage(format!("create RSS history directory: {error}"))
        })?;
        let mut connection = self.open()?;
        let journal: String = connection
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(|error| CoreError::Storage(format!("enable RSS WAL mode: {error}")))?;
        if !journal.eq_ignore_ascii_case("wal") {
            return Err(CoreError::Storage(format!(
                "RSS history database refused WAL mode and reported {journal}"
            )));
        }
        let version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|error| CoreError::Storage(format!("read RSS schema version: {error}")))?;
        if version > RSS_SCHEMA_VERSION {
            return Err(CoreError::Storage(format!(
                "unsupported RSS history schema version {version}; expected at most {RSS_SCHEMA_VERSION}"
            )));
        }
        if version == 0 {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| CoreError::Storage(format!("begin RSS schema: {error}")))?;
            transaction
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS feed_state (
                        subscription_id TEXT PRIMARY KEY NOT NULL,
                        etag TEXT,
                        last_modified TEXT,
                        last_poll_at INTEGER,
                        last_success_at INTEGER,
                        next_attempt_at INTEGER,
                        last_error TEXT,
                        consecutive_failures INTEGER NOT NULL DEFAULT 0,
                        baseline_established INTEGER NOT NULL DEFAULT 0
                     ) STRICT;
                     CREATE TABLE IF NOT EXISTS listings (
                        id INTEGER PRIMARY KEY,
                        subscription_id TEXT NOT NULL,
                        entry_key TEXT NOT NULL,
                        guid TEXT,
                        title TEXT NOT NULL,
                        link TEXT,
                        category TEXT,
                        pub_date TEXT,
                        entry_json BLOB NOT NULL,
                        torrent_keys BLOB NOT NULL DEFAULT x'5b5d',
                        status TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        first_seen_at INTEGER NOT NULL,
                        last_seen_at INTEGER NOT NULL,
                        last_decision_at INTEGER NOT NULL,
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        next_attempt_at INTEGER,
                        dismissed INTEGER NOT NULL DEFAULT 0,
                        imported_at INTEGER,
                        completed_at INTEGER,
                        UNIQUE(subscription_id, entry_key)
                     ) STRICT;
                     CREATE INDEX IF NOT EXISTS listings_feed_status
                        ON listings(subscription_id, status, last_decision_at, id);
                     CREATE INDEX IF NOT EXISTS listings_retry
                        ON listings(subscription_id, status, next_attempt_at);
                     CREATE TABLE IF NOT EXISTS listing_identities (
                        alias_key TEXT NOT NULL,
                        canonical_key TEXT NOT NULL,
                        subscription_id TEXT NOT NULL,
                        entry_key TEXT NOT NULL,
                        first_seen_at INTEGER NOT NULL,
                        PRIMARY KEY(alias_key, subscription_id, entry_key)
                     ) STRICT;
                     CREATE INDEX IF NOT EXISTS listing_identities_owner
                        ON listing_identities(subscription_id, entry_key);
                     CREATE TABLE IF NOT EXISTS completions (
                        canonical_key TEXT PRIMARY KEY NOT NULL,
                        first_completed_at INTEGER NOT NULL,
                        last_observed_at INTEGER NOT NULL,
                        source TEXT NOT NULL
                     ) STRICT;
                     CREATE TABLE IF NOT EXISTS completion_aliases (
                        alias_key TEXT PRIMARY KEY NOT NULL,
                        canonical_key TEXT NOT NULL
                     ) STRICT;
                     CREATE INDEX IF NOT EXISTS completion_aliases_canonical
                        ON completion_aliases(canonical_key);
                     CREATE TABLE IF NOT EXISTS decisions (
                        id INTEGER PRIMARY KEY,
                        subscription_id TEXT NOT NULL,
                        entry_key TEXT NOT NULL,
                        title TEXT NOT NULL,
                        action TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        torrent_keys BLOB NOT NULL DEFAULT x'5b5d',
                        decided_at INTEGER NOT NULL
                     ) STRICT;
                     CREATE INDEX IF NOT EXISTS decisions_feed_id
                        ON decisions(subscription_id, id DESC);
                     PRAGMA user_version = {RSS_SCHEMA_VERSION};"
                ))
                .map_err(|error| {
                    CoreError::Storage(format!("create RSS history schema: {error}"))
                })?;
            transaction
                .commit()
                .map_err(|error| CoreError::Storage(format!("commit RSS schema: {error}")))?;
        }
        self.checkpoint(connection)
    }

    fn open(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path).map_err(|error| {
            CoreError::Storage(format!(
                "open RSS history database {}: {error}",
                self.path.display()
            ))
        })?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(|error| CoreError::Storage(format!("configure RSS busy timeout: {error}")))?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 PRAGMA synchronous = FULL;
                 PRAGMA trusted_schema = OFF;",
            )
            .map_err(|error| CoreError::Storage(format!("configure RSS durability: {error}")))?;
        restrict_permissions(&self.path)?;
        Ok(connection)
    }

    fn open_read_only(&self) -> Result<Connection> {
        let connection = Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| {
                CoreError::Storage(format!(
                    "open RSS history database read-only {}: {error}",
                    self.path.display()
                ))
            })?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(|error| CoreError::Storage(format!("configure RSS busy timeout: {error}")))?;
        Ok(connection)
    }

    fn checkpoint(&self, connection: Connection) -> Result<()> {
        let _: (i64, i64, i64) = connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|error| CoreError::Storage(format!("checkpoint RSS history: {error}")))?;
        Ok(())
    }

    pub fn feed_state(&self, subscription_id: &str) -> Result<Option<RssFeedState>> {
        let connection = self.open_read_only()?;
        let value = connection
            .query_row(
                "SELECT subscription_id, etag, last_modified, last_poll_at, last_success_at,
                        next_attempt_at, last_error, consecutive_failures, baseline_established
                 FROM feed_state WHERE subscription_id = ?1",
                params![subscription_id],
                |row| {
                    Ok(RssFeedState {
                        subscription_id: row.get(0)?,
                        etag: row.get(1)?,
                        last_modified: row.get(2)?,
                        last_poll_at: row.get::<_, Option<i64>>(3)?.map(|v| v.max(0) as u64),
                        last_success_at: row.get::<_, Option<i64>>(4)?.map(|v| v.max(0) as u64),
                        next_attempt_at: row.get::<_, Option<i64>>(5)?.map(|v| v.max(0) as u64),
                        last_error: row.get(6)?,
                        consecutive_failures: row.get::<_, i64>(7)?.max(0) as u32,
                        baseline_established: row.get::<_, i64>(8)? != 0,
                    })
                },
            )
            .optional()
            .map_err(|error| CoreError::Storage(format!("read RSS feed state: {error}")))?;
        Ok(value)
    }

    pub fn save_feed_state(&self, state: &RssFeedState) -> Result<()> {
        let connection = self.open()?;
        connection
            .execute(
                "INSERT INTO feed_state(
                    subscription_id, etag, last_modified, last_poll_at, last_success_at,
                    next_attempt_at, last_error, consecutive_failures, baseline_established
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(subscription_id) DO UPDATE SET
                    etag = excluded.etag,
                    last_modified = excluded.last_modified,
                    last_poll_at = excluded.last_poll_at,
                    last_success_at = excluded.last_success_at,
                    next_attempt_at = excluded.next_attempt_at,
                    last_error = excluded.last_error,
                    consecutive_failures = excluded.consecutive_failures,
                    baseline_established = excluded.baseline_established",
                params![
                    &state.subscription_id,
                    &state.etag,
                    &state.last_modified,
                    state.last_poll_at.map(|v| v as i64),
                    state.last_success_at.map(|v| v as i64),
                    state.next_attempt_at.map(|v| v as i64),
                    &state.last_error,
                    i64::from(state.consecutive_failures),
                    i64::from(state.baseline_established),
                ],
            )
            .map_err(|error| CoreError::Storage(format!("write RSS feed state: {error}")))?;
        self.checkpoint(connection)
    }

    pub fn listing(&self, subscription_id: &str, entry_key: &str) -> Result<Option<StoredListing>> {
        let connection = self.open_read_only()?;
        let value = connection
            .query_row(
                "SELECT id, subscription_id, entry_key, entry_json, torrent_keys, status, reason,
                        first_seen_at, last_seen_at, last_decision_at, retry_count,
                        next_attempt_at, dismissed, imported_at, completed_at
                 FROM listings WHERE subscription_id = ?1 AND entry_key = ?2",
                params![subscription_id, entry_key],
                decode_listing,
            )
            .optional()
            .map_err(|error| CoreError::Storage(format!("read RSS listing: {error}")))?;
        Ok(value)
    }

    pub fn pending_retries(
        &self,
        subscription_id: &str,
        now: u64,
        limit: usize,
    ) -> Result<Vec<StoredListing>> {
        let connection = self.open_read_only()?;
        let mut statement = connection
            .prepare(
                "SELECT id, subscription_id, entry_key, entry_json, torrent_keys, status, reason,
                        first_seen_at, last_seen_at, last_decision_at, retry_count,
                        next_attempt_at, dismissed, imported_at, completed_at
                 FROM listings
                 WHERE subscription_id = ?1 AND status IN ('failed', 'deferred')
                   AND dismissed = 0 AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?2
                 ORDER BY next_attempt_at ASC, id ASC LIMIT ?3",
            )
            .map_err(|error| CoreError::Storage(format!("prepare RSS retry read: {error}")))?;
        let rows = statement
            .query_map(
                params![subscription_id, now as i64, limit as i64],
                decode_listing,
            )
            .map_err(|error| CoreError::Storage(format!("read RSS retries: {error}")))?;
        let mut listings = Vec::new();
        for row in rows {
            listings.push(
                row.map_err(|error| CoreError::Storage(format!("decode RSS retry: {error}")))?,
            );
        }
        Ok(listings)
    }

    /// Listings eligible for an explicit reconsider operation: discovered
    /// entries that were never successfully imported.
    pub fn reconsiderable_listings(
        &self,
        subscription_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredListing>> {
        let connection = self.open_read_only()?;
        let mut statement = connection
            .prepare(
                "SELECT id, subscription_id, entry_key, entry_json, torrent_keys, status, reason,
                        first_seen_at, last_seen_at, last_decision_at, retry_count,
                        next_attempt_at, dismissed, imported_at, completed_at
                 FROM listings
                 WHERE subscription_id = ?1 AND imported_at IS NULL
                 ORDER BY last_decision_at DESC, id DESC LIMIT ?2",
            )
            .map_err(|error| CoreError::Storage(format!("prepare RSS reconsider read: {error}")))?;
        let rows = statement
            .query_map(params![subscription_id, limit as i64], decode_listing)
            .map_err(|error| CoreError::Storage(format!("read RSS reconsider set: {error}")))?;
        let mut listings = Vec::new();
        for row in rows {
            listings.push(
                row.map_err(|error| CoreError::Storage(format!("decode RSS listing: {error}")))?,
            );
        }
        Ok(listings)
    }

    /// Check whether any full identity alias already has verified completion.
    pub fn completed_alias(&self, aliases: &[String]) -> Result<Option<String>> {
        if aliases.is_empty() {
            return Ok(None);
        }
        let connection = self.open_read_only()?;
        for alias in aliases {
            let canonical: Option<String> = connection
                .query_row(
                    "SELECT canonical_key FROM completion_aliases WHERE alias_key = ?1",
                    params![alias],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| CoreError::Storage(format!("lookup RSS completion: {error}")))?;
            if canonical.is_some() {
                return Ok(canonical);
            }
        }
        Ok(None)
    }

    /// Check whether another feed already imported the same torrent.
    pub fn imported_alias(&self, aliases: &[String]) -> Result<Option<String>> {
        if aliases.is_empty() {
            return Ok(None);
        }
        let connection = self.open_read_only()?;
        for alias in aliases {
            let owner: Option<String> = connection
                .query_row(
                    "SELECT canonical_key FROM listing_identities WHERE alias_key = ?1",
                    params![alias],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| {
                    CoreError::Storage(format!("lookup RSS listing identity: {error}"))
                })?;
            if let Some(canonical) = owner {
                let imported: Option<i64> = connection
                    .query_row(
                        "SELECT imported_at FROM listings
                         WHERE subscription_id = (SELECT subscription_id FROM listing_identities WHERE alias_key = ?1)
                           AND entry_key = (SELECT entry_key FROM listing_identities WHERE alias_key = ?1)
                           AND imported_at IS NOT NULL AND completed_at IS NULL AND dismissed = 0",
                        params![alias],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| CoreError::Storage(format!("lookup RSS import owner: {error}")))?;
                if imported.is_some() {
                    return Ok(Some(canonical));
                }
            }
        }
        Ok(None)
    }

    /// Persist one poll decision. Returns the decision-history row id.
    #[allow(clippy::too_many_arguments)]
    pub fn record_decision(
        &self,
        subscription_id: &str,
        entry: &RssEntry,
        entry_key: &str,
        action: RssAction,
        reason: &str,
        torrent_keys: &[String],
        now: u64,
        retry_at: Option<u64>,
        dismissed: bool,
        imported: bool,
    ) -> Result<i64> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| CoreError::Storage(format!("begin RSS decision: {error}")))?;
        let entry_json = serde_json::to_vec(entry)
            .map_err(|error| CoreError::Storage(format!("encode RSS entry: {error}")))?;
        let keys_json = serde_json::to_vec(torrent_keys)
            .map_err(|error| CoreError::Storage(format!("encode RSS identities: {error}")))?;
        let category = entry.categories.first().cloned();
        let existing = transaction
            .query_row(
                "SELECT id, retry_count, first_seen_at, imported_at, completed_at
                 FROM listings WHERE subscription_id = ?1 AND entry_key = ?2",
                params![subscription_id, entry_key],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| CoreError::Storage(format!("read existing RSS listing: {error}")))?;
        let (retry_count, first_seen_at, imported_at, completed_at) = existing
            .map(|(_, retry, first, imported, completed)| {
                (retry.max(0) as u32, first, imported, completed)
            })
            .unwrap_or((0, now as i64, None, None));
        let retry_count = if action == RssAction::Failed {
            retry_count.saturating_add(1)
        } else if action == RssAction::Import {
            0
        } else {
            retry_count
        };
        let imported_at = if imported {
            Some(imported_at.unwrap_or(now as i64))
        } else {
            imported_at
        };
        let completed_at = if action == RssAction::PreviouslyCompleted {
            Some(completed_at.unwrap_or(now as i64))
        } else {
            completed_at
        };
        let status = decision_status(action);
        transaction
            .execute(
                "INSERT INTO listings(
                    subscription_id, entry_key, guid, title, link, category, pub_date, entry_json,
                    torrent_keys, status, reason, first_seen_at, last_seen_at, last_decision_at,
                    retry_count, next_attempt_at, dismissed, imported_at, completed_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?12, ?13, ?14, ?15, ?16, ?17)
                 ON CONFLICT(subscription_id, entry_key) DO UPDATE SET
                    guid = excluded.guid,
                    title = excluded.title,
                    link = excluded.link,
                    category = excluded.category,
                    pub_date = excluded.pub_date,
                    entry_json = excluded.entry_json,
                    torrent_keys = excluded.torrent_keys,
                    status = excluded.status,
                    reason = excluded.reason,
                    last_seen_at = excluded.last_seen_at,
                    last_decision_at = excluded.last_decision_at,
                    retry_count = excluded.retry_count,
                    next_attempt_at = excluded.next_attempt_at,
                    dismissed = excluded.dismissed,
                    imported_at = excluded.imported_at,
                    completed_at = excluded.completed_at",
                params![
                    subscription_id,
                    entry_key,
                    &entry.guid,
                    &entry.title,
                    &entry.link,
                    &category,
                    &entry.pub_date,
                    entry_json,
                    keys_json,
                    status,
                    reason,
                    first_seen_at,
                    retry_count as i64,
                    retry_at.map(|value| value as i64),
                    i64::from(dismissed),
                    imported_at,
                    completed_at,
                ],
            )
            .map_err(|error| CoreError::Storage(format!("write RSS listing: {error}")))?;
        for alias in torrent_keys {
            transaction
                .execute(
                    "INSERT OR IGNORE INTO listing_identities(
                        alias_key, canonical_key, subscription_id, entry_key, first_seen_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        alias,
                        torrent_keys.first(),
                        subscription_id,
                        entry_key,
                        now as i64
                    ],
                )
                .map_err(|error| CoreError::Storage(format!("write RSS identity: {error}")))?;
        }
        transaction
            .execute(
                "INSERT INTO decisions(
                    subscription_id, entry_key, title, action, reason, torrent_keys, decided_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    subscription_id,
                    entry_key,
                    &entry.title,
                    action.as_str(),
                    reason,
                    keys_json,
                    now as i64,
                ],
            )
            .map_err(|error| CoreError::Storage(format!("write RSS decision: {error}")))?;
        let decision_id = transaction.last_insert_rowid();
        transaction
            .commit()
            .map_err(|error| CoreError::Storage(format!("commit RSS decision: {error}")))?;
        self.checkpoint(connection)?;
        Ok(decision_id)
    }

    /// Record discovery without changing an existing terminal decision. Used
    /// by repeated unchanged polls so listings gain no extra rows.
    pub fn touch_listing(
        &self,
        subscription_id: &str,
        entry: &RssEntry,
        entry_key: &str,
        now: u64,
    ) -> Result<()> {
        let connection = self.open()?;
        let entry_json = serde_json::to_vec(entry)
            .map_err(|error| CoreError::Storage(format!("encode RSS entry: {error}")))?;
        let category = entry.categories.first().cloned();
        connection
            .execute(
                "INSERT INTO listings(
                    subscription_id, entry_key, guid, title, link, category, pub_date, entry_json,
                    torrent_keys, status, reason, first_seen_at, last_seen_at, last_decision_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, '[]', 'baseline', 'first observed', ?9, ?9, ?9)
                 ON CONFLICT(subscription_id, entry_key) DO UPDATE SET
                    guid = excluded.guid,
                    title = excluded.title,
                    link = excluded.link,
                    category = excluded.category,
                    pub_date = excluded.pub_date,
                    entry_json = excluded.entry_json,
                    last_seen_at = excluded.last_seen_at",
                params![
                    subscription_id,
                    entry_key,
                    &entry.guid,
                    &entry.title,
                    &entry.link,
                    &category,
                    &entry.pub_date,
                    entry_json,
                    now as i64,
                ],
            )
            .map_err(|error| CoreError::Storage(format!("touch RSS listing: {error}")))?;
        self.checkpoint(connection)
    }

    /// Explicit operator retry clears dismissal for one or all listings.
    pub fn clear_dismissal(
        &self,
        subscription_id: &str,
        entry_key: Option<&str>,
        now: u64,
    ) -> Result<usize> {
        let connection = self.open()?;
        let changed = match entry_key {
            Some(key) => connection.execute(
                "UPDATE listings SET status = 'failed', reason = 'explicit retry requested',
                    dismissed = 0, retry_count = 0, next_attempt_at = ?3,
                    imported_at = CASE WHEN completed_at IS NULL THEN NULL ELSE imported_at END
                 WHERE subscription_id = ?1 AND entry_key = ?2 AND completed_at IS NULL",
                params![subscription_id, key, now as i64],
            ),
            None => connection.execute(
                "UPDATE listings SET status = 'failed', reason = 'explicit retry requested',
                    dismissed = 0, retry_count = 0, next_attempt_at = ?2,
                    imported_at = CASE WHEN completed_at IS NULL THEN NULL ELSE imported_at END
                 WHERE subscription_id = ?1 AND completed_at IS NULL",
                params![subscription_id, now as i64],
            ),
        }
        .map_err(|error| CoreError::Storage(format!("clear RSS dismissal: {error}")))?;
        self.checkpoint(connection)?;
        Ok(changed)
    }

    /// Apply one durable library event. This operation is idempotent.
    pub fn apply_event(&self, event: &PendingLibraryEvent) -> Result<()> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| CoreError::Storage(format!("begin RSS event apply: {error}")))?;
        if event.kind == PendingEventKind::Completed || event.was_completed {
            let mut aliases = event.aliases.clone();
            if !aliases.iter().any(|alias| alias == &event.canonical_key) {
                aliases.push(event.canonical_key.clone());
            }
            transaction
                .execute(
                    "INSERT INTO completions(canonical_key, first_completed_at, last_observed_at, source)
                     VALUES (?1, ?2, ?2, 'library')
                     ON CONFLICT(canonical_key) DO UPDATE SET
                        last_observed_at = max(completions.last_observed_at, excluded.last_observed_at)",
                    params![&event.canonical_key, event.occurred_at as i64],
                )
                .map_err(|error| CoreError::Storage(format!("record RSS completion: {error}")))?;
            for alias in &aliases {
                transaction
                    .execute(
                        "INSERT OR IGNORE INTO completion_aliases(alias_key, canonical_key)
                         VALUES (?1, ?2)",
                        params![alias, &event.canonical_key],
                    )
                    .map_err(|error| {
                        CoreError::Storage(format!("record RSS completion alias: {error}"))
                    })?;
            }
            for alias in &aliases {
                transaction
                    .execute(
                        "UPDATE listings SET completed_at = coalesce(completed_at, ?2)
                         WHERE (subscription_id, entry_key) IN (
                            SELECT subscription_id, entry_key FROM listing_identities WHERE alias_key = ?1
                         )",
                        params![alias, event.occurred_at as i64],
                    )
                    .map_err(|error| CoreError::Storage(format!("mark RSS listing completion: {error}")))?;
            }
        }
        if event.kind == PendingEventKind::Removed && !event.was_completed {
            for alias in event.identities() {
                transaction
                    .execute(
                        "UPDATE listings SET dismissed = 1, status = 'dismissed',
                            reason = 'removed from library before completion; explicit retry required',
                            next_attempt_at = NULL
                         WHERE (subscription_id, entry_key) IN (
                            SELECT subscription_id, entry_key FROM listing_identities WHERE alias_key = ?1
                         ) AND imported_at IS NOT NULL AND completed_at IS NULL",
                        params![alias],
                    )
                    .map_err(|error| CoreError::Storage(format!("dismiss RSS listing: {error}")))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| CoreError::Storage(format!("commit RSS event apply: {error}")))?;
        self.checkpoint(connection)
    }

    pub fn counts(&self, subscription_id: &str) -> Result<RssCounts> {
        let connection = self.open_read_only()?;
        let mut counts = RssCounts::default();
        let mut statement = connection
            .prepare(
                "SELECT status, COUNT(*) FROM listings
                 WHERE subscription_id = ?1 GROUP BY status",
            )
            .map_err(|error| CoreError::Storage(format!("prepare RSS counts: {error}")))?;
        let rows = statement
            .query_map(params![subscription_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|error| CoreError::Storage(format!("read RSS counts: {error}")))?;
        for row in rows {
            let (status, count) =
                row.map_err(|error| CoreError::Storage(format!("decode RSS count: {error}")))?;
            counts.listings = counts.listings.saturating_add(count);
            match status.as_str() {
                "import" | "imported" => counts.imported = counts.imported.saturating_add(count),
                "already_present" => {
                    counts.already_present = counts.already_present.saturating_add(count)
                }
                "previously_completed" | "completed" => {
                    counts.previously_completed = counts.previously_completed.saturating_add(count)
                }
                "filtered" => counts.filtered = counts.filtered.saturating_add(count),
                "dismissed" => counts.dismissed = counts.dismissed.saturating_add(count),
                "unsupported" => counts.unsupported = counts.unsupported.saturating_add(count),
                "failed" => counts.failed = counts.failed.saturating_add(count),
                "deferred" => {}
                "baseline" => counts.baseline = counts.baseline.saturating_add(count),
                _ => {}
            }
        }
        counts.completed = count_completed(&connection, subscription_id)?;
        counts.pending_retry = counts
            .pending_retry
            .saturating_add(count_pending_retry(&connection, subscription_id)?);
        Ok(counts)
    }

    pub fn decision_page(
        &self,
        subscription_id: &str,
        limit: usize,
        before: Option<i64>,
    ) -> Result<RssDecisionPage> {
        let limit = limit.clamp(1, 500);
        let connection = self.open_read_only()?;
        let mut statement = connection
            .prepare(
                "SELECT id, entry_key, title, action, reason, torrent_keys, decided_at
                 FROM decisions
                 WHERE subscription_id = ?1 AND (?2 IS NULL OR id < ?2)
                 ORDER BY id DESC LIMIT ?3",
            )
            .map_err(|error| CoreError::Storage(format!("prepare RSS decision page: {error}")))?;
        let rows = statement
            .query_map(
                params![subscription_id, before, (limit + 1) as i64],
                |row| {
                    let keys: Vec<u8> = row.get(5)?;
                    let keys: Vec<String> = serde_json::from_slice(&keys).unwrap_or_default();
                    Ok(RssDecision {
                        decision_id: Some(row.get(0)?),
                        entry_key: row.get(1)?,
                        title: row.get(2)?,
                        link: None,
                        categories: Vec::new(),
                        pub_date: None,
                        action: action_from_str(&row.get::<_, String>(3)?)
                            .unwrap_or(RssAction::Failed),
                        reason: row.get(4)?,
                        torrent_keys: keys,
                        decided_at: row.get::<_, i64>(6).ok().map(|value| value.max(0) as u64),
                    })
                },
            )
            .map_err(|error| CoreError::Storage(format!("read RSS decision page: {error}")))?;
        let mut decisions = Vec::new();
        for row in rows {
            decisions.push(
                row.map_err(|error| CoreError::Storage(format!("decode RSS decision: {error}")))?,
            );
        }
        let next_before = if decisions.len() > limit {
            decisions.pop();
            decisions.last().and_then(|decision| decision.decision_id)
        } else {
            None
        };
        Ok(RssDecisionPage {
            decisions,
            next_before,
        })
    }

    /// Prune routine decision/listings rows while retaining every completion
    /// identity and every imported/completed listing attribution.
    pub fn prune(&self, decision_retention: i64, listing_retention: i64) -> Result<()> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| CoreError::Storage(format!("begin RSS prune: {error}")))?;
        transaction
            .execute(
                "DELETE FROM decisions WHERE id IN (
                    SELECT id FROM decisions d
                    WHERE (SELECT COUNT(*) FROM decisions x
                           WHERE x.subscription_id = d.subscription_id AND x.id >= d.id) > ?1
                 )",
                params![decision_retention],
            )
            .map_err(|error| CoreError::Storage(format!("prune RSS decisions: {error}")))?;
        transaction
            .execute(
                "DELETE FROM listings WHERE id IN (
                    SELECT id FROM listings l
                    WHERE l.status NOT IN ('imported', 'completed', 'previously_completed')
                      AND l.imported_at IS NULL
                      AND l.completed_at IS NULL
                      AND (SELECT COUNT(*) FROM listings x
                           WHERE x.subscription_id = l.subscription_id AND x.id >= l.id) > ?1
                 )",
                params![listing_retention],
            )
            .map_err(|error| CoreError::Storage(format!("prune RSS listings: {error}")))?;
        transaction
            .commit()
            .map_err(|error| CoreError::Storage(format!("commit RSS prune: {error}")))?;
        self.checkpoint(connection)
    }

    /// Explicitly clear verified completion identities. Routine pruning and
    /// feed deletion never call this.
    pub fn clear_completions(&self) -> Result<usize> {
        let connection = self.open()?;
        let count = connection
            .execute("DELETE FROM completion_aliases", [])
            .map_err(|error| {
                CoreError::Storage(format!("clear RSS completion aliases: {error}"))
            })?;
        connection
            .execute("DELETE FROM completions", [])
            .map_err(|error| CoreError::Storage(format!("clear RSS completions: {error}")))?;
        self.checkpoint(connection)?;
        Ok(count)
    }

    pub fn completion(&self, canonical_key: &str) -> Result<Option<CompletionRecord>> {
        let connection = self.open_read_only()?;
        connection
            .query_row(
                "SELECT canonical_key, first_completed_at, last_observed_at, source
                 FROM completions WHERE canonical_key = ?1",
                params![canonical_key],
                |row| {
                    Ok(CompletionRecord {
                        canonical_key: row.get(0)?,
                        first_completed_at: row.get::<_, i64>(1)?.max(0) as u64,
                        last_observed_at: row.get::<_, i64>(2)?.max(0) as u64,
                        source: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(|error| CoreError::Storage(format!("read RSS completion: {error}")))
    }

    /// Diagnostic used by scale tests: confirm that alias/entry lookups use
    /// indexed plans rather than scanning the history.
    pub fn duplicate_lookup_uses_index(&self) -> Result<bool> {
        let connection = self.open_read_only()?;
        let completion_plan: String = connection
            .query_row(
                "EXPLAIN QUERY PLAN SELECT canonical_key FROM completion_aliases WHERE alias_key = 'x'",
                [],
                |row| row.get(3),
            )
            .map_err(|error| CoreError::Storage(format!("explain RSS completion lookup: {error}")))?;
        let listing_plan: String = connection
            .query_row(
                "EXPLAIN QUERY PLAN SELECT canonical_key FROM listing_identities WHERE alias_key = 'x'",
                [],
                |row| row.get(3),
            )
            .map_err(|error| CoreError::Storage(format!("explain RSS listing lookup: {error}")))?;
        Ok(completion_plan.to_ascii_lowercase().contains("index")
            && listing_plan.to_ascii_lowercase().contains("index"))
    }
}

fn decode_listing(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredListing> {
    let entry_json: Vec<u8> = row.get(3)?;
    let entry: RssEntry = serde_json::from_slice(&entry_json).unwrap_or_default();
    let torrent_keys: Vec<String> =
        serde_json::from_slice(&row.get::<_, Vec<u8>>(4)?).unwrap_or_default();
    Ok(StoredListing {
        id: row.get(0)?,
        subscription_id: row.get(1)?,
        entry_key: row.get(2)?,
        entry,
        torrent_keys,
        status: row.get(5)?,
        reason: row.get(6)?,
        first_seen_at: row.get::<_, i64>(7)?.max(0) as u64,
        last_seen_at: row.get::<_, i64>(8)?.max(0) as u64,
        last_decision_at: row.get::<_, i64>(9)?.max(0) as u64,
        retry_count: row.get::<_, i64>(10)?.max(0) as u32,
        next_attempt_at: row.get::<_, Option<i64>>(11)?.map(|v| v.max(0) as u64),
        dismissed: row.get::<_, i64>(12)? != 0,
        imported_at: row.get::<_, Option<i64>>(13)?.map(|v| v.max(0) as u64),
        completed_at: row.get::<_, Option<i64>>(14)?.map(|v| v.max(0) as u64),
    })
}

fn decision_status(action: RssAction) -> &'static str {
    match action {
        RssAction::Import => "imported",
        RssAction::AlreadyPresent => "already_present",
        RssAction::PreviouslyCompleted => "previously_completed",
        RssAction::Filtered => "filtered",
        RssAction::Dismissed => "dismissed",
        RssAction::Unsupported => "unsupported",
        RssAction::Failed => "failed",
        RssAction::Deferred => "deferred",
        RssAction::Baseline => "baseline",
    }
}

fn action_from_str(value: &str) -> Option<RssAction> {
    match value {
        "import" | "imported" => Some(RssAction::Import),
        "already_present" => Some(RssAction::AlreadyPresent),
        "previously_completed" | "completed" => Some(RssAction::PreviouslyCompleted),
        "filtered" => Some(RssAction::Filtered),
        "dismissed" => Some(RssAction::Dismissed),
        "unsupported" => Some(RssAction::Unsupported),
        "failed" => Some(RssAction::Failed),
        "deferred" => Some(RssAction::Deferred),
        "baseline" => Some(RssAction::Baseline),
        _ => None,
    }
}

fn count_completed(connection: &Connection, subscription_id: &str) -> Result<u64> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM listings WHERE subscription_id = ?1 AND completed_at IS NOT NULL",
            params![subscription_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value.max(0) as u64)
        .map_err(|error| CoreError::Storage(format!("count RSS completions: {error}")))
}

fn count_pending_retry(connection: &Connection, subscription_id: &str) -> Result<u64> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM listings
             WHERE subscription_id = ?1 AND status IN ('failed', 'deferred')
               AND dismissed = 0 AND next_attempt_at IS NOT NULL",
            params![subscription_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value.max(0) as u64)
        .map_err(|error| CoreError::Storage(format!("count RSS retries: {error}")))
}

fn restrict_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, permissions).map_err(|error| {
            CoreError::Storage(format!(
                "restrict RSS history database permissions: {error}"
            ))
        })?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
