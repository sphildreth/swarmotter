// SPDX-License-Identifier: Apache-2.0

//! RSS subscription polling, intake, preview, and durable-history handoff.
//!
//! Feed fetches and torrent attachments use the shared contained HTTP client.
//! Matching is pure (`swarmotter_core::rss`), intake uses the existing add
//! mutation, and every library completion/removal is captured in the main
//! database outbox before it is applied idempotently to the RSS database.

use super::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use swarmotter_core::hash::TorrentKey;
use swarmotter_core::magnet::Magnet;
use swarmotter_core::rss::{
    entry_offers, parse_feed, redact_feed_url, redact_urls_in_text, retry_backoff_seconds,
    RssAction, RssConfig, RssDecision, RssDecisionPage, RssEntry, RssFeedState, RssOffer,
    RssPollResult, RssPreview, RssSubscription, RssSubscriptionStatus,
};
use swarmotter_core::torrent::Torrent;

use crate::rss_store::{DuplicateLookup, PendingEventKind, PendingLibraryEvent, RssStore};

const RSS_OUTBOX_BATCH: usize = 512;
const RSS_RETRY_BATCH: usize = 32;

#[derive(Debug, Default)]
struct ResolvedOffer {
    aliases: Vec<String>,
    magnet: Option<String>,
    torrent_bytes: Option<Vec<u8>>,
    errors: Vec<String>,
    transient_failure: bool,
}

impl DaemonRuntime {
    /// Resolve the shared RSS history store, initializing it on first use and
    /// following a configuration change to a different path.
    pub(super) async fn rss_history_store(&self) -> Result<Arc<RssStore>> {
        let config = self.config.read().await.rss.clone();
        let path = config
            .history_path(self.state_path.as_deref())?
            .ok_or_else(|| {
                CoreError::InvalidConfig(
                    "RSS history database is unavailable: configure rss.history_file or a state file"
                        .into(),
                )
            })?;
        {
            let cached = self.rss_store.read().await;
            if let Some((cached_path, store)) = cached.as_ref() {
                if cached_path == &path {
                    return Ok(store.clone());
                }
            }
        }
        let path_for_task = path.clone();
        let store = tokio::task::spawn_blocking(move || {
            let store = RssStore::new(path_for_task);
            store.initialize()?;
            Ok::<_, CoreError>(Arc::new(store))
        })
        .await
        .map_err(|error| CoreError::Storage(format!("open RSS history task: {error}")))??;
        let mut cached = self.rss_store.write().await;
        if let Some((cached_path, initialized)) = cached.as_ref() {
            if cached_path == &path {
                return Ok(initialized.clone());
            }
        }
        *cached = Some((path, store.clone()));
        Ok(store)
    }

    /// Queue one library event for the next transactional persistence. Exact
    /// duplicates are collapsed so a progress tick cannot enqueue the same
    /// completion repeatedly before persistence.
    pub(super) async fn queue_rss_event(&self, event: PendingLibraryEvent) {
        let mut pending = self.rss_pending_events.lock().await;
        if let Some(existing) = pending.iter_mut().find(|candidate| {
            candidate.kind == event.kind && candidate.canonical_key == event.canonical_key
        }) {
            if event.occurred_at > existing.occurred_at {
                *existing = event;
            }
            return;
        }
        pending.push(event);
    }

    async fn queue_rss_completion(&self, torrent: &Torrent) {
        let canonical = torrent.key().to_locator();
        let aliases = torrent
            .keys()
            .into_iter()
            .map(TorrentKey::to_locator)
            .collect::<Vec<_>>();
        self.queue_rss_event(PendingLibraryEvent::completion(
            canonical,
            aliases,
            torrent.meta.name.clone(),
            now(),
        ))
        .await;
    }

    /// Capture verified completion and removal evidence before a torrent
    /// record disappears. Selfish removal and every explicit removal path call
    /// this helper.
    pub(super) async fn queue_rss_removal(&self, torrent: &Torrent) {
        for event in Self::rss_removal_events(torrent) {
            self.queue_rss_event(event).await;
        }
    }

    pub(super) fn rss_removal_events(torrent: &Torrent) -> Vec<PendingLibraryEvent> {
        let completed = torrent.date_completed.is_some()
            || matches!(
                torrent.state,
                TorrentState::Completed | TorrentState::Seeding
            )
            || torrent.progress.is_complete();
        let key = torrent.key().to_locator();
        let aliases: Vec<_> = torrent
            .keys()
            .into_iter()
            .map(TorrentKey::to_locator)
            .collect();
        let name = torrent.meta.name.clone();
        let timestamp = now();
        let mut events = Vec::with_capacity(if completed { 2 } else { 1 });
        if completed {
            events.push(PendingLibraryEvent::completion(
                key.clone(),
                aliases.clone(),
                name.clone(),
                timestamp,
            ));
        }
        events.push(PendingLibraryEvent::removal(
            key, aliases, name, completed, timestamp,
        ));
        events
    }

    /// Capture a verified completion transition once. The event is persisted
    /// with the library change by the next state transaction.
    pub(super) async fn queue_rss_verified_completion(&self, torrent: &Torrent) {
        if torrent.date_completed.is_some() || torrent.progress.is_complete() {
            self.queue_rss_completion(torrent).await;
        }
    }

    /// Apply all pending outbox events idempotently, then acknowledge only the
    /// rows whose history application committed.
    pub(super) async fn apply_rss_outbox_once(&self) -> Result<usize> {
        let Some(path) = self.state_path.clone() else {
            return Ok(0);
        };
        let _state_guard = self.state_write_lock.lock().await;
        let store = self.rss_history_store().await?;
        let events = tokio::task::spawn_blocking({
            let path = path.clone();
            move || crate::state_store::load_outbox_events(&path, RSS_OUTBOX_BATCH)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("read RSS outbox task: {error}")))??;
        if events.is_empty() {
            return Ok(0);
        }
        let mut applied_ids = Vec::new();
        for event in &events {
            let store = store.clone();
            let event_clone = event.clone();
            tokio::task::spawn_blocking(move || store.apply_event(&event_clone))
                .await
                .map_err(|error| CoreError::Storage(format!("apply RSS event task: {error}")))??;
            if let Some(id) = event.id {
                applied_ids.push(id);
            }
        }
        if !applied_ids.is_empty() {
            tokio::task::spawn_blocking(move || {
                crate::state_store::acknowledge_outbox_events(&path, &applied_ids)
            })
            .await
            .map_err(|error| CoreError::Storage(format!("ack RSS outbox task: {error}")))??;
        }
        Ok(events.len())
    }

    /// Best-effort immediate outbox drain after a successful save.
    pub(super) fn wake_rss_outbox(&self) {
        if self.state_path.is_none() {
            return;
        }
        let runtime = self.clone();
        tokio::spawn(async move {
            for _ in 0..32 {
                match runtime.apply_rss_outbox_once().await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "RSS history handoff deferred; outbox retained"
                        );
                        break;
                    }
                }
            }
        });
    }

    /// Apply events that were committed by a previous process generation
    /// immediately after state restore.
    pub(super) async fn recover_rss_outbox(&self) {
        if self.state_path.is_none() {
            return;
        }
        self.apply_rss_outbox_best_effort("restore").await;
    }

    async fn apply_rss_outbox_best_effort(&self, reason: &'static str) {
        match self.apply_rss_outbox_once().await {
            Ok(0) => {}
            Ok(count) => tracing::info!(count, reason, "applied RSS history outbox"),
            Err(error) => tracing::warn!(
                reason,
                error = %error,
                error_code = %error.code(),
                "RSS history unavailable; automatic imports will be deferred"
            ),
        }
    }

    /// Background loop: apply the outbox and poll due subscriptions under the
    /// configured concurrency bound.
    pub async fn rss_loop(self: Arc<Self>) {
        // Give restore a moment to finish before the first automatic poll.
        tokio::time::sleep(Duration::from_secs(2)).await;
        self.recover_rss_outbox().await;
        self.backfill_rss_completion_evidence().await;
        while !self.is_stopping() {
            self.apply_rss_outbox_best_effort("rss_loop").await;
            let config = self.config.read().await.rss.clone();
            let now = now();
            let mut due = Vec::new();
            for subscription in &config.subscriptions {
                if !subscription.enabled {
                    continue;
                }
                let state = match self.rss_history_store().await {
                    Ok(store) => {
                        let id = subscription.id.clone();
                        tokio::task::spawn_blocking(move || store.feed_state(&id))
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .flatten()
                    }
                    Err(_) => None,
                };
                let due_now = match state {
                    Some(state) => {
                        state.next_attempt_at.is_some_and(|at| at <= now)
                            || state
                                .last_poll_at
                                .map(|at| {
                                    at.saturating_add(subscription.poll_interval_seconds) <= now
                                })
                                .unwrap_or(true)
                    }
                    None => true,
                };
                if due_now {
                    due.push(subscription.id.clone());
                }
            }
            let concurrency = config.max_concurrent_polls.max(1);
            futures_util::stream::iter(due)
                .for_each_concurrent(concurrency, |id| {
                    let runtime = self.clone();
                    async move {
                        if let Err(error) = runtime.poll_rss_subscription(&id, false, false).await {
                            tracing::warn!(
                                subscription = %id,
                                error = %error,
                                error_code = %error.code(),
                                "RSS poll incomplete; eligible work retained"
                            );
                        }
                    }
                })
                .await;
            if let Ok(store) = self.rss_history_store().await {
                let config = self.config.read().await.rss.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    store.prune(config.decision_retention, config.listing_retention)
                })
                .await;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    /// One poll, serialized per subscription. `import_current` is the explicit
    /// "import current matches" action; normal automatic polls use the
    /// configured first-poll behavior.
    pub(super) async fn poll_rss_subscription(
        &self,
        subscription_id: &str,
        import_current: bool,
        reconsider: bool,
    ) -> Result<RssPollResult> {
        let config = self.config.read().await.rss.clone();
        let subscription = config
            .subscription(subscription_id)
            .cloned()
            .ok_or_else(|| CoreError::NotFound(format!("rss subscription {subscription_id}")))?;
        let lock = {
            let mut locks = self.rss_poll_locks.lock().await;
            locks
                .entry(subscription.id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _poll_guard = lock.lock().await;
        self.rss_polls_in_progress
            .write()
            .await
            .insert(subscription.id.clone());
        let result = self
            .poll_rss_subscription_inner(&config, &subscription, import_current, reconsider)
            .await;
        self.rss_polls_in_progress
            .write()
            .await
            .remove(&subscription.id);
        result
    }

    async fn poll_rss_subscription_inner(
        &self,
        config: &RssConfig,
        subscription: &RssSubscription,
        import_current: bool,
        reconsider: bool,
    ) -> Result<RssPollResult> {
        let store = self.rss_history_store().await?;
        let store_for_state = store.clone();
        let id = subscription.id.clone();
        let mut state = tokio::task::spawn_blocking(move || store_for_state.feed_state(&id))
            .await
            .map_err(|error| CoreError::Storage(format!("read RSS feed state task: {error}")))??;
        let mut state = state
            .take()
            .unwrap_or_else(|| RssFeedState::new(&subscription.id));
        let started_at = now();
        state.last_poll_at = Some(started_at);

        if reconsider {
            return self
                .reconsider_rss_listings(config, subscription, &store)
                .await;
        }

        let binder = self.make_binder().await;
        let client = swarmotter_core::net::ContainedHttpClient::with_data_timeout(
            binder.as_ref(),
            Duration::from_secs(config.request_timeout_seconds),
        );
        let fetch = client
            .get_feed(
                &subscription.url,
                state.etag.as_deref(),
                state.last_modified.as_deref(),
                config.max_feed_bytes,
            )
            .await;
        let response = match fetch {
            Ok(response) => response,
            Err(error) => {
                let failures = state.consecutive_failures.saturating_add(1);
                state.consecutive_failures = failures;
                let safe_error = redact_urls_in_text(&error.to_string());
                state.last_error = Some(safe_error.clone());
                state.next_attempt_at = retry_backoff_seconds(
                    failures,
                    config.retry_base_seconds,
                    config.retry_max_seconds,
                    config.max_retries,
                )
                .map(|seconds| started_at.saturating_add(seconds));
                let state_to_save = state.clone();
                let store_for_save = store.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    store_for_save.save_feed_state(&state_to_save)
                })
                .await;
                tracing::warn!(
                    subscription = %subscription.id,
                    url = %redact_feed_url(&subscription.url),
                    error = %safe_error,
                    error_code = %error.code(),
                    "RSS feed fetch failed; eligible work retained"
                );
                return Ok(RssPollResult {
                    subscription_id: subscription.id.clone(),
                    success: false,
                    baseline_established: state.baseline_established,
                    conditional_not_modified: false,
                    decisions: Vec::new(),
                    imported: Vec::new(),
                    error: Some(safe_error),
                });
            }
        };

        if !(200..300).contains(&response.status) && response.status != 304 {
            let failures = state.consecutive_failures.saturating_add(1);
            state.consecutive_failures = failures;
            let safe_error = format!(
                "feed {} returned HTTP {}",
                redact_feed_url(&response.final_url),
                response.status
            );
            state.last_error = Some(safe_error.clone());
            let calculated = retry_backoff_seconds(
                failures,
                config.retry_base_seconds,
                config.retry_max_seconds,
                config.max_retries,
            );
            let server_backoff = response
                .retry_after_seconds
                .map(|seconds| seconds.min(config.retry_max_seconds));
            state.next_attempt_at = match (calculated, server_backoff) {
                (Some(calculated), Some(server)) => {
                    Some(started_at.saturating_add(calculated.max(server)))
                }
                (Some(calculated), None) => Some(started_at.saturating_add(calculated)),
                (None, Some(server)) => Some(started_at.saturating_add(server)),
                (None, None) => None,
            };
            let state_to_save = state.clone();
            let store_for_save = store.clone();
            let _ =
                tokio::task::spawn_blocking(move || store_for_save.save_feed_state(&state_to_save))
                    .await;
            tracing::warn!(
                subscription = %subscription.id,
                url = %redact_feed_url(&subscription.url),
                status = response.status,
                error = %safe_error,
                "RSS feed returned an error status; eligible work retained"
            );
            return Ok(RssPollResult {
                subscription_id: subscription.id.clone(),
                success: false,
                baseline_established: state.baseline_established,
                conditional_not_modified: false,
                decisions: Vec::new(),
                imported: Vec::new(),
                error: Some(safe_error),
            });
        }

        let conditional_not_modified = response.status == 304;
        // A 304 advances the poll clock but still has durable attachment retries
        // to process. Only fresh feed entries need parsing.
        let feed = if conditional_not_modified {
            Ok(Default::default())
        } else {
            parse_feed(&response.body, config.max_feed_bytes, config.max_entries)
        };
        let feed = match feed {
            Ok(feed) => feed,
            Err(error) => {
                let failures = state.consecutive_failures.saturating_add(1);
                state.consecutive_failures = failures;
                let safe_error = redact_urls_in_text(&error.to_string());
                state.last_error = Some(safe_error.clone());
                state.next_attempt_at = retry_backoff_seconds(
                    failures,
                    config.retry_base_seconds,
                    config.retry_max_seconds,
                    config.max_retries,
                )
                .map(|seconds| started_at.saturating_add(seconds));
                let state_to_save = state.clone();
                let store_for_save = store.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    store_for_save.save_feed_state(&state_to_save)
                })
                .await;
                return Ok(RssPollResult {
                    subscription_id: subscription.id.clone(),
                    success: false,
                    baseline_established: state.baseline_established,
                    conditional_not_modified: false,
                    decisions: Vec::new(),
                    imported: Vec::new(),
                    error: Some(format!("RSS parse failed: {error}")),
                });
            }
        };

        let establishing_baseline = !state.baseline_established;
        let effective_import_current = import_current
            || (establishing_baseline
                && subscription.import_mode == swarmotter_core::rss::RssImportMode::CurrentMatches);
        let mut decisions = Vec::new();
        let mut imported = Vec::new();
        let mut seen_entries = HashSet::new();

        for entry in &feed.entries {
            let entry_key = entry.identity();
            if !seen_entries.insert(entry_key.clone()) {
                continue;
            }
            let existing = {
                let store = store.clone();
                let id = subscription.id.clone();
                let key = entry_key.clone();
                tokio::task::spawn_blocking(move || store.listing(&id, &key))
                    .await
                    .map_err(|error| {
                        CoreError::Storage(format!("read RSS listing task: {error}"))
                    })??
            };
            // A changed feed may still contain a failed listing. Respect the
            // same deadline and retry budget as retries from the stored feed.
            if !import_current
                && existing.as_ref().is_some_and(|listing| {
                    matches!(listing.action(), RssAction::Failed | RssAction::Deferred)
                        && listing.next_attempt_at.is_none_or(|at| at > started_at)
                })
            {
                continue;
            }
            let decision = self
                .process_rss_entry(
                    config,
                    subscription,
                    &store,
                    entry,
                    &entry_key,
                    existing.as_ref(),
                    establishing_baseline,
                    effective_import_current,
                    false,
                    &mut imported,
                )
                .await?;
            decisions.push(decision);
        }

        // Retry transient failures even when the listing has left the feed.
        let retries = {
            let store = store.clone();
            let id = subscription.id.clone();
            tokio::task::spawn_blocking(move || store.pending_retries(&id, now(), RSS_RETRY_BATCH))
                .await
                .map_err(|error| CoreError::Storage(format!("read RSS retries task: {error}")))??
        };
        for listing in retries {
            if seen_entries.contains(&listing.entry_key) {
                continue;
            }
            let decision = self
                .process_rss_entry(
                    config,
                    subscription,
                    &store,
                    &listing.entry,
                    &listing.entry_key,
                    Some(&listing),
                    false,
                    false,
                    true,
                    &mut imported,
                )
                .await?;
            decisions.push(decision);
        }

        state.last_success_at = Some(now());
        state.next_attempt_at = None;
        state.last_error = None;
        state.consecutive_failures = 0;
        state.baseline_established |= !conditional_not_modified;
        if response.etag.is_some() {
            state.etag = response.etag;
        } else if response.status == 200 {
            state.etag = None;
        }
        if response.last_modified.is_some() {
            state.last_modified = response.last_modified;
        }
        let state_to_save = state.clone();
        let store_for_save = store.clone();
        tokio::task::spawn_blocking(move || store_for_save.save_feed_state(&state_to_save))
            .await
            .map_err(|error| CoreError::Storage(format!("save RSS feed state task: {error}")))??;

        let config_prune = config.clone();
        let store_for_prune = store.clone();
        let _ = tokio::task::spawn_blocking(move || {
            store_for_prune.prune(
                config_prune.decision_retention,
                config_prune.listing_retention,
            )
        })
        .await;

        Ok(RssPollResult {
            subscription_id: subscription.id.clone(),
            success: true,
            baseline_established: state.baseline_established,
            conditional_not_modified,
            decisions,
            imported,
            error: None,
        })
    }

    /// Read pending and applied completion evidence in the same state-write
    /// lane as the outbox consumer so no completion can disappear between reads.
    async fn rss_history_duplicate(
        &self,
        store: &RssStore,
        aliases: &[String],
    ) -> Result<Option<DuplicateLookup>> {
        let _state_guard = self.state_write_lock.lock().await;
        let queued = self.rss_pending_events.lock().await.iter().any(|event| {
            (event.kind == PendingEventKind::Completed || event.was_completed)
                && aliases
                    .iter()
                    .any(|key| key == &event.canonical_key || event.aliases.contains(key))
        });
        if queued {
            return Ok(Some(DuplicateLookup::Completed));
        }
        let path = self.state_path.clone();
        let store = store.clone();
        let aliases = aliases.to_vec();
        tokio::task::spawn_blocking(move || {
            if store.completed_alias(&aliases)?.is_some() {
                return Ok(Some(DuplicateLookup::Completed));
            }
            if let Some(path) = path {
                if crate::state_store::pending_completion_matches(&path, &aliases)? {
                    return Ok(Some(DuplicateLookup::Completed));
                }
            }
            Ok(store
                .imported_alias(&aliases)?
                .map(|_| DuplicateLookup::ImportedElsewhere))
        })
        .await
        .map_err(|error| CoreError::Storage(format!("check RSS duplicate task: {error}")))?
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_rss_entry(
        &self,
        config: &RssConfig,
        subscription: &RssSubscription,
        store: &RssStore,
        entry: &RssEntry,
        entry_key: &str,
        existing: Option<&crate::rss_store::StoredListing>,
        establishing_baseline: bool,
        import_current: bool,
        retrying: bool,
        imported: &mut Vec<String>,
    ) -> Result<RssDecision> {
        let now_value = now();
        let action;
        let reason;
        let mut torrent_keys;
        let mut imported_now = false;
        let mut dismissed = false;

        if let Some(existing) = existing {
            if existing.dismissed && !retrying {
                let decision = RssDecision {
                    entry_key: entry_key.to_string(),
                    title: entry.title.clone(),
                    link: entry.link.clone(),
                    categories: entry.categories.clone(),
                    pub_date: entry.pub_date.clone(),
                    action: RssAction::Dismissed,
                    reason: existing.reason.clone(),
                    torrent_keys: existing.torrent_keys.clone(),
                    decision_id: None,
                    decided_at: Some(existing.last_decision_at),
                };
                self.touch_rss_listing(store, &subscription.id, entry, entry_key, now_value)
                    .await?;
                return Ok(decision);
            }
            if existing.imported_at.is_some() || existing.completed_at.is_some() {
                let completed_in_store = matches!(
                    self.rss_history_duplicate(store, &existing.torrent_keys)
                        .await?,
                    Some(DuplicateLookup::Completed)
                );
                let (action, reason) = if existing.completed_at.is_some() || completed_in_store {
                    (
                        RssAction::PreviouslyCompleted,
                        "verified completion already recorded for this torrent identity"
                            .to_string(),
                    )
                } else if self.library_has_aliases(&existing.torrent_keys).await {
                    (
                        RssAction::AlreadyPresent,
                        "torrent is already present in the live library".to_string(),
                    )
                } else {
                    (
                        RssAction::Dismissed,
                        "previously imported torrent is no longer in the library; explicit retry required".to_string(),
                    )
                };
                return self
                    .record_rss_decision(
                        store,
                        subscription,
                        entry,
                        entry_key,
                        Some(existing),
                        action,
                        &reason,
                        &existing.torrent_keys,
                        now_value,
                        None,
                        action == RssAction::Dismissed,
                        false,
                    )
                    .await;
            }
        }

        let rule_decision = subscription.rules.evaluate(entry);
        if !rule_decision.included {
            return self
                .record_rss_decision(
                    store,
                    subscription,
                    entry,
                    entry_key,
                    existing,
                    RssAction::Filtered,
                    &rule_decision.reason,
                    &[],
                    now_value,
                    None,
                    false,
                    false,
                )
                .await;
        }

        if establishing_baseline && !import_current {
            return self
                .record_rss_decision(
                    store,
                    subscription,
                    entry,
                    entry_key,
                    existing,
                    RssAction::Baseline,
                    "first successful poll established the baseline; new-entries-only mode did not import this listing",
                    &[],
                    now_value,
                    None,
                    false,
                    false,
                )
                .await;
        }

        let (offers, page_links) = entry_offers(entry);
        if offers.is_empty() {
            let reason = if page_links.is_empty() {
                "listing has no magnet or torrent attachment".to_string()
            } else {
                format!(
                    "page-only listing: no magnet or torrent attachment (link {})",
                    page_links[0].url
                )
            };
            return self
                .record_rss_decision(
                    store,
                    subscription,
                    entry,
                    entry_key,
                    existing,
                    RssAction::Unsupported,
                    &reason,
                    &[],
                    now_value,
                    None,
                    false,
                    false,
                )
                .await;
        }

        let resolved = self
            .resolve_rss_offers(
                &offers,
                config.max_attachment_bytes,
                config.request_timeout_seconds,
            )
            .await;
        if resolved.aliases.is_empty() {
            if resolved.transient_failure {
                let retry_at = next_listing_retry(config, existing, now_value);
                return self
                    .record_rss_decision(
                        store,
                        subscription,
                        entry,
                        entry_key,
                        existing,
                        RssAction::Failed,
                        &format!(
                            "transient attachment failure: {}; eligible for retry with backoff",
                            redact_urls_in_text(&resolved.errors.join("; "))
                        ),
                        &[],
                        now_value,
                        retry_at,
                        false,
                        false,
                    )
                    .await;
            }
            let reason = if resolved.errors.is_empty() {
                "no usable torrent identity in listing".to_string()
            } else {
                format!(
                    "no valid torrent could be obtained: {}",
                    redact_urls_in_text(&resolved.errors.join("; "))
                )
            };
            return self
                .record_rss_decision(
                    store,
                    subscription,
                    entry,
                    entry_key,
                    existing,
                    RssAction::Unsupported,
                    &reason,
                    &[],
                    now_value,
                    None,
                    false,
                    false,
                )
                .await;
        }
        torrent_keys = resolved.aliases.clone();

        // Fetch attachments concurrently, then serialize the final checks,
        // intake and durable attribution across feeds.
        let _import_guard = self.rss_import_lock.lock().await;
        let live = self.first_live_library_key(&torrent_keys).await;
        let duplicate = self.rss_history_duplicate(store, &torrent_keys).await;
        let mut retry_at = None;
        let skip = match (duplicate, live) {
            (Err(error), _) => {
                retry_at = next_listing_retry(config, existing, now_value);
                Some((RssAction::Deferred, format!("RSS duplicate history unavailable: {error}")))
            }
            (Ok(Some(DuplicateLookup::Completed)), _) => Some((
                RssAction::PreviouslyCompleted,
                "verified completion recorded for this torrent identity; no second add".into(),
            )),
            (Ok(_), Some(key)) if self.library_key_is_complete(key).await => {
                let torrent = self.registry.lock().await.get(&key).cloned();
                if let Some(torrent) = torrent {
                    self.queue_rss_completion(&torrent).await;
                }
                Some((RssAction::PreviouslyCompleted, "live library already contains this completed torrent; no second add".into()))
            }
            (Ok(_), Some(_)) => Some((
                RssAction::AlreadyPresent,
                "live library already contains this torrent; its paused or incomplete state is preserved".into(),
            )),
            (Ok(Some(DuplicateLookup::ImportedElsewhere)), None) => Some((
                RssAction::AlreadyPresent, "another RSS subscription already imported this torrent".into(),
            )),
            (Ok(None), None) => None,
        };
        if let Some((action, reason)) = skip {
            return self
                .record_rss_decision(
                    store,
                    subscription,
                    entry,
                    entry_key,
                    existing,
                    action,
                    &reason,
                    &torrent_keys,
                    now_value,
                    retry_at,
                    false,
                    false,
                )
                .await;
        }

        let paused = matches!(
            subscription.start_behavior,
            swarmotter_core::config::StartBehavior::Paused
        );
        let options = AddTorrentOptions::request(
            None,
            paused,
            true,
            subscription.profile.clone(),
            subscription.labels.clone(),
        );
        let result = if let Some(magnet) = resolved.magnet.as_deref() {
            <Self as DaemonOps>::add_magnet(self, magnet, options).await
        } else if let Some(bytes) = resolved.torrent_bytes.clone() {
            <Self as DaemonOps>::add_torrent_file(self, bytes, options).await
        } else {
            Err(CoreError::Internal(
                "resolved RSS offer has no usable intake representation".into(),
            ))
        };

        match result {
            Ok(key) => {
                imported_now = true;
                let key_locator = key.to_locator();
                if !torrent_keys.iter().any(|alias| alias == &key_locator) {
                    torrent_keys.push(key_locator.clone());
                }
                imported.push(key_locator.clone());
                action = RssAction::Import;
                reason = format!(
                    "imported {} through the standard intake profile workflow",
                    key_locator
                );
            }
            Err(CoreError::DuplicateTorrent(_)) => {
                action = RssAction::AlreadyPresent;
                reason = "torrent was registered by a concurrent intake; no second add".into();
            }
            Err(error) if rss_error_is_transient(&error) => {
                action = RssAction::Failed;
                reason = format!(
                    "transient {} failure: {error}; eligible for retry with backoff",
                    error.code()
                );
                let retry_at = next_listing_retry(config, existing, now_value);
                return self
                    .record_rss_decision(
                        store,
                        subscription,
                        entry,
                        entry_key,
                        existing,
                        action,
                        &reason,
                        &torrent_keys,
                        now_value,
                        retry_at,
                        false,
                        false,
                    )
                    .await;
            }
            Err(error) => {
                action = RssAction::Unsupported;
                reason = format!("permanent {} failure: {error}", error.code());
                dismissed = false;
            }
        }

        self.record_rss_decision(
            store,
            subscription,
            entry,
            entry_key,
            existing,
            action,
            &reason,
            &torrent_keys,
            now_value,
            None,
            dismissed,
            imported_now,
        )
        .await
    }

    async fn resolve_rss_offers(
        &self,
        offers: &[RssOffer],
        max_bytes: usize,
        timeout_seconds: u64,
    ) -> ResolvedOffer {
        let mut resolved = ResolvedOffer::default();
        let binder = self.make_binder().await;
        let client = swarmotter_core::net::ContainedHttpClient::with_data_timeout(
            binder.as_ref(),
            Duration::from_secs(timeout_seconds),
        );
        let mut seen = HashSet::new();
        for offer in offers {
            match offer.kind {
                swarmotter_core::rss::RssOfferKind::Magnet => match Magnet::parse(&offer.url) {
                    Ok(magnet) => {
                        for alias in
                            swarmotter_core::rss::torrent_identity_aliases(&magnet.identity)
                        {
                            if seen.insert(alias.clone()) {
                                resolved.aliases.push(alias);
                            }
                        }
                        if resolved.magnet.is_none() {
                            resolved.magnet = Some(offer.url.clone());
                        }
                    }
                    Err(error) => resolved
                        .errors
                        .push(format!("malformed magnet {}: {error}", offer.url)),
                },
                swarmotter_core::rss::RssOfferKind::Attachment => {
                    let response = match client.get_attachment(&offer.url, max_bytes).await {
                        Ok(response) => response,
                        Err(error) => {
                            if rss_error_is_transient(&error) {
                                resolved.transient_failure = true;
                            }
                            resolved
                                .errors
                                .push(format!("attachment {}: {error}", offer.url));
                            continue;
                        }
                    };
                    if let Some(content_type) = response.content_type.as_deref() {
                        let lowered = content_type.to_ascii_lowercase();
                        if lowered.starts_with("text/html") {
                            resolved.errors.push(format!(
                                "attachment {} returned HTML instead of torrent metainfo",
                                offer.url
                            ));
                            continue;
                        }
                    }
                    match meta::parse_torrent(&response.body) {
                        Ok(parsed) => {
                            for alias in
                                swarmotter_core::rss::torrent_identity_aliases(&parsed.identity)
                            {
                                if seen.insert(alias.clone()) {
                                    resolved.aliases.push(alias);
                                }
                            }
                            if resolved.torrent_bytes.is_none() {
                                resolved.torrent_bytes = Some(response.body);
                            }
                        }
                        Err(error) => resolved.errors.push(format!(
                            "malformed torrent attachment {}: {error}",
                            offer.url
                        )),
                    }
                }
                swarmotter_core::rss::RssOfferKind::PageOnly => {}
            }
        }
        resolved
    }

    async fn library_has_aliases(&self, aliases: &[String]) -> bool {
        self.first_live_library_key(aliases).await.is_some()
    }

    async fn first_live_library_key(&self, aliases: &[String]) -> Option<TorrentKey> {
        let registry = self.registry.lock().await;
        for alias in aliases {
            if let Ok(key) = TorrentKey::from_locator(alias) {
                if let Some(canonical) = registry.canonical_key(&key) {
                    return Some(canonical);
                }
            }
        }
        None
    }

    async fn library_key_is_complete(&self, key: TorrentKey) -> bool {
        self.registry.lock().await.get(&key).is_some_and(|torrent| {
            torrent.date_completed.is_some()
                || matches!(
                    torrent.state,
                    TorrentState::Completed | TorrentState::Seeding
                )
                || torrent.progress.is_complete()
        })
    }

    async fn touch_rss_listing(
        &self,
        store: &RssStore,
        subscription_id: &str,
        entry: &RssEntry,
        entry_key: &str,
        now_value: u64,
    ) -> Result<()> {
        let store = store.clone();
        let subscription_id = subscription_id.to_string();
        let entry = entry.clone();
        let entry_key = entry_key.to_string();
        tokio::task::spawn_blocking(move || {
            store.touch_listing(&subscription_id, &entry, &entry_key, now_value)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("touch RSS listing task: {error}")))?
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_rss_decision(
        &self,
        store: &RssStore,
        subscription: &RssSubscription,
        entry: &RssEntry,
        entry_key: &str,
        existing: Option<&crate::rss_store::StoredListing>,
        action: RssAction,
        reason: &str,
        torrent_keys: &[String],
        now_value: u64,
        retry_at: Option<u64>,
        dismissed: bool,
        imported: bool,
    ) -> Result<RssDecision> {
        let same = existing.is_some_and(|listing| {
            listing.action() == action && listing.reason == reason && listing.dismissed == dismissed
        });
        let transient = matches!(action, RssAction::Failed | RssAction::Deferred);
        if same && !transient && !imported {
            self.touch_rss_listing(store, &subscription.id, entry, entry_key, now_value)
                .await?;
            let existing = existing.expect("existing listing checked above");
            return Ok(RssDecision {
                entry_key: entry_key.to_string(),
                title: entry.title.clone(),
                link: entry.link.clone(),
                categories: entry.categories.clone(),
                pub_date: entry.pub_date.clone(),
                action,
                reason: reason.to_string(),
                torrent_keys: torrent_keys.to_vec(),
                decision_id: None,
                decided_at: Some(existing.last_decision_at),
            });
        }
        let store = store.clone();
        let subscription_id = subscription.id.clone();
        let decision_entry = entry.clone();
        let decision_entry_key = entry_key.to_string();
        let decision_reason = reason.to_string();
        let keys = torrent_keys.to_vec();
        let decision_id = tokio::task::spawn_blocking(move || {
            store.record_decision(
                &subscription_id,
                &decision_entry,
                &decision_entry_key,
                action,
                &decision_reason,
                &keys,
                now_value,
                retry_at,
                dismissed,
                imported,
            )
        })
        .await
        .map_err(|error| CoreError::Storage(format!("write RSS decision task: {error}")))??;
        Ok(RssDecision {
            entry_key: entry_key.to_string(),
            title: entry.title.clone(),
            link: entry.link.clone(),
            categories: entry.categories.clone(),
            pub_date: entry.pub_date.clone(),
            action,
            reason: reason.to_string(),
            torrent_keys: torrent_keys.to_vec(),
            decision_id: Some(decision_id),
            decided_at: Some(now_value),
        })
    }

    async fn reconsider_rss_listings(
        &self,
        config: &RssConfig,
        subscription: &RssSubscription,
        store: &RssStore,
    ) -> Result<RssPollResult> {
        let store_for_read = store.clone();
        let id = subscription.id.clone();
        let listings =
            tokio::task::spawn_blocking(move || store_for_read.reconsiderable_listings(&id, 500))
                .await
                .map_err(|error| {
                    CoreError::Storage(format!("read RSS reconsider set task: {error}"))
                })??;
        let mut decisions = Vec::new();
        let mut imported = Vec::new();
        for listing in listings {
            let existing = Some(listing.clone());
            let decision = self
                .process_rss_entry(
                    config,
                    subscription,
                    store,
                    &listing.entry,
                    &listing.entry_key,
                    existing.as_ref(),
                    true,
                    true,
                    true,
                    &mut imported,
                )
                .await?;
            decisions.push(decision);
        }
        Ok(RssPollResult {
            subscription_id: subscription.id.clone(),
            success: true,
            baseline_established: true,
            conditional_not_modified: false,
            decisions,
            imported,
            error: None,
        })
    }

    /// Preview one saved feed without adding a torrent or marking any entry
    /// seen.
    pub(super) async fn preview_rss_subscription(
        &self,
        subscription_id: &str,
    ) -> Result<RssPreview> {
        let config = self.config.read().await.rss.clone();
        let subscription = config
            .subscription(subscription_id)
            .cloned()
            .ok_or_else(|| CoreError::NotFound(format!("rss subscription {subscription_id}")))?;
        self.preview_rss_subscription_with(config, subscription)
            .await
    }

    /// Preview an unsaved subscription body. This supports the Web UI's
    /// Preview button before the operator commits the subscription to config.
    pub(super) async fn preview_rss_draft(
        &self,
        subscription: &RssSubscription,
    ) -> Result<RssPreview> {
        subscription.validate()?;
        let config = self.config.read().await.rss.clone();
        self.preview_rss_subscription_with(config, subscription.clone())
            .await
    }

    async fn preview_rss_subscription_with(
        &self,
        config: RssConfig,
        subscription: RssSubscription,
    ) -> Result<RssPreview> {
        let store = self.rss_history_store().await?;
        let state = {
            let store = store.clone();
            let id = subscription.id.clone();
            tokio::task::spawn_blocking(move || store.feed_state(&id))
                .await
                .map_err(|error| {
                    CoreError::Storage(format!("read RSS feed state task: {error}"))
                })??
        };
        let state = state.unwrap_or_else(|| RssFeedState::new(&subscription.id));
        let binder = self.make_binder().await;
        let client = swarmotter_core::net::ContainedHttpClient::with_data_timeout(
            binder.as_ref(),
            Duration::from_secs(config.request_timeout_seconds),
        );
        let response = client
            .get_feed(&subscription.url, None, None, config.max_feed_bytes)
            .await?;
        if !(200..300).contains(&response.status) && response.status != 304 {
            return Err(CoreError::HttpStatus(format!(
                "feed {} returned HTTP {}",
                redact_feed_url(&response.final_url),
                response.status
            )));
        }
        if response.status == 304 {
            return Ok(RssPreview {
                subscription_id: subscription.id.clone(),
                feed_title: String::new(),
                fetched_at: now(),
                conditional_not_modified: true,
                decisions: Vec::new(),
            });
        }
        let feed = parse_feed(&response.body, config.max_feed_bytes, config.max_entries)?;
        let mut decisions = Vec::new();
        for entry in &feed.entries {
            let entry_key = entry.identity();
            let existing = {
                let store = store.clone();
                let id = subscription.id.clone();
                let key = entry_key.clone();
                tokio::task::spawn_blocking(move || store.listing(&id, &key))
                    .await
                    .map_err(|error| {
                        CoreError::Storage(format!("read RSS listing task: {error}"))
                    })??
            };
            decisions.push(
                self.preview_rss_entry(
                    &config,
                    &subscription,
                    &store,
                    entry,
                    &entry_key,
                    existing.as_ref(),
                    state.baseline_established,
                )
                .await?,
            );
        }
        Ok(RssPreview {
            subscription_id: subscription.id.clone(),
            feed_title: feed.title,
            fetched_at: now(),
            conditional_not_modified: false,
            decisions,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn preview_rss_entry(
        &self,
        config: &RssConfig,
        subscription: &RssSubscription,
        store: &RssStore,
        entry: &RssEntry,
        entry_key: &str,
        existing: Option<&crate::rss_store::StoredListing>,
        baseline_established: bool,
    ) -> Result<RssDecision> {
        let mut torrent_keys = Vec::new();
        if let Some(existing) = existing {
            if existing.dismissed {
                return Ok(RssDecision {
                    entry_key: entry_key.to_string(),
                    title: entry.title.clone(),
                    link: entry.link.clone(),
                    categories: entry.categories.clone(),
                    pub_date: entry.pub_date.clone(),
                    action: RssAction::Dismissed,
                    reason: existing.reason.clone(),
                    torrent_keys: existing.torrent_keys.clone(),
                    decision_id: None,
                    decided_at: None,
                });
            }
            if existing.imported_at.is_some() || existing.completed_at.is_some() {
                let action = if existing.completed_at.is_some()
                    || matches!(
                        self.rss_history_duplicate(store, &existing.torrent_keys)
                            .await?,
                        Some(DuplicateLookup::Completed)
                    ) {
                    RssAction::PreviouslyCompleted
                } else if self.library_has_aliases(&existing.torrent_keys).await {
                    RssAction::AlreadyPresent
                } else {
                    RssAction::Dismissed
                };
                return Ok(RssDecision {
                    entry_key: entry_key.to_string(),
                    title: entry.title.clone(),
                    link: entry.link.clone(),
                    categories: entry.categories.clone(),
                    pub_date: entry.pub_date.clone(),
                    action,
                    reason: "existing listing state is preserved".into(),
                    torrent_keys: existing.torrent_keys.clone(),
                    decision_id: None,
                    decided_at: None,
                });
            }
        }
        let rule = subscription.rules.evaluate(entry);
        if !rule.included {
            return Ok(RssDecision {
                entry_key: entry_key.to_string(),
                title: entry.title.clone(),
                link: entry.link.clone(),
                categories: entry.categories.clone(),
                pub_date: entry.pub_date.clone(),
                action: RssAction::Filtered,
                reason: rule.reason,
                torrent_keys,
                decision_id: None,
                decided_at: None,
            });
        }
        if !baseline_established
            && subscription.import_mode == swarmotter_core::rss::RssImportMode::NewOnly
        {
            return Ok(RssDecision {
                entry_key: entry_key.to_string(),
                title: entry.title.clone(),
                link: entry.link.clone(),
                categories: entry.categories.clone(),
                pub_date: entry.pub_date.clone(),
                action: RssAction::Baseline,
                reason: "first successful poll would establish the baseline and skip this entry in new-entries-only mode; preview did not mark it seen".into(),
                torrent_keys,
                decision_id: None,
                decided_at: None,
            });
        }
        let (offers, page_links) = entry_offers(entry);
        if offers.is_empty() {
            return Ok(RssDecision {
                entry_key: entry_key.to_string(),
                title: entry.title.clone(),
                link: entry.link.clone(),
                categories: entry.categories.clone(),
                pub_date: entry.pub_date.clone(),
                action: RssAction::Unsupported,
                reason: if page_links.is_empty() {
                    "listing has no magnet or torrent attachment".into()
                } else {
                    format!(
                        "page-only listing: no magnet or torrent attachment (link {})",
                        page_links[0].url
                    )
                },
                torrent_keys,
                decision_id: None,
                decided_at: None,
            });
        }
        let resolved = self
            .resolve_rss_offers(
                &offers,
                config.max_attachment_bytes,
                config.request_timeout_seconds,
            )
            .await;
        if resolved.aliases.is_empty() {
            return Ok(RssDecision {
                entry_key: entry_key.to_string(),
                title: entry.title.clone(),
                link: entry.link.clone(),
                categories: entry.categories.clone(),
                pub_date: entry.pub_date.clone(),
                action: RssAction::Unsupported,
                reason: format!(
                    "no valid torrent could be obtained: {}",
                    redact_urls_in_text(&resolved.errors.join("; "))
                ),
                torrent_keys,
                decision_id: None,
                decided_at: None,
            });
        }
        torrent_keys = resolved.aliases.clone();
        let live = self.first_live_library_key(&torrent_keys).await;
        let duplicate = self.rss_history_duplicate(store, &torrent_keys).await?;
        let (action, reason) = match (duplicate, live) {
            (Some(DuplicateLookup::Completed), _) => (
                RssAction::PreviouslyCompleted,
                "verified completion recorded for this torrent identity; would skip",
            ),
            (_, Some(key)) if self.library_key_is_complete(key).await => (
                RssAction::PreviouslyCompleted,
                "live library already contains this completed torrent; would skip",
            ),
            (Some(DuplicateLookup::ImportedElsewhere), _) | (_, Some(_)) => (
                RssAction::AlreadyPresent,
                "already present or imported by another feed; would preserve its state",
            ),
            (None, None) => (
                RssAction::Import,
                "would import through the configured intake profile",
            ),
        };
        Ok(RssDecision {
            entry_key: entry_key.to_string(),
            title: entry.title.clone(),
            link: entry.link.clone(),
            categories: entry.categories.clone(),
            pub_date: entry.pub_date.clone(),
            action,
            reason: reason.into(),
            torrent_keys,
            decision_id: None,
            decided_at: None,
        })
    }

    pub(super) async fn rss_status(&self) -> Vec<RssSubscriptionStatus> {
        let config = self.config.read().await.rss.clone();
        let (store, history_error) = match self.rss_history_store().await {
            Ok(store) => (Some(store), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let mut statuses = Vec::with_capacity(config.subscriptions.len());
        for subscription in &config.subscriptions {
            let in_progress = self
                .rss_polls_in_progress
                .read()
                .await
                .contains(&subscription.id);
            let (state, counts) = match store.as_ref() {
                Some(store) => {
                    let store_state = store.clone();
                    let id = subscription.id.clone();
                    let state = tokio::task::spawn_blocking(move || store_state.feed_state(&id))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .flatten();
                    let store_counts = store.clone();
                    let id = subscription.id.clone();
                    let counts = tokio::task::spawn_blocking(move || store_counts.counts(&id))
                        .await
                        .ok()
                        .and_then(Result::ok);
                    (state, counts)
                }
                None => (None, None),
            };
            let state = state.unwrap_or_else(|| RssFeedState::new(&subscription.id));
            statuses.push(RssSubscriptionStatus {
                id: subscription.id.clone(),
                name: subscription.name.clone(),
                url: redact_feed_url(&subscription.url),
                enabled: subscription.enabled,
                poll_interval_seconds: subscription.poll_interval_seconds,
                profile: subscription.profile.clone(),
                start_behavior: subscription.start_behavior,
                import_mode: subscription.import_mode,
                rules: subscription.rules.clone(),
                labels: subscription.labels.clone(),
                last_poll_at: state.last_poll_at,
                last_success_at: state.last_success_at,
                next_attempt_at: state.next_attempt_at,
                latest_error: state.last_error.clone().or_else(|| history_error.clone()),
                consecutive_failures: state.consecutive_failures,
                baseline_established: state.baseline_established,
                counts: counts.unwrap_or_default(),
                poll_in_progress: in_progress,
            });
        }
        statuses
    }

    pub(super) async fn rss_decisions(
        &self,
        subscription_id: &str,
        limit: usize,
        before: Option<i64>,
    ) -> Result<RssDecisionPage> {
        let store = self.rss_history_store().await?;
        let id = subscription_id.to_string();
        tokio::task::spawn_blocking(move || store.decision_page(&id, limit, before))
            .await
            .map_err(|error| CoreError::Storage(format!("read RSS decisions task: {error}")))?
    }

    pub(super) async fn retry_rss_listing(
        &self,
        subscription_id: &str,
        entry_key: Option<&str>,
    ) -> Result<RssPollResult> {
        let store = self.rss_history_store().await?;
        let id = subscription_id.to_string();
        let key = entry_key.map(str::to_string);
        let now_value = now();
        tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.clear_dismissal(&id, key.as_deref(), now_value)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("clear RSS dismissal task: {error}")))??;
        self.poll_rss_subscription(subscription_id, false, true)
            .await
    }

    pub(super) async fn clear_rss_completions(&self) -> Result<usize> {
        let store = self.rss_history_store().await?;
        tokio::task::spawn_blocking(move || store.clear_completions())
            .await
            .map_err(|error| CoreError::Storage(format!("clear RSS completions task: {error}")))?
    }

    pub(super) async fn rss_duplicate_lookup_is_indexed(&self) -> Result<bool> {
        let store = self.rss_history_store().await?;
        tokio::task::spawn_blocking(move || store.duplicate_lookup_uses_index())
            .await
            .map_err(|error| CoreError::Storage(format!("RSS index check task: {error}")))?
    }

    /// Queue completion evidence for torrents already completed in durable
    /// state, then flush it through the outbox. This is the only upgrade
    /// backfill path: removed downloads with no surviving durable completion
    /// evidence cannot be reconstructed and are not claimed.
    pub(super) async fn backfill_rss_completion_evidence(&self) {
        let completed = {
            let registry = self.registry.lock().await;
            registry
                .torrents
                .iter()
                .filter(|(_, torrent)| {
                    torrent.date_completed.is_some()
                        || matches!(
                            torrent.state,
                            TorrentState::Completed | TorrentState::Seeding
                        )
                })
                .map(|(_, torrent)| torrent.clone())
                .collect::<Vec<_>>()
        };
        if completed.is_empty() {
            return;
        }
        let store = match self.rss_history_store().await {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(error = %error, "RSS completion backfill deferred: history unavailable");
                return;
            }
        };
        let mut queued = false;
        for torrent in completed {
            let aliases = torrent
                .keys()
                .into_iter()
                .map(TorrentKey::to_locator)
                .collect::<Vec<_>>();
            let already = tokio::task::spawn_blocking({
                let store = store.clone();
                let aliases = aliases.clone();
                move || store.completed_alias(&aliases)
            })
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .is_some();
            if !already {
                self.queue_rss_completion(&torrent).await;
                queued = true;
            }
        }
        if queued {
            let _ = self.persist_state().await;
        }
    }
}

fn next_listing_retry(
    config: &RssConfig,
    existing: Option<&crate::rss_store::StoredListing>,
    now: u64,
) -> Option<u64> {
    let failures = existing.map_or(1, |listing| listing.retry_count.saturating_add(1));
    retry_backoff_seconds(
        failures,
        config.retry_base_seconds,
        config.retry_max_seconds,
        config.max_retries,
    )
    .map(|seconds| now.saturating_add(seconds))
}

fn rss_error_is_transient(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::NetworkBlocked(_)
            | CoreError::Elapsed(_)
            | CoreError::Io(_)
            | CoreError::HttpProtocol(_)
            | CoreError::HttpStatus(_)
            | CoreError::Proxy(_)
            | CoreError::Storage(_)
            | CoreError::Internal(_)
    )
}
