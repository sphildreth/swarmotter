// SPDX-License-Identifier: Apache-2.0

use super::*;
use swarmotter_api::handlers::torrents::{TorrentListQuery, TorrentListResponse};
use swarmotter_core::policy::PolicyProfilesConfig;

type LibrarySnapshot = Arc<[Arc<TorrentSummary>]>;

#[derive(Default)]
pub(super) struct LibraryQueries {
    registry_revision: u64,
    queue_revision: u64,
    positions: HashMap<TorrentKey, usize>,
    profiles: PolicyProfilesConfig,
    ratio_limit: Option<f64>,
    idle_limit: Option<u64>,
    snapshot: LibrarySnapshot,
    last_page: Option<(TorrentListQuery, TorrentListResponse)>,
}

impl DaemonRuntime {
    pub(super) async fn library_snapshot(&self) -> LibrarySnapshot {
        let config = self.config.read().await.clone();
        let _lifecycle = self.seeder_lifecycle_lock.lock().await;
        let mut registry = self.registry.lock().await;
        let queue = self.queue.lock().await;
        let mut cache = self.library_queries.lock().await;
        let order_changed = cache.queue_revision != queue.order_revision();
        if order_changed {
            cache.positions = queue
                .order
                .iter()
                .enumerate()
                .map(|(i, key)| (*key, i + 1))
                .collect();
            cache.queue_revision = queue.order_revision();
        }
        drop(queue);
        let reset = order_changed
            || cache.profiles != config.profiles
            || cache.ratio_limit != config.seeding.global_ratio_limit
            || cache.idle_limit != config.seeding.global_idle_limit;
        if reset || cache.registry_revision != registry.list_revision() {
            cache.snapshot = registry
                .projected_summaries(reset, |torrent| {
                    let mut row = torrent.to_summary();
                    row.queue_position = cache.positions.get(&torrent.key()).copied();
                    let policy = Self::effective_policy_with_config(&config, torrent);
                    if policy.seed_forever.value {
                        row.effective_ratio_limit = None;
                        row.effective_idle_limit = None;
                    } else {
                        row.effective_ratio_limit = policy.ratio_limit.value;
                        row.effective_idle_limit = policy.idle_limit.value;
                    }
                    row
                })
                .into();
            cache.registry_revision = registry.list_revision();
            if reset {
                cache.profiles = config.profiles.clone();
                cache.ratio_limit = config.seeding.global_ratio_limit;
                cache.idle_limit = config.seeding.global_idle_limit;
            }
            cache.last_page = None;
        }
        cache.snapshot.clone()
    }

    pub(super) async fn library_page(&self, query: TorrentListQuery) -> TorrentListResponse {
        let snapshot = self.library_snapshot().await;
        {
            let cache = self.library_queries.lock().await;
            if Arc::ptr_eq(&cache.snapshot, &snapshot) {
                if let Some((_, result)) = cache.last_page.as_ref().filter(|(key, _)| key == &query)
                {
                    return result.clone();
                }
            }
        }
        // Filtering, facet counts and ordering operate on shared immutable rows
        // after releasing the registry, queue and lifecycle locks.
        let result = query.apply(snapshot.to_vec());
        let mut cache = self.library_queries.lock().await;
        if Arc::ptr_eq(&cache.snapshot, &snapshot) {
            cache.last_page = Some((query, result.clone()));
        }
        result
    }
}
