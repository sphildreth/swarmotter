// SPDX-License-Identifier: Apache-2.0

use super::*;

use swarmotter_core::hash::PeerInfoHash;

impl TorrentEngine {
    /// Return the protocol's explicit 20-byte discovery identity. Pure-v2
    /// torrents use the BEP 52 truncation; v1 and hybrid transfers retain
    /// their v1 swarm identity.
    fn discovery_wire_hash(&self) -> PeerInfoHash {
        if self.meta.requires_v2_data_plane() {
            self.meta
                .identity
                .v2_peer_info_hash()
                .expect("validated pure-v2 metadata has a v2 identity")
        } else {
            PeerInfoHash::from_v1(self.meta.info_hash)
        }
    }

    pub(super) async fn refresh_discovery_peers(&self, force: bool) -> Vec<PeerAddr> {
        let mut refreshed = Vec::new();
        if force || self.tracker_announce_due().await {
            refreshed = self.announce(AnnounceEvent::Empty).await;
        }
        if force || self.dht_lookup_due().await {
            merge_unique_peers(&mut refreshed, self.discover_dht_peers().await);
        }
        dedupe_peers(&mut refreshed);
        refreshed
    }

    pub(super) async fn tracker_announce_due(&self) -> bool {
        if swarmotter_core::policy::prioritized_tracker_tiers(
            self.meta.announce.as_deref(),
            &self.meta.announce_list,
            &self.tracker_host_rules,
        )
        .is_empty()
        {
            return false;
        }
        let state = self.state.lock().await;
        let Some(last_announce) = state.last_announce else {
            return true;
        };
        let interval = state
            .tracker_interval_seconds
            .max(PEER_REFRESH_INTERVAL.as_secs());
        now_secs().saturating_sub(last_announce) >= interval
    }

    pub(super) async fn dht_lookup_due(&self) -> bool {
        if self.meta.is_private() || self.dht.is_none() {
            return false;
        }
        self.state
            .lock()
            .await
            .dht_last_lookup
            .is_none_or(|last| last.elapsed() >= PEER_REFRESH_INTERVAL)
    }

    pub(super) async fn discover_dht_peers(&self) -> Vec<PeerAddr> {
        if self.meta.is_private() {
            return Vec::new();
        }
        let Some(dht) = &self.dht else {
            return Vec::new();
        };
        self.state.lock().await.dht_last_lookup = Some(Instant::now());
        let result = tokio::time::timeout(
            DHT_DISCOVERY_TIMEOUT,
            dht.get_peers_with_stats(self.discovery_wire_hash(), DHT_DISCOVERY_ROUNDS),
        )
        .await;
        match result {
            Ok(Ok(lookup)) => {
                let peers = self.filter_allowed_peers(lookup.peers);
                if lookup.responding_nodes > 0 || !peers.is_empty() {
                    let mut s = self.state.lock().await;
                    s.dht_discovery_ok = !peers.is_empty();
                    s.dht_last_seen = Some(Instant::now());
                }
                tracing::debug!(
                    queried = lookup.queried_nodes,
                    responding = lookup.responding_nodes,
                    peers = peers.len(),
                    "DHT peer discovery completed"
                );
                peers
            }
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "DHT peer discovery failed");
                Vec::new()
            }
            Err(_) => {
                tracing::debug!("DHT peer discovery timed out");
                Vec::new()
            }
        }
    }
}

impl TorrentEngine {
    /// Announce to all HTTP/UDP trackers and return discovered peers.
    pub(super) async fn announce(&self, event: AnnounceEvent) -> Vec<PeerAddr> {
        let tiers = swarmotter_core::policy::prioritized_tracker_tiers(
            self.meta.announce.as_deref(),
            &self.meta.announce_list,
            &self.tracker_host_rules,
        );
        let scrape_urls = tiers.iter().flatten().cloned().collect::<Vec<_>>();
        let (uploaded, downloaded, left) = {
            let s = self.state.lock().await;
            (
                s.uploaded,
                s.downloaded,
                s.total_length.saturating_sub(s.bytes_completed),
            )
        };
        let outcome = self
            .announce_tracker_tiers(
                self.discovery_wire_hash(),
                tiers,
                uploaded,
                downloaded,
                left,
                event,
            )
            .await;
        self.record_tracker_activity(self.discovery_wire_hash(), &outcome, scrape_urls)
            .await;
        self.filter_allowed_peers(outcome.peers)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn announce_tracker_tiers(
        &self,
        info_hash: PeerInfoHash,
        tiers: Vec<Vec<String>>,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: AnnounceEvent,
    ) -> TrackerAnnounceOutcome {
        if tiers.is_empty() {
            return TrackerAnnounceOutcome {
                message: Some("no trackers configured".into()),
                ..Default::default()
            };
        }
        let mut aggregate = TrackerAnnounceOutcome::default();
        'tiers: for tier in tiers {
            for url in tier {
                let outcome = self
                    .announce_trackers(info_hash, vec![url], uploaded, downloaded, left, event)
                    .await;
                let succeeded = outcome.ok;
                merge_tracker_outcome(&mut aggregate, outcome);
                if succeeded {
                    break 'tiers;
                }
            }
        }
        dedupe_peers(&mut aggregate.peers);
        aggregate
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn announce_trackers(
        &self,
        info_hash: PeerInfoHash,
        trackers: Vec<String>,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: AnnounceEvent,
    ) -> TrackerAnnounceOutcome {
        if trackers.is_empty() {
            return TrackerAnnounceOutcome {
                message: Some("no trackers configured".into()),
                ..Default::default()
            };
        }

        let announce_at = now_secs();
        let mut outcome = TrackerAnnounceOutcome::default();
        let mut tasks = tokio::task::JoinSet::new();
        for url in trackers {
            outcome.tracker_results.insert(
                url.clone(),
                TrackerAnnounceSnapshot {
                    status: TrackerStatus::Updating,
                    seeders: 0,
                    leechers: 0,
                    downloads: 0,
                    last_error: None,
                    last_message: Some("announce in progress".into()),
                    last_announce: Some(announce_at),
                },
            );
            let binder = self.binder.clone();
            let req = AnnounceRequest {
                tracker_url: url.clone(),
                info_hash,
                peer_id: self.peer_id,
                port: self.listen_port,
                uploaded,
                downloaded,
                left,
                event,
                numwant: Some(200),
                compact: true,
            };
            tasks.spawn(async move {
                let result = timeout(TRACKER_ANNOUNCE_TIMEOUT, async {
                    if url.starts_with("udp://") {
                        udp_tracker::udp_announce(binder.as_ref(), &req).await
                    } else {
                        tracker::http_announce(binder.as_ref(), &req).await
                    }
                })
                .await
                .map_err(|_| CoreError::Internal("tracker announce timed out".into()))
                .and_then(|r| r);
                (url, result)
            });
        }

        while let Some(joined) = tasks.join_next().await {
            record_tracker_joined_result(&mut outcome, joined, announce_at);
        }
        dedupe_peers(&mut outcome.peers);
        outcome
    }

    pub(super) async fn record_tracker_announce_outcome(&self, outcome: &TrackerAnnounceOutcome) {
        let mut s = self.state.lock().await;
        s.tracker_ok = outcome.ok;
        s.tracker_message = outcome.message.clone();
        s.last_announce = Some(now_secs());
        if let Some(interval) = outcome.interval_seconds {
            s.tracker_interval_seconds = interval;
        }
        for (url, result) in &outcome.tracker_results {
            s.tracker_announces.insert(url.clone(), result.clone());
        }
        if outcome.ok {
            s.tracker_last_ok = Some(Instant::now());
            if outcome.failures == 0 {
                s.tracker_failures_recent = 0;
            }
        }
        if outcome.failures > 0 {
            s.tracker_failures_recent = s.tracker_failures_recent.saturating_add(outcome.failures);
        }
    }

    /// Retain announce state, then schedule one scrape for every configured
    /// tracker. This shared path is used by initial download
    /// discovery, explicit/periodic reannounce, completion, and magnet
    /// metadata discovery.
    pub(super) async fn record_tracker_activity(
        &self,
        info_hash: PeerInfoHash,
        outcome: &TrackerAnnounceOutcome,
        scrape_urls: Vec<String>,
    ) {
        self.record_tracker_announce_outcome(outcome).await;
        run_tracker_scrapes(
            self.state.clone(),
            self.binder.clone(),
            info_hash,
            scrape_urls,
        )
        .await;
    }

    pub(super) async fn discover_magnet_dht_peers(&self, info_hash: PeerInfoHash) -> Vec<PeerAddr> {
        let Some(dht) = &self.dht else {
            return Vec::new();
        };
        let dht_result = tokio::time::timeout(
            DHT_DISCOVERY_TIMEOUT,
            dht.get_peers_with_stats(info_hash, DHT_DISCOVERY_ROUNDS),
        )
        .await;
        match dht_result {
            Ok(Ok(lookup)) => {
                let peers = self.filter_allowed_peers(lookup.peers);
                if lookup.responding_nodes > 0 || !peers.is_empty() {
                    let mut s = self.state.lock().await;
                    s.dht_discovery_ok = !peers.is_empty();
                    s.dht_last_seen = Some(Instant::now());
                }
                tracing::debug!(
                    queried = lookup.queried_nodes,
                    responding = lookup.responding_nodes,
                    peers = peers.len(),
                    "DHT magnet metadata peer discovery completed"
                );
                peers
            }
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "DHT magnet metadata peer discovery failed");
                Vec::new()
            }
            Err(_) => {
                tracing::debug!("DHT magnet metadata peer discovery timed out");
                Vec::new()
            }
        }
    }

    pub(super) async fn sync_have_from_state(&self, have: &mut PieceBitfield, piece_count: usize) {
        let state_have = {
            let s = self.state.lock().await;
            if s.pieces_have.count(piece_count) <= have.count(piece_count) {
                None
            } else {
                Some(s.pieces_have.clone())
            }
        };
        let Some(state_have) = state_have else {
            return;
        };
        for piece in 0..piece_count {
            if state_have.has(piece) {
                have.set(piece);
            }
        }
    }

    /// Fetch complete executable magnet metadata through contained peers.
    ///
    /// The candidate discovery identity is the magnet's prescribed 20-byte
    /// wire value. A pure-v2 result continues from BEP 9 on the same contained
    /// session to retrieve and verify its top-level BEP 52 piece layers before
    /// this method returns; callers therefore never mistake raw v2 `info`
    /// bytes for executable metainfo.
    pub(super) async fn fetch_magnet_metadata(
        &self,
        magnet: &MagnetParams,
    ) -> Result<crate::metadata::ResolvedMagnetMetadata> {
        let mut candidates = Vec::new();
        let mut last_error: Option<CoreError> = None;
        let discovery_trackers = swarmotter_core::policy::prioritize_tracker_urls(
            &magnet.trackers,
            &self.tracker_host_rules,
        );

        for round in 1..=MAGNET_METADATA_MAX_ROUNDS {
            match self.poll_commands().await {
                CommandOutcome::Stop => {
                    return Err(CoreError::Internal("magnet metadata fetch stopped".into()));
                }
                CommandOutcome::Reannounce
                | CommandOutcome::RelaxPeerBackoff
                | CommandOutcome::Continue
                | CommandOutcome::Pause => {}
            }

            // Avoid recording a synthetic announce for a trackerless magnet.
            // Direct peers and DHT may still resolve contained metadata, but a
            // metadata-only preview must not look as though payload discovery
            // announced when there was no tracker request to make.
            if !discovery_trackers.is_empty() {
                let outcome = self
                    .announce_trackers(
                        magnet.wire_info_hash,
                        discovery_trackers.clone(),
                        0,
                        0,
                        1,
                        if round == 1 {
                            AnnounceEvent::Started
                        } else {
                            AnnounceEvent::Empty
                        },
                    )
                    .await;
                self.record_tracker_activity(
                    magnet.wire_info_hash,
                    &outcome,
                    discovery_trackers.clone(),
                )
                .await;
                merge_unique_peers(&mut candidates, self.filter_allowed_peers(outcome.peers));
            }

            for p in &self.seed_peers {
                if self.peer_allowed(p) && !candidates.contains(p) {
                    candidates.push(*p);
                }
            }

            let dht_peers = self.discover_magnet_dht_peers(magnet.wire_info_hash).await;
            merge_unique_peers(&mut candidates, dht_peers);
            dedupe_peers(&mut candidates);
            self.state.lock().await.peers = candidates.clone();

            if candidates.is_empty() {
                last_error = Some(CoreError::Internal(
                    "magnet metadata fetch: no peers discovered".into(),
                ));
                tracing::debug!(round, "magnet metadata discovery found no peers");
            } else {
                tracing::debug!(
                    round,
                    candidates = candidates.len(),
                    "attempting magnet metadata fetch from discovered peers"
                );
                match crate::metadata::fetch_resolved_metadata_from_candidates_with_budget(
                    crate::metadata::MetadataFetchContext::for_identity(
                        self.peer_session_budget.clone(),
                        self.binder.clone(),
                        magnet.identity.clone(),
                        self.peer_id,
                        self.utp_enabled,
                        self.utp_prefer_tcp,
                        self.encryption_mode,
                    )
                    .with_peer_filter(self.peer_filter.clone()),
                    &candidates,
                    &magnet.trackers,
                )
                .await
                {
                    Ok(metadata) => {
                        tracing::info!(
                            info_hash = %magnet.info_hash,
                            round,
                            candidates = candidates.len(),
                            metadata_bytes = metadata.raw_info.len(),
                            "magnet metadata fetched"
                        );
                        return Ok(metadata);
                    }
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            round,
                            candidates = candidates.len(),
                            "magnet metadata fetch round failed; will retry discovery"
                        );
                        last_error = Some(e);
                    }
                }
            }

            if round < MAGNET_METADATA_MAX_ROUNDS {
                self.sleep_or_stop(MAGNET_METADATA_RETRY_PAUSE).await;
            }
        }

        Err(CoreError::Internal(format!(
            "magnet metadata fetch failed after discovery retries: {}",
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no metadata candidates".into())
        )))
    }
}

pub(super) fn record_tracker_joined_result(
    outcome: &mut TrackerAnnounceOutcome,
    joined: std::result::Result<
        (String, Result<tracker::AnnounceResponse>),
        tokio::task::JoinError,
    >,
    announce_at: u64,
) {
    match joined {
        Ok((url, Ok(resp))) => {
            let effective_interval = resp
                .interval
                .max(resp.min_interval.unwrap_or(0))
                .clamp(30, 86_400);
            outcome.interval_seconds = Some(
                outcome
                    .interval_seconds
                    .unwrap_or(0)
                    .max(effective_interval),
            );
            if let Some(fr) = resp.failure_reason {
                let aggregate = format!("{url}: {fr}");
                outcome.failures = outcome.failures.saturating_add(1);
                if !outcome.ok {
                    outcome.message = Some(aggregate);
                }
                outcome.tracker_results.insert(
                    url,
                    TrackerAnnounceSnapshot {
                        status: TrackerStatus::Error,
                        seeders: resp.seeders,
                        leechers: resp.leechers,
                        downloads: 0,
                        last_error: Some(fr),
                        last_message: None,
                        last_announce: Some(announce_at),
                    },
                );
                return;
            }
            outcome.ok = true;
            let peer_count = resp.peers.len();
            let last_message = if resp.peers.is_empty() {
                if outcome.message.is_none() {
                    let message = format!(
                        "{url}: announce returned 0 peers (seeders={}, leechers={})",
                        resp.seeders, resp.leechers
                    );
                    outcome.message = Some(message.clone());
                    message
                } else {
                    format!(
                        "{url}: announce returned 0 peers (seeders={}, leechers={})",
                        resp.seeders, resp.leechers
                    )
                }
            } else {
                let message = format!(
                    "{url}: announce returned {peer_count} peers (seeders={}, leechers={})",
                    resp.seeders, resp.leechers
                );
                outcome.message = Some(message.clone());
                message
            };
            outcome.tracker_results.insert(
                url,
                TrackerAnnounceSnapshot {
                    status: TrackerStatus::Ok,
                    seeders: resp.seeders,
                    leechers: resp.leechers,
                    downloads: 0,
                    last_error: None,
                    last_message: Some(last_message),
                    last_announce: Some(announce_at),
                },
            );
            outcome.peers.extend(resp.peers);
        }
        Ok((url, Err(e))) => {
            let error = e.to_string();
            outcome.failures = outcome.failures.saturating_add(1);
            if !outcome.ok {
                outcome.message = Some(format!("{url}: {error}"));
            }
            tracing::debug!(tracker = %url, error = %error, "tracker announce failed");
            outcome.tracker_results.insert(
                url,
                TrackerAnnounceSnapshot {
                    status: TrackerStatus::Error,
                    seeders: 0,
                    leechers: 0,
                    downloads: 0,
                    last_error: Some(error),
                    last_message: None,
                    last_announce: Some(announce_at),
                },
            );
        }
        Err(e) => {
            outcome.failures = outcome.failures.saturating_add(1);
            if !outcome.ok {
                outcome.message = Some(format!("tracker announce task failed: {e}"));
            }
        }
    }
}

pub(super) fn merge_tracker_outcome(
    aggregate: &mut TrackerAnnounceOutcome,
    mut outcome: TrackerAnnounceOutcome,
) {
    aggregate.ok |= outcome.ok;
    aggregate.failures = aggregate.failures.saturating_add(outcome.failures);
    aggregate.peers.append(&mut outcome.peers);
    aggregate.tracker_results.extend(outcome.tracker_results);
    if let Some(interval) = outcome.interval_seconds {
        aggregate.interval_seconds = Some(
            aggregate
                .interval_seconds
                .map_or(interval, |current| current.min(interval)),
        );
    }
    if outcome.ok || aggregate.message.is_none() {
        aggregate.message = outcome.message;
    }
}

/// Run supported HTTP(S) scrapes concurrently through the same contained
/// binder used for announce traffic. Join failures retain the URL by task ID,
/// so a panic/cancellation is visible instead of silently disappearing.
pub(crate) async fn run_tracker_scrapes(
    state: Arc<Mutex<EngineState>>,
    binder: Arc<dyn NetworkBinder>,
    info_hash: PeerInfoHash,
    tracker_urls: Vec<String>,
) {
    let mut unique = HashSet::new();
    let tracker_urls = tracker_urls
        .into_iter()
        .filter(|url| unique.insert(url.clone()))
        .collect::<Vec<_>>();
    if tracker_urls.is_empty() {
        return;
    }

    {
        let mut engine = state.lock().await;
        for url in &tracker_urls {
            let snapshot = engine.tracker_scrapes.entry(url.clone()).or_default();
            snapshot.status = TrackerScrapeStatus::Updating;
            snapshot.last_error = None;
        }
    }

    let attempted_at = now_secs();
    let mut tasks = tokio::task::JoinSet::new();
    let mut task_urls = HashMap::new();
    for url in tracker_urls {
        let task_url = url.clone();
        let task_binder = binder.clone();
        let handle = tasks.spawn(async move {
            tracker::http_scrape(task_binder.as_ref(), &task_url, &[info_hash]).await
        });
        task_urls.insert(handle.id(), url);
    }

    while let Some(joined) = tasks.join_next_with_id().await {
        match joined {
            Ok((task_id, result)) => {
                let Some(url) = task_urls.remove(&task_id) else {
                    continue;
                };
                let mut engine = state.lock().await;
                let snapshot = engine.tracker_scrapes.entry(url).or_default();
                snapshot.last_scrape = Some(attempted_at);
                match result {
                    Ok(tracker::ScrapeOutcome::Unsupported) => {
                        snapshot.status = TrackerScrapeStatus::Unsupported;
                        snapshot.last_error = None;
                    }
                    Ok(tracker::ScrapeOutcome::Success(mut counts)) => {
                        if let Some(counts) = counts.remove(&info_hash) {
                            snapshot.status = TrackerScrapeStatus::Ok;
                            snapshot.seeders = Some(counts.seeders);
                            snapshot.leechers = Some(counts.leechers);
                            snapshot.downloads = Some(counts.downloads);
                            snapshot.last_error = None;
                        } else {
                            snapshot.status = TrackerScrapeStatus::Error;
                            snapshot.last_error = Some(format!(
                                "tracker scrape omitted requested info hash {}",
                                info_hash.to_hex()
                            ));
                            engine.tracker_failures_recent =
                                engine.tracker_failures_recent.saturating_add(1);
                        }
                    }
                    Err(error) => {
                        snapshot.status = TrackerScrapeStatus::Error;
                        snapshot.last_error = Some(error.to_string());
                        engine.tracker_failures_recent =
                            engine.tracker_failures_recent.saturating_add(1);
                    }
                }
            }
            Err(error) => {
                let url = task_urls
                    .remove(&error.id())
                    .unwrap_or_else(|| "unknown tracker scrape task".into());
                let mut engine = state.lock().await;
                let snapshot = engine.tracker_scrapes.entry(url).or_default();
                snapshot.status = TrackerScrapeStatus::Error;
                snapshot.last_scrape = Some(attempted_at);
                snapshot.last_error = Some(format!("tracker scrape task failed: {error}"));
                engine.tracker_failures_recent = engine.tracker_failures_recent.saturating_add(1);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PeerCandidateCounts {
    pub(super) discovered: usize,
    pub(super) eligible: usize,
    pub(super) filtered: usize,
    pub(super) failed: usize,
    pub(super) backed_off: usize,
}

pub(super) fn peer_allowed_by_config(peer: &PeerAddr, allow_ipv6: bool) -> bool {
    peer.port != 0 && (allow_ipv6 || !peer.ip.is_ipv6())
}

pub(super) fn classify_peer_candidates(
    discovered: &[PeerAddr],
    bad_peers: &HashMap<SocketAddr, Instant>,
    peer_backoff: &HashMap<SocketAddr, Instant>,
    allow_ipv6: bool,
    peer_filter: &swarmotter_core::peer_filter::PeerFilter,
) -> (Vec<PeerAddr>, PeerCandidateCounts) {
    let mut eligible = Vec::new();
    let mut counts = PeerCandidateCounts {
        discovered: discovered.len(),
        ..Default::default()
    };
    for peer in discovered {
        if !peer_allowed_by_config(peer, allow_ipv6) || !peer_filter.admit_ip(peer.ip).is_allowed()
        {
            counts.filtered += 1;
            continue;
        }
        if peer_is_backed_off(bad_peers, peer.socket_addr()) {
            counts.failed += 1;
            continue;
        }
        if peer_is_backed_off(peer_backoff, peer.socket_addr()) {
            counts.backed_off += 1;
            continue;
        }
        counts.eligible += 1;
        eligible.push(*peer);
    }
    (eligible, counts)
}

pub(super) fn no_usable_peer_candidates(counts: &PeerCandidateCounts) -> bool {
    counts.discovered == 0
        || (counts.eligible == 0
            && counts.filtered.saturating_add(counts.failed) >= counts.discovered)
}

pub(super) fn balance_peer_families(peers: &mut Vec<PeerAddr>) {
    if peers.len() < 2 {
        return;
    }
    let mut ipv4 = Vec::new();
    let mut ipv6 = Vec::new();
    for peer in peers.iter().copied() {
        if peer.ip.is_ipv6() {
            ipv6.push(peer);
        } else {
            ipv4.push(peer);
        }
    }
    if ipv4.is_empty() || ipv6.is_empty() {
        return;
    }

    let mut balanced = Vec::with_capacity(peers.len());
    let mut v4 = 0usize;
    let mut v6 = 0usize;
    while v4 < ipv4.len() || v6 < ipv6.len() {
        if v4 < ipv4.len() {
            balanced.push(ipv4[v4]);
            v4 += 1;
        }
        if v6 < ipv6.len() {
            balanced.push(ipv6[v6]);
            v6 += 1;
        }
    }
    *peers = balanced;
}

pub(super) fn peer_scheduler_reason(counts: &PeerCandidateCounts) -> Option<String> {
    if counts.discovered == 0 {
        return Some("no peers discovered".into());
    }
    if counts.eligible == 0 {
        if counts.filtered > 0 && counts.failed == 0 && counts.backed_off == 0 {
            return Some("all discovered peers filtered by configuration".into());
        }
        if counts.failed > 0 || counts.backed_off > 0 {
            return Some(
                "all discovered peers are cooling down after failures or no progress".into(),
            );
        }
        return Some("no eligible peers after scheduler filtering".into());
    }
    if counts.eligible == 1 {
        return Some("one eligible peer; using serial fallback when parallel round is idle".into());
    }
    None
}

pub(super) fn dedupe_peers(peers: &mut Vec<PeerAddr>) {
    let mut seen = HashSet::new();
    peers.retain(|peer| seen.insert(peer.socket_addr()));
}

pub(super) fn merge_unique_peers<I>(discovered: &mut Vec<PeerAddr>, peers: I) -> usize
where
    I: IntoIterator<Item = PeerAddr>,
{
    let before = discovered.len();
    for peer in peers {
        if !discovered.contains(&peer) {
            discovered.push(peer);
        }
    }
    discovered.len().saturating_sub(before)
}

pub(super) fn add_pex_peers<I>(
    discovered: &mut Vec<PeerAddr>,
    peers: I,
    allow_ipv6: bool,
    peer_filter: &swarmotter_core::peer_filter::PeerFilter,
    max_peers: usize,
) where
    I: IntoIterator<Item = PeerAddr>,
{
    for peer in peers {
        if !peer_allowed_by_config(&peer, allow_ipv6) {
            continue;
        }
        let decision = peer_filter.admit_ip(peer.ip);
        if !decision.is_allowed() {
            tracing::debug!(
                peer = %peer.socket_addr(),
                reason = decision.audit_reason(),
                detail = ?decision.rejection_message(),
                "PEX peer rejected by admission policy"
            );
            continue;
        }
        if max_peers > 0 && discovered.len() >= max_peers {
            break;
        }
        if !discovered.contains(&peer) {
            discovered.push(peer);
        }
    }
}
