// SPDX-License-Identifier: Apache-2.0

use super::*;

fn torrent_piece_count(torrent: &Torrent) -> usize {
    torrent
        .meta
        .data_piece_count()
        .unwrap_or_else(|_| torrent.meta.piece_count())
}

fn validate_torrent_piece_progress(torrent: &Torrent, piece_count: usize) -> Result<()> {
    let expected_bitfield_bytes = piece_count.div_ceil(8);
    if torrent.progress.total != piece_count
        || torrent.progress.bitfield().as_bytes().len() != expected_bitfield_bytes
        || (piece_count..expected_bitfield_bytes.saturating_mul(8))
            .any(|index| torrent.progress.bitfield().has(index))
    {
        return Err(CoreError::Storage(format!(
            "daemon state for {} has inconsistent piece progress (total={}, expected_total={}, bitfield_bytes={}, expected_bitfield_bytes={})",
            torrent.key(),
            torrent.progress.total,
            piece_count,
            torrent.progress.bitfield().as_bytes().len(),
            expected_bitfield_bytes,
        )));
    }
    Ok(())
}

fn normalize_legacy_unresolved_magnet_progress(torrent: &mut Torrent, piece_count: usize) -> bool {
    if piece_count == 0
        || !torrent.needs_metadata
        || torrent.progress.total != 0
        || torrent.progress.pieces_have() != 0
        || !torrent.progress.bitfield().as_bytes().is_empty()
        || torrent.files.iter().any(|file| file.bytes_completed != 0)
    {
        return false;
    }

    let empty = swarmotter_core::storage::PieceBitfield::new(piece_count);
    torrent.progress.replace_from_bitfield(&empty, piece_count);
    true
}

fn daemon_state_for_persistence(
    torrents: Vec<Torrent>,
    queue: QueueState<TorrentKey>,
) -> Result<crate::state_store::DaemonState> {
    for torrent in &torrents {
        validate_torrent_piece_progress(torrent, torrent_piece_count(torrent))?;
    }
    Ok(crate::state_store::DaemonState::new(torrents, queue))
}

impl DaemonRuntime {
    pub async fn restore_persisted_state(&self) -> Result<usize> {
        let Some(path) = self.state_path.clone() else {
            return Ok(0);
        };
        let Some(mut stored) = tokio::task::spawn_blocking(move || crate::state_store::load(&path))
            .await
            .map_err(|error| CoreError::Storage(format!("load daemon state task: {error}")))??
        else {
            return Ok(0);
        };
        let traffic_allowed = self.network_health.read().await.traffic_allowed;
        let restore_config = self.config.read().await.clone();
        let mut restored = TorrentRegistry::default();
        let mut normalized_legacy_progress = 0usize;
        for mut torrent in stored.torrents.drain(..) {
            let persisted_state = torrent.state;
            if torrent
                .seeding
                .ratio_limit
                .is_some_and(|value| !value.is_finite() || value < 0.0)
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has invalid seeding.ratio_limit",
                    torrent.key()
                )));
            }
            torrent.meta.validate().map_err(|error| {
                CoreError::Storage(format!(
                    "invalid metadata for restored torrent {}: {error}",
                    torrent.key()
                ))
            })?;
            if torrent.files.len() != torrent.meta.files.len()
                || torrent.priorities.len() != torrent.meta.files.len()
                || torrent.wanted.len() != torrent.meta.files.len()
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent file settings",
                    torrent.key()
                )));
            }
            if (torrent.needs_metadata
                && torrent.magnet_info_hash.is_none()
                && torrent.magnet_identity.is_none())
                || (torrent.needs_metadata
                    && torrent.magnet_identity.is_none()
                    && torrent.magnet_info_hash == Some(InfoHash::ZERO))
                || (!torrent.needs_metadata
                    && (torrent.magnet_info_hash.is_some() || torrent.magnet_identity.is_some()))
                || (!torrent.needs_metadata && !torrent.magnet_select_only_file_indices.is_empty())
                || (torrent.needs_metadata
                    && torrent
                        .magnet_identity
                        .as_ref()
                        .is_some_and(|identity| identity.primary_key().is_none()))
                || torrent
                    .magnet_identity
                    .as_ref()
                    .is_some_and(|identity| identity.v1_info_hash() != torrent.magnet_info_hash)
            {
                return Err(CoreError::Storage(format!(
                    "daemon state for {} has inconsistent magnet identity",
                    torrent.key()
                )));
            }
            swarmotter_core::magnet::validate_direct_peers(&torrent.magnet_direct_peers).map_err(
                |error| {
                    CoreError::Storage(format!(
                        "daemon state for {} has invalid magnet direct peers: {error}",
                        torrent.key()
                    ))
                },
            )?;
            if torrent.needs_metadata {
                swarmotter_core::magnet::validate_select_only_file_indices(
                    &torrent.magnet_select_only_file_indices,
                    swarmotter_core::magnet::MAX_MAGNET_SELECT_ONLY_INDICES,
                )
                .map_err(|error| {
                    CoreError::Storage(format!(
                        "daemon state for {} has invalid magnet select-only state: {error}",
                        torrent.key()
                    ))
                })?;
            }
            let piece_count = torrent_piece_count(&torrent);
            if normalize_legacy_unresolved_magnet_progress(&mut torrent, piece_count) {
                normalized_legacy_progress += 1;
            }
            validate_torrent_piece_progress(&torrent, piece_count)?;
            let restored_bitfield = torrent.progress.bitfield().clone();
            torrent
                .progress
                .replace_from_bitfield(&restored_bitfield, piece_count);
            let previous_files = std::mem::take(&mut torrent.files);
            torrent.files = torrent
                .meta
                .files
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    let bytes_completed = previous_files[index].bytes_completed;
                    if bytes_completed > file.length {
                        return Err(CoreError::Storage(format!(
                            "daemon state for {} has file progress beyond file length",
                            torrent.key()
                        )));
                    }
                    Ok(TorrentFile {
                        index,
                        path: file.path.join("/"),
                        length: file.length,
                        bytes_completed,
                        priority: torrent.priorities[index],
                        wanted: torrent.wanted[index],
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            torrent.recompute_file_bytes_completed();
            torrent.rate_down = 0;
            torrent.rate_up = 0;
            torrent.active_peer_workers = 0;
            torrent.known_peers = 0;
            torrent.health = swarmotter_core::models::torrent::TorrentHealth::unknown();
            torrent.state = match torrent.state {
                TorrentState::Downloading => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::Downloading);
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::DownloadingMetadata => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::DownloadingMetadata);
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::Checking => {
                    if traffic_allowed {
                        TorrentState::Queued
                    } else {
                        // Recheck is storage-only and was not a live torrent
                        // transport. Preserve the block without granting an
                        // automatic network recovery intent.
                        TorrentState::NetworkBlocked
                    }
                }
                TorrentState::Seeding => {
                    if traffic_allowed {
                        TorrentState::Completed
                    } else {
                        torrent.containment_recovery_intent =
                            Some(ContainmentRecoveryIntent::Seeding);
                        TorrentState::NetworkBlocked
                    }
                }
                state => state,
            };
            if traffic_allowed && torrent.state == TorrentState::NetworkBlocked {
                if let Some(intent) = torrent.containment_recovery_intent.take() {
                    torrent.error = None;
                    torrent.state = match intent {
                        ContainmentRecoveryIntent::Downloading
                        | ContainmentRecoveryIntent::DownloadingMetadata => TorrentState::Queued,
                        ContainmentRecoveryIntent::Seeding => TorrentState::Completed,
                    };
                }
            }
            recompute_restored_seeding_lifecycle(
                &mut torrent,
                persisted_state,
                &restore_config,
                now(),
            );
            let hash = torrent.key();
            restored.add(torrent).map_err(|_| {
                CoreError::Storage(format!("duplicate torrent {hash} in daemon state"))
            })?;
        }
        if normalized_legacy_progress > 0 {
            tracing::warn!(
                count = normalized_legacy_progress,
                "normalized legacy unresolved-magnet piece progress during state restore"
            );
        }

        let config = self.config.read().await.clone();
        validate_restored_storage_ownership(restored.torrents.values(), &config)?;

        let known = restored.torrents.keys().copied().collect::<HashSet<_>>();
        let stale_queue_entries = stored
            .queue
            .order
            .iter()
            .filter(|hash| !known.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        stored.queue.remove_many(stale_queue_entries);
        stored.queue.add_many(known.iter().copied());
        stored.queue.limits = self.config.read().await.queue.clone();
        let count = restored.torrents.len();
        *self.torrent_limiters.write().await = restored
            .torrents
            .iter()
            .map(|(hash, torrent)| {
                let policy = Self::effective_policy_with_config(&config, torrent);
                (
                    *hash,
                    Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                        policy.download_limit.value,
                        policy.upload_limit.value,
                    )),
                )
            })
            .collect();
        let per_torrent_peer_limit =
            Self::effective_per_torrent_peer_limit(config.bandwidth.max_peers_per_torrent);
        *self.torrent_peer_permit_pools.write().await = restored
            .torrents
            .keys()
            .map(|hash| {
                PeerPermitPool::new(per_torrent_peer_limit, self.peer_sessions_denied.clone())
                    .map(|pool| (*hash, pool))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        *self.registry.lock().await = restored;
        *self.queue.lock().await = stored.queue;
        self.verify_restored_completed_torrents().await?;
        self.reconcile_queue().await;
        self.reconcile_seeders().await;
        self.persist_state().await?;
        tracing::info!(count, path = %self.state_path.as_ref().unwrap().display(), "restored daemon state");
        Ok(count)
    }

    pub(super) async fn verify_restored_completed_torrents(&self) -> Result<()> {
        let cfg = self.config.read().await.clone();
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .filter(|torrent| torrent.state == TorrentState::Completed)
            .cloned()
            .collect::<Vec<_>>();
        for torrent in torrents {
            let hash = torrent.key();
            let complete_dir = self.resolve_download_dir(&torrent).await;
            let storage_dir = if torrent.progress.is_complete() {
                complete_dir
            } else {
                self.resolve_incomplete_dir_for(&torrent).await
            };
            let storage =
                storage_io_with_config(torrent.meta.clone(), PathBuf::from(storage_dir), &cfg)
                    .with_partial_file_suffix(
                        (!torrent.progress.is_complete())
                            .then(|| Self::partial_file_suffix_for_active_storage(&torrent))
                            .flatten(),
                    );
            match self
                .recheck_storage_under_root_control(&storage, None)
                .await
            {
                Ok(bitfield) => {
                    let selection_complete = torrent_selection_complete(&torrent, &bitfield)?;
                    let traffic_allowed = self.network_health.read().await.traffic_allowed;
                    if let Some(restored) = self.registry.lock().await.get_mut(&hash) {
                        restored.progress.replace_from_bitfield(
                            &bitfield,
                            restored
                                .meta
                                .data_piece_count()
                                .unwrap_or_else(|_| restored.meta.piece_count()),
                        );
                        restored.recompute_file_bytes_completed();
                        if !selection_complete {
                            restored.state = if traffic_allowed {
                                TorrentState::Queued
                            } else {
                                TorrentState::NetworkBlocked
                            };
                            restored.error = Some(
                                "restored payload failed verification; selected pieces queued for recovery"
                                    .into(),
                            );
                            restored.seeding_status = SeedingStatus::NotEligible;
                        } else {
                            restored.seeding_status = SeedingStatus::Queued;
                        }
                    }
                }
                Err(error) => {
                    if let Some(restored) = self.registry.lock().await.get_mut(&hash) {
                        restored.state = TorrentState::StorageError;
                        restored.error = Some(error.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn persist_state(&self) -> Result<()> {
        self.persist_state_with_original_metainfo(None).await
    }

    /// Persist the registry/queue generation and, for a freshly accepted
    /// `.torrent` document, retain its exact original bytes in the same local
    /// SQLite transaction. The raw document is intentionally distinct from
    /// canonical BEP 9 `info` metadata; the authenticated native export path
    /// may return only this exact representation.
    async fn persist_state_with_original_metainfo(
        &self,
        original_metainfo: Option<crate::state_store::OriginalMetainfo>,
    ) -> Result<()> {
        let Some(path) = self.state_path.clone() else {
            return Ok(());
        };
        let _write_guard = self.state_write_lock.lock().await;
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        let queue = self.queue.lock().await.clone();
        let state = daemon_state_for_persistence(torrents, queue)?;
        tokio::task::spawn_blocking(move || {
            crate::state_store::save_with_original_metainfo(&path, &state, original_metainfo)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("save daemon state task: {error}")))??;
        Ok(())
    }

    /// Return the exact full `.torrent` document retained when this torrent
    /// was originally added from a file or watch import.
    ///
    /// This is strictly local, read-only state access. Magnet metadata and
    /// canonical BEP 9 `info` bytes are intentionally not substitutes for an
    /// original full metainfo document.
    pub(super) async fn retained_original_metainfo(&self, hash: TorrentKey) -> Result<Vec<u8>> {
        if !self.registry.lock().await.contains(&hash) {
            return Err(CoreError::NotFound("torrent".into()));
        }
        let Some(path) = self.state_path.clone() else {
            return Err(CoreError::NotFound(
                "original torrent metainfo is unavailable for this torrent".into(),
            ));
        };
        // Serialize with state saves so the read-only lookup's sidecar cleanup
        // cannot remove a WAL file a concurrent writer is still relying on.
        let _write_guard = self.state_write_lock.lock().await;
        let bytes = tokio::task::spawn_blocking(move || {
            crate::state_store::load_original_metainfo(&path, hash)
        })
        .await
        .map_err(|error| {
            CoreError::Storage(format!("read original metainfo state task: {error}"))
        })??;
        bytes.ok_or_else(|| {
            CoreError::NotFound("original torrent metainfo is unavailable for this torrent".into())
        })
    }

    /// Persist a state transition that must be paired with another durable
    /// transaction. A state-directory sync can fail after rename, so capture
    /// and restore the previous raw file while holding the state-write lock.
    /// On error the state file is therefore returned to the generation that
    /// existed before this call whenever the rollback succeeds.
    pub(super) async fn persist_state_with_file_rollback(&self) -> Result<()> {
        let Some(path) = self.state_path.clone() else {
            return Ok(());
        };
        let _write_guard = self.state_write_lock.lock().await;
        let capture_path = path.clone();
        let snapshot =
            tokio::task::spawn_blocking(move || crate::state_store::capture_file(&capture_path))
                .await
                .map_err(|error| {
                    CoreError::Storage(format!("capture daemon state task: {error}"))
                })??;
        let torrents = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect();
        let queue = self.queue.lock().await.clone();
        let state = daemon_state_for_persistence(torrents, queue)?;
        let write_path = path.clone();
        let persisted =
            tokio::task::spawn_blocking(move || crate::state_store::save(&write_path, &state))
                .await
                .map_err(|error| CoreError::Storage(format!("save daemon state task: {error}")))?;
        if let Err(error) = persisted {
            let rollback_path = path.clone();
            let rollback_snapshot = snapshot.clone();
            let rollback = tokio::task::spawn_blocking(move || {
                crate::state_store::restore_file(&rollback_path, &rollback_snapshot)
            })
            .await
            .map_err(|join_error| {
                CoreError::Storage(format!("restore daemon state task: {join_error}"))
            })?;
            return Err(CoreError::Storage(format!(
                "save daemon state: {error}; state rollback: {rollback:?}"
            )));
        }
        Ok(())
    }

    pub(super) async fn persist_state_best_effort(&self, reason: &'static str) {
        if let Err(error) = self.persist_state().await {
            tracing::error!(reason, %error, "failed to persist daemon state");
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        // Release an opted-in router lease while its original contained binder
        // is still alive. A failed release is non-fatal and never attempts a
        // default-route fallback; the bounded router lease remains the final
        // cleanup guard.
        self.release_port_mapping_on_shutdown().await;
        self.reconcile_engine_progress().await;
        let hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.stop_all_torrent_tasks(&hashes).await;
        self.persist_state().await
    }

    pub(super) fn publish_event(&self, event: Event) {
        self.event_broker.publish(event);
    }

    pub(super) fn publish_torrent_event(
        &self,
        kind: &'static str,
        hash: TorrentKey,
        state: TorrentState,
    ) {
        self.publish_event(torrent_event(kind, hash, state));
    }

    #[allow(dead_code)]
    pub async fn add_torrent_file(
        &self,
        bytes: Vec<u8>,
        download_dir: Option<String>,
    ) -> Result<TorrentKey> {
        self.add_torrent_file_with_options(bytes, AddTorrentOptions::new(download_dir, false))
            .await
    }

    #[allow(dead_code)]
    pub async fn add_magnet(
        &self,
        magnet: &str,
        download_dir: Option<String>,
    ) -> Result<TorrentKey> {
        self.add_magnet_with_options(magnet, AddTorrentOptions::new(download_dir, false))
            .await
    }

    pub async fn add_torrent_file_with_options(
        &self,
        bytes: Vec<u8>,
        options: AddTorrentOptions,
    ) -> Result<TorrentKey> {
        // Keep profile resolution and the durable registration in the same
        // transaction as profile replacement. Otherwise a profile could be
        // removed after this add validates it but before its attachment is
        // persisted into daemon state.
        let _config_transaction = self.config_write_lock.lock().await;
        let parsed = match meta::parse_torrent(&bytes) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    error_code = %e.code(),
                    "torrent file add rejected"
                );
                return Err(e);
            }
        };
        let mut t = Torrent::new(parsed, now());
        let hash = t.key();
        if let Some(d) = options.download_dir.clone() {
            t.download_dir = Some(d);
        }
        if let Some(d) = options.incomplete_dir.clone() {
            t.policy.overrides.incomplete_dir = Some(d);
        }
        let paused = self.apply_add_profile(&mut t, &options).await?;
        match self
            .add_torrent_mutation_with_original_metainfo(
                t,
                paused,
                "torrent_file_added",
                Some(bytes),
            )
            .await?
        {
            TorrentAddMutationOutcome::Inserted { state, .. } => {
                tracing::info!(
                    info_hash = %hash,
                    network_blocked = state == TorrentState::NetworkBlocked,
                    paused = state == TorrentState::Paused,
                    "torrent file added"
                );
                Ok(hash)
            }
            TorrentAddMutationOutcome::Duplicate { .. } => {
                tracing::warn!(
                    info_hash = %hash,
                    error_code = %CoreError::DuplicateTorrent(hash.to_locator()).code(),
                    "torrent file add rejected: duplicate"
                );
                Err(CoreError::DuplicateTorrent(hash.to_locator()))
            }
        }
    }

    pub(super) async fn add_magnet_with_options(
        &self,
        magnet: &str,
        options: AddTorrentOptions,
    ) -> Result<TorrentKey> {
        // See `add_torrent_file_with_options`: this lock makes profile
        // selection and durable registration indivisible from profile PUT.
        let _config_transaction = self.config_write_lock.lock().await;
        let m = Magnet::parse(magnet)?;
        let hash = m.identity.primary_key().ok_or_else(|| {
            CoreError::UnsupportedTorrentFeature(
                "magnet is missing a supported full torrent identity".into(),
            )
        })?;
        let name = m.display_name.clone().unwrap_or_else(|| hash.to_locator());
        // Build a placeholder single-file torrent so the registry has a record;
        // the real metadata is fetched via BEP 9 from peers once the engine
        // starts. The registry is keyed by the magnet's real info hash.
        let bytes = meta::build_single_file_torrent(
            &name,
            b"magnet placeholder data",
            16,
            m.trackers.first().map(|s| s.as_str()),
            false,
        );
        let parsed = meta::parse_torrent(&bytes)?;
        // Preserve the placeholder's own canonical raw v1 metadata. The
        // registry key comes from `magnet_info_hash`, while the full magnet
        // identity remains separate so a synthetic raw `info` value is never
        // mislabeled as a real hybrid identity.
        let mut t = Torrent::new(parsed, now());
        t.needs_metadata = true;
        t.magnet_info_hash = m.v1_info_hash();
        t.magnet_identity = Some(m.identity.clone());
        t.magnet_name = Some(name);
        t.magnet_trackers = m.trackers.clone();
        t.magnet_select_only_file_indices = m.select_only_file_indices.clone();
        t.magnet_direct_peers = m.direct_peers.clone();
        if let Some(d) = options.download_dir.clone() {
            t.download_dir = Some(d);
        }
        if let Some(d) = options.incomplete_dir.clone() {
            t.policy.overrides.incomplete_dir = Some(d);
        }
        let paused = self.apply_add_profile(&mut t, &options).await?;
        match self.add_torrent_mutation(t, paused, "magnet_added").await? {
            TorrentAddMutationOutcome::Inserted { state, .. } => {
                tracing::info!(
                    info_hash = %hash,
                    network_blocked = state == TorrentState::NetworkBlocked,
                    paused = state == TorrentState::Paused,
                    tracker_count = m.trackers.len(),
                    "magnet added"
                );
                Ok(hash)
            }
            TorrentAddMutationOutcome::Duplicate { .. } => {
                Err(CoreError::DuplicateTorrent(hash.to_locator()))
            }
        }
    }

    /// Shared durable add transaction for API, magnet, and watch ingestion.
    /// Parsing happens before entry. Storage and containment preflight mutate
    /// only the candidate. The storage-ownership lock then spans path
    /// validation, exact hash snapshots, insertion, persistence, and rollback.
    pub(super) async fn add_torrent_mutation(
        &self,
        torrent: Torrent,
        requested_paused: bool,
        schedule_reason: &'static str,
    ) -> Result<TorrentAddMutationOutcome> {
        self.add_torrent_mutation_with_original_metainfo(
            torrent,
            requested_paused,
            schedule_reason,
            None,
        )
        .await
    }

    pub(super) async fn add_torrent_mutation_with_original_metainfo(
        &self,
        mut torrent: Torrent,
        requested_paused: bool,
        schedule_reason: &'static str,
        original_metainfo: Option<Vec<u8>>,
    ) -> Result<TorrentAddMutationOutcome> {
        let hash = torrent.key();
        let mutation_guard = self.storage_ownership_lock.lock().await;
        let previous_torrent = self.registry.lock().await.get(&hash).cloned();
        let previous_queue = self.queue.lock().await.membership_snapshot(&hash);
        if previous_torrent.is_some() {
            return Ok(TorrentAddMutationOutcome::Duplicate { hash });
        }
        self.preflight_storage_for_torrent(
            &torrent,
            if torrent.needs_metadata {
                0
            } else {
                torrent.meta.total_length
            },
        )
        .await?;
        apply_network_state(&mut torrent, &self.network_health).await;
        if requested_paused && torrent.state != TorrentState::NetworkBlocked {
            torrent.state = TorrentState::Paused;
        }
        let committed_state = torrent.state;

        self.ensure_storage_paths_available_for_torrent(&torrent, None)
            .await?;

        self.registry
            .lock()
            .await
            .add(torrent)
            .map_err(|_| CoreError::DuplicateTorrent(hash.to_locator()))?;
        self.queue.lock().await.add(hash);

        let persistence = if self.add_mutation_persistence_failure_injected() {
            Err(CoreError::Storage(
                "injected shared torrent-add persistence failure".into(),
            ))
        } else {
            self.persist_state_with_original_metainfo(
                original_metainfo
                    .map(|bytes| crate::state_store::OriginalMetainfo::new(hash, bytes)),
            )
            .await
        };
        if let Err(error) = persistence {
            let mut registry = self.registry.lock().await;
            registry.remove(&hash);
            if let Some(previous) = previous_torrent {
                registry.add(previous).map_err(|_| {
                    CoreError::Internal("restore prior torrent after failed add".into())
                })?;
            }
            drop(registry);
            self.queue
                .lock()
                .await
                .restore_membership(hash, previous_queue);
            return Err(error);
        }

        let inserted = self
            .registry
            .lock()
            .await
            .get(&hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        let policy = self.effective_policy(&inserted).await;
        self.ensure_torrent_limiter(hash, policy.download_limit.value, policy.upload_limit.value)
            .await;
        self.ensure_torrent_peer_permit_pool(hash).await;
        drop(mutation_guard);
        if committed_state == TorrentState::Queued {
            self.schedule_reconcile_queue(schedule_reason).await;
        }
        self.publish_torrent_event("torrent_added", hash, committed_state);
        self.publish_event(stats_updated_event());
        Ok(TorrentAddMutationOutcome::Inserted {
            hash,
            state: committed_state,
        })
    }

    pub(super) async fn remove_torrents_with_single_reconcile(
        &self,
        hashes: Vec<TorrentKey>,
        delete_data: bool,
    ) -> Result<Vec<TorrentKey>> {
        let mut unique_hashes = Vec::with_capacity(hashes.len());
        let mut seen = HashSet::with_capacity(hashes.len());
        for hash in hashes {
            if seen.insert(hash) {
                unique_hashes.push(hash);
            }
        }

        let targets = {
            let reg = self.registry.lock().await;
            let mut canonical = HashSet::with_capacity(unique_hashes.len());
            unique_hashes
                .into_iter()
                .filter_map(|hash| {
                    reg.canonical_key(&hash)
                        .and_then(|key| reg.get(&key).cloned().map(|torrent| (key, torrent)))
                })
                .filter(|(hash, _)| canonical.insert(*hash))
                .collect::<Vec<_>>()
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let cfg = self.config.read().await.clone();
        for (hash, _) in &targets {
            self.force_stop_engine(hash).await;
        }
        if delete_data {
            for (hash, torrent) in &targets {
                let complete_dir = self.resolve_download_dir(torrent).await;
                let active_dir = self.resolve_incomplete_dir_for(torrent).await;
                let active_partial_file_suffix =
                    Self::partial_file_suffix_for_active_storage(torrent);
                let mut storages = vec![storage_io_with_config(
                    torrent.meta.clone(),
                    std::path::PathBuf::from(&active_dir),
                    &cfg,
                )
                .with_partial_file_suffix(active_partial_file_suffix.clone())];
                // A shared active/complete root can contain both incomplete
                // suffixed files and already-final canonical files. Remove
                // both representations without relying on a root-only dedup.
                if active_dir != complete_dir || active_partial_file_suffix.is_some() {
                    storages.push(storage_io_with_config(
                        torrent.meta.clone(),
                        std::path::PathBuf::from(&complete_dir),
                        &cfg,
                    ));
                }
                for storage in storages {
                    if let Err(error) = storage.remove_all().await {
                        if let Some(torrent) = self.registry.lock().await.get_mut(hash) {
                            torrent.state = TorrentState::StorageError;
                            torrent.error = Some(error.to_string());
                        }
                        self.persist_state_best_effort("remove_failed").await;
                        return Err(error);
                    }
                }
            }
        }
        {
            let mut reg = self.registry.lock().await;
            for (hash, _) in &targets {
                reg.remove(hash);
            }
        }
        self.queue
            .lock()
            .await
            .remove_many(targets.iter().map(|(hash, _)| *hash));
        {
            let mut rate_samples = self.rate_samples.write().await;
            let mut decisions = self.autopilot_decisions.write().await;
            let mut last_actions = self.autopilot_last_action.write().await;
            let mut limiters = self.torrent_limiters.write().await;
            let mut peer_permit_pools = self.torrent_peer_permit_pools.write().await;
            for (hash, _) in &targets {
                rate_samples.remove(hash);
                decisions.remove(hash);
                last_actions.remove(hash);
                limiters.remove(hash);
                peer_permit_pools.remove(hash);
            }
        }
        self.persist_state().await?;
        self.reconcile_queue().await;
        let removed_hashes = targets
            .into_iter()
            .map(|(hash, _)| {
                self.publish_event(torrent_removed_event(hash, delete_data));
                hash
            })
            .collect();
        self.publish_event(stats_updated_event());
        Ok(removed_hashes)
    }

    /// Resolve the download directory for a torrent: per-torrent override,
    /// profile creation snapshot, then global config, then a default temp dir.
    pub(super) async fn resolve_download_dir(&self, t: &Torrent) -> String {
        self.policy_storage_paths(t).await.0
    }

    /// Resolve the active incomplete path for a torrent. Unlike the legacy
    /// helper, this sees a profile's create-time incomplete-path snapshot.
    pub(super) async fn resolve_incomplete_dir_for(&self, t: &Torrent) -> String {
        self.policy_storage_paths(t).await.1
    }

    pub(super) async fn ensure_torrent_limiter(
        &self,
        hash: TorrentKey,
        download_limit: u64,
        upload_limit: u64,
    ) -> Arc<swarmotter_core::bandwidth::RateLimiter> {
        self.torrent_limiters
            .write()
            .await
            .entry(hash)
            .or_insert_with(|| {
                Arc::new(swarmotter_core::bandwidth::RateLimiter::new(
                    download_limit,
                    upload_limit,
                ))
            })
            .clone()
    }

    pub(super) fn effective_per_torrent_peer_limit(configured: usize) -> usize {
        if configured == 0 {
            DEFAULT_PER_TORRENT_PEER_LIMIT
        } else {
            configured
        }
    }

    pub(super) async fn ensure_torrent_peer_permit_pool(
        &self,
        hash: TorrentKey,
    ) -> Arc<PeerPermitPool> {
        if let Some(pool) = self
            .torrent_peer_permit_pools
            .read()
            .await
            .get(&hash)
            .cloned()
        {
            return pool;
        }
        let configured = self.config.read().await.bandwidth.max_peers_per_torrent;
        let limit = Self::effective_per_torrent_peer_limit(configured);
        let candidate = PeerPermitPool::new(limit, self.peer_sessions_denied.clone())
            .unwrap_or_else(|_| {
                PeerPermitPool::invalid_fail_closed(limit, self.peer_sessions_denied.clone())
            });
        self.torrent_peer_permit_pools
            .write()
            .await
            .entry(hash)
            .or_insert(candidate)
            .clone()
    }

    pub(super) async fn peer_session_budget(&self, hash: TorrentKey) -> PeerSessionBudget {
        let global = self.peer_permit_pool.read().await.clone();
        let torrent = self.ensure_torrent_peer_permit_pool(hash).await;
        PeerSessionBudget::new(global, torrent)
    }

    pub(super) async fn peer_permit_snapshot(&self) -> PeerPermitSnapshot {
        self.peer_permit_pool.read().await.snapshot()
    }

    pub(super) async fn build_peer_permit_configuration(
        &self,
        config: &Config,
    ) -> Result<PeerPermitConfiguration> {
        let global = PeerPermitPool::new(
            config.bandwidth.max_peers,
            self.peer_sessions_denied.clone(),
        )?;
        let per_torrent_limit =
            Self::effective_per_torrent_peer_limit(config.bandwidth.max_peers_per_torrent);
        let hashes = self
            .registry
            .lock()
            .await
            .torrents
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let per_torrent = hashes
            .into_iter()
            .map(|hash| {
                PeerPermitPool::new(per_torrent_limit, self.peer_sessions_denied.clone())
                    .map(|pool| (hash, pool))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(PeerPermitConfiguration {
            global,
            per_torrent,
        })
    }

    pub(super) async fn install_peer_permit_configuration(&self, next: PeerPermitConfiguration) {
        *self.peer_permit_pool.write().await = next.global;
        *self.torrent_peer_permit_pools.write().await = next.per_torrent;
    }

    pub(super) async fn current_peer_permit_configuration(&self) -> PeerPermitConfiguration {
        PeerPermitConfiguration {
            global: self.peer_permit_pool.read().await.clone(),
            per_torrent: self.torrent_peer_permit_pools.read().await.clone(),
        }
    }

    pub(super) async fn verify_peer_permit_configuration_identity(
        &self,
        expected: &PeerPermitConfiguration,
    ) -> Result<()> {
        let actual = self.current_peer_permit_configuration().await;
        let same = Arc::ptr_eq(&actual.global, &expected.global)
            && actual.global.snapshot().limit == expected.global.snapshot().limit
            && actual.per_torrent.len() == expected.per_torrent.len()
            && expected.per_torrent.iter().all(|(hash, pool)| {
                actual.per_torrent.get(hash).is_some_and(|actual| {
                    Arc::ptr_eq(actual, pool) && actual.snapshot().limit == pool.snapshot().limit
                })
            });
        if same {
            Ok(())
        } else {
            Err(CoreError::Internal(
                "peer permit configuration identity or size mismatch".into(),
            ))
        }
    }

    pub(super) async fn wait_for_peer_permit_configuration_drain(
        &self,
        permits: &PeerPermitConfiguration,
    ) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let global_drained = permits.global.snapshot().in_use == 0;
                let torrents_drained = permits
                    .per_torrent
                    .values()
                    .all(|pool| pool.snapshot().in_use == 0);
                if global_drained && torrents_drained {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            CoreError::Internal(
                "timed out awaiting old peer-session permits during reconstruction".into(),
            )
        })
    }

    #[cfg(test)]
    pub(super) fn inject_peer_reconfiguration_failure_after_teardown(&self) {
        self.peer_reconfiguration_fail_after_teardown
            .store(true, Ordering::Release);
    }

    pub(super) fn peer_reconfiguration_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.peer_reconfiguration_fail_after_teardown
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    pub(super) fn inject_peer_reconfiguration_persistence_failure(&self) {
        self.peer_reconfiguration_fail_persistence
            .store(true, Ordering::Release);
    }

    pub(super) fn peer_reconfiguration_persistence_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.peer_reconfiguration_fail_persistence
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    pub(super) async fn pause_peer_reconfiguration_before_reconstruction(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.peer_reconfiguration_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    pub(super) async fn wait_at_peer_reconfiguration_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self.peer_reconfiguration_pause.lock().await.take() {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    #[cfg(test)]
    pub(super) async fn pause_peer_reconfiguration_before_persistence(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.peer_reconfiguration_persistence_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    pub(super) async fn wait_at_peer_reconfiguration_persistence_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self
            .peer_reconfiguration_persistence_pause
            .lock()
            .await
            .take()
        {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    #[cfg(test)]
    pub(super) fn inject_add_mutation_persistence_failure(&self) {
        self.add_mutation_fail_persistence
            .store(true, Ordering::Release);
    }

    pub(super) fn add_mutation_persistence_failure_injected(&self) -> bool {
        #[cfg(test)]
        {
            self.add_mutation_fail_persistence
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    #[cfg(test)]
    pub(super) async fn pause_watch_after_bounded_read(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        *self.watch_after_read_pause.lock().await = Some((reached_tx, continue_rx));
        (reached_rx, continue_tx)
    }

    pub(super) async fn wait_at_watch_after_read_test_pause(&self) {
        #[cfg(test)]
        if let Some((reached, continue_rx)) = self.watch_after_read_pause.lock().await.take() {
            let _ = reached.send(());
            let _ = continue_rx.await;
        }
    }

    /// Preflight a candidate using its resolved profile storage snapshot.
    pub(super) async fn preflight_storage_for_torrent(
        &self,
        torrent: &Torrent,
        total_length: u64,
    ) -> Result<()> {
        let cfg = self.config.read().await.clone();
        if cfg.storage.minimum_free_space_bytes == 0 && cfg.storage.minimum_free_space_percent == 0
        {
            return Ok(());
        }
        let (complete_dir, active_dir) = Self::policy_storage_paths_with_config(&cfg, torrent);
        for dir in unique_pathbufs([PathBuf::from(active_dir), PathBuf::from(complete_dir)]) {
            swarmotter_core::storage::check_storage_preflight(&dir, &cfg.storage, total_length)?;
        }
        Ok(())
    }

    /// Validate ownership for a candidate torrent at its effective storage
    /// paths. Existing profile-derived paths are resolved from their durable
    /// snapshots; no profile value is copied into an override.
    pub(super) async fn ensure_storage_paths_available_for_torrent(
        &self,
        torrent: &Torrent,
        exclude: Option<TorrentKey>,
    ) -> Result<()> {
        let cfg = self.config.read().await.clone();
        let (complete_dir, active_dir) = Self::policy_storage_paths_with_config(&cfg, torrent);
        // A magnet placeholder deliberately retains its synthetic canonical
        // metadata (including raw `info` bytes). Storage ownership must still
        // use the real canonical registry key so two unresolved magnets
        // with the same display path cannot claim the same payload location.
        // This clone is used only for collision detection; it is never
        // persisted or presented as resolved metainfo.
        self.ensure_storage_paths_available_at_paths_except(
            &torrent.meta,
            &complete_dir,
            &active_dir,
            Self::partial_file_suffix_for_active_storage(torrent).as_deref(),
            exclude,
        )
        .await
    }

    pub(super) async fn ensure_storage_paths_available_at_paths_except(
        &self,
        meta: &meta::TorrentMeta,
        complete_dir: &str,
        active_dir: &str,
        active_partial_file_suffix: Option<&str>,
        exclude: Option<TorrentKey>,
    ) -> Result<()> {
        let cfg = self.config.read().await.clone();
        let candidates = vec![
            storage_io_with_config(meta.clone(), PathBuf::from(complete_dir), &cfg)
                .path_ownership()?,
            storage_io_with_config(meta.clone(), PathBuf::from(active_dir), &cfg)
                .with_partial_file_suffix(active_partial_file_suffix.map(str::to_owned))
                .path_ownership()?,
        ];
        let existing = self
            .registry
            .lock()
            .await
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for torrent in existing {
            if exclude.is_some_and(|hash| torrent.key() == hash) {
                continue;
            }
            let (complete_dir, active_dir) = Self::policy_storage_paths_with_config(&cfg, &torrent);
            let ownerships = vec![
                storage_io_with_config(torrent.meta.clone(), PathBuf::from(complete_dir), &cfg)
                    .path_ownership()?,
                storage_io_with_config(torrent.meta.clone(), PathBuf::from(active_dir), &cfg)
                    .with_partial_file_suffix(Self::partial_file_suffix_for_active_storage(
                        &torrent,
                    ))
                    .path_ownership()?,
            ];
            for ownership in ownerships {
                for candidate in &candidates {
                    candidate.ensure_compatible_with(&ownership)?;
                }
            }
        }
        Ok(())
    }

    /// Reject an unresolved magnet's deferred file selection once its real
    /// metadata makes the file count authoritative. The failure itself is
    /// durable: otherwise a bad request could look accepted after a restart
    /// even though it never produced a reviewable file tree.
    pub(super) async fn validate_resolved_magnet_intake_selection(
        &self,
        hash: TorrentKey,
        file_count: usize,
    ) -> Result<()> {
        let selection_error = {
            let registry = self.registry.lock().await;
            let torrent = registry
                .get(&hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            swarmotter_core::magnet::validate_select_only_file_indices(
                &torrent.magnet_select_only_file_indices,
                file_count,
            )
            .err()
            .or_else(|| {
                torrent
                    .policy
                    .intake_snapshot
                    .as_ref()
                    .and_then(|snapshot| {
                        swarmotter_core::policy::validate_intake_selection_indices(
                            snapshot, file_count,
                        )
                        .err()
                    })
            })
        };
        let Some(selection_error) = selection_error else {
            return Ok(());
        };

        let previous = self
            .registry
            .lock()
            .await
            .get(&hash)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
        {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(&hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            torrent.state = TorrentState::Error;
            torrent.seeding_status = SeedingStatus::NotEligible;
            torrent.error = Some(selection_error.clone());
        }
        if let Err(error) = self.persist_state_with_file_rollback().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                *torrent = previous;
            }
            return Err(error);
        }
        Err(CoreError::InvalidArgument(selection_error))
    }

    /// Commit a metadata-only preview after BEP 9 resolution.
    ///
    /// Unlike a normal magnet resolution this deliberately does not reserve a
    /// storage root or touch a payload path: the engine has stopped before
    /// storage preflight, layout creation, announce, or piece requests. The
    /// resolved file tree and its add-time intake selection are made visible
    /// only after the complete paused record is durable. If the state write
    /// fails, both the on-disk generation and the in-memory placeholder are
    /// restored, so a later retry cannot inherit an unreviewed file list.
    ///
    pub(super) async fn commit_metadata_preview_resolution(
        &self,
        hash: TorrentKey,
        resolved: meta::TorrentMeta,
    ) -> Result<()> {
        if resolved.identity.primary_key() != Some(hash) {
            return Err(CoreError::MalformedTorrent(
                "resolved magnet metadata info hash changed during preview".into(),
            ));
        }
        self.validate_resolved_magnet_intake_selection(hash, resolved.files.len())
            .await?;

        // Hold the state-write generation lock from the pre-mutation disk
        // snapshot through save/rollback. A concurrent normal persistence
        // therefore cannot publish this preview's metadata before this
        // transaction succeeds.
        let _write_guard = self.state_write_lock.lock().await;
        let state_path = self.state_path.clone();
        let disk_snapshot = if let Some(path) = state_path.as_ref() {
            let capture_path = path.clone();
            Some(
                tokio::task::spawn_blocking(move || {
                    crate::state_store::capture_file(&capture_path)
                })
                .await
                .map_err(|error| {
                    CoreError::Storage(format!("capture daemon state task: {error}"))
                })??,
            )
        } else {
            None
        };

        let previous = {
            let mut registry = self.registry.lock().await;
            let torrent = registry
                .get_mut(&hash)
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            // Do not let a completed metadata-only task overwrite an explicit
            // Start/Resume that has already released the durable payload gate.
            if !torrent.needs_metadata || !torrent.policy.preview_until_started {
                return Err(storage_work_cancelled_error());
            }
            let previous = torrent.clone();
            let piece_count = resolved
                .data_piece_count()
                .unwrap_or_else(|_| resolved.piece_count());
            let empty_state = EngineState {
                pieces_have: swarmotter_core::storage::PieceBitfield::new(piece_count),
                piece_count,
                total_length: resolved.total_length,
                ..EngineState::default()
            };
            apply_resolved_metadata(torrent, &resolved, &empty_state);
            torrent.state = TorrentState::Paused;
            torrent.seeding_status = SeedingStatus::NotEligible;
            torrent.error = None;
            previous
        };

        let persisted = if let Some(path) = state_path.as_ref() {
            let torrents = self
                .registry
                .lock()
                .await
                .torrents
                .values()
                .cloned()
                .collect();
            let queue = self.queue.lock().await.clone();
            match daemon_state_for_persistence(torrents, queue) {
                Ok(state) => {
                    let write_path = path.clone();
                    tokio::task::spawn_blocking(move || {
                        crate::state_store::save(&write_path, &state)
                    })
                    .await
                    .map_err(|error| {
                        CoreError::Storage(format!("save daemon state task: {error}"))
                    })?
                }
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };

        if let Err(error) = persisted {
            let rollback = match (state_path.as_ref(), disk_snapshot.as_ref()) {
                (Some(path), Some(snapshot)) => {
                    let rollback_path = path.clone();
                    let rollback_snapshot = snapshot.clone();
                    tokio::task::spawn_blocking(move || {
                        crate::state_store::restore_file(&rollback_path, &rollback_snapshot)
                    })
                    .await
                    .map_err(|join_error| {
                        CoreError::Storage(format!("restore daemon state task: {join_error}"))
                    })?
                }
                _ => Ok(()),
            };
            if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                // An explicit lifecycle start waits for this engine task, but
                // preserve a newer non-preview mutation defensively rather
                // than clobbering it with the placeholder.
                if torrent.policy.preview_until_started {
                    *torrent = previous;
                }
            }
            return Err(CoreError::Storage(format!(
                "persist metadata preview resolution: {error}; state rollback: {rollback:?}"
            )));
        }
        Ok(())
    }

    pub(super) async fn reserve_resolved_magnet_metadata(
        &self,
        hash: TorrentKey,
        resolved: meta::TorrentMeta,
        complete_dir: String,
        active_dir: String,
        cancellation: StorageWorkCancellation,
    ) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(storage_work_cancelled_error());
        }
        if resolved.identity.primary_key() != Some(hash) {
            return Err(CoreError::MalformedTorrent(
                "resolved magnet metadata info hash changed during preflight".into(),
            ));
        }
        self.validate_resolved_magnet_intake_selection(hash, resolved.files.len())
            .await?;
        // Validate ownership before waiting for a root budget, but never hold
        // the ownership mutex while a bounded root is saturated. Other adds,
        // moves, and metadata resolutions must remain able to make progress.
        let initial_previous = {
            let _storage_ownership = tokio::select! {
                guard = self.storage_ownership_lock.lock() => guard,
                _ = cancellation.cancelled() => return Err(storage_work_cancelled_error()),
            };
            let current = self
                .registry
                .lock()
                .await
                .get(&hash)
                .cloned()
                .ok_or_else(|| CoreError::NotFound("torrent".into()))?;
            let partial_file_suffix = Self::partial_file_suffix_for_active_storage(&current);
            self.ensure_storage_paths_available_at_paths_except(
                &resolved,
                &complete_dir,
                &active_dir,
                partial_file_suffix.as_deref(),
                Some(hash),
            )
            .await?;
            current
        };
        let storage_admission = loop {
            if cancellation.is_cancelled() {
                return Err(storage_work_cancelled_error());
            }
            let storage_admission = {
                let cfg = self.config.read().await;
                storage_root_admission_for_path(&cfg, Path::new(&active_dir))
            };
            let Some(admission) = storage_admission else {
                break None;
            };
            if !self
                .storage_admissions
                .declared_bytes_can_fit(&admission, resolved.total_length)
            {
                return Err(CoreError::Storage(format!(
                    "storage root admission cannot fit resolved magnet metadata for {}: declared payload {} bytes exceeds configured active-byte limit {}",
                    admission.root.display(),
                    resolved.total_length,
                    admission.max_active_bytes
                )));
            }
            // Register before testing the atomic reservation so a completed
            // root engine or a configuration change cannot lose its wake-up.
            let changed = self.storage_admissions.changed();
            if self
                .storage_admissions
                .reserve(hash, &admission, resolved.total_length)
                .await
                .is_ok()
            {
                break Some(admission);
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Err(storage_work_cancelled_error()),
                _ = changed => {}
            }
        };
        if cancellation.is_cancelled() {
            if storage_admission.is_some() {
                self.storage_admissions.release(&hash).await;
            }
            return Err(storage_work_cancelled_error());
        }
        let _storage_ownership = tokio::select! {
            guard = self.storage_ownership_lock.lock() => guard,
            _ = cancellation.cancelled() => {
                if storage_admission.is_some() {
                    self.storage_admissions.release(&hash).await;
                }
                return Err(storage_work_cancelled_error());
            }
        };
        if cancellation.is_cancelled() {
            if storage_admission.is_some() {
                self.storage_admissions.release(&hash).await;
            }
            return Err(storage_work_cancelled_error());
        }
        if let Err(error) = self
            .ensure_storage_paths_available_at_paths_except(
                &resolved,
                &complete_dir,
                &active_dir,
                Self::partial_file_suffix_for_active_storage(&initial_previous).as_deref(),
                Some(hash),
            )
            .await
        {
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, initial_previous.meta.total_length)
                    .await;
            }
            return Err(error);
        }
        if cancellation.is_cancelled() {
            if storage_admission.is_some() {
                self.storage_admissions.release(&hash).await;
            }
            return Err(storage_work_cancelled_error());
        }
        let previous = self.registry.lock().await.get(&hash).cloned();
        let Some(previous) = previous else {
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, initial_previous.meta.total_length)
                    .await;
            }
            return Err(CoreError::NotFound("torrent".into()));
        };
        let updated = {
            let mut registry = self.registry.lock().await;
            registry.get_mut(&hash).map(|torrent| {
                let piece_count = resolved
                    .data_piece_count()
                    .unwrap_or_else(|_| resolved.piece_count());
                let empty_state = EngineState {
                    pieces_have: swarmotter_core::storage::PieceBitfield::new(piece_count),
                    piece_count,
                    total_length: resolved.total_length,
                    ..EngineState::default()
                };
                apply_resolved_metadata(torrent, &resolved, &empty_state);
            })
        };
        if updated.is_none() {
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, previous.meta.total_length)
                    .await;
            }
            return Err(CoreError::NotFound("torrent".into()));
        }
        if let Err(error) = self.persist_state().await {
            if let Some(torrent) = self.registry.lock().await.get_mut(&hash) {
                *torrent = previous.clone();
            }
            if let Some(admission) = &storage_admission {
                let _ = self
                    .storage_admissions
                    .reserve(hash, admission, previous.meta.total_length)
                    .await;
            }
            return Err(error);
        }
        self.publish_event(torrent_metadata_event(hash));
        Ok(())
    }
}
