// SPDX-License-Identifier: Apache-2.0

use super::*;

impl DaemonRuntime {
    /// Watch-folder scan loop: periodically scans configured folders and imports
    /// newly-stabilized `.torrent` files.
    pub async fn watch_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if let Err(error) = self.scan_watch_folders().await {
                tracing::warn!(
                    error = %error,
                    error_code = %error.code(),
                    "automatic watch-folder scan incomplete; observations retained for retry"
                );
            }
        }
    }

    pub(super) async fn scan_watch_folders(&self) -> Result<()> {
        let _scan_guard = self.watch_scan_lock.lock().await;
        let cfg = self.config.read().await.clone();
        let mut configured_roots = HashSet::new();
        for folder in &cfg.watch {
            configured_roots.insert(watch::lexical_absolute(Path::new(&folder.path))?);
        }
        self.watch_observations
            .lock()
            .await
            .retain(|key, _| configured_roots.contains(&key.root));

        let mut successful_seen: HashMap<PathBuf, HashSet<watch::ObservationKey>> = HashMap::new();
        let mut incomplete_roots = HashSet::new();
        let mut first_error = None;
        for folder in &cfg.watch {
            let scan_folder = folder.clone();
            let scan = tokio::task::spawn_blocking(move || watch::scan_watch_folder(&scan_folder))
                .await
                .map_err(|error| CoreError::Storage(format!("watch scan task failed: {error}")))?;
            let scan = match scan {
                Ok(scan) => scan,
                Err(error) => {
                    if let Ok(root) = watch::lexical_absolute(Path::new(&folder.path)) {
                        incomplete_roots.insert(root);
                    }
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };
            let seen = successful_seen.entry(scan.root.clone()).or_default();
            for file in scan.files {
                seen.insert(file.key.clone());
                if self.observe_watch_file(&file).await {
                    self.process_watch_file(&file, folder).await;
                }
            }
        }

        let mut observations = self.watch_observations.lock().await;
        observations.retain(|key, _| {
            if incomplete_roots.contains(&key.root) {
                return true;
            }
            successful_seen
                .get(&key.root)
                .is_none_or(|seen| seen.contains(key))
        });
        drop(observations);
        first_error.map_or(Ok(()), Err)
    }

    /// Advance one observation. The first sighting and every changed
    /// fingerprint start at one stable scan; the next identical scan is
    /// eligible unless this exact fingerprint already reached a terminal
    /// processed outcome.
    pub(super) async fn observe_watch_file(&self, file: &watch::ScannedTorrentFile) -> bool {
        let mut observations = self.watch_observations.lock().await;
        match observations.get_mut(&file.key) {
            Some(observation) if observation.fingerprint == file.fingerprint => {
                observation.stable_scans = observation.stable_scans.saturating_add(1);
                observation.stable_scans >= 2
                    && observation.processed_fingerprint != Some(file.fingerprint)
            }
            Some(observation) => {
                observation.fingerprint = file.fingerprint;
                observation.stable_scans = 1;
                observation.processed_fingerprint = None;
                false
            }
            None => {
                observations.insert(
                    file.key.clone(),
                    WatchObservation {
                        fingerprint: file.fingerprint,
                        stable_scans: 1,
                        processed_fingerprint: None,
                    },
                );
                false
            }
        }
    }

    pub(super) async fn process_watch_file(
        &self,
        file: &watch::ScannedTorrentFile,
        folder: &swarmotter_core::config::WatchFolderConfig,
    ) {
        let path = file.path();
        let bytes = match self.read_stable_watch_file(file).await {
            Ok(WatchReadOutcome::Stable(bytes)) => bytes,
            Ok(WatchReadOutcome::Changed(fingerprint)) => {
                if let Some(observation) = self.watch_observations.lock().await.get_mut(&file.key) {
                    observation.fingerprint = fingerprint;
                    observation.stable_scans = 1;
                    observation.processed_fingerprint = None;
                }
                return;
            }
            Err(error) => {
                self.finish_watch_attempt(file, folder, None, Err(error))
                    .await;
                return;
            }
        };

        let parsed = match meta::parse_torrent(&bytes) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.finish_watch_attempt(file, folder, None, Err(error))
                    .await;
                return;
            }
        };
        let hash = parsed.info_hash;
        let mut torrent = Torrent::new(parsed, now());
        watch::apply_folder_defaults(&mut torrent, folder);
        let labels = torrent.labels.clone();
        // Watch-folder start behavior remains an explicit creation choice;
        // the optional profile supplies storage snapshot and all live policy
        // fields without being copied into per-torrent overrides.
        let mutation = {
            // Profile resolution and durable registration must share the
            // profile/config transaction. A concurrent profile replacement
            // cannot delete a profile after this watch import validates it.
            let _config_transaction = self.config_write_lock.lock().await;
            match self
                .apply_add_profile(
                    &mut torrent,
                    None,
                    labels,
                    true,
                    matches!(
                        folder.start_behavior,
                        swarmotter_core::config::StartBehavior::Paused
                    ),
                )
                .await
            {
                Ok(paused) => {
                    self.add_torrent_mutation(torrent, paused, "watch_import_added")
                        .await
                }
                Err(error) => Err(error),
            }
        };
        self.finish_watch_attempt(file, folder, Some(hash), mutation)
            .await;
        tracing::debug!(path = %path.display(), "watch torrent attempt finished");
    }

    pub(super) async fn read_stable_watch_file(
        &self,
        file: &watch::ScannedTorrentFile,
    ) -> Result<WatchReadOutcome> {
        let path = file.path();
        let expected = file.fingerprint;
        let read_path = path.clone();
        let read = tokio::task::spawn_blocking(move || {
            watch::read_bounded_watch_file(&read_path, expected)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("watch read task failed: {error}")))??;
        let bytes = match read {
            watch::BoundedWatchRead::Stable(bytes) => bytes,
            watch::BoundedWatchRead::Changed(fingerprint) => {
                return Ok(WatchReadOutcome::Changed(fingerprint));
            }
        };
        self.wait_at_watch_after_read_test_pause().await;
        let recheck_path = path;
        let after = tokio::task::spawn_blocking(move || -> Result<watch::FileFingerprint> {
            let metadata = fs::symlink_metadata(&recheck_path).map_err(CoreError::from)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(CoreError::Storage(format!(
                    "watch source is not a regular file after read: {}",
                    recheck_path.display()
                )));
            }
            watch::FileFingerprint::from_metadata(&metadata)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("watch recheck task failed: {error}")))??;
        if after != expected {
            Ok(WatchReadOutcome::Changed(after))
        } else {
            Ok(WatchReadOutcome::Stable(bytes))
        }
    }

    pub(super) async fn finish_watch_attempt(
        &self,
        file: &watch::ScannedTorrentFile,
        folder: &swarmotter_core::config::WatchFolderConfig,
        parsed_hash: Option<InfoHash>,
        result: Result<TorrentAddMutationOutcome>,
    ) {
        let path = file.path();
        let (outcome, info_hash, error) = match result {
            Ok(TorrentAddMutationOutcome::Inserted { hash, .. }) => {
                (watch::ImportOutcome::Imported, Some(hash), None)
            }
            Ok(TorrentAddMutationOutcome::Duplicate { hash }) => {
                (watch::ImportOutcome::Duplicate, Some(hash), None)
            }
            Err(error) if is_permanent_watch_error(&error) => (
                watch::ImportOutcome::PermanentFailure,
                parsed_hash,
                Some(error.to_string()),
            ),
            Err(error) => (
                watch::ImportOutcome::TransientFailure,
                parsed_hash,
                Some(error.to_string()),
            ),
        };
        let processed = outcome != watch::ImportOutcome::TransientFailure;
        let action = match outcome {
            watch::ImportOutcome::Imported | watch::ImportOutcome::Duplicate => {
                Some(watch::post_import_action(folder, &path))
            }
            watch::ImportOutcome::PermanentFailure => {
                Some(watch::post_failure_action(folder, &path))
            }
            watch::ImportOutcome::TransientFailure => None,
        };
        let post_action_error = if let Some(action) = action {
            let action_path = path.clone();
            tokio::task::spawn_blocking(move || {
                watch::execute_post_import_action(&action_path, &action)
            })
            .await
            .map_err(|join| CoreError::Storage(format!("watch post-action task failed: {join}")))
            .and_then(|result| result)
            .err()
            .map(|error| error.to_string())
        } else {
            None
        };

        if processed {
            if let Some(observation) = self.watch_observations.lock().await.get_mut(&file.key) {
                if observation.fingerprint == file.fingerprint {
                    observation.processed_fingerprint = Some(file.fingerprint);
                }
            }
        }
        let import = watch::ImportResult {
            path: path.display().to_string(),
            success: matches!(
                outcome,
                watch::ImportOutcome::Imported | watch::ImportOutcome::Duplicate
            ),
            info_hash_hex: info_hash.map(|hash| hash.to_hex()),
            error,
            duplicate: outcome == watch::ImportOutcome::Duplicate,
            post_action_error,
            outcome,
        };
        self.record_watch_import(import.clone()).await;
        self.publish_watch_event(&import);
    }

    pub(super) async fn record_watch_import(&self, result: watch::ImportResult) {
        let mut history = self.watch_imports.lock().await;
        while history.len() >= watch::MAX_IMPORT_HISTORY {
            history.pop_front();
        }
        history.push_back(result);
    }

    pub(super) fn publish_watch_event(&self, result: &watch::ImportResult) {
        let kind = if result.success {
            "watch_folder_imported"
        } else {
            "watch_folder_failed"
        };
        let payload = json!({
            "path": result.path,
            "outcome": result.outcome.as_str(),
            "success": result.success,
            "duplicate": result.duplicate,
            "info_hash": result.info_hash_hex,
            "error": result.error,
            "post_action_error": result.post_action_error,
        });
        let mut event = Event::new(kind, payload);
        if let Some(hash) = &result.info_hash_hex {
            event = event.with_info_hash(hash.clone());
        }
        self.publish_event(event);
    }
}
