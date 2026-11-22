// SPDX-License-Identifier: Apache-2.0

use super::*;

impl TorrentEngine {
    /// Download a bounded batch of missing pieces from BEP 19 webseeds. Webseed
    /// traffic is HTTP byte-range traffic and must go through the same
    /// contained binder path as tracker HTTP.
    pub(super) async fn run_webseed_round(
        &self,
        storage: &StorageIo,
        have: &mut PieceBitfield,
        mirror_cursor: &mut usize,
    ) -> bool {
        if self.meta.webseeds.is_empty() || !self.binder.traffic_allowed() {
            return false;
        }
        let mut webseeds = webseed_http_urls(&self.meta);
        if webseeds.is_empty() {
            return false;
        }
        {
            let mut state = self.state.lock().await;
            for url in &webseeds {
                state
                    .webseeds
                    .entry(url.clone())
                    .or_insert_with(|| WebseedSnapshot {
                        status: TrackerStatus::NotContacted,
                        ..Default::default()
                    });
            }
        }

        let piece_count = self.meta.piece_count();
        let mut missing: Vec<usize> = (0..piece_count)
            .filter(|&piece| self.piece_selection.includes(piece) && !have.has(piece))
            .collect();
        missing.sort_by_key(|piece| std::cmp::Reverse(self.piece_selection.priority(*piece)));
        missing.truncate(WEBSEED_BATCH_PIECES);
        if missing.is_empty() {
            return false;
        }

        // Keep each piece's per-round retry budget while allowing its next
        // round to continue with mirrors outside the previous attempt window.
        if webseeds.len() > WEBSEED_MAX_MIRROR_ATTEMPTS {
            webseeds.rotate_left(*mirror_cursor);
            let stride = webseed_mirror_stride(webseeds.len());
            *mirror_cursor =
                (*mirror_cursor + WEBSEED_MAX_MIRROR_ATTEMPTS * stride) % webseeds.len();
        }
        let shared_have = Arc::new(Mutex::new(have.clone()));
        let storage = Arc::new(storage.clone());
        let webseeds = Arc::new(webseeds);
        let mut tasks = tokio::task::JoinSet::new();
        let mut next_piece = 0usize;
        while next_piece < missing.len() && tasks.len() < WEBSEED_MAX_CONCURRENT_REQUESTS {
            spawn_webseed_piece_task(
                &mut tasks,
                missing[next_piece],
                self.meta.clone(),
                self.binder.clone(),
                storage.clone(),
                shared_have.clone(),
                self.state.clone(),
                self.limiter.clone(),
                webseeds.clone(),
            );
            next_piece += 1;
        }

        let mut progressed = false;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((piece_index, Ok(piece_progressed))) => {
                    progressed |= piece_progressed;
                    tracing::debug!(
                        piece = piece_index,
                        progressed = piece_progressed,
                        "webseed piece task completed"
                    );
                }
                Ok((piece_index, Err(e))) => {
                    tracing::debug!(piece = piece_index, error = %e, "webseed piece task failed");
                }
                Err(e) => {
                    tracing::debug!(error = %e, "webseed piece task join failed");
                }
            }

            let complete = {
                let have = shared_have.lock().await;
                self.piece_selection.complete(&have)
            };
            if complete {
                tasks.abort_all();
                break;
            }

            if next_piece < missing.len() {
                spawn_webseed_piece_task(
                    &mut tasks,
                    missing[next_piece],
                    self.meta.clone(),
                    self.binder.clone(),
                    storage.clone(),
                    shared_have.clone(),
                    self.state.clone(),
                    self.limiter.clone(),
                    webseeds.clone(),
                );
                next_piece += 1;
            }
        }

        if progressed {
            let merged = shared_have.lock().await.clone();
            let piece_count = self.meta.piece_count();
            let newly_verified = merged
                .count(piece_count)
                .saturating_sub(have.count(piece_count));
            for _ in 0..newly_verified {
                self.mark_resume_piece_verified();
            }
            *have = merged.clone();
            self.update_progress(&merged).await;
            if let Err(e) = self
                .maybe_persist_resume(storage.as_ref(), &merged, false)
                .await
            {
                tracing::warn!(error = %e, "webseed resume persist failed");
            }
        }
        progressed
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_webseed_piece_task(
    tasks: &mut tokio::task::JoinSet<(usize, Result<bool>)>,
    piece_index: usize,
    meta: Arc<TorrentMeta>,
    binder: Arc<dyn NetworkBinder>,
    storage: Arc<StorageIo>,
    shared_have: Arc<Mutex<PieceBitfield>>,
    state: Arc<Mutex<EngineState>>,
    limiter: ShapedLimiter,
    webseeds: Arc<Vec<String>>,
) {
    tasks.spawn(async move {
        let result = download_webseed_piece(
            binder,
            meta,
            piece_index,
            storage,
            shared_have,
            state,
            limiter,
            webseeds,
        )
        .await;
        (piece_index, result)
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn download_webseed_piece(
    binder: Arc<dyn NetworkBinder>,
    meta: Arc<TorrentMeta>,
    piece_index: usize,
    storage: Arc<StorageIo>,
    shared_have: Arc<Mutex<PieceBitfield>>,
    state: Arc<Mutex<EngineState>>,
    limiter: ShapedLimiter,
    webseeds: Arc<Vec<String>>,
) -> Result<bool> {
    {
        let have = shared_have.lock().await;
        if have.has(piece_index) {
            return Ok(false);
        }
    }

    let slices = storage.piece_file_slices(piece_index)?;
    let data = fetch_webseed_piece(binder, &meta, piece_index, &slices, &webseeds, &state).await?;
    limiter
        .acquire(RateDirection::Download, data.len() as u64)
        .await;
    storage.write_piece(piece_index, &data).await?;
    record_webseed_block(&state, data.len() as u64).await;

    let have_snapshot = {
        let mut have = shared_have.lock().await;
        if have.has(piece_index) {
            return Ok(false);
        }
        have.set(piece_index);
        have.clone()
    };
    update_progress_state(&state, &meta, &have_snapshot).await;
    Ok(true)
}

pub(super) async fn fetch_webseed_piece(
    binder: Arc<dyn NetworkBinder>,
    meta: &TorrentMeta,
    piece_index: usize,
    slices: &[FileSlice],
    webseeds: &[String],
    state: &Arc<Mutex<EngineState>>,
) -> Result<Vec<u8>> {
    if webseeds.is_empty() {
        return Err(CoreError::Internal("torrent has no usable webseeds".into()));
    }

    let attempts = webseeds.len().min(WEBSEED_MAX_MIRROR_ATTEMPTS);
    let mut last_error = None;
    for attempt in 0..attempts {
        let stride = webseed_mirror_stride(webseeds.len());
        let index = (piece_index
            .wrapping_mul(stride)
            .wrapping_add(attempt.wrapping_mul(stride)))
            % webseeds.len();
        let base = &webseeds[index];
        let started = Instant::now();
        {
            let mut state = state.lock().await;
            if let Some(snapshot) = state.webseeds.get_mut(base) {
                snapshot.status = TrackerStatus::Updating;
                snapshot.attempts = snapshot.attempts.saturating_add(1);
                snapshot.last_attempt = Some(now_secs());
            }
        }
        match fetch_piece_from_webseed(binder.as_ref(), meta, piece_index, slices, base).await {
            Ok(piece) => {
                let mut state = state.lock().await;
                if let Some(snapshot) = state.webseeds.get_mut(base) {
                    snapshot.status = TrackerStatus::Ok;
                    snapshot.bytes_verified =
                        snapshot.bytes_verified.saturating_add(piece.len() as u64);
                    snapshot.retries = 0;
                    snapshot.last_success = Some(now_secs());
                    snapshot.last_error = None;
                    snapshot.latency_ms =
                        Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
                }
                return Ok(piece);
            }
            Err(e) => {
                let mut state = state.lock().await;
                if let Some(snapshot) = state.webseeds.get_mut(base) {
                    snapshot.status = TrackerStatus::Error;
                    snapshot.retries = snapshot.retries.saturating_add(1);
                    snapshot.last_error = Some(e.to_string());
                    snapshot.recent_errors.push(e.to_string());
                    snapshot.recent_errors.truncate(8);
                    snapshot.latency_ms =
                        Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
                }
                tracing::debug!(piece = piece_index, webseed = %base, error = %e, "webseed mirror failed");
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| CoreError::Internal("all webseed mirrors failed".into())))
}

pub(super) async fn fetch_piece_from_webseed(
    binder: &dyn NetworkBinder,
    meta: &TorrentMeta,
    piece_index: usize,
    slices: &[FileSlice],
    base_url: &str,
) -> Result<Vec<u8>> {
    let piece_len = usize::try_from(piece_length_for_meta(meta, piece_index))
        .map_err(|_| CoreError::Internal(format!("piece {piece_index} length exceeds usize")))?;
    let mut piece = Vec::with_capacity(piece_len);
    for slice in slices {
        let file_url = webseed_file_url(base_url, meta, slice.file_index)?;
        let end_exclusive = slice
            .offset_in_file
            .checked_add(slice.length)
            .ok_or_else(|| {
                CoreError::Internal(format!("webseed range overflow for piece {piece_index}"))
            })?;
        let response = timeout(
            WEBSEED_REQUEST_TIMEOUT,
            binder.http_get_range(&file_url, slice.offset_in_file, end_exclusive),
        )
        .await
        .map_err(|_| {
            CoreError::Internal(format!("webseed range request timed out: {file_url}"))
        })??;
        if response.status != 206 {
            return Err(CoreError::Internal(format!(
                "webseed returned HTTP {} instead of 206 for {file_url}",
                response.status
            )));
        }
        let expected_len = usize::try_from(slice.length).map_err(|_| {
            CoreError::Internal(format!("webseed slice length exceeds usize for {file_url}"))
        })?;
        if response.body.len() != expected_len {
            return Err(CoreError::Internal(format!(
                "webseed returned {} bytes, expected {expected_len} for {file_url}",
                response.body.len()
            )));
        }

        piece.extend_from_slice(&response.body);
    }
    if piece.len() != piece_len || !verify_piece(meta, piece_index, &piece) {
        return Err(CoreError::Internal(format!(
            "webseed piece {piece_index} hash mismatch"
        )));
    }
    Ok(piece)
}

pub(super) fn webseed_http_urls(meta: &TorrentMeta) -> Vec<String> {
    meta.webseeds
        .iter()
        .filter_map(|url| {
            let parsed = url::Url::parse(url).ok()?;
            matches!(parsed.scheme(), "http" | "https").then(|| url.clone())
        })
        .collect()
}

pub(super) fn webseed_mirror_stride(len: usize) -> usize {
    for candidate in [31usize, 17, 13, 7, 5, 3] {
        if len > candidate && gcd(len, candidate) == 1 {
            return candidate;
        }
    }
    1
}

pub(super) fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

pub(super) fn webseed_file_url(
    base_url: &str,
    meta: &TorrentMeta,
    file_index: usize,
) -> Result<String> {
    let file = meta
        .files
        .get(file_index)
        .ok_or_else(|| CoreError::Internal(format!("file index {file_index} out of range")))?;
    let parsed = url::Url::parse(base_url)
        .map_err(|e| CoreError::InvalidArgument(format!("bad webseed url: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(CoreError::InvalidArgument(format!(
            "unsupported webseed scheme: {}",
            parsed.scheme()
        )));
    }
    if !meta.is_multi_file && !base_url.ends_with('/') {
        return Ok(parsed.to_string());
    }

    let mut url = parsed;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| CoreError::InvalidArgument("webseed URL cannot be a base".into()))?;
        for segment in &file.path {
            segments.push(segment);
        }
    }
    Ok(url.to_string())
}

pub(super) fn piece_length_for_meta(meta: &TorrentMeta, piece_index: usize) -> u64 {
    if piece_index + 1 == meta.piece_count() {
        meta.last_piece_length()
    } else {
        meta.piece_length
    }
}
