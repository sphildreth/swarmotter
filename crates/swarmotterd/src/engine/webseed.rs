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
    ) -> bool {
        if self.meta.webseeds.is_empty() || !self.binder.traffic_allowed() {
            return false;
        }
        let webseeds = webseed_http_urls(&self.meta);
        if webseeds.is_empty() {
            return false;
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
            *have = merged.clone();
            self.update_progress(&merged).await;
            if let Err(e) = self.persist_resume(storage.as_ref(), &merged).await {
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
    meta: TorrentMeta,
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
    meta: TorrentMeta,
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

    let data = fetch_webseed_piece(binder, &meta, piece_index, &webseeds).await?;
    if !verify_piece(&meta, piece_index, &data) {
        return Err(CoreError::Internal(format!(
            "webseed piece {piece_index} hash mismatch"
        )));
    }

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
    webseeds: &[String],
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
        match fetch_piece_from_webseed(binder.as_ref(), meta, piece_index, base).await {
            Ok(piece) => return Ok(piece),
            Err(e) => {
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
    base_url: &str,
) -> Result<Vec<u8>> {
    let (piece_start, _) = meta
        .piece_byte_range(piece_index as u64)
        .ok_or_else(|| CoreError::Internal(format!("piece {piece_index} out of range")))?;
    let piece_len = usize::try_from(piece_length_for_meta(meta, piece_index))
        .map_err(|_| CoreError::Internal(format!("piece {piece_index} length exceeds usize")))?;
    let mut piece = vec![0u8; piece_len];

    for slice in piece_file_ranges(meta, piece_index)? {
        if slice.length == 0 {
            continue;
        }
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

        let file_start = file_absolute_start(meta, slice.file_index)?;
        let absolute_start = file_start
            .checked_add(slice.offset_in_file)
            .ok_or_else(|| CoreError::Internal("webseed absolute offset overflow".into()))?;
        let piece_offset = absolute_start
            .checked_sub(piece_start)
            .ok_or_else(|| CoreError::Internal("webseed slice starts before piece".into()))?;
        let piece_offset = usize::try_from(piece_offset)
            .map_err(|_| CoreError::Internal("webseed piece offset exceeds usize".into()))?;
        let end = piece_offset
            .checked_add(expected_len)
            .ok_or_else(|| CoreError::Internal("webseed piece copy range overflowed".into()))?;
        let dest = piece.get_mut(piece_offset..end).ok_or_else(|| {
            CoreError::Internal(format!("webseed slice exceeds piece {piece_index} bounds"))
        })?;
        dest.copy_from_slice(&response.body);
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

pub(super) fn file_absolute_start(meta: &TorrentMeta, file_index: usize) -> Result<u64> {
    if file_index >= meta.files.len() {
        return Err(CoreError::Internal(format!(
            "file index {file_index} out of range"
        )));
    }
    meta.files
        .iter()
        .take(file_index)
        .try_fold(0u64, |acc, file| {
            acc.checked_add(file.length)
                .ok_or_else(|| CoreError::Internal("torrent file offset overflow".into()))
        })
}

pub(super) fn piece_length_for_meta(meta: &TorrentMeta, piece_index: usize) -> u64 {
    if piece_index + 1 == meta.piece_count() {
        meta.last_piece_length()
    } else {
        meta.piece_length
    }
}
