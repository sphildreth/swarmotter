// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Debug, Deserialize)]
pub struct AddMagnetBody {
    pub magnet: String,
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub start_behavior: Option<StartBehavior>,
}
#[derive(Debug, Default, Deserialize)]
pub struct AddTorrentQuery {
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub start_behavior: Option<StartBehavior>,
}
/// Add via magnet (JSON body with magnet) or file (multipart). Dispatches based
/// on content-type: application/json -> magnet; multipart -> file.
pub async fn add_torrent_file_or_magnet(
    State(state): State<SharedState>,
    Query(query): Query<AddTorrentQuery>,
    Extension(ConfiguredRequestBodyLimit(configured_limit)): Extension<ConfiguredRequestBodyLimit>,
    request: Request,
) -> Response {
    let is_json = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|content_type| content_type.contains("application/json"));
    if is_json {
        let body = match read_body_bounded(request.into_body(), configured_limit).await {
            Ok(body) => body,
            Err(BoundedBodyReadError::LimitExceeded { observed }) => {
                return payload_too_large_response(configured_limit, observed);
            }
            Err(error) => return bounded_body_failure_response(error),
        };
        match serde_json::from_slice::<AddMagnetBody>(&body) {
            Ok(b) => {
                let options = match add_options(
                    b.download_dir.clone(),
                    b.paused,
                    b.start_behavior,
                    Some(&query),
                ) {
                    Ok(options) => options,
                    Err(e) => return err_response(e),
                };
                return into_response(
                    state
                        .daemon
                        .add_magnet(&b.magnet, options)
                        .await
                        .map(|h| h.to_hex()),
                );
            }
            Err(e) => return err_response(CoreError::InvalidArgument(e.to_string())),
        }
    }
    // Treat raw body as torrent file bytes.
    let options = match add_options(None, None, None, Some(&query)) {
        Ok(options) => options,
        Err(e) => return err_response(e),
    };
    let body = match read_torrent_metadata_body(request.into_body(), configured_limit).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    into_response(
        state
            .daemon
            .add_torrent_file(body, options)
            .await
            .map(|h| h.to_hex()),
    )
}

pub async fn add_magnet(
    State(state): State<SharedState>,
    Query(query): Query<AddTorrentQuery>,
    Json(body): Json<AddMagnetBody>,
) -> Response {
    let options = match add_options(
        body.download_dir.clone(),
        body.paused,
        body.start_behavior,
        Some(&query),
    ) {
        Ok(options) => options,
        Err(e) => return err_response(e),
    };
    into_response(
        state
            .daemon
            .add_magnet(&body.magnet, options)
            .await
            .map(|h| h.to_hex()),
    )
}

pub async fn add_torrent_file(
    State(state): State<SharedState>,
    Query(query): Query<AddTorrentQuery>,
    Extension(ConfiguredRequestBodyLimit(configured_limit)): Extension<ConfiguredRequestBodyLimit>,
    request: Request,
) -> Response {
    let options = match add_options(None, None, None, Some(&query)) {
        Ok(options) => options,
        Err(e) => return err_response(e),
    };
    let body = match read_torrent_metadata_body(request.into_body(), configured_limit).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    into_response(
        state
            .daemon
            .add_torrent_file(body, options)
            .await
            .map(|h| h.to_hex()),
    )
}

pub(super) fn validate_torrent_metadata_size(len: usize) -> Result<()> {
    if len > MAX_TORRENT_METADATA_BYTES {
        return Err(CoreError::MalformedTorrent(format!(
            "torrent metadata size {len} exceeds maximum {MAX_TORRENT_METADATA_BYTES}"
        )));
    }
    Ok(())
}

pub(in crate::handlers) fn decode_torrent_metainfo_base64(input: &str) -> Result<Vec<u8>> {
    match decode_base64_bounded(input, MAX_TORRENT_METADATA_BYTES) {
        Ok(bytes) => Ok(bytes),
        Err(BoundedBase64DecodeError::InvalidEncoding) => Err(CoreError::InvalidArgument(
            "metainfo must be valid base64".into(),
        )),
        Err(BoundedBase64DecodeError::LimitExceeded) => Err(CoreError::MalformedTorrent(format!(
            "decoded torrent metadata exceeds maximum {MAX_TORRENT_METADATA_BYTES}"
        ))),
        Err(BoundedBase64DecodeError::AllocationFailed) => Err(CoreError::Internal(
            "unable to allocate bounded torrent metadata output".into(),
        )),
    }
}

#[derive(Debug)]
pub(super) enum BoundedBodyReadError {
    LimitExceeded { observed: usize },
    Read(String),
    AllocationFailed,
}

pub(super) async fn read_body_bounded(
    body: Body,
    limit: usize,
) -> std::result::Result<Vec<u8>, BoundedBodyReadError> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| BoundedBodyReadError::Read(error.to_string()))?;
        let next_len =
            bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(BoundedBodyReadError::LimitExceeded {
                    observed: usize::MAX,
                })?;
        if next_len > limit {
            return Err(BoundedBodyReadError::LimitExceeded { observed: next_len });
        }
        bytes
            .try_reserve_exact(chunk.len())
            .map_err(|_| BoundedBodyReadError::AllocationFailed)?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(super) async fn read_torrent_metadata_body(
    body: Body,
    configured_limit: usize,
) -> std::result::Result<Vec<u8>, Response> {
    let effective_limit = configured_limit.min(MAX_TORRENT_METADATA_BYTES);
    match read_body_bounded(body, effective_limit).await {
        Ok(bytes) => Ok(bytes),
        Err(BoundedBodyReadError::LimitExceeded { observed })
            if configured_limit < MAX_TORRENT_METADATA_BYTES =>
        {
            Err(payload_too_large_response(configured_limit, observed))
        }
        Err(BoundedBodyReadError::LimitExceeded { observed }) => {
            Err(err_response(CoreError::MalformedTorrent(format!(
                "torrent metadata size {observed} exceeds maximum {MAX_TORRENT_METADATA_BYTES}"
            ))))
        }
        Err(error) => Err(bounded_body_failure_response(error)),
    }
}

pub(super) fn payload_too_large_response(limit: usize, observed: usize) -> Response {
    let body = crate::envelope::error_to_json(
        "payload_too_large",
        &format!("request body size {observed} exceeds configured maximum {limit}"),
    );
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

pub(super) fn bounded_body_failure_response(error: BoundedBodyReadError) -> Response {
    match error {
        BoundedBodyReadError::Read(message) => err_response(CoreError::InvalidArgument(format!(
            "request body read failed: {message}"
        ))),
        BoundedBodyReadError::AllocationFailed => err_response(CoreError::Internal(
            "unable to allocate bounded request body".into(),
        )),
        BoundedBodyReadError::LimitExceeded { observed } => err_response(
            CoreError::InvalidArgument(format!("request body exceeds bounded size at {observed}")),
        ),
    }
}

pub(super) fn add_options(
    download_dir: Option<String>,
    body_paused: Option<bool>,
    body_start_behavior: Option<StartBehavior>,
    query: Option<&AddTorrentQuery>,
) -> Result<AddTorrentOptions> {
    let paused = merge_paused(body_paused, query.and_then(|q| q.paused), "paused")?;
    let start_behavior =
        merge_start_behavior(body_start_behavior, query.and_then(|q| q.start_behavior))?;
    Ok(AddTorrentOptions::new(
        download_dir,
        resolve_start_paused(paused, start_behavior)?,
    ))
}

pub(super) fn merge_paused(
    body: Option<bool>,
    query: Option<bool>,
    field: &str,
) -> Result<Option<bool>> {
    match (body, query) {
        (Some(a), Some(b)) if a != b => Err(CoreError::InvalidArgument(format!(
            "body and query {field} values conflict"
        ))),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        _ => Ok(None),
    }
}

pub(super) fn merge_start_behavior(
    body: Option<StartBehavior>,
    query: Option<StartBehavior>,
) -> Result<Option<StartBehavior>> {
    match (body, query) {
        (Some(a), Some(b)) if !start_behavior_eq(a, b) => Err(CoreError::InvalidArgument(
            "body and query start_behavior values conflict".into(),
        )),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        _ => Ok(None),
    }
}

pub(super) fn start_behavior_eq(a: StartBehavior, b: StartBehavior) -> bool {
    matches!(
        (a, b),
        (StartBehavior::Start, StartBehavior::Start)
            | (StartBehavior::Paused, StartBehavior::Paused)
    )
}

pub(super) fn resolve_start_paused(
    paused: Option<bool>,
    start_behavior: Option<StartBehavior>,
) -> Result<bool> {
    if let (Some(paused), Some(start_behavior)) = (paused, start_behavior) {
        let behavior_paused = matches!(start_behavior, StartBehavior::Paused);
        if paused != behavior_paused {
            return Err(CoreError::InvalidArgument(
                "paused and start_behavior values conflict".into(),
            ));
        }
    }
    Ok(paused.unwrap_or(matches!(start_behavior, Some(StartBehavior::Paused))))
}
