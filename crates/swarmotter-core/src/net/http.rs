// SPDX-License-Identifier: Apache-2.0

//! Contained HTTP/1.1 client for torrent data-plane requests.
//!
//! Hyper is used only as a codec over a TCP stream obtained from
//! [`NetworkBinder`]. This module has no connector, resolver, pool, cookie jar,
//! or authorization support.

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::header::{CONNECTION, CONTENT_RANGE, CONTENT_TYPE, HOST, LOCATION, RANGE, USER_AGENT};
use hyper::{Method, Request, StatusCode, Version};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};

use super::NetworkBinder;
use crate::error::{CoreError, Result};

/// Maximum decoded tracker announce/scrape body size.
pub const MAX_TRACKER_HTTP_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Maximum decoded body accepted from a local UPnP IGD control action.
pub const MAX_UPNP_SOAP_BODY_BYTES: usize = 256 * 1024;

const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REDIRECTS: usize = 5;
const MAX_HTTP1_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP1_HEADERS: usize = 128;

/// A final contained HTTP response after redirect handling and body decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub final_url: String,
    pub content_range: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum RequestPolicy {
    Tracker,
    WebseedRange { start: u64, end_exclusive: u64 },
    UpnpSoap,
}

impl RequestPolicy {
    fn body_limit(self) -> Result<usize> {
        match self {
            Self::Tracker => Ok(MAX_TRACKER_HTTP_BODY_BYTES),
            Self::WebseedRange {
                start,
                end_exclusive,
            } => range_length(start, end_exclusive),
            Self::UpnpSoap => Ok(MAX_UPNP_SOAP_BODY_BYTES),
        }
    }

    fn range_header(self) -> Option<String> {
        match self {
            Self::Tracker => None,
            Self::WebseedRange {
                start,
                end_exclusive,
            } => Some(format!("bytes={start}-{}", end_exclusive - 1)),
            Self::UpnpSoap => None,
        }
    }

    fn accepts(self, status: StatusCode) -> bool {
        match self {
            Self::Tracker => status.is_success(),
            Self::WebseedRange { .. } => status == StatusCode::PARTIAL_CONTENT,
            Self::UpnpSoap => status.is_success(),
        }
    }
}

struct RawResponse {
    status: StatusCode,
    body: Vec<u8>,
    location: Option<String>,
    content_range: Option<String>,
}

/// One-shot contained HTTP client. Every hop resolves and connects through the
/// supplied binder; no connection survives a request.
pub struct ContainedHttpClient<'a, B: NetworkBinder + ?Sized> {
    binder: &'a B,
    tls_config: Arc<rustls::ClientConfig>,
    request_timeout: Duration,
}

impl<'a, B: NetworkBinder + ?Sized> ContainedHttpClient<'a, B> {
    pub fn new(binder: &'a B) -> Self {
        Self {
            binder,
            tls_config: default_tls_config(),
            request_timeout: HTTP_REQUEST_TIMEOUT,
        }
    }

    /// Construct with an explicit trust store for generated local TLS
    /// fixtures. Production callers always use [`Self::new`].
    #[cfg(any(test, feature = "test-binder"))]
    pub fn with_tls_config(binder: &'a B, tls_config: Arc<rustls::ClientConfig>) -> Self {
        Self {
            binder,
            tls_config,
            request_timeout: HTTP_REQUEST_TIMEOUT,
        }
    }

    /// Construct with a shorter logical timeout for deterministic cancellation
    /// tests. Production callers cannot override the 30-second policy.
    #[cfg(any(test, feature = "test-binder"))]
    pub fn with_timeout(binder: &'a B, request_timeout: Duration) -> Self {
        Self {
            binder,
            tls_config: default_tls_config(),
            request_timeout,
        }
    }

    /// GET a tracker announce or scrape response. Redirects are followed under
    /// the contained policy; only the final 2xx response is accepted.
    pub async fn get_tracker(&self, url: &str) -> Result<HttpResponse> {
        let response = self.request(url, RequestPolicy::Tracker).await?;
        if !(200..300).contains(&response.status) {
            return Err(CoreError::HttpStatus(format!(
                "tracker {} returned HTTP {}",
                response.final_url, response.status
            )));
        }
        Ok(response)
    }

    /// GET one exact inclusive webseed range. The caller supplies an exclusive
    /// end to match storage range conventions.
    pub async fn get_webseed_range(
        &self,
        url: &str,
        start: u64,
        end_exclusive: u64,
    ) -> Result<HttpResponse> {
        let expected_len = range_length(start, end_exclusive)?;
        let response = self
            .request(
                url,
                RequestPolicy::WebseedRange {
                    start,
                    end_exclusive,
                },
            )
            .await?;
        if response.status != StatusCode::PARTIAL_CONTENT.as_u16() {
            return Err(CoreError::HttpStatus(format!(
                "webseed {} returned HTTP {} instead of 206",
                response.final_url, response.status
            )));
        }
        let content_range = response.content_range.as_deref().ok_or_else(|| {
            CoreError::HttpProtocol(format!(
                "webseed {} omitted Content-Range",
                response.final_url
            ))
        })?;
        let (actual_start, actual_end) = parse_content_range(content_range)?;
        let expected_end = end_exclusive - 1;
        if (actual_start, actual_end) != (start, expected_end) {
            return Err(CoreError::HttpProtocol(format!(
                "webseed {} returned Content-Range bytes {actual_start}-{actual_end}, expected bytes {start}-{expected_end}",
                response.final_url
            )));
        }
        if response.body.len() != expected_len {
            return Err(CoreError::HttpProtocol(format!(
                "webseed {} returned {} decoded bytes, expected {expected_len}",
                response.final_url,
                response.body.len()
            )));
        }
        Ok(response)
    }

    /// Fetch one UPnP device description through the contained binder without
    /// following redirects. Discovery responses are unauthenticated local
    /// multicast input, so this narrow helper deliberately rejects every
    /// non-2xx status rather than reusing tracker redirect behavior.
    pub async fn get_upnp_description(&self, url: &str) -> Result<HttpResponse> {
        let parsed = parse_http_url(url)?;
        if parsed.scheme() != "http" {
            return Err(CoreError::InvalidArgument(
                "UPnP device description URLs must use http".into(),
            ));
        }
        let response = tokio::time::timeout(
            self.request_timeout,
            self.request_one(&parsed, RequestPolicy::UpnpSoap, MAX_UPNP_SOAP_BODY_BYTES),
        )
        .await??;
        if !response.status.is_success() {
            return Err(CoreError::HttpStatus(format!(
                "UPnP device description {} returned HTTP {}",
                parsed, response.status
            )));
        }
        Ok(HttpResponse {
            status: response.status.as_u16(),
            body: response.body,
            final_url: parsed.to_string(),
            content_range: None,
        })
    }

    /// POST one bounded UPnP Internet Gateway Device SOAP action. The URL is
    /// resolved and connected through the supplied binder, exactly like a
    /// tracker request. UPnP control URLs do not follow redirects: accepting
    /// a redirect from a multicast-advertised local device would weaken the
    /// narrow, auditable router-control surface.
    pub async fn post_upnp_soap(
        &self,
        url: &str,
        soap_action: &str,
        body: &[u8],
    ) -> Result<HttpResponse> {
        if body.len() > MAX_UPNP_SOAP_BODY_BYTES {
            return Err(CoreError::InvalidArgument(format!(
                "UPnP SOAP request body exceeds {MAX_UPNP_SOAP_BODY_BYTES} bytes"
            )));
        }
        if soap_action.trim().is_empty() || soap_action.contains('\r') || soap_action.contains('\n')
        {
            return Err(CoreError::InvalidArgument(
                "UPnP SOAP action must be a non-empty single header value".into(),
            ));
        }
        let parsed = parse_http_url(url)?;
        if parsed.scheme() != "http" {
            return Err(CoreError::InvalidArgument(
                "UPnP SOAP control URLs must use http".into(),
            ));
        }
        let request = build_upnp_soap_request(&parsed, soap_action, body)?;
        let response = tokio::time::timeout(
            self.request_timeout,
            self.request_one_with_request(
                &parsed,
                RequestPolicy::UpnpSoap,
                MAX_UPNP_SOAP_BODY_BYTES,
                request,
            ),
        )
        .await??;
        if !response.status.is_success() {
            return Err(CoreError::HttpStatus(format!(
                "UPnP SOAP {} returned HTTP {}",
                parsed, response.status
            )));
        }
        Ok(HttpResponse {
            status: response.status.as_u16(),
            body: response.body,
            final_url: parsed.to_string(),
            content_range: None,
        })
    }

    async fn request(&self, url: &str, policy: RequestPolicy) -> Result<HttpResponse> {
        tokio::time::timeout(
            self.request_timeout,
            self.request_with_redirects(url, policy),
        )
        .await?
    }

    async fn request_with_redirects(
        &self,
        url: &str,
        policy: RequestPolicy,
    ) -> Result<HttpResponse> {
        let mut current = parse_http_url(url)?;
        let mut visited = HashSet::new();
        let body_limit = policy.body_limit()?;

        for followed in 0..=MAX_REDIRECTS {
            let key = current.as_str().to_string();
            if !visited.insert(key.clone()) {
                return Err(CoreError::HttpProtocol(format!(
                    "HTTP redirect loop detected before requesting {key}"
                )));
            }

            let response = self.request_one(&current, policy, body_limit).await?;
            if is_followed_redirect(response.status) {
                let location = response
                    .location
                    .as_deref()
                    .filter(|location| !location.trim().is_empty())
                    .ok_or_else(|| {
                        CoreError::HttpProtocol(format!(
                            "HTTP {} from {} omitted a single valid Location header",
                            response.status, current
                        ))
                    })?;
                if followed == MAX_REDIRECTS {
                    return Err(CoreError::HttpProtocol(format!(
                        "HTTP redirect limit of {MAX_REDIRECTS} exceeded at {current}"
                    )));
                }
                let next = resolve_redirect(&current, location)?;
                if current.scheme() == "https" && next.scheme() == "http" {
                    return Err(CoreError::HttpProtocol(format!(
                        "HTTPS downgrade redirect rejected: {current} -> {next}"
                    )));
                }
                current = next;
                continue;
            }

            return Ok(HttpResponse {
                status: response.status.as_u16(),
                body: response.body,
                final_url: current.to_string(),
                content_range: response.content_range,
            });
        }

        Err(CoreError::HttpProtocol(
            "HTTP redirect state exhausted unexpectedly".into(),
        ))
    }

    async fn request_one(
        &self,
        url: &url::Url,
        policy: RequestPolicy,
        body_limit: usize,
    ) -> Result<RawResponse> {
        let request = build_request(url, policy)?;
        self.request_one_with_request(url, policy, body_limit, request)
            .await
    }

    async fn request_one_with_request(
        &self,
        url: &url::Url,
        policy: RequestPolicy,
        body_limit: usize,
        request: Request<Full<Bytes>>,
    ) -> Result<RawResponse> {
        let host = connection_host(url)?;
        let port = url.port_or_known_default().ok_or_else(|| {
            CoreError::InvalidArgument(format!("HTTP URL has no known port: {url}"))
        })?;
        // Keep hostnames intact until the binder chooses its connection
        // strategy. The ordinary contained binder resolves here through its
        // default `connect_host` implementation; a SOCKS5 binder instead
        // sends a domain-form CONNECT request so target DNS remains remote.
        let stream = self.binder.connect_host(&host, port).await?;

        if url.scheme() == "https" {
            let connector = tokio_rustls::TlsConnector::from(self.tls_config.clone());
            let server_name: rustls::pki_types::ServerName<'static> =
                host.clone().try_into().map_err(|error| {
                    CoreError::HttpProtocol(format!("invalid TLS server name {host}: {error}"))
                })?;
            let tls = connector
                .connect(server_name, stream)
                .await
                .map_err(|error| {
                    CoreError::HttpProtocol(format!("TLS handshake for {url} failed: {error}"))
                })?;
            request_over_stream(tls, request, policy, body_limit, url).await
        } else {
            request_over_stream(stream, request, policy, body_limit, url).await
        }
    }
}

fn default_tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        })
        .clone()
}

fn parse_http_url(raw: &str) -> Result<url::Url> {
    let mut url = url::Url::parse(raw)
        .map_err(|error| CoreError::InvalidArgument(format!("invalid HTTP URL: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(CoreError::InvalidArgument(format!(
            "unsupported HTTP URL scheme: {}",
            url.scheme()
        )));
    }
    if url.host().is_none() {
        return Err(CoreError::InvalidArgument(format!(
            "HTTP URL has no host: {url}"
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CoreError::InvalidArgument(
            "HTTP URL user information is not permitted".into(),
        ));
    }
    url.set_fragment(None);
    Ok(url)
}

fn resolve_redirect(current: &url::Url, location: &str) -> Result<url::Url> {
    let joined = current.join(location).map_err(|error| {
        CoreError::HttpProtocol(format!("invalid redirect Location from {current}: {error}"))
    })?;
    parse_http_url(joined.as_str()).map_err(|error| {
        CoreError::HttpProtocol(format!(
            "redirect from {current} is not a permitted HTTP URL: {error}"
        ))
    })
}

fn build_request(url: &url::Url, policy: RequestPolicy) -> Result<Request<Full<Bytes>>> {
    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };
    let target = match url.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };
    let authority = host_authority(url)?;
    let mut builder = Request::builder()
        .method(Method::GET)
        .version(Version::HTTP_11)
        .uri(target)
        .header(HOST, authority)
        .header(CONNECTION, "close")
        .header(USER_AGENT, "SwarmOtter/1.0");
    if let Some(range) = policy.range_header() {
        builder = builder.header(RANGE, range);
    }
    builder.body(Full::new(Bytes::new())).map_err(|error| {
        CoreError::HttpProtocol(format!("could not build contained HTTP request: {error}"))
    })
}

fn build_upnp_soap_request(
    url: &url::Url,
    soap_action: &str,
    body: &[u8],
) -> Result<Request<Full<Bytes>>> {
    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };
    let target = match url.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };
    let authority = host_authority(url)?;
    Request::builder()
        .method(Method::POST)
        .version(Version::HTTP_11)
        .uri(target)
        .header(HOST, authority)
        .header(CONNECTION, "close")
        .header(USER_AGENT, "SwarmOtter/1.0")
        .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
        .header("SOAPACTION", format!("\"{soap_action}\""))
        .body(Full::new(Bytes::copy_from_slice(body)))
        .map_err(|error| {
            CoreError::HttpProtocol(format!(
                "could not build contained UPnP SOAP request: {error}"
            ))
        })
}

fn host_authority(url: &url::Url) -> Result<String> {
    let host = match url
        .host()
        .ok_or_else(|| CoreError::InvalidArgument(format!("HTTP URL has no host: {url}")))?
    {
        url::Host::Domain(domain) => domain.to_string(),
        url::Host::Ipv4(address) => address.to_string(),
        url::Host::Ipv6(address) => format!("[{address}]"),
    };
    let default_port = match url.scheme() {
        "http" => 80,
        "https" => 443,
        scheme => {
            return Err(CoreError::InvalidArgument(format!(
                "unsupported HTTP URL scheme: {scheme}"
            )));
        }
    };
    Ok(match url.port() {
        Some(port) if port != default_port => format!("{host}:{port}"),
        _ => host,
    })
}

fn connection_host(url: &url::Url) -> Result<String> {
    Ok(
        match url
            .host()
            .ok_or_else(|| CoreError::InvalidArgument(format!("HTTP URL has no host: {url}")))?
        {
            url::Host::Domain(domain) => domain.to_string(),
            url::Host::Ipv4(address) => address.to_string(),
            url::Host::Ipv6(address) => address.to_string(),
        },
    )
}

async fn request_over_stream<S>(
    stream: S,
    request: Request<Full<Bytes>>,
    policy: RequestPolicy,
    body_limit: usize,
    url: &url::Url,
) -> Result<RawResponse>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = http1::Builder::new();
    builder
        .max_buf_size(MAX_HTTP1_HEADER_BYTES)
        .max_headers(MAX_HTTP1_HEADERS);
    let (mut sender, connection) =
        builder
            .handshake(TokioIo::new(stream))
            .await
            .map_err(|error| {
                CoreError::HttpProtocol(format!("HTTP/1 handshake for {url} failed: {error}"))
            })?;
    let driver = ConnectionDriverGuard::new(tokio::spawn(connection));
    let response_result = sender.send_request(request).await;
    drop(sender);
    let response = match response_result {
        Ok(response) => response,
        Err(error) => {
            let driver_detail = driver.abort_and_finish(url).await.err();
            return Err(CoreError::HttpProtocol(format!(
                "HTTP/1 request to {url} failed: {error}{}",
                driver_detail
                    .map(|detail| format!("; {detail}"))
                    .unwrap_or_default()
            )));
        }
    };
    let status = response.status();
    if let Err(error) = validate_response_headers(response.headers(), url) {
        let driver_error = driver.abort_and_finish(url).await.err();
        return Err(merge_http_errors(error, driver_error));
    }
    if is_followed_redirect(status) {
        let location = match one_header(response.headers(), LOCATION, "Location", url) {
            Ok(location) => location,
            Err(error) => {
                let driver_error = driver.abort_and_finish(url).await.err();
                return Err(merge_http_errors(error, driver_error));
            }
        };
        driver.abort_and_finish(url).await?;
        return Ok(RawResponse {
            status,
            body: Vec::new(),
            location,
            content_range: None,
        });
    }

    if !policy.accepts(status) {
        driver.abort_and_finish(url).await?;
        return Ok(RawResponse {
            status,
            body: Vec::new(),
            location: None,
            content_range: None,
        });
    }

    let content_range = if matches!(policy, RequestPolicy::WebseedRange { .. }) {
        match one_header(response.headers(), CONTENT_RANGE, "Content-Range", url) {
            Ok(content_range) => content_range,
            Err(error) => {
                let driver_error = driver.abort_and_finish(url).await.err();
                return Err(merge_http_errors(error, driver_error));
            }
        }
    } else {
        None
    };

    let body = match accumulate_body(response.into_body(), body_limit, url).await {
        Ok(body) => {
            driver.abort_and_finish(url).await?;
            body
        }
        Err(error) => {
            let driver_error = driver.abort_and_finish(url).await.err();
            return Err(merge_http_errors(error, driver_error));
        }
    };
    Ok(RawResponse {
        status,
        body,
        location: None,
        content_range,
    })
}

struct ConnectionDriverGuard {
    handle: Option<tokio::task::JoinHandle<std::result::Result<(), hyper::Error>>>,
}

impl ConnectionDriverGuard {
    fn new(handle: tokio::task::JoinHandle<std::result::Result<(), hyper::Error>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn abort_and_finish(mut self, url: &url::Url) -> Result<()> {
        let handle = self.handle.as_mut().ok_or_else(|| {
            CoreError::HttpProtocol("HTTP/1 connection driver handle was missing".into())
        })?;
        handle.abort();
        let result = match handle.await {
            Err(error) if error.is_cancelled() => Ok(()),
            result => driver_join_result(result, url),
        };
        self.handle.take();
        result
    }
}

impl Drop for ConnectionDriverGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

fn driver_join_result(
    result: std::result::Result<std::result::Result<(), hyper::Error>, tokio::task::JoinError>,
    url: &url::Url,
) -> Result<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(CoreError::HttpProtocol(format!(
            "HTTP/1 connection driver for {url} failed: {error}"
        ))),
        Err(error) => Err(CoreError::HttpProtocol(format!(
            "HTTP/1 connection driver task for {url} failed: {error}"
        ))),
    }
}

fn is_followed_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn merge_http_errors(primary: CoreError, secondary: Option<CoreError>) -> CoreError {
    match secondary {
        Some(secondary) => CoreError::HttpProtocol(format!("{primary}; {secondary}")),
        None => primary,
    }
}

fn one_header(
    headers: &hyper::HeaderMap,
    name: hyper::header::HeaderName,
    display_name: &str,
    url: &url::Url,
) -> Result<Option<String>> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(CoreError::HttpProtocol(format!(
            "HTTP response from {url} has multiple {display_name} headers"
        )));
    }
    value
        .to_str()
        .map(|value| Some(value.to_string()))
        .map_err(|error| {
            CoreError::HttpProtocol(format!(
                "HTTP response from {url} has invalid {display_name}: {error}"
            ))
        })
}

fn validate_response_headers(headers: &hyper::HeaderMap, url: &url::Url) -> Result<()> {
    if headers.len() > MAX_HTTP1_HEADERS {
        return Err(CoreError::HttpProtocol(format!(
            "HTTP response from {url} exceeded {MAX_HTTP1_HEADERS} headers"
        )));
    }
    let mut encoded_bytes = 0usize;
    for (name, value) in headers {
        encoded_bytes = encoded_bytes
            .checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .and_then(|total| total.checked_add(4))
            .ok_or_else(|| {
                CoreError::HttpProtocol(format!("HTTP response header size from {url} overflowed"))
            })?;
        if encoded_bytes > MAX_HTTP1_HEADER_BYTES {
            return Err(CoreError::HttpProtocol(format!(
                "HTTP response headers from {url} exceeded {MAX_HTTP1_HEADER_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

async fn accumulate_body(mut body: Incoming, limit: usize, url: &url::Url) -> Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(limit.min(8 * 1024));
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            CoreError::HttpProtocol(format!(
                "HTTP/1 response body from {url} is malformed or truncated: {error}"
            ))
        })?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let remaining = limit.checked_sub(decoded.len()).ok_or_else(|| {
            CoreError::HttpProtocol(format!(
                "HTTP decoded body accumulator for {url} exceeded its limit"
            ))
        })?;
        if data.len() > remaining {
            return Err(CoreError::HttpProtocol(format!(
                "HTTP decoded body from {url} exceeded {limit} bytes"
            )));
        }
        decoded.extend_from_slice(&data);
    }
    Ok(decoded)
}

fn range_length(start: u64, end_exclusive: u64) -> Result<usize> {
    let length = end_exclusive
        .checked_sub(start)
        .filter(|length| *length > 0)
        .ok_or_else(|| {
            CoreError::InvalidArgument("HTTP byte range end must be greater than start".into())
        })?;
    usize::try_from(length).map_err(|_| {
        CoreError::InvalidArgument("HTTP byte range is too large for this platform".into())
    })
}

fn parse_content_range(value: &str) -> Result<(u64, u64)> {
    let value = value.trim();
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(|| CoreError::HttpProtocol(format!("invalid Content-Range unit: {value}")))?;
    let (range, total) = value
        .split_once('/')
        .ok_or_else(|| CoreError::HttpProtocol(format!("invalid Content-Range syntax: {value}")))?;
    let total = if total == "*" {
        None
    } else {
        Some(total.parse::<u64>().map_err(|error| {
            CoreError::HttpProtocol(format!("invalid Content-Range total {total}: {error}"))
        })?)
    };
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| CoreError::HttpProtocol(format!("invalid Content-Range bounds: {range}")))?;
    let start = start.parse::<u64>().map_err(|error| {
        CoreError::HttpProtocol(format!("invalid Content-Range start {start}: {error}"))
    })?;
    let end = end.parse::<u64>().map_err(|error| {
        CoreError::HttpProtocol(format!("invalid Content-Range end {end}: {error}"))
    })?;
    if end < start {
        return Err(CoreError::HttpProtocol(format!(
            "invalid Content-Range with end before start: {start}-{end}"
        )));
    }
    if total.is_some_and(|total| total <= end) {
        return Err(CoreError::HttpProtocol(format!(
            "invalid Content-Range total for inclusive end {end}"
        )));
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{ContainedUdpSocket, PeerListener};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct SpyBinder {
        routes: Arc<HashMap<(String, u16), SocketAddr>>,
        resolves: Arc<Mutex<Vec<(String, u16)>>>,
        connects: Arc<Mutex<Vec<SocketAddr>>>,
    }

    impl SpyBinder {
        fn new(routes: impl IntoIterator<Item = ((String, u16), SocketAddr)>) -> Self {
            Self {
                routes: Arc::new(routes.into_iter().collect()),
                resolves: Arc::new(Mutex::new(Vec::new())),
                connects: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn resolve_calls(&self) -> Vec<(String, u16)> {
            self.resolves.lock().unwrap().clone()
        }

        fn connect_count(&self) -> usize {
            self.connects.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl NetworkBinder for SpyBinder {
        async fn connect_peer(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
            self.connects.lock().unwrap().push(addr);
            tokio::net::TcpStream::connect(addr)
                .await
                .map_err(CoreError::from)
        }

        async fn resolve_host(&self, host: &str, port: u16) -> Result<SocketAddr> {
            self.resolves.lock().unwrap().push((host.to_string(), port));
            self.routes
                .get(&(host.to_string(), port))
                .copied()
                .ok_or_else(|| {
                    CoreError::NetworkBlocked(format!(
                        "spy binder has no contained route for {host}:{port}"
                    ))
                })
        }

        async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
            Err(CoreError::Internal("unused in HTTP fixture".into()))
        }

        async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
            Err(CoreError::Internal("unused in HTTP fixture".into()))
        }

        fn traffic_allowed(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    enum ServerFinish {
        Close,
        HoldUntilClientClose(Arc<Notify>),
        StallUntilClientClose(Arc<Notify>),
    }

    #[derive(Clone)]
    struct ScriptedResponse {
        bytes: Vec<u8>,
        finish: ServerFinish,
        response_delay: Duration,
    }

    impl ScriptedResponse {
        fn close(bytes: Vec<u8>) -> Self {
            Self {
                bytes,
                finish: ServerFinish::Close,
                response_delay: Duration::ZERO,
            }
        }

        fn delayed_close(bytes: Vec<u8>, response_delay: Duration) -> Self {
            Self {
                bytes,
                finish: ServerFinish::Close,
                response_delay,
            }
        }

        fn hold(bytes: Vec<u8>, closed: Arc<Notify>) -> Self {
            Self {
                bytes,
                finish: ServerFinish::HoldUntilClientClose(closed),
                response_delay: Duration::ZERO,
            }
        }

        fn stall(closed: Arc<Notify>) -> Self {
            Self {
                bytes: Vec::new(),
                finish: ServerFinish::StallUntilClientClose(closed),
                response_delay: Duration::ZERO,
            }
        }
    }

    async fn spawn_http_scripts(
        scripts: Vec<ScriptedResponse>,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let task = tokio::spawn(async move {
            for script in scripts {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                captured.lock().unwrap().push(request);
                tokio::time::sleep(script.response_delay).await;
                if !script.bytes.is_empty() {
                    let _ = stream.write_all(&script.bytes).await;
                }
                match script.finish {
                    ServerFinish::Close => {
                        let _ = stream.shutdown().await;
                    }
                    ServerFinish::HoldUntilClientClose(closed)
                    | ServerFinish::StallUntilClientClose(closed) => {
                        wait_for_client_close(&mut stream).await;
                        closed.notify_one();
                    }
                }
            }
        });
        (address, requests, task)
    }

    async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> String {
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while request.windows(4).all(|window| window != b"\r\n\r\n") {
            let count = stream.read(&mut chunk).await.unwrap();
            if count == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..count]);
            assert!(request.len() <= 128 * 1024, "fixture request was unbounded");
        }
        String::from_utf8(request).unwrap()
    }

    async fn wait_for_client_close<S: AsyncRead + Unpin>(stream: &mut S) {
        let mut byte = [0u8; 1];
        loop {
            match stream.read(&mut byte).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    }

    fn content_length_response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{headers}\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn chunked_response(status: &str, headers: &str, chunks: &[&[u8]]) -> Vec<u8> {
        let mut response =
            format!("HTTP/1.1 {status}\r\nTransfer-Encoding: chunked\r\n{headers}\r\n")
                .into_bytes();
        for chunk in chunks {
            response.extend_from_slice(format!("{:X}\r\n", chunk.len()).as_bytes());
            response.extend_from_slice(chunk);
            response.extend_from_slice(b"\r\n");
        }
        response.extend_from_slice(b"0\r\n\r\n");
        response
    }

    fn redirect_response(status: &str, location: Option<&str>) -> Vec<u8> {
        let location = location
            .map(|location| format!("Location: {location}\r\n"))
            .unwrap_or_default();
        format!("HTTP/1.1 {status}\r\n{location}Content-Length: 0\r\n\r\n").into_bytes()
    }

    async fn wait_closed(closed: &Arc<Notify>) {
        tokio::time::timeout(Duration::from_secs(1), closed.notified())
            .await
            .expect("client did not close the one-shot connection");
    }

    #[tokio::test]
    async fn content_length_and_chunked_complete_without_waiting_for_eof() {
        let length_closed = Arc::new(Notify::new());
        let chunked_closed = Arc::new(Notify::new());
        let scripts = vec![
            ScriptedResponse::hold(
                content_length_response("200 OK", "Connection: keep-alive\r\n", b"length"),
                length_closed.clone(),
            ),
            ScriptedResponse::hold(
                chunked_response("200 OK", "Connection: keep-alive\r\n", &[b"chunk", b"ed"]),
                chunked_closed.clone(),
            ),
        ];
        let (address, _, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([(("framing.test".into(), 18080), address)]);
        let client = ContainedHttpClient::new(&binder);

        let length = tokio::time::timeout(
            Duration::from_secs(1),
            client.get_tracker("http://framing.test:18080/length"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(length.body, b"length");
        wait_closed(&length_closed).await;

        let chunked = tokio::time::timeout(
            Duration::from_secs(1),
            client.get_tracker("http://framing.test:18080/chunked"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(chunked.body, b"chunked");
        wait_closed(&chunked_closed).await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn legal_close_delimited_body_is_decoded() {
        let response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nclose-delimited".to_vec();
        let (address, _, server) =
            spawn_http_scripts(vec![ScriptedResponse::close(response)]).await;
        let binder = SpyBinder::new([(("close.test".into(), 80), address)]);
        let response = ContainedHttpClient::new(&binder)
            .get_tracker("http://close.test/announce")
            .await
            .unwrap();
        assert_eq!(response.body, b"close-delimited");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn upnp_soap_post_uses_the_contained_binder_and_does_not_follow_redirects() {
        let scripts = vec![
            ScriptedResponse::close(content_length_response("200 OK", "", b"ok")),
            ScriptedResponse::close(redirect_response("302 Found", Some("/other"))),
            ScriptedResponse::close(redirect_response("302 Found", Some("/other"))),
        ];
        let (address, requests, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([(("router.test".into(), 49000), address)]);
        let client = ContainedHttpClient::new(&binder);

        let response = client
            .post_upnp_soap(
                "http://router.test:49000/control",
                "urn:schemas-upnp-org:service:WANIPConnection:1#AddPortMapping",
                b"<soap />",
            )
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        let request = requests.lock().unwrap()[0].clone();
        assert!(request.starts_with("POST /control HTTP/1.1"));
        assert!(request.to_ascii_lowercase().contains(
            "soapaction: \"urn:schemas-upnp-org:service:wanipconnection:1#addportmapping\""
        ));
        assert_eq!(binder.resolve_calls(), vec![("router.test".into(), 49000)]);
        assert_eq!(binder.connect_count(), 1);

        let error = client
            .post_upnp_soap(
                "http://router.test:49000/control",
                "urn:schemas-upnp-org:service:WANIPConnection:1#AddPortMapping",
                b"<soap />",
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::HttpStatus(_)));
        assert_eq!(binder.connect_count(), 2);

        let error = client
            .get_upnp_description("http://router.test:49000/root.xml")
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::HttpStatus(_)));
        assert_eq!(binder.connect_count(), 3);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn truncated_and_malformed_chunk_bodies_are_typed_protocol_errors() {
        let scripts = vec![
            ScriptedResponse::close(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabc".to_vec()),
            ScriptedResponse::close(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nZ\r\nabc\r\n0\r\n\r\n"
                    .to_vec(),
            ),
        ];
        let (address, _, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([(("broken.test".into(), 80), address)]);
        let client = ContainedHttpClient::new(&binder);
        for path in ["truncated", "bad-chunk"] {
            let error = client
                .get_tracker(&format!("http://broken.test/{path}"))
                .await
                .unwrap_err();
            assert!(matches!(error, CoreError::HttpProtocol(_)), "{error}");
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn decoded_tracker_cap_fails_on_first_excess_and_closes_connection() {
        let closed = Arc::new(Notify::new());
        let body = vec![b'x'; MAX_TRACKER_HTTP_BODY_BYTES + 1];
        let response = content_length_response("200 OK", "Connection: keep-alive\r\n", &body);
        let (address, _, server) =
            spawn_http_scripts(vec![ScriptedResponse::hold(response, closed.clone())]).await;
        let binder = SpyBinder::new([(("cap.test".into(), 80), address)]);
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            ContainedHttpClient::new(&binder).get_tracker("http://cap.test/announce"),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(error, CoreError::HttpProtocol(_)));
        assert!(error.to_string().contains("exceeded 2097152 bytes"));
        wait_closed(&closed).await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_header_bytes_and_count_are_rejected() {
        let oversized = format!(
            "HTTP/1.1 200 OK\r\nX-Oversized: {}\r\nContent-Length: 0\r\n\r\n",
            "x".repeat(MAX_HTTP1_HEADER_BYTES)
        )
        .into_bytes();
        let mut too_many = String::from("HTTP/1.1 200 OK\r\n");
        for index in 0..=MAX_HTTP1_HEADERS {
            too_many.push_str(&format!("X-{index}: value\r\n"));
        }
        too_many.push_str("Content-Length: 0\r\n\r\n");
        let scripts = vec![
            ScriptedResponse::close(oversized),
            ScriptedResponse::close(too_many.into_bytes()),
        ];
        let (address, _, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([(("headers.test".into(), 80), address)]);
        let client = ContainedHttpClient::new(&binder);
        for path in ["bytes", "count"] {
            let error = client
                .get_tracker(&format!("http://headers.test/{path}"))
                .await
                .unwrap_err();
            assert!(matches!(error, CoreError::HttpProtocol(_)), "{error}");
        }
        server.await.unwrap();
    }

    #[test]
    fn aggregate_header_limit_accepts_exact_boundary_and_rejects_one_over() {
        let url = url::Url::parse("http://headers.test/").unwrap();
        let mut exact = hyper::HeaderMap::new();
        exact.insert(
            "x",
            hyper::header::HeaderValue::from_bytes(&vec![b'a'; MAX_HTTP1_HEADER_BYTES - 5])
                .unwrap(),
        );
        assert!(validate_response_headers(&exact, &url).is_ok());

        let mut over = hyper::HeaderMap::new();
        over.insert(
            "x",
            hyper::header::HeaderValue::from_bytes(&vec![b'a'; MAX_HTTP1_HEADER_BYTES - 4])
                .unwrap(),
        );
        assert!(matches!(
            validate_response_headers(&over, &url),
            Err(CoreError::HttpProtocol(_))
        ));
    }

    #[tokio::test]
    async fn tracker_redirect_loop_and_five_follow_boundary_have_exact_request_counts() {
        let success_scripts = (0..5)
            .map(|index| {
                ScriptedResponse::close(redirect_response(
                    "302 Found",
                    Some(&format!("/hop{}", index + 1)),
                ))
            })
            .chain(std::iter::once(ScriptedResponse::close(
                content_length_response("200 OK", "", b"ok"),
            )))
            .collect();
        let (success_address, success_requests, success_server) =
            spawn_http_scripts(success_scripts).await;
        let success_binder = SpyBinder::new([(("redirect.test".into(), 18081), success_address)]);
        let response = ContainedHttpClient::new(&success_binder)
            .get_tracker("http://redirect.test:18081/hop0")
            .await
            .unwrap();
        assert_eq!(response.body, b"ok");
        success_server.await.unwrap();
        assert_eq!(success_requests.lock().unwrap().len(), 6);
        assert_eq!(success_binder.connect_count(), 6);

        let limit_scripts = (0..6)
            .map(|index| {
                ScriptedResponse::close(redirect_response(
                    "302 Found",
                    Some(&format!("/hop{}", index + 1)),
                ))
            })
            .collect();
        let (limit_address, limit_requests, limit_server) = spawn_http_scripts(limit_scripts).await;
        let limit_binder = SpyBinder::new([(("limit.test".into(), 18082), limit_address)]);
        let error = ContainedHttpClient::new(&limit_binder)
            .get_tracker("http://limit.test:18082/hop0")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("redirect limit of 5"));
        limit_server.await.unwrap();
        assert_eq!(limit_requests.lock().unwrap().len(), 6);
        assert_eq!(limit_binder.connect_count(), 6);

        let (loop_address, loop_requests, loop_server) =
            spawn_http_scripts(vec![ScriptedResponse::close(redirect_response(
                "302 Found",
                Some("/loop"),
            ))])
            .await;
        let loop_binder = SpyBinder::new([(("loop.test".into(), 80), loop_address)]);
        let error = ContainedHttpClient::new(&loop_binder)
            .get_tracker("http://loop.test/loop")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("redirect loop"));
        loop_server.await.unwrap();
        assert_eq!(loop_requests.lock().unwrap().len(), 1);
        assert_eq!(loop_binder.connect_count(), 1);
    }

    #[tokio::test]
    async fn tracker_redirect_validation_and_status_errors_keep_status_context() {
        let status_closed = Arc::new(Notify::new());
        let scripts = vec![
            ScriptedResponse::close(redirect_response("302 Found", None)),
            ScriptedResponse::close(redirect_response("302 Found", Some("http://[bad"))),
            ScriptedResponse::close(
                b"HTTP/1.1 302 Found\r\nLocation: /one\r\nLocation: /two\r\nContent-Length: 0\r\n\r\n"
                    .to_vec(),
            ),
            ScriptedResponse::close(redirect_response("300 Multiple Choices", Some("/unused"))),
            ScriptedResponse::hold(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 9999999\r\n\r\n".to_vec(),
                status_closed.clone(),
            ),
            ScriptedResponse::close(content_length_response(
                "500 Internal Server Error",
                "",
                b"ignored",
            )),
        ];
        let (address, _, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([(("policy.test".into(), 80), address)]);
        let client = ContainedHttpClient::new(&binder);
        for path in ["missing", "bad", "duplicate"] {
            let error = client
                .get_tracker(&format!("http://policy.test/{path}"))
                .await
                .unwrap_err();
            assert!(matches!(error, CoreError::HttpProtocol(_)), "{error}");
        }
        for (path, status) in [("nonstandard", 300), ("not-found", 404), ("server", 500)] {
            let error = client
                .get_tracker(&format!("http://policy.test/{path}"))
                .await
                .unwrap_err();
            assert!(matches!(error, CoreError::HttpStatus(_)), "{error}");
            assert!(error.to_string().contains(&status.to_string()), "{error}");
            if status == 404 {
                wait_closed(&status_closed).await;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn relative_and_cross_host_redirects_repeat_binder_resolution_and_connect() {
        let (second_address, second_requests, second_server) =
            spawn_http_scripts(vec![ScriptedResponse::close(content_length_response(
                "200 OK",
                "",
                b"cross-host",
            ))])
            .await;
        let cross_location = "http://second.test:19002/final";
        let (first_address, first_requests, first_server) =
            spawn_http_scripts(vec![ScriptedResponse::close(redirect_response(
                "302 Found",
                Some(cross_location),
            ))])
            .await;
        let binder = SpyBinder::new([
            (("first.test".into(), 19001), first_address),
            (("second.test".into(), 19002), second_address),
        ]);
        let response = ContainedHttpClient::new(&binder)
            .get_tracker("http://first.test:19001/base/start")
            .await
            .unwrap();
        assert_eq!(response.body, b"cross-host");
        first_server.await.unwrap();
        second_server.await.unwrap();
        assert_eq!(
            binder.resolve_calls(),
            vec![("first.test".into(), 19001), ("second.test".into(), 19002)]
        );
        assert!(first_requests.lock().unwrap()[0].starts_with("GET /base/start HTTP/1.1"));
        assert!(second_requests.lock().unwrap()[0].starts_with("GET /final HTTP/1.1"));
    }

    async fn spawn_tls_scripts(
        scripts: Vec<ScriptedResponse>,
    ) -> (
        SocketAddr,
        Arc<rustls::ClientConfig>,
        Arc<Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certified = rcgen::generate_simple_self_signed(vec!["secure.test".into()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key =
            rustls::pki_types::PrivateKeyDer::try_from(certified.key_pair.serialize_der()).unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let task = tokio::spawn(async move {
            for script in scripts {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = acceptor.accept(stream).await.unwrap();
                let request = read_request(&mut stream).await;
                captured.lock().unwrap().push(request);
                tokio::time::sleep(script.response_delay).await;
                if !script.bytes.is_empty() {
                    let _ = stream.write_all(&script.bytes).await;
                }
                match script.finish {
                    ServerFinish::Close => {
                        let _ = stream.shutdown().await;
                    }
                    ServerFinish::HoldUntilClientClose(closed)
                    | ServerFinish::StallUntilClientClose(closed) => {
                        wait_for_client_close(&mut stream).await;
                        closed.notify_one();
                    }
                }
            }
        });
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let client_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        (address, client_config, requests, task)
    }

    #[tokio::test]
    async fn https_upgrade_uses_injected_trust_and_downgrade_is_rejected() {
        let (tls_address, tls_config, tls_requests, tls_server) =
            spawn_tls_scripts(vec![ScriptedResponse::close(content_length_response(
                "200 OK", "", b"secure",
            ))])
            .await;
        let (http_address, _, http_server) = spawn_http_scripts(vec![ScriptedResponse::close(
            redirect_response("302 Found", Some("https://secure.test:19443/final")),
        )])
        .await;
        let binder = SpyBinder::new([
            (("upgrade.test".into(), 19080), http_address),
            (("secure.test".into(), 19443), tls_address),
        ]);
        let response = ContainedHttpClient::with_tls_config(&binder, tls_config)
            .get_tracker("http://upgrade.test:19080/start")
            .await
            .unwrap();
        assert_eq!(response.body, b"secure");
        http_server.await.unwrap();
        tls_server.await.unwrap();
        assert!(tls_requests.lock().unwrap()[0]
            .to_ascii_lowercase()
            .contains("host: secure.test:19443"));
        assert_eq!(binder.connect_count(), 2);

        let (downgrade_address, downgrade_config, _, downgrade_server) =
            spawn_tls_scripts(vec![ScriptedResponse::close(redirect_response(
                "302 Found",
                Some("http://plain.test/final"),
            ))])
            .await;
        let downgrade_binder = SpyBinder::new([(("secure.test".into(), 20443), downgrade_address)]);
        let error = ContainedHttpClient::with_tls_config(&downgrade_binder, downgrade_config)
            .get_tracker("https://secure.test:20443/start")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("downgrade"));
        assert_eq!(downgrade_binder.connect_count(), 1);
        downgrade_server.await.unwrap();
    }

    #[tokio::test]
    async fn origin_form_and_host_authority_keep_nondefault_port_and_ipv6_brackets() {
        let scripts = vec![
            ScriptedResponse::close(content_length_response("200 OK", "", b"one")),
            ScriptedResponse::close(content_length_response("200 OK", "", b"two")),
        ];
        let (address, requests, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([
            (("authority.test".into(), 18090), address),
            (("::1".into(), 18091), address),
        ]);
        let client = ContainedHttpClient::new(&binder);
        client
            .get_tracker("http://authority.test:18090/a/b?q=1")
            .await
            .unwrap();
        client
            .get_tracker("http://[::1]:18091/v6?q=2")
            .await
            .unwrap();
        server.await.unwrap();
        let requests = requests.lock().unwrap().clone();
        assert!(requests[0].starts_with("GET /a/b?q=1 HTTP/1.1\r\n"));
        let first_lower = requests[0].to_ascii_lowercase();
        assert!(first_lower.contains("\r\nhost: authority.test:18090\r\n"));
        assert!(!requests[0].contains("http://"));
        assert!(!first_lower.contains("authorization:"));
        assert!(!first_lower.contains("cookie:"));
        assert!(requests[1].starts_with("GET /v6?q=2 HTTP/1.1\r\n"));
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("\r\nhost: [::1]:18091\r\n"));

        let error = client
            .get_tracker("http://user:secret@authority.test:18090/no")
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::InvalidArgument(_)));
        assert_eq!(binder.connect_count(), 2);
    }

    #[tokio::test]
    async fn webseed_range_policy_covers_exact_redirect_and_all_mismatch_cases() {
        let exact_closed = Arc::new(Notify::new());
        let chunked_closed = Arc::new(Notify::new());
        let status_closed = Arc::new(Notify::new());
        let scripts = vec![
            ScriptedResponse::hold(
                content_length_response(
                    "206 Partial Content",
                    "Content-Range: bytes 5-8/20\r\nConnection: keep-alive\r\n",
                    b"abcd",
                ),
                exact_closed.clone(),
            ),
            ScriptedResponse::hold(
                chunked_response(
                    "206 Partial Content",
                    "Content-Range: bytes 5-8/20\r\nConnection: keep-alive\r\n",
                    &[b"ab", b"cd"],
                ),
                chunked_closed.clone(),
            ),
            ScriptedResponse::close(redirect_response("302 Found", Some("/redirected"))),
            ScriptedResponse::close(content_length_response(
                "206 Partial Content",
                "Content-Range: bytes 5-8/20\r\n",
                b"abcd",
            )),
            ScriptedResponse::close(content_length_response(
                "206 Partial Content",
                "Content-Range: bytes 4-7/20\r\n",
                b"abcd",
            )),
            ScriptedResponse::close(content_length_response("206 Partial Content", "", b"abcd")),
            ScriptedResponse::close(content_length_response(
                "206 Partial Content",
                "Content-Range: bytes 5-8/20\r\nContent-Range: bytes 5-8/20\r\n",
                b"abcd",
            )),
            ScriptedResponse::close(content_length_response(
                "206 Partial Content",
                "Content-Range: bytes 5-8/8\r\n",
                b"abcd",
            )),
            ScriptedResponse::hold(
                b"HTTP/1.1 200 OK\r\nContent-Length: 9999999\r\n\r\n".to_vec(),
                status_closed.clone(),
            ),
            ScriptedResponse::close(content_length_response(
                "206 Partial Content",
                "Content-Range: bytes 5-8/20\r\n",
                b"abc",
            )),
            ScriptedResponse::close(content_length_response(
                "206 Partial Content",
                "Content-Range: bytes 5-8/20\r\n",
                b"abcde",
            )),
        ];
        let (address, requests, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([(("webseed.test".into(), 80), address)]);
        let client = ContainedHttpClient::new(&binder);

        for path in ["exact", "chunked", "redirect"] {
            let response = client
                .get_webseed_range(&format!("http://webseed.test/{path}"), 5, 9)
                .await
                .unwrap();
            assert_eq!(response.body, b"abcd");
            if path == "exact" {
                wait_closed(&exact_closed).await;
            } else if path == "chunked" {
                wait_closed(&chunked_closed).await;
            }
        }
        for (path, variant) in [
            ("wrong-range", "protocol"),
            ("missing-range", "protocol"),
            ("duplicate-range", "protocol"),
            ("invalid-total", "protocol"),
            ("status-200", "status"),
            ("short", "protocol"),
            ("excess", "protocol"),
        ] {
            let error = client
                .get_webseed_range(&format!("http://webseed.test/{path}"), 5, 9)
                .await
                .unwrap_err();
            match variant {
                "status" => assert!(matches!(error, CoreError::HttpStatus(_))),
                _ => assert!(matches!(error, CoreError::HttpProtocol(_)), "{error}"),
            }
            if path == "status-200" {
                wait_closed(&status_closed).await;
            }
        }
        server.await.unwrap();
        let requests = requests.lock().unwrap();
        assert!(requests.iter().all(|request| request
            .to_ascii_lowercase()
            .contains("\r\nrange: bytes=5-8\r\n")));
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.starts_with("GET /redirect"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn logical_timeout_aborts_driver_and_closes_server_connection() {
        let closed = Arc::new(Notify::new());
        let (address, _, server) =
            spawn_http_scripts(vec![ScriptedResponse::stall(closed.clone())]).await;
        let binder = SpyBinder::new([(("timeout.test".into(), 80), address)]);
        let error = ContainedHttpClient::with_timeout(&binder, Duration::from_millis(50))
            .get_tracker("http://timeout.test/stall")
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::Elapsed(_)));
        assert_eq!(error.code().as_str(), "timeout");
        wait_closed(&closed).await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn one_logical_timeout_spans_cumulative_redirect_hops() {
        let scripts = vec![
            ScriptedResponse::delayed_close(
                redirect_response("302 Found", Some("/second")),
                Duration::from_millis(100),
            ),
            ScriptedResponse::delayed_close(
                content_length_response("200 OK", "", b"too-late"),
                Duration::from_millis(100),
            ),
        ];
        let (address, requests, server) = spawn_http_scripts(scripts).await;
        let binder = SpyBinder::new([(("cumulative.test".into(), 80), address)]);
        let error = ContainedHttpClient::with_timeout(&binder, Duration::from_millis(150))
            .get_tracker("http://cumulative.test/first")
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::Elapsed(_)));
        assert_eq!(error.code().as_str(), "timeout");
        server.await.unwrap();
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert_eq!(binder.connect_count(), 2);
    }

    #[test]
    fn production_http_path_has_no_general_client_resolver_pool_or_raw_socket() {
        let source = include_str!("http.rs");
        let production = source.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            "reqwest",
            "hyper::Client",
            "TcpStream::connect",
            "ToSocketAddrs",
            "read_to_end",
            "parse_http_response",
        ] {
            assert!(!production.contains(forbidden), "found {forbidden}");
        }
        let daemon_binder = include_str!("../../../swarmotterd/src/netbinder.rs");
        for removed in ["read_to_end", "parse_http_response", "contained_http_get"] {
            assert!(!daemon_binder.contains(removed), "found {removed}");
        }
    }
}
