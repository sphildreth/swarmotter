// SPDX-License-Identifier: Apache-2.0

//! Opt-in, contained listen-port reachability diagnostics.
//!
//! This module is intentionally independent of port-mapping lifecycle. It
//! never binds a default-route socket and does not change torrent scheduling:
//! a result is only an operator-visible diagnostic. Every endpoint request is
//! made through [`NetworkBinder`], which enforces the active containment gate.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use swarmotter_core::net::NetworkBinder;
use swarmotter_core::port_test::{PortTestConfig, PortTestState, PortTestStatus};

use super::*;

const MAX_PORT_TEST_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_PORT_TEST_DETAIL_BYTES: usize = 512;

#[derive(Clone)]
struct CachedPortTest {
    enabled: bool,
    endpoint: Option<String>,
    listen_port: u16,
    status: PortTestStatus,
}

/// A small runtime-owned cache and single-flight guard. The cache is not
/// persisted: a process restart correctly returns to `unknown` until the
/// configured operator endpoint is contacted again.
#[derive(Clone)]
pub(super) struct PortTestRuntime {
    cache: Arc<Mutex<Option<CachedPortTest>>>,
    run_lock: Arc<Mutex<()>>,
}

impl Default for PortTestRuntime {
    fn default() -> Self {
        Self {
            cache: Arc::new(Mutex::new(None)),
            run_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl PortTestRuntime {
    async fn status(&self, config: &PortTestConfig, listen_port: u16) -> PortTestStatus {
        let fallback = baseline_status(config, listen_port);
        let cache = self.cache.lock().await;
        cache
            .as_ref()
            .filter(|entry| cache_matches(entry, config, listen_port))
            .map(|entry| entry.status.clone())
            .unwrap_or(fallback)
    }

    async fn run(
        &self,
        config: &PortTestConfig,
        listen_port: u16,
        binder: Arc<dyn NetworkBinder>,
        listener_already_bound: bool,
        force: bool,
    ) -> PortTestStatus {
        let baseline = baseline_status(config, listen_port);
        if !config.enabled || config.endpoint.is_none() {
            self.store(config, listen_port, baseline.clone()).await;
            return baseline;
        }

        let now = unix_now();
        if !force {
            if let Some(status) = self.cached_if_fresh(config, listen_port, now).await {
                return status;
            }
        }

        // Serialize endpoint traffic. A second click waits for the current
        // request, then sees its cached result rather than launching another
        // outbound request.
        let _run = self.run_lock.lock().await;
        if !force {
            if let Some(status) = self.cached_if_fresh(config, listen_port, unix_now()).await {
                return status;
            }
        }

        // The shared seeding listener is present only while there is active
        // seeding work. Reserve the same contained TCP port for this short
        // diagnostic when it is otherwise idle so an endpoint can test the
        // actual configured listener rather than reporting a false closure.
        // The diagnostic binder path deliberately does not latch a failed
        // local bind into the global containment gate; it still never falls
        // back to a different route or interface.
        let _temporary_listener = if listener_already_bound {
            None
        } else {
            match binder.bind_diagnostic_listener(listen_port).await {
                Ok(listener) => Some(listener),
                Err(error) => {
                    let detail = if error.is_network_blocked() {
                        "the contained network path is unavailable; the port test was not sent"
                    } else {
                        "the configured TCP listener could not be reserved for the port test"
                    };
                    let status = completed_status(
                        config,
                        listen_port,
                        PortTestState::Error,
                        unix_now(),
                        Some(detail.into()),
                    );
                    self.store(config, listen_port, status.clone()).await;
                    return status;
                }
            }
        };

        let status = self
            .request(config, listen_port, binder.as_ref(), unix_now())
            .await;
        self.store(config, listen_port, status.clone()).await;
        status
    }

    async fn cached_if_fresh(
        &self,
        config: &PortTestConfig,
        listen_port: u16,
        now: u64,
    ) -> Option<PortTestStatus> {
        let cache = self.cache.lock().await;
        let entry = cache
            .as_ref()
            .filter(|entry| cache_matches(entry, config, listen_port))?;
        entry
            .status
            .cache_expires_at
            .filter(|expires_at| now < *expires_at)
            .map(|_| entry.status.clone())
    }

    async fn store(&self, config: &PortTestConfig, listen_port: u16, status: PortTestStatus) {
        *self.cache.lock().await = Some(CachedPortTest {
            enabled: config.enabled,
            endpoint: config.endpoint.clone(),
            listen_port,
            status,
        });
    }

    async fn request(
        &self,
        config: &PortTestConfig,
        listen_port: u16,
        binder: &dyn NetworkBinder,
        checked_at: u64,
    ) -> PortTestStatus {
        let Some(endpoint) = config.endpoint.as_deref() else {
            return PortTestStatus::unconfigured(listen_port);
        };
        let endpoint = match endpoint_with_request_parameters(endpoint, listen_port) {
            Ok(endpoint) => endpoint,
            Err(()) => {
                return completed_status(
                    config,
                    listen_port,
                    PortTestState::Error,
                    checked_at,
                    Some("the configured port-test endpoint is invalid".into()),
                );
            }
        };

        let response = tokio::time::timeout(
            Duration::from_secs(config.timeout_seconds),
            binder.http_get(&endpoint),
        )
        .await;
        match response {
            Err(_) => completed_status(
                config,
                listen_port,
                PortTestState::Timeout,
                checked_at,
                Some("the contained port-test request timed out".into()),
            ),
            Ok(Err(error)) if error.is_network_blocked() => completed_status(
                config,
                listen_port,
                PortTestState::Error,
                checked_at,
                Some(
                    "the contained network path is unavailable; the port test was not sent".into(),
                ),
            ),
            Ok(Err(_)) => completed_status(
                config,
                listen_port,
                PortTestState::Error,
                checked_at,
                Some("the contained port-test request failed".into()),
            ),
            Ok(Ok(response)) => match parse_endpoint_result(&response.body) {
                Ok((state, detail)) => {
                    completed_status(config, listen_port, state, checked_at, detail)
                }
                Err(()) => completed_status(
                    config,
                    listen_port,
                    PortTestState::Error,
                    checked_at,
                    Some("the port-test endpoint returned an unsupported result".into()),
                ),
            },
        }
    }
}

impl DaemonRuntime {
    /// Return a status snapshot without issuing network traffic.
    pub(crate) async fn listen_port_test_status(&self) -> PortTestStatus {
        let config = self.config.read().await.clone();
        self.port_test
            .status(&config.port_test, config.torrent.listen_port)
            .await
    }

    /// Run an opt-in contained port test. `force` is reserved for internal
    /// lifecycle integrations (for example, a newly successful mapping); the
    /// native control endpoint intentionally uses the cache.
    pub(crate) async fn run_listen_port_test(&self, force: bool) -> PortTestStatus {
        let config = self.config.read().await.clone();
        let binder = self.make_binder().await;
        let listener_already_bound = self
            .seeder_listener_handle
            .lock()
            .await
            .as_ref()
            .is_some_and(|handle| !handle.is_finished());
        let status = self
            .port_test
            .run(
                &config.port_test,
                config.torrent.listen_port,
                binder,
                listener_already_bound,
                force,
            )
            .await;
        self.publish_event(Event::new(
            "port_test_changed",
            json!({ "port_test": status.clone() }),
        ));
        status
    }
}

fn baseline_status(config: &PortTestConfig, listen_port: u16) -> PortTestStatus {
    if !config.enabled {
        PortTestStatus::disabled(listen_port)
    } else if config.endpoint.is_none() {
        PortTestStatus::unconfigured(listen_port)
    } else {
        PortTestStatus::unknown(listen_port)
    }
}

fn cache_matches(entry: &CachedPortTest, config: &PortTestConfig, listen_port: u16) -> bool {
    entry.enabled == config.enabled
        && entry.endpoint == config.endpoint
        && entry.listen_port == listen_port
}

fn completed_status(
    config: &PortTestConfig,
    listen_port: u16,
    state: PortTestState,
    checked_at: u64,
    detail: Option<String>,
) -> PortTestStatus {
    PortTestStatus {
        enabled: true,
        endpoint_configured: true,
        listen_port,
        state,
        checked_at: Some(checked_at),
        cache_expires_at: Some(checked_at.saturating_add(config.cache_ttl_seconds)),
        detail: detail.map(sanitize_detail),
    }
}

fn endpoint_with_request_parameters(
    endpoint: &str,
    listen_port: u16,
) -> std::result::Result<String, ()> {
    let mut endpoint = url::Url::parse(endpoint).map_err(|_| ())?;
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err(());
    }
    // Keep operator query parameters such as a routing token, but replace
    // protocol-owned parameters so an endpoint cannot accidentally receive a
    // stale port from its configured URL before the actual listener port.
    let retained_query = endpoint
        .query_pairs()
        .filter(|(key, _)| !matches!(key.as_ref(), "listen_port" | "protocol" | "format"))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    {
        let mut query = endpoint.query_pairs_mut();
        query.clear();
        for (key, value) in &retained_query {
            query.append_pair(key, value);
        }
        query.append_pair("listen_port", &listen_port.to_string());
        query.append_pair("protocol", "tcp");
        query.append_pair("format", "swarmotter-port-test-v1");
    }
    Ok(endpoint.into())
}

fn parse_endpoint_result(body: &[u8]) -> std::result::Result<(PortTestState, Option<String>), ()> {
    if body.len() > MAX_PORT_TEST_RESPONSE_BYTES {
        return Err(());
    }
    let text = std::str::from_utf8(body).map_err(|_| ())?.trim();
    match text.to_ascii_lowercase().as_str() {
        "open" => return Ok((PortTestState::Open, None)),
        "closed" => return Ok((PortTestState::Closed, None)),
        _ => {}
    }
    let value: serde_json::Value = serde_json::from_str(text).map_err(|_| ())?;
    let object = value.as_object().ok_or(())?;
    let state = object
        .get("reachable")
        .or_else(|| object.get("open"))
        .and_then(serde_json::Value::as_bool)
        .map(|reachable| {
            if reachable {
                PortTestState::Open
            } else {
                PortTestState::Closed
            }
        })
        .or_else(|| {
            object
                .get("status")
                .or_else(|| object.get("state"))
                .and_then(serde_json::Value::as_str)
                .and_then(parse_endpoint_state)
        })
        .ok_or(())?;
    let detail = object
        .get("detail")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    Ok((state, detail))
}

fn parse_endpoint_state(value: &str) -> Option<PortTestState> {
    match value.trim().to_ascii_lowercase().as_str() {
        "open" => Some(PortTestState::Open),
        "closed" => Some(PortTestState::Closed),
        "error" => Some(PortTestState::Error),
        "timeout" => Some(PortTestState::Timeout),
        _ => None,
    }
}

fn sanitize_detail(detail: String) -> String {
    let mut sanitized = String::with_capacity(detail.len().min(MAX_PORT_TEST_DETAIL_BYTES));
    for character in detail.chars() {
        if sanitized.len().saturating_add(character.len_utf8()) > MAX_PORT_TEST_DETAIL_BYTES {
            break;
        }
        if character.is_control() && !matches!(character, '\n' | '\t') {
            sanitized.push(' ');
        } else {
            sanitized.push(character);
        }
    }
    sanitized
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmotter_core::net::binder::{BlockedBinder, LoopbackBinder};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn enabled_config(endpoint: String) -> PortTestConfig {
        PortTestConfig {
            enabled: true,
            endpoint: Some(endpoint),
            cache_ttl_seconds: 120,
            timeout_seconds: 2,
        }
    }

    #[test]
    fn endpoint_parameters_preserve_operator_query_and_parse_compatible_results() {
        let url = endpoint_with_request_parameters(
            "https://example.test/check?token=operator&listen_port=1",
            51413,
        )
        .unwrap();
        assert!(url.contains("token=operator"));
        assert!(url.contains("listen_port=51413"));
        assert!(!url.contains("listen_port=1&"));
        assert!(url.contains("protocol=tcp"));
        assert_eq!(
            parse_endpoint_result(br#"{"reachable":true,"detail":"reachable"}"#).unwrap(),
            (PortTestState::Open, Some("reachable".into()))
        );
        assert_eq!(
            parse_endpoint_result(b"closed").unwrap(),
            (PortTestState::Closed, None)
        );
        assert!(parse_endpoint_result(br#"{"status":"unknown"}"#).is_err());

        let bounded = sanitize_detail("é".repeat(MAX_PORT_TEST_DETAIL_BYTES));
        assert!(bounded.len() <= MAX_PORT_TEST_DETAIL_BYTES);
        assert!(bounded.is_char_boundary(bounded.len()));
    }

    #[tokio::test]
    async fn uses_contained_binder_and_caches_operator_endpoint_result() {
        let temporary_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let count = stream.read(&mut request).await.unwrap();
            let request = std::str::from_utf8(&request[..count]).unwrap();
            assert!(request.starts_with("GET /check?"));
            assert!(request.contains(&format!("listen_port={temporary_port}")));
            assert!(request.contains("protocol=tcp"));
            assert!(request.contains("format=swarmotter-port-test-v1"));
            let reachable = tokio::net::TcpStream::connect(("127.0.0.1", temporary_port))
                .await
                .is_ok();
            let body = if reachable {
                b"{\"reachable\":true}".as_slice()
            } else {
                b"{\"reachable\":false}".as_slice()
            };
            stream
                .write_all(
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(body).await.unwrap();
        });
        let runtime = PortTestRuntime::default();
        let config = enabled_config(format!("http://{address}/check"));
        let binder: Arc<dyn NetworkBinder> = Arc::new(LoopbackBinder);

        let first = runtime
            .run(&config, temporary_port, binder.clone(), false, false)
            .await;
        let second = runtime
            .run(&config, temporary_port, binder, false, false)
            .await;

        assert_eq!(first.state, PortTestState::Open);
        assert_eq!(second.state, PortTestState::Open);
        assert_eq!(first.checked_at, second.checked_at);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn blocked_containment_is_informational_and_never_falls_back() {
        let runtime = PortTestRuntime::default();
        let config = enabled_config("http://127.0.0.1:9/check".into());
        let binder: Arc<dyn NetworkBinder> = Arc::new(BlockedBinder);

        let status = runtime.run(&config, 51413, binder, false, false).await;

        assert_eq!(status.state, PortTestState::Error);
        assert!(status
            .detail
            .as_deref()
            .unwrap()
            .contains("contained network path"));
        assert!(status.checked_at.is_some());
    }
}
