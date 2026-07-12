// SPDX-License-Identifier: Apache-2.0

//! Real-router matrix for the browser-origin policy shared by the native,
//! Transmission, and qBittorrent control surfaces. See ADR-0044/ADR-0049.

#[allow(dead_code)]
mod fake_daemon;

use axum::body::{to_bytes, Body};
use axum::http::{header, HeaderValue, Request, Response, StatusCode};
use swarmotter_api::app_router;
use swarmotter_api::routes::app_router_with_body_limit;
use swarmotter_api::state::{AddTorrentOptions, DaemonOps, SharedState};
use swarmotter_core::config::Config;
use swarmotter_core::hash::InfoHash;
use swarmotter_core::models::network::NetworkContainmentMode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

use fake_daemon::FakeDaemon;

const HOST: &str = "127.0.0.1:9091";
const TOKEN: &str = "test-token";
const SEEDED_MAGNET: &str =
    "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=origin-seeded";
const ADDED_MAGNET: &str =
    "magnet:?xt=urn:btih:89abcdef0123456789abcdef0123456789abcdef&dn=origin-added";
const EXTENSION_ID: &str = "abcdefghijklmnopabcdefghijklmnop";
const QBIT_ADDED_MAGNET_FORM: &str = concat!(
    "urls=magnet%3A%3Fxt%3Durn%3Abtih%3A89abcdef0123456789abcdef0123456789abcdef",
    "%26dn%3Dorigin-qbit-added"
);

#[derive(Clone, Copy, Debug)]
enum AuthMode {
    Enabled,
    Disabled,
}

impl AuthMode {
    const ALL: [Self; 2] = [Self::Enabled, Self::Disabled];

    fn label(self) -> &'static str {
        match self {
            Self::Enabled => "auth-enabled",
            Self::Disabled => "auth-disabled",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Surface {
    Native,
    Transmission,
    Qbittorrent,
}

#[derive(Clone, Copy, Debug)]
enum ControlRoute {
    NativeAdd,
    NativeBulkAdd,
    NativePause,
    NativeRemove,
    NativeSettings,
    NativeWebSocket,
    NativeSse,
    TransmissionNegotiation,
    TransmissionMutation,
    QbittorrentLogin,
    QbittorrentAdd,
    QbittorrentPause,
    QbittorrentResume,
    QbittorrentDelete,
}

impl ControlRoute {
    const ALL: [Self; 14] = [
        Self::NativeAdd,
        Self::NativeBulkAdd,
        Self::NativePause,
        Self::NativeRemove,
        Self::NativeSettings,
        Self::NativeWebSocket,
        Self::NativeSse,
        Self::TransmissionNegotiation,
        Self::TransmissionMutation,
        Self::QbittorrentLogin,
        Self::QbittorrentAdd,
        Self::QbittorrentPause,
        Self::QbittorrentResume,
        Self::QbittorrentDelete,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::NativeAdd => "native add",
            Self::NativeBulkAdd => "native bulk add",
            Self::NativePause => "native pause",
            Self::NativeRemove => "native remove",
            Self::NativeSettings => "native settings",
            Self::NativeWebSocket => "native WebSocket",
            Self::NativeSse => "native SSE",
            Self::TransmissionNegotiation => "Transmission negotiation",
            Self::TransmissionMutation => "Transmission mutation",
            Self::QbittorrentLogin => "qBittorrent login",
            Self::QbittorrentAdd => "qBittorrent add",
            Self::QbittorrentPause => "qBittorrent pause",
            Self::QbittorrentResume => "qBittorrent resume",
            Self::QbittorrentDelete => "qBittorrent delete",
        }
    }

    fn surface(self) -> Surface {
        match self {
            Self::NativeAdd
            | Self::NativeBulkAdd
            | Self::NativePause
            | Self::NativeRemove
            | Self::NativeSettings
            | Self::NativeWebSocket
            | Self::NativeSse => Surface::Native,
            Self::TransmissionNegotiation | Self::TransmissionMutation => Surface::Transmission,
            Self::QbittorrentLogin
            | Self::QbittorrentAdd
            | Self::QbittorrentPause
            | Self::QbittorrentResume
            | Self::QbittorrentDelete => Surface::Qbittorrent,
        }
    }

    fn accepted_status(self) -> StatusCode {
        match self {
            // A Tower oneshot has no Hyper OnUpgrade extension, so Axum
            // reaches WebSocket extraction and returns Upgrade Required. The
            // production server supplies that extension and returns 101.
            Self::NativeWebSocket => StatusCode::UPGRADE_REQUIRED,
            Self::TransmissionNegotiation => StatusCode::CONFLICT,
            _ => StatusCode::OK,
        }
    }

    fn expected_daemon_call(self) -> &'static str {
        match self {
            Self::NativeAdd | Self::NativeBulkAdd | Self::QbittorrentAdd => "add_magnet",
            Self::NativePause | Self::QbittorrentPause => "pause",
            Self::NativeRemove | Self::QbittorrentDelete => "remove_torrent",
            Self::NativeSettings | Self::TransmissionMutation => "update_settings",
            Self::QbittorrentResume => "resume",
            Self::NativeWebSocket
            | Self::NativeSse
            | Self::TransmissionNegotiation
            | Self::QbittorrentLogin => "get_config",
        }
    }

    fn request(self, state: &SharedState, hash: &InfoHash) -> Request<Body> {
        let hash = hash.to_hex();
        let (method, uri, content_type, body) = match self {
            Self::NativeAdd => (
                "POST",
                "/api/v1/torrents/magnet".to_string(),
                "application/json",
                format!(r#"{{"magnet":"{ADDED_MAGNET}"}}"#),
            ),
            Self::NativeBulkAdd => (
                "POST",
                "/api/v1/torrents/bulk".to_string(),
                "application/json",
                serde_json::json!({ "magnets": [ADDED_MAGNET] }).to_string(),
            ),
            Self::NativePause => (
                "POST",
                format!("/api/v1/torrents/{hash}/pause"),
                "application/json",
                String::new(),
            ),
            Self::NativeRemove => (
                "DELETE",
                format!("/api/v1/torrents/{hash}?delete_data=false"),
                "application/json",
                String::new(),
            ),
            Self::NativeSettings => (
                "PATCH",
                "/api/v1/settings".to_string(),
                "application/json",
                "{}".to_string(),
            ),
            Self::NativeWebSocket => (
                "GET",
                "/api/v1/ws".to_string(),
                "application/json",
                String::new(),
            ),
            Self::NativeSse => (
                "GET",
                "/api/v1/events".to_string(),
                "application/json",
                String::new(),
            ),
            Self::TransmissionNegotiation => (
                "POST",
                "/transmission/rpc".to_string(),
                "application/json",
                r#"{"method":"session-get","arguments":{},"tag":1}"#.to_string(),
            ),
            Self::TransmissionMutation => (
                "POST",
                "/transmission/rpc".to_string(),
                "application/json",
                r#"{"method":"session-set","arguments":{"speed-limit-down":1},"tag":2}"#
                    .to_string(),
            ),
            Self::QbittorrentLogin => (
                "POST",
                "/api/v2/auth/login".to_string(),
                "application/x-www-form-urlencoded",
                format!("username=admin&password={TOKEN}"),
            ),
            Self::QbittorrentAdd => (
                "POST",
                "/api/v2/torrents/add".to_string(),
                "application/x-www-form-urlencoded",
                QBIT_ADDED_MAGNET_FORM.to_string(),
            ),
            Self::QbittorrentPause => (
                "POST",
                "/api/v2/torrents/pause".to_string(),
                "application/x-www-form-urlencoded",
                format!("hashes={hash}"),
            ),
            Self::QbittorrentResume => (
                "POST",
                "/api/v2/torrents/resume".to_string(),
                "application/x-www-form-urlencoded",
                format!("hashes={hash}"),
            ),
            Self::QbittorrentDelete => (
                "POST",
                "/api/v2/torrents/delete".to_string(),
                "application/x-www-form-urlencoded",
                format!("hashes={hash}&deleteFiles=false"),
            ),
        };

        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, HOST)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .body(Body::from(body))
            .expect("matrix request must be valid");

        if matches!(self, Self::TransmissionMutation) {
            request.headers_mut().insert(
                "x-transmission-session-id",
                HeaderValue::from_str(state.transmission.session_id())
                    .expect("test session id must be a header value"),
            );
        }
        if matches!(self, Self::NativeWebSocket) {
            request
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
            request
                .headers_mut()
                .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
            request
                .headers_mut()
                .insert("sec-websocket-version", HeaderValue::from_static("13"));
            request.headers_mut().insert(
                "sec-websocket-key",
                HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ=="),
            );
        }
        request
    }
}

#[derive(Clone, Copy, Debug)]
enum RejectedHeaders {
    SameSite,
    CrossSite,
    ForeignOrigin,
    MalformedOrigin,
    NullOrigin,
    DuplicateOrigin,
    MultiValueOrigin,
    InvalidOriginBytes,
    InvalidFetchBytes,
    UnsupportedFetchSite,
    DuplicateFetchSite,
    OriginPath,
    OriginQuery,
    OriginFragment,
    OriginUserInfo,
    ChromeExtensionShortId,
    ChromeExtensionInvalidId,
    ChromeExtensionPort,
    DuplicateHost,
    InvalidHostBytes,
}

impl RejectedHeaders {
    const ALL: [Self; 20] = [
        Self::SameSite,
        Self::CrossSite,
        Self::ForeignOrigin,
        Self::MalformedOrigin,
        Self::NullOrigin,
        Self::DuplicateOrigin,
        Self::MultiValueOrigin,
        Self::InvalidOriginBytes,
        Self::InvalidFetchBytes,
        Self::UnsupportedFetchSite,
        Self::DuplicateFetchSite,
        Self::OriginPath,
        Self::OriginQuery,
        Self::OriginFragment,
        Self::OriginUserInfo,
        Self::ChromeExtensionShortId,
        Self::ChromeExtensionInvalidId,
        Self::ChromeExtensionPort,
        Self::DuplicateHost,
        Self::InvalidHostBytes,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::SameSite => "same-site Fetch Metadata",
            Self::CrossSite => "cross-site Fetch Metadata",
            Self::ForeignOrigin => "foreign Origin",
            Self::MalformedOrigin => "malformed/opaque Origin",
            Self::NullOrigin => "Origin null",
            Self::DuplicateOrigin => "duplicate Origin fields",
            Self::MultiValueOrigin => "multi-value Origin",
            Self::InvalidOriginBytes => "invalid UTF-8 Origin",
            Self::InvalidFetchBytes => "invalid UTF-8 Sec-Fetch-Site",
            Self::UnsupportedFetchSite => "unsupported Sec-Fetch-Site",
            Self::DuplicateFetchSite => "duplicate Sec-Fetch-Site fields",
            Self::OriginPath => "Origin path",
            Self::OriginQuery => "Origin query",
            Self::OriginFragment => "Origin fragment",
            Self::OriginUserInfo => "Origin userinfo",
            Self::ChromeExtensionShortId => "short Chrome extension ID",
            Self::ChromeExtensionInvalidId => "invalid Chrome extension ID alphabet",
            Self::ChromeExtensionPort => "Chrome extension Origin with a port",
            Self::DuplicateHost => "duplicate Host fields",
            Self::InvalidHostBytes => "invalid UTF-8 Host",
        }
    }

    fn apply(self, request: &mut Request<Body>) {
        let headers = request.headers_mut();
        let same_origin = HeaderValue::from_static("http://127.0.0.1:9091");
        match self {
            Self::SameSite => {
                headers.insert(header::ORIGIN, same_origin);
                headers.insert("sec-fetch-site", HeaderValue::from_static("same-site"));
            }
            Self::CrossSite => {
                headers.insert(header::ORIGIN, same_origin);
                headers.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
            }
            Self::ForeignOrigin => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("https://foreign.example"),
                );
            }
            Self::MalformedOrigin => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("data:text/plain-opaque"),
                );
            }
            Self::NullOrigin => {
                headers.insert(header::ORIGIN, HeaderValue::from_static("null"));
            }
            Self::DuplicateOrigin => {
                headers.append(header::ORIGIN, same_origin.clone());
                headers.append(header::ORIGIN, same_origin);
            }
            Self::MultiValueOrigin => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("http://127.0.0.1:9091 http://127.0.0.1:9091"),
                );
            }
            Self::InvalidOriginBytes => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_bytes(&[0x80]).expect("obs-text is a valid header byte"),
                );
            }
            Self::InvalidFetchBytes => {
                headers.insert(
                    "sec-fetch-site",
                    HeaderValue::from_bytes(&[0x80]).expect("obs-text is a valid header byte"),
                );
            }
            Self::UnsupportedFetchSite => {
                headers.insert("sec-fetch-site", HeaderValue::from_static("same-party"));
            }
            Self::DuplicateFetchSite => {
                headers.append("sec-fetch-site", HeaderValue::from_static("same-origin"));
                headers.append("sec-fetch-site", HeaderValue::from_static("none"));
            }
            Self::OriginPath => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("http://127.0.0.1:9091/a"),
                );
            }
            Self::OriginQuery => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("http://127.0.0.1:9091?query=1"),
                );
            }
            Self::OriginFragment => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("http://127.0.0.1:9091#fragment"),
                );
            }
            Self::OriginUserInfo => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("http://user@127.0.0.1:9091"),
                );
            }
            Self::ChromeExtensionShortId => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("chrome-extension://abcdefghijklmnop"),
                );
                headers.insert("sec-fetch-site", HeaderValue::from_static("none"));
            }
            Self::ChromeExtensionInvalidId => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static("chrome-extension://abcdefghijklmnopabcdefghijklmnoq"),
                );
                headers.insert("sec-fetch-site", HeaderValue::from_static("none"));
            }
            Self::ChromeExtensionPort => {
                headers.insert(
                    header::ORIGIN,
                    HeaderValue::from_static(
                        "chrome-extension://abcdefghijklmnopabcdefghijklmnop:443",
                    ),
                );
                headers.insert("sec-fetch-site", HeaderValue::from_static("none"));
            }
            Self::DuplicateHost => {
                headers.insert(header::ORIGIN, same_origin);
                headers.append(header::HOST, HeaderValue::from_static(HOST));
            }
            Self::InvalidHostBytes => {
                headers.insert(header::ORIGIN, same_origin);
                headers.insert(
                    header::HOST,
                    HeaderValue::from_bytes(&[0x80]).expect("obs-text is a valid header byte"),
                );
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum AcceptedHeaders {
    SameOrigin,
    FetchNone,
    Absent,
}

impl AcceptedHeaders {
    const ALL: [Self; 3] = [Self::SameOrigin, Self::FetchNone, Self::Absent];

    fn label(self) -> &'static str {
        match self {
            Self::SameOrigin => "same origin",
            Self::FetchNone => "Sec-Fetch-Site none",
            Self::Absent => "absent browser headers",
        }
    }

    fn apply(self, request: &mut Request<Body>) {
        match self {
            Self::SameOrigin => {
                // Deliberately use a different scheme to prove authority-only
                // comparison for a TLS-terminating reverse proxy.
                request.headers_mut().insert(
                    header::ORIGIN,
                    HeaderValue::from_static("https://127.0.0.1:9091"),
                );
                request
                    .headers_mut()
                    .insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
            }
            Self::FetchNone => {
                request
                    .headers_mut()
                    .insert("sec-fetch-site", HeaderValue::from_static("none"));
            }
            Self::Absent => {}
        }
    }
}

fn apply_chrome_extension_origin(request: &mut Request<Body>) {
    request.headers_mut().insert(
        header::ORIGIN,
        HeaderValue::from_str(&format!("chrome-extension://{EXTENSION_ID}"))
            .expect("fixed extension Origin must be a header value"),
    );
    request
        .headers_mut()
        .insert("sec-fetch-site", HeaderValue::from_static("none"));
}

async fn test_context(auth_mode: AuthMode) -> (SharedState, std::sync::Arc<FakeDaemon>, InfoHash) {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.api.require_auth = matches!(auth_mode, AuthMode::Enabled);
    config.api.auth_token = Some(TOKEN.to_string());
    config.compatibility.transmission.enabled = true;
    config.compatibility.qbittorrent.enabled = true;
    let (state, daemon) = fake_daemon::fake_state_with_config_and_daemon(config);
    let hash = daemon
        .add_magnet(SEEDED_MAGNET, AddTorrentOptions::default())
        .await
        .expect("seed torrent must be valid");
    daemon.clear_calls().await;
    (state, daemon, hash)
}

async fn assert_rejection_shape(surface: Surface, response: Response<Body>, context: &str) {
    assert_eq!(response.status(), StatusCode::FORBIDDEN, "{context}");
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("rejection body must be readable");
    match surface {
        Surface::Native => {
            assert_eq!(content_type, "application/json", "{context}");
            let json: serde_json::Value =
                serde_json::from_slice(&body).expect("native error must be JSON");
            assert_eq!(json["success"], false, "{context}");
            assert_eq!(json["error"]["code"], "cross_origin_forbidden", "{context}");
        }
        Surface::Transmission => {
            assert_eq!(content_type, "application/json", "{context}");
            let json: serde_json::Value =
                serde_json::from_slice(&body).expect("Transmission error must be JSON");
            assert!(json["error"].is_string(), "{context}: {json}");
            assert!(json.get("success").is_none(), "{context}: {json}");
        }
        Surface::Qbittorrent => {
            assert_eq!(content_type, "text/plain; charset=utf-8", "{context}");
            assert_eq!(&body[..], b"Forbidden", "{context}");
        }
    }
}

async fn assert_extension_rejection_shape(
    surface: Surface,
    response: Response<Body>,
    context: &str,
) {
    assert_eq!(response.status(), StatusCode::FORBIDDEN, "{context}");
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("extension rejection body must be readable");
    match surface {
        Surface::Native => {
            assert_eq!(content_type, "application/json", "{context}");
            let json: serde_json::Value =
                serde_json::from_slice(&body).expect("native error must be JSON");
            assert_eq!(json["success"], false, "{context}");
            assert_eq!(
                json["error"]["code"], "extension_origin_forbidden",
                "{context}"
            );
            assert!(
                json["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("api.require_auth = true")),
                "{context}: {json}"
            );
        }
        Surface::Transmission => {
            assert_eq!(content_type, "application/json", "{context}");
            let json: serde_json::Value =
                serde_json::from_slice(&body).expect("Transmission error must be JSON");
            assert!(
                json["error"]
                    .as_str()
                    .is_some_and(|message| message.contains("api.require_auth = true")),
                "{context}: {json}"
            );
        }
        Surface::Qbittorrent => {
            assert_eq!(content_type, "text/plain; charset=utf-8", "{context}");
            assert_eq!(&body[..], b"Forbidden", "{context}");
        }
    }
}

#[tokio::test]
async fn real_router_origin_matrix_covers_all_control_routes_and_auth_modes() {
    // Every rejected header shape is exercised against every named route in
    // both auth modes. A fresh context per header shape lets all routes share a
    // seeded torrent while preserving an exact before/after state comparison.
    for auth_mode in AuthMode::ALL {
        for rejected in RejectedHeaders::ALL {
            let (state, daemon, hash) = test_context(auth_mode).await;
            let app = app_router(state.clone());
            for route in ControlRoute::ALL {
                daemon.clear_calls().await;
                let before = daemon.state_snapshot().await;
                let mut request = route.request(&state, &hash);
                rejected.apply(&mut request);
                let context = format!(
                    "{} / {} / {}",
                    auth_mode.label(),
                    route.label(),
                    rejected.label()
                );
                let response = app
                    .clone()
                    .oneshot(request)
                    .await
                    .expect("real router must answer");
                assert_rejection_shape(route.surface(), response, &context).await;
                assert_eq!(
                    daemon.observed_calls().await,
                    Vec::<&'static str>::new(),
                    "{context}: rejection must precede auth, compatibility checks, and daemon calls"
                );
                assert_eq!(
                    daemon.state_snapshot().await,
                    before,
                    "{context}: rejected request changed daemon state"
                );
            }
        }
    }

    // Allowed values and headerless automation must preserve the handler's
    // documented behavior. Each route gets fresh state because remove/delete
    // operations intentionally consume the seeded torrent.
    for auth_mode in AuthMode::ALL {
        for accepted in AcceptedHeaders::ALL {
            for route in ControlRoute::ALL {
                let (state, daemon, hash) = test_context(auth_mode).await;
                let app = app_router(state.clone());
                let mut request = route.request(&state, &hash);
                accepted.apply(&mut request);
                let context = format!(
                    "{} / {} / {}",
                    auth_mode.label(),
                    route.label(),
                    accepted.label()
                );
                let response = app.oneshot(request).await.expect("real router must answer");
                assert_eq!(response.status(), route.accepted_status(), "{context}");
                let calls = daemon.observed_calls().await;
                assert!(
                    calls.contains(&route.expected_daemon_call()),
                    "{context}: expected production-boundary call {:?}, got {calls:?}",
                    route.expected_daemon_call()
                );
            }
        }
    }
}

#[tokio::test]
async fn authenticated_chrome_extension_origin_reaches_every_control_surface() {
    for route in ControlRoute::ALL {
        let (state, daemon, hash) = test_context(AuthMode::Enabled).await;
        let app = app_router(state.clone());
        let mut request = route.request(&state, &hash);
        apply_chrome_extension_origin(&mut request);
        let context = format!("authenticated Chrome extension / {}", route.label());
        let response = app.oneshot(request).await.expect("real router must answer");
        assert_eq!(response.status(), route.accepted_status(), "{context}");
        let calls = daemon.observed_calls().await;
        assert!(
            calls.contains(&route.expected_daemon_call()),
            "{context}: expected production-boundary call {:?}, got {calls:?}",
            route.expected_daemon_call()
        );
    }

    // The direct API-token header is equivalent to Bearer authentication for
    // extension clients and must reach the reported bulk-add regression path.
    let (state, daemon, hash) = test_context(AuthMode::Enabled).await;
    let app = app_router(state.clone());
    let mut request = ControlRoute::NativeBulkAdd.request(&state, &hash);
    request.headers_mut().remove(header::AUTHORIZATION);
    request
        .headers_mut()
        .insert("x-swarmotter-auth", HeaderValue::from_static(TOKEN));
    apply_chrome_extension_origin(&mut request);
    let response = app.oneshot(request).await.expect("real router must answer");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(daemon.observed_calls().await.contains(&"add_magnet"));
}

#[tokio::test]
async fn chrome_extension_origin_requires_authenticated_mode_and_one_valid_token() {
    #[derive(Clone, Copy)]
    enum Rejection {
        AuthenticationDisabled,
        MissingToken,
        InvalidToken,
        DuplicateAuthorization,
        MultipleCredentialHeaders,
    }

    for rejection in [
        Rejection::AuthenticationDisabled,
        Rejection::MissingToken,
        Rejection::InvalidToken,
        Rejection::DuplicateAuthorization,
        Rejection::MultipleCredentialHeaders,
    ] {
        for route in ControlRoute::ALL {
            let auth_mode = if matches!(rejection, Rejection::AuthenticationDisabled) {
                AuthMode::Disabled
            } else {
                AuthMode::Enabled
            };
            let (state, daemon, hash) = test_context(auth_mode).await;
            let app = app_router(state.clone());
            let mut request = route.request(&state, &hash);
            let credential_label = match rejection {
                Rejection::AuthenticationDisabled => "authentication disabled",
                Rejection::MissingToken => {
                    request.headers_mut().remove(header::AUTHORIZATION);
                    "missing token"
                }
                Rejection::InvalidToken => {
                    request.headers_mut().insert(
                        header::AUTHORIZATION,
                        HeaderValue::from_static("Bearer invalid-token"),
                    );
                    "invalid token"
                }
                Rejection::DuplicateAuthorization => {
                    request.headers_mut().append(
                        header::AUTHORIZATION,
                        HeaderValue::from_static("Bearer invalid-token"),
                    );
                    "duplicate Authorization"
                }
                Rejection::MultipleCredentialHeaders => {
                    request
                        .headers_mut()
                        .insert("x-swarmotter-auth", HeaderValue::from_static(TOKEN));
                    "multiple credential headers"
                }
            };
            apply_chrome_extension_origin(&mut request);
            let before = daemon.state_snapshot().await;
            daemon.clear_calls().await;
            let context = format!("Chrome extension / {} / {credential_label}", route.label());
            let response = app.oneshot(request).await.expect("real router must answer");
            assert_extension_rejection_shape(route.surface(), response, &context).await;
            assert_eq!(
                daemon.observed_calls().await,
                vec!["get_config"],
                "{context}: only the outer extension credential check may run"
            );
            assert_eq!(
                daemon.state_snapshot().await,
                before,
                "{context}: rejected extension request changed daemon state"
            );
        }
    }
}

#[tokio::test]
async fn chrome_extension_origin_still_requires_valid_host_and_fetch_metadata() {
    #[derive(Clone, Copy)]
    enum Rejection {
        MissingHost,
        DuplicateHost,
        InvalidHostBytes,
        CrossSite,
    }

    for rejection in [
        Rejection::MissingHost,
        Rejection::DuplicateHost,
        Rejection::InvalidHostBytes,
        Rejection::CrossSite,
    ] {
        let (state, daemon, hash) = test_context(AuthMode::Enabled).await;
        let app = app_router(state.clone());
        let mut request = ControlRoute::NativeBulkAdd.request(&state, &hash);
        apply_chrome_extension_origin(&mut request);
        let rejection_label = match rejection {
            Rejection::MissingHost => {
                request.headers_mut().remove(header::HOST);
                "missing Host"
            }
            Rejection::DuplicateHost => {
                request
                    .headers_mut()
                    .append(header::HOST, HeaderValue::from_static(HOST));
                "duplicate Host"
            }
            Rejection::InvalidHostBytes => {
                request.headers_mut().insert(
                    header::HOST,
                    HeaderValue::from_bytes(&[0x80]).expect("obs-text is a valid header byte"),
                );
                "invalid Host bytes"
            }
            Rejection::CrossSite => {
                request
                    .headers_mut()
                    .insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
                "cross-site Fetch Metadata"
            }
        };
        let before = daemon.state_snapshot().await;
        daemon.clear_calls().await;
        let context = format!("Chrome extension bulk add / {rejection_label}");
        let response = app.oneshot(request).await.expect("real router must answer");
        assert_rejection_shape(Surface::Native, response, &context).await;
        assert!(
            daemon.observed_calls().await.is_empty(),
            "{context}: malformed request reached the extension credential lookup"
        );
        assert_eq!(
            daemon.state_snapshot().await,
            before,
            "{context}: rejected extension request changed daemon state"
        );
    }
}

#[tokio::test]
async fn origin_guard_precedes_auth_session_and_compatibility_enabled_checks() {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.api.require_auth = true;
    config.api.auth_token = Some(TOKEN.to_string());
    // Both adapters intentionally remain disabled.
    let (state, daemon) = fake_daemon::fake_state_with_config_and_daemon(config);
    // One byte is deliberately smaller than all three request bodies below;
    // the origin guard must still return its 403 before body extraction can
    // return 413.
    let app = app_router_with_body_limit(state.clone(), 1);
    let hash = InfoHash::from_hex("0123456789abcdef0123456789abcdef01234567")
        .expect("fixed test hash must be valid");

    for route in [
        ControlRoute::NativeSettings,
        ControlRoute::TransmissionNegotiation,
        ControlRoute::QbittorrentLogin,
    ] {
        daemon.clear_calls().await;
        let mut request = route.request(&state, &hash);
        request.headers_mut().remove(header::AUTHORIZATION);
        RejectedHeaders::CrossSite.apply(&mut request);
        let response = app
            .clone()
            .oneshot(request)
            .await
            .expect("real router must answer");
        let context = format!("outer-layer ordering / {}", route.label());
        assert_rejection_shape(route.surface(), response, &context).await;
        assert!(
            daemon.observed_calls().await.is_empty(),
            "{context}: auth or compatibility code ran before the guard"
        );
    }

    // The root health alias is deliberately public and outside the guard.
    let mut health = Request::builder()
        .uri("/health")
        .header(header::HOST, HOST)
        .body(Body::empty())
        .expect("health request must be valid");
    RejectedHeaders::CrossSite.apply(&mut health);
    let response = app.oneshot(health).await.expect("health route must answer");
    assert_ne!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn same_origin_websocket_upgrades_on_the_real_http_server() {
    let (state, daemon, _hash) = test_context(AuthMode::Enabled).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("local control listener must bind");
    let address = listener
        .local_addr()
        .expect("local control listener must have an address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app_router(state))
            .await
            .expect("local control server must run");
    });

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("local control client must connect");
    let handshake = format!(
        concat!(
            "GET /api/v1/ws HTTP/1.1\r\n",
            "Host: {address}\r\n",
            "Origin: https://{address}\r\n",
            "Sec-Fetch-Site: same-origin\r\n",
            "Authorization: Bearer {token}\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "\r\n"
        ),
        address = address,
        token = TOKEN
    );
    stream
        .write_all(handshake.as_bytes())
        .await
        .expect("WebSocket handshake must write");
    let mut response = [0_u8; 2048];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.read(&mut response),
    )
    .await
    .expect("WebSocket handshake response timed out")
    .expect("WebSocket handshake response must read");
    let response = String::from_utf8_lossy(&response[..read]);
    assert!(
        response.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "same-origin WebSocket did not upgrade: {response}"
    );
    assert!(
        daemon.observed_calls().await.contains(&"get_config"),
        "native authentication did not run after the origin guard"
    );

    server.abort();
    let _ = server.await;
}
