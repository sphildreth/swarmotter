// SPDX-License-Identifier: Apache-2.0

//! Configuration and public status models for opt-in listen-port testing.
//!
//! A port-test endpoint is deliberately operator configured: SwarmOtter does
//! not ship a default third-party reachability service. The daemon sends the
//! request through its contained data-plane binder and reports the endpoint's
//! bounded result as an informational diagnostic.

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};

/// Default lifetime of a successful or failed reachability result.
pub const DEFAULT_PORT_TEST_CACHE_TTL_SECONDS: u64 = 15 * 60;
/// Default upper bound for one endpoint request.
pub const DEFAULT_PORT_TEST_TIMEOUT_SECONDS: u64 = 10;
/// Keep a manual test bounded even when an endpoint is slow or unavailable.
pub const MAX_PORT_TEST_TIMEOUT_SECONDS: u64 = 30;
/// Avoid configurations that effectively disable rate limiting of external
/// checks while still allowing an operator to choose a reasonably short cache.
pub const MAX_PORT_TEST_CACHE_TTL_SECONDS: u64 = 24 * 60 * 60;

/// Opt-in configuration for a compatible operator-hosted reachability
/// endpoint.
///
/// The endpoint receives an HTTP GET through the contained data-plane path.
/// The daemon appends `listen_port`, `protocol=tcp`, and
/// `format=swarmotter-port-test-v1` query parameters. It accepts a small JSON
/// response with either `reachable`/`open` boolean or a `status` of `open` or
/// `closed` (and accepts the corresponding plain-text tokens for simple
/// endpoints).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortTestConfig {
    /// Enable outbound reachability checks. Disabled by default so the daemon
    /// never contacts an external service without explicit operator consent.
    #[serde(default)]
    pub enabled: bool,
    /// HTTP or HTTPS endpoint operated/configured by the administrator.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Reuse the last result within this window rather than opening another
    /// external request.
    #[serde(default = "default_port_test_cache_ttl_seconds")]
    pub cache_ttl_seconds: u64,
    /// Per-request upper bound. This is intentionally no longer than the
    /// contained HTTP client's own timeout.
    #[serde(default = "default_port_test_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_port_test_cache_ttl_seconds() -> u64 {
    DEFAULT_PORT_TEST_CACHE_TTL_SECONDS
}

fn default_port_test_timeout_seconds() -> u64 {
    DEFAULT_PORT_TEST_TIMEOUT_SECONDS
}

impl Default for PortTestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            cache_ttl_seconds: default_port_test_cache_ttl_seconds(),
            timeout_seconds: default_port_test_timeout_seconds(),
        }
    }
}

impl PortTestConfig {
    /// Validate local configuration only. Endpoint resolution and all network
    /// I/O remain in the daemon's contained network layer.
    pub fn validate(&self) -> Result<()> {
        if self.cache_ttl_seconds == 0 || self.cache_ttl_seconds > MAX_PORT_TEST_CACHE_TTL_SECONDS {
            return Err(CoreError::InvalidConfig(format!(
                "port_test.cache_ttl_seconds must be between 1 and {MAX_PORT_TEST_CACHE_TTL_SECONDS}"
            )));
        }
        if self.timeout_seconds == 0 || self.timeout_seconds > MAX_PORT_TEST_TIMEOUT_SECONDS {
            return Err(CoreError::InvalidConfig(format!(
                "port_test.timeout_seconds must be between 1 and {MAX_PORT_TEST_TIMEOUT_SECONDS}"
            )));
        }

        let Some(endpoint) = self.endpoint.as_deref() else {
            return if self.enabled {
                Err(CoreError::InvalidConfig(
                    "port_test.endpoint must be configured when port_test.enabled is true".into(),
                ))
            } else {
                Ok(())
            };
        };
        if endpoint.trim().is_empty() {
            return Err(CoreError::InvalidConfig(
                "port_test.endpoint must not be empty when set".into(),
            ));
        }
        let parsed = url::Url::parse(endpoint).map_err(|error| {
            CoreError::InvalidConfig(format!("port_test.endpoint is not a valid URL: {error}"))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(CoreError::InvalidConfig(
                "port_test.endpoint must use http or https".into(),
            ));
        }
        if parsed.host_str().is_none() {
            return Err(CoreError::InvalidConfig(
                "port_test.endpoint must include a host".into(),
            ));
        }
        if parsed.fragment().is_some() {
            return Err(CoreError::InvalidConfig(
                "port_test.endpoint must not include a URL fragment".into(),
            ));
        }
        Ok(())
    }
}

/// The last known reachability outcome. It is informational only: no outcome
/// changes torrent scheduling or containment state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortTestState {
    /// No compatible test has completed for the current endpoint and port.
    #[default]
    Unknown,
    /// The endpoint observed that the configured TCP listener was reachable.
    Open,
    /// The endpoint observed that the configured TCP listener was not
    /// reachable.
    Closed,
    /// A contained request or endpoint response failed.
    Error,
    /// The endpoint did not complete within the configured request bound.
    Timeout,
}

impl PortTestState {
    pub fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }
}

/// Public, non-sensitive snapshot of the current configured listen-port test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortTestStatus {
    /// Whether testing is opted in.
    pub enabled: bool,
    /// Whether a compatible endpoint is currently configured. The endpoint URL
    /// itself is intentionally not repeated in routine diagnostics.
    pub endpoint_configured: bool,
    /// TCP peer listener port tested or ready to be tested.
    pub listen_port: u16,
    pub state: PortTestState,
    /// Unix timestamp for the last endpoint result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<u64>,
    /// Unix timestamp after which a new POST may contact the endpoint again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_expires_at: Option<u64>,
    /// Bounded operator-facing detail; failures remain informational.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl PortTestStatus {
    pub fn disabled(listen_port: u16) -> Self {
        Self {
            enabled: false,
            endpoint_configured: false,
            listen_port,
            state: PortTestState::Unknown,
            checked_at: None,
            cache_expires_at: None,
            detail: Some("listen-port reachability testing is disabled".into()),
        }
    }

    pub fn unconfigured(listen_port: u16) -> Self {
        Self {
            enabled: true,
            endpoint_configured: false,
            listen_port,
            state: PortTestState::Unknown,
            checked_at: None,
            cache_expires_at: None,
            detail: Some("no port-test endpoint is configured".into()),
        }
    }

    pub fn unknown(listen_port: u16) -> Self {
        Self {
            enabled: true,
            endpoint_configured: true,
            listen_port,
            state: PortTestState::Unknown,
            checked_at: None,
            cache_expires_at: None,
            detail: Some("not tested yet".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_default_does_not_require_an_endpoint() {
        let cfg = PortTestConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn enabled_requires_http_endpoint_and_bounded_values() {
        let mut cfg = PortTestConfig {
            enabled: true,
            endpoint: Some("https://port-test.example/check".into()),
            ..PortTestConfig::default()
        };
        assert!(cfg.validate().is_ok());

        cfg.endpoint = None;
        assert!(cfg.validate().unwrap_err().to_string().contains("endpoint"));
        cfg.endpoint = Some("udp://port-test.example/check".into());
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("http or https"));
        cfg.endpoint = Some("https://port-test.example/check#fragment".into());
        assert!(cfg.validate().unwrap_err().to_string().contains("fragment"));
        cfg.endpoint = Some("https://port-test.example/check".into());
        cfg.timeout_seconds = MAX_PORT_TEST_TIMEOUT_SECONDS + 1;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("timeout_seconds"));
    }

    #[test]
    fn status_keeps_endpoint_private_and_open_is_detectable() {
        let mut status = PortTestStatus::unknown(51413);
        status.state = PortTestState::Open;
        let json = serde_json::to_value(&status).unwrap();
        assert!(status.state.is_open());
        assert_eq!(json["state"], "open");
        assert!(json.get("endpoint").is_none());
    }
}
