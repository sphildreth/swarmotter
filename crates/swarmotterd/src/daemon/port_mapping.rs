// SPDX-License-Identifier: Apache-2.0

//! Opt-in, contained UPnP IGD and NAT-PMP listener mapping lifecycle.
//!
//! This module deliberately owns no raw socket. Discovery, NAT-PMP datagrams,
//! device-description reads, and SOAP calls all go through `NetworkBinder`,
//! which binds them to the configured torrent path and applies the live
//! containment gate. A blocked path is a visible mapping status, never a
//! reason to use a default-route fallback.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::net::NetworkBinder;
use swarmotter_core::port_mapping::{
    PortMappingConfig, PortMappingProtocol, PortMappingState, PortMappingStatus,
};
use tokio::sync::{Mutex, Notify, RwLock};

use super::*;

const PORT_MAPPING_RETRY_DELAY: Duration = Duration::from_secs(60);
const NAT_PMP_TIMEOUT: Duration = Duration::from_secs(3);
const UPNP_DISCOVERY_WINDOW: Duration = Duration::from_secs(3);
const UPNP_DESCRIPTION_TIMEOUT: Duration = Duration::from_secs(5);
const UPNP_MULTICAST: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 255, 255, 250)), 1900);
const NAT_PMP_PORT: u16 = 5351;
const MAX_SSDP_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_UPNP_DESCRIPTION_BYTES: usize = 256 * 1024;
const MAX_MAPPING_DETAIL_BYTES: usize = 512;
const DEFAULT_UPNP_SERVICE_TYPE: &str = "urn:schemas-upnp-org:service:WANIPConnection:1";

/// Runtime state is intentionally process-local. Router leases are renewed
/// from the active generation; after a restart, the daemon performs a fresh
/// contained discovery instead of trusting a stale persisted mapping.
pub(super) struct PortMappingRuntime {
    status: RwLock<PortMappingStatus>,
    active: Mutex<Option<ActivePortMapping>>,
    identity: Mutex<Option<PortMappingIdentity>>,
    operation_lock: Mutex<()>,
    wake: Notify,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PortMappingIdentity {
    config: PortMappingConfig,
    network: swarmotter_core::net::NetworkConfig,
    listen_port: u16,
}

impl PortMappingIdentity {
    fn from_config(config: &Config) -> Self {
        Self {
            config: config.port_mapping.clone(),
            network: config.network.clone(),
            listen_port: config.torrent.listen_port,
        }
    }
}

struct ActivePortMapping {
    binder: Arc<dyn NetworkBinder>,
    protocol: PortMappingProtocol,
    endpoint: ActiveMappingEndpoint,
    external_port: u16,
    renew_at: Instant,
}

enum ActiveMappingEndpoint {
    NatPmp {
        gateway: SocketAddr,
        internal_port: u16,
    },
    Upnp {
        control_url: String,
        service_type: String,
        internal_client: String,
        internal_port: u16,
    },
}

struct MappingSuccess {
    protocol: PortMappingProtocol,
    endpoint: ActiveMappingEndpoint,
    external_port: u16,
    granted_lease_seconds: u32,
    gateway: Option<String>,
    detail: String,
}

#[derive(Debug)]
struct UpnpControlEndpoint {
    control_url: String,
    service_type: String,
    gateway: Option<String>,
}

impl PortMappingRuntime {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            status: RwLock::new(if config.port_mapping.enabled {
                PortMappingStatus::pending(&config.port_mapping, config.torrent.listen_port)
            } else {
                PortMappingStatus::disabled(config.torrent.listen_port)
            }),
            active: Mutex::new(None),
            identity: Mutex::new(None),
            operation_lock: Mutex::new(()),
            wake: Notify::new(),
        }
    }

    async fn identity_changed(&self, identity: PortMappingIdentity) -> bool {
        let mut current = self.identity.lock().await;
        if current.as_ref() == Some(&identity) {
            false
        } else {
            *current = Some(identity);
            true
        }
    }

    fn wake(&self) {
        // `notify_one` retains one permit, so a configuration transition that
        // happens between loop iterations still triggers prompt reconciliation.
        self.wake.notify_one();
    }
}

impl DaemonRuntime {
    /// Current opt-in router mapping status without sending network traffic.
    /// The native API and diagnostics surfaces use this as an informational
    /// snapshot; mapping state never changes torrent scheduling.
    pub async fn port_mapping_status(&self) -> PortMappingStatus {
        let config = self.config.read().await.clone();
        if !config.port_mapping.enabled {
            return PortMappingStatus::disabled(config.torrent.listen_port);
        }
        self.port_mapping.status.read().await.clone()
    }

    /// Force an immediate contained mapping renewal/discovery attempt. This
    /// remains non-fatal: callers receive a status explaining a blocked or
    /// unavailable router rather than an error that changes torrent state.
    pub async fn refresh_port_mapping(&self) -> PortMappingStatus {
        let _ = self.port_mapping_tick_inner(true).await;
        self.port_mapping_status().await
    }

    /// Long-running mapping lifecycle. It starts with an immediate attempt,
    /// renews before the accepted lease expires, and wakes promptly for a
    /// settings or containment transition.
    pub async fn port_mapping_loop(self: Arc<Self>) {
        let mut delay = Duration::ZERO;
        loop {
            if !delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    _ = self.port_mapping.wake.notified() => {},
                }
            }
            delay = self.port_mapping_tick_inner(false).await;
        }
    }

    /// One lifecycle iteration, exposed to daemon tests so renewal and
    /// fail-closed behavior do not depend on wall-clock sleeps.
    #[cfg(test)]
    pub(crate) async fn port_mapping_tick(&self) -> Duration {
        self.port_mapping_tick_inner(false).await
    }

    /// Wake the mapping loop after a live configuration or containment
    /// transition. No traffic is issued here; the next tick validates the
    /// gate and current config before it touches a router.
    pub(super) fn notify_port_mapping_reconcile(&self) {
        self.port_mapping.wake();
    }

    /// Best-effort release used during graceful daemon shutdown. Its stored
    /// binder is the original contained generation, so teardown cannot move a
    /// mapping request onto a newly configured or default-route interface.
    pub(super) async fn release_port_mapping_on_shutdown(&self) {
        let _operation = self.port_mapping.operation_lock.lock().await;
        self.release_active_port_mapping("daemon shutdown").await;
    }

    async fn port_mapping_tick_inner(&self, force: bool) -> Duration {
        // Mapping create/renew/delete is single-flight even when an operator
        // presses refresh while the background lifecycle is waking for a
        // configuration transition.
        let _operation = self.port_mapping.operation_lock.lock().await;
        let config = self.config.read().await.clone();
        let identity = PortMappingIdentity::from_config(&config);
        if self.port_mapping.identity_changed(identity).await {
            self.release_active_port_mapping("mapping configuration changed")
                .await;
        }

        if !config.port_mapping.enabled {
            self.set_port_mapping_status(PortMappingStatus::disabled(config.torrent.listen_port))
                .await;
            return PORT_MAPPING_RETRY_DELAY;
        }

        // Runtime construction is intentionally usable by tests before a
        // configuration file is validated. Keep the non-negotiable mapping
        // boundary here too: even such a runtime must never send a router
        // request under disabled, preferred, source-only, or non-fail-closed
        // containment.
        if config.network.mode != NetworkContainmentMode::Strict
            || !config.network.fail_closed
            || config.network.required_interface.is_none()
        {
            self.set_port_mapping_status(PortMappingStatus {
                enabled: true,
                protocols: config.port_mapping.protocols.clone(),
                state: PortMappingState::Blocked,
                active_protocol: None,
                listen_port: config.torrent.listen_port,
                external_port: None,
                gateway: None,
                attempted_at: Some(unix_now()),
                lease_expires_at: None,
                detail: "router mapping requires strict fail-closed containment on one configured interface; no request was sent".into(),
            })
            .await;
            return PORT_MAPPING_RETRY_DELAY;
        }

        let health = self.network_health.read().await.clone();
        if !health.traffic_allowed || !self.containment_gate.traffic_allowed() {
            // Do not send a delete operation after the gate closed. Drop the
            // local record so recovery performs a new contained mapping; any
            // router rule naturally expires at its bounded lease time.
            self.port_mapping.active.lock().await.take();
            self.set_port_mapping_status(PortMappingStatus {
                enabled: true,
                protocols: config.port_mapping.protocols.clone(),
                state: PortMappingState::Blocked,
                active_protocol: None,
                listen_port: config.torrent.listen_port,
                external_port: None,
                gateway: None,
                attempted_at: Some(unix_now()),
                lease_expires_at: None,
                detail: "contained network path is unavailable; router mapping was not sent".into(),
            })
            .await;
            return PORT_MAPPING_RETRY_DELAY;
        }

        if !force {
            let active = self.port_mapping.active.lock().await;
            if let Some(active) = active.as_ref() {
                let now = Instant::now();
                if active.renew_at > now {
                    return active.renew_at.duration_since(now);
                }
            }
        }

        self.set_port_mapping_status(PortMappingStatus {
            enabled: true,
            protocols: config.port_mapping.protocols.clone(),
            state: PortMappingState::Pending,
            active_protocol: None,
            listen_port: config.torrent.listen_port,
            external_port: None,
            gateway: None,
            attempted_at: Some(unix_now()),
            lease_expires_at: None,
            detail: "performing contained router discovery or lease renewal".into(),
        })
        .await;

        let binder = self.make_binder().await;
        if !binder.traffic_allowed() {
            self.set_port_mapping_status(blocked_mapping_status(&config))
                .await;
            return PORT_MAPPING_RETRY_DELAY;
        }

        let mut failures = Vec::new();
        for protocol in &config.port_mapping.protocols {
            let result = match protocol {
                PortMappingProtocol::NatPmp => {
                    map_nat_pmp(binder.clone(), &config, config.torrent.listen_port).await
                }
                PortMappingProtocol::Upnp => match self.upnp_internal_client(&config) {
                    Ok(internal_client) => {
                        map_upnp(
                            binder.clone(),
                            &config,
                            config.torrent.listen_port,
                            &internal_client,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                },
            };
            match result {
                Ok(success) => {
                    let lease_seconds = success.granted_lease_seconds.max(1);
                    let renewal_seconds = lease_seconds
                        .saturating_sub(config.port_mapping.refresh_before_expiry_seconds)
                        .max(1);
                    let now = unix_now();
                    let status = PortMappingStatus {
                        enabled: true,
                        protocols: config.port_mapping.protocols.clone(),
                        state: PortMappingState::Active,
                        active_protocol: Some(success.protocol),
                        listen_port: config.torrent.listen_port,
                        external_port: Some(success.external_port),
                        gateway: success.gateway,
                        attempted_at: Some(now),
                        lease_expires_at: Some(now.saturating_add(u64::from(lease_seconds))),
                        detail: success.detail,
                    };
                    let active = ActivePortMapping {
                        binder,
                        protocol: success.protocol,
                        endpoint: success.endpoint,
                        external_port: success.external_port,
                        renew_at: Instant::now() + Duration::from_secs(u64::from(renewal_seconds)),
                    };
                    *self.port_mapping.active.lock().await = Some(active);
                    self.set_port_mapping_status(status).await;
                    // A mapping confirmation says only that a router accepted
                    // the lease. An opted-in contained reachability test can
                    // independently verify inbound access without changing
                    // this mapping result.
                    let runtime = self.clone();
                    tokio::spawn(async move {
                        let _ = runtime.run_listen_port_test(true).await;
                    });
                    return Duration::from_secs(u64::from(renewal_seconds));
                }
                Err(error) if error.is_network_blocked() => {
                    self.port_mapping.active.lock().await.take();
                    self.set_port_mapping_status(blocked_mapping_status(&config))
                        .await;
                    return PORT_MAPPING_RETRY_DELAY;
                }
                Err(error) => {
                    failures.push(format!("{}: {}", protocol.as_str(), bounded_detail(&error)))
                }
            }
        }

        self.set_port_mapping_status(PortMappingStatus {
            enabled: true,
            protocols: config.port_mapping.protocols.clone(),
            state: PortMappingState::Unavailable,
            active_protocol: None,
            listen_port: config.torrent.listen_port,
            external_port: None,
            gateway: None,
            attempted_at: Some(unix_now()),
            lease_expires_at: None,
            detail: bounded_detail_text(if failures.is_empty() {
                "no port-mapping protocol was configured".into()
            } else {
                failures.join("; ")
            }),
        })
        .await;
        PORT_MAPPING_RETRY_DELAY
    }

    async fn set_port_mapping_status(&self, status: PortMappingStatus) {
        let changed = {
            let mut current = self.port_mapping.status.write().await;
            if *current == status {
                false
            } else {
                *current = status.clone();
                true
            }
        };
        if changed {
            self.publish_event(Event::new(
                "port_mapping_changed",
                json!({ "port_mapping": status }),
            ));
        }
    }

    async fn release_active_port_mapping(&self, reason: &'static str) {
        let active = self.port_mapping.active.lock().await.take();
        let Some(active) = active else {
            return;
        };
        if let Err(error) = delete_mapping(&active).await {
            // A failed delete never justifies a different route. The active
            // lease is bounded, and status reconciliation will report a fresh
            // mapping after containment recovers.
            tracing::debug!(%error, protocol = active.protocol.as_str(), reason, "contained router mapping deletion was not confirmed");
        }
    }

    fn upnp_internal_client(&self, config: &Config) -> Result<String> {
        if let Some(source) = config.network.required_source_ipv4.as_deref() {
            return source
                .parse::<Ipv4Addr>()
                .map(|ip| ip.to_string())
                .map_err(|error| {
                    CoreError::InvalidConfig(format!(
                        "network.required_source_ipv4 cannot be used for UPnP mapping: {error}"
                    ))
                });
        }
        let interface = config.network.required_interface.as_deref().ok_or_else(|| {
            CoreError::InvalidConfig(
                "port_mapping.enabled requires network.required_interface to select UPnP internal client"
                    .into(),
            )
        })?;
        self.interface_probe
            .find(interface)
            .and_then(|info| {
                info.addresses
                    .into_iter()
                    .find_map(|address| match address {
                        IpAddr::V4(address) if !address.is_unspecified() => {
                            Some(address.to_string())
                        }
                        _ => None,
                    })
            })
            .ok_or_else(|| {
                CoreError::InvalidConfig(format!(
                    "contained interface {interface} has no IPv4 address for UPnP internal client"
                ))
            })
    }
}

fn blocked_mapping_status(config: &Config) -> PortMappingStatus {
    PortMappingStatus {
        enabled: true,
        protocols: config.port_mapping.protocols.clone(),
        state: PortMappingState::Blocked,
        active_protocol: None,
        listen_port: config.torrent.listen_port,
        external_port: None,
        gateway: None,
        attempted_at: Some(unix_now()),
        lease_expires_at: None,
        detail: "contained network path is unavailable; router mapping was not sent".into(),
    }
}

async fn map_nat_pmp(
    binder: Arc<dyn NetworkBinder>,
    config: &Config,
    listen_port: u16,
) -> Result<MappingSuccess> {
    let gateway = nat_pmp_gateway(config)?;
    let response = nat_pmp_map_tcp(
        binder.as_ref(),
        gateway,
        listen_port,
        listen_port,
        config.port_mapping.lease_seconds,
    )
    .await?;
    Ok(MappingSuccess {
        protocol: PortMappingProtocol::NatPmp,
        endpoint: ActiveMappingEndpoint::NatPmp {
            gateway,
            internal_port: listen_port,
        },
        external_port: response.external_port,
        granted_lease_seconds: response.lifetime_seconds,
        gateway: Some(gateway.ip().to_string()),
        detail: format!(
            "NAT-PMP mapped TCP listen port {listen_port} to external port {}",
            response.external_port
        ),
    })
}

async fn map_upnp(
    binder: Arc<dyn NetworkBinder>,
    config: &Config,
    listen_port: u16,
    internal_client: &str,
) -> Result<MappingSuccess> {
    let control = discover_upnp_control(binder.as_ref(), &config.port_mapping).await?;
    upnp_add_port_mapping(
        binder.as_ref(),
        &control.control_url,
        &control.service_type,
        internal_client,
        listen_port,
        config.port_mapping.lease_seconds,
    )
    .await?;
    Ok(MappingSuccess {
        protocol: PortMappingProtocol::Upnp,
        endpoint: ActiveMappingEndpoint::Upnp {
            control_url: control.control_url,
            service_type: control.service_type,
            internal_client: internal_client.to_string(),
            internal_port: listen_port,
        },
        external_port: listen_port,
        granted_lease_seconds: config.port_mapping.lease_seconds,
        gateway: control.gateway,
        detail: format!("UPnP IGD mapped TCP listen port {listen_port}"),
    })
}

async fn delete_mapping(active: &ActivePortMapping) -> Result<()> {
    match &active.endpoint {
        ActiveMappingEndpoint::NatPmp {
            gateway,
            internal_port,
        } => {
            // The external port is part of the NAT-PMP delete tuple. A
            // best-effort send is sufficient during shutdown; waiting for a
            // router reply would delay daemon termination without improving
            // containment correctness.
            let socket = active.binder.udp_socket_for(Some(*gateway)).await?;
            let request = nat_pmp_tcp_request(*internal_port, active.external_port, 0);
            socket.send_to(*gateway, &request).await
        }
        ActiveMappingEndpoint::Upnp {
            control_url,
            service_type,
            internal_client,
            internal_port,
        } => {
            let body = upnp_delete_body(service_type, internal_client, *internal_port);
            let action = format!("{service_type}#DeletePortMapping");
            active
                .binder
                .http_post_upnp_soap(control_url, &action, body.as_bytes())
                .await
                .map(|_| ())
        }
    }
}

fn nat_pmp_gateway(config: &Config) -> Result<SocketAddr> {
    let gateway = match config.port_mapping.nat_pmp_gateway.as_deref() {
        Some(gateway) => gateway.trim().parse::<Ipv4Addr>().map_err(|error| {
            CoreError::InvalidConfig(format!(
                "port_mapping.nat_pmp_gateway is not a valid IPv4 address: {error}"
            ))
        })?,
        None => discover_nat_pmp_gateway_for_interface(
            config
                .network
                .required_interface
                .as_deref()
                .ok_or_else(|| {
                    CoreError::InvalidConfig(
                        "port_mapping.enabled requires network.required_interface".into(),
                    )
                })?,
        )?,
    };
    Ok(SocketAddr::new(IpAddr::V4(gateway), NAT_PMP_PORT))
}

#[cfg(target_os = "linux")]
fn discover_nat_pmp_gateway_for_interface(interface: &str) -> Result<Ipv4Addr> {
    let routes = std::fs::read_to_string("/proc/net/route").map_err(|error| {
        CoreError::Internal(format!(
            "read contained interface route table for NAT-PMP discovery: {error}"
        ))
    })?;
    for line in routes.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let Some(route_interface) = fields.next() else {
            continue;
        };
        let Some(destination) = fields.next() else {
            continue;
        };
        let Some(gateway) = fields.next() else {
            continue;
        };
        if route_interface != interface || destination != "00000000" || gateway == "00000000" {
            continue;
        }
        let gateway = u32::from_str_radix(gateway, 16).map_err(|error| {
            CoreError::Internal(format!("parse NAT-PMP gateway route: {error}"))
        })?;
        return Ok(Ipv4Addr::from(gateway.to_le_bytes()));
    }
    Err(CoreError::InvalidConfig(format!(
        "no IPv4 default gateway is configured for contained interface {interface}; set port_mapping.nat_pmp_gateway explicitly or use UPnP"
    )))
}

#[cfg(not(target_os = "linux"))]
fn discover_nat_pmp_gateway_for_interface(interface: &str) -> Result<Ipv4Addr> {
    Err(CoreError::InvalidConfig(format!(
        "automatic NAT-PMP gateway discovery is unavailable for contained interface {interface}; set port_mapping.nat_pmp_gateway explicitly"
    )))
}

struct NatPmpMappingResponse {
    external_port: u16,
    lifetime_seconds: u32,
}

async fn nat_pmp_map_tcp(
    binder: &dyn NetworkBinder,
    gateway: SocketAddr,
    internal_port: u16,
    requested_external_port: u16,
    lifetime_seconds: u32,
) -> Result<NatPmpMappingResponse> {
    let socket = binder.udp_socket_for(Some(gateway)).await?;
    let request = nat_pmp_tcp_request(internal_port, requested_external_port, lifetime_seconds);
    socket.send_to(gateway, &request).await?;
    let mut response = [0u8; 32];
    let (source, length) = tokio::time::timeout(NAT_PMP_TIMEOUT, socket.recv_from(&mut response))
        .await
        .map_err(|_| {
            CoreError::Internal("NAT-PMP gateway did not respond before timeout".into())
        })??;
    if source.ip() != gateway.ip() || source.port() != NAT_PMP_PORT {
        return Err(CoreError::HttpProtocol(
            "NAT-PMP response was not sent by the configured gateway".into(),
        ));
    }
    parse_nat_pmp_mapping_response(&response[..length], internal_port)
}

fn nat_pmp_tcp_request(
    internal_port: u16,
    requested_external_port: u16,
    lifetime_seconds: u32,
) -> [u8; 12] {
    let mut request = [0u8; 12];
    request[1] = 2; // TCP map request
    request[4..6].copy_from_slice(&internal_port.to_be_bytes());
    request[6..8].copy_from_slice(&requested_external_port.to_be_bytes());
    request[8..12].copy_from_slice(&lifetime_seconds.to_be_bytes());
    request
}

fn parse_nat_pmp_mapping_response(
    response: &[u8],
    expected_internal_port: u16,
) -> Result<NatPmpMappingResponse> {
    if response.len() < 16 {
        return Err(CoreError::HttpProtocol(
            "NAT-PMP mapping response is shorter than 16 bytes".into(),
        ));
    }
    if response[0] != 0 || response[1] != 130 {
        return Err(CoreError::HttpProtocol(
            "NAT-PMP mapping response has an unexpected version or opcode".into(),
        ));
    }
    let result = u16::from_be_bytes([response[2], response[3]]);
    if result != 0 {
        return Err(CoreError::Internal(format!(
            "NAT-PMP gateway rejected TCP mapping with result code {result}"
        )));
    }
    let internal_port = u16::from_be_bytes([response[8], response[9]]);
    if internal_port != expected_internal_port {
        return Err(CoreError::HttpProtocol(format!(
            "NAT-PMP mapping response returned internal port {internal_port}, expected {expected_internal_port}"
        )));
    }
    let external_port = u16::from_be_bytes([response[10], response[11]]);
    if external_port == 0 {
        return Err(CoreError::HttpProtocol(
            "NAT-PMP mapping response returned external port 0".into(),
        ));
    }
    let lifetime_seconds =
        u32::from_be_bytes([response[12], response[13], response[14], response[15]]);
    if lifetime_seconds == 0 {
        return Err(CoreError::HttpProtocol(
            "NAT-PMP gateway returned a zero-length mapping lease".into(),
        ));
    }
    Ok(NatPmpMappingResponse {
        external_port,
        lifetime_seconds,
    })
}

async fn discover_upnp_control(
    binder: &dyn NetworkBinder,
    config: &PortMappingConfig,
) -> Result<UpnpControlEndpoint> {
    if let Some(control_url) = config.upnp_service_url.as_deref() {
        let url = url::Url::parse(control_url).map_err(|error| {
            CoreError::InvalidConfig(format!("invalid configured UPnP control URL: {error}"))
        })?;
        return Ok(UpnpControlEndpoint {
            control_url: url.to_string(),
            service_type: DEFAULT_UPNP_SERVICE_TYPE.into(),
            gateway: url.host_str().map(ToOwned::to_owned),
        });
    }

    let socket = binder.udp_socket_for(Some(UPNP_MULTICAST)).await?;
    socket
        .send_to(UPNP_MULTICAST, SSDP_DISCOVERY_REQUEST.as_bytes())
        .await?;
    let deadline = tokio::time::Instant::now() + UPNP_DISCOVERY_WINDOW;
    let mut errors = Vec::new();
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let mut response = [0u8; MAX_SSDP_RESPONSE_BYTES];
        let remaining = deadline.saturating_duration_since(now);
        let received = tokio::time::timeout(remaining, socket.recv_from(&mut response)).await;
        let (source, length) = match received {
            Err(_) => break,
            Ok(Err(error)) => return Err(error),
            Ok(Ok(value)) => value,
        };
        let location = match parse_ssdp_location(&response[..length], source.ip()) {
            Ok(location) => location,
            Err(error) => {
                errors.push(bounded_detail(&error));
                continue;
            }
        };
        let description = match tokio::time::timeout(
            UPNP_DESCRIPTION_TIMEOUT,
            binder.http_get_upnp_description(&location),
        )
        .await
        {
            Err(_) => {
                errors.push("UPnP device description request timed out".into());
                continue;
            }
            Ok(Err(error)) => {
                if error.is_network_blocked() {
                    return Err(error);
                }
                errors.push(bounded_detail(&error));
                continue;
            }
            Ok(Ok(response)) => response,
        };
        match upnp_control_from_description(&location, &description.body, source.ip()) {
            Ok(mut control) => {
                control.gateway = Some(source.ip().to_string());
                return Ok(control);
            }
            Err(error) => errors.push(bounded_detail(&error)),
        }
    }
    Err(CoreError::Internal(bounded_detail_text(
        if errors.is_empty() {
            "no contained UPnP IGD response was received".into()
        } else {
            format!(
                "no usable contained UPnP IGD response: {}",
                errors.join("; ")
            )
        },
    )))
}

const SSDP_DISCOVERY_REQUEST: &str = "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 1\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n";

fn parse_ssdp_location(response: &[u8], expected_source: IpAddr) -> Result<String> {
    if response.len() > MAX_SSDP_RESPONSE_BYTES {
        return Err(CoreError::HttpProtocol(
            "UPnP SSDP response exceeded its bounded size".into(),
        ));
    }
    let response = std::str::from_utf8(response)
        .map_err(|_| CoreError::HttpProtocol("UPnP SSDP response was not UTF-8".into()))?;
    let mut location = None;
    for line in response.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("location")
            && location.replace(value.trim().to_string()).is_some()
        {
            return Err(CoreError::HttpProtocol(
                "UPnP SSDP response contains multiple LOCATION headers".into(),
            ));
        }
    }
    let location = location.ok_or_else(|| {
        CoreError::HttpProtocol("UPnP SSDP response omitted a LOCATION header".into())
    })?;
    let parsed = url::Url::parse(&location).map_err(|error| {
        CoreError::HttpProtocol(format!("UPnP SSDP LOCATION is invalid: {error}"))
    })?;
    if parsed.scheme() != "http" || parsed.host_str().is_none() {
        return Err(CoreError::HttpProtocol(
            "UPnP SSDP LOCATION must be an http URL with a host".into(),
        ));
    }
    // SSDP advertisements are received from an unauthenticated multicast
    // responder. Accepting a hostname or a different literal address here
    // would turn discovery into a contained-but-arbitrary HTTP client. Only
    // the responder's literal address is eligible for descriptor retrieval.
    let location_ip = match parsed.host() {
        Some(url::Host::Ipv4(address)) => IpAddr::V4(address),
        Some(url::Host::Ipv6(address)) => IpAddr::V6(address),
        Some(url::Host::Domain(_)) | None => {
            return Err(CoreError::HttpProtocol(
                "UPnP SSDP LOCATION must use a literal IP matching the SSDP responder".into(),
            ));
        }
    };
    if location_ip != expected_source {
        return Err(CoreError::HttpProtocol(
            "UPnP SSDP LOCATION host did not match the SSDP responder".into(),
        ));
    }
    Ok(parsed.to_string())
}

fn upnp_control_from_description(
    location: &str,
    body: &[u8],
    expected_source: IpAddr,
) -> Result<UpnpControlEndpoint> {
    if body.len() > MAX_UPNP_DESCRIPTION_BYTES {
        return Err(CoreError::HttpProtocol(
            "UPnP device description exceeded its bounded size".into(),
        ));
    }
    let body = std::str::from_utf8(body)
        .map_err(|_| CoreError::HttpProtocol("UPnP device description was not UTF-8".into()))?;
    let mut remainder = body;
    while let Some(start) = remainder.find("<service>") {
        remainder = &remainder[start + "<service>".len()..];
        let Some(end) = remainder.find("</service>") else {
            break;
        };
        let service = &remainder[..end];
        remainder = &remainder[end + "</service>".len()..];
        let Some(service_type) = xml_tag_value(service, "serviceType") else {
            continue;
        };
        if !service_type.contains("WANIPConnection") && !service_type.contains("WANPPPConnection") {
            continue;
        }
        let control = xml_tag_value(service, "controlURL")
            .ok_or_else(|| CoreError::HttpProtocol("UPnP WAN service omitted controlURL".into()))?;
        let base = url::Url::parse(location).map_err(|error| {
            CoreError::HttpProtocol(format!("UPnP device description URL is invalid: {error}"))
        })?;
        let control = base.join(control).map_err(|error| {
            CoreError::HttpProtocol(format!("UPnP control URL is invalid: {error}"))
        })?;
        if control.scheme() != "http" || control.host_str().is_none() {
            return Err(CoreError::HttpProtocol(
                "UPnP controlURL must resolve to an http URL with a host".into(),
            ));
        }
        // A device description is untrusted input too. Its control endpoint
        // is intentionally restricted to the same literal address that sent
        // the SSDP response; relative controls naturally satisfy this.
        let control_ip = match control.host() {
            Some(url::Host::Ipv4(address)) => IpAddr::V4(address),
            Some(url::Host::Ipv6(address)) => IpAddr::V6(address),
            Some(url::Host::Domain(_)) | None => {
                return Err(CoreError::HttpProtocol(
                    "UPnP controlURL must remain a literal IP on the discovered gateway".into(),
                ));
            }
        };
        if control_ip != expected_source {
            return Err(CoreError::HttpProtocol(
                "UPnP controlURL host did not match the discovered gateway".into(),
            ));
        }
        if control.scheme() != base.scheme()
            || control.host() != base.host()
            || control.port_or_known_default() != base.port_or_known_default()
        {
            return Err(CoreError::HttpProtocol(
                "UPnP controlURL did not remain on the discovered descriptor origin".into(),
            ));
        }
        return Ok(UpnpControlEndpoint {
            control_url: control.to_string(),
            service_type: service_type.to_string(),
            gateway: None,
        });
    }
    Err(CoreError::Internal(
        "UPnP device description did not advertise WANIPConnection or WANPPPConnection".into(),
    ))
}

fn xml_tag_value<'a>(source: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = source.find(&open)? + open.len();
    let end = source[start..].find(&close)? + start;
    Some(source[start..end].trim())
}

async fn upnp_add_port_mapping(
    binder: &dyn NetworkBinder,
    control_url: &str,
    service_type: &str,
    internal_client: &str,
    listen_port: u16,
    lease_seconds: u32,
) -> Result<()> {
    let action = format!("{service_type}#AddPortMapping");
    let body = upnp_add_body(service_type, internal_client, listen_port, lease_seconds);
    binder
        .http_post_upnp_soap(control_url, &action, body.as_bytes())
        .await
        .map(|_| ())
}

fn upnp_add_body(
    service_type: &str,
    internal_client: &str,
    listen_port: u16,
    lease_seconds: u32,
) -> String {
    format!(
        "<?xml version=\"1.0\"?>\r\n<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:AddPortMapping xmlns:u=\"{service_type}\"><NewRemoteHost></NewRemoteHost><NewExternalPort>{listen_port}</NewExternalPort><NewProtocol>TCP</NewProtocol><NewInternalPort>{listen_port}</NewInternalPort><NewInternalClient>{internal_client}</NewInternalClient><NewEnabled>1</NewEnabled><NewPortMappingDescription>SwarmOtter</NewPortMappingDescription><NewLeaseDuration>{lease_seconds}</NewLeaseDuration></u:AddPortMapping></s:Body></s:Envelope>"
    )
}

fn upnp_delete_body(service_type: &str, internal_client: &str, listen_port: u16) -> String {
    format!(
        "<?xml version=\"1.0\"?>\r\n<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:DeletePortMapping xmlns:u=\"{service_type}\"><NewRemoteHost></NewRemoteHost><NewExternalPort>{listen_port}</NewExternalPort><NewProtocol>TCP</NewProtocol><NewInternalClient>{internal_client}</NewInternalClient></u:DeletePortMapping></s:Body></s:Envelope>"
    )
}

fn bounded_detail(error: &CoreError) -> String {
    bounded_detail_text(error.to_string())
}

fn bounded_detail_text(value: String) -> String {
    value.chars().take(MAX_MAPPING_DETAIL_BYTES).collect()
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
    use async_trait::async_trait;
    use swarmotter_core::net::{ContainedUdpSocket, PeerListener};

    type SentNatPmpPackets = Arc<Mutex<Vec<(SocketAddr, Vec<u8>)>>>;

    #[derive(Clone, Default)]
    struct NatPmpBinder {
        requested: Arc<Mutex<Vec<Option<SocketAddr>>>>,
        sent: SentNatPmpPackets,
    }

    struct NatPmpSocket {
        sent: SentNatPmpPackets,
    }

    #[async_trait]
    impl ContainedUdpSocket for NatPmpSocket {
        async fn send_to(&self, addr: SocketAddr, data: &[u8]) -> Result<()> {
            self.sent.lock().await.push((addr, data.to_vec()));
            Ok(())
        }

        async fn recv_from(&self, buffer: &mut [u8]) -> Result<(SocketAddr, usize)> {
            let mut response = [0u8; 16];
            response[1] = 130;
            response[8..10].copy_from_slice(&51413u16.to_be_bytes());
            response[10..12].copy_from_slice(&62000u16.to_be_bytes());
            response[12..16].copy_from_slice(&3600u32.to_be_bytes());
            buffer[..response.len()].copy_from_slice(&response);
            Ok((
                SocketAddr::from(([192, 168, 1, 1], NAT_PMP_PORT)),
                response.len(),
            ))
        }

        fn local_addr(&self) -> Result<SocketAddr> {
            Ok(SocketAddr::from(([192, 168, 1, 2], 40000)))
        }
    }

    #[async_trait]
    impl NetworkBinder for NatPmpBinder {
        async fn connect_peer(&self, _addr: SocketAddr) -> Result<tokio::net::TcpStream> {
            Err(CoreError::Internal(
                "unused NAT-PMP test TCP connection".into(),
            ))
        }

        async fn resolve_host(&self, _host: &str, _port: u16) -> Result<SocketAddr> {
            Err(CoreError::Internal("unused NAT-PMP test resolver".into()))
        }

        async fn udp_socket(&self) -> Result<Box<dyn ContainedUdpSocket>> {
            self.udp_socket_for(None).await
        }

        async fn udp_socket_for(
            &self,
            remote: Option<SocketAddr>,
        ) -> Result<Box<dyn ContainedUdpSocket>> {
            self.requested.lock().await.push(remote);
            Ok(Box::new(NatPmpSocket {
                sent: self.sent.clone(),
            }))
        }

        async fn bind_peer_listener(&self, _port: u16) -> Result<Box<dyn PeerListener>> {
            Err(CoreError::Internal("unused NAT-PMP test listener".into()))
        }

        fn traffic_allowed(&self) -> bool {
            true
        }
    }

    #[test]
    fn nat_pmp_wire_messages_require_matching_confirmed_ports() {
        let request = nat_pmp_tcp_request(51413, 51413, 3600);
        assert_eq!(request[1], 2);
        assert_eq!(u16::from_be_bytes([request[4], request[5]]), 51413);
        let mut response = [0u8; 16];
        response[1] = 130;
        response[8..10].copy_from_slice(&51413u16.to_be_bytes());
        response[10..12].copy_from_slice(&62000u16.to_be_bytes());
        response[12..16].copy_from_slice(&3600u32.to_be_bytes());
        let parsed = parse_nat_pmp_mapping_response(&response, 51413).unwrap();
        assert_eq!(parsed.external_port, 62000);
        assert_eq!(parsed.lifetime_seconds, 3600);

        response[8..10].copy_from_slice(&51414u16.to_be_bytes());
        assert!(parse_nat_pmp_mapping_response(&response, 51413).is_err());
    }

    #[tokio::test]
    async fn nat_pmp_exchange_uses_only_the_contained_udp_binder() {
        let binder = NatPmpBinder::default();
        let gateway = SocketAddr::from(([192, 168, 1, 1], NAT_PMP_PORT));
        let response = nat_pmp_map_tcp(&binder, gateway, 51413, 51413, 3600)
            .await
            .unwrap();
        assert_eq!(response.external_port, 62000);
        assert_eq!(response.lifetime_seconds, 3600);
        assert_eq!(binder.requested.lock().await.as_slice(), &[Some(gateway)]);
        let sent = binder.sent.lock().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, gateway);
        assert_eq!(sent[0].1, nat_pmp_tcp_request(51413, 51413, 3600));
    }

    #[test]
    fn upnp_description_selects_wan_service_and_resolves_relative_control_url() {
        let description = br#"<root><device><serviceList><service><serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType><controlURL>/upnp/control/WANIPConn1</controlURL></service></serviceList></device></root>"#;
        let control = upnp_control_from_description(
            "http://192.168.1.1:49000/root.xml",
            description,
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        )
        .unwrap();
        assert_eq!(
            control.control_url,
            "http://192.168.1.1:49000/upnp/control/WANIPConn1"
        );
        assert!(control.service_type.contains("WANIPConnection"));
    }

    #[test]
    fn ssdp_location_and_soap_body_are_bounded_and_protocol_specific() {
        let location = parse_ssdp_location(
            b"HTTP/1.1 200 OK\r\nLOCATION: http://192.168.1.1/root.xml\r\n\r\n",
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        )
        .unwrap();
        assert_eq!(location, "http://192.168.1.1/root.xml");
        let body = upnp_add_body(DEFAULT_UPNP_SERVICE_TYPE, "192.168.1.2", 51413, 3600);
        assert!(body.contains("<NewProtocol>TCP</NewProtocol>"));
        assert!(body.contains("<NewInternalClient>192.168.1.2</NewInternalClient>"));
        assert!(body.contains("<NewLeaseDuration>3600</NewLeaseDuration>"));
    }

    #[test]
    fn ssdp_discovery_rejects_location_and_control_url_ssrf_targets() {
        let response = b"HTTP/1.1 200 OK\r\nLOCATION: http://198.51.100.2/root.xml\r\n\r\n";
        let error =
            parse_ssdp_location(response, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))).unwrap_err();
        assert!(error.to_string().contains("did not match"));

        let description = br#"<root><device><serviceList><service><serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType><controlURL>http://198.51.100.2/control</controlURL></service></serviceList></device></root>"#;
        let error = upnp_control_from_description(
            "http://192.168.1.1/root.xml",
            description,
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("did not match"));
    }

    #[test]
    fn mapping_status_is_blocked_without_a_contained_path() {
        let mut config = Config::default();
        config.network.mode = NetworkContainmentMode::Disabled;
        config.port_mapping.enabled = true;
        let status = blocked_mapping_status(&config);
        assert_eq!(status.state, PortMappingState::Blocked);
        assert!(status.detail.contains("not sent"));
    }

    #[tokio::test]
    async fn runtime_refuses_mapping_before_any_uncontained_binder_operation() {
        let mut config = Config::default();
        config.network.mode = NetworkContainmentMode::Disabled;
        config.port_mapping.enabled = true;
        let runtime = DaemonRuntime::new(
            config,
            NetworkHealth {
                mode: NetworkContainmentMode::Disabled,
                status: NetworkContainmentStatus::Disabled,
                required_interface: None,
                required_source_ipv4: None,
                required_source_ipv6: None,
                allow_ipv6: true,
                fail_closed: false,
                detail: "containment is disabled for this test".into(),
                traffic_allowed: true,
            },
        );
        let _ = runtime.port_mapping_tick().await;
        let status = runtime.port_mapping_status().await;
        assert_eq!(status.state, PortMappingState::Blocked);
        assert!(status.detail.contains("no request was sent"));
    }
}
