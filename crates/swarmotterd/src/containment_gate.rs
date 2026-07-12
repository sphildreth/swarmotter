// SPDX-License-Identifier: Apache-2.0

//! Process-wide containment gate shared by every torrent data-plane component.
//!
//! The gate is the single live authority for whether torrent traffic is
//! permitted. Every bind, connect, resolve, accept-loop iteration, UDP send,
//! tracker request, webseed request, and DHT send calls [`ContainmentGate::enforce`].
//! The control-plane listener never uses this gate.
//!
//! The gate uses atomics plus `tokio::sync::Notify`, not a new dependency. See
//! ADR-0051.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::models::network::NetworkContainmentStatus;
use swarmotter_core::net::{InterfaceInfo, InterfaceProbe, InterfaceStatus, NetworkConfig};
use tokio::sync::Notify;

/// One process-wide containment gate owned by `DaemonRuntime`.
///
/// The generation advances on every blocked-to-allowed and allowed-to-blocked
/// transition so long-lived tasks can observe cancellation via
/// [`ContainmentGate::cancelled_since`].
#[derive(Debug)]
pub struct ContainmentGate {
    allowed: AtomicBool,
    generation: AtomicU64,
    status: std::sync::Mutex<Option<NetworkContainmentStatus>>,
    detail: std::sync::Mutex<String>,
    notify: Notify,
}

impl ContainmentGate {
    pub fn new(traffic_allowed: bool) -> Arc<Self> {
        Arc::new(Self {
            allowed: AtomicBool::new(traffic_allowed),
            generation: AtomicU64::new(0),
            status: std::sync::Mutex::new(None),
            detail: std::sync::Mutex::new(String::new()),
            notify: Notify::new(),
        })
    }

    /// Permit traffic and advance generation only on blocked-to-allowed.
    pub fn allow(&self) {
        let was_allowed = self.allowed.swap(true, Ordering::SeqCst);
        if !was_allowed {
            self.generation.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut status) = self.status.lock() {
                *status = None;
            }
            if let Ok(mut detail) = self.detail.lock() {
                detail.clear();
            }
            self.notify.notify_waiters();
        }
    }

    /// Deny operations, store status/detail, advance the generation, and
    /// notify waiters.
    pub fn block(&self, status: NetworkContainmentStatus, detail: impl Into<String>) {
        let was_allowed = self.allowed.swap(false, Ordering::SeqCst);
        if let Ok(mut guard) = self.status.lock() {
            *guard = Some(status);
        }
        if let Ok(mut guard) = self.detail.lock() {
            *guard = detail.into();
        }
        if was_allowed {
            self.generation.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    /// Return `CoreError::NetworkBlocked` when denied.
    pub fn enforce(&self) -> Result<()> {
        if self.allowed.load(Ordering::SeqCst) {
            Ok(())
        } else {
            let detail = self
                .detail
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default();
            let status = self
                .status
                .lock()
                .ok()
                .and_then(|guard| *guard)
                .unwrap_or(NetworkContainmentStatus::BlockedFailClosed);
            Err(CoreError::NetworkBlocked(format!(
                "torrent data plane blocked: {}{}",
                status.as_str(),
                if detail.is_empty() {
                    String::new()
                } else {
                    format!(": {detail}")
                }
            )))
        }
    }

    /// Whether traffic is currently permitted (synchronous snapshot).
    pub fn traffic_allowed(&self) -> bool {
        self.allowed.load(Ordering::SeqCst)
    }

    /// Current blocked status, if any.
    #[allow(dead_code)]
    pub fn blocked_status(&self) -> Option<NetworkContainmentStatus> {
        self.status.lock().ok().and_then(|guard| *guard)
    }

    /// Current blocked detail, if any.
    #[allow(dead_code)]
    pub fn blocked_detail(&self) -> String {
        self.detail
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Current generation counter.
    #[allow(dead_code)]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Complete when the generation advances to a blocked state past the given
    /// starting generation. Tasks select normal work against this so connected
    /// TCP/TLS streams drop on block.
    #[allow(dead_code)]
    pub async fn cancelled_since(&self, start_generation: u64) {
        loop {
            if !self.allowed.load(Ordering::SeqCst)
                && self.generation.load(Ordering::SeqCst) != start_generation
            {
                return;
            }
            self.notify.notified().await;
        }
    }
}

/// A mutable, injectable interface probe for deterministic containment
/// testing. Tests flip the required interface healthy/missing between health
/// ticks to drive live path-loss transitions without real hardware. See
/// ADR-0051.
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct FakeInterfaceProbe {
    state: Arc<Mutex<FakeProbeState>>,
}

#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
struct FakeProbeState {
    interfaces: HashMap<String, InterfaceInfo>,
    route_valid: bool,
    dns_ok: bool,
    namespace_ok: bool,
}

impl FakeInterfaceProbe {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a named interface with the given status and addresses.
    #[allow(dead_code)]
    pub fn set_interface(&self, name: &str, status: InterfaceStatus, addrs: Vec<std::net::IpAddr>) {
        let mut state = self.state.lock().unwrap();
        state.interfaces.insert(
            name.to_string(),
            InterfaceInfo {
                name: name.to_string(),
                status,
                addresses: addrs,
            },
        );
    }

    /// Remove a named interface (simulating path loss).
    #[allow(dead_code)]
    pub fn remove_interface(&self, name: &str) {
        let mut state = self.state.lock().unwrap();
        state.interfaces.remove(name);
    }

    #[allow(dead_code)]
    pub fn set_route_valid(&self, valid: bool) {
        self.state.lock().unwrap().route_valid = valid;
    }

    #[allow(dead_code)]
    pub fn set_dns_ok(&self, ok: bool) {
        self.state.lock().unwrap().dns_ok = ok;
    }

    #[allow(dead_code)]
    pub fn set_namespace_ok(&self, ok: bool) {
        self.state.lock().unwrap().namespace_ok = ok;
    }
}

impl InterfaceProbe for FakeInterfaceProbe {
    fn list(&self) -> Vec<InterfaceInfo> {
        self.state
            .lock()
            .unwrap()
            .interfaces
            .values()
            .cloned()
            .collect()
    }
    fn find(&self, name: &str) -> Option<InterfaceInfo> {
        self.state.lock().unwrap().interfaces.get(name).cloned()
    }
    fn source_assigned(&self, addr: &str, iface: Option<&str>) -> bool {
        let state = self.state.lock().unwrap();
        if let Some(name) = iface {
            let Some(info) = state.interfaces.get(name) else {
                return false;
            };
            info.addresses.iter().any(|a| a.to_string() == addr)
        } else {
            state
                .interfaces
                .values()
                .any(|i| i.addresses.iter().any(|a| a.to_string() == addr))
        }
    }
    fn route_valid(&self, _config: &NetworkConfig) -> bool {
        self.state.lock().unwrap().route_valid
    }
    fn dns_constrained(&self, _config: &NetworkConfig) -> bool {
        self.state.lock().unwrap().dns_ok
    }
    fn namespace_available(&self, _ns: &str) -> bool {
        self.state.lock().unwrap().namespace_ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_then_block_advances_generation() {
        let gate = ContainmentGate::new(true);
        assert!(gate.traffic_allowed());
        assert_eq!(gate.generation(), 0);
        gate.allow(); // already allowed; no generation change
        assert_eq!(gate.generation(), 0);
        gate.block(NetworkContainmentStatus::InterfaceMissing, "tun0 gone");
        assert!(!gate.traffic_allowed());
        assert_eq!(gate.generation(), 1);
        assert_eq!(
            gate.blocked_status(),
            Some(NetworkContainmentStatus::InterfaceMissing)
        );
        assert!(gate.blocked_detail().contains("tun0 gone"));
        let err = gate.enforce().unwrap_err();
        assert!(err.is_network_blocked());
        gate.allow();
        assert!(gate.traffic_allowed());
        assert_eq!(gate.generation(), 2);
        assert_eq!(gate.blocked_status(), None);
        assert!(gate.blocked_detail().is_empty());
    }

    #[tokio::test]
    async fn cancelled_since_returns_on_block() {
        use std::time::Duration;
        let gate = ContainmentGate::new(true);
        let start = gate.generation();
        let g = gate.clone();
        let handle = tokio::spawn(async move {
            g.cancelled_since(start).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());
        gate.block(NetworkContainmentStatus::InterfaceDown, "down");
        handle.await.unwrap();
    }

    #[test]
    fn block_when_already_blocked_keeps_status_detail() {
        let gate = ContainmentGate::new(false);
        gate.block(NetworkContainmentStatus::RouteInvalid, "no route");
        assert_eq!(gate.generation(), 0); // was already blocked
        assert_eq!(
            gate.blocked_status(),
            Some(NetworkContainmentStatus::RouteInvalid)
        );
        assert!(gate.blocked_detail().contains("no route"));
    }
}
