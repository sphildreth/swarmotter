// SPDX-License-Identifier: Apache-2.0

//! Process-wide and per-torrent peer-session permits.
//!
//! A permit is acquired before a peer transport is opened and is held for the
//! complete connect, handshake, and session lifetime. Trackers, webseeds, DHT,
//! and DNS do not use this budget. See ADR-0053.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use swarmotter_core::error::{CoreError, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Operational per-torrent cap used when `max_peers_per_torrent = 0`.
pub const DEFAULT_PER_TORRENT_PEER_LIMIT: usize = 64;

/// Snapshot exposed through global scheduler diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerPermitSnapshot {
    pub limit: usize,
    pub in_use: usize,
    pub available: Option<usize>,
    pub denied: u64,
}

/// One peer-session budget. A zero limit is unlimited but still counts live
/// guards so diagnostics report observed session concurrency.
#[derive(Debug)]
pub struct PeerPermitPool {
    limit: usize,
    semaphore: Option<Arc<Semaphore>>,
    in_use: Arc<AtomicUsize>,
    denied: Arc<AtomicU64>,
}

impl PeerPermitPool {
    pub fn new(limit: usize, denied: Arc<AtomicU64>) -> Result<Arc<Self>> {
        if limit > Semaphore::MAX_PERMITS {
            return Err(CoreError::InvalidConfig(format!(
                "peer limit {limit} exceeds runtime maximum {}",
                Semaphore::MAX_PERMITS
            )));
        }
        Ok(Arc::new(Self {
            limit,
            semaphore: (limit > 0).then(|| Arc::new(Semaphore::new(limit))),
            in_use: Arc::new(AtomicUsize::new(0)),
            denied,
        }))
    }

    pub fn unlimited() -> Arc<Self> {
        Arc::new(Self {
            limit: 0,
            semaphore: None,
            in_use: Arc::new(AtomicUsize::new(0)),
            denied: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Fail-closed constructor used only when a caller bypassed configuration
    /// validation before building a runtime. No acquisition can succeed.
    pub fn invalid_fail_closed(limit: usize, denied: Arc<AtomicU64>) -> Arc<Self> {
        let semaphore = Arc::new(Semaphore::new(0));
        semaphore.close();
        Arc::new(Self {
            limit,
            semaphore: Some(semaphore),
            in_use: Arc::new(AtomicUsize::new(0)),
            denied,
        })
    }

    /// Wait for capacity. Cancellation while waiting owns no permit and does
    /// not change the observed in-use count.
    pub async fn acquire(self: &Arc<Self>) -> Result<PeerPermit> {
        let owned = match &self.semaphore {
            Some(semaphore) => Some(
                semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| CoreError::Internal("peer permit pool closed".into()))?,
            ),
            None => None,
        };
        self.in_use.fetch_add(1, Ordering::AcqRel);
        Ok(PeerPermit {
            _owned: owned,
            in_use: self.in_use.clone(),
        })
    }

    /// Nonblocking acquisition for an already-accepted inbound socket. A
    /// denial closes the caller's socket before peer handshake and increments
    /// the shared process-wide rejection counter.
    pub fn try_acquire(self: &Arc<Self>) -> Option<PeerPermit> {
        let owned = match &self.semaphore {
            Some(semaphore) => match semaphore.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                    self.denied.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            },
            None => None,
        };
        self.in_use.fetch_add(1, Ordering::AcqRel);
        Some(PeerPermit {
            _owned: owned,
            in_use: self.in_use.clone(),
        })
    }

    pub fn snapshot(&self) -> PeerPermitSnapshot {
        let in_use = self.in_use.load(Ordering::Acquire);
        PeerPermitSnapshot {
            limit: self.limit,
            in_use,
            // Report a coherent tuple from one counter observation. Reading
            // Semaphore::available_permits separately can straddle the small
            // acquire/drop window where semaphore ownership and the observed
            // session counter are being updated.
            available: self.semaphore.as_ref().map(|semaphore| {
                if semaphore.is_closed() {
                    0
                } else {
                    self.limit.saturating_sub(in_use)
                }
            }),
            denied: self.denied.load(Ordering::Relaxed),
        }
    }
}

/// RAII lifetime guard for one pool. It releases both the semaphore capacity
/// and observed in-use count on success, error, cancellation, EOF, or panic.
#[derive(Debug)]
pub struct PeerPermit {
    _owned: Option<OwnedSemaphorePermit>,
    in_use: Arc<AtomicUsize>,
}

impl Drop for PeerPermit {
    fn drop(&mut self) {
        let previous = self.in_use.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "peer permit in-use counter underflow");
    }
}

/// The two budgets every peer session must satisfy. Outbound work waits for a
/// per-torrent permit and then a process-wide permit before opening a socket.
#[derive(Debug, Clone)]
pub struct PeerSessionBudget {
    global: Arc<PeerPermitPool>,
    torrent: Arc<PeerPermitPool>,
}

impl PeerSessionBudget {
    pub fn new(global: Arc<PeerPermitPool>, torrent: Arc<PeerPermitPool>) -> Self {
        Self { global, torrent }
    }

    pub fn unlimited() -> Self {
        Self {
            global: PeerPermitPool::unlimited(),
            torrent: PeerPermitPool::unlimited(),
        }
    }

    pub async fn acquire_outbound(&self) -> Result<PeerSessionPermit> {
        // Per-torrent first avoids holding scarce global capacity while a
        // single torrent waits on its own smaller budget.
        let torrent = self.torrent.acquire().await?;
        let global = self.global.acquire().await?;
        Ok(PeerSessionPermit {
            _global: global,
            _torrent: torrent,
        })
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn try_acquire_global_inbound(&self) -> Option<PeerPermit> {
        self.global.try_acquire()
    }

    pub fn try_acquire_torrent_inbound(&self) -> Option<PeerPermit> {
        self.torrent.try_acquire()
    }
}

/// Combined outbound guard. Field drop order is irrelevant because both are
/// released at the same session boundary.
#[derive(Debug)]
pub struct PeerSessionPermit {
    _global: PeerPermit,
    _torrent: PeerPermit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn counter() -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(0))
    }

    #[tokio::test]
    async fn bounded_pool_never_exceeds_limit_and_releases_on_drop() {
        let pool = PeerPermitPool::new(2, counter()).unwrap();
        let first = pool.acquire().await.unwrap();
        let second = pool.acquire().await.unwrap();
        assert_eq!(pool.snapshot().in_use, 2);
        assert_eq!(pool.snapshot().available, Some(0));

        let waiting_pool = pool.clone();
        let waiting = tokio::spawn(async move { waiting_pool.acquire().await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        let third = waiting.await.unwrap();
        assert_eq!(pool.snapshot().in_use, 2);
        drop(second);
        drop(third);
        assert_eq!(pool.snapshot().in_use, 0);
        assert_eq!(pool.snapshot().available, Some(2));
    }

    #[tokio::test]
    async fn unlimited_pool_reports_observed_in_use_and_null_available() {
        let pool = PeerPermitPool::new(0, counter()).unwrap();
        let first = pool.acquire().await.unwrap();
        let second = pool.acquire().await.unwrap();
        assert_eq!(pool.snapshot().limit, 0);
        assert_eq!(pool.snapshot().available, None);
        assert_eq!(pool.snapshot().in_use, 2);
        drop((first, second));
        assert_eq!(pool.snapshot().in_use, 0);
    }

    #[tokio::test]
    async fn inbound_denial_and_cancelled_wait_do_not_leak() {
        let denied = counter();
        let pool = PeerPermitPool::new(1, denied.clone()).unwrap();
        let held = pool.try_acquire().unwrap();
        assert!(pool.try_acquire().is_none());
        assert_eq!(denied.load(Ordering::Relaxed), 1);

        let waiting_pool = pool.clone();
        let waiting = tokio::spawn(async move { waiting_pool.acquire().await });
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(pool.snapshot().in_use, 1);
        drop(held);
        assert_eq!(pool.snapshot().in_use, 0);
    }

    #[tokio::test]
    async fn task_panic_releases_raii_permit() {
        let pool = PeerPermitPool::new(1, counter()).unwrap();
        let task_pool = pool.clone();
        let task = tokio::spawn(async move {
            let _permit = task_pool.acquire().await.unwrap();
            panic!("injected peer session panic");
        });
        assert!(task.await.unwrap_err().is_panic());
        tokio::time::timeout(Duration::from_secs(1), pool.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pool.snapshot().in_use, 0);
    }

    #[tokio::test]
    async fn combined_budget_requires_both_global_and_torrent_capacity() {
        let denied = counter();
        let global = PeerPermitPool::new(2, denied.clone()).unwrap();
        let torrent = PeerPermitPool::new(1, denied).unwrap();
        let budget = PeerSessionBudget::new(global.clone(), torrent.clone());
        let held = budget.acquire_outbound().await.unwrap();
        let waiting_budget = budget.clone();
        let waiting = tokio::spawn(async move { waiting_budget.acquire_outbound().await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        assert_eq!(global.snapshot().in_use, 1);
        assert_eq!(torrent.snapshot().in_use, 1);
        drop(held);
        drop(waiting.await.unwrap());
        assert_eq!(global.snapshot().in_use, 0);
        assert_eq!(torrent.snapshot().in_use, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_churn_snapshots_are_always_internally_coherent() {
        let pool = PeerPermitPool::new(7, counter()).unwrap();
        let mut workers = Vec::new();
        for _ in 0..32 {
            let pool = pool.clone();
            workers.push(tokio::spawn(async move {
                for _ in 0..250 {
                    let permit = pool.acquire().await.unwrap();
                    tokio::task::yield_now().await;
                    drop(permit);
                }
            }));
        }
        while workers.iter().any(|worker| !worker.is_finished()) {
            let snapshot = pool.snapshot();
            assert!(snapshot.in_use <= snapshot.limit, "{snapshot:?}");
            assert_eq!(
                snapshot.available,
                Some(snapshot.limit - snapshot.in_use),
                "{snapshot:?}"
            );
            tokio::task::yield_now().await;
        }
        for worker in workers {
            worker.await.unwrap();
        }
        assert_eq!(pool.snapshot().in_use, 0);
        assert_eq!(pool.snapshot().available, Some(7));
    }

    #[test]
    fn defensive_invalid_pool_reports_fail_closed_zero_availability() {
        let pool = PeerPermitPool::invalid_fail_closed(Semaphore::MAX_PERMITS + 1, counter());
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.limit, Semaphore::MAX_PERMITS + 1);
        assert_eq!(snapshot.in_use, 0);
        assert_eq!(snapshot.available, Some(0));
        assert!(pool.try_acquire().is_none());
    }
}
