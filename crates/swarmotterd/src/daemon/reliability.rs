// SPDX-License-Identifier: Apache-2.0

//! Process liveness is independent of VPN availability. Each essential loop
//! reports completed work; sleeping mapping leases report progress separately.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex as StdMutex;

const STALL_LIMIT: Duration = Duration::from_secs(300);

pub(super) struct Reliability {
    progress: StdMutex<[Instant; 5]>,
    stopping: AtomicBool,
}

impl Default for Reliability {
    fn default() -> Self {
        Self {
            progress: StdMutex::new([Instant::now(); 5]),
            stopping: AtomicBool::new(false),
        }
    }
}

impl DaemonRuntime {
    pub(super) fn heartbeat(&self, task: usize) {
        if let Ok(mut progress) = self.reliability.progress.lock() {
            progress[task] = Instant::now();
        }
    }

    pub fn is_stopping(&self) -> bool {
        self.reliability.stopping.load(Ordering::SeqCst)
    }

    /// Synchronous: a stuck async registry or queue cannot prevent containment.
    pub fn begin_shutdown(&self) {
        self.reliability.stopping.store(true, Ordering::SeqCst);
        self.containment_gate.stop();
        self.event_broker.shutdown();
    }

    fn loops_responsive(&self) -> bool {
        self.reliability
            .progress
            .lock()
            .is_ok_and(|progress| progress.iter().all(|last| last.elapsed() < STALL_LIMIT))
    }

    /// Checks the same locks needed by torrent list and queue operations,
    /// without letting a blocked request tie up the liveness endpoint.
    pub async fn application_live(&self) -> bool {
        if self.is_stopping() || !self.loops_responsive() {
            return false;
        }
        tokio::time::timeout(Duration::from_millis(500), async {
            let _registry = self.registry.lock().await;
            let _queue = self.queue.lock().await;
        })
        .await
        .is_ok()
    }

    /// Start only after restoration. An OS thread detects even executor
    /// starvation and enforces a final shutdown deadline. Normal shutdown
    /// explicitly disarms it after the durable checkpoint.
    pub fn start_watchdog(self: &Arc<Self>) -> Result<Watchdog> {
        if let Ok(mut progress) = self.reliability.progress.lock() {
            *progress = [Instant::now(); 5];
        }
        let completed = Arc::new(AtomicBool::new(false));
        let disarmed = completed.clone();
        let runtime = self.clone();
        std::thread::Builder::new()
            .name("swarmotter-watchdog".into())
            .spawn(move || {
                let mut stopping_since = None;
                while !disarmed.load(Ordering::SeqCst) {
                    if !runtime.loops_responsive() {
                        runtime.begin_shutdown();
                    }
                    if runtime.is_stopping() {
                        let started = stopping_since.get_or_insert_with(Instant::now);
                        if started.elapsed() >= Duration::from_secs(35) {
                            // No runtime/logging locks on the last-resort path.
                            std::process::exit(1);
                        }
                    }
                    std::thread::sleep(Duration::from_millis(250));
                }
            })
            .map_err(CoreError::from)?;
        Ok(Watchdog { completed })
    }

    /// Executor heartbeat and lock health probe run independently of workers.
    pub async fn supervise_progress(&self) -> Result<()> {
        let mut failures = 0;
        loop {
            self.heartbeat(4);
            if self.application_live().await {
                failures = 0;
            } else {
                failures += 1;
            }
            if self.is_stopping() || failures >= 30 {
                return Err(CoreError::Internal(
                    "application progress watchdog failed".into(),
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub struct Watchdog {
    completed: Arc<AtomicBool>,
}
impl Watchdog {
    pub fn disarm(self) {
        self.completed.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn liveness_checks_registry_and_queue_but_not_vpn_availability() {
        let runtime = DaemonRuntime::new(
            Config::default(),
            NetworkHealth::blocked(
                NetworkContainmentMode::Strict,
                NetworkContainmentStatus::BlockedFailClosed,
                "test",
            ),
        );
        assert!(runtime.application_live().await);
        let registry = runtime.registry.lock().await;
        assert!(!runtime.application_live().await);
        drop(registry);
        assert!(runtime.application_live().await);
        let queue = runtime.queue.lock().await;
        assert!(!runtime.application_live().await);
        drop(queue);
        runtime.begin_shutdown();
        runtime.containment_gate.allow();
        assert!(!runtime.containment_gate.traffic_allowed());
        assert!(runtime.containment_gate.enforce().is_err());
        assert!(!runtime.application_live().await);
    }
    #[tokio::test(start_paused = true)]
    async fn sustained_lock_stall_requests_process_recovery() {
        let runtime = DaemonRuntime::new(
            Config::default(),
            NetworkHealth::blocked(
                NetworkContainmentMode::Disabled,
                NetworkContainmentStatus::Disabled,
                "test",
            ),
        );
        let _registry = runtime.registry.lock().await;
        assert!(runtime.supervise_progress().await.is_err());
    }

    #[tokio::test]
    async fn stalled_worker_is_unhealthy_and_supervisor_reports_failure() {
        let runtime = DaemonRuntime::new(
            Config::default(),
            NetworkHealth::blocked(
                NetworkContainmentMode::Disabled,
                NetworkContainmentStatus::Disabled,
                "test",
            ),
        );
        runtime.reliability.progress.lock().unwrap()[0] = Instant::now() - STALL_LIMIT;
        assert!(!runtime.application_live().await);
        runtime.begin_shutdown();
        assert!(runtime.supervise_progress().await.is_err());
    }
}
