// SPDX-License-Identifier: Apache-2.0

//! Runtime admission and pressure controls for configured storage roots.
//!
//! These controls intentionally govern only local storage work. They neither
//! create sockets nor alter the contained torrent network path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{Mutex, Notify};

use swarmotter_core::bandwidth::{RateDirection, RateLimiter};
use swarmotter_core::config::Config;
use swarmotter_core::error::{CoreError, Result};
use swarmotter_core::hash::InfoHash;
use swarmotter_core::storage::StorageIo;

use super::DaemonRuntime;

const STORAGE_WORK_CANCELLED_MESSAGE: &str = "storage work cancelled";

/// A small cancellation primitive for daemon-owned storage work.
///
/// It deliberately has no relationship to the torrent network data plane.
/// Its only purpose is to make local root-admission and verification waits
/// responsive to lifecycle operations such as pause, recheck, and move.
#[derive(Clone, Default)]
pub(super) struct StorageWorkCancellation {
    inner: Arc<StorageWorkCancellationInner>,
}

#[derive(Default)]
struct StorageWorkCancellationInner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl StorageWorkCancellation {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.notify.notify_waiters();
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Wait for cancellation without a lost wake-up between the state check
    /// and subscription to the notification.
    pub(super) async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let changed = self.inner.notify.notified();
            if self.is_cancelled() {
                return;
            }
            changed.await;
        }
    }
}

pub(super) fn storage_work_cancelled_error() -> CoreError {
    CoreError::Storage(STORAGE_WORK_CANCELLED_MESSAGE.into())
}

pub(super) fn is_storage_work_cancelled(error: &CoreError) -> bool {
    matches!(error, CoreError::Storage(message) if message == STORAGE_WORK_CANCELLED_MESSAGE)
}

/// One in-flight explicit API recheck, with a completion signal for lifecycle
/// operations that must not race its disk access (notably data moves).
#[derive(Clone)]
pub(super) struct ExplicitRecheckOperation {
    cancellation: StorageWorkCancellation,
    completed: Arc<ExplicitRecheckCompletion>,
}

#[derive(Default)]
struct ExplicitRecheckCompletion {
    finished: AtomicBool,
    notify: Notify,
}

impl ExplicitRecheckOperation {
    pub(super) fn new() -> Self {
        Self {
            cancellation: StorageWorkCancellation::new(),
            completed: Arc::new(ExplicitRecheckCompletion::default()),
        }
    }

    pub(super) fn cancellation(&self) -> StorageWorkCancellation {
        self.cancellation.clone()
    }

    pub(super) fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub(super) fn is_same_operation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.completed, &other.completed)
    }

    pub(super) fn finish(&self) {
        self.completed.finished.store(true, Ordering::Release);
        self.completed.notify.notify_waiters();
    }

    pub(super) async fn wait_finished(&self) {
        loop {
            if self.completed.finished.load(Ordering::Acquire) {
                return;
            }
            let changed = self.completed.notify.notified();
            if self.completed.finished.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

/// The resolved, most-specific storage-root policy for an active write path.
#[derive(Debug, Clone)]
pub(super) struct StorageRootAdmission {
    pub root: PathBuf,
    pub max_active_downloads: usize,
    pub max_active_bytes: u64,
    pub max_write_bytes_per_second: u64,
    pub max_concurrent_rechecks: usize,
}

/// Resolve the configured control that owns an active write path.
pub(super) fn storage_root_admission_for_path(
    config: &Config,
    path: &Path,
) -> Option<StorageRootAdmission> {
    let control = config.storage.root_control_for_path(path)?;
    Some(StorageRootAdmission {
        root: control.normalized_path().ok()?,
        max_active_downloads: control.max_active_downloads,
        max_active_bytes: control.max_active_bytes,
        max_write_bytes_per_second: control.max_write_bytes_per_second,
        max_concurrent_rechecks: control.max_concurrent_rechecks,
    })
}

#[derive(Debug, Clone)]
pub(super) struct StorageAdmissionRecord {
    pub hash: InfoHash,
    pub root: PathBuf,
    pub declared_bytes: u64,
}

#[derive(Debug, Clone)]
struct StorageAdmissionReservation {
    root: PathBuf,
    declared_bytes: u64,
}

/// A pure admission snapshot used by queue planning before engine creation.
#[derive(Debug, Clone, Default)]
pub(super) struct StorageAdmissionPlan {
    reservations: HashMap<InfoHash, StorageAdmissionReservation>,
}

impl StorageAdmissionPlan {
    pub(super) fn from_records(records: impl IntoIterator<Item = StorageAdmissionRecord>) -> Self {
        Self {
            reservations: records
                .into_iter()
                .map(|record| {
                    (
                        record.hash,
                        StorageAdmissionReservation {
                            root: record.root,
                            declared_bytes: record.declared_bytes,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Add one planned engine reservation. Existing work remains under its
    /// original root snapshot when root controls are replaced, so it may
    /// finish safely while new work uses the replacement policy.
    pub(super) fn admit(
        &mut self,
        hash: InfoHash,
        admission: &StorageRootAdmission,
        declared_bytes: u64,
    ) -> Result<()> {
        if self.reservations.contains_key(&hash) {
            return Ok(());
        }
        admit_reservation(&mut self.reservations, hash, admission, declared_bytes)
    }
}

/// Runtime-owned reservations and shared write limiters.
///
/// A reservation starts before an engine is made visible and is removed when
/// its task exits or is forcibly stopped. This makes root admission atomic
/// even when magnet metadata becomes available concurrently.
#[derive(Clone, Default)]
pub(super) struct StorageAdmissionController {
    reservations: Arc<Mutex<HashMap<InfoHash, StorageAdmissionReservation>>>,
    write_limiters: Arc<Mutex<HashMap<PathBuf, RateLimiter>>>,
    notify: Arc<Notify>,
}

impl StorageAdmissionController {
    pub(super) async fn reserve(
        &self,
        hash: InfoHash,
        admission: &StorageRootAdmission,
        declared_bytes: u64,
    ) -> Result<()> {
        let mut reservations = self.reservations.lock().await;
        admit_reservation(&mut reservations, hash, admission, declared_bytes)
    }

    pub(super) async fn release(&self, hash: &InfoHash) {
        if self.reservations.lock().await.remove(hash).is_some() {
            self.notify.notify_waiters();
        }
    }

    pub(super) async fn clear(&self) {
        self.reservations.lock().await.clear();
        self.write_limiters.lock().await.clear();
        self.notify.notify_waiters();
    }

    pub(super) async fn records(&self) -> Vec<StorageAdmissionRecord> {
        self.reservations
            .lock()
            .await
            .iter()
            .map(|(hash, reservation)| StorageAdmissionRecord {
                hash: *hash,
                root: reservation.root.clone(),
                declared_bytes: reservation.declared_bytes,
            })
            .collect()
    }

    /// Register for a root reservation release or configuration replacement.
    /// Call this before [`Self::reserve`] when a metadata engine may wait for
    /// active bytes to become available.
    pub(super) fn changed(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.notify.notified()
    }

    pub(super) fn declared_bytes_can_fit(
        &self,
        admission: &StorageRootAdmission,
        declared_bytes: u64,
    ) -> bool {
        admission.max_active_bytes == 0 || declared_bytes <= admission.max_active_bytes
    }

    pub(super) fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }

    /// Return the process-shared payload-write limiter for one root. A cloned
    /// [`RateLimiter`] shares its token buckets, so every `StorageIo` using
    /// this root consumes one sustained budget.
    pub(super) async fn write_limiter(
        &self,
        admission: &StorageRootAdmission,
    ) -> Option<RateLimiter> {
        if admission.max_write_bytes_per_second == 0 {
            return None;
        }
        let mut limiters = self.write_limiters.lock().await;
        let limiter = limiters
            .entry(admission.root.clone())
            .or_insert_with(|| RateLimiter::new(admission.max_write_bytes_per_second, 0))
            .clone();
        limiter.set_capacity(
            RateDirection::Download,
            admission.max_write_bytes_per_second,
        );
        Some(limiter)
    }
}

fn admit_reservation(
    reservations: &mut HashMap<InfoHash, StorageAdmissionReservation>,
    hash: InfoHash,
    admission: &StorageRootAdmission,
    declared_bytes: u64,
) -> Result<()> {
    let mut active_downloads = 0usize;
    let mut active_bytes = 0u64;
    for (existing_hash, reservation) in reservations.iter() {
        if *existing_hash != hash && reservation.root == admission.root {
            active_downloads = active_downloads.saturating_add(1);
            active_bytes = active_bytes.saturating_add(reservation.declared_bytes);
        }
    }
    if admission.max_active_downloads > 0 && active_downloads >= admission.max_active_downloads {
        return Err(CoreError::Storage(format!(
            "storage root admission blocked for {}: {} active downloads reach configured limit {}",
            admission.root.display(),
            active_downloads,
            admission.max_active_downloads
        )));
    }
    let requested_bytes = active_bytes.saturating_add(declared_bytes);
    if admission.max_active_bytes > 0 && requested_bytes > admission.max_active_bytes {
        return Err(CoreError::Storage(format!(
            "storage root admission blocked for {}: {} declared active bytes would exceed configured limit {}",
            admission.root.display(),
            requested_bytes,
            admission.max_active_bytes
        )));
    }
    reservations.insert(
        hash,
        StorageAdmissionReservation {
            root: admission.root.clone(),
            declared_bytes,
        },
    );
    Ok(())
}

/// Bounded full-recheck accounting. A permit is RAII so request cancellation
/// cannot leave a root permanently saturated.
#[derive(Clone, Default)]
pub(super) struct StorageRecheckController {
    active: Arc<StdMutex<HashMap<PathBuf, usize>>>,
    notify: Arc<Notify>,
}

pub(super) struct StorageRecheckPermit {
    root: PathBuf,
    active: Arc<StdMutex<HashMap<PathBuf, usize>>>,
    notify: Arc<Notify>,
}

impl StorageRecheckController {
    /// Register for an active-permit release or configuration replacement.
    /// Call this before [`Self::try_acquire`] to avoid a lost wake-up.
    pub(super) fn changed(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.notify.notified()
    }

    pub(super) fn active_counts(&self) -> HashMap<PathBuf, usize> {
        lock_unpoisoned(&self.active).clone()
    }

    /// Wake pending requests after a configuration replacement. They re-read
    /// their control before creating a fresh request; existing permits are
    /// deliberately allowed to finish safely.
    pub(super) fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }

    pub(super) fn try_acquire(
        &self,
        admission: &StorageRootAdmission,
    ) -> Option<StorageRecheckPermit> {
        let mut active = lock_unpoisoned(&self.active);
        let count = active.entry(admission.root.clone()).or_default();
        if admission.max_concurrent_rechecks > 0 && *count >= admission.max_concurrent_rechecks {
            return None;
        }
        *count = count.saturating_add(1);
        Some(StorageRecheckPermit {
            root: admission.root.clone(),
            active: self.active.clone(),
            notify: self.notify.clone(),
        })
    }
}

impl DaemonRuntime {
    /// Run one full verification under the currently resolved root control.
    ///
    /// The permit covers only the on-disk work. Configuration replacement
    /// wakes pending callers so they resolve the current control again;
    /// already-acquired permits are allowed to finish safely.
    pub(super) async fn run_root_scoped_recheck<T, F>(
        &self,
        storage_dir: &Path,
        cancellation: Option<&StorageWorkCancellation>,
        work: F,
    ) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        let permit = loop {
            if cancellation.is_some_and(StorageWorkCancellation::is_cancelled) {
                return Err(storage_work_cancelled_error());
            }
            let admission = {
                let config = self.config.read().await;
                storage_root_admission_for_path(&config, storage_dir)
            };
            let Some(admission) = admission else {
                break None;
            };
            // Subscribe before checking capacity so a release or replacement
            // cannot be missed between testing capacity and waiting.
            let changed = self.storage_rechecks.changed();
            if let Some(permit) = self.storage_rechecks.try_acquire(&admission) {
                break Some(permit);
            }
            if let Some(cancellation) = cancellation {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(storage_work_cancelled_error()),
                    _ = changed => {}
                }
            } else {
                changed.await;
            }
        };

        let result = if let Some(cancellation) = cancellation {
            tokio::select! {
                _ = cancellation.cancelled() => Err(storage_work_cancelled_error()),
                result = work => result,
            }
        } else {
            work.await
        };
        // Keep release next to the work/select so future changes cannot make
        // a cancelled request retain a root permit.
        drop(permit);
        result
    }

    pub(super) async fn recheck_storage_under_root_control(
        &self,
        storage: &StorageIo,
        cancellation: Option<&StorageWorkCancellation>,
    ) -> Result<swarmotter_core::storage::resume::PieceBitfield> {
        self.run_root_scoped_recheck(storage.base_dir(), cancellation, storage.recheck())
            .await
    }
}

impl Drop for StorageRecheckPermit {
    fn drop(&mut self) {
        let mut active = lock_unpoisoned(&self.active);
        if let Some(count) = active.get_mut(&self.root) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&self.root);
            }
        }
        drop(active);
        self.notify.notify_waiters();
    }
}

fn lock_unpoisoned<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(
        path: &str,
        max_active_downloads: usize,
        max_active_bytes: u64,
    ) -> StorageRootAdmission {
        StorageRootAdmission {
            root: PathBuf::from(path),
            max_active_downloads,
            max_active_bytes,
            max_write_bytes_per_second: 0,
            max_concurrent_rechecks: 0,
        }
    }

    #[test]
    fn admission_plan_enforces_count_and_declared_bytes_atomically() {
        let root = admission("/srv/torrents", 2, 10);
        let mut plan = StorageAdmissionPlan::default();
        plan.admit(InfoHash::from_bytes([1; 20]), &root, 6).unwrap();
        let error = plan
            .admit(InfoHash::from_bytes([2; 20]), &root, 5)
            .unwrap_err();
        assert_eq!(error.code().as_str(), "storage_error");
        plan.admit(InfoHash::from_bytes([2; 20]), &root, 4).unwrap();
        let error = plan
            .admit(InfoHash::from_bytes([3; 20]), &root, 0)
            .unwrap_err();
        assert!(error.to_string().contains("active downloads"));
    }

    #[test]
    fn admission_plan_grandfathers_existing_root_records_on_replacement() {
        let old_root = admission("/srv/old", 1, 0);
        let replacement_root = admission("/srv/new", 1, 0);
        let existing = InfoHash::from_bytes([1; 20]);
        let subsequent = InfoHash::from_bytes([2; 20]);
        let mut plan = StorageAdmissionPlan::default();
        plan.admit(existing, &old_root, 1).unwrap();

        // Re-evaluating the active hash under a new root must not rewrite its
        // reservation. Otherwise a root-control-only replacement could make
        // existing work consume a new root's first slot or be torn down.
        plan.admit(existing, &replacement_root, 1).unwrap();
        plan.admit(subsequent, &replacement_root, 1).unwrap();
    }

    #[tokio::test]
    async fn recheck_permit_releases_when_dropped() {
        let controls = StorageRecheckController::default();
        let root = StorageRootAdmission {
            root: PathBuf::from("/srv/torrents"),
            max_active_downloads: 0,
            max_active_bytes: 0,
            max_write_bytes_per_second: 0,
            max_concurrent_rechecks: 1,
        };
        let permit = controls.try_acquire(&root).unwrap();
        assert_eq!(controls.active_counts().get(&root.root), Some(&1));
        drop(permit);
        assert!(controls.active_counts().is_empty());
        let permit = controls.try_acquire(&root).unwrap();
        drop(permit);
    }

    #[tokio::test]
    async fn waiting_recheck_rechecks_the_control_after_a_configuration_wake() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let controls = StorageRecheckController::default();
        let first_control = StorageRootAdmission {
            root: PathBuf::from("/srv/torrents"),
            max_active_downloads: 0,
            max_active_bytes: 0,
            max_write_bytes_per_second: 0,
            max_concurrent_rechecks: 1,
        };
        let first = controls.try_acquire(&first_control).unwrap();
        let configured_limit = Arc::new(AtomicUsize::new(1));
        let waiting_controls = controls.clone();
        let waiting_limit = configured_limit.clone();
        let waiting_control = first_control.clone();
        let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
        let mut waiter = tokio::spawn(async move {
            let mut registered_tx = Some(registered_tx);
            loop {
                let admission = StorageRootAdmission {
                    max_concurrent_rechecks: waiting_limit.load(Ordering::Acquire),
                    ..waiting_control.clone()
                };
                let changed = waiting_controls.changed();
                if let Some(registered_tx) = registered_tx.take() {
                    let _ = registered_tx.send(());
                }
                if let Some(permit) = waiting_controls.try_acquire(&admission) {
                    return permit;
                }
                changed.await;
            }
        });

        registered_rx.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err());

        configured_limit.store(2, Ordering::Release);
        controls.notify_waiters();
        let second = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("configuration wake should release the waiting recheck")
            .unwrap();
        assert_eq!(controls.active_counts().get(&first_control.root), Some(&2));
        drop(second);
        drop(first);
        assert!(controls.active_counts().is_empty());
    }

    #[tokio::test]
    async fn root_write_limiters_share_one_aggregate_bucket() {
        let controller = StorageAdmissionController::default();
        let root = StorageRootAdmission {
            root: PathBuf::from("/srv/torrents"),
            max_active_downloads: 0,
            max_active_bytes: 0,
            max_write_bytes_per_second: 4_096,
            max_concurrent_rechecks: 0,
        };
        let first = controller.write_limiter(&root).await.unwrap();
        let second = controller.write_limiter(&root).await.unwrap();
        assert_eq!(
            first.capacity(RateDirection::Download),
            root.max_write_bytes_per_second
        );
        assert!(Arc::ptr_eq(&first.download, &second.download));
    }
}
