// SPDX-License-Identifier: Apache-2.0

//! Master session traffic switch.
//!
//! The switch is durable with the queue generation and deliberately separate
//! from per-torrent operator pauses. Activating it force-stops every contained
//! downloader, seeder, metadata fetch, and shared inbound listener, then keeps
//! queue and seeding reconciliation from starting new data-plane work until it
//! is explicitly cleared.

use super::*;

impl DaemonRuntime {
    pub(super) async fn session_pause_state(&self) -> bool {
        self.queue.lock().await.is_paused()
    }

    pub(super) async fn set_session_pause_state(&self, paused: bool) -> Result<()> {
        // Serialize the switch with every engine/seeder start and with data
        // plane configuration reconstruction. The queue flag is set while the
        // transition lock is owned, so no task can be admitted between the
        // policy change and the stop sweep.
        let transition = self.data_plane_transition_lock.lock().await;
        let was_paused = {
            let mut queue = self.queue.lock().await;
            let was_paused = queue.is_paused();
            queue.set_paused(paused);
            was_paused
        };

        if paused {
            // Capture final verified progress before force-stopping. This is
            // the same transition boundary used by data-plane reconfiguration.
            self.reconcile_engine_progress_for_transition().await;

            let hashes = self
                .registry
                .lock()
                .await
                .torrents
                .keys()
                .copied()
                .collect::<Vec<_>>();
            self.stop_all_torrent_tasks(&hashes).await;
            self.stop_dht_runner().await;
            self.stop_seeder_listener(false).await;

            let changed = {
                let mut registry = self.registry.lock().await;
                let mut changed = Vec::new();
                for (hash, torrent) in &mut registry.torrents {
                    let next = match torrent.state {
                        TorrentState::Downloading | TorrentState::DownloadingMetadata => {
                            torrent.seeding_status = SeedingStatus::NotEligible;
                            Some(TorrentState::Queued)
                        }
                        TorrentState::Seeding => {
                            torrent.seeding_status = if torrent.progress.is_complete() {
                                SeedingStatus::Queued
                            } else {
                                SeedingStatus::NotEligible
                            };
                            Some(TorrentState::Completed)
                        }
                        _ => None,
                    };
                    if let Some(next) = next {
                        if torrent.state != next {
                            torrent.state = next;
                            changed.push((*hash, next));
                        }
                    }
                }
                changed
            };

            // The queue projection carries the paused flag. Keep the
            // transition lock until the durable write succeeds so no resume
            // request can start work against a flag that was never persisted.
            // The in-memory switch stays set on failure, so refusing to start
            // traffic remains the fail-closed outcome.
            self.persist_state().await?;
            drop(transition);

            for (hash, state) in changed {
                self.publish_torrent_event("torrent_changed", hash, state);
            }
            if !was_paused {
                tracing::info!("master session pause activated; all torrent traffic stopped");
            }
        } else {
            // Unpause is persisted before reconciliation so a crash cannot
            // restore a state that resumes traffic unexpectedly.
            if let Err(error) = self.persist_state().await {
                self.queue.lock().await.set_paused(true);
                return Err(error);
            }
            drop(transition);

            if was_paused {
                tracing::info!("master session pause cleared; reconciling torrent traffic");
            }
            self.reconcile_queue().await;
            self.reconcile_seeders().await;
        }

        self.publish_event(Event::new("session_changed", json!({ "paused": paused })));
        self.publish_event(stats_updated_event());
        Ok(())
    }
}
