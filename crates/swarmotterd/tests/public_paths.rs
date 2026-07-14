// SPDX-License-Identifier: Apache-2.0

//! Compile-time coverage for the library facade consumed by the binary and
//! downstream integration tests.

use swarmotterd::daemon::{DaemonRuntime, HealthReport};
use swarmotterd::engine::{
    EngineCommand, EngineState, MagnetParams, TorrentEngine, TrackerAnnounceSnapshot,
    TrackerScrapeSnapshot,
};
use swarmotterd::logging;

#[test]
fn daemon_engine_and_logging_public_paths_remain_importable() {
    let exported_types = [
        std::any::type_name::<DaemonRuntime>(),
        std::any::type_name::<HealthReport>(),
        std::any::type_name::<EngineCommand>(),
        std::any::type_name::<EngineState>(),
        std::any::type_name::<MagnetParams>(),
        std::any::type_name::<TorrentEngine>(),
        std::any::type_name::<TrackerAnnounceSnapshot>(),
        std::any::type_name::<TrackerScrapeSnapshot>(),
    ];
    assert!(exported_types
        .iter()
        .all(|path| path.starts_with("swarmotterd::")));

    let _logging_initializer = logging::init;
}
