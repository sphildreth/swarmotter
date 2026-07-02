# ADR-0021: Selfish Completion Policy

## Status

Accepted

## Context

SwarmOtter's default behavior is to keep a completed torrent managed by the
daemon: it transitions to the completed state and continues seeding/uploading
to peers via the inbound `Seeder`. Some operators want SwarmOtter to act as a
fetcher only: download content, verify all pieces, and then immediately stop
managing the torrent without seeding it, while keeping the downloaded files on
disk.

The key constraints for such a mode are:

- It must be opt-in and default to off so existing completion/seeding behavior
  is unchanged.
- On completion, the engine and seeder must stop so the torrent is not seeded.
- The torrent record must be removed from the daemon (and therefore from the
  API/UI torrent list), equivalent to a `remove_torrent(delete_data = false)`.
- Downloaded payload data must never be deleted by this mode; it is a
  data-preserving removal, not a delete-data operation.
- The removal must be safe to trigger from within the spawned engine task
  without deadlocking on the engine task's own join handle.

## Decision

- Add a global config flag `torrent.selfish` (bool, default `false`) in
  `swarmotter-core::config::TorrentConfig`, configurable via TOML and the
  `SWARMOTTER_TORRENT__SELFISH` environment override.
- When `selfish = true` and a torrent's download finishes (all pieces
  verified, `EngineState::finished`), the daemon performs a selfish-mode
  removal: stop the inbound seeder, clear live engine/seeder bookkeeping,
  detach the (already-returning) engine task without awaiting its own handle,
  and remove the torrent record from the registry. A structured log entry
  records the removal with `info_hash`, `name`, `selfish = true`, and
  `delete_data = false`.
- The removal path intentionally does not invoke delete-data behavior;
  downloaded files are preserved. This is equivalent to
  `remove_torrent(delete_data = false)` driven automatically on completion.
- When `selfish = false`, the existing completion/seeding behavior is
  unchanged.
- The policy is scoped to the download-completion path in the engine task. It
  does not fire on manual `recheck` of already-present data, to avoid
  surprising removal during a verify operation.

## Consequences

- Operators can run SwarmOtter as a fetch-and-stop client without seeding,
  while retaining the downloaded payload.
- A selfish-completed torrent disappears from the API/UI torrent list because
  it is removed from the registry; clients observing completion must not assume
  the torrent persists after completion when this mode is enabled.
- The removal is implemented as an associated function taking shared
  `Arc<Mutex<...>>` handles (not `&self`) precisely so the engine task can
  call it safely without awaiting its own join handle (which would deadlock).
- Selfish mode never deletes data; explicit `remove_torrent(delete_data =
  true)` via the API continues to delete payload data when requested,
  independent of the selfish setting.
- Runtime settings patch (`SettingsPatch`) does not expose `torrent.selfish`;
  it is a config-file/env setting, consistent with other torrent transport
  settings that require a restart to change safely.

## Related Documents

- `crates/swarmotter-core/src/config.rs` (`TorrentConfig::selfish`)
- `crates/swarmotterd/src/daemon.rs` (`DaemonRuntime::selfish_remove_completed`,
  `DaemonRuntime::start_engine`)
- `config/swarmotter.toml.example`
- `design/configuration.md`
- `design/requirements.md` (Seeding/ratio completion policy)
- ADR-0016 (task/runtime model for the live engine)
