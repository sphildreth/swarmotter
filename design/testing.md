# Testing

This document describes SwarmOtter's testing strategy. Testing is tracked by
feature completion and acceptance criteria, not by time estimates.

## General expectations

- Add or update tests alongside feature work.
- Prefer generated local torrents and local swarm fixtures so tests do not
  depend on third-party content.
- Run `cargo fmt`, `cargo check`, and `cargo test` before considering work done.
  Fix all reported issues.

## Required test areas

### Unit tests

- Magnet parsing.
- Torrent parsing.
- Info hash handling.
- Tracker tier handling.
- UDP tracker source, action, and transaction correlation.
- uTP header, connection-ID, extension-chain, and SACK handling.
- Piece selection.
- Piece verification.
- Queue behavior.
- Ratio/seeding behavior.
- Bandwidth limit logic.
- Config validation.
- Network containment validation logic.
- Per-torrent health calculation: complete / network-blocked / paused /
  missing pieces with zero sources / good active swarm / many connected but
  useless peers / slow-but-completable / private torrent (no DHT/PEX
  penalty) / bar+label mapping.
- Bencode parser budgets and adversarial cases (ADR-0050): depth boundary
  (128 accepted, 129 rejected), node-count boundary (`MAX_BENCODE_NODES`
  accepted, one more rejected), overflowing/beyond-input string lengths,
  missing terminators, empty/leading-zero/negative-zero integer forms,
  duplicate and non-string dictionary keys, unsorted-but-unique keys accepted,
  trailing bytes rejected, and the 16 MiB metadata byte limit accepted at the
  boundary and rejected one byte over. Every malformed corpus case must be
  panic-free under `std::panic::catch_unwind`. BEP 9 prefix decoding repeats
  the same depth/node/duplicate/length checks while preserving trailing piece
  bytes.
- Metainfo shape budgets (ADR-0050): piece length zero/over-limit, mismatched
  piece count, non-20-byte-multiple pieces, too many files, too many pieces,
  total-length overflow, and cumulative storage file-offset overflow all
  produce typed `MalformedTorrent` errors without panicking.
- Durable JSON-state metadata: SHA-1 hashes of 0, 19, 20, and 21 decoded bytes;
  only 20 succeeds, with the torrent record and piece index in the error and no
  payload data or content paths. The sequence accepts exactly
  `MAX_TORRENT_PIECES` hashes and rejects one more. Restored `TorrentMeta`
  values must also pass the file-count, piece-count, piece-length,
  total-length, and piece-count consistency checks in `TorrentMeta::validate()`.
  JSON state does not pass through the bencode byte, depth, or node budgets.
- Production metadata ingress bounds: `.torrent` bodies at and one byte over
  `MAX_TORRENT_METADATA_BYTES` through the core parser, dedicated and
  multiplexed raw API add, bulk/Transmission bounded-base64 add, watch import,
  and BEP 9 metadata assembly. A configured API body limit below the metadata
  limit retains its HTTP 413 behavior.
- Watch scanner/read boundary (ADR-0054): sorted recursive/non-recursive walks,
  child/root symlink rejection, composite lexical root-relative keys, exact
  bounded-read limit, typed metadata-change result, and create-new
  non-overwriting archive/failure actions.

### Integration tests

- Add magnet through API.
- Add torrent file through API.
- Upload torrent file through Web UI/API path.
- Reject cross-origin native API mutations and WebSocket handshakes while
  preserving same-origin browser requests and non-browser API clients.
- Route matrix (ADR-0044/ADR-0049, Phase 3): a table-driven real-router matrix
  for authentication enabled/disabled covering native single/bulk
  add/pause/remove/settings mutation/WebSocket/SSE, Transmission session
  negotiation and a mutating RPC method, and qBittorrent
  login/add/pause/resume/delete form endpoints. It accepts same-origin
  (including scheme-changing TLS termination), `Sec-Fetch-Site: none`, and
  absent browser headers. It rejects same-site, cross-site, unknown, duplicate,
  and invalid-byte Fetch Metadata plus foreign, malformed, opaque/`null`,
  duplicate/multi-value, invalid-byte, path, query, fragment, and userinfo
  origins and duplicate/invalid Hosts. Every rejection returns the
  surface-specific 403 shape before auth/session/compatibility checks and leaves
  fake-daemon calls and state unchanged.
- Chrome extension matrix (ADR-0044/ADR-0049, Phase 3): a realistic Manifest V3
  service-worker Origin with a 32-character `a`-`p` extension ID and
  `Sec-Fetch-Site: none` reaches every named native/Transmission/qBittorrent
  route only in authenticated mode with a valid Bearer or
  `X-SwarmOtter-Auth` token. Native bulk add must mutate through the real router.
  Auth-disabled mode and missing, invalid, or duplicated credentials return the
  surface-specific 403 before body extraction or mutation. Short IDs, invalid
  ID alphabet, extension ports, missing/malformed/duplicate Host, and ordinary
  foreign HTTP(S) Origins remain rejected; snapshots prove rejected requests do
  not change daemon state.
- Start the real local HTTP control server and complete a same-origin,
  authenticated WebSocket handshake with HTTP 101, proving the accepted path
  reaches Hyper's production upgrade extension rather than only a Tower
  extraction boundary.
- Validate Web UI static security headers and required operation wiring. The
  ES-module route matrix covers `/app.js` and every `/js/*.js` feature module,
  asserting HTTP 200, JavaScript content type, the shared `script-src 'self'`
  CSP, and the intentionally unchanged entry-script cache policy.
- Import torrent from watch folder.
- Pause/resume/remove lifecycle.
- Recheck lifecycle.
- Tracker announce behavior.
- DHT peer discovery behavior.
- PEX peer discovery behavior.
- File priority behavior.
- Queue behavior.
- Settings behavior.
- Concurrent atomic configuration replacement.
- Durable torrent and queue restoration after daemon reconstruction.
- WebSocket/SSE event delivery.
- Per-torrent health serialization: `TorrentSummary` and the torrent detail
  endpoint both include a `health` object with score, bars, label, and
  per-component sub-scores.

### Watch-folder stability and atomicity acceptance matrix

ADR-0054 is accepted only when every production boundary below passes. Scanner
helpers alone do not substitute for daemon/API/event/UI behavior.

| Capability | Required assertions | Acceptance evidence |
| --- | --- | --- |
| Stable bounded ingestion | A partial copy needs two unchanged scans after its last change. A deterministic change between bounded read and post-read metadata check discards bytes, resets to one, and emits no result/action. Exact 16 MiB is accepted and one-over is rejected before input-sized allocation. | `watch::tests::bounded_watch_read_accepts_exact_limit_and_rejects_one_over_before_read`, `daemon::tests::watch_partial_copy_and_read_time_change_reset_without_terminal_result` |
| Safe deterministic walk | Recursive/non-recursive paths are sorted; child symlinks are ignored; a symlink/missing root is incomplete; normalized overlapping roots do not alias. Strict-descendant archive/failure exclusions are component-aware and scoped to one configured folder; an equal-root or whitespace-only path is invalid. A failed root scan retains observations, while successful disappearance and removed config roots prune them. | `watch::tests::scans_torrent_files`, `watch::tests::scan_ignores_file_and_directory_symlinks`, `watch::tests::symlink_watch_root_is_an_incomplete_scan_error`, `watch::tests::configured_scan_exclusions_are_descendant_component_aware_and_per_folder`, `config::tests::watch_paths_reject_whitespace_and_action_destination_equal_to_root`, `daemon::tests::overlapping_watch_roots_have_distinct_composite_observation_keys`, `daemon::tests::watch_action_exclusion_does_not_hide_separately_configured_overlapping_root`, `daemon::tests::incomplete_watch_root_scan_retains_prior_observations`, `daemon::tests::watch_observations_prune_disappeared_files_and_removed_roots` |
| Idempotence and status | `leave` produces one result per unchanged fingerprint; read-only status calls do not advance stability and exclude processed unchanged files from pending; replacement processes once. Restart re-observes, returns duplicate success, applies the success action once, and preserves exact existing torrent/queue state. Recursive in-root archive and permanent-failure moves are excluded and remain one history result after later scans. Concurrent manual scans serialize around the complete scan and create one terminal result. | `daemon::tests::watch_leave_processes_each_fingerprint_once_and_status_excludes_it`, `daemon::tests::watch_restart_duplicate_runs_success_action_once_without_mutation`, `daemon::tests::recursive_watch_excludes_in_root_archive_after_success`, `daemon::tests::recursive_watch_excludes_in_root_failure_after_permanent_failure`, `daemon::tests::concurrent_manual_watch_scans_produce_one_terminal_result` |
| Shared durable add | Deterministic persistence failure restores exact registry/queue membership, creates no limiter/permit pool, emits no torrent/watch-success event, and schedules nothing. The real HTTP file-add route retains its success/error envelope contract. | `daemon::tests::shared_add_persistence_failure_restores_exact_state_and_has_no_side_effects`, `daemon::tests::api_add_uses_shared_injected_rollback_without_event_or_schedule`, `api_torrent_file_add_retains_envelope_and_shared_rollback_contract` in `crates/swarmotterd/tests/daemon_download.rs` |
| Outcomes and actions | Only the four parser variants are permanent and move to failure; transient storage/persistence errors stay and retry. Existing destinations remain byte-for-byte unchanged, primary outcome survives in history/event, `post_action_error` is populated, and the fingerprint is not retried. | `daemon::tests::watch_error_classification_has_only_the_four_permanent_variants`, `daemon::tests::watch_permanent_failure_moves_while_transient_stays_and_retries`, `daemon::tests::watch_destination_collision_preserves_both_files_and_processes_once` |
| Bounded history, events, and UI | Entry 10,001 evicts entry 1. Imported/duplicate/failure events expose stable payloads only after determination. The Web UI distinguishes all four outcomes and warns for a post-action error while retaining compatibility fields. | `daemon::tests::watch_history_evicts_oldest_entry_at_ten_thousand_and_one`, broker assertions in the daemon tests above, `swarmotter_web::tests::web_ui_renders_stable_watch_outcomes_and_post_action_errors`, `node crates/swarmotter-web/tests/watch-history.test.js` |

Run the watch renderer harness directly with the other Web checks:

```bash
node crates/swarmotter-web/tests/watch-history.test.js
```

### Contained HTTP, webseed, and tracker scrape acceptance matrix

ADR-0055 is complete only when local generated fixtures cross the shared
contained transport and the live scheduling/API/UI boundaries.

| Capability | Required assertions | Acceptance evidence |
| --- | --- | --- |
| HTTP/1 framing and bounds | Content-Length and chunked finish before EOF; legal close-delimited bodies finish on EOF; truncated/malformed chunks, excessive decoded bytes, header bytes/counts, and driver/body errors retain typed context and close the connection. | All 15 `net::http::tests`, including `content_length_and_chunked_complete_without_waiting_for_eof`, `legal_close_delimited_body_is_decoded`, `truncated_and_malformed_chunk_bodies_are_typed_protocol_errors`, `decoded_tracker_cap_fails_on_first_excess_and_closes_connection`, and both logical-timeout cases. |
| Redirect, authority, and containment | Follow at most five redirects, reject the sixth/loops/bad Location/status/downgrade, allow HTTPS upgrade and cross-host hops, repeat binder resolution/connect for every hop, preserve origin-form and exact non-default/IPv6 Host authority, and construct no general client/raw socket. | `tracker_redirect_loop_and_five_follow_boundary_have_exact_request_counts`, `tracker_redirect_validation_and_status_errors_keep_status_context`, `relative_and_cross_host_redirects_repeat_binder_resolution_and_connect`, `https_upgrade_uses_injected_trust_and_downgrade_is_rejected`, `origin_form_and_host_authority_keep_nondefault_port_and_ipv6_brackets`, `production_http_path_has_no_general_client_resolver_pool_or_raw_socket`. |
| Exact webseed ranges | Preserve Range across redirects; require final 206, one exact Content-Range, exact decoded length, and immediate rejection of short/excess/200/missing/wrong responses for both framed forms. | `net::http::tests::webseed_range_policy_covers_exact_redirect_and_all_mismatch_cases`, plus generated local swarm webseed download. |
| Bounded scrape protocol | Derive `announce`, `announce.php`, and suffix paths; preserve unrelated query text; send one binary hash pair; make no call for unsupported/UDP; require every exact 20-byte key and all nonnegative counts; use the same contained HTTP and injected-trust HTTPS client. | All seven core scrape tests in `tracker::tests`, including `contained_http_scrape_returns_only_exact_matching_counts` and `injected_trust_https_scrape_uses_the_same_contained_client`. |
| Runtime retention and scheduling | Initial/explicit reannounce, magnet real-hash discovery, and seeder activity schedule scrape. Failure preserves prior success counts; task panic is attributed and counted; list rows retain announce priority with scrape fallback. | `engine::tests::started_and_reannounce_paths_schedule_contained_scrapes`, `engine::tests::magnet_tracker_activity_scrapes_the_real_magnet_info_hash`, `daemon::tests::seeder_announce_schedules_scrape_into_the_shared_engine_state`, `engine::tests::scrape_failure_retains_last_success_counts_and_is_accounted`, `engine::tests::scrape_task_panic_is_retained_for_the_exact_tracker`, `daemon::tests::list_trackers_exposes_scrape_state_and_falls_back_without_announce_success`. |
| API and Web UI | Native rows expose stable additive status/time/count/error fields through the real router; compatibility fields remain; the tracker table shows and escapes status, time, counts, and errors. | `daemon::tests::tracker_scrape_snapshot_serializes_through_the_real_native_router`, `trackers_crud_and_bad_hash` in API integration, and `swarmotter_web::tests::web_ui_renders_and_escapes_tracker_scrape_state`. |

### Seeding policy, lifecycle, and accounting acceptance matrix

ADR-0052 is complete only when every row below passes through the named
production boundary. Helper-only assertions do not substitute for daemon/API/UI
entry-point coverage.

| Capability | Required production boundary and assertions | Acceptance evidence |
| --- | --- | --- |
| Effective policy | Resolve nullable per-torrent ratio/idle fields against `[seeding]`; explicit or inherited ratio zero stops immediately even when a fully verified import has no downloaded counter; nonzero targets retain the divide-by-zero guard; seed-forever suppresses both effective targets without erasing stored overrides; ratio targets reject negative and non-finite values. | `ratio::tests::zero_ratio_target_stops_without_download_accounting`, `ratio::tests::explicit_zero_overrides_inherited_targets`, `ratio::tests::effective_targets_distinguish_inherit_override_and_forever`, `config::tests::rejects_negative_and_non_finite_global_ratio_limits` |
| Durable wire state | Serialize exactly `not_eligible`, `queued`, `active`, `stopped_ratio`, `stopped_idle`, and `stopped_manual`; load legacy version-1 records with defaults and no version bump; round-trip every status and policy. | `models::torrent::tests::seeding_statuses_serialize_with_exact_wire_values`, `state_store::tests::legacy_state_defaults_absent_seeding_fields_without_version_bump`, `state_store::tests::every_seeding_status_round_trips_in_version_one_state` |
| Policy replacement transaction | Call `DaemonOps::set_torrent_seeding`; persist the complete replacement before reconciling live tasks; on write failure restore only the prior policy and prove state, status, registration, and task ownership never changed. | `daemon::tests::seeding_policy_persistence_failure_restores_policy_status_and_state` |
| Completion and seed slots | Drive real `DaemonRuntime` completion/reconciliation. Fully verified content is `completed` + `queued` until a slot exists, then `seeding` + `active`; `GlobalStats.active_seeds` equals live `SeedRegistry` registrations. Exercise ratio and idle stops, policy relaxation, seed-forever, manual pause, Resume, Start Now, forced task cancellation, listener failure, and removal. Every `torrent_changed` event must report the state present after reconciliation. | `daemon::tests::complete_seeding_lifecycle_policy_slots_tasks_and_limiter_identity_are_truthful`, `daemon::tests::failed_shared_listener_bind_does_not_register_or_announce_seeder`, `daemon::tests::reconcile_publishes_completion_events` |
| Restart and containment | Reconstruct eligible seeders after durable restore while leaving automatic and manual stops stopped. On containment loss, atomically stop the accepting task and registration, preserve recovery status/intent under `network_blocked`, report zero active seeds, and make list/detail/stats/events agree. On recovery, rebuild only eligible live intent and publish the reconstructed state. | `daemon::tests::restart_reconstructs_eligible_seeder_and_preserves_automatic_and_manual_stops`, `daemon::tests::active_seeding_containment_block_preserves_status_and_recovery_rebuilds_task` |
| Exact file accounting | Calculate completed bytes from intersections between verified piece byte ranges and each file range. Cover a short final piece and a piece spanning a multi-file boundary after restore, partial forced recheck, and full forced recheck; never derive file bytes from torrent-wide completion fraction. | `torrent::tests::exact_single_file_bytes_use_actual_final_piece_length`, `torrent::tests::exact_multi_file_bytes_split_verified_boundary_pieces`, `daemon::tests::single_file_final_piece_bytes_are_exact_after_restore_and_recheck`, `daemon::tests::boundary_file_bytes_are_exact_after_restore_and_each_recheck` |
| Live upload shaping | Start a production `DaemonRuntime` seeder and accepted TCP peer request. Prove the initial burst, the old 1 KiB/s window at 400 ms, then update through `DaemonOps::set_torrent_limits` and prove release in the bounded next window. Assert the persisted value and `Arc<RateLimiter>` identity in the daemon map and live registration do not change; global shaping remains an additional layer. | `daemon::tests::daemon_limit_update_changes_active_registered_upload_without_replacement` |
| Native API contract | Send strict `PUT /api/v1/torrents/:hash/seeding` requests through the real router. Require all and only `ratio_limit`, `idle_limit`, and `seed_forever`; reject missing/unknown/negative/non-finite/fractional-idle/overflow/wrong-type values as `invalid_argument`; verify stored and effective fields in both list and detail. | `native_seeding_put_replaces_policy_and_list_detail_are_truthful`, `native_seeding_put_rejects_non_replacement_and_invalid_values` in `crates/swarmotter-api/tests/api_integration.rs` |
| Web UI and compatibility | Render stored/effective/status values in Torrent Details, distinguish inherit from explicit zero, submit the exact replacement body, and retain rendered state while displaying a server rejection. Transmission and qBittorrent responses must retain only their previously documented ratio/uploaded fields; do not invent policy fields. | `node crates/swarmotter-web/tests/seeding-policy.test.js`, `swarmotter_web::tests::web_ui_renders_and_replaces_seeding_policy_without_optimistic_drift`, `compatibility_keeps_only_previously_supported_ratio_and_upload_fields` |

Run the executable DOM-state harness directly in addition to the Rust suite:

```bash
node crates/swarmotter-web/tests/seeding-policy.test.js
```

The matrix definition of done also requires `cargo fmt --all -- --check`,
`cargo check --locked --workspace --all-targets --all-features`,
`cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`,
`cargo test --locked --workspace --all-targets --all-features`, `node --check`
for every file below `crates/swarmotter-web/assets`, both executable DOM
harnesses, `mdbook build`, and `git diff --check` to pass. Documentation
must keep `design/requirements.md`, architecture/API/configuration design,
operator API/configuration/Web UI guides, the completion tracker, changelog,
and affected ADRs (including ADR-0052 and ADR-0054) aligned with the tested
behavior.

### Network containment tests

- Required interface missing.
- Required interface down.
- Source IP missing.
- Route invalid.
- Socket bind failure.
- VPN path removed while torrents are active.
- Torrent traffic blocked when fail-closed is active.
- API listener remains available when torrent data plane is blocked, if
  configured that way.
- Injected fake-probe live path-loss transition (ADR-0051): a mutable fake
  `InterfaceProbe` drives `network_health_tick()` directly without sleeping;
  flipping the required interface healthy-to-missing proves the gate blocks
  before teardown, data-plane registries empty, torrent/API status is blocked,
  and the control API still responds. Recovery consumes durable intent only for
  demonstrably live work, not paused/queued/ratio/idle-stopped or stale blocked
  records. A block followed immediately by allow still cancels old-generation
  tasks, and cancellation waiter registration has no lost-wakeup window.
  Concrete source/listener bind failure blocks immediately and stays latched
  across healthy probe ticks; only a full replacement with successful UDP and
  peer-listener bind validation clears it. Generic strict policy denial exposes
  `blocked_fail_closed` through the production control API.
- Config matrix (ADR-0051): omitted table/file, strict with path, explicit
  disabled, partial network table, env override, and `--check-config`.
- Privileged Linux namespace transition (ADR-0051):
  `scripts/test-network-containment-transition.sh` creates two temporary
  PID-qualified namespaces joined by a veth pair with no default route. It
  generates lawful payload/metainfo, runs a compact HTTP tracker and throttled
  TCP BitTorrent seed, registers the raw torrent through the real API, proves
  partial tracker-discovered peer-wire traffic, deletes the daemon veth, and
  requires `interface_missing`, `network_blocked`, stable partial bytes, empty
  scheduler registries, diagnostics, and a responsive control route. CI builds
  and invokes the script as its normal user. Only `sudo ip` handles namespace
  and link operations; `setpriv` gives the daemon only `CAP_NET_RAW` for
  `SO_BINDTODEVICE`, while the tracker, seed, generator, and curl clients have
  no capabilities.

### Storage tests

- Fast resume.
- Same-size changed-file detection and corrupt-resume quarantine.
- Forced recheck.
- Interrupted write recovery.
- Missing file detection.
- Partial download behavior.
- File selection behavior.
- Cross-torrent storage path collision rejection.
- Move complete behavior.
- Rename path behavior.

### Local swarm tests

- Tracker-based peer discovery (HTTP, compact peers): covered
- Tracker-based peer discovery (UDP/BEP 15, compact peers): covered
- Download completion: covered (generated payload, in-process seed peer)
- Direct-peer (PEX/DHT-style) discovery: covered (directly-supplied seed)
- Seeding/upload behavior: covered (the shared inbound `SeederHub` routes
  multiple completed torrents through one contained listener and owns accepted
  sessions until completion or cancellation)
- Daemon-driven download through `DaemonOps`: covered
- Magnet metadata fetch: covered (BEP 9 ut_metadata, info-hash verified)
- DHT-based peer discovery: covered (local KRPC `get_peers` fixture)
- PEX-based peer exchange: covered (BEP 10/11, peer discovered via PEX)
- uTP (BEP 29) peer transport: covered (a contained uTP-capable seed serves a
  generated payload over the contained UDP socket; the engine completes the
  download over uTP, verifying piece hashes and final file contents; a
  fail-closed test proves the `BlockedBinder` blocks uTP swarm downloads)
- Recheck after completion: covered via `StorageIo::recheck`
- Per-torrent health during active download: an actively-downloading
  generated lawful local payload reports a non-zero health score and at
  least one bar, computed from the live engine state.
- Peer-session budgets (ADR-0053): generated stalling swarms sample live
  diagnostics while five torrents share a global cap, while one torrent has a
  smaller per-torrent cap, and while normal parallel and endgame sessions hold
  permits. Serial cancellation/removal, BEP 9 metadata, TCP connect/handshake/
  EOF/cancellation, uTP metadata, mixed inbound/outbound routing, inbound
  denial, unlimited observation, panic release, and concurrent snapshot churn
  have focused production-path or RAII tests.
- Peer-limit reconstruction tests cover PATCH and full PUT success,
  post-provisional failure, post-reconstruction persistence failure, exact pool
  identity/lifecycle/queue/config/state restoration, blocked and recovering
  containment, occupied listeners, unrelated-start exclusion, old/new pool
  non-overlap, candidate-only task rollback, and pre-commit selfish-completion
  suppression.

### Scale tests

- Queue data-structure tests cover 10,000-entry add/remove/reorder behavior.
- Daemon lifecycle tests cover 1,000- and 10,000-record stale-active recovery,
  metadata retry backoff, desired active cap enforcement, and bulk removal.
- API integration tests cover 1,000-torrent rapid add, bulk add, and
  query/filter/group behavior with generated lawful magnets.
- Ignored opt-in scale tests cover larger synthetic flows:
  `ignored_thousand_mixed_state_torrents_keep_scheduler_bounds` validates a
  1,200-record daemon library across queued, checking, downloading metadata,
  downloading, seeding, paused, completed, and error states while asserting
  scheduler request/grant bounds.
  `ignored_scale_harness_add_query_retry_remove_reset_2000_torrents` validates
  a 2,000-torrent API add/query/recheck/reannounce/remove/reset flow using
  generated lawful torrent files.

Run ignored scale tests explicitly when validating large-library behavior:

```bash
cargo test -p swarmotterd ignored_thousand_mixed_state_torrents_keep_scheduler_bounds -- --ignored
cargo test -p swarmotter-api --test scale_harness -- --ignored
```

## Test data

Tests must use clearly lawful sources (generated local torrents, public-domain
files, open datasets, Linux distribution examples, project-owned sample files).
See `content-policy.md`.

## TODO

- Keep this document aligned with `requirements.md`.
