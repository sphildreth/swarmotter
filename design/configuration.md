# Configuration Design Notes

This document records SwarmOtter's configuration architecture and compatibility
contract. User-facing examples and option reference belong in the published
mdBook page: `../docs/configuration.md`.

The implementation lives in `swarmotter-core::config`.

## Sources

SwarmOtter is configured through a TOML configuration file plus environment
variable overrides. Environment overrides use the `SWARMOTTER_` prefix with
nested fields separated by `__`.

Invalid required configuration must produce clear startup errors. Runtime
updates use two API paths:

- `PATCH /api/v1/settings` for live-safe partial updates.
- `PUT /api/v1/settings` for full config replacement after validation.

## Design constraints

- Defaults should be safe and operator-friendly.
- Strict network containment must require an enforceable data-plane path.
- `NetworkConfig::default()` and Serde omission both select strict mode. An
  omitted `[network]` table therefore fails validation until an interface,
  source address, or current namespace is configured; only explicit
  `mode = "disabled"` disables in-process containment (ADR-0051).
- API auth must require a non-empty token when enabled.
- Chrome extension API access requires authenticated mode plus the configured
  API token at the outer browser-origin guard. Merely retaining `auth_token`
  while `require_auth = false` never authorizes an extension Origin; no separate
  extension allowlist is implied (ADR-0044/ADR-0049).
- Non-loopback control-plane listeners should use API auth by default; an
  operator may deliberately trust the reachable network and disable it
  (ADR-0049).
- `GET /api/v1/settings` must redact `api.auth_token`.
- Full config replacement must preserve the existing auth token when the
  request omits it.
- Runtime updates must report fields that require restart.
- A concrete bind failure is not cleared by periodic interface health. It is
  latched until `PUT /api/v1/settings` supplies a full validated replacement
  whose ephemeral contained UDP and peer-listener binds both succeed. A failed
  replacement leaves the prior configuration and latch authoritative.
- Unknown top-level and nested fields must be rejected rather than silently
  ignored.
- Environment overrides must be applied before final validation and pass
  through the same validation as file config.
- `seeding.global_ratio_limit`, when present, must be finite and non-negative;
  invalid values are rejected as `invalid_config` before lifecycle evaluation.
- `bandwidth.max_peers` is the exact process-wide peer-session limit shared by
  inbound and outbound TCP/uTP sessions for every torrent. Zero is unlimited.
  `max_peers_per_torrent` is an additional shared inbound/outbound limit; zero
  selects the daemon default of 64. Trackers, webseeds, DHT, and DNS are outside
  this peer-session budget (ADR-0053).
- Peer limits cannot exceed Tokio's runtime semaphore maximum. Live changes
  through PATCH or full PUT replace pool objects and reconstruct peer-bearing
  work transactionally; failed reconstruction or persistence restores the old
  config, pool identities, tasks, and persistent files.
- `[peer_filter]` is disabled by default and defines one global peer-admission
  policy. It accepts individual IPs, CIDRs, inclusive ranges, bounded local
  eMule/PeerGuardian-style import paths, manual IP bans, and printable
  peer-ID-prefix rules. Remote blocklist URLs are intentionally unsupported.
  Rules compile before startup or replacement commits; a failed compile leaves
  the old policy active, and construction failures deny rather than silently
  admitting peers. The policy applies before contained peer connection/service
  but never changes route binding or fail-closed containment (ADR-0058).
- `[[storage.root_controls]]` is a repeatable local-storage scheduling surface.
  Controls use the most-specific matching lexical active-write root; duplicate
  normalized paths are invalid while nested paths are intentional. Each root
  can independently cap active engines, declared active payload bytes,
  sustained verified payload writes, and concurrent full rechecks. A zero cap
  is unlimited. Root admission is atomic, changes never weaken containment,
  and diagnostics expose both the matching control and saturation state
  (ADR-0056).
- Per-torrent seeding overrides are durable torrent state, not configuration
  fields. `null` overrides inherit `[seeding]` globals, while `seed_forever`
  suppresses effective targets without erasing stored overrides (ADR-0052).
- `[profiles]` holds named policy definitions and deterministic
  label-to-profile mappings. An explicitly attached add/watch/torrent profile
  wins over a matching label; per-field torrent overrides win last. Resolved
  storage and the initial admission decision are captured in durable torrent
  state at registration, while queue priority, seeding, and bandwidth resolve
  live for inheriting records. Reassignment and later label changes preserve
  existing storage paths; snapshot-bearing torrents retain their captured
  admission when profile/global start settings change. Legacy records are
  migrated transactionally on a profile replacement (ADR-0057).
- Each `[[watch]]` root is a lexical path boundary, not a canonicalized one.
  Scans reject a symlink root, skip child symlinks, require two identical
  length/modified-time observations, and serialize manual/background runs.
  Whitespace-only `path`, `archive_dir`, and `failure_dir` values are invalid;
  an action directory must not normalize exactly to its watch root. A strict
  in-root archive/failure descendant and its subtree are excluded only from
  that configured folder's scan using component-aware path comparison. A
  separately configured overlapping root retains its own independent view.
  `archive_dir` and `failure_dir` actions create missing directories but never
  overwrite an existing destination. Observation/history state is deliberately
  not durable (ADR-0054).
- Transport option changes are release-facing compatibility decisions.

## Compatibility boundaries

Configuration table names, field names, environment override names, defaults,
and validation behavior are release-facing. Breaking changes should follow
`VERSIONING_GUIDE.md`.

## Metadata trust boundary

Bencoded torrent metadata ingress — `.torrent` uploads, bulk base64 metainfo,
magnet `info` dicts fetched via BEP 9, and watch-folder files — shares one
bounded parser in `swarmotter-core` (see ADR-0050). The shared
`MAX_TORRENT_METADATA_BYTES` (16 MiB) limit is enforced by the core parser
before any piece-sized allocation, independent of `api.max_request_body_bytes`
(which may be higher for other request payloads). Restored daemon state is
JSON; its piece hashes require an exact 20-byte decode and each restored
`TorrentMeta` must pass `TorrentMeta::validate()` before runtime use.
Watch files are read through a bounded reader that checks stable size before
allocation and verifies path/opened-file length and modified time before and
after the read. A stable oversize file is rejected with a typed permanent input
error. Any metadata change discards the bytes, resets the in-memory observation
to one stable scan, and produces no terminal result/action. Malformed metadata
never panics the daemon (ADR-0054).
- Autopilot control is compatible through `[autopilot].mode`, with exactly
  three values: `disabled`, `observe`, and `act`. Default is `act`.
  In `act` mode, stalled active torrents with no recent block progress are
  eligible for bounded queue-slot release so queued torrents can proceed.

Compatibility adapter settings belong under `[compatibility.*]` so optional
adapter surfaces remain isolated from native daemon configuration.

Compatibility adapters, including `[compatibility.qbittorrent]`, are contract
surfaces that do not change torrent transport behavior. They must route through
the native API and keep containment and socket policy unchanged.

`[torrent].encryption_mode` is the protocol-transport compatibility option for
peer-wire negotiation:

- `disabled`: permit plaintext handshakes.
- `preferred` (default): TCP peer attempts use MSE/PE first, with plaintext
  fallback when allowed; TCP/uTP ordering still follows `torrent.utp_prefer_tcp`.
- `required`: refuse plaintext and require encrypted stream negotiation.

Changing this mode stops and rebuilds the complete torrent data plane before
the replacement is reported as applied. Engines, tracker sidecars, DHT work,
the shared listener, and accepted sessions created under the previous policy
are awaited before eligible tasks start with the new policy (ADR-0047).

This phase is TCP-only; no uTP encryption is included yet. Named policy
profiles are configuration and lifecycle policy only; they do not introduce a
per-profile or per-torrent network path.

All containment-affecting full replacements share the data-plane transaction
lock. The live containment gate blocks socket creation immediately; each block
advances its cancellation generation so tasks from an older generation exit
even when recovery follows before they next poll. Configuration replacement
never changes strict mode to disabled as a recovery fallback.

## Maintenance

When configuration behavior changes:

1. Update `swarmotter-core::config` and validation tests.
2. Update any affected API settings handlers.
3. Update `../docs/configuration.md` for user-facing examples and option
   reference.
4. Update this document only when the configuration model or compatibility
   contract changes.
