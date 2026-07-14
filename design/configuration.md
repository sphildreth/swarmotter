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
- `[network.socks5]` is an explicit TCP `CONNECT` overlay, never an alternate
  route. Its proxy hostname is resolved and connected through the contained
  binder; tracker/webseed target hostnames use SOCKS domain-form remote DNS and
  proxy failure never retries a target directly. It supports no-authentication
  or complete RFC 1929 username/password credentials (ADR-0062).
- SOCKS5 configuration is TCP-only: enabling it requires `dht.enabled = false`
  and `torrent.utp_enabled = false`; the proxy layer rejects UDP tracker, DHT,
  uTP, and direct target-resolution paths rather than falling back. Router
  mapping remains a separately contained local-gateway operation.
- API auth must require a non-empty token when enabled.
- Chrome extension API access requires authenticated mode plus the configured
  API token at the outer browser-origin guard. Merely retaining `auth_token`
  while `require_auth = false` never authorizes an extension Origin; no separate
  extension allowlist is implied (ADR-0044/ADR-0049).
- Non-loopback control-plane listeners should use API auth by default; an
  operator may deliberately trust the reachable network and disable it
  (ADR-0049).
- `GET /api/v1/settings` and full-replacement responses must redact
  `api.auth_token` and `network.socks5.password`.
- Full config replacement must preserve the existing auth token when the
  request omits it, and preserve a redacted SOCKS5 password only when its
  username is unchanged.
- Runtime updates must report fields that require restart.
- A concrete bind failure is not cleared by periodic interface health. It is
  latched until `PUT /api/v1/settings` supplies a full validated replacement
  whose peer-listener bind and, outside SOCKS5 TCP-only mode, ephemeral
  contained UDP bind succeed. A failed replacement leaves the prior
  configuration and latch authoritative.
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
- `[port_mapping]` is disabled by default and controls NAT-PMP/UPnP forwarding
  of the TCP peer listener. It is valid only with strict fail-closed
  containment and an explicit required interface. Gateway discovery, mapping,
  renewal, and best-effort deletion all use `NetworkBinder`; an unavailable
  router or blocked path is observable but never causes a default-route
  fallback or changes healthy torrent scheduling (ADR-0059).
- `[port_test]` is disabled by default and requires an operator-owned HTTP(S)
  endpoint before it performs any outbound diagnostic. Requests run through
  `NetworkBinder`, have bounded timeout/cache behavior, never expose the
  configured endpoint in routine status, and record informational results
  without changing the containment gate (ADR-0060).
- `[[storage.root_controls]]` is a repeatable local-storage scheduling surface.
  Controls use the most-specific matching lexical active-write root; duplicate
  normalized paths are invalid while nested paths are intentional. Each root
  can independently cap active engines, declared active payload bytes,
  sustained verified payload writes, and concurrent full rechecks. A zero cap
  is unlimited. Root admission is atomic, changes never weaken containment,
  and diagnostics expose both the matching control and saturation state
  (ADR-0056).
- `[storage]` also separates durable placement from payload placement:
  `resume_dir` stores info-hash-named fast-resume metadata, `state_dir` is the
  default SQLite state-store directory after restart when no CLI/environment state path
  is explicit, and `temp_dir` is only the fallback payload root when no
  download directory is configured. State/resume atomic temporary files stay
  beside their targets to preserve same-filesystem rename and directory-sync
  semantics. `storage.state_dir` replacement is restart-required;
  `storage.resume_dir` replacement is rejected while incomplete or
  selected-file resume state remains; a fallback `temp_dir` cannot change
  while an existing torrent depends on it (ADR-0064).
- `storage.cow_strategy = "conservative"` is the default and never changes
  filesystem flags. `disable_for_new_files` is an explicit Linux Btrfs-only
  NOCOW request applied before a newly created file is sized or written; it
  rejects unsupported roots, never changes an existing file, and rejects an
  unflagged existing file for further writes rather than silently changing its
  strategy. Sparse and preallocation remain separate capacity/layout choices,
  while piece-hash verification remains the payload-integrity authority
  (ADR-0064).
- Per-torrent seeding overrides are durable torrent state, not configuration
  fields. `null` overrides inherit `[seeding]` globals, while `seed_forever`
  suppresses effective targets without erasing stored overrides (ADR-0052).
- `[profiles]` holds named policy definitions and deterministic
  label-to-profile mappings. An explicitly attached add/watch/torrent profile
  wins over a matching label; per-field torrent overrides win last. Resolved
  storage and the initial admission decision are captured in durable torrent
  state at registration. Intake file-selection and content-organization rules
  are likewise snapshotted before payload transfer, while queue priority,
  seeding, bandwidth, and optional `encryption_mode` resolve live for
  inheriting records. Reassignment and later
  label changes preserve existing storage paths; snapshot-bearing torrents
  retain their captured admission when profile/global start settings change.
  Legacy records are migrated transactionally on a profile replacement
  (ADR-0057, ADR-0063).
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
(which may be higher for other request payloads). Restored daemon state uses a
versioned local SQLite store; validated legacy JSON migrates in place on its
first successful save, and each restored `TorrentMeta` must pass
`TorrentMeta::validate()` before runtime use.
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

qBittorrent categories remain labels in durable native state. An exact category
match to a configured named profile may select that profile at registration;
otherwise regular deterministic label mapping applies. Transmission's optional
`profile` compatibility field is translated to the same assignment and accepts
explicit `null` to clear it. Compatibility status, lifecycle, location, file,
and tracker operations delegate to native durable operations rather than a
parallel state store (ADR-0061).

`[torrent].encryption_mode` is the global fallback for peer-wire encryption
over contained TCP and uTP byte streams:

- `disabled`: use plaintext peer-wire sessions only.
- `preferred` (default): attempt MSE/PE first, then reconnect the same selected
  contained TCP or uTP transport as plaintext if negotiation fails. TCP/uTP
  ordering still follows `torrent.utp_prefer_tcp`.
- `required`: negotiate MSE/PE over the selected contained TCP or uTP stream
  and refuse plaintext without a fallback retry.

Pure-v2 transfers retain their full SHA-256 identity locally while using the
explicit 20-byte `PeerInfoHash` required by the peer/MSE wire format.
`preferred` and `required` therefore preserve the same contained negotiation
and no-plaintext-fallback rules as v1/hybrid work; a peer-wire truncation is
never accepted as a durable torrent identity.

Profiles may set an optional `profiles.profiles.<name>.encryption_mode`, and a
durable torrent `policy.overrides.encryption_mode` can be set or explicitly
cleared through the native API. Resolution is torrent override, selected
profile (explicit assignment or label), then this global fallback. The
effective-policy response records the source. A global mode replacement still
stops and rebuilds the complete data plane before reporting success. A profile
or label-map replacement updates active seeder registrations for future inbound
TCP sessions and restarts only active download/metadata engines whose resolved
mode changed after persistence; existing negotiated sessions retain their wire
stream (ADR-0047, ADR-0063).

MSE/PE itself never opens a socket: it wraps the `NetworkBinder`-created stream.
Named policy profiles do not introduce a per-profile or per-torrent network
path, proxy, or production inbound-uTP listener.

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
