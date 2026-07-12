# Configuration

SwarmOtter uses a TOML configuration file plus optional environment variable
overrides. The daemon validates configuration at startup and refuses invalid
settings.

Most sections can be omitted entirely. When a section is present with only a
few fields, unspecified fields use their documented defaults.

## Environment overrides

Environment variables use the `SWARMOTTER_` prefix. Nested fields are separated
with double underscores:

```bash
SWARMOTTER_API__BIND_ADDRESS=0.0.0.0:9091
SWARMOTTER_API__REQUIRE_AUTH=true
SWARMOTTER_API__AUTH_TOKEN=replace-with-a-long-random-token
SWARMOTTER_AUTOPILOT__MODE=act
SWARMOTTER_NETWORK__MODE=strict
SWARMOTTER_NETWORK__REQUIRED_INTERFACE=br0
SWARMOTTER_TORRENT__LISTEN_PORT=51413
SWARMOTTER_TORRENT__ENCRYPTION_MODE=preferred
SWARMOTTER_COMPATIBILITY__QBITTORRENT__ENABLED=true
SWARMOTTER_COMPATIBILITY__TRANSMISSION__ENABLED=true
```

## Runtime configuration editing

SwarmOtter exposes two update modes:

- `PATCH /api/v1/settings` updates live-safe fields (bandwidth, queue, and seeding policy).
- `PUT /api/v1/settings` replaces the full config after validation and persists it atomically.
  The existing `api.auth_token` is preserved when omitted from the request body.

The `PUT /api/v1/settings` response reports which fields were applied live, which
fields require restart, and whether the write was persisted.
Supported package and Compose deployments provide a private writable config
directory. If persistence is unavailable, the Web UI can fall back to PATCH for
only bandwidth, queue, seeding, and autopilot settings.

Network containment, peer listen port, IP-family policy, uTP policy, peer
encryption mode, and DHT changes are applied live by stopping the complete old
data-plane task set and rebuilding eligible tasks with fresh binders. API
listener/body-limit and logging destination changes are reported as requiring a
process restart.

Changing a global storage root is rejected when an existing torrent still
depends on the old root. Assign explicit locations with move-data before
changing `storage.download_dir`; complete or remove incomplete payloads before
changing `storage.incomplete_dir`. This prevents a settings update from making
existing payload data appear missing.

Unknown top-level or nested TOML fields are rejected. This prevents a misspelled
containment or security setting from silently falling back to a default.

## Durable daemon state

Torrent records, queue order, labels, file choices, and per-torrent controls
are stored separately from configuration in a versioned state file. Select its
path with `--state-file PATH` or `SWARMOTTER_STATE_FILE=PATH`.

Without an explicit path, the daemon uses the first available location:

1. The systemd `STATE_DIRECTORY`, as `state.json`.
2. `/var/lib/swarmotter/state.json` when that directory exists.
3. `$XDG_STATE_HOME/swarmotter/state.json`.
4. `$HOME/.local/state/swarmotter/state.json`.
5. `./swarmotter-state.json`.

The state file is atomically replaced and mode `0600` on Unix. Corrupt or
unsupported state stops startup with an explicit error instead of presenting
an empty library. Restored completed torrents are rechecked before seeding.

## Common configuration: bind torrents to `br0`

Use this when the interface name is stable but addresses are assigned by DHCP,
SLAAC, or router advertisements.

```toml
[api]
bind_address = "0.0.0.0:9091"
require_auth = true
auth_token = "replace-with-a-long-random-token"

[storage]
download_dir = "/mnt/incoming/swarmotter/downloads"
incomplete_dir = "/mnt/incoming/swarmotter/incomplete"

[network]
required_interface = "br0"
allow_ipv6 = true
fail_closed = true
validate_route = true
validate_dns = true

[torrent]
listen_port = 51413
allow_ipv6 = true
utp_enabled = true
utp_prefer_tcp = true
encryption_mode = "preferred"
```

If a `[network]` table contains `required_interface` but omits `mode`,
SwarmOtter treats it as strict containment. Setting `mode = "strict"`
explicitly is also valid.

On Linux, this binds torrent data-plane sockets to the named interface using
`SO_BINDTODEVICE`. The kernel may choose the current IPv4 or IPv6 source
address from that interface, so address changes do not break the configuration.

On Linux, SwarmOtter validates DNS for this interface mode before resolving
torrent hostnames. The common systemd-resolved setup is accepted when
`resolvectl dns br0` reports link DNS servers. Static nameservers in
`/etc/resolv.conf` are accepted when their routes go through `br0`. If DNS
cannot be proven constrained to the configured path, hostname tracker and DHT
bootstrap resolution fails closed instead of using an unconstrained resolver.

## Static source address containment

Use source addresses when the address is stable and should be enforced.

```toml
[network]
mode = "strict"
required_interface = "tun0"
required_source_ipv4 = "10.8.0.2"
allow_ipv6 = false
fail_closed = true
validate_route = true
validate_dns = true
```

For dual-stack static containment:

```toml
[network]
mode = "strict"
required_interface = "tun0"
required_source_ipv4 = "10.8.0.2"
required_source_ipv6 = "fd00:8::2"
allow_ipv6 = true
fail_closed = true
validate_route = true
validate_dns = true

[torrent]
allow_ipv6 = true
```

If `required_source_ipv6` is set, `network.allow_ipv6` must be `true`.

## Network namespace containment

Use a network namespace when the daemon should run inside a prebuilt contained
network stack, such as a VPN namespace.

```toml
[network]
mode = "strict"
required_network_namespace = "vpn"
allow_ipv6 = true
fail_closed = true
validate_route = true
validate_dns = false
```

The process must actually be running in the required namespace. If it is not,
network health reports `network_namespace_unavailable`.

## Throughput-oriented defaults

The default torrent data-plane settings favor throughput while preserving
fail-closed behavior when strict containment is configured:

- `torrent.utp_enabled = true`
- `torrent.utp_prefer_tcp = true`
- `torrent.allow_ipv6 = true`
- `network.allow_ipv6 = true`
- `torrent.encryption_mode = "preferred"`
- `dht.enabled = true`
- `pex.enabled = true`
- Bandwidth limits default to `0`, meaning unlimited.
- Peer limits default to `0`, meaning unlimited where the specific limit uses
  that convention.

Use bandwidth and queue limits when the host needs resource caps. Leaving them
unlimited or high is better for raw transfer throughput.

## Adaptive autopilot controls

The adaptive swarm performance autopilot is configurable and can be staged safely:

- Global behavior is controlled by `[autopilot].mode`, defaulting to `act`.
- `mode` is one of `disabled`, `observe`, or `act`.
- In `observe` mode, SwarmOtter reports slowdown causes without applying
  tuning actions.
- In `act` mode, SwarmOtter can apply bounded daemon/engine actions such as
  discovery refresh, peer-worker adjustment, peer-backoff relaxation, and
  queue-slot release.
- Queue-slot release is prioritized for active torrents with no recent block
  progress so queued torrents are not blocked behind stalled work.
- Unfinished engine exits and retryable metadata-discovery exits return to the
  queue with bounded retry backoff, which prevents stale active-looking records
  from occupying download slots.
- Queue reconciliation also recovers active records that no longer have a
  running engine task, returning them to the queue behind waiting work.
- Per-torrent control is an override through API/UI.
- Recommendations are constrained by existing hard caps and never ignore
  `bandwidth`, `queue`, or containment limits.

Example:

```toml
[autopilot]
mode = "act"  # optional; defaults to act
```

## Option reference

### `[api]`

| Option | Default | Meaning |
| --- | --- | --- |
| `bind_address` | `"127.0.0.1:9091"` | Address for the Web UI and API control plane. |
| `require_auth` | `false` | Requires API/Web UI token auth. Strongly recommended for non-loopback listeners. |
| `auth_token` | unset | Required when `require_auth = true`. |
| `max_request_body_bytes` | `16777216` | Maximum API request body size, including `.torrent` uploads. |

### `[compatibility.qbittorrent]`

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Enable the optional qBittorrent-compatible compatibility endpoint at `/api/v2`. |

When enabled, `/api/v2` is an optional compatibility adapter over native SwarmOtter
operations and does not add any separate torrent data-plane pathways.

Authentication follows `api.require_auth`:

- If auth is required, `Authorization: Bearer <token>` and
  `X-SwarmOtter-Auth: <token>` are accepted.
- For qBittorrent-style cookie sessions, POST to `/api/v2/auth/login` with
  credentials where `password` matches `api.auth_token`; the response sets a `SID`
  cookie that can be reused for subsequent `/api/v2` requests.

Represented compatibility endpoints used by automation include:

- `GET /api/v2/app/version`
- `GET /api/v2/app/webapiVersion`
- `GET /api/v2/torrents/info`
- `POST /api/v2/torrents/add`
- `POST /api/v2/torrents/delete`
- `POST /api/v2/torrents/pause`
- `POST /api/v2/torrents/resume`
- `POST /api/v2/torrents/start`
- `POST /api/v2/torrents/stop`
- `POST /api/v2/torrents/setCategory`

The adapter is intentionally limited: no indexer/search/discovery endpoints are
exposed through the compatibility surface.

### `[compatibility.transmission]`

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Enable the optional Transmission RPC compatibility endpoint at `/transmission/rpc`. |

When enabled, SwarmOtter maps compatible requests to existing daemon operations.
Auth mapping follows `api.require_auth`: when auth is required, Transmission
Basic auth password must match `api.auth_token`; username is not security-
significant.

The adapter supports common Transmission session, torrent lifecycle, queue, and
helper calls, including mutating calls such as `torrent-remove`, `torrent-set`,
`torrent-set-location`, and `torrent-rename-path`. `torrent-remove` maps
`delete-local-data` / `delete_local_data` to SwarmOtter's native delete-data
behavior, so clients using that flag can delete payload data.

`torrent-add` accepts only:

- magnet links (`filename`)
- base64-encoded metainfo (`metainfo`)

Remote HTTP/HTTPS URLs for torrent metadata are rejected by this adapter.

### `[storage]`

| Option | Default | Meaning |
| --- | --- | --- |
| `download_dir` | unset | Final directory for verified completed downloads. |
| `incomplete_dir` | unset | Active write directory for incomplete downloads. |
| `preallocate` | `false` | Pre-size files before downloading. |
| `sparse` | `true` | When `false`, active payload files are sized up front even if `preallocate = false`. |
| `minimum_free_space_bytes` | `0` | If > 0, reject new adds when target-root usable space falls below this number of bytes. |
| `minimum_free_space_percent` | `0` | If > 0, reject new adds when free space on the target root falls below this percent of total root size. |

The minimum reserve fields apply to add/start-time preflight and are checked
before payload data is written. Both fields are optional; when both are set,
the preflight uses the stricter reserve.

When `incomplete_dir` is set, SwarmOtter writes partial pieces and partial
fast-resume metadata there while the torrent is downloading. After every piece
is verified, the daemon moves the torrent data into `download_dir` and removes
SwarmOtter fast-resume metadata so the completed directory contains only user
payload files. If `incomplete_dir` is unset, the active and final directory are
both `download_dir`. With `preallocate = false` and `sparse = true`, active
single-file torrents still create a zero-length placeholder in
`incomplete_dir` when the engine starts; the file is not sized to the full
payload until data is written. With `sparse = false`, active payload files are
sized up front.

### `[network]`

| Option | Default | Meaning |
| --- | --- | --- |
| `mode` | `strict` | Torrent data-plane containment mode. An omitted `[network]` table produces strict mode without a path, which fails startup with `invalid_config`. Explicit `mode = "disabled"` is for development or a separately enforced boundary only. See ADR-0051. |
| `required_interface` | unset | Interface name, such as `br0` or `tun0`. |
| `required_source_ipv4` | unset | Required IPv4 source address. |
| `required_source_ipv6` | unset | Required IPv6 source address. |
| `required_network_namespace` | unset | Required Linux network namespace name. |
| `allow_ipv6` | `true` | Enables IPv6 torrent networking when the path is contained. |
| `fail_closed` | `true` | Blocks torrent networking when strict containment is unhealthy. |
| `validate_route` | `false` | Requires route validation when supported by the probe. |
| `validate_dns` | `false` | Reports `dns_not_constrained` in network health when DNS cannot be proven constrained. Hostname resolution is still fail-closed unless DNS is constrained or a network namespace is used. |

Strict mode is the default and requires at least one enforceable path: interface,
source address, or network namespace. **Breaking change (ADR-0051):** an omitted
`[network]` table no longer selects disabled mode. It produces strict mode
without a path and the daemon fails at startup with `invalid_config`. Existing
users who relied on the disabled default must configure a strict path or set
`mode = "disabled"` explicitly. Never infer disabled mode from a missing
file/table, platform, bind failure, or unavailable interface; never auto-change
strict to preferred or disabled.

The daemon observes one process-wide containment gate shared by every torrent
data-plane component (binder, DHT, listener, engine, seeder, tracker, webseed,
metadata). On live path loss the gate blocks immediately, the inbound listener
and DHT runner stop, data-plane tasks are aborted, and active torrents enter
`network_blocked` while the control plane remains available. On recovery the
gate reopens and only work carrying durable formerly-live recovery intent
resumes; paused, queued, stale blocked, and automatically seed-stopped torrents
remain stopped. Every block advances the gate generation, so tasks from before
a blocked interval cancel even if recovery is immediate.

Concrete bind failures block synchronously and latch `socket_bind_failed` (or
`blocked_fail_closed` for a generic policy denial). A healthy probe alone does
not clear the latch. Only an explicit `PUT /api/v1/settings` replacement whose
contained UDP and peer-listener bind validation succeeds may recover it; a
failed replacement preserves the prior configuration and blocked state. On
Linux, route and DNS path validation invoke `ip route get`; direct and tarball
installs must provide the `ip` utility from the distribution's `iproute2` or
`iproute` package.

### `[autopilot]`

| Option | Default | Meaning |
| --- | --- | --- |
| `mode` | `act` | Autopilot mode: `disabled` (no analysis), `observe` (reasons only), or `act` (reasons plus bounded automatic actions). |

### `[torrent]`

| Option | Default | Meaning |
| --- | --- | --- |
| `listen_port` | `51413` | Inbound peer TCP and DHT/uTP UDP port. |
| `allow_ipv6` | `true` | Enables IPv6 peers when network containment also allows IPv6; when false, IPv6 peers are filtered before connecting. |
| `utp_enabled` | `true` | Enables uTP peer transport through contained UDP sockets. |
| `utp_prefer_tcp` | `true` | Tries TCP first, with uTP fallback. |
| `encryption_mode` | `preferred` | TCP MSE/PE peer wire mode. `disabled` permits plaintext handshakes. `preferred` enables MSE/PE with plaintext fallback for TCP attempts while preserving the configured TCP/uTP preference. `required` refuses plaintext and requires encrypted TCP stream negotiation. Changing this setting rebuilds active data-plane tasks before it is reported as applied. |
| `selfish` | `false` | Removes a torrent after verified completion and does not seed it; already-completed managed records are also removed on runtime reconciliation while preserving downloaded data. |

### `[bandwidth]`

| Option | Default | Meaning |
| --- | --- | --- |
| `global_download` | `0` | Global download bytes/sec, `0` means unlimited. |
| `global_upload` | `0` | Global upload bytes/sec, `0` means unlimited. |
| `alt_download` | `0` | Alternate download bytes/sec. |
| `alt_upload` | `0` | Alternate upload bytes/sec. |
| `alt_enabled` | `false` | Uses alternate limits when true. |
| `max_peers` | `0` | Exact process-wide peer-session cap shared by inbound and outbound peer TCP/uTP across all torrents. `0` is unlimited. Trackers, webseeds, DHT, and DNS are excluded. |
| `max_peers_per_torrent` | `0` | Additional per-torrent session cap shared by inbound and outbound peers. `0` uses the daemon default of 64. |

### `[queue]`

| Option | Default | Meaning |
| --- | --- | --- |
| `max_active_downloads` | `5` | Simultaneous active downloads, `0` means unlimited. |
| `max_active_metadata_fetches` | `100` | Simultaneous active magnet metadata fetches, `0` means unlimited. Does not consume download/seed active slots. |
| `max_active_seeds` | `5` | Simultaneous active seeds, `0` means unlimited. |
| `auto_start` | `true` | Starts newly added torrents automatically. |

Queue limits are enforced by the daemon scheduler. `auto_start = false` leaves
new torrents queued until resume/start-now is requested. Queue move operations
change the real scheduling order, and `max_active_downloads` controls how many
queued downloads may run at once.

### Performance tuning for large libraries

When managing 1,000+ torrents, consider these configuration adjustments to
maintain responsive performance:

**Queue limits:**
- Set `max_active_downloads` to a reasonable value (e.g., 50-100) to prevent
  resource exhaustion. With 1,000 torrents all downloading simultaneously,
  peer connections and file descriptors can overwhelm the system.
- Set `max_active_metadata_fetches` to limit concurrent magnet metadata
  fetches (default 100). High values can cause tracker rate limiting.
- Set `max_active_seeds` to limit concurrent seeders (default 5). Seeding
  torrents consume upload bandwidth and peer connections.

**Peer limits:**
- Set `max_peers` to a nonzero value to hard-bound total peer sessions across
  all torrents. Size it together with the service file-descriptor limit and
  leave headroom for files, trackers, DHT, and control-plane descriptors.
- Set `max_peers_per_torrent` to limit per-torrent peer connections (default
  64). Lower values (e.g., 30-50) reduce resource usage with minimal impact
  on download speed for well-seeded torrents.

Both limits apply for the full peer-session lifetime, including metadata,
normal serial/parallel, endgame, seeding, TCP, and uTP paths. An inbound socket
that cannot obtain capacity is closed before its peer session starts. Live
changes replace the permit pools and synchronously reconstruct eligible work;
if reconstruction or full-config persistence fails, the old limits and live
ownership remain in effect.

**Bandwidth limits:**
- Set `global_download` and `global_upload` to prevent network saturation.
  The atomic rate limiter efficiently distributes bandwidth across all active
  torrents without mutex contention.
- Use alternate speed limits (`alt_download`, `alt_upload`, `alt_enabled`)
  for scheduled bandwidth reduction during peak hours.

**File descriptors:**
- Ensure the daemon has sufficient file descriptor limits (see
  [Deployment](deployment.md#file-descriptor-requirements)). Peer descriptors
  are bounded by `max_peers` when configured, with additional workload-specific
  file, tracker, DHT, and control-plane overhead.

**Autopilot:**
- Enable `autopilot.mode = "act"` (default) to allow automatic queue slot
  release for stalled torrents, peer worker adjustments, and discovery
  refresh. This helps maintain throughput across large libraries without
  manual intervention.

Example configuration for a 1,000-torrent library:

```toml
[queue]
max_active_downloads = 50
max_active_metadata_fetches = 100
max_active_seeds = 20
auto_start = true

[bandwidth]
global_download = 0
global_upload = 0
max_peers = 10000
max_peers_per_torrent = 50

[autopilot]
mode = "act"
```

### `[seeding]`

| Option | Default | Meaning |
| --- | --- | --- |
| `global_ratio_limit` | `2.0` | Stops seeding after this ratio, unless overridden. |
| `global_idle_limit` | `1800` | Stops idle seeding after this many seconds, unless overridden. |

Omit a field to use its default.

`global_ratio_limit` must be finite and non-negative. Invalid negative or
non-finite values fail configuration validation with `invalid_config`.

Per-torrent policy is stored in durable daemon state rather than TOML. Set it
with `PUT /api/v1/torrents/:hash/seeding`: a `null` ratio/idle value inherits
these globals, explicit zero is a real immediate target, and `seed_forever`
temporarily suppresses both effective targets without deleting the stored
values. Policy and automatic/manual status survive restart without a daemon
state version bump because legacy records default to inherited targets.

### `[dht]`

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Enables DHT for non-private torrents. |
| `port` | `51413` | Local UDP port used by the shared DHT runner. |
| `bootstrap_nodes` | built-in public bootstrap hostnames | DHT bootstrap nodes. |

In strict mode, bootstrap hostnames are subject to DNS containment policy.

### `[pex]`

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Enables peer exchange for non-private torrents. |
| `max_peers` | `0` | PEX peer addition cap, `0` means unlimited. |

### `[[watch]]`

| Option | Default | Meaning |
| --- | --- | --- |
| `path` | required | Folder to scan for `.torrent` files. |
| `recursive` | `false` | Scans child folders when true. |
| `download_dir` | unset | Per-watch download directory override. |
| `label` | unset | Label applied to imports. |
| `start_behavior` | `"start"` | `"start"` or `"paused"`. |
| `archive_dir` | unset | Where imported files are archived. |
| `failure_dir` | unset | Where failed imports are moved. |
| `delete_after_import` | `true` | Deletes imported watch files when no archive is configured. |

Watch files are read through a bounded reader that enforces the shared 16 MiB
metadata limit (`MAX_TORRENT_METADATA_BYTES`) before parsing and before any
piece-sized allocation, regardless of `max_request_body_bytes`. Oversized or
malformed watch files are rejected as `malformed_torrent` / `bencode_error`
and never panic the daemon. See ADR-0050.

Watch ingestion is stability-gated (ADR-0054). The scanner walks in a blocking
filesystem task, sorts root-relative paths, rejects a configured symlink root,
and skips every child symlink without descending through symlinked directories.
A file is eligible only after two consecutive scans report the same length and
modified timestamp. The bounded read rechecks both the path and opened-file
metadata; a change discards the bytes and restarts stability without recording
an import result. Manual and automatic scans are serialized.

`path`, `archive_dir`, and `failure_dir` must not be whitespace-only. An archive
or failure directory must not lexically normalize to the watch root itself. If
one is a strict descendant of its watch root, that destination and its subtree
are excluded from this configured folder's scan; this prevents recursive scans
from re-importing moved inputs. Exclusion uses path-component boundaries
without resolving symlinks; similarly named siblings are not excluded. A
separately configured overlapping watch root evaluates its own destinations and
can still scan that path.

Observations are memory-only. Restart requires a fresh first observation. An
unchanged registered torrent then becomes a successful `duplicate` on the
second scan: its existing path, labels, queue position/bypass, and settings are
unchanged, while the configured success action runs once. With no archive and
`delete_after_import = false`, `leave` marks that fingerprint processed, so it
does not repeat until length or modified time changes. Watch status does not
advance stability and excludes unchanged processed files from its pending
count.

Only bencode, malformed-torrent, invalid-info-hash, and parse errors are
permanent input failures; they execute `failure_dir` handling and do not retry
unchanged input. Storage, I/O, persistence, containment, and internal failures
are transient: the source stays and a later stable scan retries it. Archive and
failure directories are created when absent, and create-new copy/remove actions
never overwrite a destination. A delete/copy/remove/collision error preserves
the primary result, appears as `post_action_error`, and leaves the fingerprint
processed for manual resolution. A crash during an archive/failure copy can
leave source plus a partial destination; recovery will not overwrite it.

`GET /api/v1/watch/history` and Watch status retain the newest 10,000 results in
insertion order for the current daemon run. Each result keeps compatibility
fields (`success`, `duplicate`, `error`) and reports `outcome` as `imported`,
`duplicate`, `permanent_failure`, or `transient_failure`, plus an optional
`post_action_error`. This operational history is not persisted.

### `[logging]`

| Option | Default | Meaning |
| --- | --- | --- |
| `level` | `"info"` | Log level. |
| `json` | `false` | Emits JSON logs when true. |
| `file` | `true` | Records daemon logs to a file as well as stderr/journal. |
| `file_path` | unset | Log file path. When unset, uses `$XDG_STATE_HOME/swarmotter/swarmotterd.log` or `~/.local/state/swarmotter/swarmotterd.log`. |

Default logging is intentionally simple: terminal starts still show logs in the
terminal, systemd starts still show logs in the journal, and the daemon also
records the same logs to a per-user file.
