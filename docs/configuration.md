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

## Common configuration: bind torrents to `br0`

Use this when the interface name is stable but addresses are assigned by DHCP,
SLAAC, or router advertisements.

```toml
[api]
bind_address = "0.0.0.0:9091"

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
| `require_auth` | `false` | Requires API/Web UI token auth when true. |
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
| `mode` | `disabled` when the entire table is omitted; `strict` inside a partial table | Torrent data-plane containment mode. |
| `required_interface` | unset | Interface name, such as `br0` or `tun0`. |
| `required_source_ipv4` | unset | Required IPv4 source address. |
| `required_source_ipv6` | unset | Required IPv6 source address. |
| `required_network_namespace` | unset | Required Linux network namespace name. |
| `allow_ipv6` | `true` | Enables IPv6 torrent networking when the path is contained. |
| `fail_closed` | `true` | Blocks torrent networking when strict containment is unhealthy. |
| `validate_route` | `false` | Requires route validation when supported by the probe. |
| `validate_dns` | `false` | Reports `dns_not_constrained` in network health when DNS cannot be proven constrained. Hostname resolution is still fail-closed unless DNS is constrained or a network namespace is used. |

Strict mode requires at least one enforceable path: interface, source address,
or network namespace.

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
| `encryption_mode` | `preferred` | TCP MSE/PE peer wire mode. `disabled` permits plaintext handshakes. `preferred` enables MSE/PE with plaintext fallback for TCP attempts while preserving the configured TCP/uTP preference. `required` refuses plaintext and requires encrypted TCP stream negotiation. Changing this setting is reported as restart-required for already-running torrent tasks. |
| `selfish` | `false` | Removes a torrent after verified completion and does not seed it; already-completed managed records are also removed on runtime reconciliation while preserving downloaded data. |

### `[bandwidth]`

| Option | Default | Meaning |
| --- | --- | --- |
| `global_download` | `0` | Global download bytes/sec, `0` means unlimited. |
| `global_upload` | `0` | Global upload bytes/sec, `0` means unlimited. |
| `alt_download` | `0` | Alternate download bytes/sec. |
| `alt_upload` | `0` | Alternate upload bytes/sec. |
| `alt_enabled` | `false` | Uses alternate limits when true. |
| `max_peers` | `0` | Global peer worker cap divided across active downloads, `0` means no global cap. |
| `max_peers_per_torrent` | `0` | Per-torrent peer worker cap. `0` uses the daemon default worker pool of 64. |

### `[queue]`

| Option | Default | Meaning |
| --- | --- | --- |
| `max_active_downloads` | `5` | Simultaneous active downloads, `0` means unlimited. |
| `max_active_seeds` | `5` | Simultaneous active seeds, `0` means unlimited. |
| `auto_start` | `true` | Starts newly added torrents automatically. |

Queue limits are enforced by the daemon scheduler. `auto_start = false` leaves
new torrents queued until resume/start-now is requested. Queue move operations
change the real scheduling order, and `max_active_downloads` controls how many
queued downloads may run at once.

### `[seeding]`

| Option | Default | Meaning |
| --- | --- | --- |
| `global_ratio_limit` | `2.0` | Stops seeding after this ratio, unless overridden. |
| `global_idle_limit` | `1800` | Stops idle seeding after this many seconds, unless overridden. |

Omit a field to use its default.

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
