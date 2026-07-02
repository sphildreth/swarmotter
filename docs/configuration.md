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
SWARMOTTER_NETWORK__MODE=strict
SWARMOTTER_NETWORK__REQUIRED_INTERFACE=br0
SWARMOTTER_TORRENT__LISTEN_PORT=51413
```

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
validate_dns = false

[torrent]
listen_port = 51413
allow_ipv6 = true
utp_enabled = true
```

If a `[network]` table contains `required_interface` but omits `mode`,
SwarmOtter treats it as strict containment. Setting `mode = "strict"`
explicitly is also valid.

On Linux, this binds torrent data-plane sockets to the named interface using
`SO_BINDTODEVICE`. The kernel may choose the current IPv4 or IPv6 source
address from that interface, so address changes do not break the configuration.

`validate_dns = false` is intentional for this interface-only example. It keeps
strict mode from approving DNS unless DNS containment is separately available.
When DNS is not constrained, SwarmOtter blocks torrent hostname resolution
instead of resolving names through an unconstrained path. IP-literal peers and
trackers can still be used. For hostname-heavy deployments, prefer a contained
network namespace or container network where DNS is part of the constrained
path.

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
validate_dns = false
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
validate_dns = false

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
- `dht.enabled = true`
- `pex.enabled = true`
- Bandwidth limits default to `0`, meaning unlimited.
- Peer limits default to `0`, meaning unlimited where the specific limit uses
  that convention.

Use bandwidth and queue limits when the host needs resource caps. Leaving them
unlimited or high is better for raw transfer throughput.

## Option reference

### `[api]`

| Option | Default | Meaning |
| --- | --- | --- |
| `bind_address` | `"127.0.0.1:9091"` | Address for the Web UI and API control plane. |
| `require_auth` | `false` | Requires API/Web UI token auth when true. |
| `auth_token` | unset | Required when `require_auth = true`. |
| `max_request_body_bytes` | `16777216` | Maximum API request body size, including `.torrent` uploads. |

### `[storage]`

| Option | Default | Meaning |
| --- | --- | --- |
| `download_dir` | unset | Completed download directory. |
| `incomplete_dir` | unset | Incomplete download directory. |
| `preallocate` | `false` | Pre-size files before downloading. |
| `sparse` | `true` | Use sparse files where supported. |

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
| `validate_dns` | `false` | Requires DNS containment validation when enabled. |

Strict mode requires at least one enforceable path: interface, source address,
or network namespace.

### `[torrent]`

| Option | Default | Meaning |
| --- | --- | --- |
| `listen_port` | `51413` | Inbound peer TCP and DHT/uTP UDP port. |
| `allow_ipv6` | `true` | Enables IPv6 peers when network containment also allows IPv6. |
| `utp_enabled` | `true` | Enables uTP peer transport through contained UDP sockets. |
| `utp_prefer_tcp` | `true` | Tries TCP first, with uTP fallback. |
| `selfish` | `false` | Removes a torrent after verified completion and does not seed it. |

### `[bandwidth]`

| Option | Default | Meaning |
| --- | --- | --- |
| `global_download` | `0` | Global download bytes/sec, `0` means unlimited. |
| `global_upload` | `0` | Global upload bytes/sec, `0` means unlimited. |
| `alt_download` | `0` | Alternate download bytes/sec. |
| `alt_upload` | `0` | Alternate upload bytes/sec. |
| `alt_enabled` | `false` | Uses alternate limits when true. |
| `max_peers` | `0` | Global peer cap, `0` means unlimited. |
| `max_peers_per_torrent` | `0` | Per-torrent peer cap, `0` means unlimited. |

### `[queue]`

| Option | Default | Meaning |
| --- | --- | --- |
| `max_active_downloads` | `5` | Simultaneous active downloads, `0` means unlimited. |
| `max_active_seeds` | `5` | Simultaneous active seeds, `0` means unlimited. |
| `auto_start` | `true` | Starts newly added torrents automatically. |

### `[seeding]`

| Option | Default | Meaning |
| --- | --- | --- |
| `global_ratio_limit` | `2.0` | Stops seeding after this ratio, unless overridden. |
| `global_idle_limit` | `1800` | Stops idle seeding after this many seconds, unless overridden. |

Omit a field to use its default.

### `[dht]`

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Enables DHT. |
| `port` | `51413` | DHT UDP port. |
| `bootstrap_nodes` | built-in public bootstrap hostnames | DHT bootstrap nodes. |

In strict interface-only mode, bootstrap hostnames are subject to DNS
containment policy.

### `[pex]`

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Enables peer exchange. |
| `max_peers` | `0` | PEX peer cap, `0` means unlimited. |

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
