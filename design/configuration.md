# Configuration

This document describes SwarmOtter's configuration model. The implementation
lives in `swarmotter-core::config`.

For operator-facing examples and option reference, see
`docs/configuration.md`.

## Sources

SwarmOtter is configured through a TOML configuration file plus environment
variable overrides. Invalid required configuration produces clear startup
errors. Safe defaults are provided where possible. Runtime settings updates are
supported for bandwidth, queue, and seeding limits; settings requiring restart
must be changed in the config file.

## Environment variable overrides

Settings can be overridden via environment variables using the prefix
`SWARMOTTER_` with nested fields separated by `__`. Values are parsed as
integers, booleans, or strings as appropriate. Examples:

```bash
SWARMOTTER_API__BIND_ADDRESS=0.0.0.0:9091
SWARMOTTER_TORRENT__LISTEN_PORT=51414
SWARMOTTER_NETWORK__MODE=strict
SWARMOTTER_NETWORK__REQUIRED_INTERFACE=br0
SWARMOTTER_NETWORK__ALLOW_IPV6=true
SWARMOTTER_TORRENT__ALLOW_IPV6=true
SWARMOTTER_API__MAX_REQUEST_BODY_BYTES=16777216
```

## Configuration areas

- **API** (`api`): `bind_address`, `require_auth`, `auth_token`,
  `max_request_body_bytes`. When `require_auth` is true, `auth_token` is
  required and all `/api/v1` routes require either
  `Authorization: Bearer <token>` or `X-SwarmOtter-Auth: <token>`.
  `GET /api/v1/settings` redacts the token. `max_request_body_bytes` bounds API
  request bodies, including torrent file uploads.
- **Storage** (`storage`): `download_dir`, `incomplete_dir`, `preallocate`,
  `sparse`. Incomplete data is written under `incomplete_dir` when configured
  and moved to `download_dir` after all pieces verify. When `preallocate` is
  true, the engine sizes files before downloading; when false, it creates
  directories and writes pieces as needed.
- **Network containment** (`network`): see `vpn-network-containment.md`
  (`mode`, `required_interface`, `required_source_ipv4`,
  `required_source_ipv6`, `required_network_namespace`, `allow_ipv6`,
  `fail_closed`, `validate_route`, `validate_dns`). `required_interface`
  binds torrent data-plane sockets to all current addresses on that interface
  on supported platforms, which is the DHCP/SLAAC-safe configuration. Source
  address fields are optional refinements for static-address deployments.
- **Torrent** (`torrent`): `listen_port`, `allow_ipv6`, `utp_enabled`,
  `utp_prefer_tcp`, `selfish`. When `utp_enabled` is true the engine attempts uTP
  (BEP 29) peer connections through the contained UDP socket alongside TCP; uTP
  traffic fail-closes with the rest of the data plane. `utp_prefer_tcp` selects
  which transport is tried first (with the other as a fallback). When
  `utp_enabled` is false, only TCP is used. `selfish` is an optional completion
  policy: when `true`, SwarmOtter removes a torrent from the daemon immediately
  after its download completes (all pieces verified), stops its engine and
  seeder, and preserves the downloaded data on disk (no delete-data behavior);
  SwarmOtter will not seed the torrent after completion. When `false` (the
  default), normal completion and seeding behavior is unchanged.
- **Bandwidth** (`bandwidth`): global/per-torrent download/upload limits,
  alternate speed mode, max peers. Global limits live in this section and are
  enforced as a shared aggregate across all active torrents; per-torrent limits
  (`download_limit`/`upload_limit`, 0 = unlimited) live on each torrent record
  and are set/changed live via `POST /api/v1/torrents/:hash/limits`. Both are
  enforced live by the engine/seeder rate shapers.
- **Queue** (`queue`): `max_active_downloads`, `max_active_seeds`,
  `auto_start`.
- **Seeding** (`seeding`): `global_ratio_limit`, `global_idle_limit`.
- **DHT** (`dht`): `enabled`, `port`, `bootstrap_nodes`.
- **PEX** (`pex`): `enabled`, `max_peers`.
- **Watch folders** (`watch`): array of `{ path, recursive, download_dir,
  label, start_behavior, archive_dir, failure_dir, delete_after_import }`.
- **Logging** (`logging`): `level`, `json`, `file`, `file_path`. File logging
  is enabled by default so daemon logs are recorded even for direct terminal
  starts.

## Example

A complete annotated example is in `config/swarmotter.toml.example`. A short
form:

```toml
[api]
bind_address = "0.0.0.0:9091"
max_request_body_bytes = 16777216

[storage]
download_dir = "/data/downloads"
incomplete_dir = "/data/incomplete"
preallocate = false

[network]
mode = "strict"
required_interface = "br0"
allow_ipv6 = true
fail_closed = true
validate_route = true
# Reports dns_not_constrained if DNS cannot be proven on br0.
validate_dns = true

[torrent]
listen_port = 51413
allow_ipv6 = true
```

For this layout, `incomplete_dir` is the active write root for partial torrent
data. Once all pieces verify, SwarmOtter moves the completed files to
`download_dir`. If `incomplete_dir` is omitted, both active and completed data
use `download_dir`.

## Validation rules

- Strict fail-closed network containment requires an enforceable torrent socket
  path: `required_interface`, `required_source_ipv4`,
  `required_source_ipv6`, or `required_network_namespace`.
- `required_interface` means "bind torrent data-plane sockets to this
  interface." On Linux this uses `SO_BINDTODEVICE`; if the daemon cannot enforce
  the bind, torrent traffic fails closed.
- `required_source_ipv6` requires `allow_ipv6 = true`.
- `api.bind_address` must not be empty and must parse as a socket address.
- `api.auth_token` must be set when `api.require_auth = true`.
- `api.max_request_body_bytes` must be > 0.
- `torrent.listen_port` must be > 0.
- Watch folder paths must not be empty.

Validation runs at load time and on env-override merge; failures abort startup
with a clear error message.

## DHCP/SLAAC Interface Binding

For an interface whose IPv4 or IPv6 addresses can change, configure the
interface name and omit fixed source addresses:

```toml
[network]
mode = "strict"
required_interface = "br0"
allow_ipv6 = true
fail_closed = true
validate_route = true
# Reports dns_not_constrained if DNS cannot be proven on br0.
validate_dns = true

[torrent]
listen_port = 51413
allow_ipv6 = true
```

This constrains torrent peer TCP, uTP/UDP, UDP trackers, DHT UDP, and inbound
peer listening to `br0` while allowing the kernel to choose the current source
address for each address family. The API/Web UI control plane remains
separate; `[api].bind_address` still takes a socket address, not an interface
name.

DNS containment is separate from socket binding. In strict fail-closed mode,
hostname resolution for torrent operations is blocked unless DNS containment
can be validated or supplied by the current network namespace. Linux interface
mode validates common systemd-resolved link DNS with `resolvectl dns <iface>`
and static resolver routes that go through the required interface.
