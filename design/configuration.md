# Configuration

This document describes SwarmOtter's configuration model. The implementation
lives in `swarmotter-core::config`.

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
SWARMOTTER_NETWORK__REQUIRED_INTERFACE=tun0
```

## Configuration areas

- **API** (`api`): `bind_address`, `require_auth`, `auth_token`.
- **Storage** (`storage`): `download_dir`, `incomplete_dir`, `preallocate`,
  `sparse`.
- **Network containment** (`network`): see `vpn-network-containment.md`
  (`mode`, `required_interface`, `required_source_ipv4`,
  `required_source_ipv6`, `required_network_namespace`, `allow_ipv6`,
  `fail_closed`, `validate_route`, `validate_dns`).
- **Torrent** (`torrent`): `listen_port`, `allow_ipv6`.
- **Bandwidth** (`bandwidth`): global/per-torrent download/upload limits,
  alternate speed mode, max peers.
- **Queue** (`queue`): `max_active_downloads`, `max_active_seeds`,
  `auto_start`.
- **Seeding** (`seeding`): `global_ratio_limit`, `global_idle_limit`.
- **DHT** (`dht`): `enabled`, `port`, `bootstrap_nodes`.
- **PEX** (`pex`): `enabled`, `max_peers`.
- **Watch folders** (`watch`): array of `{ path, recursive, download_dir,
  label, start_behavior, archive_dir, failure_dir, delete_after_import }`.
- **Logging** (`logging`): `level`, `json`.

## Example

A complete annotated example is in `config/swarmotter.toml.example`. A short
form:

```toml
[api]
bind_address = "0.0.0.0:9091"

[storage]
download_dir = "/data/downloads"
incomplete_dir = "/data/incomplete"

[network]
mode = "strict"
required_interface = "tun0"
required_source_ipv4 = "10.8.0.2"
allow_ipv6 = false
fail_closed = true
validate_route = true
validate_dns = true

[torrent]
listen_port = 51413
```

## Validation rules

- Strict network containment requires a configured path (interface, source
  address, or network namespace).
- `required_source_ipv6` requires `allow_ipv6 = true`.
- `api.bind_address` must not be empty and must parse as a socket address.
- `torrent.listen_port` must be > 0.
- Watch folder paths must not be empty.

Validation runs at load time and on env-override merge; failures abort startup
with a clear error message.