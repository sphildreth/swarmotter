# Configuration

This document describes SwarmOtter's configuration model. It is a stub; schemas
and defaults will be finalized during implementation.

## Sources

SwarmOtter is configured through a configuration file plus environment
variable overrides. Invalid required configuration must produce clear startup
errors. Safe defaults are provided where possible. Runtime settings updates
are supported where safe; settings requiring restart must be reported.

## Configuration areas

- API bind address and authentication.
- Download directories (incomplete, completed, per-torrent).
- Watch folders (paths, recursive, download location defaults, labels,
  paused/start behavior, archive/failure/leave/delete handling).
- Torrent listen port.
- DHT enablement and settings.
- PEX enablement and settings.
- Tracker settings.
- Peer limits (global and per-torrent).
- Bandwidth limits (global and per-torrent download/upload, alternate speed
  mode, max peers).
- Queue limits (active download/seed limits).
- Ratio/seeding limits (global and per-torrent ratio and idle seed limits,
  seed-forever option).
- VPN/NIC/network containment settings (see `vpn-network-containment.md`).
- IPv4/IPv6 behavior (ability to disable IPv6 to reduce leak risk).
- Logging and metrics.

## Example

```toml
[network]
mode = "strict"
required_interface = "tun0"
required_source_ipv4 = "10.8.0.2"
allow_ipv6 = false
fail_closed = true
validate_route = true
validate_dns = true

[api]
bind_address = "0.0.0.0:9091"

[torrent]
listen_port = 51413
```

## TODO

- Finalize config file format (TOML) and schema.
- Specify environment variable override naming and precedence.
- Specify validation rules and safe defaults per setting.
- Keep this document aligned with `vpn-network-containment.md` and
  `deployment.md`.