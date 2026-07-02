# Troubleshooting

## Where logs are recorded

SwarmOtter writes logs to stderr and to a file by default.

For a terminal run, logs appear in the terminal and are also recorded at:

```text
$XDG_STATE_HOME/swarmotter/swarmotterd.log
```

If `XDG_STATE_HOME` is not set, the default is:

```text
~/.local/state/swarmotter/swarmotterd.log
```

Override the file path when needed:

```toml
[logging]
file = true
file_path = "/var/log/swarmotter/swarmotterd.log"
```

For systemd deployments, logs are also available through the journal:

```bash
journalctl -u swarmotterd -f
```

## `missing field mode`

Older builds required `network.mode` whenever `[network]` was present.
Current SwarmOtter accepts this DHCP/SLAAC-safe configuration:

```toml
[network]
required_interface = "br0"
```

That partial table defaults to strict containment with IPv6 enabled. Rebuild
and rerun the current binary if the daemon still reports:

```text
missing field `mode`
```

## Web UI shows `interface_missing`

`interface_missing` means the daemon cannot see the configured interface in its
current network namespace.

Check the interface name on the same host or namespace where the daemon runs:

```bash
ip a show br0
```

Then confirm the config matches exactly:

```toml
[network]
required_interface = "br0"
```

Common causes:

- The daemon is running inside a container that does not have `br0`.
- The systemd unit runs in a different network namespace.
- The interface name is different from the host interface name.
- The daemon process lacks permission to create device-bound sockets when
  torrent networking starts.
- You are running an older binary after editing source code.

## Web UI shows `no_interface_address`

The interface exists and is up, but SwarmOtter did not find a usable address.

Check:

```bash
ip a show br0
```

For IPv6, both settings must allow it:

```toml
[network]
allow_ipv6 = true

[torrent]
allow_ipv6 = true
```

## Web UI shows `dns_not_constrained`

This means strict containment was configured to validate DNS but DNS containment
could not be proven.

For interface-bound configurations, first check whether Linux can see DNS on
that interface:

```bash
resolvectl dns br0
```

If this reports DNS servers for `br0`, current SwarmOtter builds allow torrent
hostname resolution through that constrained path.

If DNS cannot be proven constrained and you still set:

```toml
[network]
validate_dns = true
```

network health reports `dns_not_constrained`. Use a contained network
namespace, container network, or IP-literal trackers/bootstrap nodes when the
host cannot prove DNS is on the contained path.

## IPv6 peers do not connect

Check all of the following:

```toml
[network]
allow_ipv6 = true

[torrent]
allow_ipv6 = true
```

Also confirm the interface has a usable IPv6 address:

```bash
ip -6 addr show dev br0
ip -6 route
```

If strict mode uses static source binding, `required_source_ipv6` must match an
address assigned to the configured path.

## `.torrent` drag-and-drop does nothing

Only `.torrent` files are accepted by drag-and-drop. Check the browser console
and daemon logs for upload errors, especially authentication failures and
`api.max_request_body_bytes` rejections.

Increase the upload limit if needed:

```toml
[api]
max_request_body_bytes = 33554432
```

## API requests fail with `unauthorized`

When `api.require_auth = true`, include one of these headers:

```text
Authorization: Bearer <token>
```

or:

```text
X-SwarmOtter-Auth: <token>
```

The Web UI uses the same API routes as external clients.

## Torrents are added but stay at `0 B/s`

If torrents appear in the Web UI but stay at `0 B/s`, check tracker status:

```bash
curl -sS http://127.0.0.1:9091/api/v1/torrents/<info_hash>/trackers
```

Check live per-torrent counters and engine diagnostics:

```bash
curl -sS http://127.0.0.1:9091/api/v1/torrents/<info_hash>/stats
```

Useful fields:

- `rate_down`, `rate_up`: smoothed transfer rates in bytes/sec.
- `active_peer_workers`: current bounded peer download workers.
- `known_peers`: peers currently discovered by trackers, DHT, PEX, or direct
  input.
- `tracker_ok`, `tracker_message`, `last_announce`: last tracker announce
  status from the live engine.

Common causes:

- The torrent has no live seeders.
- The tracker hostnames cannot resolve under strict DNS containment.
- UDP tracker traffic is blocked by the network path.
- Only WebTorrent `wss://` trackers are present; those are not BitTorrent TCP
  or UDP trackers.

In strict interface mode, hostname trackers and DHT bootstrap hostnames need
constrained DNS. On Linux, SwarmOtter accepts systemd-resolved link DNS for the
required interface, for example DNS servers shown by `resolvectl dns br0`.
