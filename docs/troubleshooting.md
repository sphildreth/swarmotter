# Troubleshooting

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

For interface-only configurations, use:

```toml
[network]
validate_dns = false
```

With that setting, SwarmOtter blocks torrent hostname resolution unless DNS is
otherwise constrained. Use a contained network namespace or container network
when hostname trackers and DHT bootstrap hostnames must resolve through the
contained path.

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
