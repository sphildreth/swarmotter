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

If a trusted-LAN deployment should not require a token, set
`api.require_auth = false` in the mounted TOML configuration, or set
`SWARMOTTER_API_REQUIRE_AUTH=false` for the Compose deployment. Non-loopback
listeners log a warning because every reachable client can then control
SwarmOtter.

## Chrome extension POST returns `extension_origin_forbidden`

Chrome Manifest V3 service workers are cross-origin clients. A privileged
extension request normally carries both:

```text
Origin: chrome-extension://<32-character-extension-id>
Sec-Fetch-Site: none
```

SwarmOtter accepts that Origin only with authenticated API mode and a valid API
token. Configure:

```toml
[api]
require_auth = true
auth_token = "replace-with-a-long-random-token"
```

Then send the same token on the extension service worker's request:

```text
Authorization: Bearer <token>
```

or:

```text
X-SwarmOtter-Auth: <token>
```

Also grant the exact SwarmOtter API origin in the extension manifest's
`host_permissions`; HTTP and HTTPS permissions are separate. Do not try to set
`Origin` or `Sec-Fetch-Site` in extension code—the browser owns those headers.

Check the native JSON error code and message:

- `extension_origin_forbidden`: authenticated mode is off, the token is absent
  or invalid, an authentication header is duplicated, or both supported token
  header forms were sent together.
- `cross_origin_forbidden`: Fetch Metadata, Origin, or Host failed the ordinary
  browser-origin policy. `same-site`/`cross-site`, foreign HTTP(S), `null`,
  opaque, malformed (including an invalid extension ID), and multi-value
  Origins remain intentionally rejected.

Setting only `auth_token` while `require_auth = false` does not enable extension
access. SwarmOtter does not broadly trust all installed extensions on an
unauthenticated listener.

## Update helper health check reports connection resets

If `deploy/update-swarmotter.sh` reports repeated `curl: (56) Recv failure:
Connection reset by peer` while checking `http://127.0.0.1:9091/health`, inspect
the service status and recent logs printed by the updater. Current release
images are also configuration-checked before the healthy stack is replaced.
To distinguish a daemon failure from host port filtering, verify whether the
daemon is healthy inside the shared Gluetun network namespace:

```bash
docker compose --env-file .env -f compose.yml exec swarmotter \
  curl -fsS http://127.0.0.1:9091/health
```

If that succeeds but host `curl http://127.0.0.1:9091/health` fails, Gluetun is
blocking the published control-plane port. Set this in `gluetun.env`:

```dotenv
FIREWALL_INPUT_PORTS=9091
```

This opens the SwarmOtter API/Web UI port on Gluetun's default interface. It
does not expose torrent peer, tracker, DHT, webseed, or torrent DNS traffic
outside the Gluetun VPN namespace.

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
- `peer_scheduler`: live scheduler counts showing discovered, eligible,
  filtered, failed-backoff, no-progress-backoff, parallel candidate, worker
  limit, and serial-fallback state. Use this when `known_peers` is high but
  `active_peer_workers` is low or zero.
- `useful_peers`: connected peers observed with pieces the torrent still needs
  and an unchoked or recently useful state.
- `unchoked_peers`: connected peers the engine has observed as unchoked.
- `choked_peers`: reserved for explicit choke-state telemetry; currently
  `null` until the engine records positive per-peer choke state.
- `recent_peer_failures`, `recent_tracker_failures`: recent failed peer
  sessions and tracker announce/scrape failures reported by the live engine.
- `tracker_ok`, `tracker_message`, `last_announce`: last tracker announce
  status from the live engine.
- `tracker_last_ok_seconds_ago`, `dht_last_seen_seconds_ago`,
  `pex_last_seen_seconds_ago`: freshness of the last successful tracker, DHT,
  and PEX discovery signals when live engine data is available.
- `dht_discovery_ok`, `pex_discovery_ok`: whether DHT or PEX discovery has
  succeeded recently in the live engine.

Tracker rows from `/api/v1/torrents/<info_hash>/trackers` report per-tracker
announce and scrape results. `last_error`/`last_message` remain announce-only.
`scrape_status`, `last_scrape`, nullable `scrape_seeders`/`scrape_leechers`/
`scrape_downloads`, and `last_scrape_error` describe scrape. A failed scrape
retains the previous successful counts. `unsupported` is expected for UDP and
HTTP(S) URLs whose final path does not begin with `announce`; it does not mean
UDP announce failed. If announce is not successful, compatibility seed/leech
counts fall back to retained scrape data.

Common causes:

- The torrent has no live seeders.
- The tracker hostnames cannot resolve under strict DNS containment.
- UDP tracker traffic is blocked by the network path.
- A supported HTTP(S) scrape is redirected to an HTTPS-to-HTTP downgrade,
  returns malformed/missing exact-key BEP 48 data, or exceeds the decoded cap.
- Only WebTorrent `wss://` trackers are present; those are not BitTorrent TCP
  or UDP trackers.

In strict interface mode, hostname trackers and DHT bootstrap hostnames need
constrained DNS. On Linux, SwarmOtter accepts systemd-resolved link DNS for the
required interface, for example DNS servers shown by `resolvectl dns br0`.

## Performance with large libraries (1,000+ torrents)

When managing large torrent libraries, monitor these indicators:

### Symptoms of resource exhaustion

- API responses slow down significantly (multiple seconds).
- SSE/WebSocket subscribers receive `events_dropped` lag notifications.
- Torrents stay in `queued` state despite available slots.
- Daemon logs show repeated peer connection failures or tracker timeouts.
- High CPU usage from lock contention or excessive reconciliation.

### Check file descriptor usage

Peer-session descriptors are bounded by a nonzero `max_peers`; payload files,
trackers, DHT, the shared listener, and the control plane add workload-specific
overhead. Check the daemon's current limit and usage:

```bash
PID=$(pgrep swarmotterd)
cat /proc/$PID/limits | grep "Max open files"
ls /proc/$PID/fd | wc -l
```

If usage approaches the limit, increase it (see
[Deployment](deployment.md#file-descriptor-requirements)).

### Check scheduler saturation

The stats endpoint reports scheduler pressure:

```bash
curl -sS http://127.0.0.1:9091/api/v1/stats | jq .scheduler
```

Key fields:

- `requested_downloads` vs `granted_downloads`: if requested exceeds granted,
  the download slot cap is the bottleneck.
- `requested_metadata_fetches` vs `granted_metadata_fetches`: if requested exceeds granted,
  the metadata fetch slot cap is the bottleneck.
- `peer_limit`, `peer_permits_in_use`, and `peer_permits_available`: the
  authoritative process-wide peer-session cap and current usage. Available is
  `null` when unlimited.
- `peer_sessions_denied`: inbound sockets rejected before session start by an
  applicable global or per-torrent cap.
- `peer_worker_budget_saturated` (and legacy peer-worker budget fields): engine
  worker-pressure compatibility telemetry. It does not mean the process-wide
  peer connection cap is full; use the permit fields above for that decision.
- `retry_backoff_torrents`: high values indicate many torrents waiting for retry
  after transient failures.

### Check event subscriber lag

If SSE or WebSocket clients report `events_dropped`, the broadcast buffer
(default 4,096) is overflowing. This happens during reconciliation bursts when
many torrents change state simultaneously. Clients should reconnect and
request a full state refresh after receiving a lag notification.

### Reduce resource pressure

If performance degrades with large libraries:

1. Lower `max_active_downloads` to reduce concurrent peer connections.
2. Lower `max_peers_per_torrent` to reduce per-torrent resource usage.
3. Set a global `max_peers` cap to bound total connection count.
4. Ensure file descriptor limits are sufficient (65,536+ for 1,000 torrents).
5. Enable `autopilot.mode = "act"` for automatic stalled-torrent mitigation.
