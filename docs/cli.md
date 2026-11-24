# swarmotterctl CLI

`swarmotterctl` is an API-backed command-line client for a running SwarmOtter
daemon. It uses the same `/api/v1` contract, authentication, validation, and
state transactions as the Web UI. It does not read daemon-private state or open
torrent data-plane sockets.

## Install

Release tarballs include `bin/swarmotterctl`. Linux packages install it at:

```text
/usr/bin/swarmotterctl
```

The container image also includes `/usr/local/bin/swarmotterctl`. From source:

```bash
cargo build --locked --release --bin swarmotterctl
install -m 0755 target/release/swarmotterctl ~/.local/bin/swarmotterctl
```

## Connect

Local daemon:

```bash
export SWARMOTTER_API_URL=http://127.0.0.1:9091
swarmotterctl list
```

Remote daemon:

```bash
export SWARMOTTER_API_URL=https://swarm.example.net:9091
export SWARMOTTER_API_TOKEN_FILE="$HOME/.config/swarmotter/api-token"
chmod 0600 "$SWARMOTTER_API_TOKEN_FILE"
swarmotterctl --json list --state downloading
```

Resolution order:

- Endpoint: `--url`, then `SWARMOTTER_API_URL`, then
  `http://127.0.0.1:9091`.
- Token: `--token-file`, then `SWARMOTTER_API_TOKEN`, then
  `SWARMOTTER_API_TOKEN_FILE`.
- Timeout: `--timeout-seconds`, then `SWARMOTTER_API_TIMEOUT_SECONDS`, then 30.

Do not put a token in a command-line argument. The client rejects API URLs with
user information and never prints the token. HTTPS certificates are validated
against the public webpki roots; use a reverse proxy with a valid certificate
for remote deployments.

## Common operations

```bash
# Add
swarmotterctl add magnet 'magnet:?xt=urn:btih:...' --paused --label archive
swarmotterctl add file ./release.torrent --paused

# Inspect
swarmotterctl list --state paused --sort added --direction desc
swarmotterctl list --q linux --health good --per-page 100
swarmotterctl show <locator>
swarmotterctl health <locator>

# Operate
swarmotterctl pause <locator>
swarmotterctl resume <locator>
swarmotterctl pause-all
swarmotterctl resume-all
swarmotterctl stop <locator>
swarmotterctl recheck <locator>
swarmotterctl reannounce <locator>
swarmotterctl limits <locator> --download 1048576 --upload 524288
swarmotterctl remove <locator> --delete-data
```

Locators are the API's full 40-character v1/hybrid-primary or 64-character
pure-v2 values. The daemon owns all lifecycle semantics; an existing paused or
queued torrent behaves exactly as it does through the API/Web UI.

`pause-all` activates the durable master traffic switch. It stops every
downloader, metadata fetch, seeder, DHT lookup, and shared inbound peer
listener, and prevents new transfer work from starting, including start-now
requests. `resume-all` clears the switch and reconciles queue and seeding
policy. Both commands are equivalent to `PUT /api/v1/session`.

Lists use the server-side query endpoint. `--page` and `--per-page` request one
page (maximum 500). `--all` fetches every server page and combines the rows for
local display or JSON piping.

## API discovery

The daemon serves the version-matched OpenAPI document at `/api/openapi.json`
and a self-contained explorer at `/api/docs`. The explorer uses normal
same-origin authentication and marks state-changing operations; the CLI remains
the preferred automation surface.

## JSON mode

`--json` writes one document to stdout:

```json
{
  "format_version": 1,
  "command": "list",
  "success": true,
  "data": {
    "rows": [],
    "total": 0
  },
  "error": null
}
```

Failures are still one JSON document on stdout with `success: false` and:

```json
{
  "code": "authentication_failed",
  "message": "authentication failed: missing or invalid API token",
  "status": null
}
```

Human-readable output goes to stdout; human diagnostics go to stderr. In JSON
mode, stdout remains machine-parseable and diagnostics are not mixed into it.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Daemon API or local I/O failure |
| `2` | Usage or local configuration error |
| `3` | Authentication/authorization failure |
| `4` | Missing torrent or resource |
| `5` | Connection, DNS, TLS, or timeout failure |
| `6` | Validation or rejected request |

### Scripting example

```bash
#!/usr/bin/env bash
set -euo pipefail
export SWARMOTTER_API_URL="https://swarm.example.net:9091"
export SWARMOTTER_API_TOKEN_FILE="$HOME/.config/swarmotter/api-token"

swarmotterctl --json list --state downloading --per-page 500 \
  | jq -r '.data.rows[] | [.info_hash, .name, .state] | @tsv'
```

`jq` is not required by `swarmotterctl`; it is one way to consume the versioned
JSON contract. Use the exit code to separate authentication, missing-item,
validation, and connection failures from normal empty results.
