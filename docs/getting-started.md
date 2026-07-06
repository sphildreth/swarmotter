# Getting Started

## Build

```bash
git clone https://github.com/sphildreth/swarmotter.git
cd swarmotter
cargo build --release
```

The daemon binary is:

```bash
./target/release/swarmotterd
```

## Create a config file

Create directories for downloads and incomplete data:

```bash
mkdir -p ~/.config/swarmotter
mkdir -p ~/Downloads/swarmotter/downloads ~/Downloads/swarmotter/incomplete
```

Minimal local-only configuration:

```toml
[api]
bind_address = "127.0.0.1:9091"

[storage]
download_dir = "/home/YOU/Downloads/swarmotter/downloads"
incomplete_dir = "/home/YOU/Downloads/swarmotter/incomplete"

[network]
mode = "disabled"

[torrent]
listen_port = 51413
allow_ipv6 = true
utp_enabled = true
utp_prefer_tcp = true
encryption_mode = "preferred"
```

With this layout, active downloads write partial data under `incomplete`.
Completed torrents move to `downloads` only after all pieces verify.

Save it as:

```text
~/.config/swarmotter/config.toml
```

Then start:

```bash
./target/release/swarmotterd --config ~/.config/swarmotter/config.toml
```

Open:

```text
http://127.0.0.1:9091/
```

## Add content

Use the Web UI to add a magnet link, choose a `.torrent` file, or drag a
`.torrent` file anywhere onto the app window. The same operation is available
through the API:

```bash
curl -X POST http://127.0.0.1:9091/api/v1/torrents/file \
  --data-binary @example.torrent \
  -H 'Content-Type: application/x-bittorrent'
```

## LAN access

To reach the Web UI from another machine on your LAN, bind the control plane to
all IPv4 addresses:

```toml
[api]
bind_address = "0.0.0.0:9091"
```

When exposing the API or Web UI off localhost, enable authentication:

```toml
[api]
bind_address = "0.0.0.0:9091"
require_auth = true
auth_token = "replace-with-a-long-random-token"
```

API clients can authenticate with either:

```text
Authorization: Bearer <token>
```

or:

```text
X-SwarmOtter-Auth: <token>
```

## Optional Transmission-compatible endpoint

SwarmOtter can expose an optional compatibility endpoint at
`/transmission/rpc` for existing Transmission-style clients and scripts when
`compatibility.transmission.enabled = true`.

```toml
[compatibility.transmission]
enabled = true
```

Auth mapping uses the same API token flow as the native API:

- `Authorization` and `X-SwarmOtter-Auth` are accepted by the daemon.
- If a client uses HTTP Basic auth, the username is ignored and the password must
  equal `api.auth_token`.

The adapter supports `torrent-add` for:

- magnet links via `filename`
- base64-encoded `.torrent` metadata via `metainfo`

It also supports common Transmission session, torrent lifecycle, queue, and
helper calls. Mutating calls map to native SwarmOtter operations; for example,
`torrent-remove` with `delete-local-data` / `delete_local_data` can delete
payload data.

Remote HTTP/HTTPS torrent URL fetching is not supported through this endpoint.

## Optional qBittorrent-compatible endpoint

SwarmOtter can also expose an optional qBittorrent-compatible endpoint at
`/api/v2` when enabled:

```toml
[compatibility.qbittorrent]
enabled = true
```

Use the same API auth token to protect the endpoint as you do for native API:

```toml
[api]
require_auth = true
auth_token = "replace-with-a-long-random-token"
```

Authentication is supported through:

- Bearer token via `Authorization: Bearer <token>` (and
  `X-SwarmOtter-Auth`).
- qBittorrent-style SID cookie flow:

```bash
curl -i -X POST \
  http://127.0.0.1:9091/api/v2/auth/login \
  --data "username=swarmotter&password=replace-with-a-long-random-token"
```

Use the returned `SID` cookie for subsequent `/api/v2` requests.

For automation, the shim currently documents and supports:

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

The shim is opt-in by design, keeps the native API as the source of truth, and
does not expose indexer/search/discovery compatibility endpoints.
