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
```

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
