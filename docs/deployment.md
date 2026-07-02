# Deployment

## Basic Linux service

Build the daemon:

```bash
cargo build --release
```

Install a config:

```bash
sudo install -d /etc/swarmotter /var/lib/swarmotter
sudo install -m 0644 config/swarmotter.toml.example /etc/swarmotter/swarmotter.toml
```

Edit `/etc/swarmotter/swarmotter.toml`, then run:

```bash
./target/release/swarmotterd --config /etc/swarmotter/swarmotter.toml
```

Logs are written to stderr and to the configured daemon log file. With default
logging, the per-user file is `~/.local/state/swarmotter/swarmotterd.log`
unless `XDG_STATE_HOME` is set.

## Systemd

An example unit is provided in:

```text
deploy/swarmotterd.service
```

Install it:

```bash
sudo install -m 0644 deploy/swarmotterd.service /etc/systemd/system/swarmotterd.service
sudo systemctl daemon-reload
sudo systemctl enable --now swarmotterd
```

Make sure the service user can read the config and write the storage
directories.

## LAN Web UI with contained torrents

This exposes the control plane to the LAN while binding torrent data-plane
sockets to `br0`:

```toml
[api]
bind_address = "0.0.0.0:9091"
require_auth = true
auth_token = "replace-with-a-long-random-token"

[storage]
download_dir = "/mnt/incoming/swarmotter/downloads"
incomplete_dir = "/mnt/incoming/swarmotter/incomplete"

[network]
mode = "strict"
required_interface = "br0"
allow_ipv6 = true
fail_closed = true
validate_route = true
validate_dns = true

[torrent]
listen_port = 51413
allow_ipv6 = true
utp_enabled = true
```

## Container or VPN namespace

For stronger isolation, run SwarmOtter inside a network namespace or container
whose only torrent data-plane path is the intended VPN or NIC path.

Container sketch:

```bash
docker build -t swarmotter .
docker run -d --name swarmotter \
  -p 9091:9091 \
  -v /data/downloads:/data/downloads \
  -v /data/incomplete:/data/incomplete \
  -v /etc/swarmotter:/etc/swarmotter:ro \
  swarmotter
```

Attach the container to the intended contained network instead of the default
bridge when strict data-plane containment is required.

## Reverse proxy

A reverse proxy may sit in front of the API/Web UI. Keep authentication enabled
unless another trusted auth layer protects access.

```nginx
server {
    listen 80;
    server_name swarmotter.example;

    location / {
        proxy_pass http://127.0.0.1:9091;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /api/v1/ws {
        proxy_pass http://127.0.0.1:9091;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    location /api/v1/events {
        proxy_pass http://127.0.0.1:9091;
        proxy_buffering off;
        proxy_read_timeout 1h;
    }
}
```
