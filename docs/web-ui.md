# Web UI

The Web UI is served by `swarmotterd` from the same address as the API.

```text
http://127.0.0.1:9091/
```

Change the listener with:

```toml
[api]
bind_address = "0.0.0.0:9091"
```

When binding outside localhost, enable API authentication.

## Add torrents

The Web UI supports:

- Magnet link entry.
- File picker upload for `.torrent` files.
- Drag-and-drop upload for `.torrent` files anywhere in the app window.

Dropped `.torrent` files are sent to:

```text
POST /api/v1/torrents/file
```

The app refreshes the torrent list after successful upload.

## Network health

The header shows network containment health from:

```text
GET /api/v1/network/health
```

If the UI shows `interface_missing`, the daemon cannot see the configured
interface name in its current network namespace. See
[Troubleshooting](troubleshooting.md).

## Browser assets

The daemon serves the Web UI favicon set and app manifest from the embedded
graphics assets. The header uses the SwarmOtter icon next to the app name.
