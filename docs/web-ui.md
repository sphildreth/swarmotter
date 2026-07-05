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

## Torrent list

The Peers column shows active peer workers / known peers from the torrent
summary response. The main UI area uses the available browser width so wide
tables can show operational details without being capped to a narrow centered
column. Per-row torrent actions are icon buttons with accessible labels.

Torrent rows can be selected with checkboxes. The torrent toolbar can select
all currently visible rows, clear the current selection, and remove all selected
torrents. Bulk removal removes torrent records through `POST
/api/v1/torrents/remove` and keeps downloaded data.

## Notifications

Transient operation feedback is shown as toast notifications instead of inline
status text. This includes torrent add/upload results, user-initiated torrent
removal, removals observed from automatic completion policy, bandwidth setting
saves, and watch-folder scan results.

Toasts display for 5 seconds by default. The display time is a browser-local UI
preference that can be changed in Settings > Notifications.

## Network health

The UI shows network containment health from:

```text
GET /api/v1/network/health
```

Detailed network checks and path diagnostics use:

```text
GET /api/v1/network/diagnostics
```

If the UI shows `interface_missing`, the daemon cannot see the configured
interface name in its current network namespace. See
[Troubleshooting](troubleshooting.md).

## Logs, Watch status, and doctor report

Operational diagnostics in the UI come from:

- `GET /api/v1/watch/status` for enabled folders and recent watch-folder activity.
- `GET /api/v1/logs/recent` for live-tail style log snapshots.
- `GET /api/v1/doctor` for a consolidated operational check summary.
- `GET /api/v1/version` for the application version shown in the Doctor view.

The Settings view also exposes a destructive Reset action. After confirmation,
it calls `POST /api/v1/reset` to stop torrent activity, remove torrent records,
empty the configured download and incomplete directories while preserving those
root directories, and clear daemon log files.

## Browser assets

The daemon serves the Web UI favicon set and app manifest from the embedded
graphics assets. The header uses the SwarmOtter icon next to the app name.
