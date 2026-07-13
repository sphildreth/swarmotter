# Web UI

The Web UI is served by `swarmotterd` from the same address as the API.

```text
http://127.0.0.1:9091/
```

Change the listener with:

```toml
[api]
bind_address = "0.0.0.0:9091"
require_auth = true
auth_token = "replace-with-a-long-random-token"
```

Authenticated access is strongly recommended when binding outside localhost.
With `require_auth = true`, the Web UI asks for the token once and keeps it in
browser-local storage. A trusted-LAN deployment may set `require_auth = false`;
the UI then uses the same-origin API without a token prompt. Every client that
can reach an unauthenticated listener can control SwarmOtter. Browser requests
must remain same-origin, and reverse proxies must preserve the public `Host`.

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
column. Per-row torrent actions are icon buttons with accessible labels. The
Details action opens keyboard-accessible lifecycle, queue, move, label,
bandwidth-limit, file-rename, and tracker-edit controls. Removing one torrent
offers separate Cancel, keep-data, and delete-data choices.

The torrent list is an interactive table. Click a column header to sort by
that column, and click it again to reverse the direction. Header filters can
filter individual columns: status and health use list filters, while numeric
columns such as size, progress, rates, ratio, and peers accept comparisons
such as `> 0`, `>= 50`, `< 10`, or `= 1`. The toolbar search remains a global
filter across common torrent summary fields, and Clear Filters resets both the
toolbar search and column filters.

Torrent rows can be selected with checkboxes. The torrent toolbar can select
all currently visible rows, clear the current selection, and remove all selected
torrents. Bulk removal removes torrent records through `POST
/api/v1/torrents/remove` and keeps downloaded data.

## Tracker details

Torrent Details → Trackers keeps announce status separate from scrape status.
The table shows the last scrape time, retained seeders/leechers/downloads in
`S / L / D` order, and the compatibility counts used elsewhere. A scrape error
is displayed beside `error` while the last successful counts remain visible;
`unsupported` means the tracker is UDP or its final path is not derivable from
`announce*`. UDP announce remains supported—only UDP scrape is unsupported.

All tracker URL, status, time, count, and error values are escaped before being
inserted into the table. Scrape is operational telemetry scheduled by download,
magnet, reannounce, completion, and active seeder tracker activity; it is not a
separate user mutation.

The Details summary also displays **Last error** from the native torrent
summary. If every attempted configured tracker fails and no usable alternative
source exists, the state becomes `tracker error` and this row retains the last
tracker failure. Reannounce, Resume, or Start Now clears the terminal error and
starts a new attempt.

## Per-torrent seeding policy

Torrent Details includes a Seeding Policy card. Its read-only summary reports
the uploaded-byte count, ratio, exact seeding status, stored ratio/idle targets,
effective ratio/idle targets after global inheritance, and whether seed-forever
is enabled. Status values are displayed as `not eligible`, `queued`, `active`,
`stopped ratio`, `stopped idle`, or `stopped manual`.

Use the Ratio target and Idle target controls as follows:

- Select **Inherit global ratio** or **Inherit global idle** to store `null` and
  use the corresponding value from Settings > Seeding.
- Clear inheritance and enter `0` to request an immediate automatic stop. Zero
  is a real target; it is not the same as inheritance.
- Select **Seed forever** to suppress both effective automatic targets while
  preserving the stored overrides for later use.

**Save Seeding Policy** replaces all three per-torrent fields together. The UI
waits for the server response and reloads Torrent Details before displaying the
new summary; it does not predict a status transition locally. Invalid input or
a persistence failure is shown in the card's alert and leaves the last rendered
stored/effective values unchanged. A policy edit never resumes a torrent that
an operator manually paused; use Resume or Start Now when that is intentional.

## Large-library operations console

For large libraries, the Operations Console is optimized for speed and low
layout churn. The list is designed for high-count visibility with:

- server-side search plus state, health, and performance-condition filters,
- table sorting that round-trips through the server query endpoint,
- a browser-local saved view for search/filter/page-size/sort state,
- count-oriented list requests and pagination for incremental refresh,
- clear confirmation paths for bulk destructive operations, and
- detail views that avoid forcing a full table reload.

The underlying `/api/v1/torrents/query` endpoint also supports label, storage
root, peer/rate threshold, counts-only, and optional grouping parameters for
external automation and future UI views.

## Protocol encryption controls

SwarmOtter can negotiate MSE/PE peer encryption. The Settings screen exposes
`torrent.encryption_mode` with these choices:

- `disabled` (plaintext handshakes only),
- `preferred` (TCP attempts use MSE/PE first, with plaintext fallback),
- `required` (refuse plaintext).

The default is `preferred`. The UI keeps this control in the same Settings edit
flow as other daemon config because it changes peer-wire compatibility behavior.
Per-profile and per-torrent override controls are planned for a later phase.

## Storage root diagnostics

The Doctor view surfaces storage diagnostics from `GET /api/v1/storage/roots`
so operators can:

- review per-root free/available bytes before large add bursts,
- identify which roots are close to configured reserve thresholds, and
- diagnose storage pressure alongside active write/recheck activity in future views.

Storage reserve fields in configuration are `[storage].minimum_free_space_bytes`
and `[storage].minimum_free_space_percent`. When configured, add operations are
rejected before writing data when the target root cannot satisfy the configured
reserve.

## Performance diagnostics and autopilot visibility

The torrent detail view uses `/api/v1/torrents/:hash/stats` as its primary
diagnostic source. Existing health sub-scores and `reasons` are the basis for the
autopilot-oriented "why is this slow?" explanation and are updated from the same
contained network observations as engine and network health reporting. In
`act` mode, the daemon may apply bounded actions from those observations; the
details page shows the current decision and rationale.

In autopilot visibility mode, the UI reads:

- `GET /api/v1/autopilot/status` for the global autopilot mode.
- `GET /api/v1/network/health` and `GET /api/v1/network/diagnostics` for any
  containment condition that may block or bias tuning decisions.
- `GET /api/v1/torrents/:hash/stats` for peer-level health and scheduler signals.
- `GET /api/v1/torrents/:hash/autopilot` and `POST /api/v1/torrents/:hash/autopilot`
  for per-torrent decision views and mode override controls.

The Settings tab includes an Autopilot card for the global
`disabled` / `observe` / `act` mode. The default is `act`, and Torrent Details
keeps the per-torrent override control.

The Settings screen uses a two-panel layout: section navigation on the left and
the selected settings group on the right. Save, reload, and reset controls sit
in the Settings header. Saving submits the full configuration snapshot. If an
operator intentionally makes the config path read-only, a failed persistence
attempt falls back only to the live-safe bandwidth, queue, seeding, and
autopilot PATCH; the UI reports that other changes were not applied.

The details page renders a compact "why is this slow?" report with these fields:

- active/global/autopilot mode state.
- machine-readable reason identifiers and recommendations or applied-action
  candidates.
- no-progress queue-slot release recommendations when a stalled active torrent
  is eligible to let queued work proceed.
- snapshot signals and network-conditions impact for operational context.

The UI should present autopilot recommendations as human-readable entries with
underlying machine-readable identifiers (for operators and automation clients) and
continue to honor the fail-closed containment model.

## Notifications

Transient operation feedback is shown as toast notifications instead of inline
status text. This includes torrent add/upload results, user-initiated torrent
removal, external removals observed while the complete unfiltered library is
visible, bandwidth setting saves, and watch-folder scan results. Filtered or
paginated result changes are never treated as proof that a torrent was removed.

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

The Watch history table has a separate stable Outcome column: `imported`,
`duplicate`, `permanent failure`, or `transient failure`. Duplicate means the
existing torrent was retained unchanged and the configured success action ran.
Transient failures remain eligible for a later stable scan; permanent failures
do not retry an unchanged fingerprint. The Status column is warning-colored
when `post_action_error` is present even if the primary outcome is imported or
duplicate, and Detail shows both the primary error and archive/delete/failure-
move error so the operator can resolve a retained source or destination
collision. Pending counts include unseen, changed, stabilizing, and transient-
retry files but exclude unchanged processed files. Watch history contains only
the current daemon run and retains its newest 10,000 results.

The Settings view also exposes a destructive Reset action. After confirmation,
it calls `POST /api/v1/reset` to stop torrent activity, remove torrent records,
empty the configured download and incomplete directories while preserving those
root directories, and clear daemon log files.

## Browser assets

The daemon serves the Web UI favicon set and app manifest from the embedded
graphics assets. The header uses the SwarmOtter icon next to the app name and
includes a light/dark theme icon. The Web UI defaults to dark mode and stores
the selected theme in browser `localStorage` under `swarmotter.theme`.
Web assets use a self-only content security policy and cannot be framed by
another site.
