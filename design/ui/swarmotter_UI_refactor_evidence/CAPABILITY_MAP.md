# Capability map

All paths below are relative to `crates/swarmotter-web/assets`. The daemon embeds these assets through `src/lib.rs` in the same crate. The existing router switches sections at `/` and `/index.html`. The separate Doctor and Torrent Details sections keep their existing entry buttons. Magnet pasting and file dropping still use the existing authenticated API.

| Capability | Existing entry | Data or operation | Destination | Verification |
|---|---|---|---|---|
| Appearance, authentication, health doctor | app.js, js/ui.js, js/api.js, js/events.js | localStorage theme and token, /doctor, /version, /storage/roots | Shared header, health button | Toggle theme, navigate, open Doctor |
| Library, sorting, numeric column filters, pagination, saved query | js/torrents.js, js/state.js | /torrents/query, localStorage torrentQueryView | Table, Smart Filters, Labels, View menu | Search, change state, sort, save/load |
| Selection, restart, bulk export, removal with separate payload choice | js/torrents.js, js/provenance.js | Hash-keyed bulk endpoints and removal dialog | Toolbar, View menu, row menu | Select checkboxes independently of inspector |
| File selection/priorities, peer/trackers/webseed diagnostics, annotations, magnets, storage, seeding, overrides, queue, autopilot | js/details.js, js/provenance.js, seeding-policy.js | /torrents/{hash} and subresources | Inspector and full Torrent Details | Open by hash, inspect tabs and full controls |
| Magnet, file, drag/drop, metadata-gated queued intake, profiles, labels, queued registration | js/torrents.js | /profiles, /torrents/magnet, /torrents/file | Add forms and global file drop | Validate request and payload, retain failed input |
| Author and seed local v1/v2/hybrid metainfo | js/create.js | /torrents/create/roots, /torrents/create | Create torrent tab | Form, validation, result and metainfo export |
| Binding, DNS, interfaces, port tests, router mappings | js/events.js | /network/health, /network/diagnostics, explicit mapping refresh and port test POSTs | Network monitor and diagnostics | Inspect with GET, run operations explicitly |
| All configuration categories, storage limits, policy profiles, filters and manual unban, notifications | js/settings.js | GET and PUT /settings, runtime PATCH fallback, /peer-filter, localStorage | Settings rail and editor | Load, edit, save, check persistence/restart response |
| Reset downloads and clear logs | js/settings.js | confirmed POST /reset | Settings inspector | Confirmation and unchanged destructive scope |
| RSS CRUD, preview, poll, reconsider, retry, first poll policy, paginated decisions | rss.js, js/rss-id.js | /rss collection and per-ID operations | Watch feeds, inspector, feed actions, history | Select by ID, preview separately from poll |
| Watch folders, scan/import history, retention and trash | js/events.js, watch-history.js, js/trash.js | /watch/status, /watch/scan, /trash and operations | Watch rail and Settings folder editor | Configured-folder scan gating, preview/apply, restore/purge |
| Recent log tail and live events | js/events.js | /logs/recent and authenticated fetch /events, bounded browser buffer | Logs stream, filters and inspector | Search, inspect, pause, export and clear locally |

## Data boundaries

The library query reports state/label counts for the filtered result. Global active/paused/seed counts and rates come from /stats. A library-wide total size is computable when the query includes the complete library. Piece counts describe aggregate completion. The API does not expose individual piece positions.

The API registers a persistent paused or metadata-first torrent for the intake flag `preview`. A separate pre-import inspection may read fields already present in a magnet URI without registering it. Full peer-resolved metadata requires explicit queued intake. The selected named profile can supply a queue priority. Per-import priority overrides, skip-recheck, token expiry, independent web listener toggles and a restore-configuration-defaults action have no corresponding controls in the current service. Reset downloads has a different, destructive scope. Feed and log details must use fields actually returned.

## Illustrated controls with different service semantics

| Illustration | Operational presentation |
|---|---|
| File integrity verified, file listing from an unresolved magnet | Local URI inspection shows declared metadata and the exact topic. Only a queued import can resolve remote metainfo. The Files tab explains the boundary. |
| Per-piece position map and event log per torrent | Completion grid shows the aggregate piece count. The inspector derives recent activity from the torrent and tracker timestamps and identifies each observation. |
| Saved view badges | Named views store their queries locally. The rail uses serialized count-only server queries and shows each view’s last observed count. Active library reads refresh the matching count. |
| All containment checks pass with containment disabled | Show the reported mode, traffic permission, fail-closed configuration, diagnostics and per-check status. Refresh diagnostics reads current service observations. The separately configured port test has an explicit action. |
| Authentication expiry, independent UI listener, request tracing switch, defaults reset | Display token lifetime and the listener relationship. Use the existing Logging settings for daemon logging. The separate confirmed reset action names its actual scope. |
| All feeds active, imports today, last successful scan time | Show enabled subscriptions, recorded feed imports, pending files and last feed poll from their respective sources. Scan results and decision histories retain their own scopes. |
| Thousands of logs and arbitrary trace information | Counts describe the bounded browser view. Render original text and any structured fields supplied by the source. A related subscription requires an actual feed identifier in the event. |
| Hidden paste-anywhere shortcut | The Add field accepts normal paste. File dropping retains the original global drop behavior. |
