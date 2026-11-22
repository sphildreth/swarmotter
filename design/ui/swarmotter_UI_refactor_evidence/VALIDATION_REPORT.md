# Web UI refactor validation

All six workspaces run in the embedded application. Torrents remains the index. The shared header and telemetry are DOM elements. The two charts use SVG and measured rates. None of the design screenshots is served by the production asset router. All six canonical pane bounds match the measurements in the visual specification. The unmasked comparisons still show differences from the approved images, especially in the original Torrents table and inspector.

## Inspect the result

Every pair shows the approved reference on the left and the application on the right. The overlay blends both complete images equally. Difference images show absolute differences in the original sRGB channel values.

| View | Actual | Side by side | 50% overlay | Absolute difference |
|---|---|---|---|---|
| Torrents | [Capture](actual/torrents.png) | [Pair](comparisons/torrents.png) | [Overlay](overlays/torrents.png) | [Difference](diffs/torrents.png) |
| Add | [Capture](actual/add.png) | [Pair](comparisons/add.png) | [Overlay](overlays/add.png) | [Difference](diffs/add.png) |
| Network | [Capture](actual/network.png) | [Pair](comparisons/network.png) | [Overlay](overlays/network.png) | [Difference](diffs/network.png) |
| Settings | [Capture](actual/settings.png) | [Pair](comparisons/settings.png) | [Overlay](overlays/settings.png) | [Difference](diffs/settings.png) |
| Watch | [Capture](actual/watch.png) | [Pair](comparisons/watch.png) | [Overlay](overlays/watch.png) | [Difference](diffs/watch.png) |
| Logs | [Capture](actual/logs.png) | [Pair](comparisons/logs.png) | [Overlay](overlays/logs.png) | [Difference](diffs/logs.png) |

The shared-shell strip stacks the Torrents reference, live Torrents and live Add at native scale. Each row uses the same x 0–1672 and y 0–145 coordinates. The browser captures share a byte-identical telemetry strip across all six views. The navigation comparison allows differences only within the outgoing and incoming active buttons and their shadow extents. [Shared-shell comparison](comparisons/shared-shell.png)

## Direct daemon comparison

Playwright visited the embedded Rust server on `http://127.0.0.1:9091/` in a fresh Chromium context with its ordinary clock and live API. At 1672 × 941 all six views matched the measured header, telemetry and three pane bounds. The browser recorded zero failed requests, console errors, uncaught page errors or writes. This instance used a disposable empty library, strict containment on loopback and its own daemon log. Add shows a locally inspected URI. The Watch and Torrents captures show their empty states. [Requests and measurements](live-daemon/browser-results.json)

| Live comparison | Live comparison |
|---|---|
| [Torrents](live-daemon/comparisons/torrents.png) | [Settings](live-daemon/comparisons/settings.png) |
| [Add](live-daemon/comparisons/add.png) | [Watch](live-daemon/comparisons/watch.png) |
| [Network](live-daemon/comparisons/network.png) | [Logs](live-daemon/comparisons/logs.png) |

The direct run exposed an amber panel for healthy strict containment and amber selection styling on informational log entries. The final Network capture shows the healthy shield in green. The final Logs capture shows the selected informational event in teal. Warnings retain their amber treatment. The empty library and RSS preview now give instructions appropriate to their state. [Unmasked overlays](live-daemon/overlays/) · [Absolute differences](live-daemon/diffs/)

In a separate browser context, the same daemon served the document, CSP, scripts, fonts and images while the existing test fixture supplied the illustrated API state. The resulting PNGs had identical SHA-256 hashes to all six recorded reference-state captures above. [Reference-state checksums](live-daemon/reference-state-results.json)

## Reproduce

Run from the repository root. The browser tools used here were Node 22.23.1, Playwright Core 1.64.1 and its Chromium 156.0.8078.4. Python comparison used Pillow 12.3.0 and NumPy 2.5.5. Install Playwright Core in a local test-tools directory or expose an existing installation through `PLAYWRIGHT_CORE`. Its matching Chromium browser needs to be installed. For a new test-tools installation, the following commands keep its dependencies outside the repository.

```sh
ui_tools=$(mktemp -d)
npm install --prefix "$ui_tools" playwright-core@1.64.1
"$ui_tools/node_modules/.bin/playwright-core" install chromium
export PLAYWRIGHT_CORE="$ui_tools/node_modules/playwright-core"

node crates/swarmotter-web/tests/visual/capture.mjs
python crates/swarmotter-web/tests/visual/compare.py
cargo build -p swarmotterd
node crates/swarmotter-web/tests/visual/daemon-smoke.mjs
```

With the local daemon already running, these commands reproduce both browser passes. The second pass directs all API requests, including test mutations and the event stream, to an in-memory test server. It writes the populated captures to a temporary directory.

```sh
node crates/swarmotter-web/tests/visual/live-capture.mjs http://127.0.0.1:9091
python crates/swarmotter-web/tests/visual/compare.py --live
ui_capture_dir=$(mktemp -d)
UI_DAEMON_ORIGIN=http://127.0.0.1:9091 UI_EVIDENCE_DIR="$ui_capture_dir" node crates/swarmotter-web/tests/visual/capture.mjs
diff -qr design/ui/swarmotter_UI_refactor_evidence/actual "$ui_capture_dir/actual"
```

The capture server binds an ephemeral loopback port. It serves the production HTML, CSS, grid, ES modules, images and fonts with in-memory v1 responses. The clock starts at 2026-11-21 21:40:12 UTC. The harness advances it while reading telemetry through the production sampler. It specifies en-US, UTC, dark theme, reduced motion, 1672 × 941, device scale one and browser-default zoom. It waits for API-backed content, selected rows, Web fonts and decoded images. The fixture stores named queries in an isolated browser context. The operational checks run after canonical capture. The production router has no fixture path or activation switch.

Two consecutive runs in fresh Chromium processes produced identical SHA-256 hashes for each of the six full-size application screenshots. The capture script uses one raster thread and disables GPU and partial rasterization to make glyph and SVG rendering repeatable.

The pre-change captures came from the actual embedded daemon on a private loopback listener, with fresh temporary state, strict loopback source binding and DHT disabled. They show the empty starting application at commit `ef86aa11014c0925d87fc708f94e1edc49e4a3b3`. The source PNG hashes are checked on every comparison. The original PNGs remain the design authority. [Before refactor](baseline/) · [Source manifest](reference-manifest.json)

## Visual findings

The region report records each crop box, mean absolute channel error, count of differing pixels and surface samples. All reference comparisons use native-sized pixels, zero noise tolerance, no registration shifts and no masks. A high or low aggregate score does not decide acceptance. [Region measurements](region-results.json) · [Browser geometry](layout-results.json) · [Visual specification](VISUAL_SPEC.md)

| Region | Expected and observed | Correction and remaining difference |
|---|---|---|
| Global header and telemetry | Header 59 px. Telemetry starts at (9,69) and measures 1654 × 66 on every route. At (1000,25) both headers are RGB (13,15,18). The reference and browser share the healthy dot, arrow directions, cylinders, circled controls, drive and gauge. | Replaced the original header and corrected glyph baselines and spacing against the native image. The pause glyph’s bright-pixel bounds match. The database outline extends one pixel above and right of the reference. The supplied project otter bitmap, stroke contours, font outlines, dashboard values and rate curves are visibly distinguishable. |
| Torrents rail, toolbar and table | Panes start at x 9, 271 and 1273. Center footer starts at y 867. The first torrent name occupies x 356–532 and y 329–339 in both images, measured on bright text pixels. | Removed the grid’s inherited 88-rem minimum, corrected table-header height, progress/status treatment and name width. Individual glyphs, row dividers and badge shapes differ. A partially selected page has an indeterminate header checkbox. The fixture uses queue order to produce the illustrated sequence. Sort arrows represent the actual sort. |
| Torrents inspector | Original shows a spatial piece map and supplied activity descriptions. The application displays completed piece counts, tracker observations and dated activity, plus access to full operational detail. | The grid preserves the map’s footprint. Its cells represent aggregate completion. The reference’s spatial pattern, peer-connection history and exact time estimate cannot be established from the summary. File sizes, calculated ETA and tracker timestamps follow the fixture’s arithmetic. |
| Add | Center begins at x 268. Method tabs begin at y 222. Both magnet fields span y 335–389. The import strip begins at y 708. The form’s lower border differs by about one pixel. | Reused header and telemetry and matched input, form and strip geometry. URI inspection shows declared name, size, exact topic and tracker count. The reference’s checksum-file row and verified-content label would overstate the available evidence. Start policies remain exclusive. Native form text and the right-hand note wrap differently. |
| Network | Four metric cards begin at y 222. The chart panel ends at y 604 and the next panel starts at y 616. Inspector and chart use the same workspace bounds as the reference. | Aligned chart, check panel and warning surfaces. Lines use the observed rates, including a distinct upload series. Disabled enforcement is visible in the checks. Interface count, addresses, mapping status and test state use actual response fields. These values change portions of the inspector’s vertical spacing. |
| Settings | The API endpoint panel begins at y 281. Its lower border is at y 449 in the reference and y 450 in the capture. | Matched the field grid, authentication switch and warning strip. DHT and PEX retain their separate settings entries. Static examples of a hidden token, independent web listener switch and restore-defaults operation are replaced by the actual credential, logging and confirmed reset workflows. The inspector shows the information available before the first save. |
| Watch | RSS rows have a 67 px pitch. The first row begins around y 424 in the reference and y 423 in the application. Left and right panes have the companion bounds. | Matched row pitch, selected accent, preview spacing and pinned actions. The fixture has three subscriptions, two enabled, two folders and six recorded imports. The service does not provide the illustration’s daily-import counter or last-successful-scan time. The inspector distinguishes polling history, count scope and stable subscription ID. |
| Logs | Table header begins at y 277. Header/body boundary is at y 315–316 in both images. Rows have a 37 px pitch. The native full-message inspector and Copy event action remain available. | Matched the table origin, warning selection, monospace layout and footer. Counts reflect 13 fixture records. Structured fields, retry information and a related subscription come from the selected fixture record. Long original messages remain available in a disclosure. Native timestamp precision and the available field set differ from the sample. |

The fixture’s 128 torrents contain 12 downloading, 108 seeding and 8 paused. Their declared sizes sum to 18.4 TiB. The visible rows alone exceed the illustration’s 6.8 MiB/s aggregate upload, so the fixture derives 12.1 MiB/s from the library. Saved-view and category counts are calculated from those same records. The transfer strip uses global statistics throughout filtering.

## Functional verification

| Area | Exercised |
|---|---|
| Shared application | All six routes, persisted theme, active navigation, independent panes, one statistics request per sampling interval and one active event receiver after repeated navigation. |
| Torrents | Server-side search, state filter, sort, named query save/load, off-page inspector identity, independent bulk targets, pause/resume/recheck with a partial failure and confirmed removal requesting `delete_data: false`. |
| Add | Invalid magnet, local inspection with zero queued torrents, preserved draft across tabs, explicit start mode, failed import retaining input, duplicate-click suppression, profile/labels/destination payload, binary upload and local torrent creation. |
| Network | Read-only diagnostics, failure and recovery, unavailable health, disabled containment and explicitly initiated port-test request. The disposable daemon used strict containment. External reachability and router mapping were not invoked against a real network. |
| Settings | Native value validation, dirty indication, cancelling and accepting reload, server rejection retaining edits, successful persistence, restart response, folder controls and retaining RSS on unrelated edits. The disposable daemon test reads the updated TOML. |
| Watch | Saved and draft preview, feed selection by ID, registration, editing, disabling polling, explicit poll and scan, folder controls and cleanup preview. A real loopback RSS preview leaves the subscription list empty until the subsequent explicit save. |
| Logs | File tail, authenticated fetch/SSE connection, severity and text filters, event copy, filtered export, on-screen pause/resume, CRLF input, bounded 500-row view, disconnect/reconnect, local clear and reloading retained daemon logs. |
| Responsive and keyboard | All six routes at 1280 × 800 and 390 × 844. Tests check document width, selection and inspector reachability, Escape and focus restoration. The mobile library check also verifies a visible first row and no toolbar/search overlap. |

The fixture tests observe request methods, stable IDs, payloads and UI state. The embedded daemon smoke test separately creates metainfo from a temporary local file, uploads it paused, inspects its real file list, reads strict diagnostics, persists a non-credential setting, previews and saves a disabled loopback feed, and reads the actual daemon log tail and event stream. Its temporary directories, state, configuration and listener are scoped to the test. Destructive data deletion, credential rotation, router mapping and external feeds were not exercised against user data. Existing Rust tests cover the engine, local swarms, containment, API actions, deduplication, cleanup, and durable state. [Browser assertions](browser-results.json) · [Secondary screenshots](secondary/) · [Capability map](CAPABILITY_MAP.md)

Repository checks executed from the root:

```sh
cargo fmt --all
cargo fmt --all --check
cargo check
cargo test
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check
scripts/check-web-js-modules.sh
scripts/check-web-ui-startup.sh
node crates/swarmotter-web/tests/seeding-policy.test.js
node crates/swarmotter-web/tests/watch-history.test.js
git diff --check
```

Formatting, compilation, Clippy, JavaScript checks and the embedded-daemon smoke test passed. The workspace test run passed 1,085 tests with zero failures. Three opt-in scale and persistence profiles retained their default ignored status. The final web-crate rerun passed all 40 tests. The supply-chain audit passed advisories, bans, licenses and sources, with informational duplicate-version warnings.
