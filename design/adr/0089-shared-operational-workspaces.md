# ADR-0089: Shared operational workspaces in the embedded Web UI

## Status

Accepted.

## Context

The approved desktop design puts the library, intake, network, configuration, automation and log workflows under a single navigation bar and telemetry strip. The design package contains illustrative daemon data, gradients and a raster header. The daemon serves static assets and the browser already uses native ES modules, the v1 control API and a vendored Tabulator grid.

## Decision

Version 3.0.0 marks the change to the operator-facing Web UI workflows,
including separate inspection and bulk selection in the library. The native
API keeps its `/api/v1` namespace.

Keep the existing Axum asset router, entry URLs, native forms, theme preference, Tabulator and same-origin authentication. A shared DOM shell owns navigation and telemetry. The browser samples global statistics and containment health through the existing control plane. Rate history is bounded and survives view switches. File sizes use binary units.

Each primary workspace has a rail, working area and inspector. The library uses its own measured column widths. At compact widths, operators can open either side pane. Inspecting a torrent and selecting a bulk operation target have independent identities. Full torrent controls, diagnostic details, configuration sections, intake, and automation maintenance remain reachable from these workspaces.

Read-only magnet inspection describes fields present in the URI. Explicit metadata-first intake registers a torrent and uses the daemon’s existing contained metadata path. Watch decision previews and log-view controls retain their existing server-side boundaries. A browser-local, named list of queries supplements the existing saved default query. Pausing Logs freezes the visible entries while the bounded event receiver continues reading.

Use a loopback test adapter and the production assets to reproduce design states. Keep the source design, browser captures and regression evidence separately identifiable. Exercise persistence and import behavior against a disposable embedded daemon as well.

## Consequences

The UI ships without a frontend runtime dependency or externally hosted fonts. Adding a metric requires a defined data source and unavailable state. The first chart samples describe only time observed by this browser. A partial library query cannot establish a new library-wide size. Missing per-piece positions, unobserved events and unverified metadata are visible limits on the illustrated state.

Changes to shared chrome affect six views and require the six-route comparison. The browser test harness needs Chromium and Playwright Core. Neither is part of the daemon distribution.

## Related Documents

- [Operator Web UI](../../docs/web-ui.md)
- [Visual specification](../ui/swarmotter_UI_refactor_evidence/VISUAL_SPEC.md)
- [Verification and evidence](../ui/swarmotter_UI_refactor_evidence/VALIDATION_REPORT.md)
- [Network containment](../vpn-network-containment.md)
