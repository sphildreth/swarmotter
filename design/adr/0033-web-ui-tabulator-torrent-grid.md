# ADR-0033: Web UI Tabulator Torrent Grid

## Status

Accepted

## Context

The Web UI torrent list needs standard table behavior expected by operators:
clickable column sorting, reversible sort direction, per-column filters, stable
selection while the daemon refreshes torrent summaries, and room for future
large-library behavior. The existing implementation rebuilt a plain HTML table
body on every refresh and only supported a global name filter.

Hand-rolling a richer table would push sorting, filtering, column UI,
selection preservation, accessibility, and refresh behavior into local
application code. A headless table library would still require SwarmOtter to
build most of that UI.

## Decision

Use Tabulator 6.5.0 as a vendored browser asset for the Web UI torrent list.
The daemon serves the Tabulator JavaScript, theme CSS, and license from
embedded static assets under `/vendor/tabulator/`; the Web UI does not depend
on runtime CDN access or a frontend build step.

Tabulator owns the torrent grid's column sorting, header filters, movable
columns, and row refresh behavior. SwarmOtter keeps its existing torrent
selection state, bulk remove workflow, row action buttons, health rendering,
and periodic API refresh logic around the grid.

Tabulator is used only in the browser control-plane UI. It is not used by the
torrent data plane and does not create torrent sockets, DNS lookups, tracker
requests, peer connections, webseed requests, DHT traffic, or PEX traffic.

## Consequences

The torrent list gains standard table capabilities without growing a local
table framework inside `app.js`. Sorts and header filters survive the normal
polling refresh because the UI updates Tabulator row data rather than
rebuilding DOM rows directly.

The repository now carries a vendored MIT-licensed frontend dependency and its
license notice. Future Tabulator upgrades must review license compatibility,
release notes, accessibility behavior, bundle size, and whether any new feature
would introduce browser network traffic outside SwarmOtter's normal API/Web UI
control plane.

The Web UI remains framework-light: Tabulator is the table/grid component, not
a general application framework. If future requirements introduce server-side
pagination, grouping, saved filters, or large-library query APIs, those API
contracts should be designed explicitly and may require a new ADR.

## Related Documents

- [Function-over-form Web UI ADR](0006-function-over-form-web-ui.md)
- [Third-party licenses](../../THIRD_PARTY_LICENSES.md)
- [Web UI guide](../../docs/web-ui.md)
