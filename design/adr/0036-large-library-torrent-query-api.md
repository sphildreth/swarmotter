# ADR-0036: Large-Library Torrent Query API

## Status

Accepted

## Context

The P0 large-library operations console requires the Web UI to stay useful with
hundreds or thousands of torrents. The existing `GET /api/v1/torrents` endpoint
returns a full array and is already consumed by automation and compatibility
code. Changing its response shape would be a breaking native API change.

Operators also need count metadata, filtered totals, saved operational views,
pagination, grouping summaries, and server-side sorting without forcing every
poll to transfer the full torrent list.

## Decision

Keep `GET /api/v1/torrents` as the legacy full-list endpoint and add
`GET /api/v1/torrents/query` for large-library list operations.

The query endpoint accepts explicit control-plane filters and view state:
search text, state, health, label, storage root, performance condition, peer
and rate thresholds, sort field/direction, page size, page number, and optional
grouping. It returns paged `rows`, unpaged `total` and `filtered` counts, page
metadata, bucket counts, and optional group summaries.

The endpoint delegates to existing daemon summaries and performs only
control-plane filtering, sorting, counting, and pagination. It does not create
torrent sockets, DNS lookups, tracker requests, peer connections, webseed
requests, DHT traffic, PEX traffic, or network probes.

## Consequences

Existing `/api/v1/torrents` clients remain compatible. The Web UI and
automation clients that need large-library behavior can opt into the query
contract without a new API version prefix.

The server now owns stable semantics for large-list filtering and counts, while
the browser can still use Tabulator for current-page rendering, column
presentation, and client-side header filters. Future additions to saved views,
streaming deltas, or cursor pagination should extend this query surface rather
than changing the legacy list endpoint.

Because filtering and sorting use summary fields already exposed by the native
API, this decision does not weaken network containment or introduce a data-plane
bypass.

## Related Documents

- [Backlog feature: Large-Library Web UI Operations Console](../BACKLOG.md)
- [API reference](../../docs/api.md)
- [API design notes](../api.md)
- [Web UI guide](../../docs/web-ui.md)
