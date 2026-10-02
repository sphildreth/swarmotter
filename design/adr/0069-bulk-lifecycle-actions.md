# ADR-0069: Bulk Lifecycle Actions for Torrent Selections

## Status

Accepted

## Context

ADR-0031 added batch add and remove endpoints, but lifecycle operations still
required one request per torrent. The Web UI torrent table supports multi-row
selection (checkbox plus Select/Deselect All), yet the only selection action
was Remove Selected. Operators managing a library need to pause, resume
(unpause), recheck, or restart a whole selection — for example after a storage
move, a containment recovery, or a daemon upgrade — without issuing one
request per row and without individual rows failing the whole batch.

The single-torrent lifecycle endpoints (`pause`, `resume`, `recheck`,
`stop`, `start_now`) already preserve per-torrent rollback and durability
semantics. Restart has no single-operation endpoint: it means stop the live
engine and start the torrent again so engine, tracker, and announce state are
rebuilt from the durable registry.

## Decision

The native `/api/v1` API exposes bulk lifecycle actions, following the
ADR-0031 per-item result contract:

- `POST /api/v1/torrents/bulk/pause`
- `POST /api/v1/torrents/bulk/resume`
- `POST /api/v1/torrents/bulk/recheck`
- `POST /api/v1/torrents/bulk/restart`

All four accept `{ info_hashes: [locator, ...] }` and return
`{ action, succeeded, failed, not_found }`. Locators are parsed and deduped
first; malformed locators are reported as per-item `invalid_info_hash`
failures instead of failing the request. Each action is applied per torrent
through the existing `DaemonOps` lifecycle operations, so every item keeps the
exact rollback, persistence, and event behavior of the single-torrent
endpoint. `not_found` locators are reported without failing the batch, which
makes retries idempotent. Restart is implemented as `stop` followed by
`start_now`.

The Web UI selection toolbar gains Pause Selected, Resume Selected, Recheck
Selected, and Restart Selected buttons alongside Remove Selected. They share
one in-flight flag with bulk remove so selection controls stay disabled until
the batch completes, and they summarize succeeded, not-found, and failed
counts in toasts.

## Consequences

- Easier: multi-row lifecycle management in the Web UI and for API clients; a
  bad locator or a torrent that disappears mid-batch never blocks the rest of
  the selection.
- Harder: each item is applied with its own persistence and reconciliation
  pass, so a very large selection performs one durable write per torrent.
  This preserves per-item rollback semantics at the cost of batch width; if
  library-scale bulk transitions need single-reconcile batching, that is a
  follow-up daemon capability and must not change the per-item result
  contract.
- Required: new bulk actions must continue to classify results per item and
  must never turn a per-item failure into a whole-request error, matching
  ADR-0031 behavior.

## Related Documents

- [API docs](../../docs/api.md)
- [Web UI docs](../../docs/web-ui.md)
- [API design notes](../api.md)
- [Bulk torrent API operations](0031-bulk-torrent-api-operations.md)
- [Coalesced rapid add queue reconciliation](0030-coalesced-rapid-add-queue-reconciliation.md)
- [Paused torrent add API](0029-paused-torrent-add-api.md)
