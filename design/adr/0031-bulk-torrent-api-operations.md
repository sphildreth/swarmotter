# ADR-0031: Bulk Torrent API Operations

## Status

Accepted

## Context

API clients and the Web UI can operate on large torrent sets. Adding hundreds
of torrents one request at a time is supported, but native clients also need a
single request shape for batch submission with per-item results. The Web UI
selection workflow also needs reliable removal of many selected torrents
without issuing one delete request per row.

Bulk removal must preserve the existing single-torrent delete behavior while
avoiding repeated queue reconciliation work for one selected operation.

## Decision

The native `/api/v1` API exposes batch torrent operations:

- `POST /api/v1/torrents/bulk` accepts JSON with `magnets`,
  `torrent_files`, and shared add-time options such as `download_dir`,
  `paused`, and `start_behavior`.
- `torrent_files` entries carry base64 `.torrent` bytes in a `metainfo`
  field, matching the compatibility adapter's existing metainfo convention.
- Bulk add returns `added` and `failed` arrays. Failures are per item and
  include `kind`, `index`, `code`, and `message`.
- `POST /api/v1/torrents/remove` accepts `info_hashes` and optional
  `delete_data`, and returns `removed` and `not_found` arrays.

The Web UI uses the bulk remove endpoint for selected torrent removal. The real
daemon removes all found torrents from the registry and queue, stops any live
torrent tasks for those hashes, performs optional data deletion, and reconciles
queue state once for the batch. The existing `DELETE /api/v1/torrents/:hash`
endpoint remains available and preserves `not_found` as an error for single
deletes.

## Consequences

Clients that submit or manage many torrents have a stable native batch API
without depending on repeated single-item request sequencing. One invalid or
duplicate add item does not prevent valid items in the same batch from being
registered.

Bulk remove is more reliable for Web UI selected-row workflows and avoids
queue reconciliation per selected torrent. Missing hashes are reported without
turning the whole batch into an error, which lets clients handle already-gone
items idempotently.

Bulk add request size remains governed by the configured API request body
limit.

## Related Documents

- [API docs](../../docs/api.md)
- [Web UI docs](../../docs/web-ui.md)
- [API design notes](../api.md)
- [Paused torrent add API](0029-paused-torrent-add-api.md)
- [Coalesced rapid add queue reconciliation](0030-coalesced-rapid-add-queue-reconciliation.md)
