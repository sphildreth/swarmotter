# ADR-0050: Bounded Untrusted Metainfo Parsing

## Status

Accepted

## Context

`swarmotter-core`'s bencode decoder accepted recursive input without a depth
or node budget, performed unchecked string-bound arithmetic, and did not
require the decoder to consume the entire input. `meta.rs` did not impose a
complete metainfo shape budget. Engine paths narrowed the declared piece
length and allocated piece buffers after parsing. Watch-folder imports read
an entire file before applying the API/BEP 9 metadata-size policy. Malformed
durable piece-hash data could reach an exact-slice copy without first checking
the decoded length.

A crafted `.torrent` file, magnet metadata payload, or restored daemon-state
record could therefore consume unbounded stack, memory, or disk-read resources,
or panic the decoder. The same trust boundary applies to API uploads, watch
folders, magnet metadata, restored state, and direct core parser callers.

## Decision

Define one shared set of public parser budgets in `swarmotter-core/src/meta.rs`
and enforce them at every untrusted metainfo ingress:

- `MAX_TORRENT_METADATA_BYTES = 16 * 1024 * 1024`
- `MAX_BENCODE_DEPTH = 128` (root is depth zero; entering a list/dict increments)
- `MAX_BENCODE_NODES = 250_000` (each integer, byte string, list, and dict is one)
- `MAX_TORRENT_FILES = 100_000`
- `MAX_TORRENT_PIECES = 750_000`
- `MAX_PIECE_LENGTH = 64 * 1024 * 1024`

The bencode decoder:

- Rejects input larger than `MAX_TORRENT_METADATA_BYTES` before parsing.
- Counts depth and nodes and rejects any node that would exceed its budget.
- Uses `checked_add` for every cursor/length calculation and verifies the end is
  no greater than input length before slicing or allocating.
- Rejects empty integers, leading zeroes other than `0`, negative zero, missing
  terminators, non-string dictionary keys, and duplicate dictionary keys. It
  continues to accept unsorted unique keys for interoperability; info-hash
  calculation still uses the original encoded `info` slice.
- Requires EOF after exactly one top-level value; trailing bytes are an error.
- Returns `CoreError::Bencode` or `CoreError::MalformedTorrent` with a short
  reason. No malformed input may panic.

Metainfo construction checks, before building `TorrentMeta`:

- `piece length` is in `1..=MAX_PIECE_LENGTH`.
- The pieces string is an exact multiple of 20 and its hash count does not exceed
  `MAX_TORRENT_PIECES`.
- File count does not exceed `MAX_TORRENT_FILES`.
- Total-length and file-offset additions are checked with `checked_add`.
- Empty torrents are invalid; a non-empty torrent provides exactly
  `ceil(total_length / piece_length)` hashes.

At every engine/storage boundary that needs a `u32` piece length, the code uses
`u32::try_from(meta.piece_length)` and returns `MalformedTorrent` instead of
narrowing with `as`. No piece-sized buffer is allocated until limits and
conversions pass.

Before reading a watch file, the daemon checks metadata length against
`MAX_TORRENT_METADATA_BYTES`, allocates only the checked length, and uses a
bounded read that rejects growth over the limit. The same limit applies to API
adds and BEP 9 metadata even when `api.max_request_body_bytes` is higher for
other requests.

In durable-state deserialization, each decoded SHA-1 piece hash is required to be
exactly 20 bytes before copying. Errors include torrent record and piece index
context and carry no payload data or content paths.

These limits apply equally to API uploads, watch folders, magnet metadata,
restored state, and direct core parser callers.

## Consequences

Crafted or corrupted metainfo can no longer exhaust stack, memory, or disk-read
resources or panic the daemon. Operators receive typed errors with enough
context to identify the offending torrent record. The fixed budgets are part of
the parser trust boundary documented in `design/architecture.md` and the limits
documented in `design/api.md`, `docs/api.md`, `design/configuration.md`, and
`docs/configuration.md`. Adversarial corpus cases are part of the required test
set in `design/testing.md`.

## Related Documents

- `design/2026-07-12.REVIEW.md` (Phase 1, concern C-01)
- `design/architecture.md`
- `design/api.md`, `docs/api.md`
- `design/configuration.md`, `docs/configuration.md`
- `design/testing.md`
- `CHANGELOG.md`