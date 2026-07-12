# ADR-0050: Bounded Untrusted Metainfo Parsing

## Status

Proposed

## Context

`swarmotter-core`'s bencode decoder accepted recursive input without a depth
or node budget, performed unchecked string-bound arithmetic, and did not
require the decoder to consume the entire input. `meta.rs` did not impose a
complete metainfo shape budget. Engine paths narrowed the declared piece
length and allocated piece buffers after parsing. Watch-folder imports read
an entire file before applying the API/BEP 9 metadata-size policy. Malformed
durable piece-hash data could reach an exact-slice copy without first checking
the decoded length.

A crafted `.torrent` file or magnet metadata payload could therefore consume
unbounded stack, memory, or disk-read resources or panic the bencode decoder.
Malformed JSON daemon state could independently panic while decoding a piece
hash. These are adjacent but distinct trust boundaries: API uploads, watch
folders, magnet metadata, and direct bencoded parser callers use the shared
bencode decoder, while restored daemon state bypasses that decoder and requires
validation of its deserialized `TorrentMeta` values and piece hashes.

## Decision

Define one shared set of public metainfo limits in `swarmotter-core/src/meta.rs`.
Enforce the byte, depth, and node budgets at every bencoded metainfo ingress.
Enforce the file-count, piece-count, and piece-length limits both while building
parsed metainfo and when validating a `TorrentMeta` restored from JSON state:

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

Raw API uploads are streamed into an accumulator bounded by the lower of
`api.max_request_body_bytes` and `MAX_TORRENT_METADATA_BYTES`. A lower configured
API limit retains its HTTP 413 `payload_too_large` contract; when that limit
permits a larger request, the metadata limit returns `MalformedTorrent`. Bulk
and Transmission base64 metainfo use a bounded decoder that checks output length
before reserving or appending each decoded byte.

BEP 9 applies the byte limit to the raw `info` dictionary advertised and
assembled on the wire. After that dictionary passes the shared bencode budgets,
core metainfo construction parses it directly and attaches trusted tracker
context as values; it does not add an internal bencode wrapper that would make
an exact-limit wire payload appear oversized.

BEP 9 message dictionaries use the same hardened parser through a prefix-decode
entry point that returns the consumed header length while preserving the
trailing binary piece. It retains the depth, node, duplicate-key, grammar, byte,
and checked-arithmetic rules of full-document decoding. Advertised sizes,
per-message totals, assembled length, and the final info hash are validated
before the metadata reaches metainfo construction.

In durable-state deserialization, the piece-hash sequence is capped at
`MAX_TORRENT_PIECES`. Each encoded SHA-1 hash must represent exactly 20 bytes
before hex decoding and copying. Errors include torrent record and piece index
context and carry no payload data or content paths. Before restored metadata
reaches runtime engine or storage paths, `TorrentMeta::validate()` enforces the
applicable shape limits and invariants.

API uploads, watch folders, magnet metadata, and direct core callers that accept
bencoded bytes share the byte, depth, and node budgets. Restored daemon state is
JSON and is therefore not described as passing through the bencode decoder or
the 16 MiB bencoded-document limit.

## Consequences

Crafted bencoded metainfo is rejected within fixed parser and shape budgets.
Malformed durable piece hashes are rejected before copying, and invalid
restored `TorrentMeta` shapes are rejected before runtime use. Operators receive
typed errors with enough context to identify the offending torrent record. The
separate bencoded-input and restored-state trust boundaries are documented in
`design/architecture.md`; operator-visible limits are documented in
`design/api.md`, `docs/api.md`, `design/configuration.md`, and
`docs/configuration.md`. Adversarial corpus cases are part of the required test
set in `design/testing.md`.

## Related Documents

- `design/2026-07-12.REVIEW.md` (Phase 1, concern C-01)
- `design/architecture.md`
- `design/api.md`, `docs/api.md`
- `design/configuration.md`, `docs/configuration.md`
- `design/testing.md`
- `CHANGELOG.md`
