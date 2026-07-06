# ADR-0039: TCP MSE/PE Protocol Encryption as Optional Transport Mode

## Status

Accepted

## Context

SwarmOtter requires broad swarm interoperability and must keep wire-level transport
behaviors inside the configured containment layer. Message Stream Encryption /
Protocol Encryption (MSE/PE) is table-stakes for many mainstream clients, and
some trackers and peers reject plaintext BitTorrent handshakes. BEP 8 is related
tracker peer-obfuscation context; the TCP peer stream phase implements the
de facto MSE/PE peer negotiation.

At the same time, SwarmOtter must not create new egress paths for peer traffic,
must fail closed on containment failure, and must avoid protocol-path complexity
beyond the selected phase scope.

## Decision

For the v1.1.0 phase, implement MSE/PE only for TCP peers using the existing peer
connection pipeline from `NetworkBinder`.

The daemon now supports:

- `[torrent].encryption_mode` with values:
  - `disabled` (plaintext permitted),
  - `preferred` (TCP attempts use MSE/PE first, with plaintext fallback),
  - `required` (refuse plaintext and allow encrypted stream only),
  with default `preferred`.
- No separate sockets for encryption mode.
- No containment bypass: encrypted and plaintext peer traffic both use the same
  contained TCP peer path.

`required` is explicitly enforced at negotiation; plaintext-only acceptance is not
silent. `preferred` preserves the configured TCP/uTP transport ordering; it does
not force TCP ahead of uTP when `torrent.utp_prefer_tcp = false`.

## Consequences

- TCP interoperability improves against peers and private trackers requiring MSE.
- Wire-level compatibility is documented as interoperability and traffic-handshake
  compatibility, not as evasive behavior.
- No separate socket allocation path is introduced, so `NetworkBinder` and existing
  fail-closed logic remain authoritative for peer transport.
- uTP encryption and per-profile/per-torrent overrides remain out of scope for
  this phase and stay in the backlog.
- Changing the encryption mode requires restarting or recreating existing torrent
  tasks; new tasks use the updated configuration.

## Related Documents

- `design/BACKLOG.md` (`Protocol Encryption / MSE-PE` section)
- `design/COMPARISON.md`
- `docs/configuration.md`
- `docs/api.md`
- `docs/web-ui.md`
- `README.md`
- `CHANGELOG.md`
- `design/configuration.md`
- `design/api.md`
