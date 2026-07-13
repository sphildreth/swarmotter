# ADR-0063: Contained MSE/PE for TCP and uTP with Effective Encryption Policy

## Status

Accepted

## Context

ADR-0039 introduced MSE/PE for contained TCP peer streams with a single global
encryption mode. That left a visible compatibility gap: a required mode could
not use the already-contained uTP byte stream, and operators could not express
a different encryption requirement for a named profile or individual torrent.

Peer-wire encryption must not create a second socket path, weaken fail-closed
containment, or quietly turn a required encrypted session into a plaintext one.
The process-wide inbound TCP listener also needs to identify a torrent before
it can apply a profile or durable per-torrent override.

## Decision

- Treat MSE/PE as a wrapper around an already-established `PeerDuplex` byte
  stream. Both TCP and `UtpStream` are obtained through `NetworkBinder` before
  MSE/PE begins; the encryption layer creates no socket, resolver, or fallback
  path of its own.
- Apply the existing modes consistently to outbound peer and BEP 9 metadata
  sessions over either selected transport:
  - `disabled` uses the raw contained stream;
  - `preferred` tries MSE/PE, then reconnects only the same selected contained
    transport as plaintext if negotiation fails; and
  - `required` negotiates MSE/PE and never makes a plaintext retry.
  TCP/uTP ordering remains controlled by `utp_enabled` and `utp_prefer_tcp`.
- Add optional `encryption_mode` to each named profile and a durable optional
  `policy.overrides.encryption_mode` on each torrent. Resolve one effective
  mode with deterministic precedence: explicit torrent override, explicitly
  assigned or label-selected profile, then global `[torrent].encryption_mode`.
  The native policy response includes both the value and its source.
- Expose the durable torrent override through
  `PUT /api/v1/torrents/:hash/encryption-mode`. The request must contain
  `encryption_mode`; a JSON `null` explicitly clears the override and restores
  profile/label/global inheritance.
- Persist a successful override or profile/configuration update before
  restarting affected active download/metadata engines. A profile or label-map
  replacement restarts only torrents whose effective mode actually changes;
  a global mode replacement retains the existing complete data-plane
  reconfiguration transaction. Active seeder registrations update their mode
  for subsequently accepted inbound TCP sessions. Existing negotiated sessions
  retain their established wire stream.
- The shared inbound TCP listener may inspect enough of a plaintext handshake
  or MSE/PE stream to identify its torrent, then rejects plaintext for an
  effective `required` mode and encrypted sessions for `disabled`. This ADR
  does not add a production inbound uTP listener or a per-profile network
  path.

## Consequences

- Required encryption works over contained uTP as well as TCP and never
  silently downgrades to plaintext. Preferred mode keeps interoperability
  without changing the configured transport preference.
- Operators can make a policy class or a single torrent stricter or looser
  without copying a global setting into unrelated torrents. The API and Web UI
  can explain why a mode applies and can distinguish inheritance from an
  explicit `null` clear operation.
- Durable state and configuration replacement retain their existing rollback
  boundaries: failed persistence leaves the preceding override/configuration
  and live engines in force.
- MSE/PE remains peer-wire interoperability behavior, not a proxy, anonymity,
  tracker, DHT, webseed, or network-path feature. TCP and uTP stay subject to
  the same contained binder and fail-closed policy.

## Related Documents

- [ADR-0039: TCP MSE/PE Protocol Encryption](0039-tcp-mse-pe-protocol-encryption.md)
- [ADR-0020: uTP Implementation Strategy](0020-utp-implementation-strategy.md)
- [ADR-0057: Policy Profiles and Inherited Torrent Settings](0057-policy-profiles-and-inherited-settings.md)
- [Configuration design](../configuration.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Testing strategy](../testing.md)
