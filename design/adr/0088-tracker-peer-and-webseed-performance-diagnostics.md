# ADR-0088: Tracker, Peer-Source, and Webseed Performance Diagnostics

## Status

Accepted

## Context

Operators diagnosing a weak swarm need to follow discovery from a tracker or
webseed request through peer yield and transfer outcome. Existing policy
profiles already control tracker priority and enablement, and the engine already
retains some tracker announce/scrape state. That evidence was incomplete:
effective policy was not shown per tracker, last attempt was conflated with last
success, latency/peer yield were absent, peers had no discovery provenance, and
webseed contribution/errors were not exposed. Maintenance actions were also
missing, and copied diagnostics could expose tracker passkeys.

## Decision

- Extend tracker runtime evidence with last attempt, last success, consecutive
  retries, latency, measured peer yield, bounded recent errors, and a sample
  status (`current`, `stale`, `unavailable`). `peers_returned = null` means no
  sample; `0` is a measured zero yield. A successful tracker sample remains
  visible when another tracker fails.
- Expose effective tracker policy per URL: allowed/disabled, priority (`low`,
  `normal`, `high`), and policy source. Combine profile host rules with explicit
  per-torrent exact-URL disables. Disabled trackers remain visible with
  `disabled` status rather than disappearing.
- Attribute peer discovery where observed: tracker URL, DHT, PEX, direct, or
  resume. Retain multiple sources per peer; empty provenance means unknown, and
  no source is invented. Tracker results carry per-URL peer lists into engine
  state; DHT/PEX/direct call sites record their actual source.
- Expose enabled webseeds with status, verified byte contribution, attempts,
  retries, last success/attempt, latency, last error, and bounded recent errors.
  A failing tracker/peer source does not hide a successful webseed or another
  successful tracker.
- Add safe maintenance operations: retry-now, enable/disable one exact tracker,
  failure-threshold cleanup preview, confirmed cleanup apply, and copyable
  diagnostics. Preview lists affected tracker URLs; apply revalidates. All
  maintenance uses the existing authenticated control plane and durable state.
- Redact tracker user information and sensitive query values (passkey, key,
  token, auth, secret, signature) from copied diagnostics. Bounded recent-error
  lists and a diagnostics size cap keep large libraries bounded.
- Keep all discovery/announce/scrape/webseed traffic on the existing contained
  binder. Diagnostics only read retained runtime evidence and never open a
  network path. Local tracker/peer/webseed fixtures exercise success/failure
  histories and effective policy changes.

## Consequences

- Operators can distinguish “never measured,” “measured zero,” stale samples,
  and failures, and can correlate tracker/DHT/PEX/direct discovery with the peer
  list and transfer outcome.
- Tracker policy and runtime evidence are visible together without changing
  profile policy semantics; exact per-torrent disables are reversible.
- Cleanup preview is mandatory before disabling affected trackers, and copied
  diagnostics are safe to share after redaction.
- Runtime evidence is bounded to the configured trackers/webseeds and observed
  peers plus fixed-size recent-error lists; it does not grow with history.
- Future diagnostics must preserve sample semantics, provenance honesty,
  containment, bounded retention, redaction, and maintenance previews.

## Related Documents

- [ADR-0014: Tracker Implementation Strategy](0014-tracker-implementation-strategy.md)
- [ADR-0055: Contained HTTP/1 Client Framing and Redirect Policy](0055-contained-http1-client-framing-and-redirect-policy.md)
- [ADR-0057: Policy Profiles and Inherited Settings](0057-policy-profiles-and-inherited-settings.md)
- [ADR-0070: Discovery-Aware Empty-Swarm Recovery](0070-discovery-aware-empty-swarm-recovery.md)
- [API design](../api.md)
- [Architecture](../architecture.md)
- [Network containment](../vpn-network-containment.md)
- [Testing strategy](../testing.md)
- [Operator API](../../docs/api.md)
- [Web UI](../../docs/web-ui.md)
- [Troubleshooting](../../docs/troubleshooting.md)
