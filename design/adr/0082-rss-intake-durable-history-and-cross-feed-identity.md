# ADR-0082: RSS Intake, Durable History, and Cross-Feed Identity

## Status

Accepted

## Context

SwarmOtter operators need to follow distribution feeds without relying on
bundled discovery providers. RSS is a user-configured intake channel, not a
search or indexer feature. It therefore has to reuse the existing bounded
metainfo, policy profile, queue, lifecycle, and fail-closed containment paths
rather than introduce a second add workflow.

RSS also creates state that outlives the library: a feed may advertise the same
torrent under another title after the original download completed and was
removed by selfish mode. Re-deciding from the live library alone would add that
torrent again. The durable evidence must be written at the same time as the
library change, survive crashes, share identity across v1, v2, and hybrid
torrents, and remain recoverable with the main database.

## Decision

- RSS subscriptions are validated configuration entries (`[rss]`) with a
  stable id, feed URL, enable/pause flag, interval, intake profile, start
  behavior, first-poll mode, labels, and include/exclude rules. The native API
  and Watch UI edit them through the daemon's full configuration transaction.
  A subscription id is immutable; renaming is delete plus create.
- Feed XML is parsed by a bounded, dependency-free RSS 2.0 and Atom 1.0 reader. Input size
  and item count are capped. A document without a recognized item or Atom entry is an
  error, not an empty successful feed. Listing identity is the RSS GUID or Atom entry ID when
  present, then the documented fallback link, first enclosure URL,
  `title|pubDate`, and finally title. Repeated unchanged polls update discovery
  timestamps but do not create additional listing rows.
- Rules are case-insensitive `*`/`?` globs over title and category. Every
  populated field in a rule must match; a missing category cannot satisfy a
  category condition. Any include rule qualifies, an empty include list accepts
  all, and any matching exclusion wins. Reasons name the rule number and the
  failing field.
- The first successful feed poll establishes the baseline. `new_only` records
  current matches as seen without adding them. `current_matches` is the explicit
  alternative and imports current rule matches. The poll endpoint also accepts
  an explicit `import_current=true` action. Preview performs the same rule,
  identity, and duplicate checks but never writes a listing, marks an entry
  seen, or adds a torrent. An explicit reconsider/retry action re-evaluates
  stored older listings.
- Accepted magnets and torrent attachments use the existing
  `DaemonOps::add_magnet` / `add_torrent_file` mutation with the subscription's
  profile, labels, and start behavior. The profile governs files inside the
  torrent; RSS rules only decide whether a listing is offered. Page-only
  listings, malformed attachments, and HTTP failures produce distinct decisions,
  and one malformed attachment does not stop valid siblings.
- Feed requests, DNS, redirects, and attachment downloads use the shared
  contained HTTP/1 client. The client has no independent connector, resolver,
  pool, or authorization path. RSS adds bounded feed/attachment policies,
  conditional `If-None-Match` / `If-Modified-Since`, exact ETag/Last-Modified
  capture, and a configurable but validated timeout. Loss of the configured
  path fails closed and leaves eligible work retryable. URL user information is
  rejected; sensitive query values are redacted from status and logs.
- One dedicated SQLite database, `<state file>.rss.sqlite` by default (or
  `rss.history_file`), is shared by every subscription. It holds poll state,
  listings, listing identity attribution, decisions, and verified completions.
  `listing_identities(alias_key)` and `completion_aliases(alias_key)` are
  primary-key indexes over full 40-character v1/hybrid-primary and
  64-character v2 locators. A hybrid's v2 locator is an alias of its v1
  canonical key; peer-wire truncations are never stored.
- The main state database gains a transactional `rss_outbox`. Every verified
  completion and removal path, including explicit removal, selfish completion,
  the selfish sweep, and recheck transitions, writes an event in the same
  transaction as the library change. Completed evidence is captured before
  removal can delete the record. The RSS consumer applies events idempotently
  (`INSERT OR IGNORE` completion/aliases, idempotent dismissal) and only then
  acknowledges and deletes the outbox rows. A crash before or after apply
  therefore replays one logical event.
- Duplicate checks consider stored completions, unacknowledged completion
  events, listing identity attribution, and the live registry. If the history
  database cannot be opened or checked, automatic adds are deferred with a
  visible error rather than risking a duplicate. An existing paused or
  incomplete torrent is `already_present` and keeps its state. A verified
  completion from RSS, watch, or manual intake is `previously_completed` for a
  later RSS listing. A removed unfinished RSS import is `dismissed` for that
  listing until explicit retry.
- Transient failures retain a durable listing row and a bounded exponential
  retry schedule even when the listing leaves its feed. Retries, polls per
  subscription, and the configured global concurrency bound are serialized or
  bounded. Conditional 304 responses are successful polls and do not multiply
  rows.
- Routine retention prunes bounded decision and non-imported listing rows only.
  Imported/completed listing attribution and all completion identities survive
  pruning and subscription deletion. Only an explicit authenticated clear
  request deletes completion aliases.
- The paired RSS database is backed up, verified, and restored together with the
  main state file. The updater archives both files and their SQLite sidecars;
  the offline verifier validates SQLite integrity and the required RSS tables
  when the history database is present. Absence remains valid for installations
  that never enabled RSS. On upgrade, completion backfill is limited to
  torrents whose current durable state supplies completion evidence; downloads
  removed before tracking began cannot be reconstructed and are not claimed.

## Consequences

- Operators can preview and explain feed decisions before intake, then rely on
  the same profile, queue, storage, and containment behavior as manual adds.
- One dedicated history database keeps cross-feed identity compact and indexed
  while preserving per-feed listing attribution.
- The transactional outbox adds a second database to recovery and backup scope.
  A missing or corrupt history database defers automatic RSS intake rather than
  silently re-adding completed content.
- A suffixless download endpoint is recognized by a path segment or an explicit
  download action. Hostnames alone cannot identify torrent files.
- TorrentDownload and LimeTorrents publish an expected v1 info hash alongside
  a public detail link. Only these origin-checked formats permit a single
  contained HTTPS detail request, capped at 512 KiB with redirects disabled.
  A magnet anchor must match the advertised hash. Other detail pages are
  recorded as page-only. This scope keeps unrelated pages, recommendations and
  account workflows outside automatic intake.
- Version 2 of the RSS history schema repairs older failed or unsupported
  baseline rows when retained decisions establish their original exclusion.
  Scheduled polling preserves this state until explicit operator action.
  Other LimeTorrents listings whose old attachment failed on a downgrade can
  use the verified page magnet when they next appear in the feed.
- Feed and attachment parsing remain bounded and local. No search, indexer, or
  external discovery service is introduced.
- Future RSS changes must preserve the fail-closed contained client, rule
  precedence, baseline semantics, full-identity aliases, transactional outbox,
  idempotent handoff, retention of completion evidence, and paired backup
  behavior.

## Related Documents

- [ADR-0054: Watch-Folder Stability, Idempotence, and Import Atomicity](0054-watch-folder-stability-idempotence-and-import-atomicity.md)
- [ADR-0055: Contained HTTP/1 Client Framing and Redirect Policy](0055-contained-http1-client-framing-and-redirect-policy.md)
- [ADR-0065: BEP 52 V2 and Hybrid Torrent Identity](0065-bep-52-v2-hybrid-torrent-identity.md)
- [ADR-0066: Policy-Driven Metadata-First Intake](0066-policy-driven-metadata-first-intake.md)
- [ADR-0067: SQLite Durable Library State](0067-sqlite-durable-library-state.md)
- [Feature backlog](../BACKLOG.md)
- [Configuration design](../configuration.md)
- [API design](../api.md)
- [Network containment](../vpn-network-containment.md)
- [Testing strategy](../testing.md)
- [Operator configuration](../../docs/configuration.md)
- [Operator API](../../docs/api.md)
- [Web UI](../../docs/web-ui.md)
- [Deployment and backup](../../docs/deployment.md)
