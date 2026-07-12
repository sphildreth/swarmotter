# ADR-0047: Transactional Live Data-Plane Reconfiguration

## Status

Accepted

## Context

Torrent engines, DHT runners, tracker tasks, inbound listeners, and accepted
peer sessions capture a network binder and transport settings when they start.
Replacing configuration without rebuilding those tasks can leave traffic on an
obsolete interface, source address, namespace, port, or encryption policy.
Declaring all such changes restart-required avoids that bug but leaves an
important operational configuration surface unnecessarily inconsistent.

## Decision

Apply network and torrent data-plane configuration changes as one serialized
daemon transaction.

- Full replacements are guarded by a configuration transaction lock and are
  validated before persistence or runtime mutation.
- Persistent configuration replacement uses a unique mode-`0600` temporary
  file, file and parent-directory synchronization, and atomic rename.
- A change to network containment, listen port, IP-family policy, uTP policy,
  peer encryption mode, or DHT configuration first reconciles progress, then
  stops and awaits all engines, tracker sidecars, DHT work, the shared inbound
  listener, and accepted peer sessions created under the old policy.
- Engine construction and handle installation use the same transition lock as
  configuration replacement. A task cannot start between old-task shutdown
  and the configuration swap, and concurrent reconciliations cannot detach a
  duplicate engine handle.
- uTP streams own their connection-driver task. Stream cancellation aborts the
  driver so its contained UDP socket cannot remain active under an obsolete
  policy.
- The daemon installs the new configuration, re-evaluates containment, and
  reconciles eligible torrent tasks using newly constructed binders.
- Peer-limit changes use the same transaction. The transition lock remains
  held through provisional pool/config installation and policy-eligible engine
  and seeder reconstruction, so an unrelated start cannot enter the partially
  rebuilt set. Candidate pools are never resized in place (ADR-0053).
- Failed provisional reconstruction or post-reconstruction persistence restores the
  exact prior pool identities, configuration bytes, torrent lifecycle/recovery
  intent, durable state, and formerly owned task set. Irreversible completion
  policy is activated only after persistent commit.
- Control-plane bind/body-limit and logging destination changes remain
  explicitly restart-required because the running server or logging
  subscriber owns those resources.
- Global storage-root changes are rejected while a torrent still depends on
  the old root; operators must first move those payloads to explicit locations.

No old data-plane task is allowed to survive the configuration boundary.

## Consequences

- Network, listen-port, uTP, encryption, and DHT settings take effect without
  a process restart while preserving fail-closed behavior.
- A full replacement can interrupt active transfers while tasks are rebuilt;
  verified progress is retained and resumed.
- New data-plane subsystems must participate in the same stop-and-rebuild
  transaction rather than caching configuration independently.
- Configuration replacement and runtime task ownership are coupled and must be
  exercised together in integration tests.

## Related Documents

- `../configuration.md`
- `../vpn-network-containment.md`
- `../../docs/configuration.md`
- `../../crates/swarmotterd/src/daemon.rs`
- ADR-0012 (centralized network binder)
- ADR-0025 (runtime diagnostics and atomic config replacement)
- ADR-0046 (shared inbound peer listener)
- [ADR-0053: Process-Wide Peer Session Permit Pool](0053-process-wide-peer-session-permit-pool.md)
