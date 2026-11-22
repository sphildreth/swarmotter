# ADR-0080: Library and container recovery

## Status

Accepted

## Context

A VPN container restart can replace its network namespace while the daemon
continues serving loopback in the old namespace. A library rehash can also
delay the control plane during ordinary recovery. Copying a changing SQLite
database and its journal across separate reads can produce an inconsistent
upgrade backup.

## Decision

Bind the control-plane listener before restoration. Expose liveness and
aggregate verification progress, gate library operations until restoration
commits, and run up to two restore checks within storage root limits. The
process watchdog monitors restore progress before it switches to worker heartbeats.
Use each torrent's own piece address space for verification and upload.

After a successful Gluetun health probe, atomically publish its namespace ID
on a shared volume mounted read-only by SwarmOtter. On a mismatch the daemon
container's health check signals PID 1 to shut down. Docker's existing restart
policy reattaches the daemon. Preserve the shared namespace as the containment
boundary. Before replacing an image, stop the state writer and verify the
archived database, including its sidecars, in a temporary restore directory.

Pair the previous image with the quiescent library and RSS history generation
taken before replacement. A late validation failure can occur after migration
or new writes. Stop the stack, retain the failed generation separately, stage
both verified original databases and their sidecars on their destination
filesystems, and health-check the previous image after restoring them. Leave
the stack stopped if the paired restore fails. Resolve external RSS history
using the same environment-over-TOML precedence as the daemon.

Parallel rounds keep a sequence of completed pieces for their lifetime. Each
session advertises from its own cursor. The coordinator owns checkpoints and
handles pending commands while workers run. Cached library summaries are
invalidated by record mutations. SQLite retention counters and timestamp
indexes are derived data. Transactional triggers account for inserts and
deletes by current and older schema-v2 writers. Full integrity verification
continues at load, migration and offline maintenance boundaries.

Use rusqlite 0.41 with its patched bundled SQLite. Its compiler requirement
raises the source-build minimum to Rust 1.89. CI checks the locked dependency
graph on that compiler.

## Consequences

Verified payload remains a prerequisite for seeding. Operators can distinguish
startup verification from a stopped daemon. A changed VPN namespace interrupts
active transfers until the daemon has rejoined it. Backup verification requires
host Python with SQLite and temporary disk space for the state directory.
Per-round memory for notifications scales with the number of pieces newly
verified in that round. The first incremental save initializes retention counts
and indexes on an existing database.

## Related Documents

- [SQLite library state](0067-sqlite-durable-library-state.md)
- [Changed-record persistence](0073-proportional-changed-record-persistence.md)
- [Resume checkpoints](0074-coalesced-resume-checkpoints.md)
- [Downloader serving](0075-downloader-inbound-serving.md)
- [Process supervision](0078-supervised-shutdown-and-bounded-logging.md)
- [Compose deployment](../../docs/deployment.md)
