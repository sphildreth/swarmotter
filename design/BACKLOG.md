# Feature backlog

The SwarmOtter product backlog is maintained in Hamstik, organization `sph`,
project `SWO`. Each work item holds its scope, acceptance criteria, design
references and current status. [Open Hamstik](https://hamstik.com)

Use the Hamstik CLI to list the backlog or read an individual item.

```sh
hamstik --org sph --project SWO work list --status backlog --all
hamstik --org sph --project SWO work view SWO-1
```

The migration covered 44 ranked candidates. Each item's description records
its original rank, such as `P1.01`, and links to its original proposal. In
the original ordering, P1 covers high-value product work, P2 covers
specialized or follow-on work, and P3 covers research decisions. The three
conditional P1 items document their product or authorization prerequisites.
[Archived Feature Map and detailed proposals](https://github.com/sphildreth/swarmotter/blob/0897ca30e99ab3faa0338457e6b294876a5d0baf/design/BACKLOG.md#feature-map)

The RSS subscription story is `SWO-1`. The migration covers the entries in
the Feature Map. Proposals for built-in search, bundled indexers or discovery
plugins follow the ordinary project review process.
[Project stance](adr/0079-neutral-project-stance-and-user-responsibility.md)
