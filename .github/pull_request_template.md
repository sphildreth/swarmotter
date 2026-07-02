## Summary

<!-- Brief description of the change and why it is needed. -->

## Checklist

Before requesting review, confirm the following:

- [ ] `cargo fmt`, `cargo check`, and `cargo test` pass.
- [ ] An ADR was created or updated if this change introduces, removes, or
  materially alters an architectural decision. **Was an ADR required?**
- [ ] `design/` documentation was updated where the corresponding surface
  changed. **Were design docs updated?**
- [ ] Tests were added or updated alongside the change. **Were tests added or
  updated?**
- [ ] Network containment behavior considered. **Does this affect network
  containment?** If yes, torrent traffic must remain constrained and
  fail-closed behavior must not be weakened.
- [ ] Legal/lawful-use posture considered. **Does this affect legal/lawful-use
  posture?** No piracy-oriented features, indexers, or infringing examples.
- [ ] New dependencies reviewed. **Does this introduce new dependencies?** If
  yes, they were reviewed for license compatibility (Apache-2.0), maintenance
  quality, security posture, complexity impact, and network containment
  effects.
- [ ] Licenses reviewed. **Have licenses been reviewed?** Significant
  additions recorded in `THIRD_PARTY_LICENSES.md`.
- [ ] No time/duration estimates added to docs, commits, or this PR.
- [ ] `CHANGELOG.md` updated for notable changes.

## Notes

<!-- Any additional context, breaking changes, or migration notes. -->