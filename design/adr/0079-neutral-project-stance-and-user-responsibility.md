# ADR-0079: Neutral Project Stance and User Responsibility

## Status

Accepted

Supersedes [ADR-0008](0008-lawful-use-and-no-piracy-oriented-features.md).

## Context

SwarmOtter is a general-purpose BitTorrent client intended for legal torrenting.
The previous project policy repeated content restrictions and prescribed use
cases throughout documentation, contributor rules, issue templates, and release
acceptance criteria. It also permanently excluded some feature proposals based
on presumed use. These gates distract from documenting and reviewing the
software's capabilities and imply a role in policing users that the project
does not take on.

## Decision

Users choose the content they download and share and are responsible for their
use of SwarmOtter. The application does not police those choices.
Documentation states this briefly and describes capabilities in factual terms.

Remove project-specific content restrictions, prescribed wording, and
lawful-use review gates from contributor instructions, issue/PR templates,
requirements, and planning documents. Evaluate proposals for functionality,
correctness, security, maintenance, and license compatibility. Removing a gate
does not implement or commit to any additional feature.

Keep Apache-2.0 license terms, dependency license review, supply-chain auditing,
and technical safeguards, including fail-closed network containment. Prefer
generated local test fixtures for reproducibility and independence from external
services. This stance does not grant rights to third-party content or establish
a legal guarantee about contributor or user liability.

## Consequences

- User documentation has a concise use and license notice rather than lists
  of approved uses or prohibited content.
- Contributors and issue reporters do not need to pass a content-policy review.
- Unimplemented discovery features are documented as absent rather than
  permanently prohibited by policy; future proposals still need normal design
  and acceptance criteria.
- Network containment, authentication, storage safety, and existing features
  retain their technical requirements and tests.
- ADR-0008 remains a historical record with a superseded status. Its former
  policy pages and dedicated content-policy reporting template are removed.
- Release packaging and documentation navigation use the consolidated notice.

## Related Documents

- [ADR-0008](0008-lawful-use-and-no-piracy-oriented-features.md)
- [ADR-0007](0007-apache-2-license.md)
- [Contributor guidance](../../AGENTS.md)
- [Contribution workflow](../../CONTRIBUTING.md)
- [Project stance and license](../legal.md)
- [Use and license notice](../../docs/legal.md)
- [Requirements](../requirements.md)
- [Testing](../testing.md)
