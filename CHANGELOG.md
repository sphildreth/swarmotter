# Changelog

This file records notable project changes. It follows the
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format and uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

All notable changes are recorded by capability and acceptance criteria, not by
date or duration estimates. SwarmOtter's first release is `v1.0.0`; there is no
MVP release.

## [Unreleased]

### Added

- Repository scaffolding: governance documentation, ADR process, legal design
  docs, GitHub templates, and a minimal Rust workspace skeleton.
- ADRs 0001 through 0008 recording foundational project decisions.
- Lawful-use, content-policy, and legal posture documentation.
- VPN/NIC network containment design describing fail-closed behavior.
- Documentation stubs for requirements, architecture, API, configuration,
  deployment, and testing.

### Changed

- Restructured the repository from a single broken crate into a Rust
  workspace under `crates/` (`swarmotterd`, `swarmotter-core`,
  `swarmotter-api`, `swarmotter-web`).

### Notes

- The BitTorrent engine is intentionally not implemented yet. The repository
  is in the early setup phase preceding `v1.0.0` development.