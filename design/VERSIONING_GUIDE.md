# SwarmOtter Versioning Guide

This guide defines how SwarmOtter version jumps work and which files must be
updated when the project version changes.

## 1. Versioning policy

SwarmOtter uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html):

- **Major (`X.0.0`)** for breaking changes to public APIs, compatibility
  adapters, configuration, on-disk state, container contracts, or other
  operator-facing compatibility boundaries.
- **Minor (`X.Y.0`)** for backwards-compatible feature additions.
- **Patch (`X.Y.Z`)** for backwards-compatible fixes, packaging adjustments,
  CI fixes, and documentation updates that do not change the public contract.

Compatibility boundaries include:

- Native REST API routes under `/api/vN`, the `{ success, data, error }`
  envelope, stable error codes, SSE/WebSocket event shapes, and the
  `/api/v1/version` response.
- Optional compatibility surfaces such as `/transmission/rpc`.
- Configuration file tables and fields, `SWARMOTTER_` environment overrides,
  CLI flags, and documented defaults.
- Persistent daemon state, fast-resume metadata, storage layout expectations,
  and safe handling of existing downloaded data.
- Docker image entrypoint, exposed ports, volumes, environment variables,
  healthcheck behavior, and published GHCR image names/tags.
- Network-containment and fail-closed behavior promised to operators.

### Choosing the bump when a branch has mixed changes

Pick the **highest-impact** change class in the branch:

1. Any breaking compatibility change => **Major**
2. Otherwise, any new user-visible capability => **Minor**
3. Otherwise (fixes/tooling/docs only) => **Patch**

Examples:

- Native API field addition + bug fix in one branch => **Minor** (not Patch)
- Docker packaging fix + CI fix + docs only => **Patch**
- Removing or changing a stable `/api/v1` field => **Major** unless a new API
  namespace is added and `/api/v1` remains compatible

### Public release line

The first public SwarmOtter release line begins at `v1.0.0`. SwarmOtter does
not use an MVP release model; see ADR-0003.

## 2. Source of truth

The root `Cargo.toml` `[workspace.package].version` value is the canonical
SwarmOtter release version.

SwarmOtter does not currently have a root `VERSION` file or a version bump
script. When the release version changes, update the release-facing files
below and let Cargo refresh local workspace package versions in `Cargo.lock`.

### Core Rust workspace

- `Cargo.toml`
  Update `[workspace.package].version`. The workspace crates inherit this
  version.
- `Cargo.lock`
  Refresh the local path package versions for `swarmotter-core`,
  `swarmotter-api`, `swarmotter-web`, and `swarmotterd`.

### Runtime version metadata

The daemon reports the Cargo package version through `/api/v1/version` and
Transmission-compatible session responses. Do not hard-code those versions in
Rust source; they should continue to come from Cargo package metadata.

The build commit is separate from the release version. CI/container builds pass
`SWARMOTTER_BUILD_COMMIT` so `/api/v1/version` can report the Git revision.

### Container and GHCR metadata

- `deploy/Dockerfile`
  `ARG SWARMOTTER_VERSION` defaults to the current workspace release version for
  local builds. Release workflows pass the tag-derived version explicitly.
- `.github/workflows/ci.yml`
  Main-branch image tags are `main` and `sha-<shortsha>`.
- `.github/workflows/release.yml`
  Stable `vX.Y.Z` tags publish Linux tarballs, `.deb`/`.rpm` packages,
  `SHA256SUMS`, and GHCR image tags `vX.Y.Z`, `X.Y.Z`, `X.Y`, `X`, `latest`,
  and `sha-<shortsha>`.
- `deploy/compose.yml` and `deploy/.env.example`
  These usually do not need a version bump because the default image tracks
  `latest`. Update them only if a release requires a new image name, port,
  volume, or environment contract.

### Documentation

- `CHANGELOG.md`
  Add or update release notes under `Unreleased` or the new version heading,
  depending on the release process being used.
- `README.md`, `docs/**`, and `design/**`
  Update only user-facing version references, release-line statements, API
  namespace references, or deployment instructions that changed as part of the
  release.
- `design/v1-completion-tracker.md`
  Keep this as historical/current `v1.0.0` completion tracking unless the
  document is intentionally retired or replaced for a later release line.

## 3. Files that usually do **not** need a version bump

Do **not** update unrelated version-like numbers just to match the SwarmOtter
release.

Examples:

- Dependency versions in `Cargo.toml` or `Cargo.lock`.
- Protocol version constants such as uTP version `1`.
- API namespace strings such as `/api/v1` when the native v1 API remains
  compatible.
- Transmission RPC compatibility fields that represent the Transmission RPC
  protocol version, not the SwarmOtter release version.
- Documentation examples that mention older releases for historical context.

## 4. Recommended version-bump procedure

1. Decide the next version according to SemVer using the highest-impact rule
   above.
2. Update `Cargo.toml` `[workspace.package].version`.
3. Refresh lockfile metadata:

   ```bash
   cargo metadata --format-version 1 >/dev/null
   ```

4. Check whether `deploy/Dockerfile` `SWARMOTTER_VERSION` default should change
   for local image metadata.
5. Update `CHANGELOG.md`.
6. Re-scan the repository for stale release-version strings.
7. Validate that package metadata, tests, Docker build metadata, and docs still
   line up.
8. Create the release tag when the project is ready to publish.

## 5. Validation checklist

After a version bump, verify:

- `Cargo.toml` has the intended workspace version.
- `Cargo.lock` local SwarmOtter packages reflect the same version.
- `/api/v1/version` reports the intended version in a built daemon.
- `CHANGELOG.md` explains the release and any important versioning context.
- Docker image labels report the intended version.
- No stale old-version references remain in release-facing files.
- CI and GHCR tag rules still match the current tag format.

Useful commands:

```bash
cargo metadata --no-deps --format-version 1 >/dev/null
cargo test --all --all-features
docker build -f deploy/Dockerfile -t swarmotter:version-check .
docker image inspect swarmotter:version-check \
  --format '{{ index .Config.Labels "org.opencontainers.image.version" }}'

rg 'OLD_VERSION|vOLD_VERSION' \
  Cargo.toml \
  Cargo.lock \
  CHANGELOG.md \
  README.md \
  docs \
  design \
  deploy \
  .github/workflows
```

Replace `OLD_VERSION` with the version you are replacing.

## 6. Release tag rules

When publishing a stable release, use Git tags with a leading `v`:

- Stable release: `v1.0.0`

Stable release tags publish native Linux artifacts and GHCR tags with both the
leading-`v` tag and SemVer tags without the leading `v`.
