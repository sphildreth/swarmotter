#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/bump_version.sh vX.Y.Z

Updates SwarmOtter release-version points:
  - Cargo.toml [workspace.package].version
  - Cargo.lock local workspace package versions via cargo metadata
  - deploy/Dockerfile SWARMOTTER_VERSION default
  - CHANGELOG.md top Unreleased heading, when present

The argument may include or omit the leading "v".
EOF
}

die() {
    printf 'bump_version: ERROR: %s\n' "$*" >&2
    exit 1
}

if [[ "${1-}" == "-h" || "${1-}" == "--help" ]]; then
    usage
    exit 0
fi

if (($# != 1)); then
    usage >&2
    exit 2
fi

input="$1"
version="${input#v}"
tag="v$version"

[[ "$version" =~ ^[0-9]+[.][0-9]+[.][0-9]+$ ]] \
    || die "version must be SemVer X.Y.Z, optionally prefixed with v"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$repo_root"

[[ -f Cargo.toml ]] || die "Cargo.toml not found at repository root"
[[ -f deploy/Dockerfile ]] || die "deploy/Dockerfile not found"
[[ -f CHANGELOG.md ]] || die "CHANGELOG.md not found"

current_version="$(
    sed -n '/^\[workspace.package\]/,/^\[/s/^version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml \
        | head -n 1
)"
[[ -n "$current_version" ]] || die "could not read Cargo.toml workspace package version"

tmp="$(mktemp)"
awk -v version="$version" '
    BEGIN { in_workspace_package = 0; updated = 0 }
    /^\[workspace.package\]/ {
        in_workspace_package = 1
        print
        next
    }
    /^\[/ {
        in_workspace_package = 0
    }
    in_workspace_package && /^version[[:space:]]*=/ {
        print "version = \"" version "\""
        updated = 1
        next
    }
    { print }
    END {
        if (!updated) {
            exit 1
        }
    }
' Cargo.toml > "$tmp" || {
    rm -f "$tmp"
    die "failed to update Cargo.toml"
}
chmod --reference=Cargo.toml "$tmp"
mv "$tmp" Cargo.toml

tmp="$(mktemp)"
awk -v version="$version" '
    /^ARG SWARMOTTER_VERSION=/ {
        print "ARG SWARMOTTER_VERSION=" version
        updated = 1
        next
    }
    { print }
    END {
        if (!updated) {
            exit 1
        }
    }
' deploy/Dockerfile > "$tmp" || {
    rm -f "$tmp"
    die "failed to update deploy/Dockerfile"
}
chmod --reference=deploy/Dockerfile "$tmp"
mv "$tmp" deploy/Dockerfile

if grep -q '^## \[Unreleased\]' CHANGELOG.md; then
    today="$(date -u +%Y-%m-%d)"
    tmp="$(mktemp)"
    awk -v version="$version" -v today="$today" '
        !updated && /^## \[Unreleased\]/ {
            print "## [" version "] - [" today "]"
            updated = 1
            next
        }
        { print }
    ' CHANGELOG.md > "$tmp" || {
        rm -f "$tmp"
        die "failed to update CHANGELOG.md"
    }
    chmod --reference=CHANGELOG.md "$tmp"
    mv "$tmp" CHANGELOG.md
elif ! grep -q "^## \\[$version\\]" CHANGELOG.md; then
    printf 'bump_version: WARNING: CHANGELOG.md has no Unreleased or %s heading; leaving changelog headings unchanged\n' "$version" >&2
fi

cargo metadata --format-version 1 >/dev/null

printf 'bump_version: %s -> %s (%s)\n' "$current_version" "$version" "$tag"
