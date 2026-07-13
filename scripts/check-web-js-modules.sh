#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

while IFS= read -r -d '' javascript; do
    if ! node --input-type=module --check < "$javascript"; then
        echo "error: JavaScript module syntax check failed: $javascript" >&2
        exit 1
    fi
done < <(find crates/swarmotter-web/assets -type f -name '*.js' -print0 | sort -z)
