#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

node crates/swarmotter-web/tests/app-startup.test.mjs
node crates/swarmotter-web/tests/peer-filter.test.mjs
node crates/swarmotter-web/tests/policy-source-labels.test.mjs
node crates/swarmotter-web/tests/port-mapping.test.mjs
node crates/swarmotter-web/tests/port-test.test.mjs
