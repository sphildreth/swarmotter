#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
set -eu
if [ -n "${VPN_NAMESPACE_FILE:-}" ]; then
    # Gluetun publishes its namespace atomically after its own probe passes.
    # The read-only volume carries no VPN settings or credentials.
    expected=$(cat "$VPN_NAMESPACE_FILE")
    current=$(readlink /proc/self/ns/net)
    case "$expected" in net:\[*\]) ;; *) exit 1 ;; esac
    if [ "$current" != "$expected" ]; then
        echo "VPN network namespace changed; restarting SwarmOtter" >&2
        kill -TERM 1
        exit 1
    fi
fi
exec curl --noproxy '*' --max-time 4 -fsS http://127.0.0.1:9091/live
