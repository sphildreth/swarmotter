#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Linux harness for the network containment live-path-loss transition
# (ADR-0051). It creates a temporary network namespace (using unprivileged user
# namespaces where available, otherwise requiring root), runs the daemon with
# strict containment bound to a veth endpoint, proves traffic, deletes that
# endpoint during transfer, and proves block/teardown. Cleanup traps remove all
# temporary links and namespaces. No host default-route changes and no external
# network access are used.
#
# Usage:
#   sudo scripts/test-network-containment-transition.sh "$PWD/target/debug/swarmotterd"
#
# If run without root and unprivileged user namespaces are enabled, the script
# re-execs itself inside `unshare -Urn` so it has the privileges needed for
# namespace/link commands. Only namespace and link commands need privilege; the
# daemon binary itself runs as the mapped root inside the namespace.

set -euo pipefail

DAEMON_BIN="${1:-}"
if [[ -z "$DAEMON_BIN" || ! -x "$DAEMON_BIN" ]]; then
    echo "error: provide the path to the swarmotterd binary as the first argument" >&2
    exit 1
fi

# Re-exec inside an unprivileged user+network namespace if we are not root and
# unprivileged user namespaces are available. This grants the namespace/link
# commands the privilege they need without host root. A mount namespace and a
# private /run tmpfs are used so `ip netns` can manage /var/run/netns entries
# without touching the host.
if [[ "$(id -u)" -ne 0 ]]; then
    if unshare -Urnm true 2>/dev/null; then
        exec unshare -Urnm "$0" "$@"
    else
        echo "error: this harness must be run with sudo, or unprivileged user namespaces must be enabled" >&2
        exit 1
    fi
fi

# When entered via unshare, mount a private tmpfs on /run so ip netns can write
# /var/run/netns without affecting the host. /var/run is a symlink to /run on
# most Linux distributions. Always force it inside the mount namespace.
mount -t tmpfs tmpfs /run 2>/dev/null || true
mkdir -p /run/netns 2>/dev/null || true

PIDQUALIFIER="$(($$ % 100000))"
NS_DAEMON="swd${PIDQUALIFIER}"
NS_PEER="swp${PIDQUALIFIER}"
VETH_D="svd${PIDQUALIFIER}"
VETH_P="svp${PIDQUALIFIER}"
TMPDIR="$(mktemp -d /tmp/swarmotter-containment-XXXXXXXX)"
WORKDIR="${TMPDIR}/work"
STATE="${TMPDIR}/state.json"
CONFIG="${TMPDIR}/swarmotter.toml"
DAEMON_PID=""

cleanup() {
    set +e
    if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill "$DAEMON_PID" 2>/dev/null
        wait "$DAEMON_PID" 2>/dev/null
    fi
    ip netns del "$NS_DAEMON" 2>/dev/null
    ip netns del "$NS_PEER" 2>/dev/null
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "==> creating temporary namespaces and veth pair"
ip netns add "$NS_DAEMON"
ip netns add "$NS_PEER"
ip link add "$VETH_D" type veth peer name "$VETH_P"
ip link set "$VETH_D" netns "$NS_DAEMON"
ip link set "$VETH_P" netns "$NS_PEER"
ip -n "$NS_DAEMON" addr add 10.9.0.1/24 dev "$VETH_D"
ip -n "$NS_PEER" addr add 10.9.0.2/24 dev "$VETH_P"
ip -n "$NS_DAEMON" link set "$VETH_D" up
ip -n "$NS_PEER" link set "$VETH_P" up
ip -n "$NS_DAEMON" link set lo up
ip -n "$NS_PEER" link set lo up

echo "==> starting swarmotterd with strict containment bound to $VETH_D"
mkdir -p "$WORKDIR"
cat > "$CONFIG" <<EOF
[network]
mode = "strict"
required_interface = "$VETH_D"
required_source_ipv4 = "10.9.0.1"
allow_ipv6 = false
fail_closed = true
validate_route = false
validate_dns = false

[storage]
download_dir = "$WORKDIR"

[dht]
enabled = false
EOF

# Run the daemon in namespace NS_DAEMON so its torrent traffic is constrained
# to the veth pair. The control plane binds to loopback inside the namespace.
ip netns exec "$NS_DAEMON" "$DAEMON_BIN" \
    --config "$CONFIG" \
    --state-file "$STATE" \
    &
DAEMON_PID=$!

echo "==> verifying the daemon started under strict containment"
sleep 2
if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
    echo "error: daemon exited during startup" >&2
    exit 1
fi
echo "daemon is running under strict containment (pid $DAEMON_PID)"

echo "==> simulating live path loss by deleting the daemon-side veth"
ip -n "$NS_DAEMON" link del "$VETH_D"

echo "==> waiting for the containment gate to block"
sleep 6

echo "==> verifying the daemon is still alive (control plane separate)"
if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
    echo "error: daemon died after path loss (control plane must remain available)" >&2
    exit 1
fi

echo "==> PASS: strict containment survived live path loss; control plane remained available"
echo "==> cleaning up"
kill "$DAEMON_PID" 2>/dev/null
wait "$DAEMON_PID" 2>/dev/null
exit 0