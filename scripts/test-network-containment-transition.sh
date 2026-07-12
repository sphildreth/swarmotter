#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Linux acceptance harness for ADR-0051. The script itself, generated fixtures,
# HTTP server, API clients, and daemon run as the invoking non-root user. Each
# namespace/link operation is an explicit `sudo ip` call. `ip netns exec`
# immediately drops back to the caller identity; only swarmotterd retains the
# single CAP_NET_RAW capability required by Linux SO_BINDTODEVICE.
#
# The fixture is entirely local and generated at runtime:
#
#   daemon namespace (10.203.0.1) <-- veth --> peer namespace (10.203.0.2)
#
# Neither namespace has a default route. A capability-free fixture in the peer
# namespace hosts an HTTP compact-peer tracker and a throttled TCP BitTorrent
# seed for generated lawful bytes. SwarmOtter runs in strict mode bound to the
# daemon-side veth. The harness registers the raw generated torrent through the
# real API, observes partial verified peer-wire progress, deletes the daemon
# veth, and proves interface_missing, network_blocked, data-plane teardown,
# stable progress, and continued control-plane service.
#
# Usage (build and invoke without sudo; the script scopes sudo internally):
#   cargo build --locked -p swarmotterd
#   scripts/test-network-containment-transition.sh \
#     "$PWD/target/debug/swarmotterd"

set -euo pipefail
umask 077

readonly DAEMON_BIN_INPUT="${1:-}"
if [[ -z "$DAEMON_BIN_INPUT" || ! -x "$DAEMON_BIN_INPUT" ]]; then
    echo "error: provide an executable swarmotterd binary as the first argument" >&2
    exit 2
fi
if [[ "$(id -u)" -eq 0 ]]; then
    echo "error: run this harness as the normal build user; it scopes sudo to namespace/link operations" >&2
    exit 2
fi

for command in ip curl python3 mktemp readlink setpriv sudo; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command is unavailable: $command" >&2
        exit 2
    fi
done

# Preflight the exact command family used below. This works with a
# command-scoped NOPASSWD rule for `ip`; a broad `sudo -v` would incorrectly
# fail such least-privilege CI configuration.
if ! sudo -- ip netns list >/dev/null; then
    echo "error: sudo permission is required for ip namespace/link commands" >&2
    exit 2
fi

RUN_UID="$(id -u)"
readonly RUN_UID
RUN_GID="$(id -g)"
readonly RUN_GID

DAEMON_BIN="$(readlink -f "$DAEMON_BIN_INPUT")"
readonly DAEMON_BIN
QUALIFIER="$(printf '%05d' "$((BASHPID % 100000))")"
readonly QUALIFIER
readonly NS_DAEMON="swarmotter-daemon-${QUALIFIER}"
readonly NS_PEER="swarmotter-peer-${QUALIFIER}"
readonly VETH_DAEMON="sod${QUALIFIER}"
readonly VETH_PEER="sop${QUALIFIER}"
readonly DAEMON_ADDRESS="10.203.0.1"
readonly PEER_ADDRESS="10.203.0.2"
readonly API_PORT=19091
readonly TRACKER_PORT=18080
readonly PEER_PORT=51414
readonly API_BASE="http://127.0.0.1:${API_PORT}"

FIXTURE_ROOT="$(mktemp -d "/tmp/swarmotter-containment-${QUALIFIER}-XXXXXXXX")"
readonly FIXTURE_ROOT
readonly DOWNLOAD_DIR="${FIXTURE_ROOT}/downloads"
readonly INCOMPLETE_DIR="${FIXTURE_ROOT}/incomplete"
readonly STATE_FILE="${FIXTURE_ROOT}/state.json"
readonly CONFIG_FILE="${FIXTURE_ROOT}/swarmotter.toml"
readonly PAYLOAD_FILE="${FIXTURE_ROOT}/containment-payload.bin"
readonly TORRENT_FILE="${FIXTURE_ROOT}/containment-payload.torrent"
readonly EXPECTED_HASH_FILE="${FIXTURE_ROOT}/expected-info-hash"
readonly DAEMON_LOG="${FIXTURE_ROOT}/daemon.log"
readonly SWARM_LOG="${FIXTURE_ROOT}/local-swarm.log"

DAEMON_PID=""
DAEMON_WRAPPER_PID=""
SWARM_PID=""
SWARM_WRAPPER_PID=""

sudo_ip() {
    sudo -- ip "$@"
}

netns_exec_unprivileged() {
    local namespace="$1"
    shift
    sudo_ip netns exec "$namespace" setpriv \
        --reuid="$RUN_UID" --regid="$RUN_GID" --clear-groups \
        --inh-caps=-all --ambient-caps=-all --bounding-set=-all \
        --no-new-privs "$@"
}

netns_exec_daemon() {
    local namespace="$1"
    shift
    sudo_ip netns exec "$namespace" setpriv \
        --reuid="$RUN_UID" --regid="$RUN_GID" --clear-groups \
        --inh-caps=-all,+net_raw --ambient-caps=-all,+net_raw \
        --bounding-set=-all,+net_raw --no-new-privs "$@"
}

namespace_process_pid() {
    local namespace="$1"
    local needle="$2"
    local pid=""
    for pid in $(sudo_ip netns pids "$namespace" 2>/dev/null); do
        if [[ "$(stat -c '%u' "/proc/${pid}" 2>/dev/null || true)" == "$RUN_UID" ]] && \
           tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null | grep -Fq -- "$needle"; then
            printf '%s\n' "$pid"
            return 0
        fi
    done
    return 1
}

terminate_pid() {
    local pid="${1:-}"
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    wait "$pid" 2>/dev/null || true
}

cleanup() {
    local status=$?
    set +e
    terminate_pid "$DAEMON_PID"
    terminate_pid "$SWARM_PID"
    if sudo_ip netns list 2>/dev/null | awk '{print $1}' | grep -Fxq "$NS_DAEMON"; then
        for pid in $(sudo_ip netns pids "$NS_DAEMON" 2>/dev/null); do
            if [[ "$(stat -c '%u' "/proc/${pid}" 2>/dev/null || true)" == "$RUN_UID" ]]; then
                kill -KILL "$pid" 2>/dev/null || true
            fi
        done
        sudo_ip netns del "$NS_DAEMON" 2>/dev/null
    fi
    if sudo_ip netns list 2>/dev/null | awk '{print $1}' | grep -Fxq "$NS_PEER"; then
        for pid in $(sudo_ip netns pids "$NS_PEER" 2>/dev/null); do
            if [[ "$(stat -c '%u' "/proc/${pid}" 2>/dev/null || true)" == "$RUN_UID" ]]; then
                kill -KILL "$pid" 2>/dev/null || true
            fi
        done
        sudo_ip netns del "$NS_PEER" 2>/dev/null
    fi
    wait "$DAEMON_WRAPPER_PID" 2>/dev/null || true
    wait "$SWARM_WRAPPER_PID" 2>/dev/null || true
    rm -rf "$FIXTURE_ROOT"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
    echo "error: $*" >&2
    if [[ -s "$DAEMON_LOG" ]]; then
        echo "--- swarmotterd log (tail) ---" >&2
        tail -n 120 "$DAEMON_LOG" >&2 || true
    fi
    if [[ -s "$SWARM_LOG" ]]; then
        echo "--- local tracker/seed log (tail) ---" >&2
        tail -n 80 "$SWARM_LOG" >&2 || true
    fi
    exit 1
}

namespace_exists() {
    sudo_ip netns list | awk '{print $1}' | grep -Fxq "$1"
}

if namespace_exists "$NS_DAEMON" || namespace_exists "$NS_PEER"; then
    fail "PID-qualified namespace name already exists"
fi

mkdir -p "$DOWNLOAD_DIR" "$INCOMPLETE_DIR"

echo "==> generating local payload and tracker-backed torrent"
python3 - "$PAYLOAD_FILE" "$TORRENT_FILE" "$EXPECTED_HASH_FILE" \
    "http://${PEER_ADDRESS}:${TRACKER_PORT}/announce" <<'PY'
import hashlib
import pathlib
import sys

payload_path = pathlib.Path(sys.argv[1])
torrent_path = pathlib.Path(sys.argv[2])
hash_path = pathlib.Path(sys.argv[3])
announce = sys.argv[4].encode("ascii")

piece_length = 256 * 1024
piece_count = 256
seed = b"SwarmOtter lawful local containment acceptance fixture\n"
piece = (seed * ((piece_length // len(seed)) + 1))[:piece_length]
pieces = []
with payload_path.open("wb") as payload:
    for _ in range(piece_count):
        payload.write(piece)
        pieces.append(hashlib.sha1(piece).digest())

def bstr(value: bytes) -> bytes:
    return str(len(value)).encode("ascii") + b":" + value

def bint(value: int) -> bytes:
    return b"i" + str(value).encode("ascii") + b"e"

name = b"containment-payload.bin"
info = (
    b"d"
    + bstr(b"length") + bint(piece_length * piece_count)
    + bstr(b"name") + bstr(name)
    + bstr(b"piece length") + bint(piece_length)
    + bstr(b"pieces") + bstr(b"".join(pieces))
    + b"e"
)
torrent = b"d" + bstr(b"announce") + bstr(announce) + bstr(b"info") + info + b"e"
torrent_path.write_bytes(torrent)
hash_path.write_text(hashlib.sha1(info).hexdigest() + "\n", encoding="ascii")
PY

cat > "$CONFIG_FILE" <<EOF
[api]
bind_address = "127.0.0.1:${API_PORT}"
require_auth = false
max_request_body_bytes = 16777216

[storage]
download_dir = "${DOWNLOAD_DIR}"
incomplete_dir = "${INCOMPLETE_DIR}"
minimum_free_space_bytes = 0
minimum_free_space_percent = 0
preallocate = false
sparse = true

[network]
mode = "strict"
required_interface = "${VETH_DAEMON}"
required_source_ipv4 = "${DAEMON_ADDRESS}"
allow_ipv6 = false
fail_closed = true
validate_route = false
validate_dns = false

[torrent]
listen_port = 51413
allow_ipv6 = false
utp_enabled = false
utp_prefer_tcp = true
encryption_mode = "disabled"
selfish = false

[bandwidth]
global_download = 0
global_upload = 0
alt_download = 0
alt_upload = 0
alt_enabled = false
max_peers = 1
max_peers_per_torrent = 1

[queue]
max_active_downloads = 1
max_active_metadata_fetches = 1
max_active_seeds = 0
auto_start = true

[autopilot]
mode = "disabled"

[dht]
enabled = false
bootstrap_nodes = []
port = 51413

[pex]
enabled = false
max_peers = 0

[logging]
level = "info"
json = false
file = false
EOF

echo "==> creating PID-qualified namespaces and route-less veth path"
sudo_ip netns add "$NS_DAEMON"
sudo_ip netns add "$NS_PEER"
sudo_ip link add "$VETH_DAEMON" type veth peer name "$VETH_PEER"
sudo_ip link set "$VETH_DAEMON" netns "$NS_DAEMON"
sudo_ip link set "$VETH_PEER" netns "$NS_PEER"
sudo_ip -n "$NS_DAEMON" address add "${DAEMON_ADDRESS}/24" dev "$VETH_DAEMON"
sudo_ip -n "$NS_PEER" address add "${PEER_ADDRESS}/24" dev "$VETH_PEER"
sudo_ip -n "$NS_DAEMON" link set lo up
sudo_ip -n "$NS_PEER" link set lo up
sudo_ip -n "$NS_DAEMON" link set "$VETH_DAEMON" up
sudo_ip -n "$NS_PEER" link set "$VETH_PEER" up

if [[ -n "$(sudo_ip -n "$NS_DAEMON" route show default)" ]] || \
   [[ -n "$(sudo_ip -n "$NS_PEER" route show default)" ]]; then
    fail "a fixture namespace unexpectedly has a default route"
fi

# Assert the exact runtime privilege boundary before any fixture traffic. The
# daemon identity is the caller UID with CAP_NET_RAW (bit 13) and no other
# effective capability; ordinary fixture processes have no effective caps.
netns_exec_daemon "$NS_DAEMON" python3 - "$RUN_UID" <<'PY'
import os
import pathlib
import sys

expected_uid = int(sys.argv[1])
status = pathlib.Path("/proc/self/status").read_text(encoding="ascii")
cap_eff = int(next(line.split()[1] for line in status.splitlines() if line.startswith("CapEff:")), 16)
if os.geteuid() != expected_uid or cap_eff != (1 << 13):
    raise SystemExit(f"unexpected daemon privilege boundary: euid={os.geteuid()} CapEff={cap_eff:#x}")
PY
netns_exec_unprivileged "$NS_PEER" python3 - "$RUN_UID" <<'PY'
import os
import pathlib
import sys

expected_uid = int(sys.argv[1])
status = pathlib.Path("/proc/self/status").read_text(encoding="ascii")
cap_eff = int(next(line.split()[1] for line in status.splitlines() if line.startswith("CapEff:")), 16)
if os.geteuid() != expected_uid or cap_eff != 0:
    raise SystemExit(f"unexpected fixture privilege boundary: euid={os.geteuid()} CapEff={cap_eff:#x}")
PY

echo "==> starting local HTTP tracker and throttled TCP BitTorrent seed"
netns_exec_unprivileged "$NS_PEER" python3 - \
    "$PAYLOAD_FILE" "$EXPECTED_HASH_FILE" "$PEER_ADDRESS" "$TRACKER_PORT" "$PEER_PORT" \
    >"$SWARM_LOG" 2>&1 <<'PY' &
import http.server
import pathlib
import socketserver
import struct
import sys
import threading
import time
import urllib.parse

payload_path = pathlib.Path(sys.argv[1])
info_hash = bytes.fromhex(pathlib.Path(sys.argv[2]).read_text(encoding="ascii").strip())
bind_address = sys.argv[3]
tracker_port = int(sys.argv[4])
peer_port = int(sys.argv[5])
payload_size = payload_path.stat().st_size
piece_length = 256 * 1024
piece_count = payload_size // piece_length
peer_id = b"-SWHARN-000000000000"

def recv_exact(stream, length):
    data = bytearray()
    while len(data) < length:
        chunk = stream.recv(length - len(data))
        if not chunk:
            raise ConnectionError("peer disconnected")
        data.extend(chunk)
    return bytes(data)

def send_message(stream, message_id, payload=b""):
    stream.sendall(struct.pack("!I", 1 + len(payload)) + bytes([message_id]) + payload)

class TrackerHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if urllib.parse.urlsplit(self.path).path != "/announce":
            self.send_error(404)
            return
        compact_peer = bytes(int(part) for part in bind_address.split(".")) + struct.pack("!H", peer_port)
        body = (
            b"d8:intervali30e8:completei1e10:incompletei1e5:peers"
            + str(len(compact_peer)).encode("ascii") + b":" + compact_peer + b"e"
        )
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "text/plain")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        self.wfile.write(body)

    def log_message(self, message, *args):
        sys.stderr.write("tracker: " + (message % args) + "\n")

class PeerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        stream = self.request
        stream.settimeout(10)
        try:
            handshake = recv_exact(stream, 68)
            if handshake[0] != 19 or handshake[1:20] != b"BitTorrent protocol" or handshake[28:48] != info_hash:
                return
            stream.sendall(
                b"\x13BitTorrent protocol" + b"\x00" * 8 + info_hash + peer_id
            )
            bitfield = bytearray((piece_count + 7) // 8)
            for index in range(piece_count):
                bitfield[index // 8] |= 1 << (7 - (index % 8))
            send_message(stream, 5, bytes(bitfield))

            with payload_path.open("rb") as payload:
                while True:
                    length = struct.unpack("!I", recv_exact(stream, 4))[0]
                    if length == 0:
                        continue
                    message = recv_exact(stream, length)
                    message_id = message[0]
                    if message_id == 2:  # interested
                        send_message(stream, 1)  # unchoke
                    elif message_id == 6 and len(message) == 13:  # request
                        piece, begin, block_length = struct.unpack("!III", message[1:])
                        absolute = piece * piece_length + begin
                        if piece >= piece_count or block_length > 16 * 1024 or absolute + block_length > payload_size:
                            return
                        payload.seek(absolute)
                        block = payload.read(block_length)
                        # One peer worker plus this delay keeps a deterministic
                        # partial-transfer window before the health-loop edge.
                        time.sleep(0.005)
                        send_message(stream, 7, struct.pack("!II", piece, begin) + block)
        except (BrokenPipeError, ConnectionError, ConnectionResetError, OSError, TimeoutError):
            pass

class ThreadedTracker(http.server.ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

class ThreadedPeerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

tracker = ThreadedTracker((bind_address, tracker_port), TrackerHandler)
peer = ThreadedPeerServer((bind_address, peer_port), PeerHandler)
threading.Thread(target=tracker.serve_forever, daemon=True).start()
threading.Thread(target=peer.serve_forever, daemon=True).start()
sys.stderr.write(f"local swarm ready: tracker={bind_address}:{tracker_port} peer={bind_address}:{peer_port}\n")
while True:
    time.sleep(3600)
PY
SWARM_WRAPPER_PID=$!

for _ in $(seq 1 50); do
    if SWARM_PID="$(namespace_process_pid "$NS_PEER" "$PAYLOAD_FILE")"; then
        break
    fi
    ps -p "$SWARM_WRAPPER_PID" >/dev/null 2>&1 || fail "local tracker/seed wrapper exited"
    sleep 0.1
done
[[ -n "$SWARM_PID" ]] || fail "could not identify non-root tracker/seed process"

for _ in $(seq 1 100); do
    if netns_exec_unprivileged "$NS_PEER" curl --silent --show-error --fail \
        --max-time 2 \
        "http://${PEER_ADDRESS}:${TRACKER_PORT}/announce" \
        >/dev/null 2>&1; then
        break
    fi
    kill -0 "$SWARM_PID" 2>/dev/null || fail "local tracker/seed exited during startup"
    sleep 0.1
done
netns_exec_unprivileged "$NS_PEER" curl --silent --show-error --fail \
    --max-time 2 \
    "http://${PEER_ADDRESS}:${TRACKER_PORT}/announce" \
    >/dev/null || fail "local HTTP tracker did not become ready"

echo "==> starting strict SwarmOtter daemon in daemon namespace"
netns_exec_daemon "$NS_DAEMON" "$DAEMON_BIN" \
    --config "$CONFIG_FILE" \
    --state-file "$STATE_FILE" \
    >"$DAEMON_LOG" 2>&1 &
DAEMON_WRAPPER_PID=$!

for _ in $(seq 1 50); do
    if DAEMON_PID="$(namespace_process_pid "$NS_DAEMON" "$DAEMON_BIN")"; then
        break
    fi
    ps -p "$DAEMON_WRAPPER_PID" >/dev/null 2>&1 || fail "swarmotterd wrapper exited"
    sleep 0.1
done
[[ -n "$DAEMON_PID" ]] || fail "could not identify non-root swarmotterd process"

api_get() {
    local path="$1"
    netns_exec_unprivileged "$NS_DAEMON" curl --silent --show-error --fail \
        --connect-timeout 1 --max-time 3 "${API_BASE}${path}"
}

for _ in $(seq 1 150); do
    if api_get "/health" >/dev/null 2>&1; then
        break
    fi
    kill -0 "$DAEMON_PID" 2>/dev/null || fail "swarmotterd exited during startup"
    sleep 0.1
done
api_get "/health" >/dev/null || fail "control API did not become ready"

echo "==> registering generated torrent through raw production API"
ADD_RESPONSE="$(
    netns_exec_unprivileged "$NS_DAEMON" curl --silent --show-error --fail \
        --connect-timeout 1 --max-time 5 \
        -H "Content-Type: application/x-bittorrent" \
        --data-binary "@${TORRENT_FILE}" \
        "${API_BASE}/api/v1/torrents/file"
)" || fail "raw torrent API registration failed"
INFO_HASH="$(printf '%s' "$ADD_RESPONSE" | python3 -c '
import json, sys
document = json.load(sys.stdin)
value = document.get("data")
if not document.get("success") or not isinstance(value, str) or len(value) != 40:
    raise SystemExit(1)
print(value)
')" || fail "raw torrent API returned an unexpected envelope: ${ADD_RESPONSE}"
EXPECTED_HASH="$(tr -d '\n' < "$EXPECTED_HASH_FILE")"
[[ "$INFO_HASH" == "$EXPECTED_HASH" ]] || \
    fail "API info hash ${INFO_HASH} did not match generated torrent ${EXPECTED_HASH}"

poll_json() {
    local path="$1"
    local expression="$2"
    local description="$3"
    local attempts="${4:-200}"
    local response=""
    for _ in $(seq 1 "$attempts"); do
        if response="$(api_get "$path" 2>/dev/null)" && \
           printf '%s' "$response" | python3 -c '
import json, sys
expression = sys.argv[1]
document = json.load(sys.stdin)
ok = bool(eval(expression, {"__builtins__": {}}, {"j": document, "any": any}))
raise SystemExit(0 if ok else 1)
' "$expression"; then
            printf '%s\n' "$response"
            return 0
        fi
        kill -0 "$DAEMON_PID" 2>/dev/null || fail "swarmotterd exited while waiting for ${description}"
        sleep 0.2
    done
    [[ -n "$response" ]] && echo "last API response: $response" >&2
    fail "timed out waiting for ${description}"
}

echo "==> proving verified payload progress over the strict veth path"
PARTIAL_RESPONSE="$(poll_json \
    "/api/v1/torrents/${INFO_HASH}/stats" \
    'j.get("success") is True and 0 < j["data"]["bytes_completed"] < j["data"]["total_length"] and j["data"]["state"] == "downloading" and j["data"]["known_peers"] > 0' \
    "partial tracker-discovered peer transfer" 200)"
PARTIAL_BYTES="$(printf '%s' "$PARTIAL_RESPONSE" | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["data"]["bytes_completed"])')"
TOTAL_BYTES="$(printf '%s' "$PARTIAL_RESPONSE" | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["data"]["total_length"])')"
echo "observed ${PARTIAL_BYTES}/${TOTAL_BYTES} verified bytes before path loss"

echo "==> deleting daemon-side veth during the active transfer"
sudo_ip -n "$NS_DAEMON" link delete "$VETH_DAEMON"

echo "==> proving fail-closed health and torrent state"
poll_json "/api/v1/network/health" \
    'j.get("success") is True and j["data"]["status"] == "interface_missing" and j["data"]["traffic_allowed"] is False' \
    "interface_missing network health" 200 >/dev/null

BLOCKED_RESPONSE="$(poll_json \
    "/api/v1/torrents/${INFO_HASH}/stats" \
    'j.get("success") is True and j["data"]["state"] == "network_blocked" and 0 < j["data"]["bytes_completed"] < j["data"]["total_length"]' \
    "network_blocked torrent state" 100)"
BLOCKED_BYTES="$(printf '%s' "$BLOCKED_RESPONSE" | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["data"]["bytes_completed"])')"

poll_json "/api/v1/network/diagnostics" \
    'j.get("success") is True and j["data"]["health"]["status"] == "interface_missing" and j["data"]["health"]["traffic_allowed"] is False and not any(item.get("selected") for item in j["data"]["interfaces"])' \
    "network diagnostics teardown" 100 >/dev/null

poll_json "/api/v1/stats" \
    'j.get("success") is True and j["data"]["active_downloads"] == 0 and j["data"]["active_seeds"] == 0 and j["data"]["scheduler"]["running_engines"] == 0 and j["data"]["scheduler"]["running_downloads"] == 0 and j["data"]["scheduler"]["running_metadata_fetches"] == 0 and j["data"]["scheduler"]["active_peer_workers"] == 0' \
    "empty data-plane scheduler registries" 100 >/dev/null

sleep 2
STABLE_RESPONSE="$(api_get "/api/v1/torrents/${INFO_HASH}/stats")" || \
    fail "torrent diagnostics stopped responding after path loss"
STABLE_BYTES="$(printf '%s' "$STABLE_RESPONSE" | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["data"]["bytes_completed"])')"
[[ "$STABLE_BYTES" == "$BLOCKED_BYTES" ]] || \
    fail "verified bytes advanced after teardown (${BLOCKED_BYTES} -> ${STABLE_BYTES})"

api_get "/health" >/dev/null || fail "public control-plane health route stopped responding"
kill -0 "$DAEMON_PID" 2>/dev/null || fail "daemon process exited after path loss"

echo "==> PASS: ${PARTIAL_BYTES} verified peer-wire bytes traversed the strict veth after local tracker discovery; path loss latched interface_missing, blocked the torrent, emptied data-plane diagnostics, and preserved the control API"
