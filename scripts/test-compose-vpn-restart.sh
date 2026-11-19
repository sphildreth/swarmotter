#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Exercise the built daemon through Docker's published port after restarting
# a healthy stand-in for Gluetun. The generated library is empty.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
fixture=$(mktemp -d)
project="swarmotter-recovery-$$"
compose=(docker compose -p "$project" -f "$fixture/compose.json")
cleanup() {
    if [[ -f "$fixture/compose.json" ]]; then
        "${compose[@]}" down --volumes >/dev/null
    fi
    rm -rf -- "$fixture"
}
trap cleanup EXIT
cp "$root/deploy/compose.yml" "$fixture"
touch "$fixture/gluetun.env"
docker compose -f "$fixture/compose.yml" config --format json >"$fixture/template.json"
# Copy the current executable before stripping debug symbols. The fixture's
# Debian runtime provides glibc and the health probe client.
binary=$(cd "$root" && cargo build --locked --bin swarmotterd --message-format=json |
    python3 -c 'import json, sys
for line in sys.stdin:
    artifact = json.loads(line)
    if artifact.get("target", {}).get("name") == "swarmotterd" and artifact.get("executable"):
        print(artifact["executable"])')
strip -o "$fixture/swarmotterd" "$binary"
cp "$root/config/swarmotter.container.toml.example" "$fixture/daemon.toml"
cp "$root/deploy/container-health.sh" "$fixture/"
cat > "$fixture/gluetun-entrypoint" <<'SH'
#!/bin/sh
test "$1" = healthcheck
SH
cat > "$fixture/curl" <<'SH'
#!/bin/sh
exec python3 -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:9091/live", timeout=2).read()'
SH
chmod 755 "$fixture/gluetun-entrypoint" "$fixture/curl"
python3 - "$fixture" <<'PY'
import json, sys
from pathlib import Path
root = Path(sys.argv[1])
template = json.loads((root / "template.json").read_text())["services"]
volume = "vpn-namespace:/run/swarmotter-vpn"
vpn = dict(image="alpine:3.21", command=["sleep","86400"],
           healthcheck={**template["vpn"]["healthcheck"], "interval":"1s"},
           volumes=[volume, f"{root}/gluetun-entrypoint:/gluetun-entrypoint:ro,Z"],
           ports=["127.0.0.1::9091"], restart="unless-stopped")
app = dict(image="python:3.12-slim", network_mode=template["swarmotter"]["network_mode"],
           depends_on=template["swarmotter"]["depends_on"], restart=template["swarmotter"]["restart"],
           environment={**template["swarmotter"]["environment"], "SWARMOTTER_API__AUTH_TOKEN":"namespace-fixture-local-auth-token"},
           user="10001:10001", tmpfs=["/var/lib/swarmotter:uid=10001,gid=10001,mode=0700"],
           command=["/daemon", "--config", "/daemon.toml"],
           volumes=[volume+":ro", f"{root}/swarmotterd:/daemon:ro,Z",f"{root}/daemon.toml:/daemon.toml:ro,Z",
                    f"{root}/container-health.sh:/probe:ro,Z",f"{root}/curl:/usr/local/bin/curl:ro,Z"],
           healthcheck={"test":["CMD","/probe"],"interval":"1s","timeout":"3s","retries":2})
(root/"compose.json").write_text(json.dumps({"services":{"vpn":vpn,"swarmotter":app},"volumes":{"vpn-namespace":{}}}))
PY
if ! "${compose[@]}" up -d --wait --wait-timeout 60; then
    "${compose[@]}" logs --tail 5
    for container in $("${compose[@]}" ps -aq); do docker inspect -f '{{json .State.Health}}' "$container"; done
    exit 1
fi
vpn=$("${compose[@]}" ps -q vpn)
app=$("${compose[@]}" ps -q swarmotter)
port=$("${compose[@]}" port vpn 9091)
curl --noproxy '*' -fsS -o /dev/null "http://$port/health"
docker restart "$vpn" >/dev/null
for _ in {1..60}; do
    if [[ $(docker inspect -f '{{.RestartCount}}' "$app") -gt 0 ]] &&
       [[ $(docker exec "$app" readlink /proc/self/ns/net) == "$(docker exec "$vpn" readlink /proc/self/ns/net)" ]] &&
       curl --noproxy '*' --max-time 2 -fsS -o /dev/null "http://$("${compose[@]}" port vpn 9091)/health"; then
        exit 0
    fi
    sleep 1
done
"${compose[@]}" ps
exit 1
