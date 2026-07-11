#!/bin/sh
set -eu

if [ -d /etc/swarmotter ]; then
    chown swarmotter:swarmotter /etc/swarmotter
    chmod 0700 /etc/swarmotter
fi

if [ -f /etc/swarmotter/swarmotter.toml ]; then
    chown swarmotter:swarmotter /etc/swarmotter/swarmotter.toml
    chmod 0600 /etc/swarmotter/swarmotter.toml
fi

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
