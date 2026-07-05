#!/bin/sh
set -eu

if ! getent group swarmotter >/dev/null 2>&1; then
    groupadd --system swarmotter
fi

if ! id -u swarmotter >/dev/null 2>&1; then
    nologin=/usr/sbin/nologin
    if [ ! -x "$nologin" ] && [ -x /sbin/nologin ]; then
        nologin=/sbin/nologin
    fi

    useradd \
        --system \
        --gid swarmotter \
        --home-dir /var/lib/swarmotter \
        --shell "$nologin" \
        swarmotter
fi
