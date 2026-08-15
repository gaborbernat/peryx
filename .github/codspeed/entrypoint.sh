#!/usr/bin/env bash
set -euo pipefail

uid=${PERYX_UID:?PERYX_UID is required}
gid=${PERYX_GID:?PERYX_GID is required}

if ! getent group "$gid" >/dev/null; then
    groupadd --gid "$gid" peryx
fi
if ! getent passwd "$uid" >/dev/null; then
    useradd --no-create-home --uid "$uid" --gid "$gid" --home-dir "$HOME" --shell /usr/sbin/nologin peryx
fi

exec setpriv --reuid="$uid" --regid="$gid" --init-groups "$@"
