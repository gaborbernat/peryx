#!/usr/bin/env bash
set -euo pipefail

peryx=${1:?path to the peryx binary}
conformance=${2:?path to the conformance.test binary}

port=18102
work=$(mktemp -d)
cleanup() {
  if [[ -n ${server_pid:-} ]]; then
    kill "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

cat >"$work/peryx.toml" <<EOF
host = "127.0.0.1"
port = $port
data_dir = "$work/data"

[[index]]
name = "store"
route = "store"
ecosystem = "oci"
hosted = true

[[index.access_token]]
name = "uploader"
secret = "conformance"
actions = ["write", "delete"]
EOF

"$peryx" serve --config "$work/peryx.toml" >"$work/server.log" 2>&1 &
server_pid=$!

if ! timeout 30 grep -m1 -q "peryx listening" < <(tail --pid="$server_pid" -n +1 -F "$work/server.log"); then
  status=running
  if ! kill -0 "$server_pid" 2>/dev/null; then
    set +e
    wait "$server_pid"
    status=$?
    set -e
  fi
  echo "peryx did not report a listening socket within 30s; process status: $status"
  cat "$work/server.log"
  exit 1
fi

if ! curl -sf "http://127.0.0.1:$port/v2/" >/dev/null; then
  echo "peryx reported its listener but the OCI endpoint failed"
  cat "$work/server.log"
  exit 1
fi

OCI_REGISTRY="127.0.0.1:$port" \
  OCI_TLS=disabled \
  OCI_REPO1=store/conformance \
  OCI_REPO2=store/crossmount \
  OCI_USERNAME=_ \
  OCI_PASSWORD=conformance \
  OCI_DATA_SHA512=false \
  "$conformance"
