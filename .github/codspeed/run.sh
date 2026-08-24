#!/usr/bin/env bash
set -euo pipefail

package=${1:?Rust package to benchmark}
jobs=${2:-4}

mkdir -p .tox/codspeed/cargo .tox/codspeed/home
PERYX_UID=$(id -u)
PERYX_GID=$(id -g)
export PERYX_UID PERYX_GID
if [[ -n ${GITHUB_EVENT_PATH:-} ]]; then
  PERYX_CODSPEED_EVENT_PATH=$GITHUB_EVENT_PATH
else
  PERYX_CODSPEED_EVENT_PATH=$PWD/.tox/codspeed/event.json
  printf '{}\n' > "$PERYX_CODSPEED_EVENT_PATH"
fi
export PERYX_CODSPEED_EVENT_PATH

if [[ -z ${PERYX_CODSPEED_IMAGE:-} ]]; then
  docker compose --profile codspeed build codspeed
fi
docker compose --profile codspeed run --rm codspeed "$package" "$jobs"
