#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd -P)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin" "$scratch/work"

cat >"$scratch/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s:%s\t%s\n' "$PERYX_UID" "$PERYX_GID" "$*" >>"$COMMAND_LOG"
EOF
chmod +x "$scratch/bin/docker"
export COMMAND_LOG="$scratch/commands.log"
export PATH="$scratch/bin:$PATH"

(cd "$scratch/work" && "$repo/.github/codspeed/run.sh" owner 2)

cat >"$scratch/expected.log" <<EOF
$(id -u):$(id -g)	compose --profile codspeed build codspeed
$(id -u):$(id -g)	compose --profile codspeed run --rm codspeed owner 2
EOF
cmp "$scratch/expected.log" "$COMMAND_LOG"
grep -Fqx '      HOME: /workspace/.tox/codspeed/home' "$repo/compose.yaml"
grep -Fqx "      PERYX_GID: \${PERYX_GID:-1000}" "$repo/compose.yaml"
grep -Fqx "      PERYX_UID: \${PERYX_UID:-1000}" "$repo/compose.yaml"
grep -Fqx '    entrypoint: ["peryx-codspeed-entrypoint", "ci/run-codspeed.sh"]' "$repo/compose.yaml"
