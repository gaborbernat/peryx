#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd -P)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin"

cat >"$scratch/bin/getent" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'getent\t%s\n' "$*" >>"$COMMAND_LOG"
[[ ${IDENTITY_EXISTS:-false} == true ]]
EOF
cat >"$scratch/bin/groupadd" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'groupadd\t%s\n' "$*" >>"$COMMAND_LOG"
EOF
cat >"$scratch/bin/useradd" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'useradd\t%s\n' "$*" >>"$COMMAND_LOG"
EOF
cat >"$scratch/bin/setpriv" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'setpriv\t%s\n' "$*" >>"$COMMAND_LOG"
EOF
chmod +x "$scratch/bin/"*
export COMMAND_LOG="$scratch/commands.log"
export HOME=/workspace/.tox/codspeed/home
export PATH="$scratch/bin:$PATH"
export PERYX_GID=456
export PERYX_UID=123

"$repo/.github/codspeed/entrypoint.sh" command argument
cat >"$scratch/expected.log" <<'EOF'
getent	group 456
groupadd	--gid 456 peryx
getent	passwd 123
useradd	--no-create-home --uid 123 --gid 456 --home-dir /workspace/.tox/codspeed/home --shell /usr/sbin/nologin peryx
setpriv	--reuid=123 --regid=456 --init-groups command argument
EOF
cmp "$scratch/expected.log" "$COMMAND_LOG"

: >"$COMMAND_LOG"
IDENTITY_EXISTS=true "$repo/.github/codspeed/entrypoint.sh" command
cat >"$scratch/expected.log" <<'EOF'
getent	group 456
getent	passwd 123
setpriv	--reuid=123 --regid=456 --init-groups command
EOF
cmp "$scratch/expected.log" "$COMMAND_LOG"
