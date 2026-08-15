#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin" "$scratch/output"

cat >"$scratch/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s|%s|%s\n' "$CARGO_BUILD_JOBS" "$CARGO_INCREMENTAL" "$CARGO_PROFILE_DEV_DEBUG" >"$CARGO_TRACE"
printf '{}\n'
EOF

cat >"$scratch/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ ${*: -1} == *api.github.com* ]]; then
  printf '{"tag_name":"v0"}\n'
else
  printf 'archive'
fi
EOF

cat >"$scratch/bin/python3" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ ${1:-} == -c ]]; then
  cat >/dev/null
  printf 'v0\n'
fi
EOF

cat >"$scratch/bin/tar" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " != *" xz "* ]]; then
  exec "$REAL_TAR" "$@"
fi
while (($#)); do
  if [[ $1 == -C ]]; then
    tools=$2
    break
  fi
  shift
done
cat >/dev/null
cat >"$tools/zola" <<'TOOL'
#!/usr/bin/env bash
set -euo pipefail
while (($#)); do
  if [[ $1 == --root ]]; then
    root=$2
    break
  fi
  shift
done
mkdir -p "$root/public"
printf '<html></html>\n' >"$root/public/index.html"
TOOL
cat >"$tools/pagefind" <<'TOOL'
#!/usr/bin/env bash
exit 0
TOOL
chmod +x "$tools/zola" "$tools/pagefind"
EOF
chmod +x "$scratch/bin/"*

real_tar=$(command -v tar)
PATH="$scratch/bin:$PATH" \
  REAL_TAR="$real_tar" \
  CARGO_TRACE="$scratch/cargo" \
  READTHEDOCS_CANONICAL_URL=https://docs.example.invalid \
  READTHEDOCS_OUTPUT="$scratch/output" \
  "$repo/site/scripts/readthedocs.sh"

[[ $(cat "$scratch/cargo") == '2|0|0' ]]
[[ -f $scratch/output/html/index.html ]]
