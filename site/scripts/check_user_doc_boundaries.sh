#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=${1:-"$script_dir/../.."}
cd "$repo"

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

printf '%s\n' '\b(crate|crates|cargo|rust|rustc|classes|traits)\b' >"$scratch/patterns"
while IFS= read -r package; do
  printf '\\b%s\\b\n' "${package//-/[-_]}" >>"$scratch/patterns"
done < <(
  find crates -type f -name Cargo.toml -exec awk -F'"' '
    /^\[package\]$/ { package = 1; next }
    /^\[/ { package = 0 }
    package && /^name = "peryx-/ { print $2; package = 0 }
  ' {} + | sort -u
)

roots=(README.md site/content site/data site/templates)
while IFS= read -r docs; do
  roots+=("$docs")
done < <(find crates -type d -name docs | sort)

find "${roots[@]}" \
  \( -path 'site/content/contributing' -o -path 'site/content/contributing/*' \) -prune \
  -o -type f -print0 >"$scratch/sources"

while IFS= read -r -d '' source; do
  if grep -E -H -n -i -f "$scratch/patterns" -- "$source" >>"$scratch/matches"; then
    continue
  else
    grep_code=$?
  fi
  ((grep_code == 1)) || exit "$grep_code"
done <"$scratch/sources"

if [[ -s $scratch/matches ]]; then
  cat "$scratch/matches" >&2
  printf '%s\n' 'user documentation contains implementation vocabulary outside site/content/contributing' >&2
  exit 1
fi
