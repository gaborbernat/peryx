#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd -- "$script_dir/../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
base="$scratch/base"

ci_runtime_filters=$(sed -n \
  '/^            rust:/,/^            frontend_tests:/p' \
  "$repo/.github/workflows/ci.yml")
codspeed_runtime_filters=$(sed -n '/^            shared:/,/^            workflow:/p' \
  "$repo/.github/workflows/codspeed.yml")
if grep -E -q "^[[:space:]]+- 'site/" <<<"$ci_runtime_filters"; then
  printf 'CI runtime filters include site automation\n' >&2
  exit 1
fi
if grep -E -q "^[[:space:]]+- 'site/" <<<"$codspeed_runtime_filters"; then
  printf 'CodSpeed runtime filters include site automation\n' >&2
  exit 1
fi

write() {
  local root=$1
  local path=$2
  local content=$3
  mkdir -p "$(dirname -- "$root/$path")"
  printf '%s\n' "$content" >"$root/$path"
}

write "$base" README.md 'Peryx stores artifacts.'
write "$base" crates/peryx-core/Cargo.toml $'[package]\nname = "peryx-core"'
write "$base" crates/peryx-example/docs/content/index.md 'Configure an artifact format.'
write "$base" site/content/core/index.md 'Configure indexes and storage.'
write "$base" site/data/roadmap.toml 'name = "Compiled packages"'
write "$base" site/templates/page.html '<main>{{ page.content }}</main>'
write "$base" site/content/contributing/build.md \
  'A Rust crate can expose traits. Build it with Cargo or rustc from crates/peryx-core.'
"$script_dir/check_user_doc_boundaries.sh" "$base"

reject() {
  local name=$1
  local path=$2
  local content=$3
  local root="$scratch/$name"
  local output
  cp -R "$base" "$root"
  write "$root" "$path" "$content"
  if output=$("$script_dir/check_user_doc_boundaries.sh" "$root" 2>&1); then
    printf '%s was accepted\n' "$name" >&2
    exit 1
  fi
  if ! grep -Fq 'user documentation contains implementation vocabulary' <<<"$output"; then
    printf '%s reported the wrong failure:\n%s\n' "$name" "$output" >&2
    exit 1
  fi
}

for fixture in \
  'crate|README.md|One crate.' \
  'crates|site/content/core/index.md|Shared crates.' \
  'cargo-upper|site/content/core/index.md|Use Cargo.' \
  'cargo-lower|site/data/roadmap.toml|name = "cargo"' \
  'rust|site/templates/page.html|<main>Rust</main>' \
  'rustc|crates/peryx-example/docs/content/index.md|Run rustc.' \
  'classes|site/content/core/index.md|Request classes.' \
  'traits|site/content/core/index.md|Shared traits.' \
  'package-hyphen|site/content/core/index.md|peryx-core' \
  'package-underscore|crates/peryx-example/docs/content/index.md|peryx_core'; do
  IFS='|' read -r name path content <<<"$fixture"
  reject "$name" "$path" "$content"
done

mkdir "$scratch/bin"
write "$scratch" bin/grep $'#!/usr/bin/env bash\nexit 2'
chmod +x "$scratch/bin/grep"
if output=$(PATH="$scratch/bin:$PATH" "$script_dir/check_user_doc_boundaries.sh" "$base" 2>&1); then
  printf 'grep failure was accepted\n' >&2
  exit 1
else
  checker_code=$?
fi
if ((checker_code != 2)); then
  printf 'grep failure returned %s instead of 2:\n%s\n' "$checker_code" "$output" >&2
  exit 1
fi
