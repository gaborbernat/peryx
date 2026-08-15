#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd -P)
cd "$repo"
mapfile -t packages < <(scripts/ci/workspace-package-list contracts)
workflow=.github/workflows/ci.yml
action=.github/actions/crate-contract/action.yml
grep -Fxq '    default: "2"' "$action"
grep -Fxq "        CARGO_BUILD_JOBS: \${{ inputs.build-jobs }}" "$action"
[[ $(rg -c '^    name: crate contract \(' "$workflow") == "${#packages[@]}" ]]
for package in "${packages[@]}"; do
  job="crate-contract-$package"
  block=$(sed -n "/^  $job:/,/^  [a-z][a-z-]*:/p" "$workflow" | sed '$d')
  grep -Fxq "    name: crate contract ($package)" <<<"$block"
  grep -Fq "contains(fromJSON(needs.changes.outputs.contract_packages), '$package')" <<<"$block"
  grep -Fxq "          package: $package" <<<"$block"
  grep -Fxq "      - $job" "$workflow"
  if [[ $package == peryx-bench-core ]]; then
    grep -Fxq '          build-jobs: 1' <<<"$block"
  elif grep -Fq 'build-jobs:' <<<"$block"; then
    printf 'unexpected build job override: %s\n' "$package" >&2
    exit 1
  fi
done
[[ $(rg -c '^          build-jobs: 1$' "$workflow") == 1 ]]
contracts=$(sed -n '/^  crate-contract-peryx:/,/^  crate-contracts:/p' "$workflow")
if grep -Fq 'matrix:' <<<"$contracts"; then
  printf 'crate contract checks use a matrix\n' >&2
  exit 1
fi
rg -Uq 'mkdir -p \.tox\n +just affected-contract-packages "\$EVENT_NAME" > \.tox/affected-contract-packages' \
  "$workflow"

available=$(printf '%s\n' "${packages[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')
results() {
  jq -cn --argjson available "$available" --argjson selected "$1" '
    reduce $available[] as $package ({changes: {result: "success"}};
      .["crate-contract-" + $package] = {
        result: (if $selected | index($package) != null then "success" else "skipped" end)
      })
  '
}
selected='["peryx-core", "peryx-http"]'
selected_results=$(results "$selected")
context=$(jq -cn --argjson available "$available" --argjson selected "$selected" \
  '{available: $available, selected: $selected}')
scripts/ci/check-job-results contracts "$selected_results" "$context" >/dev/null
scripts/ci/check-job-results contracts "$(results '[]')" \
  "$(jq -cn --argjson available "$available" '{available: $available, selected: []}')" >/dev/null
scripts/ci/check-job-results contracts "$(results "$available")" \
  "$(jq -cn --argjson available "$available" '{available: $available, selected: $available}')" >/dev/null

assert_rejected() {
  if scripts/ci/check-job-results contracts "$1" "$context" >/dev/null 2>&1; then
    printf 'invalid contract check results passed\n' >&2
    exit 1
  fi
}
assert_rejected "$(jq 'del(."crate-contract-peryx-web")' <<<"$selected_results")"
assert_rejected "$(jq '."crate-contract-peryx-web".result = "success"' <<<"$selected_results")"
assert_rejected "$(jq '."crate-contract-peryx-core".result = "skipped"' <<<"$selected_results")"

scripts/ci/test-affected-contract-packages
