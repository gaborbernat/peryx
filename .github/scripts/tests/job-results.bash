#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd -P)
context=$(jq -cn '{
  event: "pull_request",
  rust: true,
  contracts: false,
  docs: false,
  frontend_tests: false,
  automation: true,
  shared: false
}')
results=$(jq -cn '{
  changes: {result: "success"},
  coverage: {result: "skipped"},
  "crate-contracts": {result: "skipped"},
  docs: {result: "skipped"},
  frontend: {result: "success"},
  "lint-automation": {result: "success"},
  "lint-contracts": {result: "success"},
  "lint-deps": {result: "success"},
  "lint-docs": {result: "success"},
  "lint-source": {result: "success"},
  "mutation-diff": {result: "success"},
  "platform-test": {result: "success"},
  "scheduled-mutation": {result: "skipped"},
  "scheduled-nightly": {result: "skipped"},
  system: {result: "success"}
}')

"$repo/scripts/ci/check-job-results" main "$results" "$context" >/dev/null

context=$(jq '.contracts = true' <<<"$context")
results=$(jq '."crate-contracts".result = "success"' <<<"$results")
if output=$("$repo/scripts/ci/check-job-results" main "$results" "$context" 2>&1); then
    printf 'selected contracts allowed skipped coverage\n' >&2
    exit 1
fi
[[ $output == 'coverage=skipped' ]]

results=$(jq '.coverage.result = "success"' <<<"$results")
"$repo/scripts/ci/check-job-results" main "$results" "$context" >/dev/null
