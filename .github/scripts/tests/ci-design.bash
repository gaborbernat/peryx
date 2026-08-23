#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd -P)
cd "$repo"
if rg -n 'TWINE_SPEC|uv tool install.*twine|pipx:twine' .github/workflows scripts/ci/Dockerfile mise.toml; then
  printf 'shared automation installs an owner client\n' >&2
  exit 1
fi
if rg -n -- '--partition|partition:' .github/workflows/ci.yml; then
  printf 'platform CI uses hash partitions\n' >&2
  exit 1
fi
if rg -n 'actions/download-artifact' .github/workflows/ci.yml; then
  printf 'CI uses the deprecated Node artifact extractor\n' >&2
  exit 1
fi
download_actions=$(rg 'uses: actions/download-artifact@' .github/workflows/publish-pypi.yml .github/workflows/release.yml | wc -l | tr -d ' ')
linked_suppressions=$(rg 'download-artifact/issues/484' .github/workflows/publish-pypi.yml .github/workflows/release.yml | wc -l | tr -d ' ')
[[ $download_actions == "$linked_suppressions" ]]
grep -Fq 'gh run download' .github/workflows/ci.yml
grep -Fq 'Join-Path' .github/workflows/ci.yml
if rg -n 'docker login' .github/codspeed/run.sh; then
  printf 'CodSpeed runner manages registry credentials\n' >&2
  exit 1
fi
[[ $(rg -c 'docker/login-action@' .github/workflows/codspeed.yml) == 2 ]]
[[ $(rg -c 'just platform-contract' .github/workflows/ci.yml) == 1 ]]
coverage_job=$(sed -n '/^  coverage:/,/^  docs:/p' .github/workflows/ci.yml)
grep -Fq "needs.changes.outputs.contracts == 'true'" <<<"$coverage_job"
grep -Fq '          name: coverage-lcov' <<<"$coverage_job"
grep -Fq '            .tox/coverage/lcov.info' <<<"$coverage_job"
if rg -n 'codecov/codecov-action' .github/workflows; then
  printf 'workflow sends coverage outside GitHub Actions\n' >&2
  exit 1
fi
rg -q 'os: \[macos-26, windows-2025\]' .github/workflows/ci.yml
grep -Fq 'peryx_test_support' .config/nextest.toml
grep -Fq 'peryx_bench' .config/nextest.toml
grep -Fq 'machine::tests' .config/nextest.toml
grep -Fq 's3_backend' .config/nextest.toml
grep -Fq 'test_build_state_runs_an_exec_credential_provider' .config/nextest.toml
rg -q 'just fuzz-package peryx-ecosystem-pypi 30' .github/workflows/ci.yml
rg -q 'just fuzz-package peryx-ecosystem-oci 30' .github/workflows/ci.yml
contracts_job=$(sed -n '/^  lint-contracts:/,/^  platform-test:/p' .github/workflows/ci.yml)
grep -Fq 'github.event.repository.parent.full_name || github.repository' <<<"$contracts_job"
grep -Fq 'main:refs/remotes/baseline/main' <<<"$contracts_job"
grep -Fq 'github.event.pull_request.base.sha || github.event.before' <<<"$contracts_job"
grep -Fq 'BASE_REV=refs/remotes/baseline/main' <<<"$contracts_job"
for range in \
  '/^  scheduled-mutation:/,/^  mutation-diff:/p' \
  '/^  mutation-diff:/,/^  fuzz-pypi:/p'; do
  mutation_job=$(sed -n "$range" .github/workflows/ci.yml)
  grep -Fq 'uses: actions/setup-python@' <<<"$mutation_job"
  grep -Fq 'uses: astral-sh/setup-uv@' <<<"$mutation_job"
  grep -Fq 'toxiproxy-server-linux-amd64' <<<"$mutation_job"
done
mutation_diff=$(sed -n '/^  mutation-diff:/,/^  fuzz-pypi:/p' .github/workflows/ci.yml)
grep -Fq 'needs: changes' <<<"$mutation_diff"
if grep -Fq -- '--baseline skip' .github/scripts/mutation-diff; then
  printf 'mutation diff skips its baseline\n' >&2
  exit 1
fi
while IFS= read -r config; do
  grep -Fq '  retries: 0,' "$config"
done < <(find crates -path '*/tests/frontend/playwright.config.mjs' -print)
sed -n '/^  frontend:/,/^  coverage:/p' .github/workflows/ci.yml | grep -Fq 'sudo apt-get install --yes lcov'
rg -Fq -- "- 'crates/*/docs/**'" .github/workflows/ci.yml
rg -Fq -- "- '!crates/**/docs/**'" .github/workflows/ci.yml
[[ $(rg -c 'predicate-quantifier: some-with-excludes' .github/workflows/ci.yml) == 1 ]]
codspeed_shared=$(sed -n '/^            shared:/,/^            runner:/p' \
  .github/workflows/codspeed.yml)
if grep -Fxq "              - 'crates/**'" <<<"$codspeed_shared"; then
  printf 'CodSpeed shared changes include every owner path\n' >&2
  exit 1
fi
for pattern in \
  "crates/peryx-archive/{Cargo.toml,build.rs,src/*,src/!(tests)/**,benches/**}" \
  "crates/peryx-core/{Cargo.toml,build.rs,src/*,src/!(tests)/**,benches/**}" \
  "crates/peryx-ha/{Cargo.toml,build.rs,src/*,src/!(tests)/**,benches/**}" \
  "crates/peryx-ha-distributed/{Cargo.toml,build.rs,src/*,src/!(tests)/**,benches/**}" \
  "crates/peryx-pql/{Cargo.toml,build.rs,src/*,src/!(tests)/**,benches/**}" \
  "crates/peryx-ecosystem-pypi/{Cargo.toml,build.rs,src/*,src/!(tests)/**,benches/**}" \
  "crates/peryx-ecosystem-oci/{Cargo.toml,build.rs,src/*,src/!(tests)/**,benches/**}"; do
  grep -Fq -- "- '$pattern'" .github/workflows/codspeed.yml
done
codspeed_baseline=$(sed -n "/^          if \\[\\[ \"\\\$status\"/,/^          fi\$/p" \
  .github/workflows/codspeed.yml)
grep -Fq 'exact base benchmark is unavailable' <<<"$codspeed_baseline"
grep -Fq 'exit 0' <<<"$codspeed_baseline"
bash .github/scripts/tests/contract-checks.bash
metadata=$(cargo metadata --no-deps --format-version 1)
workspace_crates=$(jq -r '.packages[].manifest_path | sub("/Cargo.toml$"; "")' <<<"$metadata")
while IFS= read -r crate; do
  if ! grep -Fxq "$repo/$crate" <<<"$workspace_crates"; then
    printf 'CodSpeed filter names a missing workspace crate: %s\n' "$crate" >&2
    exit 1
  fi
done < <(rg -o "crates/[^/{']+" .github/workflows/codspeed.yml | sort -u)
package_ref=\$PACKAGE
grep -Fq "just crate-contracts \".tox/crate-contracts/$package_ref\" \"$package_ref\"" \
  .github/actions/crate-contract/action.yml
rg -q 'just conformance peryx-ecosystem-oci' .github/workflows/conformance.yml
if rg -n 'DISTRIBUTION_SPEC_REF|fcfba1ec' .github/workflows; then
  printf 'workflow contains an owner conformance revision\n' >&2
  exit 1
fi
[[ $(find . -maxdepth 2 -type f -name 'compose*.yaml' -print | sort) == ./compose.yaml ]]
rg -q '^[[:space:]]+GITHUB_ACTOR_ID:$' compose.yaml
while IFS= read -r target; do
  if rg -Fn "$target" .github/workflows; then
    printf 'workflow contains owner fuzz target: %s\n' "$target" >&2
    exit 1
  fi
done < <(jq -r '.packages[].metadata["peryx-ci"].fuzz.targets[]?' <<<"$metadata")
jq -e '
  all(.packages[] | select(.metadata["peryx-ci"].codspeed != null);
    (.metadata["peryx-ci"].codspeed.benches | type) == "array")
  and all(.packages[] | select(.metadata["peryx-ci"].fuzz != null);
    (.metadata["peryx-ci"].fuzz.targets | length) > 0)
  and all(.packages[] | select(.metadata["peryx-ci"].conformance != null);
    (.metadata["peryx-ci"].conformance.revision | length) == 40
    and (.metadata["peryx-ci"].conformance.runner | length) > 0)
' <<<"$metadata" >/dev/null
