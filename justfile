set shell := ["bash", "-euo", "pipefail", "-c"]

project_tmp := justfile_directory() + "/.tox/tmp"
coverage_root := justfile_directory() + "/.tox/coverage"
frontend_root := justfile_directory() + "/.tox/frontend"
tools_root := justfile_directory() + "/.tox/tools"
export TMPDIR := project_tmp
export TMP := project_tmp
export TEMP := project_tmp
export PERYX_TEST_TMPDIR := project_tmp
export PLAYWRIGHT_BROWSERS_PATH := frontend_root + "/browsers"

default: test

_project-temp:
    mkdir -p "{{ project_tmp }}"

_docker-ready:
    docker info >/dev/null

format-check: _project-temp
    cargo fmt --all --check --

check: _project-temp
    cargo check --workspace --all-targets --all-features

clippy: _project-temp
    cargo clippy --workspace --all-targets --all-features -- -D warnings

lint-source: format-check clippy

lint-docs: _project-temp
    site/scripts/test_readthedocs.sh
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
    prek run mdformat --all-files
    prek run codespell --all-files

lint-automation: _project-temp compose-check
    SKIP=cargo-fmt,cargo-clippy,render-diagrams,mdformat,codespell prek run --all-files

lint-deps: _project-temp
    cargo deny check

snapshots: _project-temp
    cargo insta test --package peryx-ecosystem-pypi --lib --all-features \
      --unreferenced reject --test-runner nextest --nextest-profile ci

semver base="origin/main": _project-temp
    cargo semver-checks check-release --workspace --default-features --baseline-rev "{{ base }}"

lint-contracts base="origin/main": snapshots
    just semver "{{ base }}"
    just release-plan

lint base="origin/main": _project-temp
    just lint-source
    just lint-docs
    just lint-automation
    just lint-deps
    just lint-contracts "{{ base }}"

test-deps: _project-temp
    PATH="{{ tools_root }}/bin:$PATH" UV_TOOL_BIN_DIR="{{ tools_root }}/bin" \
      UV_TOOL_DIR="{{ tools_root }}" uv tool install twine

test: test-deps
    PATH="{{ tools_root }}/bin:$PATH" cargo nextest run \
      --workspace --exclude peryx-storage --all-features --profile ci \
      -E 'not(test(e2e_live))'
    cargo nextest run --package peryx-storage --profile ci
    cargo test --workspace --all-features --doc
    just benchmark

benchmark: _project-temp
    cargo test --workspace --all-features --bench '*' --no-fail-fast

platform-test: _project-temp
    cargo check --workspace --all-targets --all-features
    cargo nextest run --package peryx --test cli_entrypoint --all-features --profile ci
    cargo nextest run --package peryx-upstream --all-features --profile ci
    cargo nextest run --package peryx-test-support --all-features --profile ci
    cargo nextest run --package peryx-storage --all-features --test integration \
      --profile ci -E 'test(/blob_backend/)'

e2e: _project-temp
    cargo nextest run -p peryx-pypi-system-tests --features e2e --test e2e -E 'not(test(e2e_live))'

e2e-live: _project-temp
    cargo nextest run -p peryx-pypi-system-tests --features e2e-live --test e2e -E 'test(e2e_live)'

pypi-system: _project-temp
    cargo nextest run -p peryx-pypi-system-tests --tests \
      -E 'not(binary(e2e)) & not(binary(availability)) & not(binary(s3_upload))'

oci-system: _project-temp
    cargo nextest run -p peryx-oci-system-tests --tests -E 'not(binary(availability))'

s3: _project-temp
    cargo nextest run -p peryx-pypi-system-tests --test s3_upload

storage-s3: _project-temp _docker-ready
    cargo nextest run -p peryx-storage --features container-tests --test integration

availability: _project-temp
    cargo nextest run -p peryx --features availability-e2e --test availability --test cluster --test observability
    cargo nextest run -p peryx-pypi-system-tests --test availability
    cargo nextest run -p peryx-oci-system-tests --test availability

simulation filter="all()": _project-temp
    cargo nextest run -p peryx --features sim-campaign --test sim_campaign -E '{{ filter }}'

features: _project-temp
    cargo hack --workspace --each-feature check --all-targets

direct-minimum: _project-temp
    #!/usr/bin/env bash
    scratch=$(mktemp -d "{{ project_tmp }}/direct-minimum.XXXXXX")
    trap 'rm -rf "$scratch"' EXIT
    rsync -a --exclude .git --exclude .tox --exclude target ./ "$scratch/"
    cd "$scratch"
    cargo +nightly update -Z direct-minimal-versions
    cargo +nightly check --workspace --all-targets

miri: _project-temp
    #!/usr/bin/env bash
    export TMPDIR="${RUNNER_TEMP:-/tmp}"
    cargo +nightly miri test --package peryx-core --lib --tests
    cargo +nightly miri test --package peryx-pql --lib --tests
    cargo +nightly miri test --package peryx-policy --lib --tests

loom: _project-temp
    RUSTFLAGS="--cfg peryx_loom" cargo test --package peryx-ha-distributed --lib runtime_worker::loom_tests

sanitizer-address: test-deps
    ASAN_OPTIONS=allow_addr2line=1 PATH="{{ tools_root }}/bin:$PATH" cargo +nightly nextest run --workspace \
      --target x86_64-unknown-linux-gnuasan --profile ci --build-jobs 1 \
      --test-threads 1 -E 'not(test(e2e_live))'

sanitizer-thread: test-deps
    TSAN_OPTIONS=allow_addr2line=1:halt_on_error=1 PATH="{{ tools_root }}/bin:$PATH" \
      cargo +nightly nextest run --workspace \
      --target x86_64-unknown-linux-gnutsan --profile ci --build-jobs 1 \
      --test-threads 1 -E 'not(test(e2e_live))'

fuzz package target seconds="60": _project-temp
    cd "crates/{{ package }}/fuzz" && cargo +nightly fuzz run \
      --target "$(rustc +nightly --print host-tuple)" "{{ target }}" -- -max_total_time="{{ seconds }}"

mutation shard="0/1" in_place="false" jobs="2": test-deps
    #!/usr/bin/env bash
    args=(--workspace --all-features --test-tool nextest --shard "{{ shard }}" --output .tox/mutants)
    if [[ "{{ in_place }}" == true ]]; then
      args+=(--in-place)
    else
      args+=(--jobs "{{ jobs }}" --jobserver-tasks "{{ jobs }}")
    fi
    PATH="{{ tools_root }}/bin:$PATH" cargo mutants "${args[@]}" -- -E 'not(test(e2e_live))'

# Install browser-test dependencies for the shared and owner suites.
frontend-deps: _project-temp
    npm --prefix crates/peryx-web/tests/frontend ci
    npm --prefix crates/peryx-ecosystem-pypi/tests/frontend ci
    npm --prefix crates/peryx-ecosystem-oci/tests/frontend ci

# Install Chromium and optional host dependencies for browser tests.
frontend-browser-deps *args: _project-temp
    npm --prefix crates/peryx-web/tests/frontend exec -- playwright install {{ args }} chromium

# Run the shared and owner browser suites against an existing build.
frontend-test: _project-temp
    npm --prefix crates/peryx-web/tests/frontend test
    npm --prefix crates/peryx-ecosystem-pypi/tests/frontend test
    npm --prefix crates/peryx-ecosystem-oci/tests/frontend test

# Print tool versions used by local and container validation.
versions: _project-temp
    rustc --version
    cargo --version
    cargo nextest --version
    cargo llvm-cov --version
    just --version
    node --version
    npm --version

# Build and test the browser application.
frontend: frontend-deps _project-temp
    just frontend-browser-deps
    cargo leptos build
    just frontend-test

# Stage the shared site shell and owner documentation.
site-stage: _project-temp
    site/scripts/stage.sh

# Check committed Mermaid partials against their source hashes.
diagrams: _project-temp
    node site/scripts/render_diagrams.mjs --check

# Regenerate committed Mermaid partials.
render-diagrams: _project-temp
    npm --prefix site ci
    npm --prefix site run render

# Build and validate the assembled documentation site.
docs: _project-temp diagrams site-stage
    mkdir -p .tox/site/static
    cargo run --quiet --package peryx --bin peryx -- openapi > .tox/site/static/openapi.json
    zola --root .tox/site check
    zola --root .tox/site build
    python3 .tox/site/scripts/inline_diagrams.py .tox/site/public

# Check links in the assembled site.
site-links: docs
    node .tox/site/scripts/check_external_links.mjs .tox/site

# Build the assembled site.
site: docs

# Validate the cargo-dist release plan.
release-plan: _project-temp
    cargo dist plan --output-format=json > /dev/null

# Run one package's external conformance suite.
conformance package suite binary="": _project-temp
    cargo build --bin peryx
    binary="{{ binary }}"; \
      scripts/ci/conformance "{{ package }}" "{{ suite }}" \
        "${binary:-${CARGO_TARGET_DIR:-target}/debug/peryx}"

# Run one CodSpeed benchmark in the CI container.
codspeed package jobs="4": _project-temp
    .github/codspeed/run.sh "{{ package }}" "{{ jobs }}"

# Select CodSpeed benchmark legs from named owner changes.
codspeed-matrix event runner shared +changes: _project-temp
    @scripts/ci/codspeed-matrix "{{ event }}" "{{ runner }}" "{{ shared }}" {{ changes }}

# Hash a CodSpeed runtime revision.
codspeed-runtime-id revision: _project-temp
    scripts/ci/codspeed-runtime-id "{{ revision }}"

# Name a CodSpeed image from its definition.
codspeed-image-tag image: _project-temp
    scripts/ci/codspeed-image-tag "{{ image }}"

# Hash benchmark source state.
codspeed-source-key: _project-temp
    scripts/ci/codspeed-source-key

# Preserve compatible CodSpeed cache timestamps.
codspeed-preserve-cache current restored: _project-temp
    scripts/ci/codspeed-preserve-cache "{{ current }}" "{{ restored }}"

# Record CodSpeed source state.
codspeed-record-sources: _project-temp
    scripts/ci/codspeed-record-sources

# Build a local Python wheel.
package-wheel +args: _project-temp
    scripts/ci/package-python wheel dist {{ args }}

# Build a local source distribution.
package-sdist output="dist": _project-temp
    scripts/ci/package-python sdist "{{ output }}"

coverage-native output=".tox/coverage/native.lcov": test-deps _docker-ready
    mkdir -p "$(dirname "{{ output }}")"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --workspace --all-features --bench '*' --no-report
    PATH="{{ tools_root }}/bin:$PATH" cargo llvm-cov nextest --workspace \
      --all-features --profile ci --lib --bins --tests --examples \
      -E 'not(test(e2e_live))' --no-report
    cargo llvm-cov report --no-default-ignore-filename-regex \
      --ignore-filename-regex '/(\.cargo/(registry|git)|\.rustup/toolchains|rustc/[0-9a-f]+)/' \
      --fail-uncovered-lines 0 --show-missing-lines --lcov --output-path "{{ output }}"

coverage-frontend: _project-temp
    scripts/coverage-frontend

coverage output=".tox/coverage": _project-temp
    just coverage-native "{{ output }}/native.lcov"
    just frontend-deps
    scripts/coverage-frontend "{{ output }}/frontend-native.lcov" \
      "{{ output }}/frontend-wasm.lcov" "{{ output }}/frontend.lcov"

# Remove local Rust coverage build artifacts and locks.
coverage-clean:
    bash scripts/ci/cleanup-workspace-artifacts coverage

# Remove transient project-owned artifacts.
clean:
    bash scripts/ci/cleanup-workspace-artifacts normal

# Remove project-owned artifacts, including reusable build state.
clean-all:
    bash scripts/ci/cleanup-workspace-artifacts all

# Run repository hooks against all files.
pre-commit: _project-temp
    prek run --all-files

# Prepare writable bind-mounted build caches.
_linux-dirs:
    mkdir -p .tox/docker/cache .tox/docker/cargo .tox/docker/data .tox/docker/home \
      .tox/docker/target .tox/docker/tmp "{{ coverage_root }}" "{{ frontend_root }}" "{{ project_tmp }}"

# Validate the Linux test service definitions.
compose-check: _project-temp
    docker compose --profile test --profile system --profile analysis --profile 16g \
      --profile system-16g --profile codspeed config --quiet

# Run a Just recipe in the lightweight Linux container.
linux +args: _linux-dirs
    PERYX_UID="$(id -u)" PERYX_GID="$(id -g)" docker compose --profile test run --rm test {{ args }}

# Run a Just recipe with Docker-backed Linux services.
linux-system +args: _linux-dirs
    PERYX_UID="$(id -u)" PERYX_GID="$(id -g)" bash scripts/ci/compose-run system system {{ args }}

# Run a dynamic-analysis recipe in Linux.
linux-analysis +args: _linux-dirs
    PERYX_UID="$(id -u)" PERYX_GID="$(id -g)" docker compose --profile analysis run --rm test {{ args }}

# Run a Just recipe with a 16 GiB Linux memory limit.
linux-16g +args: _linux-dirs
    PERYX_UID="$(id -u)" PERYX_GID="$(id -g)" docker compose --profile 16g run --rm test-16g {{ args }}

# Run a Just recipe with Docker-backed services within a 16 GiB limit.
linux-system-16g +args: _linux-dirs
    PERYX_UID="$(id -u)" PERYX_GID="$(id -g)" PERYX_DOCKER_MEMORY=4g \
      bash scripts/ci/compose-run system-16g system-16g {{ args }}

# Remove Docker-backed Linux services.
linux-system-clean:
    bash scripts/ci/compose-run clean

# Rebuild the Linux test image from current upstream images and print tool versions.
linux-image:
    docker compose --profile test build --pull test
    PERYX_UID="$(id -u)" PERYX_GID="$(id -g)" docker compose --profile test run --rm test versions

ci: _linux-dirs
    just linux all

all: lint coverage docs
