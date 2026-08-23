#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd -P)
scratch=$(mktemp -d "${RUNNER_TEMP:-/tmp}/peryx-nightly-analysis.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin" "$scratch/runner"
cat >"$scratch/bin/rustc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'host: x86_64-unknown-linux-gnu\n'
EOF
cat >"$scratch/bin/rustup" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'rustup\t%s\n' "$*" >>"$NIGHTLY_ANALYSIS_LOG"
EOF
cat >"$scratch/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'cargo\tTMPDIR=%s\tASAN_OPTIONS=%s\tDEBUG=%s\t%s\n' \
  "$TMPDIR" "${ASAN_OPTIONS:-}" "${CARGO_PROFILE_DEV_DEBUG:-}" "$*" >>"$NIGHTLY_ANALYSIS_LOG"
EOF
touch "$scratch/bin/llvm-symbolizer"
chmod +x "$scratch/bin/"*
export NIGHTLY_ANALYSIS_LOG=$scratch/calls
export PATH=$scratch/bin:$PATH
export RUNNER_TEMP=$scratch/runner
export TMPDIR=$repo/.tox/tmp
existing_debug=${CARGO_PROFILE_DEV_DEBUG:-}

ASAN_OPTIONS=existing=1 "$repo/.github/scripts/sanitizer" address
grep -Fq $'rustup\ttoolchain install nightly --profile minimal --component rust-src' "$NIGHTLY_ANALYSIS_LOG"
grep -Fq \
  $'cargo\tTMPDIR='"$TMPDIR"$'\tASAN_OPTIONS=existing=1\tDEBUG=line-tables-only\t+nightly nextest run -Z build-std --workspace --target x86_64-unknown-linux-gnu --lib --bins --tests --examples --build-jobs 1 --test-threads 1' \
  "$NIGHTLY_ANALYSIS_LOG"

: >"$NIGHTLY_ANALYSIS_LOG"
"$repo/.github/scripts/sanitizer" thread
grep -Fq $'rustup\ttarget add --toolchain nightly x86_64-unknown-linux-gnutsan' "$NIGHTLY_ANALYSIS_LOG"
grep -Fq \
  $'cargo\tTMPDIR='"$TMPDIR"$'\tASAN_OPTIONS=\tDEBUG=line-tables-only\t+nightly nextest run --workspace --target x86_64-unknown-linux-gnutsan --lib --bins --tests --examples --build-jobs 1 --test-threads 1' \
  "$NIGHTLY_ANALYSIS_LOG"

: >"$NIGHTLY_ANALYSIS_LOG"
"$repo/.github/scripts/miri" peryx-core peryx-pql
grep -Fq $'cargo\tTMPDIR='"$RUNNER_TEMP"$'\tASAN_OPTIONS=\tDEBUG='"$existing_debug"$'\t+nightly miri setup' \
  "$NIGHTLY_ANALYSIS_LOG"
grep -Fq $'cargo\tTMPDIR='"$TMPDIR"$'\tASAN_OPTIONS=\tDEBUG='"$existing_debug"$'\t+nightly miri test --package peryx-core --lib --tests' \
  "$NIGHTLY_ANALYSIS_LOG"
grep -Fq $'cargo\tTMPDIR='"$TMPDIR"$'\tASAN_OPTIONS=\tDEBUG='"$existing_debug"$'\t+nightly miri test --package peryx-pql --lib --tests' \
  "$NIGHTLY_ANALYSIS_LOG"
