+++
title = "Run the benchmarks"
description = "Own, test, and run package benchmarks."
weight = 5
+++

`peryx-bench-core` owns neutral measurements and reports. `peryx-bench` owns neutral execution and comparison. Ecosystem
owners keep workloads, fixtures, command arguments, and result interpretation in their own crates.

The PyPI workload and server adapters live in `crates/peryx-ecosystem-pypi/src/bench/`. The OCI workload lives in
`crates/peryx-ecosystem-oci/src/bench/`. Each owner exposes its workload through a `peryx-bench-*` binary.

## Benchmark contracts

`just test` runs crate tests, then executes all workspace benchmark harnesses:

```shell
just benchmark
```

A benchmark target must fail when setup or the measured path fails. Keep behavior assertions in the crate's `tests/`
tree; measurement code belongs in `benches/`. A benchmark does not replace a unit or integration test.

## Run one target

List targets without measuring them:

```shell
cargo bench --workspace --all-features --no-run
```

Execute one harness through the same Cargo mode used by the repository gate:

```shell
cargo test --locked -p PACKAGE --all-features --bench TARGET -- --help
```

Replace `--help` with the target's documented arguments. Keep the toolchain and `Cargo.lock` fixed between revisions.
Record the host, power mode, scratch filesystem, and competing load with measured results. Wait for service readiness
during setup. A pacing sleep is valid when elapsed time is part of the workload.

## Ecosystem suites

Inspect an ecosystem benchmark command or run one target:

```shell
cargo run -p peryx-ecosystem-pypi --features bench --bin peryx-bench-pypi -- --help
cargo bench --locked -p peryx-ecosystem-pypi --bench parse
cargo run -p peryx-ecosystem-oci --features bench --bin peryx-bench-oci -- --help
cargo bench --locked -p peryx-ecosystem-oci --bench manifest_by_digest
```

The PyPI comparison suite resolves an exact request corpus for each supported platform. Regenerate both corpus manifests
after changing `requirements.in` or the pinned Python or pip version:

```shell
mise install --locked
uv run --python 3.14.7 --no-project benchmark_corpus.py macos-aarch64
uv run --python 3.14.7 --no-project benchmark_corpus.py linux-x86_64
```

uv resolves the requirements for the target platform, and pip 26.2.1 then selects one wheel per pin. pip alone would
evaluate environment markers against the machine running the script, so a Linux corpus generated on macOS would drop
torch's Linux-only CUDA dependencies. The generator refuses a corpus whose dependency closure, extras included, has a
gap. It records each wheel's URL, size, SHA-256 digest, the dependencies that apply on the target, and whether the
project is a root request. A rerun keeps the existing pins and only resolves what they lack; pass `--fresh` after
changing `requirements.in`. Review both manifest diffs before committing them. PyPI may remove or replace invalid
releases, so regeneration is an explicit corpus update rather than part of every run.

Run the comparison with the lockfile, corpus, schedule seed, report path, and scratch boundary fixed:

```shell
cargo run --locked -p peryx-ecosystem-pypi --features bench --bin peryx-bench-pypi -- \
  --rounds 5 --seed 1161 --report target/pypi-benchmark.toml --scratch .tox/bench/scratch
```

Before timing, the runner downloads each recorded artifact into the content-addressed `fixture-v1` scratch cache and
verifies its size and SHA-256 digest. Every server then has to expose the same normalized project, version, filename,
and digest set. The command stops if any candidate is missing or extra. The report records the seed, shuffled
server-round schedule, exact roots and artifacts, server and client versions, commands, and every raw timing sample. The
displayed tables are summaries of those samples. `site/data/benchmark-machine.toml` records the host and scratch storage
profile.

The report marks a cell as an error when any scheduled round fails instead of summarizing the surviving rounds. It
retains successful samples in the machine-readable cell for diagnosis.

The PyPI root-catalog fixture is `crates/peryx-ecosystem-pypi/src/bench/packages.rs`. Build its million-project example
before collecting at least five rounds from one machine and power state:

```shell
CARGO_TARGET_DIR=.tox/target-catalog cargo build --release \
  -p peryx-ecosystem-pypi --example catalog_million

/usr/bin/time -l .tox/target-catalog/release/examples/catalog_million
```

## CodSpeed

CodSpeed runs owner-selected benchmarks in simulation mode on standard GitHub-hosted runners:

```shell
just codspeed PACKAGE
```

PyPI parsing and serving benchmarks use separate builds so parser measurements exclude serving dependencies. CI runs
those selections in parallel; the local recipe runs both.

`peryx-ecosystem-pypi` builds its benchmarks as a single codegen unit. Under Cargo's default of 16, editing any file in
the crate repartitions the units, and local ThinLTO then folds a different amount of the measured pipeline into the hot
body, so simulation reported double-digit movement on commits that never touched the measured path. Keep a benchmarked
crate on one unit when its hot path is a large inlined body.

Use `simulation` for in-process compute paths. Compare wall-time revisions under the same host conditions outside CI.
