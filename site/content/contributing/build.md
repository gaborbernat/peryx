+++
title = "Build from source"
description = "Build and run a checkout with the repository-pinned toolchain."
weight = 2
+++

Install the pinned tools and build from the repository root:

```shell
rustup show
mise install
cargo build --release
```

The executable is `target/release/peryx` on Unix and `target/release/peryx.exe` on Windows. Start a local process with:

```shell
./target/release/peryx serve
```

Repeat `mise install` when a checkout changes `mise.toml`. Run `rustup show` when the compiler version changes; it
downloads the version pinned by `rust-toolchain.toml`. A linker or system-library error means the host lacks a build
dependency for its target. Check the failing package name and install that dependency through the host package manager.

Use `just docs`, `just site-links`, and `just pre-commit` before sending documentation changes. The
[contributing guide](@/contributing/_index.md) lists the full validation suite.
