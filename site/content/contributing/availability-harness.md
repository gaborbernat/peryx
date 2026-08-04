+++
title = "Availability test harness"
description = "How the multi-process availability tests spawn real peryx binaries, inject network faults through Toxiproxy, and observe a datacenter group over its public HTTP surface."
weight = 40
+++

The availability harness stands up a group of real `peryx serve` processes, faults the network between them, and asserts
what an operator would see over HTTP. It lives in `crates/peryx/tests/harness/` and the self-tests that exercise it are
in `crates/peryx/tests/availability.rs`. Both sit behind the `availability-e2e` feature, so the default `cargo test` and
the coverage gate skip them; CI runs them in a dedicated job that installs `toxiproxy-server`.

Run them locally with the binary on your `PATH`:

```console
$ brew install toxiproxy     # or download toxiproxy-server from the releases
$ cargo test -p peryx --features availability-e2e --test availability
```

## What it gives a test

A test describes a group with a `Topology` and spawns it into a `Cluster`. `Topology::single()` is one stand-alone node;
`Topology::dc(group, members)` and `Topology::ha(group, members)` build a datacenter roster from `MemberSpec`s,
generating each node's config, ports, and the shared `[[availability.member]]` roster so every node agrees on it. Each
spawned `Node` owns a temp data directory, a captured log, and its ports; the handle drives it (`await_ready`, `kill`,
`restart`) and observes it (`status`, `readiness`, `topology`, `is_running`). Every process runs in its own group and is
killed when the `Cluster` drops, so a panicking test leaks nothing.

`Toxiproxy` wraps a managed `toxiproxy-server`: `proxy(upstream)` puts a controllable listener in front of a node's
socket, and the returned `Proxy` cuts (`partition`), restores (`heal`), or slows (`pause`) the link. This is how a test
partitions two nodes without touching their processes.

When an assertion fails, `Cluster::failure_report()` renders a per-node artifact: the topology snapshot, the status
body, and the tail of each log, so a red run is diagnosable from what peryx saw.

## What is not wired yet

The embedded ownership Raft node runs, but a multi-node consensus group cannot form yet: the inbound peer-RPC router is
not mounted, so a bootstrap never reaches quorum, and there is no HTTP surface to submit an ownership write or read the
current authority. The harness reflects this. `Topology::validate_config()` proves a generated `ha` or `dc` roster is
configuration peryx accepts (through `peryx config check`, no server), which is the reachable assertion today. The
`OwnershipControl` methods (`submit_ownership_write`, `leader`, `await_authority_transfer`) are defined but return
`HarnessError::Unsupported`; the failover test tier fills them once the write and authority endpoints land
([#540](https://github.com/tox-dev/peryx/issues/540)), and once the peer-RPC router is mounted the `ha` topologies will
spawn a live cluster rather than only validate their config.

## Extending it

The downstream availability tests ([#558](https://github.com/tox-dev/peryx/issues/558),
[#559](https://github.com/tox-dev/peryx/issues/559), and the cross-mode suites) add their own `tests/*.rs` files that
`mod harness;` this module and drive clusters through the same API. Add a capability to the harness rather than to each
test: a new observation goes on `Node`, a new fault on `Proxy`, and a new ownership control fills the `OwnershipControl`
trait when its endpoint exists.
