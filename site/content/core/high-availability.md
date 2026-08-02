+++
title = "High availability"
description = "Run one writer with read replicas and promote a replica during a planned failover."
weight = 7
+++

peryx supports one writer with multiple read replicas. Send mutation traffic to the writer. Replicas serve data copied
from the writer and reject mutation requests with `503 Service Unavailable`.

This page operates the `none` [availability contract](@/core/availability-contracts.md): peryx provides local durability
and leaves copying and failover to you. That contract also defines the `dc` and `ha` modes later work adds, and the
normative meaning of every acknowledgement below.

Give every writer a distinct, stable identity:

```toml
writer_identity = "writer-a"
```

At startup, the writer claims that identity in the metadata store. A writer configured with another identity cannot open
the store until an operator promotes it. This prevents a restored copy from starting as a second writer.

Enable replica mode in TOML:

```toml
read_only = true
```

A replica must retain the writer's identity in its configuration, and the copied metadata store must contain the same
claim. peryx stops startup unless that claim matches a nonblank configuration value.

```shell
PERYX_READ_ONLY=true peryx serve --config peryx.toml
peryx serve --config peryx.toml --read-only
```

The environment variable and command-line flag provide the same setting. Replica mode does not claim the configured
writer identity, so a restored configuration may retain the source writer's identity. It disables upstream cache fills,
webhook delivery, and background maintenance. A node that follows a primary through the
[`[availability]`](@/core/configuration.md#availability) table's `replica` role enforces the same writer-identity check.

Populate each replica's data directory from a verified backup or an external replication system before routing traffic
to it. Copy the metadata store and referenced blobs from the same point in time. peryx does not copy data between nodes
or coordinate a shared blob store.

## Load-balancer probes

`GET /+health` is the liveness probe. It returns `200 OK` with `{"status":"live"}` while the HTTP process can answer.
Metadata, blob-store, and upstream failures do not fail liveness because a restart cannot repair those dependencies.

`GET /+ready` checks the local metadata store and blob-store root used by package requests. It returns `200 OK` with
`{"status":"ready"}` or `503 Service Unavailable` with `{"status":"not_ready"}`. It does not scan metadata, enumerate
repositories, or contact an upstream. `GET /+ready?writes=true` also requires a writer; replicas return `503` for that
query while remaining ready for reads.

Both public probes are anonymous, bypass the hosted request limiter, and send `Cache-Control: no-store`. Their fixed
documents contain no repository, upstream, user, topology, or failure details. `GET /+status` is the detailed operator
surface: it stays reachable anonymously for coarse health, adds the process counters for `operator:read`, and reveals
the index topology and upstream reachability only for `administration:read`. That per-class filtering already keeps the
topology off an unauthenticated response, so an ingress rule is defense in depth rather than the primary control.

For [Kubernetes probes](https://kubernetes.io/docs/concepts/workloads/pods/probes/), let readiness remove a pod from
service before liveness restarts it:

```yaml
livenessProbe:
  httpGet:
    path: /+health
    port: 4433
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /+ready
    port: 4433
  periodSeconds: 5
  failureThreshold: 2
```

A generic load balancer should use readiness to select backends. For example, an
[HAProxy HTTP health check](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/reliability/health-checks/)
can use the same route for a read pool:

```haproxy
backend peryx-readers
    option httpchk GET /+ready
    http-check expect status 200
    server peryx-1 10.0.0.11:4433 check
    server peryx-2 10.0.0.12:4433 check
```

Use `/+ready?writes=true` for the writer pool. Do not use `/+health` for load balancing because it detects a process
that cannot answer at all, so it remains successful during recoverable dependency failures.

## Availability health and readiness

A `dc` or `ha` node serves two more probes scoped to replication itself. A `none` node runs no availability subsystem,
so it mounts neither.

`GET /+replication/v1/health` is the availability liveness probe. It answers `200 OK` in every configured mode with a
document a load balancer can ignore and an operator can read. It never fails on a frontier gap, because a restart cannot
advance a replica toward its primary.

`GET /+replication/v1/ready` is the availability readiness probe. It answers `200 OK` when the node can serve at its
frontier and `503 Service Unavailable` otherwise, naming every cause in `reasons`:

- `blob_store` — the mounted blob store failed its reachability check, so the mount cannot answer package requests.
- `frontier_lag` — a replica has not yet reached the primary's latest observed serial.
- `sync_error` — a replica's last poll of its primary failed.
- `incompatible_schema` — a replica's primary speaks an unsupported replication protocol version, which a later poll
  cannot resolve without upgrading the primary.

Both documents are filtered to the caller's class, like `/+status`. Any caller reads `mode`, `role`, `ready`, and
`reasons`. `operator:read` adds a replica's `serial`, `primary_serial`, `lag`, and synced counters, or a primary's own
`serial`. `administration:read` adds the redacted `upstream` origin a replica follows, with credentials, query, and
fragment removed. An anonymous or repository-only caller never reads a serial, lag, or peer origin, so the topology
stays off an unauthenticated response. Both probes send `Cache-Control: no-store`.

Point a replica read pool at readiness so a lagging or disconnected replica leaves rotation without a restart:

```haproxy
backend peryx-replicas
    option httpchk GET /+replication/v1/ready
    http-check expect status 200
    server replica-1 10.0.0.21:4433 check
    server replica-2 10.0.0.22:4433 check
```

When readiness reports `frontier_lag`, compare the replica's `lag` against the primary's write rate: a lag that never
reaches zero points at a stalled poll, which readiness reports as `sync_error` once a poll fails. An
`incompatible_schema` reason means the primary and replica were built against different replication protocol versions;
upgrade the primary before routing reads to that replica.

## Availability topology snapshot

An operator surface needs one picture of the whole group, not a probe against each node. `GET /+availability/topology`
returns that: one immutable snapshot of the configured availability topology, taken at a single instant and filtered to
the caller's class, so a page renders it without traversing live membership and storage state on every poll. Every node
serves it, including a `none` node, which reports its own single-node view.

The snapshot names the `mode`, the `group`, and a `nodes` roster drawn from the
[`[[availability.member]]`](@/core/configuration.md#availability) configuration. Each roster entry carries its `node`
identity, `dc`, `role`, and a `local` flag marking the node that produced the snapshot. A `local` block reports this
node's own live self-observation, which the process always knows: its `role`, its `liveness`, and the metadata
`frontier` it has committed. `captured_at` dates the snapshot in Unix seconds, and `node_count` reports the full roster
size when the `nodes` list is capped, so a stale or truncated render is visible rather than passing for a healthy,
complete one.

A peer's `liveness` in this snapshot is always `unknown`, with no frontier, because a node observes only itself until a
consensus layer reports its peers, and a snapshot never lets stale peer data read as `live`. This placeholder is not the
writer's beacon view: until that layer lands, a `dc` or `ha` writer already ages each replica's heartbeats into `alive`,
`suspect`, or `dead` on the `peers` field of its own `/+replication/v1/ready` and `/+replication/v1/health` documents
(see [Node liveness](@/core/availability-liveness.md)), so read peer liveness there rather than from the topology
snapshot. The local node reports `live` when its metadata and blob stores can serve and `unready` otherwise.

Fields are filtered to the caller's class, like `/+status`. Any caller reads `mode`, `group`, `captured_at`,
`node_count`, and each node's `node`, `dc`, `role`, and `local` flag. `operator:read` adds the `liveness` of every node
and the local `frontier`. `administration:read` adds each node's advertised `address`. An anonymous or repository-only
caller never reads a liveness, frontier, or peer address. The response sends `Cache-Control: no-store`, and the node
list is capped so one request cannot return an unbounded roster.

## Manual promotion

1. Stop or fence the old writer so it cannot accept another mutation.

1. Finish copying its metadata and blobs to the selected replica and verify the copy.

1. With the replica stopped and still configured with the old identity, replace the store's writer claim:

   ```shell
   peryx writer promote writer-b --config peryx.toml
   ```

   The command compares the configured identity with the store's current claim and refuses a stale or missing value.

1. Set `writer_identity = "writer-b"`, remove replica mode, and start the selected replica.

1. Wait for `GET /+ready?writes=true` to return `200`, then move write traffic to it.

1. Rebuild former writer nodes as replicas before returning them to service.

Promotion changes the store's claim; it does not copy data or stop the old process. peryx does not provide leader
election or online promotion. Do not promote until you fence the old writer, and do not start two writers against copies
that can diverge.

## Related

- Size and stand up each shape: [availability deployment and sizing](@/core/availability-deployment.md)
- What each mode's acknowledgement promises: [availability contracts](@/core/availability-contracts.md)
- The mode and replication keys: [`[availability]`](@/core/configuration.md#availability)
