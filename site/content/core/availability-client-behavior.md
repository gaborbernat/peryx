+++
title = "Client behavior across availability modes"
description = "What a PyPI or OCI client observes as a node runs in none, dc, or ha mode: which statuses retry, how a retry resolves to one result, how to watch a write settle, and the protocol limits that hold in every mode."
weight = 11
+++

An availability mode changes how a write becomes durable, not the protocol a client speaks. A `twine`, `pip`, `uv`,
`docker`, or `oras` client uses the same PyPI and OCI requests against a `none`, `dc`, or `ha` node, and sees the same
success on the happy path. What the mode changes is the failure surface: which requests a partition refuses, which
statuses a client should retry, and how a lost response reconciles. This page is the client-side companion to the
normative [availability contracts](@/core/availability-contracts.md); read that page for what each acknowledgement
promises, and read the operator pages for how a node is [stood up](@/core/availability-deployment.md) and
[recovered](@/core/availability-failover-recovery.md).

## The modes a client meets

| Mode   | Acknowledges when                                                                | Select it with                                                             | A partition returns                                                                       |
| ------ | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `none` | the write is durable on local storage                                            | the omitted `[availability]` table, or `mode = "none"`                     | nothing to refuse: a single node either serves or is down                                 |
| `dc`   | metadata and bytes are durable in a second failure domain in the same datacenter | `[availability.replication]` with `role = "primary"` or `role = "replica"` | `503 Service Unavailable` on an authoritative mutation whose second domain is unreachable |
| `ha`   | metadata is durable in a remote datacenter; bytes converge behind it             | the `dc` keys plus a consensus roster and `node_identity`                  | `503 Service Unavailable` on an authoritative mutation whose remote domain is unreachable |

A mode never commits and then denies it: an acknowledgement means the write reached the durability the mode names, and a
mutation that cannot reach it refuses rather than lying. Reads never refuse for durability, and a cache fill never gates
on a peer because it is reconstructible from its upstream. The full matrix, including the per-mutation retry semantics
summarized below, is in the [contracts](@/core/availability-contracts.md#what-each-mode-acknowledges).

Selecting a mode is a configuration change; see the [`[availability]`](@/core/configuration.md#availability) table for
the `role`, `source`, `upstream`, `token`, and `[[availability.member]]` keys.

## Authorization does not change

Authorization is deny-by-default and decided once, at the request boundary, ahead of any durable write. A mode never
widens who may write: the credential that publishes on a `none` writer is the credential that publishes on a `dc` or
`ha` writer, and a read-only replica authorizes a read exactly as its writer does. Replication moves applied state,
never authority.

## Statuses a client retries

Three refusals are transient and safe to retry. A client should treat each as "try again", not "give up".

{% mermaid() %}
flowchart TD
req["client mutation"] --> role{writer or replica?}
role -- replica --> ro["503 read_only_replica"]
role -- writer --> home{authority current?}
home -- moved --> fence["PyPI 409 / OCI 503 UNAVAILABLE"]
home -- current --> dom{failure domain reachable?}
dom -- no --> busy["503 Service Unavailable"]
dom -- yes --> ok["written and acknowledged"]
class req,role,home,dom accent
class ok good
class ro,fence,busy warn
{% end %}

**A read-only replica refuses every mutation.** A replica answers a mutation with `503 Service Unavailable` and the body
`{"error":"read_only_replica","message":"this replica does not accept mutations"}`, ahead of any handler. Route mutation
traffic to the writer and read traffic to replicas; a load balancer keyed on `GET /+ready?writes=true` for the writer
pool and `GET /+replication/v1/ready` for the replica pool does this without a client change.

**A superseded authority fences a stale write.** After an `ha` authority moves to a survivor, a mutation the former home
still had in flight is refused so it cannot land under an epoch the group has advanced past. A standalone node runs no
group, so it holds no epoch and fences nothing. The two ecosystems report the fence differently, and a client that
publishes to both should handle both:

- OCI answers `503 Service Unavailable` with the error code `UNAVAILABLE` and the message "the repository authority
  moved while the request was in flight; retry the request".
- PyPI answers `409 Conflict` with the message "the project's authority advanced to a newer epoch; retry this control".

**Ingress backpressure sheds load.** When a `dc`/`ha` ingress node has staged its bounded backlog of un-finalized
uploads, a further PyPI upload is refused with `503 Service Unavailable` and "ingress admission backlog is full". Retry
with backoff; the backlog drains as the home finalizes.

A retry of any of these repeats the original request unchanged. Because every authoritative mutation is idempotent (see
below), a client can retry without reasoning about whether the first attempt half-applied.

## A retry resolves to one result

Every authoritative mutation carries an operation identity, and its terminal result is recorded under that identity. A
retry re-presents the same identity, finds the recorded outcome, and replays it instead of running the mutation a second
time. This is what makes the retries above safe: a client that loses the response to an upload and resends it converges
on one result rather than publishing twice or racing itself.

- An upload or push is idempotent by digest: re-sending the identical bytes resolves to the same success. A different
  payload under a filename or tag that is already taken is a real conflict, answered `409 Conflict` ("File already
  exists") rather than silently overwriting.
- A yank, unyank, delete, or restore is idempotent to its target state: repeating it leaves the same visible outcome.

The guarantee holds across a home failure at the response boundary. A write that became durable at the new home but
whose success never reached the client resolves, on the client's retry, to that original success.

## Watching a write settle

An acknowledgement is synchronous: the mode does not return success until the write is durable to the degree the mode
promises, so a client that received `200`/`201` holds a durable result and has nothing to poll. An operator watching the
group settle reads the pending surface instead of the client:

- `GET /+availability/operations` returns a health summary — `pending`, `published`, `failed`, `expired`, and `total`
  counts — for `operator:read`, with per-operation rows for `administration:read`. See
  [reading the operations view](@/core/availability-observability.md#pending-operations).
- The `peryx_dc_ack_pending_total` metric counts client writes still awaiting datacenter durability within their
  deadline, so a rising value is a settling backlog rather than a lost write.

There is no client-visible "pending" response on the mutation path: a `dc`/`ha` write either acknowledges durably or
refuses with a `503` the client retries.

## Reads never serve the wrong bytes

A read never trades correctness for availability. A replica holds a mutable read — a PyPI Simple page, an OCI tag —
behind its [readable frontier](@/core/availability-derived-views.md) until it has applied the serial that published it,
answering `404` rather than a stale view; a by-digest read is content-addressed and never held. When `ha` has the
metadata for a digest whose bytes have not yet converged, a fetch reports the bytes as unavailable rather than returning
wrong content: the client learns exactly what it is missing.

## Protocol limits hold in every mode

These limits are properties of the request surface, identical in `none`, `dc`, and `ha`:

- An OCI manifest body is capped at 4 MiB; an oversize manifest answers `413 Payload Too Large` under the `SIZE_INVALID`
  code, the spec having no payload-too-large code of its own.
- An OCI tags-list response is capped at 4 MiB and 100 pages; a client pages with the `Link` header.
- An OCI blob upload larger than the index's configured size limit is refused with the `DENIED` code.
- A PyPI upload that crosses a repository's configured quota is refused rather than partially stored.

## Not yet documented as working

Two adjacent capabilities are specified but not operable today, so this guide does not describe them as client behavior:

- Rolling upgrade and rollback runbooks depend on the upgrade tooling tracked in
  [availability observability](@/core/availability-observability.md#not-yet-available); until it ships, the operable
  path is the operator-driven [manual promotion](@/core/high-availability.md#manual-promotion) and
  [authority transfer and drain](@/core/availability-authority-transfer.md).
- A real-client example suite driving `pip`, `twine`, and `oras` against a multi-node cluster is forthcoming. The
  single-node real-client behavior these examples build on runs against a spawned server today.

## Related

- The normative meaning of each acknowledgement: [availability contracts](@/core/availability-contracts.md)
- Size and stand up each mode's shape: [availability deployment and sizing](@/core/availability-deployment.md)
- Read the topology, placement, and operations surfaces:
  [availability observability](@/core/availability-observability.md)
- Move authority off a failed home: [authority transfer and drain](@/core/availability-authority-transfer.md)
- The `[availability]` configuration keys: [`[availability]`](@/core/configuration.md#availability)
