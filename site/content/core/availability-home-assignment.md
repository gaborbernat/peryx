+++
title = "Home assignment on first publish"
description = "How the first successful publish assigns a project or repository its home datacenter under the control quorum, the canonical authority key that keeps name variants together, and how concurrent first publishes resolve to one home."
weight = 8
+++

An authority — a project's or repository's write ownership — has one home datacenter. That home is assigned on the first
successful publish and held by the first winner, and every later write fences on the epoch the assignment minted. This
page is the assignment mechanism: the canonical key a publish routes through, how the home is chosen, how concurrent
first publishes resolve to one home, and what a partition can and cannot do. It is the `ha` counterpart to
[authority transfer and drain](@/core/availability-authority-transfer.md), which moves a home after it is assigned, and
it builds on the [availability contracts](@/core/availability-contracts.md) for the durability each step preserves.

## Canonical authority keys

A publish routes to a home under an authority key, and the key is canonical so every spelling of the same identity
resolves to one authority. A PyPI project's key is its
[PEP 503](https://packaging.python.org/en/latest/specifications/name-normalization/) normalized name: case, and any run
of `-`, `_`, or `.`, fold to one key, so `Flask`, `flask`, and `Flask` all home the same project. An OCI repository's
key is its repository path under a scheme prefix, so distinct paths stay distinct.

The two ecosystems share one keyspace, so the keys must not collide. PyPI holds the unprefixed keyspace; a normalized
name never contains a colon. Every other ecosystem prefixes its keys with a scheme — `oci:` for a repository — which a
normalized name can never match, so a single-segment repository named `flask` and the PyPI project `flask` home two
distinct authorities.

## Choosing the home

The home a first publish assigns is the ingress datacenter that received it — the datacenter where the project or
repository was first published. That datacenter is, by construction, an eligible live member of the committed cluster:
it is the one serving the publish. There is no separate selection pass and no operator preassignment; the first place a
project is published is its home.

## Concurrent first publishes resolve to one home

Assignment is a compare-and-set on the control quorum. The command homes a *previously unassigned* authority and mints
its first epoch; an authority that already has a home rejects the command. So when several datacenters receive a first
publish for the same authority at once, the Raft log orders their commands and the first to commit wins the home. Every
later command finds the authority already assigned and commits as a rejection, which the losing datacenter reads as
"already homed" — it keeps the committed home rather than overwrite it.

{% mermaid() %}
flowchart TB
p1["DC east: first publish"] --> cas{"authority homed?"}
p2["DC west: first publish"] --> cas
p3["DC north: first publish"] --> cas
cas -->|"no — first to commit"| win["assign home, mint epoch one"]
cas -->|"yes — already homed"| lose["reject: keep the committed home"]
class win good
class lose warn
{% end %}

The winning command records an assignment audit alongside the home: its cause (first publish), the committed log
position — leader term and log index — that carried it, and the epoch it minted. That audit rides in the ownership
snapshot, so a replay or an operator reconstructs where and how each home was first assigned. Because the audit belongs
to the winning command, a rejected concurrent command never overwrites it: the trail names the home that was actually
assigned, not a loser's later position.

## Retries and partitions

A datacenter that is in a control-plane minority cannot commit an assignment — it forwards to the leader rather than
home the authority locally — so a partition cannot produce two homes. A publish whose claim cannot reach a quorum is not
lost: the claim is best-effort and logged, and a later publish reassigns nothing because the home, once committed, is
final. A restarted or rejoined candidate that missed the race finds the authority already homed and defers to the
committed winner, so a partition heal or a restart cannot expose a conflicting first publish.

## Related

- Moving a home after it is assigned: [authority transfer and drain](@/core/availability-authority-transfer.md)
- The durability each step preserves: [availability contracts](@/core/availability-contracts.md)
- The failure signal a transfer waits on: [node liveness](@/core/availability-liveness.md)
