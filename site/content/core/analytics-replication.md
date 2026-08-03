+++
title = "Analytics replication"
description = "How a replica folds a producer's daily usage aggregates in exactly once: the additive schema, producer identity, replay protection, and the frontier that bounds deduplication memory."
weight = 8
+++

A node records download usage into low-cardinality daily buckets, off the request path. Under the `dc` and `ha`
[availability contracts](@/core/availability-contracts.md) a producer ships those buckets to a replica so a promoted
replica reports the same totals a planned failover would. This page fixes the apply contract: what a batch carries, how
a replica accepts each producer interval once, and how deduplication state stays bounded. The wire transfer that carries
a batch between nodes and the durable apply state a replica keeps land with the multi-peer transport; in `none` mode
nothing here runs and a package request performs no cross-node analytics call.

## The additive aggregate

A batch carries additive rows. Each row is one dimension and its totals:

- `day`: the UTC day, in whole days since the Unix epoch.
- `repository` and `project`: the index and the package the downloads belong to.
- `version`: the distribution version, or empty when the ecosystem reports none (a content-addressed OCI layer has
  none).
- `source`: the routed upstream a cache miss fetched from, or empty when the bytes came from the local store.
- `downloads` and `bytes`: the request count and byte total to fold into that dimension.

Every field is a bounded server-side label, never a client identity, address, or credential. A batch carries no raw
request or actor history, so it stays low-cardinality and reveals nothing about who made a request. Additive means
order-free: folding two batches for one dimension in either order yields the same sum, which is what lets a delayed or
reordered batch converge.

## Producer identity and replay protection

Each producer interval is stamped with an identity that makes applying it idempotent: the producer, its authority epoch,
and the interval's monotonic sequence within that epoch. A replica accepts each distinct identity once and folds its
rows into the accepted totals. A duplicate, reordered, delayed, or retried batch that repeats an already-accepted
identity is recognized and dropped without changing a total.

A producer restart reuses the same identity for an interval it has already emitted, so replaying it after the restart is
a duplicate that never double-counts. A failover advances the producer's epoch, so the new authority's intervals are
distinct identities that apply rather than collide with the old authority's accepted work. Totals saturate rather than
wrap, so a corrupt or hostile producer total can never move an accepted sum backward.

## Frontiers, retention, and limits

Replay protection costs memory: the replica retains each accepted interval's identity to recognize its replay. A
**frontier** bounds that cost. It records the highest interval sequence each producer and epoch has durably passed
everywhere replay protection must outlast, the producer, this replica, and the backup. Compaction releases only the
identities the combined frontier covers, because a producer never resends below the sequence it has been acknowledged
for, so those identities can no longer be replayed. Accepted totals are never touched by compaction, so releasing an
identity preserves the sum.

Two bounds fail closed on untrusted input. A batch wider than the row limit is rejected before it folds. The
retained-identity set cannot grow past its limit: once full, a new interval is refused until compaction past the
frontier releases room, rather than letting a stalled frontier grow deduplication memory without bound.

The [`none` availability contract](@/core/high-availability.md) leaves usage node-local, so none of this runs. See
[Monitor usage and cache health](@/core/monitor.md) for reading a single node's daily usage, and
[Back up and restore](@/core/backup-restore.md) for the recovery point a replica's state is pinned to.
