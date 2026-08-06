+++
title = "Analytics replication"
description = "How a replica folds a producer's daily usage aggregates in exactly once: the additive schema, producer identity, replay protection, and the frontier that bounds deduplication memory."
weight = 8
+++

A node records download usage into low-cardinality daily buckets, off the request path. Under the `dc` and `ha`
[availability contracts](@/core/availability-contracts.md) a producer ships those buckets to a replica so a promoted
replica reports the same totals a planned failover would. This page fixes the apply contract: what a batch carries, how
a replica accepts each producer interval once, how the transfer moves batches, and how deduplication state stays
bounded. In `none` mode nothing here runs and a package request performs no cross-node analytics call.

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

## Producing and pulling batches

A producer emits each completed UTC day as its own batch. A day is sealed once the current UTC day has moved past it, so
its buckets can no longer grow, and the day itself is the interval sequence, so the mapping from a day to its identity
never shifts. The current day is withheld until it seals. A producer serves its sealed batches after a requested day on
`+replication/v1/analytics`, bearer-gated by the replication token, off the request path.

A replica runs a background analytics worker on its bounded availability pool, the same place its metadata and blob
pulls run. Each pass asks its upstream for the sealed days beyond the highest it has accepted, folds each batch into its
durable apply state exactly once, and persists the converged state after any pass that applied something. It resumes
from that cursor, so a batch it already holds is a recognized duplicate and a restart never re-folds an accepted day. A
transport loss or a refused batch is logged and retried at the next poll rather than stopping the worker.

The producing node's analytics generation is durable and assigned once, reused across restarts, so a re-served sealed
day keeps the same identity and a replica recognizes the replay. A replica's durable apply state, its accepted totals,
its per-producer cursor, and its frontier, restore together under a schema tag, so a snapshot written by an unrecognized
build is refused rather than rebuilt from zero, which would double-count the next replay.

Cross-process transfer under real replicas is exercised by a follow-up harness test ([#949]); the produce, transfer,
apply, deduplicate, and compact path is proven in-process here.

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

## Querying completeness

`GET /+analytics/completeness` reports the accepted totals over a window and whether they cover every producer they
should. The picture a node answers from is its own converged apply state, read off the durable store per request, so the
query runs off the pull path and needs no live producer.

### Filters

- `repository`: an index route to confine the totals to, at most 512 bytes; omit for a cross-repository query.
- `from` and `to`: Unix timestamps, each floored to its UTC day. The end defaults to today and never runs past it; the
  start defaults to a trailing month and is capped to a bounded span, so one query can never scan an unbounded range.
- `limit`: day buckets to return, 1 through 100, defaulting to 25.
- `cursor`: the opaque `next_cursor` from the previous page.

### Completeness semantics

Completeness is measured against the cluster's own **accepted frontier**, the highest sealed day any expected producer
has been folded through, not the wall clock. Each expected producer is required to reach that frontier, capped at the
window end, at its own accepted epoch (a higher epoch always orders ahead). The verdict is:

- `complete`: every expected producer has reached the required day.
- `delayed`: every expected producer has an accepted frontier but at least one trails the required day, so the missing
  totals should still arrive.
- `unavailable`: an expected producer has delivered nothing, or no writer is configured at all. This is the fail-closed
  answer: a picture vouched for against zero producers cannot be told apart from a filter that narrowed to nothing.

Measuring against the frontier rather than the clock means a producer idle for a quiet day still reads as caught up once
it has reached the frontier; how stale the frontier itself is against today is reported separately as the lag. A
historical window whose end sits below the frontier requires coverage only through its own end, so a producer still
catching up to today can still be complete for the past. The expected producer set is the configured topology's writer
members, so a writer that has never delivered a batch is still expected and still marks the range incomplete.

### Role visibility

The verdict, the resolved interval, the accepted totals, and the day buckets are visible to any caller a repository
admits. The per-producer frontier list, the cluster frontier, the required day, and the lag are operator-only: a
repository-scoped caller (a repository reader or a per-index upload token) reads only the verdict and its own totals,
and never learns which datacenters exist or how they lag. An operator-wide query, with no `repository`, needs operator
authority; an operator may still narrow to one repository and keep the frontier.

### Partial results

A `delayed` operator response names the producers and where each sits:

```json
{
  "completeness": "delayed",
  "interval": {
    "from_day": 19722,
    "to_day": 19752,
    "retained_from_day": null,
    "window_clamped_to_retention": false
  },
  "totals": {
    "downloads": 128,
    "bytes": 64733247
  },
  "buckets": [
    {
      "day": 19752,
      "start_unix": 1706572800,
      "end_unix": 1706659200,
      "downloads": 12,
      "bytes": 9000000
    }
  ],
  "next_cursor": null,
  "frontier_day": 19752,
  "required_day": 19752,
  "lag_days": 1,
  "producers": [
    {
      "producer": "east-writer",
      "dc": "east",
      "state": "complete",
      "accepted_epoch": 1,
      "accepted_day": 19752
    },
    {
      "producer": "west-writer",
      "dc": "west",
      "state": "delayed",
      "accepted_epoch": 1,
      "accepted_day": 19750
    }
  ]
}
```

An `unavailable` response carries `accepted_epoch` and `accepted_day` of `null` for a producer that has delivered
nothing, and a repository-scoped caller sees the same `completeness` verdict without the `producers`, `frontier_day`,
`required_day`, or `lag_days` fields.

### Pagination, retention, and limits

The day buckets page over the opaque `cursor`; a present `next_cursor` means more buckets follow, and a null one ends
the series. The window resolves against the same daily-bucket retention the other usage views honor: `retained_from_day`
is the retention floor and `window_clamped_to_retention` marks a requested start that predated it, so a reader tells an
empty range apart from data aged out of retention. The window span, the day-bucket cardinality it bounds, and the row
limit are all capped, so one completeness query stays bounded whatever it is handed.

### Metrics and backup

`frontier_day`, `lag_days`, and each producer's `state` are the low-cardinality health signals the query exposes: the
lag says how old the newest sealed day is, and the per-producer states say which datacenters agree on it. The
completeness state, the accepted totals and the per-producer accepted frontier, lives in the replica's durable apply
snapshot, so it is captured and restored with the metadata store: a restored replica reports the same totals and the
same verdict it held at the backup's recovery point.

The [`none` availability contract](@/core/high-availability.md) leaves usage node-local, so none of this runs. See
[Monitor usage and cache health](@/core/monitor.md) for reading a single node's daily usage, and
[Back up and restore](@/core/backup-restore.md) for the recovery point a replica's state is pinned to.

[#949]: https://github.com/tox-dev/peryx/issues/949
