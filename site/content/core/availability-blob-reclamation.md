+++
title = "Blob reclamation"
description = "How a replicated cluster selects an unreferenced blob safe to reclaim, gating deletion behind references, serveability, replication frontiers, and a fencing epoch."
weight = 9
+++

A content-addressed blob is shared across every project that names its digest, so single-node cache purge deletes a blob
the moment one reference snapshot no longer names it. In a replicated cluster that snapshot is not enough. A lagging
replica or a backup can still be replaying the very metadata that names the blob, so deleting its bytes now would strand
a plane that has not caught up. This page describes how the cluster selects a blob *safe* to reclaim. Selection records
a durable decision and never deletes bytes — the backend-delete executor is a separate concern.

## The retained-reference inventory

A digest is a reclamation candidate only when nothing still needs its bytes. The selector unions every source that
retains a reference before it considers a digest:

- **Visible metadata.** Every blob any ecosystem's metadata names — a cached file URL, a wheel's PEP 658 metadata
  sibling, a hosted upload, an OCI manifest's config and layers — read across the installed drivers.
- **Trash.** A trashed artifact can be restored, so it still pins its bytes until the trash entry is purged.
- **Serveable placements.** A verified [placement](@/core/blob-placement.md) a replica can serve keeps the digest, read
  in the same transaction as the selection so a copy that lands mid-scan is never raced.

A digest absent from every source is a candidate; a digest any source still names is spared, and an already-selected
candidate a reference reappeared for is abandoned rather than deleted.

## The frontier rule

Selecting a candidate records a *reclamation tombstone* stamped with the **required frontier**: the authoritative
metadata serial in force when the digest was selected. The bytes may be deleted only once every replication plane — each
live replica and each configured backup — has durably applied through that serial. A plane still reconstructing state
from not-yet-applied metadata that names the digest therefore keeps its bytes until it catches up. The gate is
conjunctive: a lagging replica or a lagging backup each holds the candidate pending, and only a candidate both planes
have cleared becomes eligible.

The observed per-plane applied frontier rides the cluster's liveness beacon. Until that source is wired, the selector
observes the closed `{replica: 0, backup: 0}` frontier: selection and tombstoning run live, and readiness stays
conservative — a candidate at a nonzero required frontier never advances — so no blob is ever marked deletable on
incomplete evidence.

## Fencing

Reclamation decides destructive storage state, so exactly one node runs it cluster-wide. The scheduler leases the pass
under the ownership group's monotonic cluster term through a cluster-singleton lease, and the pass stamps that term as
each tombstone's fence. A partitioned former holder mints a stale term, and both the lease claim and every tombstone
write reject it, so two workers under different epochs can never mark the same candidate. A process running no ownership
group reads term `0` and reclaims nothing.

## Plan state and retry

A tombstone moves through three states:

- **Pending** — selected as unreferenced and unservable, awaiting its frontiers.
- **Ready** — every plane cleared the required frontier and a final reference and serveability re-check passed under the
  fence, so a backend-delete executor may remove the bytes.
- **Skipped** — abandoned for a classified reason, because a reference returned or a placement became serveable again.

Each fenced advance increments the tombstone's `attempts`, so an operator sees retry pressure and a reader detects a
concurrent change. Re-selecting a digest raises its required frontier and re-arms even a candidate that had reached
`Ready`, so a reference or frontier that changed after a candidate was marked ready is re-proven before deletion. The
pass runs in bounded batches off the request path, scanning a bounded slice of the ledger per pass and yielding between
units of work.

## Backup interaction

A backup is an offline, operator-driven copy-out, not a live reference holder: a completed backup captures the
referenced set at command time and does not pin digests at runtime. What reclamation waits on is the backup's *applied
frontier* — how far a configured backup has captured — as one half of the frontier rule above, so a candidate is never
deleted before a backup that still needs it has captured through its serial.

## Metrics and recovery

The tombstone backlog exposes a bounded, low-cardinality progress view — the count of pending, ready, and skipped
tombstones — for a vacuum-style operator dashboard. Tombstones are durable metadata rows: they survive a restart, a
snapshot, and a restore with their state and retry counters intact, so a pass that resumes after a crash re-drives each
candidate from where it left off rather than re-selecting from scratch. A skipped tombstone is terminal until the digest
is re-selected, and a bounded prune clears the terminal backlog.

## Related

- [Blob placement](@/core/blob-placement.md) — the verified-copy ledger serveability reads.
- [Fenced cluster jobs](@/core/high-availability.md) — the cluster-singleton lease and fencing epoch.
- [Backup and restore](@/core/backup-restore.md) — the offline copy-out reclamation's backup frontier tracks.
