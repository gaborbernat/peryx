+++
title = "Failover and recovery"
description = "Symptom-driven runbooks for the shipped single-writer model: restart a crashed writer, promote a replica, restore a verified backup, and validate the result."
weight = 8
+++

This page is the operator runbook for recovering a peryx deployment. It turns the reference material into procedures you
follow when something has already failed: each section names a symptom, states what you must know before you act, gives
the exact control commands, and tells you the state to expect, how to validate it, and how much data the path can lose.

Everything here operates the `none` [availability contract](@/core/availability-contracts.md): one writer with local
durability, read replicas you populate yourself, and operator-driven failover. The `dc` and `ha` modes that acknowledge
into a second failure domain are the contract's later work; their automated failover, pending-write inspection, and
quorum recovery are not yet available, so this runbook covers neither. Where a procedure would differ under those modes,
the contract page states the promise they will make.

## Classify the failure first

Recovery starts by naming what failed, because a crashed process and a lost disk are different events with different
data-loss bounds. The contract draws the line: a
[process crash with storage intact](@/core/availability-contracts.md#crash-versus-storage-loss) loses no acknowledged
mutation, while storage loss on a `none` node loses every mutation since the last backup. Match the symptom to a path
before touching anything.

{% mermaid() %}
flowchart TB
start["failure detected"] --> what{"what failed?"}
what -->|writer process, disk intact| restart["restart the writer"]
what -->|writer disk or host| repl{"a current replica?"}
what -->|a replica| rebuild["rebuild the replica from a writer backup"]
what -->|local blob store| damaged["restore from backup or repair the bucket"]
repl -->|yes| promote["promote the replica"]
repl -->|no| restore["restore the newest verified backup"]
class start accent
class restart,promote,rebuild good
class restore,damaged warn
{% end %}

Two facts make every path below faster, so establish them before an incident rather than during one. Know the writer's
configured `writer_identity`: promotion and restore both turn on it. And keep a verified backup within your
recovery-point budget, because [`backup verify`](@/core/backup-restore.md#verify-a-backup) run on the day you need a
backup is too late to learn it rotted. A backup you have not verified is not a recovery option.

## Writer process crash, storage intact

The writer process exited or was killed, but its data directory is undamaged. This is not a data-loss event. Everything
an acknowledgement covered is on durable local storage and survives the restart; only `none`'s non-durable freshness
cache can drop a cached page, which costs a refetch, never a mutation.

1. Confirm the disk is intact: the data directory mounts, and `peryx backup verify` against a recent backup still
   passes, so you know the store you are about to reopen is sound.

1. Start the writer against the same data directory:

   ```shell
   peryx serve --config peryx.toml
   ```

   The writer reclaims its configured identity from the store because no other node holds the claim.

1. Wait for `GET /+ready?writes=true` to return `200 OK`, then return write traffic. `GET /+status` reports the process
   role and last observed upstream reachability once it is up.

Data at risk: none acknowledged. If the process will not stay up, treat the event as storage loss and move to a restore
rather than restart-looping a corrupt store.

## Writer host or storage loss, with a current replica

The writer's host or disk is gone and you run a read replica that followed it. Promote the replica in place. Promotion
rewrites the store's writer claim; it neither copies data nor stops any process, so fencing is yours to enforce.

1. Fence the old writer so it can never accept another mutation. A promoted replica and a returning old writer both
   holding the same identity is the split you are preventing; do not skip this because the host "looks dead".

1. Bring the replica's copy current if you can still reach the old writer's storage: finish copying its metadata and any
   trailing blobs to the replica and verify the copy. What the replica never received is the data you will lose.

1. With the replica stopped and its configuration still carrying the old `writer_identity`, replace the store's claim:

   ```shell
   peryx writer promote writer-b --config peryx.toml
   ```

   The command reads `writer_identity` from the config as the expected claim, compares it against the store's current
   claim, and refuses a stale or missing value. On success it prints a tab-separated `writer` line naming the old and
   new identity. A `Changed` error means the store's claim is not the identity you configured; stop and reconcile before
   forcing anything.

1. Set `writer_identity = "writer-b"`, remove `read_only` (and the `[availability]` `replica` role if you set one), and
   start the node as the writer.

1. Wait for `GET /+ready?writes=true` to return `200 OK`, then move write traffic to the new writer.

1. Rebuild the former writer as a replica before returning it to service, so it never competes for the claim.

Data at risk: every mutation the replica had not yet copied at promotion, bounded by its frontier — the highest serial
it holds. If it goes wrong: because promotion only rewrote a claim, the old storage, if it survives, still holds the
original history; recover from it or from a backup rather than from the promoted node.

## Writer host or storage loss, no current replica

The writer is gone and no replica was following it. Restore the newest verified backup onto a fresh node. Restore
verifies the whole backup before it writes a byte, so a corrupt backup halts recovery instead of seeding a damaged
store.

1. Pick the newest backup inside your recovery-point budget and verify it on the host that will restore it:

   ```shell
   peryx backup verify /backups/peryx-2026-08-01
   ```

   A backup that passed at creation but fails after a copy across hosts has caught the corruption before your restore
   depended on it. Do not restore a backup that does not print `ok`.

1. Restore into an empty data directory:

   ```shell
   peryx restore /backups/peryx-2026-08-01 --data-dir /var/lib/peryx
   ```

   Restore refuses a target that already holds files unless you pass `--force`, which replaces it wholesale — that guard
   keeps a restore from colliding with a node that is still live. If it warns that the snapshot's `data_dir` differs
   from `--data-dir`, reconcile the two before serving: point the node at the new path with `--data-dir`, or edit
   `data_dir` in the restored `config.toml` so the snapshot and the on-disk layout agree.

1. Start the writer against the restored directory and wait for `GET /+ready?writes=true` to return `200 OK`:

   ```shell
   peryx serve --data-dir /var/lib/peryx
   ```

Data at risk: every mutation acknowledged after the backup's instant. A backup is a point-in-time image, not a journal,
so the interval between backups is your worst-case loss. If it goes wrong: restore is idempotent into an empty
directory, so a failed attempt leaves the backup untouched — verify a second backup and restore that instead.

## Replica lost or unrecoverably behind

A replica crashed, its disk failed, or its frontier fell so far behind that readiness reports `frontier_lag` that never
reaches zero and then `sync_error`. A replica holds no authoritative state, so you rebuild it from the writer rather
than recover it.

1. Take the replica out of rotation. A read pool pointed at `GET /+replication/v1/ready` drops a replica that answers
   `503` on its own, so a lagging or disconnected replica leaves service without your intervention; confirm it is gone
   before you rebuild.

1. Populate a fresh data directory from a verified writer backup, or from your external replication system, copying the
   metadata store and referenced blobs from the same point in time.

1. Start the node in replica mode with the writer's identity in its configuration:

   ```shell
   peryx serve --config peryx.toml --read-only
   ```

   Replica mode keeps the writer's `writer_identity` in configuration and in the copied store; peryx refuses to start
   unless that claim matches a nonblank value. It serves reads and rejects mutations with `503 Service Unavailable`.

1. Return the replica to the read pool once `GET /+replication/v1/ready` reports `200 OK` and its `lag` is closing
   toward the primary's serial.

Data at risk: none authoritative. A rebuilt replica only re-copies state the writer still holds; the writer's history is
untouched throughout.

## Local blob store damage

Metadata is intact but referenced blob bytes on the local filesystem store under `<data_dir>/blobs` are missing or no
longer hash to their digest. A reader that resolves the metadata but reaches a damaged blob is told the blob is
unavailable, not handed wrong bytes, so the damage is contained to the affected artifacts.

1. Scope the damage. `peryx backup verify` against a backup taken before the damage confirms which digests a good copy
   holds; the live store's failing reads name the artifacts a client can no longer fetch.

1. If the node stores blobs on the local filesystem, restore the newest verified backup as in the no-replica path above.
   Restore rehashes every blob against its digest as it writes, so a restore cannot reintroduce the corruption it is
   replacing.

1. If the node stores blobs in an [S3-compatible bucket](@/core/configuration.md#blob), the bytes are the bucket's to
   protect, not the backup's to carry: recover them with the object store's own versioning, replication, or
   lifecycle-managed copy, then pair the recovered bucket with the metadata image the backup holds.

Data at risk: only blobs absent from every backup and, for a cache fill, refetchable from the upstream on the next miss.
A metadata record whose bytes are gone stays resolvable and reports the missing bytes rather than failing the whole
index.

## Partition between writer and replicas

The network cut the writer off from its replicas while both keep running. Under the `none` contract this is not a
failover event, and promoting during a partition is how you create two divergent writers.

1. Leave the writer serving. A `none` writer commits locally and acknowledges without waiting on any peer, so a
   partition never blocks its mutations. `GET /+ready?writes=true` stays `200 OK` throughout.

1. Leave the replicas serving reads at their frontier. A replica keeps answering the state it holds — a stale read a
   client can reason about beats an error — and its readiness reports `frontier_lag` while the poll cannot reach the
   writer, then `sync_error` once a poll fails. Both are expected during the partition.

1. Do not promote a replica to break the partition. There is one authoritative writer and it is still up; a second
   writer started against a replica's copy diverges the moment both accept a mutation, and no `none`-contract tool
   merges the two histories afterward.

1. When the link returns, replicas resume polling and their frontiers advance toward the writer's serial on their own.
   No operator action closes the gap.

Data at risk: none, provided you do not promote. The one way this event loses data is an operator forcing a second
writer during the partition.

## Recovery objectives

The contract states the recovery point and recovery time for `none` as a serial, not a stopwatch: the data at risk on a
storage loss is everything after the last external backup's serial, and return to service is the operator-driven restore
and promotion this page walks through. The
[recovery objectives table](@/core/availability-contracts.md#recovery-objectives) gives the same bounds beside the
stronger `dc` and `ha` promises for when those modes ship.

## Related

- The single-writer model, replica setup, promotion, and probes: [high availability](@/core/high-availability.md)
- Create, verify, and restore a backup in detail: [back up and restore](@/core/backup-restore.md)
- What each mode acknowledges and how much a failure risks: [availability contracts](@/core/availability-contracts.md)
- The exact flags on each command: [command line reference](@/core/cli.md)
