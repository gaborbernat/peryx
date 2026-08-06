+++
title = "Authority transfer and drain"
description = "How a confirmed home failure moves an authority to a survivor under the control quorum, and how the writes the old home retained drain into the new one."
weight = 8
+++

An authority — a repository's write ownership — has one home datacenter, assigned on its first publish and held by the
first winner. When that home fails for good, its authority has to move to a survivor and the writes it never finalized
have to follow. This page is the mechanism: what threshold moves a home, how the target is chosen, what the move
commits, and how the retained writes drain into the new home. It is the `ha` counterpart to the single-writer
[failover and recovery](@/core/availability-failover-recovery.md) runbook, and it builds on
[node liveness](@/core/availability-liveness.md) for the failure signal and the
[availability contracts](@/core/availability-contracts.md) for the durability each step preserves.

## Suspicion never moves a home

Liveness aging is a routing hint, not a decision. A home that misses heartbeats becomes
[`Suspect`](@/core/availability-liveness.md) and then, past the dead-after threshold, `Dead`, but neither state moves
its authority on its own: a suspicion is a delay, not a failure, and a home that recovers within tolerance keeps
everything it owned. Only a home the tracker has confirmed `Dead` is a failover trigger, and even then the move is a
proposal the control quorum commits through consensus before any write is touched. This is deliberate — moving a home is
the one action that, done on a false positive, can split ownership, so it waits for a confirmed failure and a committed
decision rather than a timeout.

The wall-clock cost of a failover — its recovery-time objective — is therefore dominated not by the selection, which is
a bounded in-memory choice, but by the dead-after threshold the tracker waits out before it will call a home dead, plus
the one consensus round the control quorum takes to commit the move. Tune the failover RTO through the liveness
thresholds, not by weakening the confirmation.

## Choosing the target

Given a confirmed-dead home, the failover policy picks the datacenter to move it to from the candidates the roster
offers, each carrying the liveness the tracker holds for it. The choice is a single bounded pass:

- Only an `Alive` candidate is eligible. A candidate that is itself suspect, dead, or never heard from cannot receive a
  home, so a failover never moves an authority onto a datacenter that is already in doubt.
- The first eligible candidate in the caller's order wins. The caller orders the candidates, so the outcome is
  deterministic rather than dependent on map iteration.
- The pass weighs at most a bounded number of candidates, so a long roster cannot stall one decision.

When no candidate is alive, the policy holds: authority stays put and the old home's writes stay retained until a
candidate recovers, rather than move a home onto a datacenter that cannot serve it.

## Committing the move

The chosen move commits on the control quorum, which mints the authority's next epoch. That new epoch is the fence: any
write the old home had in flight under the previous epoch is now stale and is rejected, so a former home that comes back
cannot finalize a write against an authority it no longer owns. A datacenter that is in a control-plane minority cannot
commit the move at all — it forwards to the leader rather than transfer authority locally — so a partition cannot
produce two homes.

{% mermaid() %}
flowchart TB
alive["home Alive or Suspect"] -->|"within tolerance"| hold["hold: authority stays"]
dead["home confirmed Dead"] --> pick{"an Alive candidate?"}
pick -->|"no"| none["hold: writes stay retained"]
pick -->|"yes"| commit["control quorum commits the move, mints the fencing epoch"]
commit --> drain["drain the old home's retained intents at the new home"]
class hold,none warn
class commit,drain good
{% end %}

## Draining the retained writes

Moving the home settles ownership; it does not settle the writes the old home was still holding. Before a home finalizes
a write, the ingress datacenter that received it retains it as an intent (see the ingress staging model in the
[availability contracts](@/core/availability-contracts.md)). When the home moves, those intents have to be finalized at
the new home. That is the drain, run with [`peryx job drain`](@/core/cli.md#job-drain):

- **Ordered.** It finalizes the retained intents in the stable order they were admitted, held by a durable never-reused
  sequence that survives a restart, so the drain is deterministic and two operators running it reach the same result.
- **Resumable.** Each finalize only advances an intent, never re-applies it, so a drain interrupted partway resumes at
  the first intent still pending rather than double-finalizing the ones already settled. Re-running a completed drain is
  a no-op.
- **Bounded.** It finalizes in batches, so a large backlog drains in bounded transactions rather than one unbounded
  scan.
- **Fence-protected.** The run leases the authority's committed epoch. If the authority transfers again while the drain
  runs, the run wrote under a now-superseded epoch, so its success is fenced out and it fails with `authority_fenced`
  rather than finalize under an authority it no longer holds. Re-run the drain at the current home.

Every retained operation reaches exactly one outcome — finalized at the new home, or retired because a newer write
already superseded it — so a home loss at the transfer boundary yields one home and one settled outcome per operation,
never a double-write and never a dropped one.

## Reconciling old-epoch operations

A transfer mints a higher epoch, and the [authority fence](@/core/availability-contracts.md) stops the old home from
applying more work under the epoch it lost. The operations it durably recorded before the transfer still sit in its log,
though, and each needs one terminal disposition so the two homes never disagree about what the authority did.

Reconciliation classifies every such operation deterministically from its committed record, its epoch, and the current
metadata, into exactly one of four outcomes. An operation whose effect the committed state already carries is **already
applied** and reconciles to a no-op. A durable operation a newer operation has since overwritten is **superseded** and
dropped. An operation that never reached a durable commit **failed** with nothing to apply. A durable operation that
still stands is **replayable**: the new home re-issues it under the current epoch. The precedence is fixed, so the
outcome is single-valued and independent of evaluation order: a never-committed operation fails ahead of everything, and
an already-applied one is a no-op even when a later operation also superseded it, because idempotency has already
settled its effect.

A replay re-issues the operation under the new epoch while keeping its original source and serial, so it stays
idempotent, and continues the W3C trace the original authored rather than starting a disconnected one, so its audit
identity carries across the transfer. A replay that reaches an authority already past that serial is a no-op under the
same idempotency, so a retried or duplicated reconciliation never double-applies.

A reconciled record is retained until both the required-replica frontier and the operator audit-retention frontier have
passed its serial, then released. Holding it until every required replica has applied the outcome keeps a lagging
replica from re-litigating the operation after a restore, and holding it through the audit window keeps the operation
answerable to an operator query; releasing it once both frontiers cover it bounds the retained backlog.

The backlog is durable, so the drain is restart-safe: a home that stops mid-reconciliation resumes from the operations
still pending and settles each exactly once, and a re-scan of an already-settled operation never resets it. The drain
and the prune run in bounded batches, so the reconciliation scan and the retained backlog stay within their limits; the
pending backlog depth and the drain throughput are the signals to alert on, since a backlog that stops draining means
the new home is not settling what the old home left behind.

## Operator recovery

For a confirmed permanent home loss in an `ha` deployment:

1. Confirm the home is genuinely gone, not merely suspect — the transfer only proceeds on a `Dead` home, and promoting
   on a false positive is the outcome the confirmation exists to prevent.
1. Let the control quorum commit the transfer to the selected survivor; a minority cannot, so ensure the quorum is
   reachable.
1. Run `peryx job drain --authority <name>` at the new home to finalize the retained intents. Read the run back with
   `peryx job show` to confirm it succeeded; a run that reports `authority_fenced` raced a further transfer, so re-run
   it at the current home.

Data at risk: nothing acknowledged. A retained intent is durable at its ingress datacenter, so it survives the home loss
and the drain finalizes it; an unacknowledged in-flight write that never became an intent is the caller's to retry.

## Related

- The failure signal that a home is dead: [node liveness](@/core/availability-liveness.md)
- The durability each step preserves: [availability contracts](@/core/availability-contracts.md)
- The single-writer recovery runbook this parallels: [failover and recovery](@/core/availability-failover-recovery.md)
- The `job drain` command and its flags: [command line reference](@/core/cli.md#job-drain)
