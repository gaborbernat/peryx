+++
title = "Finalizing admitted uploads"
description = "How the home datacenter turns an admitted upload into a published release: the fence and validations that run before publication, the one transaction that commits metadata and its outbox together, and how a retry after a timeout or restart returns the same result."
weight = 8
+++

An upload is [admitted](@/ecosystems/pypi/reference/uploads.md#durable-ingress-admission) wherever a client reaches, but
one datacenter turns it into an authoritative release: the [home](@/core/availability-home-assignment.md) of the
upload's authority. Finalization is that step. It validates the admitted intent against current state, then commits the
release metadata, the outbox entry a replica reconciles, the operation's terminal outcome, and the intent's advance in
one local transaction. This page is the finalize mechanism: the states an intent moves through, the checks that reject
before publication, how a retry replays one result, when an artifact becomes visible, and what it needs from remote
placement. Routing an admitted intent to its home and moving the bytes there is
[authority routing](@/core/availability-home-assignment.md); finalization is the home-side mechanism routing invokes.

## Finalization states

A finalize reads the durable [ingress intent](@/ecosystems/pypi/reference/uploads.md#durable-ingress-admission)
admission recorded and drives it to a terminal state. The intent advances only forward.

| State      | Meaning                                                                                      |
| ---------- | -------------------------------------------------------------------------------------------- |
| `pending`  | Admitted and staged, not yet finalized. Reclaimed by expiry if it is never finalized.        |
| `admitted` | Finalized: the release metadata and its outbox entry committed, and the artifact is visible. |

A committed publish also records a terminal **operation outcome** keyed by the admission's operation id, holding the
acknowledgement a retry replays. Only a publish records one: a refusal is transient and records nothing, so the presence
of the outcome is the source of truth for whether the work is done.

## Validation before publication

Finalization fails closed. Each check below runs before anything is published, and a failure publishes nothing and
records no durable outcome, so a retry re-evaluates it.

| Check           | Rejects when                                                                                                              |
| --------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Authority fence | The authority's committed epoch does not admit the finalize: its home moved, or the authority was never assigned.         |
| Checksum        | The digest or byte size to publish disagrees with what admission recorded.                                                |
| Placement       | The artifact's bytes are not placed, so a published page would reference a file no store holds.                           |
| Authorization   | The principal no longer holds write on the index. Permissions are re-checked at finalization, not trusted from admission. |

The fence is first: a datacenter that lost the authority's home to a
[transfer](@/core/availability-authority-transfer.md) stops finalizing its backlog under the stale epoch, so two homes
cannot both publish the same authority. The remaining checks are independent; any one rejects.

## One transaction

A passing finalize commits four facts together in one local transaction: the release rows, the outbox journal entry a
replica reconciles, the `published` operation outcome, and the intent's advance to `admitted`. They are one fact. Split
across transactions, a crash between the release rows and the journal entry would leave a file served here that no
replica ever receives, an outcome a retry cannot find, or an intent a home finalizes twice. Committing them together is
also the visibility rule below.

## Retry, timeout, and restart

The publish is idempotent on the operation id. Before it validates or publishes anything, a finalize reads the operation
outcome; a committed publish is replayed verbatim.

| Prior outcome | A retry returns                                                      |
| ------------- | -------------------------------------------------------------------- |
| none          | The finalize runs: it validates, then publishes or refuses.          |
| `published`   | The same success, with no second publish and no second outbox entry. |

A refusal is not durable. A finalize that refuses because the artifact's bytes were not yet placed publishes on its next
attempt once the bytes arrive, because a refusal records nothing for a retry to replay; only a committed publish is
sticky. A client that loses a response and resends, or a home that restarts mid-finalize and reruns its backlog, reaches
one committed publish for the operation, never two.

## Visibility

An artifact is visible only once its metadata and a matching committed outbox entry exist. Because finalization commits
both in one transaction, there is no window where a release is served without the journal entry a replica reconciles
from, and none where the journal names a release the metadata does not hold. A finalize that refuses, or one still
`pending`, exposes nothing.

## Remote placement

Finalization commits authoritative metadata without waiting for the bytes to move. It requires only that the artifact is
placed — that some store in the topology holds its verified bytes — recorded by admission at the ingress datacenter.
Copying those bytes to the home datacenter or to additional replicas is
[replication](@/core/availability-derived-views.md), which runs after finalization from the outbox entry it committed;
it is not part of turning the upload into a release. A read that resolves to a datacenter without the bytes yet fetches
them through the same path any cold read uses.
