+++
title = "Artifact source and availability"
description = "Two typed dimensions peryx records for every artifact: where its bytes came from, and whether this instance can serve them without an upstream fetch. The transition table, the repair pass, and the storage guarantees behind them."
weight = 9
+++

peryx records two orthogonal facts about every artifact, apart from policy, yank, trash, and
[revocation](@/core/digest-revocations.md). **Source** is where the bytes came from. **Byte availability** is whether
this instance can serve them right now. A package read resolves both from one indexed lookup, so a listing never probes
the content store per artifact.

The two dimensions are independent. A proxied artifact can be locally cached or not; a hosted artifact is local until
its bytes are lost. Neither says anything about whether a policy permits the download, whether the publisher yanked the
release, or whether an administrator revoked the digest — those are separate dimensions a read applies on top.

## Glossary

**Source** — where an artifact's bytes originate. Intrinsic: caching or evicting the bytes never changes it.

- `hosted` — published into this instance. No upstream can resupply the bytes once they are lost.
- `proxy` — cached from an upstream index. A local miss is answered by re-fetching from upstream.
- `generated` — produced by this instance, such as a rendered index page or a derived metadata sibling. A local miss is
  answered by regenerating, not by an upstream fetch.

**Byte availability** — whether this instance can serve the bytes now. A projection, kept in step with the content store
by the events below.

- `local` — the configured storage holds verified bytes; a read serves them without an upstream fetch.
- `remote_only` — no local bytes, but a known upstream can supply them.
- `unavailable` — no local bytes and no upstream to supply them.

`local` means verified, complete bytes. [Metadata](@/core/glossary.md#artifact) alone, or a partial transfer that never
verified against its digest, does not make an artifact local.

## Transition table

An artifact's placement starts when it is first recorded and moves only along byte-availability. The source is fixed at
recording time. `has upstream` is true only for a `proxy` source.

| Event                                                                        | New availability                             |
| ---------------------------------------------------------------------------- | -------------------------------------------- |
| Recorded with verified local bytes (publish, generate, completed cache fill) | `local`                                      |
| Recorded without local bytes (discovered upstream)                           | `remote_only` if `proxy`, else `unavailable` |
| Verified bytes written                                                       | `local`                                      |
| Local bytes removed (eviction)                                               | `remote_only` if `proxy`, else `unavailable` |
| Write or cache fill failed                                                   | unchanged                                    |
| Repaired, bytes observed present                                             | `local`                                      |
| Repaired, bytes observed absent                                              | `remote_only` if `proxy`, else `unavailable` |

The failed-write row is the load-bearing one. A cache fill that does not verify leaves the prior placement exactly as it
was: it can neither drop a previously verified `local` copy nor fabricate one from a partial transfer. So a metadata
fetch or a truncated download can never produce `local`.

## Repair

The availability projection can drift from the content store: a blob removed out of band, a fill that crashed between
writing bytes and recording them. A repair pass reconciles it. Repair reads a bounded batch of placements in digest
order, checks each digest's byte presence, and rewrites any row whose projection disagrees. It returns a cursor to
resume the next batch, so it runs in fixed steps off the request path rather than as one long scan.

Repair touches the availability projection only. Source stays intrinsic, and policy, yank, trash, and revocation are
never read or written, so a stale-projection repair cannot alter an access decision. A batch is capped, so a repair pass
never holds a read span over the whole table.

## Storage guarantees

- **One indexed lookup.** Source and availability live in one record keyed by content digest. A package read resolves
  both without a per-artifact call into the content store.
- **Verified-only local.** A placement reaches `local` only after its bytes are written and verified against their
  digest. The content store is content-addressed, so anything present is by construction correct.
- **Source is intrinsic.** Caching, evicting, or repairing an artifact never rewrites its source. Only a different
  artifact taking the digest's place does.
- **Availability is a projection.** It is derived state, reconstructible by a repair pass from the content store, so a
  lost or stale projection is recoverable without upstream coordination.

## Cache-failure behavior

A cache fill streams upstream bytes into a pending blob and commits only when the digest verifies. The placement update
follows the outcome:

- The fill verifies and commits — the proxied artifact becomes `local`.
- The fill fails, aborts, or verifies to the wrong digest — the placement is left unchanged. A prior `local` copy stays
  `local`; an artifact that had no local bytes stays `remote_only`.

A store error while updating the projection is not fatal to the fill. The bytes are already correct on disk; the next
repair pass reconciles the projection. This keeps a transient metadata-store fault from failing a download or corrupting
the source dimension.

## API schema

The typed placement serializes with stable `snake_case` spellings, so a client matches on a value rather than parsing
prose:

```json
{
  "source": "proxy",
  "availability": "remote_only"
}
```

`source` is one of `hosted`, `proxy`, `generated`. `availability` is one of `local`, `remote_only`, `unavailable`. The
two vary independently: any `source` pairs with any `availability` its transitions allow.
