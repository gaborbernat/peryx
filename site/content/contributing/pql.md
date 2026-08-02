+++
title = "Query language (PQL) RFC"
description = "Why peryx's growing set of typed read endpoints should fold into one safe, read-only query language, and how it stays ecosystem-agnostic and scope-bound."
weight = 15
+++

Status: draft for discussion. This document proposes no implementation and no schema change. It argues for a design and
asks for a decision on direction before any code lands. Bracketed `#NNN` references point to
[peryx issues](https://github.com/tox-dev/peryx/issues).

## Summary

peryx is growing a set of typed read endpoints: the `/+analytics/*` usage family
([#457](https://github.com/tox-dev/peryx/issues/457), merged in [#645](https://github.com/tox-dev/peryx/issues/645)),
[`/+policy/decisions`](@/core/policy-decisions.md) ([#459](https://github.com/tox-dev/peryx/issues/459)), trash
inspection ([#460](https://github.com/tox-dev/peryx/issues/460)), and retention plan inspection
([#481](https://github.com/tox-dev/peryx/issues/481) / [#482](https://github.com/tox-dev/peryx/issues/482)). Each one
hand-rolls its own request struct, its own cursor, and one of two different authorization paths. This RFC proposes
folding that surface into a single query endpoint, `POST /+query`, over a small, safe query language (PQL, the Peryx
Query Language), the way Artifactory exposes
[AQL](https://jfrog.com/help/r/jfrog-artifactory-documentation/artifactory-query-language), but designed to avoid the
footguns AQL is known for. The existing typed endpoints stay as thin, stable presets over the same executor.

The core bet: one parser, one authorization path, one pagination and cost model, one place to reason about what a read
can and cannot see. Ecosystem-specific data (PyPI, OCI) reaches the language through a trait each ecosystem crate
implements, so the neutral core keeps its hard rule of no per-ecosystem branching.

## Motivation

Today a caller who wants to read peryx's operational state meets a patchwork:

- `GET /+stats` returns an untyped three-level JSON tree from `Metrics::drill()`.
- `GET /+analytics/top-packages` takes `limit` only, no cursor, and reads a `Vec<PackageUsage>`.
- `GET /+policy/decisions` takes eight query parameters and a `pd_{serial:016x}` cursor.
- `GET /+revocations` pages on its own cursor and gates on `AdministrationRead`.
- Retention ([#481](https://github.com/tox-dev/peryx/issues/481)) has a finished typed model (`RetentionDecision`,
  `RetentionSummary`) and no wire exposure at all.
- Trash lives per-driver in `DRIVER_KV` with no neutral cross-ecosystem read.

Three problems repeat across all of them:

1. **Duplicated pagination.** `PolicyDecisionPage` (`{ decisions, next_cursor }`) and `DigestRevocationPage`
   (`{ revocations, next_cursor }`) are the same list-plus-cursor shape with the same default-25 / max-100 limit and the
   same `InvalidLimit` / `InvalidCursor` errors, factored nowhere.
1. **Two authorization models, chosen ad hoc.** `/+policy/decisions` gates on the per-index token ACL
   (`authorize_all(.., Action::Write)`); `/+revocations` gates on the RBAC scope model
   (`authorize_scoped(.., Scope::AdministrationRead, &Resource::Operator)`). A reader has to learn which endpoint speaks
   which model.
1. **Every new read is a new endpoint.** Trash inspection and retention inspection are each a fresh handler, cursor, and
   ACL wiring, so the marginal cost of exposing operational state stays high.

A unified query surface collapses the marginal cost to "declare a domain" and gives one audited place to enforce scope,
bound cost, and classify fields.

## Goals

- One read surface (`POST /+query`) spanning usage, policy decisions, trash, retention, quota, revocations, and
  package/artifact metadata.
- A small, **non-Turing-complete**, side-effect-free language: selection, filtering, ordering, bounded pagination, and a
  fixed set of declared aggregates. It has no loops, recursion, user-defined functions, or mutation.
- Authorization enforced **structurally inside the evaluator**, not as a field the caller must remember to include. One
  result semantics for every caller; a grant narrows which rows and fields are visible, never what a query means.
- Ecosystem-specific data exposed through a trait each ecosystem crate implements. The neutral core stays
  ecosystem-agnostic.
- Predictable, bounded cost: a static estimate before execution, hard result caps, cursor pagination, optional streaming
  export, wall-clock timeout, and per-caller concurrency limits, all on by default.
- Existing typed endpoints preserved as thin presets over the same executor, so nothing regresses.

## Non-goals

- **Writes.** PQL is read-only. No `delete` / `update` actions in the language. The single largest AQL footgun, a query
  language that mutates, is out by construction.
- **Cross-domain joins in v1.** AQL's cross-domain `include` is where its cost and correctness fall apart. v1 queries
  hit exactly one domain.
- **Full-text search.** `/+search` (tantivy) already owns ranked text search over cached packages. PQL is structured
  selection over metadata, not a competitor to it.
- **A general expression or scripting language.** No arithmetic beyond comparison, no string building, no arbitrary
  computation. A caller who needs that post-processes the result.
- **Replacing typed endpoints.** They remain, as presets. This RFC does not propose removing them.

## When PQL wins, and when it does not

PQL wins when the read is ad hoc, spans dimensions a fixed endpoint did not anticipate, or crosses domains an operator
wants to correlate by hand: "blocked policy decisions for `team/*` in the last week, newest first", "cached retention
removals over 100 MB", "trash across both ecosystems that is still restorable". Adding a dimension is a filter, not a
new endpoint.

Typed endpoints still win when the read is hot, fixed, and cacheable, like a dashboard's top-packages widget, a
Prometheus scrape, or an uploader polling its own quota. A preset has a stable shape, a cache policy, and no parser or
cost estimator in the path. So PQL is the general surface and the presets are the fast paths, and the presets run as PQL
plans internally, so there is still one executor and one authorization path.

## Prior art and the lessons taken

### Artifactory AQL: the footguns to design out

AQL's shape is `items.find({criteria}).include(fields).sort(...).offset(n).limit(n)`, over domains (`items`, `builds`,
`releases`, ...) joined by relation paths. It is powerful and widely disliked for concrete, documented reasons this
design treats as requirements to avoid:

- **Scope is a field you must remember, not an invariant.** A non-admin item query *must* include `repo`, `path`, and
  `name` so the server can filter results by permission; omit them and the query errors ("AQL minimal field expectation
  error: repo, path and name") or spams warnings. Scope lives at the projection layer. PQL instead makes the caller's
  authorized scope a mandatory predicate the **evaluator injects**, that the query text can neither name nor remove.
- **Permission filtering happens after `limit`.** With `.limit(10)` and access to 3 of the first 10 rows, AQL returns 3.
  The count is wrong and the work is wasted. PQL applies the scope predicate before limit and pagination, as
  `peryx-search`'s `SearchAccess` already "carries ACL predicates into the query so totals and pagination cannot leak
  private resources".
- **Two result semantics.** Admins see everything regardless of include/exclude patterns; non-admins are filtered. The
  same query text means different things. PQL keeps one semantics; a broader grant only widens the injected scope set.
- **Mutation in a query language.** `.delete()` / `.update()` make a read tool a write tool. PQL is read-only.
- **Unbounded cost, bolted-on limits.** AQL's protections are real and worth copying: a max result set, a concurrency
  cap (429), timeouts (408), "avoid leading-wildcard `$match`", "avoid `items.find({})`", "limit join depth". But they
  are per-deployment configuration a fresh install lacks. PQL makes the equivalents defaults and rejects the expensive
  shapes at plan time.

### GraphQL: keep the typed schema, drop the per-resolver authz discipline

GraphQL's typed schema and introspection are the right idea: declared domains and typed fields make validation and field
classification static. But GraphQL's field-level authorization is a discipline applied at every resolver, easy to miss,
and its single endpoint invites query-cost and depth attacks that need separate complexity analysis. PQL takes the typed
schema and makes authorization structural, owned by the evaluator rather than opted into per field, and bounds cost
before execution rather than defending against nesting.

### OData: the closest ergonomic match

OData's `$filter` / `$select` / `$orderby` / `$top` over typed entity sets maps almost one-to-one onto peryx's domains
and is the most familiar shape for the job. PQL borrows the structure but prefers opaque cursors over `$skip` (offset
pagination is slow at scale, another AQL lesson) and keeps the function surface tiny.

### CEL, JMESPath, JSONPath: the predicate sublanguage

The `where` predicate is the part most tempting to over-power. [CEL](https://cel.dev/) (Common Expression Language) is
the model: non-Turing-complete by design, linear-time, mutation-free, with predictable cost measured in nanoseconds.
JSONPath's recursive descent and JMESPath's open function set are more than a filter needs and carry no complexity
guarantee. PQL's predicate is CEL-shaped and smaller: comparisons, membership, prefix match, and boolean logic over
declared typed fields, nothing else.

## The domain and entity model

A **domain** is a named, typed relation. Each domain declares its columns, each column's type, each column's
`FieldClassification` (`Public` / `Repository` / `Operator` / `Administrator`, reusing `peryx-http`'s
`response_security`), whether the column is indexed (usable as a cheap leading filter), and the authorization the domain
requires.

### Neutral domains (owned by `peryx-storage` / `peryx-events` / `peryx-policy`)

| Domain             | Backing type / source                    | Key columns                                                                                         | Authorization                       |
| ------------------ | ---------------------------------------- | --------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `usage.downloads`  | `PackageUsage` (durable totals)          | `repository, project, downloads, bytes`                                                             | repository-read, or `AnalyticsRead` |
| `usage.daily`      | `DailyUsage` (daily aggregate)           | `day, repository, project, version, source, downloads, bytes`                                       | as above; `source` is operator-only |
| `policy.decisions` | `PolicyDecisionRecord`                   | `repository, project, version, filename, source, action, state, rule, reason, evaluated_at, fresh`  | repository-read or `OperatorRead`   |
| `retention.plan`   | `RetentionDecision` + `RetentionSummary` | `project, version, artifact, digest, class, visibility, source, bytes, outcome, rule`               | `OperatorRead` (per repository)     |
| `trash`            | neutral `TrashInfo` + per-ecosystem rows | `ecosystem, repository, project, artifact, digest, reason, deleted_at, deadline, restorable, actor` | `OperatorRead`                      |
| `quota`            | `QuotaUsage` / `QuotaProjectUsage`       | `repository, project, bytes, limit`                                                                 | repository-read or `OperatorRead`   |
| `revocations`      | `DigestRevocation`                       | `digest, reason, state, revoked_at`                                                                 | `AdministrationRead`                |

### Ecosystem domains (owned by the ecosystem crates)

`packages`, `versions`, and `files` are **not** neutral. Package, version, and artifact records live as opaque blobs in
`DRIVER_KV`; the metadata store never interprets them. So these domains are served by the ecosystem driver, and each
ecosystem declares their exact columns:

| Domain       | PyPI source          | OCI source              | Shared columns                                     |
| ------------ | -------------------- | ----------------------- | -------------------------------------------------- |
| `packages`   | project index        | repository / tags       | `repository, name, ecosystem`                      |
| `versions`   | `Uploaded.version`   | manifest digests / tags | `repository, name, version`                        |
| `files`      | `File` / `Uploaded`  | manifest layers / blobs | `repository, name, version, digest, bytes, source` |
| `provenance` | PEP 740 attestations | (none in v1)            | `repository, name, version, digest, kind` (PyPI)   |

An ecosystem may declare columns beyond the shared set (`provenance` is PyPI-only). A query that names an ecosystem-only
column is valid only when the query is scoped to that ecosystem; the validator rejects it otherwise, so the neutral core
never learns what those columns mean.

## Surface syntax

The wire form is a JSON body carrying the query text and optional bound parameters; the query text is a small textual
DSL. A body, not a URL, because a query can carry a `where` clause longer than a query string should hold, and because
parameters bind out of band, never string-spliced. A `POST /+query` body:

```json
{
  "query": "from policy.decisions where repository == :repo and state == \"blocked\" order by evaluated_at desc limit 50",
  "params": {
    "repo": "pypi-proxy"
  },
  "cursor": null
}
```

The DSL stays close to SQL and OData reading order, one domain per query:

```
from <domain>
[ where <predicate> ]
[ select <field> [, <field> ...] ]
[ order by <field> [asc|desc] [, ...] ]
[ limit <n> ]
```

The `where` predicate grammar, CEL-shaped and non-Turing-complete:

```
predicate   := or_expr
or_expr     := and_expr ("or" and_expr)*
and_expr    := unary ("and" unary)*
unary       := "not" unary | comparison | "(" predicate ")"
comparison  := field op literal | field "in" "(" literal ("," literal)* ")"
             | field "starts_with" string
op          := "==" | "!=" | "<" | "<=" | ">" | ">="
literal     := string | int | bool | timestamp
timestamp   := "@" rfc3339          // e.g. @2026-06-01T00:00:00Z
```

No arithmetic, no functions, and no wildcards other than `starts_with`, a prefix match; leading wildcards are the AQL
full-scan trap and stay inexpressible. Fields resolve against the chosen domain's declared columns only; an unknown
field is a validation error, not an empty result.

Aggregation is a small, declared extension, not general `group by`. A domain may declare aggregate views; usage needs
sums:

```
from usage.daily where repository == :repo and day >= @2026-06-01
  aggregate sum(downloads) as downloads, sum(bytes) as bytes by project, version
  order by downloads desc limit 25
```

Only declared aggregate functions (`sum`, `count`, `min`, `max`) over declared numeric columns are allowed, so the
executor can push them into the store rather than materializing rows.

### Worked examples across the surface

Usage, top projects by download in a window:

```
from usage.daily where repository == :repo and day >= @2026-06-01
  aggregate sum(downloads) as downloads by project
  order by downloads desc limit 50
```

Policy decisions, recent blocks newest first (the `/+policy/decisions` preset, written by hand):

```
from policy.decisions where repository == :repo and state == "blocked"
  order by evaluated_at desc limit 25
```

Trash, restorable across both ecosystems, correlated by the operator:

```
from trash where restorable == true and deleted_at >= @2026-07-01
  select ecosystem, repository, project, artifact, digest, deadline
  order by deleted_at desc
```

Retention, large cached removals a plan proposes:

```
from retention.plan where repository == :repo and outcome == "remove" and class == "cached"
  select project, version, artifact, bytes, rule
  order by bytes desc limit 100
```

Packages, hosted PyPI projects with a name prefix (routes through the ecosystem seam):

```
from packages where repository == :repo and ecosystem == "pypi" and name starts_with "num"
  select name, ecosystem
  order by name asc limit 100
```

## Authorization

Authorization is the part to get right. The design principle is one line: the caller never writes their own scope; the
evaluator injects it, and the query cannot widen or remove it. That is the structural fix for the AQL scope-escape class
of bug.

### Resolving the caller across both models

peryx has two [access models](@/core/access-explained.md) the query layer must bridge:

- **Token / ACL (model A):** `IndexAcl::identify(header) -> Principal`, then `authorize` / `authorize_all` over `Action`
  and project globs. This is repository-scoped: a credential's grant covers some set of `project` globs on one index.
- **RBAC (model B):** `AuthorizationService::authorize_scoped(user, Scope, Resource)` over
  `Role { Administrator, RepositoryPublisher, RepositoryReader, Operator }` and
  `Scope { RepositoryRead, ..., OperatorRead, AnalyticsRead, AdministrationRead, ... }`, deny by default, fail closed.

PQL resolves the caller once, up front, into a single `QueryScope`: the set of repositories and project globs the caller
may read (from model A grants and model B `RepositoryRead` grants), plus the operator-level scopes the caller holds
(`OperatorRead`, `AnalyticsRead`, `AdministrationRead`, mapped to `Resource::Operator`). Both models feed one resolved
scope; the language exposes neither.

### Injecting scope into the plan

Every domain declares the authorization it requires. During planning:

1. **Operator-only domains** ([`retention.plan`](@/core/retention.md), `trash`, the `source` column of `usage.daily`,
   [`revocations`](@/core/digest-revocations.md)) require the matching operator scope. Absent it, the plan is refused.
   Matching `/+revocations` today, the refusal is a `404`-style non-disclosure, so an unauthorized caller cannot even
   confirm the domain exists for a repository.
1. **Repository-scoped domains** get a mandatory injected predicate: `repository in {authorized repositories}`
   intersected with the caller's project globs. A caller holding `OperatorRead` or `AnalyticsRead` gets the
   operator-wide set; a repository token gets exactly its own repository. This mirrors the merged analytics rule. Naming
   a `repository` scopes to a route the caller can read; omitting it runs operator-wide behind the analytics grant.
1. The injected predicate is ANDed at the **root** of the plan, below any user predicate, and applies before `order`,
   `limit`, and cursor generation. Counts and pagination run over authorized rows only, with no post-limit filtering and
   no leaked totals.

### Field-level authorization

Row-level scope decides which rows; `response_security` decides which columns. Each column's `FieldClassification` is
checked against the caller's `ResponseAuthorization`. A caller below a column's classification cannot `select` it
(validation error) and cannot `order by` it; if a preset or `select *` would include it, `filter_fields` drops it in the
single bounded pass that already exists for [#456](https://github.com/tox-dev/peryx/issues/456). So
`usage.daily.source`, a property of the server's upstream routing rather than the repository, stays invisible to a
repository-scoped credential, as it is on the merged endpoint.

### One semantics, auditable denials

There is no admin-sees-everything divergence: a broader grant only widens the injected scope set, it does not change
what a query means. Denials record a bounded security event through `peryx-events`'s security channel. Following
[#456](https://github.com/tox-dev/peryx/issues/456), the event and the error body exclude the query text and any
parameter values, so a denial never echoes back a secret the caller embedded in a predicate.

## Resource bounds

All of these are defaults, not optional configuration a fresh install lacks.

- **Static cost estimate before execution.** From the validated AST: a per-domain base cost plus penalties for
  predicates on unindexed columns, `order by` on an unindexed column, high `limit`, and aggregation width. Over budget
  returns `400` with a problem-detail naming the expensive clause. This is where the AQL advice ("filter on indexed
  fields first", "no unfiltered `find({})`") becomes an enforced rule: a query with no indexed leading predicate on a
  large domain is rejected, not run.
- **No leading-wildcard match.** `starts_with` is a prefix; there is no `contains`. The full-scan shape AQL warns about
  stays inexpressible.
- **Hard result cap and cursor pagination.** One shared `Page<T>` / `Cursor` type replaces the three ad-hoc copies.
  Default `limit` 25, max 100 for interactive reads; the opaque cursor encodes the domain, the resolved scope hash, and
  the scan position, and is rejected if the scope hash no longer matches, so a cursor cannot be replayed under a
  different grant.
- **Streaming export for large plans.** Retention and trash exports ([#482](https://github.com/tox-dev/peryx/issues/482)
  already asks for this) use a `format: "jsonl"` mode that streams JSON Lines with backpressure and a documented
  resumable boundary, bypassing the interactive result cap without holding an unbounded result in memory.
- **Timeouts and concurrency.** A wall-clock timeout per query returns `408`; a per-caller and per-repository
  concurrency cap returns `429`. Both mirror AQL's operational controls, on by default.
- **Query text size cap**, so the parser never receives an unbounded string.

## Architecture

A new crate, `peryx-pql`, owns the language and nothing else. It depends on `peryx-core` (the `Ecosystem` axis, neutral
DTOs) and `peryx-identity` (scopes), but not on `peryx-http` or any ecosystem crate. The ecosystem seam is a trait
defined here and implemented outward, the same inversion
[`peryx-search`'s `PackageIndexer` and `peryx-driver`'s `EcosystemDriver`](@/contributing/architecture.md) already use.

```
                 POST /+query  (peryx-http router.rs, before the catch-all)
                        │
                        ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  peryx-pql                                                     │
   │                                                                │
   │  parse  ──▶ Ast ──▶ validate+authorize ──▶ Plan ──▶ execute   │
   │   │                     │                    │         │       │
   │   │                     │                    │         │       │
   │  grammar            catalog +            cost-bounded  DataSource
   │  (small DSL)        QueryScope           physical plan  trait  │
   │                     injection                                  │
   └───────────────────────────────────────────────────┬──────────┘
                                                        │
                        ┌───────────────────────────────┴───────────────┐
                        ▼                                                ▼
              neutral DataSources                          ecosystem DataSources
        (MetaStore, Metrics, retention engine)        (per-ecosystem, via a trait each
                                                        ecosystem crate implements)
```

Pipeline stages:

1. **parse:** the DSL grammar to an `Ast`. Bounded input, no backtracking blow-up; a hand-written or `winnow`-style
   parser, no runtime code generation.
1. **catalog:** the static registry of domains, giving each one's columns, types, `FieldClassification`, index flags,
   and required authorization. Neutral domains register themselves; ecosystem domains register through the seam. This
   registry is what makes validation and field classification static rather than per-row.
1. **validate + authorize:** resolve the domain, type-check the predicate, reject unknown or over-classified fields,
   resolve the caller's `QueryScope`, and inject the mandatory scope predicate. Output is an authorized logical plan.
1. **plan:** lower to a cost-bounded physical plan, pushing filters, order, limit, and aggregates into the `DataSource`
   so the store does the work, not the executor. Estimate cost; reject if over budget.
1. **execute:** run the plan over the `DataSource`, stream rows, apply `filter_fields` for column-level visibility, and
   page or stream the result.

### The data-source seam

```rust
// in peryx-pql: neutral, ecosystem-agnostic
pub trait DataSource: Send + Sync {
    fn domains(&self) -> &[DomainSchema];
    fn scan(&self, plan: &PhysicalPlan, scope: &QueryScope) -> Result<RowStream, PqlError>;
}

// implemented in each ecosystem crate (peryx-ecosystem-pypi, peryx-ecosystem-oci),
// registered on the DriverSet the way EcosystemDriver already is
pub trait EcosystemDataSource: DataSource {
    fn ecosystem(&self) -> Ecosystem;
}
```

Neutral domains get one `DataSource` backed by `MetaStore`, `Metrics`, and the retention engine. Each ecosystem crate
provides an `EcosystemDataSource` reaching its `DRIVER_KV`-owned records through the existing `EcosystemDriver` hooks
(`project_names`, `project_page`, `browse_project`, `manifest_view`, `cache_record_counts`, ...). The executor never
interprets an ecosystem's bytes; it asks the ecosystem's data source for typed rows. The neutral core stays free of
PyPI/OCI branches.

### Wiring `POST /+query`

The route registers in `crates/peryx-http/src/router.rs` **before** the trailing catch-all `/{*path}` and before the OCI
absolute `/v2/` mount, since the catch-all consumes everything else. It is read-only, so `reject_replica_mutation` must
learn to let it through on a [read replica](@/core/high-availability.md), alongside GET/HEAD/OPTIONS and
driver-classified service POSTs. A companion `GET /+query/schema` returns the catalog the caller is authorized to see,
meaning the domains, columns, and types visible under their scope, so the surface is introspectable without leaking
operator-only domains.

### Error model

`PqlError` is a closed enum mapped to RFC 9457 problem details (already the house style in
[#460](https://github.com/tox-dev/peryx/issues/460) / [#482](https://github.com/tox-dev/peryx/issues/482)): `ParseError`
and `ValidationError` (carrying the offending clause, never the parameter values) return `400`; `Unauthorized` returns a
`404`-style non-disclosure for operator-only domains and `403` where the domain is known to the caller; `CostExceeded`
returns `400` naming the expensive clause; `Timeout` returns `408`; `TooManyConcurrent` returns `429`;
`StorageUnavailable` returns `503`. Authenticated responses carry RFC 9111 private / `no-store` cache controls for
operator-classified data, reusing the [#456](https://github.com/tox-dev/peryx/issues/456) helpers.

## Migration plan

Sequenced so nothing merges before its prerequisites and no existing endpoint regresses. Phase 0 is the current
in-flight work; PQL starts only after it lands.

**Phase 0: land the typed endpoints and factor the shared pieces (in flight).** Merge
[#460](https://github.com/tox-dev/peryx/issues/460) (trash inspection) and
[#482](https://github.com/tox-dev/peryx/issues/482) (retention API); [#645](https://github.com/tox-dev/peryx/issues/645)
(analytics) is done. Independently, extract the duplicated `{ items, next_cursor }` pagination into one `Page<T>` /
`Cursor` type and confirm `response_security::FieldClassification` covers every column the domains above expose. These
help on their own and are the substrate PQL reuses.

**Phase 1: introduce `peryx-pql` and `POST /+query`, neutral domains first.** Stand up the crate and the endpoint
read-only over the neutral domains (`usage.*`, `policy.decisions`, `retention.plan`, `trash`, `quota`, `revocations`),
each behind the scope it already uses. No ecosystem domains yet. The existing typed endpoints stay untouched. This phase
is where the authorization injection, cost model, and pagination earn their tests.

**Phase 2: ecosystem domains through the seam.** Add the `EcosystemDataSource` trait and implement it in
`peryx-ecosystem-pypi` and `peryx-ecosystem-oci` for `packages` / `versions` / `files` (and PyPI `provenance`). Register
them on the `DriverSet`. Now `POST /+query` spans neutral and ecosystem data with no core branching.

**Phase 3: reimplement the typed endpoints as presets over PQL.** Rewrite each existing handler to build a fixed,
internal PQL plan and run it through the one executor, keeping its stable request and response shape.
`/+policy/decisions` becomes a preset that builds the `from policy.decisions ...` plan; `/+analytics/top-packages`
becomes a `usage.*` aggregate preset; retention and trash inspection become presets. One authorization path, one cost
model, one paginator underneath; the familiar URLs and shapes on top. No endpoint is removed.

**Phase 4: deprecate only the duplicated internals.** Delete the ad-hoc cursor and per-endpoint authorization code once
every preset routes through the executor. The public endpoints stay as documented convenience presets indefinitely.

## Open questions and risks

- **Aggregation scope.** Usage analytics needs sums; how far does declared aggregation go before it becomes a second,
  harder-to-bound language? The proposal caps it at `sum` / `count` / `min` / `max` over declared numeric columns with
  declared group keys. Is that enough for the dashboard, or does the timeline view need windowing the language would
  have to grow?
- **Cross-domain correlation.** v1 forbids joins. Operators will want "trash for projects a retention plan marks
  remove". Is the answer a second query, or a bounded, declared join between two domains on a shared key later, and can
  that be cost-bounded without reintroducing the AQL join trap?
- **`DRIVER_KV` scan cost.** Ecosystem domains sit behind opaque key-ordered blobs. Some predicates have no cheap index
  there, so the cost model must know each ecosystem source's real indexability or it will admit expensive scans. This
  needs per-source cost metadata, not a neutral guess.
- **Wire form.** Textual DSL against a JSON AST on the wire. The proposal is text with out-of-band parameter binding
  (never spliced); a JSON AST is more machine-friendly but harder to write by hand. Support both, or pick one?
- **Cursor stability under grant change.** Binding the scope hash into the cursor rejects replay under a changed grant,
  which is safe but surfaces as a mid-pagination error when a grant is edited. Is that the right trade, or should a
  changed grant re-scope in place?
- **Replica and cache semantics.** A read replica can serve `POST /+query`, but operator-classified results must stay
  `no-store`; confirm the replica path honors the same field classification.
- **Scope creep into a write surface.** The strongest risk is future pressure to add `delete` "just for trash". The
  non-goal is load-bearing: writes stay in their own audited, typed endpoints, never in the query language.
