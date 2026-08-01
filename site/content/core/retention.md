+++
title = "Retention plans"
description = "Evaluate an index's retention rules into a deterministic, side-effect-free removal plan."
weight = 10
+++

A retention plan names which artifacts an index keeps and which become eligible for removal. Evaluation reads one
metadata snapshot, applies the configured rules, and returns an ordered decision per artifact. It changes no metadata
and touches no blob. A plan is a preview: a local administrator inspects it over the
[CLI](#preview-and-export-from-the-cli) or the [HTTP API](#preview-and-export-over-http), both driven by one query, so
the same request yields the same ordered candidates whichever way it is asked. Applying a plan and reclaiming bytes come
later and consume this planner and its preview without rebuilding either.

The subject is an index's hosted upload records. Cache maintenance evicts cached upstream pages, while blob collection
reclaims unreferenced blobs; neither is a retention decision.

## Rules

A policy holds two ordered rule groups. `keep` rules protect an artifact; `expire` rules mark it for removal. A rule
matches one dimension:

- `age`: an artifact published at least `older_than_seconds` before now.
- `source`: an artifact routed from the named source.
- `project-prefix`: an artifact whose project name begins with `prefix`.
- `keep-latest`: an artifact among the newest `count` versions of its project.
- `cached`: a cached artifact.
- `trash`: a soft-deleted artifact.
- `orphan`: an artifact no live reference reaches.

The same rule protects in `keep` and removes in `expire`; the group gives it meaning. An `age` rule matches nothing when
the artifact carries no publish time or the evaluation supplies no clock, so the planner ages only what it can date.

Rules load from configuration as a tagged list:

```toml
keep = [
  { selector = "keep-latest", count = 10 },
  { selector = "age", older_than_seconds = 2592000 },
]
expire = [
  { selector = "trash" },
  { selector = "project-prefix", prefix = "scratch-" },
]
```

## Precedence

A `keep` rule always wins over an `expire` rule, the precedence
[Google Artifact Registry cleanup policies](https://cloud.google.com/artifact-registry/docs/repositories/cleanup-policy)
define. The planner evaluates each artifact in order: the first matching `keep` rule retains it; otherwise the first
matching `expire` rule removes it; otherwise it is retained with no rule. Each decision names the rule that decided it,
so an operator reads why an artifact survived a policy that could have removed it.

## Version ordering

Versions rank newest first within a project. Python versions order under [PEP 440](https://peps.python.org/pep-0440/),
so `2.0` outranks `2.0rc1` and `2.0+local` outranks `2.0`. Two spellings of one release (`1.0` and `1.0.0`) collapse to
one rank, so `keep-latest` counts releases, not filenames. A version that is not valid PEP 440 ranks after every valid
one, ordered by its string, so a legacy spelling still gets a stable, documented position rather than an arbitrary one.

`keep-latest` reads this rank: `count = 10` protects the ten newest releases and their files.

## Output

Evaluation streams one decision per artifact, ordered newest release first, then by filename, then by digest. The order
is total, so repeating an evaluation over the same snapshot and policy produces byte-identical output.

Each decision records the artifact's project, version, filename, digest, storage class, and logical visibility (active,
yanked, or hidden). A removal decision adds:

- `outcome`: `remove`, against `retain` for a kept artifact.
- `rule`: the rule that decided it.
- `bytes`: the artifact's estimated physical size, the capacity a removal would reclaim.
- `retained_alternatives`: the project's surviving versions, so a reader sees what a removal leaves in place.

## Snapshot and policy identity

A plan carries the identity of both inputs it read, so a later apply step can reject a plan built against stale state:

- `policy_version`: a stable content hash of the compiled rules. Equal rules produce an equal version, and every rule
  contributes a typed, length-framed value to the hash input.
- `frontier`: the metadata generation the scan read, combining the repository serial, the catalog generation, and the
  policy generation. It mirrors the store's policy-input generation.

## Side-effect-free contract

Evaluation opens read transactions only. It reads indexed metadata and digest references and never enumerates backend
blobs. It groups one project at a time and streams that project's decisions before reading the next, so a large index
never holds as one in-memory plan. A dropped connection, a cancelled request, or a crash stops the scan mid-pass, and
the store keeps the state it already held, because the scan wrote none.

## Preview and export over HTTP

Two administrator endpoints expose a plan. Both take a JSON body naming the repository and the rules, and both require a
local administrator: a caller without the administration-read role receives `404`, so an unauthorized caller cannot
infer a repository's contents from the shape of the failure. Neither endpoint changes any metadata.

`POST /+retention/plan` returns one ordered page:

```json
{
  "repository": "root/pypi",
  "keep": [
    {
      "selector": "keep-latest",
      "count": 3
    }
  ],
  "expire": [
    {
      "selector": "age",
      "older_than_seconds": 7776000
    }
  ],
  "limit": 100
}
```

The response carries the plan `summary` (its `policy_version` and `frontier`), the page's `candidates`, and a
`next_cursor` when more remain:

```json
{
  "summary": {
    "policy_version": 42,
    "frontier": {
      "repository": 7,
      "catalog": 3,
      "policy": 2
    }
  },
  "candidates": [
    {
      "project": "example",
      "version": "1.0",
      "artifact": "example-1.0-py3-none-any.whl",
      "digest": "sha256:0123\u2026",
      "class": "hosted",
      "visibility": "active",
      "bytes": 20480,
      "outcome": "remove",
      "rule": "age",
      "retained_alternatives": [
        "2.0"
      ]
    }
  ],
  "next_cursor": null
}
```

`POST /+retention/export` streams the whole plan as [JSON Lines](https://jsonlines.org/) under `application/x-ndjson`.
The first line is the `summary`; each following line is one candidate. The response `ETag` is the plan identity, so a
later apply can present it with `If-Match` to catch a repository that changed under the preview.

## Preview and export from the CLI

`peryx retention dry-run` prints one page of tab-separated candidates followed by a `summary` row and, when a page
fills, a `next-cursor` row. `peryx retention export` streams the plan as JSON Lines, the identity first, matching the
HTTP export byte for byte. Both read the local store directly and load rules from a `--rules` TOML file in the
[configuration form](#rules); without one, the policy retains everything.

```console
$ peryx retention dry-run --index root/pypi --rules retention.toml --limit 100
$ peryx retention export --index root/pypi --rules retention.toml > plan.jsonl
```

## Pagination and resumable export

A cursor is an opaque token that folds the resume offset together with the plan identity. Presenting a page's
`next_cursor` back both continues where the last page ended and rejects the resume when the repository has changed since
the cursor was issued: a plan built against a shifted frontier returns `409 Conflict` from HTTP, or a stale-cursor error
from the CLI, rather than splicing rows from two snapshots. An export restarts from its documented boundary, the last
candidate a reader consumed, by passing that page's cursor; the plan is deterministic, so the same snapshot and policy
reproduce the same ordered candidates. The stream is unique to one snapshot, so HTTP byte ranges do not apply and the
export advertises `Accept-Ranges: none`.

## Memory and concurrency limits

A page holds at most its `limit` candidates; an export holds one candidate at a time and backpressures the scan when a
reader falls behind, so neither materializes an unbounded plan. Each repository admits a small, fixed number of
concurrent plans; a request beyond that bound receives `429 Too Many Requests`, so one repository's full-scan previews
cannot starve the rest.
