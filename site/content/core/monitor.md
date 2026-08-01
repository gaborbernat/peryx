+++
title = "Monitor usage and cache health"
description = "Read the usage counters, drill down to files, watch for upstream changes, and scrape Prometheus."
weight = 8
+++

peryx counts everything it serves, off the request path. Four surfaces show the same numbers: the dashboard, the admin
status page, the `/+stats` JSON endpoint, and [Prometheus](https://prometheus.io/) `/metrics`.

## Drill down from the dashboard

Each index card on the dashboard shows its pages, downloads, and bytes served, with a `usage` link. The link opens the
drill-down: totals for the index and a table of its projects, each project linking to its per-file counters. The same
pages live at `/stats`, `/stats?index={route}`, and `/stats?index={route}&project={name}`.

{{ screen(alt="The usage drill-down for one index: totals up top, busiest projects first", name="stats-index") }}

{{ screen(alt="One project drilled to file level: downloads, bytes, and metadata hits per artifact", name="stats-project") }}

## Query the counters

```shell
curl -s http://127.0.0.1:4433/+stats | jq
curl -s 'http://127.0.0.1:4433/+stats?index=root/pypi' | jq '.projects | to_entries | sort_by(-.value.downloads)[:5]'
curl -s 'http://127.0.0.1:4433/+stats?index=root/pypi&project=numpy' | jq .files
```

These counters live in memory and reset on restart; for durable time series, scrape `/metrics`, and see
[daily version and source usage](#daily-version-and-source-usage) for the one aggregate that survives a restart.
Prometheus sums repositories by the bounded `ecosystem` and `role` labels. A query can split PyPI from OCI traffic
without storing repository names:

```text
peryx_artifacts_served_total{ecosystem="pypi",role="virtual"}
peryx_artifacts_served_total{ecosystem="oci",role="virtual"}
```

Repository, project, file, user, path, error, credential, token, and URL values are excluded from metric names and
labels. Use `/+stats` for repository and package drill-down. The
[endpoint reference](@/ecosystems/pypi/reference/endpoints.md#metrics-compatibility) lists renamed series and
replacement queries.

## Daily version and source usage

Alongside the in-memory counters, peryx keeps one durable aggregate that survives a restart: daily download totals
broken down by version and routed source. Each successful artifact response folds into a bucket keyed by repository,
project, distribution version, routed source, and the UTC day it landed on, holding that bucket's download count and
byte total. The fold, its persistence, and retention all run on the aggregator thread, so nothing here touches the
request path.

The dimensions stay narrow. peryx parses `version` from the artifact filename, so a PyPI wheel or sdist carries its
release and a content-addressed OCI layer carries none. `source` is the named upstream a cache miss routed to; a
response served from the local store carries no source, because it routed to no upstream. A bucket never holds a
client's identity, address, or credential, and never an unbounded label. The label set stays bounded by the configured
repositories, their projects and versions, the configured sources, and the retention window.

### Counting rule

One fully delivered response is one download, counted once when its last expected byte leaves the body:

| Response                                    | Counted                  |
| ------------------------------------------- | ------------------------ |
| Full `200` delivered to the last byte       | Once                     |
| `206` range delivered to the last byte      | Once, for the bytes sent |
| Truncated, cancelled, or disconnected       | No                       |
| `4xx`/`5xx`, unauthorized, or policy-denied | No                       |

A range request counts once and records only the bytes it transmitted, so a resumed download is not double-counted
against the whole artifact. A transfer that fails its digest is rejected, not counted.

### Retention

`usage_retention_days` bounds how many days of buckets to keep; leave it unset to keep them without limit. Expiry drops
whole expired days on the aggregator thread and never touches a retained day's totals, so tightening the window only
reclaims durable storage. Existing durable snapshots that predate this aggregate, carry missing dimensions, or fail to
parse rebuild from zero rather than blocking startup.

## Query package usage

The `/+analytics/*` endpoints read the [daily aggregate](#daily-version-and-source-usage) back as authorized queries.
Five views share one set of parameters:

| View                       | Rows keyed by                | Answers                                  |
| -------------------------- | ---------------------------- | ---------------------------------------- |
| `/+analytics/top-packages` | repository, project          | Which projects are downloaded most       |
| `/+analytics/versions`     | repository, project, version | Which releases carry the traffic         |
| `/+analytics/sources`      | repository, project, source  | Which upstream a cache miss routed to    |
| `/+analytics/unused`       | repository, project          | Which projects went idle over the window |
| `/+analytics/timeline`     | UTC day                      | How downloads move day to day            |

Name a `repository` (an index route) you can read to scope the query to it; omit it for an operator-wide query across
every repository, which needs the operator analytics grant. The `sources` view is operator-only: a repository-scoped
credential cannot inspect which upstream served a project, since that is a property of the server's routing, not the
repository. A repository's legacy upload token reaches its own repository when presented with the `__token__` username
and a read grant.

```shell
# An operator's top ten across all repositories, last 30 days
curl -s -u admin:"$PERYX_ADMIN_PASSWORD" 'http://127.0.0.1:4433/+analytics/top-packages?limit=10' | jq

# One repository's release breakdown over an explicit range
curl -s -u __token__:"$UPLOAD_TOKEN" \
  'http://127.0.0.1:4433/+analytics/versions?repository=root/pypi&from=1704067200&to=1706659200' | jq

# Walk the daily series page by page
curl -s -u admin:"$PERYX_ADMIN_PASSWORD" 'http://127.0.0.1:4433/+analytics/timeline?repository=root/pypi&limit=50' | jq
```

Every response carries its rows, the resolved `interval`, and a `next_cursor`:

```json
{
  "packages": [
    {
      "repository": "root/pypi",
      "project": "pandas",
      "downloads": 42,
      "bytes": 64733247
    }
  ],
  "interval": {
    "from_day": 19722,
    "to_day": 19752,
    "from_unix": 1703980800,
    "to_unix": 1706659200,
    "retained_from_day": 19387,
    "window_clamped_to_retention": false
  },
  "next_cursor": null
}
```

`from` and `to` are Unix seconds, each floored to its UTC day. Leave them out for a trailing 30 days ending today; a
window never runs past today and spans at most 366 days. The `interval` echoes the resolved day bounds so a caller reads
the exact boundaries the totals cover rather than inferring them. Rows are deterministically ordered by downloads, then
bytes, then identity, so ties never reshuffle between pages. Page with `limit` (1 through 100, default 25) and `cursor`:
pass a response's `next_cursor` back as `cursor` for the next page, and stop when it is `null`.

`retained_from_day` is the retention floor, absent under unbounded retention. When a query asks for data older than that
floor, peryx clamps the start and sets `window_clamped_to_retention` to `true`. This is what tells an idle project apart
from an aged-out one: an `unused` row reports `lifetime_downloads`, so a project with lifetime downloads but none in the
window was genuinely idle over it, while `window_clamped_to_retention` warns that anything before the floor was pruned
rather than quiet. Version and source rows report `null` for a distribution the ecosystem gave no version and for bytes
served from the local store. No response ever holds a client's identity, address, or credential; the dimensions stay as
bounded as the aggregate they read.

## Query repository quotas

The `/+quota` endpoints read the durable [quota counters](@/core/quotas.md) back without naming an artifact.
`GET /+quota` lists every repository's committed and reserved use against its configured limits, for a local
administrator; `GET /+quota/repository?repository=<route>` returns one repository for a caller who can read it. Both
report the `remaining` headroom, absent when a counter is unlimited, and mark the response private so a shared cache
never holds one caller's view for another.

```shell
# Every repository's quota, first page
curl -s -u admin:"$PERYX_ADMIN_PASSWORD" 'http://127.0.0.1:4433/+quota?limit=25' | jq

# One repository a token can read
curl -s -u __token__:"$UPLOAD_TOKEN" 'http://127.0.0.1:4433/+quota/repository?repository=root/pypi' | jq
```

Repository and project names stay out of the Prometheus metrics: the quota counters live in the durable store the
`/+quota` reads expose, and the enforcement families (`peryx_pypi_quota_*`, `quota_admitted`, `quota_rejected`) carry
the hosted role with no repository or project label, keeping metric cardinality bounded.

## Check operational status

`/admin/status` combines `GET /+status?details=admin` with top-level `GET /+stats`. It shows the configured index
topology next to the same cache-health counters listed below, plus observed project counts, upload counts, recent
uploads, cached index URLs, and redacted token/authentication state. The page does not fetch upstreams or read artifacts
while it renders.

The top-level `blob_storage` object names the selected backend. Its fields report durability and a support level for
each operation, including local staging. The backend implements `native` operations; peryx composes `emulated`
operations from lower-level calls. `filesystem` durability reports acknowledgment from the configured filesystem, whose
mount determines crash and replication guarantees. `health.blob_store` reports the current reachability check.

## What the cache-health counters mean

| Counter           | Signal                                                                      |
| ----------------- | --------------------------------------------------------------------------- |
| `refreshes`       | Revalidations against upstream, on demand or from the background sweep      |
| `changed`         | Revalidations that found new upstream content; also logged at `info`        |
| `stale_served`    | Pages served from cache because upstream was down; rising means an outage   |
| `upstream_errors` | Upstream failures with nothing cached to fall back to; clients saw an error |
| `rejected`        | Downloads whose bytes did not match the advertised digest; never cached     |

A steady `refreshes` with zero `changed` is the normal idle state. `rejected` above zero deserves attention: either the
upstream served corrupt bytes or something rewrote them in transit.

## Inspect the disk cache

Use the cache CLI when you need the state on disk, not request counters. It works the same for either ecosystem: the
blob store is content-addressed and shared, and `--index` selects a PyPI or an OCI index by route.

```shell
peryx cache size --data-dir /var/lib/peryx
peryx cache list --data-dir /var/lib/peryx --stale
peryx cache list --data-dir /var/lib/peryx --index dockerhub
peryx cache fsck --data-dir /var/lib/peryx
```

`cache size` reports cached pages, stale pages, blob files, bytes, and metadata row counts. `cache list --stale` lists
the stale pages with age and freshness lifetime. `cache fsck` validates metadata row shapes and stream-hashes blob files
against their sha256 path.

Use two steps for project purge. First print the plan, then rerun with `--yes` if the row counts match what you expect.

```shell
peryx cache purge project --data-dir /var/lib/peryx --index pypi --project flask
peryx cache purge project --data-dir /var/lib/peryx --index pypi --project flask --yes
peryx cache purge orphaned-blobs --data-dir /var/lib/peryx
peryx cache purge orphaned-blobs --data-dir /var/lib/peryx --yes
```

Project purge removes metadata rows and leaves blobs in place. Orphaned-blob purge removes blob files that no metadata
row references.

## Related

- Where the counters come from: [architecture](@/core/architecture.md)
- The endpoint and counter reference: [HTTP endpoints](@/ecosystems/pypi/reference/endpoints.md)
- The counters in a browser: [the web UI](@/core/web-ui.md)
