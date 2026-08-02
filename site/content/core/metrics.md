+++
title = "Prometheus metrics"
description = "Every series /metrics exposes, the labels it carries, when it appears, and the alerts worth building on it."
weight = 8
+++

`GET /metrics` renders the [Prometheus](https://prometheus.io/) text exposition (`text/plain; version=0.0.4`) of every
counter and gauge peryx keeps. The counters live in memory and reset on restart, so this endpoint is where you turn them
into durable time series. It is the alerting surface; [the usage pages](@/core/monitor.md) are the drill-down when you
need to know which repository or package moved a number.

Every label here is bounded. Series carry `ecosystem`, `role`, `class`, `kind`, `outcome`, or `reason`, and nothing
else: no repository, project, file, user, path, error, credential, token, or URL ever becomes a metric name or a label
value. Cardinality therefore stays flat as the store grows, but a metric can never tell you *which* repository is
failing. Use [`/+stats`](@/core/monitor.md#query-package-usage) for that. Peryx counts one download when the last
expected byte leaves the body, so the [counting rule](@/core/monitor.md#counting-rule) that governs `/+stats` governs
`peryx_artifacts_served_total` too.

## Scrape it

The route is always on, unauthenticated, and unaffected by index access control. Keep it reachable only from your
monitoring network; put it behind the same ingress ACL as `/+status`. The absent labels mean a leaked scrape reveals
volumes and error rates, not who fetched what, but it is still operational data.

```yaml
scrape_configs:
  - job_name: peryx
    metrics_path: /metrics
    static_configs:
      - targets: [peryx-1:4433, peryx-2:4433]
```

## The label vocabulary

Values from every configured repository fold into a small fixed set before rendering, so a series exists only for a
label combination some configured index produces.

| Label       | Values                                               | On                          |
| ----------- | ---------------------------------------------------- | --------------------------- |
| `ecosystem` | `pypi`, `oci`                                        | Serving and quota series    |
| `role`      | `cached`, `hosted`, `virtual`                        | Serving and quota series    |
| `class`     | `listing`, `metadata`, `artifact`, `upload`, `admin` | Rate-limiter series         |
| `kind`      | `cache_maintenance`, `catalog_sync`                  | Job series                  |
| `outcome`   | `succeeded`, `failed`, `cancelled`                   | `peryx_jobs_finished_total` |
| `reason`    | `conflict`, `queue_full`                             | `peryx_jobs_rejected_total` |

A serving family is scoped to the role that produces it: a cached-only family emits `role="cached"` series and no
others. It emits one series per ecosystem that has an index in that role, so two cached indexes on different ecosystems
give two series and let a query split them without ever naming a repository.

```text
peryx_artifacts_served_total{ecosystem="pypi",role="virtual"}
peryx_artifacts_served_total{ecosystem="oci",role="virtual"}
```

## Request and limiter series

Process-wide totals with no ecosystem split.

| Series                                   | Type    | Labels  | Meaning                                              |
| ---------------------------------------- | ------- | ------- | ---------------------------------------------------- |
| `peryx_requests_total`                   | counter |         | HTTP requests served.                                |
| `peryx_rate_limit_allowed_total`         | counter | `class` | Requests the hosted rate limiter allowed.            |
| `peryx_rate_limit_denied_total`          | counter | `class` | Requests the hosted rate limiter denied with `429`.  |
| `peryx_upstream_rate_limit_denied_total` | counter |         | Cache fills refused by the upstream concurrency cap. |
| `peryx_upstream_inflight_fetches`        | gauge   |         | Upstream fetches holding a concurrency slot.         |

The `class` labels follow the limiter's route classes, so a browsing client (`listing`, `metadata`) and a publishing
client (`upload`) are budgeted apart. Tune the budgets with [`[rate_limit]`](@/core/configuration.md#rate-limit).

## Serving series

Emitted for every configured `ecosystem`/`role` pair.

| Series                               | Type    | Roles      | Meaning                                    |
| ------------------------------------ | ------- | ---------- | ------------------------------------------ |
| `peryx_pages_served_total`           | counter | all        | Index listings served.                     |
| `peryx_artifacts_served_total`       | counter | all        | Artifacts served.                          |
| `peryx_artifacts_served_bytes_total` | counter | all        | Artifact bytes served.                     |
| `peryx_artifacts_rejected_total`     | counter | all        | Downloads that failed digest verification. |
| `peryx_metadata_served_total`        | counter | all (PyPI) | PEP 658 `.metadata` siblings served.       |
| `peryx_provenance_served_total`      | counter | all (PyPI) | PEP 740 provenance objects served.         |

A rising `peryx_metadata_served_total` proves resolvers take the metadata fast path instead of downloading whole
distributions to read their dependencies. `peryx_artifacts_rejected_total` above zero means bytes did not match the
advertised digest: either an upstream served corruption or something rewrote them in transit. Those bytes are never
cached and never counted as a download.

## Cache and upstream health

These families emit only for `role="cached"`, one series per cached ecosystem.

| Series                               | Type    | Meaning                                                      |
| ------------------------------------ | ------- | ------------------------------------------------------------ |
| `peryx_upstream_refreshes_total`     | counter | Revalidations against upstream, on demand or from the sweep. |
| `peryx_upstream_pages_changed_total` | counter | Revalidations that found upstream content had changed.       |
| `peryx_stale_pages_served_total`     | counter | Pages served from cache because upstream was unreachable.    |
| `peryx_upstream_errors_total`        | counter | Upstream failures with nothing cached to fall back to.       |
| `peryx_catalog_syncs_total`          | counter | Remote root-catalog synchronizations attempted.              |
| `peryx_catalog_published_total`      | counter | Root-catalog generations published from a sync.              |
| `peryx_catalog_not_modified_total`   | counter | Root-catalog revalidations the upstream answered unchanged.  |
| `peryx_catalog_errors_total`         | counter | Failed root-catalog synchronizations.                        |
| `peryx_catalog_projects`             | gauge   | Projects in the current published root catalog.              |

A steady `peryx_upstream_refreshes_total` with a flat `peryx_upstream_pages_changed_total` is the normal idle state:
peryx keeps checking and upstream keeps saying nothing moved. `peryx_stale_pages_served_total` rising is the signature
of an upstream outage that clients did not feel, and `peryx_upstream_errors_total` rising is the outage they did.

The `catalog_*` families track the [`catalog_sync` job](@/core/configuration.md#schedules) that keeps a cached index's
project list warm. They advance only for cached PyPI indexes, because the root catalog is a PyPI concept; a cached OCI
index reports them as zero.

## Uploads and quota

Emitted only for `role="hosted"`.

| Series                            | Type    | Meaning                                                |
| --------------------------------- | ------- | ------------------------------------------------------ |
| `peryx_artifacts_uploaded_total`  | counter | Distributions uploaded to a hosted index.              |
| `peryx_pypi_quota_admitted_total` | counter | Hosted PyPI uploads admitted against a project quota.  |
| `peryx_pypi_quota_rejected_total` | counter | Hosted PyPI uploads refused by a project quota.        |
| `peryx_oci_quota_admitted_total`  | counter | Hosted OCI pushes admitted against a repository quota. |
| `peryx_oci_quota_rejected_total`  | counter | Hosted OCI pushes refused by a repository quota.       |

A rising rejection counter means publishers are hitting a wall. It does not say which repository; read the offending
quota with [`/+stats`](@/core/quotas.md#reading-quota-status) once an alert fires.

## Job scheduler

Present when the node runs [`[jobs]` in `local` mode](@/core/configuration.md#jobs), the default. A
[read replica](@/core/high-availability.md) runs no scheduler, so these series are absent there. Labels stay
low-cardinality: the static `kind` and a bounded `outcome` or `reason`, never a repository.

| Series                      | Type    | Labels            | Meaning                             |
| --------------------------- | ------- | ----------------- | ----------------------------------- |
| `peryx_jobs_started_total`  | counter | `kind`            | Job runs started.                   |
| `peryx_jobs_finished_total` | counter | `kind`, `outcome` | Job runs finished, by outcome.      |
| `peryx_jobs_rejected_total` | counter | `kind`, `reason`  | Submissions refused before running. |
| `peryx_jobs_running`        | gauge   | `kind`            | Job runs in flight.                 |

A `reason="conflict"` rejection is the scheduler skipping a tick because the previous run of that kind was still going,
which is the signal to lengthen the interval or shorten the run. A `reason="queue_full"` rejection means work arrived
faster than the worker drained it.

## Replication

Present only on a [read replica](@/core/high-availability.md). The last two series appear once the replica has observed
a serial from its primary.

| Series                                | Type    | Meaning                                                                 |
| ------------------------------------- | ------- | ----------------------------------------------------------------------- |
| `peryx_replication_caught_up`         | gauge   | `1` when the replica has reached the observed primary serial, else `0`. |
| `peryx_replication_serial`            | gauge   | Last serial the replica committed.                                      |
| `peryx_replication_changes_total`     | counter | Metadata changes the replica committed.                                 |
| `peryx_replication_blobs_total`       | counter | Blobs the replica fetched.                                              |
| `peryx_replication_sync_errors_total` | counter | Replica synchronization failures.                                       |
| `peryx_replication_primary_serial`    | gauge   | Latest serial reported by the primary.                                  |
| `peryx_replication_lag`               | gauge   | Serial distance between primary and replica.                            |

`peryx_replication_lag` is the number to alert on: it is how far behind the replica is in committed serials, not
seconds, so read it against your write rate. It pairs with the
[`/+ready`](@/core/high-availability.md#load-balancer-probes) probe that already gates the replica out of a read pool
while it catches up.

## Alerts worth building

The queries below assume the `job="peryx"` scrape above. They are starting points; set thresholds to your traffic.

Upstream is down and clients are being served stale copies:

```promql
sum by (ecosystem) (rate(peryx_stale_pages_served_total[5m])) > 0
```

Clients saw a hard upstream failure with nothing cached:

```promql
sum by (ecosystem) (rate(peryx_upstream_errors_total[5m])) > 0
```

Corrupt or rewritten downloads reached a client:

```promql
sum(rate(peryx_artifacts_rejected_total[15m])) > 0
```

A background job is failing rather than succeeding:

```promql
sum by (kind) (rate(peryx_jobs_finished_total{outcome="failed"}[15m])) > 0
```

Publishers are being turned away by quota:

```promql
sum by (ecosystem) (rate(peryx_pypi_quota_rejected_total[15m]))
  + sum by (ecosystem) (rate(peryx_oci_quota_rejected_total[15m])) > 0
```

A replica has fallen too far behind to promote without risking divergence:

```promql
peryx_replication_lag > 1000
```

## Related

- Reading the counters and the one durable daily aggregate: [monitor usage and cache health](@/core/monitor.md)
- Repository and package drill-down instead of aggregates: [`/+stats`](@/core/monitor.md#query-package-usage)
- The route list these series sit beside: [HTTP endpoints](@/ecosystems/pypi/reference/endpoints.md#metrics)
- Replica lag and failover: [high availability](@/core/high-availability.md)
