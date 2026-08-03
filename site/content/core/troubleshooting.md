+++
title = "Troubleshooting"
description = "Map a symptom to its cause: read the log stream, the status probes, and the HTTP code, then reach for the CLI check that confirms it."
weight = 14
+++

peryx reports trouble in three places, and knowing which one to read saves most of the diagnosis. The **log stream**
carries every startup refusal and every served request; a **status probe** (`/+ready`, `/+status`) answers whether the
process is healthy right now; and the **HTTP status code** the client sees names the specific fault. This page walks the
symptoms an operator hits, from a server that will not start to an installer that cannot find a package, and points at
the [CLI check](@/core/cli.md) that confirms each one.

## peryx will not start

Configuration is validated before the socket binds, so a rejected config exits non-zero and prints the reason instead of
serving a broken topology. Precedence is `defaults < TOML file < environment < flags`
([configuration](@/core/configuration.md)), so a surprising value often comes from a `PERYX_*` variable or a flag
overriding the file you edited. The common refusals:

| Message                                                                     | Cause                                        | Fix                                                                     |
| --------------------------------------------------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------- |
| `log sink 'file' requires a log file path (--log-file or log.file)`         | `sink = "file"` with no path                 | Set `--log-file`/`log.file`, or pick another sink                       |
| `the journald log sink is only available on Linux`                          | `sink = "journald"` off Linux                | Use `stdout`, `file`, or `syslog`                                       |
| `` `[tls]` needs both `cert` and `key` ``                                   | Only one of the pair set                     | Provide both, or use `[acme]`; see [serve HTTPS](@/core/serve-https.md) |
| `` `[acme]` needs at least one domain ``                                    | ACME table without `domains`                 | Add the domains the certificate covers                                  |
| `` index needs one of `cached`, `hosted`, or `layers` ``                    | An `[[index]]` declares no role              | Give the index exactly one role                                         |
| `` `cached` and `[[index.upstream]]` are mutually exclusive ``              | Both upstream forms on one index             | Keep the shorthand or the table, not both                               |
| `secret file {path} holds no secret`                                        | A `_file` credential points at an empty file | Write the secret, or drop the reference                                 |
| `credential environment variable {var} is unset, empty, or not valid UTF-8` | A `_env` credential names a missing variable | Export the variable before starting peryx                               |

A credential that resolves from a `_file` or `_env` source stops startup when the source is missing rather than starting
without it, so a rolled secret fails loud instead of serving anonymously. Validate a config without binding by running
the inspection commands below; `peryx index list` loads and checks the same topology `serve` would.

## A second process cannot open the store

The metadata store takes an exclusive lock, and only one process may hold a `data_dir` at a time. A second `peryx serve`
on the same directory fails to open the store; run one server per data directory. On top of that lock, peryx enforces a
durable single-writer claim: a writer records its `writer_identity` in the store, and a differently named writer is
refused with `metadata store is claimed by writer {active}; refusing {requested}`. This is the guard that stops a
restored copy from starting as a second writer. Promotion is deliberate, not automatic — use `peryx writer promote`
during a planned failover, covered in [high availability](@/core/high-availability.md).

## Replicas and offline nodes answer 503

A replica (`read_only = true` or `PERYX_READ_ONLY=true`) serves reads and rejects every mutation with `503` and the body
`{"error":"read_only_replica","message":"this replica does not accept mutations"}`. A `503` from `/+ready` instead means
the node is not caught up yet; send write traffic only to the writer. See
[high availability](@/core/high-availability.md) for the replica and failover model.

Offline mode is a different `503`. An `offline` cached index never reaches upstream, so a request for something it has
not already cached returns `503` with `offline mode has no cached {target}`. Contrast this with an online index whose
upstream is unreachable and whose page is not cached: that returns `502` with
`upstream is unavailable and no cached page exists`. The distinction matters when you read logs — a `503` here is a
local policy (you chose offline), while a `502` is a real upstream fault you can retry.

## 401 versus 403: why a bad token looks anonymous

peryx runs one neutral access decision for every ecosystem ([authentication](@/core/authentication.md)). A request with
no credential the action accepts, or a credential that matches no token, is treated as **anonymous** and refused with
`401` and `WWW-Authenticate: Basic realm="peryx"`. A `403` means something narrower: a *recognized* token that lacks a
grant for this project and action. So a mistyped or revoked token produces a `401`, not a `403` — an invalid token is
exactly as privileged as no token. If a client that you expect to be authorized gets a `401`, suspect the credential
itself before the grant; if it gets a `403`, the token is valid but its glob or action set does not cover the request.

OCI clients negotiate differently. The registry challenges with `Bearer realm=…,service=…` and mints repository-scoped
tokens, so a `docker pull` against a restricted index first does the `/v2/token` handshake. A `401` there carries
`error="invalid_token"` (retry with fresh credentials) or, once a valid token lacks the scope,
`error="insufficient_scope"` (do not retry — fix the grant).

## Operator and admin endpoints hide themselves

The usage drill-down (`/+stats`) and the administration endpoints require an operator or administrator credential, not a
per-index token. Create the first administrator with
[`peryx bootstrap-administrator`](@/core/bootstrap-administrator.md) and authenticate with HTTP Basic, as
[monitor usage](@/core/monitor.md) shows. One surprise worth knowing: an *authenticated* caller who lacks the operator
grant receives `404`, not `403`. The denial deliberately looks like a missing route so a probe cannot confirm the
endpoint exists. If `/+stats` returns `404` for a user you believe is an operator, check the role grant rather than the
URL.

## pip or uv cannot find a package

An installer that reports no matching distribution is reading one of several distinct server responses; the HTTP code
and the log line tell them apart.

| Response                                              | Meaning                                                          | Where to look                                                                                  |
| ----------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `404` `project … was not found on index …`            | The project is not hosted here and the upstream has no such name | Check the index route and the upstream; run `peryx mirror plan` to see what a fetch would pull |
| `503` `offline mode has no cached …`                  | The index is offline and never cached this project               | Populate it while online, or drop `offline`                                                    |
| `502` `upstream is unavailable …`                     | The upstream is down and the page is not cached                  | Retry; inspect upstream health                                                                 |
| `403` `project … is {status}; downloads are disabled` | The project exists but is yanked or quarantined                  | Expected for [revoked content](@/core/digest-revocations.md); unyank if unintended             |

A normal, empty index page returns `200` — an installer finding zero files is not the same as a `404`, and the bodies
above let you separate a genuine miss from a policy decision in the request log.

## docker or podman says "unauthorized"

`unauthorized: authentication required` (body
`{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`) means the index is restricted and the
request carried no valid token; the client should follow the `WWW-Authenticate` challenge to `/v2/token`. If the
handshake itself fails, the token endpoint is explicit: `token authentication is not enabled` when no signing key is
configured, `requested service is not available` when the `service` parameter does not match the registry audience, and
`invalid credentials` when the Basic login matches no live token. A missing image is separate: a pull for an unknown
repository or tag returns `404` with `manifest unknown`, while an upstream fault during a pull-through returns `502`.
See [token authentication](@/ecosystems/oci/reference/token-auth.md) for the realm setup.

## Uploads rejected: quota, size, and rate limits

A hosted write path enforces [repository quotas](@/core/quotas.md) and per-file size limits before content becomes
discoverable. An upload that would push a project past its byte quota is refused with `403` and
`project size {total} would exceed limit {limit}`; a single file over the configured cap is refused with the same policy
shape and `file size {size} exceeds limit {limit}`. An archive that is too large or too deeply nested to inspect returns
`413`. When an upstream signals backpressure, peryx returns `429` with a `Retry-After` header and
`rate limit exceeded; retry after {n} seconds` — honor the header rather than retrying immediately. A quota configured
in audit mode records the violation instead of denying it, so a write that you expected to be blocked but succeeded may
be running under `quota_audit`; the [quotas](@/core/quotas.md) reference explains when a configured limit audits rather
than enforces.

## The diagnostic toolbox

When a symptom does not point at an obvious cause, these commands and endpoints confirm the state of a running or
stopped node.

`peryx cache fsck` rehashes every blob and verifies each against its content-addressed path, then prints one row per
problem — a mismatched digest, an unreadable blob, or a path that does not match its hash. It reports problems on stdout
but exits `0` regardless, so read its output rather than its exit code. `peryx backup verify` and `peryx mirror verify`
apply the same rehash to a backup archive and to a mirror's cached set respectively, and both *do* exit non-zero on any
problem, which makes them safe to gate a script on.

`peryx job list` prints background runs newest-first; `peryx job show <id>` expands one. A failed catalog sync or cache
refresh shows `state = failed` and an `error` whose leading category tells you whether to retry — `retryable_upstream`
and `retryable_timeout` are transient, while `upstream`, `catalog_sync`, and `project_sync` need investigation. The
[configuration reference](@/core/configuration.md) documents the job model these records come from.

A run that is stuck rather than failed can be stopped over HTTP: `POST /+jobs/{id}/cancel` as a server administrator
reaches the cooperative cancellation signal, which lives in the process running the job and so cannot be delivered by
the `job` CLI from a separate process. The run observes the signal and unwinds within its grace period, so a delivered
signal answers `202`; a run already finished, or one this node is not currently running, answers `409`; an unknown run,
like a caller without the administration-write scope, answers `404`.

`peryx index show <index>` prints one index's resolved role, upstream, and offline flag without starting the server —
the fastest way to confirm that the topology peryx loaded matches the file you edited.

Three HTTP probes report liveness and readiness without a credential. `/+health` always returns `200`
`{"status":"live"}` once the process is up; do not use it for load-balancer health, since it stays green even when a
store is unhealthy. `/+ready` returns `200` `{"status":"ready"}` or `503` `{"status":"not_ready"}`, and
`/+ready?writes=true` additionally requires the writer role, so a replica answers `503` to it. `/+status` returns a
detailed JSON health map — store reachability, upstream reachability, and the node's `writer` or `replica` role — for a
human or a dashboard. The [availability contracts](@/core/availability-contracts.md) define what each probe promises.

For the request-level detail behind a symptom, raise the log level for one module rather than the whole server, for
example `--log-level "info,peryx_upstream=debug"`, and read the structured security events peryx emits on every index
action; both are covered in [configure logging](@/core/logging.md).
