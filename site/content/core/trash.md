+++
title = "Trash inspection"
description = "List and inspect soft-deleted PyPI and OCI artifacts and whether each can still be restored."
weight = 11
+++

Deleting a hosted artifact moves it to trash rather than erasing it. The PyPI soft-delete and the OCI manifest
soft-delete both keep the content and record who deleted it, when, and why. This endpoint reads those records across
every ecosystem through one query, so an operator can review what was removed and whether it can still come back. It
does not delete, restore, or change retention; it only reads.

Each record carries the ecosystem, repository, artifact name, reference, digest, deletion reason, deleting actor,
deletion time, recovery deadline, and derived restorable state. A PyPI file's reference is its distribution filename; an
OCI record's reference is its tag, absent for an untagged manifest deletion. One schema covers both ecosystems.

| Field             | Meaning                                                                      |
| ----------------- | ---------------------------------------------------------------------------- |
| `ecosystem`       | `pypi` or `oci`                                                              |
| `repository`      | Configured repository the artifact was deleted from                          |
| `name`            | PyPI project or OCI repository path                                          |
| `reference`       | Distribution filename or OCI tag, absent for an untagged manifest            |
| `digest`          | Content digest, when the ecosystem addresses the artifact by one             |
| `reason`          | Operator's stated deletion reason, when the delete request supplied one      |
| `actor`           | Identity that deleted it, shown only to callers the role filter admits       |
| `deleted_at_unix` | UTC Unix timestamp of the deletion                                           |
| `deadline_unix`   | When the recovery window closes and a retention sweep may reclaim the record |
| `state`           | `restorable` or `expired`, derived at query time                             |
| `restorable`      | Whether the content is still retained and the recovery window is open        |

Restorability is derived, not stored. A record is `restorable` while its content is still retained and the current time
is before `deadline_unix`; past the deadline, or once the content a restore needs is gone, it reads as `expired`. The
window follows the deletion time plus a fixed recovery grace, so the API and the web view compute the same answer
without a second store read. This mirrors soft-delete recovery windows in systems such as Azure Container Registry and
cloud object stores, where a deleted item stays recoverable until a retention deadline computed from the delete time and
the current policy.

List one repository with its upload token:

```console
curl -u __token__:$TOKEN \
  'http://127.0.0.1:4433/+trash?repository=hosted&ecosystem=pypi&state=restorable&limit=25'
```

The endpoint accepts `repository`, `ecosystem`, `state`, and `deadline_before` filters. `deadline_before` keeps records
whose recovery deadline is at or before a UTC Unix time, so an operator can surface entries about to expire. Results use
newest-deletion-first order. Pass `next_cursor` as `cursor` for the next page. `limit` defaults to 25 and accepts 1
through 100. The cursor is a stable identity key, so a page boundary holds even as another artifact enters trash between
requests.

Inspect one record with `GET /+trash/record`, identifying it by `ecosystem`, `repository`, and `name`, plus `reference`
and `digest` when they distinguish it:

```console
curl -u admin:$PASSWORD \
  'http://127.0.0.1:4433/+trash/record?ecosystem=oci&repository=images&name=app&reference=1.0'
```

Authorization runs before the trash scan. A local administrator can omit `repository` to list every repository, or
select one by its route. A repository reader or publisher must select a repository covered by their grant, and a
repository upload token reaches only its own repository under the reserved `__token__` username. Callers without
operator or repository access cannot enumerate trash. The `actor` field follows a role filter on top of that: an
administrator sees it wherever they read, while a repository-scoped caller sees the record with the actor omitted. Peryx
returns the same `404 Not Found` for a missing repository and one outside an authenticated user's reach.

Responses exclude credentials, authorization headers, and client addresses, and carry `Cache-Control: no-store` so an
authenticated view never enters a shared cache. The read-only browser at `/admin/trash` exposes the same filters, cursor
pagination, and role-filtered actor column.

## Troubleshooting

Send local passwords and repository tokens over HTTPS, except for a loopback-only server. Configure Peryx TLS or
terminate TLS at a trusted reverse proxy before exposing this page.

| Result                      | Check                                                                                         |
| --------------------------- | --------------------------------------------------------------------------------------------- |
| No rows                     | Remove filters, then confirm that an artifact has been soft-deleted in a reachable repository |
| `400 Bad Request`           | Use a page size from 1 through 100, a known ecosystem and state, and a cursor from this query |
| `401 Unauthorized`          | Use a local login, or use `__token__` with a repository token and select its repository       |
| `403 Forbidden`             | Give the selected repository token a write grant; a read-only token cannot inspect trash      |
| `404 Not Found`             | Check the repository route and the local user's grant; Peryx gives both failures one response |
| `500 Internal Server Error` | Inspect the metadata store and server log for a trash query failure                           |
| `503 Service Unavailable`   | Restore user, grant, or authentication storage before retrying                                |

An expired record stays visible for audit until a retention sweep reclaims it. Restoring content is a separate operation
outside this inspection endpoint.
