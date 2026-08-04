+++
title = "Repository management API"
description = "Create, inspect, list, update, and disable repositories over versioned HTTP operations."
weight = 15
+++

A repository is a persistent record that binds a unique route to an ecosystem and a definition. The store keys each
record by a stable, opaque id and keeps that id fixed across a rename or a state change, so a reference never re-homes.
The management API exposes the record's whole life over `/+repositories`: create, inspect, list, update, disable, and
re-enable. Every operation is administrator-only and every mutation commits exactly one repository version.

The API is not a reload switch. A management transaction commits one record; it does not reparse configuration, rebuild
unrelated indexes, or delay package downloads behind that write.

## The record

A repository serializes as an opaque record. The `id` is stable; the `route` and `ecosystem` are fixed for the record's
life; `display_name` and `definition` are the mutable surface; `version` increments on every commit.

```json
{
  "id": "repo_2f7e6a1b9c4d4e2f8a1b2c3d4e5f6a7b",
  "route": "root/pypi",
  "display_name": "PyPI mirror",
  "ecosystem": "pypi",
  "definition": {},
  "state": "enabled",
  "version": 1,
  "created_by": "usr_550e8400e29b41d4a716446655440000",
  "created_at_unix": 1700000000,
  "updated_by": "usr_550e8400e29b41d4a716446655440000",
  "updated_at_unix": 1700000000
}
```

## Authorization

Every route authenticates an administrator with HTTP Basic credentials and checks a management scope over the operator
resource. Reads require `administration:read`; mutations require `administration:write`. A caller that authenticates but
lacks the scope reads the same `404` as a caller asking for a repository that does not exist, so an outsider cannot tell
an inaccessible repository from an absent one.

| Operation | Route                         | Method | Scope                  | Precondition |
| --------- | ----------------------------- | ------ | ---------------------- | ------------ |
| List      | `/+repositories`              | `GET`  | `administration:read`  | —            |
| Create    | `/+repositories`              | `POST` | `administration:write` | —            |
| Inspect   | `/+repositories/{id}`         | `GET`  | `administration:read`  | —            |
| Update    | `/+repositories/{id}`         | `PUT`  | `administration:write` | `If-Match`   |
| Disable   | `/+repositories/{id}/disable` | `POST` | `administration:write` | `If-Match`   |
| Enable    | `/+repositories/{id}/enable`  | `POST` | `administration:write` | `If-Match`   |

A missing or wrong credential returns `401` with a `WWW-Authenticate: Basic` challenge. A denied-but-authenticated
caller returns `404`.

## Create a repository

`POST /+repositories` mints a stable id under a unique route. The route and ecosystem are fixed once set. The response
carries the record, an `ETag` for a later `If-Match`, and a `Location` for the new resource.

```console
$ curl -sS -u "$ADMIN" https://packages.example/+repositories \
    -H 'content-type: application/json' \
    -d '{"route": "root/pypi", "display_name": "PyPI mirror", "ecosystem": "pypi", "definition": {}}'
```

A second repository on the same route returns `409`. An empty or oversized field returns `422`. A body that is not
`application/json` returns `415`; a malformed JSON body returns `422`.

## List repositories

`GET /+repositories` returns repositories in id order with a bounded page and an opaque cursor. Filter by `state`
(`enabled` or `disabled`), page with `limit` (1..=100, default 25), and continue with `cursor` from the prior page's
`next_cursor`. A `next_cursor` of `null` marks the last page.

```console
$ curl -sS -u "$ADMIN" 'https://packages.example/+repositories?state=enabled&limit=50'
```

```json
{
  "repositories": [
    {
      "id": "repo_2f7e6a1b9c4d4e2f8a1b2c3d4e5f6a7b",
      "route": "root/pypi",
      "display_name": "PyPI mirror",
      "ecosystem": "pypi",
      "definition": {},
      "state": "enabled",
      "version": 1,
      "created_by": "usr_550e8400e29b41d4a716446655440000",
      "created_at_unix": 1700000000,
      "updated_by": "usr_550e8400e29b41d4a716446655440000",
      "updated_at_unix": 1700000000
    }
  ],
  "next_cursor": "repo_9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d"
}
```

A `limit` outside `1..=100` returns `400`.

## Conditional updates

Update, disable, and enable are conditional. Each read returns the current version in an `ETag`; each mutation must echo
that version in `If-Match`. A mutation with no `If-Match` returns `428 Precondition Required`; an `If-Match` that is not
a version returns `400`. When the stored version has moved on, the write loses with `409 Conflict`, the winning version
rides back on both the body's `current_version` and the response `ETag`, and the stored record is untouched — refetch,
re-apply, and retry.

```console
$ etag=$(curl -sS -u "$ADMIN" -D - -o /dev/null \
    https://packages.example/+repositories/repo_2f7e6a1b9c4d4e2f8a1b2c3d4e5f6a7b \
    | awk -F': ' 'tolower($1) == "etag" { print $2 }' | tr -d '\r')

$ curl -sS -u "$ADMIN" -X PUT \
    https://packages.example/+repositories/repo_2f7e6a1b9c4d4e2f8a1b2c3d4e5f6a7b \
    -H "if-match: $etag" -H 'content-type: application/json' \
    -d '{"display_name": "PyPI mirror (west)", "definition": {}}'
```

The id survives the rename; the version increments and the new value returns in the `ETag`. Only `display_name` and
`definition` change — an attempt to move the route or ecosystem is not expressible through update.

## Disable and enable

`POST /+repositories/{id}/disable` takes a repository out of service under the same `If-Match` rule;
`POST /+repositories/{id}/enable` restores it. Disable is idempotent: disabling an already-disabled repository at its
current version returns it unchanged. A stale precondition conflicts exactly as an update does.

```console
$ curl -sS -u "$ADMIN" -X POST -H "if-match: $etag" \
    https://packages.example/+repositories/repo_2f7e6a1b9c4d4e2f8a1b2c3d4e5f6a7b/disable
```

## Relationship to `[[index]]` configuration

Repositories defined statically under `[[index]]` in the configuration file continue to route exactly as before; the
configuration file remains their source of truth. The management API operates on repository records in the store and is
the way to create, rename, and disable repositories at runtime without reloading the process. Bringing configured
`[[index]]` repositories under API management — reconciling each into a stored record with a stable id derived from its
route, so a restart is idempotent and a later rename does not re-home references — lands with the server-startup change
that owns configuration parsing; the store already provides the idempotent reconcile primitive it builds on.
