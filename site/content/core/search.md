+++
title = "Package search"
description = "Query the derived package index over /+search: substring and regex matching, source filters, and ACL-scoped results."
weight = 8
+++

Peryx keeps a search index over every project and repository it has served, and answers queries against it at
`/+search`. The index is ecosystem-neutral: a PyPI project and an OCI repository are the same kind of searchable record,
and each result names the ecosystem it came from so a client can label it in that ecosystem's own words (a `package` for
PyPI, an `image` for OCI).

The index is a cache derived from the metadata store, not the store itself. A query is an in-memory lookup that never
touches a blob, and each request runs on a blocking worker so a burst of searches cannot stall concurrent serving. The
index lives under `<data_dir>/search-v1` and repopulates itself as pages and tags are served; a mutation that changes
what a project would match marks the index stale, and the next query rebuilds it before answering. Nothing has to be
configured to turn search on.

## Endpoints

Two routes answer searches, both returning the same JSON document and taking the same query parameters:

- `GET /+search` searches across every configured index.
- `GET /{route}/+search` searches one index, filling `route` from the path so a caller cannot widen the scope.

The [PyPI endpoint reference](@/ecosystems/pypi/reference/endpoints.md) lists these alongside the ecosystem routes.

## Query parameters

- `q`: the search text. An empty or missing `q` matches every readable record, so `/+search` with no query is a paged
  listing.
- `route`: restrict a global search to one index route. The per-index endpoint sets it for you and ignores a `route` a
  caller passes.
- `type`: the source filter, one of `all` (the default), `uploaded`, `cached`, or `override`.
- `page`: the 1-based page number. A value below `1`, or one that does not parse, falls back to `1`.
- `page_size`: results per page, one of `25` (the default), `50`, or `100`. Any other value falls back to `25`.

## Matching

`q` matches a name by substring, ignoring case: `lask` finds `Flask`, and `django-rest` finds `djangorestframework` only
if that run of characters appears in the name. Matching covers the display and normalized names, not descriptions.

A query that begins with `re:` is a regular expression instead, evaluated over the whole name and ignoring case.
`re:^flask` anchors to names that start with `flask`; `re:client$` to names that end with `client`. An empty pattern
after `re:` matches everything, the same as an empty `q`. The expression is
[Tantivy](https://github.com/quickwit-oss/tantivy)'s regex dialect, and an unparseable one returns `400 Bad Request`
rather than an empty page.

## Source filter

Every result carries the class of the record it matched, and `type` narrows a search to one class:

- `uploaded`: a project on a hosted index, published to this instance.
- `cached`: a name mirrored from an upstream, served through a cached index.
- `override`: on a virtual index, a name the index's own upload target serves, shadowing the upstream of the same route.
  This is the [dependency-confusion](@/core/indexes.md) case made visible: a search can show which names a private
  upload is answering in place of a public one.

## Response

The response is a JSON object echoing the query and paging back, with the matched records:

```json
{
  "query": "flask",
  "route": "root/pypi",
  "type": "all",
  "page": 1,
  "page_size": 25,
  "total": 3,
  "results": [
    {
      "display_name": "Flask",
      "normalized_name": "flask",
      "route": "root/pypi",
      "index": "pypi",
      "ecosystem": "pypi",
      "type_label": "package",
      "type": "cached",
      "summary": "A simple framework for building complex web applications."
    }
  ]
}
```

`total` is the count of every readable match, not the size of the returned page, so a client pages through it with
`page` and `page_size`. `type_label` is the ecosystem's own word for a searchable record, filled in on the server so a
browser renders it without an ecosystem lookup of its own. `route` is omitted when the search was not scoped to one, and
`summary` is absent when the record carries none. Results follow a fixed order: display name, then route, then
normalized name. Repeating a query returns the same page in the same order, and search does not rank by relevance.

## Access control

Search never leaks a private name through a count. When a searched index requires authentication to read, peryx compiles
the caller's read grants into the query itself, so `total` and every page contain only the resources that caller may
read. An unreadable project never appears in the results and never counts toward the total, so a count cannot betray a
name a client may not see. A caller carries its grants the same way as on any other request; see
[authentication and access control](@/core/authentication.md). When every searched index grants anonymous reads, no such
filter applies and the search runs unauthenticated.

## Rebuilding the index

Because the index is derived, peryx can rebuild it from authoritative metadata at any time without data loss. It does
this on its own when the on-disk index came from a version whose schema no longer matches: it discards the stale index
and rebuilds it rather than failing to start. To force a full rebuild, for example after restoring metadata from a
backup, run the reindex job:

```console
$ peryx job reindex
```

It rebuilds the index from the metadata store in committed chunks. Ordinary operation needs no manual reindex; the index
keeps itself current as projects change.
