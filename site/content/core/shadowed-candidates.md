+++
title = "Shadowed candidates"
description = "Explain which member a virtual repository selected for a project and which candidates it shadowed."
weight = 11
+++

A virtual repository aggregates other indexes. When it resolves a project, each distribution filename is served by
exactly one member, and any other member offering the same filename is shadowed. This endpoint replays that resolution
and reports both sides: the selected candidate for each filename and every candidate a member shadowed, with the reason
it lost. An operator uses it to explain why one file won and another never appears in an installer's view. It changes no
member order, installer response, or policy evaluation; it only reads.

The query is scoped to one virtual repository and one project. It reads stored records — a hosted member's uploads and a
cached member's already-fetched page — rather than probing every member, so it never triggers an upstream fetch and
stays bounded. A cached member with no stored page contributes nothing.

| Field      | Meaning                                                                             |
| ---------- | ----------------------------------------------------------------------------------- |
| `member`   | Configured member index the candidate came from                                     |
| `source`   | `hosted` for an uploaded artifact, `cached` for one mirrored from an upstream index |
| `filename` | Distribution filename the candidate offers                                          |
| `digest`   | Candidate's content digest, when the ecosystem addresses it by one                  |
| `selected` | Whether this candidate is the one the repository serves for its filename            |
| `reason`   | Why a candidate was shadowed, absent for the selected candidate                     |

A shadowed candidate carries one of two reasons. `precedence` means a higher-precedence member already supplied that
filename: peryx orders hosted members ahead of cached ones, so an upload shadows an identical upstream file. `fallback`
means the repository's fallback policy excluded the member: `private-first` drops cached candidates whenever a hosted
member holds the project, and `no-fallback` never consults cached members at all. Plain `fallback` mode shadows only by
precedence, so a distinct cached file the hosted member does not offer is still selected.

List the candidates for one project with a repository token or a local login:

```console
curl -u __token__:$TOKEN \
  'http://127.0.0.1:4433/+shadow/candidates?repository=root/pypi&project=example'
```

An example response, with the selected candidate leading its filename group:

```json
{
  "candidates": [
    {
      "member": "hosted",
      "source": "hosted",
      "filename": "example-1.0-py3-none-any.whl",
      "digest": "sha256:1111\u2026",
      "selected": true
    },
    {
      "member": "pypi",
      "source": "cached",
      "filename": "example-1.0-py3-none-any.whl",
      "digest": "sha256:2222\u2026",
      "selected": false,
      "reason": "precedence"
    }
  ],
  "next_cursor": null
}
```

The endpoint requires `repository` (a virtual repository route) and `project` (normalized to the ecosystem's canonical
form). Results are ordered by filename, then the selected candidate before the ones it shadows, then member name. Pass
`next_cursor` as `cursor` for the next page. `limit` defaults to 25 and accepts 1 through 100. The cursor is a stable
identity key, so a page boundary holds and never skips or duplicates a candidate.

Authorization runs before any candidate is read. A caller who can read the repository — a local repository reader,
publisher, or administrator, or the repository's upload token under the reserved `__token__` username — may inspect its
shadowing. The server operator role, which carries no repository access, cannot. A caller without that access cannot
infer a member name, project, filename, or digest: peryx returns the same `404 Not Found` for a missing repository and
one outside an authenticated user's reach, and `401 Unauthorized` for an anonymous request. Candidates never carry an
upstream URL or credential.

Shadowed candidates are an explanation, not a selection. They stay absent from the HTML and JSON installer responses,
which continue to serve only the selected candidate for each filename. Responses exclude credentials, authorization
headers, and client addresses, and carry `Cache-Control: no-store` so an authenticated view never enters a shared cache.

## Troubleshooting

Send local passwords and repository tokens over HTTPS, except for a loopback-only server. Configure Peryx TLS or
terminate TLS at a trusted reverse proxy before exposing this page.

| Result                      | Check                                                                                               |
| --------------------------- | --------------------------------------------------------------------------------------------------- |
| No candidates               | Confirm the repository is virtual and the project resolves; a cached member must have a stored page |
| `400 Bad Request`           | Use a page size from 1 through 100, a cursor from this query, and a project within 512 bytes        |
| `401 Unauthorized`          | Use a local login, or use `__token__` with a repository token for the virtual repository            |
| `403 Forbidden`             | Give the repository token a write grant; a read-only token cannot inspect shadowing                 |
| `404 Not Found`             | Check the repository route and the local user's grant; Peryx gives both failures one response       |
| `500 Internal Server Error` | Inspect the metadata store and server log for a member resolution failure                           |
| `503 Service Unavailable`   | Restore user, grant, or authentication storage before retrying                                      |
