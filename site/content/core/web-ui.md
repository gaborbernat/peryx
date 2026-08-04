+++
title = "Use the web UI"
description = "Search packages, browse indexes, read package pages, inspect status, and inspect archives from the browser."
weight = 7
+++

peryx serves a reactive web interface on its own port: server-rendered pages that hydrate in the browser, in the same
visual style as this site.

## Dashboard

`http://<host>:<port>/` shows the version, the change serial, and the counters in two groups: a **Global** group with
the instance-wide request count, then one group per ecosystem (labelled with its badge) holding that ecosystem's scoped
counters: [PyPI](https://pypi.org/)'s listings, artifacts, and [PEP 658](https://peps.python.org/pep-0658/) metadata
hits; [OCI](https://opencontainers.org/)'s served manifests (pages), pulled blobs (downloads), and pushed images
(uploads). Below the counters sits a card per configured index (PyPI and OCI alike) with its ecosystem badge, kind,
route, layers, whether it accepts uploads, and its usage. The counters refresh every few seconds.

{{ screen(alt="The dashboard: counters on top, one card per index with its layer stack and usage", name="dashboard") }}

Each card's `usage` link opens the drill-down described in [monitoring](@/core/monitor.md): index totals, a per-project
table, and per-file download counts.

## Admin status

`/admin/status` reads `GET /+status` and top-level `GET /+stats`, and it renders each field at the caller's class: the
index list, routes, and upload targets stay public, the cache-health counters need operator authority, and the upstream
hosts, upload-token state, project and file counts, and recent uploads need administrator authority. A page loaded
without a credential shows the routes but not the counters or those sensitive fields; authenticate the page request as a
server administrator to see the full status. It links to the JSON status, JSON stats,
[Prometheus](https://prometheus.io/) metrics,
[Simple API](https://packaging.python.org/en/latest/specifications/simple-repository-api/), browse, and usage pages.

The admin status document scans metadata keys once to count observed projects and uploaded files, then keeps only a
capped recent-upload list per index. It does not fetch upstreams, read package detail pages, read artifacts, or expose
upload tokens, upstream passwords, bearer tokens, URL user info, URL queries, or URL fragments.

## Availability topology

`/admin/topology` reads `GET /+availability/topology` and renders one role-filtered snapshot of the availability group,
taken at a single instant. The mode, group, node identities, datacenters, and roles stay public; each node's liveness
and this node's committed frontier need operator authority; the advertised peer addresses need administrator authority.
A field above the caller's class reads as restricted rather than showing, so a withheld value never looks healthy.

This node reports its own role, liveness, and committed frontier from a live self-observation. A peer stays `unknown`
until a consensus layer observes it, so stale peer data never reads as healthy; the writer ages its replica heartbeats
on the [replication readiness document](@/core/high-availability.md) until then. The snapshot carries the UTC time it
was taken, so an old render shows as age rather than passing for health.

The roster uses a native table with a caption and columns for node, datacenter, role, health, frontier, and, for an
administrator, address. Every role and health state carries a text label, so the states stay distinguishable without
colour, and narrow screens scroll the table inside its page. A role filter narrows the roster to writers or replicas,
and the default shows every node so the page stays complete without scripting. A node with no configured roster reports
itself as a standalone single node.

The snapshot caps the rendered roster while still reporting the full node count, so a large group cannot return an
unbounded list. The page traverses no live membership or storage state per view and holds no credentials.

The page subscribes to `GET /+availability/topology/stream`, a bounded Server-Sent Events feed, so it reflects this
node's frontier and liveness as they move instead of polling. The stream reuses the one-shot endpoint's projection and
authentication: it inherits the browser's credentials and filters every event to the caller's class, so a live feed
never reveals a field the snapshot would withhold.

Its traffic tracks the change rate rather than the roster size or the count of open pages. One event carries the current
snapshot on connect, and a later event fires only when the meaningful state changes, so `captured_at` advancing on its
own emits nothing and an idle group carries only a keep-alive comment every fifteen seconds. Each sample re-reads live
state and the connection buffers no backlog, so a slow reader coalesces to the latest snapshot rather than a queue, and
the server drops a client too slow to drain the socket rather than growing memory to hold its backlog.

A feed badge beside the title reads `Live`, `Reconnecting`, or `Offline`. Each event's id increases, so the browser
resumes from the last one it saw on reconnect; while it retries, the badge shows `Reconnecting`, and once it gives up,
`Offline`. A paused feed stops the snapshot time from advancing and never reads as `Live`, so a frozen render shows as
stale rather than passing for health.

## Policy decisions

`/admin/policy-decisions` queries the bounded [policy decision history](@/core/policy-decisions.md). Administrators can
inspect all repositories; repository readers and publishers select a repository covered by their grant. The server
operator role does not grant repository access. A repository upload token remains valid for that repository.

Filters cover repository, outcome, rule, routed source, UTC evaluation range, and page size. Submitting changed filters
starts from the newest row; Previous and Next retain the cursor chain for the active filter set. The page holds the
username and password in memory, disables password autocomplete, and sends them in the Basic authorization header. It
does not write them to the URL or browser storage or include them in the server-rendered document or visible error text.

The results use a native table with a caption and separate columns for repository, package, version, file, source,
action, rule, reason, evaluation time, and next eligible time. Every outcome has a text label, including stale
decisions, and narrow screens scroll the table inside its page rather than widening the document. The policy-decision
guide lists [error remedies and credential requirements](@/core/policy-decisions.md#troubleshooting).

## Trash inspection

`/admin/trash` queries [soft-deleted artifacts](@/core/trash.md) across PyPI and OCI. Administrators can inspect every
repository; repository readers and publishers select a repository covered by their grant. A repository upload token
remains valid for that repository under the reserved `__token__` username.

Filters cover repository, ecosystem, state, and page size. The results use a native table with a caption and columns for
state, ecosystem, repository, artifact, reference, digest, reason, actor, deletion time, and recovery deadline. The
actor column follows the same role filter as the API, so a repository-scoped caller sees a dash where an administrator
sees the deleting identity. Every state has a text label, and narrow screens scroll the table inside its page rather
than widening the document. The page holds the username and password in memory, disables password autocomplete, and
sends them in the Basic authorization header without writing them to the URL, browser storage, or the server-rendered
document. The trash guide lists [error remedies and credential requirements](@/core/trash.md#troubleshooting).

## Usage analytics

`/admin/analytics` reads the [`/+analytics/*` usage queries](@/core/monitor.md#query-package-usage) over the retained
daily download aggregate. The **View** selector switches between the five read-only shapes: top packages, version usage,
source split, unused packages, and a daily timeline. Each view queries its own endpoint and renders its own columns, so
a version row carries a version and a timeline row carries its UTC day window.

Authorization matches the API. An operator analytics grant reads every repository at once when you leave the repository
blank; naming a repository you can read scopes the query to it. The source split is operator-only, because which
upstream served a cache miss is a property of the server's routing rather than of the repository. A repository's legacy
upload token reaches its own repository with the `__token__` username and a read grant. The page holds the username and
password in memory, disables password autocomplete, and sends them only in the Basic authorization header. It does not
write them to the URL or browser storage.

Filters map to the documented API query fields: repository, a UTC day range (`from` and `to`, sent as day-aligned Unix
seconds), and page size. Submitting changed filters starts from the newest page; Previous and Next retain the cursor
chain for the active view and filter set. Every page states its resolved UTC window. When a requested start predates the
retention floor the page adds a distinct note that earlier data has aged out, so an empty window reads differently from
one clamped to retention or from a failed query, which shows the HTTP outcome as bounded text without echoing the
response body.

The results use native tables with a caption and column headers. Absent values read as explicit text — a missing version
as an em dash, a local-store hit as `local store` — never as an empty cell, and counts never depend on color. Narrow
screens scroll each table inside its wrapper rather than widening the document. These choices follow the
[WAI-ARIA table pattern](https://www.w3.org/WAI/ARIA/apg/patterns/table/) and [WCAG 2.2](https://www.w3.org/TR/WCAG22/)
requirements for structure, link purpose, and reflow.

## Browsing packages

The header search box starts suggesting matches after two characters, across every ecosystem's indexes. Suggestions and
the full `/search` page use the same `GET /+search` API, so uploaded files, cached upstream pages, and virtual-index
overrides rank from one indexed view. Index policy filters search results before they reach the UI. Each result carries
a type badge in its ecosystem's own word (a PyPI package or an OCI image), so a mixed result set stays legible.

`/search` keeps `q`, `type`, `page`, and `page_size` in the URL. The `type` filter accepts uploaded, cached, and
override packages; the UI labels the last one as `Override`. Page size choices are 25, 50, and 100, and the browser
stores the last selected size for the next search. Matching is case-insensitive and folds accented and non-Latin letters
the way the index does, so a search for `café` finds `Café` and one for `ZÜRICH` finds `zürich`.

An index card links to its project list, filterable as you type. For a PyPI index, a project page shows everything an
index page carries: the rendered long description, summary, install command with a copy button, versions, dependencies,
keywords, license, author, project links, grouped classifiers, and release-grouped file tables with sizes, upload dates,
sha256 digests, and per-file badges for yank state, metadata siblings, and
[artifact source and availability](#artifact-source-and-availability). Release groups follow the page's existing
newest-first [PEP 440](https://packaging.python.org/en/latest/specifications/version-specifiers/) order. When matching a
file's version does not identify exactly one declared release, peryx keeps it visible in **Legacy or unassociated
files**; this includes malformed filenames, undeclared versions, and ambiguous PEP 440-equivalent declarations.

The version links select one exact displayed release through the `version` query parameter. A declared release with no
files has a different empty state from an unknown release, which helps distinguish an empty publication from a stale
link. Filename substring and regular-expression filters remain in the URL when moving between releases. People can share
a filtered view, and browser history restores both selections.

The release list uses native links in document order and adds `aria-current` to the current view. A heading labels each
release section and its data table. At narrow widths the table scrolls inside its wrapper instead of widening the page.
These choices follow [WCAG 2.2](https://www.w3.org/TR/WCAG22/) requirements for structure, focus order, link purpose,
visible focus, and reflow.

### Artifact source and availability

Each file row carries two chips drawn from the [artifact source and availability](@/core/artifact-source.md) record, so
a reader distinguishes an upload from a mirror, and a locally-stored blob from an upstream-only catalog entry. The first
chip is the **source**, the second is the **byte availability**, and the two vary independently.

| Chip                       | Meaning                                                                      |
| -------------------------- | ---------------------------------------------------------------------------- |
| source `hosted`            | Published into this instance. No upstream can resupply the bytes once lost.  |
| source `proxy`             | Cached from an upstream index. A local miss re-fetches from upstream.        |
| source `generated`         | Produced by this instance, such as a derived sibling.                        |
| availability `local`       | The configured storage holds verified bytes; a read needs no upstream fetch. |
| availability `remote-only` | No local bytes, but a known upstream can supply them.                        |
| availability `unavailable` | No local bytes and no upstream to supply them.                               |

A `proxy` + `local` pair is the familiar "cached" state; `hosted` + `unavailable` is a published file whose bytes were
lost. Wording carries the meaning. Each chip states its dimension and value in text, names that dimension for a screen
reader with `aria-label`, and never relies on colour, so the states stay distinguishable under the
[WCAG rule against colour-only meaning](https://www.w3.org/WAI/WCAG22/Understanding/use-of-color.html) and the
[WAI-ARIA Authoring Practices](https://www.w3.org/WAI/ARIA/apg/).

**Precedence.** The chips compose with the other per-file states rather than replacing them. A yanked file keeps both
its yank badge and reason alongside its source and availability; a file blocked or revoked by policy is still labelled
for what it is, since the page reports state and never makes the access decision. Hidden and trashed files are already
filtered out of the served listing upstream of the UI, so they do not reach a row to label. The page issues no fetch,
eviction, repair, or prefetch, and shows neither an upstream credential nor a signed URL.

**Stale projection.** Availability is a projection the storage layer keeps in step with the content store; the page
reads it in one indexed lookup per file and never probes a blob per row. If a blob is removed out of band, or a cache
fill crashes between writing bytes and recording them, the chip can lag the real bytes until the next
[repair pass](@/core/artifact-source.md#repair) reconciles it, showing `remote-only` on a file whose bytes are in fact
present, or the reverse. The source chip cannot drift this way, since it is intrinsic and only a different artifact
taking the digest's place rewrites it. If a file's availability looks wrong, trigger a repair pass rather than
re-uploading.

### Provenance and attestations

A file that advertises [PEP 740](https://peps.python.org/pep-0740/) provenance carries a provenance panel: a
keyboard-operable disclosure whose summary states two things in words, and whose body lists what the document claims.
The panel is built from digest-indexed metadata peryx already holds. It fetches nothing and verifies no signature while
rendering, so it reports what a bundle claims and how peryx obtained it, never that any attestation is trustworthy.

The summary carries a **source** chip and a **validation** chip:

| Chip                          | Meaning                                                                            |
| ----------------------------- | ---------------------------------------------------------------------------------- |
| source `hosted`               | Uploaded here; peryx bound each attestation to this distribution at upload.        |
| source `mirrored`             | Advertised by an upstream index; peryx relays the claim without reading it.        |
| validation `binding verified` | peryx enforced the subject binding at upload. Sigstore signatures are not checked. |
| validation `unverified claim` | Upstream advertises provenance; peryx neither fetched nor verified the document.   |
| validation `unreadable`       | Provenance is advertised but its stored document could not be read.                |

Expanding a hosted panel lists one row per attestation: its in-toto `predicateType`, named under the
[SLSA provenance model](https://slsa.dev/spec/v1.0/provenance), and how its subject binds. A subject row reads
`subject matched` when an attestation names this file's sha256, `subject mismatch` when none does, and `subject unknown`
when the statement carries no readable subject. A mirrored panel states only that upstream advertises a claim, since
peryx does not read a document it did not store.

**Security.** A valid envelope proves a publisher issued an attestation for this file, not that the file is safe or that
its signature is genuine — peryx checks the subject binding, never the Sigstore signature, certificate, or
transparency-log inclusion. Predicate types and any other bundle-supplied text render as escaped text and are bounded,
never as markup. The panel never inlines a full bundle: the summary links the provenance document, which is reachable
only through the same download route, and its authorization, that serves the file itself.

**Accessibility.** Every chip and subject row states its value in text and names its dimension for a screen reader with
`aria-label`, so the states stay distinguishable without colour under the
[WCAG rule against colour-only meaning](https://www.w3.org/WAI/WCAG22/Understanding/use-of-color.html). A file that
advertises no provenance shows no panel.

{{ screen(alt="A project page: description and files on the left, metadata panel on the right", name="project") }}

An OCI index browses the same way: its card opens the list of repositories it holds, a repository page lists its tags,
and a tag opens its manifest: the config and layer blobs of an image, or the per-platform children of an image index,
each by digest and size. Each tar layer carries a `contents` link that opens the same archive browser a wheel does,
listing the layer's files and previewing text members in bounded chunks.

{{ screen(alt="An OCI manifest: the pull command, the config digest, and the layer table with a contents link", name="oci-manifest") }}

Inspectable wheels, zips, zipped eggs, `.tar`, `.tar.gz`, and `.tgz` archives get a `contents` link. It opens the
archive browser: members with their sizes, and member text in bounded chunks for large generated files. Other legacy
compressed tar formats such as `.tar.bz2`, `.tbz`, `.tar.xz`, `.txz`, `.tlz`, `.tar.lz`, `.tar.lzma`, and `.tar.zst`
still show as downloadable files, but do not get a broken archive link. The browser URL stores the file's sha256,
display filename, selected member, and chunk offset as separate query parameters. That keeps links stable for filenames
and member paths containing spaces, slashes, `#`, or `?`.

Browse pages keep empty results separate from request failures. A failed project lookup, metadata fetch, archive list,
or member preview shows the HTTP status and response body from peryx, including the index, project, digest, or file
context the server can provide.

## Managing uploads

The `Upload` page sends one wheel or `.tar.gz` source distribution to an upload-enabled PyPI index. It reports transfer
progress, offers cancellation while bytes remain in flight, and shows a server rejection beside the selected filename.
See [upload from a browser](@/ecosystems/pypi/guides/browser-upload.md) for the permission and CSRF rules.

"Manage uploads" on a project page takes the index's upload token and offers yank, un-yank, and delete per version, plus
whole-project delete. The buttons drive the same HTTP endpoints as [curl would](@/ecosystems/pypi/guides/remove.md), so
the rules match: deleting uploads needs a `volatile` index, and files served from a cached index are hidden reversibly
rather than deleted.

## Requirements

The interactive layer is a wasm bundle built by [cargo-leptos](https://github.com/leptos-rs/cargo-leptos)
(`cargo leptos build --release`, output in `ui/pkg/`, served at `/pkg`). Without the bundle every page still renders
server-side; typeahead, filtering, live counters, stored page-size choices, and the admin buttons need it.

## Related

- The endpoints the UI reads: [HTTP endpoints](@/ecosystems/pypi/reference/endpoints.md)
- Operational counters and status data: [monitoring](@/core/monitor.md)
- How the UI is built and tested: [architecture](@/core/architecture.md)
