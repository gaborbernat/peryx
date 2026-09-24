+++
title = "Performance"
description = "Cold and warm installs, file throughput, parallel CI, and request throughput for peryx, devpi, proxpi, pypiserver, and pypicloud."
weight = 2
+++

Each result includes the exact commands, versions, resolved artifacts, run order, and raw timing samples that produced
it. See [performance and methodology](@/core/operations/performance.md) for the shared test controls.

{{<machine />}}

## Benchmark setup

The workload installs an exact corpus of popular PyPI packages, torch among them for one large wheel, into a fresh
virtualenv with a fresh installer cache, so every byte must come through the index:

```shell
uv venv --python 3.14.7 fresh-venv
env VIRTUAL_ENV=$PWD/fresh-venv UV_CACHE_DIR=$PWD/fresh-cache \
    uv pip install --index-url http://127.0.0.1:4433/root/pypi/simple/ \
        --only-binary :all: boto3==1.43.100 urllib3==2.8.0 ... torch==2.11.0
```

`--only-binary :all:` keeps the run honest. Without it a package that ships no wheel for the interpreter is compiled
from source, and that build lands inside the measured install, dwarfing anything the index server contributes.

The versioned corpus feeds a local content-addressed upstream fixture, so every server receives identical bytes without
measuring PyPI or its CDN. Before timing, the runner rejects any server whose JSON or HTML index exposes a missing or
extra project, version, filename, or digest. Each cell is the median of recorded independent rounds. Every round starts
the server on empty state, and a seeded shuffle interleaves server-round pairs. "Cold" is the empty first pass; "warm"
reruns against the now-full cache. The harness kills each server's whole process group between rounds, so a forked
worker cannot outlive its round and steal CPU from the next measurement. See
[performance and methodology](@/core/operations/performance.md) for how the suite treats rounds and spread.

An install is a blunt instrument for measuring an index server. uv's own resolve, unzip, and install work dominates the
wall clock, so a faster index cannot rescue a slow client. The request swarm and file-throughput rows isolate server
work.

## Compared servers

The comparison includes every alternative that starts from a published package without external services. **Direct**
means [uv](https://docs.astral.sh/uv/) talking to the immutable local fixture without a proxy and provides the baseline
for each ratio. The benchmarks measure cache-miss data paths and concurrent misses.

- [peryx](@/contributing/runtime-architecture.md) streams misses to the client and content-addressed store from one
  process. It supports private indexes with scoped tokens.
- [devpi](https://devpi.net/docs/) parses pages and streams files into SQLite keyfs and SHA-256-addressed storage. It
  uses per-user access control.
- [proxpi](https://github.com/EpicWink/proxpi) downloads misses to temporary storage and keeps its index in memory. It
  has no private-upload path.
- [pypiserver](https://github.com/pypiserver/pypiserver) serves a package directory and redirects misses to its
  configured upstream. It supports htpasswd authentication per directory.
- [pypicloud](https://pypicloud.readthedocs.io/) buffers misses before serving them and stores metadata in SQLite or a
  remote database. It supports user and group access.

### Cache-miss data path

The cold rows measure how each server moves an uncached wheel from the local fixture to the client.

{{<diagram file="pypi-cache-miss" />}}

- **peryx** never buffers a whole response.
  [Page and artifact bytes stream to the client and into the store at once](@/contributing/runtime-architecture.md);
  peryx transforms a page chunk by chunk mid-flight, and tees a wheel to a temp file, hashes it, and renames it into the
  store once the client already has its bytes. A miss costs upstream wire time plus one hop. That sets the cold-install
  and cold-throughput numbers.
- **devpi** handles artifacts much as peryx does. `FileStreamer` writes each chunk to a local file and yields it to the
  client, then commits the sha256-addressed file once the body completes. Simple pages take the slower route: devpi
  fetches the upstream page, parses it, writes the link list into its SQLite keyfs, and only then renders a response
  from its own store. That parse-and-store step runs under a single-writer transaction model.
- **proxpi** downloads a missed file to disk in a background thread while the requesting client blocks on
  `thread.join(0.9 s)`; if the download outruns that `PROXPI_DOWNLOAD_TIMEOUT`, proxpi redirects the client to the
  configured upstream and lets the thread finish caching for next time. Its file cache defaults to a
  `tempfile.mkdtemp()` that proxpi deletes on shutdown, so without a configured `PROXPI_CACHE_DIR` the cache does not
  survive a restart. proxpi serves cached files from disk via `send_file`, not from an in-memory blob. The resident
  memory in the resource rows comes from four gunicorn worker processes, each holding its own unshared in-RAM index
  cache.
- **pypiserver** serves a directory of your own packages; with `--fallback-url` a miss is a bare `302` redirect to the
  configured upstream's simple page. It downloads and caches nothing. That is why its CPU sits near zero and its cold
  and warm columns differ little: there is no cache to warm, and a miss is a formatted redirect string.
- **pypicloud** was the closest design to peryx (a `fallback = cache` read-through mirror), but its cold path buffers
  the full response. It pulls the entire upstream file into a `TemporaryFile`, computes hashes, writes it to storage and
  a row into its cache DB, and only then sets the response body. The client waits for the download, the disk write, and
  the DB commit before its first byte. pypicloud stores files by `name/version/filename`, not by hash. Its maintainers
  archived it in 2023. The benchmark pins it to Python 3.10.21 and [SQLAlchemy](https://www.sqlalchemy.org/) 1.4.54.

peryx serves [PEP 658](https://peps.python.org/pep-0658/) `.metadata` by default and
[synthesizes it with byte-range reads](@/contributing/runtime-architecture.md) when an upstream lacks it. The fixture
serves each wheel's exact embedded `METADATA` as its sibling so this path remains reproducible. proxpi passes that
sibling through and pypiserver redirects to it. devpi keeps core metadata behind its experimental
`--enable-core-metadata` flag, which the benchmark leaves off, and pypicloud does not serve it, so their metadata cells
read `error`.

### Concurrent cold bursts

The parallel-install and throughput cold rows send concurrent requests for the same uncached object. peryx uses
single-flight, so misses for one page or file share one upstream fetch. Two competitors can fail under this load,
depending on how the requests interleave.

**devpi, the empty first page.** Each request reads devpi's database through a snapshot taken when it arrives. On the
first concurrent fetch of a project, one request takes the per-project lock, fetches the upstream page, and commits the
links. The others wait on that lock, then read their older snapshot, find no links, and answer `200` with an empty page.
uv reads the empty page as "no version of polars" and the install fails. The empty answer is not cached: the next
request sees every file, so only a burst of cold requests fails.

{{<diagram file="devpi-concurrency" />}}

**pypicloud, the concurrent INSERT.** The cache-on-miss path has no dedup and no locking. Four clients asking for one
wheel each download the whole file, then each try to write the same `filename` primary key into single-writer SQLite.
The commits serialize; the losers hit a `UNIQUE` constraint (or `database is locked`), and because
[pyramid_tm](https://docs.pylonsproject.org/projects/pyramid_tm/) commits after the view returns with no retry
configured, the exception surfaces as `HTTP 500`.

{{<diagram file="pypicloud-concurrency" />}}

Read this way, each table below is a controlled test of one axis: cold latency, warm overhead, a concurrent cold burst,
a fleet installing at once, a swarm reading pages. The architecture above says in advance which servers should struggle
where.

## Benchmark suite

The benchmark runner executes one workload against peryx and its competitors, then records process samples and reports.
Cell colors rank each row. Parenthesized ratios compare each server with the no-proxy **direct** baseline. See
[run the benchmarks](@/contributing/benchmarking.md) for source ownership and commands.

### Root-catalog synchronization

The root-catalog benchmark generates exactly one million valid project names, serves one PEP 691 response over loopback,
and runs the same streaming parser, 10,000-name transactions, and generation swap as `mirror sync --mode all`. While the
sync runs, another runtime worker repeatedly reads an unrelated project record. The report includes wall time, upstream
request count, completed foreground reads, and their p99 latency.

Peak RSS includes the generated JSON and the loopback server's response buffer. Compare deltas only between revisions of
this benchmark. The one-request assertion catches accidental refetches; the project-count assertion catches partial
publication.

The table covers every alternative that can start from a published package without external services: peryx, devpi,
proxpi, pypiserver (whose upstream fallback is a redirect rather than a cache), and pypicloud (archived upstream; the
benchmark pins its compatible interpreter and dependencies). [Pulp](https://pulpproject.org/) needs
[PostgreSQL](https://www.postgresql.org/) plus four services,
[nginx_pypi_cache](https://github.com/hauntsaninja/nginx_pypi_cache) is a [Docker](https://www.docker.com/)
configuration rather than a package, and [Artifactory](https://jfrog.com/artifactory/),
[Nexus](https://www.sonatype.com/products/nexus-repository), and the cloud registries need licenses or accounts, so none
of them can be measured this way.

The install workload contains exact pins for 51 popular PyPI packages, including torch for one large wheel, installed
with uv into a fresh virtual environment and client cache. **Cold** is the first install against a server with empty
state; **warm** reruns it with the server's cache full and only the client reset.

{{<bench file="install-uv" owner="pypi" />}}

The same workload through [pip](https://pip.pypa.io/) separates client behavior from server behavior. pip installs
serially and does more work between requests than uv, so the client contributes more of the measured wall time.

{{<bench file="install-pip" owner="pypi" />}}

The throughput workload moves the corpus's platform-specific torch wheel. The cold row models a CI fleet reacting to a
new release. Four clients ask for the same wheel at once, and the server either fans one upstream transfer out to every
waiter or serializes them. peryx runs the transfer as a detached task every client tails. The hot rows measure how fast
a cached wheel leaves the server, alone and under eight parallel readers.

{{<bench file="throughput" owner="pypi" />}}

The parallel-install workload is that fleet end to end: ten virtualenvs install polars at once, each with its own empty
client cache, exactly like ten CI jobs landing on the same runner pool. The server sees ten simultaneous copies of every
page and wheel request. A failure cell in this table means the server broke an install under concurrent cold misses.

{{<bench file="parallel-install" owner="pypi" />}}

The request workload drives a swarm against each warm server: one user, then 32, each a client that fetches ten corpus
project pages and reads every byte of the body, the way a resolver does. Each page exposes one selected artifact. Every
client sends the `Accept` header pip and uv send, because peryx picks the representation from it: a swarm asking for
`*/*` receives the [PEP 503](https://peps.python.org/pep-0503/) HTML render instead, which is not the page an installer
gets and prices work no install performs.

The measured table reports throughput, p95 latency, and normalized server CPU from the same raw rounds. A warm peryx hit
is a lookup and a copy of bytes it has already transformed; competitors that parse or render pages on each request do
more work on this path.

{{<bench file="load" owner="pypi" />}}

Every table ends with two resource rows: the CPU the server's whole process tree burned while its workload ran, and its
peak resident memory, compared against peryx (direct runs no server, so it cannot anchor them). The load table prices
that CPU **per thousand requests served**: a fixed-duration swarm hands a slower server less work, so absolute CPU would
reward low throughput.

`direct` is the no-proxy fixture path. Its rows measure the client and local fixture without charging a server process.
The fixture runs inside the benchmark process, so under the request swarm it shares a runtime with the load clients,
while every other party runs as its own process.

Read throughput beside memory and CPU. pypiserver's low server cost reflects that it redirects file downloads instead of
serving them.

## Endpoint coverage

The client workloads cover the project page, wheel, and PEP 658 metadata endpoints. The endpoint benchmark measures one
warm request to every other served endpoint.

It is peryx against itself, not against the field. A PyPI server chooses its own url shapes and decides what its index
root contains: pypi.org answers `/pypi/{project}/json` where peryx answers `{index}/{project}/json`, devpi addresses
files by an internal path, and a proxy's index root lists what it has cached while pypi.org's lists every project that
exists. Rows across those servers would compare different work and read as a ranking. The comparisons live in the tables
above, which drive one client against everyone.

{{<bench file="endpoints" owner="pypi" />}}

peryx serves the JSON project page from its transformed-page cache, so it costs a lookup and a copy. The HTML render and
the legacy `/{project}/json` API are not cached: each request parses the stored page and renders it again. Installers
ask for JSON, so no install pays this; a browser and an old client do.

Every server is measured the same way, on the same machine, in the same run, and one command reproduces every table: see
[run the benchmarks](@/contributing/benchmarking.md).

## Related

- Benchmark controls and interpretation: [performance and methodology](@/core/operations/performance.md)
- Put the cache in front of CI: [the CI guide](@/ecosystems/pypi/guides/ci-cache.md)
