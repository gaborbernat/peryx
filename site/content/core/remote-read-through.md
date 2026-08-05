+++
title = "Remote read-through"
description = "Serving a public download by streaming its bytes from a peer data center that holds a verified placement, when the local store misses."
weight = 11
+++

When a public download misses the local content store but a peer data center holds a
[verified placement](@/core/blob-placement.md) of the same digest, a read-through fetches the bytes from that peer, on
demand, and stages them locally so the request — and every later one — serves from disk. It is the serving counterpart
to the background cross-data-center copier: the copier fills the local store ahead of demand from a backlog, the
read-through fills it in response to a miss.

The read-through is one shared capability both ecosystems reach through. A PyPI file download and an OCI blob pull hit
the same selection, streaming, verification, and staging path; neither carries its own copy of it.

## When it runs

A read-through is installed only where it can help: a `dc` or `ha` node with a configured member roster, a replication
token to authenticate to its peers, and a datacenter identity of its own. A node that carries no identity — a replica
that does not name itself in the roster — installs none yet and resolves a miss through its ecosystem's upstream path,
unchanged. A single-node deployment never installs one.

Every request first checks the local store. Only a genuine miss reaches the read-through, and only under the same
single-flight gate that already collapses concurrent cold requests for one digest, so one peer fetch serves every
waiter.

## Selecting a source

The read-through reads the digest's placements and keeps the ones a remote data center has verified. It orders them the
way the copier does — highest generation first, then a stable tie-break — so two runs over the same placements choose
the same peer. A [circuit breaker](@/core/availability-liveness.md) drops any data center that has failed repeatedly and
not yet cooled down, so a request does not spend its attempt budget on a peer that keeps refusing. One peer is tried per
data center, and the fan-out is capped, so a digest with placements in many data centers still bounds how many sources
one fetch reaches.

The total length the fetch reassembles against comes from the verified placement record's own size — replicated metadata
this node already trusts — never a peer's advertisement, so a lying peer cannot inflate the reassembly buffer.

## Streaming and verifying

The blob is drawn in bounded byte ranges rather than one unbounded body. Each range is fetched from the first available
source, falling through to the next on a loss, so one stale or wrong peer does not block a fetch another peer can serve;
a transient loss backs off and retries under a bounded schedule, and a peer that answers nothing terminal gives up
rather than retrying forever. A ranged read carries no per-chunk checksum, so the ranges are reassembled and the whole
blob is digest-verified against the requested digest before anything trusts it.

The verified bytes are then staged to disk and published only if they verify again at their content address. A corrupt
or failed source therefore leaves **no local content and no served response** — only a fall-through to the next source
or, when none can answer, to the ecosystem's own upstream path. The response a client receives always matches the digest
and size the catalog names.

Advertising the fetched bytes as a local placement is deliberately out of scope: that record is fence-coupled and owned
by the placement lifecycle, so a read-through only populates the content store. A later request finds the bytes local
and serves them without a peer round-trip.

## Bounding memory

A range fetch buffers a whole blob in memory before it verifies and stages it. The per-data-center transport bounds how
many such fetches run at once, so a saturated node holds at most that many reassembling blobs per peer in memory rather
than one per in-flight request. Once staged, a blob serves from disk, not memory. Incremental chunk-by-chunk streaming
to the client — serving each verified range as it lands instead of buffering the whole blob — is tracked separately as a
follow-up and is not part of this path.

## Configuration

The bounds default to conservative values and are tuned under `[availability.read-through]`, which a `dc` or `ha` mode
accepts and single-node `none` rejects:

```toml
[availability.read-through]
concurrency = 8            # concurrent streams one peer's transport runs (the memory bound)
per-fetch-bytes = 67108864 # byte cap each fetch streams under
chunk-bytes = 8388608      # span of one ranged request
max-fanout = 4             # most verified sources one fetch tries
trip-after = 3             # consecutive losses that open a source's circuit
cooldown-secs = 30         # how long an open source stays skipped

[availability.read-through.retry]
base-ms = 100       # first backoff delay
multiplier = 2      # per-attempt growth
max-delay-secs = 30 # backoff cap
max-attempts = 10   # retries before giving up
```

Every field is optional; an omitted field keeps its default, and the `retry` sub-table, when present, sets the whole
reconnect schedule. A zero for a bound that must be positive is rejected when the configuration is read.
