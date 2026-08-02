+++
title = "Node liveness"
description = "Track datacenter replica health from bounded heartbeats without ever changing the configured roster."
weight = 7
+++

A datacenter replication group is a fixed roster: one writer and its read replicas, set by the
[`[[availability.member]]`](@/core/configuration.md#availability) configuration and changed only by an operator editing
it. Liveness tracking observes how recently each replica reported in, so routing and operators can tell a lagging
replica from a healthy one. It never edits that roster. A missed heartbeat cannot evict a replica, promote a replica to
writer, or transfer authority; only a reviewed configuration edit changes membership.

## Heartbeats

Each replica beacons its health to the group writer at the writer's bearer-authenticated replication endpoint:

```http
POST /+replication/v1/heartbeat
Authorization: Bearer <replication-token>
Content-Type: application/json

{"node": "replica-a", "incarnation": 3, "sequence": 128}
```

The writer accepts a beacon only from a configured member. `incarnation` rises when a node restarts and `sequence` rises
with each beat, so the pair totally orders one node's beacons. The writer keeps the latest accepted beacon per member
and drops any report that does not advance that position, which discards a replayed or reordered beacon. A report
carrying no bearer credential, a wrong credential, an unconfigured node, or a body over 4 KiB is refused and cannot mark
a node healthy. The tracked state is bounded by the roster size, one observation per member, and that body cap, so a
looping or hostile reporter cannot grow it.

## Suspicion

The writer ages the most recent accepted beacon into one verdict per member:

| Verdict   | Meaning                                                   |
| --------- | --------------------------------------------------------- |
| `alive`   | A beacon arrived within the last 15 seconds.              |
| `suspect` | The last beacon is between 15 and 45 seconds old.         |
| `dead`    | The last beacon is older than 45 seconds.                 |
| `unknown` | The member is configured but has sent no accepted beacon. |

Suspicion is derived independently on each observer from the observations it holds, so an asymmetric partition can leave
two writers holding different verdicts for the same replica while neither changes committed membership.

## Reading liveness

An operator or administrator reading the writer's availability health document sees a `peers` array, one entry per
configured replica, with its verdict and last-seen age:

```json
{
  "mode": "dc",
  "role": "primary",
  "ready": true,
  "reasons": [],
  "serial": 42,
  "peers": [
    {
      "node": "replica-a",
      "suspicion": "alive",
      "incarnation": 3,
      "sequence": 128,
      "last_seen_seconds": 2
    }
  ]
}
```

The `peers` field is operator-classified: an unauthenticated caller reading the same document sees only the public mode,
role, and readiness verdict. Peer suspicion is a routing hint. It never gates the writer's own readiness, so a suspect
or dead replica does not remove the writer from a pool or stop it accepting writes.
