+++
title = "React to changes with webhooks"
description = "Receive signed webhook deliveries when an index changes: the event catalog, the payload, verifying the HMAC signature, and the delivery guarantees to design around."
weight = 11
+++

peryx watches for uploads, deletes, yanks, and restores, and posts a signed JSON body to an endpoint you run. That turns
an index mutation into a trigger: rebuild a downstream lockfile when a package lands, page an operator when a release is
yanked, or feed an audit log that lives outside the server. Webhooks fire off the request path, so the client that
uploaded a wheel never waits for your endpoint.

Reach for a webhook when something outside peryx has to act on a change and you would otherwise poll for it. When you
only need to read what changed after the fact, [the usage and stats surfaces](@/core/monitor.md) already hold that
without an integration to maintain.

## Configure a target

Webhook tables live under the index that should emit them, so a hosted PyPI store and a hosted OCI registry each carry
their own targets. [The `[[index.webhook]]` reference](@/core/configuration.md) lists every key; the shape is a name, a
URL, a signing secret, and an optional event filter:

```toml
[[index]]
name = "root/pypi"
layers = ["hosted", "pypi"]
upload = "hosted"

[[index.webhook]]
name = "ci"
url = "https://ci.example/hooks/peryx"
secret_env = "PERYX_WEBHOOK_SECRET"
events = ["upload", "delete", "restore"]
```

Keep the secret out of the config file with `secret_env`, which names an environment variable, rather than `secret`,
which holds the literal value. Omit `events` to receive every kind. A target on a virtual index receives events for
requests made through that route, and the payload names the hosted layer that stored the change, so you can put one
target on `root/pypi` instead of on each member.

The URL must be `http` or `https` with no userinfo, query, or fragment; peryx rejects those at startup so a secret never
rides along in the URL. It also rejects a duplicate target name on one index and an empty secret.

## What a delivery looks like

Each event becomes one POST with a JSON body and these headers:

| Header              | Value                                                   |
| ------------------- | ------------------------------------------------------- |
| `content-type`      | `application/json`                                      |
| `user-agent`        | `peryx/<version>`                                       |
| `x-peryx-event`     | the event name, matching the body's `event` field       |
| `x-peryx-delivery`  | a stable id for this delivery, unchanged across retries |
| `x-peryx-timestamp` | the Unix second the attempt was signed                  |
| `x-peryx-signature` | `sha256=` followed by the hex HMAC (see below)          |

The body carries the change and the request that caused it:

```json
{
  "event": "upload",
  "created_at": 1750000000,
  "index": "root/pypi",
  "route": "root/pypi",
  "hosted_index": "hosted",
  "project": "example",
  "version": "1.4.0",
  "file": {
    "filename": "example-1.4.0-py3-none-any.whl",
    "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
  },
  "count": 1,
  "actor": "ci-token",
  "request_id": "req-42"
}
```

`index` and `route` are the index the request hit; `hosted_index` is the layer that stored the change, which differs
from `index` only when the request went through a virtual index. `count` is how many files the mutation touched, so a
delete that yanks a whole version reports the real number rather than one row per file. `version`, `file`, `actor`, and
`request_id` are present only when they apply: `actor` names the token or user when the request was authenticated, and
`request_id` echoes the caller's `x-request-id` header, which lets you stitch a delivery back to the originating request
in your logs.

## The event catalog

peryx emits five kinds from its write endpoints today. The three reserved names below parse in config so a filter
written against them keeps working, but nothing sends them yet.

| Event            | Fires when                                               |
| ---------------- | -------------------------------------------------------- |
| `upload`         | a file is published (PyPI) or a manifest is pushed (OCI) |
| `yank`           | a PyPI release is yanked                                 |
| `unyank`         | a yanked PyPI release is restored to normal serving      |
| `delete`         | a file, version, manifest, or blob is removed            |
| `restore`        | a soft-deleted item is brought back                      |
| `promote`        | reserved; not emitted in this release                    |
| `project-status` | reserved; not emitted in this release                    |
| `management`     | reserved; not emitted in this release                    |

The same runtime serves every ecosystem, so a hosted OCI registry delivers `upload` on a push and `delete` on a manifest
or blob removal the same way a hosted PyPI index delivers them for wheels. For OCI, `version` is the tag when a tagged
reference changed and `file.filename` holds the manifest or blob digest.

## Verify the signature

Treat any request to your endpoint as untrusted until the HMAC checks out. peryx signs with HMAC-SHA256 over the
`x-peryx-timestamp` value, a `.`, the `x-peryx-delivery` value, another `.`, and then the raw request body, keyed by the
target's secret. Sign the bytes you received, not a re-serialized copy of the parsed JSON, or a difference in key order
or spacing breaks the match:

```python
import hashlib
import hmac


def verify(secret: str, headers, body: bytes) -> bool:
    message = f"{headers['x-peryx-timestamp']}.{headers['x-peryx-delivery']}.".encode() + body
    expected = "sha256=" + hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, headers["x-peryx-signature"])
```

Compare in constant time, as `hmac.compare_digest` does, so a mismatched signature does not leak where it diverged.
Because the timestamp is signed and sent, you can also reject a delivery whose `x-peryx-timestamp` is far from your own
clock to blunt replay of a captured request.

## Delivery guarantees to design around

peryx queues each delivery in its metadata database and sends it from a background worker, so a slow or unreachable
endpoint never stalls the upload and a pending delivery survives a restart.

{% mermaid() %}
sequenceDiagram
    participant Client
    participant peryx
    participant Queue as Delivery queue
    participant You as Your endpoint
    Client->>peryx: upload / delete / yank
    peryx-->>Client: 200, request done
    peryx->>Queue: enqueue signed delivery
    Queue->>You: POST (attempt 1)
    You-->>Queue: non-2xx or timeout
    Queue->>You: POST (retry, capped backoff)
    You-->>Queue: 2xx, delivered
{% end %}

Design your endpoint around these properties:

- Any `2xx` marks a delivery done. Return one only after you have durably accepted the event; return non-`2xx` to have
  it retried.
- A failed delivery retries up to five attempts with backoff of 5, 15, 45, and 135 seconds, then stops. Each attempt
  times out after ten seconds, so keep your handler fast and do slow work asynchronously.
- Deliveries are at-least-once. A slow `2xx` that peryx recorded as a timeout arrives again, so dedupe on
  `x-peryx-delivery`, which stays constant across retries.
- Ordering is not guaranteed. Two changes to the same project can arrive out of order under retry, so treat each event
  as a fact about a moment, keyed by `created_at`, rather than a step in a sequence.

Rotate a secret by pointing `secret_env` at a new value and reloading; deliveries signed before the change fail
verification, so drain the queue during a quiet window or accept a short retry storm. The delivery log records the
target, attempt count, next retry, response status, and last error for troubleshooting, and it never stores the secret
or the signature. Watch it alongside [the rest of your logging](@/core/logging.md); the `peryx::webhook` target carries
every delivery outcome.
