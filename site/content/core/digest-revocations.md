+++
title = "Digest revocations"
description = "Manage administrator-controlled artifact revocations without deleting evidence."
weight = 9
+++

A digest revocation records a server-wide decision about immutable artifact bytes. Peryx keys the current record by
canonical `sha256:<64 lowercase hex>` and returns the digest in the in-toto shape `{"sha256":"<hex>"}`. Each record
keeps its reason and lifecycle evidence, including the authenticated administrator's stable user ID.

Revocation leaves package visibility and retention unchanged. A lifecycle change does not alter yank, trash, or
repository policy state. It also leaves stored bytes and external caches intact. PyPI yanks can satisfy exact pins;
download handlers use revocation as a separate fail-closed decision.

## Manage records from the CLI

The management command calls the live HTTP API. It reads the administrator password from standard input or a secret
file; no argument or environment option accepts the secret. Input must be UTF-8 and no larger than 1 MiB. The reader
removes one terminal LF or CRLF. The client refuses redirects, applies the same 1 MiB limit to responses, and permits
plain HTTP for `localhost` and loopback IP addresses.

```console
$ digest=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
$ printf '%s\n' "$PERYX_ADMIN_PASSWORD" | peryx revocation put \
    --server https://packages.example \
    --user admin \
    --password-stdin \
    --reason 'compromised build host' \
    "$digest"
$ admin=(--server https://packages.example --user admin --password-file /run/secrets/peryx-admin-password)
$ peryx revocation inspect "${admin[@]}" "$digest"
$ peryx revocation list "${admin[@]}" --status active --limit 25
$ peryx revocation lift "${admin[@]}" "$digest"
```

The output is JSON. `put` creates an absent record. A retry with the same active reason returns the unchanged record; a
different reason conflicts. Putting a lifted digest starts another active incident with a higher revision and fresh
lifecycle evidence. Repeating `lift` leaves an existing lifted record unchanged.

## HTTP operations and authorization

The API uses local-user HTTP Basic authentication. Inspect and list require `administration:read`; put and lift require
`administration:write`. The built-in administrator role carries both. Peryx derives the actor from the authenticated
user ID and rejects actor fields in JSON.

| Operation                | Request                                               |
| ------------------------ | ----------------------------------------------------- |
| Create, retry, or reopen | `PUT /+revocations/{digest}` with `{"reason":"..."}`  |
| Inspect                  | `GET /+revocations/{digest}`                          |
| List                     | `GET /+revocations?status=active&limit=25&cursor=...` |
| Lift                     | `POST /+revocations/{digest}/lift`                    |

Reasons must contain non-whitespace text and cannot exceed 2,048 UTF-8 bytes. The administrator API is their sole read
path. Mutation security events include the stable actor and digest plus the transition result. They exclude the
free-form reason and credential material.

Authentication and authorization run before digest parsing or record lookup. Invalid credentials receive one generic
challenge. Peryx returns `404 Not Found` to authenticated users without administrator authority for any target state,
including malformed input. Protected responses carry `Cache-Control: no-store`.

## Decision cache and failure behavior

The serving decision API caches clear and revoked answers in a capacity-bounded Moka cache with a 60-second TTL. Hot
hits do not lock. A cache miss holds a shared gate while it reads the digest-indexed metadata row. Mutations hold the
exclusive gate through commit and exact-digest invalidation, preventing an older clear answer from entering the cache.

The cache does not store metadata errors. A failed store read returns unavailable so download handlers can deny the
artifact. The CLI changes records through the live HTTP process and triggers exact-key invalidation. Replication and
cross-process cache purge remain outside this lifecycle. PyPI and OCI responses limit compliant client and proxy caches
to the same 60-second bound; see [revoked PyPI content](@/ecosystems/pypi/revoked-content.md) and
[revoked OCI content](@/ecosystems/oci/revoked-content.md).
