+++
title = "Availability control listener"
description = "The private, administrator-authenticated socket a dc or ha node exposes for availability control, kept off the public package routes."
weight = 8
+++

Availability controls never share the public package routes. A node running the `dc` or `ha`
[availability contract](@/core/availability-contracts.md) exposes its control surface on a second socket that
authenticates every request and admits only a server administrator. Single-node `none` opens no listener and allocates
no socket, timer, or task for it, so the default single-writer deployment pays nothing for a control plane it does not
run.

This page describes the listener's bind, transport, authentication, scopes, request limits, audit trail, and network
segmentation. The listener currently serves one read-only status endpoint; membership and transfer commands arrive in
later work behind the same gate.

## Enabling the listener

The listener is configured under the `[availability.listener]` table and requires `dc` or `ha` mode. Configuring it
under `none` is rejected, so a single-node process cannot open the control plane by accident.

```toml
[availability]
mode = "dc"

[availability.replication]
role = "primary"
source = "https://writer.internal/"
token_file = "/run/secrets/replication-token"

[availability.listener]
bind = "127.0.0.1:4460"
```

`bind` defaults to `127.0.0.1:4460`, a loopback address, so the control plane stays private until an operator widens it.
A node reads its own listener from configuration; a restored backup does not carry it, because the bind is a per-node
network fact rather than cluster state.

## Transport and network segmentation

Keep the listener on a management network an administrator reaches and package clients do not. A non-loopback `bind`
must terminate TLS or explicitly opt in to plaintext, so the control plane is never exposed to the network unencrypted
by omission:

```toml
[availability.listener]
bind = "10.0.0.5:4460"

[availability.listener.tls]
cert = "/etc/peryx/control-cert.pem"
key = "/etc/peryx/control-key.pem"
```

A non-loopback bind without a `[availability.listener.tls]` block is refused unless `allow-remote-plaintext = true`
states the intent, which suits only a trusted, isolated segment that terminates TLS in front of the node.

## Authentication and scopes

The listener reuses the same identity store as the package API; it holds no second user database. A request presents
HTTP Basic credentials for a local user, and the node admits it only when that user holds the server-wide administration
read scope over the operator resource, the same standing the operator API requires. A request without a credential, or
with an invalid one, receives `401 Unauthorized` with a `WWW-Authenticate` challenge. An authenticated user without the
administration scope receives `403 Forbidden`. Rotating a user's password immediately rejects the old one, and revoking
the administration grant immediately forbids a prior administrator.

## The status endpoint

```
GET /availability/v1/status
```

The response reports the advertised protocol version, the node's mode and authority role, and whether it currently
serves read-only:

```json
{
  "protocol_version": 1,
  "mode": "dc",
  "role": "writer",
  "read_only": false
}
```

The path carries a version segment so a client pins the protocol versions it understands and refuses an incompatible
peer rather than guessing a wire shape. An unknown path answers `404 Not Found` without consulting the identity store,
so an unauthenticated caller cannot probe the surface.

## Request limits and audit

The listener bounds each request body so a later command endpoint cannot be handed an unbounded body on the control
plane. Every admitted request records an audit line naming the actor and the path, so administrative access to the
control plane is attributable. Keep the listener behind a management-network boundary that bounds connection volume; the
node applies its request-body and authorization gates on every call.
