+++
title = "Configure logging"
description = "Choose a level, a format, and a sink: stdout, rotating file, journald, or syslog."
weight = 9
+++

The log level comes from `--log-level {error,warn,info,debug,trace}` or the `level` key under `[log]` in the config
file. `-v` raises the level to debug and `-vv` to trace. A directive can target one module, which keeps the rest of the
output quiet:

```shell
peryx serve --log-level "info,peryx_upstream=debug"
```

peryx logs each HTTP request with its method, path, status, and latency at info, the default level, so you can watch
[pip](https://pip.pypa.io/) and [uv](https://docs.astral.sh/uv/) take the [PEP 658](https://peps.python.org/pep-0658/)
`.metadata` path, or [docker](https://www.docker.com/) fetch a manifest before its blobs, without raising verbosity.

## Sinks

Output goes to one sink, selected with `--log-sink` or `[log] sink`:

- `stdout` (default): pretty for a terminal, or one JSON object per line with `--log-format json` for log aggregation.
- `file`: a daily-rotating file at `--log-file <path>`.
- `journald`: the [systemd](https://systemd.io/) journal (Linux only).
- `syslog`: the local syslog daemon (Unix only).

In the config file:

```toml
[log]
level = "info"
format = "json"
sink = "file"
file = "/var/log/peryx/peryx.log"
```

peryx validates the combination at startup and refuses, for example, a `file` sink without a path.

## Security Events

Index actions emit structured records on the `peryx::security` target. Use JSON output when downstream tooling needs to
filter by actor, action, target, or result.

```shell
peryx serve --log-format json --log-sink file --log-file /var/log/peryx/events.log
```

Each record sets `security_event=true` and `event=index_action`. Peryx also writes `action`, `result`, `actor`, `index`,
`local_index`, `project`, `version`, `filename`, `digest`, `count`, `changed`, `reason`, `request_id`, and `user_agent`.
Missing values use empty strings or zero values. Peryx leaves credentials, bearer tokens, Basic auth passwords, and URL
secrets out of these records.

The current action names are `token_use`, `upload`, `yank`, `unyank`, `delete`, `restore`, and `mirror_sync`. Results
are `success`, `denied`, `failure`, or `noop`.

Peryx emits `event=authorization` for server-role checks. Allowed records include `user`, `scope`, `resource_kind`,
`resource`, `result`, and `reason`. Peryx omits `resource_kind` and `resource` from denied records; they contain only
the user, required scope, `result=denied`, and either `reason=no_grant` or `reason=storage_unavailable`. A rejected URL
and its query parameters stay outside the denial record.

Plain-file queries:

```shell
grep '"security_event":true' /var/log/peryx/events.log
grep '"action":"delete"' /var/log/peryx/events.log | grep '"result":"denied"'
```

JSON queries:

```shell
jq 'select(.fields.security_event == true and .fields.action == "upload")' /var/log/peryx/events.log
jq 'select(.fields.security_event == true and .fields.actor == "__token__")' /var/log/peryx/events.log
```

## Availability operation trace context

A replicated write carries a [W3C trace context](https://www.w3.org/TR/trace-context/) on its operation
envelope, so the span that authored it, the follower that applies it, and the background copy that moves
its bytes all belong to one trace. The apply side joins the author's trace rather than starting a
disconnected one, taking the envelope's `traceparent` as its parent and a fresh span id of its own, so a
restart or a home failover that replays the envelope keeps the same trace and the same operation identity.

An operation the trace sampled records one structured `availability operation` event with these fields:

| Field                   | Meaning                                                            |
| ----------------------- | ----------------------------------------------------------------- |
| `operation.source`      | The producer datacenter identity that authored the operation.     |
| `operation.epoch`       | The authority epoch the operation was admitted under.             |
| `operation.serial`      | The producer's per-operation serial, unique and idempotent.       |
| `operation.kind`        | `upload`, `yank`, `delete`, `cache-fill`, `oci-push`, `oci-delete`, or `visibility`. |
| `operation.traceparent` | The W3C traceparent that correlates the operation across nodes.   |

The fields are the operation's identity and its trace linkage alone. The change payload — the event bytes,
the metadata mutations, and the blob references — never appears, so no credential, artifact byte, or
private path reaches a log through this surface. Recording is gated on the traceparent's own sampled flag:
an operation a producer chose not to trace, or one that clears the sampled bit, emits nothing, so a burst
of writes a trace opted out of adds no event volume.

Correlate one operation across nodes by its traceparent trace-id, or by its `source`/`serial` identity:

```shell
jq 'select(.fields.message == "availability operation" and .fields."operation.serial" == 7)' /var/log/peryx/events.log
```

## Related

- Every logging flag and TOML key: [configuration](@/core/configuration.md)
- Numbers instead of lines: [monitoring](@/core/monitor.md)
