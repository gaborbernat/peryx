//! Server-level operations: discovery, search, status, stats, metrics, and this document.

use serde_json::json;
use utoipa::openapi::content::ContentBuilder;
use utoipa::openapi::path::{HttpMethod, OperationBuilder, ParameterBuilder, ParameterIn, PathItemBuilder};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::{PathsBuilder, Required, ResponseBuilder, SecurityRequirement};

use peryx_driver::openapi::{api_json_response, package_search, text_response};

/// Register the `/+analytics/*` usage query family, kept apart so the service path list stays short.
fn analytics_paths(paths: PathsBuilder) -> PathsBuilder {
    paths
        .path(
            "/+analytics/top-packages",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, analytics_top())
                .build(),
        )
        .path(
            "/+analytics/unused",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, analytics_unused())
                .build(),
        )
        .path(
            "/+analytics/versions",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, analytics_versions())
                .build(),
        )
        .path(
            "/+analytics/sources",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, analytics_sources())
                .build(),
        )
        .path(
            "/+analytics/timeline",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, analytics_timeline())
                .build(),
        )
}

pub(super) fn service_paths(paths: PathsBuilder) -> PathsBuilder {
    analytics_paths(paths)
        .path(
            "/+status",
            PathItemBuilder::new().operation(HttpMethod::Get, status()).build(),
        )
        .path(
            "/+health",
            PathItemBuilder::new().operation(HttpMethod::Get, health()).build(),
        )
        .path(
            "/+ready",
            PathItemBuilder::new().operation(HttpMethod::Get, readiness()).build(),
        )
        .path(
            "/+acl",
            PathItemBuilder::new().operation(HttpMethod::Get, acl()).build(),
        )
        .path(
            "/+api",
            PathItemBuilder::new().operation(HttpMethod::Get, discovery()).build(),
        )
        .path(
            "/+search",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, package_search(false))
                .build(),
        )
        .path(
            "/+stats",
            PathItemBuilder::new().operation(HttpMethod::Get, stats()).build(),
        )
        .path(
            "/+policy/decisions",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, policy_decisions())
                .build(),
        )
        .path(
            "/+revocations",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, list_revocations())
                .build(),
        )
        .path(
            "/+revocations/{digest}",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, inspect_revocation())
                .operation(HttpMethod::Put, put_revocation())
                .build(),
        )
        .path(
            "/+revocations/{digest}/lift",
            PathItemBuilder::new()
                .operation(HttpMethod::Post, lift_revocation())
                .build(),
        )
        .path(
            "/metrics",
            PathItemBuilder::new().operation(HttpMethod::Get, metrics()).build(),
        )
        .path(
            "/api-docs/openapi.json",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, openapi_endpoint())
                .build(),
        )
        .path(
            "/_/oidc/audience",
            PathItemBuilder::new()
                .operation(HttpMethod::Get, oidc_audience())
                .build(),
        )
        .path(
            "/_/oidc/mint-token",
            PathItemBuilder::new()
                .operation(HttpMethod::Post, oidc_mint_token())
                .build(),
        )
}

fn acl() -> OperationBuilder {
    OperationBuilder::new()
        .tag("operations")
        .summary(Some("An index's access control"))
        .description(Some(
            "The tokens, grants, expiry, and anonymous-read policy one index is configured with. peryx \
             has no server-wide administrator, so the gate is the index's own: authenticate with HTTP \
             Basic as a token holding write over every project here (the `upload_token` standing). Token \
             secrets are never returned, only a marker that one is set.",
        ))
        .security(SecurityRequirement::new("uploadToken", Vec::<String>::new()))
        .parameter(
            ParameterBuilder::new()
                .name("index")
                .parameter_in(ParameterIn::Query)
                .required(Required::True)
                .description(Some("The route of the index to describe"))
                .example(Some(json!("hosted"))),
        )
        .response(
            "200",
            api_json_response(
                "The index's tokens and read policy, secrets redacted",
                json!({
                    "index": "hosted",
                    "route": "hosted",
                    "anonymous_read": true,
                    "tokens": [
                        {"name": "upload_token", "secret": {"configured": true, "redacted": "<redacted>"},
                         "expires_at": null, "grants": [{"projects": ["*"], "actions": ["write", "delete"]}]},
                        {"name": "ci", "secret": {"configured": true, "redacted": "<redacted>"},
                         "expires_at": 1_800_000_000, "grants": [{"projects": ["team/*"], "actions": ["read"]}]}
                    ]
                }),
            ),
        )
        .response(
            "401",
            ResponseBuilder::new().description("No credential the index accepts was presented"),
        )
        .response(
            "403",
            ResponseBuilder::new().description("The credential does not administer this index"),
        )
        .response("404", ResponseBuilder::new().description("No index has this route"))
}

fn discovery() -> OperationBuilder {
    OperationBuilder::new()
        .tag("discovery")
        .summary(Some("Discover this server"))
        .description(Some(
            "A compact server document with global URLs and one discovery entry per configured \
             index. It is built from configuration and request context, without reading package \
             indexes.",
        ))
        .response(
            "200",
            api_json_response(
                "The server discovery document",
                json!({
                    "version": "0.0.1",
                    "urls": {
                        "api": "http://127.0.0.1:4433/+api",
                        "health": "http://127.0.0.1:4433/+health",
                        "readiness": "http://127.0.0.1:4433/+ready",
                        "status": "http://127.0.0.1:4433/+status",
                        "stats": "http://127.0.0.1:4433/+stats",
                        "openapi": "http://127.0.0.1:4433/api-docs/openapi.json",
                        "web": "http://127.0.0.1:4433/"
                    },
                    "indexes": []
                }),
            ),
        )
}

fn status() -> OperationBuilder {
    OperationBuilder::new()
        .tag("operations")
        .summary(Some("Health and identity"))
        .description(Some(
            "Version, counters, and the configured indexes. Add `?details=admin` for bounded metadata \
             summaries used by the read-only admin status page.",
        ))
        .parameter(
            ParameterBuilder::new()
                .name("details")
                .parameter_in(ParameterIn::Query)
                .description(Some(
                    "Use `admin` to include observed project counts, uploaded file counts, and recent uploads.",
                ))
                .example(Some(json!("admin"))),
        )
        .response(
            "200",
            ResponseBuilder::new().description("The status document").content(
                "application/json",
                ContentBuilder::new()
                    .example(Some(json!({
                        "version": env!("CARGO_PKG_VERSION"),
                        "serial": 42,
                        "role": "writer",
                        "health": {
                            "serving_reads": true,
                            "accepting_writes": true,
                            "metadata_store": "healthy",
                            "blob_store": "healthy",
                            "upstreams": {"reachable": 1, "unreachable": 0, "unknown": 0, "disabled": 0}
                        },
                        "requests": 128,
                        "by_ecosystem": [
                            {"ecosystem": "pypi", "pages": 128, "downloads": 6, "bytes": 64_733_247,
                             "rejected": 0, "uploads": 4, "families": {"metadata": 37}}
                        ],
                        "metric_families": [
                            {"key": "metadata", "label": "PEP 658 metadata hits",
                             "roles": ["cached", "hosted", "virtual"]}
                        ],
                        "indexes": [
                            {"name": "pypi", "route": "pypi", "kind": "cached", "layers": [],
                             "uploads": false, "volatile_deletes": false, "upload_to": null,
                             "upstream": {"url": "https://pypi.org/simple/", "auth": {"kind": "none", "redacted": null}, "status": "configured", "offline": false},
                             "hosted": null, "project_count": 128, "upload_count": 0, "recent_uploads": []},
                            {"name": "hosted", "route": "hosted", "kind": "hosted", "layers": [],
                             "uploads": true, "volatile_deletes": true, "upload_to": null, "upstream": null,
                             "hosted": {"volatile": true, "upload_token": {"configured": true, "redacted": "<redacted>"}},
                             "project_count": 2, "upload_count": 4,
                             "recent_uploads": [{"project": "peryxpkg", "filename": "peryxpkg-1.0-py3-none-any.whl",
                                                "version": "1.0", "uploaded_at": "2026-01-01T00:00:00Z", "size": 1832}]},
                            {"name": "root/pypi", "route": "root/pypi", "kind": "virtual",
                             "layers": ["hosted", "pypi"], "uploads": true, "volatile_deletes": true,
                             "upload_to": "hosted",
                             "upstream": null, "hosted": null, "project_count": 0, "upload_count": 0,
                             "recent_uploads": []}
                        ]
                    })))
                    .build(),
            ),
        )
}

fn health() -> OperationBuilder {
    OperationBuilder::new()
        .tag("operations")
        .summary(Some("Process liveness"))
        .description(Some(
            "Returns a fixed public document while the HTTP process can answer requests. Local-store and upstream failures do not fail liveness.",
        ))
        .response(
            "200",
            api_json_response("The process is live", json!({"status": "live"})),
        )
}

fn readiness() -> OperationBuilder {
    OperationBuilder::new()
        .tag("operations")
        .summary(Some("Read or write readiness"))
        .description(Some(
            "Checks the bounded local metadata and blob-store dependencies used to serve package requests. Set `writes=true` to require a writer. The probe does not enumerate repositories or contact upstreams.",
        ))
        .parameter(
            ParameterBuilder::new()
                .name("writes")
                .parameter_in(ParameterIn::Query)
                .description(Some("Require the node to accept writes"))
                .example(Some(json!(true))),
        )
        .response(
            "200",
            api_json_response("The requested traffic class is ready", json!({"status": "ready"})),
        )
        .response(
            "503",
            api_json_response(
                "A required local dependency is unavailable or write traffic reached a replica",
                json!({"status": "not_ready"}),
            ),
        )
}

fn stats() -> OperationBuilder {
    OperationBuilder::new()
        .tag("operations")
        .summary(Some("Usage statistics"))
        .description(Some(
            "Counters aggregated off the request path, drillable: no parameters for per-index totals, \
             `?index={route}` for one index's projects, `&project={name}` for one project's files. \
             Counters are grouped by the role that owns them: a neutral `base` group every index \
             reports, a `cached` group only a caching index fills, a `hosted` group only an upload \
             store fills, and an `ecosystem` map of the driver's own counters (PyPI's PEP 658 \
             sibling under `metadata`).",
        ))
        .parameter(
            ParameterBuilder::new()
                .name("index")
                .parameter_in(ParameterIn::Query)
                .description(Some("Drill into one index's projects"))
                .example(Some(json!("root/pypi"))),
        )
        .parameter(
            ParameterBuilder::new()
                .name("project")
                .parameter_in(ParameterIn::Query)
                .description(Some("With `index`, drill into one project's files"))
                .example(Some(json!("pandas"))),
        )
        .response(
            "200",
            ResponseBuilder::new()
                .description("The counters at the requested depth")
                .content(
                    "application/json",
                    ContentBuilder::new()
                        .example(Some(json!({
                            "root/pypi": {
                                "base": {"pages": 12, "downloads": 6, "bytes": 64_733_247, "rejected": 0},
                                "cached": {"refreshes": 2, "changed": 1, "stale_served": 0, "upstream_errors": 0},
                                "hosted": {"uploads": 0},
                                "ecosystem": {"metadata": 6}
                            }
                        })))
                        .build(),
                ),
        )
}

/// The example resolved window every analytics response echoes back.
fn analytics_interval() -> serde_json::Value {
    json!({
        "from_day": 19_722,
        "to_day": 19_752,
        "from_unix": 1_703_980_800_i64,
        "to_unix": 1_706_659_200_i64,
        "retained_from_day": 19_387,
        "window_clamped_to_retention": false,
    })
}

/// The query parameters, security, and failure responses shared by every `/+analytics/*` view. An
/// operator query omits `repository`; a repository query names an index route the caller can read.
fn analytics_query(operation: OperationBuilder) -> OperationBuilder {
    let mut operation = operation
        .tag("operations")
        .security(SecurityRequirement::new("uploadToken", Vec::<String>::new()))
        .security(SecurityRequirement::new("administratorPassword", Vec::<String>::new()))
        .response(
            "400",
            api_json_response(
                "The limit, cursor, time range, or repository filter is invalid",
                json!({"error": "limit must be between 1 and 100"}),
            ),
        )
        .response(
            "401",
            ResponseBuilder::new().description("No valid local user credential or repository token was presented"),
        )
        .response(
            "403",
            ResponseBuilder::new().description("The credential cannot inspect this view or repository"),
        )
        .response(
            "404",
            ResponseBuilder::new().description("The repository does not exist or is not visible to the caller"),
        )
        .response(
            "503",
            api_json_response(
                "Authentication, authorization, or analytics storage is unavailable",
                json!({"error": "analytics service unavailable"}),
            ),
        );
    for (name, description, example) in [
        (
            "repository",
            "Index route to scope the query to; omit for an operator-wide query, at most 512 bytes",
            json!("root/pypi"),
        ),
        (
            "from",
            "Minimum Unix timestamp, floored to its UTC day",
            json!(1_703_980_800_i64),
        ),
        (
            "to",
            "Maximum Unix timestamp, floored to its UTC day",
            json!(1_706_659_200_i64),
        ),
        (
            "cursor",
            "Opaque cursor from the prior page's next_cursor",
            json!("MjU"),
        ),
        ("limit", "Rows to return, from 1 through 100; defaults to 25", json!(25)),
    ] {
        operation = operation.parameter(
            ParameterBuilder::new()
                .name(name)
                .parameter_in(ParameterIn::Query)
                .description(Some(description))
                .example(Some(example)),
        );
    }
    operation
}

fn analytics_top() -> OperationBuilder {
    analytics_query(
        OperationBuilder::new()
            .summary(Some("Most-downloaded packages"))
            .description(Some(
                "Downloads and bytes grouped by repository and project over the resolved window, ordered by \
                 downloads, bytes, repository, then project.",
            )),
    )
    .response(
        "200",
        api_json_response(
            "The highest-usage projects, newest window first",
            json!({
                "packages": [{"repository": "root/pypi", "project": "pandas", "downloads": 42, "bytes": 64_733_247}],
                "interval": analytics_interval(),
                "next_cursor": null,
            }),
        ),
    )
}

fn analytics_unused() -> OperationBuilder {
    analytics_query(
        OperationBuilder::new()
            .summary(Some("Unused packages"))
            .description(Some(
                "Projects with durable lifetime downloads but none inside the window, ordered by lifetime \
                 downloads, repository, then project. A `window_clamped_to_retention` interval marks results \
                 assessed only over retained data.",
            )),
    )
    .response(
        "200",
        api_json_response(
            "Projects idle across the window",
            json!({
                "unused": [{"repository": "root/pypi", "project": "legacy-tool", "lifetime_downloads": 7}],
                "interval": analytics_interval(),
                "next_cursor": null,
            }),
        ),
    )
}

fn analytics_versions() -> OperationBuilder {
    analytics_query(
        OperationBuilder::new()
            .summary(Some("Per-version usage"))
            .description(Some(
                "Downloads and bytes grouped by repository, project, and version over the window. A null \
                 version is a distribution the ecosystem reported no version for.",
            )),
    )
    .response(
        "200",
        api_json_response(
            "The highest-usage versions",
            json!({
                "versions": [
                    {"repository": "root/pypi", "project": "pandas", "version": "2.2.0", "downloads": 30, "bytes": 48_000_000}
                ],
                "interval": analytics_interval(),
                "next_cursor": null,
            }),
        ),
    )
}

fn analytics_sources() -> OperationBuilder {
    analytics_query(
        OperationBuilder::new()
            .summary(Some("Per-source usage"))
            .description(Some(
                "Downloads and bytes grouped by the routed upstream a cache miss fetched from; a null source is \
                 the local store. The source dimension is operator-scoped, so a repository-only credential \
                 cannot inspect this view.",
            )),
    )
    .response(
        "200",
        api_json_response(
            "The highest-usage sources",
            json!({
                "sources": [
                    {"repository": "root/pypi", "project": "pandas", "source": "pypi", "downloads": 40, "bytes": 60_000_000}
                ],
                "interval": analytics_interval(),
                "next_cursor": null,
            }),
        ),
    )
}

fn analytics_timeline() -> OperationBuilder {
    analytics_query(
        OperationBuilder::new()
            .summary(Some("Usage over time"))
            .description(Some(
                "Downloads and bytes bucketed by UTC day, ascending, each carrying explicit half-open \
                 `[start_unix, end_unix)` bounds for the day it aggregates.",
            )),
    )
    .response(
        "200",
        api_json_response(
            "The daily usage series",
            json!({
                "buckets": [
                    {"day": 19_752, "start_unix": 1_706_572_800_i64, "end_unix": 1_706_659_200_i64, "downloads": 12, "bytes": 9_000_000}
                ],
                "interval": analytics_interval(),
                "next_cursor": null,
            }),
        ),
    )
}

fn policy_decisions() -> OperationBuilder {
    let mut operation = OperationBuilder::new()
        .tag("operations")
        .summary(Some("Repository policy decisions"))
        .description(Some(
            "Bounded policy decision history. Administrators may inspect all repositories or select one. Repository \
             readers and publishers may inspect a selected repository they can read; the server operator role carries \
             no repository access. A repository's legacy upload token retains access to that repository when presented \
             with the `__token__` username. Records contain package subjects and matched rule IDs without credentials \
             or request headers. `fresh` is false after repository data, catalog, or policy inputs change.",
        ))
        .security(SecurityRequirement::new("uploadToken", Vec::<String>::new()))
        .security(SecurityRequirement::new("administratorPassword", Vec::<String>::new()))
        .response(
            "200",
            api_json_response(
                "The matching decisions, newest first",
                json!({
                    "decisions": [{
                        "id": "550e8400-e29b-41d4-a716-446655440000",
                        "repository": "private",
                        "project": "example",
                        "version": "1.0",
                        "filename": "example-1.0-py3-none-any.whl",
                        "source": "pypi",
                        "action": "serve",
                        "state": "deny",
                        "rule": "blocked-project",
                        "reason": "project is blocked",
                        "evaluated_at_unix": 1_800_000_000,
                        "input_generation": {"repository": 42, "catalog": 7, "policy": 3},
                        "next_eligible_at_unix": null,
                        "fresh": true
                    }],
                    "next_cursor": "pd_000000000000002a"
                }),
            ),
        )
        .response(
            "400",
            api_json_response(
                "The limit, cursor, or text filter is invalid",
                json!({"error": "limit must be between 1 and 100"}),
            ),
        )
        .response(
            "401",
            ResponseBuilder::new().description("No valid local user credential or repository token was presented"),
        )
        .response(
            "403",
            ResponseBuilder::new().description("The repository token cannot inspect policy decisions"),
        )
        .response(
            "404",
            ResponseBuilder::new().description("The repository does not exist or is not available to the local user"),
        )
        .response(
            "500",
            api_json_response(
                "The decision store could not complete the query",
                json!({"error": "policy decision query failed"}),
            ),
        )
        .response(
            "503",
            api_json_response(
                "Authentication or authorization storage is unavailable",
                json!({"error": "policy decision service unavailable"}),
            ),
        );
    for (name, description, example) in [
        (
            "repository",
            "Repository route to inspect, at most 512 bytes",
            json!("private"),
        ),
        ("state", "Filter by `allow`, `deny`, or `wait`", json!("deny")),
        (
            "rule",
            "Filter by matched rule ID, at most 512 bytes",
            json!("blocked-project"),
        ),
        ("source", "Filter by routed source, at most 512 bytes", json!("pypi")),
        ("from", "Minimum evaluation Unix timestamp", json!(1_700_000_000)),
        ("to", "Maximum evaluation Unix timestamp", json!(1_800_000_000)),
        (
            "cursor",
            "Exclusive cursor from the prior page",
            json!("pd_000000000000002a"),
        ),
        ("limit", "Rows to return, from 1 through 100; defaults to 25", json!(25)),
    ] {
        let parameter = ParameterBuilder::new()
            .name(name)
            .parameter_in(ParameterIn::Query)
            .description(Some(description))
            .example(Some(example));
        operation = operation.parameter(parameter);
    }
    operation
}

fn revocation_example() -> serde_json::Value {
    json!({
        "digest": {"sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
        "reason": "compromised build host",
        "created_by": "usr_550e8400e29b41d4a716446655440000",
        "created_at_unix": 1_800_000_000,
        "state": {"status": "active"},
        "revision": 1
    })
}

fn digest_parameter() -> utoipa::openapi::path::Parameter {
    ParameterBuilder::new()
        .name("digest")
        .parameter_in(ParameterIn::Path)
        .required(Required::True)
        .description(Some("Canonical `sha256:<64 lowercase hex>` artifact digest"))
        .example(Some(json!(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        )))
        .build()
}

fn administrator_errors(operation: OperationBuilder) -> OperationBuilder {
    operation
        .response(
            "401",
            ResponseBuilder::new().description("No valid local user credential was presented"),
        )
        .response(
            "404",
            ResponseBuilder::new().description("The caller cannot discover this record, or it does not exist"),
        )
        .response(
            "503",
            api_json_response(
                "Authentication, authorization, or revocation storage is unavailable",
                json!({"error": "revocation service unavailable"}),
            ),
        )
}

fn inspect_revocation() -> OperationBuilder {
    administrator_errors(
        OperationBuilder::new()
            .tag("operations")
            .summary(Some("Inspect a digest revocation"))
            .description(Some(
                "Returns the current lifecycle record without changing package yank, trash, retention, or policy state.",
            ))
            .security(SecurityRequirement::new(
                "administratorPassword",
                Vec::<String>::new(),
            ))
            .parameter(digest_parameter())
            .response("200", api_json_response("The current revocation record", revocation_example()))
            .response(
                "400",
                api_json_response("The digest is not canonical SHA-256", json!({"error": "invalid digest"})),
            ),
    )
}

fn list_revocations() -> OperationBuilder {
    let mut operation = administrator_errors(
        OperationBuilder::new()
            .tag("operations")
            .summary(Some("List digest revocations"))
            .description(Some(
                "Returns a bounded page of current records in canonical digest order. Lifted records remain visible to administrators.",
            ))
            .security(SecurityRequirement::new(
                "administratorPassword",
                Vec::<String>::new(),
            ))
            .response(
                "200",
                api_json_response(
                    "The matching current records",
                    json!({"revocations": [revocation_example()], "next_cursor": null}),
                ),
            )
            .response(
                "400",
                api_json_response(
                    "The cursor or limit is invalid",
                    json!({"error": "invalid revocation cursor"}),
                ),
            ),
    );
    for (name, description, example) in [
        ("status", "Filter by `active` or `lifted`", json!("active")),
        (
            "cursor",
            "Exclusive canonical digest from the prior page",
            json!("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
        ),
        ("limit", "Rows to return, from 1 through 100; defaults to 25", json!(25)),
    ] {
        operation = operation.parameter(
            ParameterBuilder::new()
                .name(name)
                .parameter_in(ParameterIn::Query)
                .description(Some(description))
                .example(Some(example)),
        );
    }
    operation
}

fn put_revocation() -> OperationBuilder {
    administrator_errors(
        OperationBuilder::new()
            .tag("operations")
            .summary(Some("Put an active digest revocation"))
            .description(Some(
                "Creates or reopens the digest-addressed record. Retrying the same active reason is idempotent; replacing another active reason conflicts.",
            ))
            .security(SecurityRequirement::new(
                "administratorPassword",
                Vec::<String>::new(),
            ))
            .parameter(digest_parameter())
            .request_body(Some(
                RequestBodyBuilder::new()
                    .required(Some(Required::True))
                    .content(
                        "application/json",
                        ContentBuilder::new()
                            .example(Some(json!({"reason": "compromised build host"})))
                            .build(),
                    )
                    .build(),
            ))
            .response("200", api_json_response("The unchanged active record", revocation_example()))
            .response("201", api_json_response("The created or reopened record", revocation_example()))
            .response(
                "400",
                api_json_response("The digest or reason is invalid", json!({"error": "invalid digest"})),
            )
            .response(
                "409",
                api_json_response(
                    "The active record has another reason",
                    json!({"error": "digest is already revoked"}),
                ),
            )
            .response("413", ResponseBuilder::new().description("The request exceeds the fixed body limit"))
            .response("415", ResponseBuilder::new().description("The request is not JSON"))
            .response("422", ResponseBuilder::new().description("The JSON request body is invalid")),
    )
}

fn lift_revocation() -> OperationBuilder {
    administrator_errors(
        OperationBuilder::new()
            .tag("operations")
            .summary(Some("Lift a digest revocation"))
            .description(Some(
                "Transitions an active record to lifted and retains its original reason, actor, and creation time. Retrying a lift is idempotent.",
            ))
            .security(SecurityRequirement::new(
                "administratorPassword",
                Vec::<String>::new(),
            ))
            .parameter(digest_parameter())
            .response(
                "200",
                api_json_response(
                    "The lifted record",
                    json!({
                        "digest": {"sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
                        "reason": "compromised build host",
                        "created_by": "usr_550e8400e29b41d4a716446655440000",
                        "created_at_unix": 1_800_000_000,
                        "state": {
                            "status": "lifted",
                            "lifted_by": "usr_98b2271831d647c09a1e6f630cc48ef7",
                            "lifted_at_unix": 1_800_000_100
                        },
                        "revision": 2
                    }),
                ),
            )
            .response(
                "400",
                api_json_response("The digest is not canonical SHA-256", json!({"error": "invalid digest"})),
            ),
    )
}

fn metrics() -> OperationBuilder {
    OperationBuilder::new()
        .tag("operations")
        .summary(Some("Prometheus metrics"))
        .response(
            "200",
            text_response(
                "Prometheus text exposition",
                "text/plain; version=0.0.4",
                "# HELP peryx_requests_total Total HTTP requests served.\n\
                 # TYPE peryx_requests_total counter\n\
                 peryx_requests_total 128\n\
                 # HELP peryx_metadata_served_total PEP 658 metadata siblings served.\n\
                 # TYPE peryx_metadata_served_total counter\n\
                 peryx_metadata_served_total{ecosystem=\"pypi\",role=\"virtual\"} 37\n",
            ),
        )
}

fn openapi_endpoint() -> OperationBuilder {
    OperationBuilder::new()
        .tag("operations")
        .summary(Some("This document"))
        .response(
            "200",
            ResponseBuilder::new()
                .description("The OpenAPI 3.1 description of this server")
                .content("application/json", ContentBuilder::new().build()),
        )
}

fn oidc_audience() -> OperationBuilder {
    OperationBuilder::new()
        .tag("trusted publishing")
        .summary(Some("Discover the CI identity audience"))
        .description(Some(
            "Peryx returns the audience a CI provider must put in its OIDC identity. Peryx adds this route after an operator configures a trusted publisher.",
        ))
        .response(
            "200",
            api_json_response(
                "The configured OIDC audience",
                json!({"audience": "packages.example"}),
            ),
        )
        .response("404", ResponseBuilder::new().description("No trusted publisher exists"))
}

fn oidc_mint_token() -> OperationBuilder {
    OperationBuilder::new()
        .tag("trusted publishing")
        .summary(Some("Exchange a CI identity for an upload token"))
        .description(Some(
            "Peryx verifies one external OIDC identity against a configured publisher and returns a short-lived token restricted to that publisher's repository and projects.",
        ))
        .request_body(Some(
            RequestBodyBuilder::new()
                .required(Some(Required::True))
                .content(
                    "application/json",
                    ContentBuilder::new()
                        .example(Some(json!({"token": "eyJhbGciOiJSUzI1NiIs..."})))
                        .build(),
                )
                .build(),
        ))
        .response(
            "200",
            api_json_response(
                "A repository- and project-scoped upload token",
                json!({"token": "eyJhbGciOiJIUzI1NiIs...", "expires": 1_800_000_000_i64}),
            ),
        )
        .response("404", ResponseBuilder::new().description("No trusted publisher exists"))
        .response(
            "413",
            ResponseBuilder::new().description("The exchange request exceeds the fixed body limit"),
        )
        .response(
            "422",
            api_json_response(
                "The external identity is invalid or unauthorized",
                json!({"message": "identity token rejected"}),
            ),
        )
        .response(
            "503",
            api_json_response(
                "The identity provider or replay guard is unavailable",
                json!({"message": "identity provider unavailable"}),
            ),
        )
}
