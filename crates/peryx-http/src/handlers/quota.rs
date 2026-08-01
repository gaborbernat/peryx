//! The `/+quota` read surface: an operator summary of every repository's quota, and a per-repository
//! detail a reader of that repository may fetch.
//!
//! `GET /+quota` pages over the configured indexes, so its cursor stays stable while a reservation
//! changes a counter under it, and it needs operator authority to enumerate repositories and limits.
//! `GET /+quota/repository` names one repository and answers a caller who can read it, whether a local
//! user or that repository's legacy upload token. Both read committed and reserved counter rows rather
//! than scanning artifacts, and both mark their response private so a shared cache never holds one
//! authenticated caller's view for another.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Request, StatusCode, Uri, header};
use axum::response::{IntoResponse as _, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::json;

use peryx_driver::authz::{Decision, DenyReason, ScopedDecision};
use peryx_driver::quota::repository_quota;
use peryx_driver::state::{AppState, Index};
use peryx_identity::{Action, Denial, Resource, Scope, UserId, authorize_all, parse_basic};

use crate::response_security::ProtectedCachePolicy;

const DEFAULT_LIMIT: usize = 25;
const MAX_LIMIT: usize = 100;
const MAX_REPOSITORY_BYTES: usize = 512;

/// One page of every repository's quota, newest configuration order, for an operator.
pub async fn quota_summary(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (parts, _) = request.into_parts();
    let mut response = summary_response(&state, &parts.headers, &parts.uri).await;
    ProtectedCachePolicy::Private.apply(response.headers_mut());
    response
}

/// One repository's quota for a caller who can read it.
pub async fn quota_repository(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (parts, _) = request.into_parts();
    let mut response = detail_response(&state, &parts.headers, &parts.uri).await;
    ProtectedCachePolicy::Private.apply(response.headers_mut());
    response
}

/// The selectors shared by both reads: the summary uses `cursor` and `limit`, the detail uses
/// `repository`. One struct keeps a malformed `limit` a parse error on either path.
#[derive(Debug, serde::Deserialize)]
struct QuotaParams {
    repository: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn summary_response(state: &AppState, headers: &HeaderMap, uri: &Uri) -> Response {
    let identity = match authenticate(state, headers).await {
        Ok(identity) => identity,
        Err(rejection) => return rejection.response(),
    };
    let Identity::Local(actor) = identity else {
        return Rejection::Forbidden.response();
    };
    if let Err(rejection) = require_administrator(state, &actor) {
        return rejection.response();
    }
    let Ok(Query(params)) = Query::<QuotaParams>::try_from_uri(uri) else {
        return bad_request("invalid quota query");
    };
    let (offset, limit) = match page_bounds(params.limit, params.cursor.as_deref()) {
        Ok(bounds) => bounds,
        Err(message) => return bad_request(message),
    };
    let mut rows = Vec::new();
    for index in state.indexes.iter().skip(offset).take(limit + 1) {
        match state.meta.quota_usage(&index.name) {
            Ok(usage) => rows.push(repository_quota(index, &usage)),
            Err(_) => return unavailable(),
        }
    }
    let next_cursor = (rows.len() > limit).then(|| encode_cursor(offset + limit));
    rows.truncate(limit);
    axum::Json(json!({ "repositories": rows, "next_cursor": next_cursor })).into_response()
}

async fn detail_response(state: &AppState, headers: &HeaderMap, uri: &Uri) -> Response {
    let identity = match authenticate(state, headers).await {
        Ok(identity) => identity,
        Err(rejection) => return rejection.response(),
    };
    let Ok(Query(params)) = Query::<QuotaParams>::try_from_uri(uri) else {
        return bad_request("invalid quota query");
    };
    let Some(route) = params.repository.as_deref() else {
        return bad_request("repository is required");
    };
    if route.len() > MAX_REPOSITORY_BYTES {
        return bad_request("repository filter exceeds 512 bytes");
    }
    let index = match authorize_repository(state, headers, route, &identity) {
        Ok(index) => index,
        Err(rejection) => return rejection.response(),
    };
    state.meta.quota_usage(&index.name).map_or_else(
        |_| unavailable(),
        |usage| axum::Json(repository_quota(index, &usage)).into_response(),
    )
}

fn page_bounds(limit: Option<usize>, cursor: Option<&str>) -> Result<(usize, usize), &'static str> {
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err("limit must be between 1 and 100");
    }
    let offset = match cursor {
        Some(cursor) => decode_cursor(cursor).ok_or("invalid quota cursor")?,
        None => 0,
    };
    Ok((offset, limit))
}

#[derive(Debug)]
enum Identity {
    Local(UserId),
    LegacyToken,
}

#[derive(Debug, Clone, Copy)]
enum Rejection {
    Forbidden,
    NotFound,
    Unavailable,
    Unauthorized,
}

impl Rejection {
    fn response(self) -> Response {
        match self {
            Self::Forbidden => StatusCode::FORBIDDEN.into_response(),
            Self::NotFound => StatusCode::NOT_FOUND.into_response(),
            Self::Unavailable => unavailable(),
            Self::Unauthorized => unauthorized(),
        }
    }
}

async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<Identity, Rejection> {
    let credentials = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_basic)
        .ok_or(Rejection::Unauthorized)?;
    if credentials.user == "__token__" {
        return Ok(Identity::LegacyToken);
    }
    state
        .users
        .authenticate(&credentials.user, &credentials.password)
        .await
        .map_err(|_| Rejection::Unavailable)?
        .map(Identity::Local)
        .ok_or(Rejection::Unauthorized)
}

fn require_administrator(state: &AppState, actor: &UserId) -> Result<(), Rejection> {
    require_permission(
        state
            .authorization
            .authorize_scoped(actor, Scope::AdministrationRead, &Resource::Operator),
    )
}

/// Resolve the named repository for a caller who can read it: a local user through the authorization
/// service, a legacy token through the repository's own access rules.
fn authorize_repository<'state>(
    state: &'state AppState,
    headers: &HeaderMap,
    route: &str,
    identity: &Identity,
) -> Result<&'state Index, Rejection> {
    match identity {
        Identity::Local(actor) => authorize_local(state, actor, route),
        Identity::LegacyToken => authorize_legacy(state, headers, route),
    }
}

fn authorize_local<'state>(state: &'state AppState, actor: &UserId, route: &str) -> Result<&'state Index, Rejection> {
    let index = index_by_route(state, route)?;
    let decision =
        state
            .authorization
            .authorize_scoped(actor, Scope::RepositoryRead, &Resource::Repository(index.name.clone()));
    require_permission(decision)?;
    Ok(index)
}

fn authorize_legacy<'state>(
    state: &'state AppState,
    headers: &HeaderMap,
    route: &str,
) -> Result<&'state Index, Rejection> {
    let index = state
        .indexes
        .iter()
        .find(|index| index.route == route)
        .ok_or(Rejection::Unauthorized)?;
    let authorization = headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok());
    let principal = index.acl.identify(authorization, (state.clock)()).principal;
    match authorize_all(&principal, &index.acl, Action::Read) {
        Ok(()) => Ok(index),
        Err(Denial::Forbidden) => Err(Rejection::Forbidden),
        Err(Denial::Unavailable | Denial::Unauthenticated) => Err(Rejection::Unauthorized),
    }
}

const fn require_permission(decision: ScopedDecision) -> Result<(), Rejection> {
    match decision.decision() {
        Decision::Allow => Ok(()),
        Decision::Deny(DenyReason::NoGrant) => Err(Rejection::NotFound),
        Decision::Deny(DenyReason::StorageUnavailable) => Err(Rejection::Unavailable),
    }
}

fn index_by_route<'state>(state: &'state AppState, route: &str) -> Result<&'state Index, Rejection> {
    state
        .indexes
        .iter()
        .find(|index| index.route == route)
        .ok_or(Rejection::NotFound)
}

fn encode_cursor(offset: usize) -> String {
    URL_SAFE_NO_PAD.encode(offset.to_string())
}

fn decode_cursor(cursor: &str) -> Option<usize> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).ok()?;
    std::str::from_utf8(&bytes).ok()?.parse().ok()
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": message }))).into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"peryx-quota\"")],
    )
        .into_response()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(json!({ "error": "quota service unavailable" })),
    )
        .into_response()
}
