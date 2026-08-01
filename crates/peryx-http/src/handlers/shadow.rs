//! Operator inspection of virtual-repository shadowing.
//!
//! One neutral query ([`AppState::query_shadowed`](peryx_driver::state::AppState::query_shadowed))
//! replays a virtual repository's resolution of a project and returns the selected candidate for each
//! filename plus every candidate a member shadowed. Repository authorization gates the whole response,
//! so a caller who cannot read the repository learns nothing — not a member name, filename, or digest.
//! The candidates carry no upstream URLs. Responses never enter a shared cache.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Request, StatusCode, Uri, header};
use axum::response::{IntoResponse as _, Response};
use peryx_core::{ShadowCandidate, ShadowReason};
use peryx_driver::authz::{Decision, DenyReason, ScopedDecision};
use peryx_driver::shadow::{ShadowPage, ShadowQuery, ShadowQueryError};
use peryx_driver::state::AppState;
use peryx_identity::{Action, Denial, Resource, Scope, UserId, authorize_all, parse_basic};

use crate::response_security::{
    ClassifiedField, FieldClassification, ProtectedCachePolicy, ResponseAuthorization, filter_fields,
};

#[derive(Debug, serde::Deserialize)]
pub struct ShadowParams {
    repository: String,
    project: String,
    cursor: Option<String>,
    limit: Option<usize>,
}

/// `GET /+shadow/candidates`: the selected and shadowed candidates for one project in a virtual
/// repository.
pub async fn shadow_candidates(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (request, _) = request.into_parts();
    let mut response = shadow_candidates_response(&state, &request.headers, &request.uri).await;
    ProtectedCachePolicy::NoStore.apply(response.headers_mut());
    response
}

async fn shadow_candidates_response(state: &AppState, headers: &HeaderMap, uri: &Uri) -> Response {
    let identity = match authenticate(state, headers).await {
        Ok(identity) => identity,
        Err(rejection) => return rejection.response(),
    };
    let Ok(Query(params)) = Query::<ShadowParams>::try_from_uri(uri) else {
        return invalid_query();
    };
    let authorization = match authorize(state, headers, &params.repository, &identity) {
        Ok(authorization) => authorization,
        Err(rejection) => return rejection.response(),
    };
    let query = ShadowQuery {
        repository: authorization.repository,
        project: params.project,
        cursor: params.cursor,
        limit: params.limit.unwrap_or(25),
    };
    match state.query_shadowed(&query) {
        Ok(page) => shadow_page(page, authorization.response),
        Err(error) => shadow_error_response(&error),
    }
}

#[derive(Debug)]
enum ShadowIdentity {
    Local(UserId),
    LegacyToken,
}

#[derive(Debug)]
struct ShadowAuthorization {
    repository: String,
    response: ResponseAuthorization,
}

#[derive(Debug, Clone, Copy)]
enum ShadowRejection {
    Forbidden,
    NotFound,
    Unavailable,
    Unauthorized,
}

impl ShadowRejection {
    fn response(self) -> Response {
        match self {
            Self::Forbidden => StatusCode::FORBIDDEN.into_response(),
            Self::NotFound => StatusCode::NOT_FOUND.into_response(),
            Self::Unavailable => unavailable(),
            Self::Unauthorized => unauthorized(),
        }
    }
}

async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<ShadowIdentity, ShadowRejection> {
    let credentials = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_basic)
        .ok_or(ShadowRejection::Unauthorized)?;
    if credentials.user == "__token__" {
        return Ok(ShadowIdentity::LegacyToken);
    }
    state
        .users
        .authenticate(&credentials.user, &credentials.password)
        .await
        .map_err(|_| ShadowRejection::Unavailable)?
        .map(ShadowIdentity::Local)
        .ok_or(ShadowRejection::Unauthorized)
}

fn authorize(
    state: &AppState,
    headers: &HeaderMap,
    route: &str,
    identity: &ShadowIdentity,
) -> Result<ShadowAuthorization, ShadowRejection> {
    match identity {
        ShadowIdentity::Local(actor) => authorize_local(state, actor, route),
        ShadowIdentity::LegacyToken => authorize_legacy(state, headers, route),
    }
}

/// A caller who can read the repository may inspect how it resolves a project; the operator role, which
/// carries no repository access, cannot.
fn authorize_local(state: &AppState, actor: &UserId, route: &str) -> Result<ShadowAuthorization, ShadowRejection> {
    let index = index_by_route(state, route)?;
    let authorization =
        state
            .authorization
            .authorize_scoped(actor, Scope::RepositoryRead, &Resource::Repository(index.name.clone()));
    require_permission(authorization)?;
    Ok(ShadowAuthorization {
        repository: index.name.clone(),
        response: ResponseAuthorization::Scoped(authorization),
    })
}

fn authorize_legacy(
    state: &AppState,
    headers: &HeaderMap,
    route: &str,
) -> Result<ShadowAuthorization, ShadowRejection> {
    let index = state
        .indexes
        .iter()
        .find(|index| index.route == route)
        .ok_or(ShadowRejection::Unauthorized)?;
    let authorization = headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok());
    let principal = index.acl.identify(authorization, (state.clock)()).principal;
    match authorize_all(&principal, &index.acl, Action::Write) {
        Ok(()) => Ok(ShadowAuthorization {
            repository: index.name.clone(),
            response: ResponseAuthorization::Repository,
        }),
        Err(Denial::Forbidden) => Err(ShadowRejection::Forbidden),
        Err(Denial::Unavailable | Denial::Unauthenticated) => Err(ShadowRejection::Unauthorized),
    }
}

const fn require_permission(authorization: ScopedDecision) -> Result<(), ShadowRejection> {
    match authorization.decision() {
        Decision::Allow => Ok(()),
        Decision::Deny(DenyReason::NoGrant) => Err(ShadowRejection::NotFound),
        Decision::Deny(DenyReason::StorageUnavailable) => Err(ShadowRejection::Unavailable),
    }
}

fn index_by_route<'state>(
    state: &'state AppState,
    route: &str,
) -> Result<&'state peryx_driver::state::Index, ShadowRejection> {
    state
        .indexes
        .iter()
        .find(|index| index.route == route)
        .ok_or(ShadowRejection::NotFound)
}

#[derive(serde::Serialize)]
struct ShadowCandidateResponse {
    member: String,
    source: &'static str,
    filename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    digest: Option<String>,
    selected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

impl From<ShadowCandidate> for ShadowCandidateResponse {
    fn from(candidate: ShadowCandidate) -> Self {
        Self {
            member: candidate.member,
            source: candidate.source.as_str(),
            filename: candidate.filename,
            digest: candidate.digest,
            selected: candidate.selected,
            reason: candidate.reason.map(ShadowReason::as_str),
        }
    }
}

fn shadow_page(page: ShadowPage, authorization: ResponseAuthorization) -> Response {
    let candidates = page
        .candidates
        .into_iter()
        .map(ShadowCandidateResponse::from)
        .collect::<Vec<_>>();
    axum::Json(serde_json::Value::Object(
        filter_fields(
            authorization,
            [
                ClassifiedField::new(
                    "candidates",
                    FieldClassification::Repository,
                    serde_json::json!(candidates),
                ),
                ClassifiedField::new(
                    "next_cursor",
                    FieldClassification::Repository,
                    serde_json::json!(page.next_cursor),
                ),
            ],
        )
        .expect("authorization passed before the shadow query"),
    ))
    .into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"peryx-shadow\"")],
    )
        .into_response()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({"error": "shadow inspection service unavailable"})),
    )
        .into_response()
}

fn invalid_query() -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(serde_json::json!({"error": "invalid shadow query"})),
    )
        .into_response()
}

/// Keep validation failures actionable without exposing storage details.
#[must_use]
pub fn shadow_error_response(error: &ShadowQueryError) -> Response {
    let (status, message) = match error {
        ShadowQueryError::InvalidLimit | ShadowQueryError::InvalidCursor | ShadowQueryError::ProjectTooLong => {
            (StatusCode::BAD_REQUEST, error.to_string())
        }
        ShadowQueryError::Store(_) => (StatusCode::INTERNAL_SERVER_ERROR, "shadow query failed".to_owned()),
    };
    (status, axum::Json(serde_json::json!({"error": message}))).into_response()
}
