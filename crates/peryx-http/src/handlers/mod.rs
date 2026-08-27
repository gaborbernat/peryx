//! Longest-prefix routing dispatches repository traffic without encoding ecosystem paths here.

mod acl;
mod analytics;
mod discover;
mod dispatch;
mod grants;
mod jobs;
mod login;
mod policy_decisions;
mod pql;
mod query;
mod quota;
mod repositories;
mod retention;
mod revocations;
mod status;
mod tokens;
mod trash;
mod usage;

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use mediatype::{MediaType, names};
use peryx_driver::state::{AppState, Index};
use peryx_identity::{Action, Denial};

pub use acl::{AclQuery, acl};
pub use analytics::{analytics_groups, analytics_sources, analytics_timeline, analytics_top, analytics_unused};
pub use discover::{api, openapi_spec};
pub use dispatch::{dispatch_delete, dispatch_get, dispatch_post, dispatch_put, not_found};
pub use grants::{GrantsQuery, create_grant, inspect_grant, list_grants, revoke_grant};
pub use jobs::cancel_job;
pub use login::{login_callback, login_start, logout, session, session_user};
pub use policy_decisions::{PolicyDecisionsQuery, policy_decision_error_response, policy_decisions};
pub use pql::pql_query;
pub use query::{search, search_error_response, search_response, search_response_offloaded};
pub use quota::{quota_repository, quota_summary};
pub use repositories::{
    RepositoriesQuery, create_repository, disable_repository, enable_repository, inspect_repository, list_repositories,
    update_repository,
};
pub use retention::{retention_export, retention_plan};
pub use revocations::{DigestRevocationsQuery, inspect_revocation, lift_revocation, list_revocations, put_revocation};
pub use status::{ReadinessQuery, health, readiness, status, status_authorization};
pub use tokens::{ListTokensQuery, create_token, inspect_token, list_tokens, revoke_token, rotate_token};
pub use trash::{inspect_trash, list_trash, trash_error_response};
pub use usage::{StatsQuery, ecosystem_summaries, family_descriptors, metrics, stats};

fn denied(denial: Denial) -> Response {
    if denial == Denial::Forbidden {
        return StatusCode::FORBIDDEN.into_response();
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"peryx\"")],
    )
        .into_response()
}

fn index_by_route<'state>(state: &'state AppState, route: &str) -> Option<&'state Index> {
    state.serving.indexes.iter().find(|index| index.route == route)
}

fn is_json(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| MediaType::parse(value).ok())
        .is_some_and(|media_type| media_type.ty == names::APPLICATION && media_type.subty == names::JSON)
}

/// HTTP handlers distinguish an authenticated denial from a missing or invalid credential.
enum EcosystemCredentialDenied {
    Forbidden,
    Unauthorized,
}

fn authorize_ecosystem_credential<'state>(
    state: &'state AppState,
    headers: &HeaderMap,
    route: &str,
    action: Action,
) -> Result<&'state Index, EcosystemCredentialDenied> {
    let index = index_by_route(state, route).ok_or(EcosystemCredentialDenied::Unauthorized)?;
    let authorization = headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok());
    match state.authorize_index_credential(index, authorization, action) {
        Ok(()) => Ok(index),
        Err(Denial::Forbidden) => Err(EcosystemCredentialDenied::Forbidden),
        Err(Denial::Unavailable | Denial::Unauthenticated) => Err(EcosystemCredentialDenied::Unauthorized),
    }
}
