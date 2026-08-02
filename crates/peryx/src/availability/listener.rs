//! The private, administrator-authenticated availability control listener.
//!
//! Availability controls never share the public package routes. A `dc` or `ha` node binds this router
//! on its own socket (see [`AvailabilityListenerConfig`]), authenticates every request against the same
//! identity store the package API uses, and admits only a principal holding the server-wide
//! [`Scope::AdministrationRead`] over [`Resource::Operator`]. Single-node `none` builds none of this, so
//! the control plane costs a single-writer process nothing.
//!
//! This module assembles and authorizes the router; the process entrypoint owns the socket, TLS
//! termination, and graceful drain. The initial surface is a read-only status endpoint that reports the
//! node's availability posture; mutating membership and transfer commands arrive in later work behind
//! the same gate.
//!
//! [`AvailabilityListenerConfig`]: crate::config::AvailabilityListenerConfig

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use peryx_driver::authz::Decision;
use peryx_driver::state::AppState;
use peryx_identity::{Resource, Scope, UserId, parse_basic};
use serde_json::json;

use crate::config::{AvailabilityConfig, ReplicationConfig};

/// The availability control protocol version this node advertises to a client of the listener.
///
/// A client pins the versions it understands and refuses an incompatible peer rather than guessing a
/// wire shape.
pub const AVAILABILITY_PROTOCOL_VERSION: u32 = 1;

/// The largest control request body the listener reads, in bytes. The status surface carries none; the
/// bound stands so a later command endpoint cannot be handed an unbounded body on the control plane.
const MAX_CONTROL_BODY_BYTES: usize = 64 * 1024;

/// A node's availability posture: the mode it runs and the authority role it holds.
///
/// The status endpoint reports it, built from the resolved [`AvailabilityConfig`], so it exists only for
/// `dc` and `ha`.
#[derive(Clone, Copy)]
pub struct AvailabilityPosture {
    mode: &'static str,
    role: &'static str,
}

impl AvailabilityPosture {
    /// The posture a `dc` or `ha` node reports, or `None` under single-node `none`, which runs no
    /// listener and therefore has no posture to expose.
    #[must_use]
    pub fn from_config(availability: &AvailabilityConfig) -> Option<Self> {
        let role = match availability.replication()? {
            ReplicationConfig::Primary { .. } => "writer",
            ReplicationConfig::Replica { .. } => "replica",
        };
        Some(Self {
            mode: availability.mode().as_str(),
            role,
        })
    }
}

/// The state the listener's router and its authentication middleware share.
#[derive(Clone)]
struct ListenerState {
    app: Arc<AppState>,
    posture: AvailabilityPosture,
}

/// Assemble the availability control router: an administrator-authenticated, version-prefixed surface
/// bounded to [`MAX_CONTROL_BODY_BYTES`] and kept apart from the public package routes.
///
/// The caller binds the returned router on the private [`AvailabilityListenerConfig`] socket. Every
/// matched route runs behind [`authenticate`]; an unmatched path answers `404` without touching the
/// identity store, so an unauthenticated caller cannot probe the surface.
///
/// [`AvailabilityListenerConfig`]: crate::config::AvailabilityListenerConfig
pub fn router(app: Arc<AppState>, posture: AvailabilityPosture) -> Router {
    let state = ListenerState { app, posture };
    Router::new()
        .route("/availability/v1/status", get(status))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(DefaultBodyLimit::max(MAX_CONTROL_BODY_BYTES))
        .with_state(state)
}

/// Admit a request only for an authenticated server administrator, recording an audit line naming the
/// actor and path. A missing or invalid credential is `401`; an authenticated non-administrator is
/// `403`; an identity store that cannot answer is `503`.
async fn authenticate(State(state): State<ListenerState>, request: Request, next: Next) -> Response {
    match authorize(&state.app, request.headers()).await {
        Ok(actor) => {
            tracing::info!(%actor, path = %request.uri().path(), "availability control request admitted");
            next.run(request).await
        }
        Err(response) => response,
    }
}

/// Resolve the request's Basic credential to a server administrator, reusing the package API's identity
/// store and authorization service so the control plane holds no second user database.
async fn authorize(app: &AppState, headers: &HeaderMap) -> Result<UserId, Response> {
    let credentials = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_basic)
        .ok_or_else(unauthorized)?;
    let actor = app
        .users
        .authenticate(&credentials.user, &credentials.password)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(unauthorized)?;
    let decision = app
        .authorization
        .authorize_scoped(&actor, Scope::AdministrationRead, &Resource::Operator);
    if decision.decision() != Decision::Allow {
        return Err(forbidden());
    }
    Ok(actor)
}

/// Report the node's availability posture: the advertised protocol version, its mode and authority role,
/// and whether it currently serves read-only.
async fn status(State(state): State<ListenerState>) -> Response {
    let body = json!({
        "protocol_version": AVAILABILITY_PROTOCOL_VERSION,
        "mode": state.posture.mode,
        "role": state.posture.role,
        "read_only": state.app.read_only,
    });
    (StatusCode::OK, Json(body)).into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"peryx-availability\"")],
    )
        .into_response()
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        "availability control requires the administration scope",
    )
        .into_response()
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "identity store unavailable").into_response()
}
