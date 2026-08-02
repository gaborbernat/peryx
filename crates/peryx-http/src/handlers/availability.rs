//! `GET /+availability/topology`: one immutable, role-filtered picture of the availability group.
//!
//! An operator page renders this instead of traversing live membership and storage state on every poll.
//! The roster identities, datacenters, and roles stay public; the live frontier and per-node liveness
//! need `operator:read`; the advertised peer addresses need `administration:read`. A caller reads only
//! the fields at or below its class, the response is never cached, and the snapshot carries its own
//! observation time so a stale render shows as age rather than passing for health.

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse as _, Response};
use peryx_core::{LocalStatus, NodeLiveness, TopologyView};
use peryx_driver::state::AppState;

use super::status::status_authorization;
use crate::response_security::{FieldClassification, ProtectedCachePolicy, ResponseAuthorization};

/// `GET /+availability/topology`: the availability topology snapshot, filtered to the caller's class.
pub async fn availability_topology(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let view = topology_view(status_authorization(&state, &headers).await);
    let local = local_status(&state).await;
    let snapshot = state.availability_topology().snapshot(view, local, (state.clock)());
    let mut response = axum::Json(snapshot).into_response();
    ProtectedCachePolicy::NoStore.apply(response.headers_mut());
    response
}

/// The topology view a caller reads at. A repository token administers no server surface, so it reads
/// the same public roster as an anonymous caller; only an operator or administrator role widens it.
const fn topology_view(authorization: ResponseAuthorization) -> TopologyView {
    match authorization.field_class() {
        Some(FieldClassification::Administrator) => TopologyView::Administrator,
        Some(FieldClassification::Operator) => TopologyView::Operator,
        // A repository token, a public caller, and a denied decision all read the public roster.
        _ => TopologyView::Public,
    }
}

/// This node's own live self-observation: its role, whether its local stores can serve, and the metadata
/// frontier it has committed. The role is the configured authority role, so a read-only primary reports
/// itself as the writer rather than reading its read-only posture as a replica.
async fn local_status(state: &AppState) -> LocalStatus {
    let serial = state.meta.current_serial();
    let serving = serial.is_ok() && state.blobs.health().await.is_ok();
    LocalStatus {
        role: state.availability_role(),
        liveness: if serving {
            NodeLiveness::Live
        } else {
            NodeLiveness::Unready
        },
        frontier: serial.unwrap_or(0),
    }
}
