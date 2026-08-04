use std::sync::Arc;

use axum::http::HeaderMap;
use leptos::prelude::*;
use peryx_core::{LocalStatus, NodeLiveness, TopologySnapshot, TopologyView};
use peryx_driver::AppState;
use peryx_http::response_security::FieldClassification;

/// The availability topology snapshot, projected to the caller's class.
///
/// Mirrors `GET /+availability/topology`, so a rendered page never carries a field the API would
/// withhold. The snapshot stamps its own observation time, so a stale render shows as age.
#[must_use]
pub async fn topology() -> TopologySnapshot {
    let app = expect_context::<Arc<AppState>>();
    let view = topology_view(&app).await;
    let local = local_status(&app).await;
    app.availability_topology().snapshot(view, local, (app.clock)())
}

/// The view class the caller reads at, resolved by the same authority as the `/+status` and
/// `/+availability/topology` APIs. A repository token administers no server surface, so it reads the
/// public roster; only an operator or administrator role widens it.
async fn topology_view(app: &AppState) -> TopologyView {
    // `HeaderMap` extraction is infallible, so a failure defaults to no headers rather than branching.
    let headers = leptos_axum::extract::<HeaderMap>().await.unwrap_or_default();
    match peryx_http::handlers::status_authorization(app, &headers)
        .await
        .field_class()
    {
        Some(FieldClassification::Administrator) => TopologyView::Administrator,
        Some(FieldClassification::Operator) => TopologyView::Operator,
        _ => TopologyView::Public,
    }
}

/// This node's own live self-observation: its configured role, whether its local stores can serve, and
/// the metadata frontier it has committed.
async fn local_status(app: &AppState) -> LocalStatus {
    let serial = app.meta.current_serial();
    let serving = serial.is_ok() && app.blobs.health().await.is_ok();
    LocalStatus {
        role: app.availability_role(),
        liveness: if serving {
            NodeLiveness::Live
        } else {
            NodeLiveness::Unready
        },
        frontier: serial.unwrap_or(0),
    }
}
