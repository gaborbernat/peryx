//! `GET /+availability/placements`: the artifact placement-health view, filtered to the caller's class.
//!
//! An administrator watches how much of the store serves locally, how much depends on an upstream, and
//! how much cannot be served at all, without paging every artifact by hand. The aggregate health needs
//! `operator:read`; the per-digest rows carry a content digest, so they need `administration:read`. A
//! caller below operator reads nothing, the response is never cached, and the view carries its own
//! observation time so a stale render shows as age rather than passing for health.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse as _, Response};
use peryx_core::{PlacementHealth, PlacementRow, PlacementView, UiArtifactSource, UiByteAvailability};
use peryx_driver::state::AppState;
use peryx_storage::meta::{
    ArtifactPlacementQuery, ArtifactPlacementQueryError, ArtifactPlacementRow, ArtifactSource, ByteAvailability,
};

use super::status::status_authorization;
use crate::response_security::{FieldClassification, ProtectedCachePolicy};

/// The default rows a page returns when the caller names no limit, matching the other admin listings.
const DEFAULT_PLACEMENT_LIMIT: usize = 25;

#[derive(Debug, serde::Deserialize)]
struct PlacementsQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

/// `GET /+availability/placements`: the placement-health view, filtered to the caller's class.
pub async fn placements(State(state): State<Arc<AppState>>, headers: HeaderMap, uri: Uri) -> Response {
    let mut response = placements_response(&state, &headers, &uri).await;
    ProtectedCachePolicy::NoStore.apply(response.headers_mut());
    response
}

async fn placements_response(state: &AppState, headers: &HeaderMap, uri: &Uri) -> Response {
    // A repository token and a public caller read no operational surface, so only operator and
    // administrator classes see a placement view at all.
    let class = status_authorization(state, headers).await.field_class();
    if !matches!(
        class,
        Some(FieldClassification::Operator | FieldClassification::Administrator)
    ) {
        return StatusCode::FORBIDDEN.into_response();
    }
    // Read the administrator's rows before the aggregate, so a store fault on either read surfaces as a
    // server error rather than one read masking the other.
    let mut rows = None;
    let mut next_cursor = None;
    if class == Some(FieldClassification::Administrator) {
        let Ok(Query(query)) = Query::<PlacementsQuery>::try_from_uri(uri) else {
            return bad_request("invalid placement query");
        };
        let query = ArtifactPlacementQuery {
            cursor: query.cursor,
            limit: query.limit.unwrap_or(DEFAULT_PLACEMENT_LIMIT),
        };
        match state.meta.list_artifact_placements(&query) {
            Ok(page) => {
                rows = Some(page.rows.into_iter().map(placement_row).collect());
                next_cursor = page.next_cursor;
            }
            Err(ArtifactPlacementQueryError::InvalidLimit) => return bad_request("placement limit out of range"),
            Err(ArtifactPlacementQueryError::Store(_)) => return internal_error(),
        }
    }
    let Ok(health) = state.meta.artifact_placement_health() else {
        return internal_error();
    };
    axum::Json(PlacementView {
        captured_at: (state.clock)(),
        health: PlacementHealth {
            local: health.local,
            remote_only: health.remote_only,
            unavailable: health.unavailable,
            total: health.total(),
        },
        rows,
        next_cursor,
    })
    .into_response()
}

fn placement_row(row: ArtifactPlacementRow) -> PlacementRow {
    PlacementRow {
        digest: row.digest,
        source: source_class(row.source),
        availability: availability_class(row.availability),
    }
}

const fn source_class(source: ArtifactSource) -> UiArtifactSource {
    match source {
        ArtifactSource::Hosted => UiArtifactSource::Hosted,
        ArtifactSource::Proxy => UiArtifactSource::Proxy,
        ArtifactSource::Generated => UiArtifactSource::Generated,
    }
}

const fn availability_class(availability: ByteAvailability) -> UiByteAvailability {
    match availability {
        ByteAvailability::Local => UiByteAvailability::Local,
        ByteAvailability::RemoteOnly => UiByteAvailability::RemoteOnly,
        ByteAvailability::Unavailable => UiByteAvailability::Unavailable,
    }
}

fn bad_request(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(serde_json::json!({ "error": message })),
    )
        .into_response()
}

fn internal_error() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(serde_json::json!({ "error": "placement query failed" })),
    )
        .into_response()
}
