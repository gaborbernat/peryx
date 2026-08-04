use std::sync::Arc;

use axum::http::HeaderMap;
use leptos::prelude::*;
use peryx_core::{PlacementHealth, PlacementRow, PlacementView, UiArtifactSource, UiByteAvailability};
use peryx_driver::AppState;
use peryx_http::response_security::FieldClassification;
use peryx_storage::meta::{ArtifactPlacementQuery, ArtifactPlacementRow, ArtifactSource, ByteAvailability};

/// The rows the first server render lists, matching the API's default page so a hydrated paginator
/// resumes from the same point.
const DEFAULT_PLACEMENT_LIMIT: usize = 25;

/// The placement-health view, projected to the caller's class exactly as `GET /+availability/placements`
/// would, so a rendered page never carries a field the API withholds.
///
/// # Errors
///
/// Returns a message when the caller lacks operator access or the store cannot be read.
pub async fn placements() -> Result<PlacementView, String> {
    let app = expect_context::<Arc<AppState>>();
    let headers = leptos_axum::extract::<HeaderMap>().await.unwrap_or_default();
    let class = peryx_http::handlers::status_authorization(&app, &headers)
        .await
        .field_class();
    if !matches!(
        class,
        Some(FieldClassification::Operator | FieldClassification::Administrator)
    ) {
        return Err("You do not have access to placement health.".to_owned());
    }
    let health = app
        .meta
        .artifact_placement_health()
        .map_err(|_| "Placement health could not be read.".to_owned())?;
    let (rows, next_cursor) = if class == Some(FieldClassification::Administrator) {
        let page = app
            .meta
            .list_artifact_placements(&ArtifactPlacementQuery {
                cursor: None,
                limit: DEFAULT_PLACEMENT_LIMIT,
            })
            .map_err(|_| "Placement rows could not be read.".to_owned())?;
        (
            Some(page.rows.into_iter().map(placement_row).collect()),
            page.next_cursor,
        )
    } else {
        (None, None)
    };
    Ok(PlacementView {
        captured_at: (app.clock)(),
        health: PlacementHealth {
            local: health.local,
            remote_only: health.remote_only,
            unavailable: health.unavailable,
            total: health.total(),
        },
        rows,
        next_cursor,
    })
}

fn placement_row(row: ArtifactPlacementRow) -> PlacementRow {
    PlacementRow {
        digest: row.digest,
        source: match row.source {
            ArtifactSource::Hosted => UiArtifactSource::Hosted,
            ArtifactSource::Proxy => UiArtifactSource::Proxy,
            ArtifactSource::Generated => UiArtifactSource::Generated,
        },
        availability: match row.availability {
            ByteAvailability::Local => UiByteAvailability::Local,
            ByteAvailability::RemoteOnly => UiByteAvailability::RemoteOnly,
            ByteAvailability::Unavailable => UiByteAvailability::Unavailable,
        },
    }
}
