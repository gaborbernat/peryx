use std::sync::Arc;

use axum::http::HeaderMap;
use leptos::prelude::*;
use peryx_core::{BlobPlacementView, PlacementView};
use peryx_driver::AppState;
use peryx_ha::{AvailabilityPageQuery, AvailabilityViewReader, BlobPlacementViewError, PlacementViewError};
use peryx_http::response_security::FieldClassification;

const DEFAULT_PLACEMENT_LIMIT: usize = 25;

/// # Errors
///
/// Returns a message when the caller lacks operator access or placement data cannot be read.
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
    app.serving
        .placement_view(AvailabilityPageQuery {
            cursor: None,
            limit: DEFAULT_PLACEMENT_LIMIT,
            include_rows: class == Some(FieldClassification::Administrator),
        })
        .map_err(placement_error)
}

fn placement_error(error: PlacementViewError) -> String {
    match error {
        PlacementViewError::InvalidLimit => "The placement page limit is invalid.",
        PlacementViewError::HealthRead => "Placement health could not be read.",
        PlacementViewError::RowsRead => "Placement rows could not be read.",
    }
    .to_owned()
}

/// # Errors
///
/// Returns a message when the caller is not an administrator or blob placement cannot be read.
pub async fn blob_placements(digest: String) -> Result<BlobPlacementView, String> {
    let app = expect_context::<Arc<AppState>>();
    let headers = leptos_axum::extract::<HeaderMap>().await.unwrap_or_default();
    let class = peryx_http::handlers::status_authorization(&app, &headers)
        .await
        .field_class();
    if class != Some(FieldClassification::Administrator) {
        return Err("You do not have access to blob placement.".to_owned());
    }
    app.serving.blob_placement_view(&digest).map_err(blob_placement_error)
}

fn blob_placement_error(error: BlobPlacementViewError) -> String {
    match error {
        BlobPlacementViewError::InvalidDigest => "That is not a valid artifact digest.",
        BlobPlacementViewError::Read => "Blob placement could not be read.",
    }
    .to_owned()
}

#[cfg(test)]
#[path = "../../tests/unit/ssr/placement/tests.rs"]
mod tests;
