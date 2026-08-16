use std::sync::Arc;

use axum::http::HeaderMap;
use leptos::prelude::*;
use peryx_core::OperationsView;
use peryx_driver::AppState;
use peryx_ha::{AvailabilityPageQuery, AvailabilityViewReader, OperationsViewError};
use peryx_http::response_security::FieldClassification;

const DEFAULT_OPERATION_LIMIT: usize = 25;

/// # Errors
///
/// Returns a message when the caller lacks operator access or operation data cannot be read.
pub async fn operations() -> Result<OperationsView, String> {
    let app = expect_context::<Arc<AppState>>();
    let headers = leptos_axum::extract::<HeaderMap>().await.unwrap_or_default();
    let class = peryx_http::handlers::status_authorization(&app, &headers)
        .await
        .field_class();
    if !matches!(
        class,
        Some(FieldClassification::Operator | FieldClassification::Administrator)
    ) {
        return Err("You do not have access to operation health.".to_owned());
    }
    app.serving
        .operations_view(AvailabilityPageQuery {
            cursor: None,
            limit: DEFAULT_OPERATION_LIMIT,
            include_rows: class == Some(FieldClassification::Administrator),
        })
        .map_err(operation_error)
}

fn operation_error(error: OperationsViewError) -> String {
    match error {
        OperationsViewError::InvalidLimit => "The operation page limit is invalid.",
        OperationsViewError::HealthRead => "Operation health could not be read.",
        OperationsViewError::RowsRead => "Operation rows could not be read.",
    }
    .to_owned()
}

#[cfg(test)]
#[path = "../../tests/unit/ssr/operations/tests.rs"]
mod tests;
