use std::sync::Arc;

use axum::http::HeaderMap;
use leptos::prelude::*;
use peryx_core::{OperationRow, OperationsHealth, OperationsView, UiOperationStatus};
use peryx_driver::AppState;
use peryx_http::response_security::FieldClassification;
use peryx_storage::meta::{OperationOutcomeQuery, OperationOutcomeRow, OperationState};

/// The rows the first server render lists, matching the API's default page so a hydrated paginator
/// resumes from the same point.
const DEFAULT_OPERATION_LIMIT: usize = 25;

/// The pending-operations-health view, projected to the caller's class exactly as
/// `GET /+availability/operations` would, so a rendered page never carries a field the API withholds.
///
/// # Errors
///
/// Returns a message when the caller lacks operator access or the store cannot be read.
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
    let now = (app.clock)();
    let health = app
        .meta
        .operation_outcome_health(now)
        .map_err(|_| "Operation health could not be read.".to_owned())?;
    let (rows, next_cursor) = if class == Some(FieldClassification::Administrator) {
        let page = app
            .meta
            .list_operation_outcomes(&OperationOutcomeQuery {
                cursor: None,
                limit: DEFAULT_OPERATION_LIMIT,
            })
            .map_err(|_| "Operation rows could not be read.".to_owned())?;
        (
            Some(page.rows.into_iter().map(|row| operation_row(row, now)).collect()),
            page.next_cursor,
        )
    } else {
        (None, None)
    };
    Ok(OperationsView {
        captured_at: now,
        health: OperationsHealth {
            pending: health.pending,
            published: health.published,
            failed: health.failed,
            expired: health.expired,
            total: health.total(),
        },
        rows,
        next_cursor,
    })
}

fn operation_row(row: OperationOutcomeRow, now: i64) -> OperationRow {
    OperationRow {
        operation: row.operation,
        status: UiOperationStatus::derive(
            matches!(row.state, OperationState::Published),
            matches!(row.state, OperationState::Failed),
            row.expiry_unix,
            now,
        ),
        updated_at: row.updated_at_unix,
        expires_at: row.expiry_unix,
    }
}
