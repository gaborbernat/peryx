//! `GET /+availability/operations`: the pending-operations-health view, filtered to the caller's class.
//!
//! An administrator watches how many admitted writes are still in flight, how many finalized, and how
//! many failed or expired, without paging the ledger by hand. The aggregate health needs `operator:read`;
//! the per-operation rows carry an operation id, so they need `administration:read`. A caller below
//! operator reads nothing, the response is never cached, and the view carries its own observation time so
//! a stale render shows as age rather than passing for a settled ledger.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse as _, Response};
use peryx_core::{OperationRow, OperationsHealth, OperationsView, UiOperationStatus};
use peryx_driver::state::AppState;
use peryx_storage::meta::{OperationOutcomeQuery, OperationOutcomeQueryError, OperationOutcomeRow, OperationState};

use super::status::status_authorization;
use crate::response_security::{FieldClassification, ProtectedCachePolicy};

/// The default rows a page returns when the caller names no limit, matching the other admin listings.
const DEFAULT_OPERATION_LIMIT: usize = 25;

#[derive(Debug, serde::Deserialize)]
struct OperationsQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

/// `GET /+availability/operations`: the operations-health view, filtered to the caller's class.
pub async fn operations(State(state): State<Arc<AppState>>, headers: HeaderMap, uri: Uri) -> Response {
    let mut response = operations_response(&state, &headers, &uri).await;
    ProtectedCachePolicy::NoStore.apply(response.headers_mut());
    response
}

async fn operations_response(state: &AppState, headers: &HeaderMap, uri: &Uri) -> Response {
    // A repository token and a public caller read no operational surface, so only operator and
    // administrator classes see an operations view at all.
    let class = status_authorization(state, headers).await.field_class();
    if !matches!(
        class,
        Some(FieldClassification::Operator | FieldClassification::Administrator)
    ) {
        return StatusCode::FORBIDDEN.into_response();
    }
    // One clock reading dates the view, derives each row's status, and buckets the aggregate, so the page
    // and its summary read the ledger at a single instant.
    let now = (state.clock)();
    // Read the administrator's rows before the aggregate, so a store fault on either read surfaces as a
    // server error rather than one read masking the other.
    let mut rows = None;
    let mut next_cursor = None;
    if class == Some(FieldClassification::Administrator) {
        let Ok(Query(query)) = Query::<OperationsQuery>::try_from_uri(uri) else {
            return bad_request("invalid operation query");
        };
        let query = OperationOutcomeQuery {
            cursor: query.cursor,
            limit: query.limit.unwrap_or(DEFAULT_OPERATION_LIMIT),
        };
        match state.meta.list_operation_outcomes(&query) {
            Ok(page) => {
                rows = Some(page.rows.into_iter().map(|row| operation_row(row, now)).collect());
                next_cursor = page.next_cursor;
            }
            Err(OperationOutcomeQueryError::InvalidLimit) => return bad_request("operation limit out of range"),
            Err(OperationOutcomeQueryError::Store(_)) => return internal_error(),
        }
    }
    let Ok(health) = state.meta.operation_outcome_health(now) else {
        return internal_error();
    };
    axum::Json(OperationsView {
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
    .into_response()
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
        axum::Json(serde_json::json!({ "error": "operation query failed" })),
    )
        .into_response()
}
