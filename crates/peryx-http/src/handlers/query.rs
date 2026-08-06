//! Cached-package search: the `/+search` handlers and their response rendering.

use std::sync::Arc;

use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use peryx_driver::access::ReadAccess;
use peryx_driver::state::{AppState, Index};
use peryx_search::{SearchAccess, SearchError, SearchParams};

/// `GET /{route}/+search`: search cached packages scoped to one index.
pub(super) async fn index_search(
    state: Arc<AppState>,
    position: usize,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
) -> Response {
    let mut params = match SearchParams::from_query(uri.query()) {
        Ok(params) => params,
        Err(err) => return search_error_response(&err),
    };
    params.route = Some(state.index_at(position).route.clone());
    let access = search_access(&state, headers, std::slice::from_ref(state.index_at(position)));
    search_response_offloaded(state, params, access).await
}

/// `GET /+search`: search cached packages across configured indexes.
pub async fn search(State(state): State<Arc<AppState>>, OriginalUri(uri): OriginalUri, headers: HeaderMap) -> Response {
    match SearchParams::from_query(uri.query()) {
        Ok(params) => {
            let access = search_access(&state, &headers, &state.indexes);
            search_response_offloaded(state, params, access).await
        }
        Err(err) => search_error_response(&err),
    }
}

fn search_access(state: &AppState, headers: &HeaderMap, indexes: &[Index]) -> Option<SearchAccess> {
    if indexes.iter().all(|index| index.acl.anonymous_read) {
        return None;
    }
    Some(ReadAccess::from_headers(state, headers).search_access(indexes))
}

/// Run [`search_response`] on the blocking pool. A tantivy query is mmap I/O plus CPU scoring, so
/// keeping it off the async workers stops a burst of searches from stalling concurrent serving.
///
/// [`search_response`] renders every error as a response, so the task is not expected to panic. An
/// unexpected one is contained here as a `500` rather than propagated, so a single faulting query
/// fails its own request instead of aborting the runtime.
pub async fn search_response_offloaded(
    state: Arc<AppState>,
    params: SearchParams,
    access: Option<SearchAccess>,
) -> Response {
    offload_search(move || search_response(&state, params, access.as_ref())).await
}

/// Run a search-rendering closure on the blocking pool, mapping a task panic to a `500` response.
async fn offload_search<F>(render: F) -> Response
where
    F: FnOnce() -> Response + Send + 'static,
{
    match tokio::task::spawn_blocking(render).await {
        Ok(response) => response,
        Err(err) => search_task_panic_response(&err),
    }
}

/// Render the `500` response for a search task that panicked on the blocking pool.
fn search_task_panic_response(err: &tokio::task::JoinError) -> Response {
    tracing::error!(error = %err, "search task panicked");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(serde_json::json!({ "error": "search failed" })),
    )
        .into_response()
}

/// Run a search over cached package documents and render the result document.
#[must_use]
pub fn search_response(state: &AppState, params: SearchParams, access: Option<&SearchAccess>) -> Response {
    let result = if let Some(access) = access {
        state.search.search_authorized(&state.search_ctx(), params, access)
    } else {
        state.search.search(&state.search_ctx(), params)
    };
    match result {
        Ok(results) => axum::Json(results).into_response(),
        Err(err) => search_error_response(&err),
    }
}

/// Map a [`SearchError`] to a JSON error response.
#[must_use]
pub fn search_error_response(err: &SearchError) -> Response {
    let status = if err.is_bad_request() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, axum::Json(serde_json::json!({ "error": err.to_string() }))).into_response()
}

#[cfg(test)]
mod pure_tests {
    //! Direct coverage for the blocking-pool wrapper. A real search never panics, so the router flow
    //! cannot reach the join-error arm; these drive the wrapper with closures the handler would never
    //! produce.
    use axum::http::StatusCode;
    use axum::response::IntoResponse as _;

    use super::{offload_search, search_task_panic_response};

    #[tokio::test]
    async fn test_offload_search_returns_the_closure_response() {
        let response = offload_search(|| StatusCode::OK.into_response()).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_offload_search_maps_a_panicked_task_to_500() {
        let response = offload_search(|| panic!("boom")).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_search_task_panic_response_is_500() {
        let err = tokio::task::spawn_blocking(|| panic!("boom")).await.unwrap_err();
        assert_eq!(
            search_task_panic_response(&err).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
