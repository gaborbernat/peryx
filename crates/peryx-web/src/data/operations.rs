#![allow(
    clippy::future_not_send,
    reason = "browser fetch futures are single-threaded by nature; callers wrap them in SendWrapper"
)]

use peryx_core::OperationsView;

/// The pending-operations-health view, projected to the caller's class.
///
/// The server reads and projects `AppState`; the hydrated browser fetches `/+availability/operations`,
/// which projects the same fields, so both sides yield the identical [`OperationsView`]. `cursor` pages
/// the administrator's rows in operation-id order; an operator reads only the aggregate and ignores it.
///
/// # Errors
///
/// Returns a message when the view cannot be reached, access is denied, or the response does not parse.
pub async fn load_operations(cursor: Option<String>) -> Result<OperationsView, String> {
    #[cfg(feature = "ssr")]
    {
        let _ = cursor;
        crate::ssr::operations().await
    }
    #[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
    {
        send_wrapper::SendWrapper::new(async move {
            let request = gloo_net::http::Request::get("/+availability/operations");
            // The cursor is an opaque operation id, so it rides as an encoded query value rather than
            // being interpolated into the path.
            let request = match &cursor {
                Some(cursor) => request.query([("cursor", cursor.as_str())]),
                None => request,
            };
            let response = request
                .header("accept", "application/json")
                .send()
                .await
                .map_err(|_| "Operation health could not be reached.".to_owned())?;
            match response.status() {
                200 => response
                    .json()
                    .await
                    .map_err(|_| "Operation health returned invalid data.".to_owned()),
                400 => Err("The operation page request was invalid.".to_owned()),
                401 | 403 => Err("You do not have access to operation health.".to_owned()),
                _ => Err("Operation health is unavailable.".to_owned()),
            }
        })
        .await
    }
    #[cfg(all(not(feature = "ssr"), not(feature = "hydrate")))]
    {
        let _ = cursor;
        Err("Operation health is unavailable.".to_owned())
    }
}
