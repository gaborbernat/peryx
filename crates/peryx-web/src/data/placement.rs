#![allow(
    clippy::future_not_send,
    reason = "browser fetch futures are single-threaded by nature; callers wrap them in SendWrapper"
)]

use peryx_core::PlacementView;

/// The artifact placement-health view, projected to the caller's class.
///
/// The server reads and projects `AppState`; the hydrated browser fetches `/+availability/placements`,
/// which projects the same fields, so both sides yield the identical [`PlacementView`]. `cursor` pages
/// the administrator's rows in digest order; an operator reads only the aggregate and ignores it.
///
/// # Errors
///
/// Returns a message when the view cannot be reached, access is denied, or the response does not parse.
pub async fn load_placements(cursor: Option<String>) -> Result<PlacementView, String> {
    #[cfg(feature = "ssr")]
    {
        let _ = cursor;
        crate::ssr::placements().await
    }
    #[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
    {
        send_wrapper::SendWrapper::new(async move {
            // Digests are `sha256:` plus hex, so they carry no character a query value must escape.
            let url = cursor.map_or_else(
                || "/+availability/placements".to_owned(),
                |cursor| format!("/+availability/placements?cursor={cursor}"),
            );
            let response = gloo_net::http::Request::get(&url)
                .header("accept", "application/json")
                .send()
                .await
                .map_err(|_| "Placement health could not be reached.".to_owned())?;
            match response.status() {
                200 => response
                    .json()
                    .await
                    .map_err(|_| "Placement health returned invalid data.".to_owned()),
                400 => Err("The placement page request was invalid.".to_owned()),
                401 | 403 => Err("You do not have access to placement health.".to_owned()),
                _ => Err("Placement health is unavailable.".to_owned()),
            }
        })
        .await
    }
    #[cfg(all(not(feature = "ssr"), not(feature = "hydrate")))]
    {
        let _ = cursor;
        Err("Placement health is unavailable.".to_owned())
    }
}
