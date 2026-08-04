#![allow(
    clippy::future_not_send,
    reason = "browser fetch futures are single-threaded by nature; callers wrap them in SendWrapper"
)]

use crate::model::TopologySnapshot;

/// The availability topology snapshot, projected to the caller's class.
///
/// The server reads and projects `AppState`; the hydrated browser fetches `/+availability/topology`,
/// which projects the same fields. Both sides yield the identical `TopologySnapshot`.
///
/// # Errors
///
/// Returns a message when the snapshot cannot be reached or does not parse.
pub async fn load_topology() -> Result<TopologySnapshot, String> {
    #[cfg(feature = "ssr")]
    {
        Ok(crate::ssr::topology().await)
    }
    #[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
    {
        send_wrapper::SendWrapper::new(async {
            let value = super::fetch_json_required("/+availability/topology")
                .await
                .map_err(|_| "The availability topology could not be reached.".to_owned())?;
            serde_json::from_value(value).map_err(|_| "The availability topology returned invalid data.".to_owned())
        })
        .await
    }
    #[cfg(all(not(feature = "ssr"), not(feature = "hydrate")))]
    {
        Err("The availability topology is unavailable.".to_owned())
    }
}
