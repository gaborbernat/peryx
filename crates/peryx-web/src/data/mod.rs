//! Data loading for the UI, compiled per side: the server reads `AppState` directly, the hydrated
//! browser fetches peryx's own JSON API. Both produce the same view models.
#![allow(
    clippy::future_not_send,
    reason = "browser fetch futures are single-threaded by nature; callers wrap them in SendWrapper"
)]

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
mod admin;
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
mod analytics;
mod archive;
mod manifest;
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
mod policy_decisions;
mod search;
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
mod shadow;
mod simple;
mod stats;
mod status;
mod topology;
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
mod trash;

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
pub use admin::admin_request;
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
pub use analytics::load_analytics;
pub use archive::{load_member_chunk, load_members};
pub use manifest::{load_layer_chunk, load_layer_members, load_manifest};
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
pub use policy_decisions::load_policy_decisions;
pub use search::load_search;
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
pub use shadow::load_shadow_candidates;
pub use simple::{load_project_view, load_projects};
pub use stats::load_stats;
pub use status::{load_admin_snapshot, load_snapshot};
pub use topology::load_topology;
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
pub use topology::{TopologyStream, subscribe_topology};
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
pub use trash::load_trash;

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
async fn fetch_json(url: &str) -> Option<serde_json::Value> {
    fetch_json_required(url).await.ok()
}

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
async fn fetch_json_required(url: &str) -> Result<serde_json::Value, String> {
    let Some(value) = fetch_json_optional(url).await? else {
        return Err(format!("404 from {url}: not found"));
    };
    Ok(value)
}

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
async fn fetch_json_optional(url: &str) -> Result<Option<serde_json::Value>, String> {
    let response = gloo_net::http::Request::get(url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|err| format!("request failed for {url}: {err}"))?;
    if response.status() == 404 {
        return Ok(None);
    }
    if !response.ok() {
        return Err(response_error(response, url).await);
    }
    response
        .json()
        .await
        .map(Some)
        .map_err(|err| format!("invalid JSON from {url}: {err}"))
}

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
async fn response_error(response: gloo_net::http::Response, url: &str) -> String {
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if text.is_empty() {
        format!("{status} from {url}")
    } else {
        format!("{status} from {url}: {text}")
    }
}

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
async fn fetch_member_chunk(url: &str) -> Result<crate::model::UiMemberChunk, String> {
    let response = gloo_net::http::Request::get(url)
        .send()
        .await
        .map_err(|err| format!("request failed for {url}: {err}"))?;
    if !response.ok() {
        return Err(response_error(response, url).await);
    }
    let content_type = response.headers().get("content-type").unwrap_or_default();
    if !content_type.starts_with("text/plain") {
        return Err(format!("{url} returned {content_type}; text/plain expected"));
    }
    let size = parse_header_u64(&response, "x-peryx-member-size");
    let offset = parse_header_u64(&response, "x-peryx-member-offset").unwrap_or_default();
    let next_offset = parse_header_u64(&response, "x-peryx-next-offset");
    Ok(crate::model::UiMemberChunk {
        text: response
            .text()
            .await
            .map_err(|err| format!("response body from {url} could not be read: {err}"))?,
        size,
        offset,
        next_offset,
    })
}

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
fn parse_header_u64(response: &gloo_net::http::Response, name: &str) -> Option<u64> {
    response.headers().get(name)?.parse().ok()
}
