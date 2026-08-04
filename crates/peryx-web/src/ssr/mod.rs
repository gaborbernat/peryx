//! The server half: an axum router that renders the app with data read straight from `AppState`,
//! plus the data builders the resource fetchers use during server rendering.

mod archive;
mod placement;
mod router;
mod search;
mod simple;
mod snapshot;
mod topology;

pub use archive::{member_chunk, members};
pub use placement::placements;
pub use router::{UiState, ui_router};
pub use search::search;
pub use simple::{layer_chunk, layer_members, manifest, project_view, projects};
pub use snapshot::{admin_snapshot, snapshot, stats};
pub use topology::topology;

async fn read_access(app: &peryx_driver::AppState) -> Result<peryx_driver::access::ReadAccess, String> {
    let headers = leptos_axum::extract::<axum::http::HeaderMap>()
        .await
        .map_err(|err| format!("request headers: {err}"))?;
    Ok(peryx_driver::access::ReadAccess::from_headers(app, &headers))
}

fn access_error(_: peryx_identity::Denial) -> String {
    "read access denied".to_owned()
}
