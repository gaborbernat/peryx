//! The server half: an axum router that renders the app with data read straight from `AppState`,
//! plus the data builders the resource fetchers use during server rendering.

mod archive;
mod login;
mod operations;
mod placement;
mod router;
mod search;
mod simple;
mod snapshot;
mod topology;

pub use archive::{member_chunk, members};
pub use login::login_state;
pub use operations::operations;
pub use placement::{blob_placements, placements};
pub use router::{UiState, ui_router};
pub use search::search;
pub use simple::{layer_chunk, layer_members, manifest, project_view, projects};
pub use snapshot::{admin_snapshot, snapshot, stats};
pub use topology::topology;

use std::sync::Arc;

use peryx_driver::AppState;

async fn read_access(app: &AppState) -> Result<peryx_driver::access::ReadAccess, String> {
    let headers = leptos_axum::extract::<axum::http::HeaderMap>()
        .await
        .map_err(|err| format!("request headers: {err}"))?;
    Ok(peryx_driver::access::ReadAccess::from_headers(app, &headers))
}

fn access_error(_: peryx_identity::Denial) -> String {
    "read access denied".to_owned()
}

/// The position of the index at `route` and the driver serving its ecosystem.
fn resolve<'a>(
    app: &'a AppState,
    route: &str,
) -> Result<(usize, &'a Arc<dyn peryx_driver::serving::EcosystemDriver>), String> {
    let position = app
        .indexes
        .iter()
        .position(|index| index.route == route)
        .ok_or_else(|| format!("index {route:?} is not configured"))?;
    let driver = app
        .driver_for(app.index_at(position).ecosystem)
        .ok_or_else(|| format!("index {route:?} has no ecosystem driver"))?;
    Ok((position, driver))
}

/// Authorize a read of `project` on the index at `position`, mirroring the JSON browse endpoints so an
/// SSR render and its hydrated fetch enforce the same access.
async fn authorize_project(app: &AppState, position: usize, project: &str) -> Result<(), String> {
    if app.index_at(position).acl.anonymous_read {
        return Ok(());
    }
    read_access(app)
        .await?
        .for_index(app.index_at(position))
        .authorize_project(project)
        .map_err(access_error)
}
