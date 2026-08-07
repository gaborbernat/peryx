use std::sync::Arc;

use leptos::prelude::*;
use peryx_driver::AppState;
use peryx_storage::archive;

use super::{authorize_project, resolve};
use crate::model::{UiMember, UiMemberChunk};

/// The member listing of a cached archive, for server rendering.
///
/// # Errors
/// Returns a user-visible message when the project cannot be read, or the artifact cannot be found,
/// fetched, or listed.
pub async fn members(
    route: &str,
    project: &str,
    sha256: &str,
    filename: &str,
    containers: &[String],
) -> Result<Vec<UiMember>, String> {
    let lease = artifact_path(route, project, sha256, filename).await?;
    let archive = filename.to_owned();
    let containers = containers.to_vec();
    let members =
        tokio::task::spawn_blocking(move || archive::list_members_nested_path(&archive, lease.path(), &containers))
            .await
            .map_err(|err| format!("archive listing on index {route:?} for file {filename:?}: {err}"))?
            .map_err(|err| format!("archive listing on index {route:?} for file {filename:?}: {err}"))?;
    Ok(members
        .into_iter()
        .map(|member| UiMember {
            path: member.path,
            size: member.size,
            kind: member.kind.as_str().to_owned(),
            previewable: member.previewable,
        })
        .collect())
}

/// One archive member chunk, for server rendering.
///
/// # Errors
/// Returns a user-visible message when the project cannot be read, or the member cannot be previewed
/// as UTF-8 text.
pub async fn member_chunk(
    route: &str,
    project: &str,
    sha256: &str,
    filename: &str,
    containers: &[String],
    member: &str,
    offset: u64,
) -> Result<UiMemberChunk, String> {
    let lease = artifact_path(route, project, sha256, filename).await?;
    let archive = filename.to_owned();
    let containers = containers.to_vec();
    let selected = member.to_owned();
    let chunk = tokio::task::spawn_blocking(move || {
        archive::read_text_member_chunk_nested_path(
            &archive,
            lease.path(),
            &containers,
            &selected,
            offset,
            archive::DEFAULT_MEMBER_CHUNK,
        )
    })
    .await
    .map_err(|err| format!("archive member {member:?} on index {route:?} for file {filename:?}: {err}"))?
    .map_err(|err| format!("archive member {member:?} on index {route:?} for file {filename:?}: {err}"))?;
    Ok(UiMemberChunk {
        text: String::from_utf8(chunk.bytes).map_err(|err| {
            format!("archive member {member:?} on index {route:?} for file {filename:?} is not valid UTF-8: {err}")
        })?,
        size: Some(chunk.size),
        offset: chunk.offset,
        next_offset: chunk.next_offset,
    })
}

/// The local path of `project`'s artifact `sha256`/`filename` on the index at `route`, fetched through
/// that index's ecosystem driver so this crate carries no format-specific fetch logic. The project is
/// authorized first and the driver proves the file belongs to it, so a browse route cannot read a
/// private project's blob by pairing an accessible project name with a foreign digest.
async fn artifact_path(
    route: &str,
    project: &str,
    sha256: &str,
    filename: &str,
) -> Result<peryx_storage::blob::BlobLease, String> {
    let app = expect_context::<Arc<AppState>>();
    let (position, driver) = resolve(&app, route)?;
    authorize_project(&app, position, project).await?;
    let driver = driver.clone();
    driver
        .artifact_path_in_project(
            app.serving.clone(),
            position,
            project.to_owned(),
            sha256.to_owned(),
            filename.to_owned(),
        )
        .await
}
