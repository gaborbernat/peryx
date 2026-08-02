//! Producing the web UI's neutral view models from the `PyPI` serving layer, so the web crate renders
//! a project page without any knowledge of the Simple API, wheels, or PEP 658.

use std::collections::BTreeSet;
use std::sync::Arc;

use peryx_core::{UiArtifactSource, UiByteAvailability, UiMeta, UiProject};
use peryx_driver::ServingState;
use peryx_index::{Index, IndexKind};
use peryx_storage::blob::{BlobLease, Digest};
use peryx_storage::meta::{ArtifactSource, ByteAvailability};

use crate::cache::{self, CacheError};
use crate::store::PypiStore as _;
use crate::{
    ProjectDetail, file_matches_version, normalize_name, parse_version, to_json, ui_meta, ui_project_from_detail,
};

/// The project names of the cached/hosted/virtual index at `position`.
pub(super) fn project_names(state: &ServingState, position: usize) -> Result<Vec<String>, String> {
    let list = cache::resolve_list(state, state.index_at(position))?;
    Ok(list.projects.into_iter().map(|entry| entry.name).collect())
}

/// A project's page data: its files as a neutral [`UiProject`], and the neutral [`UiMeta`] the
/// page's default release carries in a PEP 658 metadata sibling.
pub(super) async fn project_page(
    state: Arc<ServingState>,
    position: usize,
    project: String,
) -> Result<Option<(UiProject, UiMeta)>, String> {
    let route = state.index_at(position).route.clone();
    let normalized = normalize_name(&project);
    let index = state.index_at(position);
    let Some((detail, hosted)) = resolve_detail_and_hosted(&state, index, &normalized, &route)
        .await
        .map_err(|err| {
            format!(
                "project detail on index {route:?} for project {normalized:?}: {}",
                err.user_message()
            )
        })?
    else {
        return Ok(None);
    };
    // `to_json` serializes the detail, so parsing it straight back cannot fail.
    let value = serde_json::from_str(&to_json(&detail)).expect("to_json emits JSON that round-trips");
    let mut ui = ui_project_from_detail(&value);
    apply_placement(&state, &hosted, &mut ui);
    let default = default_version(&ui);
    // A pre-PEP 700 upstream names no versions, so no release owns a file and the newest sibling stands in.
    let sibling = match default.as_deref() {
        Some(version) => metadata_file(&ui, version),
        None => ui.files.iter().rev().find(|file| file.has_metadata),
    };
    let mut meta = match sibling {
        Some(file) => metadata_for(&state, &route, file).await?,
        None => UiMeta::default(),
    };
    meta.version = default.or(meta.version);
    Ok(Some((ui, meta)))
}

/// Resolve a project's detail together with the filenames its hosted layers published, so both store
/// reads share the caller's one error mapping.
async fn resolve_detail_and_hosted(
    state: &ServingState,
    index: &Index,
    project: &str,
    route: &str,
) -> Result<Option<(ProjectDetail, BTreeSet<String>)>, CacheError> {
    let Some(detail) = cache::resolve_detail(state, index, project, route).await? else {
        return Ok(None);
    };
    let mut hosted = BTreeSet::new();
    collect_hosted_filenames(state, index, project, &mut hosted)?;
    Ok(Some((detail, hosted)))
}

/// The filenames an index's hosted (upload) layers published for `project`, unioned across a virtual
/// index's layers so a merged page can tell an uploaded file from a mirrored one.
fn collect_hosted_filenames(
    state: &ServingState,
    index: &Index,
    project: &str,
    names: &mut BTreeSet<String>,
) -> Result<(), peryx_storage::meta::MetaError> {
    match &index.kind {
        IndexKind::Hosted { .. } => {
            for (filename, _record) in state.meta.list_upload_entries(&index.name, project)? {
                names.insert(filename);
            }
        }
        IndexKind::Virtual { layers, .. } => {
            for &pos in layers {
                collect_hosted_filenames(state, state.index_at(pos), project, names)?;
            }
        }
        IndexKind::Cached { .. } => {}
    }
    Ok(())
}

/// Resolve each file's #441 placement (its source and its projected byte availability) from one
/// indexed lookup per file, without probing the content store. A hosted upload shadows a same-named
/// upstream file, the dependency-confusion order [`cache::resolve_detail`] merged the page by, so
/// hosted-layer membership forces the `Hosted` source over any stale proxied placement.
///
/// The availability comes straight from the stored projection, which a repair pass keeps in step with
/// the content store; a listing therefore never reads a blob per row. A file the placement store has
/// not recorded — an upstream catalog entry never fetched — stays proxied and remote-only. A store
/// read that fails falls back to that same default: the page was built from earlier reads of the same
/// store, so a failure here is a torn database the caller cannot recover a truer answer from.
fn apply_placement(state: &ServingState, hosted: &BTreeSet<String>, ui: &mut UiProject) {
    for file in &mut ui.files {
        let is_hosted = hosted.contains(&file.filename);
        file.upstream = if is_hosted {
            None
        } else {
            state
                .meta
                .get_file_url(&file.sha256)
                .ok()
                .flatten()
                .and_then(|source| source.upstream)
        };
        let placement = state.meta.get_artifact_placement(&file.sha256).ok().flatten();
        if is_hosted {
            file.source = UiArtifactSource::Hosted;
            // The upload is authoritative and its bytes are local. Only a hosted-source placement,
            // which the upload path records and eviction or repair can move, marks it unavailable; a
            // stale proxied row left by a same-digest mirror is ignored.
            file.availability = match placement {
                Some(placement) if matches!(placement.source, ArtifactSource::Hosted) => {
                    ui_availability(placement.availability)
                }
                _ => UiByteAvailability::Local,
            };
        } else if let Some(placement) = placement {
            file.source = ui_source(placement.source);
            file.availability = ui_availability(placement.availability);
        } else {
            file.source = UiArtifactSource::Proxy;
            file.availability = UiByteAvailability::RemoteOnly;
        }
    }
}

/// Map the neutral storage source onto its view-model twin.
const fn ui_source(source: ArtifactSource) -> UiArtifactSource {
    match source {
        ArtifactSource::Hosted => UiArtifactSource::Hosted,
        ArtifactSource::Proxy => UiArtifactSource::Proxy,
        ArtifactSource::Generated => UiArtifactSource::Generated,
    }
}

/// Map the neutral storage availability projection onto its view-model twin.
const fn ui_availability(availability: ByteAvailability) -> UiByteAvailability {
    match availability {
        ByteAvailability::Local => UiByteAvailability::Local,
        ByteAvailability::RemoteOnly => UiByteAvailability::RemoteOnly,
        ByteAvailability::Unavailable => UiByteAvailability::Unavailable,
    }
}

/// The release the project page defaults to. An active release (one the publisher has not yanked
/// whole) outranks a yanked one, a stable release outranks a pre-release, and the greatest PEP 440
/// version wins within a class, the order the file-yanking specification and Warehouse use.
///
/// A version that does not parse as PEP 440 counts as neither stable nor greater than a parseable
/// one, so it wins only when nothing else can, and then the greatest string takes it.
fn default_version(project: &UiProject) -> Option<String> {
    project
        .versions
        .iter()
        .map(|release| {
            let parsed = parse_version(&release.version);
            let stable = parsed.as_ref().is_some_and(|parsed| !parsed.any_prerelease());
            ((!release.yanked, stable, parsed), &release.version)
        })
        .max()
        .map(|(_, version)| version.clone())
}

/// The file whose PEP 658 metadata sibling describes `version`, so the page never borrows another
/// release's metadata. An active file outranks a yanked one, and the filename settles the rest, so a
/// release with several siblings always renders the same one.
fn metadata_file<'a>(project: &'a UiProject, version: &str) -> Option<&'a peryx_core::UiFile> {
    project
        .files
        .iter()
        .filter(|file| file.has_metadata && file_matches_version(&file.filename, version))
        .min_by(|left, right| (left.yanked, &left.filename).cmp(&(right.yanked, &right.filename)))
}

/// Fetch and parse the PEP 658 metadata sibling of `file` into the neutral view model.
async fn metadata_for(state: &Arc<ServingState>, route: &str, file: &peryx_core::UiFile) -> Result<UiMeta, String> {
    let Some(digest) = Digest::from_hex(&file.sha256) else {
        return Err(format!(
            "metadata fetch on index {route:?} for file {:?}: invalid sha256 digest {:?}",
            file.filename, file.sha256
        ));
    };
    let metadata_filename = format!("{}.metadata", file.filename);
    let bytes = cache::metadata_bytes(state, &digest, route, &metadata_filename)
        .await
        .map_err(|err| {
            format!(
                "metadata fetch on index {route:?} for file {:?} with digest {}: {}",
                file.filename,
                digest.as_str(),
                err.user_message()
            )
        })?;
    ui_meta(&String::from_utf8_lossy(&bytes))
        .map_err(|err| format!("metadata parse on index {route:?} for file {:?}: {err}", file.filename))
}

/// The local blob-store path of the artifact `digest_hex`/`filename` on the index at `position`,
/// fetching it through the proxy on a miss.
pub(super) async fn artifact_path(
    state: Arc<ServingState>,
    position: usize,
    digest_hex: String,
    filename: String,
) -> Result<BlobLease, String> {
    let route = state.index_at(position).route.clone();
    let Some(digest) = Digest::from_hex(&digest_hex) else {
        return Err(format!(
            "artifact on index {route:?} for file {filename:?}: invalid sha256 digest {digest_hex:?}"
        ));
    };
    cache::file_path(state, digest, route.clone(), filename.clone())
        .await
        .map_err(|err| {
            format!(
                "artifact on index {route:?} for file {filename:?} with digest {digest_hex}: {}",
                err.user_message()
            )
        })
}
