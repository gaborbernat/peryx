use std::sync::Arc;

use axum::http::HeaderMap;
use leptos::prelude::*;
use peryx_core::{
    BlobDatacenterPlacement, BlobPlacementStatus, BlobPlacementView, PlacementHealth, PlacementRow, PlacementView,
    UiArtifactSource, UiByteAvailability,
};
use peryx_driver::AppState;
use peryx_http::response_security::FieldClassification;
use peryx_identity::ArtifactDigest;
use peryx_storage::meta::{
    ArtifactPlacementQuery, ArtifactPlacementRow, ArtifactSource, BlobPlacementRecord, BlobPlacementState,
    ByteAvailability,
};

/// The rows the first server render lists, matching the API's default page so a hydrated paginator
/// resumes from the same point.
const DEFAULT_PLACEMENT_LIMIT: usize = 25;

/// The placement-health view, projected to the caller's class exactly as `GET /+availability/placements`
/// would, so a rendered page never carries a field the API withholds.
///
/// # Errors
///
/// Returns a message when the caller lacks operator access or the store cannot be read.
pub async fn placements() -> Result<PlacementView, String> {
    let app = expect_context::<Arc<AppState>>();
    let headers = leptos_axum::extract::<HeaderMap>().await.unwrap_or_default();
    let class = peryx_http::handlers::status_authorization(&app, &headers)
        .await
        .field_class();
    if !matches!(
        class,
        Some(FieldClassification::Operator | FieldClassification::Administrator)
    ) {
        return Err("You do not have access to placement health.".to_owned());
    }
    let health = app
        .meta
        .artifact_placement_health()
        .map_err(|_| "Placement health could not be read.".to_owned())?;
    let (rows, next_cursor) = if class == Some(FieldClassification::Administrator) {
        let page = app
            .meta
            .list_artifact_placements(&ArtifactPlacementQuery {
                cursor: None,
                limit: DEFAULT_PLACEMENT_LIMIT,
            })
            .map_err(|_| "Placement rows could not be read.".to_owned())?;
        (
            Some(page.rows.into_iter().map(placement_row).collect()),
            page.next_cursor,
        )
    } else {
        (None, None)
    };
    Ok(PlacementView {
        captured_at: (app.clock)(),
        health: PlacementHealth {
            local: health.local,
            remote_only: health.remote_only,
            unavailable: health.unavailable,
            total: health.total(),
        },
        rows,
        next_cursor,
    })
}

/// Where one blob's bytes are placed across datacenters.
///
/// Projected exactly as `GET /+availability/placements/{digest}` would, so a rendered detail never
/// carries a field the API withholds. Administrator only, because the datacenter layout is topology an
/// operator does not read.
///
/// # Errors
///
/// Returns a message when the caller is not an administrator or the digest or store cannot be read.
pub async fn blob_placements(digest: String) -> Result<BlobPlacementView, String> {
    let app = expect_context::<Arc<AppState>>();
    let headers = leptos_axum::extract::<HeaderMap>().await.unwrap_or_default();
    if peryx_http::handlers::status_authorization(&app, &headers)
        .await
        .field_class()
        != Some(FieldClassification::Administrator)
    {
        return Err("You do not have access to blob placement.".to_owned());
    }
    let digest = digest
        .parse::<ArtifactDigest>()
        .map_err(|_| "That is not a valid artifact digest.".to_owned())?;
    let records = app
        .meta
        .blob_placements(&digest)
        .map_err(|_| "Blob placement could not be read.".to_owned())?;
    let mut datacenters: Vec<BlobDatacenterPlacement> = records.iter().map(datacenter_placement).collect();
    datacenters.sort_by(|left, right| {
        left.data_center
            .cmp(&right.data_center)
            .then(left.updated_at.cmp(&right.updated_at))
    });
    Ok(BlobPlacementView {
        digest: digest.canonical(),
        datacenters,
    })
}

fn datacenter_placement(record: &BlobPlacementRecord) -> BlobDatacenterPlacement {
    let (status, size) = match record.state {
        BlobPlacementState::Pending => (BlobPlacementStatus::Pending, None),
        BlobPlacementState::Verified { size } => (BlobPlacementStatus::Verified, Some(size)),
        BlobPlacementState::Failed { .. } => (BlobPlacementStatus::Failed, None),
        BlobPlacementState::Revoked => (BlobPlacementStatus::Revoked, None),
    };
    BlobDatacenterPlacement {
        data_center: record.key.data_center.as_str().to_owned(),
        status,
        size,
        updated_at: record.updated_at_unix,
    }
}

fn placement_row(row: ArtifactPlacementRow) -> PlacementRow {
    PlacementRow {
        digest: row.digest,
        source: match row.source {
            ArtifactSource::Hosted => UiArtifactSource::Hosted,
            ArtifactSource::Proxy => UiArtifactSource::Proxy,
            ArtifactSource::Generated => UiArtifactSource::Generated,
        },
        availability: match row.availability {
            ByteAvailability::Local => UiByteAvailability::Local,
            ByteAvailability::RemoteOnly => UiByteAvailability::RemoteOnly,
            ByteAvailability::Unavailable => UiByteAvailability::Unavailable,
        },
    }
}
