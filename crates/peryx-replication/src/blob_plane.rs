//! The async whole-blob plane: pull the blobs a metadata page referenced but this replica lacks, commit
//! their verified bytes, and record their local presence.
//!
//! [`sync_metadata`](crate::replica::Replica::sync_metadata) commits a page's metadata ahead of its
//! bytes and hands back the referenced `(digest, size)` set. This drives that set to ground: it skips the
//! blobs already present, pulls the rest whole through [`fetch_missing`], commits each verified blob, and
//! flips its [`ArtifactPlacement`](peryx_storage::meta::ArtifactPlacement) to
//! [`Local`](peryx_storage::meta::ByteAvailability::Local). It touches no metadata and moves no frontier:
//! advancing [`BLOB_VIEW`] once the bytes are down is the loop's job, gated by the blob-availability
//! frontier so a serial stays out of the readable frontier until its blobs are here.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use bytes::Bytes;
use peryx_storage::blob::{BlobStorage, Digest};
use peryx_storage::meta::{ArtifactSource, MetaStore};

use crate::blob::BlobTransport;
use crate::blob_fetch::{FetchOutcome, FetchReport, fetch_missing};
use crate::error::SyncError;

/// The derived-view name whose frontier tracks how far a replica's metadata is backed by blobs it holds.
///
/// The loop advances this with [`set_view_frontier`](peryx_storage::meta::MetaStore::set_view_frontier)
/// and the driver's readable frontier gates visibility on it alongside the search view, so a metadata
/// serial is not exposed until its blobs are present.
pub const BLOB_VIEW: &str = "blob";

/// What one blob-plane pass made of the blobs a page referenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobPlaneReport {
    /// How many absent blobs this pass fetched, committed, and marked local.
    pub fetched: usize,
    /// How many blobs a retryable loss left for a later pass, so the caller retries without failing.
    pub pending: usize,
}

/// Pull every referenced blob this replica lacks, commit it, and record it local.
///
/// A blob already present is skipped: its bytes are down, so its placement is already
/// [`Local`](peryx_storage::meta::ByteAvailability::Local). The absent ones are pulled whole through
/// [`fetch_missing`], which digest-verifies each in transport, and each fetched blob is committed under
/// its digest and its placement flipped to local. A retryable loss leaves the affected blobs
/// [pending](BlobPlaneReport::pending) for the next pass; a terminal loss on a whole-blob fetch is a real
/// failure the caller records and retries, not a silent skip — the frontier holds that serial back until
/// the byte lands regardless.
///
/// # Errors
/// [`SyncError::BlobFetchFailed`] on a terminal fetch loss, [`SyncError::BlobSizeMismatch`] when a
/// fetched blob is not its referenced size, or a store error committing bytes or recording placement.
pub async fn pull_referenced<T: BlobTransport>(
    transport: &T,
    blobs: &BlobStorage,
    meta: &MetaStore,
    referenced: &[(Digest, u64)],
    concurrency: NonZeroUsize,
) -> Result<BlobPlaneReport, SyncError> {
    let mut absent = Vec::new();
    for (digest, size) in referenced {
        if blobs.head(digest).await?.is_none() {
            absent.push((digest.clone(), *size));
        }
    }
    if absent.is_empty() {
        return Ok(BlobPlaneReport { fetched: 0, pending: 0 });
    }
    let digests: Vec<Digest> = absent.iter().map(|(digest, _)| digest.clone()).collect();
    let FetchReport { fetched, outcome } = fetch_missing(transport, &digests, concurrency).await;
    let fetched_count = fetched.len();
    let mut bytes_by_digest: HashMap<Digest, Vec<u8>> = fetched.into_iter().collect();
    // Committing off `absent` keeps each blob paired with the size its reference declared. A retryable
    // pass leaves some absent blobs unfetched, so a digest with no bytes here is simply skipped.
    for (digest, size) in &absent {
        if let Some(bytes) = bytes_by_digest.remove(digest) {
            commit_blob(blobs, meta, digest, *size, bytes).await?;
        }
    }
    match outcome {
        FetchOutcome::Complete => Ok(BlobPlaneReport {
            fetched: fetched_count,
            pending: 0,
        }),
        FetchOutcome::Backpressured { pending } => Ok(BlobPlaneReport {
            fetched: fetched_count,
            pending,
        }),
        FetchOutcome::Failed { reason, digest } => Err(SyncError::BlobFetchFailed {
            reason,
            digest: digest.as_str().to_owned(),
        }),
    }
}

/// Commit one verified blob's bytes under its digest and record it locally present.
async fn commit_blob(
    blobs: &BlobStorage,
    meta: &MetaStore,
    digest: &Digest,
    size: u64,
    bytes: Vec<u8>,
) -> Result<(), SyncError> {
    if bytes.len() as u64 != size {
        return Err(SyncError::BlobSizeMismatch {
            digest: digest.as_str().to_owned(),
            expected: size,
            actual: bytes.len() as u64,
        });
    }
    let mut write = blobs.begin().await?;
    write.write_chunk(Bytes::from(bytes)).await?;
    write.commit(digest).await?;
    // A replicated blob is resupply-able from the primary, so it records under the resupply-able source
    // (`Proxy` projects `RemoteOnly` when its bytes are absent, not `Unavailable`). A whole-blob #826 page
    // carries no per-artifact origin, so the artifact's true source is unknown here; #830 placement
    // descriptors riding in the page will supply it. `present = true` flips availability to `Local`.
    meta.record_artifact_placement(digest.as_str(), ArtifactSource::Proxy, true)?;
    Ok(())
}
