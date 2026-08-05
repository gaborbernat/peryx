//! Selecting which datacenter targets a verified filesystem artifact must be copied to.
//!
//! Local-filesystem HA keeps a configured set of datacenters each holding a copy of an artifact. A
//! blob's advertised placements say which datacenters already hold it and at what generation; the
//! background copier reads those against the configured targets and plans the copies still owed. This
//! is the pure selection: it reads the advertisements, the configured targets, and the committed
//! placement generation, and returns the targets to copy to, that no source can serve the copy, or that
//! every target already holds it.
//!
//! Generation fences the plan. Metadata advances a blob's placement generation when it supersedes the
//! bytes, so a placement below the committed generation is stale: it counts neither as a source to copy
//! from nor as a target that already holds the current bytes, and a target whose only copy is stale is
//! still owed a fresh one. Only a [`Verified`](PlacementAvailability::Verified) placement at the
//! committed generation is a real source or a satisfied target.
//!
//! This is the pure core. Streaming the bytes over a blob transport, staging and atomically publishing
//! them with a placement receipt, bounding concurrency and bandwidth, and retrying under the worker
//! runtime are the copier's deferred wiring.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;

use futures_util::StreamExt as _;
use peryx_storage::blob::{BlobError, BlobStore, Digest};

use crate::blob::{BlobRequest, BlobTransport};
use crate::peer::TransportError;
use crate::protocol::{PlacementAvailability, PlacementDescriptor};

/// The datacenter copies a background transfer should make, or why it makes none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyPlan {
    /// The target datacenters still owed a copy, deduplicated and in a deterministic order.
    Targets(Vec<String>),
    /// No placement is a verified source at the committed generation, so the copy waits for one.
    NoSource,
    /// Every configured target already holds a verified copy at the committed generation.
    Complete,
}

/// Plan the background datacenter copies for a blob from its advertised `placements` to `targets`,
/// fenced to `committed_generation`.
///
/// Only a [`Verified`](PlacementAvailability::Verified) placement at `committed_generation` counts: it
/// is a source to copy from and, for a target datacenter, proof the target already holds the current
/// bytes. With no such source the plan is [`CopyPlan::NoSource`]. Otherwise the plan is the configured
/// targets that hold no current verified copy, deduplicated and ordered so two runs over the same
/// advertisements plan the same order; when none remain the plan is [`CopyPlan::Complete`].
#[must_use]
pub fn plan_dc_copy(placements: &[PlacementDescriptor], targets: &[String], committed_generation: u64) -> CopyPlan {
    let is_current_source = |placement: &PlacementDescriptor| {
        placement.generation == committed_generation && placement.availability == PlacementAvailability::Verified
    };
    if !placements.iter().any(is_current_source) {
        return CopyPlan::NoSource;
    }
    let held: BTreeSet<&str> = placements
        .iter()
        .filter(|&placement| is_current_source(placement))
        .map(|placement| placement.data_center.as_str())
        .collect();
    let owed: BTreeSet<String> = targets
        .iter()
        .filter(|target| !held.contains(target.as_str()))
        .cloned()
        .collect();
    if owed.is_empty() {
        CopyPlan::Complete
    } else {
        CopyPlan::Targets(owed.into_iter().collect())
    }
}

/// A failure copying one blob to one target datacenter.
#[derive(Debug, thiserror::Error)]
pub enum CopyError {
    /// The source could not serve the blob's bytes.
    #[error("copy source could not serve the blob: {0}")]
    Fetch(#[source] TransportError),
    /// The target could not durably publish the fetched bytes.
    #[error("copy target could not publish the blob: {0}")]
    Publish(#[source] BlobError),
    /// The plan owed a copy to a datacenter with no configured target store.
    #[error("no target store is configured for datacenter {data_center}")]
    Unconfigured { data_center: String },
}

/// Copy `digest` from `source` to `target`, verifying and durably publishing the bytes.
///
/// Fetches the whole blob under the transport's byte cap, which a whole-blob fetch digest-verifies, then
/// writes it through [`write_verified`](BlobStore::write_verified), which re-checks the digest and
/// publishes with an atomic filesystem operation, so a target never exposes bytes that do not match the
/// digest and a partial write never reaches a served path.
///
/// # Errors
/// Returns [`CopyError::Fetch`] when the source cannot serve the blob and [`CopyError::Publish`] when the
/// target cannot durably write it.
pub async fn copy_blob_to_target(
    source: &(dyn BlobTransport + Sync),
    target: &BlobStore,
    digest: &Digest,
) -> Result<(), CopyError> {
    let bytes = source
        .fetch_blob(BlobRequest {
            digest: digest.clone(),
            range: None,
        })
        .await
        .map_err(CopyError::Fetch)?;
    target.write_verified(&bytes, digest).map_err(CopyError::Publish)
}

/// One target datacenter's copy result.
#[derive(Debug)]
pub struct TargetCopy {
    pub data_center: String,
    pub outcome: Result<(), CopyError>,
}

/// Copy `digest` to every datacenter the `plan` owes, resolving each target's store through `stores`, at
/// most `max_concurrent` copies in flight.
///
/// A [`CopyPlan::NoSource`] or [`CopyPlan::Complete`] plan owes no copy and yields no results. Each owed
/// target is copied independently, so one target's failure never abandons the rest, and a target with no
/// configured store fails as [`CopyError::Unconfigured`] rather than silently vanishing. The results come
/// back ordered by datacenter so two runs over the same plan report the same order.
pub async fn run_dc_copy<S: std::hash::BuildHasher + Sync>(
    plan: &CopyPlan,
    source: &(dyn BlobTransport + Sync),
    stores: &HashMap<String, BlobStore, S>,
    digest: &Digest,
    max_concurrent: NonZeroUsize,
) -> Vec<TargetCopy> {
    let CopyPlan::Targets(owed) = plan else {
        return Vec::new();
    };
    let mut results: Vec<TargetCopy> = futures_util::stream::iter(owed.iter().cloned())
        .map(|data_center| async move {
            let outcome = match stores.get(&data_center) {
                Some(target) => copy_blob_to_target(source, target, digest).await,
                None => Err(CopyError::Unconfigured {
                    data_center: data_center.clone(),
                }),
            };
            TargetCopy { data_center, outcome }
        })
        .buffer_unordered(max_concurrent.get())
        .collect()
        .await;
    results.sort_by(|a, b| a.data_center.cmp(&b.data_center));
    results
}
