//! The reclamation executor: deleting a blob's bytes once the selector has proved it safe.
//!
//! The selector in [`meta::reclamation`](crate::meta) records a durable tombstone and promotes a
//! digest to [`Ready`](crate::meta::ReclamationState::Ready) only once no metadata references it, no
//! replica still needs it, and every plane has caught up. This module carries out the deletion the
//! tombstone authorizes: given a caller that holds the job lease at a fencing epoch and a digest
//! already `Ready` under that fence, it removes the bytes through a [`BlobBackend`](crate::blob), then
//! credits quota and clears the tombstone.
//!
//! It never re-derives references. The `Ready` tombstone is the proof, produced under the fence the
//! caller still holds, so the executor consults it rather than re-running the reference scan and
//! reopening the read-then-delete window the selector closed. The lease and the epoch arrive as
//! parameters; the orchestrator that mints the epoch, holds the lease, and schedules this work lives
//! above, in the still-unbuilt cluster-authority layer. No metadata transaction spans the delete: each
//! metadata read and write commits on its own, and the backend call sits between them.

use peryx_identity::ArtifactDigest;
use uuid::Uuid;

use crate::blob::{BlobBackend, BlobError, Digest};
use crate::meta::{LeaseState, MetaError, MetaStore, QuotaError, ReclamationError, ReclamationState};

/// What a caller supplies to reclaim one `Ready` digest.
#[derive(Debug, Clone, Copy)]
pub struct ReclaimRequest<'a> {
    pub digest: &'a ArtifactDigest,
    /// The job lease the caller holds, which authorizes this reclamation.
    pub job: &'a str,
    pub holder: &'a str,
    /// The epoch the caller holds the lease under; the tombstone is fenced against it.
    pub epoch: u64,
    /// The committed quota reservations to credit once the bytes are gone, empty when the blob carried
    /// none. Each is released at most once across retries.
    pub reservations: &'a [Uuid],
}

/// The effect of a reclamation attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimOutcome {
    /// The bytes were removed, or proved already absent, and the tombstone cleared. `deleted` is
    /// whether a blob was present; `credited` is how many reservations this call released.
    Reclaimed { deleted: bool, credited: usize },
    /// The tombstone had been abandoned; it was pruned and no bytes were touched.
    Abandoned,
    /// The tombstone is still pending its frontiers, so nothing was done.
    NotReady,
}

/// A rejected reclamation.
#[derive(Debug, thiserror::Error)]
pub enum ReclaimError {
    #[error(transparent)]
    Store(#[from] MetaError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Quota(#[from] QuotaError),
    #[error(transparent)]
    Reclamation(#[from] ReclamationError),
    #[error("the caller does not hold the {job:?} lease at epoch {epoch}")]
    NotLeaseHolder { job: String, epoch: u64 },
    #[error("no reclamation candidate exists for this digest")]
    MissingCandidate,
    #[error("a newer fence {current} owns the candidate, superseding the applied fence {applied}")]
    Superseded { current: u64, applied: u64 },
}

/// Reclaim the bytes of a digest the selector has promoted to `Ready`.
///
/// The caller must hold `request.job`'s lease as `request.holder` at `request.epoch`; otherwise this
/// rejects with [`NotLeaseHolder`](ReclaimError::NotLeaseHolder) without touching anything. A tombstone
/// a newer epoch owns rejects with [`Superseded`](ReclaimError::Superseded). A `Pending` tombstone
/// returns [`NotReady`](ReclaimOutcome::NotReady); a `Skipped` one is pruned and returns
/// [`Abandoned`](ReclaimOutcome::Abandoned). A `Ready` tombstone drives the deletion: the backend
/// removes the bytes (a missing blob counts as an idempotent success), each reservation is credited
/// once, and the tombstone is cleared under the fence.
///
/// # Errors
/// Returns [`ReclaimError`] when the caller does not hold the lease, the candidate is missing or
/// superseded, the backend delete fails, or a metadata read or write fails.
///
/// # Panics
/// Panics if the digest is not canonical sha256 hex, which its [`ArtifactDigest`] type guarantees.
pub async fn reclaim_ready_blob<B: BlobBackend>(
    meta: &MetaStore,
    backend: &B,
    request: ReclaimRequest<'_>,
) -> Result<ReclaimOutcome, ReclaimError> {
    let holds_lease = meta.job_lease(request.job)?.is_some_and(|lease| {
        matches!(lease.state, LeaseState::Held) && lease.holder == request.holder && lease.epoch == request.epoch
    });
    if !holds_lease {
        return Err(ReclaimError::NotLeaseHolder {
            job: request.job.to_owned(),
            epoch: request.epoch,
        });
    }
    let Some(tombstone) = meta.reclamation_tombstone(request.digest)? else {
        return Err(ReclaimError::MissingCandidate);
    };
    if tombstone.fence > request.epoch {
        return Err(ReclaimError::Superseded {
            current: tombstone.fence,
            applied: request.epoch,
        });
    }
    match tombstone.state {
        ReclamationState::Pending => return Ok(ReclaimOutcome::NotReady),
        ReclamationState::Skipped { .. } => {
            meta.forget_reclamation_tombstone(request.digest, request.epoch)?;
            return Ok(ReclaimOutcome::Abandoned);
        }
        ReclamationState::Ready => {}
    }
    let blob = Digest::from_hex(request.digest.sha256()).expect("an ArtifactDigest is canonical sha256 hex");
    let deleted = backend.delete(blob).await?;
    let mut credited = 0;
    for &reservation in request.reservations {
        if meta.release_quota_reservation(reservation)? {
            credited += 1;
        }
    }
    meta.forget_reclamation_tombstone(request.digest, request.epoch)?;
    Ok(ReclaimOutcome::Reclaimed { deleted, credited })
}
