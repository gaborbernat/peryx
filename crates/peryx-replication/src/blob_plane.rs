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
use peryx_identity::ArtifactDigest;
use peryx_storage::blob::{BlobStorage, Digest};
use peryx_storage::meta::{ArtifactSource, MetaStore};

use crate::blob::BlobTransport;
use crate::blob_availability::{BlobAvailability, ReferencedBlob, blob_availability};
use crate::blob_fetch::{FetchOutcome, FetchReport, fetch_missing};
use crate::blob_placement::{FetchPlan, plan_blob_fetch};
use crate::blob_pull::{PullError, pull_ranged_blob};
use crate::error::SyncError;
use crate::protocol::{PlacementAvailability, PlacementDescriptor};

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

/// The transports one blob-plane pass may draw bytes from.
///
/// `simple` is the whole-blob transport every single-source blob falls through to, the digest-verified
/// whole-blob path the plane has always used. `delegates` maps a data center to its ranged transport and
/// `local_dc` names this replica's own data center, so a same-datacenter source is preferred: a blob whose
/// verified placements resolve to two or more distinct delegates is pulled as a multi-source ranged fetch
/// instead. `delegates` is empty until placement descriptors ride in the replicated page, so today every
/// blob is single-source and takes the whole-blob path; once they arrive a multi-placement blob draws its
/// ranges across the peers that hold it.
pub struct BlobSources<'a, T> {
    /// The whole-blob transport for a single-source blob.
    pub simple: &'a T,
    /// The per-data-center ranged transports a multi-source blob draws from.
    pub delegates: &'a HashMap<String, T>,
    /// This replica's own data center, preferred when a blob has a local placement.
    pub local_dc: &'a str,
}

/// Pull every blob the journal tail after the current [`BLOB_VIEW`] frontier references that this replica
/// still lacks.
///
/// A multi-placement blob is drawn as ranges across the peers that hold it; a single-source blob is drawn
/// whole. This is the self-healing counterpart to [`pull_referenced`]: it derives the outstanding set from
/// durable state — the same bounded journal tail [`advance_blob_frontier`] examines — rather than the
/// page a single cycle produced. So a blob that back-pressured or failed on an earlier cycle is retried
/// on a later one, and a restarted replica re-derives the set from the journal with no lost in-memory
/// pending.
///
/// Each absent blob is classified from its advertised placements: one whose verified placements resolve to
/// two or more distinct [`delegates`](BlobSources::delegates) is fetched as a ranged multi-source pull
/// ([`pull_ranged_blob`]) that falls through source to source and verifies the reassembled whole; every
/// other blob defers to [`pull_referenced`] over [`simple`](BlobSources::simple). The trusted journal size
/// bounds the ranged reassembly's pre-allocation, never a peer advertisement. Only verified whole blobs
/// are committed, so a ranged fetch that a peer corrupts fails closed rather than landing bad bytes.
///
/// # Errors
/// The same failures as [`pull_referenced`], a terminal ranged-fetch loss ([`SyncError::BlobFetchFailed`]),
/// or a store error reading the journal tail or a blob's placements.
pub async fn pull_outstanding<T: BlobTransport>(
    sources: &BlobSources<'_, T>,
    meta: &MetaStore,
    blobs: &BlobStorage,
    batch: NonZeroUsize,
    concurrency: NonZeroUsize,
) -> Result<BlobPlaneReport, SyncError> {
    let referenced = referenced_over_tail(meta, batch)?;
    let mut simple = Vec::new();
    let mut ranged = Vec::new();
    for (digest, size) in &referenced {
        if blobs.head(digest).await?.is_some() {
            continue;
        }
        let plan = planned_source_dcs(meta, digest, sources)?;
        if plan.len() >= 2 {
            ranged.push((digest.clone(), *size, plan));
        } else {
            simple.push((digest.clone(), *size));
        }
    }
    let mut report = pull_referenced(sources.simple, blobs, meta, &simple, concurrency).await?;
    for (digest, size, plan) in ranged {
        pull_one_ranged(meta, blobs, sources, &digest, size, &plan, &mut report).await?;
    }
    Ok(report)
}

/// The ordered, distinct data centers a ranged pull of `digest` should draw from: its verified placements
/// planned by [`plan_blob_fetch`], kept only where a [`delegate`](BlobSources::delegates) transport can
/// reach them, and deduplicated so two placements in one data center count as one source. A digest with no
/// advertised placement, an unverified set, or no reachable delegate yields an empty plan and falls to the
/// whole-blob path.
fn planned_source_dcs<T>(
    meta: &MetaStore,
    digest: &Digest,
    sources: &BlobSources<'_, T>,
) -> Result<Vec<String>, SyncError> {
    let artifact = ArtifactDigest::from_sha256(digest.as_str()).expect("a journal digest is canonical sha256 hex");
    let placements = meta.blob_placements(&artifact)?;
    let descriptors: Vec<PlacementDescriptor> = placements.iter().map(PlacementDescriptor::from).collect();
    let FetchPlan::Sources(ordered) = plan_blob_fetch(&descriptors, sources.local_dc) else {
        return Ok(Vec::new());
    };
    let mut dcs = Vec::new();
    for descriptor in ordered {
        if sources.delegates.contains_key(&descriptor.data_center) && !dcs.contains(&descriptor.data_center) {
            dcs.push(descriptor.data_center);
        }
    }
    Ok(dcs)
}

/// Drive one ranged multi-source pull and fold its result into `report`. A source exhaustion leaves the
/// blob [pending](BlobPlaneReport::pending) for a later pass; a length or verification failure is a
/// terminal [`SyncError::BlobFetchFailed`] the caller records and retries.
async fn pull_one_ranged<T: BlobTransport>(
    meta: &MetaStore,
    blobs: &BlobStorage,
    sources: &BlobSources<'_, T>,
    digest: &Digest,
    size: u64,
    dcs: &[String],
    report: &mut BlobPlaneReport,
) -> Result<(), SyncError> {
    let transports: Vec<&T> = dcs.iter().filter_map(|dc| sources.delegates.get(dc)).collect();
    // The trusted journal size bounds the reassembly pre-allocation, never a peer advertisement.
    let total_length = usize::try_from(size).expect("a blob fits addressable memory");
    let reason = match pull_ranged_blob(&transports, digest, total_length).await {
        Ok(bytes) => {
            commit_blob(blobs, meta, digest, size, bytes.to_vec()).await?;
            report.fetched += 1;
            return Ok(());
        }
        // Every source lost the range: retryable, so the blob is left for a later pass.
        Err(PullError::Exhausted { .. }) => {
            report.pending += 1;
            return Ok(());
        }
        // A source served the wrong number of bytes for a range, or the reassembled whole did not verify:
        // fail closed rather than committing bytes a peer corrupted.
        Err(PullError::Piece(_)) => "range_length_mismatch",
        Err(PullError::Reassembly(_)) => "reassembly_failed",
    };
    Err(SyncError::BlobFetchFailed {
        reason,
        digest: digest.as_str().to_owned(),
    })
}

/// The `(digest, size)` set every journal record after the current [`BLOB_VIEW`] frontier references,
/// bounded to `batch` records. A digest that cannot parse is left out: it cannot be pulled, and
/// [`advance_blob_frontier`] holds the frontier below it regardless.
fn referenced_over_tail(meta: &MetaStore, batch: NonZeroUsize) -> Result<Vec<(Digest, u64)>, SyncError> {
    let frontier = meta.view_frontier(BLOB_VIEW)?.unwrap_or(0);
    let (_authority, records) = meta.journal_page_after(frontier, batch.get())?;
    let mut referenced = Vec::new();
    for record in &records {
        for blob in &record.blobs {
            if let Some(digest) = Digest::from_hex(&blob.sha256) {
                referenced.push((digest, blob.size));
            }
        }
    }
    Ok(referenced)
}

/// Advance the [`BLOB_VIEW`] frontier as far as this replica's local blobs allow, and return the serial
/// it reached.
///
/// The frontier re-derives from durable state alone: it reads the journal tail after the current
/// [`BLOB_VIEW`] frontier, bounded to `batch` records, and folds each referenced blob's local presence
/// through [`blob_availability`]. No separate durable cursor is kept, so a restart recomputes the same
/// serial from the journal and the blob store. The `batch` bound caps the work: a persistently-missing
/// blob pins the frontier at that serial at `O(batch)` cost per pass rather than an unbounded rescan, and
/// the frontier simply advances one batch at a time when the plane keeps up. It moves only the frontier,
/// committing no bytes and no metadata.
///
/// # Errors
/// Returns a store error reading the journal, probing blob presence, or writing the frontier.
pub async fn advance_blob_frontier(
    meta: &MetaStore,
    blobs: &BlobStorage,
    batch: NonZeroUsize,
) -> Result<u64, SyncError> {
    let frontier = meta.view_frontier(BLOB_VIEW)?.unwrap_or(0);
    let (_authority, records) = meta.journal_page_after(frontier, batch.get())?;
    // The bounded batch is the ceiling this pass can reach: the frontier advances at most to the highest
    // serial examined, so serials past the batch are left for a later pass rather than assumed backed.
    let batch_end = records.last().map_or(frontier, |record| record.serial);
    let mut referenced = Vec::new();
    for record in &records {
        for blob in &record.blobs {
            let present_locally = match Digest::from_hex(&blob.sha256) {
                Some(digest) => blobs.head(&digest).await?.is_some(),
                // A journal digest that cannot parse cannot be served, so it holds the frontier closed.
                None => false,
            };
            referenced.push(ReferencedBlob {
                serial: record.serial,
                digest: blob.sha256.clone(),
                // #830: a whole-blob #826 page advertises no placement, so a referenced blob is treated as
                // Verified and only local presence gates it; when placement descriptors ride in the page,
                // the real advertised availability replaces this.
                availability: PlacementAvailability::Verified,
                present_locally,
            });
        }
    }
    let BlobAvailability { serial, .. } = blob_availability(batch_end, &referenced);
    meta.set_view_frontier(BLOB_VIEW, serial)?;
    Ok(serial)
}

/// Commit one verified blob's bytes under its digest and record it locally present.
async fn commit_blob(
    blobs: &BlobStorage,
    meta: &MetaStore,
    digest: &Digest,
    size: u64,
    bytes: Vec<u8>,
) -> Result<(), SyncError> {
    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual != size {
        return Err(SyncError::BlobSizeMismatch {
            digest: digest.as_str().to_owned(),
            expected: size,
            actual,
        });
    }
    let mut write = blobs.begin().await?;
    write.write_chunk(Bytes::from(bytes)).await?;
    // The commit now returns a durable placement receipt, proof the bytes are digest-verified, synced, and
    // atomically published past the filesystem boundary. Quorum and cross-DC placement consume that
    // evidence downstream; here the local durable copy only needs recording as present.
    write.commit(digest).await?;
    // A replicated blob is resupply-able from the primary, so it records under the resupply-able source
    // (`Proxy` projects `RemoteOnly` when its bytes are absent, not `Unavailable`). A whole-blob #826 page
    // carries no per-artifact origin, so the artifact's true source is unknown here; #830 placement
    // descriptors riding in the page will supply it. `present = true` flips availability to `Local`.
    meta.record_artifact_placement(digest.as_str(), ArtifactSource::Proxy, true)?;
    Ok(())
}
